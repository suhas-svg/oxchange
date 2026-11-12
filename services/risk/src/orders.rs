//! PlaceOrder, CancelOrder, GetOrder and ListOrders.

use std::time::Duration;

use domain::{MAX_ORDER_VALUE_CENTS, Price, Qty, Side, Symbol, market_protected_price};
use prost::Message;
use proto_gen::accounts::v1 as apb;
use proto_gen::events::v1 as ev;
use proto_gen::risk::v1 as pb;
use uuid::Uuid;

use crate::accounts_api::{error_code, outcome_unknown};
use crate::{
    AccountsApi, MAX_OPEN_ORDERS, MAX_UNPUBLISHED, OPEN_STATUSES, ORDER_COLUMNS,
    ORDERS_COMMANDS_TOPIC, OrderRow, Result, Risk, RiskError, now_ns,
};

const MAX_CLIENT_ORDER_ID: usize = 64;
const MAX_PAGE: i32 = 100;
const DEFAULT_PAGE: i32 = 50;
/// Attempts at voiding a reservation inside the request before leaving the
/// order for the sweeper.
const VOID_ATTEMPTS: u32 = 3;

/// Reject reason for an order whose reservation result is unknown (spec).
pub(crate) const ACCOUNTS_UNAVAILABLE: &str = "ACCOUNTS_UNAVAILABLE";

fn invalid(msg: impl Into<String>) -> RiskError {
    RiskError::Validation(msg.into())
}

fn parse_uuid(s: &str, what: &str) -> Result<Uuid> {
    Uuid::parse_str(s).map_err(|_| invalid(format!("{what} must be a UUID")))
}

/// The idempotency key that voids an order's reservation. The request path
/// and the sweeper share it, so voiding twice is harmless.
pub(crate) fn void_key(order_id: Uuid) -> String {
    format!("void:{order_id}")
}

pub(crate) fn void_request(order_id: Uuid) -> apb::ReleaseRequest {
    apb::ReleaseRequest {
        order_id: order_id.to_string(),
        idempotency_key: void_key(order_id),
        amount: 0,
        void_if_missing: true,
    }
}

/// A checked order, ready to save.
struct NewOrder {
    user: Uuid,
    client_order_id: String,
    symbol: Symbol,
    side: Side,
    order_type: &'static str,
    tif: &'static str,
    limit: Price,
    qty: Qty,
}

fn command(symbol: &Symbol, body: ev::command::Body) -> Vec<u8> {
    ev::Command {
        command_id: Uuid::now_v7().to_string(),
        symbol: symbol.as_str().to_owned(),
        accepted_at_ns: now_ns(),
        body: Some(body),
    }
    .encode_to_vec()
}

impl<A: AccountsApi> Risk<A> {
    async fn find(&self, user: Uuid, client_order_id: &str) -> Result<Option<OrderRow>> {
        Ok(sqlx::query_as(&format!(
            "SELECT {ORDER_COLUMNS} FROM orders WHERE user_id = $1 AND client_order_id = $2"
        ))
        .bind(user)
        .bind(client_order_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// The last trade price from `ticker:{symbol}`. Without it (Redis down,
    /// or the stock has never traded) market orders are rejected and limit
    /// orders carry on (spec: "Redis → If Redis is lost").
    async fn reference_price(&self, symbol: &Symbol) -> Result<Price> {
        let Some(prices) = &self.prices else {
            return Err(RiskError::Unavailable(
                "market orders are paused: no market data; use a limit order".into(),
            ));
        };
        match prices.read(symbol.as_str()).await {
            Ok(Some(t)) => Price::new(t.last_price_cents)
                .map_err(|_| RiskError::Unavailable("no usable reference price".into())),
            Ok(None) => Err(RiskError::Unavailable(
                "no trades yet, so no reference price for a market order; use a limit order".into(),
            )),
            Err(e) => {
                tracing::warn!(error = %e, "ticker unavailable; market order rejected");
                Err(RiskError::Unavailable(
                    "market orders are paused: market data is unavailable; use a limit order"
                        .into(),
                ))
            }
        }
    }

    /// Spec checks 1 to 5, in order. Check 6 is the reservation itself.
    async fn check(&self, r: &pb::PlaceOrderRequest, user: Uuid) -> Result<NewOrder> {
        let symbol = Symbol::new(&r.symbol).map_err(|_| RiskError::SymbolNotTrading)?;
        let side = match ev::Side::try_from(r.side) {
            Ok(ev::Side::Buy) => Side::Buy,
            Ok(ev::Side::Sell) => Side::Sell,
            _ => return Err(invalid("side must be BUY or SELL")),
        };
        let order_type = ev::OrderType::try_from(r.order_type)
            .ok()
            .filter(|t| *t != ev::OrderType::Unspecified)
            .ok_or_else(|| invalid("type must be LIMIT or MARKET"))?;
        let tif = ev::TimeInForce::try_from(r.time_in_force)
            .ok()
            .filter(|t| *t != ev::TimeInForce::Unspecified);

        // 1. The stock exists and is open for trading.
        let trading: Option<bool> =
            sqlx::query_scalar("SELECT trading FROM symbols WHERE symbol = $1")
                .bind(symbol.as_str())
                .fetch_optional(&self.pool)
                .await?;
        if trading != Some(true) {
            return Err(RiskError::SymbolNotTrading);
        }

        // 2. Price and quantity are within the limits. 5. A market order has
        //    a reference price: the last trade, from Market Data's ticker.
        let qty = Qty::new_order(r.quantity).map_err(|e| invalid(e.to_string()))?;
        let (limit, order_type, tif) = match order_type {
            ev::OrderType::Market => {
                let reference = self.reference_price(&symbol).await?;
                // Market orders always run as IOC (spec: "Order types").
                (market_protected_price(side, reference), "MARKET", "IOC")
            }
            _ => {
                let tif = match tif {
                    Some(ev::TimeInForce::Gtc) => "GTC",
                    Some(_) => "IOC",
                    None => return Err(invalid("time_in_force must be GTC or IOC")),
                };
                let price = Price::new(r.price_cents).map_err(|e| invalid(e.to_string()))?;
                (price, "LIMIT", tif)
            }
        };

        // 4. The order value is at most $1,000,000.
        let value = limit
            .notional(qty)
            .map_err(|_| RiskError::RiskLimit("order value is too large".into()))?;
        if value.get() > MAX_ORDER_VALUE_CENTS {
            return Err(RiskError::RiskLimit(
                "order value is above $1,000,000".into(),
            ));
        }
        Ok(NewOrder {
            user,
            client_order_id: r.client_order_id.clone(),
            symbol,
            side,
            order_type,
            tif,
            limit,
            qty,
        })
    }

    pub async fn place_order(&self, r: &pb::PlaceOrderRequest) -> Result<pb::PlaceOrderResponse> {
        let user = parse_uuid(&r.user_id, "user_id")?;
        let n = r.client_order_id.len();
        if n == 0 || n > MAX_CLIENT_ORDER_ID || r.client_order_id.chars().any(char::is_control) {
            return Err(invalid(format!(
                "client_order_id must be 1 to {MAX_CLIENT_ORDER_ID} characters"
            )));
        }
        // A retry with the same client_order_id returns the original order.
        if let Some(existing) = self.find(user, &r.client_order_id).await? {
            return Ok(pb::PlaceOrderResponse {
                order: Some(existing.to_pb()),
                existing: true,
            });
        }
        let o = self.check(r, user).await?;
        if outbox::unpublished(&self.pool).await? > MAX_UNPUBLISHED {
            return Err(RiskError::Unavailable(
                "too many commands waiting to be published".into(),
            ));
        }

        // Saga step 1: save PENDING_RISK. A per-user lock makes the open-order
        // count exact even when the same user places orders concurrently.
        let order_id = Uuid::now_v7();
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))")
            .bind(user)
            .execute(&mut *tx)
            .await?;
        // 3. The user has fewer than 200 open orders.
        let (open,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM orders WHERE user_id = $1
             AND status IN ('PENDING_RISK', 'ACCEPTED', 'OPEN', 'PARTIALLY_FILLED')",
        )
        .bind(user)
        .fetch_one(&mut *tx)
        .await?;
        if open >= MAX_OPEN_ORDERS {
            return Err(RiskError::RiskLimit(format!(
                "at most {MAX_OPEN_ORDERS} open orders; cancel some first"
            )));
        }
        let side = if o.side == Side::Buy { "BUY" } else { "SELL" };
        let inserted: Option<OrderRow> = sqlx::query_as(&format!(
            "INSERT INTO orders (order_id, user_id, client_order_id, symbol, side, order_type,
                                 time_in_force, limit_price_cents, quantity, status)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'PENDING_RISK')
             ON CONFLICT (user_id, client_order_id) DO NOTHING
             RETURNING {ORDER_COLUMNS}"
        ))
        .bind(order_id)
        .bind(o.user)
        .bind(&o.client_order_id)
        .bind(o.symbol.as_str())
        .bind(side)
        .bind(o.order_type)
        .bind(o.tif)
        .bind(o.limit.cents())
        .bind(o.qty.shares())
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        if inserted.is_none() {
            // A concurrent retry with the same client_order_id won the race.
            let existing = self
                .find(user, &r.client_order_id)
                .await?
                .ok_or(RiskError::NotFound)?;
            return Ok(pb::PlaceOrderResponse {
                order: Some(existing.to_pb()),
                existing: true,
            });
        }

        // Saga step 2: reserve the money (buys) or shares (sells).
        let value = o.limit.cents() * o.qty.shares(); // checked above
        let reserve = apb::ReserveRequest {
            order_id: order_id.to_string(),
            user_id: user.to_string(),
            asset: if o.side == Side::Buy {
                "USD".into()
            } else {
                o.symbol.as_str().into()
            },
            amount: if o.side == Side::Buy {
                value
            } else {
                o.qty.shares()
            },
            limit_price_cents: if o.side == Side::Buy {
                o.limit.cents()
            } else {
                0
            },
        };
        match self.accounts.reserve(reserve).await {
            Ok(_) => {
                // Spec failure test "Exact-moment Risk crash": the funds are
                // reserved but the status save never happens.
                fail::fail_point!(crate::CRASH_AFTER_RESERVE, |arg: Option<String>| {
                    if arg.as_deref() == Some("exit") {
                        crate::crash_now(crate::CRASH_AFTER_RESERVE);
                    }
                    Err(RiskError::Unavailable("crash point after Reserve".into()))
                });
            }
            Err(s) if outcome_unknown(&s) => {
                // The reply may be lost after Reserve worked: void it, which
                // also blocks a delayed Reserve from succeeding later.
                tracing::warn!(%order_id, status = %s, "reserve outcome unknown; voiding");
                if self.void(order_id).await {
                    self.reject(order_id, crate::orders::ACCOUNTS_UNAVAILABLE)
                        .await?;
                }
                // Otherwise the sweeper finishes the void within about 35 s.
                return Err(RiskError::Unavailable(
                    "accounts is unavailable; retry with a new client_order_id".into(),
                ));
            }
            Err(s) => {
                let reason = error_code(&s).to_owned();
                self.reject(order_id, &reason).await?;
                return Err(if reason == "INSUFFICIENT_FUNDS" {
                    RiskError::InsufficientFunds
                } else {
                    RiskError::Rejected(reason)
                });
            }
        }

        // Saga step 3: ACCEPTED and the outbox command, together.
        let mut tx = self.pool.begin().await?;
        let accepted: Option<OrderRow> = sqlx::query_as(&format!(
            "UPDATE orders SET status = 'ACCEPTED', updated_at = now()
             WHERE order_id = $1 AND status = 'PENDING_RISK'
             RETURNING {ORDER_COLUMNS}"
        ))
        .bind(order_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(accepted) = accepted else {
            // The sweeper got here first and voided the reservation.
            return Err(RiskError::Unavailable(
                "order timed out; retry with a new client_order_id".into(),
            ));
        };
        let new_order = ev::command::Body::NewOrder(ev::NewOrder {
            order_id: order_id.to_string(),
            user_id: user.to_string(),
            side: if o.side == Side::Buy {
                ev::Side::Buy
            } else {
                ev::Side::Sell
            } as i32,
            order_type: if o.order_type == "MARKET" {
                ev::OrderType::Market
            } else {
                ev::OrderType::Limit
            } as i32,
            time_in_force: if o.tif == "GTC" {
                ev::TimeInForce::Gtc
            } else {
                ev::TimeInForce::Ioc
            } as i32,
            price_cents: o.limit.cents(),
            quantity: o.qty.shares(),
        });
        outbox::enqueue(
            &mut tx,
            ORDERS_COMMANDS_TOPIC,
            o.symbol.as_str(),
            &command(&o.symbol, new_order),
            &[],
        )
        .await?;
        tx.commit().await?;
        Ok(pb::PlaceOrderResponse {
            order: Some(accepted.to_pb()),
            existing: false,
        })
    }

    /// Tries a few times to void the order's reservation. False means the
    /// sweeper will have to finish it.
    pub(crate) async fn void(&self, order_id: Uuid) -> bool {
        let mut wait = Duration::from_millis(50);
        for attempt in 1..=VOID_ATTEMPTS {
            match self.accounts.release(void_request(order_id)).await {
                Ok(_) => return true,
                Err(s) => {
                    tracing::warn!(%order_id, attempt, status = %s, "void failed");
                    tokio::time::sleep(wait).await;
                    wait *= 2;
                }
            }
        }
        false
    }

    pub(crate) async fn reject(&self, order_id: Uuid, reason: &str) -> Result<()> {
        sqlx::query(
            "UPDATE orders SET status = 'REJECTED', reject_reason = $2, updated_at = now()
             WHERE order_id = $1 AND status = 'PENDING_RISK'",
        )
        .bind(order_id)
        .bind(reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn owned(&self, user: &str, order_id: &str) -> Result<OrderRow> {
        let user = parse_uuid(user, "user_id")?;
        let order_id = parse_uuid(order_id, "order_id")?;
        // Every query filters by the verified user: another user's order is
        // simply not found.
        sqlx::query_as(&format!(
            "SELECT {ORDER_COLUMNS} FROM orders WHERE order_id = $1 AND user_id = $2"
        ))
        .bind(order_id)
        .bind(user)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(RiskError::NotFound)
    }

    /// Asks the engine to cancel. The cancel completes asynchronously: the
    /// engine answers with OrderCanceled, or CancelRejected if the order
    /// already traded fully.
    pub async fn cancel_order(
        &self,
        user: &str,
        order_id: &str,
    ) -> Result<pb::CancelOrderResponse> {
        let order = self.owned(user, order_id).await?;
        if OPEN_STATUSES.contains(&order.status.as_str()) {
            let symbol = Symbol::new(&order.symbol).map_err(|e| invalid(e.to_string()))?;
            let body = ev::command::Body::CancelOrder(ev::CancelOrder {
                order_id: order.order_id.to_string(),
                user_id: order.user_id.to_string(),
            });
            let mut conn = self.pool.acquire().await?;
            outbox::enqueue(
                &mut conn,
                ORDERS_COMMANDS_TOPIC,
                symbol.as_str(),
                &command(&symbol, body),
                &[],
            )
            .await?;
        }
        Ok(pb::CancelOrderResponse {
            order: Some(order.to_pb()),
        })
    }

    pub async fn get_order(&self, user: &str, order_id: &str) -> Result<pb::GetOrderResponse> {
        Ok(pb::GetOrderResponse {
            order: Some(self.owned(user, order_id).await?.to_pb()),
        })
    }

    /// Every listed stock, in symbol order.
    pub async fn list_symbols(&self) -> Result<pb::ListSymbolsResponse> {
        let rows: Vec<(String, String, bool, i64)> = sqlx::query_as(
            "SELECT symbol, name, trading, listing_price_cents FROM symbols ORDER BY symbol",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(pb::ListSymbolsResponse {
            symbols: rows
                .into_iter()
                .map(
                    |(symbol, name, trading, listing_price_cents)| pb::SymbolInfo {
                        symbol,
                        name,
                        trading,
                        listing_price_cents,
                    },
                )
                .collect(),
        })
    }

    pub async fn list_orders(&self, r: &pb::ListOrdersRequest) -> Result<pb::ListOrdersResponse> {
        let user = parse_uuid(&r.user_id, "user_id")?;
        let before = if r.cursor.is_empty() {
            None
        } else {
            Some(parse_uuid(&r.cursor, "cursor")?)
        };
        let page = match r.page_size {
            0 => DEFAULT_PAGE,
            n if (1..=MAX_PAGE).contains(&n) => n,
            _ => return Err(invalid(format!("page_size must be 1 to {MAX_PAGE}"))),
        };
        let open: Vec<&str> = if r.open_only {
            OPEN_STATUSES.to_vec()
        } else {
            Vec::new()
        };
        // Order IDs are UUIDv7, so ID order is creation order.
        let mut rows: Vec<OrderRow> = sqlx::query_as(&format!(
            "SELECT {ORDER_COLUMNS} FROM orders
             WHERE user_id = $1
               AND ($2 = '' OR symbol = $2)
               AND (cardinality($3::text[]) = 0 OR status = ANY($3))
               AND ($4::uuid IS NULL OR order_id < $4)
             ORDER BY order_id DESC LIMIT $5"
        ))
        .bind(user)
        .bind(&r.symbol)
        .bind(&open)
        .bind(before)
        .bind(i64::from(page) + 1)
        .fetch_all(&self.pool)
        .await?;
        let more = rows.len() > page as usize;
        rows.truncate(page as usize);
        Ok(pb::ListOrdersResponse {
            next_cursor: match (more, rows.last()) {
                (true, Some(last)) => last.order_id.to_string(),
                _ => String::new(),
            },
            orders: rows.iter().map(OrderRow::to_pb).collect(),
        })
    }
}
