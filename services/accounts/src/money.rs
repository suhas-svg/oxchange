//! Deposits, withdrawals, reservations, releases and trade settlement.

use domain::{MAX_PRICE_CENTS, MAX_QTY, MIN_PRICE_CENTS, Symbol};
use prost::Message;
use proto_gen::accounts::v1 as pb;
use proto_gen::events::v1 as ev;
use uuid::Uuid;

use crate::ledger::{self, AccountKey, Claim, Kind, Tx};
use crate::{ACCOUNTS_EVENTS_TOPIC, Accounts, AccountsError, Result, now_ns};

pub const USD: &str = "USD";

/// Largest single deposit or withdrawal: $10,000,000.00, or that many shares.
/// Keeps balances far from i64 overflow.
pub const MAX_TRANSFER_UNITS: i64 = 1_000_000_000;

/// (entry id, transaction id, transaction kind, asset, account kind, amount, time)
type LedgerRow = (i64, Uuid, String, String, String, i64, time::OffsetDateTime);

/// (order id, user, asset, limit price, remaining, status)
type ReservationRow = (Uuid, Option<Uuid>, Option<String>, Option<i64>, i64, String);

const MAX_IDEMPOTENCY_KEY_LEN: usize = 128;
const MAX_LEDGER_PAGE: i32 = 100;
const DEFAULT_LEDGER_PAGE: i32 = 50;

fn invalid(msg: impl Into<String>) -> AccountsError {
    AccountsError::Validation(msg.into())
}

/// "USD" (also for an empty string) or a valid stock symbol.
fn parse_asset(asset: &str) -> Result<String> {
    if asset.is_empty() || asset == USD {
        return Ok(USD.to_owned());
    }
    Symbol::new(asset)
        .map(|s| s.as_str().to_owned())
        .map_err(|_| invalid(format!("unknown asset {asset:?}")))
}

fn parse_uuid(s: &str, what: &str) -> Result<Uuid> {
    Uuid::parse_str(s).map_err(|_| invalid(format!("{what} must be a UUID")))
}

fn check_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > MAX_IDEMPOTENCY_KEY_LEN {
        return Err(invalid(format!(
            "idempotency key must be 1 to {MAX_IDEMPOTENCY_KEY_LEN} characters"
        )));
    }
    Ok(())
}

fn check_transfer(amount: i64) -> Result<()> {
    if !(1..=MAX_TRANSFER_UNITS).contains(&amount) {
        return Err(invalid(format!(
            "amount must be between 1 and {MAX_TRANSFER_UNITS}"
        )));
    }
    Ok(())
}

/// Writes an accounts.events message in the same transaction (outbox).
async fn outbox_account_event(tx: &mut Tx<'_>, event: &ev::AccountEvent) -> Result<()> {
    sqlx::query("INSERT INTO outbox (topic, message_key, payload) VALUES ($1, $2, $3)")
        .bind(ACCOUNTS_EVENTS_TOPIC)
        .bind(&event.user_id)
        .bind(event.encode_to_vec())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Direction {
    In,
    Out,
}

impl Accounts {
    pub async fn deposit(
        &self,
        user: &str,
        key: &str,
        asset: &str,
        amount: i64,
    ) -> Result<pb::DepositResponse> {
        let (txn, available) = self
            .transfer(Direction::In, user, key, asset, amount)
            .await?;
        Ok(pb::DepositResponse {
            transaction_id: txn,
            available_cents: available,
        })
    }

    pub async fn withdraw(
        &self,
        user: &str,
        key: &str,
        asset: &str,
        amount: i64,
    ) -> Result<pb::WithdrawResponse> {
        let (txn, available) = self
            .transfer(Direction::Out, user, key, asset, amount)
            .await?;
        Ok(pb::WithdrawResponse {
            transaction_id: txn,
            available_cents: available,
        })
    }

    /// A deposit or withdrawal against the asset's external account, which
    /// stands for money (or shares) outside the system.
    async fn transfer(
        &self,
        dir: Direction,
        user: &str,
        client_key: &str,
        asset: &str,
        amount: i64,
    ) -> Result<(String, i64)> {
        let user_id = parse_uuid(user, "user_id")?;
        let asset = parse_asset(asset)?;
        check_key(client_key)?;
        check_transfer(amount)?;
        let kind = if dir == Direction::In {
            "deposit"
        } else {
            "withdraw"
        };
        // Spec: {kind}:{user_id}:{Idempotency-Key}.
        let key = format!("{kind}:{user_id}:{client_key}");
        let hash = ledger::request_hash(&[&asset, &amount.to_string()]);

        let mut tx = self.pool.begin().await?;
        // Both responses share a shape; store it as a DepositResponse.
        let txn = match ledger::claim::<pb::DepositResponse>(&mut tx, &key, kind, &hash).await? {
            Claim::Replay(r) => return Ok((r.transaction_id, r.available_cents)),
            Claim::New(id) => id,
        };
        let user_avail = AccountKey::user(user_id, &asset, Kind::Available);
        let signed = if dir == Direction::In {
            amount
        } else {
            -amount
        };
        let balances = ledger::post(
            &mut tx,
            txn,
            &[
                (AccountKey::external(&asset), -signed),
                (user_avail.clone(), signed),
            ],
        )
        .await?;
        let available = balances[&user_avail];

        let body = if dir == Direction::In {
            ev::account_event::Body::Deposited(ev::Deposited {
                amount_cents: amount,
                balance_cents: available,
                asset: asset.clone(),
            })
        } else {
            ev::account_event::Body::Withdrawn(ev::Withdrawn {
                amount_cents: amount,
                balance_cents: available,
                asset: asset.clone(),
            })
        };
        outbox_account_event(
            &mut tx,
            &ev::AccountEvent {
                event_id: txn.to_string(),
                user_id: user_id.to_string(),
                event_time_ns: now_ns(),
                body: Some(body),
            },
        )
        .await?;

        let response = pb::DepositResponse {
            transaction_id: txn.to_string(),
            available_cents: available,
        };
        ledger::finish(&mut tx, txn, &response).await?;
        tx.commit().await?;
        Ok((response.transaction_id, available))
    }

    /// Moves `amount` of `asset` from available to reserved for an order.
    pub async fn reserve(&self, r: &pb::ReserveRequest) -> Result<pb::ReserveResponse> {
        let order_id = parse_uuid(&r.order_id, "order_id")?;
        let user_id = parse_uuid(&r.user_id, "user_id")?;
        let asset = parse_asset(&r.asset)?;
        if r.amount <= 0 {
            return Err(invalid("amount must be positive"));
        }
        let limit = match (asset == USD, r.limit_price_cents) {
            (true, p) if (MIN_PRICE_CENTS..=MAX_PRICE_CENTS).contains(&p) => Some(p),
            (true, _) => return Err(invalid("a USD reservation needs its limit price")),
            (false, 0) => None,
            (false, _) => return Err(invalid("a share reservation has no limit price")),
        };
        let key = format!("reserve:{order_id}");
        let hash = ledger::request_hash(&[
            &user_id.to_string(),
            &asset,
            &r.amount.to_string(),
            &r.limit_price_cents.to_string(),
        ]);

        let mut tx = self.pool.begin().await?;
        let txn =
            match ledger::claim::<pb::ReserveResponse>(&mut tx, &key, "reserve", &hash).await? {
                Claim::Replay(resp) => return Ok(resp),
                Claim::New(id) => id,
            };
        let inserted = sqlx::query(
            "INSERT INTO reservations
               (order_id, user_id, asset, limit_price_cents, original_amount, remaining_amount, status)
             VALUES ($1, $2, $3, $4, $5, $5, 'active')
             ON CONFLICT (order_id) DO NOTHING",
        )
        .bind(order_id)
        .bind(user_id)
        .bind(&asset)
        .bind(limit)
        .bind(r.amount)
        .execute(&mut *tx)
        .await
        .map_err(ledger::map_missing_user)?
        .rows_affected();
        if inserted == 0 {
            // The reserve key is new, so the only way a row exists is a Release
            // that arrived first and voided it (late reserve protection).
            return Err(AccountsError::ReservationVoided);
        }
        ledger::post(
            &mut tx,
            txn,
            &[
                (
                    AccountKey::user(user_id, &asset, Kind::Available),
                    -r.amount,
                ),
                (AccountKey::user(user_id, &asset, Kind::Reserved), r.amount),
            ],
        )
        .await?;
        let response = pb::ReserveResponse {
            reserved_amount: r.amount,
        };
        ledger::finish(&mut tx, txn, &response).await?;
        tx.commit().await?;
        Ok(response)
    }

    /// Returns an exact amount from reserved to available. With
    /// `void_if_missing`, releases whatever remains, and voids a reservation
    /// that does not exist yet.
    pub async fn release(&self, r: &pb::ReleaseRequest) -> Result<pb::ReleaseResponse> {
        let order_id = parse_uuid(&r.order_id, "order_id")?;
        check_key(&r.idempotency_key)?;
        if r.amount < 0 || (r.amount == 0 && !r.void_if_missing) {
            return Err(invalid("amount must be positive"));
        }
        let hash = ledger::request_hash(&[
            &order_id.to_string(),
            &r.amount.to_string(),
            &r.void_if_missing.to_string(),
        ]);

        let mut tx = self.pool.begin().await?;
        let txn = match ledger::claim::<pb::ReleaseResponse>(
            &mut tx,
            &r.idempotency_key,
            "release",
            &hash,
        )
        .await?
        {
            Claim::Replay(resp) => return Ok(resp),
            Claim::New(id) => id,
        };

        if r.void_if_missing {
            // Waits for a concurrent Reserve of this order; inserts only if
            // there is still no reservation once that one finishes.
            let voided = sqlx::query(
                "INSERT INTO reservations (order_id, original_amount, remaining_amount, status)
                 VALUES ($1, 0, 0, 'voided') ON CONFLICT (order_id) DO NOTHING",
            )
            .bind(order_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if voided == 1 {
                let response = pb::ReleaseResponse {
                    released_amount: 0,
                    remaining_amount: 0,
                };
                ledger::finish(&mut tx, txn, &response).await?;
                tx.commit().await?;
                return Ok(response);
            }
        }

        let row: Option<(Option<Uuid>, Option<String>, i64, String)> = sqlx::query_as(
            "SELECT user_id, asset, remaining_amount, status FROM reservations
             WHERE order_id = $1 FOR UPDATE",
        )
        .bind(order_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((user, asset, remaining, status)) = row else {
            return Err(AccountsError::NotFound("reservation"));
        };
        let release = if r.void_if_missing {
            remaining
        } else {
            r.amount
        };
        if release > remaining {
            return Err(AccountsError::Invariant(format!(
                "release of {release} exceeds the {remaining} still reserved for order {order_id}"
            )));
        }
        if release > 0 {
            let (Some(user), Some(asset)) = (user, asset) else {
                return Err(AccountsError::Invariant(format!(
                    "{status} reservation {order_id} has no owner"
                )));
            };
            ledger::post(
                &mut tx,
                txn,
                &[
                    (AccountKey::user(user, &asset, Kind::Reserved), -release),
                    (AccountKey::user(user, &asset, Kind::Available), release),
                ],
            )
            .await?;
            update_reservation(&mut tx, order_id, remaining - release).await?;
        }
        let response = pb::ReleaseResponse {
            released_amount: release,
            remaining_amount: remaining - release,
        };
        ledger::finish(&mut tx, txn, &response).await?;
        tx.commit().await?;
        Ok(response)
    }

    /// Moves money and shares for one trade, for both traders, in one
    /// transaction. The buyer's funds were reserved at the limit price, so
    /// the difference to the trade price goes back to the buyer.
    pub async fn settle_trade(
        &self,
        r: &pb::SettleTradeRequest,
    ) -> Result<pb::SettleTradeResponse> {
        if r.trade_id.is_empty() || r.trade_id.len() > MAX_IDEMPOTENCY_KEY_LEN {
            return Err(invalid("trade_id is required"));
        }
        let symbol = Symbol::new(&r.symbol)
            .map_err(|_| invalid("unknown symbol"))?
            .as_str()
            .to_owned();
        if !(MIN_PRICE_CENTS..=MAX_PRICE_CENTS).contains(&r.price_cents) {
            return Err(invalid("price out of range"));
        }
        if !(1..=MAX_QTY).contains(&r.quantity) {
            return Err(invalid("quantity out of range"));
        }
        let buy_order = parse_uuid(&r.buy_order_id, "buy_order_id")?;
        let sell_order = parse_uuid(&r.sell_order_id, "sell_order_id")?;
        let buyer = parse_uuid(&r.buyer_id, "buyer_id")?;
        let seller = parse_uuid(&r.seller_id, "seller_id")?;
        if buyer == seller || buy_order == sell_order {
            return Err(invalid("a trade needs two different traders and orders"));
        }
        let key = format!("settle:{}", r.trade_id);
        let hash = ledger::request_hash(&[
            &symbol,
            &r.price_cents.to_string(),
            &r.quantity.to_string(),
            &buy_order.to_string(),
            &sell_order.to_string(),
            &buyer.to_string(),
            &seller.to_string(),
        ]);

        let mut tx = self.pool.begin().await?;
        let txn =
            match ledger::claim::<pb::SettleTradeResponse>(&mut tx, &key, "settle", &hash).await? {
                Claim::Replay(resp) => return Ok(resp),
                Claim::New(id) => id,
            };

        // Reservations before balances, each in ascending order (deadlock rule).
        let rows: Vec<ReservationRow> = sqlx::query_as(
            "SELECT order_id, user_id, asset, limit_price_cents, remaining_amount, status
                 FROM reservations WHERE order_id = ANY($1) ORDER BY order_id FOR UPDATE",
        )
        .bind(vec![buy_order, sell_order])
        .fetch_all(&mut *tx)
        .await?;
        let find = |id: Uuid| rows.iter().find(|row| row.0 == id);
        let bug = |m: String| AccountsError::Invariant(format!("trade {}: {m}", r.trade_id));

        let Some((_, Some(b_user), Some(b_asset), Some(limit), b_remaining, b_status)) =
            find(buy_order)
        else {
            return Err(bug("buy order has no usable reservation".into()));
        };
        let Some((_, Some(s_user), Some(s_asset), _, s_remaining, s_status)) = find(sell_order)
        else {
            return Err(bug("sell order has no usable reservation".into()));
        };
        if *b_user != buyer || b_asset != USD || b_status != "active" {
            return Err(bug("buy reservation does not match the trade".into()));
        }
        if *s_user != seller || *s_asset != symbol || s_status != "active" {
            return Err(bug("sell reservation does not match the trade".into()));
        }
        if r.price_cents > *limit {
            return Err(bug(format!(
                "price {} is above the buy limit {limit}",
                r.price_cents
            )));
        }
        let overflow = || bug("amount overflows".into());
        let held = limit.checked_mul(r.quantity).ok_or_else(overflow)?;
        let paid = r.price_cents.checked_mul(r.quantity).ok_or_else(overflow)?;
        let improvement = held - paid;
        if held > *b_remaining {
            return Err(bug(format!(
                "needs {held} cents, only {b_remaining} reserved"
            )));
        }
        if r.quantity > *s_remaining {
            return Err(bug(format!(
                "needs {} shares, only {s_remaining} reserved",
                r.quantity
            )));
        }
        let (b_left, s_left) = (b_remaining - held, s_remaining - r.quantity);

        ledger::post(
            &mut tx,
            txn,
            &[
                (AccountKey::user(buyer, USD, Kind::Reserved), -held),
                (AccountKey::user(seller, USD, Kind::Available), paid),
                (AccountKey::user(buyer, USD, Kind::Available), improvement),
                (
                    AccountKey::user(seller, &symbol, Kind::Reserved),
                    -r.quantity,
                ),
                (
                    AccountKey::user(buyer, &symbol, Kind::Available),
                    r.quantity,
                ),
            ],
        )
        .await
        .map_err(|e| match e {
            // Reservations covered the amounts, so this is a ledger bug.
            AccountsError::InsufficientFunds => bug("reserved balance below reservations".into()),
            other => other,
        })?;
        update_reservation(&mut tx, buy_order, b_left).await?;
        update_reservation(&mut tx, sell_order, s_left).await?;

        let response = pb::SettleTradeResponse {
            transaction_id: txn.to_string(),
            price_improvement_cents: improvement,
        };
        ledger::finish(&mut tx, txn, &response).await?;
        tx.commit().await?;
        Ok(response)
    }

    pub async fn balances(&self, user: &str) -> Result<pb::GetBalancesResponse> {
        let user_id = parse_uuid(user, "user_id")?;
        let rows: Vec<(String, i64, i64)> = sqlx::query_as(
            "SELECT asset,
                    COALESCE(SUM(balance) FILTER (WHERE kind = 'available'), 0)::bigint,
                    COALESCE(SUM(balance) FILTER (WHERE kind = 'reserved'), 0)::bigint
             FROM ledger_accounts WHERE owner_id = $1
             GROUP BY asset ORDER BY asset",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(pb::GetBalancesResponse {
            balances: rows
                .into_iter()
                .map(|(asset, available, reserved)| pb::Balance {
                    asset,
                    available,
                    reserved,
                })
                .collect(),
        })
    }

    /// The user's ledger entries, newest first, with cursor paging.
    pub async fn ledger(
        &self,
        user: &str,
        cursor: &str,
        page_size: i32,
    ) -> Result<pb::ListLedgerResponse> {
        let user_id = parse_uuid(user, "user_id")?;
        let before: Option<i64> = if cursor.is_empty() {
            None
        } else {
            Some(cursor.parse().map_err(|_| invalid("bad cursor"))?)
        };
        let page = match page_size {
            0 => DEFAULT_LEDGER_PAGE,
            n if (1..=MAX_LEDGER_PAGE).contains(&n) => n,
            _ => return Err(invalid(format!("page_size must be 1 to {MAX_LEDGER_PAGE}"))),
        };
        let mut rows: Vec<LedgerRow> = sqlx::query_as(
            "SELECT e.id, t.id, t.kind, a.asset, a.kind, e.amount, t.created_at
                 FROM ledger_entries e
                 JOIN ledger_accounts a ON a.id = e.account_id
                 JOIN ledger_transactions t ON t.id = e.transaction_id
                 WHERE a.owner_id = $1 AND ($2::bigint IS NULL OR e.id < $2)
                 ORDER BY e.id DESC
                 LIMIT $3",
        )
        .bind(user_id)
        .bind(before)
        .bind(i64::from(page) + 1)
        .fetch_all(&self.pool)
        .await?;
        let more = rows.len() > page as usize;
        rows.truncate(page as usize);
        let next_cursor = match (more, rows.last()) {
            (true, Some(last)) => last.0.to_string(),
            _ => String::new(),
        };
        Ok(pb::ListLedgerResponse {
            entries: rows
                .into_iter()
                .map(
                    |(_, txn, kind, asset, account_kind, amount, at)| pb::LedgerEntry {
                        transaction_id: txn.to_string(),
                        kind,
                        asset,
                        account_kind,
                        amount,
                        created_at_ns: i64::try_from(at.unix_timestamp_nanos()).unwrap_or(i64::MAX),
                    },
                )
                .collect(),
            next_cursor,
        })
    }
}

async fn update_reservation(tx: &mut Tx<'_>, order_id: Uuid, remaining: i64) -> Result<()> {
    sqlx::query(
        "UPDATE reservations
         SET remaining_amount = $2,
             status = CASE WHEN $2 = 0 THEN 'closed' ELSE 'active' END
         WHERE order_id = $1",
    )
    .bind(order_id)
    .bind(remaining)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
