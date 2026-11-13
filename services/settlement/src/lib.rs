//! Settlement: turns each trade into the final movement of money and shares
//! (spec: "Service specifications → Settlement").
//!
//! For each `Trade` in `matching.events`:
//! 1. Insert the trade as PENDING, ignoring duplicates (together with the
//!    consumer's seq, in one transaction).
//! 2. Call `Accounts.SettleTrade` with the trade ID. Accounts makes it
//!    idempotent by `settle:{trade_id}`, so retries never move money twice.
//! 3. Mark the trade SETTLED and write `TradeSettled` to the outbox, in one
//!    transaction.
//! 4. The consume loop commits the Kafka offset only after that.
//!
//! A crash anywhere repeats from the stored state: a repeated event finds the
//! trade still PENDING and settles it; Accounts returns the first answer.
//! A valid trade always settles, so a refusal from Accounts is a bug: the
//! partition halts instead of skipping the trade.

mod accounts_api;

use consumer::{Claims, Decision, HandlerError};
use prost::Message;
use proto_gen::accounts::v1 as apb;
use proto_gen::events::v1 as ev;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

pub use accounts_api::{AccountsApi, GrpcAccounts, SETTLE_TIMEOUT, outcome_unknown};

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub const MATCHING_EVENTS_TOPIC: &str = "matching.events";
pub const SETTLEMENT_EVENTS_TOPIC: &str = "settlement.events";
/// Advisory lock ID for Settlement's outbox relay ("SETL" in ASCII).
pub const OUTBOX_LOCK_KEY: i64 = 0x5345_544C;

/// Named crash point: Accounts has settled the trade, but it is not yet
/// marked SETTLED (spec failure test "Exact-moment settlement crash").
pub const CRASH_AFTER_SETTLE_TRADE: &str = "settlement::after_settle_trade";

/// What a crash point does when FAILPOINTS sets it to `return(exit)`: the
/// process ends on the spot, with no cleanup, like a real crash. Live
/// failure tests (tools/chaos) use this; tests in this crate use `return`.
pub fn crash_now(point: &str) -> ! {
    eprintln!("crash point {point} fired; exiting");
    std::process::exit(86)
}

pub struct Settlement<A> {
    pool: PgPool,
    accounts: A,
}

fn halt(m: impl Into<String>) -> HandlerError {
    HandlerError::Halt(m.into())
}

/// A broken CHECK constraint means the event contradicts itself: a bug.
fn db_error(e: sqlx::Error) -> HandlerError {
    match &e {
        sqlx::Error::Database(d) if d.code().as_deref() == Some("23514") => {
            halt(format!("trade breaks an invariant: {e}"))
        }
        _ => HandlerError::Retry(e.to_string()),
    }
}

fn uuid(s: &str, what: &str) -> Result<Uuid, HandlerError> {
    Uuid::parse_str(s).map_err(|_| halt(format!("bad {what} {s:?} in trade")))
}

/// One row of `trades`.
type TradeRow = (String, String, i64, i64, Uuid, Uuid, Uuid, Uuid, String);

impl<A: AccountsApi> Settlement<A> {
    pub fn new(pool: PgPool, accounts: A) -> Self {
        Self { pool, accounts }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Handles one `matching.events` message. Only trades matter here, but
    /// every event advances the seq, so a gap is still noticed.
    pub async fn apply_event(&self, e: &ev::MatchingEvent) -> Result<(), HandlerError> {
        self.apply_events(std::slice::from_ref(e)).await
    }

    /// Stores the trades among `events` in one transaction (all or none),
    /// then settles each one in order.
    pub async fn apply_events(&self, events: &[ev::MatchingEvent]) -> Result<(), HandlerError> {
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let mut claims = Claims::default();
        let mut trades = Vec::new();
        for e in events {
            if let Some(t) = self.store_in(&mut tx, &mut claims, e).await? {
                trades.push(t);
            }
        }
        claims.save(&mut tx).await.map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        // Also on a repeat: the first delivery may have crashed before
        // settling, and the trade is still PENDING.
        self.settle_all(&trades).await
    }

    /// Claims one event and stores it if it is a new trade. Returns the
    /// trade to settle, if it is one.
    async fn store_in<'e>(
        &self,
        tx: &mut sqlx::PgConnection,
        claims: &mut Claims,
        e: &'e ev::MatchingEvent,
    ) -> Result<Option<&'e str>, HandlerError> {
        let decision = claims.claim(tx, &e.symbol, e.seq).await.map_err(db_error)?;
        if let Decision::Gap { expected, got } = decision {
            return Err(halt(format!(
                "{}: expected seq {expected}, got {got}; events are missing",
                e.symbol
            )));
        }
        let trade = match &e.body {
            Some(ev::matching_event::Body::Trade(t)) => Some(t),
            Some(_) => None,
            None => return Err(halt(format!("event {} has no body", e.event_id))),
        };
        if let (Decision::Apply, Some(t)) = (decision, trade) {
            let executed_at =
                OffsetDateTime::from_unix_timestamp_nanos(i128::from(e.event_time_ns))
                    .map_err(|_| halt(format!("trade {} has a bad time", t.trade_id)))?;
            sqlx::query(
                "INSERT INTO trades (trade_id, symbol, seq, price_cents, quantity, buy_order_id,
                   sell_order_id, buyer_id, seller_id, status, executed_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'PENDING', $10)
                 ON CONFLICT (trade_id) DO NOTHING",
            )
            .bind(&t.trade_id)
            .bind(&e.symbol)
            .bind(i64::try_from(e.seq).map_err(|_| halt("seq out of range"))?)
            .bind(t.price_cents)
            .bind(t.quantity)
            .bind(uuid(&t.buy_order_id, "buy_order_id")?)
            .bind(uuid(&t.sell_order_id, "sell_order_id")?)
            .bind(uuid(&t.buyer_id, "buyer_id")?)
            .bind(uuid(&t.seller_id, "seller_id")?)
            .bind(executed_at)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        }
        Ok(trade.map(|t| t.trade_id.as_str()))
    }

    /// Settles one stored trade if it is still PENDING.
    pub async fn settle(&self, trade_id: &str) -> Result<(), HandlerError> {
        self.settle_all(&[trade_id]).await
    }

    /// Settles stored trades, in order, that are still PENDING: one
    /// `SettleTrade` call each, then one transaction marks them SETTLED and
    /// queues their `TradeSettled` messages. If a call fails, the trades
    /// settled before it are still marked, and the error is returned.
    async fn settle_all(&self, trade_ids: &[&str]) -> Result<(), HandlerError> {
        if trade_ids.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = trade_ids.iter().map(|&t| t.to_owned()).collect();
        let rows: Vec<TradeRow> = sqlx::query_as(
            "SELECT symbol, status, price_cents, quantity, buy_order_id, sell_order_id,
                    buyer_id, seller_id, trade_id
             FROM trades WHERE trade_id = ANY($1)",
        )
        .bind(&ids)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let mut done = Vec::with_capacity(trade_ids.len());
        let mut failed = None;
        for &trade_id in trade_ids {
            let Some(row) = rows.iter().find(|r| r.8 == trade_id) else {
                // Its seq was applied, so the row must exist.
                failed = Some(halt(format!("trade {trade_id} was never stored")));
                break;
            };
            if row.1 == "SETTLED" || done.iter().any(|r: &&TradeRow| r.8 == trade_id) {
                continue;
            }
            if let Err(e) = self.call_settle(row).await {
                failed = Some(e);
                break;
            }
            done.push(row);
        }
        self.mark_settled(&done).await?;
        failed.map_or(Ok(()), Err)
    }

    async fn call_settle(&self, row: &TradeRow) -> Result<(), HandlerError> {
        let (symbol, _, price, qty, buy_order, sell_order, buyer, seller, trade_id) = row;
        let req = apb::SettleTradeRequest {
            trade_id: trade_id.clone(),
            symbol: symbol.clone(),
            price_cents: *price,
            quantity: *qty,
            buy_order_id: buy_order.to_string(),
            sell_order_id: sell_order.to_string(),
            buyer_id: buyer.to_string(),
            seller_id: seller.to_string(),
        };
        if let Err(s) = self.accounts.settle_trade(req).await {
            // Internal is how Accounts reports a broken ledger invariant.
            return Err(
                if outcome_unknown(&s) && s.code() != tonic::Code::Internal {
                    HandlerError::Retry(format!("settling {trade_id}: {s}"))
                } else {
                    // Funds were reserved before trading, so a valid trade always
                    // settles. Never skip it: stop and alert.
                    halt(format!("Accounts refused to settle {trade_id}: {s}"))
                },
            );
        }
        fail::fail_point!(CRASH_AFTER_SETTLE_TRADE, |arg: Option<String>| {
            if arg.as_deref() == Some("exit") {
                crash_now(CRASH_AFTER_SETTLE_TRADE);
            }
            Err(HandlerError::Retry("crash point after SettleTrade".into()))
        });
        Ok(())
    }

    /// Marks trades that Accounts has settled as SETTLED and queues a
    /// `TradeSettled` for each, in one transaction.
    async fn mark_settled(&self, rows: &[&TradeRow]) -> Result<(), HandlerError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        for (symbol, _, price, qty, buy_order, sell_order, buyer, seller, trade_id) in
            rows.iter().copied()
        {
            // Also returns the spec's "trade to final balance" delay.
            let settled: Option<(OffsetDateTime, f64)> = sqlx::query_as(
                "UPDATE trades SET status = 'SETTLED', settled_at = now()
                 WHERE trade_id = $1 AND status = 'PENDING'
                 RETURNING settled_at, extract(epoch FROM clock_timestamp() - executed_at)::float8",
            )
            .bind(trade_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_error)?;
            let Some((at, delay)) = settled else {
                continue;
            };
            metrics::histogram!("settlement_seconds").record(delay);
            let msg = ev::TradeSettled {
                trade_id: trade_id.clone(),
                symbol: symbol.clone(),
                price_cents: *price,
                quantity: *qty,
                buy_order_id: buy_order.to_string(),
                sell_order_id: sell_order.to_string(),
                buyer_id: buyer.to_string(),
                seller_id: seller.to_string(),
                settled_at_ns: i64::try_from(at.unix_timestamp_nanos()).unwrap_or(i64::MAX),
            };
            outbox::enqueue(
                &mut tx,
                SETTLEMENT_EVENTS_TOPIC,
                trade_id,
                &msg.encode_to_vec(),
                &[],
            )
            .await
            .map_err(db_error)?;
        }
        tx.commit().await.map_err(db_error)
    }

    /// Trades waiting for Accounts (the `settlement_pending` gauge).
    pub async fn pending(&self) -> sqlx::Result<i64> {
        let (n,): (i64,) = sqlx::query_as("SELECT count(*) FROM trades WHERE status = 'PENDING'")
            .fetch_one(&self.pool)
            .await?;
        Ok(n)
    }
}
