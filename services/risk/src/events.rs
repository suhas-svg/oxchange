//! Applies `matching.events` to orders (spec: "Risk ... reads
//! matching.events to update order status and release funds for canceled
//! quantity").
//!
//! The release amount comes from the canceled quantity, not from whatever
//! remains, so cancels and settlements can arrive in either order and still
//! produce the same balance.

use consumer::{Decision, HandlerError};
use proto_gen::accounts::v1 as apb;
use proto_gen::events::v1 as ev;
use tonic::Code;
use uuid::Uuid;

use crate::accounts_api::outcome_unknown;
use crate::{AccountsApi, Risk};

fn halt(m: impl Into<String>) -> HandlerError {
    HandlerError::Halt(m.into())
}

/// A broken CHECK constraint means the event contradicts what Risk knows:
/// a bug, not something a retry fixes.
fn db_error(e: sqlx::Error) -> HandlerError {
    match &e {
        sqlx::Error::Database(d) if d.code().as_deref() == Some("23514") => {
            halt(format!("event breaks an order invariant: {e}"))
        }
        _ => HandlerError::Retry(e.to_string()),
    }
}

fn order_id(s: &str) -> Result<Uuid, HandlerError> {
    Uuid::parse_str(s).map_err(|_| halt(format!("bad order id {s:?} in event")))
}

impl<A: AccountsApi> Risk<A> {
    /// Applies one event, exactly once, in one transaction.
    pub async fn apply_event(&self, e: &ev::MatchingEvent) -> Result<(), HandlerError> {
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        match consumer::claim(&mut tx, &e.symbol, e.seq)
            .await
            .map_err(db_error)?
        {
            Decision::Repeat => return Ok(()),
            Decision::Gap { expected, got } => {
                return Err(halt(format!(
                    "{}: expected seq {expected}, got {got}; events are missing",
                    e.symbol
                )));
            }
            Decision::Apply => {}
        }
        match &e.body {
            Some(ev::matching_event::Body::OrderAccepted(a)) => {
                sqlx::query(
                    "UPDATE orders SET status = 'OPEN', updated_at = now()
                     WHERE order_id = $1 AND status = 'ACCEPTED'",
                )
                .bind(order_id(&a.order_id)?)
                .execute(&mut *tx)
                .await
                .map_err(db_error)?;
            }
            Some(ev::matching_event::Body::Trade(t)) => {
                for id in [&t.buy_order_id, &t.sell_order_id] {
                    sqlx::query(
                        "UPDATE orders SET
                           filled_quantity = filled_quantity + $2,
                           status = CASE
                             WHEN filled_quantity + $2 + canceled_quantity < quantity THEN 'PARTIALLY_FILLED'
                             WHEN canceled_quantity = 0 THEN 'FILLED'
                             ELSE 'CANCELED' END,
                           updated_at = now()
                         WHERE order_id = $1",
                    )
                    .bind(order_id(id)?)
                    .bind(t.quantity)
                    .execute(&mut *tx)
                    .await
                    .map_err(db_error)?;
                }
                // The reference price for market orders (see the migration).
                sqlx::query("UPDATE symbols SET last_price_cents = $2 WHERE symbol = $1")
                    .bind(&e.symbol)
                    .bind(t.price_cents)
                    .execute(&mut *tx)
                    .await
                    .map_err(db_error)?;
            }
            Some(ev::matching_event::Body::OrderCanceled(c)) => {
                let id = order_id(&c.order_id)?;
                let row: Option<(String, i64)> = sqlx::query_as(
                    "SELECT side, limit_price_cents FROM orders WHERE order_id = $1 FOR UPDATE",
                )
                .bind(id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db_error)?;
                if let Some((side, limit)) = row {
                    let amount = if side == "BUY" {
                        c.canceled_quantity
                            .checked_mul(limit)
                            .ok_or_else(|| halt("release amount overflows"))?
                    } else {
                        c.canceled_quantity
                    };
                    if amount > 0 {
                        // Idempotent by event ID, so a replayed event or a
                        // crash before commit just repeats the same release.
                        self.release(id, &e.event_id, amount).await?;
                    }
                    sqlx::query(
                        "UPDATE orders SET
                           canceled_quantity = canceled_quantity + $2,
                           status = CASE
                             WHEN filled_quantity + canceled_quantity + $2 < quantity THEN
                               CASE WHEN filled_quantity > 0 THEN 'PARTIALLY_FILLED' ELSE 'OPEN' END
                             ELSE 'CANCELED' END,
                           updated_at = now()
                         WHERE order_id = $1",
                    )
                    .bind(id)
                    .bind(c.canceled_quantity)
                    .execute(&mut *tx)
                    .await
                    .map_err(db_error)?;
                }
            }
            // A rejected cancel changes nothing; book depth is Market Data's.
            Some(ev::matching_event::Body::CancelRejected(_))
            | Some(ev::matching_event::Body::BookChanged(_)) => {}
            None => return Err(halt(format!("event {} has no body", e.event_id))),
        }
        tx.commit().await.map_err(db_error)
    }

    async fn release(&self, order: Uuid, event_id: &str, amount: i64) -> Result<(), HandlerError> {
        let r = apb::ReleaseRequest {
            order_id: order.to_string(),
            idempotency_key: format!("release:{event_id}"),
            amount,
            void_if_missing: false,
        };
        match self.accounts.release(r).await {
            Ok(_) => Ok(()),
            Err(s) if outcome_unknown(&s) && s.code() != Code::Internal => {
                Err(HandlerError::Retry(format!("release for {order}: {s}")))
            }
            // Accounts says this release can never succeed (for example more
            // than is reserved): a bug somewhere, so stop and alert.
            Err(s) => Err(halt(format!("release for {order} refused: {s}"))),
        }
    }
}
