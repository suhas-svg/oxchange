//! Events in, stored notifications out.

use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use prost::Message;
use proto_gen::events::v1 as ev;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::wake::Publisher;
use crate::{ACCOUNTS_EVENTS_TOPIC, MATCHING_EVENTS_TOPIC, SETTLEMENT_EVENTS_TOPIC};

/// How long a WebSocket ticket can be redeemed (spec: 30 seconds).
pub const TICKET_SECONDS: i32 = 30;

/// Why a message was not handled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// Something temporary (PostgreSQL is down): try the same message again.
    Retry(String),
    /// The message can never be handled: send it to the DLQ and continue
    /// (spec: "Bad messages": a missed message never changes a balance).
    Skip(String),
}

impl From<sqlx::Error> for Failure {
    fn from(e: sqlx::Error) -> Self {
        Failure::Retry(e.to_string())
    }
}

/// A stored notification. REST and the WebSocket send it the same way.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Notification {
    pub id: i64,
    /// `order_update`, `fill`, `settlement` or `account_update`.
    pub kind: String,
    pub body: Value,
    pub created_at: OffsetDateTime,
}

impl Notification {
    /// `{ "type": "fill", "id": 7, "created_at_ms": ..., "data": { ... } }`
    pub fn json(&self) -> Value {
        json!({
            "type": self.kind,
            "id": self.id,
            "created_at_ms": i64::try_from(self.created_at.unix_timestamp_nanos() / 1_000_000)
                .unwrap_or(i64::MAX),
            "data": self.body,
        })
    }
}

/// One message for one user, not yet stored.
#[derive(Debug, PartialEq)]
struct Draft {
    user: Uuid,
    event_id: String,
    kind: &'static str,
    body: Value,
}

fn user(id: &str) -> Result<Uuid, Failure> {
    id.parse()
        .map_err(|_| Failure::Skip(format!("user ID {id:?} is not a UUID")))
}

fn event_id(id: &str) -> Result<String, Failure> {
    if id.is_empty() {
        Err(Failure::Skip("the event has no ID".into()))
    } else {
        Ok(id.to_owned())
    }
}

fn ms(ns: i64) -> i64 {
    ns / 1_000_000
}

/// Fills, cancels and refused cancels. Accepted orders and book changes
/// produce nothing: the client already has the first from its own request,
/// and the second is public data.
fn from_matching(e: &ev::MatchingEvent) -> Result<Vec<Draft>, Failure> {
    use ev::matching_event::Body;
    let at_ms = ms(e.event_time_ns);
    Ok(match &e.body {
        Some(Body::Trade(t)) => {
            let id = event_id(&e.event_id)?;
            // Each side sees only its own order, never the other party.
            let fill = |user_id: &str, order_id: &str, side: &str| {
                Ok::<_, Failure>(Draft {
                    user: user(user_id)?,
                    event_id: id.clone(),
                    kind: "fill",
                    body: json!({
                        "symbol": e.symbol,
                        "trade_id": t.trade_id,
                        "order_id": order_id,
                        "side": side,
                        "price_cents": t.price_cents,
                        "quantity": t.quantity,
                        "executed_at_ms": at_ms,
                    }),
                })
            };
            vec![
                fill(&t.buyer_id, &t.buy_order_id, "BUY")?,
                fill(&t.seller_id, &t.sell_order_id, "SELL")?,
            ]
        }
        Some(Body::OrderCanceled(c)) => {
            let reason = ev::CancelReason::try_from(c.reason)
                .unwrap_or(ev::CancelReason::Unspecified)
                .as_str_name()
                .trim_start_matches("CANCEL_REASON_");
            vec![Draft {
                user: user(&c.user_id)?,
                event_id: event_id(&e.event_id)?,
                kind: "order_update",
                body: json!({
                    "symbol": e.symbol,
                    "order_id": c.order_id,
                    "update": "CANCELED",
                    "canceled_quantity": c.canceled_quantity,
                    "reason": reason,
                    "at_ms": at_ms,
                }),
            }]
        }
        Some(Body::CancelRejected(r)) => {
            let reason = ev::CancelRejectReason::try_from(r.reason)
                .unwrap_or(ev::CancelRejectReason::Unspecified)
                .as_str_name()
                .trim_start_matches("CANCEL_REJECT_REASON_");
            vec![Draft {
                user: user(&r.user_id)?,
                event_id: event_id(&e.event_id)?,
                kind: "order_update",
                body: json!({
                    "symbol": e.symbol,
                    "order_id": r.order_id,
                    "update": "CANCEL_REJECTED",
                    "reason": reason,
                    "at_ms": at_ms,
                }),
            }]
        }
        Some(Body::OrderAccepted(_) | Body::BookChanged(_)) | None => Vec::new(),
    })
}

fn from_settlement(s: &ev::TradeSettled) -> Result<Vec<Draft>, Failure> {
    if s.trade_id.is_empty() {
        return Err(Failure::Skip("the settlement has no trade ID".into()));
    }
    // `TradeSettled` has no event ID of its own; a trade settles once.
    let id = format!("settled:{}", s.trade_id);
    let side = |user_id: &str, order_id: &str, side: &str| {
        Ok::<_, Failure>(Draft {
            user: user(user_id)?,
            event_id: id.clone(),
            kind: "settlement",
            body: json!({
                "symbol": s.symbol,
                "trade_id": s.trade_id,
                "order_id": order_id,
                "side": side,
                "price_cents": s.price_cents,
                "quantity": s.quantity,
                "settled_at_ms": ms(s.settled_at_ns),
            }),
        })
    };
    Ok(vec![
        side(&s.buyer_id, &s.buy_order_id, "BUY")?,
        side(&s.seller_id, &s.sell_order_id, "SELL")?,
    ])
}

fn from_account(a: &ev::AccountEvent) -> Result<Vec<Draft>, Failure> {
    use ev::account_event::Body;
    let (change, amount, balance, asset) = match &a.body {
        Some(Body::Deposited(d)) => ("DEPOSIT", d.amount_cents, d.balance_cents, &d.asset),
        Some(Body::Withdrawn(w)) => ("WITHDRAWAL", w.amount_cents, w.balance_cents, &w.asset),
        None => return Ok(Vec::new()),
    };
    // For USD the amounts are cents; for a stock they are whole shares.
    Ok(vec![Draft {
        user: user(&a.user_id)?,
        event_id: event_id(&a.event_id)?,
        kind: "account_update",
        body: json!({
            "change": change,
            "asset": asset,
            "amount": amount,
            "available": balance,
            "at_ms": ms(a.event_time_ns),
        }),
    }])
}

fn hash(ticket: &str) -> Vec<u8> {
    Sha256::digest(ticket.as_bytes()).to_vec()
}

pub struct Notifier {
    pool: PgPool,
    wakes: Arc<Publisher>,
}

impl Notifier {
    pub fn new(pool: PgPool, wakes: Arc<Publisher>) -> Self {
        Self { pool, wakes }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Handles one Kafka message from one of [`crate::TOPICS`]. Returns how
    /// many notifications were new; a redelivered message stores none.
    pub async fn on_message(&self, topic: &str, payload: &[u8]) -> Result<usize, Failure> {
        let undecodable = |e: prost::DecodeError| Failure::Skip(format!("undecodable: {e}"));
        let drafts = match topic {
            MATCHING_EVENTS_TOPIC => {
                from_matching(&ev::MatchingEvent::decode(payload).map_err(undecodable)?)?
            }
            SETTLEMENT_EVENTS_TOPIC => {
                from_settlement(&ev::TradeSettled::decode(payload).map_err(undecodable)?)?
            }
            ACCOUNTS_EVENTS_TOPIC => {
                from_account(&ev::AccountEvent::decode(payload).map_err(undecodable)?)?
            }
            other => return Err(Failure::Skip(format!("unexpected topic {other:?}"))),
        };
        self.save(drafts).await
    }

    async fn save(&self, mut drafts: Vec<Draft>) -> Result<usize, Failure> {
        if drafts.is_empty() {
            return Ok(0);
        }
        // Locks are taken in user order, so two events that share users
        // cannot deadlock.
        drafts.sort_by_key(|d| d.user);
        let mut tx = self.pool.begin().await?;
        let mut stored = 0;
        let mut newest: Vec<(Uuid, i64)> = Vec::with_capacity(drafts.len());
        for d in &drafts {
            // One writer per user at a time, held until commit. IDs are
            // handed out after the lock, so a user's rows become visible in
            // ID order and "everything after ID n" never skips a row that
            // commits late.
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                .bind(d.user.to_string())
                .execute(&mut *tx)
                .await?;
            let id: Option<i64> = sqlx::query_scalar(
                "INSERT INTO notifications (user_id, event_id, kind, body)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (event_id, user_id) DO NOTHING RETURNING id",
            )
            .bind(d.user)
            .bind(&d.event_id)
            .bind(d.kind)
            .bind(&d.body)
            .fetch_optional(&mut *tx)
            .await?;
            stored += usize::from(id.is_some());
            newest.push((d.user, id.unwrap_or(0)));
        }
        tx.commit().await?;

        // Wake-ups go out even for repeats: if a copy crashed between the
        // commit above and this publish, the redelivery is what wakes the
        // socket. An extra wake-up finds nothing new and sends nothing.
        for (user, id) in newest {
            if let Err(e) = self.wakes.publish(user, id).await {
                tracing::debug!(error = %e, %user, "wake-up not published; sockets will poll");
            }
        }
        Ok(stored)
    }

    /// The newest notification ID this user has, or 0.
    pub async fn latest_id(&self, user: Uuid) -> sqlx::Result<i64> {
        sqlx::query_scalar("SELECT coalesce(max(id), 0) FROM notifications WHERE user_id = $1")
            .bind(user)
            .fetch_one(&self.pool)
            .await
    }

    /// Up to `limit` of this user's notifications with an ID above `after`,
    /// oldest first.
    pub async fn after(
        &self,
        user: Uuid,
        after: i64,
        limit: i64,
    ) -> sqlx::Result<Vec<Notification>> {
        sqlx::query_as(
            "SELECT id, kind, body, created_at FROM notifications
             WHERE user_id = $1 AND id > $2 ORDER BY id LIMIT $3",
        )
        .bind(user)
        .bind(after)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
    }

    /// This user's newest `limit` notifications, oldest first.
    pub async fn latest(&self, user: Uuid, limit: i64) -> sqlx::Result<Vec<Notification>> {
        let mut rows: Vec<Notification> = sqlx::query_as(
            "SELECT id, kind, body, created_at FROM notifications
             WHERE user_id = $1 ORDER BY id DESC LIMIT $2",
        )
        .bind(user)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.reverse();
        Ok(rows)
    }

    /// A new one-time ticket that opens this user's private stream within
    /// [`TICKET_SECONDS`].
    pub async fn issue_ticket(&self, user: Uuid) -> sqlx::Result<String> {
        let mut raw = [0u8; 32];
        getrandom::fill(&mut raw).expect("OS random source");
        let ticket = B64.encode(raw);
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM ws_tickets WHERE expires_at < now()")
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO ws_tickets (ticket_hash, user_id, expires_at)
             VALUES ($1, $2, now() + make_interval(secs => $3::int4))",
        )
        .bind(hash(&ticket))
        .bind(user)
        .bind(TICKET_SECONDS)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(ticket)
    }

    /// Uses up a ticket. `None` if it is unknown, expired or already used.
    pub async fn redeem_ticket(&self, ticket: &str) -> sqlx::Result<Option<Uuid>> {
        sqlx::query_scalar(
            "DELETE FROM ws_tickets WHERE ticket_hash = $1 AND expires_at > now()
             RETURNING user_id",
        )
        .bind(hash(ticket))
        .fetch_optional(&self.pool)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade() -> ev::MatchingEvent {
        ev::MatchingEvent {
            event_id: "ACME-7".into(),
            symbol: "ACME".into(),
            seq: 7,
            event_time_ns: 1_794_319_205_123_000_000,
            command_id: String::new(),
            body: Some(ev::matching_event::Body::Trade(ev::TradeExecuted {
                trade_id: "ACME-7".into(),
                price_cents: 1000,
                quantity: 25,
                buy_order_id: "buy-order".into(),
                sell_order_id: "sell-order".into(),
                buyer_id: Uuid::from_u128(1).to_string(),
                seller_id: Uuid::from_u128(2).to_string(),
                aggressor_side: ev::Side::Buy as i32,
            })),
        }
    }

    #[test]
    fn a_trade_tells_each_side_about_its_own_order_only() {
        let drafts = from_matching(&trade()).unwrap();
        assert_eq!(drafts.len(), 2);
        let (buy, sell) = (&drafts[0], &drafts[1]);
        assert_eq!((buy.user, buy.kind), (Uuid::from_u128(1), "fill"));
        assert_eq!(buy.body["order_id"], "buy-order");
        assert_eq!(buy.body["side"], "BUY");
        assert_eq!(buy.body["executed_at_ms"], 1_794_319_205_123_i64);
        assert_eq!(sell.user, Uuid::from_u128(2));
        assert_eq!(sell.body["order_id"], "sell-order");
        for d in &drafts {
            let text = d.body.to_string();
            let other = if d.user == Uuid::from_u128(1) {
                (Uuid::from_u128(2).to_string(), "sell-order")
            } else {
                (Uuid::from_u128(1).to_string(), "buy-order")
            };
            assert!(!text.contains(&other.0) && !text.contains(other.1));
        }
    }

    #[test]
    fn a_bad_user_id_is_skipped_not_retried() {
        let mut e = trade();
        if let Some(ev::matching_event::Body::Trade(t)) = &mut e.body {
            t.seller_id = "nobody".into();
        }
        assert!(matches!(from_matching(&e), Err(Failure::Skip(_))));
    }

    #[test]
    fn cancel_reasons_use_short_names() {
        let mut e = trade();
        e.body = Some(ev::matching_event::Body::OrderCanceled(ev::OrderCanceled {
            order_id: "o1".into(),
            user_id: Uuid::from_u128(1).to_string(),
            canceled_quantity: 5,
            reason: ev::CancelReason::IocRemainder as i32,
        }));
        let d = from_matching(&e).unwrap();
        assert_eq!(d[0].kind, "order_update");
        assert_eq!(d[0].body["update"], "CANCELED");
        assert_eq!(d[0].body["reason"], "IOC_REMAINDER");
    }
}
