//! The sweeper (spec: "every 5 seconds, Risk finds PENDING_RISK orders
//! older than 30 seconds and runs the same void path"). `FOR UPDATE SKIP
//! LOCKED` lets several Risk copies share the work without colliding.

use std::time::Duration;

use uuid::Uuid;

use crate::orders::{ACCOUNTS_UNAVAILABLE, void_request};
use crate::{AccountsApi, Result, Risk};

const BATCH: i64 = 100;

impl<A: AccountsApi> Risk<A> {
    /// Voids and rejects PENDING_RISK orders older than `older_than`.
    /// Returns how many it rejected.
    pub async fn sweep(&self, older_than: Duration) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        let stale: Vec<(Uuid,)> = sqlx::query_as(
            "SELECT order_id FROM orders
             WHERE status = 'PENDING_RISK'
               AND created_at < now() - ($1::bigint * interval '1 millisecond')
             ORDER BY created_at LIMIT $2
             FOR UPDATE SKIP LOCKED",
        )
        .bind(i64::try_from(older_than.as_millis()).unwrap_or(i64::MAX))
        .bind(BATCH)
        .fetch_all(&mut *tx)
        .await?;
        let mut rejected = 0;
        for (order_id,) in stale {
            // Releases the full reservation if Reserve worked, or blocks a
            // delayed Reserve from ever succeeding.
            if let Err(s) = self.accounts.release(void_request(order_id)).await {
                tracing::warn!(%order_id, status = %s, "sweeper could not void; will retry");
                break;
            }
            sqlx::query(
                "UPDATE orders SET status = 'REJECTED', reject_reason = $2, updated_at = now()
                 WHERE order_id = $1",
            )
            .bind(order_id)
            .bind(ACCOUNTS_UNAVAILABLE)
            .execute(&mut *tx)
            .await?;
            rejected += 1;
        }
        tx.commit().await?;
        if rejected > 0 {
            tracing::info!(rejected, "sweeper rejected stale orders");
        }
        Ok(rejected)
    }
}
