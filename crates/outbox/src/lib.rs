//! Transactional outbox (spec: "Failure handling → Transactional outbox").
//!
//! 1. A service saves its change and the outgoing message in one database
//!    transaction ([`enqueue`]).
//! 2. A [`Relay`] reads unpublished rows in ID order, publishes them, waits
//!    for Kafka to confirm, then marks them published.
//!
//! A crash between publishing and marking republishes those rows; consumers
//! ignore the repeats. Only one relay per service runs at a time: it holds a
//! PostgreSQL advisory lock on its own connection, so if that copy dies the
//! lock is released and another copy takes over.
//!
//! Every service that publishes uses the same table:
//!
//! ```sql
//! CREATE TABLE outbox (
//!   id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
//!   topic        TEXT NOT NULL,
//!   message_key  TEXT NOT NULL,
//!   payload      BYTEA NOT NULL,
//!   headers      JSONB NOT NULL DEFAULT '{}',
//!   created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
//!   published_at TIMESTAMPTZ
//! );
//! CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
//! ```
//!
//! Metrics: `outbox_unpublished` (rows waiting, sampled every 2 s by the
//! relay holding the lock) and `outbox_publish_seconds{topic}` (from saving
//! a row to Kafka confirming it).
//!
//! The current span's trace context is saved with each message as a
//! `traceparent` header, so the trace continues in whoever consumes it.

use std::future::Future;
use std::time::{Duration, Instant};

use sqlx::pool::PoolConnection;
use sqlx::{PgConnection, PgPool, Postgres};

#[cfg(feature = "kafka")]
pub mod kafka;

/// One message waiting to be published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub id: i64,
    pub topic: String,
    pub key: String,
    pub payload: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

/// Sends a batch and returns only once every message in it is confirmed
/// (for Kafka: `acks=all`). Rows must be sent in the order given.
pub trait Publisher: Send + Sync {
    fn publish(&self, rows: &[Row]) -> impl Future<Output = Result<(), String>> + Send;
}

/// Queues a message inside the caller's transaction. Unless `headers`
/// already has one, the current trace context is added as `traceparent`.
pub async fn enqueue(
    conn: &mut PgConnection,
    topic: &str,
    key: &str,
    payload: &[u8],
    headers: &[(String, String)],
) -> sqlx::Result<()> {
    let mut headers: serde_json::Map<String, serde_json::Value> = headers
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    if !headers.contains_key("traceparent")
        && let Some(tp) = telemetry::current_traceparent()
    {
        headers.insert("traceparent".into(), serde_json::Value::String(tp));
    }
    sqlx::query(
        "INSERT INTO outbox (topic, message_key, payload, headers) VALUES ($1, $2, $3, $4)",
    )
    .bind(topic)
    .bind(key)
    .bind(payload)
    .bind(serde_json::Value::Object(headers))
    .execute(conn)
    .await?;
    Ok(())
}

/// Messages saved but not yet confirmed by Kafka (the `outbox_unpublished` gauge).
pub async fn unpublished(pool: &PgPool) -> sqlx::Result<i64> {
    let (n,): (i64,) = sqlx::query_as("SELECT count(*) FROM outbox WHERE published_at IS NULL")
        .fetch_one(pool)
        .await?;
    Ok(n)
}

/// How often the relay samples `outbox_unpublished`.
const BACKLOG_SAMPLE: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct RelayConfig {
    /// Advisory lock ID; one per service, so each service has one relay.
    pub lock_key: i64,
    pub batch_size: i64,
    /// How long to wait when there is nothing to publish.
    pub idle_wait: Duration,
    /// How long to wait after an error, or while another copy holds the lock.
    pub retry_wait: Duration,
}

impl RelayConfig {
    pub fn new(lock_key: i64) -> Self {
        Self {
            lock_key,
            batch_size: 500,
            idle_wait: Duration::from_millis(10),
            retry_wait: Duration::from_secs(1),
        }
    }
}

/// Publishes the next batch of unpublished rows, in ID order, and marks them
/// published once the publisher confirms. Returns how many it published.
///
/// The caller must hold the relay lock on `conn`.
pub async fn publish_batch<P: Publisher>(
    conn: &mut PgConnection,
    publisher: &P,
    batch_size: i64,
) -> Result<usize, String> {
    let rows: Vec<(i64, String, String, Vec<u8>, serde_json::Value, f64)> = sqlx::query_as(
        "SELECT id, topic, message_key, payload, headers,
                extract(epoch FROM clock_timestamp() - created_at)::float8
         FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT $1",
    )
    .bind(batch_size)
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| e.to_string())?;
    if rows.is_empty() {
        return Ok(0);
    }
    let waited: Vec<(String, f64)> = rows.iter().map(|r| (r.1.clone(), r.5)).collect();
    let sent = Instant::now();
    let rows: Vec<Row> = rows
        .into_iter()
        .map(|(id, topic, key, payload, headers, _)| Row {
            id,
            topic,
            key,
            payload,
            headers: match headers {
                serde_json::Value::Object(m) => m
                    .into_iter()
                    .filter_map(|(k, v)| v.as_str().map(|v| (k, v.to_owned())))
                    .collect(),
                _ => Vec::new(),
            },
        })
        .collect();
    publisher.publish(&rows).await?;
    // Age when read, plus the time Kafka took to confirm.
    let confirm = sent.elapsed();
    for (topic, age) in waited {
        let age = Duration::try_from_secs_f64(age).unwrap_or_default() + confirm;
        metrics::histogram!("outbox_publish_seconds", "topic" => topic).record(age.as_secs_f64());
    }
    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    sqlx::query("UPDATE outbox SET published_at = now() WHERE id = ANY($1)")
        .bind(&ids)
        .execute(&mut *conn)
        .await
        .map_err(|e| e.to_string())?;
    Ok(rows.len())
}

/// Takes the relay lock on a dedicated connection, or returns `None` if
/// another copy holds it.
pub async fn try_lock(
    pool: &PgPool,
    lock_key: i64,
) -> sqlx::Result<Option<PoolConnection<Postgres>>> {
    let mut conn = pool.acquire().await?;
    let (got,): (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
        .bind(lock_key)
        .fetch_one(&mut *conn)
        .await?;
    if got { Ok(Some(conn)) } else { Ok(None) }
}

/// Runs the relay until `shutdown` resolves.
pub async fn run<P: Publisher>(
    pool: PgPool,
    publisher: P,
    config: RelayConfig,
    shutdown: impl Future<Output = ()>,
) {
    tokio::pin!(shutdown);
    let mut lock: Option<PoolConnection<Postgres>> = None;
    let backlog = metrics::gauge!("outbox_unpublished");
    let mut sampled: Option<Instant> = None;
    loop {
        if lock.is_some() && sampled.is_none_or(|t| t.elapsed() >= BACKLOG_SAMPLE) {
            sampled = Some(Instant::now());
            if let Ok(n) = unpublished(&pool).await {
                backlog.set(n as f64);
            }
        }
        let wait = match lock.as_mut() {
            None => match try_lock(&pool, config.lock_key).await {
                Ok(Some(conn)) => {
                    tracing::info!(lock_key = config.lock_key, "outbox relay took the lock");
                    lock = Some(conn);
                    Duration::ZERO
                }
                Ok(None) => config.retry_wait,
                Err(e) => {
                    tracing::warn!(error = %e, "outbox relay could not reach the database");
                    config.retry_wait
                }
            },
            Some(conn) => match publish_batch(conn, &publisher, config.batch_size).await {
                Ok(0) => config.idle_wait,
                Ok(n) => {
                    tracing::debug!(published = n, "outbox batch published");
                    Duration::ZERO
                }
                Err(e) => {
                    // Drop the connection: if it broke, the lock is gone too,
                    // and another copy may already be publishing.
                    tracing::warn!(error = %e, "outbox publish failed; will retry");
                    if let Some(c) = lock.take() {
                        drop(c.detach());
                    }
                    config.retry_wait
                }
            },
        };
        tokio::select! {
            () = &mut shutdown => break,
            () = tokio::time::sleep(wait) => {}
        }
    }
    // Closing the connection releases the advisory lock.
    if let Some(c) = lock {
        drop(c.detach());
    }
}
