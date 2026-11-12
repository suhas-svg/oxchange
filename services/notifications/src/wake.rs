//! The Redis channel `user:{id}` (spec: "Data storage → Redis": routes
//! private live messages; not stored; clients catch up through REST).
//!
//! A message on the channel means "this user has new notifications". The
//! payload is the newest notification ID, for people reading the channel;
//! receivers ignore it and read the rows from PostgreSQL.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::StreamExt;
use uuid::Uuid;

use crate::Hub;

/// Every Redis call gives up after this long.
const TIMEOUT: Duration = Duration::from_millis(300);
/// How often the subscription checks that Redis still answers.
const PING_EVERY: Duration = Duration::from_secs(5);
/// While the subscription is down, sockets check PostgreSQL this often, so
/// live messages arrive late instead of not at all.
pub const FALLBACK_POLL: Duration = Duration::from_secs(2);

const PATTERN: &str = "user:*";

pub fn channel(user: Uuid) -> String {
    format!("user:{user}")
}

fn user_of(channel: &str) -> Option<Uuid> {
    channel.strip_prefix("user:")?.parse().ok()
}

/// Publishes wake-ups over a lazily connected, self-healing connection, so
/// the service starts and keeps storing notifications while Redis is down.
pub struct Publisher {
    conn: ticker::LazyConnection,
}

impl Publisher {
    /// Does not connect yet.
    pub fn new(url: &str) -> Result<Self, String> {
        Ok(Self {
            conn: ticker::LazyConnection::new(url, TIMEOUT)?,
        })
    }

    /// Tells every copy that `user` has notifications up to `newest_id`.
    pub async fn publish(&self, user: Uuid, newest_id: i64) -> Result<(), String> {
        let mut c = self.conn.get().await?;
        let sent = tokio::time::timeout(
            TIMEOUT,
            redis::cmd("PUBLISH")
                .arg(channel(user))
                .arg(newest_id)
                .query_async::<i64>(&mut c),
        )
        .await;
        match sent {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => {
                // Start over with a fresh connection next time.
                self.conn.broken();
                Err(e.to_string())
            }
            Err(_) => {
                self.conn.broken();
                Err("timed out".into())
            }
        }
    }
}

/// Wakes this copy's sockets as wake-ups arrive, until `shutdown` resolves.
///
/// Whenever the subscription (re)starts, every socket checks once, which
/// picks up anything published while it was down. While it is down, every
/// socket checks each [`FALLBACK_POLL`].
pub async fn listen(
    url: &str,
    hub: Arc<Hub>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    let client = redis::Client::open(url).map_err(|e| e.to_string())?;
    tokio::pin!(shutdown);
    let mut reported = false;
    loop {
        let ended = tokio::select! {
            () = &mut shutdown => return Ok(()),
            reason = follow(&client, &hub) => reason,
        };
        if hub.live.swap(false, Ordering::Relaxed) || !reported {
            tracing::warn!(reason = %ended, poll_ms = FALLBACK_POLL.as_millis() as u64,
                "redis subscription down; sockets poll until it is back");
            reported = true;
        }
        hub.wake_all();
        tokio::select! {
            () = &mut shutdown => return Ok(()),
            () = tokio::time::sleep(FALLBACK_POLL) => {}
        }
    }
}

/// One subscription, from connect until it breaks. Returns why it ended.
async fn follow(client: &redis::Client, hub: &Hub) -> String {
    let mut pubsub = match tokio::time::timeout(TIMEOUT * 4, client.get_async_pubsub()).await {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => return e.to_string(),
        Err(_) => return "connect timed out".into(),
    };
    match tokio::time::timeout(TIMEOUT * 4, pubsub.psubscribe(PATTERN)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return e.to_string(),
        Err(_) => return "subscribe timed out".into(),
    }
    let (mut sink, mut stream) = pubsub.split();
    hub.live.store(true, Ordering::Relaxed);
    tracing::info!("redis subscription up");
    hub.wake_all();

    let mut ping = tokio::time::interval(PING_EVERY);
    loop {
        tokio::select! {
            msg = stream.next() => match msg {
                Some(m) => {
                    if let Some(user) = user_of(m.get_channel_name()) {
                        hub.wake(user);
                    }
                }
                None => return "connection closed".into(),
            },
            _ = ping.tick() => {
                match tokio::time::timeout(TIMEOUT * 4, sink.ping::<redis::Value>()).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => return e.to_string(),
                    Err(_) => return "ping timed out".into(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_names_round_trip_and_junk_is_ignored() {
        let user = Uuid::now_v7();
        assert_eq!(user_of(&channel(user)), Some(user));
        assert_eq!(user_of("user:not-a-uuid"), None);
        assert_eq!(user_of("ticker:ACME"), None);
    }
}
