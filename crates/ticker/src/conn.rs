//! A lazily connected Redis connection that fails fast while Redis is gone.
//!
//! Shared by every client of the safe-to-lose Redis data (ticker, rate
//! limits, token revocations, wake-ups). The rules that keep callers fast
//! when Redis disappears under load:
//!
//! - Only one task tries to connect at a time; the others fail at once
//!   instead of queueing behind it.
//! - After a failed connect or a failed call, nobody tries again for
//!   [`RETRY_AFTER`]; calls in that window fail at once.
//!
//! Without these, every request waited its turn for a connect attempt that
//! was bound to time out, and the queue grew until clients timed out.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use redis::aio::{ConnectionManager, ConnectionManagerConfig};

/// How long to stay in fail-fast mode after Redis was unreachable.
pub const RETRY_AFTER: Duration = Duration::from_secs(1);

enum Slot {
    Empty,
    Up(ConnectionManager),
    Down(Instant),
}

pub struct LazyConnection {
    client: redis::Client,
    timeout: Duration,
    slot: Mutex<Slot>,
    connecting: tokio::sync::Mutex<()>,
}

impl LazyConnection {
    /// Does not connect yet, so a service can start while Redis is down.
    /// `timeout` bounds each connect (twice it, overall) and each command.
    pub fn new(url: &str, timeout: Duration) -> Result<Self, String> {
        Ok(Self {
            client: redis::Client::open(url).map_err(|e| e.to_string())?,
            timeout,
            slot: Mutex::new(Slot::Empty),
            connecting: tokio::sync::Mutex::new(()),
        })
    }

    /// The live connection, or an error at once if Redis was unreachable
    /// within the last [`RETRY_AFTER`] or another task is reconnecting.
    pub async fn get(&self) -> Result<ConnectionManager, String> {
        if let Some(r) = self.current() {
            return r;
        }
        let Ok(_connecting) = self.connecting.try_lock() else {
            return Err("reconnecting to redis".into());
        };
        // Another task may have connected while this one checked.
        if let Some(r) = self.current() {
            return r;
        }
        let config = ConnectionManagerConfig::new()
            .set_connection_timeout(self.timeout)
            .set_response_timeout(self.timeout)
            .set_number_of_retries(1);
        let connected = tokio::time::timeout(
            self.timeout * 2,
            ConnectionManager::new_with_config(self.client.clone(), config),
        )
        .await;
        let mut slot = self.lock();
        match connected {
            Ok(Ok(c)) => {
                *slot = Slot::Up(c.clone());
                Ok(c)
            }
            Ok(Err(e)) => {
                *slot = Slot::Down(Instant::now() + RETRY_AFTER);
                Err(e.to_string())
            }
            Err(_) => {
                *slot = Slot::Down(Instant::now() + RETRY_AFTER);
                Err("connect timed out".into())
            }
        }
    }

    /// A call on the connection failed in a way that suggests Redis is
    /// gone: fail fast for a while, then connect afresh.
    pub fn broken(&self) {
        *self.lock() = Slot::Down(Instant::now() + RETRY_AFTER);
    }

    /// The command timeout this connection was made with.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    fn current(&self) -> Option<Result<ConnectionManager, String>> {
        match &*self.lock() {
            Slot::Up(c) => Some(Ok(c.clone())),
            Slot::Down(until) if Instant::now() < *until => {
                Some(Err("redis is unreachable; trying again shortly".into()))
            }
            _ => None,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Slot> {
        // The slot holds no invariant a panic could break.
        self.slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_unreachable_redis_fails_fast_after_the_first_try() {
        // Nothing listens on port 9 (discard) here.
        let c = LazyConnection::new("redis://127.0.0.1:9", Duration::from_millis(200)).unwrap();
        assert!(c.get().await.is_err());
        let t = Instant::now();
        for _ in 0..100 {
            assert!(c.get().await.is_err());
        }
        assert!(t.elapsed() < Duration::from_millis(50), "{:?}", t.elapsed());
    }
}
