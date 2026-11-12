//! The `ticker:{symbol}` Redis hash (spec: "Data storage → Redis").
//!
//! Market Data writes it after recording each trade; Risk reads the last
//! price as the reference for market orders. Redis holds only data that is
//! safe to lose: when it is unreachable, [`Store`] calls fail fast and the
//! caller runs in reduced mode (Risk rejects market orders, Market Data
//! refills the hashes once Redis is back).
//!
//! Writes carry the trade's seq and only replace an older value, so a late
//! or repeated write can never move the price backwards.

use std::time::Duration;

use redis::AsyncCommands;

use redis::aio::ConnectionManager;

mod conn;
pub use conn::{LazyConnection, RETRY_AFTER};

/// Every Redis call gives up after this long.
pub const TIMEOUT: Duration = Duration::from_millis(300);

pub fn key(symbol: &str) -> String {
    format!("ticker:{symbol}")
}

/// One stock's ticker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tick {
    pub last_price_cents: i64,
    /// Shares traded since midnight UTC of the last trade's day.
    pub day_volume: i64,
    /// The `matching.events` seq of the last trade.
    pub seq: u64,
    /// Time of the last trade, Unix nanoseconds.
    pub traded_at_ns: i64,
}

const WRITE: &str = r"
local cur = redis.call('HGET', KEYS[1], 'seq')
if cur and tonumber(cur) >= tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'seq', ARGV[1], 'last_price_cents', ARGV[2],
           'day_volume', ARGV[3], 'traded_at_ns', ARGV[4])
return 1
";

#[derive(Debug, thiserror::Error)]
#[error("redis unavailable: {0}")]
pub struct Unavailable(pub String);

/// A lazily connected, self-healing Redis connection. The first call after
/// Redis comes back reconnects.
pub struct Store {
    conn: LazyConnection,
}

impl Store {
    /// Does not connect yet, so a service can start while Redis is down.
    pub fn new(url: &str) -> Result<Self, Unavailable> {
        Ok(Self {
            conn: LazyConnection::new(url, TIMEOUT).map_err(Unavailable)?,
        })
    }

    async fn run<T>(
        &self,
        f: impl AsyncFnOnce(&mut ConnectionManager) -> redis::RedisResult<T>,
    ) -> Result<T, Unavailable> {
        let mut c = self.conn.get().await.map_err(Unavailable)?;
        match tokio::time::timeout(TIMEOUT, f(&mut c)).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => {
                if e.is_io_error() || e.is_connection_dropped() || e.is_timeout() {
                    self.conn.broken();
                }
                Err(Unavailable(e.to_string()))
            }
            Err(_) => {
                self.conn.broken();
                Err(Unavailable("timed out".into()))
            }
        }
    }

    /// Stores `tick` unless the stored one has the same or a newer seq.
    /// Returns whether it was stored.
    pub async fn write(&self, symbol: &str, tick: Tick) -> Result<bool, Unavailable> {
        let key = key(symbol);
        self.run(async move |c| {
            let stored: i64 = redis::Script::new(WRITE)
                .key(key)
                .arg(tick.seq)
                .arg(tick.last_price_cents)
                .arg(tick.day_volume)
                .arg(tick.traded_at_ns)
                .invoke_async(c)
                .await?;
            Ok(stored == 1)
        })
        .await
    }

    /// The stored ticker, or `None` if this stock has none yet.
    pub async fn read(&self, symbol: &str) -> Result<Option<Tick>, Unavailable> {
        let key = key(symbol);
        self.run(async move |c| {
            let (seq, price, volume, at): (Option<u64>, Option<i64>, Option<i64>, Option<i64>) = c
                .hget(
                    &key,
                    &["seq", "last_price_cents", "day_volume", "traded_at_ns"],
                )
                .await?;
            Ok(match (seq, price) {
                (Some(seq), Some(last_price_cents)) => Some(Tick {
                    last_price_cents,
                    day_volume: volume.unwrap_or(0),
                    seq,
                    traded_at_ns: at.unwrap_or(0),
                }),
                _ => None,
            })
        })
        .await
    }
}
