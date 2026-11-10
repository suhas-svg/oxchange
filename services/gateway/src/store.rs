//! The Gateway's Redis keys (spec: "Data storage → Redis"):
//!
//! - `rate:{who}:{route}`: rate-limit buckets, expiring within 60 s.
//! - `revoked:{token_id}`: logged-out access tokens, kept until they expire
//!   on their own (at most 15 minutes).
//!
//! Redis holds only data that is safe to lose. Every call fails fast when
//! it is unreachable, and callers fall back (per-copy limits; a logged-out
//! token may work until it expires).

use std::time::Duration;

use redis::aio::ConnectionManager;
use tokio::sync::Mutex;
use uuid::Uuid;

/// Every Redis call gives up after this long.
pub const TIMEOUT: Duration = Duration::from_millis(200);

/// A token bucket in integer milli-tokens. It refills one token every
/// `interval_ms` up to `capacity`; each request takes one. Returns
/// `{allowed, wait_ms}`. The bucket expires once it would be full again,
/// and never later than 60 s.
const TAKE: &str = r"
local cap = tonumber(ARGV[1]) * 1000
local interval = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local b = redis.call('HMGET', KEYS[1], 'm', 'ts')
local m = tonumber(b[1])
local ts = tonumber(b[2])
if m == nil or ts == nil then m = cap; ts = now end
if now > ts then m = math.min(cap, m + math.floor((now - ts) * 1000 / interval)) end
local allowed = 0
local wait = 0
if m >= 1000 then
  m = m - 1000
  allowed = 1
else
  wait = math.ceil((1000 - m) * interval / 1000)
end
redis.call('HSET', KEYS[1], 'm', m, 'ts', now)
local ttl = math.ceil((cap - m) * interval / 1000)
if ttl < 1000 then ttl = 1000 end
if ttl > 60000 then ttl = 60000 end
redis.call('PEXPIRE', KEYS[1], ttl)
return {allowed, wait}
";

pub struct Store {
    client: redis::Client,
    conn: Mutex<Option<ConnectionManager>>,
}

impl Store {
    /// Does not connect yet, so the Gateway starts while Redis is down.
    pub fn new(url: &str) -> Result<Self, String> {
        Ok(Self {
            client: redis::Client::open(url).map_err(|e| e.to_string())?,
            conn: Mutex::new(None),
        })
    }

    async fn conn(&self) -> Result<ConnectionManager, String> {
        let mut slot = self.conn.lock().await;
        if let Some(c) = slot.as_ref() {
            return Ok(c.clone());
        }
        let config = redis::aio::ConnectionManagerConfig::new()
            .set_connection_timeout(TIMEOUT)
            .set_response_timeout(TIMEOUT)
            .set_number_of_retries(1);
        let c = tokio::time::timeout(
            TIMEOUT * 2,
            ConnectionManager::new_with_config(self.client.clone(), config),
        )
        .await
        .map_err(|_| "connect timed out".to_owned())?
        .map_err(|e| e.to_string())?;
        *slot = Some(c.clone());
        Ok(c)
    }

    async fn run<T: redis::FromRedisValue>(&self, cmd: redis::Cmd) -> Result<T, String> {
        let mut c = self.conn().await?;
        match tokio::time::timeout(TIMEOUT, cmd.query_async::<T>(&mut c)).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => {
                if e.is_io_error() || e.is_connection_dropped() || e.is_timeout() {
                    *self.conn.lock().await = None;
                }
                Err(e.to_string())
            }
            Err(_) => {
                *self.conn.lock().await = None;
                Err("timed out".into())
            }
        }
    }

    /// Takes one request from the bucket. `Ok(None)` if allowed, else
    /// `Ok(Some(wait_ms))`.
    pub async fn take(
        &self,
        key: &str,
        capacity: u32,
        interval_ms: u64,
    ) -> Result<Option<u64>, String> {
        let mut c = self.conn().await?;
        let script = redis::Script::new(TAKE);
        let mut inv = script.key(key);
        inv.arg(capacity).arg(interval_ms);
        let r = tokio::time::timeout(TIMEOUT, inv.invoke_async::<(i64, i64)>(&mut c)).await;
        match r {
            Ok(Ok((1, _))) => Ok(None),
            Ok(Ok((_, wait))) => Ok(Some(u64::try_from(wait).unwrap_or(1000))),
            Ok(Err(e)) => {
                *self.conn.lock().await = None;
                Err(e.to_string())
            }
            Err(_) => {
                *self.conn.lock().await = None;
                Err("timed out".into())
            }
        }
    }

    pub async fn is_revoked(&self, token_id: Uuid) -> Result<bool, String> {
        self.run(
            redis::cmd("EXISTS")
                .arg(format!("revoked:{token_id}"))
                .clone(),
        )
        .await
    }

    /// Marks an access token logged out for the rest of its life.
    pub async fn revoke(&self, token_id: Uuid, ttl_secs: u64) -> Result<(), String> {
        self.run::<()>(
            redis::cmd("SET")
                .arg(format!("revoked:{token_id}"))
                .arg(1)
                .arg("EX")
                .arg(ttl_secs.max(1))
                .clone(),
        )
        .await
    }
}
