//! Rate limits (spec: "Security → Rate limits").
//!
//! | Action                          | Limit                       | Counted per |
//! |---------------------------------|-----------------------------|-------------|
//! | Register                        | 3 per minute                | IP address  |
//! | Log in                          | 5 per minute                | IP address  |
//! | Place or cancel orders          | 10 per second, bursts to 20 | User        |
//! | Other authenticated REST calls  | 120 per minute              | User        |
//! | Public REST calls               | 60 per minute               | IP address  |
//!
//! Buckets live in Redis so every copy shares them. When Redis is down,
//! each copy keeps its own buckets in memory (spec: "Gateway uses a
//! per-copy memory limit"). WebSocket limits are in [`crate::ws`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;

use crate::error::ApiError;
use crate::store::Store;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Limit {
    Register,
    Login,
    Orders,
    Authenticated,
    Public,
}

impl Limit {
    /// (route name, capacity, ms per refilled request)
    pub fn rule(self) -> (&'static str, u32, u64) {
        match self {
            Limit::Register => ("register", 3, 20_000),
            Limit::Login => ("login", 5, 12_000),
            Limit::Orders => ("orders", 20, 100),
            Limit::Authenticated => ("user", 120, 500),
            Limit::Public => ("public", 60, 1_000),
        }
    }
}

/// Memory buckets kept at most this long without use.
const LOCAL_IDLE_MS: u64 = 60_000;
/// Prune idle memory buckets once there are this many.
const LOCAL_PRUNE_AT: usize = 10_000;

struct Bucket {
    milli: u64,
    at: Instant,
}

pub struct Limiter {
    redis: Arc<Store>,
    local: Mutex<HashMap<String, Bucket>>,
}

impl Limiter {
    pub fn new(redis: Arc<Store>) -> Self {
        Self {
            redis,
            local: Mutex::new(HashMap::new()),
        }
    }

    /// Takes one request for `who` (a user ID or an IP address), or returns
    /// RATE_LIMITED with how long to wait.
    pub async fn check(&self, limit: Limit, who: &str) -> Result<(), ApiError> {
        let (route, capacity, interval) = limit.rule();
        let key = format!("rate:{who}:{route}");
        let wait = match self.redis.take(&key, capacity, interval).await {
            Ok(w) => w,
            Err(e) => {
                tracing::debug!(error = %e, "rate limit falling back to this copy's memory");
                self.take_local(key, capacity, interval).await
            }
        };
        match wait {
            None => Ok(()),
            Some(ms) => Err(ApiError::rate_limited(ms.div_ceil(1000))),
        }
    }

    async fn take_local(&self, key: String, capacity: u32, interval: u64) -> Option<u64> {
        let cap = u64::from(capacity) * 1000;
        let now = Instant::now();
        let mut buckets = self.local.lock().await;
        if buckets.len() >= LOCAL_PRUNE_AT {
            buckets.retain(|_, b| ms_between(b.at, now) < LOCAL_IDLE_MS);
        }
        let b = buckets.entry(key).or_insert(Bucket {
            milli: cap,
            at: now,
        });
        let refill = ms_between(b.at, now).saturating_mul(1000) / interval.max(1);
        b.milli = (b.milli.saturating_add(refill)).min(cap);
        b.at = now;
        if b.milli >= 1000 {
            b.milli -= 1000;
            None
        } else {
            Some((1000 - b.milli).saturating_mul(interval).div_ceil(1000))
        }
    }
}

fn ms_between(a: Instant, b: Instant) -> u64 {
    u64::try_from(b.saturating_duration_since(a).as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn without_redis_each_copy_still_limits() {
        let l = Limiter::new(Arc::new(Store::new("redis://127.0.0.1:1").unwrap()));
        for _ in 0..3 {
            l.check(Limit::Register, "10.0.0.1").await.unwrap();
        }
        let e = l.check(Limit::Register, "10.0.0.1").await.unwrap_err();
        assert_eq!(e.code, "RATE_LIMITED");
        // About 20 s until the next register is allowed.
        assert!((19..=20).contains(&e.retry_after.unwrap()), "{e:?}");
        // Another address has its own bucket.
        l.check(Limit::Register, "10.0.0.2").await.unwrap();
    }
}
