//! Database connection-pool gauges (spec dashboard 4, "connection use").

use std::time::Duration;

/// Every 5 s, sets `db_pool_connections{state="idle"|"in_use"}` and
/// `db_pool_max_connections` for `pool`. Needs a tokio runtime.
pub fn watch_pool(pool: sqlx::PgPool) {
    tokio::spawn(async move {
        let max = metrics::gauge!("db_pool_max_connections");
        let idle = metrics::gauge!("db_pool_connections", "state" => "idle");
        let used = metrics::gauge!("db_pool_connections", "state" => "in_use");
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            if pool.is_closed() {
                return;
            }
            let size = pool.size();
            let free = u32::try_from(pool.num_idle()).unwrap_or(size);
            max.set(f64::from(pool.options().get_max_connections()));
            idle.set(f64::from(free));
            used.set(f64::from(size.saturating_sub(free)));
        }
    });
}
