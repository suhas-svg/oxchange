//! The durable side of Market Data: public trade history, 1-minute
//! candles, and the Redis ticker.
//!
//! Copies share one consumer group, so each stock is recorded by one copy
//! at a time, in seq order. A trade's row, its candle update and the
//! stock's new seq commit together; the ticker is written after the commit.
//! Repeats change nothing. A gap means public history is missing events:
//! it is logged as an error and recording continues, because nothing here
//! moves money (spec: "Public data can be fully rebuilt").

use std::sync::Arc;

use consumer::{Decision, HandlerError};
use proto_gen::events::v1 as ev;
use sqlx::PgPool;
use time::OffsetDateTime;

pub struct Recorder {
    pool: PgPool,
    ticker: Arc<ticker::Store>,
}

fn retry(e: sqlx::Error) -> HandlerError {
    match &e {
        // A CHECK failed: the event itself is impossible. Retrying cannot help.
        sqlx::Error::Database(d) if d.code().as_deref() == Some("23514") => {
            HandlerError::Halt(format!("event breaks a table rule: {e}"))
        }
        _ => HandlerError::Retry(e.to_string()),
    }
}

pub fn side_name(side: i32) -> &'static str {
    if side == ev::Side::Sell as i32 {
        "SELL"
    } else {
        "BUY"
    }
}

impl Recorder {
    pub fn new(pool: PgPool, ticker: Arc<ticker::Store>) -> Self {
        Self { pool, ticker }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn apply_event(&self, e: &ev::MatchingEvent) -> Result<(), HandlerError> {
        let mut tx = self.pool.begin().await.map_err(retry)?;
        match consumer::claim(&mut tx, &e.symbol, e.seq)
            .await
            .map_err(retry)?
        {
            Decision::Repeat => return Ok(()),
            Decision::Apply => {}
            Decision::Gap { expected, got } => {
                tracing::error!(symbol = %e.symbol, expected, got,
                    "seq gap: public trade history is missing events");
                sqlx::query("UPDATE consumer_progress SET last_seq = $2 WHERE symbol = $1")
                    .bind(&e.symbol)
                    .bind(i64::try_from(e.seq).map_err(|x| HandlerError::Halt(x.to_string()))?)
                    .execute(&mut *tx)
                    .await
                    .map_err(retry)?;
            }
        }
        let Some(ev::matching_event::Body::Trade(t)) = &e.body else {
            return tx.commit().await.map_err(retry);
        };
        let at = OffsetDateTime::from_unix_timestamp_nanos(i128::from(e.event_time_ns))
            .map_err(|x| HandlerError::Halt(format!("event time: {x}")))?;
        let seq = i64::try_from(e.seq).map_err(|x| HandlerError::Halt(x.to_string()))?;
        let new = sqlx::query(
            "INSERT INTO market_trades (symbol, seq, price_cents, quantity, aggressor_side, executed_at)
             VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
        )
        .bind(&e.symbol)
        .bind(seq)
        .bind(t.price_cents)
        .bind(t.quantity)
        .bind(side_name(t.aggressor_side))
        .bind(at)
        .execute(&mut *tx)
        .await
        .map_err(retry)?
        .rows_affected()
            == 1;
        if !new {
            return tx.commit().await.map_err(retry);
        }
        sqlx::query(
            "INSERT INTO candles_1m AS c
               (symbol, bucket_start, open_cents, high_cents, low_cents, close_cents, volume)
             VALUES ($1, date_trunc('minute', $2 AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',
                     $3, $3, $3, $3, $4)
             ON CONFLICT (symbol, bucket_start) DO UPDATE SET
               high_cents  = GREATEST(c.high_cents, EXCLUDED.high_cents),
               low_cents   = LEAST(c.low_cents, EXCLUDED.low_cents),
               close_cents = EXCLUDED.close_cents,
               volume      = c.volume + EXCLUDED.volume",
        )
        .bind(&e.symbol)
        .bind(at)
        .bind(t.price_cents)
        .bind(t.quantity)
        .execute(&mut *tx)
        .await
        .map_err(retry)?;
        let day_volume = day_volume(&mut tx, &e.symbol, at).await.map_err(retry)?;
        tx.commit().await.map_err(retry)?;

        let tick = ticker::Tick {
            last_price_cents: t.price_cents,
            day_volume,
            seq: e.seq,
            traded_at_ns: e.event_time_ns,
        };
        if let Err(err) = self.ticker.write(&e.symbol, tick).await {
            // Reduced mode: refresh_tickers fills it in once Redis is back.
            tracing::warn!(symbol = %e.symbol, error = %err, "ticker not updated");
        }
        Ok(())
    }

    /// Writes every stock's ticker from the recorded trades. Run it now and
    /// then: after a Redis wipe this restores the reference prices (writes
    /// never replace a newer value). Returns how many tickers were written.
    pub async fn refresh_tickers(&self) -> Result<usize, String> {
        let latest: Vec<(String, i64, i64, OffsetDateTime)> = sqlx::query_as(
            "SELECT DISTINCT ON (symbol) symbol, seq, price_cents, executed_at
             FROM market_trades ORDER BY symbol, seq DESC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        let mut written = 0;
        for (symbol, seq, price, at) in latest {
            let mut conn = self.pool.acquire().await.map_err(|e| e.to_string())?;
            let volume = day_volume(&mut conn, &symbol, at)
                .await
                .map_err(|e| e.to_string())?;
            let tick = ticker::Tick {
                last_price_cents: price,
                day_volume: volume,
                seq: u64::try_from(seq).unwrap_or(0),
                traded_at_ns: i64::try_from(at.unix_timestamp_nanos()).unwrap_or(0),
            };
            if self
                .ticker
                .write(&symbol, tick)
                .await
                .map_err(|e| e.to_string())?
            {
                written += 1;
            }
        }
        Ok(written)
    }
}

/// Shares of `symbol` traded on the UTC day of `at`.
pub async fn day_volume(
    conn: &mut sqlx::PgConnection,
    symbol: &str,
    at: OffsetDateTime,
) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(volume), 0)::bigint FROM candles_1m
         WHERE symbol = $1
           AND bucket_start >= date_trunc('day', $2 AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
           AND bucket_start <= $2",
    )
    .bind(symbol)
    .bind(at)
    .fetch_one(conn)
    .await
}
