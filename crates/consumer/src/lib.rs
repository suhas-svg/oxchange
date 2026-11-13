//! Shared duplicate handling for `matching.events` consumers (spec:
//! "Messaging → How consumers handle duplicates").
//!
//! Each consumer stores the last seq it applied per stock:
//! 1. seq at or below the stored value: a repeat; skip it.
//! 2. seq exactly one higher: apply it.
//! 3. seq jumps by more than one: data is missing; halt and alert.
//! 4. Save the effect and the new seq in the same database transaction.
//! 5. Commit the Kafka offset last.
//!
//! A crash between 4 and 5 only causes a repeat, which step 1 ignores.
//!
//! ```sql
//! CREATE TABLE consumer_progress (
//!   symbol   TEXT PRIMARY KEY,
//!   last_seq BIGINT NOT NULL
//! );
//! ```

use std::collections::HashMap;

use sqlx::PgConnection;

#[cfg(feature = "kafka")]
pub mod kafka;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Apply,
    Repeat,
    Gap { expected: u64, got: u64 },
}

/// The rule itself. Seqs start at 1, so a stock with no progress expects 1.
pub fn decide(last_applied: u64, seq: u64) -> Decision {
    if seq <= last_applied {
        Decision::Repeat
    } else if seq == last_applied + 1 {
        Decision::Apply
    } else {
        Decision::Gap {
            expected: last_applied + 1,
            got: seq,
        }
    }
}

/// Decides for `seq` inside the caller's transaction, and on
/// [`Decision::Apply`] records it as applied. The progress row stays locked
/// until the transaction ends, so two copies cannot apply the same event.
pub async fn claim(conn: &mut PgConnection, symbol: &str, seq: u64) -> sqlx::Result<Decision> {
    let mut claims = Claims::default();
    let decision = claims.claim(conn, symbol, seq).await?;
    claims.save(conn).await?;
    Ok(decision)
}

/// [`claim`] for many events in one transaction: each stock's progress row
/// is read and locked once, the decisions are made in memory, and
/// [`Claims::save`] writes each changed row once before the commit.
#[derive(Debug, Default)]
pub struct Claims {
    /// symbol → (last seq stored in the table, last seq applied so far).
    last: HashMap<String, (u64, u64)>,
}

impl Claims {
    pub async fn claim(
        &mut self,
        conn: &mut PgConnection,
        symbol: &str,
        seq: u64,
    ) -> sqlx::Result<Decision> {
        let entry = match self.last.get_mut(symbol) {
            Some(entry) => entry,
            None => {
                let stored = load(conn, symbol).await?;
                self.last
                    .entry(symbol.to_owned())
                    .or_insert((stored, stored))
            }
        };
        let decision = decide(entry.1, seq);
        match decision {
            Decision::Apply => entry.1 = seq,
            Decision::Repeat => {}
            // Spec alert "Event sequence gap": must stay 0.
            // The log line (from the caller's halt) names the stock.
            Decision::Gap { .. } => metrics::counter!("consumer_seq_gaps_total").increment(1),
        }
        Ok(decision)
    }

    /// Records `seq` as applied whatever came before it: for a consumer
    /// that logs a gap and carries on instead of halting.
    pub fn skip_to(&mut self, symbol: &str, seq: u64) {
        if let Some(entry) = self.last.get_mut(symbol) {
            entry.1 = entry.1.max(seq);
        }
    }

    /// Writes the new progress, in the same transaction as the effects.
    pub async fn save(self, conn: &mut PgConnection) -> sqlx::Result<()> {
        for (symbol, (stored, applied)) in self.last {
            if applied == stored {
                continue;
            }
            let applied = i64::try_from(applied).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
            sqlx::query("UPDATE consumer_progress SET last_seq = $2 WHERE symbol = $1")
                .bind(&symbol)
                .bind(applied)
                .execute(&mut *conn)
                .await?;
        }
        Ok(())
    }
}

/// The stored progress for `symbol`, its row created if missing and locked
/// until the transaction ends.
async fn load(conn: &mut PgConnection, symbol: &str) -> sqlx::Result<u64> {
    sqlx::query(
        "INSERT INTO consumer_progress (symbol, last_seq) VALUES ($1, 0) ON CONFLICT DO NOTHING",
    )
    .bind(symbol)
    .execute(&mut *conn)
    .await?;
    let (last,): (i64,) =
        sqlx::query_as("SELECT last_seq FROM consumer_progress WHERE symbol = $1 FOR UPDATE")
            .bind(symbol)
            .fetch_one(&mut *conn)
            .await?;
    Ok(u64::try_from(last).unwrap_or(0))
}

/// What a message handler reports back to the consume loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandlerError {
    /// Something temporary (a dependency is down): try the same message again.
    Retry(String),
    /// Retrying cannot help (a gap, an undecodable message, a broken
    /// invariant): stop this partition and alert.
    Halt(String),
}

impl From<sqlx::Error> for HandlerError {
    fn from(e: sqlx::Error) -> Self {
        HandlerError::Retry(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeats_are_skipped_the_next_seq_applies_and_a_jump_is_a_gap() {
        assert_eq!(decide(0, 1), Decision::Apply);
        assert_eq!(
            decide(0, 2),
            Decision::Gap {
                expected: 1,
                got: 2
            }
        );
        assert_eq!(decide(5, 6), Decision::Apply);
        assert_eq!(decide(5, 5), Decision::Repeat);
        assert_eq!(decide(5, 1), Decision::Repeat);
        assert_eq!(
            decide(5, 9),
            Decision::Gap {
                expected: 6,
                got: 9
            }
        );
    }
}
