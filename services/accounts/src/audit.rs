//! The auditor's rules that the Accounts database alone can prove (spec:
//! "Testing strategy → Auditor", rules 1 to 5). Read-only, so the auditor
//! tool can run it through a read-only login.

use sqlx::PgPool;

/// Rule 5 holds only once traffic has stopped; the rest hold at all times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum When {
    Always,
    TrafficStopped,
}

/// Every broken rule, described. Empty means the ledger balances.
pub async fn check(pool: &PgPool, when: When) -> Result<Vec<String>, sqlx::Error> {
    let mut problems = Vec::new();
    // One snapshot for every rule, so concurrent writes cannot make the
    // rules disagree with each other.
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;

    let rows: Vec<(uuid::Uuid, String, i64)> = sqlx::query_as(
        "SELECT e.transaction_id, a.asset, SUM(e.amount)::bigint
         FROM ledger_entries e JOIN ledger_accounts a ON a.id = e.account_id
         GROUP BY e.transaction_id, a.asset HAVING SUM(e.amount) <> 0",
    )
    .fetch_all(&mut *tx)
    .await?;
    problems.extend(rows.into_iter().map(|(t, asset, sum)| {
        format!("rule 1: transaction {t} adds up to {sum} {asset}, not zero")
    }));

    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT asset, SUM(balance)::bigint FROM ledger_accounts
         GROUP BY asset HAVING SUM(balance) <> 0",
    )
    .fetch_all(&mut *tx)
    .await?;
    problems.extend(
        rows.into_iter()
            .map(|(asset, sum)| format!("rule 2: all {asset} accounts add up to {sum}, not zero")),
    );

    let rows: Vec<(i64, i64, i64)> = sqlx::query_as(
        "SELECT a.id, a.balance, COALESCE(e.total, 0)::bigint
         FROM ledger_accounts a
         LEFT JOIN (SELECT account_id, SUM(amount) AS total FROM ledger_entries GROUP BY account_id) e
           ON e.account_id = a.id
         WHERE a.balance <> COALESCE(e.total, 0)",
    )
    .fetch_all(&mut *tx)
    .await?;
    problems.extend(rows.into_iter().map(|(id, balance, total)| {
        format!("rule 3: account {id} has balance {balance} but its entries add up to {total}")
    }));

    let rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT id, balance FROM ledger_accounts WHERE kind <> 'external' AND balance < 0",
    )
    .fetch_all(&mut *tx)
    .await?;
    problems.extend(
        rows.into_iter()
            .map(|(id, balance)| format!("rule 4: user account {id} is negative ({balance})")),
    );

    if when == When::TrafficStopped {
        let rows: Vec<(uuid::Uuid, String, i64, i64)> = sqlx::query_as(
            "SELECT COALESCE(a.owner_id, r.user_id), COALESCE(a.asset, r.asset),
                    COALESCE(a.balance, 0)::bigint, COALESCE(r.total, 0)::bigint
             FROM (SELECT owner_id, asset, balance FROM ledger_accounts WHERE kind = 'reserved') a
             FULL JOIN (SELECT user_id, asset, SUM(remaining_amount) AS total
                        FROM reservations WHERE status = 'active' GROUP BY user_id, asset) r
               ON r.user_id = a.owner_id AND r.asset = a.asset
             WHERE COALESCE(a.balance, 0) <> COALESCE(r.total, 0)",
        )
        .fetch_all(&mut *tx)
        .await?;
        problems.extend(rows.into_iter().map(|(user, asset, balance, total)| {
            format!(
                "rule 5: user {user} has {balance} {asset} reserved but active reservations total {total}"
            )
        }));
    }
    tx.commit().await?;
    Ok(problems)
}
