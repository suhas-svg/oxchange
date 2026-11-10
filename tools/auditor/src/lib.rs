//! The auditor (spec: "Testing strategy → Auditor"): a command-line job, not
//! a service, that proves the books still balance. It reads each database
//! through a read-only login and, with the `kafka` feature, scans
//! `matching.events`.
//!
//! | # | Rule | When |
//! |---|------|------|
//! | 1 | Each ledger transaction adds up to zero per asset | Always |
//! | 2 | All accounts add up to zero per asset | Always |
//! | 3 | Each account balance equals the sum of its entries | Always |
//! | 4 | No user balance is negative | Always |
//! | 5 | Reserved balance equals the sum of active reservations | Traffic stopped |
//! | 6 | Filled plus canceled quantity never exceeds order quantity | Always |
//! | 7 | Each trade event has exactly one settled record and one ledger transaction | Traffic stopped |
//! | 8 | Each order's filled quantity equals the sum of its trades | Traffic stopped |
//! | 9 | Event sequence numbers have no gaps | Always (needs the events) |
//! | 10 | Total shares of each stock equal the shares issued | Always |
//! | 11 | No user funds remain reserved for a finished order | Traffic stopped |
//!
//! Rules that span databases only run once traffic stops: the databases
//! cannot be read at one shared instant, and in flight they legitimately
//! disagree for a few milliseconds.

#[cfg(feature = "kafka")]
pub mod kafka;

use std::collections::{BTreeMap, HashMap, HashSet};

use proto_gen::events::v1 as ev;
use sqlx::PgPool;
use uuid::Uuid;

pub use accounts::audit::When;

pub struct Sources<'a> {
    pub accounts: &'a PgPool,
    pub risk: &'a PgPool,
    pub settlement: &'a PgPool,
    /// Every `matching.events` message, repeats included. `None` skips
    /// rule 9, and rule 7 then trusts Settlement's list of trades.
    pub events: Option<&'a [ev::MatchingEvent]>,
}

#[derive(Debug, Default)]
pub struct Report {
    /// Every broken rule, described. Empty means the books balance.
    pub problems: Vec<String>,
    /// Rules that could not run, and why.
    pub skipped: Vec<String>,
}

impl Report {
    pub fn passed(&self) -> bool {
        self.problems.is_empty()
    }
}

/// Runs every rule that applies at `when`.
pub async fn check(s: &Sources<'_>, when: When) -> Result<Report, sqlx::Error> {
    let mut r = Report {
        problems: accounts::audit::check(s.accounts, when).await?,
        ..Report::default()
    };
    rule_6(s.risk, &mut r).await?;
    match s.events {
        Some(events) => rule_9(events, &mut r),
        None => r
            .skipped
            .push("rule 9: no matching.events scan (build with the kafka feature)".into()),
    }
    rule_10(s.accounts, &mut r).await?;
    if when == When::TrafficStopped {
        let trades = settlement_trades(s.settlement).await?;
        rule_7(s, &trades, &mut r).await?;
        rule_8(s.risk, &trades, &mut r).await?;
        rule_11(s.accounts, s.risk, &mut r).await?;
    } else {
        r.skipped
            .push("rules 7, 8 and 11 run only after traffic stops".into());
    }
    Ok(r)
}

async fn rule_6(risk: &PgPool, r: &mut Report) -> sqlx::Result<()> {
    let rows: Vec<(Uuid, i64, i64, i64)> = sqlx::query_as(
        "SELECT order_id, quantity, filled_quantity, canceled_quantity FROM orders
         WHERE filled_quantity + canceled_quantity > quantity",
    )
    .fetch_all(risk)
    .await?;
    r.problems.extend(rows.into_iter().map(|(id, q, f, c)| {
        format!("rule 6: order {id} has {f} filled + {c} canceled of only {q}")
    }));
    Ok(())
}

fn rule_9(events: &[ev::MatchingEvent], r: &mut Report) {
    let mut seqs: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
    for e in events {
        seqs.entry(e.symbol.as_str()).or_default().push(e.seq);
    }
    for (symbol, mut s) in seqs {
        s.sort_unstable();
        s.dedup();
        // Retention may have dropped the oldest events, so start from the
        // first one still kept; a stream that is all there starts at 1.
        for w in s.windows(2) {
            if w[1] != w[0] + 1 {
                r.problems.push(format!(
                    "rule 9: {symbol} jumps from seq {} to {}",
                    w[0], w[1]
                ));
            }
        }
    }
}

async fn rule_10(accounts: &PgPool, r: &mut Report) -> sqlx::Result<()> {
    let mut tx = accounts.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    // Issued: what deposits brought in and withdrawals took out.
    let issued: HashMap<String, i64> = sqlx::query_as(
        "SELECT a.asset, SUM(e.amount)::bigint
         FROM ledger_entries e
         JOIN ledger_transactions t ON t.id = e.transaction_id
         JOIN ledger_accounts a ON a.id = e.account_id
         WHERE t.kind IN ('deposit', 'withdraw') AND a.kind <> 'external'
         GROUP BY a.asset",
    )
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .collect();
    // Held: what users own now, available or reserved.
    let held: HashMap<String, i64> = sqlx::query_as(
        "SELECT asset, SUM(balance)::bigint FROM ledger_accounts
         WHERE kind <> 'external' GROUP BY asset",
    )
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .collect();
    tx.commit().await?;
    let assets: HashSet<&String> = issued.keys().chain(held.keys()).collect();
    for asset in assets {
        let (i, h) = (
            issued.get(asset).copied().unwrap_or(0),
            held.get(asset).copied().unwrap_or(0),
        );
        if i != h {
            r.problems.push(format!(
                "rule 10: users hold {h} {asset} but {i} were issued"
            ));
        }
    }
    Ok(())
}

/// A row of Settlement's `trades`.
struct Trade {
    price: i64,
    quantity: i64,
    buy_order: Uuid,
    sell_order: Uuid,
    settled: bool,
}

async fn settlement_trades(settlement: &PgPool) -> sqlx::Result<HashMap<String, Trade>> {
    let rows: Vec<(String, i64, i64, Uuid, Uuid, String)> = sqlx::query_as(
        "SELECT trade_id, price_cents, quantity, buy_order_id, sell_order_id, status FROM trades",
    )
    .fetch_all(settlement)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, price, quantity, buy_order, sell_order, status)| {
            (
                id,
                Trade {
                    price,
                    quantity,
                    buy_order,
                    sell_order,
                    settled: status == "SETTLED",
                },
            )
        })
        .collect())
}

async fn rule_7(
    s: &Sources<'_>,
    trades: &HashMap<String, Trade>,
    r: &mut Report,
) -> sqlx::Result<()> {
    // Ledger transaction keys are unique, so each trade has at most one.
    let ledger: HashSet<String> = sqlx::query_as::<_, (String,)>(
        "SELECT idempotency_key FROM ledger_transactions WHERE kind = 'settle'",
    )
    .fetch_all(s.accounts)
    .await?
    .into_iter()
    .filter_map(|(k,)| k.strip_prefix("settle:").map(str::to_owned))
    .collect();

    // The trades that happened: from the events when we have them (repeats
    // collapse by trade ID), otherwise Settlement's own list.
    let executed: HashSet<String> = match s.events {
        Some(events) => {
            let mut seen = HashSet::new();
            for e in events {
                let Some(ev::matching_event::Body::Trade(t)) = &e.body else {
                    continue;
                };
                if !seen.insert(t.trade_id.clone()) {
                    continue;
                }
                match trades.get(&t.trade_id) {
                    Some(row)
                        if (row.price, row.quantity) != (t.price_cents, t.quantity)
                            || row.buy_order.to_string() != t.buy_order_id
                            || row.sell_order.to_string() != t.sell_order_id =>
                    {
                        r.problems.push(format!(
                            "rule 7: Settlement's record of trade {} differs from the event",
                            t.trade_id
                        ));
                    }
                    _ => {}
                }
            }
            seen
        }
        None => trades.keys().cloned().collect(),
    };
    for id in &executed {
        match trades.get(id) {
            None => r
                .problems
                .push(format!("rule 7: trade {id} has no settlement record")),
            Some(t) if !t.settled => r
                .problems
                .push(format!("rule 7: trade {id} is still PENDING")),
            Some(_) => {}
        }
        if !ledger.contains(id) {
            r.problems
                .push(format!("rule 7: trade {id} has no ledger transaction"));
        }
    }
    for id in trades.keys().filter(|id| !executed.contains(*id)) {
        r.problems.push(format!(
            "rule 7: settlement record {id} matches no trade event"
        ));
    }
    for id in ledger.iter().filter(|id| !executed.contains(*id)) {
        r.problems.push(format!(
            "rule 7: ledger settled {id}, which matches no trade event"
        ));
    }
    Ok(())
}

async fn rule_8(
    risk: &PgPool,
    trades: &HashMap<String, Trade>,
    r: &mut Report,
) -> sqlx::Result<()> {
    let mut traded: HashMap<Uuid, i64> = HashMap::new();
    for t in trades.values() {
        *traded.entry(t.buy_order).or_default() += t.quantity;
        *traded.entry(t.sell_order).or_default() += t.quantity;
    }
    let filled: HashMap<Uuid, i64> =
        sqlx::query_as("SELECT order_id, filled_quantity FROM orders WHERE filled_quantity > 0")
            .fetch_all(risk)
            .await?
            .into_iter()
            .collect();
    // An order with trades but no Risk fill shows up as filled 0.
    let missing: Vec<Uuid> = traded
        .keys()
        .filter(|id| !filled.contains_key(id))
        .copied()
        .collect();
    let mut known: HashSet<Uuid> = HashSet::new();
    if !missing.is_empty() {
        known.extend(
            sqlx::query_as::<_, (Uuid,)>("SELECT order_id FROM orders WHERE order_id = ANY($1)")
                .bind(&missing)
                .fetch_all(risk)
                .await?
                .into_iter()
                .map(|(id,)| id),
        );
    }
    for (id, &qty) in &traded {
        let f = filled.get(id).copied().unwrap_or(0);
        if f != qty {
            if f == 0 && !known.contains(id) {
                r.problems.push(format!(
                    "rule 8: trades name order {id}, which Risk never had"
                ));
            } else {
                r.problems.push(format!(
                    "rule 8: order {id} shows {f} filled but its trades total {qty}"
                ));
            }
        }
    }
    for (id, f) in filled.iter().filter(|(id, _)| !traded.contains_key(*id)) {
        r.problems.push(format!(
            "rule 8: order {id} shows {f} filled but has no trades"
        ));
    }
    Ok(())
}

async fn rule_11(accounts: &PgPool, risk: &PgPool, r: &mut Report) -> sqlx::Result<()> {
    let held: Vec<(Uuid, String, i64)> = sqlx::query_as(
        "SELECT order_id, asset, remaining_amount FROM reservations
         WHERE status = 'active' AND remaining_amount > 0",
    )
    .fetch_all(accounts)
    .await?;
    if held.is_empty() {
        return Ok(());
    }
    let ids: Vec<Uuid> = held.iter().map(|(id, _, _)| *id).collect();
    let status: HashMap<Uuid, String> =
        sqlx::query_as("SELECT order_id, status FROM orders WHERE order_id = ANY($1)")
            .bind(&ids)
            .fetch_all(risk)
            .await?
            .into_iter()
            .collect();
    for (id, asset, amount) in held {
        match status.get(&id).map(String::as_str) {
            Some("FILLED" | "CANCELED" | "REJECTED") => r.problems.push(format!(
                "rule 11: {amount} {asset} still reserved for finished order {id} ({})",
                status[&id]
            )),
            Some(_) => {}
            None => r.problems.push(format!(
                "rule 11: {amount} {asset} reserved for order {id}, which Risk does not have"
            )),
        }
    }
    Ok(())
}
