//! ```text
//! auditor [--traffic-stopped]
//! ```
//!
//! Checks the spec's 11 rules and exits 0 only if every rule that ran holds.
//! `--traffic-stopped` adds the rules that hold only once orders stop.
//!
//! Environment: ACCOUNTS_DATABASE_URL, RISK_DATABASE_URL and
//! SETTLEMENT_DATABASE_URL (read-only logins), and KAFKA_BROKERS for rule 9
//! (only in builds with the kafka feature). With PUSHGATEWAY_URL set, the
//! result is also pushed there for Prometheus (`ledger_imbalance_total`
//! and friends), since Prometheus cannot scrape a job that has exited.

use std::process::ExitCode;
use std::time::Duration;

use auditor::{Sources, When};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

async fn connect(var: &str) -> Result<PgPool, BoxError> {
    let url = std::env::var(var).map_err(|_| format!("{var} is required"))?;
    Ok(PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await?)
}

#[cfg(feature = "kafka")]
async fn events() -> Result<Option<Vec<proto_gen::events::v1::MatchingEvent>>, BoxError> {
    let Ok(brokers) = std::env::var("KAFKA_BROKERS") else {
        return Ok(None);
    };
    let events =
        tokio::task::spawn_blocking(move || auditor::kafka::scan(&brokers, "matching.events"))
            .await??;
    Ok(Some(events))
}

#[cfg(not(feature = "kafka"))]
async fn events() -> Result<Option<Vec<proto_gen::events::v1::MatchingEvent>>, BoxError> {
    Ok(None)
}

fn when() -> When {
    if std::env::args().any(|a| a == "--traffic-stopped") {
        When::TrafficStopped
    } else {
        When::Always
    }
}

async fn run(when: When) -> Result<bool, BoxError> {
    let (accounts, risk, settlement) = (
        connect("ACCOUNTS_DATABASE_URL").await?,
        connect("RISK_DATABASE_URL").await?,
        connect("SETTLEMENT_DATABASE_URL").await?,
    );
    let events = events().await?;
    let report = auditor::check(
        &Sources {
            accounts: &accounts,
            risk: &risk,
            settlement: &settlement,
            events: events.as_deref(),
        },
        when,
    )
    .await?;
    for s in &report.skipped {
        tracing::warn!(skipped = %s, "rule not checked");
    }
    for p in &report.problems {
        tracing::error!(problem = %p, "auditor rule broken");
    }
    tracing::info!(
        ledger_imbalance_total = report.problems.len(),
        events_scanned = events.as_ref().map_or(0, Vec::len),
        traffic_stopped = when == When::TrafficStopped,
        "{}",
        if report.passed() { "PASS" } else { "FAIL" }
    );
    // A count of broken rules from this run, so a gauge (the spec's name).
    metrics::gauge!("ledger_imbalance_total").set(report.problems.len() as f64);
    metrics::gauge!("auditor_rules_skipped").set(report.skipped.len() as f64);
    metrics::gauge!("auditor_passed").set(if report.passed() { 1.0 } else { 0.0 });
    Ok(report.passed())
}

/// Pushes this run's metrics, grouped by mode so a routine run does not
/// overwrite the last traffic-stopped one.
async fn push(telemetry: &telemetry::Telemetry, when: When, ran: bool) {
    let Ok(url) = std::env::var("PUSHGATEWAY_URL") else {
        return;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    metrics::gauge!("auditor_last_run_timestamp_seconds").set(now.as_secs_f64());
    metrics::gauge!("auditor_completed").set(if ran { 1.0 } else { 0.0 });
    let mode = match when {
        When::TrafficStopped => "traffic_stopped",
        When::Always => "always",
    };
    let body = telemetry.render_metrics();
    match telemetry::push::push(&url, &format!("auditor/mode/{mode}"), &body).await {
        Ok(()) => tracing::info!(%url, "pushed the result"),
        Err(e) => tracing::warn!(error = %e, "could not push the result"),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let telemetry = telemetry::init_with("auditor", telemetry::Mode::Batch);
    let when = when();
    let result = run(when).await;
    push(&telemetry, when, result.is_ok()).await;
    match result {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            tracing::error!(error = %e, "auditor could not run");
            ExitCode::from(2)
        }
    }
}
