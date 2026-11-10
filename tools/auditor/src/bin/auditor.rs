//! ```text
//! auditor [--traffic-stopped]
//! ```
//!
//! Checks the spec's 11 rules and exits 0 only if every rule that ran holds.
//! `--traffic-stopped` adds the rules that hold only once orders stop.
//!
//! Environment: ACCOUNTS_DATABASE_URL, RISK_DATABASE_URL and
//! SETTLEMENT_DATABASE_URL (read-only logins), and KAFKA_BROKERS for rule 9
//! (only in builds with the kafka feature).

use std::process::ExitCode;
use std::time::Duration;

use auditor::{Sources, When};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

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

async fn run() -> Result<bool, BoxError> {
    let when = if std::env::args().any(|a| a == "--traffic-stopped") {
        When::TrafficStopped
    } else {
        When::Always
    };
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
    Ok(report.passed())
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    match run().await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            tracing::error!(error = %e, "auditor could not run");
            ExitCode::from(2)
        }
    }
}
