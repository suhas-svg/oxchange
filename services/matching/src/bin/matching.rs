//! The matching engine service for one partition.
//!
//! Configuration (environment):
//! - `KAFKA_BROKERS` (required), for example `redpanda:9092`.
//! - `SNAPSHOT_DIR` (required), for example `/var/lib/matching`.
//! - `PARTITION`, or derived from a StatefulSet hostname ending `-<n>`
//!   (`matching-2` owns partition 2).
//! - `SNAPSHOT_INTERVAL` (default 100000), `DEDUP_CAPACITY` (default 1000000).
//! - `RUST_LOG` (default `info`).
//!
//! Exits non-zero when it halts, so the orchestrator restarts it and the
//! restart recovers from the newest valid snapshot.

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use matching::kafka::{KafkaSink, KafkaSource};
use matching::pipeline::{self, PipelineConfig};
use matching::{DEFAULT_DEDUP_CAPACITY, SnapshotStore, recover, snapshot};

struct Config {
    brokers: String,
    snapshot_dir: String,
    partition: u32,
    snapshot_interval: u64,
    dedup_capacity: usize,
}

fn required(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("{name} is required"))
}

fn parsed<T: std::str::FromStr>(name: &str, default: T) -> Result<T, String> {
    match std::env::var(name) {
        Ok(v) => v.parse().map_err(|_| format!("{name}={v:?} is not valid")),
        Err(_) => Ok(default),
    }
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let partition = match std::env::var("PARTITION") {
            Ok(p) => p
                .parse()
                .map_err(|_| format!("PARTITION={p:?} is not a number"))?,
            Err(_) => {
                let host = std::env::var("HOSTNAME").unwrap_or_default();
                host.rsplit('-')
                    .next()
                    .and_then(|n| n.parse().ok())
                    .ok_or("set PARTITION, or run under a hostname ending in -<n>")?
            }
        };
        Ok(Self {
            brokers: required("KAFKA_BROKERS")?,
            snapshot_dir: required("SNAPSHOT_DIR")?,
            partition,
            snapshot_interval: parsed("SNAPSHOT_INTERVAL", snapshot::DEFAULT_SNAPSHOT_INTERVAL)?,
            dedup_capacity: parsed("DEDUP_CAPACITY", DEFAULT_DEDUP_CAPACITY)?,
        })
    }
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "matching engine stopped");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let started = Instant::now();
    let cfg = Config::from_env()?;
    tracing::info!(partition = cfg.partition, "starting matching engine");

    let store = SnapshotStore::new(&cfg.snapshot_dir, cfg.partition).map_err(|e| e.to_string())?;
    let recovery = recover(&store, cfg.partition, cfg.dedup_capacity).map_err(|e| e.to_string())?;
    for s in &recovery.skipped {
        tracing::warn!(path = %s.path.display(), reason = %s.reason, "skipped unusable snapshot");
    }
    let next_offset = recovery.engine.next_offset();
    tracing::info!(
        snapshot = recovery.snapshot.as_ref().map(|p| p.display().to_string()),
        next_offset,
        load_ms = started.elapsed().as_millis() as u64,
        "engine state recovered"
    );

    let source = KafkaSource::connect(&cfg.brokers, cfg.partition, next_offset, started)
        .map_err(|e| e.to_string())?;
    let sink = KafkaSink::connect(&cfg.brokers, cfg.partition).map_err(|e| e.to_string())?;

    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = stop.clone();
        ctrlc::set_handler(move || {
            tracing::info!("shutdown requested; draining");
            stop.store(true, Ordering::Relaxed);
        })
        .map_err(|e| e.to_string())?;
    }

    let config = PipelineConfig {
        snapshot_interval: cfg.snapshot_interval,
        ..PipelineConfig::default()
    };
    let report = pipeline::run(recovery.engine, source, sink, store, config, stop)
        .map_err(|e| e.to_string())?;
    tracing::info!(?report, "matching engine shut down cleanly");
    Ok(())
}
