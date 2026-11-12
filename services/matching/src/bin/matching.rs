//! The matching engine service for one partition.
//!
//! Configuration (environment):
//! - `KAFKA_BROKERS` (required), for example `redpanda:9092`.
//! - `SNAPSHOT_DIR` (required), for example `/var/lib/matching`.
//! - `PARTITION`, or derived from a StatefulSet hostname ending `-<n>`
//!   (`matching-2` owns partition 2).
//! - `SNAPSHOT_INTERVAL` (default 100000), `DEDUP_CAPACITY` (default 1000000).
//! - `GRPC_ADDR` (default `0.0.0.0:50051`) for `MatchingService.GetBook`.
//! - `HEALTH_ADDR` (default `0.0.0.0:8081`) for `/healthz` (the process is
//!   alive) and `/readyz` (replay has finished).
//! - `RUST_LOG` (default `info`).
//!
//! Exits non-zero when it halts, so the orchestrator restarts it and the
//! restart recovers from the newest valid snapshot.

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use matching::kafka::{KafkaSink, KafkaSource};
use matching::pipeline::{self, PipelineConfig};
use matching::{DEFAULT_DEDUP_CAPACITY, SnapshotStore, recover, snapshot};

struct Config {
    brokers: String,
    snapshot_dir: String,
    partition: u32,
    snapshot_interval: u64,
    dedup_capacity: usize,
    grpc_addr: std::net::SocketAddr,
    health_addr: std::net::SocketAddr,
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
            grpc_addr: parsed("GRPC_ADDR", ([0, 0, 0, 0], 50051).into())?,
            health_addr: parsed("HEALTH_ADDR", ([0, 0, 0, 0], 8081).into())?,
        })
    }
}

/// Answers Kubernetes probes on a plain thread, so a busy matcher loop or
/// a long replay never delays them. `/healthz` is always 200; `/readyz` is
/// 200 once replay has finished and 503 before.
fn serve_health(addr: std::net::SocketAddr, ready: Arc<AtomicBool>) -> Result<(), String> {
    use std::io::{Read, Write};
    let listener =
        std::net::TcpListener::bind(addr).map_err(|e| format!("HEALTH_ADDR {addr}: {e}"))?;
    std::thread::Builder::new()
        .name("matching-health".into())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut s) = stream else { continue };
                let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
                let _ = s.set_write_timeout(Some(Duration::from_secs(2)));
                let mut buf = [0u8; 256];
                let n = s.read(&mut buf).unwrap_or(0);
                let line = String::from_utf8_lossy(&buf[..n]);
                let path = line.split_whitespace().nth(1).unwrap_or("");
                let status = match path {
                    "/healthz" => "200 OK",
                    "/readyz" if ready.load(Ordering::Relaxed) => "200 OK",
                    "/readyz" => "503 Service Unavailable",
                    _ => "404 Not Found",
                };
                let _ = s.write_all(
                    format!(
                        "HTTP/1.1 {status}
Content-Length: 0
Connection: close

"
                    )
                    .as_bytes(),
                );
            }
        })
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn main() -> ExitCode {
    let _telemetry = telemetry::init("matching");
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
    // Probes answer from the first moment: alive, but not ready until replay ends.
    let ready = Arc::new(AtomicBool::new(false));
    serve_health(cfg.health_addr, ready.clone())?;

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

    let source = KafkaSource::connect(&cfg.brokers, cfg.partition, next_offset, started, ready)
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

    // GetBook runs on its own small async runtime; the matcher answers its
    // queries between commands.
    let (queries_tx, queries) = std::sync::mpsc::sync_channel(64);
    let addr = cfg.grpc_addr;
    std::thread::Builder::new()
        .name("matching-grpc".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("grpc runtime");
            rt.block_on(async move {
                tracing::info!(%addr, "GetBook listening");
                if let Err(e) = tonic::transport::Server::builder()
                    .layer(telemetry::server::ServerLayer::grpc())
                    .add_service(matching::grpc::Service::server(queries_tx))
                    .serve(addr)
                    .await
                {
                    tracing::error!(error = %e, "GetBook server stopped");
                }
            });
        })
        .map_err(|e| e.to_string())?;

    let config = PipelineConfig {
        snapshot_interval: cfg.snapshot_interval,
        ..PipelineConfig::default()
    };
    let report =
        pipeline::run_with_queries(recovery.engine, source, sink, store, config, stop, queries)
            .map_err(|e| e.to_string())?;
    tracing::info!(?report, "matching engine shut down cleanly");
    Ok(())
}
