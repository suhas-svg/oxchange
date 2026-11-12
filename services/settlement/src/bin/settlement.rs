//! Settlement service.
//!
//! ```text
//! settlement [serve]   matching.events consumer and settlement.events relay
//! settlement migrate   apply database migrations, then exit
//! settlement health    exit 0 if this host's /readyz answers 200 (container healthcheck)
//! ```
//!
//! Environment: DATABASE_URL, ACCOUNTS_GRPC_URL, KAFKA_BROKERS; optional
//! HEALTH_ADDR (0.0.0.0:8081).

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use prost::Message;
use proto_gen::events::v1 as ev;
use settlement::{GrpcAccounts, MATCHING_EVENTS_TOPIC, MIGRATOR, OUTBOX_LOCK_KEY, Settlement};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::watch;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn env(name: &str) -> Result<String, BoxError> {
    std::env::var(name).map_err(|_| format!("{name} is required").into())
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

async fn connect() -> Result<PgPool, BoxError> {
    let max: u32 = env_or("DB_MAX_CONNECTIONS", "8").parse()?;
    Ok(PgPoolOptions::new()
        .max_connections(max)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&env("DATABASE_URL")?)
        .await?)
}

async fn ready(State(pool): State<PgPool>) -> StatusCode {
    match sqlx::query("SELECT 1").execute(&pool).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

fn health_probe() -> Result<(), BoxError> {
    use std::io::{Read, Write};
    let addr = env_or("HEALTH_ADDR", "0.0.0.0:8081").replace("0.0.0.0", "127.0.0.1");
    let mut s = std::net::TcpStream::connect(addr)?;
    s.set_read_timeout(Some(Duration::from_secs(2)))?;
    s.write_all(b"GET /readyz HTTP/1.0\r\n\r\n")?;
    let mut head = [0u8; 12];
    s.read_exact(&mut head)?;
    if head.ends_with(b" 200") {
        Ok(())
    } else {
        Err(format!("not ready: {}", String::from_utf8_lossy(&head)).into())
    }
}

async fn stopped(mut rx: watch::Receiver<bool>) {
    let _ = rx.wait_for(|stop| *stop).await;
}

async fn signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _ = ctrl_c => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}

async fn serve() -> Result<(), BoxError> {
    let brokers = env("KAFKA_BROKERS")?;
    let accounts = GrpcAccounts::connect_lazy(&env("ACCOUNTS_GRPC_URL")?)?;
    let pool = connect().await?;
    telemetry::watch_pool(pool.clone());
    let health_addr = env_or("HEALTH_ADDR", "0.0.0.0:8081");
    let settlement = Arc::new(Settlement::new(pool.clone(), accounts));
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();

    let health = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/readyz", get(ready))
        .with_state(pool.clone());
    let health_listener = tokio::net::TcpListener::bind(&health_addr).await?;
    let health_stop = stopped(stop_rx.clone());
    tasks.spawn(async move {
        let _ = axum::serve(health_listener, health)
            .with_graceful_shutdown(health_stop)
            .await;
    });

    // Every 5 s: settlement_pending (trades not yet settled) and
    // settlement_oldest_pending_seconds, which drives the "Settlement
    // halted" alert (a trade waiting over 60 s).
    let p = pool.clone();
    let mut stop = stop_rx.clone();
    tasks.spawn(async move {
        let pending = metrics::gauge!("settlement_pending");
        let oldest = metrics::gauge!("settlement_oldest_pending_seconds");
        loop {
            let row: Result<(i64, f64), _> = sqlx::query_as(
                "SELECT count(*),
                        coalesce(extract(epoch FROM now() - min(executed_at)), 0)::float8
                 FROM trades WHERE status = 'PENDING'",
            )
            .fetch_one(&p)
            .await;
            if let Ok((n, age)) = row {
                pending.set(n as f64);
                oldest.set(age);
            }
            tokio::select! {
                _ = stop.wait_for(|s| *s) => return,
                () = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        }
    });

    // Outbox relay: settlement.events.
    let publisher = outbox::kafka::KafkaPublisher::connect(&brokers)?;
    tasks.spawn(outbox::run(
        pool.clone(),
        publisher,
        outbox::RelayConfig::new(OUTBOX_LOCK_KEY),
        stopped(stop_rx.clone()),
    ));

    // matching.events consumer.
    let consumer = consumer::kafka::consumer(&brokers, "settlement", MATCHING_EVENTS_TOPIC)?;
    let s = settlement.clone();
    tasks.spawn(consumer::kafka::run(
        consumer,
        move |rec| {
            let s = s.clone();
            let decoded = ev::MatchingEvent::decode(rec.payload);
            async move {
                let e = decoded
                    .map_err(|e| consumer::HandlerError::Halt(format!("undecodable event: {e}")))?;
                s.apply_event(&e).await
            }
        },
        stopped(stop_rx.clone()),
    ));

    // The settlement_pending gauge, as a log line until metrics land.
    let s = settlement.clone();
    let mut stop = stop_rx.clone();
    tasks.spawn(async move {
        loop {
            tokio::select! {
                _ = stop.wait_for(|s| *s) => return,
                () = tokio::time::sleep(Duration::from_secs(30)) => {}
            }
            match s.pending().await {
                Ok(0) => {}
                Ok(n) => tracing::info!(settlement_pending = n, "trades waiting to settle"),
                Err(e) => tracing::warn!(error = %e, "could not count pending trades"),
            }
        }
    });

    tracing::info!(health = %health_addr, "settlement running");
    signal().await;
    tracing::info!("shutting down");
    let _ = stop_tx.send(true);
    // Spec: finish current work and exit within 25 seconds.
    let _ = tokio::time::timeout(Duration::from_secs(20), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    tasks.abort_all();
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let _telemetry = telemetry::init_with(
        "settlement",
        telemetry::Mode::for_command(args.get(1).map(String::as_str)),
    );
    let result = match args.get(1).map(String::as_str) {
        None | Some("serve") => serve().await,
        Some("migrate") => {
            async {
                MIGRATOR.run(&connect().await?).await?;
                tracing::info!("migrations applied");
                Ok(())
            }
            .await
        }
        Some("health") => health_probe(),
        Some(other) => Err(format!("unknown command {other:?}").into()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "settlement failed");
            ExitCode::FAILURE
        }
    }
}
