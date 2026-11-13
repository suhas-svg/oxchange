//! Risk service.
//!
//! ```text
//! risk [serve]   gRPC, outbox relay, matching.events consumer and sweeper
//! risk migrate   apply database migrations, then exit
//! risk health    exit 0 if this host's /readyz answers 200 (container healthcheck)
//! ```
//!
//! Environment: DATABASE_URL, ACCOUNTS_GRPC_URL, KAFKA_BROKERS, REDIS_URL; optional
//! LISTEN_ADDR (0.0.0.0:50051) and HEALTH_ADDR (0.0.0.0:8081).

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use prost::Message;
use proto_gen::events::v1 as ev;
use risk::{
    GrpcAccounts, MATCHING_EVENTS_TOPIC, MIGRATOR, OUTBOX_LOCK_KEY, PENDING_TIMEOUT, Risk, grpc,
};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::watch;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const SWEEP_EVERY: Duration = Duration::from_secs(5);

fn env(name: &str) -> Result<String, BoxError> {
    std::env::var(name).map_err(|_| format!("{name} is required").into())
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

async fn connect() -> Result<PgPool, BoxError> {
    let max: u32 = env_or("DB_MAX_CONNECTIONS", "16").parse()?;
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

/// Resolves once shutdown starts.
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
    let listen = env_or("LISTEN_ADDR", "0.0.0.0:50051").parse()?;
    let health_addr = env_or("HEALTH_ADDR", "0.0.0.0:8081");
    let prices = Arc::new(ticker::Store::new(&env("REDIS_URL")?)?);
    let risk = Arc::new(Risk::new(pool.clone(), accounts).with_prices(prices));
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

    // Outbox relay: orders.commands.
    let publisher = outbox::kafka::KafkaPublisher::connect(&brokers)?;
    tasks.spawn(outbox::run(
        pool.clone(),
        publisher,
        outbox::RelayConfig::new(OUTBOX_LOCK_KEY),
        stopped(stop_rx.clone()),
    ));

    // matching.events consumer.
    let consumer = consumer::kafka::consumer(&brokers, "risk", MATCHING_EVENTS_TOPIC)?;
    let r = risk.clone();
    tasks.spawn(consumer::kafka::run_batches(
        consumer,
        consumer::kafka::max_batch_from_env(),
        move |batch: Vec<consumer::kafka::Record<'_>>| {
            let r = r.clone();
            let decoded: Result<Vec<_>, _> = batch
                .iter()
                .map(|rec| ev::MatchingEvent::decode(rec.payload))
                .collect();
            async move {
                let events = decoded
                    .map_err(|e| consumer::HandlerError::Halt(format!("undecodable event: {e}")))?;
                r.apply_events(&events).await
            }
        },
        stopped(stop_rx.clone()),
    ));

    // Sweeper.
    let r = risk.clone();
    let mut stop = stop_rx.clone();
    tasks.spawn(async move {
        loop {
            tokio::select! {
                _ = stop.wait_for(|s| *s) => return,
                () = tokio::time::sleep(SWEEP_EVERY) => {}
            }
            if let Err(e) = r.sweep(PENDING_TIMEOUT).await {
                tracing::warn!(error = %e, "sweep failed");
            }
        }
    });

    tracing::info!(%listen, health = %health_addr, "risk serving");
    let grpc_stop = stopped(stop_rx.clone());
    let server = tonic::transport::Server::builder()
        .layer(telemetry::server::ServerLayer::grpc())
        .add_service(grpc::Service::server(risk))
        .serve_with_shutdown(listen, grpc_stop);
    tokio::select! {
        r = server => r?,
        () = signal() => tracing::info!("shutting down"),
    }
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
        "risk",
        telemetry::Mode::for_command(args.get(1).map(String::as_str)),
    );
    // Crash points for the live failure tests; only with the `failpoints`
    // feature and a non-empty FAILPOINTS (e.g. "NAME=1*return(exit)").
    let _failpoints = std::env::var("FAILPOINTS")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(|v| {
            tracing::warn!(failpoints = %v, "crash points are on");
            fail::FailScenario::setup()
        });
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
            tracing::error!(error = %e, "risk failed");
            ExitCode::FAILURE
        }
    }
}
