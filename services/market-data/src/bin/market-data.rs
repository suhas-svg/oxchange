//! Market Data service.
//!
//! ```text
//! market-data [serve]   recorder, live view, REST and /ws/public
//! market-data migrate   apply database migrations, then exit
//! market-data health    exit 0 if this host's /readyz answers 200 (container healthcheck)
//! ```
//!
//! Environment: DATABASE_URL, KAFKA_BROKERS, REDIS_URL, MATCHING_GRPC_URLS
//! (comma-separated, one per partition in partition order); optional
//! HTTP_ADDR (0.0.0.0:8080).

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use market_data::http::{App, router};
use market_data::{GrpcBooks, Live, MATCHING_EVENTS_TOPIC, MIGRATOR, RECORDER_GROUP, Recorder};
use prost::Message;
use proto_gen::events::v1 as ev;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// How often the recorder rewrites tickers, which refills Redis after a wipe.
const TICKER_REFRESH: Duration = Duration::from_secs(10);

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

fn health_probe() -> Result<(), BoxError> {
    use std::io::{Read, Write};
    let addr = env_or("HTTP_ADDR", "0.0.0.0:8080").replace("0.0.0.0", "127.0.0.1");
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
    let urls: Vec<String> = env("MATCHING_GRPC_URLS")?
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();
    let books = GrpcBooks::connect_lazy(&urls)?;
    let ticker = Arc::new(ticker::Store::new(&env("REDIS_URL")?)?);
    let pool = connect().await?;
    let http_addr = env_or("HTTP_ADDR", "0.0.0.0:8080");
    let recorder = Arc::new(Recorder::new(pool.clone(), ticker));
    let live = Arc::new(Live::new(books, Some(pool.clone())));
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();

    let app = Arc::new(App {
        live: live.clone(),
        pool: pool.clone(),
    });
    let listener = tokio::net::TcpListener::bind(&http_addr).await?;
    let http_stop = stopped(stop_rx.clone());
    tasks.spawn(async move {
        let _ = axum::serve(listener, router(app))
            .with_graceful_shutdown(http_stop)
            .await;
    });

    // Recorder: trades, candles, ticker.
    let consumer = consumer::kafka::consumer(&brokers, RECORDER_GROUP, MATCHING_EVENTS_TOPIC)?;
    let r = recorder.clone();
    tasks.spawn(consumer::kafka::run(
        consumer,
        move |rec| {
            let r = r.clone();
            let decoded = ev::MatchingEvent::decode(rec.payload);
            async move {
                let e = decoded
                    .map_err(|e| consumer::HandlerError::Halt(format!("undecodable event: {e}")))?;
                r.apply_event(&e).await
            }
        },
        stopped(stop_rx.clone()),
    ));

    // Ticker refresh: refills Redis after a wipe.
    let r = recorder.clone();
    let mut stop = stop_rx.clone();
    tasks.spawn(async move {
        loop {
            match r.refresh_tickers().await {
                Ok(0) => {}
                Ok(n) => tracing::info!(written = n, "tickers restored"),
                Err(e) => tracing::warn!(error = %e, "ticker refresh failed"),
            }
            tokio::select! {
                _ = stop.wait_for(|s| *s) => return,
                () = tokio::time::sleep(TICKER_REFRESH) => {}
            }
        }
    });

    // Live view.
    let l = live.clone();
    let live_stop = stopped(stop_rx.clone());
    tasks.spawn(async move {
        if let Err(e) =
            market_data::kafka::follow(&brokers, MATCHING_EVENTS_TOPIC, l, live_stop).await
        {
            tracing::error!(error = %e, "live view stopped");
        }
    });

    // websocket_clients and websocket_dropped_slow_total, as a log line
    // until metrics land.
    let l = live.clone();
    let mut stop = stop_rx.clone();
    tasks.spawn(async move {
        loop {
            tokio::select! {
                _ = stop.wait_for(|s| *s) => return,
                () = tokio::time::sleep(Duration::from_secs(60)) => {}
            }
            let clients = l.clients().await;
            let dropped = l.dropped_slow.load(Ordering::Relaxed);
            if clients > 0 || dropped > 0 {
                tracing::info!(
                    websocket_clients = clients,
                    websocket_dropped_slow_total = dropped,
                    "public stream"
                );
            }
        }
    });

    tracing::info!(http = %http_addr, "market data running");
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
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args: Vec<String> = std::env::args().collect();
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
            tracing::error!(error = %e, "market data failed");
            ExitCode::FAILURE
        }
    }
}
