//! Notifications service.
//!
//! ```text
//! notifications [serve]   consume events, REST and /ws/private
//! notifications migrate   apply database migrations, then exit
//! notifications health    exit 0 if this host's /readyz answers 200 (container healthcheck)
//! ```
//!
//! Environment: DATABASE_URL, KAFKA_BROKERS, REDIS_URL; optional HTTP_ADDR
//! (0.0.0.0:8080).

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use consumer::HandlerError;
use notifications::http::{App, router};
use notifications::kafka::Dlq;
use notifications::wake::Publisher;
use notifications::{Failure, GROUP, Hub, MIGRATOR, Notifier, TOPICS};
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
    let redis_url = env("REDIS_URL")?;
    let pool = connect().await?;
    telemetry::watch_pool(pool.clone());
    let http_addr = env_or("HTTP_ADDR", "0.0.0.0:8080");
    let notifier = Arc::new(Notifier::new(pool, Arc::new(Publisher::new(&redis_url)?)));
    let hub = Arc::new(Hub::default());
    let dlq = Arc::new(Dlq::connect(&brokers)?);
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();

    let app = Arc::new(App {
        notifier: notifier.clone(),
        hub: hub.clone(),
    });
    let listener = tokio::net::TcpListener::bind(&http_addr).await?;
    let http_stop = stopped(stop_rx.clone());
    tasks.spawn(async move {
        let _ = axum::serve(
            listener,
            router(app).layer(telemetry::server::ServerLayer::http()),
        )
        .with_graceful_shutdown(http_stop)
        .await;
    });

    // websocket_clients and websocket_dropped_slow_total, every 5 s.
    let l = hub.clone();
    let mut stop = stop_rx.clone();
    tasks.spawn(async move {
        let clients = metrics::gauge!("websocket_clients");
        let dropped = metrics::counter!("websocket_dropped_slow_total");
        loop {
            clients.set(l.clients() as f64);
            dropped.absolute(l.dropped_slow.load(std::sync::atomic::Ordering::Relaxed));
            tokio::select! {
                _ = stop.wait_for(|s| *s) => return,
                () = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        }
    });

    // Wake-ups from every copy, for the sockets this copy holds.
    let h = hub.clone();
    let wake_stop = stopped(stop_rx.clone());
    tasks.spawn(async move {
        if let Err(e) = notifications::wake::listen(&redis_url, h, wake_stop).await {
            tracing::error!(error = %e, "wake-up listener stopped");
        }
    });

    // Events in, notifications out. A message that can never be handled
    // goes to the DLQ; the partition keeps moving.
    let consumer = consumer::kafka::consumer_for(&brokers, GROUP, &TOPICS)?;
    let n = notifier.clone();
    tasks.spawn(consumer::kafka::run(
        consumer,
        move |rec| {
            let (n, dlq) = (n.clone(), dlq.clone());
            let (topic, key, payload) = (
                rec.topic.to_owned(),
                rec.key.map(<[u8]>::to_vec),
                rec.payload.to_vec(),
            );
            let (partition, offset) = (rec.partition, rec.offset);
            async move {
                match n.on_message(&topic, &payload).await {
                    Ok(_) => Ok(()),
                    Err(Failure::Retry(e)) => Err(HandlerError::Retry(e)),
                    Err(Failure::Skip(reason)) => {
                        tracing::warn!(%topic, partition, offset, %reason, "message sent to the DLQ");
                        dlq.send(&topic, partition, offset, key.as_deref(), &payload, &reason)
                            .await
                            .map_err(HandlerError::Retry)
                    }
                }
            }
        },
        stopped(stop_rx.clone()),
    ));

    // websocket_clients and websocket_dropped_slow_total, as a log line
    // until metrics land.
    let h = hub.clone();
    let mut stop = stop_rx.clone();
    tasks.spawn(async move {
        loop {
            tokio::select! {
                _ = stop.wait_for(|s| *s) => return,
                () = tokio::time::sleep(Duration::from_secs(60)) => {}
            }
            let clients = h.clients();
            let dropped = h.dropped_slow.load(Ordering::Relaxed);
            if clients > 0 || dropped > 0 {
                tracing::info!(
                    websocket_clients = clients,
                    websocket_dropped_slow_total = dropped,
                    redis_subscription_up = h.live.load(Ordering::Relaxed),
                    "private stream"
                );
            }
        }
    });

    tracing::info!(http = %http_addr, "notifications running");
    signal().await;
    tracing::info!("shutting down");
    let _ = stop_tx.send(true);
    // Spec: finish current work and exit within 25 seconds. Open sockets
    // do not hold the exit up; clients reconnect to another copy.
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
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
        "notifications",
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
            tracing::error!(error = %e, "notifications failed");
            ExitCode::FAILURE
        }
    }
}
