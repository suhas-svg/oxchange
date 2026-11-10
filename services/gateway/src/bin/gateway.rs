//! API Gateway service.
//!
//! ```text
//! gateway [serve]   the public API on HTTP_ADDR
//! gateway health    exit 0 if this host's /readyz answers 200 (container healthcheck)
//! ```
//!
//! Environment: ACCOUNTS_GRPC_URL, RISK_GRPC_URL, MARKET_DATA_URL,
//! NOTIFICATIONS_URL, REDIS_URL, and JWT_PUBLIC_KEY (PEM) or
//! JWT_PUBLIC_KEY_FILE. Optional: HTTP_ADDR (0.0.0.0:8080) and
//! TRUST_FORWARDED_FOR (`true` only behind a proxy that sets
//! X-Forwarded-For).

use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use gateway::{Config, Gateway, router};
use tracing_subscriber::EnvFilter;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn env(name: &str) -> Result<String, BoxError> {
    std::env::var(name).map_err(|_| format!("{name} is required").into())
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn public_key() -> Result<String, BoxError> {
    if let Ok(pem) = std::env::var("JWT_PUBLIC_KEY") {
        return Ok(pem);
    }
    let path = std::env::var("JWT_PUBLIC_KEY_FILE")
        .map_err(|_| "JWT_PUBLIC_KEY or JWT_PUBLIC_KEY_FILE is required")?;
    Ok(std::fs::read_to_string(&path).map_err(|e| format!("read {path}: {e}"))?)
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
    let config = Config {
        accounts_url: env("ACCOUNTS_GRPC_URL")?,
        risk_url: env("RISK_GRPC_URL")?,
        market_data_url: env("MARKET_DATA_URL")?,
        notifications_url: env("NOTIFICATIONS_URL")?,
        redis_url: env("REDIS_URL")?,
        jwt_public_key_pem: public_key()?,
        trust_forwarded_for: env_or("TRUST_FORWARDED_FOR", "false") == "true",
    };
    let gw = Arc::new(Gateway::new(&config)?);
    let addr = env_or("HTTP_ADDR", "0.0.0.0:8080");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(http = %addr, trust_forwarded_for = config.trust_forwarded_for, "gateway running");

    let sockets = gw.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let open = sockets.sockets.total();
            if open > 0 {
                tracing::info!(websocket_clients = open, "proxied streams");
            }
        }
    });

    // Spec: finish current work and exit within 25 seconds. Open sockets
    // are cut at the deadline; clients reconnect to another copy.
    // The 20 s drain deadline starts at the signal, not at startup.
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let server = axum::serve(
        listener,
        router(gw).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        let _ = stopped.changed().await;
    })
    .into_future();
    let mut server = std::pin::pin!(server);
    tokio::select! {
        r = &mut server => {
            if let Err(e) = r {
                tracing::error!(error = %e, "server failed");
            }
            return Ok(());
        }
        () = signal() => {}
    }
    tracing::info!("shutting down");
    let _ = stop.send(true);
    if tokio::time::timeout(Duration::from_secs(20), server)
        .await
        .is_err()
    {
        tracing::warn!("drain deadline passed; closing the remaining connections");
    }
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
        Some("health") => health_probe(),
        Some(other) => Err(format!("unknown command {other:?}").into()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "gateway failed");
            ExitCode::FAILURE
        }
    }
}
