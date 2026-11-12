//! Accounts service.
//!
//! ```text
//! accounts [serve]      gRPC on LISTEN_ADDR, /healthz and /readyz on HEALTH_ADDR
//! accounts migrate      apply database migrations, then exit
//! accounts keygen DIR   write a JWT key pair to DIR unless one is there already
//! accounts health       exit 0 if this host's /readyz answers 200 (container healthcheck)
//! ```
//!
//! Environment: DATABASE_URL, and JWT_PRIVATE_KEY (PEM) or
//! JWT_PRIVATE_KEY_FILE. Required values that are missing stop the process.

use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use accounts::{Accounts, MIGRATOR, TokenSigner, grpc};
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

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

fn signer() -> Result<TokenSigner, BoxError> {
    let pem = match std::env::var("JWT_PRIVATE_KEY") {
        Ok(pem) => pem,
        Err(_) => std::fs::read_to_string(
            env("JWT_PRIVATE_KEY_FILE")
                .map_err(|_| "JWT_PRIVATE_KEY or JWT_PRIVATE_KEY_FILE is required")?,
        )?,
    };
    Ok(TokenSigner::from_pem(&pem)?)
}

fn keygen(dir: &Path) -> Result<(), BoxError> {
    let (private, public) = (dir.join("jwt-private.pem"), dir.join("jwt-public.pem"));
    if private.exists() && public.exists() {
        tracing::info!(dir = %dir.display(), "JWT key pair already present");
        return Ok(());
    }
    let (private_pem, public_pem) = TokenSigner::generate_pem()?;
    std::fs::create_dir_all(dir)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    std::io::Write::write_all(&mut options.open(&private)?, private_pem.as_bytes())?;
    std::fs::write(&public, public_pem)?;
    tracing::info!(dir = %dir.display(), "wrote a new JWT key pair");
    Ok(())
}

/// Container healthcheck: exits 0 if this host's /readyz answers 200. The
/// runtime image has no curl, so this speaks just enough HTTP/1.0.
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

async fn ready(State(pool): State<PgPool>) -> StatusCode {
    match sqlx::query("SELECT 1").execute(&pool).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _ = ctrl_c => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
    tracing::info!("shutting down");
}

async fn serve() -> Result<(), BoxError> {
    let signer = signer()?;
    let pool = connect().await?;
    telemetry::watch_pool(pool.clone());
    let listen = env_or("LISTEN_ADDR", "0.0.0.0:50051").parse()?;
    let health_addr = env_or("HEALTH_ADDR", "0.0.0.0:8081");

    let health = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/readyz", get(ready))
        .with_state(pool.clone());
    let health_listener = tokio::net::TcpListener::bind(&health_addr).await?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(health_listener, health).await {
            tracing::error!(error = %e, "health server stopped");
        }
    });

    // Outbox relay for accounts.events. Built only with the kafka feature
    // (as in the Docker image); without it the messages wait in the outbox.
    #[cfg(feature = "kafka")]
    {
        let publisher = outbox::kafka::KafkaPublisher::connect(&env("KAFKA_BROKERS")?)?;
        tokio::spawn(outbox::run(
            pool.clone(),
            publisher,
            outbox::RelayConfig::new(accounts::OUTBOX_LOCK_KEY),
            std::future::pending(),
        ));
    }
    #[cfg(not(feature = "kafka"))]
    tracing::warn!("built without the kafka feature: accounts.events stay in the outbox");

    let accounts = Arc::new(Accounts::new(pool, signer));
    tracing::info!(%listen, health = %health_addr, "accounts serving");
    tonic::transport::Server::builder()
        .layer(telemetry::server::ServerLayer::grpc())
        .add_service(grpc::Service::server(accounts))
        .serve_with_shutdown(listen, shutdown_signal())
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let _telemetry = telemetry::init_with(
        "accounts",
        telemetry::Mode::for_command(args.get(1).map(String::as_str)),
    );
    let result = match args.get(1).map(String::as_str) {
        None | Some("serve") => serve().await,
        Some("migrate") => {
            async {
                let pool = connect().await?;
                MIGRATOR.run(&pool).await?;
                tracing::info!("migrations applied");
                Ok(())
            }
            .await
        }
        Some("keygen") => match args.get(2) {
            Some(dir) => keygen(Path::new(dir)),
            None => Err("usage: accounts keygen DIR".into()),
        },
        Some("health") => health_probe(),
        Some(other) => Err(format!("unknown command {other:?}").into()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "accounts failed");
            ExitCode::FAILURE
        }
    }
}
