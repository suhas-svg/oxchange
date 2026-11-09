//! Development order endpoint. Environment: `KAFKA_BROKERS` (required),
//! `LISTEN_ADDR` (default `0.0.0.0:8080`), `RUST_LOG` (default `info`).

use std::sync::Arc;

use order_entry::kafka::KafkaPublisher;

#[tokio::main]
async fn main() -> Result<(), String> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let brokers = std::env::var("KAFKA_BROKERS").map_err(|_| "KAFKA_BROKERS is required")?;
    let addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let publisher = Arc::new(KafkaPublisher::connect(&brokers)?);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| e.to_string())?;
    tracing::info!(%addr, "order-entry listening (development stand-in: no auth, no funds checks)");
    axum::serve(listener, order_entry::router(publisher))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|e| e.to_string())
}
