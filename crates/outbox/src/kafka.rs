//! Kafka publisher for the relay: `acks=all`, idempotent, keyed rows.

use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::{Publisher, Row};

const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);

pub struct KafkaPublisher {
    producer: FutureProducer,
}

impl KafkaPublisher {
    pub fn connect(brokers: &str) -> Result<Self, String> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("linger.ms", "1")
            // Same partitioner as every other writer, so a key always maps to
            // the same partition (orders.commands is keyed by stock symbol).
            .set("partitioner", "murmur2_random")
            .set("message.timeout.ms", "30000")
            .create()
            .map_err(|e| e.to_string())?;
        Ok(Self { producer })
    }
}

impl Publisher for KafkaPublisher {
    async fn publish(&self, rows: &[Row]) -> Result<(), String> {
        // Enqueue the whole batch in order (idempotence keeps that order per
        // partition), then wait for every confirmation.
        let mut pending = Vec::with_capacity(rows.len());
        for r in rows {
            let headers = r.headers.iter().fold(OwnedHeaders::new(), |h, (k, v)| {
                h.insert(Header {
                    key: k,
                    value: Some(v.as_bytes()),
                })
            });
            let record = FutureRecord::to(&r.topic)
                .key(&r.key)
                .payload(&r.payload)
                .headers(headers);
            match self.producer.send_result(record) {
                Ok(f) => pending.push(f),
                Err((e, _)) => return Err(format!("enqueue outbox row {}: {e}", r.id)),
            }
        }
        for (f, r) in pending.into_iter().zip(rows) {
            match tokio::time::timeout(DELIVERY_TIMEOUT, f).await {
                Ok(Ok(Ok(_))) => {}
                Ok(Ok(Err((e, _)))) => return Err(format!("outbox row {}: {e}", r.id)),
                Ok(Err(_)) => return Err(format!("outbox row {}: delivery canceled", r.id)),
                Err(_) => return Err(format!("outbox row {}: no confirmation", r.id)),
            }
        }
        Ok(())
    }
}
