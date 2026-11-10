//! The DLQ writer (spec: "Bad messages": Notifications sends a message it
//! cannot process to the DLQ and continues).

use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};

const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);

pub fn dlq_topic(source: &str) -> String {
    format!("{source}.dlq")
}

pub struct Dlq {
    producer: FutureProducer,
}

impl Dlq {
    pub fn connect(brokers: &str) -> Result<Self, String> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", "30000")
            .create()
            .map_err(|e| e.to_string())?;
        Ok(Self { producer })
    }

    /// Copies one message to `{topic}.dlq` with the same key, and returns
    /// once Kafka confirms it.
    pub async fn send(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        key: Option<&[u8]>,
        payload: &[u8],
        reason: &str,
    ) -> Result<(), String> {
        let dlq = dlq_topic(topic);
        let (partition, offset) = (partition.to_string(), offset.to_string());
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "dlq-reason",
                value: Some(reason.as_bytes()),
            })
            .insert(Header {
                key: "dlq-consumer",
                value: Some(crate::GROUP.as_bytes()),
            })
            .insert(Header {
                key: "dlq-source-partition",
                value: Some(partition.as_bytes()),
            })
            .insert(Header {
                key: "dlq-source-offset",
                value: Some(offset.as_bytes()),
            });
        let mut record = FutureRecord::<[u8], [u8]>::to(&dlq)
            .payload(payload)
            .headers(headers);
        if let Some(k) = key {
            record = record.key(k);
        }
        match self.producer.send(record, DELIVERY_TIMEOUT).await {
            Ok(_) => Ok(()),
            Err((e, _)) => Err(format!("dlq write to {dlq}: {e}")),
        }
    }
}
