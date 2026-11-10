//! The live view's Kafka reader: every partition of `matching.events`, from
//! the end, with no consumer group commits. Whatever came before is already
//! in the books that `GetBook` returns.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use proto_gen::events::v1 as ev;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

use crate::live::{BookSource, Live};

/// Feeds every new event to `live` until `shutdown` resolves.
pub async fn follow<B: BookSource>(
    brokers: &str,
    topic: &str,
    live: Arc<Live<B>>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    let c: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        // Never commits; the name only labels this copy's connection.
        .set(
            "group.id",
            format!("market-data-live-{}", uuid::Uuid::now_v7()),
        )
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .create()
        .map_err(|e| e.to_string())?;
    let meta = c
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|e| e.to_string())?;
    let partitions = meta
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .map(|t| t.partitions().len())
        .filter(|n| *n > 0)
        .ok_or_else(|| format!("topic {topic} not found"))?;
    let mut tpl = TopicPartitionList::new();
    for p in 0..partitions {
        let p = i32::try_from(p).map_err(|e| e.to_string())?;
        tpl.add_partition_offset(topic, p, Offset::End)
            .map_err(|e| e.to_string())?;
    }
    c.assign(&tpl).map_err(|e| e.to_string())?;
    tracing::info!(partitions, "following matching.events");

    tokio::pin!(shutdown);
    loop {
        let msg = tokio::select! {
            () = &mut shutdown => return Ok(()),
            m = c.recv() => m,
        };
        match msg {
            Ok(m) => match ev::MatchingEvent::decode(m.payload().unwrap_or_default()) {
                Ok(e) => live.on_event(m.partition(), &e).await,
                Err(e) => tracing::warn!(error = %e, "undecodable event skipped by the live view"),
            },
            Err(e) => {
                tracing::warn!(error = %e, "kafka receive failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
