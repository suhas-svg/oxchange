//! A consume loop: one message at a time, offset committed only after the
//! handler succeeds, retry with backoff, and halt one partition on failures
//! that retrying cannot fix.

use std::future::Future;
use std::time::Duration;

use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

use crate::HandlerError;

/// Longest wait between retries of one message (spec: up to 30 s).
const MAX_BACKOFF: Duration = Duration::from_secs(30);

pub struct Record<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<&'a [u8]>,
    pub payload: &'a [u8],
    pub headers: Vec<(String, Vec<u8>)>,
}

pub fn consumer(brokers: &str, group: &str, topic: &str) -> Result<StreamConsumer, String> {
    consumer_for(brokers, group, &[topic])
}

/// One consumer reading several topics, each message tagged with its topic.
pub fn consumer_for(brokers: &str, group: &str, topics: &[&str]) -> Result<StreamConsumer, String> {
    let c: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .create()
        .map_err(|e| e.to_string())?;
    c.subscribe(topics).map_err(|e| e.to_string())?;
    Ok(c)
}

/// Feeds every message to `handle` until `shutdown` resolves.
///
/// Consumer-group rebalancing is fine here (unlike the matcher): a message
/// handled by two copies is a repeat that the seq check drops.
pub async fn run<F, Fut>(
    consumer: StreamConsumer,
    mut handle: F,
    shutdown: impl Future<Output = ()>,
) where
    F: FnMut(Record<'_>) -> Fut,
    Fut: Future<Output = Result<(), HandlerError>>,
{
    tokio::pin!(shutdown);
    loop {
        let msg = tokio::select! {
            () = &mut shutdown => return,
            m = consumer.recv() => m,
        };
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "kafka receive failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let mut backoff = Duration::from_millis(100);
        loop {
            let record = Record {
                topic: msg.topic(),
                partition: msg.partition(),
                offset: msg.offset(),
                key: msg.key(),
                payload: msg.payload().unwrap_or_default(),
                headers: msg
                    .headers()
                    .map(|hs| {
                        rdkafka::message::Headers::iter(hs)
                            .map(|h| (h.key.to_owned(), h.value.unwrap_or_default().to_vec()))
                            .collect()
                    })
                    .unwrap_or_default(),
            };
            match handle(record).await {
                Ok(()) => {
                    // The effect is saved; only now move the offset forward.
                    if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                        tracing::warn!(error = %e, "offset commit failed; the message may repeat");
                    }
                    break;
                }
                Err(HandlerError::Retry(e)) => {
                    tracing::warn!(error = %e, partition = msg.partition(), offset = msg.offset(),
                        retry_in_ms = backoff.as_millis() as u64, "handler failed; retrying");
                    tokio::select! {
                        () = &mut shutdown => return,
                        () = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
                Err(HandlerError::Halt(e)) => {
                    tracing::error!(error = %e, topic = msg.topic(), partition = msg.partition(),
                        offset = msg.offset(), "partition halted; needs investigation");
                    let mut tpl = TopicPartitionList::new();
                    let _ = tpl.add_partition_offset(
                        msg.topic(),
                        msg.partition(),
                        Offset::Offset(msg.offset()),
                    );
                    if let Err(e) = consumer.pause(&tpl) {
                        tracing::error!(error = %e, "could not pause the partition");
                    }
                    break;
                }
            }
        }
    }
}
