//! A consume loop: one message at a time, offset committed only after the
//! handler succeeds, retry with backoff, and halt one partition on failures
//! that retrying cannot fix.
//!
//! Each message is handled inside a span that continues the producer's
//! trace (its `traceparent` header). Metrics: `consumer_lag{topic,partition}`
//! (messages between the log end and this consumer's position, from the
//! client's statistics every 5 s),
//! `consumer_halted{topic,partition}` (1 while a partition is stopped) and
//! `consumer_retries_total{topic}`.

use std::future::Future;
use std::time::Duration;

use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, ClientContext, Message, Offset, Statistics, TopicPartitionList};
use tracing::Instrument;
use tracing::field::Empty;

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

/// A consumer that reports its lag from the client's own statistics.
pub type KafkaConsumer = StreamConsumer<LagStats>;

/// Sets `consumer_lag{topic,partition}` from librdkafka's statistics: the
/// partition's log end minus the position handed to the application. No
/// extra requests; the client already tracks both.
pub struct LagStats;

impl ClientContext for LagStats {
    fn stats(&self, s: Statistics) {
        for (topic, t) in &s.topics {
            for (partition, p) in &t.partitions {
                // -1 is librdkafka's internal "unassigned" partition.
                if *partition < 0 || p.hi_offset < 0 || p.app_offset < 0 {
                    continue;
                }
                metrics::gauge!("consumer_lag",
                    "topic" => topic.clone(), "partition" => partition.to_string())
                .set((p.hi_offset - p.app_offset).max(0) as f64);
            }
        }
    }
}

impl ConsumerContext for LagStats {}

pub fn consumer(brokers: &str, group: &str, topic: &str) -> Result<KafkaConsumer, String> {
    consumer_for(brokers, group, &[topic])
}

/// One consumer reading several topics, each message tagged with its topic.
pub fn consumer_for(brokers: &str, group: &str, topics: &[&str]) -> Result<KafkaConsumer, String> {
    let c: KafkaConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .set("statistics.interval.ms", "5000")
        .create_with_context(LagStats)
        .map_err(|e| e.to_string())?;
    c.subscribe(topics).map_err(|e| e.to_string())?;
    Ok(c)
}

/// Feeds every message to `handle` until `shutdown` resolves.
///
/// Consumer-group rebalancing is fine here (unlike the matcher): a message
/// handled by two copies is a repeat that the seq check drops.
pub async fn run<F, Fut>(consumer: KafkaConsumer, mut handle: F, shutdown: impl Future<Output = ()>)
where
    F: FnMut(Record<'_>) -> Fut,
    Fut: Future<Output = Result<(), HandlerError>>,
{
    tokio::pin!(shutdown);
    // Exists at 0 from the start, so "any value above 0" can alert.
    metrics::counter!("consumer_seq_gaps_total").increment(0);
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
            let span = tracing::info_span!(
                "consume",
                otel.name = %format!("consume {}", msg.topic()),
                otel.kind = "consumer",
                otel.status_code = Empty,
                trace_id = Empty,
                partition = msg.partition(),
                offset = msg.offset(),
            );
            telemetry::follow(&span, telemetry::traceparent_header(&record.headers));
            let outcome = handle(record).instrument(span.clone()).await;
            if outcome.is_err() {
                span.record("otel.status_code", "error");
            }
            match outcome {
                Ok(()) => {
                    // The effect is saved; only now move the offset forward.
                    if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                        tracing::warn!(error = %e, "offset commit failed; the message may repeat");
                    }
                    break;
                }
                Err(HandlerError::Retry(e)) => {
                    metrics::counter!("consumer_retries_total", "topic" => msg.topic().to_owned())
                        .increment(1);
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
                    metrics::gauge!("consumer_halted",
                        "topic" => msg.topic().to_owned(),
                        "partition" => msg.partition().to_string())
                    .set(1.0);
                    break;
                }
            }
        }
    }
}
