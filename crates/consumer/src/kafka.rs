//! A consume loop: one worker per topic-partition, each handling its
//! messages in order, offsets moved forward only after the handler
//! succeeds, retry with backoff, and halt one partition on failures that
//! retrying cannot fix.
//!
//! Partitions run side by side. Kafka orders messages only within a
//! partition, and every stream here is partitioned by stock, so handlers for
//! different partitions never touch the same rows. It also lets PostgreSQL
//! flush several commits at once: one message at a time across all
//! partitions capped a consumer at one commit per disk flush.
//!
//! [`run_batches`] hands a worker's waiting messages to the handler
//! together, so one database transaction applies many events (spec,
//! "Where the time goes", step 2). Offsets are stored after each success
//! and committed to the broker on a timer, not one request per message.
//!
//! Each handler call runs inside a span that continues the first message's
//! trace (its `traceparent` header) and links the others'. Metrics:
//! `consumer_lag{topic,partition}` (messages between the log end and this
//! consumer's position, from the client's statistics every 5 s),
//! `consumer_halted{topic,partition}` (1 while a partition is stopped),
//! `consumer_retries_total{topic}` and `consumer_batch_size{topic}`.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::message::OwnedMessage;
use rdkafka::{ClientConfig, ClientContext, Message, Offset, Statistics, TopicPartitionList};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tracing::Instrument;
use tracing::field::Empty;

use crate::HandlerError;

/// Longest wait between retries of one message (spec: up to 30 s).
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Messages read ahead for one partition: the most one batch can hold. The
/// lag gauge counts from the last message read, so it can read up to this
/// many low.
const PARTITION_QUEUE: usize = 256;
/// How often stored offsets go to the broker. A crash loses at most this
/// much progress, which comes back as repeats the seq check drops.
const COMMIT_INTERVAL_MS: &str = "1000";

/// Most messages one handler call gets when `CONSUMER_MAX_BATCH` is unset.
pub const DEFAULT_MAX_BATCH: usize = 100;

/// The batch size limit: `CONSUMER_MAX_BATCH`, or [`DEFAULT_MAX_BATCH`].
/// 1 turns batching off.
pub fn max_batch_from_env() -> usize {
    std::env::var("CONSUMER_MAX_BATCH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_BATCH)
}

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
        // The client commits only what the workers stored, never just
        // because a message was read.
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("auto.commit.interval.ms", COMMIT_INTERVAL_MS)
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .set("statistics.interval.ms", "5000")
        .create_with_context(LagStats)
        .map_err(|e| e.to_string())?;
    c.subscribe(topics).map_err(|e| e.to_string())?;
    Ok(c)
}

/// Feeds every message to `handle`, one at a time, until `shutdown` resolves.
///
/// Consumer-group rebalancing is fine here (unlike the matcher): a message
/// handled by two copies is a repeat that the seq check drops.
pub async fn run<F, Fut>(consumer: KafkaConsumer, mut handle: F, shutdown: impl Future<Output = ()>)
where
    F: FnMut(Record<'_>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
{
    run_batches(
        consumer,
        1,
        move |mut batch: Vec<Record<'_>>| {
            let one = batch.pop().expect("batches are never empty");
            handle(one)
        },
        shutdown,
    )
    .await;
}

/// Feeds messages to `handle` in batches of up to `max_batch`: the messages
/// of one partition already waiting, in order. The handler must apply a
/// whole batch or none of it (one database transaction). When a batch fails
/// for any reason, its messages are handled again one at a time, so
/// retries, halts and the seq checks work exactly as with [`run`]:
/// everything before a bad message is applied, nothing after it.
pub async fn run_batches<F, Fut>(
    consumer: KafkaConsumer,
    max_batch: usize,
    handle: F,
    shutdown: impl Future<Output = ()>,
) where
    F: FnMut(Vec<Record<'_>>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
{
    tokio::pin!(shutdown);
    // Exists at 0 from the start, so "any value above 0" can alert.
    metrics::counter!("consumer_seq_gaps_total").increment(0);
    let consumer = Arc::new(consumer);
    let max_batch = max_batch.clamp(1, PARTITION_QUEUE);
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut queues: HashMap<(String, i32), mpsc::Sender<OwnedMessage>> = HashMap::new();
    let mut workers = JoinSet::new();
    'read: loop {
        let msg = tokio::select! {
            () = &mut shutdown => break 'read,
            m = consumer.recv() => m.map(|m| m.detach()),
        };
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "kafka receive failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let queue = queues
            .entry((msg.topic().to_owned(), msg.partition()))
            .or_insert_with(|| {
                let (tx, rx) = mpsc::channel(PARTITION_QUEUE);
                workers.spawn(work(
                    consumer.clone(),
                    handle.clone(),
                    max_batch,
                    rx,
                    stop_rx.clone(),
                ));
                tx
            });
        tokio::select! {
            () = &mut shutdown => break 'read,
            // An error means the worker stopped because its partition
            // halted: later messages of that partition are not handled.
            _ = queue.send(msg) => {}
        }
    }
    let _ = stop_tx.send(true);
    drop(queues);
    // Each worker finishes the batch in hand, then stops.
    while workers.join_next().await.is_some() {}
    // Send the last stored offsets now rather than at the next tick.
    if let Err(e) = consumer.commit_consumer_state(CommitMode::Sync) {
        tracing::debug!(error = %e, "final offset commit");
    }
}

/// A message read off the queue, with its headers copied out once.
struct Msg {
    msg: OwnedMessage,
    headers: Vec<(String, Vec<u8>)>,
}

impl Msg {
    fn new(msg: OwnedMessage) -> Self {
        let headers = msg
            .headers()
            .map(|hs| {
                rdkafka::message::Headers::iter(hs)
                    .map(|h| (h.key.to_owned(), h.value.unwrap_or_default().to_vec()))
                    .collect()
            })
            .unwrap_or_default();
        Self { msg, headers }
    }

    fn record(&self) -> Record<'_> {
        Record {
            topic: self.msg.topic(),
            partition: self.msg.partition(),
            offset: self.msg.offset(),
            key: self.msg.key(),
            payload: self.msg.payload().unwrap_or_default(),
            headers: self.headers.clone(),
        }
    }

    fn traceparent(&self) -> Option<&str> {
        telemetry::traceparent_header(&self.headers)
    }
}

/// The span one handler call runs in: it continues the first message's
/// trace and links the traces of the others.
fn consume_span(batch: &[Msg]) -> tracing::Span {
    let first = &batch[0].msg;
    let span = tracing::info_span!(
        "consume",
        otel.name = %format!("consume {}", first.topic()),
        otel.kind = "consumer",
        otel.status_code = Empty,
        trace_id = Empty,
        partition = first.partition(),
        offset = first.offset(),
        batch = batch.len(),
    );
    telemetry::follow(&span, batch[0].traceparent());
    for m in &batch[1..] {
        telemetry::link(&span, m.traceparent());
    }
    span
}

/// Marks everything up to `msg` as done. The client commits stored offsets
/// every [`COMMIT_INTERVAL_MS`], on a rebalance and on close.
fn store(consumer: &KafkaConsumer, msg: &OwnedMessage) {
    let mut tpl = TopicPartitionList::new();
    let stored = tpl
        .add_partition_offset(
            msg.topic(),
            msg.partition(),
            Offset::Offset(msg.offset() + 1),
        )
        .map_err(|e| e.to_string())
        .and_then(|()| consumer.store_offsets(&tpl).map_err(|e| e.to_string()));
    if let Err(e) = stored {
        // Usually the partition moved to another copy in a rebalance.
        tracing::warn!(error = %e, "offset not stored; the message may repeat");
    }
}

/// Handles one partition's messages in order until told to stop, or until
/// the partition halts.
async fn work<F, Fut>(
    consumer: Arc<KafkaConsumer>,
    mut handle: F,
    max_batch: usize,
    mut queue: mpsc::Receiver<OwnedMessage>,
    mut stop: watch::Receiver<bool>,
) where
    F: FnMut(Vec<Record<'_>>) -> Fut,
    Fut: Future<Output = Result<(), HandlerError>>,
{
    while let Some(first) = queue.recv().await {
        if *stop.borrow() {
            return;
        }
        let mut batch = vec![Msg::new(first)];
        while batch.len() < max_batch {
            match queue.try_recv() {
                Ok(m) => batch.push(Msg::new(m)),
                Err(_) => break,
            }
        }
        if batch.len() > 1 {
            let span = consume_span(&batch);
            let records = batch.iter().map(Msg::record).collect();
            match handle(records).instrument(span.clone()).await {
                Ok(()) => {
                    let last = &batch[batch.len() - 1].msg;
                    metrics::histogram!("consumer_batch_size", "topic" => last.topic().to_owned())
                        .record(batch.len() as f64);
                    store(&consumer, last);
                    continue;
                }
                Err(e) => {
                    span.record("otel.status_code", "error");
                    tracing::debug!(error = ?e, size = batch.len(),
                        "batch failed; handling its messages one at a time");
                }
            }
        }
        for m in &batch {
            if !handle_one(&consumer, &mut handle, m, &mut stop).await {
                return;
            }
        }
    }
}

/// Handles one message until it succeeds or halts. Returns false when the
/// worker must stop: the partition halted, or the consumer is stopping.
async fn handle_one<F, Fut>(
    consumer: &KafkaConsumer,
    handle: &mut F,
    m: &Msg,
    stop: &mut watch::Receiver<bool>,
) -> bool
where
    F: FnMut(Vec<Record<'_>>) -> Fut,
    Fut: Future<Output = Result<(), HandlerError>>,
{
    let msg = &m.msg;
    let mut backoff = Duration::from_millis(100);
    loop {
        let span = consume_span(std::slice::from_ref(m));
        let outcome = handle(vec![m.record()]).instrument(span.clone()).await;
        if outcome.is_err() {
            span.record("otel.status_code", "error");
        }
        match outcome {
            Ok(()) => {
                metrics::histogram!("consumer_batch_size", "topic" => msg.topic().to_owned())
                    .record(1.0);
                // The effect is saved; only now move the offset forward.
                store(consumer, msg);
                return true;
            }
            Err(HandlerError::Retry(e)) => {
                metrics::counter!("consumer_retries_total", "topic" => msg.topic().to_owned())
                    .increment(1);
                tracing::warn!(error = %e, partition = msg.partition(), offset = msg.offset(),
                    retry_in_ms = backoff.as_millis() as u64, "handler failed; retrying");
                tokio::select! {
                    _ = stop.changed() => return false,
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
                // Nothing after the halted message may be applied.
                return false;
            }
        }
    }
}
