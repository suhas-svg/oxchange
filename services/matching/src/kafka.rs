//! Kafka (Redpanda) adapters for the pipeline. Built only with `--features kafka`.
//!
//! - [`KafkaSource`] reads one partition of `orders.commands` from an exact
//!   offset. It does not use consumer-group rebalancing, which could briefly
//!   hand one partition to two engines; the partition comes from config.
//! - [`KafkaSink`] writes `matching.events` to the *same partition number*
//!   (partition mirroring) with `acks=all` and `enable.idempotence=true`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header as KHeader, Headers, OwnedHeaders};
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::{ClientContext, Message, Offset, TopicPartitionList};

use crate::pipeline::{
    CommandSource, EventSink, Fetched, Header, InputRecord, OutputRecord, OutputTopic,
    TransportError,
};

pub const COMMANDS_TOPIC: &str = "orders.commands";
pub const EVENTS_TOPIC: &str = "matching.events";
pub const COMMANDS_DLQ_TOPIC: &str = "orders.commands.dlq";

const POLL: Duration = Duration::from_millis(100);
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);
const FLUSH_TIMEOUT: Duration = Duration::from_secs(60);

fn err(e: impl std::fmt::Display) -> TransportError {
    TransportError(e.to_string())
}

// -------------------------------------------------------------------- source

pub struct KafkaSource {
    consumer: BaseConsumer<InputLag>,
    /// The partition's end when we started; reaching it means replay is done.
    replay_until: u64,
    replay_started: Instant,
    replay_done: bool,
    /// Shared with the health endpoint: true once replay is done.
    ready: Arc<AtomicBool>,
}

impl KafkaSource {
    /// Connects and positions the reader at `next_offset`, after checking the
    /// log still holds it. If retention already deleted it, replaying would
    /// silently skip commands, so this refuses to start instead.
    ///
    /// `process_started` is when the process began, so the "ready" log line
    /// reports the full restart-to-ready time the spec targets (under 10 s).
    /// `ready` is set once every command that was in the log at startup has
    /// been read (spec: "A matcher reports ready only after replay finishes").
    pub fn connect(
        brokers: &str,
        partition: u32,
        next_offset: u64,
        process_started: Instant,
        ready: Arc<AtomicBool>,
    ) -> Result<Self, TransportError> {
        let consumer: BaseConsumer<InputLag> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            // Required by the client; nothing is ever committed under it.
            .set("group.id", format!("matching-p{partition}"))
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("enable.partition.eof", "false")
            .set("statistics.interval.ms", "5000")
            .create_with_context(InputLag)
            .map_err(err)?;

        let (low, high) = consumer
            .fetch_watermarks(COMMANDS_TOPIC, partition as i32, METADATA_TIMEOUT)
            .map_err(err)?;
        let (low, high) = (low.max(0) as u64, high.max(0) as u64);
        if next_offset < low {
            return Err(TransportError(format!(
                "partition {partition} starts at offset {low}, but the engine needs {next_offset}: \
                 commands were deleted by retention before a snapshot covered them"
            )));
        }
        if next_offset > high {
            return Err(TransportError(format!(
                "snapshot says next offset {next_offset}, but partition {partition} ends at {high}: \
                 the snapshot does not belong to this log"
            )));
        }

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(
            COMMANDS_TOPIC,
            partition as i32,
            Offset::Offset(next_offset as i64),
        )
        .map_err(err)?;
        consumer.assign(&tpl).map_err(err)?;
        tracing::info!(
            partition,
            next_offset,
            log_end = high,
            replay = high - next_offset,
            "reading orders.commands"
        );
        metrics::gauge!("matcher_ready").set(0.0);
        Ok(Self {
            consumer,
            replay_until: high,
            replay_started: process_started,
            replay_done: next_offset >= high,
            ready,
        })
        .inspect(|s| {
            if s.replay_done {
                s.mark_ready();
                tracing::info!(
                    recovery_ms = process_started.elapsed().as_millis() as u64,
                    "nothing to replay; matcher is ready"
                );
            }
        })
    }
}

/// Sets `matcher_input_lag` from librdkafka's statistics (every 5 s): the
/// partition's log end minus the next command to hand to the engine.
pub struct InputLag;

impl ClientContext for InputLag {
    fn stats(&self, s: rdkafka::Statistics) {
        let Some(t) = s.topics.get(COMMANDS_TOPIC) else {
            return;
        };
        for (partition, p) in &t.partitions {
            if *partition >= 0 && p.hi_offset >= 0 && p.app_offset >= 0 {
                metrics::gauge!("matcher_input_lag")
                    .set((p.hi_offset - p.app_offset).max(0) as f64);
            }
        }
    }
}

impl rdkafka::consumer::ConsumerContext for InputLag {}

impl KafkaSource {
    /// Replay is over: ready for traffic, and the restart time is known.
    fn mark_ready(&self) {
        self.ready.store(true, Ordering::Relaxed);
        metrics::gauge!("matcher_ready").set(1.0);
        metrics::histogram!("matcher_recovery_seconds")
            .record(self.replay_started.elapsed().as_secs_f64());
    }
}

impl CommandSource for KafkaSource {
    fn fetch(&mut self) -> Result<Fetched, TransportError> {
        let Some(result) = self.consumer.poll(POLL) else {
            // Nothing arrived for a whole poll: caught up.
            metrics::gauge!("matcher_input_lag").set(0.0);
            return Ok(Fetched::Idle);
        };
        let msg = result.map_err(err)?;
        let offset = msg.offset() as u64;
        if !self.replay_done && offset + 1 >= self.replay_until {
            self.replay_done = true;
            self.mark_ready();
            tracing::info!(
                recovery_ms = self.replay_started.elapsed().as_millis() as u64,
                "replay complete; matcher is ready"
            );
        }
        let headers = msg
            .headers()
            .map(|hs| {
                hs.iter()
                    .map(|h| (h.key.to_owned(), h.value.unwrap_or_default().to_vec()))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Fetched::Record(InputRecord {
            offset,
            payload: msg.payload().unwrap_or_default().to_vec(),
            headers,
        }))
    }
}

// ---------------------------------------------------------------------- sink

/// Remembers the first delivery failure. With idempotence on, the client
/// retries transient errors itself, so anything reported here is permanent.
#[derive(Default, Clone)]
pub struct DeliveryTracker {
    first_error: Arc<Mutex<Option<String>>>,
}

impl ClientContext for DeliveryTracker {}

impl ProducerContext for DeliveryTracker {
    type DeliveryOpaque = ();
    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        if let Err((e, _)) = result {
            let mut slot = self.first_error.lock().unwrap();
            if slot.is_none() {
                *slot = Some(e.to_string());
            }
        }
    }
}

pub struct KafkaSink {
    producer: ThreadedProducer<DeliveryTracker>,
    tracker: DeliveryTracker,
    partition: i32,
}

impl KafkaSink {
    pub fn connect(brokers: &str, partition: u32) -> Result<Self, TransportError> {
        let tracker = DeliveryTracker::default();
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("linger.ms", "1")
            .create_with_context(tracker.clone())
            .map_err(err)?;
        Ok(Self {
            producer,
            tracker,
            partition: partition as i32,
        })
    }

    fn check(&self) -> Result<(), TransportError> {
        match self.tracker.first_error.lock().unwrap().clone() {
            Some(e) => Err(TransportError(format!("delivery failed: {e}"))),
            None => Ok(()),
        }
    }
}

fn kafka_headers(headers: &[Header]) -> OwnedHeaders {
    headers.iter().fold(OwnedHeaders::new(), |hs, (k, v)| {
        hs.insert(KHeader {
            key: k,
            value: Some(v.as_slice()),
        })
    })
}

impl EventSink for KafkaSink {
    fn send(&mut self, r: OutputRecord) -> Result<(), TransportError> {
        self.check()?;
        let (topic, partition) = match r.topic {
            OutputTopic::Events => (EVENTS_TOPIC, self.partition),
            OutputTopic::DeadLetter => (COMMANDS_DLQ_TOPIC, 0),
        };
        let headers = kafka_headers(&r.headers);
        let mut record = BaseRecord::to(topic)
            .partition(partition)
            .key(&r.key)
            .payload(&r.payload)
            .headers(headers);
        loop {
            match self.producer.send(record) {
                Ok(()) => return Ok(()),
                // The client's local queue is full: wait for deliveries to drain.
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), back)) => {
                    record = back;
                    thread::sleep(Duration::from_millis(1));
                }
                Err((e, _)) => return Err(err(e)),
            }
        }
    }

    fn flush(&mut self) -> Result<(), TransportError> {
        self.producer.flush(FLUSH_TIMEOUT).map_err(err)?;
        self.check()
    }
}
