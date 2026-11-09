//! The matcher's thread layout (spec: "Matching Engine"):
//!
//! ```text
//! reader thread → bounded queue → matcher thread → bounded queue → writer thread
//!                                                                     ↓ (after flush)
//!                                                               snapshot thread
//! ```
//!
//! The pipeline is written against [`CommandSource`], [`EventSink`] and
//! [`SnapshotPersist`], so its ordering and safety rules are tested without
//! Kafka. The Kafka adapters live in the `kafka` module (feature `kafka`).
//!
//! **Snapshot safety.** A snapshot at offset X claims every command before X
//! is fully handled, events included. If the file were saved before Kafka
//! confirmed those events, a crash would lose them for good, because recovery
//! resumes at X. So the matcher sends a checkpoint *through the writer's
//! queue*, behind every event it has produced; the writer flushes the sink
//! (all earlier records acknowledged) and only then hands the state to the
//! snapshot thread. Capture happens on the matcher thread; encoding and
//! writing happen on the snapshot thread, off the matching path.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread;
use std::time::Instant;

use domain::Symbol;
use order_book::Command;
use thiserror::Error;

use crate::codec;
use crate::engine::{Engine, EngineError, EngineState, ProcessOutcome};
use crate::snapshot::SnapshotStore;

/// A Kafka header: trace context and DLQ details travel here.
pub type Header = (String, Vec<u8>);

/// One record read from `orders.commands`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputRecord {
    pub offset: u64,
    pub payload: Vec<u8>,
    pub headers: Vec<Header>,
}

#[derive(Debug)]
pub enum Fetched {
    Record(InputRecord),
    /// Nothing available right now; the reader checks for shutdown and asks again.
    Idle,
    /// The source has ended (used by tests and bounded replays).
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputTopic {
    /// `matching.events`, same partition number as the input.
    Events,
    /// The dead-letter queue for commands that could not be decoded.
    DeadLetter,
}

/// One record to publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputRecord {
    pub topic: OutputTopic,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub headers: Vec<Header>,
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct TransportError(pub String);

pub trait CommandSource: Send + 'static {
    /// Returns the next record, waiting briefly if none is ready.
    fn fetch(&mut self) -> Result<Fetched, TransportError>;
}

pub trait EventSink: Send + 'static {
    /// Queues a record for publishing, in order.
    fn send(&mut self, record: OutputRecord) -> Result<(), TransportError>;
    /// Returns once every record sent so far is durably acknowledged
    /// (Kafka `acks=all`), or fails if any of them cannot be.
    fn flush(&mut self) -> Result<(), TransportError>;
}

pub trait SnapshotPersist: Send + 'static {
    fn persist(&mut self, state: &EngineState) -> io::Result<()>;
}

impl SnapshotPersist for SnapshotStore {
    fn persist(&mut self, state: &EngineState) -> io::Result<()> {
        self.save(state).map(|_| ())
    }
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Commands between snapshots (spec: 100,000).
    pub snapshot_interval: u64,
    /// Capacity of each bounded queue. When full, the stage before it waits,
    /// so memory stays bounded instead of growing.
    pub queue_capacity: usize,
    /// Take a final snapshot on clean shutdown, so the next start replays nothing.
    pub snapshot_on_shutdown: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            snapshot_interval: crate::snapshot::DEFAULT_SNAPSHOT_INTERVAL,
            queue_capacity: 8_192,
            snapshot_on_shutdown: true,
        }
    }
}

/// What a finished run did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PipelineReport {
    pub commands_applied: u64,
    pub already_applied: u64,
    pub duplicate_commands: u64,
    pub duplicate_order_ids: u64,
    pub dead_lettered: u64,
    pub events_published: u64,
    pub snapshots_saved: u64,
    pub snapshot_failures: u64,
    /// The engine's next offset when the run ended.
    pub next_offset: u64,
}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("reading commands: {0}")]
    Source(TransportError),
    #[error("publishing events: {0}")]
    Sink(TransportError),
    /// The partition must stop: continuing would publish from a corrupt book.
    #[error("engine halted: {0}")]
    Halted(EngineError),
    #[error("a pipeline thread panicked: {0}")]
    Panicked(&'static str),
}

/// Decoded input, passed from the reader to the matcher.
enum Input {
    Command {
        offset: u64,
        symbol: Symbol,
        cmd: Command,
        headers: Vec<Header>,
    },
    Undecodable {
        offset: u64,
        payload: Vec<u8>,
        headers: Vec<Header>,
        reason: String,
    },
}

/// Work for the writer, in the order the matcher produced it.
enum Output {
    Record(OutputRecord),
    /// Everything before this has been sent; persist `state` once it is flushed.
    Checkpoint(Box<EngineState>),
}

/// Runs the matcher until `stop` is set or the source ends, then drains every
/// queue, flushes the sink and returns. Any stage failing stops the others.
pub fn run<S, K, P>(
    engine: Engine,
    mut source: S,
    mut sink: K,
    mut persist: P,
    config: PipelineConfig,
    stop: Arc<AtomicBool>,
) -> Result<PipelineReport, PipelineError>
where
    S: CommandSource,
    K: EventSink,
    P: SnapshotPersist,
{
    let (in_tx, in_rx) = sync_channel::<Input>(config.queue_capacity);
    let (out_tx, out_rx) = sync_channel::<Output>(config.queue_capacity);
    // A snapshot waiting to be written is replaced by a newer one only after it
    // is done, so one slot is enough.
    let (snap_tx, snap_rx) = sync_channel::<Box<EngineState>>(1);

    let reader = thread::Builder::new()
        .name("matching-reader".into())
        .spawn(move || read_loop(&mut source, &in_tx, &stop))
        .expect("spawn reader");
    let matcher_cfg = config.clone();
    let matcher = thread::Builder::new()
        .name("matching-matcher".into())
        .spawn(move || match_loop(engine, &in_rx, &out_tx, &matcher_cfg))
        .expect("spawn matcher");
    let writer = thread::Builder::new()
        .name("matching-writer".into())
        .spawn(move || write_loop(&mut sink, &out_rx, &snap_tx))
        .expect("spawn writer");
    let snapshotter = thread::Builder::new()
        .name("matching-snapshot".into())
        .spawn(move || snapshot_loop(&mut persist, &snap_rx))
        .expect("spawn snapshot");

    // Join in pipeline order. A failure upstream closes its queue, which ends
    // every later stage cleanly; a failure downstream makes upstream sends fail.
    let read_result = reader
        .join()
        .map_err(|_| PipelineError::Panicked("reader"))?;
    let match_result = matcher
        .join()
        .map_err(|_| PipelineError::Panicked("matcher"))?;
    let write_result = writer
        .join()
        .map_err(|_| PipelineError::Panicked("writer"))?;
    let (saved, failed) = snapshotter
        .join()
        .map_err(|_| PipelineError::Panicked("snapshot"))?;

    // Report the root cause: the earliest stage that failed on its own.
    let (mut report, match_err) = match_result;
    let events_published = write_result.as_ref().map_or(0, |n| *n);
    read_result.map_err(PipelineError::Source)?;
    if let Some(e) = match_err {
        return Err(PipelineError::Halted(e));
    }
    write_result.map_err(PipelineError::Sink)?;
    report.events_published = events_published;
    report.snapshots_saved = saved;
    report.snapshot_failures = failed;
    Ok(report)
}

fn read_loop(
    source: &mut impl CommandSource,
    tx: &SyncSender<Input>,
    stop: &AtomicBool,
) -> Result<(), TransportError> {
    while !stop.load(Ordering::Relaxed) {
        let rec = match source.fetch()? {
            Fetched::Record(r) => r,
            Fetched::Idle => continue,
            Fetched::End => break,
        };
        // Decoding here keeps the matcher thread doing nothing but matching.
        let input = match codec::decode_command(&rec.payload) {
            Ok((symbol, cmd)) => Input::Command {
                offset: rec.offset,
                symbol,
                cmd,
                headers: rec.headers,
            },
            Err(e) => Input::Undecodable {
                offset: rec.offset,
                payload: rec.payload,
                headers: rec.headers,
                reason: e.to_string(),
            },
        };
        if tx.send(input).is_err() {
            // The matcher stopped (it halted); its error is the one to report.
            break;
        }
    }
    Ok(())
}

fn match_loop(
    mut engine: Engine,
    rx: &Receiver<Input>,
    tx: &SyncSender<Output>,
    config: &PipelineConfig,
) -> (PipelineReport, Option<EngineError>) {
    let mut report = PipelineReport::default();
    let mut events = Vec::with_capacity(16);
    let mut halted = None;

    'commands: for input in rx.iter() {
        match input {
            Input::Command {
                offset,
                symbol,
                cmd,
                headers,
            } => {
                let outcome = match engine.process(offset, &symbol, &cmd, &mut events) {
                    Ok(o) => o,
                    Err(e) => {
                        tracing::error!(error = %e, offset, "engine halted");
                        halted = Some(e);
                        break 'commands;
                    }
                };
                match outcome {
                    ProcessOutcome::Applied => report.commands_applied += 1,
                    ProcessOutcome::AlreadyApplied => report.already_applied += 1,
                    ProcessOutcome::DuplicateCommand => report.duplicate_commands += 1,
                    ProcessOutcome::DuplicateOrderId => {
                        report.duplicate_order_ids += 1;
                        tracing::warn!(offset, command_id = %cmd.command_id,
                            "duplicate order id escaped the duplicate memory");
                    }
                }
                for e in events.drain(..) {
                    let record = OutputRecord {
                        topic: OutputTopic::Events,
                        key: symbol.as_str().as_bytes().to_vec(),
                        payload: codec::encode_event(&symbol, &e),
                        // Resulting events copy the command's trace context.
                        headers: headers.clone(),
                    };
                    if tx.send(Output::Record(record)).is_err() {
                        break 'commands;
                    }
                }
            }
            Input::Undecodable {
                offset,
                payload,
                mut headers,
                reason,
            } => {
                if !engine.skip(offset) {
                    report.already_applied += 1;
                    continue;
                }
                tracing::warn!(offset, %reason, "undecodable command sent to the dead-letter queue");
                report.dead_lettered += 1;
                headers.push(("dlq-reason".into(), reason.into_bytes()));
                headers.push(("dlq-source-offset".into(), offset.to_string().into_bytes()));
                let record = OutputRecord {
                    topic: OutputTopic::DeadLetter,
                    key: Vec::new(),
                    payload,
                    headers,
                };
                if tx.send(Output::Record(record)).is_err() {
                    break 'commands;
                }
            }
        }

        if engine.commands_since_snapshot() >= config.snapshot_interval {
            let started = Instant::now();
            let state = engine.take_snapshot();
            tracing::debug!(
                capture_ms = started.elapsed().as_millis() as u64,
                next_offset = state.next_offset,
                "snapshot captured"
            );
            if tx.send(Output::Checkpoint(Box::new(state))).is_err() {
                break 'commands;
            }
        }
    }

    // A halted engine must not snapshot: its state is the thing in doubt.
    if halted.is_none() && config.snapshot_on_shutdown && engine.commands_since_snapshot() > 0 {
        let _ = tx.send(Output::Checkpoint(Box::new(engine.take_snapshot())));
    }
    report.next_offset = engine.next_offset();
    (report, halted)
}

fn write_loop(
    sink: &mut impl EventSink,
    rx: &Receiver<Output>,
    snap_tx: &SyncSender<Box<EngineState>>,
) -> Result<u64, TransportError> {
    let mut published = 0u64;
    for out in rx.iter() {
        match out {
            Output::Record(r) => {
                sink.send(r)?;
                published += 1;
            }
            Output::Checkpoint(state) => {
                // Only a fully acknowledged prefix may be covered by a snapshot.
                sink.flush()?;
                if snap_tx.send(state).is_err() {
                    break;
                }
            }
        }
    }
    sink.flush()?;
    Ok(published)
}

fn snapshot_loop(
    persist: &mut impl SnapshotPersist,
    rx: &Receiver<Box<EngineState>>,
) -> (u64, u64) {
    let (mut saved, mut failed) = (0, 0);
    for state in rx.iter() {
        let started = Instant::now();
        match persist.persist(&state) {
            Ok(()) => {
                saved += 1;
                tracing::info!(
                    next_offset = state.next_offset,
                    write_ms = started.elapsed().as_millis() as u64,
                    "snapshot saved"
                );
            }
            Err(e) => {
                // Not fatal: the previous snapshot is intact; replay just gets longer.
                failed += 1;
                tracing::error!(error = %e, next_offset = state.next_offset, "snapshot failed");
            }
        }
    }
    (saved, failed)
}
