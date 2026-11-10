//! Pipeline tests with in-memory transports: output order, snapshot safety,
//! crash and resume, dead-letter routing, backpressure, failure handling.

#[path = "../benches/workload/mod.rs"]
mod workload;

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use matching::codec::{decode_event, encode_command, encode_event};
use matching::pipeline::{
    self, CommandSource, EventSink, Fetched, InputRecord, OutputRecord, OutputTopic,
    PipelineConfig, PipelineError, SnapshotPersist, TransportError,
};
use matching::{Engine, EngineState};

const CAP: usize = 100_000;

fn records(n: usize, seed: u64) -> Vec<InputRecord> {
    let w = workload::generate(n, seed);
    w.inputs
        .iter()
        .map(|i| InputRecord {
            offset: i.offset,
            payload: encode_command(&w.symbols[i.symbol], &i.cmd),
            headers: vec![(
                "traceparent".into(),
                format!("00-{:032x}-01", i.offset).into_bytes(),
            )],
        })
        .collect()
}

/// Events a single engine produces for these records, grouped by input.
fn reference(records: &[InputRecord]) -> Vec<Vec<Vec<u8>>> {
    let mut e = Engine::new(0, CAP);
    records
        .iter()
        .map(|r| {
            let Ok((sym, cmd)) = matching::codec::decode_command(&r.payload) else {
                e.skip(r.offset);
                return Vec::new();
            };
            let mut out = Vec::new();
            e.process(r.offset, &sym, &cmd, &mut out).unwrap();
            out.iter().map(|ev| encode_event(&sym, ev)).collect()
        })
        .collect()
}

// ------------------------------------------------------------ fake transports

struct VecSource {
    records: std::vec::IntoIter<InputRecord>,
}

impl VecSource {
    fn new(records: Vec<InputRecord>) -> Self {
        Self {
            records: records.into_iter(),
        }
    }
}

impl CommandSource for VecSource {
    fn fetch(&mut self) -> Result<Fetched, TransportError> {
        Ok(self.records.next().map_or(Fetched::End, Fetched::Record))
    }
}

/// Records are "sent" first and only "acknowledged" on flush, like Kafka.
#[derive(Default)]
struct Log {
    pending: Vec<OutputRecord>,
    acked: Vec<OutputRecord>,
    flushes: usize,
}

#[derive(Clone, Default)]
struct MemSink {
    log: Arc<Mutex<Log>>,
    /// Fail the Nth flush (1-based), if set.
    fail_flush: Option<usize>,
}

impl EventSink for MemSink {
    fn send(&mut self, r: OutputRecord) -> Result<(), TransportError> {
        self.log.lock().unwrap().pending.push(r);
        Ok(())
    }
    fn flush(&mut self) -> Result<(), TransportError> {
        let mut log = self.log.lock().unwrap();
        log.flushes += 1;
        if Some(log.flushes) == self.fail_flush {
            return Err(TransportError("broker unavailable".into()));
        }
        let pending = std::mem::take(&mut log.pending);
        log.acked.extend(pending);
        Ok(())
    }
}

impl MemSink {
    fn acked_events(&self) -> Vec<Vec<u8>> {
        let log = self.log.lock().unwrap();
        log.acked
            .iter()
            .filter(|r| r.topic == OutputTopic::Events)
            .map(|r| r.payload.clone())
            .collect()
    }
}

/// Keeps every persisted snapshot and checks, at the moment of saving, that
/// all events of the commands it covers were already acknowledged.
#[derive(Clone)]
struct CheckingPersist {
    saved: Arc<Mutex<Vec<EngineState>>>,
    sink: MemSink,
    expected: Arc<Vec<Vec<Vec<u8>>>>,
    first_offset: u64,
    violations: Arc<Mutex<Vec<String>>>,
}

impl SnapshotPersist for CheckingPersist {
    fn persist(&mut self, state: &EngineState) -> io::Result<()> {
        let covered: usize = self.expected[self.first_offset as usize..state.next_offset as usize]
            .iter()
            .map(Vec::len)
            .sum();
        let acked = self.sink.acked_events().len();
        if acked < covered {
            self.violations.lock().unwrap().push(format!(
                "snapshot at {} saved with {acked} of {covered} events acknowledged",
                state.next_offset
            ));
        }
        self.saved.lock().unwrap().push(state.clone());
        Ok(())
    }
}

fn config(interval: u64, shutdown_snapshot: bool) -> PipelineConfig {
    PipelineConfig {
        snapshot_interval: interval,
        queue_capacity: 64,
        snapshot_on_shutdown: shutdown_snapshot,
    }
}

fn persist_for(sink: &MemSink, expected: &Arc<Vec<Vec<Vec<u8>>>>, first: u64) -> CheckingPersist {
    CheckingPersist {
        saved: Arc::default(),
        sink: sink.clone(),
        expected: expected.clone(),
        first_offset: first,
        violations: Arc::default(),
    }
}

// ----------------------------------------------------------------------- tests

#[test]
fn output_matches_a_direct_engine_run_and_snapshots_wait_for_acks() {
    let recs = records(20_000, 1);
    let expected = Arc::new(reference(&recs));
    let sink = MemSink::default();
    let persist = persist_for(&sink, &expected, 0);

    let report = pipeline::run(
        Engine::new(0, CAP),
        VecSource::new(recs),
        sink.clone(),
        persist.clone(),
        config(1_000, true),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();

    let want: Vec<Vec<u8>> = expected.iter().flatten().cloned().collect();
    assert_eq!(sink.acked_events(), want);
    assert_eq!(report.events_published as usize, want.len());
    assert_eq!(report.next_offset, 20_000);
    assert_eq!(report.snapshots_saved, 20);
    assert!(
        persist.violations.lock().unwrap().is_empty(),
        "{:?}",
        persist.violations
    );
}

#[test]
fn events_carry_the_commands_trace_headers_and_symbol_key() {
    let recs = records(200, 2);
    let sink = MemSink::default();
    let expected = Arc::new(reference(&recs));
    pipeline::run(
        Engine::new(0, CAP),
        VecSource::new(recs.clone()),
        sink.clone(),
        persist_for(&sink, &expected, 0),
        config(1_000, false),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    let by_offset: HashMap<String, &InputRecord> = recs
        .iter()
        .map(|r| {
            (
                matching::codec::decode_command(&r.payload)
                    .unwrap()
                    .1
                    .command_id
                    .to_string(),
                r,
            )
        })
        .collect();
    for out in &sink.log.lock().unwrap().acked {
        let (sym, ev) = decode_event(&out.payload).unwrap();
        assert_eq!(out.key, sym.as_str().as_bytes());
        let input = by_offset[&ev.command_id.to_string()];
        assert_eq!(out.headers, input.headers);
    }
}

#[test]
fn crash_then_resume_from_snapshot_republishes_identical_events() {
    let recs = records(30_000, 3);
    let expected = Arc::new(reference(&recs));

    // Run 1 dies at offset 17,500 without a shutdown snapshot: the last
    // periodic snapshot is at 15,000, so 2,500 commands are replayed.
    let sink1 = MemSink::default();
    let persist1 = persist_for(&sink1, &expected, 0);
    pipeline::run(
        Engine::new(0, CAP),
        VecSource::new(recs[..17_500].to_vec()),
        sink1.clone(),
        persist1.clone(),
        config(5_000, false),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    let last = persist1.saved.lock().unwrap().last().cloned().unwrap();
    assert_eq!(last.next_offset, 15_000);

    // Run 2 recovers from that snapshot and reads from its offset.
    let engine = Engine::from_state(last.clone(), CAP).unwrap();
    let sink2 = MemSink::default();
    let persist2 = persist_for(&sink2, &expected, last.next_offset);
    pipeline::run(
        engine,
        VecSource::new(recs[last.next_offset as usize..].to_vec()),
        sink2.clone(),
        persist2.clone(),
        config(5_000, true),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    assert!(persist1.violations.lock().unwrap().is_empty());
    assert!(persist2.violations.lock().unwrap().is_empty());

    // What a consumer sees: both runs' output. Repeats must be byte-identical,
    // and after dropping them by (symbol, seq) the stream is exactly right.
    let mut seen: HashMap<(String, u64), Vec<u8>> = HashMap::new();
    let mut deduped = Vec::new();
    let mut repeats = 0;
    for payload in sink1.acked_events().into_iter().chain(sink2.acked_events()) {
        let (sym, ev) = decode_event(&payload).unwrap();
        let key = (sym.to_string(), ev.seq);
        match seen.get(&key) {
            Some(prev) => {
                assert_eq!(prev, &payload, "repeat of {key:?} differs");
                repeats += 1;
            }
            None => {
                seen.insert(key, payload.clone());
                deduped.push(payload);
            }
        }
    }
    let want: Vec<Vec<u8>> = expected.iter().flatten().cloned().collect();
    assert!(
        repeats > 0,
        "the replay window should republish some events"
    );
    assert_eq!(deduped, want);
}

#[test]
fn undecodable_command_goes_to_the_dead_letter_queue_and_is_skipped() {
    let mut recs = records(500, 4);
    recs[100].payload = vec![0xFF, 0x00, 0x13];
    let expected = Arc::new(reference(&recs));
    let sink = MemSink::default();
    let report = pipeline::run(
        Engine::new(0, CAP),
        VecSource::new(recs.clone()),
        sink.clone(),
        persist_for(&sink, &expected, 0),
        config(1_000, false),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    assert_eq!(report.dead_lettered, 1);
    assert_eq!(report.next_offset, 500);
    let log = sink.log.lock().unwrap();
    let dlq: Vec<_> = log
        .acked
        .iter()
        .filter(|r| r.topic == OutputTopic::DeadLetter)
        .collect();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].payload, recs[100].payload);
    assert!(
        dlq[0]
            .headers
            .iter()
            .any(|(k, v)| k == "dlq-source-offset" && v == b"100")
    );
    assert!(dlq[0].headers.iter().any(|(k, _)| k == "dlq-reason"));
    drop(log);
    let want: Vec<Vec<u8>> = expected.iter().flatten().cloned().collect();
    assert_eq!(sink.acked_events(), want);
}

#[test]
fn failed_flush_stops_the_pipeline_before_any_snapshot_is_saved() {
    let recs = records(10_000, 5);
    let expected = Arc::new(reference(&recs));
    let sink = MemSink {
        fail_flush: Some(1),
        ..MemSink::default()
    };
    let persist = persist_for(&sink, &expected, 0);
    let err = pipeline::run(
        Engine::new(0, CAP),
        VecSource::new(recs),
        sink.clone(),
        persist.clone(),
        config(1_000, true),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap_err();
    assert!(matches!(err, PipelineError::Sink(_)), "{err}");
    assert!(persist.saved.lock().unwrap().is_empty());
}

/// A source that never ends, to test shutdown.
struct Endless(u64);

impl CommandSource for Endless {
    fn fetch(&mut self) -> Result<Fetched, TransportError> {
        if self.0 > 5_000 {
            thread::sleep(Duration::from_millis(1));
            return Ok(Fetched::Idle);
        }
        let rec = records(1, self.0).remove(0);
        let offset = self.0;
        self.0 += 1;
        Ok(Fetched::Record(InputRecord { offset, ..rec }))
    }
}

#[test]
fn stop_flag_shuts_down_cleanly_with_a_final_snapshot() {
    let stop = Arc::new(AtomicBool::new(false));
    let sink = MemSink::default();
    let saved = Arc::new(Mutex::new(Vec::new()));
    struct Keep(Arc<Mutex<Vec<EngineState>>>);
    impl SnapshotPersist for Keep {
        fn persist(&mut self, s: &EngineState) -> io::Result<()> {
            self.0.lock().unwrap().push(s.clone());
            Ok(())
        }
    }
    let stopper = {
        let stop = stop.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(300));
            stop.store(true, Ordering::Relaxed);
        })
    };
    let report = pipeline::run(
        Engine::new(0, CAP),
        Endless(0),
        sink,
        Keep(saved.clone()),
        config(1_000_000, true),
        stop,
    )
    .unwrap();
    stopper.join().unwrap();
    let saved = saved.lock().unwrap();
    assert_eq!(saved.len(), 1, "only the shutdown snapshot");
    assert_eq!(saved[0].next_offset, report.next_offset);
}

/// Feeds records slowly, so queries land while commands are flowing.
struct SlowSource(VecSource, usize);

impl CommandSource for SlowSource {
    fn fetch(&mut self) -> Result<Fetched, TransportError> {
        self.1 += 1;
        if self.1.is_multiple_of(20) {
            thread::sleep(Duration::from_micros(200));
        }
        self.0.fetch()
    }
}

/// GetBook's contract: an answer is the book exactly as of `as_of_seq`,
/// never halfway through a command.
#[test]
fn book_queries_are_answered_between_commands() {
    let recs = records(6_000, 9);
    let symbol = workload::generate(6_000, 9).symbols[0].clone();
    // The reference: the book after every event seq of `symbol`.
    let mut by_seq = HashMap::new();
    {
        let mut e = Engine::new(0, CAP);
        by_seq.insert(0, pipeline::book_view(&e, &symbol));
        for r in &recs {
            let (sym, cmd) = matching::codec::decode_command(&r.payload).unwrap();
            let mut out = Vec::new();
            e.process(r.offset, &sym, &cmd, &mut out).unwrap();
            if sym == symbol {
                let v = pipeline::book_view(&e, &symbol);
                by_seq.insert(v.as_of_seq, v);
            }
        }
    }

    let (queries_tx, queries) = std::sync::mpsc::sync_channel(4);
    let runner = thread::spawn(move || {
        pipeline::run_with_queries(
            Engine::new(0, CAP),
            SlowSource(VecSource::new(recs), 0),
            MemSink::default(),
            NoPersist,
            config(1_000_000, false),
            Arc::new(AtomicBool::new(false)),
            queries,
        )
    });
    let mut answered = Vec::new();
    while !runner.is_finished() {
        let (reply, answer) = std::sync::mpsc::sync_channel(1);
        if queries_tx
            .send(pipeline::BookQuery {
                symbol: symbol.clone(),
                reply,
            })
            .is_err()
        {
            break;
        }
        if let Ok(v) = answer.recv_timeout(Duration::from_secs(1)) {
            answered.push(v);
        }
    }
    runner.join().unwrap().unwrap();

    let distinct: std::collections::HashSet<u64> = answered.iter().map(|v| v.as_of_seq).collect();
    assert!(distinct.len() > 5, "queries only saw {distinct:?}");
    for v in &answered {
        assert_eq!(Some(v), by_seq.get(&v.as_of_seq), "seq {}", v.as_of_seq);
    }
}

struct NoPersist;

impl SnapshotPersist for NoPersist {
    fn persist(&mut self, _: &EngineState) -> io::Result<()> {
        Ok(())
    }
}
