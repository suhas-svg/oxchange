//! End-to-end checks for the matching engine against a running Redpanda.
//!
//! ```text
//! e2e produce <count> <rate_per_sec> <seed>
//! e2e verify  [timeout_secs]
//! e2e republish <seconds> <percent> [seed]
//! ```
//!
//! `republish` is the spec's "Duplicate events" failure: for `seconds` it
//! reads every new message on the four main topics and writes a copy of
//! `percent` of them back to the same partition (same key, payload and
//! headers). Consumers must ignore the copies.
//!
//! `verify` is the Gate 1 "restart output is identical" check. For every
//! partition it rereads all of `orders.commands`, runs a fresh engine over it
//! to get the events that *should* exist, then reads all of `matching.events`.
//! Repeats (from matchers replaying after a crash) must be byte-identical to
//! the first copy, and once repeats are dropped the stream must equal the
//! expected one exactly: nothing missing, nothing extra, nothing reordered.
//! Because the expectation is rebuilt from the log, the check does not care
//! when or how often matchers were killed.

#![allow(clippy::float_arithmetic)]

#[path = "../../../services/matching/benches/workload/mod.rs"]
mod workload;

use std::collections::HashMap;
use std::process::ExitCode;
use std::thread;
use std::time::{Duration, Instant};

use matching::codec::{decode_command, decode_event, encode_command, encode_event};
use matching::{DEFAULT_DEDUP_CAPACITY, Engine};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use rdkafka::{Message, Offset, TopicPartitionList};

const COMMANDS: &str = "orders.commands";
const EVENTS: &str = "matching.events";
const DLQ: &str = "orders.commands.dlq";
const TIMEOUT: Duration = Duration::from_secs(10);

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "redpanda:9092".into());
    let result = match args.get(1).map(String::as_str) {
        Some("produce") => {
            let n = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(50_000);
            let rate = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(2_000);
            let seed = args.get(4).and_then(|v| v.parse().ok()).unwrap_or(42);
            produce(&brokers, n, rate, seed)
        }
        Some("verify") => {
            let secs = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(180);
            verify(&brokers, Duration::from_secs(secs))
        }
        Some("republish") => {
            let secs = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(60);
            let percent = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(1);
            let seed = args.get(4).and_then(|v| v.parse().ok()).unwrap_or(42);
            republish(&brokers, Duration::from_secs(secs), percent, seed)
        }
        _ => Err("usage: e2e produce <count> <rate> <seed> | e2e verify [timeout_secs]                   | e2e republish <seconds> <percent> [seed]"
            .into()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("FAIL: {e}");
            ExitCode::FAILURE
        }
    }
}

fn produce(brokers: &str, n: usize, rate: u64, seed: u64) -> Result<(), String> {
    let producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("partitioner", "murmur2_random")
        .create()
        .map_err(|e| e.to_string())?;
    let w = workload::generate(n, seed);
    let started = Instant::now();
    for (i, input) in w.inputs.iter().enumerate() {
        // Fixed schedule: send each command at its time, whatever happens.
        let due = Duration::from_secs_f64(i as f64 / rate as f64);
        if let Some(wait) = due.checked_sub(started.elapsed()) {
            thread::sleep(wait);
        }
        let symbol = &w.symbols[input.symbol];
        let payload = encode_command(symbol, &input.cmd);
        let mut record = BaseRecord::<str, [u8]>::to(COMMANDS)
            .key(symbol.as_str())
            .payload(&payload);
        loop {
            match producer.send(record) {
                Ok(()) => break,
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), back)) => {
                    record = back;
                    thread::sleep(Duration::from_millis(1));
                }
                Err((e, _)) => return Err(e.to_string()),
            }
        }
        if (i + 1) % 10_000 == 0 {
            println!("produced {} commands", i + 1);
        }
    }
    producer
        .flush(Duration::from_secs(60))
        .map_err(|e| e.to_string())?;
    println!(
        "produced {n} commands in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn consumer(brokers: &str) -> Result<BaseConsumer, String> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "e2e-verify")
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .create()
        .map_err(|e| e.to_string())
}

fn partitions(c: &BaseConsumer, topic: &str) -> Result<i32, String> {
    let md = c
        .fetch_metadata(Some(topic), TIMEOUT)
        .map_err(|e| e.to_string())?;
    let t = md
        .topics()
        .first()
        .ok_or(format!("topic {topic} not found"))?;
    Ok(t.partitions().len() as i32)
}

/// Every payload in `topic`/`partition` from offset 0 up to its current end.
fn read_all(c: &BaseConsumer, topic: &str, partition: i32) -> Result<Vec<Vec<u8>>, String> {
    let (low, high) = c
        .fetch_watermarks(topic, partition, TIMEOUT)
        .map_err(|e| e.to_string())?;
    if low != 0 {
        return Err(format!(
            "{topic}/{partition} starts at {low}: retention deleted data"
        ));
    }
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Beginning)
        .map_err(|e| e.to_string())?;
    c.assign(&tpl).map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(high as usize);
    let deadline = Instant::now() + Duration::from_secs(120);
    while (out.len() as i64) < high {
        if Instant::now() > deadline {
            return Err(format!("timed out reading {topic}/{partition}"));
        }
        if let Some(m) = c.poll(Duration::from_millis(200)) {
            let m = m.map_err(|e| e.to_string())?;
            out.push(m.payload().unwrap_or_default().to_vec());
        }
    }
    Ok(out)
}

/// The events a correct matcher must publish for these commands, in order.
fn expected_events(commands: &[Vec<u8>], partition: i32) -> Vec<Vec<u8>> {
    let mut engine = Engine::new(partition as u32, DEFAULT_DEDUP_CAPACITY);
    let mut events = Vec::new();
    let mut out = Vec::new();
    for (offset, payload) in commands.iter().enumerate() {
        match decode_command(payload) {
            Ok((symbol, cmd)) => {
                engine
                    .process(offset as u64, &symbol, &cmd, &mut out)
                    .expect("reference engine halted");
                events.extend(out.drain(..).map(|e| encode_event(&symbol, &e)));
            }
            Err(_) => {
                engine.skip(offset as u64);
            }
        }
    }
    events
}

struct PartitionCheck {
    commands: usize,
    events: usize,
    repeats: usize,
}

fn check_partition(brokers: &str, p: i32, deadline: Instant) -> Result<PartitionCheck, String> {
    let c = consumer(brokers)?;
    let commands = read_all(&c, COMMANDS, p)?;
    let expected = expected_events(&commands, p);
    loop {
        let published = read_all(&c, EVENTS, p)?;
        let mut seen: HashMap<(String, u64), usize> = HashMap::new();
        let mut deduped: Vec<&Vec<u8>> = Vec::new();
        let mut repeats = 0;
        for payload in &published {
            let (sym, ev) = decode_event(payload).map_err(|e| format!("undecodable event: {e}"))?;
            let key = (sym.to_string(), ev.seq);
            match seen.get(&key) {
                Some(&first) => {
                    if deduped[first] != payload {
                        return Err(format!(
                            "partition {p}: repeat of {}-{} differs",
                            key.0, key.1
                        ));
                    }
                    repeats += 1;
                }
                None => {
                    seen.insert(key, deduped.len());
                    deduped.push(payload);
                }
            }
        }
        // The matcher may still be catching up; a prefix is fine until the deadline.
        let n = deduped.len().min(expected.len());
        if let Some(i) = (0..n).find(|&i| *deduped[i] != expected[i]) {
            return Err(format!(
                "partition {p}: event {i} differs from the expected stream"
            ));
        }
        if deduped.len() > expected.len() {
            return Err(format!(
                "partition {p}: {} events published, only {} expected",
                deduped.len(),
                expected.len()
            ));
        }
        if deduped.len() == expected.len() {
            return Ok(PartitionCheck {
                commands: commands.len(),
                events: expected.len(),
                repeats,
            });
        }
        if Instant::now() > deadline {
            return Err(format!(
                "partition {p}: only {} of {} events published before the deadline",
                deduped.len(),
                expected.len()
            ));
        }
        thread::sleep(Duration::from_secs(2));
    }
}

fn verify(brokers: &str, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let c = consumer(brokers)?;
    let parts = partitions(&c, COMMANDS)?;
    let mut totals = (0, 0, 0);
    for p in 0..parts {
        let r = check_partition(brokers, p, deadline)?;
        println!(
            "partition {p}: {} commands, {} events match exactly, {} byte-identical repeats",
            r.commands, r.events, r.repeats
        );
        totals = (
            totals.0 + r.commands,
            totals.1 + r.events,
            totals.2 + r.repeats,
        );
    }
    let dlq = read_all(&c, DLQ, 0)?.len();
    if dlq != 0 {
        return Err(format!("{dlq} commands in the dead-letter queue"));
    }
    println!(
        "PASS: {} commands, {} events, {} repeats from replays; no gaps, no extras, no differences",
        totals.0, totals.1, totals.2
    );
    Ok(())
}

/// Topics whose messages `republish` copies.
const REPUBLISHED: [&str; 4] = [COMMANDS, EVENTS, "settlement.events", "accounts.events"];

fn republish(brokers: &str, run_for: Duration, percent: u64, seed: u64) -> Result<(), String> {
    let c = consumer(brokers)?;
    let mut tpl = TopicPartitionList::new();
    for topic in REPUBLISHED {
        for p in 0..partitions(&c, topic)? {
            tpl.add_partition_offset(topic, p, Offset::End)
                .map_err(|e| e.to_string())?;
        }
    }
    c.assign(&tpl).map_err(|e| e.to_string())?;
    let producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .create()
        .map_err(|e| e.to_string())?;
    let started = Instant::now();
    let (mut read, mut copied) = (0u64, HashMap::<&str, u64>::new());
    while started.elapsed() < run_for {
        let Some(msg) = c.poll(Duration::from_millis(200)) else {
            continue;
        };
        let msg = msg.map_err(|e| e.to_string())?;
        read += 1;
        // The same seed picks the same messages, whatever the timing.
        let pick = mix(seed ^ mix(msg.offset() as u64) ^ ((msg.partition() as u64) << 48)) % 10_000
            < percent * 100;
        if !pick {
            continue;
        }
        let topic = REPUBLISHED
            .into_iter()
            .find(|t| *t == msg.topic())
            .unwrap_or(EVENTS);
        let mut headers = OwnedHeaders::new();
        if let Some(hs) = msg.headers() {
            for h in hs.iter() {
                headers = headers.insert(Header {
                    key: h.key,
                    value: h.value,
                });
            }
        }
        let mut record = BaseRecord::<[u8], [u8]>::to(topic)
            .partition(msg.partition())
            .payload(msg.payload().unwrap_or_default())
            .headers(headers);
        if let Some(k) = msg.key() {
            record = record.key(k);
        }
        let mut record = Some(record);
        while let Some(r) = record.take() {
            match producer.send(r) {
                Ok(()) => *copied.entry(topic).or_default() += 1,
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), r)) => {
                    thread::sleep(Duration::from_millis(5));
                    record = Some(r);
                }
                Err((e, _)) => return Err(e.to_string()),
            }
        }
    }
    producer.flush(TIMEOUT).map_err(|e| e.to_string())?;
    let total: u64 = copied.values().sum();
    println!("read {read} messages, republished {total} ({percent}%): {copied:?}");
    Ok(())
}

/// splitmix64's finalizer.
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}
