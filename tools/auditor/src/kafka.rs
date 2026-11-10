//! Reads all of `matching.events` that is still retained, up to the end
//! offsets seen at the start of the scan.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use prost::Message as _;
use proto_gen::events::v1 as ev;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

const TIMEOUT: Duration = Duration::from_secs(10);

/// Blocking: call it from `spawn_blocking`.
pub fn scan(brokers: &str, topic: &str) -> Result<Vec<ev::MatchingEvent>, String> {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        // Never commits, so the group name only labels the connection.
        .set("group.id", "auditor")
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .create()
        .map_err(|e| e.to_string())?;
    let meta = c
        .fetch_metadata(Some(topic), TIMEOUT)
        .map_err(|e| e.to_string())?;
    let t = meta
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| format!("topic {topic} not found"))?;

    let mut tpl = TopicPartitionList::new();
    // Partition -> end offset to read up to.
    let mut ends: HashMap<i32, i64> = HashMap::new();
    for p in t.partitions() {
        let (low, high) = c
            .fetch_watermarks(topic, p.id(), TIMEOUT)
            .map_err(|e| e.to_string())?;
        if high > low {
            ends.insert(p.id(), high);
            tpl.add_partition_offset(topic, p.id(), Offset::Offset(low))
                .map_err(|e| e.to_string())?;
        }
    }
    c.assign(&tpl).map_err(|e| e.to_string())?;

    let mut events = Vec::new();
    let mut last_progress = Instant::now();
    while !ends.is_empty() {
        match c.poll(Duration::from_millis(500)) {
            Some(Ok(m)) => {
                last_progress = Instant::now();
                let e =
                    ev::MatchingEvent::decode(m.payload().unwrap_or_default()).map_err(|e| {
                        format!("undecodable event at {}:{}: {e}", m.partition(), m.offset())
                    })?;
                events.push(e);
                if ends
                    .get(&m.partition())
                    .is_some_and(|&end| m.offset() + 1 >= end)
                {
                    ends.remove(&m.partition());
                }
            }
            Some(Err(e)) => return Err(e.to_string()),
            None if last_progress.elapsed() > TIMEOUT => {
                return Err(format!("scan stalled; partitions left: {ends:?}"));
            }
            None => {}
        }
    }
    Ok(events)
}
