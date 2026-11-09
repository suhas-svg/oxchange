//! Snapshot and recovery tests (spec: test level "Snapshot", and the failure
//! tests "Engine crash", "Crash during replay", "Corrupted snapshot" and
//! "Duplicate events", at the engine level).

use std::fs;

use domain::{CommandId, OrderId, OrderType, Side, Symbol, TimeInForce, UserId};
use matching::snapshot::{DecodeError, decode, encode};
use matching::{Engine, ProcessOutcome, SnapshotStore, recover};
use order_book::{BookEvent, CancelOrder, Command, CommandBody, NewOrder};
use proptest::prelude::*;

const CAP: usize = 1_000;

/// One record of `orders.commands`: its offset, key, and payload.
#[derive(Debug, Clone)]
struct Input {
    offset: u64,
    symbol: Symbol,
    cmd: Command,
}

#[derive(Debug, Clone)]
enum Op {
    New {
        sym: u8,
        user: u8,
        buy: bool,
        ioc: bool,
        price: i64,
        qty: i64,
    },
    Cancel(prop::sample::Index),
    /// The outbox publishes an earlier command again.
    Republish(prop::sample::Index),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (0u8..3, 0u8..4, any::<bool>(), prop::bool::weighted(0.2), 95i64..=105, 1i64..=20)
            .prop_map(|(sym, user, buy, ioc, price, qty)| Op::New { sym, user, buy, ioc, price, qty }),
        2 => any::<prop::sample::Index>().prop_map(Op::Cancel),
        1 => any::<prop::sample::Index>().prop_map(Op::Republish),
    ]
}

const SYMBOLS: [&str; 3] = ["ACME", "BOLT", "CRUX"];

fn inputs(ops: &[Op]) -> Vec<Input> {
    let mut out: Vec<Input> = Vec::new();
    let mut placed: Vec<(Symbol, OrderId, UserId)> = Vec::new();
    for (i, op) in ops.iter().enumerate() {
        let n = i as u128 + 1;
        // Kafka offsets may skip numbers; leave a gap now and then.
        let offset = i as u64 * 2;
        let (symbol, cmd) = match op {
            Op::New {
                sym,
                user,
                buy,
                ioc,
                price,
                qty,
            } => {
                let symbol = Symbol::new(SYMBOLS[*sym as usize]).unwrap();
                let order_id = OrderId::from_u128(n);
                let user_id = UserId::from_u128(*user as u128 + 1);
                placed.push((symbol.clone(), order_id, user_id));
                let body = CommandBody::NewOrder(NewOrder {
                    order_id,
                    user_id,
                    side: if *buy { Side::Buy } else { Side::Sell },
                    order_type: OrderType::Limit,
                    time_in_force: if *ioc {
                        TimeInForce::Ioc
                    } else {
                        TimeInForce::Gtc
                    },
                    price_cents: *price,
                    quantity: *qty,
                });
                (symbol, fresh(n, body))
            }
            Op::Cancel(idx) if !placed.is_empty() => {
                let (symbol, order_id, user_id) = placed[idx.index(placed.len())].clone();
                let body = CommandBody::CancelOrder(CancelOrder { order_id, user_id });
                (symbol, fresh(n, body))
            }
            Op::Republish(idx) if !out.is_empty() => {
                let prev = &out[idx.index(out.len())];
                (prev.symbol.clone(), prev.cmd.clone())
            }
            _ => continue,
        };
        out.push(Input {
            offset,
            symbol,
            cmd,
        });
    }
    out
}

fn fresh(n: u128, body: CommandBody) -> Command {
    Command {
        command_id: CommandId::from_u128(n),
        accepted_at_ns: n as i64 * 1_000,
        body,
    }
}

/// Feeds inputs to an engine; returns the events produced by each input.
fn feed(engine: &mut Engine, inputs: &[Input]) -> Vec<Vec<BookEvent>> {
    inputs
        .iter()
        .map(|i| {
            let mut out = Vec::new();
            engine
                .process(i.offset, &i.symbol, &i.cmd, &mut out)
                .expect("engine halted");
            out
        })
        .collect()
}

/// Inputs at or after `offset`, which is what a restarted engine rereads.
fn from_offset(inputs: &[Input], offset: u64) -> &[Input] {
    let start = inputs.partition_point(|i| i.offset < offset);
    &inputs[start..]
}

/// What a restarted engine does: reread from its own `next_offset` to the end.
fn replay(engine: &mut Engine, inputs: &[Input]) -> Vec<Vec<BookEvent>> {
    let offset = engine.next_offset();
    feed(engine, from_offset(inputs, offset))
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// Snapshot plus replay equals an uninterrupted run, and the crash window
    /// (commands applied after the snapshot, then lost) republishes the
    /// exact same events.
    #[test]
    fn snapshot_then_replay_equals_uninterrupted_run(
        ops in prop::collection::vec(op(), 1..250),
        snap_at in any::<prop::sample::Index>(),
        crash_at in any::<prop::sample::Index>(),
    ) {
        let inputs = inputs(&ops);
        prop_assume!(!inputs.is_empty());
        let mut reference = Engine::new(0, CAP);
        let expected = feed(&mut reference, &inputs);

        let k = snap_at.index(inputs.len() + 1);
        let mut live = Engine::new(0, CAP);
        let mut produced = feed(&mut live, &inputs[..k]);
        let bytes = encode(&live.take_snapshot());

        // Keep running past the snapshot, then "crash" and lose the engine.
        let m = k + crash_at.index(inputs.len() - k + 1);
        produced.extend(feed(&mut live, &inputs[k..m]));
        drop(live);

        let state = decode(&bytes).unwrap();
        let mut restored = Engine::from_state(state, CAP).unwrap();
        let replayed = replay(&mut restored, &inputs);

        // Replay republishes everything after the snapshot with identical
        // seqs and IDs (consumers drop the k..m repeats by seq) ...
        prop_assert_eq!(&replayed[..m - k], &produced[k..m]);
        // ... and the full output matches a run that never crashed.
        prop_assert_eq!(&expected[k..], &replayed[..]);
    }

    #[test]
    fn encode_decode_round_trips(ops in prop::collection::vec(op(), 0..250)) {
        let mut e = Engine::new(7, CAP);
        feed(&mut e, &inputs(&ops));
        let state = e.state();
        prop_assert_eq!(decode(&encode(&state)).unwrap(), state);
    }

    #[test]
    fn any_single_bit_flip_is_detected(
        ops in prop::collection::vec(op(), 1..100),
        at in any::<prop::sample::Index>(),
        bit in 0u8..8,
    ) {
        let mut e = Engine::new(0, CAP);
        feed(&mut e, &inputs(&ops));
        let mut bytes = encode(&e.state());
        let i = at.index(bytes.len());
        bytes[i] ^= 1 << bit;
        prop_assert!(decode(&bytes).is_err());
    }

    #[test]
    fn truncation_is_detected(ops in prop::collection::vec(op(), 1..100), cut in any::<prop::sample::Index>()) {
        let mut e = Engine::new(0, CAP);
        feed(&mut e, &inputs(&ops));
        let bytes = encode(&e.state());
        let len = cut.index(bytes.len());
        prop_assert!(decode(&bytes[..len]).is_err());
    }
}

#[test]
fn republished_command_is_applied_once() {
    let symbol = Symbol::new("ACME").unwrap();
    let cmd = fresh(
        1,
        CommandBody::NewOrder(NewOrder {
            order_id: OrderId::from_u128(1),
            user_id: UserId::from_u128(1),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            price_cents: 1000,
            quantity: 5,
        }),
    );
    let mut e = Engine::new(0, CAP);
    let mut out = Vec::new();
    assert_eq!(
        e.process(0, &symbol, &cmd, &mut out),
        Ok(ProcessOutcome::Applied)
    );
    let n = out.len();
    assert_eq!(
        e.process(1, &symbol, &cmd, &mut out),
        Ok(ProcessOutcome::DuplicateCommand)
    );
    assert_eq!(
        e.process(0, &symbol, &cmd, &mut out),
        Ok(ProcessOutcome::AlreadyApplied)
    );
    assert_eq!(out.len(), n);
    assert_eq!(e.next_offset(), 2);

    // The duplicate memory survives a snapshot.
    let mut restored = Engine::from_state(e.state(), CAP).unwrap();
    assert_eq!(
        restored.process(5, &symbol, &cmd, &mut out),
        Ok(ProcessOutcome::DuplicateCommand)
    );
}

// ------------------------------------------------------------- store on disk

fn sample_inputs(n: usize) -> Vec<Input> {
    let ops: Vec<Op> = (0..n)
        .map(|i| Op::New {
            sym: (i % 3) as u8,
            user: (i % 4) as u8,
            buy: i % 2 == 0,
            ioc: false,
            price: 95 + (i % 11) as i64,
            qty: 1 + (i % 7) as i64,
        })
        .collect();
    inputs(&ops)
}

#[test]
fn store_keeps_the_newest_three() {
    let dir = tempfile::tempdir().unwrap();
    let store = SnapshotStore::new(dir.path(), 2).unwrap();
    let inputs = sample_inputs(50);
    let mut e = Engine::new(2, CAP);
    for chunk in inputs.chunks(10) {
        feed(&mut e, chunk);
        store.save(&e.take_snapshot()).unwrap();
    }
    let files = store.list().unwrap();
    assert_eq!(files.len(), 3);
    let (loaded, skipped) = store.load_latest().unwrap();
    assert!(skipped.is_empty());
    assert_eq!(loaded.unwrap().state.next_offset, e.next_offset());
}

#[test]
fn no_snapshot_means_fresh_start() {
    let dir = tempfile::tempdir().unwrap();
    let store = SnapshotStore::new(dir.path(), 0).unwrap();
    let r = recover(&store, 0, CAP).unwrap();
    assert!(r.snapshot.is_none());
    assert_eq!(r.engine.next_offset(), 0);
}

#[test]
fn corrupted_newest_snapshot_falls_back_to_previous() {
    let dir = tempfile::tempdir().unwrap();
    let store = SnapshotStore::new(dir.path(), 0).unwrap();
    let inputs = sample_inputs(40);
    let mut reference = Engine::new(0, CAP);
    let expected = feed(&mut reference, &inputs);

    let mut e = Engine::new(0, CAP);
    feed(&mut e, &inputs[..20]);
    let older = store.save(&e.take_snapshot()).unwrap();
    feed(&mut e, &inputs[20..30]);
    let newest = store.save(&e.take_snapshot()).unwrap();

    // Damage one byte in the middle of the newest file.
    let mut bytes = fs::read(&newest).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    fs::write(&newest, bytes).unwrap();

    let mut r = recover(&store, 0, CAP).unwrap();
    assert_eq!(r.snapshot.as_deref(), Some(older.as_path()));
    assert_eq!(r.skipped.len(), 1);
    assert!(
        r.skipped[0].reason.contains("checksum"),
        "{}",
        r.skipped[0].reason
    );

    // The longer replay from the older snapshot still gives the same output.
    let replayed = replay(&mut r.engine, &inputs);
    assert_eq!(&expected[20..], &replayed[..]);
}

#[test]
fn crash_during_snapshot_write_leaves_only_an_ignored_temp_file() {
    let dir = tempfile::tempdir().unwrap();
    let store = SnapshotStore::new(dir.path(), 0).unwrap();
    let inputs = sample_inputs(20);
    let mut e = Engine::new(0, CAP);
    feed(&mut e, &inputs[..10]);
    let good = store.save(&e.take_snapshot()).unwrap();

    // Simulate dying halfway through writing the next one.
    feed(&mut e, &inputs[10..]);
    let full = encode(&e.state());
    let tmp = dir
        .path()
        .join(format!("snapshot-p000-{:020}.snap.tmp", e.next_offset()));
    fs::write(&tmp, &full[..full.len() / 2]).unwrap();

    let r = recover(&store, 0, CAP).unwrap();
    assert_eq!(r.snapshot.as_deref(), Some(good.as_path()));
    assert!(r.skipped.is_empty());

    // The next successful save cleans the temp file up.
    store.save(&e.take_snapshot()).unwrap();
    assert!(!tmp.exists());
}

#[test]
fn crash_during_replay_recovers_again() {
    let dir = tempfile::tempdir().unwrap();
    let store = SnapshotStore::new(dir.path(), 0).unwrap();
    let inputs = sample_inputs(60);
    let mut reference = Engine::new(0, CAP);
    let expected = feed(&mut reference, &inputs);

    let mut e = Engine::new(0, CAP);
    feed(&mut e, &inputs[..25]);
    store.save(&e.take_snapshot()).unwrap();
    drop(e);

    // First recovery dies partway through its replay.
    let mut first = recover(&store, 0, CAP).unwrap().engine;
    let rest = from_offset(&inputs, first.next_offset());
    feed(&mut first, &rest[..10]);
    drop(first);

    // Second recovery starts over from the same snapshot and gets it right.
    let mut second = recover(&store, 0, CAP).unwrap().engine;
    let replayed = replay(&mut second, &inputs);
    assert_eq!(&expected[25..], &replayed[..]);
}

#[test]
fn wrong_partition_file_is_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let store = SnapshotStore::new(dir.path(), 1).unwrap();
    let e = Engine::new(9, CAP);
    // Write partition 9's state under partition 1's name.
    let path = dir.path().join(format!("snapshot-p001-{:020}.snap", 0));
    fs::write(&path, encode(&e.state())).unwrap();
    let r = recover(&store, 1, CAP).unwrap();
    assert!(r.snapshot.is_none());
    assert_eq!(r.skipped.len(), 1);
}

#[test]
fn decode_rejects_garbage() {
    assert_eq!(decode(b""), Err(DecodeError::Truncated));
    assert_eq!(decode(b"NOTASNAPSHOTFILE"), Err(DecodeError::BadMagic));
}
