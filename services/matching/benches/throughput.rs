//! Matcher throughput (spec target: at least 500,000 commands/s on one core,
//! engine only, no Kafka). Run with `cargo bench -p matching --bench throughput`.
//!
//! Each measurement starts from a book already warmed with WARMUP commands,
//! so it reflects a realistic, populated book rather than an empty one.

#![allow(clippy::float_arithmetic)]

mod workload;

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use matching::{DEFAULT_DEDUP_CAPACITY, Engine, snapshot};
use order_book::OrderBook;

const WARMUP: usize = 200_000;
const BATCH: usize = 100_000;

fn engine_process(c: &mut Criterion) {
    let w = workload::generate(WARMUP + BATCH, 42);
    let (warm, measured) = w.inputs.split_at(WARMUP);

    let mut warmed = Engine::new(0, DEFAULT_DEDUP_CAPACITY);
    let mut out = Vec::new();
    for i in warm {
        warmed
            .process(i.offset, &w.symbols[i.symbol], &i.cmd, &mut out)
            .unwrap();
        out.clear();
    }

    let mut g = c.benchmark_group("matcher");
    g.throughput(Throughput::Elements(BATCH as u64));
    g.sample_size(30);

    // Full engine path: offset check, duplicate memory, book lookup, matching.
    g.bench_function("engine_process", |b| {
        b.iter_batched(
            || (warmed.clone(), Vec::with_capacity(16)),
            |(mut engine, mut out)| {
                for i in measured {
                    engine
                        .process(i.offset, &w.symbols[i.symbol], &i.cmd, &mut out)
                        .unwrap();
                    black_box(&out);
                    out.clear();
                }
                engine
            },
            BatchSize::LargeInput,
        )
    });

    // The book alone, to show what the engine's bookkeeping costs on top.
    let mut books: Vec<OrderBook> = w.symbols.iter().cloned().map(OrderBook::new).collect();
    for i in warm {
        let _ = books[i.symbol].apply(&i.cmd, &mut out);
        out.clear();
    }
    g.bench_function("book_apply", |b| {
        b.iter_batched(
            || (books.clone(), Vec::with_capacity(16)),
            |(mut books, mut out)| {
                for i in measured {
                    let _ = books[i.symbol].apply(&i.cmd, &mut out);
                    black_box(&out);
                    out.clear();
                }
                books
            },
            BatchSize::LargeInput,
        )
    });
    g.finish();
}

fn snapshot_encode(c: &mut Criterion) {
    // A worst-case snapshot: the duplicate memory completely full.
    let w = workload::generate(DEFAULT_DEDUP_CAPACITY + 1, 7);
    let mut engine = Engine::new(0, DEFAULT_DEDUP_CAPACITY);
    let mut out = Vec::new();
    for i in &w.inputs {
        engine
            .process(i.offset, &w.symbols[i.symbol], &i.cmd, &mut out)
            .unwrap();
        out.clear();
    }
    let state = engine.state();
    let bytes = snapshot::encode(&state);
    eprintln!(
        "full snapshot: {:.1} MiB, {} resting orders, {} remembered command IDs",
        bytes.len() as f64 / (1024.0 * 1024.0),
        state.books.iter().map(|b| b.orders.len()).sum::<usize>(),
        state.dedup_ids.len()
    );

    let mut g = c.benchmark_group("snapshot");
    g.sample_size(10);
    g.bench_function("capture_full", |b| b.iter(|| black_box(engine.state())));
    g.bench_function("encode_full", |b| {
        b.iter(|| black_box(snapshot::encode(&state)))
    });
    g.finish();
}

criterion_group!(benches, engine_process, snapshot_encode);
criterion_main!(benches);
