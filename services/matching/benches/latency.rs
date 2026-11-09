//! Matcher per-command latency and sustained throughput (spec targets: p99 at
//! most 10 µs per command; at least 500,000 commands/s on one core). Engine
//! only, no Kafka. Run with `cargo bench -p matching --bench latency`.
//!
//! Criterion reports averages; the spec's target is a percentile, so this
//! harness times every single command into an HDR histogram instead.

#![allow(clippy::float_arithmetic)]

mod workload;

use std::time::{Duration, Instant};

use domain::TimeInForce;
use hdrhistogram::Histogram;
use matching::{DEFAULT_DEDUP_CAPACITY, Engine};
use order_book::{CommandBody, NewOrder};

const WARMUP: usize = 200_000;
const MEASURED: usize = 2_000_000;
const THROUGHPUT_RUNS: usize = 5;

fn main() {
    // `cargo bench` passes `--bench`; ignore arguments.
    let w = workload::generate(WARMUP + MEASURED, 42);
    let (warm, measured) = w.inputs.split_at(WARMUP);

    let mut warmed = Engine::new(0, DEFAULT_DEDUP_CAPACITY);
    let mut out = Vec::with_capacity(16);
    for i in warm {
        warmed
            .process(i.offset, &w.symbols[i.symbol], &i.cmd, &mut out)
            .unwrap();
        out.clear();
    }

    // ---- sustained throughput: no per-command timers, whole-run wall clock.
    let mut rates = Vec::new();
    let mut events = 0usize;
    for _ in 0..THROUGHPUT_RUNS {
        let mut engine = warmed.clone();
        events = 0;
        let start = Instant::now();
        for i in measured {
            engine
                .process(i.offset, &w.symbols[i.symbol], &i.cmd, &mut out)
                .unwrap();
            events += out.len();
            out.clear();
        }
        rates.push(MEASURED as f64 / start.elapsed().as_secs_f64());
        std::hint::black_box(&engine);
    }
    rates.sort_by(f64::total_cmp);
    let median_rate = rates[rates.len() / 2];

    // ---- per-command latency.
    let new_hist = || Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();
    let mut all = new_hist();
    let mut resting = new_hist();
    let mut immediate = new_hist();
    let mut cancels = new_hist();
    let mut trades = 0usize;
    let mut engine = warmed.clone();
    for i in measured {
        let t0 = Instant::now();
        engine
            .process(i.offset, &w.symbols[i.symbol], &i.cmd, &mut out)
            .unwrap();
        let ns = t0.elapsed().as_nanos().max(1) as u64;
        trades += out
            .iter()
            .filter(|e| matches!(e.body, order_book::EventBody::Trade(_)))
            .count();
        out.clear();
        all.record(ns).unwrap();
        match &i.cmd.body {
            CommandBody::CancelOrder(_) => cancels.record(ns).unwrap(),
            CommandBody::NewOrder(NewOrder {
                time_in_force: TimeInForce::Ioc,
                ..
            }) => immediate.record(ns).unwrap(),
            CommandBody::NewOrder(_) => resting.record(ns).unwrap(),
        }
    }

    // ---- what the timer itself costs, so it can be read out of the numbers.
    let mut timer = new_hist();
    for _ in 0..1_000_000 {
        let t0 = Instant::now();
        let ns = t0.elapsed().as_nanos().max(1) as u64;
        timer.record(ns).unwrap();
    }

    let resting_orders: usize = engine.state().books.iter().map(|b| b.orders.len()).sum();

    println!();
    println!("Workload: {MEASURED} measured commands after {WARMUP} warm-up commands,");
    println!(
        "{} stocks, {} users, {} events ({} trades), {} orders resting at the end.",
        workload::STOCKS,
        workload::USERS,
        events,
        trades,
        resting_orders
    );
    println!();
    println!(
        "Sustained throughput: {:.0} commands/s (median of {THROUGHPUT_RUNS}; min {:.0}, max {:.0})",
        median_rate,
        rates[0],
        rates[rates.len() - 1]
    );
    println!();
    println!("| Commands | Count | p50 | p90 | p99 | p99.9 | p99.99 | max |");
    println!("|---|---|---|---|---|---|---|---|");
    for (name, h) in [
        ("All", &all),
        ("New, rests", &resting),
        ("New, IOC", &immediate),
        ("Cancel", &cancels),
        ("Timer only", &timer),
    ] {
        println!(
            "| {name} | {} | {} | {} | {} | {} | {} | {} |",
            h.len(),
            fmt(h.value_at_quantile(0.50)),
            fmt(h.value_at_quantile(0.90)),
            fmt(h.value_at_quantile(0.99)),
            fmt(h.value_at_quantile(0.999)),
            fmt(h.value_at_quantile(0.9999)),
            fmt(h.max()),
        );
    }
    let p99 = Duration::from_nanos(all.value_at_quantile(0.99));
    println!();
    println!(
        "Targets: throughput >= 500000/s -> {}; p99 <= 10 µs -> {} ({:?})",
        pass(median_rate >= 500_000.0),
        pass(p99 <= Duration::from_micros(10)),
        p99
    );
}

fn fmt(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.2} µs", ns as f64 / 1e3)
    } else {
        format!("{:.2} ms", ns as f64 / 1e6)
    }
}

fn pass(ok: bool) -> &'static str {
    if ok { "PASS" } else { "FAIL" }
}
