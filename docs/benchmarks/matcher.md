# Matcher benchmark

Gate 1 requires the matcher benchmark to be recorded. This page records it: the matching engine alone, single-threaded, with no Kafka, network, or disk in the measured path.

**Result: both targets pass.**

| Measure | Target | Measured | Result |
|---|---|---|---|
| Matcher throughput | ≥ 500,000 commands/s on one core | 2.59M/s sustained (2.4M–3.4M depending on method, see below) | Pass, ~5× headroom |
| Matcher processing time | p99 ≤ 10 µs per command | p99 1.1 µs | Pass, ~9× headroom |

Recorded 2026-11-09. Replace these numbers whenever the engine changes; never quote them for a different machine.

## Setup

| Item | Value |
|---|---|
| CPU | Intel Core i7-8750H @ 2.20 GHz (6 cores, 12 threads; 2018 laptop part) |
| RAM | 16 GB |
| OS | Windows 11 Home (Insider Preview), on AC power |
| Rust | 1.99.1 stable, `x86_64-pc-windows-gnullvm` |
| Build | `bench` profile: `opt-level = 3`, `lto = "fat"`, `codegen-units = 1` |
| Thread | One thread, not pinned to a core; other desktop software running |

This is **not** the spec's 8-core Linux target machine. The matcher target is per core, so this setup is valid for Gate 1, but the laptop's lack of core pinning and Windows scheduling inflate the rare tail values (see [Tail latency](#tail-latency)).

## Workload

Generated deterministically (seed 42) by `services/matching/benches/workload/mod.rs`, following the shape of the spec's load test:

- 20 stocks, 500 users, prices starting near $100 with a slow random walk.
- 60% limit orders that rest 1–20 cents from the market price.
- 30% cancels, mostly of recent orders. Some reach orders that have already filled, as in real traffic.
- 10% IOC orders that cross the spread by up to 25 cents and trade at once.
- Quantities from 1 to 100 shares.

The engine is warmed with 200,000 commands before measuring, so the books are populated rather than empty. The full engine path is measured: offset check, duplicate memory (1,000,000 IDs, as the spec sets), book lookup, and matching.

Over the 2,000,000 measured commands, the engine produced 4,605,114 events, including 675,377 trades. 230,695 orders were resting at the end.

## Throughput

Two independent methods:

| Method | What it measures | Result |
|---|---|---|
| `latency` harness, sustained | 2,000,000 commands in one run on a growing book, no per-command timers; median of 5 runs | **2.59M commands/s** (range 2.44M–2.62M) |
| criterion `matcher/engine_process` | 100,000 commands after warm-up, 30 samples | **3.40M commands/s** (95% CI 3.38M–3.42M) |
| criterion `matcher/book_apply` | The same commands on bare `OrderBook`s, no engine bookkeeping | **8.40M commands/s** |

The sustained run is lower because its books keep growing for the whole 2M commands. The criterion run measures a fixed window just after warm-up. The sustained figure is the one to compare against the target.

## Latency per command

Each command is timed individually into an HDR histogram. Windows' high-resolution timer ticks every 100 ns, so values are rounded to 100 ns ("Timer only" row: the cost of the measurement itself).

| Commands | Count | p50 | p90 | p99 | p99.9 | p99.99 | max |
|---|---|---|---|---|---|---|---|
| All | 2,000,000 | 400 ns | 600 ns | **1.10 µs** | 3.50 µs | 27.01 µs | 3.34 ms |
| New, rests | 1,199,398 | 400 ns | 600 ns | 1.10 µs | 4.00 µs | 32.30 µs | 3.34 ms |
| New, IOC | 199,797 | 400 ns | 700 ns | 1.30 µs | 3.50 µs | 23.61 µs | 37.12 µs |
| Cancel | 600,805 | 400 ns | 600 ns | 1.10 µs | 2.70 µs | 23.10 µs | 1.48 ms |
| Timer only | 1,000,000 | 1 ns | 100 ns | 100 ns | 100 ns | 100 ns | 60.13 µs |

## Where the time goes

- **The book itself is cheap.** At 8.4M commands/s it costs about 120 ns per command.
- **The duplicate memory is the largest engine cost.** It's a 1,000,000-entry hash set (about 40 MB), far larger than the CPU cache, so most lookups miss the cache. This is the price of the spec's window size. A faster non-cryptographic hasher could cut it, but the target is already met, so per the spec we stopped here.
- **Fixed during this benchmark:** looking up a stock's book allocated a new `String` key on every command. Looking up before inserting removed that allocation: **+15% engine throughput** (criterion, p < 0.05).

## Tail latency

The first run had a **25 ms** maximum. The duplicate memory's hash set grew by doubling and rehashing every entry, which near 1M entries pauses the matcher for milliseconds. It now allocates its full size at startup, which brought the maximum down to **3.3 ms**.

The remaining rare spikes, at p99.99 and above, are on the "rests" path. They fit two causes:

1. The order book's own storage growing as the book gets deeper.
2. Windows scheduling on a thread that isn't pinned to a core.

The spec's Kubernetes plan reserves whole cores for each matcher, which addresses the second. Preallocating book storage would address the first, but only if a real measurement on the target machine shows a need.

## Snapshot cost (finding for the Kafka loop)

| Step | Time | Note |
|---|---|---|
| Capture state (`Engine::state`) | 11.9 ms | Must happen on the matcher thread, between commands |
| Encode to bytes | 12.5 ms | Can run on any thread |
| Full snapshot size | 21.6 MiB | 136,181 resting orders, 1,000,000 remembered IDs |

Taken every 100,000 commands, both steps together would stall the matcher for about 25 ms. **The Kafka loop must therefore encode and write snapshots on a separate thread**, leaving only the capture on the matcher thread. At the spec's load of 2,000 orders/s, a 12 ms capture once every 50 seconds delays about 24 commands, which stays well beyond p99.9.

## Reproduce

```bash
cargo bench -p matching --bench latency
```

```bash
cargo bench -p matching --bench throughput
```

Criterion stores its reports under `target/criterion/`, and later runs report their change against the last one.
