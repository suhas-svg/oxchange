# System benchmark

The whole system under the spec's load-test workload: every service, Redpanda,
PostgreSQL, Redis and the observability stack, on one laptop. Recorded
2026-11-12. The matching engine on its own is in [matcher.md](matcher.md).

**In short:** on this laptop the system sustains about **50 orders/s** with
every latency target met, accepts up to **300 orders/s** for a minute, and
recovers a matcher with **138,000 commands to replay in 6.7 s**. It is far
from the spec's 2,000 orders/s. The disk is the first limit (about 10
orders/s with PostgreSQL on it); with the disk out of the way, the services
that consume `matching.events`, each applying one event per PostgreSQL
transaction, are the next.

## Results against the targets

| Measure | Target | Measured | Result |
|---|---|---|---|
| Matcher throughput | ≥ 500,000 commands/s, one core | 2.59 M/s ([matcher.md](matcher.md)) | Pass |
| Matcher processing time | p99 ≤ 10 µs | 1.1 µs | Pass |
| Sustained order rate | 2,000 orders/s for 30 min | 200/s offered for 30 min: 98.9% accepted, but consumers fell behind (below). Sustainable: about 50/s | **Miss** |
| Order accepted response | p99 ≤ 50 ms | 46 ms at 50/s; 67 ms at 100/s; 264 ms at 200/s sustained | Pass at 50/s only |
| Order to fill notification | p99 ≤ 100 ms | 47 ms at 50/s; 79 ms at 100/s | Pass up to 100/s |
| Trade to final balance | p99 ≤ 250 ms | 92 ms at 50/s; 180 ms at 100/s | Pass up to 100/s |
| Trade to public price update | p99 ≤ 100 ms with 5,000 clients | 26 ms at 50/s, 75 ms at 100/s, **with 20 clients** | Pass, but not at the spec's client count |
| Matcher recovery | ≤ 10 s with 100,000 commands to replay | 6.7 s with 138,181; 4.2 s with 85,817 | Pass |
| Memory | No growth trend over 1 hour | 30-minute run only; see [Memory](#memory) | Not shown |
| Correctness | 0 auditor failures | 0 after all 11 failure tests ([failure-tests.md](../failure-tests.md)); 0 after the sustained run once consumers caught up ([below](#correctness-after-the-sustained-run)) | Pass |

Rates in the sweeps are 60-second runs. "Accepted" is the HTTP response to
`POST /v1/orders`, measured by the load generator from each order's scheduled
send time (open loop, so a stall cannot hide its own delay).

## Test machine

| Item | Value |
|---|---|
| CPU | Intel Core i7-8750H, 6 cores / 12 threads (2018 laptop part) |
| RAM | 16 GB |
| Disk | Kingston SNVS250G NVMe, no DRAM cache |
| OS | Windows 11 Home, Docker Desktop with the WSL 2 VM (12 CPUs, 8 GB) |
| Inside the VM | All 7 services, 4 matchers, 1 Redpanda broker (1 core), PostgreSQL 16, Redis 7, Prometheus, Tempo, Loki, Grafana, OpenTelemetry collector, and the load generator itself |

The spec's target machine is a dedicated 8-core server. Everything here
shares 12 hardware threads with a desktop OS, and the load generator runs
on the same machine it is measuring.

**The VM's disk.** `pg_test_fsync` measures about 22 synchronous flushes per
second (45 ms per `fdatasync`). The disk also stops completing writes for 10
to 18 s at a time, even with no containers running (the VM's I/O pressure
stays at 50–70% "full" at idle). Both come from the virtual-disk layer, not
from the code.

## Workload

`tools/loadgen` with its defaults: 20 stocks, 500 users (each funded with
cash and shares in two stocks through Accounts), 60% limit orders that rest
near the market, 30% cancels of the user's oldest resting order, 10% orders
that cross the spread at once. 50 users also hold a private WebSocket open
(fill notifications) and 20 clients follow every stock's public trades.

```bash
python tools/loadgen/bench.py sweep --compose "-p oxbench" --rates 10,25,50,100,200
```

```bash
python tools/loadgen/bench.py sustained --compose "-p oxbench -f docker-compose.yml -f tools/loadgen/compose.fast-db.yml" --rate 200 --duration 1800
```

## 1. PostgreSQL on the laptop's disk

The default `docker-compose.yml`: full durability (`synchronous_commit=on`).

| Offered/s | Accepted/s | OK | Accepted p50 | Accepted p99 | Fill notification p99 | Trade to settled p99 | Public trade p99 |
|---|---|---|---|---|---|---|---|
| 10 | 10 | 96.7% | 268 ms | 943 ms | 986 ms | 9.1 s | 771 ms |
| 25 | 19 | 75.3% | 208 ms | 1,544 ms | 1,599 ms | 28.9 s | 793 ms |

Each accepted order needs at least three durable commits (save, reserve,
accept), and every consumer commits once per event. With about 22 flushes a
second shared by all of them, the system cannot keep up with even 10
orders/s. Every failure was a clean `503 SERVICE_UNAVAILABLE` (Risk or
Accounts out of time); no request errored or hung, and no data was wrong.

## 2. PostgreSQL on a fast disk (simulated)

The spec's tuning step 3 is "put PostgreSQL on a dedicated fast SSD". This
laptop has none, so [tools/loadgen/compose.fast-db.yml](../../tools/loadgen/compose.fast-db.yml)
puts PostgreSQL's data in memory with `synchronous_commit=off`. Commits stay
atomic and visible; only a crash of PostgreSQL itself could lose the last
fraction of a second. Read these numbers as "what the services do once the
disk is not the limit", not as a durable configuration.

| Offered/s | Accepted/s | OK | Accepted p50 | Accepted p99 | Fill notification p99 | Trade to settled p99 | Public trade p99 |
|---|---|---|---|---|---|---|---|
| 50 | 50 | 100% | 12 ms | 46 ms | 47 ms | 92 ms | 26 ms |
| 100 | 100 | 100% | 15 ms | 67 ms | 79 ms | 180 ms | 75 ms |
| 200 | 200 | 100% | 37 ms | 210 ms | 217 ms | 1.8 s | 60 ms |
| 300 | 300 | 100% | 72 ms | 399 ms | 12.2 s | ≥ 30 s | 11.8 s |
| 400 | 101 | 25% | 1.4 s | 2.0 s | 2.4 s | ≥ 30 s | 388 ms |

"≥ 30 s" is the top of the histogram: settlement was more than 30 s behind.
At 400/s the Gateway's HTTP/2 connection to Risk was closed by the client
library's protection against too many cancelled streams; those requests got
500 instead of 503, which is now fixed (`ResourceExhausted` maps to 503).

## 3. Thirty minutes at 200 orders/s (fast disk)

| Offered/s | Accepted/s | OK | Accepted p50 | Accepted p99 | Fill notification p99 | Trade to settled p99 | Public trade p99 |
|---|---|---|---|---|---|---|---|
| 200 | 198 | 98.9% | 28 ms | 264 ms | 61 s | ≥ 30 s | 20.6 s |

360,000 orders were offered: 355,909 accepted, 2,455 rejected with 503 and
1,636 timed out on the client (10 s). The failures cluster in 30 of the 1,800
seconds, mostly one 24-second stall about 26 minutes in, when Risk's
consumer-group session with the broker timed out and the group rebalanced.

The API kept up, but the services behind Kafka did not. The matchers wrote
1.46 M events (about 810 a second), and when traffic stopped the consumers of
`matching.events` were between 540,000 and 770,000 events behind. Measured
catch-up rates, with nothing else running:

| Consumer of matching.events | Events/s |
|---|---|
| Risk (order status, releases) | 200–490 |
| Settlement | 450–560 |
| Market Data | 670–750 |
| Notifications | 1,500 |

At about four events per order, Risk's consumer limits the sustained rate to
roughly **50 orders/s** on this machine, which matches where every latency
target is met in the sweep.

### Matcher recovery

After the run, each matcher was killed (SIGKILL) and started again. Snapshots
were turned off for the run (`SNAPSHOT_INTERVAL=1000000`), so each one had to
replay every command of its partition:

| Matcher | Commands replayed | Restart to ready |
|---|---|---|
| matching-1 | 138,181 | 6.66 s |
| matching-0 | 85,817 | 4.19 s |
| matching-2 | 69,578 | 2.82 s |
| matching-3 | 51,175 | 2.60 s |

About 20,000 commands/s of replay, read from Redpanda, so 100,000 commands
take about 5 s, under the 10 s target. With the normal snapshot interval
(every 100,000 commands) a restart never replays more than that. In the
failure tests, with snapshots every 500 commands, recovery took 17–326 ms.

### Memory

Sampled every 30 s with `docker stats` during the 30-minute run. Trend is a
least-squares slope over the second half.

| Container | Start | End | Trend (second half) |
|---|---|---|---|
| matching-0..3 | 8–9 MiB | 42–46 MiB | −4 to −9 MiB/h (flat) |
| settlement | 6 MiB | 37 MiB | +14 MiB/h |
| notifications | 6 MiB | 54 MiB | +30 MiB/h |
| market-data | 5 MiB | 56 MiB | +67 MiB/h |
| gateway | 3 MiB | 84 MiB | +317 MiB/h |
| risk | 6 MiB | 117 MiB | +233 MiB/h |
| accounts | 5 MiB | 180 MiB | −3.2 GiB/h (Argon2 setup memory being returned) |
| postgres | 142 MiB | 1.4 GiB | data in memory (expected) |
| tempo | 29 MiB | 2.2 GiB | every trace kept (expected in Compose) |

The matchers are flat. Gateway and Risk were still growing at the end of 30
minutes; whether that levels off (allocator caches, connection pools) or is
a leak needs the spec's full 1-hour run, which was not done. The target is
**not shown** yet.

## Correctness after the sustained run

Run 60 s after traffic stopped, the auditor failed: 3,529 orders showed
fewer filled shares than their trades (rule 8) and a few canceled orders
still had funds reserved (rule 11). Both rules are checked only "after
traffic stops", and the consumers had not applied those events yet; they
were still hundreds of thousands of events behind.

Once every consumer had caught up (51 minutes later, part of it with the
machine stalled for memory, see below), the auditor passed all 11 rules
with **0 problems over 1,465,204 events**. Nothing was lost or applied
twice; it was late.

The machine ran out of memory during that catch-up: Tempo was keeping every
trace from the run (about 7.7 GB an hour at this rate) next to PostgreSQL's
in-memory data, in an 8 GB VM, and Docker stopped answering until Tempo,
Loki and Grafana were stopped. Long runs on this machine should keep fewer
traces (`TRACE_KEEP_PERCENT`).

## Where the time goes

1. **The disk.** On this machine PostgreSQL's flushes are the first limit,
   as the spec predicts: about 10 orders/s with full durability.
2. **One database transaction per consumed event.** With the disk out of the
   way, the API accepts 300 orders/s, but each consumer of `matching.events`
   applies events one at a time per partition, each in its own PostgreSQL
   transaction (sequence claim plus effect), and commits the Kafka offset
   after every event. During the catch-up PostgreSQL used 2.1 cores and the
   single-core broker 0.75. This is the spec's tuning step 2, "batch outbox
   publishing and consumer database writes", and the next thing to change:
   apply a batch of events per transaction and let Kafka offsets be committed
   on a timer (the sequence check already makes repeats harmless).
3. **The shared machine.** Every service, PostgreSQL, the broker, the
   observability stack and the load generator share 12 hardware threads; the
   generator itself fell up to 3.8 s behind its schedule during the stall.

## Not measured

- 5,000 public WebSocket clients: the runs used 20. One machine running the
  clients and the system at once would mostly measure itself.
- The 1-hour memory run.
- A 3-broker cluster under load (the failure tests use one; these runs use
  the default single broker).
