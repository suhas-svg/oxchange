# System benchmark

The whole system under the spec's load-test workload: every service, Redpanda,
PostgreSQL, Redis and the observability stack, on one laptop. Recorded
2026-11-12; sections 4–6 were added on 2026-11-13, after the consumers
began applying events in batches. The matching engine
on its own is in [matcher.md](matcher.md).

**In short:** on this laptop the system met every latency target at **50
orders/s**, accepts up to **300 orders/s** for a minute, and recovers a
matcher with **138,000 commands to replay in 6.7 s**. It is far from the
spec's 2,000 orders/s. The disk is the first limit (about 10 orders/s with
PostgreSQL on it). With the disk out of the way, the services consuming
`matching.events` were the next: one PostgreSQL transaction per event held
them to about 50 orders/s. They now apply events in batches and **keep up
with 200 orders/s for 30 minutes** (0 events behind at the end, against
540,000–770,000 before). At 250–300 orders/s the limit is CPU: PostgreSQL
and every service share the laptop's 12 threads, and Accounts' connections
wait on PostgreSQL, not on the pool. Memory stays flat for **an hour at
100 orders/s**.

## Results against the targets

| Measure | Target | Measured | Result |
|---|---|---|---|
| Matcher throughput | ≥ 500,000 commands/s, one core | 2.59 M/s ([matcher.md](matcher.md)) | Pass |
| Matcher processing time | p99 ≤ 10 µs | 1.1 µs | Pass |
| Sustained order rate | 2,000 orders/s for 30 min | 200/s for 30 min: 99.1% accepted, 0 client timeouts, consumers caught up ([section 4](#4-batched-consumer-writes)). Before batching: 98.9%, consumers 540,000+ events behind | **Miss** |
| Order accepted response | p99 ≤ 50 ms | 46 ms at 50/s; 67 ms at 100/s; 264 ms at 200/s sustained | Pass at 50/s only |
| Order to fill notification | p99 ≤ 100 ms | 47 ms at 50/s; 79 ms at 100/s | Pass up to 100/s |
| Trade to final balance | p99 ≤ 250 ms | 92 ms at 50/s; 180 ms at 100/s | Pass up to 100/s |
| Trade to public price update | p99 ≤ 100 ms with 5,000 clients | 26 ms at 50/s, 75 ms at 100/s, **with 20 clients** | Pass, but not at the spec's client count |
| Matcher recovery | ≤ 10 s with 100,000 commands to replay | 6.7 s with 138,181; 4.2 s with 85,817 | Pass |
| Memory | No growth trend over 1 hour | 1 hour at 100/s: every service flat in the second half; Accounts holds about 1 GB after the sign-up burst ([section 6](#6-one-hour-at-100-orderss-memory)) | Pass |
| Correctness | 0 auditor failures | 0 after all 11 failure tests, run again with batching ([failure-tests.md](../failure-tests.md)); 0 after the sustained run once consumers caught up ([below](#correctness-after-the-sustained-run)) | Pass |

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
a leak needed the spec's full 1-hour run: [section 6](#6-one-hour-at-100-orderss-memory)
has it, and both are flat.

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

## 4. Batched consumer writes

Measured 2026-11-13. The spec's tuning step 2: each consumer of
`matching.events` now applies all the messages waiting for one partition
(up to 100, `CONSUMER_MAX_BATCH`) in one PostgreSQL transaction, reads
and locks each stock's sequence row once per batch, and lets the Kafka
client commit offsets once a second instead of after every message.
Settlement also marks a batch's settled trades in one transaction, and
Market Data writes each stock's ticker once per batch. If a batch fails
for any reason, its messages are handled again one at a time, so retries,
halts and the sequence check behave exactly as before. Notifications
still handles one message at a time; only its offset commits changed.
All 11 failure tests pass with it ([failure-tests.md](../failure-tests.md)).

**The machine was slower that day.** Master's code, run again for
comparison on the same fast-disk setup, accepted only 31% of orders at
300/s (100% the day before) and 94.6% at 200/s. So the rows below compare
the two versions on the same day, not with sections 2 and 3.

| Version | Offered/s | Accepted/s | OK | Accepted p50 | Accepted p99 | Fill notification p99 | Trade to settled p99 | Public trade p99 |
|---|---|---|---|---|---|---|---|---|
| master | 100 | 100 | 100% | 24 ms | 180 ms | 313 ms | 470 ms | 152 ms |
| batched | 100 | 100 | 100% | 21 ms | 136 ms | 207 ms | 231 ms | 51 ms |
| master | 200 | 189 | 94.6% | 90 ms | 2,003 ms | 2,049 ms | 29.6 s | 294 ms |
| batched | 200 | 200 | 100% | 46 ms | 543 ms | 3,777 ms | 4,437 ms | 3,455 ms |
| master | 300 | 93 | 31.1% | 1,601 ms | 2,010 ms | 2,316 ms | 27.1 s | 430 ms |
| batched | 300 | 52 | 17.4% | 1,488 ms | 2,008 ms | 4,362 ms | 4,233 ms | 2,441 ms |

At 50 orders/s the batched version met the public-trade target (39 ms) but
not the other p99 targets on that day: accepted 98 ms, fill notification
206 ms, settled 201 ms.

At 200/s, with batching, settlement is no longer tens of seconds behind
and every order got an answer. The fill and public-trade p99s of that
minute come from one stall, during which Risk's consumer fell 422 events
behind for about 20 s and then caught up. At 300/s both versions fail the
same way, in the request path rather than the consumers: Risk and Accounts
both had all 16 database connections in use, and Accounts' `Reserve` took
58 ms at the median, so orders waited for a connection until the Gateway
gave up. Turning batching off (`CONSUMER_MAX_BATCH=1`) on the same stack
gave 32% at 300/s, the same as master.

### Thirty minutes at 200 orders/s, batched

Traces kept: 10% (`TRACE_KEEP_PERCENT=10`), so Tempo could not fill the
VM's memory as it did in section 3.

| Offered/s | Accepted/s | OK | Accepted p50 | Accepted p99 | Fill notification p50 / p99 | Trade to settled p99 | Public trade p50 / p99 |
|---|---|---|---|---|---|---|---|
| 200 | 198 | 99.13% | 40 ms | 1,559 ms | 66 ms / 5.7 s | 10.3 s | 19 ms / 4.8 s |

360,000 orders were offered: 356,860 accepted, 3,140 rejected with 503 and
**0 timed out** on the client (1,636 before). The rejections fall in 38 of
the 1,800 seconds, in two clusters about 12 and 29 minutes in; the p99s
come from those stalls, while the medians stay in tens of milliseconds.

Consumer lag, sampled every 5 minutes: 0 for all four consumers at every
sample but one (Risk, 4,133 events behind 16 minutes in, back to 0 by the
next sample). When traffic stopped, Risk was 580 events behind and the
others 0. Before batching, the same run left them 540,000–770,000 events
behind. Average batch size at 200/s was 3–5 events: the batches grow only
when a consumer falls behind, which is when they help.

Memory over the run: every service ended between 14 and 50 MiB except
Accounts (566 MiB, falling in the second half; Argon2 setup memory). Gateway
and Risk, which were still growing in section 3, ended at 32 and 30 MiB,
flat. PostgreSQL (data in memory) and Redpanda (its log) grew, as expected.
This is still a 30-minute run, not the spec's hour.

## 5. Connection pools and slow queries

Measured 2026-11-13: the spec's tuning step 1, at 250 orders/s, just under
where Accounts' 16 connections were all busy in section 4. The benchmark
overlay now loads `pg_stat_statements`.

**One slow query.** Every posting to the ledger locks its balance rows with
one statement. It joined the accounts on `owner_id IS NOT DISTINCT FROM`,
which the `(owner_id, asset, kind)` index cannot serve, so each posting
read all 3,019 accounts: 5.7 ms on average under load, about half of
Accounts' database time. It now finds each account through the index
(equality for user accounts, `IS NULL` for the shared external ones) and
still locks them in ascending ID order: **1.0 ms**, and Accounts' total
database time per minute of load fell by about 40%.

**Pools are not the limit.** No query waited on a lock, and Accounts'
connections were busy because PostgreSQL was short of CPU: every
statement, even a primary-key update, took around 1 ms. Statement
execution added up to about 2.3 cores of PostgreSQL's 4.5–4.9; the rest is
the cost of about 6,000 statements and 1,600 commits a second. All
containers together used 890–980% CPU of the VM's 1,200% (12 threads on 6
cores). More connections would only queue more work for the same cores.

**Too noisy to see a change in throughput.** Three 60-second runs at
250 orders/s gave 96.7% (before the fix), then 74.9% and 100% (after): the
run-to-run spread on this laptop is wider than anything one query changes.
At this rate the machine itself is the limit. What would still lower the
load is fewer statements per order (spec step 4, combining Risk's writes)
and a machine where PostgreSQL has its own cores (step 3).

## 6. One hour at 100 orders/s (memory)

Measured 2026-11-13 with the batched consumers and the section 5 fix: the
spec's 1-hour memory run, at 100 orders/s (well inside capacity, so the run
measures memory rather than overload), fast-disk overlay, 10% of traces
kept. `docker stats` every 30 s; trend is a least-squares slope over the
second half.

| Rate | OK | Accepted p99 | Fill notification p99 | Trade to settled p99 | Public trade p99 |
|---|---|---|---|---|---|
| 100/s for 1 h | 100.00% | 342 ms | 1,087 ms | 767 ms | 131 ms |

The latencies are higher than in the 60-second sweep at 100/s (67, 79 and
180 ms): this laptop was slower today, as section 4 notes.

| Container | Start | End | Trend (second half) |
|---|---|---|---|
| matching-0..3 | 8–9 MiB | 45–47 MiB | +1.5 to +3.0 MiB/h (flat) |
| gateway | 3 MiB | 17 MiB | +1.1 MiB/h |
| risk | 6 MiB | 20 MiB | −0.2 MiB/h |
| settlement | 6 MiB | 13 MiB | +2.1 MiB/h |
| notifications | 5 MiB | 21 MiB | +0.9 MiB/h |
| market-data | 7 MiB | 17 MiB | +0.2 MiB/h |
| accounts | 6 MiB | 966 MiB | −0.4 MiB/h (see below) |
| redpanda | 243 MiB | 561 MiB | +318 MiB/h: its cache filling toward its fixed `--memory=1G` |
| postgres | 149 MiB | 1.4 GiB | +765 MiB/h: the data, kept in memory by the overlay |
| tempo | 28 MiB | 182 MiB | a 950 MiB spike at 32 min, then back down |

**No service grows.** Every Oxchange service is flat over the second half,
within a few MiB/h. Gateway and Risk, which were still growing at the end
of the 30-minute run in section 3, ended at 17 and 20 MiB (84 and 117 MiB
then, before batching). The two that grow are bounded by design: Redpanda's
memory is fixed at start, and PostgreSQL's data lives in memory only in this
overlay. **Target met.**

**Accounts holds about 1 GB.** It jumps to 1.5 GB within 30 s of the start,
while the load generator signs up and logs in 500 users, then stays flat
(966 MiB after a drop at 30 minutes). It does not grow with orders. A short
test reproduces it: 500 sign-ups take an idle Accounts from 6 MiB to
1.3 GB. Hashing is already limited to 8 Argon2id hashes at once (about
19 MiB each), so the memory is held after the hashes finish, not used by
them. Capping glibc's arenas (`MALLOC_ARENA_MAX=2`) did not help (1.0 GB).
Not yet explained; it is a fixed cost after a burst of logins, not a leak.

## Where the time goes

1. **The disk.** On this machine PostgreSQL's flushes are the first limit,
   as the spec predicts: about 10 orders/s with full durability.
2. **One database transaction per consumed event** (done). Each consumer
   of `matching.events` applied events one at a time per partition, each in
   its own PostgreSQL transaction, and committed the Kafka offset after
   every event; during the catch-up PostgreSQL used 2.1 cores and the
   single-core broker 0.75. Batching (the spec's tuning step 2,
   [section 4](#4-batched-consumer-writes)) lets them keep up with 200
   orders/s.
3. **CPU, through PostgreSQL** (pools and slow queries checked). At
   250–300 orders/s Accounts' 16 connections are all in use, but because
   PostgreSQL is short of CPU, not because there are too few connections;
   the one slow query is fixed ([section 5](#5-connection-pools-and-slow-queries)).
   The whole VM is near its 12 threads at 250/s. Next: fewer statements per
   order (spec step 4) and PostgreSQL on its own cores (step 3).
4. **The shared machine.** Every service, PostgreSQL, the broker, the
   observability stack and the load generator share 12 hardware threads; the
   generator itself fell up to 3.8 s behind its schedule during the stall.

## Not measured

- 5,000 public WebSocket clients: the runs used 20. One machine running the
  clients and the system at once would mostly measure itself.
- Why Accounts keeps about 1 GB after a burst of logins
  ([section 6](#6-one-hour-at-100-orderss-memory)).
- A 3-broker cluster under load (the failure tests use one; these runs use
  the default single broker).
