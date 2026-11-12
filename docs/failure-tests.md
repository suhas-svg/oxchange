# Failure tests

`tools/chaos/chaos.py` runs the spec's 11 failure tests, one after another,
each under steady trading from the load generator. A test passes only when all
three hold:

1. Its own check (the "must also show" column below).
2. The auditor passes all 11 rules after traffic stops (`auditor
   --traffic-stopped`, retried for up to a minute while the last events land).
3. Nothing is left half-done: no order still `PENDING_RISK` or `ACCEPTED` after
   Risk's sweeper has had time to act, no unpublished outbox rows in Risk,
   Accounts or Settlement, and no trade still `PENDING` in Settlement.

```bash
python tools/chaos/chaos.py                    # all 11, 10-minute engine crash test
```

```bash
python tools/chaos/chaos.py --quick --only engine_crash,broker_loss
```

Each run starts from an empty stack (PostgreSQL's data lives in memory, so it
cannot be kept across a stop). The results land in `target/chaos/results.md`
and `results.json`.

## Setup

Compose project `oxchaos`, with `docker-compose.yml` plus the overlay
[tools/chaos/compose.chaos.yml](../tools/chaos/compose.chaos.yml):

- **Toxiproxy** between Risk and Accounts, for the lost-reply and slow-Accounts
  tests.
- **Three Redpanda brokers.** Every topic, including the consumer-offset topic,
  has 3 replicas and `min.insync.replicas=2`; producers use `acks=all`.
- **Crash points.** The `fail` crate's named points are compiled into the image
  (feature `failpoints`) and do nothing until the `FAILPOINTS` variable turns
  one on. `return(exit)` ends the process on the spot, as a crash would.
- **Snapshots every 500 commands** instead of 100,000, so every matcher restart
  recovers from a snapshot plus a replay.
- **PostgreSQL in memory, with `synchronous_commit=off`.** This is a concession
  to the test machine (see below). No test crashes PostgreSQL or the machine,
  so neither setting changes anything the tests check: commits stay atomic and
  visible to every reader.

## How each failure is injected

| Scenario | Injected failure | Must also show |
|---|---|---|
| Engine crash | SIGKILL one of the four matchers every 30 s (10 minutes; 2 with `--quick`), rotating | Each recovery under 10 s, from the matcher's own "ready" log line |
| Crash during replay | Stop a matcher for 25 s so commands pile up, then start it with crash point `matching::during_replay`, which ends the process on the first replayed command (after the snapshot is loaded, before it is ready). A timed kill cannot hit this: a replay of a few hundred commands takes 10–50 ms | It crashed mid-replay, and the next start recovers |
| Corrupted snapshot | Stop a matcher, overwrite 14 bytes inside its newest snapshot, start it | It logs the damaged snapshot as unusable and recovers from an older one |
| Exact-moment Risk crash | Crash point `risk::after_reserve`: Risk exits right after Accounts confirms the reservation, before saving `ACCEPTED` | Risk's sweeper finishes the stuck order and releases the funds |
| Exact-moment settlement crash | Crash point `settlement::after_settle_trade`: Settlement exits after Accounts settles a trade, before marking it `SETTLED` | The retried `SettleTrade` changes balances only once |
| Lost reservation reply | Toxiproxy holds Accounts' replies to Risk past Risk's timeout for 8 s | Risk voids those reservations and rejects the orders |
| Slow Accounts | Toxiproxy adds 2 s of latency for 20 s | Orders are rejected cleanly (no client timeouts, no server errors) and no funds stay reserved |
| Duplicate events | `e2e republish` copies 1% of the messages on all four event and command topics back onto the same partition, with the same key and headers | No duplicate effects (auditor) |
| Broker loss | SIGKILL one of the three brokers for 40 s under traffic | `e2e verify` finds every acknowledged command answered in `matching.events` |
| Redis wipe | `FLUSHALL`, then delete the Redis container | Market orders get 503 while Redis is gone (reduced mode), and succeed again after it is back |
| Everything restarts | SIGKILL all seven services and the four matchers at once, then start them | Every service is healthy again and the books balance, with no manual step |

## Results

Full run on 2026-11-12, all 11 in one pass on the final code:

Rate 25 orders/s, 100 users, seed 42. PostgreSQL synchronous_commit=off (see tools/chaos/compose.chaos.yml).

| Scenario | Result | Auditor | Orders ok | Must also show |
|---|---|---|---|---|
| Engine crash | PASS | pass | 100.0% of 15500 | 20 kills, worst recovery 326 ms |
| Crash during replay | PASS | pass | 100.0% of 2250 | crashed mid-replay: True; second recovery 157 ms |
| Corrupted snapshot | PASS | pass | 100.0% of 2500 | skipped the damaged snapshot, recovered from `snapshot-p001-00000000000000007000.snap` |
| Exact-moment Risk crash | PASS | pass | 84.1% of 2250 | sweeper rejected 116 stale orders |
| Exact-moment settlement crash | PASS | pass | 100.0% of 2250 | crashed after SettleTrade; one ledger transaction per trade |
| Lost reservation reply | PASS | pass | 92.5% of 2000 | 149 reservations voided |
| Slow Accounts | PASS | pass | 81.1% of 2000 | 0 client timeouts; rejects {'limit_ioc_cross SERVICE_UNAVAILABLE': 45, 'limit_rest SERVICE_UNAVAILABLE': 333} |
| Duplicate events | PASS | pass | 100.0% of 2250 | 51 messages republished |
| Broker loss | PASS | pass | 100.0% of 2500 | matching.events verified: True |
| Redis wipe | PASS | pass | 100.0% of 2750 | market order while down: (503, 'SERVICE_UNAVAILABLE'); back after 4.1 s |
| Everything restarts | PASS | pass | 57.5% of 2750 | all services back in 30.6 s |

"Orders ok" below 100% is the injected failure doing its job: orders
rejected with 503 while Accounts is slow or Risk is down, never a timeout.
Engine crash recovery is the matcher's own restart-to-ready time; Docker
adds 5–15 s on this machine to start a container.

## The test machine

The tests ran on a Windows 11 laptop (Intel i7-8750H, 6 cores / 12 threads,
16 GB, Kingston SNVS250G NVMe without DRAM cache), with every service, three
brokers, PostgreSQL, Redis and the observability stack inside Docker Desktop's
WSL 2 VM (12 CPUs, 8 GB).

Two properties of that VM's virtual disk shaped the setup:

- It completes about 22 synchronous flushes per second (`pg_test_fsync`:
  45 ms per `fdatasync`). With full durability, every service that commits to
  PostgreSQL shares that budget, which capped the whole stack near 20 orders/s.
- It sometimes stops completing writes for 10 to 18 s, even with every
  container stopped (the VM's I/O pressure stays at 50–70% "full" with no
  load). During one of those pauses, any PostgreSQL backend that has to grow a
  table file waits (`IO DataFileExtend`), and every request behind it times
  out. Run on disk, this showed up as random 13-second outages in whichever
  scenario happened to be running.

Neither has anything to do with the code under test, and both disappear with
PostgreSQL in memory. Set `CHAOS_SYNC_COMMIT=on` to keep full commit
durability.
