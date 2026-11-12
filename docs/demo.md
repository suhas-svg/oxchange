# Demo script (3 minutes)

The spec's demo: traffic running, a service killed, recovery, and the auditor
passing. Set up once before recording, then follow the timeline.

## Before recording

```bash
docker compose up -d --build --wait
```

Open three things side by side:

- http://localhost:3000, signed in as a user you registered, with money
  deposited from the page's Deposit dialog.
- Grafana at http://127.0.0.1:3001, dashboard **Oxchange: system overview**,
  time range "Last 5 minutes", refresh 5 s.
- A terminal in the repository.

## Timeline

**0:00 – 0:30 · What it is.** One sentence on the claim: kill any service
during heavy trading and no money or shares are lost or counted twice. Show
the architecture diagram in the README.

**0:30 – 1:00 · Traffic.** Start the load generator (50 orders/s for 3
minutes) and show orders, trades and latency moving in Grafana and the
trading page's live book and tape:

```bash
docker compose run --rm -d loadgen --rate 50 --duration 180
```

**1:00 – 1:30 · Kill the matching engine.** Kill one matcher and start it
again. Point at the "matcher ready" panel and the log line with
`recovery_ms`:

```bash
docker compose kill -s SIGKILL matching-1
```

```bash
docker compose start matching-1
```

```bash
docker compose logs --since 30s matching-1 | grep "matcher is ready"
```

**1:30 – 2:15 · Kill Settlement mid-trade.** Kill Settlement while trades keep
coming; show the "trade to settled" panel climb, then fall back as it
catches up after the restart:

```bash
docker compose kill -s SIGKILL settlement
```

```bash
docker compose start settlement
```

**2:15 – 3:00 · The auditor.** When the load generator has finished, wait
half a minute for the consumers to catch up, then run the auditor. All 11
rules pass:

```bash
docker compose run --rm auditor --traffic-stopped
```

Close on the failure-test table in [failure-tests.md](failure-tests.md): the
same idea, automated, for all 11 failures in the spec.
