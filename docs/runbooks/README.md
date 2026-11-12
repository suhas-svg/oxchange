# Runbooks

One page per alert in `deploy/helm/oxchange/files/observability/alerts.yml`.
Each says what the alert means, how to confirm it, and what to do.

| Alert | Severity | Runbook |
|---|---|---|
| Ledger imbalance | Critical | [ledger-imbalance.md](ledger-imbalance.md) |
| Event sequence gap | Critical | [event-sequence-gap.md](event-sequence-gap.md) |
| Settlement halted | Critical | [settlement-halted.md](settlement-halted.md) |
| Matcher down | Critical | [matcher-down.md](matcher-down.md) |
| Outbox backing up | Warning | [outbox-backing-up.md](outbox-backing-up.md) |
| Consumer lag | Warning | [consumer-lag.md](consumer-lag.md) |
| Order latency | Warning | [order-latency.md](order-latency.md) |
| Service down | Warning | [service-down.md](service-down.md) |

## Where to look

- **Grafana:** http://localhost:3001 with Compose. With k3d, use the same address, or port-forward with `kubectl -n oxchange port-forward svc/grafana 3001:3000`. The Oxchange folder has four dashboards: System overview, Matching engine, Money safety, and Kafka and database.
- **Prometheus:** http://localhost:9090 with Compose. Open the Alerts page to see which alerts are firing and why.
- **Traces:** in Grafana, open Explore and pick Tempo. Search by trace ID: the `trace_id` in an API error body, or the `X-Trace-Id` response header. From a span you can open its logs.
- **Logs:** in Grafana, open Explore and pick Loki, then query `{service_name="settlement"}`. A log line's `trace_id` links to its trace. Every service also prints JSON logs to stdout, which `docker compose logs <service>` or `kubectl logs` show.
- **Auditor:** `scripts/k8s.sh audit --traffic-stopped` in the cluster, or `docker compose run --rm auditor --traffic-stopped` with Compose. It runs all 11 rules and prints each broken one.

## Rules that hold for every alert

- Never skip an event and never edit a balance by hand. The system stops instead of guessing so that nothing is lost; a fix that skips data turns a delay into lost money.
- Keep the evidence: copy the logs and the auditor output before restarting anything.
- After any fix, run the auditor with `--traffic-stopped` and confirm it passes.
