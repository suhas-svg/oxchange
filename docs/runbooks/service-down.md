# Service down

**Fires when:** Prometheus has not been able to scrape a target for 1 minute (`up == 0`, any job except the matchers, which have their own alert).
**Severity:** warning. The impact depends on the service:

| Down | Effect |
|---|---|
| Gateway | The public API and dashboard are unavailable |
| Accounts | New orders are refused (UNAVAILABLE); settlements wait |
| Risk | No new orders or cancels |
| Settlement | Trades wait to settle (see Settlement halted) |
| Market Data | Prices and books freeze; they resync on return |
| Notifications | Notifications arrive late; they catch up from Kafka |
| Redpanda | Commands and events wait in the outboxes |
| PostgreSQL | Everything that writes stops |

## Confirm

1. `kubectl -n oxchange get pods` (or `docker compose ps`): is the pod crash-looping, pending, or running but not ready?
2. `kubectl -n oxchange describe pod <pod>` shows the events: OOMKilled, failed probes, a failing image pull.
3. Read the logs of the previous run: `kubectl -n oxchange logs <pod> --previous`.

## Likely causes

- **A dependency is down at startup.** Services refuse to start without their required settings and retry their connections. Look for the first error in the log.
- **Out of memory:** the Memory per service panel shows growth before the kill.
- **A failing migration:** check the `<service>-migrate` Job's logs.
- **Only metrics are broken:** the service works but `/metrics` on port 9464 does not answer. Look for `metrics are off` in its log.

## Do

Fix the cause shown in the logs. Every service is built to restart safely: it continues from its stored progress, and repeats are ignored.
