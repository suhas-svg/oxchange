# Matcher down

**Fires when:** for 30 s, a matcher cannot be scraped (`up{job="matching"} == 0`) or reports not ready (`matcher_ready == 0`).
**Severity:** critical. Orders for that partition's stocks are accepted by Risk but not matched until the matcher returns. Nothing is lost: commands wait in `orders.commands`.

## Confirm

1. The Matching engine dashboard shows which partition is missing from Matchers ready, and its Input lag.
2. `kubectl -n oxchange get pods -l app.kubernetes.io/name=matching` (or `docker compose ps`), then read the logs of the failing pod.

## What the logs mean

- **`replay complete; matcher is ready`** has not appeared yet: the matcher is still replaying. Readiness waits for replay to finish. Recovery should take at most 10 s for 100,000 commands, and the Last recovery panel shows how long it took. A long replay usually means snapshots are not being written: look for `snapshot` errors and a full disk.
- **`engine halted`:** the engine found a broken invariant and stopped instead of corrupting its book. Kubernetes restarts it, and it recovers from the newest good snapshot. If it halts again on the same offset, that command triggers a bug. Save the snapshot directory, then read the command at that offset with `rpk topic consume orders.commands -p <partition> -o <offset> -n 1`.
- **`AllBrokersDown` or Kafka errors:** Redpanda is unreachable. See Service down.
- **`commands were deleted by retention before a snapshot covered them`:** the snapshot is older than the oldest command still in the log. The matcher cannot rebuild its state from what is left. Restore the snapshot volume, or rebuild the partition deliberately, which loses its resting orders. Either way it is a decision to make, not a restart.
- **`the snapshot does not belong to this log`:** the snapshot is ahead of the topic, for example because the topic was recreated. Don't delete snapshots to make the error go away; find out why the log is shorter.

## Do

- Usually: let Kubernetes restart it and watch it replay. Check that Input lag falls back to about 0.
- Run `audit --traffic-stopped` after any matcher that halted has recovered.
