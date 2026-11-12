# Outbox backing up

**Fires when:** a service has more than 10,000 outbox rows not yet confirmed by Kafka for 1 minute (`outbox_unpublished > 10000`).
**Severity:** warning. Its events (order commands from Risk, `accounts.events`, `settlement.events`) reach Kafka late. Nothing is lost: the rows stay in PostgreSQL until Kafka confirms them. Risk starts refusing new orders with UNAVAILABLE once its own backlog is too large.

## Confirm

1. The Kafka and database dashboard shows Outbox waiting and Outbox to Kafka per service.
2. Read the service's logs:
   - `outbox publish failed; will retry` means Kafka is unreachable or rejecting writes.
   - No `outbox relay took the lock` line in any copy means no relay is running.

## Likely causes

- **Redpanda is down, or lost too many brokers to accept `acks=all` writes.** See Service down.
- **No copy holds the relay lock.** A copy holding the advisory lock is stuck but not dead. `SELECT * FROM pg_locks WHERE locktype = 'advisory';` shows who holds it.
- **More traffic than one relay can publish.** Batches are 500 rows. Look at Outbox to Kafka latency against Orders/s.

## Do

1. Restore Kafka if it is the cause. The relay retries every second and drains the backlog in order.
2. If a stuck copy holds the lock, restart that pod. The lock is released with its connection and another copy takes over.
3. Don't delete outbox rows: they are messages that have not been sent yet.
