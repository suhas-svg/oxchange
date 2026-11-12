# Consumer lag

**Fires when:** a consumer is more than 50,000 events behind on a partition for 2 minutes (`consumer_lag > 50000`).
**Severity:** warning. What that consumer does arrives late: order status updates from Risk, settlements, market data, or notifications.

## Confirm

1. The Kafka and database dashboard's Consumer lag panel shows which service, topic and partition is behind, and whether the lag is growing or shrinking.
2. Look at Consumer retries on the Money safety dashboard, and the service's logs for `handler failed; retrying`.

## Likely causes

- **A dependency is slow or down.** Each message is retried until it succeeds, so one failing call stalls the whole partition. Settlement waits on Accounts, and Risk waits on Accounts for releases.
- **The database is slow.** Each event is one transaction. Look at Slow queries, Pool connections in use, and Longest open transaction.
- **A burst bigger than the steady rate.** Lag that rises and then falls on its own is backlog being worked off.
- **The partition is halted.** Lag grows because nothing is consumed. See Event sequence gap or Settlement halted.

## Do

1. Fix the slow dependency. Lag drains once messages succeed again.
2. If the database is the limit, see "Where the time goes" in the spec: pool sizes and slow queries first.
3. Adding copies of a consumer only helps when the lag is spread across many partitions. Each partition is read by one copy at a time.
