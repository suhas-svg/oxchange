# Order latency

**Fires when:** the end-to-end order p99 is above 100 ms for 5 minutes. End-to-end means from Risk saving the order to the matcher's answer reaching Risk (`order_end_to_end_seconds`).
**Severity:** warning. Orders still work, but slower than the target.

## Confirm

1. The System overview dashboard shows the End-to-end order latency quantiles, and Public API latency by route for the Gateway's view.
2. Find a slow order's trace. Tempo always keeps traces slower than 250 ms. In Grafana Explore, pick Tempo and search with `duration > 100ms`. The longest span shows where the time went:

   | Slow span | Look at |
   |---|---|
   | `PlaceOrder` in Risk | Accounts `Reserve` and the three PostgreSQL commits |
   | The gap before `match command` | outbox delay (Outbox to Kafka) or matcher Input lag |
   | `consume matching.events` in Risk | Risk's consumer lag |

## Likely causes

PostgreSQL commits are the expected bottleneck. Each order needs at least three: save, reserve and accept. On this laptop, Docker Desktop's disk sometimes stalls for seconds on fsync.

## Do

Work through the spec's list ("Where the time goes") in order: connection pool sizes and slow queries, then batching outbox and consumer writes, then faster storage for PostgreSQL. Record what changed and the new numbers in `docs/benchmarks/`.
