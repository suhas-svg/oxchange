# Settlement halted

**Fires when:** a trade has waited more than 60 s to settle (`settlement_oldest_pending_seconds > 60`), or a Settlement partition is halted (`consumer_halted{service="settlement"}`).
**Severity:** critical. Buyers and sellers do not get their shares and cash. Their funds stay reserved, so nothing is lost, but nothing moves either.

## Confirm

1. Open the Money safety dashboard and look at the Pending trades and Trade to final balance panels.
2. Read Settlement's logs:
   - `handler failed; retrying` means it keeps trying. Look at the error: usually Accounts is unreachable or slow.
   - `partition halted` together with `Accounts refused to settle <trade>` means Accounts gave a definite no. Reservations are made before trading, so a valid trade should always settle; a refusal means there is a bug.
3. Look at the stuck trade: `SELECT * FROM trades WHERE status = 'PENDING' ORDER BY executed_at LIMIT 5;` in the settlement database.

## Likely causes

- **Accounts is down or overloaded.** Settlement retries with backoff, up to 30 s apart, and recovers by itself once Accounts answers. See the Service down and Order latency runbooks.
- **Accounts refused, for example with INSUFFICIENT_FUNDS on a reservation.** A reservation was released or changed before the trade settled. This is a bug in the reservation flow.
- **The database is stalled:** look for slow queries on the Kafka and database dashboard.

## Do

1. If it is retrying: fix what it depends on (Accounts or PostgreSQL). Pending trades drain by themselves; watch the oldest-pending line fall.
2. If it halted: never skip the trade. Save the logs, find why Accounts refused (`ledger_transactions` for the order's `reserve:` and `release:` keys), fix the cause, then restart Settlement. It retries the same trade.
3. Once it drains, run `audit --traffic-stopped`.
