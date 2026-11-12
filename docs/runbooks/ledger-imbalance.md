# Ledger imbalance

**Fires when:** the auditor's last run broke at least one of its 11 rules (`ledger_imbalance_total > 0`). It is pushed by the auditor CronJob every 5 minutes, or by a manual run.
**Severity:** critical. Balances may be wrong. This should never happen.

## Confirm

1. Open the Money safety dashboard. The Auditor result panel shows broken rules for each mode. The `always` mode is the CronJob; `traffic_stopped` is a manual run.
2. Read the broken rules: `kubectl -n oxchange logs job/<latest auditor job>` (or `scripts/k8s.sh audit`). Each problem names the rule, the account or order, and the amounts.
3. Rules 7, 8 and 11 hold only when traffic is stopped. If one of them failed in a manual run made while orders were still flowing, that is not a finding: stop traffic, then run `audit --traffic-stopped` again.

## Likely causes

- A bug in Accounts' double-entry logic, or a migration that changed balances.
- Someone edited a database by hand.
- A settlement applied twice, or a release applied without its event. Both should be impossible because of the idempotency keys.

## Do

1. Stop placing orders. Scale the Gateway to 0, or stop the load generator. Leave the other services running so they finish their in-flight work.
2. Save the auditor output and the logs from accounts, risk and settlement covering the time since the last passing run.
3. Find the first broken entry: `ledger_transactions` in the accounts database, ordered by `created_at`. Its `idempotency_key` names the operation that wrote it (`reserve:<order>`, `release:<event>`, `settle:<trade>`).
4. Fix the cause in code, and correct the data with a new, balanced ledger transaction, never by editing old rows.
5. Run `audit --traffic-stopped` until it passes, then resume traffic.

## Don't

- Don't update `accounts.balances` directly. Rule 1 (balances equal the ledger) would then hide the real problem.
