# Event sequence gap

**Fires when:** a consumer of `matching.events` (Risk, Settlement, Market Data's recorder or Notifications) saw a stock's `seq` jump by more than one (`consumer_seq_gaps_total > 0`). It halted that Kafka partition rather than apply events with some missing.
**Severity:** critical. That partition's stocks stop updating in this service until someone acts. The halt is protecting the data.

## Confirm

1. Find the halt in the service's log: `partition halted; needs investigation`. The error before it says `<SYMBOL>: expected seq N, got M`.
2. The Money safety dashboard's Halted partitions panel shows `consumer_halted` with the topic and partition.
3. Look at the log yourself:
   `rpk topic consume matching.events -p <partition> -o <offset - 20> -n 40`
   (run it in the redpanda pod or container) and check whether seq N exists for that stock.

## Likely causes

- **Retention deleted events** the consumer had not read yet, because the consumer was down longer than the topic keeps data.
- **A matcher wrote events out of order or skipped some.** This is a bug: the matcher's per-stock seq is meant to be strictly sequential.
- **The consumer's saved progress is ahead of the log**, for example a database restored from a newer backup than Kafka.

## Do

1. Don't restart the consumer hoping it clears. On restart it reads the same message and halts again; the counter goes up again, which is expected.
2. If events were lost to retention: the data now needs a decision. Rebuild the consumer's state from a source of truth (for example the matcher's books via GetBook for Market Data), or restore Kafka. Then move `consumer_progress.last_seq` for that stock forward by hand, recording why.
3. If the matcher misbehaved: stop that matcher, save its snapshot directory and the log segment, and treat it as a bug.
4. After the fix, restart the consumer, watch `consumer_halted` drop, then run `audit --traffic-stopped`. Rule 9 checks the seqs in `matching.events`.
