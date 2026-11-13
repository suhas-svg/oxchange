//! Settlement through Risk, the real matching engine and Accounts, checked
//! by the auditor.

mod common;

use common::{DOWN, HEALTHY, LOSE_REPLY, env, status, trade_of};
use consumer::HandlerError;
use proto_gen::events::v1 as ev;
use proto_gen::risk::v1 as pb;

use ev::Side::{Buy, Sell};
use pb::OrderStatus as S;

/// The spec's example: 100 for sale at $10.00, a buy of 120 at $10.01.
#[tokio::test]
async fn a_trade_settles_once_and_every_rule_holds() {
    let e = env().await;
    let buyer = e.user("buyer", 200_000, &[]).await;
    let seller = e.user("seller", 0, &[("ACME", 100)]).await;
    e.place(&seller, "s", "ACME", Sell, 1000, 100)
        .await
        .unwrap();
    e.pump().await;
    let buy = e
        .place(&buyer, "b", "ACME", Buy, 1001, 120)
        .await
        .unwrap()
        .order
        .unwrap();
    let events = e.pump().await;
    let t = events.iter().find_map(trade_of).expect("a trade").clone();

    assert_eq!(e.trade_status(&t.trade_id).await, "SETTLED");
    // $1.00 of price improvement back; 20 × $10.01 still held for the rest.
    assert_eq!(e.balance(&buyer, "USD").await, (79_980, 20_020));
    assert_eq!(e.balance(&buyer, "ACME").await, (100, 0));
    assert_eq!(e.balance(&seller, "USD").await, (100_000, 0));
    assert_eq!(e.balance(&seller, "ACME").await, (0, 0));

    let msgs = e.settled_messages().await;
    assert_eq!(msgs.len(), 1);
    let m = &msgs[0];
    assert_eq!(
        (
            m.trade_id.as_str(),
            m.symbol.as_str(),
            m.price_cents,
            m.quantity
        ),
        (t.trade_id.as_str(), "ACME", 1000, 100)
    );
    assert_eq!((&m.buyer_id, &m.seller_id), (&buyer, &seller));
    assert!(m.settled_at_ns > 0);

    e.risk.cancel_order(&buyer, &buy.order_id).await.unwrap();
    e.pump().await;
    assert_eq!(e.balance(&buyer, "USD").await, (100_000, 0));
    assert_eq!(e.settlement.pending().await.unwrap(), 0);
    e.audit().await;
}

#[tokio::test]
async fn repeated_events_change_nothing_and_a_gap_halts() {
    let e = env().await;
    let buyer = e.user("buyer", 100_000, &[]).await;
    let seller = e.user("seller", 0, &[("ACME", 50)]).await;
    e.place(&seller, "s", "ACME", Sell, 1000, 50).await.unwrap();
    e.place(&buyer, "b1", "ACME", Buy, 1000, 20).await.unwrap();
    e.place(&buyer, "b2", "ACME", Buy, 1000, 30).await.unwrap();
    e.pump().await;
    assert_eq!(e.settle_transactions().await, 2);
    let before = (
        e.balance(&buyer, "USD").await,
        e.balance(&seller, "USD").await,
    );
    assert_eq!(e.settled_messages().await.len(), 2);

    // Kafka redelivers the whole topic.
    for ev in e.events.lock().unwrap().clone() {
        e.redeliver(ev);
    }
    e.deliver().await.unwrap();
    assert_eq!(e.settle_transactions().await, 2);
    assert_eq!(
        (
            e.balance(&buyer, "USD").await,
            e.balance(&seller, "USD").await
        ),
        before
    );
    assert!(
        e.settled_messages().await.is_empty(),
        "no second TradeSettled"
    );
    e.audit().await;

    // An event from beyond the next seq means some are missing.
    let mut far = e.events.lock().unwrap().last().unwrap().clone();
    far.seq += 2;
    e.redeliver(far);
    assert!(matches!(e.deliver().await, Err(HandlerError::Halt(_))));
}

/// The consume loop hands Settlement every waiting message at once. A
/// batch applies whole or not at all, and ends where single events would.
#[tokio::test]
async fn a_batch_applies_whole_or_not_at_all() {
    let e = env().await;
    let buyer = e.user("buyer", 100_000, &[]).await;
    let seller = e.user("seller", 0, &[("ACME", 50)]).await;
    e.place(&seller, "s", "ACME", Sell, 1000, 50).await.unwrap();
    e.place(&buyer, "b1", "ACME", Buy, 1000, 20).await.unwrap();
    e.place(&buyer, "b2", "ACME", Buy, 1000, 30).await.unwrap();
    let events = e.run_risk().await;
    assert_eq!(events.iter().filter_map(trade_of).count(), 2);

    // A gap at the end fails the whole batch: no trade stored or settled.
    let mut far = events.last().unwrap().clone();
    far.seq += 2;
    e.redeliver(far);
    assert!(matches!(
        e.deliver_batch().await,
        Err(HandlerError::Halt(_))
    ));
    assert_eq!(e.settle_transactions().await, 0);
    assert_eq!(e.settlement.pending().await.unwrap(), 0);

    // Without it, the batch settles both trades.
    e.queue_clear_last();
    e.deliver_batch().await.unwrap();
    assert_eq!(e.settle_transactions().await, 2);
    assert_eq!(e.settled_messages().await.len(), 2);
    assert_eq!(e.settlement.pending().await.unwrap(), 0);

    // The same batch again, with each event twice, changes nothing.
    for ev in e.events.lock().unwrap().clone() {
        e.redeliver(ev.clone());
        e.redeliver(ev);
    }
    e.deliver_batch().await.unwrap();
    assert_eq!(e.settle_transactions().await, 2);
    assert!(e.settled_messages().await.is_empty());
    e.audit().await;
}

#[tokio::test]
async fn settlement_waits_out_an_accounts_outage_and_catches_up() {
    let e = env().await;
    let buyer = e.user("buyer", 100_000, &[]).await;
    let seller = e.user("seller", 0, &[("BOLT", 10)]).await;
    e.place(&seller, "s", "BOLT", Sell, 2500, 10).await.unwrap();
    e.place(&buyer, "b", "BOLT", Buy, 2500, 10).await.unwrap();

    e.set_mode(DOWN);
    let t = e
        .run_risk()
        .await
        .iter()
        .find_map(trade_of)
        .unwrap()
        .clone();
    assert!(matches!(e.deliver().await, Err(HandlerError::Retry(_))));
    // Risk already shows the fill; the money waits (spec: "Fills appear,
    // balances lag").
    let filled = e.risk.get_order(&buyer, &t.buy_order_id).await.unwrap();
    assert_eq!(status(&filled.order.unwrap()), S::Filled);
    assert_eq!(e.balance(&buyer, "USD").await, (75_000, 25_000));
    assert_eq!(e.trade_status(&t.trade_id).await, "PENDING");
    assert_eq!(e.settlement.pending().await.unwrap(), 1);
    // In flight, the always-true rules still hold.
    let r = e.auditor(auditor::When::Always).await;
    assert!(r.passed(), "{:#?}", r.problems);

    e.set_mode(HEALTHY);
    e.deliver().await.unwrap();
    assert_eq!(e.balance(&buyer, "USD").await, (75_000, 0));
    assert_eq!(e.balance(&buyer, "BOLT").await, (10, 0));
    assert_eq!(e.balance(&seller, "USD").await, (25_000, 0));
    e.audit().await;
}

#[tokio::test]
async fn a_lost_reply_is_retried_and_the_trade_settles_once() {
    let e = env().await;
    let buyer = e.user("buyer", 100_000, &[]).await;
    let seller = e.user("seller", 0, &[("CRUX", 10)]).await;
    e.place(&seller, "s", "CRUX", Sell, 4200, 10).await.unwrap();
    e.place(&buyer, "b", "CRUX", Buy, 4300, 10).await.unwrap();

    e.set_mode(LOSE_REPLY);
    e.run_risk().await;
    assert!(matches!(e.deliver().await, Err(HandlerError::Retry(_))));
    // Accounts did settle; Settlement just does not know yet.
    assert_eq!(e.settle_transactions().await, 1);
    assert_eq!(e.balance(&buyer, "USD").await, (58_000, 0));

    e.set_mode(HEALTHY);
    e.deliver().await.unwrap();
    assert_eq!(e.settle_transactions().await, 1);
    assert_eq!(e.balance(&buyer, "USD").await, (58_000, 0));
    assert_eq!(e.settled_messages().await.len(), 1);
    e.audit().await;
}

#[tokio::test]
async fn a_trade_accounts_refuses_halts_instead_of_being_skipped() {
    let e = env().await;
    // A trade between two users who never reserved anything: a bug upstream.
    let bogus = ev::MatchingEvent {
        event_id: "ACME-1".into(),
        symbol: "ACME".into(),
        seq: 1,
        event_time_ns: 1_700_000_000_000_000_000,
        command_id: "c".into(),
        body: Some(ev::matching_event::Body::Trade(ev::TradeExecuted {
            trade_id: "ACME-1".into(),
            price_cents: 1000,
            quantity: 5,
            buy_order_id: uuid::Uuid::now_v7().to_string(),
            sell_order_id: uuid::Uuid::now_v7().to_string(),
            buyer_id: uuid::Uuid::now_v7().to_string(),
            seller_id: uuid::Uuid::now_v7().to_string(),
            aggressor_side: Buy as i32,
        })),
    };
    e.redeliver(bogus);
    let r = e.deliver().await;
    assert!(matches!(r, Err(HandlerError::Halt(_))), "{r:?}");
    // Kept, not skipped: it is still waiting after the halt.
    assert_eq!(e.trade_status("ACME-1").await, "PENDING");
    assert_eq!(e.settle_transactions().await, 0);
}

/// A tiny fixed-seed generator, so a failure always replays the same way.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n
    }
}

/// Spec, failure tests: "Each test runs under steady trading. After
/// traffic stops, the auditor must pass all 11 rules."
#[tokio::test]
async fn steady_trading_through_outages_and_duplicates_leaves_the_books_balanced() {
    let e = env().await;
    let stocks = [("ACME", 1000), ("GRID", 6400), ("IONX", 780)];
    let mut users = Vec::new();
    for i in 0..8 {
        let shares: Vec<(&str, i64)> = stocks.iter().map(|(s, _)| (*s, 500)).collect();
        users.push(e.user(&format!("u{i}"), 5_000_000, &shares).await);
    }
    let mut rng = Lcg(42);
    // Its own generator, so duplicates do not change the workload.
    let mut dup_rng = Lcg(7);
    let mut duplicates = 0;
    let mut open: Vec<(String, String)> = Vec::new();
    for step in 0..240u64 {
        // Accounts trouble for Settlement now and then.
        e.set_mode(match step {
            60..=89 => DOWN,
            150..=169 => LOSE_REPLY,
            _ => HEALTHY,
        });
        let u = &users[rng.next(users.len() as u64) as usize];
        if rng.next(10) < 3 && !open.is_empty() {
            let (owner, id) = open.swap_remove(rng.next(open.len() as u64) as usize);
            // It may have filled already; that cancel is simply rejected.
            e.risk.cancel_order(&owner, &id).await.unwrap();
        } else {
            let (sym, mid) = stocks[rng.next(3) as usize];
            let side = if rng.next(2) == 0 { Buy } else { Sell };
            // Prices near the market, so about half the orders cross.
            let price = mid - 20 + rng.next(41) as i64;
            let qty = 1 + rng.next(40) as i64;
            if let Ok(r) = e.place(u, &format!("o{step}"), sym, side, price, qty).await {
                open.push((u.clone(), r.order.unwrap().order_id));
            }
        }
        if step % 8 == 0 {
            let before = e.events.lock().unwrap().len();
            e.run_risk().await;
            // Spec failure test "Duplicate events": republish about 1% of
            // messages, late and out of order, to both consumers.
            let fresh: Vec<_> = e.events.lock().unwrap()[before..].to_vec();
            for ev in fresh {
                if dup_rng.next(100) == 0 {
                    e.risk.apply_event(&ev).await.unwrap();
                    e.redeliver(ev);
                    duplicates += 1;
                }
            }
            // Errors are expected during the outages; the queue keeps them.
            let _ = e.deliver().await;
            let r = e.auditor(auditor::When::Always).await;
            assert!(r.passed(), "step {step}: {:#?}", r.problems);
        }
    }

    // Traffic stops: cancel what is left and let everything drain.
    e.set_mode(HEALTHY);
    for (owner, id) in open {
        e.risk.cancel_order(&owner, &id).await.unwrap();
    }
    e.pump().await;
    let trades = e
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|ev| trade_of(ev).is_some())
        .count();
    assert!(
        trades > 30,
        "only {trades} trades; the workload is too quiet"
    );
    assert!(duplicates > 0, "no events were republished");
    assert_eq!(e.settle_transactions().await, trades as i64);
    assert_eq!(
        e.settled_messages().await.len(),
        trades,
        "one TradeSettled per trade"
    );
    e.audit().await;
    for u in &users {
        for asset in ["USD", "ACME", "GRID", "IONX"] {
            assert_eq!(
                e.balance(u, asset).await.1,
                0,
                "{u} still has {asset} reserved"
            );
        }
    }
}

/// The auditor is only worth something if it fails when the books are wrong.
#[tokio::test]
async fn the_auditor_catches_planted_bugs() {
    let e = env().await;
    let buyer = e.user("buyer", 100_000, &[]).await;
    let seller = e.user("seller", 0, &[("ACME", 100)]).await;
    e.place(&seller, "s", "ACME", Sell, 1000, 60).await.unwrap();
    let rest = e
        .place(&buyer, "b", "ACME", Buy, 1000, 80)
        .await
        .unwrap()
        .order
        .unwrap();
    e.pump().await;
    e.audit().await;
    let fails = |rule: &'static str| {
        move |r: &auditor::Report| r.problems.iter().any(|p| p.starts_with(rule))
    };
    let stopped = auditor::When::TrafficStopped;

    // Rule 9: an event lost from the log.
    {
        let gone = e.events.lock().unwrap().remove(1);
        let r = e.auditor(stopped).await;
        assert!(fails("rule 9")(&r), "{:#?}", r.problems);
        e.events.lock().unwrap().insert(1, gone);
    }

    // Rule 8: Risk lost track of part of a fill.
    sqlx::query("UPDATE orders SET filled_quantity = filled_quantity - 1 WHERE order_id = $1")
        .bind(uuid::Uuid::parse_str(&rest.order_id).unwrap())
        .execute(e.risk.pool())
        .await
        .unwrap();
    let r = e.auditor(stopped).await;
    assert!(fails("rule 8")(&r), "{:#?}", r.problems);
    sqlx::query("UPDATE orders SET filled_quantity = filled_quantity + 1 WHERE order_id = $1")
        .bind(uuid::Uuid::parse_str(&rest.order_id).unwrap())
        .execute(e.risk.pool())
        .await
        .unwrap();

    // Rule 6: an order over-filled (the CHECK has to go first).
    sqlx::query("ALTER TABLE orders DROP CONSTRAINT orders_check")
        .execute(e.risk.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE orders SET canceled_quantity = quantity WHERE order_id = $1")
        .bind(uuid::Uuid::parse_str(&rest.order_id).unwrap())
        .execute(e.risk.pool())
        .await
        .unwrap();
    let r = e.auditor(auditor::When::Always).await;
    assert!(fails("rule 6")(&r), "{:#?}", r.problems);
    sqlx::query("UPDATE orders SET canceled_quantity = 0 WHERE order_id = $1")
        .bind(uuid::Uuid::parse_str(&rest.order_id).unwrap())
        .execute(e.risk.pool())
        .await
        .unwrap();

    // Rule 11: Risk says the order is done, but Accounts still holds funds.
    sqlx::query("UPDATE orders SET status = 'CANCELED' WHERE order_id = $1")
        .bind(uuid::Uuid::parse_str(&rest.order_id).unwrap())
        .execute(e.risk.pool())
        .await
        .unwrap();
    let r = e.auditor(stopped).await;
    assert!(fails("rule 11")(&r), "{:#?}", r.problems);
    sqlx::query("UPDATE orders SET status = 'PARTIALLY_FILLED' WHERE order_id = $1")
        .bind(uuid::Uuid::parse_str(&rest.order_id).unwrap())
        .execute(e.risk.pool())
        .await
        .unwrap();

    // Rule 10: shares appear from nowhere, in a transaction that balances.
    sqlx::query(
        "WITH t AS (INSERT INTO ledger_transactions (id, idempotency_key, kind, request_hash)
                    VALUES (gen_random_uuid(), 'settle:planted', 'settle', '\\x00') RETURNING id)
         INSERT INTO ledger_entries (transaction_id, account_id, amount)
         SELECT t.id, a.id, x.amount FROM t,
           (VALUES ('available', 7), ('external', -7)) AS x(kind, amount)
           JOIN ledger_accounts a ON a.kind = x.kind AND a.asset = 'ACME'
             AND (a.owner_id = $1 OR (a.kind = 'external' AND a.owner_id IS NULL))",
    )
    .bind(uuid::Uuid::parse_str(&buyer).unwrap())
    .execute(e.accounts.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE ledger_accounts a SET balance = balance + s.total
         FROM (SELECT e.account_id, SUM(e.amount) AS total FROM ledger_entries e
               JOIN ledger_transactions t ON t.id = e.transaction_id
               WHERE t.idempotency_key = 'settle:planted' GROUP BY e.account_id) s
         WHERE a.id = s.account_id",
    )
    .execute(e.accounts.pool())
    .await
    .unwrap();
    let r = e.auditor(auditor::When::Always).await;
    assert!(fails("rule 10")(&r), "{:#?}", r.problems);

    // Rule 7: Settlement forgot a trade (and the planted ledger transaction
    // matches no trade either).
    sqlx::query("DELETE FROM trades")
        .execute(e.settlement.pool())
        .await
        .unwrap();
    let r = e.auditor(stopped).await;
    assert!(
        r.problems
            .iter()
            .any(|p| p.contains("has no settlement record")),
        "{:#?}",
        r.problems
    );
    assert!(
        r.problems.iter().any(|p| p.contains("planted")),
        "{:#?}",
        r.problems
    );
}
