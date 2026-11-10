//! The saga's failure paths, repeated events, gaps and the relay lock.

mod common;

use std::time::Duration;

use common::{DOWN, HEALTHY, LOSE_RESERVE_REPLY, env, status};
use consumer::HandlerError;
use proto_gen::accounts::v1 as apb;
use proto_gen::events::v1 as ev;
use proto_gen::risk::v1 as pb;
use risk::RiskError;

use ev::Side::{Buy, Sell};
use pb::OrderStatus as S;

/// Spec failure test "Lost reservation reply": Reserve worked but its reply
/// was dropped. The reservation is voided and the order rejected.
#[tokio::test]
async fn a_lost_reserve_reply_voids_the_reservation_and_rejects_the_order() {
    let e = env().await;
    let u = e.user("ann", 50_000, &[]).await;
    e.set_mode(LOSE_RESERVE_REPLY);
    let r = e.place(&u, "o1", "ACME", Buy, 1000, 10).await;
    assert!(matches!(r, Err(RiskError::Unavailable(_))), "{r:?}");
    e.set_mode(HEALTHY);

    let o = e
        .place(&u, "o1", "ACME", Buy, 1000, 10)
        .await
        .unwrap()
        .order
        .unwrap();
    assert_eq!(
        (status(&o), o.reject_reason.as_str()),
        (S::Rejected, "ACCOUNTS_UNAVAILABLE")
    );
    // The funds Reserve took were given back.
    assert_eq!(e.balance(&u, "USD").await, (50_000, 0));
    assert_eq!(e.pump().await.len(), 0);
    e.audit().await;
}

/// Accounts is down for Reserve and for the void: the order stays
/// PENDING_RISK until the sweeper voids it, and a delayed Reserve that
/// arrives afterwards can no longer lock funds.
#[tokio::test]
async fn the_sweeper_voids_orders_stuck_while_accounts_was_down() {
    let e = env().await;
    let u = e.user("bob", 50_000, &[]).await;
    e.set_mode(DOWN);
    let r = e.place(&u, "o1", "ACME", Buy, 1000, 10).await;
    assert!(matches!(r, Err(RiskError::Unavailable(_))), "{r:?}");
    let o = e
        .place(&u, "o1", "ACME", Buy, 1000, 10)
        .await
        .unwrap()
        .order
        .unwrap();
    assert_eq!(status(&o), S::PendingRisk);

    // Too young for the sweeper, and Accounts is still down.
    assert_eq!(e.risk.sweep(risk::PENDING_TIMEOUT).await.unwrap(), 0);
    assert_eq!(e.risk.sweep(Duration::ZERO).await.unwrap(), 0);
    e.set_mode(HEALTHY);
    assert_eq!(e.risk.sweep(Duration::ZERO).await.unwrap(), 1);
    let o = e.order(&u, &o.order_id).await;
    assert_eq!(
        (status(&o), o.reject_reason.as_str()),
        (S::Rejected, "ACCOUNTS_UNAVAILABLE")
    );

    // The original Reserve, delayed in the network, finally arrives.
    let late = e
        .accounts
        .reserve(&apb::ReserveRequest {
            order_id: o.order_id.clone(),
            user_id: u.clone(),
            asset: "USD".into(),
            amount: 10_000,
            limit_price_cents: 1000,
        })
        .await;
    assert!(
        matches!(late, Err(accounts::AccountsError::ReservationVoided)),
        "{late:?}"
    );
    assert_eq!(e.balance(&u, "USD").await, (50_000, 0));
    e.audit().await;
}

/// Spec failure test "Slow Accounts": 2 s of network delay on every call
/// from Risk to Accounts. Orders fail cleanly (the client is told to retry),
/// the requests that arrive late change nothing for good, and once the
/// delay goes no funds stay locked.
#[tokio::test]
async fn slow_accounts_rejects_orders_cleanly_and_leaves_no_stuck_funds() {
    let e = env().await;
    let ann = e.user("ann", 100_000, &[]).await;
    let bob = e.user("bob", 0, &[("ACME", 50), ("BOLT", 50)]).await;
    // Trading before the slowdown, including an order left resting.
    e.place(&bob, "s1", "ACME", Sell, 1000, 10).await.unwrap();
    e.place(&ann, "b1", "ACME", Buy, 1000, 4).await.unwrap();
    e.pump().await;

    e.set_delay(Duration::from_secs(2));
    let slow = [
        e.place(&ann, "b2", "ACME", Buy, 990, 10).await,
        e.place(&bob, "s2", "BOLT", Sell, 2000, 5).await,
        e.place(&ann, "b3", "BOLT", Buy, 1500, 3).await,
        e.place(&bob, "s3", "ACME", Sell, 1010, 5).await,
    ];
    for r in &slow {
        assert!(matches!(r, Err(RiskError::Unavailable(_))), "{r:?}");
    }
    // While the delay lasts, the sweeper cannot confirm a void either.
    assert_eq!(e.risk.sweep(Duration::ZERO).await.unwrap(), 0);
    assert_eq!(e.pending_risk().await, 4);

    // Let every delayed request land (Reserve first, then the voids), then
    // remove the delay. The sweeper finishes the job.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    e.set_delay(Duration::ZERO);
    for _ in 0..20 {
        e.risk.sweep(Duration::ZERO).await.unwrap();
        if e.pending_risk().await == 0 {
            break;
        }
    }
    assert_eq!(e.pending_risk().await, 0);
    for coid in ["b2", "s2", "b3", "s3"] {
        let (status, reason): (String, Option<String>) =
            sqlx::query_as("SELECT status, reject_reason FROM orders WHERE client_order_id = $1")
                .bind(coid)
                .fetch_one(e.risk.pool())
                .await
                .unwrap();
        assert_eq!(
            (status.as_str(), reason.as_deref()),
            ("REJECTED", Some("ACCOUNTS_UNAVAILABLE")),
            "{coid}"
        );
    }
    // Nothing reached the engine, and only the resting sell holds anything.
    assert!(e.pump().await.is_empty());
    assert_eq!(e.balance(&ann, "USD").await, (100_000 - 4_000, 0));
    assert_eq!(e.balance(&bob, "ACME").await, (40, 6));
    assert_eq!(e.balance(&bob, "BOLT").await, (50, 0));

    // Trading works again.
    e.place(&ann, "b4", "ACME", Buy, 1000, 6).await.unwrap();
    e.pump().await;
    assert_eq!(e.balance(&bob, "ACME").await, (40, 0));
    assert_eq!(e.balance(&bob, "USD").await, (10_000, 0));
    e.audit().await;
}

/// Spec failure test "Duplicate events": every event applied again has no
/// effect, including the releases.
#[tokio::test]
async fn repeated_events_change_nothing_and_a_gap_halts() {
    let e = env().await;
    let buyer = e.user("buyer", 100_000, &[]).await;
    let seller = e.user("seller", 0, &[("ACME", 10)]).await;
    e.place(&seller, "s", "ACME", Sell, 1000, 4).await.unwrap();
    let b = e
        .place(&buyer, "b", "ACME", Buy, 1000, 10)
        .await
        .unwrap()
        .order
        .unwrap();
    e.pump().await;
    e.risk.cancel_order(&buyer, &b.order_id).await.unwrap();
    e.pump().await;
    let before = (
        e.order(&buyer, &b.order_id).await,
        e.balance(&buyer, "USD").await,
    );

    e.reapply_all().await.unwrap();
    let after = (
        e.order(&buyer, &b.order_id).await,
        e.balance(&buyer, "USD").await,
    );
    assert_eq!(before, after);
    assert_eq!(after.1, (100_000 - 4_000, 0));

    // An event two past the last applied seq means data is missing: halt.
    let last = e
        .events
        .lock()
        .unwrap()
        .iter()
        .map(|x| x.seq)
        .max()
        .unwrap();
    let gap = ev::MatchingEvent {
        event_id: format!("ACME-{}", last + 2),
        symbol: "ACME".into(),
        seq: last + 2,
        body: Some(ev::matching_event::Body::BookChanged(
            ev::BookChanged::default(),
        )),
        ..Default::default()
    };
    assert!(matches!(
        e.risk.apply_event(&gap).await,
        Err(HandlerError::Halt(_))
    ));
    e.audit().await;
}

/// A release for a cancel retries while Accounts is down, and applies once
/// it is back (the consume loop retries the same event).
#[tokio::test]
async fn a_cancel_release_retries_until_accounts_is_back() {
    let e = env().await;
    let u = e.user("dan", 10_000, &[]).await;
    let o = e
        .place(&u, "o", "ACME", Buy, 1000, 5)
        .await
        .unwrap()
        .order
        .unwrap();
    e.pump().await;
    e.risk.cancel_order(&u, &o.order_id).await.unwrap();

    // Run the cancel through the engine by hand while Accounts is down.
    e.set_mode(DOWN);
    let mut events = Vec::new();
    {
        let sink = common::Collect::default();
        let mut conn = outbox::try_lock(e.risk.pool(), risk::OUTBOX_LOCK_KEY)
            .await
            .unwrap()
            .unwrap();
        outbox::publish_batch(&mut conn, &sink, 10).await.unwrap();
        for row in sink.0.lock().unwrap().iter() {
            let (sym, cmd) = matching::codec::decode_command(&row.payload).unwrap();
            let mut out = Vec::new();
            e.engine
                .lock()
                .unwrap()
                .process(99, &sym, &cmd, &mut out)
                .unwrap();
            for x in &out {
                use prost::Message;
                events.push(
                    ev::MatchingEvent::decode(matching::codec::encode_event(&sym, x).as_slice())
                        .unwrap(),
                );
            }
        }
    }
    let canceled = events
        .iter()
        .find(|x| matches!(x.body, Some(ev::matching_event::Body::OrderCanceled(_))))
        .unwrap();
    assert!(matches!(
        e.risk.apply_event(canceled).await,
        Err(HandlerError::Retry(_))
    ));
    // Nothing was saved: the seq was not consumed and the order is still open.
    assert_eq!(status(&e.order(&u, &o.order_id).await), S::Open);

    e.set_mode(HEALTHY);
    e.risk.apply_event(canceled).await.unwrap();
    assert_eq!(status(&e.order(&u, &o.order_id).await), S::Canceled);
    assert_eq!(e.balance(&u, "USD").await, (10_000, 0));
    e.audit().await;
}

#[tokio::test]
async fn only_one_relay_holds_the_outbox_lock() {
    let e = env().await;
    let first = outbox::try_lock(e.risk.pool(), risk::OUTBOX_LOCK_KEY)
        .await
        .unwrap();
    assert!(first.is_some());
    assert!(
        outbox::try_lock(e.risk.pool(), risk::OUTBOX_LOCK_KEY)
            .await
            .unwrap()
            .is_none()
    );
    // Closing the holder's connection frees the lock for another copy.
    drop(first.unwrap().detach());
    assert!(
        outbox::try_lock(e.risk.pool(), risk::OUTBOX_LOCK_KEY)
            .await
            .unwrap()
            .is_some()
    );
}
