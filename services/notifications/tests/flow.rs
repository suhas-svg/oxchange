//! Notifications end to end: events in, stored messages, the private
//! WebSocket, and catching up over REST.

mod common;

use std::time::Duration;

use common::{
    ACCOUNTS, MATCHING, SETTLEMENT, accepted, book_changed, cancel_rejected, canceled, deposited,
    env, next, quiet, refused, settled, trade,
};
use notifications::Failure;
use serde_json::Value;
use uuid::Uuid;

fn types(list: &Value) -> Vec<&str> {
    list["notifications"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["type"].as_str().unwrap())
        .collect()
}

fn ids(list: &Value) -> Vec<i64> {
    list["notifications"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["id"].as_i64().unwrap())
        .collect()
}

#[tokio::test]
async fn each_event_becomes_one_message_per_affected_user_and_repeats_add_nothing() {
    let e = env().await;
    let c = e.copy().await;
    let (buyer, seller) = (Uuid::now_v7(), Uuid::now_v7());
    let events: Vec<(&str, Vec<u8>)> = vec![
        (ACCOUNTS, deposited(buyer, 200_000)),
        (MATCHING, accepted("ACME", 1, buyer)),
        (MATCHING, book_changed("ACME", 2)),
        (MATCHING, trade("ACME", 3, buyer, seller, 1000, 25)),
        (SETTLEMENT, settled("ACME-3", buyer, seller)),
        (MATCHING, canceled("ACME", 4, buyer, 5)),
        (MATCHING, cancel_rejected("ACME", 5, seller)),
    ];
    let mut stored = 0;
    for (topic, payload) in &events {
        stored += c.notifier.on_message(topic, payload).await.unwrap();
    }
    assert_eq!(stored, 7);
    // Kafka redelivers all of it.
    for (topic, payload) in &events {
        assert_eq!(c.notifier.on_message(topic, payload).await.unwrap(), 0);
    }
    assert_eq!(e.count("SELECT count(*) FROM notifications").await, 7);

    let (status, mine) = c.request("GET", "/v1/notifications", Some(buyer)).await;
    assert_eq!(status, 200);
    assert_eq!(
        types(&mine),
        ["account_update", "fill", "settlement", "order_update"]
    );
    let fill = &mine["notifications"][1];
    assert_eq!(
        fill["data"],
        serde_json::json!({
            "symbol": "ACME", "trade_id": "ACME-3", "order_id": "buy-3", "side": "BUY",
            "price_cents": 1000, "quantity": 25, "executed_at_ms": common::T0 / 1_000_000,
        })
    );
    assert!(fill["created_at_ms"].as_i64().unwrap() > 0);
    assert_eq!(mine["notifications"][3]["data"]["update"], "CANCELED");

    let (_, theirs) = c.request("GET", "/v1/notifications", Some(seller)).await;
    assert_eq!(types(&theirs), ["fill", "settlement", "order_update"]);
    assert_eq!(theirs["notifications"][0]["data"]["side"], "SELL");
    assert_eq!(
        theirs["notifications"][2]["data"]["update"],
        "CANCEL_REJECTED"
    );
    // Nobody is told who they traded with.
    assert!(!mine.to_string().contains(&seller.to_string()));
    assert!(!theirs.to_string().contains(&buyer.to_string()));
}

/// Spec "Bad messages": Notifications sends these to the DLQ and continues.
#[tokio::test]
async fn messages_that_can_never_be_handled_are_skipped() {
    let e = env().await;
    let c = e.copy().await;
    let user = Uuid::now_v7();
    let skipped = |r: Result<usize, Failure>| matches!(r, Err(Failure::Skip(_)));

    assert!(skipped(c.notifier.on_message(MATCHING, &[0xff; 9]).await));
    assert!(skipped(c.notifier.on_message("orders.commands", &[]).await));
    let bad_user = proto_gen::events::v1::AccountEvent {
        event_id: "x".into(),
        user_id: "nobody".into(),
        event_time_ns: 0,
        body: Some(proto_gen::events::v1::account_event::Body::Deposited(
            Default::default(),
        )),
    };
    assert!(skipped(
        c.notifier
            .on_message(ACCOUNTS, &prost::Message::encode_to_vec(&bad_user))
            .await
    ));
    assert_eq!(e.count("SELECT count(*) FROM notifications").await, 0);

    // The stream keeps moving afterwards.
    assert_eq!(
        c.notifier
            .on_message(ACCOUNTS, &deposited(user, 1))
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn a_private_stream_gets_only_its_own_users_messages() {
    let e = env().await;
    let c = e.copy().await;
    let (buyer, seller, bystander) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    // Stored before anyone connects: not sent live.
    c.notifier
        .on_message(ACCOUNTS, &deposited(buyer, 500))
        .await
        .unwrap();

    let (mut b1, start) = c.open(buyer).await;
    let (mut b2, _) = c.open(buyer).await; // a second tab
    let (mut s, s_start) = c.open(seller).await;
    let (mut x, _) = c.open(bystander).await;
    assert!(start > 0, "ready reports the newest stored ID");
    assert_eq!(s_start, 0);
    assert_eq!(c.hub.clients(), 4);

    c.notifier
        .on_message(MATCHING, &trade("ACME", 1, buyer, seller, 1000, 25))
        .await
        .unwrap();
    for ws in [&mut b1, &mut b2] {
        let m = next(ws).await;
        assert_eq!(
            (&m["type"], &m["data"]["side"]),
            (&"fill".into(), &"BUY".into())
        );
        assert!(m["id"].as_i64().unwrap() > start);
    }
    let m = next(&mut s).await;
    assert_eq!(m["data"]["order_id"], "sell-1");

    // A redelivery wakes the sockets again, but nothing is sent twice.
    c.notifier
        .on_message(MATCHING, &trade("ACME", 1, buyer, seller, 1000, 25))
        .await
        .unwrap();
    c.notifier
        .on_message(SETTLEMENT, &settled("ACME-1", buyer, seller))
        .await
        .unwrap();
    assert_eq!(next(&mut b1).await["type"], "settlement");
    assert_eq!(next(&mut s).await["type"], "settlement");
    quiet(&mut x, Duration::from_millis(300)).await;

    drop(b2);
    drop(x);
    for _ in 0..50 {
        if c.hub.clients() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(c.hub.clients(), 2, "closed sockets leave the hub");
}

#[tokio::test]
async fn a_ticket_works_once_and_only_for_30_seconds() {
    let e = env().await;
    let c = e.copy().await;
    let user = Uuid::now_v7();

    let (status, body) = c.request("POST", "/v1/ws-ticket", Some(user)).await;
    assert_eq!((status, &body["expires_in_s"]), (201, &30.into()));
    let ticket = body["ticket"].as_str().unwrap().to_owned();
    assert_eq!(c.request("POST", "/v1/ws-ticket", None).await.0, 401);

    let (_ws, _) = c.connect(&ticket).await.expect("first use opens");
    assert_eq!(refused(c.connect(&ticket).await), 401, "second use");
    assert_eq!(refused(c.connect("made-up").await), 401);
    assert_eq!(refused(c.connect("").await), 401);

    let late = c.notifier.issue_ticket(user).await.unwrap();
    sqlx::query("UPDATE ws_tickets SET expires_at = now() - interval '1 second'")
        .execute(&e.pool)
        .await
        .unwrap();
    assert_eq!(refused(c.connect(&late).await), 401, "expired");
    // Only hashes are stored, and expired tickets are cleaned up.
    c.notifier.issue_ticket(user).await.unwrap();
    assert_eq!(e.count("SELECT count(*) FROM ws_tickets").await, 1);
    assert_eq!(
        e.count("SELECT count(*) FROM ws_tickets WHERE length(ticket_hash) = 32")
            .await,
        1
    );
}

/// Spec: "The copy holding that user's WebSocket delivers the message."
#[tokio::test]
async fn a_message_stored_by_one_copy_reaches_a_socket_on_another() {
    let e = env().await;
    let (consuming, holding) = (e.copy().await, e.copy().await);
    let (buyer, seller) = (Uuid::now_v7(), Uuid::now_v7());
    let (mut ws, _) = holding.open(buyer).await;

    consuming
        .notifier
        .on_message(MATCHING, &trade("ACME", 1, buyer, seller, 1000, 25))
        .await
        .unwrap();
    let m = next(&mut ws).await;
    assert_eq!(
        (&m["type"], &m["data"]["trade_id"]),
        (&"fill".into(), &"ACME-1".into())
    );
    assert_eq!(consuming.hub.clients(), 0);
}

/// Spec: "Live delivery can be missed during a reconnect, so clients call
/// GET /v1/notifications?after={id} when they reconnect."
#[tokio::test]
async fn a_reconnecting_client_catches_up_over_rest() {
    let e = env().await;
    let c = e.copy().await;
    let (user, other) = (Uuid::now_v7(), Uuid::now_v7());
    let (mut ws, _) = c.open(user).await;
    let mut seen = Vec::new();
    for seq in 1..=3 {
        c.notifier
            .on_message(MATCHING, &canceled("ACME", seq, user, 1))
            .await
            .unwrap();
        seen.push(next(&mut ws).await["id"].as_i64().unwrap());
    }
    drop(ws);
    // Missed while disconnected, with another user's traffic in between.
    for seq in 4..=7 {
        c.notifier
            .on_message(MATCHING, &canceled("ACME", seq, user, 1))
            .await
            .unwrap();
        c.notifier
            .on_message(MATCHING, &canceled("BOLT", seq, other, 1))
            .await
            .unwrap();
    }

    let (mut ws, live_from) = c.open(user).await;
    let last_seen = *seen.last().unwrap();
    let (status, page1) = c
        .request(
            "GET",
            &format!("/v1/notifications?after={last_seen}&limit=3"),
            Some(user),
        )
        .await;
    assert_eq!(status, 200);
    let got1 = ids(&page1);
    assert_eq!(got1.len(), 3);
    assert_eq!(page1["next_after"], got1[2]);
    let (_, page2) = c
        .request(
            "GET",
            &format!("/v1/notifications?after={}&limit=3", got1[2]),
            Some(user),
        )
        .await;
    let got2 = ids(&page2);
    assert_eq!(got2.len(), 1);
    assert_eq!(page2["next_after"], Value::Null);
    assert_eq!(got2[0], live_from, "REST reaches where the stream starts");
    let all: Vec<i64> = seen.iter().chain(&got1).chain(&got2).copied().collect();
    assert!(
        all.windows(2).all(|w| w[0] < w[1]),
        "IDs only go up: {all:?}"
    );

    // Live again from there.
    c.notifier
        .on_message(MATCHING, &canceled("ACME", 8, user, 1))
        .await
        .unwrap();
    assert!(next(&mut ws).await["id"].as_i64().unwrap() > live_from);

    // Without `after`: the newest page, oldest first.
    let (_, newest) = c
        .request("GET", "/v1/notifications?limit=2", Some(user))
        .await;
    assert_eq!(ids(&newest).len(), 2);
    assert_eq!(ids(&newest)[0], live_from);
    // Another user's ID as the cursor shows nothing of theirs.
    let (_, theirs) = c
        .request("GET", "/v1/notifications?after=0&limit=100", Some(other))
        .await;
    assert_eq!(ids(&theirs).len(), 4);
    assert!(ids(&theirs).iter().all(|id| !all.contains(id)));

    assert_eq!(c.request("GET", "/v1/notifications", None).await.0, 401);
    let (status, err) = c
        .request("GET", "/v1/notifications?limit=101", Some(user))
        .await;
    assert_eq!(
        (status, &err["error"]["code"]),
        (400, &"VALIDATION_FAILED".into())
    );
}

/// Events for one user arrive on different partitions and copies at once.
/// The stream must still deliver every one, once, in ID order.
#[tokio::test]
async fn concurrent_events_for_one_user_all_arrive_in_id_order() {
    let e = env().await;
    let (a, b) = (e.copy().await, e.copy().await);
    let user = Uuid::now_v7();
    let (mut ws, _) = a.open(user).await;

    const WRITERS: u64 = 8;
    const EACH: u64 = 25;
    let mut tasks = tokio::task::JoinSet::new();
    for w in 0..WRITERS {
        let notifier = if w % 2 == 0 { &a } else { &b }.notifier.clone();
        tasks.spawn(async move {
            for i in 0..EACH {
                // Half the events also involve a second user, which takes
                // two locks in one transaction.
                let payload = if i % 2 == 0 {
                    canceled("ACME", w * 1000 + i, user, 1)
                } else {
                    trade(
                        "ACME",
                        w * 1000 + i,
                        user,
                        Uuid::from_u128(u128::from(w)),
                        1000,
                        1,
                    )
                };
                notifier.on_message(MATCHING, &payload).await.unwrap();
            }
        });
    }
    let mut got = Vec::new();
    for _ in 0..WRITERS * EACH {
        got.push(next(&mut ws).await["id"].as_i64().unwrap());
    }
    while let Some(r) = tasks.join_next().await {
        r.unwrap();
    }
    assert!(got.windows(2).all(|w| w[0] < w[1]), "out of order: {got:?}");
    quiet(&mut ws, Duration::from_millis(300)).await;
    let stored: Vec<i64> =
        sqlx::query_scalar("SELECT id FROM notifications WHERE user_id = $1 ORDER BY id")
            .bind(user)
            .fetch_all(&e.pool)
            .await
            .unwrap();
    assert_eq!(got, stored);
}

/// Spec "Redis": if it is lost, the system runs in a reduced mode.
#[tokio::test]
async fn without_redis_messages_are_stored_and_still_arrive_late() {
    let e = env().await;
    // This copy's Redis address answers nothing.
    let c = e.copy_with_redis("redis://127.0.0.1:1").await;
    let user = Uuid::now_v7();
    let (mut ws, _) = c.open(user).await;

    let started = std::time::Instant::now();
    assert_eq!(
        c.notifier
            .on_message(ACCOUNTS, &deposited(user, 100))
            .await
            .unwrap(),
        1,
        "storing does not depend on Redis"
    );
    assert_eq!(next(&mut ws).await["type"], "account_update");
    assert!(
        started.elapsed() < notifications::wake::FALLBACK_POLL * 3,
        "arrived by the fallback poll"
    );
    assert!(!c.hub.live.load(std::sync::atomic::Ordering::Relaxed));
    let (status, list) = c.request("GET", "/v1/notifications", Some(user)).await;
    assert_eq!((status, ids(&list).len()), (200, 1));
}
