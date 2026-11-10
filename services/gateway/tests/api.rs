//! The Gateway's REST API, login checks and rate limits.

mod common;

use common::{env, env_with};
use serde_json::json;

#[tokio::test]
async fn login_then_the_verified_user_reaches_every_service() {
    let e = env().await;
    let r = e
        .request(
            "POST",
            "/v1/auth/login",
            &[],
            r#"{"email":"a@example.com","password":"correct horse battery"}"#,
        )
        .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body["user_id"], e.user.to_string());
    assert_eq!(r.body["token_type"], "Bearer");
    assert!(r.body["access_expires_at"].as_str().unwrap().ends_with('Z'));
    let bearer = format!("Bearer {}", r.body["access_token"].as_str().unwrap());

    let b = e
        .request(
            "GET",
            "/v1/account/balances",
            &[("Authorization", &bearer)],
            "",
        )
        .await;
    assert_eq!(b.status, 200);
    assert_eq!(
        b.body,
        json!({ "balances": [{ "asset": "USD", "available": 150_000, "reserved": 50_000 }] })
    );

    // A client cannot pick the account: its own X-User-Id is replaced, and
    // its Authorization and Cookie headers never reach the service.
    let someone = uuid::Uuid::now_v7().to_string();
    let n = e
        .request(
            "GET",
            "/v1/notifications?after=7&limit=3",
            &[
                ("Authorization", &bearer),
                ("X-User-Id", &someone),
                ("Cookie", "a=b"),
            ],
            "",
        )
        .await;
    assert_eq!(n.status, 200);
    assert_eq!(n.body["x_user_id"], e.user.to_string());
    assert_eq!(n.body["query"], "after=7&limit=3");
    assert_eq!(n.body["authorization"], serde_json::Value::Null);
    assert_eq!(n.body["cookie"], serde_json::Value::Null);
    assert_eq!(n.header("cache-control"), Some("no-store"));

    let t = e.authed("POST", "/v1/ws-ticket", "").await;
    assert_eq!(
        (t.status, &t.body["x_user_id"]),
        (201, &e.user.to_string().into())
    );

    let seen = e.seen();
    assert!(!seen.calls.is_empty());
    assert!(
        seen.calls.iter().all(|(_, u)| *u == e.user.to_string()),
        "{:?}",
        seen.calls
    );
    // Each call carries a W3C trace.
    assert!(
        seen.traceparents
            .iter()
            .all(|t| t.starts_with("00-") && t.len() == 55)
    );
}

#[tokio::test]
async fn missing_bad_expired_and_logged_out_tokens_are_refused() {
    let e = env().await;
    let check = |r: &common::Reply| {
        assert_eq!(r.status, 401, "{:?}", r.body);
        assert_eq!(r.body["error"]["code"], "UNAUTHENTICATED");
        // The body repeats the response's trace ID.
        let trace = r.header("x-trace-id").unwrap();
        assert_eq!(trace.len(), 32);
        assert_eq!(r.body["error"]["trace_id"], trace);
    };
    check(&e.request("GET", "/v1/account/balances", &[], "").await);
    check(
        &e.request(
            "GET",
            "/v1/account/balances",
            &[("Authorization", "Bearer nonsense")],
            "",
        )
        .await,
    );
    // Signed by a different key.
    let (other, _) = auth::TokenSigner::generate_pem().unwrap();
    let forged = auth::TokenSigner::from_pem(&other)
        .unwrap()
        .issue(e.user, gateway::now_secs())
        .token;
    check(
        &e.request(
            "GET",
            "/v1/account/balances",
            &[("Authorization", &format!("Bearer {forged}"))],
            "",
        )
        .await,
    );
    let old = e.signer.issue(e.user, gateway::now_secs() - 3600).token;
    let r = e
        .request(
            "GET",
            "/v1/account/balances",
            &[("Authorization", &format!("Bearer {old}"))],
            "",
        )
        .await;
    check(&r);
    assert!(
        r.body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("expired")
    );

    // Logout revokes both tokens; the access token stops working at once.
    let token = e.token();
    let bearer = format!("Bearer {token}");
    let ok = e
        .request(
            "GET",
            "/v1/account/balances",
            &[("Authorization", &bearer)],
            "",
        )
        .await;
    assert_eq!(ok.status, 200);
    let out = e
        .request(
            "POST",
            "/v1/auth/logout",
            &[("Authorization", &bearer)],
            r#"{"refresh_token":"refresh-1"}"#,
        )
        .await;
    assert_eq!(out.status, 204);
    check(
        &e.request(
            "GET",
            "/v1/account/balances",
            &[("Authorization", &bearer)],
            "",
        )
        .await,
    );
    let logout = e.seen().logouts[0].clone();
    assert_eq!(logout.refresh_token, "refresh-1");
    assert_eq!(logout.user_id, e.user.to_string());
    assert!(!logout.access_token_id.is_empty());
    // Another token of the same user still works.
    assert_eq!(
        e.authed("GET", "/v1/account/balances", "").await.status,
        200
    );

    // Wrong password: the service's answer, unchanged.
    let r = e
        .request(
            "POST",
            "/v1/auth/login",
            &[],
            r#"{"email":"a@example.com","password":"nope"}"#,
        )
        .await;
    check(&r);
}

#[tokio::test]
async fn orders_map_to_risk_and_errors_keep_the_spec_codes() {
    let e = env().await;
    let body = r#"{"client_order_id":"web-1","symbol":"ACME","side":"BUY","type":"LIMIT","time_in_force":"GTC","price_cents":1001,"quantity":120}"#;
    let r = e.authed("POST", "/v1/orders", body).await;
    assert_eq!(r.status, 202, "{:?}", r.body);
    assert_eq!(
        r.body,
        json!({
            "order_id": "0194d5a2-7b3c-7e11-9a44-2f1c6d8e0b11",
            "client_order_id": "web-1",
            "symbol": "ACME",
            "side": "BUY",
            "type": "LIMIT",
            "time_in_force": "GTC",
            "price_cents": 1001,
            "quantity": 120,
            "filled_quantity": 0,
            "canceled_quantity": 0,
            "status": "ACCEPTED",
            "reject_reason": null,
            "created_at": "2026-11-10T14:00:05.123Z",
            "updated_at": "2026-11-10T14:00:05.123Z",
        })
    );
    // The same client_order_id again: the original order, with 200.
    let repeat = body.replace("web-1", "repeat");
    assert_eq!(e.authed("POST", "/v1/orders", &repeat).await.status, 200);
    // Market orders default to GTC here; Risk turns them into IOC.
    let market =
        r#"{"client_order_id":"m1","symbol":"ACME","side":"sell","type":"market","quantity":5}"#;
    assert_eq!(e.authed("POST", "/v1/orders", market).await.status, 202);
    let sent = e.seen().orders.last().unwrap().clone();
    assert_eq!((sent.side, sent.order_type, sent.price_cents), (2, 2, 0));

    let broke = body.replace("ACME", "BROKE");
    let r = e.authed("POST", "/v1/orders", &broke).await;
    assert_eq!(
        (r.status, &r.body["error"]["code"]),
        (422, &"INSUFFICIENT_FUNDS".into())
    );
    assert_eq!(
        r.body["error"]["message"],
        "available balance is too low for this order"
    );

    for (bad, why) in [
        (body.replace("\"BUY\"", "\"HOLD\""), "side"),
        (
            body.replace("\"quantity\":120", "\"quantity\":120,\"user_id\":\"x\""),
            "unknown field",
        ),
        ("{not json".to_owned(), "JSON"),
    ] {
        let r = e.authed("POST", "/v1/orders", &bad).await;
        assert_eq!(
            (r.status, &r.body["error"]["code"]),
            (400, &"VALIDATION_FAILED".into()),
            "{why}"
        );
    }
    let big = format!(r#"{{"client_order_id":"{}"}}"#, "x".repeat(17 * 1024));
    assert_eq!(e.authed("POST", "/v1/orders", &big).await.status, 413);

    let r = e.authed("DELETE", "/v1/orders/not-a-uuid", "").await;
    assert_eq!(
        (r.status, &r.body["error"]["code"]),
        (404, &"NOT_FOUND".into())
    );
    let r = e
        .authed(
            "DELETE",
            "/v1/orders/0194d5a2-7b3c-7e11-9a44-2f1c6d8e0b11",
            "",
        )
        .await;
    assert_eq!(r.status, 404);
    let r = e
        .authed("GET", "/v1/orders?symbol=ACME&open_only=true&limit=10", "")
        .await;
    assert_eq!(
        (r.status, &r.body["next_cursor"]),
        (200, &"next-page".into())
    );
    assert_eq!(
        e.authed("GET", "/v1/orders?limit=101", "").await.status,
        400
    );
}

#[tokio::test]
async fn deposits_need_an_idempotency_key() {
    let e = env().await;
    let bearer = format!("Bearer {}", e.token());
    let body = r#"{"amount_cents":200000}"#;
    let r = e
        .request(
            "POST",
            "/v1/account/deposits",
            &[("Authorization", &bearer)],
            body,
        )
        .await;
    assert_eq!(r.status, 400);
    let r = e
        .request(
            "POST",
            "/v1/account/deposits",
            &[("Authorization", &bearer), ("Idempotency-Key", "dep-1")],
            body,
        )
        .await;
    assert_eq!(r.status, 201);
    assert_eq!(
        r.body,
        json!({ "transaction_id": "txn-1", "available": 200_000 })
    );
    assert_eq!(e.seen().deposits[0].idempotency_key, "dep-1");
    let r = e
        .request(
            "POST",
            "/v1/account/withdrawals",
            &[("Authorization", &bearer), ("Idempotency-Key", "w-1")],
            body,
        )
        .await;
    assert_eq!(
        (r.status, &r.body["error"]["code"]),
        (422, &"INSUFFICIENT_FUNDS".into())
    );
}

#[tokio::test]
async fn order_bursts_past_20_are_limited_per_user() {
    let e = env().await;
    let body = r#"{"client_order_id":"b","symbol":"ACME","side":"BUY","type":"LIMIT","price_cents":1000,"quantity":1}"#;
    let bearer = format!("Bearer {}", e.token());
    let headers = [("Authorization", bearer.as_str())];
    let started = std::time::Instant::now();
    let statuses = futures_util::future::join_all(
        (0..40).map(|_| e.request("POST", "/v1/orders", &headers, body)),
    )
    .await;
    let ok = statuses.iter().filter(|r| r.status == 202).count();
    // 20 at once, plus one more for every 100 ms the burst took.
    let refilled = usize::try_from(started.elapsed().as_millis() / 100).unwrap() + 1;
    assert!(
        (20..=20 + refilled).contains(&ok),
        "{ok} accepted, {refilled} refilled"
    );
    assert!(ok < 40, "some were limited");
    let limited = statuses.iter().find(|r| r.status == 429).unwrap();
    assert_eq!(limited.body["error"]["code"], "RATE_LIMITED");
    assert_eq!(limited.header("retry-after"), Some("1"));
    // Reading balances uses a different bucket.
    assert_eq!(
        e.request(
            "GET",
            "/v1/account/balances",
            &[("Authorization", &bearer)],
            ""
        )
        .await
        .status,
        200
    );
}

#[tokio::test]
async fn registering_is_limited_per_address() {
    let e = env().await;
    let reg = |ip: &'static str| {
        let e = &e;
        async move {
            e.request(
                "POST",
                "/v1/auth/register",
                &[("X-Forwarded-For", ip)],
                r#"{"email":"n@example.com","password":"correct horse battery"}"#,
            )
            .await
        }
    };
    for _ in 0..3 {
        assert_eq!(reg("203.0.113.7").await.status, 201);
    }
    let r = reg("203.0.113.7").await;
    assert_eq!(r.status, 429);
    let wait: u64 = r.header("retry-after").unwrap().parse().unwrap();
    assert!((19..=20).contains(&wait), "{wait}");
    assert_eq!(reg("203.0.113.8").await.status, 201);
    let taken = e
        .request(
            "POST",
            "/v1/auth/register",
            &[("X-Forwarded-For", "203.0.113.9")],
            r#"{"email":"taken@example.com","password":"correct horse battery"}"#,
        )
        .await;
    assert_eq!(
        (taken.status, &taken.body["error"]["code"]),
        (409, &"ALREADY_EXISTS".into())
    );
}

#[tokio::test]
async fn forwarded_for_is_ignored_unless_trusted() {
    let e = env_with(|c| c.trust_forwarded_for = false).await;
    let body = r#"{"email":"n@example.com","password":"correct horse battery"}"#;
    for ip in ["198.51.100.1", "198.51.100.2", "198.51.100.3"] {
        let r = e
            .request(
                "POST",
                "/v1/auth/register",
                &[("X-Forwarded-For", ip)],
                body,
            )
            .await;
        assert_eq!(r.status, 201);
    }
    // All three counted against the real address.
    let r = e
        .request(
            "POST",
            "/v1/auth/register",
            &[("X-Forwarded-For", "198.51.100.4")],
            body,
        )
        .await;
    assert_eq!(r.status, 429);
}

#[tokio::test]
async fn symbols_carry_the_ticker_and_market_routes_go_to_market_data() {
    let e = env().await;
    let tickers = ticker::Store::new(&e.redis_url).unwrap();
    tickers
        .write(
            "ACME",
            ticker::Tick {
                last_price_cents: 1010,
                day_volume: 75,
                seq: 9,
                traded_at_ns: 1_794_319_205_000_000_000,
            },
        )
        .await
        .unwrap();
    let r = e.request("GET", "/v1/symbols", &[], "").await;
    assert_eq!(r.status, 200);
    assert_eq!(
        r.body["symbols"][0],
        json!({
            "symbol": "ACME", "name": "Acme Simulated Holdings", "trading": true,
            "listing_price_cents": 1000, "last_price_cents": 1010, "day_volume": 75,
            "last_trade_at": "2026-11-10T14:00:05Z",
        })
    );
    assert_eq!(
        r.body["symbols"][1]["last_price_cents"],
        serde_json::Value::Null
    );

    let r = e
        .request(
            "GET",
            "/v1/symbols/ACME/trades?before_seq=40&limit=5",
            &[("X-User-Id", "x")],
            "",
        )
        .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body["path"], "/v1/symbols/ACME/trades");
    assert_eq!(r.body["query"], "before_seq=40&limit=5");
    assert_eq!(
        r.body["x_user_id"],
        serde_json::Value::Null,
        "public routes carry no user"
    );
    assert_eq!(
        e.request("GET", "/v1/symbols/ACME/secrets", &[], "")
            .await
            .status,
        404
    );
    assert_eq!(
        e.request("GET", "/v1/symbols/..%2Fadmin/book", &[], "")
            .await
            .status,
        400
    );
}

#[tokio::test]
async fn a_service_that_is_down_or_slow_gives_503() {
    let e = env_with(|c| {
        c.accounts_url = "http://127.0.0.1:1".into();
        c.market_data_url = "http://127.0.0.1:1".into();
    })
    .await;
    let r = e.authed("GET", "/v1/account/balances", "").await;
    assert_eq!(
        (r.status, &r.body["error"]["code"]),
        (503, &"SERVICE_UNAVAILABLE".into())
    );
    let r = e.request("GET", "/v1/symbols/ACME/book", &[], "").await;
    assert_eq!(r.status, 503);
    // Risk answers after 5 s; the Gateway gives up at 2 s.
    let slow = r#"{"client_order_id":"s","symbol":"SLOW","side":"BUY","type":"LIMIT","price_cents":1,"quantity":1}"#;
    let started = std::time::Instant::now();
    let r = e.authed("POST", "/v1/orders", slow).await;
    assert_eq!(r.status, 503);
    assert!(started.elapsed() < std::time::Duration::from_secs(4));
}
