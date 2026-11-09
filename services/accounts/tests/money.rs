//! Balances, reservations and settlement against a real PostgreSQL.

mod common;

use accounts::AccountsError;
use common::{db, order};
use proto_gen::accounts::v1 as pb;

fn reserve_buy(order_id: &str, user: &str, qty: i64, limit: i64) -> pb::ReserveRequest {
    pb::ReserveRequest {
        order_id: order_id.into(),
        user_id: user.into(),
        asset: "USD".into(),
        amount: qty * limit,
        limit_price_cents: limit,
    }
}

fn reserve_sell(order_id: &str, user: &str, symbol: &str, qty: i64) -> pb::ReserveRequest {
    pb::ReserveRequest {
        order_id: order_id.into(),
        user_id: user.into(),
        asset: symbol.into(),
        amount: qty,
        limit_price_cents: 0,
    }
}

fn release(order_id: &str, key: &str, amount: i64) -> pb::ReleaseRequest {
    pb::ReleaseRequest {
        order_id: order_id.into(),
        idempotency_key: key.into(),
        amount,
        void_if_missing: false,
    }
}

fn trade(
    id: &str,
    (price, qty): (i64, i64),
    (buy, sell): (&str, &str),
    (buyer, seller): (&str, &str),
) -> pb::SettleTradeRequest {
    pb::SettleTradeRequest {
        trade_id: id.into(),
        symbol: "ACME".into(),
        price_cents: price,
        quantity: qty,
        buy_order_id: buy.into(),
        sell_order_id: sell.into(),
        buyer_id: buyer.into(),
        seller_id: seller.into(),
    }
}

#[tokio::test]
async fn deposits_and_withdrawals_balance_against_the_external_account() {
    let d = db().await;
    let a = &d.accounts;
    let u = d.user("ann").await;
    let dep = a.deposit(&u, "k1", "", 10_000).await.unwrap();
    assert_eq!(dep.available_cents, 10_000);
    let w = a.withdraw(&u, "k2", "USD", 3_000).await.unwrap();
    assert_eq!(w.available_cents, 7_000);
    assert!(matches!(
        a.withdraw(&u, "k3", "USD", 7_001).await,
        Err(AccountsError::InsufficientFunds)
    ));
    // Shares enter the same way (not in the spec; see accounts.proto).
    a.deposit(&u, "k4", "ACME", 50).await.unwrap();
    assert_eq!(d.balance(&u, "USD").await, (7_000, 0));
    assert_eq!(d.balance(&u, "ACME").await, (50, 0));

    for bad in [0, -1, accounts::MAX_TRANSFER_UNITS + 1] {
        assert!(matches!(
            a.deposit(&u, "k5", "USD", bad).await,
            Err(AccountsError::Validation(_))
        ));
    }
    assert!(matches!(
        a.deposit(&u, "k6", "acme", 1).await,
        Err(AccountsError::Validation(_))
    ));
    assert!(matches!(
        a.deposit(&uuid::Uuid::now_v7().to_string(), "k7", "USD", 1)
            .await,
        Err(AccountsError::NotFound("user"))
    ));

    // Each deposit and withdrawal also queued an accounts.events message.
    let (n,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM outbox WHERE topic = 'accounts.events'")
            .fetch_one(a.pool())
            .await
            .unwrap();
    assert_eq!(n, 3);
    d.audit().await;
}

#[tokio::test]
async fn a_retry_returns_the_first_result_and_a_reused_key_with_new_data_is_refused() {
    let d = db().await;
    let a = &d.accounts;
    let u = d.user("bob").await;
    let first = a.deposit(&u, "same", "USD", 500).await.unwrap();
    a.deposit(&u, "other", "USD", 100).await.unwrap();
    let again = a.deposit(&u, "same", "USD", 500).await.unwrap();
    // The first response, including the balance as it was then.
    assert_eq!(again, first);
    assert_eq!(d.balance(&u, "USD").await, (600, 0));
    assert!(matches!(
        a.deposit(&u, "same", "USD", 501).await,
        Err(AccountsError::IdempotencyConflict)
    ));
    // Keys are scoped by kind and by user.
    a.withdraw(&u, "same", "USD", 100).await.unwrap();
    let v = d.user("vic").await;
    a.deposit(&v, "same", "USD", 500).await.unwrap();
    assert_eq!(d.balance(&u, "USD").await, (500, 0));
    assert_eq!(d.balance(&v, "USD").await, (500, 0));
    d.audit().await;
}

/// The spec's ledger example: 100 ACME trade at $10.00 against a $10.01 limit.
#[tokio::test]
async fn settlement_matches_the_spec_example() {
    let d = db().await;
    let a = &d.accounts;
    let buyer = d.funded("buyer", 100_100).await;
    let seller = d.user("seller").await;
    a.deposit(&seller, "shares", "ACME", 100).await.unwrap();
    let (b, s) = (order(), order());
    a.reserve(&reserve_buy(&b, &buyer, 100, 1001))
        .await
        .unwrap();
    a.reserve(&reserve_sell(&s, &seller, "ACME", 100))
        .await
        .unwrap();
    assert_eq!(d.balance(&buyer, "USD").await, (0, 100_100));
    assert_eq!(d.balance(&seller, "ACME").await, (0, 100));

    let t = trade("ACME-1042", (1000, 100), (&b, &s), (&buyer, &seller));
    let r = a.settle_trade(&t).await.unwrap();
    assert_eq!(r.price_improvement_cents, 100);
    assert_eq!(d.balance(&buyer, "USD").await, (100, 0));
    assert_eq!(d.balance(&buyer, "ACME").await, (100, 0));
    assert_eq!(d.balance(&seller, "USD").await, (100_000, 0));
    assert_eq!(d.balance(&seller, "ACME").await, (0, 0));

    // The five entries in the spec's table, and nothing else.
    let mut rows: Vec<(String, String, String, i64)> = sqlx::query_as(
        "SELECT CASE WHEN a.owner_id = $2::uuid THEN 'buyer' ELSE 'seller' END, a.kind, a.asset, e.amount
         FROM ledger_entries e JOIN ledger_accounts a ON a.id = e.account_id
         WHERE e.transaction_id = $1::uuid",
    )
    .bind(&r.transaction_id)
    .bind(&buyer)
    .fetch_all(a.pool())
    .await
    .unwrap();
    rows.sort();
    let row = |w: &str, k: &str, asset: &str, n: i64| (w.into(), k.into(), asset.into(), n);
    assert_eq!(
        rows,
        vec![
            row("buyer", "available", "ACME", 100),
            row("buyer", "available", "USD", 100),
            row("buyer", "reserved", "USD", -100_100),
            row("seller", "available", "USD", 100_000),
            row("seller", "reserved", "ACME", -100),
        ]
    );

    // Settling the same trade again changes nothing and returns the same answer.
    assert_eq!(a.settle_trade(&t).await.unwrap(), r);
    assert_eq!(d.balance(&buyer, "USD").await, (100, 0));
    d.audit().await;
}

#[tokio::test]
async fn a_reserve_without_funds_fails_and_leaves_nothing_behind() {
    let d = db().await;
    let a = &d.accounts;
    let u = d.funded("cat", 1_000).await;
    let o = order();
    assert!(matches!(
        a.reserve(&reserve_buy(&o, &u, 2, 501)).await,
        Err(AccountsError::InsufficientFunds)
    ));
    // The failed attempt kept neither the key nor a reservation.
    a.reserve(&reserve_buy(&o, &u, 2, 500)).await.unwrap();
    assert_eq!(d.balance(&u, "USD").await, (0, 1_000));
    // A retry of the successful reserve is a no-op.
    a.reserve(&reserve_buy(&o, &u, 2, 500)).await.unwrap();
    assert_eq!(d.balance(&u, "USD").await, (0, 1_000));
    assert!(matches!(
        a.reserve(&reserve_buy(&o, &u, 1, 500)).await,
        Err(AccountsError::IdempotencyConflict)
    ));
    assert!(matches!(
        a.reserve(&reserve_buy(&order(), &u, 1, 0)).await,
        Err(AccountsError::Validation(_))
    ));
    assert!(matches!(
        a.reserve(&pb::ReserveRequest {
            limit_price_cents: 5,
            ..reserve_sell(&order(), &u, "ACME", 1)
        })
        .await,
        Err(AccountsError::Validation(_))
    ));
    d.audit().await;
}

/// Spec: "Late reserve protection".
#[tokio::test]
async fn a_release_before_the_reserve_voids_it_so_the_late_reserve_fails() {
    let d = db().await;
    let a = &d.accounts;
    let u = d.funded("dan", 5_000).await;
    let o = order();
    let r = a
        .release(&pb::ReleaseRequest {
            void_if_missing: true,
            ..release(&o, "release:ACME-7", 0)
        })
        .await
        .unwrap();
    assert_eq!((r.released_amount, r.remaining_amount), (0, 0));
    assert!(matches!(
        a.reserve(&reserve_buy(&o, &u, 1, 1000)).await,
        Err(AccountsError::ReservationVoided)
    ));
    assert_eq!(d.balance(&u, "USD").await, (5_000, 0));

    // Without void_if_missing an unknown order is simply not found.
    assert!(matches!(
        a.release(&release(&order(), "release:ACME-8", 10)).await,
        Err(AccountsError::NotFound("reservation"))
    ));

    // With it, an existing reservation is released in full.
    let o2 = order();
    a.reserve(&reserve_buy(&o2, &u, 3, 1000)).await.unwrap();
    let r = a
        .release(&pb::ReleaseRequest {
            void_if_missing: true,
            ..release(&o2, "release:ACME-9", 0)
        })
        .await
        .unwrap();
    assert_eq!((r.released_amount, r.remaining_amount), (3_000, 0));
    assert_eq!(d.balance(&u, "USD").await, (5_000, 0));
    d.audit().await;
}

/// Spec (Risk): the release amount comes from the canceled quantity, so a
/// cancel and a settlement can arrive in either order with the same result.
#[tokio::test]
async fn cancel_and_settlement_in_either_order_give_the_same_balances() {
    let d = db().await;
    let a = &d.accounts;
    let mut finals = Vec::new();
    for release_first in [false, true] {
        let tag = if release_first { "r" } else { "s" };
        let buyer = d.funded(&format!("buyer-{tag}"), 20_000).await;
        let seller = d.user(&format!("seller-{tag}")).await;
        a.deposit(&seller, "shares", "ACME", 4).await.unwrap();
        let (b, s) = (order(), order());
        // Buy 10 at a $10.01 limit; 4 fill at $10.00, then 6 are canceled.
        a.reserve(&reserve_buy(&b, &buyer, 10, 1001)).await.unwrap();
        a.reserve(&reserve_sell(&s, &seller, "ACME", 4))
            .await
            .unwrap();
        let settle = trade(
            &format!("ACME-{tag}1"),
            (1000, 4),
            (&b, &s),
            (&buyer, &seller),
        );
        let cancel = release(&b, &format!("release:ACME-{tag}2"), 6 * 1001);
        if release_first {
            a.release(&cancel).await.unwrap();
            a.settle_trade(&settle).await.unwrap();
        } else {
            a.settle_trade(&settle).await.unwrap();
            let r = a.release(&cancel).await.unwrap();
            assert_eq!(r.remaining_amount, 0);
        }
        finals.push((
            d.balance(&buyer, "USD").await,
            d.balance(&buyer, "ACME").await,
            d.balance(&seller, "USD").await,
        ));
    }
    assert_eq!(finals[0], finals[1]);
    assert_eq!(finals[0], ((20_000 - 4_000, 0), (4, 0), (4_000, 0)));
    d.audit().await;
}

#[tokio::test]
async fn impossible_requests_are_reported_as_invariant_violations() {
    let d = db().await;
    let a = &d.accounts;
    let buyer = d.funded("eve", 10_000).await;
    let seller = d.user("fay").await;
    a.deposit(&seller, "shares", "ACME", 10).await.unwrap();
    let (b, s) = (order(), order());
    a.reserve(&reserve_buy(&b, &buyer, 5, 1000)).await.unwrap();
    a.reserve(&reserve_sell(&s, &seller, "ACME", 5))
        .await
        .unwrap();

    // Releasing more than is reserved.
    assert!(matches!(
        a.release(&release(&b, "release:x", 5_001)).await,
        Err(AccountsError::Invariant(_))
    ));
    // A trade above the buy limit, more shares than reserved, or the wrong buyer.
    for t in [
        trade("T1", (1001, 1), (&b, &s), (&buyer, &seller)),
        trade("T2", (1000, 6), (&b, &s), (&buyer, &seller)),
        trade("T3", (1000, 1), (&b, &s), (&seller, &buyer)),
    ] {
        assert!(
            matches!(a.settle_trade(&t).await, Err(AccountsError::Invariant(_))),
            "{t:?}"
        );
    }
    // Nothing moved.
    assert_eq!(d.balance(&buyer, "USD").await, (5_000, 5_000));
    assert_eq!(d.balance(&seller, "ACME").await, (5, 5));
    d.audit().await;
}

#[tokio::test]
async fn the_ledger_pages_newest_first() {
    let d = db().await;
    let a = &d.accounts;
    let u = d.user("gus").await;
    for i in 1..=5 {
        a.deposit(&u, &format!("d{i}"), "USD", i).await.unwrap();
    }
    let mut amounts = Vec::new();
    let mut cursor = String::new();
    let mut pages = 0;
    loop {
        let page = a.ledger(&u, &cursor, 2).await.unwrap();
        pages += 1;
        assert!(page.entries.len() <= 2);
        amounts.extend(page.entries.iter().map(|e| e.amount));
        assert!(
            page.entries
                .iter()
                .all(|e| e.kind == "deposit" && e.account_kind == "available" && e.asset == "USD")
        );
        if page.next_cursor.is_empty() {
            break;
        }
        cursor = page.next_cursor;
    }
    assert_eq!((pages, amounts), (3, vec![5, 4, 3, 2, 1]));
    assert!(matches!(
        a.ledger(&u, "", 101).await,
        Err(AccountsError::Validation(_))
    ));
}
