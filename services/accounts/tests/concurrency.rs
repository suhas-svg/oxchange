//! Concurrent requests: no overdraft, no double effect, no deadlock.

mod common;

use accounts::AccountsError;
use common::{db, order};
use proto_gen::accounts::v1 as pb;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parallel_reserves_never_overdraw() {
    let d = db().await;
    let u = d.funded("ann", 10_000).await;
    let tasks: Vec<_> = (0..50)
        .map(|_| {
            let a = d.accounts.clone();
            let req = pb::ReserveRequest {
                order_id: order(),
                user_id: u.clone(),
                asset: "USD".into(),
                amount: 1_000,
                limit_price_cents: 100,
            };
            tokio::spawn(async move { a.reserve(&req).await })
        })
        .collect();
    let (mut ok, mut short) = (0, 0);
    for t in tasks {
        match t.await.unwrap() {
            Ok(_) => ok += 1,
            Err(AccountsError::InsufficientFunds) => short += 1,
            Err(e) => panic!("unexpected {e:?}"),
        }
    }
    assert_eq!((ok, short), (10, 40));
    assert_eq!(d.balance(&u, "USD").await, (0, 10_000));
    d.audit().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_same_key_sent_at_once_applies_once() {
    let d = db().await;
    let u = d.user("bob").await;
    let tasks: Vec<_> = (0..20)
        .map(|_| {
            let (a, u) = (d.accounts.clone(), u.clone());
            tokio::spawn(async move { a.deposit(&u, "once", "USD", 700).await })
        })
        .collect();
    let mut ids = Vec::new();
    for t in tasks {
        ids.push(t.await.unwrap().unwrap().transaction_id);
    }
    ids.dedup();
    assert_eq!(ids.len(), 1, "every caller gets the first transaction");
    assert_eq!(d.balance(&u, "USD").await, (700, 0));
    d.audit().await;
}

/// Trades in both directions between the same two users touch the same rows
/// in opposite natural orders; ascending-ID locking must prevent deadlocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn crossing_settlements_do_not_deadlock() {
    let d = db().await;
    let a = &d.accounts;
    let x = d.funded("xen", 10_000_000).await;
    let y = d.funded("yan", 10_000_000).await;
    a.deposit(&x, "s", "ACME", 10_000).await.unwrap();
    a.deposit(&y, "s", "ACME", 10_000).await.unwrap();

    let mut trades = Vec::new();
    for i in 0..60 {
        let (buyer, seller) = if i % 2 == 0 { (&x, &y) } else { (&y, &x) };
        let (b, s) = (order(), order());
        a.reserve(&pb::ReserveRequest {
            order_id: b.clone(),
            user_id: buyer.clone(),
            asset: "USD".into(),
            amount: 10 * 1_000,
            limit_price_cents: 1_000,
        })
        .await
        .unwrap();
        a.reserve(&pb::ReserveRequest {
            order_id: s.clone(),
            user_id: seller.clone(),
            asset: "ACME".into(),
            amount: 10,
            limit_price_cents: 0,
        })
        .await
        .unwrap();
        trades.push(pb::SettleTradeRequest {
            trade_id: format!("ACME-{i}"),
            symbol: "ACME".into(),
            price_cents: 990,
            quantity: 10,
            buy_order_id: b,
            sell_order_id: s,
            buyer_id: buyer.clone(),
            seller_id: seller.clone(),
        });
    }
    let tasks: Vec<_> = trades
        .into_iter()
        .map(|t| {
            let a = d.accounts.clone();
            tokio::spawn(async move { a.settle_trade(&t).await })
        })
        .collect();
    for t in tasks {
        t.await.unwrap().expect("settles without deadlock");
    }
    // 30 trades each way at one price: everyone ends where they began, and
    // each buyer got the price improvement back.
    assert_eq!(d.balance(&x, "USD").await, (10_000_000, 0));
    assert_eq!(d.balance(&y, "ACME").await, (10_000, 0));
    d.audit().await;
}

/// Random deposits, withdrawals, reserves, releases and settlements from a
/// fixed seed, many failing on purpose. The ledger must still pass all five
/// Accounts auditor rules.
#[tokio::test]
async fn random_operations_keep_the_ledger_balanced() {
    let d = db().await;
    let a = &d.accounts;
    let mut rng = SplitMix(7);
    let mut users = Vec::new();
    for i in 0..5 {
        let u = d.funded(&format!("u{i}"), 1_000_000).await;
        a.deposit(&u, "s", "ACME", 1_000).await.unwrap();
        users.push(u);
    }
    // Open reservations: (order, user index, is_buy, limit, remaining qty).
    let mut open: Vec<(String, usize, bool, i64, i64)> = Vec::new();
    for step in 0..400 {
        let u = rng.below(5) as usize;
        match rng.below(6) {
            0 => {
                let amount = 1 + rng.below(50_000) as i64;
                let _ = a
                    .deposit(&users[u], &format!("d{step}"), "USD", amount)
                    .await;
            }
            1 => {
                let amount = 1 + rng.below(80_000) as i64;
                let _ = a
                    .withdraw(&users[u], &format!("w{step}"), "USD", amount)
                    .await;
            }
            2 | 3 => {
                let is_buy = rng.below(2) == 0;
                let qty = 1 + rng.below(40) as i64;
                let limit = 900 + rng.below(200) as i64;
                let o = order();
                let req = pb::ReserveRequest {
                    order_id: o.clone(),
                    user_id: users[u].clone(),
                    asset: if is_buy { "USD" } else { "ACME" }.into(),
                    amount: if is_buy { qty * limit } else { qty },
                    limit_price_cents: if is_buy { limit } else { 0 },
                };
                if a.reserve(&req).await.is_ok() {
                    open.push((o, u, is_buy, limit, qty));
                }
            }
            4 if !open.is_empty() => {
                // Cancel part of a random open order.
                let i = rng.below(open.len() as u64) as usize;
                let (o, _, is_buy, limit, qty) = open[i].clone();
                let cancel = 1 + rng.below(qty as u64) as i64;
                a.release(&pb::ReleaseRequest {
                    order_id: o,
                    idempotency_key: format!("release:{step}"),
                    amount: if is_buy { cancel * limit } else { cancel },
                    void_if_missing: false,
                })
                .await
                .unwrap();
                open[i].4 -= cancel;
            }
            _ => {
                // Trade a buy against a sell from a different user.
                let buy = open.iter().position(|r| r.2 && r.4 > 0);
                let sell = buy.and_then(|bi| {
                    open.iter()
                        .position(|r| !r.2 && r.4 > 0 && r.1 != open[bi].1)
                });
                if let (Some(bi), Some(si)) = (buy, sell) {
                    let qty = 1 + rng.below(open[bi].4.min(open[si].4) as u64) as i64;
                    let price = open[bi].3 - rng.below(50) as i64;
                    a.settle_trade(&pb::SettleTradeRequest {
                        trade_id: format!("ACME-{step}"),
                        symbol: "ACME".into(),
                        price_cents: price,
                        quantity: qty,
                        buy_order_id: open[bi].0.clone(),
                        sell_order_id: open[si].0.clone(),
                        buyer_id: users[open[bi].1].clone(),
                        seller_id: users[open[si].1].clone(),
                    })
                    .await
                    .unwrap();
                    open[bi].4 -= qty;
                    open[si].4 -= qty;
                }
            }
        }
        open.retain(|r| r.4 > 0);
        if step % 100 == 99 {
            d.audit().await;
        }
    }
    d.audit().await;
}

struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}
