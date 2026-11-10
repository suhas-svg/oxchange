//! Spec failure test "Exact-moment settlement crash": crash after
//! SettleTrade, before SETTLED. The balance must change only once.
//!
//! Its own test binary, because fail points are process-wide.

mod common;

use common::{env, trade_of};
use consumer::HandlerError;
use proto_gen::events::v1::Side::{Buy, Sell};
use settlement::CRASH_AFTER_SETTLE_TRADE;

#[tokio::test]
async fn a_crash_between_settle_trade_and_settled_moves_money_once() {
    let scenario = fail::FailScenario::setup();
    let e = env().await;
    let buyer = e.user("buyer", 100_000, &[]).await;
    let seller = e.user("seller", 0, &[("ACME", 40)]).await;
    e.place(&seller, "s", "ACME", Sell, 1000, 40).await.unwrap();
    e.place(&buyer, "b", "ACME", Buy, 1010, 40).await.unwrap();

    fail::cfg(CRASH_AFTER_SETTLE_TRADE, "return").unwrap();
    let t = e
        .run_risk()
        .await
        .iter()
        .find_map(trade_of)
        .unwrap()
        .clone();
    assert!(matches!(e.deliver().await, Err(HandlerError::Retry(_))));
    // Accounts moved the money; Settlement still has the trade PENDING and
    // published nothing.
    let moved = (
        e.balance(&buyer, "USD").await,
        e.balance(&seller, "USD").await,
    );
    assert_eq!(moved, ((60_000, 0), (40_000, 0)));
    assert_eq!(e.trade_status(&t.trade_id).await, "PENDING");
    assert!(e.settled_messages().await.is_empty());

    // The process restarts and Kafka hands the same event over again.
    fail::remove(CRASH_AFTER_SETTLE_TRADE);
    e.deliver().await.unwrap();
    assert_eq!(e.trade_status(&t.trade_id).await, "SETTLED");
    assert_eq!(e.settle_transactions().await, 1);
    assert_eq!(
        (
            e.balance(&buyer, "USD").await,
            e.balance(&seller, "USD").await
        ),
        moved
    );
    assert_eq!(e.settled_messages().await.len(), 1);
    e.audit().await;
    scenario.teardown();
}
