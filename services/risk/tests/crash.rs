//! Spec failure test "Exact-moment Risk crash": crash after Reserve, before
//! the status save. The sweeper must release the funds.
//!
//! Its own test binary, because fail points are process-wide.

mod common;

use std::time::Duration;

use common::{env, status};
use proto_gen::events::v1::Side::Buy;
use proto_gen::risk::v1::OrderStatus as S;
use risk::{CRASH_AFTER_RESERVE, RiskError};

#[tokio::test]
async fn funds_reserved_before_a_crash_are_released_by_the_sweeper() {
    let scenario = fail::FailScenario::setup();
    let e = env().await;
    let u = e.user("cat", 50_000, &[]).await;

    fail::cfg(CRASH_AFTER_RESERVE, "return").unwrap();
    let r = e.place(&u, "o1", "ACME", Buy, 1000, 10).await;
    assert!(matches!(r, Err(RiskError::Unavailable(_))), "{r:?}");
    fail::remove(CRASH_AFTER_RESERVE);

    // What the dead copy left: funds locked, the order stuck in PENDING_RISK.
    assert_eq!(e.balance(&u, "USD").await, (40_000, 10_000));
    let (order_id,): (uuid::Uuid,) =
        sqlx::query_as("SELECT order_id FROM orders WHERE client_order_id = 'o1'")
            .fetch_one(e.risk.pool())
            .await
            .unwrap();
    let o = e.order(&u, &order_id.to_string()).await;
    assert_eq!(status(&o), S::PendingRisk);
    // Too young for the sweeper.
    assert_eq!(e.risk.sweep(risk::PENDING_TIMEOUT).await.unwrap(), 0);

    // Thirty seconds later. A slow Release (500 ms limit) leaves the order
    // for the next pass, as the service's 5-second loop would.
    let mut swept = 0;
    for _ in 0..10 {
        swept = e.risk.sweep(Duration::ZERO).await.unwrap();
        if swept > 0 {
            break;
        }
    }
    assert_eq!(swept, 1);
    assert_eq!(e.balance(&u, "USD").await, (50_000, 0));
    let o = e.order(&u, &order_id.to_string()).await;
    assert_eq!(
        (status(&o), o.reject_reason.as_str()),
        (S::Rejected, "ACCOUNTS_UNAVAILABLE")
    );
    // Nothing reached the engine.
    assert!(e.pump().await.is_empty());
    e.audit().await;
    scenario.teardown();
}
