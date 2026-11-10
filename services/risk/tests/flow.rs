//! Orders through Risk, the outbox, the real matching engine and Accounts.

mod common;

use common::{env, env_with_prices, limit, status};
use proto_gen::events::v1 as ev;
use proto_gen::risk::v1 as pb;
use risk::RiskError;

use ev::Side::{Buy, Sell};
use pb::OrderStatus as S;

/// The Gate 2 path in miniature: place, match, settle, cancel, and every
/// cent accounted for.
#[tokio::test]
async fn an_order_fills_partly_settles_and_the_rest_is_canceled_and_released() {
    let e = env().await;
    let buyer = e.user("buyer", 200_000, &[]).await;
    let seller = e.user("seller", 0, &[("ACME", 100)]).await;

    let sell = e
        .place(&seller, "s1", "ACME", Sell, 1000, 100)
        .await
        .unwrap();
    assert!(!sell.existing);
    let sell = sell.order.unwrap();
    assert_eq!(status(&sell), S::Accepted);
    assert_eq!(e.balance(&seller, "ACME").await, (0, 100));
    e.pump().await;
    assert_eq!(status(&e.order(&seller, &sell.order_id).await), S::Open);

    // Buy 120 at $10.01 against 100 resting at $10.00 (the spec's example shape).
    let buy = e
        .place(&buyer, "b1", "ACME", Buy, 1001, 120)
        .await
        .unwrap()
        .order
        .unwrap();
    assert_eq!(e.balance(&buyer, "USD").await, (200_000 - 120_120, 120_120));
    e.pump().await;
    let b = e.order(&buyer, &buy.order_id).await;
    assert_eq!((status(&b), b.filled_quantity), (S::PartiallyFilled, 100));
    assert_eq!(status(&e.order(&seller, &sell.order_id).await), S::Filled);
    // Settled at the resting price; $1.00 of price improvement came back.
    assert_eq!(e.balance(&buyer, "USD").await, (79_980, 20_020));
    assert_eq!(e.balance(&buyer, "ACME").await, (100, 0));
    assert_eq!(e.balance(&seller, "USD").await, (100_000, 0));

    // Cancel the remaining 20: the engine cancels, Risk releases 20 × $10.01.
    let c = e.risk.cancel_order(&buyer, &buy.order_id).await.unwrap();
    assert_eq!(status(&c.order.unwrap()), S::PartiallyFilled);
    e.pump().await;
    let b = e.order(&buyer, &buy.order_id).await;
    assert_eq!(
        (status(&b), b.filled_quantity, b.canceled_quantity),
        (S::Canceled, 100, 20)
    );
    assert_eq!(e.balance(&buyer, "USD").await, (100_000, 0));
    e.audit().await;
}

#[tokio::test]
async fn sending_the_same_client_order_id_again_returns_the_original() {
    let e = env().await;
    let u = e.user("ann", 100_000, &[]).await;
    let first = e.place(&u, "same", "ACME", Buy, 900, 10).await.unwrap();
    let again = e.place(&u, "same", "ACME", Buy, 900, 10).await.unwrap();
    assert!(again.existing && !first.existing);
    assert_eq!(again.order.unwrap().order_id, first.order.unwrap().order_id);
    // One reservation, one command.
    assert_eq!(e.balance(&u, "USD").await, (91_000, 9_000));
    assert_eq!(e.pump().await.len(), 1 + 1, "OrderAccepted and BookChanged");
}

#[tokio::test]
async fn checks_run_in_the_spec_order_and_refuse_with_its_codes() {
    let e = env().await;
    let u = e.user("bob", 500, &[]).await;
    let r = |coid: &str, sym: &str, side: ev::Side, price: i64, qty: i64| {
        limit(&u, coid, sym, side, price, qty)
    };

    let unknown = e.risk.place_order(&r("a", "ZZZZ", Buy, 100, 1)).await;
    assert!(
        matches!(unknown, Err(RiskError::SymbolNotTrading)),
        "{unknown:?}"
    );
    for bad in [
        r("b", "ACME", Buy, 0, 1),
        r("c", "ACME", Buy, 100, 0),
        r("d", "ACME", ev::Side::Unspecified, 100, 1),
    ] {
        assert!(
            matches!(
                e.risk.place_order(&bad).await,
                Err(RiskError::Validation(_))
            ),
            "{bad:?}"
        );
    }
    let mut no_tif = r("e", "ACME", Buy, 100, 1);
    no_tif.time_in_force = 0;
    assert!(matches!(
        e.risk.place_order(&no_tif).await,
        Err(RiskError::Validation(_))
    ));
    // $1,001,000 of stock.
    let big = e
        .risk
        .place_order(&r("f", "NOVA", Buy, 100_000, 1001))
        .await;
    assert!(matches!(big, Err(RiskError::RiskLimit(_))), "{big:?}");
    // None of those saved an order.
    let (n,): (i64,) = sqlx::query_as("SELECT count(*) FROM orders")
        .fetch_one(e.risk.pool())
        .await
        .unwrap();
    assert_eq!(n, 0);

    // Not enough money: the order is saved as REJECTED, and a retry returns it.
    let poor = e.risk.place_order(&r("g", "ACME", Buy, 1000, 1)).await;
    assert!(
        matches!(poor, Err(RiskError::InsufficientFunds)),
        "{poor:?}"
    );
    let again = e
        .risk
        .place_order(&r("g", "ACME", Buy, 1000, 1))
        .await
        .unwrap();
    let o = again.order.unwrap();
    assert!(again.existing);
    assert_eq!(
        (status(&o), o.reject_reason.as_str()),
        (S::Rejected, "INSUFFICIENT_FUNDS")
    );
    assert_eq!(e.balance(&u, "USD").await, (500, 0));
    assert_eq!(e.pump().await.len(), 0, "a rejected order sends no command");
}

/// Spec check 5: a market order needs a reference price, which comes from
/// Market Data's Redis ticker. Without one, market orders are rejected and
/// limit orders carry on.
#[tokio::test]
async fn market_orders_use_a_protected_price_from_the_ticker() {
    let e = env_with_prices().await;
    let prices = e.prices.clone().unwrap();
    let buyer = e.user("buyer", 100_000, &[]).await;
    let seller = e.user("seller", 0, &[("ACME", 5)]).await;
    e.place(&seller, "s", "ACME", Sell, 1020, 5).await.unwrap();
    e.pump().await;

    let market = |coid: &str| pb::PlaceOrderRequest {
        order_type: ev::OrderType::Market as i32,
        time_in_force: 0,
        ..limit(&buyer, coid, "ACME", Buy, 0, 10)
    };
    // No trades yet, so no reference price.
    let r = e.risk.place_order(&market("m0")).await;
    assert!(matches!(r, Err(RiskError::Unavailable(_))), "{r:?}");

    // A trade at $10.20, which Market Data puts in the ticker.
    e.place(&buyer, "b", "ACME", Buy, 1020, 1).await.unwrap();
    let trade = e
        .pump()
        .await
        .into_iter()
        .find(|x| matches!(x.body, Some(ev::matching_event::Body::Trade(_))))
        .unwrap();
    prices
        .write(
            "ACME",
            ticker::Tick {
                last_price_cents: 1020,
                day_volume: 1,
                seq: trade.seq,
                traded_at_ns: trade.event_time_ns,
            },
        )
        .await
        .unwrap();

    // 5% above the last trade, and always IOC.
    let o = e
        .risk
        .place_order(&market("m1"))
        .await
        .unwrap()
        .order
        .unwrap();
    assert_eq!(o.limit_price_cents, 1071);
    assert_eq!(o.time_in_force, ev::TimeInForce::Ioc as i32);
    e.pump().await;
    let o = e.order(&buyer, &o.order_id).await;
    // 4 filled at $10.20; the other 6 canceled at once (IOC) and released.
    assert_eq!(
        (status(&o), o.filled_quantity, o.canceled_quantity),
        (S::Canceled, 4, 6)
    );
    assert_eq!(e.balance(&buyer, "USD").await, (100_000 - 5 * 1020, 0));

    // Redis is lost: market orders are rejected, limit orders continue.
    e.redis.as_ref().unwrap().stop().await.unwrap();
    let r = e.risk.place_order(&market("m2")).await;
    assert!(matches!(r, Err(RiskError::Unavailable(_))), "{r:?}");
    e.place(&buyer, "b2", "ACME", Buy, 1000, 1).await.unwrap();
    e.pump().await;
    e.audit().await;
}

#[tokio::test]
async fn at_most_200_open_orders_per_user() {
    let e = env().await;
    let u = e.user("cat", 1_000_000, &[]).await;
    for i in 0..200 {
        e.place(&u, &format!("o{i}"), "IONX", Buy, 1, 1)
            .await
            .unwrap();
    }
    let over = e.place(&u, "o200", "IONX", Buy, 1, 1).await;
    assert!(matches!(over, Err(RiskError::RiskLimit(_))), "{over:?}");

    // Closing one makes room.
    e.pump().await;
    let open = e
        .risk
        .list_orders(&pb::ListOrdersRequest {
            user_id: u.clone(),
            page_size: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    e.risk
        .cancel_order(&u, &open.orders[0].order_id)
        .await
        .unwrap();
    e.pump().await;
    e.place(&u, "o200", "IONX", Buy, 1, 1).await.unwrap();
}

#[tokio::test]
async fn orders_are_private_and_list_newest_first() {
    let e = env().await;
    let a = e.user("ann", 100_000, &[]).await;
    let b = e.user("bob", 100_000, &[]).await;
    let mut ids = Vec::new();
    for (i, sym) in ["ACME", "BOLT", "ACME"].iter().enumerate() {
        ids.push(
            e.place(&a, &format!("a{i}"), sym, Buy, 100, 1)
                .await
                .unwrap()
                .order
                .unwrap()
                .order_id,
        );
    }
    // Another user's order does not exist for you.
    assert!(matches!(
        e.risk.get_order(&b, &ids[0]).await,
        Err(RiskError::NotFound)
    ));
    assert!(matches!(
        e.risk.cancel_order(&b, &ids[0]).await,
        Err(RiskError::NotFound)
    ));

    let list = |symbol: &str, cursor: String, open_only: bool| pb::ListOrdersRequest {
        user_id: a.clone(),
        symbol: symbol.into(),
        cursor,
        page_size: 2,
        open_only,
    };
    let p1 = e
        .risk
        .list_orders(&list("", String::new(), false))
        .await
        .unwrap();
    let p2 = e
        .risk
        .list_orders(&list("", p1.next_cursor.clone(), false))
        .await
        .unwrap();
    let got: Vec<_> = p1
        .orders
        .iter()
        .chain(&p2.orders)
        .map(|o| o.order_id.clone())
        .collect();
    assert_eq!(got, ids.iter().rev().cloned().collect::<Vec<_>>());
    assert!(p2.next_cursor.is_empty());
    let acme = e
        .risk
        .list_orders(&list("ACME", String::new(), false))
        .await
        .unwrap();
    assert_eq!(acme.orders.len(), 2);

    e.risk.cancel_order(&a, &ids[1]).await.unwrap();
    e.pump().await;
    let open = e
        .risk
        .list_orders(&list("", String::new(), true))
        .await
        .unwrap();
    // Two of the three are still open, which fits on one page.
    assert_eq!(open.orders.len(), 2);
    assert!(open.next_cursor.is_empty());
    assert!(open.orders.iter().all(|o| o.order_id != ids[1]));
    assert!(
        e.risk
            .list_orders(&b_list(&b))
            .await
            .unwrap()
            .orders
            .is_empty()
    );
}

fn b_list(b: &str) -> pb::ListOrdersRequest {
    pb::ListOrdersRequest {
        user_id: b.into(),
        ..Default::default()
    }
}

/// GET /v1/symbols, through the Gateway.
#[tokio::test]
async fn the_listed_stocks_come_back_in_symbol_order() {
    let e = env().await;
    let list = e.risk.list_symbols().await.unwrap().symbols;
    assert_eq!(list.len(), 20);
    assert_eq!(
        (
            list[0].symbol.as_str(),
            list[0].name.as_str(),
            list[0].trading
        ),
        ("ACME", "Acme Simulated Holdings", true)
    );
    assert_eq!(list[0].listing_price_cents, 1000);
    assert!(list.windows(2).all(|w| w[0].symbol < w[1].symbol));
}
