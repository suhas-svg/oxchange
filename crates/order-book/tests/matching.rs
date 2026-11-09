//! One test per matching rule in the spec.

mod common;

use common::{Harness, oid, user};
use domain::{OrderType, Price, Qty, Side::*, TimeInForce};
use order_book::{CancelReason, CancelRejectReason, EventBody, TradeExecuted};

fn p(c: i64) -> Price {
    Price::new(c).unwrap()
}
fn q(n: i64) -> Qty {
    Qty::new(n).unwrap()
}

fn trades(events: &[order_book::BookEvent]) -> Vec<TradeExecuted> {
    events
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::Trade(t) => Some(t.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn resting_order_emits_accept_then_book_change() {
    let mut h = Harness::new();
    let ev = h.gtc(1, 1, Buy, 1000, 10);
    assert_eq!(ev.len(), 2);
    assert!(matches!(ev[0].body, EventBody::OrderAccepted { .. }));
    assert_eq!(
        ev[1].body,
        EventBody::BookChanged {
            side: Buy,
            price: p(1000),
            new_total_quantity: q(10)
        }
    );
    assert_eq!(h.book.best_bid(), Some(p(1000)));
    assert_eq!(h.book.best_ask(), None);
}

#[test]
fn price_first_best_ask_trades_before_worse_ask() {
    let mut h = Harness::new();
    h.gtc(1, 1, Sell, 1002, 10);
    h.gtc(2, 2, Sell, 1001, 10);
    let t = trades(&h.gtc(3, 3, Buy, 1002, 10));
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].sell_order_id, oid(2));
    assert_eq!(t[0].price, p(1001));
}

#[test]
fn time_second_earliest_order_at_a_price_trades_first() {
    let mut h = Harness::new();
    h.gtc(1, 1, Buy, 1000, 5);
    h.gtc(2, 2, Buy, 1000, 5);
    h.gtc(3, 3, Buy, 1000, 5);
    let t = trades(&h.gtc(4, 4, Sell, 1000, 7));
    assert_eq!(
        t.iter()
            .map(|t| (t.buy_order_id, t.quantity))
            .collect::<Vec<_>>(),
        vec![(oid(1), q(5)), (oid(2), q(2))]
    );
    // Order 2 keeps its place at the front with 3 left; order 3 is untouched.
    let level = h.book.orders_at(Buy, p(1000));
    assert_eq!(level[0].order_id, oid(2));
    assert_eq!(level[0].remaining, q(3));
    assert_eq!(level[1].order_id, oid(3));
}

#[test]
fn resting_price_wins() {
    let mut h = Harness::new();
    h.gtc(1, 1, Sell, 1000, 10);
    let t = trades(&h.gtc(2, 2, Buy, 1050, 10));
    assert_eq!(t[0].price, p(1000));
    assert_eq!(t[0].aggressor_side, Buy);
}

#[test]
fn keeps_matching_across_levels_then_rests_remainder() {
    let mut h = Harness::new();
    h.gtc(1, 1, Sell, 1000, 3);
    h.gtc(2, 1, Sell, 1001, 3);
    h.gtc(3, 1, Sell, 1005, 3);
    let ev = h.gtc(4, 2, Buy, 1001, 10);
    let t = trades(&ev);
    assert_eq!(t.len(), 2);
    assert_eq!((t[0].price, t[1].price), (p(1000), p(1001)));
    // 4 left rests as the new best bid, below the remaining ask at 10.05.
    assert_eq!(h.book.best_bid(), Some(p(1001)));
    assert_eq!(h.book.best_ask(), Some(p(1005)));
    assert_eq!(h.book.order(oid(4)).unwrap().remaining, q(4));
    assert_eq!(
        ev.last().unwrap().body,
        EventBody::BookChanged {
            side: Buy,
            price: p(1001),
            new_total_quantity: q(4)
        }
    );
}

#[test]
fn one_book_change_per_level_touched() {
    let mut h = Harness::new();
    h.gtc(1, 1, Sell, 1000, 2);
    h.gtc(2, 2, Sell, 1000, 2);
    h.gtc(3, 3, Sell, 1000, 2);
    let ev = h.gtc(4, 4, Buy, 1000, 5);
    let changes: Vec<_> = ev
        .iter()
        .filter(|e| matches!(e.body, EventBody::BookChanged { .. }))
        .collect();
    assert_eq!(changes.len(), 1);
    assert_eq!(
        changes[0].body,
        EventBody::BookChanged {
            side: Sell,
            price: p(1000),
            new_total_quantity: q(1)
        }
    );
}

#[test]
fn ioc_remainder_is_canceled_not_rested() {
    let mut h = Harness::new();
    h.gtc(1, 1, Sell, 1000, 4);
    let ev = h.ioc(2, 2, Buy, 1000, 10);
    assert_eq!(trades(&ev)[0].quantity, q(4));
    assert_eq!(
        ev.last().unwrap().body,
        EventBody::OrderCanceled {
            order_id: oid(2),
            user_id: user(2),
            canceled_quantity: q(6),
            reason: CancelReason::IocRemainder
        }
    );
    assert!(h.book.is_empty());
}

#[test]
fn market_order_is_ioc_at_its_protected_price() {
    let mut h = Harness::new();
    h.gtc(1, 1, Sell, 1000, 5);
    h.gtc(2, 1, Sell, 1100, 5); // beyond the 5% protected price of 10.50
    // GTC on a market order is ignored: market always behaves as IOC.
    let ev = h.order(3, 2, Buy, OrderType::Market, TimeInForce::Gtc, 1050, 10);
    assert_eq!(trades(&ev).len(), 1);
    assert!(matches!(
        ev.last().unwrap().body,
        EventBody::OrderCanceled {
            reason: CancelReason::IocRemainder,
            ..
        }
    ));
    assert_eq!(h.book.order(oid(3)), None);
}

#[test]
fn self_trade_cancels_the_incoming_remainder() {
    let mut h = Harness::new();
    h.gtc(1, 2, Sell, 1000, 3); // another user, first in line
    h.gtc(2, 1, Sell, 1000, 5); // same user as the incoming buy
    h.gtc(3, 2, Sell, 1000, 5);
    let ev = h.gtc(4, 1, Buy, 1000, 10);
    let t = trades(&ev);
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].sell_order_id, oid(1));
    assert!(ev.iter().any(|e| e.body
        == EventBody::OrderCanceled {
            order_id: oid(4),
            user_id: user(1),
            canceled_quantity: q(7),
            reason: CancelReason::SelfTrade
        }));
    // The user's own resting order and the one behind it are untouched.
    assert_eq!(h.book.order(oid(2)).unwrap().remaining, q(5));
    assert_eq!(h.book.order(oid(3)).unwrap().remaining, q(5));
    assert_eq!(h.book.order(oid(4)), None);
}

#[test]
fn user_cancel_removes_order_and_reports_quantity() {
    let mut h = Harness::new();
    h.gtc(1, 1, Buy, 1000, 10);
    h.gtc(2, 2, Sell, 1000, 4);
    let ev = h.cancel(1, 1);
    assert_eq!(
        ev[0].body,
        EventBody::OrderCanceled {
            order_id: oid(1),
            user_id: user(1),
            canceled_quantity: q(6),
            reason: CancelReason::User
        }
    );
    assert_eq!(
        ev[1].body,
        EventBody::BookChanged {
            side: Buy,
            price: p(1000),
            new_total_quantity: Qty::ZERO
        }
    );
    assert!(h.book.is_empty());
}

#[test]
fn cancel_from_middle_of_level_keeps_fifo_links() {
    let mut h = Harness::new();
    for id in 1..=3 {
        h.gtc(id, id, Sell, 1000, 1);
    }
    h.cancel(2, 2);
    let ids: Vec<_> = h
        .book
        .orders_at(Sell, p(1000))
        .iter()
        .map(|o| o.order_id)
        .collect();
    assert_eq!(ids, vec![oid(1), oid(3)]);
}

#[test]
fn cancel_loses_race_to_full_fill() {
    let mut h = Harness::new();
    h.gtc(1, 1, Buy, 1000, 5);
    h.gtc(2, 2, Sell, 1000, 5);
    let ev = h.cancel(1, 1);
    assert_eq!(
        ev[0].body,
        EventBody::CancelRejected {
            order_id: oid(1),
            user_id: user(1),
            reason: CancelRejectReason::NotFound
        }
    );
}

#[test]
fn cannot_cancel_another_users_order() {
    let mut h = Harness::new();
    h.gtc(1, 1, Buy, 1000, 5);
    let ev = h.cancel(1, 2);
    assert!(matches!(
        ev[0].body,
        EventBody::CancelRejected {
            reason: CancelRejectReason::NotOwner,
            ..
        }
    ));
    assert!(h.book.order(oid(1)).is_some());
}

#[test]
fn out_of_range_order_is_canceled_whole() {
    let mut h = Harness::new();
    let ev = h.gtc(1, 1, Buy, 0, 10);
    assert_eq!(ev.len(), 1);
    assert_eq!(
        ev[0].body,
        EventBody::OrderCanceled {
            order_id: oid(1),
            user_id: user(1),
            canceled_quantity: q(10),
            reason: CancelReason::Invalid
        }
    );
    assert!(h.book.is_empty());
}

#[test]
fn duplicate_resting_order_id_is_ignored() {
    let mut h = Harness::new();
    h.gtc(1, 1, Buy, 1000, 5);
    let seq = h.book.last_seq();
    let ev = h.gtc(1, 1, Buy, 1000, 5);
    assert!(ev.is_empty());
    assert_eq!(h.book.last_seq(), seq);
    assert_eq!(h.book.order(oid(1)).unwrap().remaining, q(5));
}

#[test]
fn spec_reservation_example_fills_at_two_prices() {
    // "The book has 100 shares for sale at $10.00 and 50 at $10.01. A user
    // places a buy limit for 120 shares at $10.01 ... fills 100 at $10.00 and
    // 20 at $10.01, costing $1,200.20."
    let mut h = Harness::new();
    h.gtc(1, 1, Sell, 1000, 100);
    h.gtc(2, 1, Sell, 1001, 50);
    let t = trades(&h.gtc(3, 2, Buy, 1001, 120));
    let cost: i64 = t
        .iter()
        .map(|t| t.price.notional(t.quantity).unwrap().get())
        .sum();
    assert_eq!(cost, 120_020);
    let reserved = p(1001).notional(q(120)).unwrap().get();
    assert_eq!(reserved - cost, 100); // $1.00 price improvement back to the buyer
}

#[test]
fn seq_is_gap_free_and_events_carry_command_time() {
    let mut h = Harness::new();
    h.gtc(1, 1, Sell, 1000, 5);
    h.gtc(2, 2, Buy, 1000, 3);
    h.cancel(1, 1);
    h.cancel(9, 1);
    for (i, e) in h.events.iter().enumerate() {
        assert_eq!(e.seq, i as u64 + 1);
    }
    assert_eq!(h.events[0].event_time_ns, 1_000);
    assert_eq!(h.events[0].event_id(h.book.symbol()), "ACME-1");
}
