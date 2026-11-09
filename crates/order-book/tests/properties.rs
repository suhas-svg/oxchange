//! Property, model-comparison and determinism tests (spec: "Testing strategy").
//!
//! A deliberately naive reference book — a flat `Vec` scanned in full for every
//! match — must produce exactly the same events as the real book for random
//! command sequences. After every command the real book's structural
//! invariants must hold, and at the end every order's quantity must be fully
//! accounted for as filled, canceled, or still resting.

use std::collections::HashMap;

use domain::{
    CommandId, MAX_PRICE_CENTS, MAX_QTY, MIN_PRICE_CENTS, MIN_QTY, OrderId, OrderType, Price, Qty,
    Side, Symbol, TimeInForce, UserId,
};
use order_book::{
    BookEvent, CancelOrder, CancelReason, CancelRejectReason, Command, CommandBody, EventBody,
    NewOrder, OrderBook, TradeExecuted,
};
use proptest::prelude::*;

// ---------------------------------------------------------------- reference

struct RefOrder {
    arrival: u64,
    id: OrderId,
    user: UserId,
    side: Side,
    price: i64,
    rem: i64,
}

#[derive(Default)]
struct RefBook {
    resting: Vec<RefOrder>,
    arrivals: u64,
}

impl RefBook {
    fn level_total(&self, side: Side, price: i64) -> Qty {
        let n = self
            .resting
            .iter()
            .filter(|o| o.side == side && o.price == price)
            .map(|o| o.rem)
            .sum();
        Qty::new(n).unwrap()
    }

    fn apply(&mut self, body: &CommandBody) -> Vec<EventBody> {
        let mut ev = Vec::new();
        match body {
            CommandBody::NewOrder(o) => {
                if self.resting.iter().any(|r| r.id == o.order_id) {
                    return ev;
                }
                let valid = (MIN_PRICE_CENTS..=MAX_PRICE_CENTS).contains(&o.price_cents)
                    && (MIN_QTY..=MAX_QTY).contains(&o.quantity);
                if !valid {
                    ev.push(EventBody::OrderCanceled {
                        order_id: o.order_id,
                        user_id: o.user_id,
                        canceled_quantity: Qty::new(o.quantity).unwrap_or(Qty::ZERO),
                        reason: CancelReason::Invalid,
                    });
                    return ev;
                }
                ev.push(EventBody::OrderAccepted {
                    order_id: o.order_id,
                    user_id: o.user_id,
                    side: o.side,
                    price: Price::new(o.price_cents).unwrap(),
                    quantity: Qty::new(o.quantity).unwrap(),
                });
                let opp = o.side.opposite();
                let crosses = |px: i64| match o.side {
                    Side::Buy => px <= o.price_cents,
                    Side::Sell => px >= o.price_cents,
                };
                let mut rem = o.quantity;
                let mut self_trade = false;
                while rem > 0 && !self_trade {
                    let prices = self
                        .resting
                        .iter()
                        .filter(|r| r.side == opp && crosses(r.price))
                        .map(|r| r.price);
                    let best = match opp {
                        Side::Buy => prices.max(),
                        Side::Sell => prices.min(),
                    };
                    let Some(px) = best else { break };
                    let mut at: Vec<usize> = (0..self.resting.len())
                        .filter(|&i| self.resting[i].side == opp && self.resting[i].price == px)
                        .collect();
                    at.sort_by_key(|&i| self.resting[i].arrival);
                    let mut touched = false;
                    for i in at {
                        let r = &mut self.resting[i];
                        if r.user == o.user_id {
                            self_trade = true;
                            break;
                        }
                        let fill = rem.min(r.rem);
                        r.rem -= fill;
                        rem -= fill;
                        touched = true;
                        let (bo, bu, so, su) = match o.side {
                            Side::Buy => (o.order_id, o.user_id, r.id, r.user),
                            Side::Sell => (r.id, r.user, o.order_id, o.user_id),
                        };
                        ev.push(EventBody::Trade(TradeExecuted {
                            price: Price::new(px).unwrap(),
                            quantity: Qty::new(fill).unwrap(),
                            buy_order_id: bo,
                            sell_order_id: so,
                            buyer_id: bu,
                            seller_id: su,
                            aggressor_side: o.side,
                        }));
                        if rem == 0 {
                            break;
                        }
                    }
                    self.resting.retain(|r| r.rem > 0);
                    if touched {
                        ev.push(EventBody::BookChanged {
                            side: opp,
                            price: Price::new(px).unwrap(),
                            new_total_quantity: self.level_total(opp, px),
                        });
                    }
                }
                if rem == 0 {
                    return ev;
                }
                let immediate =
                    o.order_type == OrderType::Market || o.time_in_force == TimeInForce::Ioc;
                if self_trade || immediate {
                    ev.push(EventBody::OrderCanceled {
                        order_id: o.order_id,
                        user_id: o.user_id,
                        canceled_quantity: Qty::new(rem).unwrap(),
                        reason: if self_trade {
                            CancelReason::SelfTrade
                        } else {
                            CancelReason::IocRemainder
                        },
                    });
                    return ev;
                }
                self.arrivals += 1;
                self.resting.push(RefOrder {
                    arrival: self.arrivals,
                    id: o.order_id,
                    user: o.user_id,
                    side: o.side,
                    price: o.price_cents,
                    rem,
                });
                ev.push(EventBody::BookChanged {
                    side: o.side,
                    price: Price::new(o.price_cents).unwrap(),
                    new_total_quantity: self.level_total(o.side, o.price_cents),
                });
            }
            CommandBody::CancelOrder(c) => {
                let Some(i) = self.resting.iter().position(|r| r.id == c.order_id) else {
                    ev.push(EventBody::CancelRejected {
                        order_id: c.order_id,
                        user_id: c.user_id,
                        reason: CancelRejectReason::NotFound,
                    });
                    return ev;
                };
                if self.resting[i].user != c.user_id {
                    ev.push(EventBody::CancelRejected {
                        order_id: c.order_id,
                        user_id: c.user_id,
                        reason: CancelRejectReason::NotOwner,
                    });
                    return ev;
                }
                let r = self.resting.remove(i);
                ev.push(EventBody::OrderCanceled {
                    order_id: r.id,
                    user_id: r.user,
                    canceled_quantity: Qty::new(r.rem).unwrap(),
                    reason: CancelReason::User,
                });
                ev.push(EventBody::BookChanged {
                    side: r.side,
                    price: Price::new(r.price).unwrap(),
                    new_total_quantity: self.level_total(r.side, r.price),
                });
            }
        }
        ev
    }
}

// --------------------------------------------------------------- generators

#[derive(Debug, Clone)]
enum Op {
    New {
        user: u8,
        buy: bool,
        market: bool,
        ioc: bool,
        price: i64,
        qty: i64,
    },
    Cancel {
        target: prop::sample::Index,
        wrong_user: bool,
    },
}

fn op() -> impl Strategy<Value = Op> {
    let new = (
        0u8..4,
        any::<bool>(),
        prop::bool::weighted(0.1),
        prop::bool::weighted(0.2),
        // Prices cluster tightly so orders cross often; rarely out of range.
        prop_oneof![20 => 95i64..=105, 1 => Just(0i64)],
        prop_oneof![20 => 1i64..=20, 1 => Just(0i64)],
    )
        .prop_map(|(user, buy, market, ioc, price, qty)| Op::New {
            user,
            buy,
            market,
            ioc,
            price,
            qty,
        });
    let cancel = (any::<prop::sample::Index>(), prop::bool::weighted(0.1))
        .prop_map(|(target, wrong_user)| Op::Cancel { target, wrong_user });
    prop_oneof![3 => new, 1 => cancel]
}

/// Turns ops into concrete commands. Cancels target a previously placed order
/// (by its owner, or occasionally by someone else).
fn commands(ops: &[Op]) -> Vec<Command> {
    let mut placed: Vec<(OrderId, UserId)> = Vec::new();
    let mut cmds = Vec::new();
    for (i, op) in ops.iter().enumerate() {
        let n = i as u128 + 1;
        let body = match op {
            Op::New {
                user,
                buy,
                market,
                ioc,
                price,
                qty,
            } => {
                let order_id = OrderId::from_u128(n);
                let user_id = UserId::from_u128(*user as u128 + 1);
                placed.push((order_id, user_id));
                CommandBody::NewOrder(NewOrder {
                    order_id,
                    user_id,
                    side: if *buy { Side::Buy } else { Side::Sell },
                    order_type: if *market {
                        OrderType::Market
                    } else {
                        OrderType::Limit
                    },
                    time_in_force: if *ioc {
                        TimeInForce::Ioc
                    } else {
                        TimeInForce::Gtc
                    },
                    price_cents: *price,
                    quantity: *qty,
                })
            }
            Op::Cancel { target, wrong_user } => {
                let (order_id, owner) = if placed.is_empty() {
                    (OrderId::from_u128(u128::MAX), UserId::from_u128(1))
                } else {
                    placed[target.index(placed.len())]
                };
                let user_id = if *wrong_user {
                    UserId::from_u128(99)
                } else {
                    owner
                };
                CommandBody::CancelOrder(CancelOrder { order_id, user_id })
            }
        };
        cmds.push(Command {
            command_id: CommandId::from_u128(n),
            accepted_at_ns: n as i64,
            body,
        });
    }
    cmds
}

fn run(cmds: &[Command]) -> (OrderBook, Vec<BookEvent>) {
    let mut book = OrderBook::new(Symbol::new("ACME").unwrap());
    let mut out = Vec::new();
    for c in cmds {
        let _ = book.apply(c, &mut out);
    }
    (book, out)
}

// -------------------------------------------------------------------- tests

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    #[test]
    fn real_book_matches_reference_model(ops in prop::collection::vec(op(), 1..300)) {
        let cmds = commands(&ops);
        let mut book = OrderBook::new(Symbol::new("ACME").unwrap());
        let mut model = RefBook::default();
        let mut expected_seq = 1u64;

        for cmd in &cmds {
            let mut out = Vec::new();
            let _ = book.apply(cmd, &mut out);
            prop_assert!(book.check_invariants().is_ok(), "{:?}", book.check_invariants());

            let want = model.apply(&cmd.body);
            let got: Vec<_> = out.iter().map(|e| e.body.clone()).collect();
            prop_assert_eq!(&got, &want, "after {:?}", cmd);

            for e in &out {
                prop_assert_eq!(e.seq, expected_seq);
                prop_assert_eq!(e.command_id, cmd.command_id);
                prop_assert_eq!(e.event_time_ns, cmd.accepted_at_ns);
                expected_seq += 1;
            }
        }
    }

    #[test]
    fn every_share_is_accounted_for(ops in prop::collection::vec(op(), 1..300)) {
        let cmds = commands(&ops);
        let (book, events) = run(&cmds);

        let mut qty: HashMap<OrderId, (Side, i64)> = HashMap::new();
        let mut filled: HashMap<OrderId, i64> = HashMap::new();
        let mut canceled: HashMap<OrderId, i64> = HashMap::new();
        for e in &events {
            match &e.body {
                EventBody::OrderAccepted { order_id, side, quantity, .. } => {
                    qty.insert(*order_id, (*side, quantity.shares()));
                }
                EventBody::Trade(t) => {
                    *filled.entry(t.buy_order_id).or_default() += t.quantity.shares();
                    *filled.entry(t.sell_order_id).or_default() += t.quantity.shares();
                    prop_assert_ne!(t.buyer_id, t.seller_id, "self-trade happened");
                }
                EventBody::OrderCanceled { order_id, canceled_quantity, reason, .. }
                    if *reason != CancelReason::Invalid =>
                {
                    *canceled.entry(*order_id).or_default() += canceled_quantity.shares();
                }
                _ => {}
            }
        }
        for (id, (_, total)) in &qty {
            let resting = book.order(*id).map_or(0, |o| o.remaining.shares());
            let f = filled.get(id).copied().unwrap_or(0);
            let c = canceled.get(id).copied().unwrap_or(0);
            prop_assert_eq!(f + c + resting, *total, "order {}", id);
        }
    }

    #[test]
    fn trades_respect_both_limits(ops in prop::collection::vec(op(), 1..300)) {
        let cmds = commands(&ops);
        let limits: HashMap<OrderId, i64> = cmds
            .iter()
            .filter_map(|c| match &c.body {
                CommandBody::NewOrder(o) => Some((o.order_id, o.price_cents)),
                _ => None,
            })
            .collect();
        let (_, events) = run(&cmds);
        for e in &events {
            if let EventBody::Trade(t) = &e.body {
                let px = t.price.cents();
                prop_assert!(px <= limits[&t.buy_order_id], "buyer paid above limit");
                prop_assert!(px >= limits[&t.sell_order_id], "seller got below limit");
            }
        }
    }

    #[test]
    fn same_input_gives_identical_output(ops in prop::collection::vec(op(), 1..300)) {
        let cmds = commands(&ops);
        let (a_book, a) = run(&cmds);
        let (b_book, b) = run(&cmds);
        prop_assert_eq!(a, b);
        prop_assert_eq!(a_book.depth(Side::Buy), b_book.depth(Side::Buy));
        prop_assert_eq!(a_book.depth(Side::Sell), b_book.depth(Side::Sell));
    }
}
