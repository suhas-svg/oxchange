//! Contract codec tests: round trips, determinism, malformed input, and the
//! spec's wire field numbers.

use domain::{CommandId, OrderId, OrderType, Side, Symbol, TimeInForce, UserId};
use matching::codec::{CodecError, decode_command, decode_event, encode_command, encode_event};
use matching::{DEFAULT_DEDUP_CAPACITY, Engine};
use order_book::{BookEvent, CancelOrder, Command, CommandBody, EventBody, NewOrder};
use proptest::prelude::*;
use prost::Message;
use proto_gen::events::v1 as pb;

fn sym(s: &str) -> Symbol {
    Symbol::new(s).unwrap()
}

prop_compose! {
    fn command()(
        id in any::<u128>(),
        order in any::<u128>(),
        user in any::<u128>(),
        at in any::<i64>(),
        cancel in any::<bool>(),
        buy in any::<bool>(),
        market in any::<bool>(),
        ioc in any::<bool>(),
        // Raw values: out-of-range numbers must survive decoding untouched.
        price in any::<i64>(),
        qty in any::<i64>(),
    ) -> Command {
        let body = if cancel {
            CommandBody::CancelOrder(CancelOrder {
                order_id: OrderId::from_u128(order),
                user_id: UserId::from_u128(user),
            })
        } else {
            CommandBody::NewOrder(NewOrder {
                order_id: OrderId::from_u128(order),
                user_id: UserId::from_u128(user),
                side: if buy { Side::Buy } else { Side::Sell },
                order_type: if market { OrderType::Market } else { OrderType::Limit },
                time_in_force: if ioc { TimeInForce::Ioc } else { TimeInForce::Gtc },
                price_cents: price,
                quantity: qty,
            })
        };
        Command { command_id: CommandId::from_u128(id), accepted_at_ns: at, body }
    }
}

/// Real engine output for a burst of crossing orders on two stocks.
fn engine_events(n: u64) -> Vec<(Symbol, BookEvent)> {
    let mut engine = Engine::new(0, DEFAULT_DEDUP_CAPACITY);
    let syms = [sym("ACME"), sym("BOLT")];
    let mut all = Vec::new();
    for i in 0..n {
        let s = &syms[(i % 2) as usize];
        let body = if i % 7 == 6 {
            CommandBody::CancelOrder(CancelOrder {
                order_id: OrderId::from_u128(u128::from(i) - 4),
                user_id: UserId::from_u128(u128::from(i % 5)),
            })
        } else {
            CommandBody::NewOrder(NewOrder {
                order_id: OrderId::from_u128(u128::from(i)),
                user_id: UserId::from_u128(u128::from(i % 5)),
                side: if i % 3 == 0 { Side::Sell } else { Side::Buy },
                order_type: OrderType::Limit,
                time_in_force: if i % 4 == 0 {
                    TimeInForce::Ioc
                } else {
                    TimeInForce::Gtc
                },
                price_cents: 1000 + (i % 9) as i64 - 4,
                quantity: 1 + (i % 13) as i64,
            })
        };
        let cmd = Command {
            command_id: CommandId::from_u128(u128::from(i) + 1),
            accepted_at_ns: i as i64,
            body,
        };
        let mut out = Vec::new();
        engine.process(i, s, &cmd, &mut out).unwrap();
        all.extend(out.into_iter().map(|e| (s.clone(), e)));
    }
    all
}

proptest! {
    #[test]
    fn command_round_trips(cmd in command(), s in "[A-Z]{1,8}") {
        let symbol = sym(&s);
        let bytes = encode_command(&symbol, &cmd);
        prop_assert_eq!(decode_command(&bytes), Ok((symbol, cmd)));
    }

    #[test]
    fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        let _ = decode_command(&bytes);
        let _ = decode_event(&bytes);
    }
}

#[test]
fn every_engine_event_round_trips() {
    let events = engine_events(2_000);
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e.body, EventBody::Trade(_)))
    );
    for (s, e) in &events {
        let bytes = encode_event(s, e);
        assert_eq!(decode_event(&bytes).as_ref(), Ok(&(s.clone(), e.clone())));
    }
}

#[test]
fn event_and_trade_ids_are_symbol_dash_seq() {
    for (s, e) in engine_events(500) {
        let m = pb::MatchingEvent::decode(encode_event(&s, &e).as_slice()).unwrap();
        assert_eq!(m.event_id, format!("{s}-{}", e.seq));
        if let Some(pb::matching_event::Body::Trade(t)) = m.body {
            assert_eq!(t.trade_id, m.event_id);
        }
    }
}

#[test]
fn replay_republishes_byte_identical_records() {
    let a: Vec<Vec<u8>> = engine_events(1_000)
        .iter()
        .map(|(s, e)| encode_event(s, e))
        .collect();
    let b: Vec<Vec<u8>> = engine_events(1_000)
        .iter()
        .map(|(s, e)| encode_event(s, e))
        .collect();
    assert_eq!(a, b);
}

// ------------------------------------------------------------ malformed input

fn valid_new_order() -> pb::Command {
    pb::Command {
        command_id: CommandId::from_u128(1).to_string(),
        symbol: "ACME".into(),
        accepted_at_ns: 5,
        body: Some(pb::command::Body::NewOrder(pb::NewOrder {
            order_id: OrderId::from_u128(2).to_string(),
            user_id: UserId::from_u128(3).to_string(),
            side: pb::Side::Buy as i32,
            order_type: pb::OrderType::Limit as i32,
            time_in_force: pb::TimeInForce::Gtc as i32,
            price_cents: 1000,
            quantity: 10,
        })),
    }
}

fn decode_pb(c: &pb::Command) -> Result<(Symbol, Command), CodecError> {
    decode_command(&c.encode_to_vec())
}

fn with_order(f: impl FnOnce(&mut pb::NewOrder)) -> pb::Command {
    let mut c = valid_new_order();
    if let Some(pb::command::Body::NewOrder(o)) = &mut c.body {
        f(o);
    }
    c
}

#[test]
fn valid_command_decodes() {
    assert!(decode_pb(&valid_new_order()).is_ok());
}

#[test]
fn malformed_commands_are_rejected_for_the_dead_letter_queue() {
    let mut no_body = valid_new_order();
    no_body.body = None;
    assert_eq!(decode_pb(&no_body), Err(CodecError::MissingBody));

    let mut bad_symbol = valid_new_order();
    bad_symbol.symbol = "acme".into();
    assert!(matches!(
        decode_pb(&bad_symbol),
        Err(CodecError::InvalidSymbol(_))
    ));

    let mut bad_id = valid_new_order();
    bad_id.command_id = "not-a-uuid".into();
    assert!(matches!(
        decode_pb(&bad_id),
        Err(CodecError::InvalidId {
            field: "command_id",
            ..
        })
    ));

    let unspecified_side = with_order(|o| o.side = pb::Side::Unspecified as i32);
    assert_eq!(
        decode_pb(&unspecified_side),
        Err(CodecError::InvalidEnum {
            field: "side",
            value: 0
        })
    );

    let unknown_tif = with_order(|o| o.time_in_force = 99);
    assert_eq!(
        decode_pb(&unknown_tif),
        Err(CodecError::InvalidEnum {
            field: "time_in_force",
            value: 99
        })
    );

    assert!(matches!(
        decode_command(&[0xFF, 0xFF]),
        Err(CodecError::Protobuf(_))
    ));
}

#[test]
fn out_of_range_numbers_pass_through_to_the_book() {
    // Not a decode error: the book must see it and cancel it, so Risk
    // releases the order's funds.
    let c = with_order(|o| {
        o.price_cents = 0;
        o.quantity = -5;
    });
    let (_, cmd) = decode_pb(&c).unwrap();
    let CommandBody::NewOrder(o) = cmd.body else {
        panic!("expected a new order")
    };
    assert_eq!((o.price_cents, o.quantity), (0, -5));
}

// ------------------------------------------------- wire numbers from the spec

/// The first byte of a message with exactly one field set is that field's
/// tag: `(field_number << 3) | wire_type`. Wire type 0 is varint, 2 is
/// length-delimited.
fn tag(field: u8, wire_type: u8) -> u8 {
    (field << 3) | wire_type
}

#[test]
fn command_field_numbers_match_the_spec() {
    let first = |c: pb::Command| c.encode_to_vec()[0];
    assert_eq!(
        first(pb::Command {
            command_id: "x".into(),
            ..Default::default()
        }),
        tag(1, 2)
    );
    assert_eq!(
        first(pb::Command {
            symbol: "x".into(),
            ..Default::default()
        }),
        tag(2, 2)
    );
    assert_eq!(
        first(pb::Command {
            accepted_at_ns: 1,
            ..Default::default()
        }),
        tag(3, 0)
    );
    let new_order = pb::Command {
        body: Some(pb::command::Body::NewOrder(Default::default())),
        ..Default::default()
    };
    assert_eq!(first(new_order), tag(10, 2));
    let cancel = pb::Command {
        body: Some(pb::command::Body::CancelOrder(Default::default())),
        ..Default::default()
    };
    assert_eq!(first(cancel), tag(11, 2));
}

#[test]
fn matching_event_field_numbers_match_the_spec() {
    use pb::matching_event::Body;
    let first = |e: pb::MatchingEvent| e.encode_to_vec()[0];
    let with = |b: Body| pb::MatchingEvent {
        body: Some(b),
        ..Default::default()
    };
    assert_eq!(
        first(pb::MatchingEvent {
            event_id: "x".into(),
            ..Default::default()
        }),
        tag(1, 2)
    );
    assert_eq!(
        first(pb::MatchingEvent {
            symbol: "x".into(),
            ..Default::default()
        }),
        tag(2, 2)
    );
    assert_eq!(
        first(pb::MatchingEvent {
            seq: 1,
            ..Default::default()
        }),
        tag(3, 0)
    );
    assert_eq!(
        first(pb::MatchingEvent {
            event_time_ns: 1,
            ..Default::default()
        }),
        tag(4, 0)
    );
    assert_eq!(
        first(pb::MatchingEvent {
            command_id: "x".into(),
            ..Default::default()
        }),
        tag(5, 2)
    );
    assert_eq!(
        first(with(Body::OrderAccepted(Default::default()))),
        tag(10, 2)
    );
    assert_eq!(first(with(Body::Trade(Default::default()))), tag(11, 2));
    assert_eq!(
        first(with(Body::OrderCanceled(Default::default()))),
        tag(12, 2)
    );
    assert_eq!(
        first(with(Body::CancelRejected(Default::default()))),
        tag(13, 2)
    );
    assert_eq!(
        first(with(Body::BookChanged(Default::default()))),
        tag(14, 2)
    );
}

#[test]
fn trade_and_cancel_field_numbers_match_the_spec() {
    let t = |m: pb::TradeExecuted| m.encode_to_vec()[0];
    assert_eq!(
        t(pb::TradeExecuted {
            trade_id: "x".into(),
            ..Default::default()
        }),
        tag(1, 2)
    );
    assert_eq!(
        t(pb::TradeExecuted {
            price_cents: 1,
            ..Default::default()
        }),
        tag(2, 0)
    );
    assert_eq!(
        t(pb::TradeExecuted {
            quantity: 1,
            ..Default::default()
        }),
        tag(3, 0)
    );
    assert_eq!(
        t(pb::TradeExecuted {
            buy_order_id: "x".into(),
            ..Default::default()
        }),
        tag(4, 2)
    );
    assert_eq!(
        t(pb::TradeExecuted {
            sell_order_id: "x".into(),
            ..Default::default()
        }),
        tag(5, 2)
    );
    assert_eq!(
        t(pb::TradeExecuted {
            buyer_id: "x".into(),
            ..Default::default()
        }),
        tag(6, 2)
    );
    assert_eq!(
        t(pb::TradeExecuted {
            seller_id: "x".into(),
            ..Default::default()
        }),
        tag(7, 2)
    );
    assert_eq!(
        t(pb::TradeExecuted {
            aggressor_side: 1,
            ..Default::default()
        }),
        tag(8, 0)
    );

    let c = |m: pb::OrderCanceled| m.encode_to_vec()[0];
    assert_eq!(
        c(pb::OrderCanceled {
            canceled_quantity: 1,
            ..Default::default()
        }),
        tag(3, 0)
    );
    assert_eq!(
        c(pb::OrderCanceled {
            reason: 1,
            ..Default::default()
        }),
        tag(4, 0)
    );
    let b = |m: pb::BookChanged| m.encode_to_vec()[0];
    assert_eq!(
        b(pb::BookChanged {
            new_total_quantity: 1,
            ..Default::default()
        }),
        tag(3, 0)
    );
}
