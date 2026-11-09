#![allow(dead_code)]

use domain::{CommandId, OrderId, OrderType, Side, Symbol, TimeInForce, UserId};
use order_book::{BookEvent, CancelOrder, Command, CommandBody, NewOrder, OrderBook};

pub fn user(n: u128) -> UserId {
    UserId::from_u128(n)
}

pub fn oid(n: u128) -> OrderId {
    OrderId::from_u128(n)
}

/// Drives a book with terse helpers and keeps every event for assertions.
pub struct Harness {
    pub book: OrderBook,
    next_cmd: u128,
    pub events: Vec<BookEvent>,
}

impl Harness {
    pub fn new() -> Self {
        Self {
            book: OrderBook::new(Symbol::new("ACME").unwrap()),
            next_cmd: 1,
            events: Vec::new(),
        }
    }

    fn run(&mut self, body: CommandBody) -> Vec<BookEvent> {
        let id = self.next_cmd;
        self.next_cmd += 1;
        let cmd = Command {
            command_id: CommandId::from_u128(id),
            accepted_at_ns: id as i64 * 1_000,
            body,
        };
        let mut out = Vec::new();
        let _ = self.book.apply(&cmd, &mut out);
        self.book.check_invariants().unwrap();
        self.events.extend(out.iter().cloned());
        out
    }

    #[allow(clippy::too_many_arguments)]
    pub fn order(
        &mut self,
        id: u128,
        u: u128,
        side: Side,
        order_type: OrderType,
        tif: TimeInForce,
        price: i64,
        qty: i64,
    ) -> Vec<BookEvent> {
        self.run(CommandBody::NewOrder(NewOrder {
            order_id: oid(id),
            user_id: user(u),
            side,
            order_type,
            time_in_force: tif,
            price_cents: price,
            quantity: qty,
        }))
    }

    pub fn gtc(&mut self, id: u128, u: u128, side: Side, price: i64, qty: i64) -> Vec<BookEvent> {
        self.order(id, u, side, OrderType::Limit, TimeInForce::Gtc, price, qty)
    }

    pub fn ioc(&mut self, id: u128, u: u128, side: Side, price: i64, qty: i64) -> Vec<BookEvent> {
        self.order(id, u, side, OrderType::Limit, TimeInForce::Ioc, price, qty)
    }

    pub fn cancel(&mut self, id: u128, u: u128) -> Vec<BookEvent> {
        self.run(CommandBody::CancelOrder(CancelOrder {
            order_id: oid(id),
            user_id: user(u),
        }))
    }
}
