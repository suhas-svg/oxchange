//! Conversions between the Kafka contracts (`events.v1`) and the engine's
//! types.
//!
//! - [`decode_command`] reads an `orders.commands` record. Anything that cannot
//!   be decoded (bad protobuf, missing body, bad ID, unknown enum) is an error.
//!   The Kafka loop sends those to the dead-letter queue and carries on: the
//!   same bytes always fail the same way, so replay still matches (spec:
//!   "Bad messages"). Out-of-range prices and quantities are *not* decode
//!   errors; they reach the book, which cancels the order so funds are released.
//! - [`encode_event`] builds the `matching.events` record, with `event_id` and,
//!   for trades, `trade_id` set to `{symbol}-{seq}`.
//!
//! [`encode_command`] and [`decode_event`] are the reverse directions, used by
//! Risk (it publishes commands, reads events) and by tests.
//!
//! prost encodes the same message to the same bytes every time, so a replay
//! republishes byte-identical events.

use domain::{CommandId, OrderId, OrderType, Price, Qty, Side, Symbol, TimeInForce, UserId};
use order_book::{
    BookEvent, CancelOrder, CancelReason, CancelRejectReason, Command, CommandBody, EventBody,
    NewOrder, TradeExecuted,
};
use prost::Message;
use proto_gen::events::v1 as pb;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CodecError {
    #[error("not a valid protobuf message: {0}")]
    Protobuf(String),
    #[error("message has no body")]
    MissingBody,
    #[error("{field} is not a valid UUID: {value:?}")]
    InvalidId { field: &'static str, value: String },
    #[error("invalid symbol {0:?}")]
    InvalidSymbol(String),
    #[error("{field} has unknown or unspecified value {value}")]
    InvalidEnum { field: &'static str, value: i32 },
    #[error("{field} is out of range: {value}")]
    OutOfRange { field: &'static str, value: i64 },
}

// ----------------------------------------------------------------- commands

/// Decodes one `orders.commands` record into its stock and command.
pub fn decode_command(bytes: &[u8]) -> Result<(Symbol, Command), CodecError> {
    let c = pb::Command::decode(bytes).map_err(|e| CodecError::Protobuf(e.to_string()))?;
    let symbol = Symbol::new(&c.symbol).map_err(|_| CodecError::InvalidSymbol(c.symbol.clone()))?;
    let command_id = CommandId(uuid("command_id", &c.command_id)?);
    let body = match c.body.ok_or(CodecError::MissingBody)? {
        pb::command::Body::NewOrder(o) => CommandBody::NewOrder(NewOrder {
            order_id: OrderId(uuid("order_id", &o.order_id)?),
            user_id: UserId(uuid("user_id", &o.user_id)?),
            side: side_from_pb("side", o.side)?,
            order_type: match pb::OrderType::try_from(o.order_type) {
                Ok(pb::OrderType::Limit) => OrderType::Limit,
                Ok(pb::OrderType::Market) => OrderType::Market,
                _ => return Err(bad_enum("order_type", o.order_type)),
            },
            time_in_force: match pb::TimeInForce::try_from(o.time_in_force) {
                Ok(pb::TimeInForce::Gtc) => TimeInForce::Gtc,
                Ok(pb::TimeInForce::Ioc) => TimeInForce::Ioc,
                _ => return Err(bad_enum("time_in_force", o.time_in_force)),
            },
            price_cents: o.price_cents,
            quantity: o.quantity,
        }),
        pb::command::Body::CancelOrder(o) => CommandBody::CancelOrder(CancelOrder {
            order_id: OrderId(uuid("order_id", &o.order_id)?),
            user_id: UserId(uuid("user_id", &o.user_id)?),
        }),
    };
    Ok((
        symbol,
        Command {
            command_id,
            accepted_at_ns: c.accepted_at_ns,
            body,
        },
    ))
}

/// Encodes a command as Risk publishes it to `orders.commands`.
pub fn encode_command(symbol: &Symbol, cmd: &Command) -> Vec<u8> {
    let body = match &cmd.body {
        CommandBody::NewOrder(o) => pb::command::Body::NewOrder(pb::NewOrder {
            order_id: o.order_id.to_string(),
            user_id: o.user_id.to_string(),
            side: side_to_pb(o.side) as i32,
            order_type: match o.order_type {
                OrderType::Limit => pb::OrderType::Limit,
                OrderType::Market => pb::OrderType::Market,
            } as i32,
            time_in_force: match o.time_in_force {
                TimeInForce::Gtc => pb::TimeInForce::Gtc,
                TimeInForce::Ioc => pb::TimeInForce::Ioc,
            } as i32,
            price_cents: o.price_cents,
            quantity: o.quantity,
        }),
        CommandBody::CancelOrder(o) => pb::command::Body::CancelOrder(pb::CancelOrder {
            order_id: o.order_id.to_string(),
            user_id: o.user_id.to_string(),
        }),
    };
    pb::Command {
        command_id: cmd.command_id.to_string(),
        symbol: symbol.as_str().to_owned(),
        accepted_at_ns: cmd.accepted_at_ns,
        body: Some(body),
    }
    .encode_to_vec()
}

// ------------------------------------------------------------------- events

/// Builds the `matching.events` message for one engine event.
pub fn event_to_pb(symbol: &Symbol, e: &BookEvent) -> pb::MatchingEvent {
    let event_id = symbol.event_id(e.seq);
    let body = match &e.body {
        EventBody::OrderAccepted {
            order_id,
            user_id,
            side,
            price,
            quantity,
        } => pb::matching_event::Body::OrderAccepted(pb::OrderAccepted {
            order_id: order_id.to_string(),
            user_id: user_id.to_string(),
            side: side_to_pb(*side) as i32,
            price_cents: price.cents(),
            quantity: quantity.shares(),
        }),
        EventBody::Trade(t) => pb::matching_event::Body::Trade(pb::TradeExecuted {
            // A trade's ID is its own event ID.
            trade_id: event_id.clone(),
            price_cents: t.price.cents(),
            quantity: t.quantity.shares(),
            buy_order_id: t.buy_order_id.to_string(),
            sell_order_id: t.sell_order_id.to_string(),
            buyer_id: t.buyer_id.to_string(),
            seller_id: t.seller_id.to_string(),
            aggressor_side: side_to_pb(t.aggressor_side) as i32,
        }),
        EventBody::OrderCanceled {
            order_id,
            user_id,
            canceled_quantity,
            reason,
        } => pb::matching_event::Body::OrderCanceled(pb::OrderCanceled {
            order_id: order_id.to_string(),
            user_id: user_id.to_string(),
            canceled_quantity: canceled_quantity.shares(),
            reason: match reason {
                CancelReason::User => pb::CancelReason::User,
                CancelReason::IocRemainder => pb::CancelReason::IocRemainder,
                CancelReason::SelfTrade => pb::CancelReason::SelfTrade,
                CancelReason::Invalid => pb::CancelReason::Invalid,
            } as i32,
        }),
        EventBody::CancelRejected {
            order_id,
            user_id,
            reason,
        } => pb::matching_event::Body::CancelRejected(pb::CancelRejected {
            order_id: order_id.to_string(),
            user_id: user_id.to_string(),
            reason: match reason {
                CancelRejectReason::NotFound => pb::CancelRejectReason::NotFound,
                CancelRejectReason::NotOwner => pb::CancelRejectReason::NotOwner,
            } as i32,
        }),
        EventBody::BookChanged {
            side,
            price,
            new_total_quantity,
        } => pb::matching_event::Body::BookChanged(pb::BookChanged {
            side: side_to_pb(*side) as i32,
            price_cents: price.cents(),
            new_total_quantity: new_total_quantity.shares(),
        }),
    };
    pb::MatchingEvent {
        event_id,
        symbol: symbol.as_str().to_owned(),
        seq: e.seq,
        event_time_ns: e.event_time_ns,
        command_id: e.command_id.to_string(),
        body: Some(body),
    }
}

/// Encodes one engine event as a `matching.events` record.
pub fn encode_event(symbol: &Symbol, e: &BookEvent) -> Vec<u8> {
    event_to_pb(symbol, e).encode_to_vec()
}

/// Decodes a `matching.events` record, as consumers read it.
pub fn decode_event(bytes: &[u8]) -> Result<(Symbol, BookEvent), CodecError> {
    let m = pb::MatchingEvent::decode(bytes).map_err(|e| CodecError::Protobuf(e.to_string()))?;
    let symbol = Symbol::new(&m.symbol).map_err(|_| CodecError::InvalidSymbol(m.symbol.clone()))?;
    let body = match m.body.ok_or(CodecError::MissingBody)? {
        pb::matching_event::Body::OrderAccepted(o) => EventBody::OrderAccepted {
            order_id: OrderId(uuid("order_id", &o.order_id)?),
            user_id: UserId(uuid("user_id", &o.user_id)?),
            side: side_from_pb("side", o.side)?,
            price: price("price_cents", o.price_cents)?,
            quantity: qty("quantity", o.quantity)?,
        },
        pb::matching_event::Body::Trade(t) => EventBody::Trade(TradeExecuted {
            price: price("price_cents", t.price_cents)?,
            quantity: qty("quantity", t.quantity)?,
            buy_order_id: OrderId(uuid("buy_order_id", &t.buy_order_id)?),
            sell_order_id: OrderId(uuid("sell_order_id", &t.sell_order_id)?),
            buyer_id: UserId(uuid("buyer_id", &t.buyer_id)?),
            seller_id: UserId(uuid("seller_id", &t.seller_id)?),
            aggressor_side: side_from_pb("aggressor_side", t.aggressor_side)?,
        }),
        pb::matching_event::Body::OrderCanceled(c) => EventBody::OrderCanceled {
            order_id: OrderId(uuid("order_id", &c.order_id)?),
            user_id: UserId(uuid("user_id", &c.user_id)?),
            canceled_quantity: qty("canceled_quantity", c.canceled_quantity)?,
            reason: match pb::CancelReason::try_from(c.reason) {
                Ok(pb::CancelReason::User) => CancelReason::User,
                Ok(pb::CancelReason::IocRemainder) => CancelReason::IocRemainder,
                Ok(pb::CancelReason::SelfTrade) => CancelReason::SelfTrade,
                Ok(pb::CancelReason::Invalid) => CancelReason::Invalid,
                _ => return Err(bad_enum("reason", c.reason)),
            },
        },
        pb::matching_event::Body::CancelRejected(c) => EventBody::CancelRejected {
            order_id: OrderId(uuid("order_id", &c.order_id)?),
            user_id: UserId(uuid("user_id", &c.user_id)?),
            reason: match pb::CancelRejectReason::try_from(c.reason) {
                Ok(pb::CancelRejectReason::NotFound) => CancelRejectReason::NotFound,
                Ok(pb::CancelRejectReason::NotOwner) => CancelRejectReason::NotOwner,
                _ => return Err(bad_enum("reason", c.reason)),
            },
        },
        pb::matching_event::Body::BookChanged(b) => EventBody::BookChanged {
            side: side_from_pb("side", b.side)?,
            price: price("price_cents", b.price_cents)?,
            new_total_quantity: qty("new_total_quantity", b.new_total_quantity)?,
        },
    };
    Ok((
        symbol,
        BookEvent {
            seq: m.seq,
            event_time_ns: m.event_time_ns,
            command_id: CommandId(uuid("command_id", &m.command_id)?),
            body,
        },
    ))
}

// ------------------------------------------------------------------ helpers

fn uuid(field: &'static str, s: &str) -> Result<Uuid, CodecError> {
    Uuid::parse_str(s).map_err(|_| CodecError::InvalidId {
        field,
        value: s.to_owned(),
    })
}

fn bad_enum(field: &'static str, value: i32) -> CodecError {
    CodecError::InvalidEnum { field, value }
}

fn side_from_pb(field: &'static str, v: i32) -> Result<Side, CodecError> {
    match pb::Side::try_from(v) {
        Ok(pb::Side::Buy) => Ok(Side::Buy),
        Ok(pb::Side::Sell) => Ok(Side::Sell),
        _ => Err(bad_enum(field, v)),
    }
}

fn side_to_pb(s: Side) -> pb::Side {
    match s {
        Side::Buy => pb::Side::Buy,
        Side::Sell => pb::Side::Sell,
    }
}

fn price(field: &'static str, v: i64) -> Result<Price, CodecError> {
    Price::new(v).map_err(|_| CodecError::OutOfRange { field, value: v })
}

fn qty(field: &'static str, v: i64) -> Result<Qty, CodecError> {
    Qty::new(v).map_err(|_| CodecError::OutOfRange { field, value: v })
}
