//! Deterministic limit order book for one stock.
//!
//! [`OrderBook::apply`] takes one [`Command`] and appends the resulting
//! [`BookEvent`]s. It never reads the clock, a random number, or the network,
//! so the same commands always produce byte-identical events. That property
//! is what makes crash recovery by replay possible.
//!
//! Matching rules (spec: "Core trading rules → Matching rules"):
//! 1. Price first: the highest bid and the lowest ask trade first.
//! 2. Time second: at the same price, the earliest order trades first.
//! 3. Resting price wins: a trade happens at the waiting order's price.
//! 4. Keep matching until filled or no acceptable price remains.
//! 5. No self-trades: the incoming remainder is canceled instead.
//! 6. Never crossed: after every command, best bid < best ask.
//! 7. No outside inputs: timestamps come from the command.

mod book;
mod command;
mod event;

pub use book::{ApplyOutcome, BookState, OrderBook, RestingView, RestoreError};
pub use command::{CancelOrder, Command, CommandBody, NewOrder};
pub use event::{BookEvent, CancelReason, CancelRejectReason, EventBody, TradeExecuted};
