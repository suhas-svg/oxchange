//! Market Data (spec: "Service specifications → Market Data"): builds the
//! public view of the market and streams it to browsers.
//!
//! Two halves, both reading `matching.events`:
//! - [`Recorder`]: shared consumer group. Records public trades and 1-minute
//!   candles in PostgreSQL and the last price and day volume in the Redis
//!   ticker, which Risk uses as the market-order reference price.
//! - [`Live`]: every copy, from the end of the topic. Keeps each stock's
//!   book in memory (loaded with `GetBook`) and streams book changes,
//!   trades and tickers over `/ws/public` ([`http`]).

pub mod http;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod live;
mod recorder;

pub use live::{BookSource, GrpcBooks, Live, MemBooks, Snapshot};
pub use recorder::{Recorder, day_volume};

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub const MATCHING_EVENTS_TOPIC: &str = "matching.events";
/// The recorder's consumer group (the live view uses no group).
pub const RECORDER_GROUP: &str = "market-data";
