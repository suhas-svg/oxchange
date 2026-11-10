//! Notifications (spec: "Service specifications → Notifications"): stores
//! user-facing messages and pushes them to the correct browser.
//!
//! - [`Notifier`] turns fills and cancels (`matching.events`), settlements
//!   (`settlement.events`), and deposits and withdrawals (`accounts.events`)
//!   into rows of `notifications`, one per affected user, then publishes to
//!   the Redis channel `user:{id}`.
//! - Every copy listens to those channels ([`wake::listen`]). The copy
//!   holding that user's WebSocket reads the new rows from PostgreSQL and
//!   sends them ([`http`]).
//!
//! The Redis message is only a wake-up; the rows themselves come from
//! PostgreSQL in ID order. So a lost, repeated or reordered Redis message
//! can delay a notification but never drop, repeat or reorder one, and
//! while Redis is down each socket falls back to checking every
//! [`wake::FALLBACK_POLL`].

pub mod http;
mod hub;
#[cfg(feature = "kafka")]
pub mod kafka;
mod notifier;
pub mod wake;

pub use hub::{Hub, Registration};
pub use notifier::{Failure, Notification, Notifier};

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub const MATCHING_EVENTS_TOPIC: &str = "matching.events";
pub const SETTLEMENT_EVENTS_TOPIC: &str = "settlement.events";
pub const ACCOUNTS_EVENTS_TOPIC: &str = "accounts.events";
pub const TOPICS: [&str; 3] = [
    MATCHING_EVENTS_TOPIC,
    SETTLEMENT_EVENTS_TOPIC,
    ACCOUNTS_EVENTS_TOPIC,
];
pub const GROUP: &str = "notifications";
