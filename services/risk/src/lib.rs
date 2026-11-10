//! Risk: accepts orders, checks them, reserves funds, and tracks each
//! order's status (spec: "Service specifications → Risk" and "Order
//! placement saga").
//!
//! - [`Risk::place_order`] runs saga steps 1 to 3: save PENDING_RISK, reserve
//!   with Accounts, then save ACCEPTED and the outbox command together.
//! - [`Risk::apply_event`] reads `matching.events`, updates order status, and
//!   releases funds for exactly the canceled quantity.
//! - [`Risk::sweep`] voids orders stuck in PENDING_RISK.

pub mod accounts_api;
mod events;
pub mod grpc;
mod orders;
mod sweeper;

use proto_gen::events::v1 as ev;
use proto_gen::risk::v1 as pb;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

pub use accounts_api::{AccountsApi, GrpcAccounts};

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub const ORDERS_COMMANDS_TOPIC: &str = "orders.commands";
pub const MATCHING_EVENTS_TOPIC: &str = "matching.events";
/// Advisory lock ID for Risk's outbox relay ("RISK" in ASCII).
pub const OUTBOX_LOCK_KEY: i64 = 0x5249_534B;

/// Named crash point between a successful Reserve and saving ACCEPTED.
pub const CRASH_AFTER_RESERVE: &str = "risk::after_reserve";

/// Spec: "The user has fewer than 200 open orders".
pub const MAX_OPEN_ORDERS: i64 = 200;
/// Spec: "Risk returns SERVICE_UNAVAILABLE when more than 50,000 commands are unpublished".
pub const MAX_UNPUBLISHED: i64 = 50_000;
/// Spec: the sweeper voids PENDING_RISK orders older than 30 seconds.
pub const PENDING_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Failures, each with the spec's API error code.
#[derive(Debug, thiserror::Error)]
pub enum RiskError {
    #[error("{0}")]
    Validation(String),
    #[error("this stock is not open for trading")]
    SymbolNotTrading,
    #[error("{0}")]
    RiskLimit(String),
    #[error("available balance is too low for this order")]
    InsufficientFunds,
    /// Accounts refused the reservation for another reason; the order is REJECTED.
    #[error("order rejected: {0}")]
    Rejected(String),
    #[error("order not found")]
    NotFound,
    #[error("{0}")]
    Unavailable(String),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

impl RiskError {
    pub fn code(&self) -> &str {
        match self {
            Self::Validation(_) => "VALIDATION_FAILED",
            Self::SymbolNotTrading => "SYMBOL_NOT_TRADING",
            Self::RiskLimit(_) => "RISK_LIMIT_EXCEEDED",
            Self::InsufficientFunds => "INSUFFICIENT_FUNDS",
            Self::Rejected(_) => "ORDER_REJECTED",
            Self::NotFound => "NOT_FOUND",
            Self::Unavailable(_) | Self::Db(_) => "SERVICE_UNAVAILABLE",
        }
    }
}

pub type Result<T, E = RiskError> = std::result::Result<T, E>;

pub struct Risk<A> {
    pool: PgPool,
    accounts: A,
    /// Reference prices for market orders; without them market orders are
    /// rejected.
    prices: Option<std::sync::Arc<ticker::Store>>,
}

impl<A: AccountsApi> Risk<A> {
    pub fn new(pool: PgPool, accounts: A) -> Self {
        Self {
            pool,
            accounts,
            prices: None,
        }
    }

    /// Reads market-order reference prices from the Redis ticker.
    pub fn with_prices(mut self, prices: std::sync::Arc<ticker::Store>) -> Self {
        self.prices = Some(prices);
        self
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// An `orders` row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct OrderRow {
    pub order_id: Uuid,
    pub user_id: Uuid,
    pub client_order_id: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub time_in_force: String,
    pub limit_price_cents: i64,
    pub quantity: i64,
    pub filled_quantity: i64,
    pub canceled_quantity: i64,
    pub status: String,
    pub reject_reason: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

pub(crate) const ORDER_COLUMNS: &str = "order_id, user_id, client_order_id, symbol, side, order_type, \
     time_in_force, limit_price_cents, quantity, filled_quantity, canceled_quantity, status, \
     reject_reason, created_at, updated_at";

/// Statuses in which an order can still trade (plus PENDING_RISK, which
/// counts toward the open-order limit while it is in flight).
pub(crate) const OPEN_STATUSES: [&str; 3] = ["ACCEPTED", "OPEN", "PARTIALLY_FILLED"];

fn ns(t: OffsetDateTime) -> i64 {
    i64::try_from(t.unix_timestamp_nanos()).unwrap_or(i64::MAX)
}

impl OrderRow {
    pub fn to_pb(&self) -> pb::Order {
        let side = match self.side.as_str() {
            "BUY" => ev::Side::Buy,
            _ => ev::Side::Sell,
        };
        let order_type = match self.order_type.as_str() {
            "LIMIT" => ev::OrderType::Limit,
            _ => ev::OrderType::Market,
        };
        let tif = match self.time_in_force.as_str() {
            "GTC" => ev::TimeInForce::Gtc,
            _ => ev::TimeInForce::Ioc,
        };
        let status = match self.status.as_str() {
            "PENDING_RISK" => pb::OrderStatus::PendingRisk,
            "ACCEPTED" => pb::OrderStatus::Accepted,
            "OPEN" => pb::OrderStatus::Open,
            "PARTIALLY_FILLED" => pb::OrderStatus::PartiallyFilled,
            "FILLED" => pb::OrderStatus::Filled,
            "CANCELED" => pb::OrderStatus::Canceled,
            "REJECTED" => pb::OrderStatus::Rejected,
            _ => pb::OrderStatus::Unspecified,
        };
        pb::Order {
            order_id: self.order_id.to_string(),
            client_order_id: self.client_order_id.clone(),
            user_id: self.user_id.to_string(),
            symbol: self.symbol.clone(),
            side: side as i32,
            order_type: order_type as i32,
            time_in_force: tif as i32,
            limit_price_cents: self.limit_price_cents,
            quantity: self.quantity,
            filled_quantity: self.filled_quantity,
            canceled_quantity: self.canceled_quantity,
            status: status as i32,
            reject_reason: self.reject_reason.clone().unwrap_or_default(),
            created_at_ns: ns(self.created_at),
            updated_at_ns: ns(self.updated_at),
        }
    }
}

pub(crate) fn now_ns() -> i64 {
    ns(OffsetDateTime::now_utc())
}
