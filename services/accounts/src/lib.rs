//! Accounts: the single source of truth for balances (spec: "Service
//! specifications → Accounts").
//!
//! - Every balance change is one PostgreSQL transaction of double-entry
//!   postings that add up to zero per asset ([`ledger`]).
//! - Every write takes an idempotency key. A retry returns the first
//!   response; the same key with different data is refused.
//! - Every transaction locks balance rows in ascending ID order, and always
//!   locks reservations before balances, so transactions cannot deadlock.

pub mod audit;
pub mod grpc;
mod ledger;
mod money;
mod users;

use sqlx::PgPool;

pub use auth::{TokenSigner, TokenVerifier};
pub use money::{MAX_TRANSFER_UNITS, USD};

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub const ACCOUNTS_EVENTS_TOPIC: &str = "accounts.events";
/// Advisory lock ID for the Accounts outbox relay ("ACCT" in ASCII).
pub const OUTBOX_LOCK_KEY: i64 = 0x4143_4354;

/// Failures, each with the spec's API error code.
#[derive(Debug, thiserror::Error)]
pub enum AccountsError {
    #[error("{0}")]
    Validation(String),
    #[error("available balance is too low")]
    InsufficientFunds,
    #[error("{0} not found")]
    NotFound(&'static str),
    #[error("{0}")]
    AlreadyExists(&'static str),
    #[error("idempotency key was already used with different data")]
    IdempotencyConflict,
    /// A Release for this order arrived first, so a late Reserve must fail
    /// instead of locking funds forever.
    #[error("reservation was voided by an earlier release")]
    ReservationVoided,
    #[error("not authenticated")]
    Unauthenticated,
    /// A request that can only come from a bug elsewhere (for example
    /// settling more than was reserved). Settlement halts and alerts on it.
    #[error("invariant violated: {0}")]
    Invariant(String),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

impl AccountsError {
    /// The spec's error code ("API → Errors"), or our own for internal ones.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Validation(_) => "VALIDATION_FAILED",
            Self::InsufficientFunds => "INSUFFICIENT_FUNDS",
            Self::NotFound(_) => "NOT_FOUND",
            Self::AlreadyExists(_) => "ALREADY_EXISTS",
            Self::IdempotencyConflict => "IDEMPOTENCY_CONFLICT",
            Self::ReservationVoided => "RESERVATION_VOIDED",
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::Invariant(_) => "INVARIANT_VIOLATED",
            Self::Db(_) => "SERVICE_UNAVAILABLE",
        }
    }
}

pub type Result<T, E = AccountsError> = std::result::Result<T, E>;

/// The Accounts service logic over its database.
pub struct Accounts {
    pool: PgPool,
    signer: TokenSigner,
}

impl Accounts {
    pub fn new(pool: PgPool, signer: TokenSigner) -> Self {
        Self { pool, signer }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Verifies the access tokens this service issues (the Gateway builds
    /// its own from the public key).
    pub fn token_verifier(&self) -> TokenVerifier {
        self.signer.verifier()
    }
}

pub(crate) fn now_ns() -> i64 {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970");
    i64::try_from(d.as_nanos()).expect("time fits i64 until 2262")
}
