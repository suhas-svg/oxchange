//! Oxchange domain types.
//!
//! All money and quantities are integers (spec: "Core trading rules → Numbers"):
//! - [`Price`] is whole cents per share.
//! - [`Qty`] is whole shares.
//! - [`Cents`] is an amount of money (for example price × quantity).
//!
//! Separate newtypes make the compiler reject code that mixes them up, and every
//! arithmetic operation is checked so overflow is an error instead of a wrong balance.
//! This crate does no I/O.

use std::fmt;

use thiserror::Error;
use uuid::Uuid;

/// Smallest accepted limit price, in cents.
pub const MIN_PRICE_CENTS: i64 = 1;
/// Largest accepted limit price, in cents ($1,000,000.00).
pub const MAX_PRICE_CENTS: i64 = 100_000_000;
/// Smallest accepted order quantity, in shares.
pub const MIN_QTY: i64 = 1;
/// Largest accepted order quantity, in shares.
pub const MAX_QTY: i64 = 1_000_000;
/// Largest order value Risk accepts, in cents ($1,000,000.00).
pub const MAX_ORDER_VALUE_CENTS: i64 = 100_000_000;

/// Why a value was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DomainError {
    #[error("price {0} cents is outside {MIN_PRICE_CENTS}..={MAX_PRICE_CENTS}")]
    PriceOutOfRange(i64),
    #[error("quantity {0} is outside {MIN_QTY}..={MAX_QTY}")]
    QtyOutOfRange(i64),
    #[error("arithmetic overflow")]
    Overflow,
    #[error("result would be negative")]
    Negative,
}

/// A limit price in whole cents per share. Always within the accepted range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(i64);

impl Price {
    /// Creates a price, refusing values outside `MIN_PRICE_CENTS..=MAX_PRICE_CENTS`.
    pub const fn new(cents: i64) -> Result<Self, DomainError> {
        if cents < MIN_PRICE_CENTS || cents > MAX_PRICE_CENTS {
            Err(DomainError::PriceOutOfRange(cents))
        } else {
            Ok(Self(cents))
        }
    }

    pub const fn cents(self) -> i64 {
        self.0
    }

    /// The value of `qty` shares at this price.
    pub fn notional(self, qty: Qty) -> Result<Cents, DomainError> {
        self.0
            .checked_mul(qty.0)
            .map(Cents)
            .ok_or(DomainError::Overflow)
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${}.{:02}", self.0 / 100, self.0 % 100)
    }
}

/// A number of whole shares. Never negative; zero is allowed for "nothing left".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Qty(i64);

impl Qty {
    pub const ZERO: Qty = Qty(0);

    /// Creates an order quantity, refusing values outside `MIN_QTY..=MAX_QTY`.
    pub const fn new_order(shares: i64) -> Result<Self, DomainError> {
        if shares < MIN_QTY || shares > MAX_QTY {
            Err(DomainError::QtyOutOfRange(shares))
        } else {
            Ok(Self(shares))
        }
    }

    /// Creates any non-negative quantity (for totals and remainders).
    pub const fn new(shares: i64) -> Result<Self, DomainError> {
        if shares < 0 {
            Err(DomainError::Negative)
        } else {
            Ok(Self(shares))
        }
    }

    pub const fn shares(self) -> i64 {
        self.0
    }

    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn checked_add(self, other: Qty) -> Result<Qty, DomainError> {
        self.0
            .checked_add(other.0)
            .map(Qty)
            .ok_or(DomainError::Overflow)
    }

    pub fn checked_sub(self, other: Qty) -> Result<Qty, DomainError> {
        match self.0.checked_sub(other.0) {
            Some(v) if v >= 0 => Ok(Qty(v)),
            Some(_) => Err(DomainError::Negative),
            None => Err(DomainError::Overflow),
        }
    }
}

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An amount of money in cents. May be negative (ledger entries are signed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Cents(i64);

impl Cents {
    pub const ZERO: Cents = Cents(0);

    pub const fn new(cents: i64) -> Self {
        Self(cents)
    }

    pub const fn get(self) -> i64 {
        self.0
    }

    pub fn checked_add(self, other: Cents) -> Result<Cents, DomainError> {
        self.0
            .checked_add(other.0)
            .map(Cents)
            .ok_or(DomainError::Overflow)
    }

    pub fn checked_sub(self, other: Cents) -> Result<Cents, DomainError> {
        self.0
            .checked_sub(other.0)
            .map(Cents)
            .ok_or(DomainError::Overflow)
    }
}

impl fmt::Display for Cents {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.0 < 0 { "-" } else { "" };
        let abs = self.0.unsigned_abs();
        write!(f, "{sign}${}.{:02}", abs / 100, abs % 100)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub const fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderType {
    Limit,
    /// Runs as IOC at a protected price that Risk sets 5% from the reference price.
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeInForce {
    /// Good-till-canceled: the unfilled part waits in the book.
    Gtc,
    /// Immediate-or-cancel: the unfilled part is canceled at once.
    Ioc,
}

/// Percentage band for market-order price protection.
pub const MARKET_PROTECTION_PERCENT: i64 = 5;

/// The protected limit price for a market order, `MARKET_PROTECTION_PERCENT` away
/// from `reference`, clamped to the valid price range. Buys round down and sells
/// round up, so protection is never looser than 5%.
pub fn market_protected_price(side: Side, reference: Price) -> Price {
    let r = reference.cents();
    let band = r * MARKET_PROTECTION_PERCENT; // ≤ 5e8, cannot overflow
    let cents = match side {
        Side::Buy => r + band / 100,
        Side::Sell => r - band / 100,
    };
    let clamped = cents.clamp(MIN_PRICE_CENTS, MAX_PRICE_CENTS);
    Price(clamped)
}

macro_rules! uuid_id {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub Uuid);

        impl $name {
            /// A new time-ordered (UUIDv7) id. Not for use inside the matching engine,
            /// which must never read the clock.
            pub fn new_v7() -> Self {
                Self(Uuid::now_v7())
            }

            pub const fn from_u128(v: u128) -> Self {
                Self(Uuid::from_u128(v))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

uuid_id!(
    /// Created by Risk; identifies the order and its reservation.
    OrderId
);
uuid_id!(
    /// A registered user.
    UserId
);
uuid_id!(
    /// Created with each command; the engine uses it to drop repeats.
    CommandId
);

/// A stock symbol such as `ACME`: 1 to 8 uppercase ASCII letters.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Symbol(String);

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid symbol {0:?}: expected 1 to 8 uppercase letters")]
pub struct InvalidSymbol(pub String);

impl Symbol {
    pub fn new(s: &str) -> Result<Self, InvalidSymbol> {
        let ok = (1..=8).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_uppercase());
        if ok {
            Ok(Self(s.to_owned()))
        } else {
            Err(InvalidSymbol(s.to_owned()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// `{symbol}-{seq}`, the format of `event_id` and `trade_id`.
    pub fn event_id(&self, seq: u64) -> String {
        format!("{}-{seq}", self.0)
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_range_is_enforced() {
        assert!(Price::new(0).is_err());
        assert!(Price::new(1).is_ok());
        assert!(Price::new(MAX_PRICE_CENTS).is_ok());
        assert!(Price::new(MAX_PRICE_CENTS + 1).is_err());
    }

    #[test]
    fn qty_range_is_enforced() {
        assert!(Qty::new_order(0).is_err());
        assert!(Qty::new_order(MAX_QTY).is_ok());
        assert!(Qty::new_order(MAX_QTY + 1).is_err());
        assert!(Qty::new(-1).is_err());
    }

    #[test]
    fn largest_order_value_fits_in_i64() {
        let p = Price::new(MAX_PRICE_CENTS).unwrap();
        let q = Qty::new_order(MAX_QTY).unwrap();
        assert_eq!(p.notional(q).unwrap().get(), 100_000_000_000_000);
    }

    #[test]
    fn qty_sub_never_goes_negative() {
        let a = Qty::new(5).unwrap();
        let b = Qty::new(6).unwrap();
        assert_eq!(a.checked_sub(b), Err(DomainError::Negative));
        assert_eq!(b.checked_sub(a), Ok(Qty::new(1).unwrap()));
    }

    #[test]
    fn display_formats_cents() {
        assert_eq!(Price::new(10125).unwrap().to_string(), "$101.25");
        assert_eq!(Cents::new(-100).to_string(), "-$1.00");
        assert_eq!(Cents::new(5).to_string(), "$0.05");
    }

    #[test]
    fn market_protection_is_five_percent() {
        let r = Price::new(1000).unwrap();
        assert_eq!(market_protected_price(Side::Buy, r).cents(), 1050);
        assert_eq!(market_protected_price(Side::Sell, r).cents(), 950);
        // Rounding never loosens protection.
        let r = Price::new(1001).unwrap();
        assert_eq!(market_protected_price(Side::Buy, r).cents(), 1051);
        assert_eq!(market_protected_price(Side::Sell, r).cents(), 951);
        // Clamped at the bottom of the range.
        let r = Price::new(1).unwrap();
        assert_eq!(market_protected_price(Side::Sell, r).cents(), 1);
    }

    #[test]
    fn symbol_validation_and_event_id() {
        let s = Symbol::new("ACME").unwrap();
        assert_eq!(s.event_id(1042), "ACME-1042");
        assert!(Symbol::new("acme").is_err());
        assert!(Symbol::new("").is_err());
        assert!(Symbol::new("TOOLONGXX").is_err());
    }
}
