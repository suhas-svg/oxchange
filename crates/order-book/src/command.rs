use domain::{CommandId, OrderId, OrderType, Side, TimeInForce, UserId};

/// One input to the book, in log order. Mirrors `Command` in `events.proto`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub command_id: CommandId,
    /// When Risk accepted the command. The only time source the engine uses.
    pub accepted_at_ns: i64,
    pub body: CommandBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandBody {
    NewOrder(NewOrder),
    CancelOrder(CancelOrder),
}

/// A new order. Price and quantity stay raw integers, exactly as decoded from
/// the log, so the book itself can refuse out-of-range values with an event
/// (which lets Risk release the funds) instead of failing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOrder {
    pub order_id: OrderId,
    pub user_id: UserId,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    /// The limit price; for market orders, the protected price Risk computed.
    pub price_cents: i64,
    pub quantity: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelOrder {
    pub order_id: OrderId,
    pub user_id: UserId,
}
