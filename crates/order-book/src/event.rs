use domain::{CommandId, OrderId, Price, Qty, Side, Symbol, UserId};

/// One output of the book. Mirrors `MatchingEvent` in `events.proto`.
///
/// `seq` is per stock, starts at 1 and has no gaps. The `event_id` (and, for
/// trades, the `trade_id`) is `{symbol}-{seq}`; see [`BookEvent::event_id`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookEvent {
    pub seq: u64,
    /// Copied from the command's `accepted_at_ns`.
    pub event_time_ns: i64,
    pub command_id: CommandId,
    pub body: EventBody,
}

impl BookEvent {
    pub fn event_id(&self, symbol: &Symbol) -> String {
        symbol.event_id(self.seq)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventBody {
    /// The book took the order. Always the first event for a valid new order.
    OrderAccepted {
        order_id: OrderId,
        user_id: UserId,
        side: Side,
        price: Price,
        quantity: Qty,
    },
    Trade(TradeExecuted),
    /// Some quantity of an order will never trade. Risk releases funds for
    /// exactly `canceled_quantity`.
    OrderCanceled {
        order_id: OrderId,
        user_id: UserId,
        canceled_quantity: Qty,
        reason: CancelReason,
    },
    CancelRejected {
        order_id: OrderId,
        user_id: UserId,
        reason: CancelRejectReason,
    },
    /// The total resting quantity at one price changed. Zero means the level is gone.
    BookChanged {
        side: Side,
        price: Price,
        new_total_quantity: Qty,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeExecuted {
    /// Always the resting order's price.
    pub price: Price,
    pub quantity: Qty,
    pub buy_order_id: OrderId,
    pub sell_order_id: OrderId,
    pub buyer_id: UserId,
    pub seller_id: UserId,
    /// The side of the incoming order.
    pub aggressor_side: Side,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReason {
    /// The user asked for it.
    User,
    /// IOC or market order: whatever did not trade at once.
    IocRemainder,
    /// The next match would have been against the same user's resting order.
    SelfTrade,
    /// The order's price or quantity was out of range, so the book refused it
    /// whole. Risk should never send one; this keeps funds from staying locked
    /// if it does. (Addition to the spec's `CancelReason`.)
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelRejectReason {
    /// Not in the book: already fully filled, already canceled, or never rested.
    NotFound,
    /// The order belongs to a different user.
    NotOwner,
}
