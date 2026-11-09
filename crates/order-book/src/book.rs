use std::collections::{BTreeMap, HashMap};

use domain::{CommandId, OrderId, OrderType, Price, Qty, Side, Symbol, TimeInForce, UserId};
use slab::Slab;

use crate::command::{CancelOrder, Command, CommandBody, NewOrder};
use crate::event::{BookEvent, CancelReason, CancelRejectReason, EventBody, TradeExecuted};

/// What `apply` did with a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum ApplyOutcome {
    /// The command was processed (it may have produced only rejections).
    Applied,
    /// A new order reused the id of an order already resting. The command was
    /// ignored and produced no events. This means a duplicate command slipped
    /// past the engine's duplicate memory and should be counted and alerted on.
    IgnoredDuplicateOrderId,
}

/// Everything needed to rebuild a book exactly. See [`OrderBook::export`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookState {
    pub symbol: Symbol,
    pub last_seq: u64,
    pub orders: Vec<RestingView>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreError {
    ZeroQuantity(OrderId),
    DuplicateOrderId(OrderId),
    Overflow,
    Crossed,
}

impl std::fmt::Display for RestoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroQuantity(id) => write!(f, "order {id} has zero quantity"),
            Self::DuplicateOrderId(id) => write!(f, "order {id} appears twice"),
            Self::Overflow => f.write_str("a price level total overflows"),
            Self::Crossed => f.write_str("restored book is crossed"),
        }
    }
}

impl std::error::Error for RestoreError {}

/// A read-only view of a resting order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestingView {
    pub order_id: OrderId,
    pub user_id: UserId,
    pub side: Side,
    pub price: Price,
    pub remaining: Qty,
}

/// One waiting order. `prev` and `next` are slab positions of its neighbors at
/// the same price, which keeps the FIFO linked list in safe Rust.
#[derive(Debug, Clone)]
struct RestingOrder {
    order_id: OrderId,
    user_id: UserId,
    side: Side,
    price: Price,
    remaining: Qty,
    prev: Option<usize>,
    next: Option<usize>,
}

/// All orders at one price, oldest first.
#[derive(Debug, Clone, Default)]
struct Level {
    head: Option<usize>,
    tail: Option<usize>,
    total_qty: Qty,
}

/// The order book for one stock.
///
/// Both sides are `BTreeMap<Price, Level>`: the best bid is the *last* bid
/// key and the best ask is the *first* ask key. (The spec sketches bids as
/// `Reverse<Price>`; one key type keeps the side-generic code simpler and the
/// ordering is identical.)
#[derive(Debug, Clone)]
pub struct OrderBook {
    symbol: Symbol,
    bids: BTreeMap<Price, Level>,
    asks: BTreeMap<Price, Level>,
    orders: Slab<RestingOrder>,
    /// Lookup only; never iterated, so its hash order cannot leak into output.
    index: HashMap<OrderId, usize>,
    last_seq: u64,
}

/// Assigns gap-free sequence numbers and appends events.
struct Emitter<'a> {
    last_seq: &'a mut u64,
    command_id: CommandId,
    event_time_ns: i64,
    out: &'a mut Vec<BookEvent>,
}

impl Emitter<'_> {
    fn emit(&mut self, body: EventBody) {
        *self.last_seq += 1;
        self.out.push(BookEvent {
            seq: *self.last_seq,
            event_time_ns: self.event_time_ns,
            command_id: self.command_id,
            body,
        });
    }
}

impl OrderBook {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: Slab::new(),
            index: HashMap::new(),
            last_seq: 0,
        }
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    /// The seq of the last event produced, or 0 before the first.
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    pub fn best_bid(&self) -> Option<Price> {
        self.bids.last_key_value().map(|(p, _)| *p)
    }

    pub fn best_ask(&self) -> Option<Price> {
        self.asks.first_key_value().map(|(p, _)| *p)
    }

    /// True if the best bid is at or above the best ask. Must never be true
    /// after `apply` returns; the engine halts the partition if it is.
    pub fn is_crossed(&self) -> bool {
        matches!((self.best_bid(), self.best_ask()), (Some(b), Some(a)) if b >= a)
    }

    /// Number of resting orders.
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    /// Price levels best first, as `(price, total quantity)`.
    pub fn depth(&self, side: Side) -> Vec<(Price, Qty)> {
        match side {
            Side::Buy => self
                .bids
                .iter()
                .rev()
                .map(|(p, l)| (*p, l.total_qty))
                .collect(),
            Side::Sell => self.asks.iter().map(|(p, l)| (*p, l.total_qty)).collect(),
        }
    }

    pub fn order(&self, order_id: OrderId) -> Option<RestingView> {
        let o = &self.orders[*self.index.get(&order_id)?];
        Some(RestingView {
            order_id: o.order_id,
            user_id: o.user_id,
            side: o.side,
            price: o.price,
            remaining: o.remaining,
        })
    }

    /// Resting orders at one price, oldest first.
    pub fn orders_at(&self, side: Side, price: Price) -> Vec<RestingView> {
        let mut out = Vec::new();
        let mut cur = self.levels(side).get(&price).and_then(|l| l.head);
        while let Some(idx) = cur {
            let o = &self.orders[idx];
            out.push(RestingView {
                order_id: o.order_id,
                user_id: o.user_id,
                side: o.side,
                price: o.price,
                remaining: o.remaining,
            });
            cur = o.next;
        }
        out
    }

    fn levels(&self, side: Side) -> &BTreeMap<Price, Level> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    /// Processes one command and appends its events to `out`.
    pub fn apply(&mut self, cmd: &Command, out: &mut Vec<BookEvent>) -> ApplyOutcome {
        let outcome = match &cmd.body {
            CommandBody::NewOrder(o) => self.new_order(cmd, o, out),
            CommandBody::CancelOrder(c) => {
                self.cancel(cmd, c, out);
                ApplyOutcome::Applied
            }
        };
        debug_assert!(!self.is_crossed(), "book crossed after {cmd:?}");
        outcome
    }

    fn new_order(&mut self, cmd: &Command, o: &NewOrder, out: &mut Vec<BookEvent>) -> ApplyOutcome {
        if self.index.contains_key(&o.order_id) {
            return ApplyOutcome::IgnoredDuplicateOrderId;
        }

        let mut em = Emitter {
            last_seq: &mut self.last_seq,
            command_id: cmd.command_id,
            event_time_ns: cmd.accepted_at_ns,
            out,
        };

        let (price, quantity) = match (Price::new(o.price_cents), Qty::new_order(o.quantity)) {
            (Ok(p), Ok(q)) => (p, q),
            _ => {
                em.emit(EventBody::OrderCanceled {
                    order_id: o.order_id,
                    user_id: o.user_id,
                    canceled_quantity: Qty::new(o.quantity).unwrap_or(Qty::ZERO),
                    reason: CancelReason::Invalid,
                });
                return ApplyOutcome::Applied;
            }
        };

        em.emit(EventBody::OrderAccepted {
            order_id: o.order_id,
            user_id: o.user_id,
            side: o.side,
            price,
            quantity,
        });

        let immediate = o.order_type == OrderType::Market || o.time_in_force == TimeInForce::Ioc;
        let opposite = o.side.opposite();
        let mut remaining = quantity;
        let mut self_trade = false;

        // Match against the opposite side, best level first.
        while !remaining.is_zero() && !self_trade {
            let levels = match opposite {
                Side::Buy => &mut self.bids,
                Side::Sell => &mut self.asks,
            };
            let entry = match opposite {
                Side::Buy => levels.last_entry(),
                Side::Sell => levels.first_entry(),
            };
            let Some(mut entry) = entry else { break };
            let level_price = *entry.key();
            let acceptable = match o.side {
                Side::Buy => level_price <= price,
                Side::Sell => level_price >= price,
            };
            if !acceptable {
                break;
            }

            let level = entry.get_mut();
            let mut touched = false;
            // Within the level, oldest first.
            while let Some(idx) = level.head {
                let resting = &mut self.orders[idx];
                if resting.user_id == o.user_id {
                    self_trade = true;
                    break;
                }
                let fill = remaining.min(resting.remaining);
                resting.remaining = sub(resting.remaining, fill);
                level.total_qty = sub(level.total_qty, fill);
                remaining = sub(remaining, fill);
                touched = true;

                let (buy_order_id, buyer_id, sell_order_id, seller_id) = match o.side {
                    Side::Buy => (o.order_id, o.user_id, resting.order_id, resting.user_id),
                    Side::Sell => (resting.order_id, resting.user_id, o.order_id, o.user_id),
                };
                em.emit(EventBody::Trade(TradeExecuted {
                    price: level_price,
                    quantity: fill,
                    buy_order_id,
                    sell_order_id,
                    buyer_id,
                    seller_id,
                    aggressor_side: o.side,
                }));

                if resting.remaining.is_zero() {
                    let next = resting.next;
                    let filled_id = resting.order_id;
                    level.head = next;
                    match next {
                        Some(n) => self.orders[n].prev = None,
                        None => level.tail = None,
                    }
                    self.orders.remove(idx);
                    self.index.remove(&filled_id);
                }
                if remaining.is_zero() {
                    break;
                }
            }

            if touched {
                em.emit(EventBody::BookChanged {
                    side: opposite,
                    price: level_price,
                    new_total_quantity: level.total_qty,
                });
            }
            if level.head.is_none() {
                entry.remove();
            }
        }

        if remaining.is_zero() {
            return ApplyOutcome::Applied;
        }

        let cancel_reason = if self_trade {
            Some(CancelReason::SelfTrade)
        } else if immediate {
            Some(CancelReason::IocRemainder)
        } else {
            None
        };
        if let Some(reason) = cancel_reason {
            em.emit(EventBody::OrderCanceled {
                order_id: o.order_id,
                user_id: o.user_id,
                canceled_quantity: remaining,
                reason,
            });
            return ApplyOutcome::Applied;
        }

        // Rest the remainder at the back of its price level.
        let levels = match o.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let resting = RestingView {
            order_id: o.order_id,
            user_id: o.user_id,
            side: o.side,
            price,
            remaining,
        };
        match append(levels, &mut self.orders, &mut self.index, resting) {
            Some(new_total) => em.emit(EventBody::BookChanged {
                side: o.side,
                price,
                new_total_quantity: new_total,
            }),
            // Unreachable with the spec's limits (it needs ~9 trillion orders at
            // one price), but money code never assumes.
            None => em.emit(EventBody::OrderCanceled {
                order_id: o.order_id,
                user_id: o.user_id,
                canceled_quantity: remaining,
                reason: CancelReason::Invalid,
            }),
        }
        ApplyOutcome::Applied
    }

    /// Every resting order, grouped by side (bids then asks), price ascending,
    /// oldest first within a price. Restoring these in this order rebuilds a
    /// book that behaves identically.
    pub fn export(&self) -> BookState {
        let mut orders = Vec::with_capacity(self.orders.len());
        for side in [Side::Buy, Side::Sell] {
            for price in self.levels(side).keys() {
                orders.extend(self.orders_at(side, *price));
            }
        }
        BookState {
            symbol: self.symbol.clone(),
            last_seq: self.last_seq,
            orders,
        }
    }

    /// Rebuilds a book from [`OrderBook::export`] output, refusing any state
    /// the book could never have produced.
    pub fn restore(state: BookState) -> Result<Self, RestoreError> {
        let mut book = OrderBook::new(state.symbol);
        book.last_seq = state.last_seq;
        for o in state.orders {
            if o.remaining.is_zero() {
                return Err(RestoreError::ZeroQuantity(o.order_id));
            }
            if book.index.contains_key(&o.order_id) {
                return Err(RestoreError::DuplicateOrderId(o.order_id));
            }
            let levels = match o.side {
                Side::Buy => &mut book.bids,
                Side::Sell => &mut book.asks,
            };
            append(levels, &mut book.orders, &mut book.index, o).ok_or(RestoreError::Overflow)?;
        }
        if book.is_crossed() {
            return Err(RestoreError::Crossed);
        }
        Ok(book)
    }

    fn cancel(&mut self, cmd: &Command, c: &CancelOrder, out: &mut Vec<BookEvent>) {
        let mut em = Emitter {
            last_seq: &mut self.last_seq,
            command_id: cmd.command_id,
            event_time_ns: cmd.accepted_at_ns,
            out,
        };

        let reject = |em: &mut Emitter<'_>, reason| {
            em.emit(EventBody::CancelRejected {
                order_id: c.order_id,
                user_id: c.user_id,
                reason,
            });
        };

        let Some(&idx) = self.index.get(&c.order_id) else {
            reject(&mut em, CancelRejectReason::NotFound);
            return;
        };
        if self.orders[idx].user_id != c.user_id {
            reject(&mut em, CancelRejectReason::NotOwner);
            return;
        }

        let order = self.orders.remove(idx);
        self.index.remove(&order.order_id);
        let levels = match order.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let level = levels
            .get_mut(&order.price)
            .expect("an indexed order always has a price level");
        match order.prev {
            Some(p) => self.orders[p].next = order.next,
            None => level.head = order.next,
        }
        match order.next {
            Some(n) => self.orders[n].prev = order.prev,
            None => level.tail = order.prev,
        }
        level.total_qty = sub(level.total_qty, order.remaining);
        let new_total = level.total_qty;
        if level.head.is_none() {
            levels.remove(&order.price);
        }

        em.emit(EventBody::OrderCanceled {
            order_id: order.order_id,
            user_id: order.user_id,
            canceled_quantity: order.remaining,
            reason: CancelReason::User,
        });
        em.emit(EventBody::BookChanged {
            side: order.side,
            price: order.price,
            new_total_quantity: new_total,
        });
    }

    /// Checks every structural invariant. Slow; for tests and audits, not the hot path.
    pub fn check_invariants(&self) -> Result<(), String> {
        if self.is_crossed() {
            return Err(format!(
                "crossed: bid {:?} >= ask {:?}",
                self.best_bid(),
                self.best_ask()
            ));
        }
        let mut seen = 0usize;
        for side in [Side::Buy, Side::Sell] {
            for (price, level) in self.levels(side) {
                let mut sum = Qty::ZERO;
                let mut prev = None;
                let mut cur = level.head;
                if cur.is_none() {
                    return Err(format!("empty level kept at {price}"));
                }
                while let Some(idx) = cur {
                    let o = self
                        .orders
                        .get(idx)
                        .ok_or_else(|| format!("dangling slab index {idx}"))?;
                    if o.side != side || o.price != *price {
                        return Err(format!("order {} filed under wrong level", o.order_id));
                    }
                    if o.remaining.is_zero() {
                        return Err(format!("order {} rests with zero quantity", o.order_id));
                    }
                    if o.prev != prev {
                        return Err(format!("order {} has a broken prev link", o.order_id));
                    }
                    if self.index.get(&o.order_id) != Some(&idx) {
                        return Err(format!("order {} missing from index", o.order_id));
                    }
                    sum = sum.checked_add(o.remaining).map_err(|e| e.to_string())?;
                    seen += 1;
                    prev = Some(idx);
                    cur = o.next;
                }
                if level.tail != prev {
                    return Err(format!("level {price} has a wrong tail"));
                }
                if sum != level.total_qty {
                    return Err(format!(
                        "level {price} total {} but orders sum to {sum}",
                        level.total_qty
                    ));
                }
            }
        }
        if seen != self.orders.len() || seen != self.index.len() {
            return Err(format!(
                "{seen} linked orders, {} in slab, {} in index",
                self.orders.len(),
                self.index.len()
            ));
        }
        Ok(())
    }
}

/// Adds an order at the back of its price level. Returns the level's new total,
/// or `None` (changing nothing) if that total would overflow.
fn append(
    levels: &mut BTreeMap<Price, Level>,
    orders: &mut Slab<RestingOrder>,
    index: &mut HashMap<OrderId, usize>,
    o: RestingView,
) -> Option<Qty> {
    let level = levels.entry(o.price).or_default();
    let Ok(new_total) = level.total_qty.checked_add(o.remaining) else {
        if level.head.is_none() {
            levels.remove(&o.price);
        }
        return None;
    };
    let idx = orders.insert(RestingOrder {
        order_id: o.order_id,
        user_id: o.user_id,
        side: o.side,
        price: o.price,
        remaining: o.remaining,
        prev: level.tail,
        next: None,
    });
    match level.tail {
        Some(t) => orders[t].next = Some(idx),
        None => level.head = Some(idx),
    }
    level.tail = Some(idx);
    level.total_qty = new_total;
    index.insert(o.order_id, idx);
    Some(new_total)
}

/// Subtraction that the matching logic guarantees cannot go negative.
fn sub(a: Qty, b: Qty) -> Qty {
    a.checked_sub(b)
        .expect("matching never subtracts more than is available")
}
