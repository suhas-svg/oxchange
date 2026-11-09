//! Deterministic benchmark workload, shaped like the spec's load test:
//! 20 stocks, 500 users, 60% limit orders that wait in the book, 30% cancels,
//! and 10% orders that trade immediately, with prices clustered near a slowly
//! moving market price. Commands are generated up front so generation cost
//! never lands inside a measurement.

#![allow(dead_code)]

use domain::{CommandId, OrderId, OrderType, Side, Symbol, TimeInForce, UserId};
use order_book::{CancelOrder, Command, CommandBody, NewOrder};

pub const STOCKS: usize = 20;
pub const USERS: u64 = 500;

pub struct Input {
    pub offset: u64,
    pub symbol: usize,
    pub cmd: Command,
}

pub struct Workload {
    pub symbols: Vec<Symbol>,
    pub inputs: Vec<Input>,
}

/// SplitMix64: tiny, fast, and the same sequence on every machine.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + self.below((hi - lo + 1) as u64) as i64
    }
}

/// Generates `n` commands with the given seed.
pub fn generate(n: usize, seed: u64) -> Workload {
    let symbols: Vec<Symbol> = (0..STOCKS)
        .map(|i| {
            let a = (b'A' + (i / 26) as u8) as char;
            let b = (b'A' + (i % 26) as u8) as char;
            Symbol::new(&format!("S{a}{b}")).unwrap()
        })
        .collect();
    let mut rng = Rng(seed);
    // Market price per stock, in cents, starting near $100.
    let mut mid: Vec<i64> = (0..STOCKS).map(|_| rng.range(9_000, 11_000)).collect();
    // Orders that may still be resting, per stock (cancel candidates).
    let mut live: Vec<Vec<(OrderId, UserId)>> = vec![Vec::new(); STOCKS];
    let mut inputs = Vec::with_capacity(n);

    for offset in 0..n as u64 {
        let s = rng.below(STOCKS as u64) as usize;
        // Slow random walk keeps the book realistic and moving.
        if rng.below(16) == 0 {
            mid[s] = (mid[s] + rng.range(-1, 1)).clamp(1_000, 100_000);
        }
        let id = u128::from(offset) + 1;
        let roll = rng.below(100);
        let body = if roll < 30 && !live[s].is_empty() {
            // Cancel: mostly recent orders, like a real client.
            let len = live[s].len();
            let back = (rng.below(len.min(64) as u64) as usize).min(len - 1);
            let (order_id, user_id) = live[s].swap_remove(len - 1 - back);
            CommandBody::CancelOrder(CancelOrder { order_id, user_id })
        } else {
            let side = if rng.below(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let user_id = UserId::from_u128(rng.below(USERS) as u128 + 1);
            let order_id = OrderId::from_u128(id);
            let qty = rng.range(1, 100);
            let immediate = roll >= 90;
            let (price, tif) = if immediate {
                // Crosses the spread by up to 25 cents and trades at once.
                let cross = rng.range(1, 25);
                let p = match side {
                    Side::Buy => mid[s] + cross,
                    Side::Sell => mid[s] - cross,
                };
                (p, TimeInForce::Ioc)
            } else {
                // Passive: rests 1 to 20 cents away from the market.
                let away = rng.range(1, 20);
                let p = match side {
                    Side::Buy => mid[s] - away,
                    Side::Sell => mid[s] + away,
                };
                live[s].push((order_id, user_id));
                (p, TimeInForce::Gtc)
            };
            CommandBody::NewOrder(NewOrder {
                order_id,
                user_id,
                side,
                order_type: OrderType::Limit,
                time_in_force: tif,
                price_cents: price,
                quantity: qty,
            })
        };
        inputs.push(Input {
            offset,
            symbol: s,
            cmd: Command {
                command_id: CommandId::from_u128(id),
                accepted_at_ns: offset as i64 * 500,
                body,
            },
        });
    }
    Workload { symbols, inputs }
}
