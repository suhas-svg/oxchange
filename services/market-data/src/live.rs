//! The live side of Market Data: every copy keeps every stock's public book
//! in memory and streams changes to WebSocket clients.
//!
//! Startup (spec: "Market Data → Startup") happens per stock, the first time
//! an event or a subscriber needs it: fetch `GetBook`, then apply only
//! events with a seq above its `as_of_seq`. Live events start at the end of
//! `matching.events`, so anything older is already in the fetched book.
//!
//! A seq gap discards that stock's state and fetches it again (spec: "Bad
//! messages → Market Data"); subscribers get a fresh `book_snapshot`.
//!
//! Slow clients (spec): each client has a queue of [`CLIENT_QUEUE`]
//! messages. When it is full the client is dropped from every channel and
//! sent `resync_required`, so one slow browser never delays the others.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use proto_gen::events::v1 as ev;
use serde::Serialize;
use sqlx::PgPool;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::recorder::side_name;

/// Messages a client may have waiting before it counts as slow.
pub const CLIENT_QUEUE: usize = 1_024;
/// Stocks tracked at most; subscribers cannot make the map grow without end.
pub const MAX_SYMBOLS: usize = 1_000;
/// After a gap, wait at least this long before fetching the same book again.
pub const RELOAD_EVERY: Duration = Duration::from_millis(500);

const NS_PER_DAY: i64 = 86_400 * 1_000_000_000;

/// A book as `GetBook` returns it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub as_of_seq: u64,
    /// `(price cents, quantity)`, best first.
    pub bids: Vec<(i64, i64)>,
    pub asks: Vec<(i64, i64)>,
}

/// Where books come from: `MatchingService.GetBook`.
pub trait BookSource: Send + Sync + 'static {
    /// `partition` is the one this stock's events arrive on, when known.
    fn get_book(
        &self,
        symbol: &str,
        partition: Option<i32>,
    ) -> impl Future<Output = Result<Snapshot, String>> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    Book,
    Trades,
    Ticker,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Level {
    pub price_cents: i64,
    pub quantity: i64,
}

/// What the server sends (spec: "WebSockets").
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Out {
    BookSnapshot {
        symbol: String,
        seq: u64,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    /// One price level changed; quantity 0 removes it. `prev_seq` is the
    /// seq of the previous book message for this stock: other events use
    /// seqs in between, so a client checks `prev_seq` against its last seq
    /// to detect a gap.
    BookDelta {
        symbol: String,
        seq: u64,
        prev_seq: u64,
        side: &'static str,
        price_cents: i64,
        quantity: i64,
    },
    Trade {
        symbol: String,
        seq: u64,
        price_cents: i64,
        quantity: i64,
        /// The side of the incoming order.
        side: &'static str,
        time_ns: i64,
    },
    Ticker {
        symbol: String,
        seq: u64,
        last_price_cents: Option<i64>,
        day_volume: i64,
    },
    ResyncRequired {
        symbol: String,
    },
    Error {
        message: String,
    },
}

impl Out {
    pub fn json(&self) -> Arc<str> {
        serde_json::to_string(self)
            .expect("plain data serializes")
            .into()
    }
}

pub type ClientId = u64;

struct Client {
    tx: mpsc::Sender<Arc<str>>,
    /// Fired once, with the stock whose message did not fit.
    kill: Option<oneshot::Sender<String>>,
}

struct Sym {
    /// Last event seq reflected here.
    seq: u64,
    /// Seq of the last book message (snapshot or delta) for `prev_seq`.
    book_seq: u64,
    partition: Option<i32>,
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    last_price: Option<i64>,
    /// The UTC day (days since 1970) `day_volume` counts.
    day: i64,
    day_volume: i64,
    subs: HashMap<Channel, HashSet<ClientId>>,
    last_load: Instant,
}

impl Sym {
    fn snapshot_msg(&self, symbol: &str) -> Out {
        let levels = |m: &mut dyn Iterator<Item = (&i64, &i64)>| {
            m.map(|(p, q)| Level {
                price_cents: *p,
                quantity: *q,
            })
            .collect()
        };
        Out::BookSnapshot {
            symbol: symbol.to_owned(),
            seq: self.seq,
            bids: levels(&mut self.bids.iter().rev()),
            asks: levels(&mut self.asks.iter()),
        }
    }

    fn ticker_msg(&self, symbol: &str) -> Out {
        Out::Ticker {
            symbol: symbol.to_owned(),
            seq: self.seq,
            last_price_cents: self.last_price,
            day_volume: self.day_volume,
        }
    }
}

#[derive(Default)]
struct State {
    syms: HashMap<String, Sym>,
    clients: HashMap<ClientId, Client>,
}

impl State {
    /// Queues `msg` for every subscriber of `channel` on `symbol`, dropping
    /// the ones that are too far behind.
    fn fan_out(&mut self, symbol: &str, channel: Channel, msg: &Out) -> u64 {
        let Some(ids) = self
            .syms
            .get(symbol)
            .and_then(|s| s.subs.get(&channel))
            .filter(|ids| !ids.is_empty())
        else {
            return 0;
        };
        let json = msg.json();
        let slow: Vec<ClientId> = ids
            .iter()
            .filter(|id| {
                self.clients
                    .get(id)
                    .is_some_and(|c| c.tx.try_send(json.clone()).is_err())
            })
            .copied()
            .collect();
        for id in &slow {
            self.drop_slow(*id, symbol);
        }
        slow.len() as u64
    }

    fn send_one(&mut self, id: ClientId, symbol: &str, msg: &Out) -> u64 {
        let full = self
            .clients
            .get(&id)
            .is_some_and(|c| c.tx.try_send(msg.json()).is_err());
        if full {
            self.drop_slow(id, symbol);
            1
        } else {
            0
        }
    }

    fn drop_slow(&mut self, id: ClientId, symbol: &str) {
        self.unsubscribe_all(id);
        if let Some(mut c) = self.clients.remove(&id)
            && let Some(kill) = c.kill.take()
        {
            let _ = kill.send(symbol.to_owned());
        }
    }

    fn unsubscribe_all(&mut self, id: ClientId) {
        for s in self.syms.values_mut() {
            for set in s.subs.values_mut() {
                set.remove(&id);
            }
        }
    }
}

/// A connected client's side: its message queue and the slow-client signal.
pub struct Connection {
    pub id: ClientId,
    pub messages: mpsc::Receiver<Arc<str>>,
    /// Resolves with a stock symbol if the client fell behind.
    pub dropped: oneshot::Receiver<String>,
}

pub struct Live<B> {
    state: Mutex<State>,
    books: B,
    /// Market Data's database, for the last price and day volume at load.
    pool: Option<PgPool>,
    next_id: AtomicU64,
    /// `websocket_dropped_slow_total`.
    pub dropped_slow: AtomicU64,
}

impl<B: BookSource> Live<B> {
    pub fn new(books: B, pool: Option<PgPool>) -> Self {
        Self {
            state: Mutex::default(),
            books,
            pool,
            next_id: AtomicU64::new(1),
            dropped_slow: AtomicU64::new(0),
        }
    }

    /// `websocket_clients`.
    pub async fn clients(&self) -> usize {
        self.state.lock().await.clients.len()
    }

    /// Builds a stock's state from `GetBook` and the recorded trades.
    async fn load(&self, symbol: &str, partition: Option<i32>) -> Result<Sym, String> {
        let snap = self.books.get_book(symbol, partition).await?;
        let (mut last_price, mut day, mut day_volume) = (None, 0, 0);
        if let Some(pool) = &self.pool {
            let as_of = i64::try_from(snap.as_of_seq).unwrap_or(i64::MAX);
            let row: Option<(i64, time::OffsetDateTime)> = sqlx::query_as(
                "SELECT price_cents, executed_at FROM market_trades
                 WHERE symbol = $1 AND seq <= $2 ORDER BY seq DESC LIMIT 1",
            )
            .bind(symbol)
            .bind(as_of)
            .fetch_optional(pool)
            .await
            .map_err(|e| e.to_string())?;
            if let Some((price, at)) = row {
                last_price = Some(price);
                let at_ns = i64::try_from(at.unix_timestamp_nanos()).unwrap_or(0);
                day = at_ns.div_euclid(NS_PER_DAY);
                day_volume = sqlx::query_scalar(
                    "SELECT COALESCE(SUM(quantity), 0)::bigint FROM market_trades
                     WHERE symbol = $1 AND seq <= $2
                       AND executed_at >= date_trunc('day', $3 AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'",
                )
                .bind(symbol)
                .bind(as_of)
                .bind(at)
                .fetch_one(pool)
                .await
                .map_err(|e| e.to_string())?;
            }
        }
        Ok(Sym {
            seq: snap.as_of_seq,
            book_seq: snap.as_of_seq,
            partition,
            bids: snap.bids.into_iter().collect(),
            asks: snap.asks.into_iter().collect(),
            last_price,
            day,
            day_volume,
            subs: HashMap::new(),
            last_load: Instant::now(),
        })
    }

    /// Makes sure `symbol` is tracked, fetching its book if it is not.
    async fn ensure(&self, symbol: &str, partition: Option<i32>) -> Result<(), String> {
        {
            let st = self.state.lock().await;
            if st.syms.contains_key(symbol) {
                return Ok(());
            }
            if st.syms.len() >= MAX_SYMBOLS {
                return Err("too many stocks tracked".into());
            }
        }
        // Fetch without holding the lock, so other stocks keep streaming.
        let sym = self.load(symbol, partition).await?;
        self.state
            .lock()
            .await
            .syms
            .entry(symbol.to_owned())
            .or_insert(sym);
        Ok(())
    }

    /// Discards `symbol`'s state and fetches it again, keeping subscribers,
    /// who get a fresh snapshot and ticker.
    async fn reload(&self, symbol: &str, partition: i32) {
        {
            let st = self.state.lock().await;
            if st
                .syms
                .get(symbol)
                .is_some_and(|s| s.last_load.elapsed() < RELOAD_EVERY)
            {
                return;
            }
        }
        let fresh = match self.load(symbol, Some(partition)).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(%symbol, error = %e, "could not fetch the book after a gap");
                return;
            }
        };
        let mut st = self.state.lock().await;
        let Some(sym) = st.syms.get_mut(symbol) else {
            return;
        };
        let subs = std::mem::take(&mut sym.subs);
        *sym = fresh;
        sym.subs = subs;
        let (book, tick) = (sym.snapshot_msg(symbol), sym.ticker_msg(symbol));
        let mut slow = st.fan_out(symbol, Channel::Book, &book);
        slow += st.fan_out(symbol, Channel::Ticker, &tick);
        self.dropped_slow.fetch_add(slow, Ordering::Relaxed);
    }

    /// Applies one `matching.events` message read from `partition`.
    pub async fn on_event(&self, partition: i32, e: &ev::MatchingEvent) {
        if domain_symbol_ok(&e.symbol).is_err() {
            return;
        }
        if let Err(err) = self.ensure(&e.symbol, Some(partition)).await {
            tracing::warn!(symbol = %e.symbol, error = %err, "book unavailable; event dropped");
            return;
        }
        let behind = {
            let mut st = self.state.lock().await;
            let Some(sym) = st.syms.get_mut(&e.symbol) else {
                return;
            };
            sym.partition = Some(partition);
            e.seq > sym.seq + 1
        };
        if behind {
            tracing::warn!(symbol = %e.symbol, seq = e.seq, "seq gap; fetching the book again");
            self.reload(&e.symbol, partition).await;
        }

        let mut st = self.state.lock().await;
        let Some(sym) = st.syms.get_mut(&e.symbol) else {
            return;
        };
        if e.seq != sym.seq + 1 {
            // A repeat, or still behind until a later fetch catches up.
            return;
        }
        sym.seq = e.seq;
        let symbol = e.symbol.clone();
        let msgs: Vec<(Channel, Out)> = match &e.body {
            Some(ev::matching_event::Body::BookChanged(b)) => {
                let side = if b.side == ev::Side::Sell as i32 {
                    &mut sym.asks
                } else {
                    &mut sym.bids
                };
                if b.new_total_quantity == 0 {
                    side.remove(&b.price_cents);
                } else {
                    side.insert(b.price_cents, b.new_total_quantity);
                }
                let prev_seq = sym.book_seq;
                sym.book_seq = e.seq;
                vec![(
                    Channel::Book,
                    Out::BookDelta {
                        symbol: symbol.clone(),
                        seq: e.seq,
                        prev_seq,
                        side: side_name(b.side),
                        price_cents: b.price_cents,
                        quantity: b.new_total_quantity,
                    },
                )]
            }
            Some(ev::matching_event::Body::Trade(t)) => {
                let day = e.event_time_ns.div_euclid(NS_PER_DAY);
                if day != sym.day {
                    sym.day = day;
                    sym.day_volume = 0;
                }
                sym.day_volume += t.quantity;
                sym.last_price = Some(t.price_cents);
                vec![
                    (
                        Channel::Trades,
                        Out::Trade {
                            symbol: symbol.clone(),
                            seq: e.seq,
                            price_cents: t.price_cents,
                            quantity: t.quantity,
                            side: side_name(t.aggressor_side),
                            time_ns: e.event_time_ns,
                        },
                    ),
                    (Channel::Ticker, sym.ticker_msg(&symbol)),
                ]
            }
            _ => Vec::new(),
        };
        let mut slow = 0;
        for (channel, msg) in &msgs {
            slow += st.fan_out(&symbol, *channel, msg);
        }
        self.dropped_slow.fetch_add(slow, Ordering::Relaxed);
    }

    /// A new client, with an empty subscription list.
    pub async fn connect(&self) -> Connection {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, messages) = mpsc::channel(CLIENT_QUEUE);
        let (kill, dropped) = oneshot::channel();
        self.state.lock().await.clients.insert(
            id,
            Client {
                tx,
                kill: Some(kill),
            },
        );
        Connection {
            id,
            messages,
            dropped,
        }
    }

    pub async fn disconnect(&self, id: ClientId) {
        let mut st = self.state.lock().await;
        st.unsubscribe_all(id);
        st.clients.remove(&id);
    }

    /// Subscribes `id`. A book subscriber first gets a `book_snapshot`, a
    /// ticker subscriber the current ticker; both are queued under the same
    /// lock as later updates, so nothing is missed or doubled.
    pub async fn subscribe(
        &self,
        id: ClientId,
        channel: Channel,
        symbol: &str,
    ) -> Result<(), String> {
        domain_symbol_ok(symbol)?;
        self.ensure(symbol, None).await?;
        let mut st = self.state.lock().await;
        if !st.clients.contains_key(&id) {
            return Err("connection closed".into());
        }
        let Some(sym) = st.syms.get_mut(symbol) else {
            return Err("stock unavailable".into());
        };
        sym.subs.entry(channel).or_default().insert(id);
        let first = match channel {
            Channel::Book => Some(sym.snapshot_msg(symbol)),
            Channel::Ticker => Some(sym.ticker_msg(symbol)),
            Channel::Trades => None,
        };
        if let Some(m) = first {
            let slow = st.send_one(id, symbol, &m);
            self.dropped_slow.fetch_add(slow, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Queues an error message for one client.
    pub async fn send_error(&self, id: ClientId, message: String) {
        let mut st = self.state.lock().await;
        let slow = st.send_one(id, "", &Out::Error { message });
        self.dropped_slow.fetch_add(slow, Ordering::Relaxed);
    }

    pub async fn unsubscribe(&self, id: ClientId, channel: Channel, symbol: &str) {
        let mut st = self.state.lock().await;
        if let Some(set) = st
            .syms
            .get_mut(symbol)
            .and_then(|s| s.subs.get_mut(&channel))
        {
            set.remove(&id);
        }
    }

    /// The current book, as `GET /v1/symbols/{symbol}/book` returns it.
    pub async fn book(&self, symbol: &str) -> Result<Out, String> {
        domain_symbol_ok(symbol)?;
        self.ensure(symbol, None).await?;
        let st = self.state.lock().await;
        st.syms
            .get(symbol)
            .map(|s| s.snapshot_msg(symbol))
            .ok_or_else(|| "stock unavailable".into())
    }
}

/// Stock symbols: 1 to 8 capital letters or digits, starting with a letter.
pub fn domain_symbol_ok(s: &str) -> Result<(), String> {
    let ok = (1..=8).contains(&s.len())
        && s.starts_with(|c: char| c.is_ascii_uppercase())
        && s.chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit());
    if ok {
        Ok(())
    } else {
        Err(format!("{s:?} is not a stock symbol"))
    }
}

/// `GetBook` over gRPC: one address per matching partition.
pub struct GrpcBooks {
    clients: Vec<
        proto_gen::matching::v1::matching_service_client::MatchingServiceClient<
            tonic::transport::Channel,
        >,
    >,
}

/// Spec: "Market Data to Matching, GetBook: 1 s".
pub const GET_BOOK_TIMEOUT: Duration = Duration::from_secs(1);

impl GrpcBooks {
    /// `urls[p]` serves partition `p`.
    pub fn connect_lazy(urls: &[String]) -> Result<Self, String> {
        let clients = urls
            .iter()
            .map(|u| {
                tonic::transport::Endpoint::from_shared(u.clone())
                    .map(|e| {
                        proto_gen::matching::v1::matching_service_client::MatchingServiceClient::new(
                            e.timeout(GET_BOOK_TIMEOUT)
                                .connect_timeout(GET_BOOK_TIMEOUT)
                                .connect_lazy(),
                        )
                    })
                    .map_err(|e| format!("{u}: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if clients.is_empty() {
            return Err("no matching addresses".into());
        }
        Ok(Self { clients })
    }
}

impl BookSource for GrpcBooks {
    async fn get_book(&self, symbol: &str, partition: Option<i32>) -> Result<Snapshot, String> {
        use proto_gen::matching::v1 as mpb;
        let ask = |mut c: mpb::matching_service_client::MatchingServiceClient<_>| {
            let req = mpb::GetBookRequest {
                symbol: symbol.to_owned(),
            };
            let req = telemetry::grpc_request(req);
            async move { c.get_book(req).await.map(tonic::Response::into_inner) }
        };
        let to_snapshot = |r: mpb::GetBookResponse| Snapshot {
            as_of_seq: r.as_of_seq,
            bids: r.bids.iter().map(|l| (l.price_cents, l.quantity)).collect(),
            asks: r.asks.iter().map(|l| (l.price_cents, l.quantity)).collect(),
        };
        if let Some(c) = partition
            .and_then(|p| usize::try_from(p).ok())
            .and_then(|p| self.clients.get(p))
        {
            return ask(c.clone())
                .await
                .map(to_snapshot)
                .map_err(|s| s.message().to_owned());
        }
        // Partition unknown: the matcher that owns the stock is the one with
        // events for it, so take the answer with the highest seq. A wrong
        // guess (its matcher down) shows up as a gap and is fetched again.
        let mut best: Option<mpb::GetBookResponse> = None;
        let mut errors = Vec::new();
        let answers =
            futures_util::future::join_all(self.clients.iter().map(|c| ask(c.clone()))).await;
        for a in answers {
            match a {
                Ok(r) if best.as_ref().is_none_or(|b| r.as_of_seq > b.as_of_seq) => best = Some(r),
                Ok(_) => {}
                Err(s) => errors.push(s.message().to_owned()),
            }
        }
        match best {
            Some(r) if r.as_of_seq > 0 || errors.is_empty() => Ok(to_snapshot(r)),
            _ => Err(format!("no matcher answered: {}", errors.join("; "))),
        }
    }
}

/// For tests: books held in memory.
#[derive(Default, Clone)]
pub struct MemBooks(pub Arc<std::sync::Mutex<HashMap<String, Snapshot>>>);

impl BookSource for MemBooks {
    async fn get_book(&self, symbol: &str, _: Option<i32>) -> Result<Snapshot, String> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .get(symbol)
            .cloned()
            .unwrap_or_default())
    }
}
