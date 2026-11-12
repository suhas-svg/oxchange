//! Open-loop load generator for Oxchange.
//!
//! Sends orders through the Gateway on a fixed schedule, whether or not
//! earlier replies have arrived, and measures every latency from the order's
//! *scheduled* send time. A stalled server therefore shows up as latency
//! instead of quietly slowing the test down (coordinated omission).
//!
//! Workload (spec, "Load test design"): 20 stocks, 500 users, 60% limit
//! orders that rest in the book, 30% cancels, 10% orders that trade
//! immediately (IOC limits priced through the resting orders). Prices
//! cluster near each stock's reference price.
//!
//! Setup talks to Accounts over gRPC (register, log in, deposit), because the
//! Gateway allows 3 sign-ups a minute per address. Everything measured goes
//! through the Gateway's public REST API.
//!
//! Also measured, when enabled:
//! - order to fill notification: private WebSockets for the first
//!   `--watchers` users, connected straight to Notifications (the Gateway
//!   caps sockets per address, and every socket here shares one address);
//! - trade to public trade message: `--public-clients` sockets on Market
//!   Data's public stream, subscribed to every stock's trades;
//! - trade to settled and order end to end, read from Prometheus.
//!
//! Usage (inside the Compose network):
//!   loadgen --rate 500 --duration 60 --out /out/run.json
//!
//! Every option has a default; see `Config::parse`.

// Rates, percentages and seconds in the report are floats; nothing here
// touches money.
#![allow(clippy::float_arithmetic)]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use proto_gen::accounts::v1 as acc;
use proto_gen::accounts::v1::accounts_service_client::AccountsServiceClient;
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tokio_tungstenite::tungstenite::Message;
use tonic::transport::Channel;

type Http = Client<HttpConnector, Full<Bytes>>;

/// Most resting orders one user keeps per stock; past it, a "rest" becomes a cancel.
const MAX_OPEN: usize = 20;
/// Requests still unanswered after this count as timeouts.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// More requests in flight than this means the generator itself is the limit.
const MAX_IN_FLIGHT: usize = 20_000;
/// Simulated cash per user: $10,000,000 (Accounts allows 1e9 cents per deposit).
const CASH_CENTS: i64 = 1_000_000_000;
/// Simulated shares per user of each stock it trades.
const SHARES: i64 = 1_000_000;

#[derive(Debug, Clone)]
struct Config {
    gateway: String,
    accounts: String,
    notifications_ws: String,
    market_data_ws: String,
    prometheus: Option<String>,
    users: usize,
    rate: u64,
    duration: Duration,
    watchers: usize,
    public_clients: usize,
    seed: u64,
    out: Option<String>,
    label: String,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut c = Config {
            gateway: "http://gateway:8080".into(),
            accounts: "http://accounts:50051".into(),
            notifications_ws: "ws://notifications:8080".into(),
            market_data_ws: "ws://market-data:8080".into(),
            prometheus: Some("http://prometheus:9090".into()),
            users: 500,
            rate: 200,
            duration: Duration::from_secs(60),
            watchers: 50,
            public_clients: 20,
            seed: 42,
            out: None,
            label: "run".into(),
        };
        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            if flag == "--help" || flag == "-h" {
                println!(
                    "loadgen [--rate N/s] [--duration S] [--users N] [--watchers N] \
                     [--public-clients N] [--seed N] [--out FILE] [--label NAME] \
                     [--gateway URL] [--accounts URL] [--notifications-ws URL] \
                     [--market-data-ws URL] [--prometheus URL|off]"
                );
                std::process::exit(0);
            }
            let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
            let num = |v: &str| v.parse::<u64>().map_err(|e| format!("{flag} {v:?}: {e}"));
            match flag.as_str() {
                "--gateway" => c.gateway = value,
                "--accounts" => c.accounts = value,
                "--notifications-ws" => c.notifications_ws = value,
                "--market-data-ws" => c.market_data_ws = value,
                "--prometheus" => c.prometheus = (value != "off").then_some(value),
                "--users" => c.users = num(&value)? as usize,
                "--rate" => c.rate = num(&value)?,
                "--duration" => c.duration = Duration::from_secs(num(&value)?),
                "--watchers" => c.watchers = num(&value)? as usize,
                "--public-clients" => c.public_clients = num(&value)? as usize,
                "--seed" => c.seed = num(&value)?,
                "--out" => c.out = Some(value),
                "--label" => c.label = value,
                other => return Err(format!("unknown option {other}")),
            }
        }
        if c.users == 0 || c.rate == 0 {
            return Err("--users and --rate must be at least 1".into());
        }
        c.watchers = c.watchers.min(c.users);
        Ok(c)
    }
}

/// splitmix64: small, fast and the same on every platform, so a seed
/// always produces the same order sequence.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// Uniform in `0..n`.
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

struct Stock {
    symbol: String,
    reference_cents: i64,
}

impl Stock {
    /// Price step used to spread orders around the reference: 0.2%, at least a cent.
    fn step(&self) -> i64 {
        (self.reference_cents / 500).max(1)
    }
}

struct User {
    id: String,
    token: RwLock<String>,
    refresh: Mutex<String>,
    stocks: [usize; 2],
    /// Resting orders per stock slot, oldest first.
    open: Mutex<[VecDeque<String>; 2]>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Kind {
    Rest,
    Cancel,
    Cross,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Rest => "limit_rest",
            Kind::Cancel => "cancel",
            Kind::Cross => "limit_ioc_cross",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Outcome {
    Ok,
    Rejected,
    RateLimited,
    Unavailable,
    Error,
    Timeout,
}

impl Outcome {
    const ALL: [Outcome; 6] = [
        Outcome::Ok,
        Outcome::Rejected,
        Outcome::RateLimited,
        Outcome::Unavailable,
        Outcome::Error,
        Outcome::Timeout,
    ];
    fn name(self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::Rejected => "rejected",
            Outcome::RateLimited => "rate_limited",
            Outcome::Unavailable => "unavailable",
            Outcome::Error => "error",
            Outcome::Timeout => "timeout",
        }
    }
    fn of(status: StatusCode) -> Self {
        match status.as_u16() {
            200..=299 => Outcome::Ok,
            429 => Outcome::RateLimited,
            502..=504 => Outcome::Unavailable,
            400..=499 => Outcome::Rejected,
            _ => Outcome::Error,
        }
    }
}

fn histogram() -> Histogram<u64> {
    // Microseconds, 1 µs to 10 minutes, 3 significant digits.
    Histogram::new_with_bounds(1, 600_000_000, 3).expect("valid histogram bounds")
}

/// Everything the run measures.
struct Stats {
    /// When sending began (reset once the sockets are up).
    started: Mutex<Instant>,
    latency: Mutex<HashMap<Kind, Histogram<u64>>>,
    outcomes: Mutex<HashMap<(Kind, Outcome), u64>>,
    reject_codes: Mutex<HashMap<String, u64>>,
    /// Per second of the run: (ok, not ok).
    timeline: Mutex<Vec<(u64, u64)>>,
    /// IOC orders of watched users: order id → scheduled send time.
    cross_sent: Mutex<HashMap<String, Instant>>,
    /// Fill notifications: order id → first arrival.
    fills_seen: Mutex<HashMap<String, Instant>>,
    public_latency: Mutex<Histogram<u64>>,
    public_messages: AtomicU64,
    skipped_overload: AtomicU64,
    in_flight: AtomicUsize,
    max_lateness_us: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Stats {
            started: Mutex::new(Instant::now()),
            latency: Mutex::new(HashMap::new()),
            outcomes: Mutex::new(HashMap::new()),
            reject_codes: Mutex::new(HashMap::new()),
            timeline: Mutex::new(Vec::new()),
            cross_sent: Mutex::new(HashMap::new()),
            fills_seen: Mutex::new(HashMap::new()),
            public_latency: Mutex::new(histogram()),
            public_messages: AtomicU64::new(0),
            skipped_overload: AtomicU64::new(0),
            in_flight: AtomicUsize::new(0),
            max_lateness_us: AtomicU64::new(0),
        }
    }

    fn record(&self, kind: Kind, outcome: Outcome, scheduled: Instant) {
        let now = Instant::now();
        *self
            .outcomes
            .lock()
            .unwrap()
            .entry((kind, outcome))
            .or_default() += 1;
        if outcome == Outcome::Ok {
            let us = now.saturating_duration_since(scheduled).as_micros() as u64;
            self.latency
                .lock()
                .unwrap()
                .entry(kind)
                .or_insert_with(histogram)
                .saturating_record(us.max(1));
        }
        let started = *self.started.lock().unwrap();
        let second = now.saturating_duration_since(started).as_secs() as usize;
        let mut t = self.timeline.lock().unwrap();
        if t.len() <= second {
            t.resize(second + 1, (0, 0));
        }
        if outcome == Outcome::Ok {
            t[second].0 += 1;
        } else {
            t[second].1 += 1;
        }
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}

async fn call(
    http: &Http,
    base: &str,
    method: Method,
    path: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> Result<(StatusCode, Value), Outcome> {
    let mut req = Request::builder()
        .method(method)
        .uri(format!("{base}{path}"))
        .header("accept", "application/json");
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    let bytes = match body {
        Some(b) => {
            req = req.header("content-type", "application/json");
            Bytes::from(b.to_string())
        }
        None => Bytes::new(),
    };
    let req = req.body(Full::new(bytes)).map_err(|_| Outcome::Error)?;
    let resp = match timeout(REQUEST_TIMEOUT, http.request(req)).await {
        Err(_) => return Err(Outcome::Timeout),
        Ok(Err(_)) => return Err(Outcome::Error),
        Ok(Ok(r)) => r,
    };
    let status = resp.status();
    let body = match timeout(REQUEST_TIMEOUT, resp.into_body().collect()).await {
        Err(_) => return Err(Outcome::Timeout),
        Ok(Err(_)) => return Err(Outcome::Error),
        Ok(Ok(b)) => b.to_bytes(),
    };
    let value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    Ok((status, value))
}

async fn load_stocks(http: &Http, gateway: &str) -> Result<Vec<Stock>, String> {
    for attempt in 0..30 {
        match call(http, gateway, Method::GET, "/v1/symbols", None, None).await {
            Ok((s, v)) if s.is_success() => {
                let stocks: Vec<Stock> = v["symbols"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|s| s["trading"].as_bool().unwrap_or(false))
                    .filter_map(|s| {
                        let symbol = s["symbol"].as_str()?.to_owned();
                        let reference_cents = s["last_price_cents"]
                            .as_i64()
                            .or_else(|| s["listing_price_cents"].as_i64())?;
                        Some(Stock {
                            symbol,
                            reference_cents,
                        })
                    })
                    .collect();
                if stocks.is_empty() {
                    return Err("no tradable stocks listed".into());
                }
                return Ok(stocks);
            }
            other => {
                if attempt == 29 {
                    return Err(format!("GET /v1/symbols failed: {other:?}"));
                }
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
    unreachable!("the loop returns on its last attempt")
}

/// Registers, logs in and funds one user. Retries brief outages.
async fn make_user(
    accounts: AccountsServiceClient<Channel>,
    run: &str,
    index: usize,
    stocks: &[Stock],
) -> Result<User, String> {
    let email = format!("load-{run}-{index}@loadgen.test");
    let password = format!("pw-{}", uuid::Uuid::new_v4().simple());
    let pair = [index % stocks.len(), (index + 7) % stocks.len()];
    let mut last = String::new();
    for _ in 0..10 {
        match fund(accounts.clone(), &email, &password, pair, stocks).await {
            Ok((id, tokens)) => {
                return Ok(User {
                    id,
                    token: RwLock::new(tokens.access_token),
                    refresh: Mutex::new(tokens.refresh_token),
                    stocks: pair,
                    open: Mutex::new([VecDeque::new(), VecDeque::new()]),
                });
            }
            Err(e) => {
                last = e;
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
    Err(format!("user {index}: {last}"))
}

async fn fund(
    mut accounts: AccountsServiceClient<Channel>,
    email: &str,
    password: &str,
    pair: [usize; 2],
    stocks: &[Stock],
) -> Result<(String, acc::Tokens), String> {
    let e = |s: tonic::Status| format!("{}: {}", s.code(), s.message());
    match accounts
        .register(acc::RegisterRequest {
            email: email.into(),
            password: password.into(),
        })
        .await
    {
        Ok(_) => {}
        // A retry after a lost reply: the account exists already.
        Err(s) if s.code() == tonic::Code::AlreadyExists => {}
        Err(s) => return Err(e(s)),
    }
    let login = accounts
        .login(acc::LoginRequest {
            email: email.into(),
            password: password.into(),
        })
        .await
        .map_err(e)?
        .into_inner();
    let tokens = login.tokens.ok_or("no tokens returned")?;
    let mut deposits = vec![("USD".to_owned(), CASH_CENTS)];
    for &i in &pair {
        if !deposits.iter().any(|(a, _)| *a == stocks[i].symbol) {
            deposits.push((stocks[i].symbol.clone(), SHARES));
        }
    }
    for (asset, amount) in deposits {
        accounts
            .deposit(acc::DepositRequest {
                user_id: login.user_id.clone(),
                idempotency_key: format!("loadgen-{email}-{asset}"),
                amount_cents: amount,
                asset,
            })
            .await
            .map_err(e)?;
    }
    Ok((login.user_id, tokens))
}

/// Access tokens last 15 minutes; renew every user's every 10.
fn spawn_refresher(accounts: AccountsServiceClient<Channel>, users: Arc<Vec<Arc<User>>>) {
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(600)).await;
            for u in users.iter() {
                let refresh = u.refresh.lock().unwrap().clone();
                let mut client = accounts.clone();
                match client
                    .refresh(acc::RefreshRequest {
                        refresh_token: refresh,
                    })
                    .await
                {
                    Ok(r) => {
                        if let Some(t) = r.into_inner().tokens {
                            *u.token.write().unwrap() = t.access_token;
                            *u.refresh.lock().unwrap() = t.refresh_token;
                        }
                    }
                    Err(s) => eprintln!("token refresh for {} failed: {s}", u.id),
                }
            }
        }
    });
}

/// A private socket for one user, straight to Notifications. Reconnects
/// with a fresh ticket if it drops.
fn spawn_watcher(http: Http, cfg: Arc<Config>, user: Arc<User>, stats: Arc<Stats>) {
    tokio::spawn(async move {
        loop {
            let token = user.token.read().unwrap().clone();
            let ticket = match call(
                &http,
                &cfg.gateway,
                Method::POST,
                "/v1/ws-ticket",
                Some(&token),
                None,
            )
            .await
            {
                Ok((s, v)) if s.is_success() => v["ticket"].as_str().unwrap_or_default().to_owned(),
                _ => {
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };
            let url = format!("{}/ws/private?ticket={ticket}", cfg.notifications_ws);
            let Ok(Ok((ws, _))) = timeout(
                Duration::from_secs(10),
                tokio_tungstenite::connect_async(url),
            )
            .await
            else {
                sleep(Duration::from_secs(2)).await;
                continue;
            };
            let (_, mut read) = ws.split();
            while let Some(Ok(msg)) = read.next().await {
                let Message::Text(text) = msg else { continue };
                let arrived = Instant::now();
                let Ok(v) = serde_json::from_str::<Value>(text.as_str()) else {
                    continue;
                };
                if v["type"] == "fill"
                    && let Some(order) = v["data"]["order_id"].as_str()
                {
                    stats
                        .fills_seen
                        .lock()
                        .unwrap()
                        .entry(order.to_owned())
                        .or_insert(arrived);
                }
            }
            sleep(Duration::from_secs(1)).await;
        }
    });
}

/// A public socket subscribed to every stock's trades, straight to Market Data.
fn spawn_public_client(cfg: Arc<Config>, symbols: Arc<Vec<String>>, stats: Arc<Stats>) {
    tokio::spawn(async move {
        loop {
            let url = format!("{}/ws/public", cfg.market_data_ws);
            let Ok(Ok((ws, _))) = timeout(
                Duration::from_secs(10),
                tokio_tungstenite::connect_async(url),
            )
            .await
            else {
                sleep(Duration::from_secs(2)).await;
                continue;
            };
            let (mut write, mut read) = ws.split();
            let mut ok = true;
            for s in symbols.iter() {
                let sub = json!({ "op": "subscribe", "channel": "trades", "symbol": s });
                if write.send(Message::text(sub.to_string())).await.is_err() {
                    ok = false;
                    break;
                }
            }
            while ok && let Some(Ok(msg)) = read.next().await {
                let Message::Text(text) = msg else { continue };
                let arrived_ns = now_ns();
                let Ok(v) = serde_json::from_str::<Value>(text.as_str()) else {
                    continue;
                };
                if v["type"] == "trade"
                    && let Some(t) = v["time_ns"].as_i64()
                {
                    let us = (arrived_ns - t).max(1) / 1_000;
                    stats
                        .public_latency
                        .lock()
                        .unwrap()
                        .saturating_record(us.max(1) as u64);
                    stats.public_messages.fetch_add(1, Ordering::Relaxed);
                }
            }
            sleep(Duration::from_secs(1)).await;
        }
    });
}

struct Op {
    kind: Kind,
    user: usize,
    slot: usize,
    buy: bool,
    offset_steps: i64,
    quantity: i64,
    watched: bool,
}

fn choose(rng: &mut Rng, users: &[Arc<User>], watchers: usize) -> Op {
    let user = rng.below(users.len() as u64) as usize;
    let slot = rng.below(2) as usize;
    let roll = rng.below(100);
    let mut kind = match roll {
        0..=59 => Kind::Rest,
        60..=89 => Kind::Cancel,
        _ => Kind::Cross,
    };
    {
        let open = users[user].open.lock().unwrap();
        if kind == Kind::Rest && open[slot].len() >= MAX_OPEN {
            kind = Kind::Cancel;
        } else if kind == Kind::Cancel && open[slot].is_empty() {
            kind = Kind::Rest;
        }
    }
    Op {
        kind,
        user,
        slot,
        buy: rng.below(2) == 0,
        offset_steps: 1 + rng.below(10) as i64,
        quantity: 1 + rng.below(if kind == Kind::Cross { 5 } else { 10 }) as i64,
        watched: user < watchers,
    }
}

async fn run_op(
    op: Op,
    scheduled: Instant,
    http: Http,
    cfg: Arc<Config>,
    users: Arc<Vec<Arc<User>>>,
    stocks: Arc<Vec<Stock>>,
    stats: Arc<Stats>,
) {
    let user = &users[op.user];
    let token = user.token.read().unwrap().clone();
    let stock = &stocks[user.stocks[op.slot]];
    let result = match op.kind {
        Kind::Cancel => {
            let id = user.open.lock().unwrap()[op.slot].pop_front();
            match id {
                Some(id) => {
                    call(
                        &http,
                        &cfg.gateway,
                        Method::DELETE,
                        &format!("/v1/orders/{id}"),
                        Some(&token),
                        None,
                    )
                    .await
                }
                // Another task took the last one; nothing to do.
                None => return,
            }
        }
        Kind::Rest | Kind::Cross => {
            let step = stock.step();
            let (price, tif) = match (op.kind, op.buy) {
                (Kind::Rest, true) => (stock.reference_cents - op.offset_steps * step, "GTC"),
                (Kind::Rest, false) => (stock.reference_cents + op.offset_steps * step, "GTC"),
                // Through every resting order on the other side.
                (_, true) => (stock.reference_cents + 11 * step, "IOC"),
                (_, false) => (stock.reference_cents - 11 * step, "IOC"),
            };
            let body = json!({
                "client_order_id": uuid::Uuid::now_v7().to_string(),
                "symbol": stock.symbol,
                "side": if op.buy { "BUY" } else { "SELL" },
                "type": "LIMIT",
                "time_in_force": tif,
                "price_cents": price.max(1),
                "quantity": op.quantity,
            });
            call(
                &http,
                &cfg.gateway,
                Method::POST,
                "/v1/orders",
                Some(&token),
                Some(body),
            )
            .await
        }
    };
    let outcome = match &result {
        Ok((status, _)) => Outcome::of(*status),
        Err(o) => *o,
    };
    stats.record(op.kind, outcome, scheduled);
    let Ok((status, body)) = result else { return };
    if outcome == Outcome::Rejected || outcome == Outcome::Unavailable {
        let code = body["error"]["code"]
            .as_str()
            .or_else(|| body["code"].as_str())
            .map_or_else(|| status.as_u16().to_string(), str::to_owned);
        *stats
            .reject_codes
            .lock()
            .unwrap()
            .entry(format!("{} {code}", op.kind.name()))
            .or_default() += 1;
    }
    if outcome != Outcome::Ok || op.kind == Kind::Cancel {
        return;
    }
    let Some(order_id) = body["order_id"].as_str() else {
        return;
    };
    let finished = matches!(
        body["status"].as_str(),
        Some("FILLED" | "CANCELED" | "REJECTED")
    );
    match op.kind {
        Kind::Rest if !finished => {
            user.open.lock().unwrap()[op.slot].push_back(order_id.to_owned());
        }
        Kind::Cross if op.watched => {
            stats
                .cross_sent
                .lock()
                .unwrap()
                .insert(order_id.to_owned(), scheduled);
        }
        _ => {}
    }
}

fn summary(h: &Histogram<u64>) -> Value {
    if h.is_empty() {
        return json!({ "count": 0 });
    }
    let ms = |q: f64| h.value_at_quantile(q) as f64 / 1_000.0;
    json!({
        "count": h.len(),
        "p50_ms": ms(0.50),
        "p90_ms": ms(0.90),
        "p99_ms": ms(0.99),
        "p999_ms": ms(0.999),
        "max_ms": h.max() as f64 / 1_000.0,
    })
}

/// p99 of a Prometheus histogram over the run, in milliseconds.
async fn prometheus_p99(http: &Http, base: &str, metric: &str, seconds: u64) -> Value {
    let query =
        format!("histogram_quantile(0.99, sum by (le) (increase({metric}_bucket[{seconds}s])))");
    let mut encoded = String::new();
    for b in query.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            encoded.push(b as char);
        } else {
            encoded.push_str(&format!("%{b:02X}"));
        }
    }
    match call(
        http,
        base,
        Method::GET,
        &format!("/api/v1/query?query={encoded}"),
        None,
        None,
    )
    .await
    {
        Ok((s, v)) if s.is_success() => v["data"]["result"][0]["value"][1]
            .as_str()
            .and_then(|x| x.parse::<f64>().ok())
            .filter(|x| x.is_finite())
            .map_or(Value::Null, |secs| json!(secs * 1_000.0)),
        _ => Value::Null,
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("loadgen: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let cfg = Arc::new(Config::parse()?);
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    let http: Http = Client::builder(TokioExecutor::new())
        .pool_max_idle_per_host(2_048)
        .pool_idle_timeout(Duration::from_secs(30))
        .build(connector);
    let accounts = AccountsServiceClient::new(
        Channel::from_shared(cfg.accounts.clone())
            .map_err(|e| e.to_string())?
            .connect_lazy(),
    );

    let stocks = Arc::new(load_stocks(&http, &cfg.gateway).await?);
    println!(
        "{} stocks; setting up {} users (cash and shares through Accounts)",
        stocks.len(),
        cfg.users
    );
    let run_id = uuid::Uuid::now_v7().simple().to_string();
    let setup_started = Instant::now();
    let gate = Arc::new(Semaphore::new(32));
    let mut tasks = Vec::with_capacity(cfg.users);
    for i in 0..cfg.users {
        let (accounts, stocks, gate, run_id) = (
            accounts.clone(),
            stocks.clone(),
            gate.clone(),
            run_id.clone(),
        );
        tasks.push(tokio::spawn(async move {
            let _permit = gate.acquire_owned().await;
            make_user(accounts, &run_id[..12], i, &stocks).await
        }));
    }
    let mut users = Vec::with_capacity(cfg.users);
    for t in tasks {
        users.push(Arc::new(t.await.map_err(|e| e.to_string())??));
    }
    let users = Arc::new(users);
    println!("setup took {:.1} s", setup_started.elapsed().as_secs_f64());
    spawn_refresher(accounts, users.clone());

    let stats = Arc::new(Stats::new());
    for u in users.iter().take(cfg.watchers) {
        spawn_watcher(http.clone(), cfg.clone(), u.clone(), stats.clone());
    }
    let symbols = Arc::new(stocks.iter().map(|s| s.symbol.clone()).collect::<Vec<_>>());
    for _ in 0..cfg.public_clients {
        spawn_public_client(cfg.clone(), symbols.clone(), stats.clone());
    }
    // Let the sockets connect before the clock starts.
    sleep(Duration::from_secs(2)).await;

    println!(
        "sending {} orders/s for {} s (open loop, seed {})",
        cfg.rate,
        cfg.duration.as_secs(),
        cfg.seed
    );
    *stats.started.lock().unwrap() = Instant::now();
    let mut rng = Rng(cfg.seed);
    let interval = Duration::from_nanos(1_000_000_000 / cfg.rate);
    let total = cfg.rate * cfg.duration.as_secs();
    let start = *stats.started.lock().unwrap();
    let mut next_report = start + Duration::from_secs(10);
    for k in 0..total {
        let scheduled = start + interval * (k as u32);
        let now = Instant::now();
        if scheduled > now {
            sleep_until(scheduled).await;
        } else {
            let late = now.duration_since(scheduled).as_micros() as u64;
            stats.max_lateness_us.fetch_max(late, Ordering::Relaxed);
        }
        if Instant::now() >= next_report {
            next_report += Duration::from_secs(10);
            let ok: u64 = stats.timeline.lock().unwrap().iter().map(|t| t.0).sum();
            let bad: u64 = stats.timeline.lock().unwrap().iter().map(|t| t.1).sum();
            println!(
                "  {:>4} s: {ok} ok, {bad} not ok, {} in flight",
                start.elapsed().as_secs(),
                stats.in_flight.load(Ordering::Relaxed)
            );
        }
        if stats.in_flight.load(Ordering::Relaxed) >= MAX_IN_FLIGHT {
            stats.skipped_overload.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let op = choose(&mut rng, &users, cfg.watchers);
        stats.in_flight.fetch_add(1, Ordering::Relaxed);
        let (http, cfg, users, stocks, stats) = (
            http.clone(),
            cfg.clone(),
            users.clone(),
            stocks.clone(),
            stats.clone(),
        );
        tokio::spawn(async move {
            run_op(
                op,
                scheduled,
                http,
                cfg.clone(),
                users,
                stocks,
                stats.clone(),
            )
            .await;
            stats.in_flight.fetch_sub(1, Ordering::Relaxed);
        });
    }
    let sent_for = start.elapsed();
    let drain_until = Instant::now() + REQUEST_TIMEOUT + Duration::from_secs(1);
    while stats.in_flight.load(Ordering::Relaxed) > 0 && Instant::now() < drain_until {
        sleep(Duration::from_millis(100)).await;
    }
    // Late fill notifications.
    sleep(Duration::from_secs(3)).await;

    let report = build_report(&cfg, &http, &stats, total, sent_for).await;
    let text = serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?;
    println!("{text}");
    if let Some(path) = &cfg.out {
        std::fs::write(path, &text).map_err(|e| format!("{path}: {e}"))?;
        println!("wrote {path}");
    }
    Ok(())
}

async fn build_report(
    cfg: &Config,
    http: &Http,
    stats: &Stats,
    total: u64,
    sent_for: Duration,
) -> Value {
    let outcomes = stats.outcomes.lock().unwrap().clone();
    let mut by_kind = serde_json::Map::new();
    let mut all_ok = histogram();
    let mut placed_ok = histogram();
    for kind in [Kind::Rest, Kind::Cancel, Kind::Cross] {
        let mut counts = serde_json::Map::new();
        for o in Outcome::ALL {
            let n = outcomes.get(&(kind, o)).copied().unwrap_or(0);
            if n > 0 {
                counts.insert(o.name().into(), n.into());
            }
        }
        let latency = stats.latency.lock().unwrap();
        let h = latency.get(&kind).cloned().unwrap_or_else(histogram);
        let _ = all_ok.add(&h);
        if kind != Kind::Cancel {
            let _ = placed_ok.add(&h);
        }
        by_kind.insert(
            kind.name().into(),
            json!({ "outcomes": counts, "latency_ok": summary(&h) }),
        );
    }
    let finished: u64 = outcomes.values().sum();
    let ok: u64 = outcomes
        .iter()
        .filter(|((_, o), _)| *o == Outcome::Ok)
        .map(|(_, n)| n)
        .sum();

    let mut fill = histogram();
    let sent = stats.cross_sent.lock().unwrap().clone();
    let seen = stats.fills_seen.lock().unwrap().clone();
    let mut crossed_without_fill = 0u64;
    for (order, at) in sent.iter() {
        match seen.get(order) {
            Some(arrived) => {
                fill.saturating_record(
                    (arrived.saturating_duration_since(*at).as_micros() as u64).max(1),
                );
            }
            None => crossed_without_fill += 1,
        }
    }

    let seconds = (sent_for.as_secs() + 30).max(60);
    let (settle, end_to_end) = match &cfg.prometheus {
        Some(p) => (
            prometheus_p99(http, p, "settlement_seconds", seconds).await,
            prometheus_p99(http, p, "order_end_to_end_seconds", seconds).await,
        ),
        None => (Value::Null, Value::Null),
    };
    let timeline = stats.timeline.lock().unwrap().clone();
    let public = stats.public_latency.lock().unwrap().clone();
    let rejects = stats.reject_codes.lock().unwrap().clone();

    json!({
        "label": cfg.label,
        "config": {
            "rate_per_s": cfg.rate,
            "duration_s": cfg.duration.as_secs(),
            "users": cfg.users,
            "watchers": cfg.watchers,
            "public_clients": cfg.public_clients,
            "seed": cfg.seed,
        },
        "scheduled": total,
        "finished": finished,
        "ok": ok,
        "ok_percent": if finished == 0 { 0.0 } else { ok as f64 * 100.0 / finished as f64 },
        "achieved_ok_per_s": ok as f64 / sent_for.as_secs_f64().max(1e-9),
        "skipped_generator_overloaded": stats.skipped_overload.load(Ordering::Relaxed),
        "max_send_lateness_ms": stats.max_lateness_us.load(Ordering::Relaxed) as f64 / 1_000.0,
        "requests": by_kind,
        "order_accepted_latency": summary(&placed_ok),
        "all_requests_latency": summary(&all_ok),
        "reject_codes": rejects,
        "order_to_fill_notification": {
            "latency": summary(&fill),
            "ioc_orders_watched": sent.len(),
            "without_fill": crossed_without_fill,
            "note": "IOC orders of watched users; an IOC that found nothing to trade has no fill",
        },
        "trade_to_public_message": {
            "latency": summary(&public),
            "messages": stats.public_messages.load(Ordering::Relaxed),
        },
        "prometheus_p99_ms": {
            "trade_to_settled": settle,
            "order_end_to_end": end_to_end,
        },
        "timeline_ok_notok_per_s": timeline,
    })
}
