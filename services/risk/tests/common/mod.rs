//! Risk against real services, in process:
//!
//! - one PostgreSQL 16 container with separate Accounts and Risk databases;
//! - the real Accounts gRPC server on a local port, reached through a TCP
//!   proxy that can add network delay (standing in for Toxiproxy);
//! - a "pump" that drains Risk's outbox with the real relay code, runs the
//!   commands through the real matching engine, applies the events back to
//!   Risk, and settles each trade with Accounts (standing in for Settlement).
//!
//! Needs Docker running.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use accounts::audit::{self, When};
use accounts::{Accounts, TokenSigner};
use consumer::HandlerError;
use matching::codec::{decode_command, encode_event};
use matching::{DEFAULT_DEDUP_CAPACITY, Engine};
use prost::Message;
use proto_gen::accounts::v1 as apb;
use proto_gen::events::v1 as ev;
use proto_gen::risk::v1 as pb;
use risk::{AccountsApi, GrpcAccounts, Risk};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tonic::Status;

pub const HEALTHY: u8 = 0;
/// Reserve runs, but its reply is lost (spec failure test "Lost reservation reply").
pub const LOSE_RESERVE_REPLY: u8 = 1;
/// Every call fails as if Accounts were down.
pub const DOWN: u8 = 2;

/// The real Accounts client, with switchable failures.
#[derive(Clone)]
pub struct Flaky {
    inner: GrpcAccounts,
    pub mode: Arc<AtomicU8>,
}

impl AccountsApi for Flaky {
    async fn reserve(&self, r: apb::ReserveRequest) -> Result<apb::ReserveResponse, Status> {
        match self.mode.load(Ordering::SeqCst) {
            DOWN => Err(Status::unavailable("accounts is down")),
            LOSE_RESERVE_REPLY => {
                let _ = self.inner.reserve(r).await;
                Err(Status::deadline_exceeded("reply lost"))
            }
            _ => self.inner.reserve(r).await,
        }
    }

    async fn release(&self, r: apb::ReleaseRequest) -> Result<apb::ReleaseResponse, Status> {
        match self.mode.load(Ordering::SeqCst) {
            DOWN => Err(Status::unavailable("accounts is down")),
            _ => self.inner.release(r).await,
        }
    }
}

/// A TCP proxy that delays every byte sent towards Accounts by `delay_ms`,
/// keeping order: requests arrive late and still run, as with Toxiproxy's
/// latency toxic.
async fn delay_proxy(
    upstream: std::net::SocketAddr,
    delay_ms: Arc<AtomicU64>,
) -> std::net::SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let Ok(server) = tokio::net::TcpStream::connect(upstream).await else {
                continue;
            };
            let (mut cr, mut cw) = client.into_split();
            let (mut sr, mut sw) = server.into_split();
            let delay_ms = delay_ms.clone();
            // Towards Accounts: stamp each chunk with its due time.
            let (tx, mut rx) =
                tokio::sync::mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 16 * 1024];
                while let Ok(n @ 1..) = cr.read(&mut buf).await {
                    let due = tokio::time::Instant::now()
                        + Duration::from_millis(delay_ms.load(Ordering::SeqCst));
                    if tx.send((due, buf[..n].to_vec())).is_err() {
                        break;
                    }
                }
            });
            tokio::spawn(async move {
                while let Some((due, bytes)) = rx.recv().await {
                    tokio::time::sleep_until(due).await;
                    if sw.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
                let _ = sw.shutdown().await;
            });
            // Replies come straight back.
            tokio::spawn(async move {
                let _ = tokio::io::copy(&mut sr, &mut cw).await;
            });
        }
    });
    addr
}

/// Collects what the relay publishes.
#[derive(Default, Clone)]
pub struct Collect(pub Arc<Mutex<Vec<outbox::Row>>>);

impl outbox::Publisher for Collect {
    async fn publish(&self, rows: &[outbox::Row]) -> Result<(), String> {
        self.0.lock().unwrap().extend_from_slice(rows);
        Ok(())
    }
}

pub struct Env {
    pub risk: Arc<Risk<Flaky>>,
    pub accounts: Arc<Accounts>,
    pub mode: Arc<AtomicU8>,
    /// Network delay added to every call from Risk to Accounts.
    pub delay_ms: Arc<AtomicU64>,
    pub engine: Mutex<Engine>,
    offset: Mutex<u64>,
    /// Every event the engine produced, in order.
    pub events: Mutex<Vec<ev::MatchingEvent>>,
    /// Market Data's ticker, for market orders (see [`env_with_prices`]).
    pub prices: Option<Arc<ticker::Store>>,
    _container: ContainerAsync<Postgres>,
    pub redis: Option<ContainerAsync<Redis>>,
}

async fn pool(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(20)
        .connect(url)
        .await
        .unwrap()
}

pub async fn env() -> Env {
    env_inner(false).await
}

/// [`env`] plus a Redis container holding the ticker Risk reads.
pub async fn env_with_prices() -> Env {
    env_inner(true).await
}

async fn env_inner(with_prices: bool) -> Env {
    let (redis, prices) = if with_prices {
        let r = Redis::default()
            .with_tag("7-alpine")
            .start()
            .await
            .expect("start redis");
        let url = format!(
            "redis://{}:{}",
            r.get_host().await.unwrap(),
            r.get_host_port_ipv4(6379).await.unwrap()
        );
        (Some(r), Some(Arc::new(ticker::Store::new(&url).unwrap())))
    } else {
        (None, None)
    };
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("start postgres (is Docker running?)");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let base = format!("postgres://postgres:postgres@{host}:{port}");
    let accounts_db = pool(&format!("{base}/postgres")).await;
    sqlx::query("CREATE DATABASE risk")
        .execute(&accounts_db)
        .await
        .unwrap();
    let risk_db = pool(&format!("{base}/risk")).await;
    accounts::MIGRATOR.run(&accounts_db).await.unwrap();
    risk::MIGRATOR.run(&risk_db).await.unwrap();

    // Real Accounts over gRPC on a free local port.
    let (private, _) = TokenSigner::generate_pem().unwrap();
    let accounts = Arc::new(Accounts::new(
        accounts_db,
        TokenSigner::from_pem(&private).unwrap(),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = accounts::grpc::Service::server(accounts.clone());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    let delay_ms = Arc::new(AtomicU64::new(0));
    let proxied = delay_proxy(addr, delay_ms.clone()).await;
    let mode = Arc::new(AtomicU8::new(HEALTHY));
    let client = Flaky {
        inner: GrpcAccounts::connect_lazy(&format!("http://{proxied}")).unwrap(),
        mode: mode.clone(),
    };
    let mut risk = Risk::new(risk_db, client);
    if let Some(p) = &prices {
        risk = risk.with_prices(p.clone());
    }
    Env {
        risk: Arc::new(risk),
        prices,
        redis,
        accounts,
        mode,
        delay_ms,
        engine: Mutex::new(Engine::new(0, DEFAULT_DEDUP_CAPACITY)),
        offset: Mutex::new(0),
        events: Mutex::new(Vec::new()),
        _container: container,
    }
}

pub const PW: &str = "correct horse battery";

impl Env {
    pub fn set_mode(&self, m: u8) {
        self.mode.store(m, Ordering::SeqCst);
    }

    /// Network delay on every call from Risk to Accounts (0 = none).
    pub fn set_delay(&self, d: Duration) {
        self.delay_ms.store(d.as_millis() as u64, Ordering::SeqCst);
    }

    /// Order IDs still in PENDING_RISK.
    pub async fn pending_risk(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM orders WHERE status = 'PENDING_RISK'")
            .fetch_one(self.risk.pool())
            .await
            .unwrap()
    }

    /// A registered user with `cents` USD and `shares` of each listed stock name.
    pub async fn user(&self, name: &str, cents: i64, shares: &[(&str, i64)]) -> String {
        let u = self
            .accounts
            .register(&format!("{name}@example.com"), PW)
            .await
            .unwrap()
            .user_id;
        if cents > 0 {
            self.accounts
                .deposit(&u, "seed", "USD", cents)
                .await
                .unwrap();
        }
        for (sym, n) in shares {
            self.accounts
                .deposit(&u, &format!("seed-{sym}"), sym, *n)
                .await
                .unwrap();
        }
        u
    }

    /// (available, reserved) of one asset.
    pub async fn balance(&self, user: &str, asset: &str) -> (i64, i64) {
        self.accounts
            .balances(user)
            .await
            .unwrap()
            .balances
            .into_iter()
            .find(|b| b.asset == asset)
            .map(|b| (b.available, b.reserved))
            .unwrap_or((0, 0))
    }

    pub async fn place(
        &self,
        user: &str,
        coid: &str,
        symbol: &str,
        side: ev::Side,
        price: i64,
        qty: i64,
    ) -> Result<pb::PlaceOrderResponse, risk::RiskError> {
        self.risk
            .place_order(&limit(user, coid, symbol, side, price, qty))
            .await
    }

    pub async fn order(&self, user: &str, order_id: &str) -> pb::Order {
        self.risk
            .get_order(user, order_id)
            .await
            .unwrap()
            .order
            .unwrap()
    }

    /// Relay → engine → Risk (and Accounts.SettleTrade for trades) until the
    /// outbox is empty. Returns the events produced.
    pub async fn pump(&self) -> Vec<ev::MatchingEvent> {
        let mut produced = Vec::new();
        loop {
            let sink = Collect::default();
            let mut conn = outbox::try_lock(self.risk.pool(), risk::OUTBOX_LOCK_KEY)
                .await
                .unwrap()
                .expect("relay lock is free");
            let n = outbox::publish_batch(&mut conn, &sink, 500).await.unwrap();
            drop(conn.detach());
            if n == 0 {
                return produced;
            }
            let rows = sink.0.lock().unwrap().clone();
            for row in rows {
                assert_eq!(row.topic, risk::ORDERS_COMMANDS_TOPIC);
                let (symbol, cmd) =
                    decode_command(&row.payload).expect("Risk sends valid commands");
                assert_eq!(row.key, symbol.as_str(), "commands are keyed by symbol");
                let mut out = Vec::new();
                {
                    let mut off = self.offset.lock().unwrap();
                    self.engine
                        .lock()
                        .unwrap()
                        .process(*off, &symbol, &cmd, &mut out)
                        .unwrap();
                    *off += 1;
                }
                for e in &out {
                    let pb_event =
                        ev::MatchingEvent::decode(encode_event(&symbol, e).as_slice()).unwrap();
                    self.risk
                        .apply_event(&pb_event)
                        .await
                        .expect("Risk applies the event");
                    if let Some(ev::matching_event::Body::Trade(t)) = &pb_event.body {
                        self.settle(&pb_event.symbol, t).await;
                    }
                    self.events.lock().unwrap().push(pb_event.clone());
                    produced.push(pb_event);
                }
            }
        }
    }

    pub async fn settle(&self, symbol: &str, t: &ev::TradeExecuted) {
        self.accounts
            .settle_trade(&apb::SettleTradeRequest {
                trade_id: t.trade_id.clone(),
                symbol: symbol.into(),
                price_cents: t.price_cents,
                quantity: t.quantity,
                buy_order_id: t.buy_order_id.clone(),
                sell_order_id: t.sell_order_id.clone(),
                buyer_id: t.buyer_id.clone(),
                seller_id: t.seller_id.clone(),
            })
            .await
            .expect("every trade settles");
    }

    pub async fn reapply_all(&self) -> Result<(), HandlerError> {
        let all = self.events.lock().unwrap().clone();
        for e in &all {
            self.risk.apply_event(e).await?;
        }
        Ok(())
    }

    /// The Accounts auditor rules, after traffic stops.
    pub async fn audit(&self) {
        let problems = audit::check(self.accounts.pool(), When::TrafficStopped)
            .await
            .unwrap();
        assert!(problems.is_empty(), "auditor found: {problems:#?}");
    }
}

pub fn limit(
    user: &str,
    coid: &str,
    symbol: &str,
    side: ev::Side,
    price: i64,
    qty: i64,
) -> pb::PlaceOrderRequest {
    pb::PlaceOrderRequest {
        user_id: user.into(),
        client_order_id: coid.into(),
        symbol: symbol.into(),
        side: side as i32,
        order_type: ev::OrderType::Limit as i32,
        time_in_force: ev::TimeInForce::Gtc as i32,
        price_cents: price,
        quantity: qty,
    }
}

pub fn status(o: &pb::Order) -> pb::OrderStatus {
    pb::OrderStatus::try_from(o.status).unwrap()
}
