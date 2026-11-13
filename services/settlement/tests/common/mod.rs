//! The whole Phase 2 money flow in process:
//!
//! - one PostgreSQL 16 container with the Accounts, Risk and Settlement
//!   databases;
//! - the real Accounts gRPC server on a local port;
//! - Risk, the real matching engine and Settlement, joined by a "pump" that
//!   plays Kafka: Risk's outbox feeds the engine, and every event goes to
//!   Risk at once and into Settlement's queue, which is delivered in order
//!   and keeps the failed message at its head, like a partition;
//! - the auditor over all of it.
//!
//! Needs Docker running.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use accounts::{Accounts, TokenSigner};
use auditor::{Report, Sources, When};
use consumer::HandlerError;
use matching::codec::{decode_command, encode_event};
use matching::{DEFAULT_DEDUP_CAPACITY, Engine};
use prost::Message;
use proto_gen::accounts::v1 as apb;
use proto_gen::events::v1 as ev;
use proto_gen::risk::v1 as pb;
use risk::Risk;
use settlement::{AccountsApi, GrpcAccounts, Settlement};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tonic::Status;

pub const HEALTHY: u8 = 0;
/// SettleTrade runs, but its reply is lost.
pub const LOSE_REPLY: u8 = 1;
/// Every call fails as if Accounts were down.
pub const DOWN: u8 = 2;

/// Settlement's Accounts client, with switchable failures.
#[derive(Clone)]
pub struct Flaky {
    inner: GrpcAccounts,
    pub mode: Arc<AtomicU8>,
}

impl AccountsApi for Flaky {
    async fn settle_trade(
        &self,
        r: apb::SettleTradeRequest,
    ) -> Result<apb::SettleTradeResponse, Status> {
        match self.mode.load(Ordering::SeqCst) {
            DOWN => Err(Status::unavailable("accounts is down")),
            LOSE_REPLY => {
                let _ = self.inner.settle_trade(r).await;
                Err(Status::deadline_exceeded("reply lost"))
            }
            _ => self.inner.settle_trade(r).await,
        }
    }
}

/// Collects what a relay publishes.
#[derive(Default, Clone)]
pub struct Collect(pub Arc<Mutex<Vec<outbox::Row>>>);

impl outbox::Publisher for Collect {
    async fn publish(&self, rows: &[outbox::Row]) -> Result<(), String> {
        self.0.lock().unwrap().extend_from_slice(rows);
        Ok(())
    }
}

pub struct Env {
    pub accounts: Arc<Accounts>,
    pub risk: Arc<Risk<risk::GrpcAccounts>>,
    pub settlement: Arc<Settlement<Flaky>>,
    pub mode: Arc<AtomicU8>,
    engine: Mutex<Engine>,
    offset: Mutex<u64>,
    /// Every event the engine produced, in order: the matching.events log.
    pub events: Mutex<Vec<ev::MatchingEvent>>,
    /// Events Settlement has not yet handled successfully.
    queue: Mutex<VecDeque<ev::MatchingEvent>>,
    _container: ContainerAsync<Postgres>,
}

async fn pool(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(20)
        .connect(url)
        .await
        .unwrap()
}

pub async fn env() -> Env {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("start postgres (is Docker running?)");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let base = format!("postgres://postgres:postgres@{host}:{port}");
    let accounts_db = pool(&format!("{base}/postgres")).await;
    for db in ["risk", "settlement"] {
        sqlx::query(&format!("CREATE DATABASE {db}"))
            .execute(&accounts_db)
            .await
            .unwrap();
    }
    let risk_db = pool(&format!("{base}/risk")).await;
    let settlement_db = pool(&format!("{base}/settlement")).await;
    accounts::MIGRATOR.run(&accounts_db).await.unwrap();
    risk::MIGRATOR.run(&risk_db).await.unwrap();
    settlement::MIGRATOR.run(&settlement_db).await.unwrap();

    let (private, _) = TokenSigner::generate_pem().unwrap();
    let accounts = Arc::new(Accounts::new(
        accounts_db,
        TokenSigner::from_pem(&private).unwrap(),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(accounts::grpc::Service::server(accounts.clone()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    let mode = Arc::new(AtomicU8::new(HEALTHY));
    let flaky = Flaky {
        inner: GrpcAccounts::connect_lazy(&url).unwrap(),
        mode: mode.clone(),
    };
    Env {
        risk: Arc::new(Risk::new(
            risk_db,
            risk::GrpcAccounts::connect_lazy(&url).unwrap(),
        )),
        settlement: Arc::new(Settlement::new(settlement_db, flaky)),
        accounts,
        mode,
        engine: Mutex::new(Engine::new(0, DEFAULT_DEDUP_CAPACITY)),
        offset: Mutex::new(0),
        events: Mutex::new(Vec::new()),
        queue: Mutex::new(VecDeque::new()),
        _container: container,
    }
}

pub const PW: &str = "correct horse battery";

impl Env {
    pub fn set_mode(&self, m: u8) {
        self.mode.store(m, Ordering::SeqCst);
    }

    /// A registered user with `cents` USD and `shares` of each listed stock.
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
            .place_order(&pb::PlaceOrderRequest {
                user_id: user.into(),
                client_order_id: coid.into(),
                symbol: symbol.into(),
                side: side as i32,
                order_type: ev::OrderType::Limit as i32,
                time_in_force: ev::TimeInForce::Gtc as i32,
                price_cents: price,
                quantity: qty,
            })
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

    /// Risk's outbox → engine → Risk, queuing every event for Settlement,
    /// until Risk has nothing left to send. Returns the events produced.
    pub async fn run_risk(&self) -> Vec<ev::MatchingEvent> {
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
                let (symbol, cmd) = decode_command(&row.payload).unwrap();
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
                    let e = ev::MatchingEvent::decode(encode_event(&symbol, e).as_slice()).unwrap();
                    // Like Risk's consume loop: retry what may be temporary.
                    let mut tries = 0;
                    loop {
                        match self.risk.apply_event(&e).await {
                            Ok(()) => break,
                            Err(HandlerError::Retry(m)) if tries < 20 => {
                                tries += 1;
                                eprintln!("Risk retrying: {m}");
                            }
                            Err(err) => panic!("Risk cannot apply {}: {err:?}", e.event_id),
                        }
                    }
                    self.events.lock().unwrap().push(e.clone());
                    self.queue.lock().unwrap().push_back(e.clone());
                    produced.push(e);
                }
            }
        }
    }

    /// Delivers Settlement's queue in order. On an error the failed event
    /// stays first in line, as an uncommitted Kafka offset would.
    pub async fn deliver(&self) -> Result<(), HandlerError> {
        loop {
            let Some(e) = self.queue.lock().unwrap().front().cloned() else {
                return Ok(());
            };
            self.settlement.apply_event(&e).await?;
            self.queue.lock().unwrap().pop_front();
        }
    }

    /// Delivers Settlement's whole queue as one batch, as the consume loop
    /// does with the messages already waiting. On an error the queue stays
    /// as it was.
    pub async fn deliver_batch(&self) -> Result<(), HandlerError> {
        let batch: Vec<_> = self.queue.lock().unwrap().iter().cloned().collect();
        self.settlement.apply_events(&batch).await?;
        self.queue.lock().unwrap().clear();
        Ok(())
    }

    /// Drops the newest event from Settlement's queue.
    pub fn queue_clear_last(&self) {
        self.queue.lock().unwrap().pop_back();
    }

    /// Sends an event to Settlement as if it were redelivered.
    pub fn redeliver(&self, e: ev::MatchingEvent) {
        self.queue.lock().unwrap().push_back(e);
    }

    /// Everything flows and settles.
    pub async fn pump(&self) -> Vec<ev::MatchingEvent> {
        let produced = self.run_risk().await;
        self.deliver().await.expect("Settlement keeps up");
        produced
    }

    /// What Settlement's relay publishes next, decoded.
    pub async fn settled_messages(&self) -> Vec<ev::TradeSettled> {
        let sink = Collect::default();
        let mut conn = outbox::try_lock(self.settlement.pool(), settlement::OUTBOX_LOCK_KEY)
            .await
            .unwrap()
            .unwrap();
        while outbox::publish_batch(&mut conn, &sink, 500).await.unwrap() > 0 {}
        drop(conn.detach());
        let rows = sink.0.lock().unwrap().clone();
        rows.iter()
            .map(|r| {
                assert_eq!(r.topic, settlement::SETTLEMENT_EVENTS_TOPIC);
                let m = ev::TradeSettled::decode(r.payload.as_slice()).unwrap();
                assert_eq!(r.key, m.trade_id, "keyed by trade ID");
                m
            })
            .collect()
    }

    pub async fn settle_transactions(&self) -> i64 {
        let (n,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM ledger_transactions WHERE kind = 'settle'")
                .fetch_one(self.accounts.pool())
                .await
                .unwrap();
        n
    }

    pub async fn trade_status(&self, trade_id: &str) -> String {
        let (s,): (String,) = sqlx::query_as("SELECT status FROM trades WHERE trade_id = $1")
            .bind(trade_id)
            .fetch_one(self.settlement.pool())
            .await
            .unwrap();
        s
    }

    pub async fn auditor(&self, when: When) -> Report {
        let events = self.events.lock().unwrap().clone();
        auditor::check(
            &Sources {
                accounts: self.accounts.pool(),
                risk: self.risk.pool(),
                settlement: self.settlement.pool(),
                events: Some(&events),
            },
            when,
        )
        .await
        .unwrap()
    }

    /// All 11 rules, after traffic stops.
    pub async fn audit(&self) {
        let r = self.auditor(When::TrafficStopped).await;
        assert!(r.skipped.is_empty(), "skipped: {:?}", r.skipped);
        assert!(r.passed(), "auditor found: {:#?}", r.problems);
    }
}

pub fn trade_of(e: &ev::MatchingEvent) -> Option<&ev::TradeExecuted> {
    match &e.body {
        Some(ev::matching_event::Body::Trade(t)) => Some(t),
        _ => None,
    }
}

pub fn status(o: &pb::Order) -> pb::OrderStatus {
    pb::OrderStatus::try_from(o.status).unwrap()
}
