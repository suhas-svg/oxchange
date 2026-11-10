//! Market Data against real dependencies, in process:
//!
//! - PostgreSQL 16 and Redis 7 containers;
//! - a real matching [`Engine`] behind the real `GetBook` gRPC server, so
//!   the live view loads books exactly as it does in production.
//!
//! Needs Docker running.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use domain::Symbol;
use market_data::Recorder;
use matching::Engine;
use matching::codec::encode_event;
use matching::pipeline::{BookQuery, book_view};
use order_book::Command;
use prost::Message;
use proto_gen::events::v1 as ev;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

pub struct Env {
    pub pool: PgPool,
    pub ticker: Arc<ticker::Store>,
    pub recorder: Recorder,
    pub redis_url: String,
    _pg: ContainerAsync<Postgres>,
    _redis: ContainerAsync<Redis>,
}

pub async fn env() -> Env {
    let pg = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("start postgres (is Docker running?)");
    let redis = Redis::default()
        .with_tag("7-alpine")
        .start()
        .await
        .expect("start redis");
    let url = format!(
        "postgres://postgres:postgres@{}:{}/postgres",
        pg.get_host().await.unwrap(),
        pg.get_host_port_ipv4(5432).await.unwrap()
    );
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&url)
        .await
        .unwrap();
    market_data::MIGRATOR.run(&pool).await.unwrap();
    let redis_url = format!(
        "redis://{}:{}",
        redis.get_host().await.unwrap(),
        redis.get_host_port_ipv4(6379).await.unwrap()
    );
    let ticker = Arc::new(ticker::Store::new(&redis_url).unwrap());
    Env {
        recorder: Recorder::new(pool.clone(), ticker.clone()),
        pool,
        ticker,
        redis_url,
        _pg: pg,
        _redis: redis,
    }
}

impl Env {
    /// Deletes everything in Redis, as the spec's "Redis wipe" test does.
    pub async fn wipe_redis(&self) {
        let client = redis::Client::open(self.redis_url.as_str()).unwrap();
        let mut c = client.get_multiplexed_async_connection().await.unwrap();
        let _: () = redis::cmd("FLUSHALL").query_async(&mut c).await.unwrap();
    }

    pub async fn count(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sql).fetch_one(&self.pool).await.unwrap()
    }
}

/// A real engine, its `GetBook` server on a local port, and the events it
/// has produced.
pub struct Matcher {
    pub engine: Arc<Mutex<Engine>>,
    pub url: String,
    offset: Mutex<u64>,
}

impl Matcher {
    pub async fn start() -> Self {
        let engine = Arc::new(Mutex::new(Engine::new(0, 100_000)));
        // The "matcher thread": answers queries while holding the engine,
        // so an answer is never mid-command (as in the real pipeline).
        let (tx, rx) = std::sync::mpsc::sync_channel::<BookQuery>(64);
        let e = engine.clone();
        std::thread::spawn(move || {
            for q in rx {
                let view = book_view(&e.lock().unwrap(), &q.symbol);
                let _ = q.reply.send(view);
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(matching::grpc::Service::server(tx))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        Self {
            engine,
            url: format!("http://{addr}"),
            offset: Mutex::new(0),
        }
    }

    /// Runs one command and returns its events as Kafka would carry them.
    pub fn process(&self, symbol: &Symbol, cmd: &Command) -> Vec<ev::MatchingEvent> {
        let mut out = Vec::new();
        let mut off = self.offset.lock().unwrap();
        self.engine
            .lock()
            .unwrap()
            .process(*off, symbol, cmd, &mut out)
            .unwrap();
        *off += 1;
        out.iter()
            .map(|e| ev::MatchingEvent::decode(encode_event(symbol, e).as_slice()).unwrap())
            .collect()
    }

    pub fn view(&self, symbol: &Symbol) -> matching::pipeline::BookView {
        book_view(&self.engine.lock().unwrap(), symbol)
    }
}

/// 2026-11-10 14:00:05 UTC, in Unix nanoseconds.
pub const T0: i64 = 1_794_319_205_000_000_000;
pub const SEC: i64 = 1_000_000_000;

pub fn event(
    symbol: &str,
    seq: u64,
    time_ns: i64,
    body: ev::matching_event::Body,
) -> ev::MatchingEvent {
    ev::MatchingEvent {
        event_id: format!("{symbol}-{seq}"),
        symbol: symbol.into(),
        seq,
        event_time_ns: time_ns,
        command_id: String::new(),
        body: Some(body),
    }
}

pub fn trade(symbol: &str, seq: u64, time_ns: i64, price: i64, qty: i64) -> ev::MatchingEvent {
    event(
        symbol,
        seq,
        time_ns,
        ev::matching_event::Body::Trade(ev::TradeExecuted {
            trade_id: format!("{symbol}-{seq}"),
            price_cents: price,
            quantity: qty,
            buy_order_id: uuid::Uuid::now_v7().to_string(),
            sell_order_id: uuid::Uuid::now_v7().to_string(),
            buyer_id: uuid::Uuid::now_v7().to_string(),
            seller_id: uuid::Uuid::now_v7().to_string(),
            aggressor_side: ev::Side::Buy as i32,
        }),
    )
}

pub fn level(symbol: &str, seq: u64, side: ev::Side, price: i64, qty: i64) -> ev::MatchingEvent {
    event(
        symbol,
        seq,
        T0,
        ev::matching_event::Body::BookChanged(ev::BookChanged {
            side: side as i32,
            price_cents: price,
            new_total_quantity: qty,
        }),
    )
}

pub fn other(symbol: &str, seq: u64) -> ev::MatchingEvent {
    event(
        symbol,
        seq,
        T0,
        ev::matching_event::Body::OrderAccepted(ev::OrderAccepted::default()),
    )
}
