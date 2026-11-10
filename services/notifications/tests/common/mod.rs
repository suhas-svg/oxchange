//! Notifications against real dependencies, in process: PostgreSQL 16 and
//! Redis 7 containers, and one or more service copies sharing them, each
//! with its own HTTP port and Redis subscription.
//!
//! Needs Docker running.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use notifications::http::{App, router};
use notifications::wake::Publisher;
use notifications::{
    ACCOUNTS_EVENTS_TOPIC, Hub, MATCHING_EVENTS_TOPIC, Notifier, SETTLEMENT_EVENTS_TOPIC,
};
use prost::Message;
use proto_gen::events::v1 as ev;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio_tungstenite::tungstenite;
use uuid::Uuid;

pub type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub struct Env {
    pub pool: PgPool,
    pub redis_url: String,
    _pg: ContainerAsync<Postgres>,
    _redis: ContainerAsync<Redis>,
}

/// One running copy of the service.
pub struct Instance {
    pub notifier: Arc<Notifier>,
    pub hub: Arc<Hub>,
    pub addr: SocketAddr,
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
        .max_connections(20)
        .connect(&url)
        .await
        .unwrap();
    notifications::MIGRATOR.run(&pool).await.unwrap();
    let redis_url = format!(
        "redis://{}:{}",
        redis.get_host().await.unwrap(),
        redis.get_host_port_ipv4(6379).await.unwrap()
    );
    Env {
        pool,
        redis_url,
        _pg: pg,
        _redis: redis,
    }
}

impl Env {
    pub async fn copy(&self) -> Instance {
        self.copy_with_redis(&self.redis_url).await
    }

    /// A copy whose Redis is at `redis_url` (a dead address simulates an
    /// outage for that copy).
    pub async fn copy_with_redis(&self, redis_url: &str) -> Instance {
        let notifier = Arc::new(Notifier::new(
            self.pool.clone(),
            Arc::new(Publisher::new(redis_url).unwrap()),
        ));
        let hub = Arc::new(Hub::default());
        let (h, url) = (hub.clone(), redis_url.to_owned());
        tokio::spawn(
            async move { notifications::wake::listen(&url, h, std::future::pending()).await },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Arc::new(App {
            notifier: notifier.clone(),
            hub: hub.clone(),
        });
        tokio::spawn(async move { axum::serve(listener, router(app)).await });
        Instance {
            notifier,
            hub,
            addr,
        }
    }

    pub async fn count(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sql).fetch_one(&self.pool).await.unwrap()
    }
}

impl Instance {
    /// Opens `user`'s private stream with a fresh ticket and returns it
    /// with the `last_id` from its `ready` message.
    pub async fn open(&self, user: Uuid) -> (Ws, i64) {
        let ticket = self.notifier.issue_ticket(user).await.unwrap();
        let (mut ws, _) = self.connect(&ticket).await.expect("private stream opens");
        let ready = next(&mut ws).await;
        assert_eq!(ready["type"], "ready");
        (ws, ready["last_id"].as_i64().unwrap())
    }

    // Test helper: the full handshake error is what the tests inspect.
    #[allow(clippy::result_large_err)]
    pub async fn connect(
        &self,
        ticket: &str,
    ) -> Result<(Ws, tungstenite::handshake::client::Response), tungstenite::Error> {
        tokio_tungstenite::connect_async(format!("ws://{}/ws/private?ticket={ticket}", self.addr))
            .await
    }

    /// One HTTP request; returns the status and the JSON body.
    pub async fn request(&self, method: &str, path: &str, user: Option<Uuid>) -> (u16, Value) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(self.addr).await.unwrap();
        let user = user.map_or(String::new(), |u| format!("X-User-Id: {u}\r\n"));
        s.write_all(
            format!("{method} {path} HTTP/1.0\r\nHost: x\r\nContent-Length: 0\r\n{user}\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
        let mut buf = String::new();
        s.read_to_string(&mut buf).await.unwrap();
        let status = buf.split(' ').nth(1).unwrap().parse().unwrap();
        let body = buf.split_once("\r\n\r\n").unwrap().1;
        (status, serde_json::from_str(body).unwrap_or(Value::Null))
    }
}

/// The HTTP status a refused WebSocket handshake came back with.
pub fn refused(
    r: Result<(Ws, tungstenite::handshake::client::Response), tungstenite::Error>,
) -> u16 {
    match r {
        Err(tungstenite::Error::Http(resp)) => resp.status().as_u16(),
        Ok(_) => panic!("the stream opened"),
        Err(e) => panic!("unexpected error: {e}"),
    }
}

/// The next JSON message, within 10 s.
pub async fn next(ws: &mut Ws) -> Value {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("a message within 10 s")
            .expect("the stream is open")
            .unwrap();
        if let tungstenite::Message::Text(t) = msg {
            return serde_json::from_str(&t).unwrap();
        }
    }
}

/// Asserts nothing arrives for `wait`.
pub async fn quiet(ws: &mut Ws, wait: Duration) {
    if let Ok(m) = tokio::time::timeout(wait, ws.next()).await {
        panic!("unexpected message: {m:?}");
    }
}

/// 2026-11-10 14:00:05 UTC, in Unix nanoseconds.
pub const T0: i64 = 1_794_319_205_000_000_000;

fn matching(symbol: &str, seq: u64, body: ev::matching_event::Body) -> Vec<u8> {
    ev::MatchingEvent {
        event_id: format!("{symbol}-{seq}"),
        symbol: symbol.into(),
        seq,
        event_time_ns: T0,
        command_id: String::new(),
        body: Some(body),
    }
    .encode_to_vec()
}

pub fn trade(symbol: &str, seq: u64, buyer: Uuid, seller: Uuid, price: i64, qty: i64) -> Vec<u8> {
    matching(
        symbol,
        seq,
        ev::matching_event::Body::Trade(ev::TradeExecuted {
            trade_id: format!("{symbol}-{seq}"),
            price_cents: price,
            quantity: qty,
            buy_order_id: format!("buy-{seq}"),
            sell_order_id: format!("sell-{seq}"),
            buyer_id: buyer.to_string(),
            seller_id: seller.to_string(),
            aggressor_side: ev::Side::Buy as i32,
        }),
    )
}

pub fn canceled(symbol: &str, seq: u64, user: Uuid, qty: i64) -> Vec<u8> {
    matching(
        symbol,
        seq,
        ev::matching_event::Body::OrderCanceled(ev::OrderCanceled {
            order_id: format!("order-{seq}"),
            user_id: user.to_string(),
            canceled_quantity: qty,
            reason: ev::CancelReason::User as i32,
        }),
    )
}

pub fn cancel_rejected(symbol: &str, seq: u64, user: Uuid) -> Vec<u8> {
    matching(
        symbol,
        seq,
        ev::matching_event::Body::CancelRejected(ev::CancelRejected {
            order_id: format!("order-{seq}"),
            user_id: user.to_string(),
            reason: ev::CancelRejectReason::NotFound as i32,
        }),
    )
}

pub fn accepted(symbol: &str, seq: u64, user: Uuid) -> Vec<u8> {
    matching(
        symbol,
        seq,
        ev::matching_event::Body::OrderAccepted(ev::OrderAccepted {
            order_id: format!("order-{seq}"),
            user_id: user.to_string(),
            side: ev::Side::Buy as i32,
            price_cents: 1000,
            quantity: 1,
        }),
    )
}

pub fn book_changed(symbol: &str, seq: u64) -> Vec<u8> {
    matching(
        symbol,
        seq,
        ev::matching_event::Body::BookChanged(ev::BookChanged {
            side: ev::Side::Buy as i32,
            price_cents: 1000,
            new_total_quantity: 1,
        }),
    )
}

pub fn settled(trade_id: &str, buyer: Uuid, seller: Uuid) -> Vec<u8> {
    ev::TradeSettled {
        trade_id: trade_id.into(),
        symbol: "ACME".into(),
        price_cents: 1000,
        quantity: 25,
        buy_order_id: "buy-1".into(),
        sell_order_id: "sell-1".into(),
        buyer_id: buyer.to_string(),
        seller_id: seller.to_string(),
        settled_at_ns: T0,
    }
    .encode_to_vec()
}

pub fn deposited(user: Uuid, amount: i64) -> Vec<u8> {
    ev::AccountEvent {
        event_id: Uuid::now_v7().to_string(),
        user_id: user.to_string(),
        event_time_ns: T0,
        body: Some(ev::account_event::Body::Deposited(ev::Deposited {
            amount_cents: amount,
            balance_cents: amount,
            asset: "USD".into(),
        })),
    }
    .encode_to_vec()
}

pub const MATCHING: &str = MATCHING_EVENTS_TOPIC;
pub const SETTLEMENT: &str = SETTLEMENT_EVENTS_TOPIC;
pub const ACCOUNTS: &str = ACCOUNTS_EVENTS_TOPIC;
