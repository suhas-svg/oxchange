//! The Gateway in process against stand-ins for everything behind it:
//!
//! - Accounts and Risk: small gRPC servers that answer from fixed rules and
//!   record what they were sent, so tests can check exactly which user ID
//!   reached each service;
//! - Market Data and Notifications: one small HTTP and WebSocket server
//!   that echoes what it received;
//! - Redis 7 in a container, for rate limits, revoked tokens and tickers.
//!
//! Needs Docker running.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use auth::TokenSigner;
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::{Path, Query};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::IntoResponse;
use futures_util::StreamExt;
use gateway::{Config, Gateway};
use proto_gen::accounts::v1 as acc;
use proto_gen::accounts::v1::accounts_service_server::{AccountsService, AccountsServiceServer};
use proto_gen::risk::v1 as rk;
use proto_gen::risk::v1::risk_service_server::{RiskService, RiskServiceServer};
use serde_json::{Value, json};
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tonic::{Request, Response, Status};
use uuid::Uuid;

pub type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// What the stand-in services were sent.
#[derive(Default)]
pub struct Seen {
    /// (method, user_id) for every Accounts and Risk call that names a user.
    pub calls: Vec<(String, String)>,
    pub logouts: Vec<acc::LogoutRequest>,
    pub deposits: Vec<acc::DepositRequest>,
    pub orders: Vec<rk::PlaceOrderRequest>,
    pub traceparents: Vec<String>,
}

type Shared = Arc<Mutex<Seen>>;

fn note<T>(seen: &Shared, method: &str, user: &str, r: &Request<T>) {
    let mut s = seen.lock().unwrap();
    s.calls.push((method.to_owned(), user.to_owned()));
    if let Some(tp) = r
        .metadata()
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
    {
        s.traceparents.push(tp.to_owned());
    }
}

struct Accounts {
    seen: Shared,
    signer: Arc<TokenSigner>,
    user: Uuid,
}

#[tonic::async_trait]
impl AccountsService for Accounts {
    async fn register(
        &self,
        r: Request<acc::RegisterRequest>,
    ) -> Result<Response<acc::RegisterResponse>, Status> {
        if r.get_ref().email == "taken@example.com" {
            return Err(Status::already_exists(
                "ALREADY_EXISTS: that email is registered",
            ));
        }
        Ok(Response::new(acc::RegisterResponse {
            user_id: self.user.to_string(),
        }))
    }
    async fn login(
        &self,
        r: Request<acc::LoginRequest>,
    ) -> Result<Response<acc::LoginResponse>, Status> {
        if r.get_ref().password != "correct horse battery" {
            return Err(Status::unauthenticated(
                "UNAUTHENTICATED: wrong email or password",
            ));
        }
        let now = gateway::now_secs();
        let t = self.signer.issue(self.user, now);
        Ok(Response::new(acc::LoginResponse {
            user_id: self.user.to_string(),
            tokens: Some(acc::Tokens {
                access_token: t.token,
                access_expires_at_ns: t.claims.exp * 1_000_000_000,
                refresh_token: "refresh-1".into(),
                refresh_expires_at_ns: (now + 86_400) * 1_000_000_000,
            }),
        }))
    }
    async fn refresh(
        &self,
        _r: Request<acc::RefreshRequest>,
    ) -> Result<Response<acc::RefreshResponse>, Status> {
        Err(Status::unimplemented("not in these tests"))
    }
    async fn logout(
        &self,
        r: Request<acc::LogoutRequest>,
    ) -> Result<Response<acc::LogoutResponse>, Status> {
        note(&self.seen, "logout", &r.get_ref().user_id, &r);
        self.seen.lock().unwrap().logouts.push(r.into_inner());
        Ok(Response::new(acc::LogoutResponse {}))
    }
    async fn deposit(
        &self,
        r: Request<acc::DepositRequest>,
    ) -> Result<Response<acc::DepositResponse>, Status> {
        note(&self.seen, "deposit", &r.get_ref().user_id, &r);
        let d = r.into_inner();
        let available = d.amount_cents;
        self.seen.lock().unwrap().deposits.push(d);
        Ok(Response::new(acc::DepositResponse {
            transaction_id: "txn-1".into(),
            available_cents: available,
        }))
    }
    async fn withdraw(
        &self,
        _r: Request<acc::WithdrawRequest>,
    ) -> Result<Response<acc::WithdrawResponse>, Status> {
        Err(Status::failed_precondition(
            "INSUFFICIENT_FUNDS: available balance is too low",
        ))
    }
    async fn get_balances(
        &self,
        r: Request<acc::GetBalancesRequest>,
    ) -> Result<Response<acc::GetBalancesResponse>, Status> {
        note(&self.seen, "get_balances", &r.get_ref().user_id, &r);
        Ok(Response::new(acc::GetBalancesResponse {
            balances: vec![acc::Balance {
                asset: "USD".into(),
                available: 150_000,
                reserved: 50_000,
            }],
        }))
    }
    async fn list_ledger(
        &self,
        r: Request<acc::ListLedgerRequest>,
    ) -> Result<Response<acc::ListLedgerResponse>, Status> {
        note(&self.seen, "list_ledger", &r.get_ref().user_id, &r);
        Ok(Response::new(acc::ListLedgerResponse {
            entries: vec![],
            next_cursor: String::new(),
        }))
    }
    async fn reserve(
        &self,
        _r: Request<acc::ReserveRequest>,
    ) -> Result<Response<acc::ReserveResponse>, Status> {
        Err(Status::permission_denied("not for the gateway"))
    }
    async fn release(
        &self,
        _r: Request<acc::ReleaseRequest>,
    ) -> Result<Response<acc::ReleaseResponse>, Status> {
        Err(Status::permission_denied("not for the gateway"))
    }
    async fn settle_trade(
        &self,
        _r: Request<acc::SettleTradeRequest>,
    ) -> Result<Response<acc::SettleTradeResponse>, Status> {
        Err(Status::permission_denied("not for the gateway"))
    }
}

struct Risk {
    seen: Shared,
}

fn order(user: &str, r: &rk::PlaceOrderRequest) -> rk::Order {
    rk::Order {
        order_id: "0194d5a2-7b3c-7e11-9a44-2f1c6d8e0b11".into(),
        client_order_id: r.client_order_id.clone(),
        user_id: user.to_owned(),
        symbol: r.symbol.clone(),
        side: r.side,
        order_type: r.order_type,
        time_in_force: r.time_in_force,
        limit_price_cents: r.price_cents,
        quantity: r.quantity,
        filled_quantity: 0,
        canceled_quantity: 0,
        status: rk::OrderStatus::Accepted as i32,
        reject_reason: String::new(),
        created_at_ns: 1_794_319_205_123_000_000,
        updated_at_ns: 1_794_319_205_123_000_000,
    }
}

#[tonic::async_trait]
impl RiskService for Risk {
    async fn place_order(
        &self,
        r: Request<rk::PlaceOrderRequest>,
    ) -> Result<Response<rk::PlaceOrderResponse>, Status> {
        note(&self.seen, "place_order", &r.get_ref().user_id, &r);
        let p = r.into_inner();
        self.seen.lock().unwrap().orders.push(p.clone());
        if p.symbol == "BROKE" {
            return Err(Status::failed_precondition(
                "INSUFFICIENT_FUNDS: available balance is too low for this order",
            ));
        }
        if p.symbol == "SLOW" {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
        Ok(Response::new(rk::PlaceOrderResponse {
            order: Some(order(&p.user_id, &p)),
            existing: p.client_order_id == "repeat",
        }))
    }
    async fn cancel_order(
        &self,
        r: Request<rk::CancelOrderRequest>,
    ) -> Result<Response<rk::CancelOrderResponse>, Status> {
        note(&self.seen, "cancel_order", &r.get_ref().user_id, &r);
        Err(Status::not_found("NOT_FOUND: order not found"))
    }
    async fn get_order(
        &self,
        r: Request<rk::GetOrderRequest>,
    ) -> Result<Response<rk::GetOrderResponse>, Status> {
        note(&self.seen, "get_order", &r.get_ref().user_id, &r);
        Err(Status::not_found("NOT_FOUND: order not found"))
    }
    async fn list_orders(
        &self,
        r: Request<rk::ListOrdersRequest>,
    ) -> Result<Response<rk::ListOrdersResponse>, Status> {
        note(&self.seen, "list_orders", &r.get_ref().user_id, &r);
        Ok(Response::new(rk::ListOrdersResponse {
            orders: vec![],
            next_cursor: "next-page".into(),
        }))
    }
    async fn list_symbols(
        &self,
        _r: Request<rk::ListSymbolsRequest>,
    ) -> Result<Response<rk::ListSymbolsResponse>, Status> {
        let s = |symbol: &str, name: &str, listing| rk::SymbolInfo {
            symbol: symbol.into(),
            name: name.into(),
            trading: true,
            listing_price_cents: listing,
        };
        Ok(Response::new(rk::ListSymbolsResponse {
            symbols: vec![
                s("ACME", "Acme Simulated Holdings", 1000),
                s("BOLT", "Bolt Simulated Motors", 2500),
            ],
        }))
    }
}

/// Market Data and Notifications in one: every HTTP route echoes what
/// arrived, and the two WebSockets behave like the real ones' handshakes.
fn upstream() -> axum::Router {
    use axum::routing::{get, post};
    fn echo(uri: &Uri, headers: &HeaderMap) -> Value {
        json!({
            "path": uri.path(),
            "query": uri.query(),
            "x_user_id": headers.get("x-user-id").and_then(|v| v.to_str().ok()),
            "authorization": headers.get("authorization").and_then(|v| v.to_str().ok()),
            "cookie": headers.get("cookie").and_then(|v| v.to_str().ok()),
        })
    }
    axum::Router::new()
        .route(
            "/v1/symbols/{s}/{view}",
            get(
                |uri: Uri, h: HeaderMap, Path((_s, _v)): Path<(String, String)>| async move {
                    axum::Json(echo(&uri, &h))
                },
            ),
        )
        .route(
            "/v1/notifications",
            get(|uri: Uri, h: HeaderMap| async move { axum::Json(echo(&uri, &h)) }),
        )
        .route(
            "/v1/ws-ticket",
            post(|uri: Uri, h: HeaderMap| async move {
                let mut body = echo(&uri, &h);
                body["ticket"] = "good-ticket".into();
                (StatusCode::CREATED, axum::Json(body))
            }),
        )
        .route(
            "/ws/public",
            get(|up: WebSocketUpgrade| async move {
                up.on_upgrade(|mut s| async move {
                    while let Some(Ok(Message::Text(t))) = s.next().await {
                        let reply = json!({ "type": "echo", "got": t.as_str() }).to_string();
                        if s.send(Message::Text(reply.into())).await.is_err() {
                            return;
                        }
                    }
                })
            }),
        )
        .route(
            "/ws/private",
            get(
                |Query(q): Query<std::collections::HashMap<String, String>>,
                 up: WebSocketUpgrade| async move {
                    if q.get("ticket").map(String::as_str) != Some("good-ticket") {
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    up.on_upgrade(|mut s| async move {
                        let _ = s
                            .send(Message::Text(
                                json!({ "type": "ready", "last_id": 0 }).to_string().into(),
                            ))
                            .await;
                        while let Some(Ok(_)) = s.next().await {}
                    })
                    .into_response()
                },
            ),
        )
}

async fn serve(router: axum::Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await });
    addr
}

async fn grpc(router: tonic::service::Routes) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_routes(router)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    addr
}

pub struct Env {
    pub addr: SocketAddr,
    pub user: Uuid,
    pub signer: Arc<TokenSigner>,
    pub seen: Shared,
    pub redis_url: String,
    pub gateway: Arc<Gateway>,
    _redis: ContainerAsync<Redis>,
}

pub async fn env() -> Env {
    env_with(|_| {}).await
}

/// `tweak` adjusts the Gateway's configuration before it starts.
pub async fn env_with(tweak: impl FnOnce(&mut Config)) -> Env {
    let redis = Redis::default()
        .with_tag("7-alpine")
        .start()
        .await
        .expect("start redis (is Docker running?)");
    let redis_url = format!(
        "redis://{}:{}",
        redis.get_host().await.unwrap(),
        redis.get_host_port_ipv4(6379).await.unwrap()
    );
    let (private, public) = TokenSigner::generate_pem().unwrap();
    let signer = Arc::new(TokenSigner::from_pem(&private).unwrap());
    let seen: Shared = Arc::default();
    let user = Uuid::now_v7();

    let accounts = grpc(tonic::service::Routes::new(AccountsServiceServer::new(
        Accounts {
            seen: seen.clone(),
            signer: signer.clone(),
            user,
        },
    )))
    .await;
    let risk = grpc(tonic::service::Routes::new(RiskServiceServer::new(Risk {
        seen: seen.clone(),
    })))
    .await;
    let up = serve(upstream()).await;

    let mut config = Config {
        accounts_url: format!("http://{accounts}"),
        risk_url: format!("http://{risk}"),
        market_data_url: format!("http://{up}"),
        notifications_url: format!("http://{up}"),
        redis_url: redis_url.clone(),
        jwt_public_key_pem: public,
        trust_forwarded_for: true,
    };
    tweak(&mut config);
    let gw = Arc::new(Gateway::new(&config).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = gateway::router(gw.clone()).into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move { axum::serve(listener, app).await });
    Env {
        addr,
        user,
        signer,
        seen,
        redis_url,
        gateway: gw,
        _redis: redis,
    }
}

pub struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Value,
}

impl Reply {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

impl Env {
    /// A valid access token for the stand-in user.
    pub fn token(&self) -> String {
        self.signer.issue(self.user, gateway::now_secs()).token
    }

    /// One HTTP request. `headers` are extra raw header lines.
    pub async fn request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) -> Reply {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(self.addr).await.unwrap();
        let mut head = format!(
            "{method} {path} HTTP/1.0\r\nHost: x\r\nContent-Length: {}\r\n",
            body.len()
        );
        if !body.is_empty() {
            head.push_str("Content-Type: application/json\r\n");
        }
        for (k, v) in headers {
            head.push_str(&format!("{k}: {v}\r\n"));
        }
        head.push_str("\r\n");
        s.write_all(head.as_bytes()).await.unwrap();
        s.write_all(body.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf).into_owned();
        let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
        let mut lines = head.lines();
        let status = lines
            .next()
            .unwrap()
            .split(' ')
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        let headers = lines
            .filter_map(|l| l.split_once(": "))
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        Reply {
            status,
            headers,
            body: serde_json::from_str(body).unwrap_or(Value::Null),
        }
    }

    pub async fn authed(&self, method: &str, path: &str, body: &str) -> Reply {
        let bearer = format!("Bearer {}", self.token());
        self.request(method, path, &[("Authorization", &bearer)], body)
            .await
    }

    pub async fn ws(&self, path: &str) -> Result<Ws, tokio_tungstenite::tungstenite::Error> {
        tokio_tungstenite::connect_async(format!("ws://{}{path}", self.addr))
            .await
            .map(|(ws, _)| ws)
    }

    pub fn seen(&self) -> std::sync::MutexGuard<'_, Seen> {
        self.seen.lock().unwrap()
    }
}

pub async fn next(ws: &mut Ws) -> Value {
    loop {
        let m = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
            .await
            .expect("a message within 5 s")
            .expect("the stream is open")
            .unwrap();
        if let tokio_tungstenite::tungstenite::Message::Text(t) = m {
            return serde_json::from_str(&t).unwrap();
        }
    }
}

pub fn refused(r: Result<Ws, tokio_tungstenite::tungstenite::Error>) -> u16 {
    match r {
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => resp.status().as_u16(),
        Ok(_) => panic!("the stream opened"),
        Err(e) => panic!("unexpected error: {e}"),
    }
}
