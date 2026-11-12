//! API Gateway (spec: "Service specifications → API Gateway"): the only
//! service exposed to the internet. It contains no trading rules.
//!
//! - Verifies login tokens and passes the user ID to internal calls; a
//!   client can never choose which account it acts on.
//! - Applies rate limits using Redis ([`limits`]).
//! - Converts REST requests into gRPC calls to Accounts and Risk
//!   ([`routes`]).
//! - Creates the trace ID for each request and starts a W3C trace.
//! - Forwards the public market routes and `/ws/public` to Market Data, and
//!   the notification routes and `/ws/private` to Notifications
//!   ([`proxy`], [`ws`]).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use auth::TokenVerifier;
use axum::Router;
use axum::extract::{ConnectInfo, FromRequestParts, Request};
use axum::http::request::Parts;
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use proto_gen::accounts::v1::accounts_service_client::AccountsServiceClient;
use proto_gen::risk::v1::risk_service_client::RiskServiceClient;
use tonic::transport::Channel;
use uuid::Uuid;

pub mod error;
pub mod limits;
pub mod proxy;
pub mod routes;
pub mod store;
pub mod ws;

pub use error::ApiError;
use limits::{Limit, Limiter};

/// Spec: "Gateway to Risk: 2 s; no automatic retry; the client retries with
/// the same ID". The same budget applies to every upstream call.
pub const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(2);
/// Spec: "Input: reject request bodies over 16 KiB".
pub const MAX_BODY: usize = 16 * 1024;

pub struct Config {
    pub accounts_url: String,
    pub risk_url: String,
    /// Base URLs, such as `http://market-data:8080`.
    pub market_data_url: String,
    pub notifications_url: String,
    pub redis_url: String,
    pub jwt_public_key_pem: String,
    /// Take the client address from `X-Forwarded-For`. Only for a trusted
    /// proxy in front (the web server or the ingress), which overwrites it.
    pub trust_forwarded_for: bool,
}

pub struct Gateway {
    pub accounts: AccountsServiceClient<Channel>,
    pub risk: RiskServiceClient<Channel>,
    pub market_data: String,
    pub notifications: String,
    verifier: TokenVerifier,
    redis: Arc<store::Store>,
    pub limiter: Limiter,
    pub tickers: ticker::Store,
    pub http: proxy::Client,
    trust_forwarded_for: bool,
    pub sockets: ws::Sockets,
}

fn channel(url: &str) -> Result<Channel, String> {
    Ok(Channel::from_shared(url.to_owned())
        .map_err(|e| format!("bad URL {url:?}: {e}"))?
        .connect_timeout(Duration::from_secs(2))
        .connect_lazy())
}

impl Gateway {
    /// Connects lazily to everything, so the Gateway starts while services
    /// are still coming up.
    pub fn new(c: &Config) -> Result<Self, String> {
        let redis = Arc::new(store::Store::new(&c.redis_url)?);
        Ok(Self {
            accounts: AccountsServiceClient::new(channel(&c.accounts_url)?),
            risk: RiskServiceClient::new(channel(&c.risk_url)?),
            market_data: c.market_data_url.trim_end_matches('/').to_owned(),
            notifications: c.notifications_url.trim_end_matches('/').to_owned(),
            verifier: TokenVerifier::from_pem(&c.jwt_public_key_pem).map_err(|e| e.to_string())?,
            limiter: Limiter::new(redis.clone()),
            redis,
            tickers: ticker::Store::new(&c.redis_url).map_err(|e| e.to_string())?,
            http: proxy::client(),
            trust_forwarded_for: c.trust_forwarded_for,
            sockets: ws::Sockets::default(),
        })
    }

    /// Takes one request from `limit`'s bucket for `who`.
    pub async fn limit(&self, limit: Limit, who: &str) -> Result<(), ApiError> {
        self.limiter.check(limit, who).await
    }

    /// Marks an access token logged out until it expires on its own.
    pub async fn revoke(&self, user: &User) {
        let left = user.expires_at - now_secs();
        if left > 0
            && let Err(e) = self.redis.revoke(user.token_id, left.unsigned_abs()).await
        {
            // Spec: "If Redis is lost: a logged-out token may work until it
            // expires". The refresh token is revoked regardless.
            tracing::warn!(error = %e, "access token not revoked; it works until it expires");
        }
    }
}

pub fn now_secs() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

/// The routes, with trace IDs and the body size limit.
pub fn router(gw: Arc<Gateway>) -> Router {
    routes::router()
        .merge(ws::router())
        .route("/healthz", axum::routing::get(|| async { StatusCode::OK }))
        .route("/readyz", axum::routing::get(|| async { StatusCode::OK }))
        .layer(axum::extract::DefaultBodyLimit::max(MAX_BODY))
        .layer(axum::middleware::from_fn(trace))
        .with_state(gw)
}

/// Gives each request a new trace ID, returned in `X-Trace-Id`. Clients
/// cannot supply their own: the trace starts here. With telemetry on, it is
/// the ID of the request's span (`telemetry::server::ServerLayer::edge`), so
/// the same ID finds the trace in Tempo and the logs in Loki.
async fn trace(req: Request, next: Next) -> Response {
    let id = telemetry::trace_id_of(&tracing::Span::current())
        .unwrap_or_else(|| Uuid::new_v4().simple().to_string());
    let mut r = error::TRACE_ID.scope(id.clone(), next.run(req)).await;
    if let Ok(v) = HeaderValue::from_str(&id) {
        r.headers_mut().insert("x-trace-id", v);
    }
    r
}

/// A gRPC request carrying the trace (`traceparent`) and the deadline.
pub fn grpc<T>(msg: T) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.set_timeout(UPSTREAM_TIMEOUT);
    telemetry::inject_metadata(r.metadata_mut());
    if r.metadata().contains_key("traceparent") {
        return r;
    }
    // No span (telemetry off): still pass the trace ID along.
    let trace = error::trace_id();
    if trace.len() == 32 {
        let span = &Uuid::new_v4().simple().to_string()[..16];
        if let Ok(v) = format!("00-{trace}-{span}-01").parse() {
            r.metadata_mut().insert("traceparent", v);
        }
    }
    r
}

/// Runs a gRPC call with the upstream deadline.
pub async fn call<T>(
    fut: impl Future<Output = Result<tonic::Response<T>, tonic::Status>>,
) -> Result<T, ApiError> {
    match tokio::time::timeout(UPSTREAM_TIMEOUT, fut).await {
        Ok(Ok(r)) => Ok(r.into_inner()),
        Ok(Err(s)) => Err(ApiError::from_status(&s)),
        Err(_) => Err(ApiError::unavailable(
            "the service did not answer in time; retry with the same request",
        )),
    }
}

/// The logged-in user, from a verified `Authorization: Bearer` access token
/// that has not been logged out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct User {
    pub id: Uuid,
    pub token_id: Uuid,
    pub expires_at: i64,
}

impl FromRequestParts<Arc<Gateway>> for User {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, gw: &Arc<Gateway>) -> Result<Self, ApiError> {
        let token = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or_else(|| ApiError::unauthenticated("log in first"))?;
        let claims = gw
            .verifier
            .verify(token.trim(), now_secs())
            .map_err(|e| match e {
                auth::TokenError::Expired => {
                    ApiError::unauthenticated("the access token expired; refresh it")
                }
                _ => ApiError::unauthenticated("the access token is not valid; log in again"),
            })?;
        match gw.redis.is_revoked(claims.jti).await {
            Ok(true) => return Err(ApiError::unauthenticated("logged out; log in again")),
            Ok(false) => {}
            Err(e) => tracing::debug!(error = %e, "revocation not checked; Redis is down"),
        }
        Ok(User {
            id: claims.sub,
            token_id: claims.jti,
            expires_at: claims.exp,
        })
    }
}

/// The client's address, for limits counted per IP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIp(pub String);

impl FromRequestParts<Arc<Gateway>> for ClientIp {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        gw: &Arc<Gateway>,
    ) -> Result<Self, std::convert::Infallible> {
        if gw.trust_forwarded_for {
            // The proxy in front appends the address it saw last.
            if let Some(ip) = parts
                .headers
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.rsplit(',').next())
                .map(str::trim)
                .filter(|v| v.parse::<std::net::IpAddr>().is_ok())
            {
                return Ok(ClientIp(ip.to_owned()));
            }
        }
        let peer = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map_or_else(|| "unknown".to_owned(), |c| c.0.ip().to_string());
        Ok(ClientIp(peer))
    }
}
