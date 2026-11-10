//! The REST API (spec: "External API → REST endpoints").
//!
//! JSON uses snake_case. Money is in integer cents; stock amounts are whole
//! shares. Times are RFC 3339 strings in UTC. Unknown JSON fields are
//! rejected.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{FromRequest, Path, Query, Request, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use proto_gen::accounts::v1 as acc;
use proto_gen::events::v1 as ev;
use proto_gen::risk::v1 as rk;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::limits::Limit;
use crate::{ApiError, ClientIp, Gateway, User, call, grpc, proxy};

type Gw = State<Arc<Gateway>>;
type Reply = Result<Response, ApiError>;

/// Most lists return this many items unless asked otherwise.
const DEFAULT_PAGE: i32 = 50;
/// Spec: "List endpoints use cursor paging, with at most 100 items per page."
const MAX_PAGE: i32 = 100;

pub fn router() -> Router<Arc<Gateway>> {
    Router::new()
        .route("/v1/auth/register", post(register))
        .route("/v1/auth/login", post(login))
        .route("/v1/auth/refresh", post(refresh))
        .route("/v1/auth/logout", post(logout))
        .route("/v1/account/balances", get(balances))
        .route("/v1/account/deposits", post(deposit))
        .route("/v1/account/withdrawals", post(withdraw))
        .route("/v1/account/ledger", get(ledger))
        .route("/v1/orders", post(place).get(list_orders))
        .route("/v1/orders/{id}", get(get_order).delete(cancel))
        .route("/v1/symbols", get(symbols))
        .route("/v1/symbols/{symbol}/{view}", get(market))
        .route("/v1/notifications", get(notifications))
        .route("/v1/ws-ticket", post(ws_ticket))
}

/// A JSON body that must match `T` exactly, with errors in the API's shape.
pub struct Body<T>(pub T);

impl<S: Send + Sync, T: DeserializeOwned> FromRequest<S> for Body<T> {
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, ApiError> {
        let bytes = Bytes::from_request(req, state).await.map_err(|e| {
            if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
                ApiError::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "VALIDATION_FAILED",
                    "the request body is over 16 KiB",
                )
            } else {
                ApiError::invalid("the request body could not be read")
            }
        })?;
        serde_json::from_slice(&bytes)
            .map(Body)
            .map_err(|e| ApiError::invalid(format!("bad JSON body: {e}")))
    }
}

fn time(ns: i64) -> Value {
    if ns <= 0 {
        return Value::Null;
    }
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ns))
        .ok()
        .and_then(|t| {
            t.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .map_or(Value::Null, Value::String)
}

fn cursor(next: String) -> Value {
    if next.is_empty() {
        Value::Null
    } else {
        Value::String(next)
    }
}

fn page(limit: Option<i32>) -> Result<i32, ApiError> {
    match limit {
        None => Ok(DEFAULT_PAGE),
        Some(n) if (1..=MAX_PAGE).contains(&n) => Ok(n),
        Some(_) => Err(ApiError::invalid(format!("limit must be 1 to {MAX_PAGE}"))),
    }
}

// --- Login ---------------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Credentials {
    email: String,
    password: String,
}

fn tokens(user_id: Option<&str>, t: Option<acc::Tokens>) -> Result<Value, ApiError> {
    let t = t.ok_or_else(|| ApiError::unavailable("no tokens returned; retry"))?;
    let mut v = json!({
        "access_token": t.access_token,
        "access_expires_at": time(t.access_expires_at_ns),
        "refresh_token": t.refresh_token,
        "refresh_expires_at": time(t.refresh_expires_at_ns),
        "token_type": "Bearer",
    });
    if let Some(u) = user_id {
        v["user_id"] = Value::String(u.to_owned());
    }
    Ok(v)
}

async fn register(State(gw): Gw, ip: ClientIp, Body(c): Body<Credentials>) -> Reply {
    gw.limit(Limit::Register, &ip.0).await?;
    let r = call(gw.accounts.clone().register(grpc(acc::RegisterRequest {
        email: c.email,
        password: c.password,
    })))
    .await?;
    Ok((StatusCode::CREATED, Json(json!({ "user_id": r.user_id }))).into_response())
}

async fn login(State(gw): Gw, ip: ClientIp, Body(c): Body<Credentials>) -> Reply {
    gw.limit(Limit::Login, &ip.0).await?;
    let r = call(gw.accounts.clone().login(grpc(acc::LoginRequest {
        email: c.email,
        password: c.password,
    })))
    .await?;
    Ok(Json(tokens(Some(&r.user_id), r.tokens)?).into_response())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RefreshBody {
    refresh_token: String,
}

async fn refresh(State(gw): Gw, ip: ClientIp, Body(b): Body<RefreshBody>) -> Reply {
    gw.limit(Limit::Public, &ip.0).await?;
    let r = call(gw.accounts.clone().refresh(grpc(acc::RefreshRequest {
        refresh_token: b.refresh_token,
    })))
    .await?;
    Ok(Json(tokens(None, r.tokens)?).into_response())
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct LogoutBody {
    #[serde(default)]
    refresh_token: String,
}

/// Revokes the refresh token (if given) and this access token.
async fn logout(State(gw): Gw, user: User, body: Bytes) -> Reply {
    gw.limit(Limit::Authenticated, &user.id.to_string()).await?;
    let b: LogoutBody = if body.is_empty() {
        LogoutBody::default()
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| ApiError::invalid(format!("bad JSON body: {e}")))?
    };
    call(gw.accounts.clone().logout(grpc(acc::LogoutRequest {
        user_id: user.id.to_string(),
        refresh_token: b.refresh_token,
        access_token_id: user.token_id.to_string(),
    })))
    .await?;
    gw.revoke(&user).await;
    Ok(StatusCode::NO_CONTENT.into_response())
}

// --- Account -------------------------------------------------------------

async fn balances(State(gw): Gw, user: User) -> Reply {
    gw.limit(Limit::Authenticated, &user.id.to_string()).await?;
    let r = call(
        gw.accounts
            .clone()
            .get_balances(grpc(acc::GetBalancesRequest {
                user_id: user.id.to_string(),
            })),
    )
    .await?;
    let balances: Vec<Value> = r
        .balances
        .into_iter()
        .map(|b| json!({ "asset": b.asset, "available": b.available, "reserved": b.reserved }))
        .collect();
    Ok(Json(json!({ "balances": balances })).into_response())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Transfer {
    amount_cents: i64,
    /// `USD` (the default) or a stock symbol, in whole shares.
    #[serde(default)]
    asset: String,
}

fn idempotency_key(headers: &HeaderMap) -> Result<String, ApiError> {
    let key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .unwrap_or_default();
    if key.is_empty() || key.len() > 128 {
        return Err(ApiError::invalid(
            "an Idempotency-Key header of 1 to 128 characters is required",
        ));
    }
    Ok(key.to_owned())
}

async fn deposit(State(gw): Gw, user: User, headers: HeaderMap, Body(t): Body<Transfer>) -> Reply {
    gw.limit(Limit::Authenticated, &user.id.to_string()).await?;
    let r = call(gw.accounts.clone().deposit(grpc(acc::DepositRequest {
        user_id: user.id.to_string(),
        idempotency_key: idempotency_key(&headers)?,
        amount_cents: t.amount_cents,
        asset: t.asset,
    })))
    .await?;
    let body = json!({ "transaction_id": r.transaction_id, "available": r.available_cents });
    Ok((StatusCode::CREATED, Json(body)).into_response())
}

async fn withdraw(State(gw): Gw, user: User, headers: HeaderMap, Body(t): Body<Transfer>) -> Reply {
    gw.limit(Limit::Authenticated, &user.id.to_string()).await?;
    let r = call(gw.accounts.clone().withdraw(grpc(acc::WithdrawRequest {
        user_id: user.id.to_string(),
        idempotency_key: idempotency_key(&headers)?,
        amount_cents: t.amount_cents,
        asset: t.asset,
    })))
    .await?;
    let body = json!({ "transaction_id": r.transaction_id, "available": r.available_cents });
    Ok((StatusCode::CREATED, Json(body)).into_response())
}

#[derive(Deserialize)]
struct PageQuery {
    cursor: Option<String>,
    limit: Option<i32>,
}

async fn ledger(State(gw): Gw, user: User, Query(q): Query<PageQuery>) -> Reply {
    gw.limit(Limit::Authenticated, &user.id.to_string()).await?;
    let r = call(
        gw.accounts
            .clone()
            .list_ledger(grpc(acc::ListLedgerRequest {
                user_id: user.id.to_string(),
                cursor: q.cursor.unwrap_or_default(),
                page_size: page(q.limit)?,
            })),
    )
    .await?;
    let entries: Vec<Value> = r
        .entries
        .into_iter()
        .map(|e| {
            json!({
                "transaction_id": e.transaction_id,
                "kind": e.kind,
                "asset": e.asset,
                "account_kind": e.account_kind,
                "amount": e.amount,
                "created_at": time(e.created_at_ns),
            })
        })
        .collect();
    Ok(Json(json!({ "entries": entries, "next_cursor": cursor(r.next_cursor) })).into_response())
}

// --- Orders --------------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NewOrder {
    client_order_id: String,
    symbol: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    #[serde(default)]
    time_in_force: Option<String>,
    #[serde(default)]
    price_cents: Option<i64>,
    quantity: i64,
}

fn short<'a>(name: &'a str, prefix: &str) -> &'a str {
    name.strip_prefix(prefix).unwrap_or(name)
}

pub fn order_json(o: &rk::Order) -> Value {
    let side = ev::Side::try_from(o.side).unwrap_or(ev::Side::Unspecified);
    let kind = ev::OrderType::try_from(o.order_type).unwrap_or(ev::OrderType::Unspecified);
    let tif = ev::TimeInForce::try_from(o.time_in_force).unwrap_or(ev::TimeInForce::Unspecified);
    let status = rk::OrderStatus::try_from(o.status).unwrap_or(rk::OrderStatus::Unspecified);
    json!({
        "order_id": o.order_id,
        "client_order_id": o.client_order_id,
        "symbol": o.symbol,
        "side": short(side.as_str_name(), "SIDE_"),
        "type": short(kind.as_str_name(), "ORDER_TYPE_"),
        "time_in_force": short(tif.as_str_name(), "TIME_IN_FORCE_"),
        // For a market order: the protected limit Risk set from the last price.
        "price_cents": o.limit_price_cents,
        "quantity": o.quantity,
        "filled_quantity": o.filled_quantity,
        "canceled_quantity": o.canceled_quantity,
        "status": short(status.as_str_name(), "ORDER_STATUS_"),
        "reject_reason": if o.reject_reason.is_empty() { Value::Null } else { o.reject_reason.clone().into() },
        "created_at": time(o.created_at_ns),
        "updated_at": time(o.updated_at_ns),
    })
}

fn enum_value(field: &str, value: &str, options: &[(&str, i32)]) -> Result<i32, ApiError> {
    options
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(value))
        .map(|(_, v)| *v)
        .ok_or_else(|| {
            let names: Vec<&str> = options.iter().map(|(n, _)| *n).collect();
            ApiError::invalid(format!("{field} must be one of {}", names.join(", ")))
        })
}

/// 202 for a new order; 200 with the original for a repeated
/// `client_order_id` (spec: "never creates a second order").
async fn place(State(gw): Gw, user: User, Body(o): Body<NewOrder>) -> Reply {
    gw.limit(Limit::Orders, &user.id.to_string()).await?;
    let side = enum_value(
        "side",
        &o.side,
        &[
            ("BUY", ev::Side::Buy as i32),
            ("SELL", ev::Side::Sell as i32),
        ],
    )?;
    let order_type = enum_value(
        "type",
        &o.order_type,
        &[
            ("LIMIT", ev::OrderType::Limit as i32),
            ("MARKET", ev::OrderType::Market as i32),
        ],
    )?;
    let time_in_force = match o.time_in_force.as_deref() {
        None => ev::TimeInForce::Gtc as i32,
        Some(t) => enum_value(
            "time_in_force",
            t,
            &[
                ("GTC", ev::TimeInForce::Gtc as i32),
                ("IOC", ev::TimeInForce::Ioc as i32),
            ],
        )?,
    };
    let r = call(gw.risk.clone().place_order(grpc(rk::PlaceOrderRequest {
        user_id: user.id.to_string(),
        client_order_id: o.client_order_id,
        symbol: o.symbol,
        side,
        order_type,
        time_in_force,
        price_cents: o.price_cents.unwrap_or(0),
        quantity: o.quantity,
    })))
    .await?;
    let order = r
        .order
        .ok_or_else(|| ApiError::unavailable("no order returned; retry with the same ID"))?;
    let status = if r.existing {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };
    Ok((status, Json(order_json(&order))).into_response())
}

#[derive(Deserialize)]
struct OrdersQuery {
    symbol: Option<String>,
    open_only: Option<bool>,
    cursor: Option<String>,
    limit: Option<i32>,
}

async fn list_orders(State(gw): Gw, user: User, Query(q): Query<OrdersQuery>) -> Reply {
    gw.limit(Limit::Authenticated, &user.id.to_string()).await?;
    let r = call(gw.risk.clone().list_orders(grpc(rk::ListOrdersRequest {
        user_id: user.id.to_string(),
        symbol: q.symbol.unwrap_or_default(),
        open_only: q.open_only.unwrap_or(false),
        cursor: q.cursor.unwrap_or_default(),
        page_size: page(q.limit)?,
    })))
    .await?;
    let orders: Vec<Value> = r.orders.iter().map(order_json).collect();
    Ok(Json(json!({ "orders": orders, "next_cursor": cursor(r.next_cursor) })).into_response())
}

fn order_id(id: &str) -> Result<String, ApiError> {
    Uuid::parse_str(id)
        .map(|u| u.to_string())
        .map_err(|_| ApiError::new(StatusCode::NOT_FOUND, "NOT_FOUND", "order not found"))
}

async fn get_order(State(gw): Gw, user: User, Path(id): Path<String>) -> Reply {
    gw.limit(Limit::Authenticated, &user.id.to_string()).await?;
    let r = call(gw.risk.clone().get_order(grpc(rk::GetOrderRequest {
        user_id: user.id.to_string(),
        order_id: order_id(&id)?,
    })))
    .await?;
    let order = r
        .order
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "NOT_FOUND", "order not found"))?;
    Ok(Json(order_json(&order)).into_response())
}

/// 202: the cancel is requested; the matching engine decides whether it
/// wins against a trade (the result arrives as a notification).
async fn cancel(State(gw): Gw, user: User, Path(id): Path<String>) -> Reply {
    gw.limit(Limit::Orders, &user.id.to_string()).await?;
    let r = call(gw.risk.clone().cancel_order(grpc(rk::CancelOrderRequest {
        user_id: user.id.to_string(),
        order_id: order_id(&id)?,
    })))
    .await?;
    let order = r
        .order
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "NOT_FOUND", "order not found"))?;
    Ok((StatusCode::ACCEPTED, Json(order_json(&order))).into_response())
}

// --- Market --------------------------------------------------------------

/// Every listed stock with its last price and day volume from the ticker.
/// Without Redis the prices are `null`; the list still answers.
async fn symbols(State(gw): Gw, ip: ClientIp) -> Reply {
    gw.limit(Limit::Public, &ip.0).await?;
    let r = call(
        gw.risk
            .clone()
            .list_symbols(grpc(rk::ListSymbolsRequest {})),
    )
    .await?;
    let mut out = Vec::with_capacity(r.symbols.len());
    let mut redis_up = true;
    for s in r.symbols {
        let tick = if redis_up {
            match gw.tickers.read(&s.symbol).await {
                Ok(t) => t,
                Err(e) => {
                    tracing::debug!(error = %e, "tickers unavailable");
                    redis_up = false;
                    None
                }
            }
        } else {
            None
        };
        out.push(json!({
            "symbol": s.symbol,
            "name": s.name,
            "trading": s.trading,
            "listing_price_cents": s.listing_price_cents,
            "last_price_cents": tick.map(|t| t.last_price_cents),
            "day_volume": tick.map_or(0, |t| t.day_volume),
            "last_trade_at": tick.map_or(Value::Null, |t| time(t.traded_at_ns)),
        }));
    }
    Ok(Json(json!({ "symbols": out })).into_response())
}

/// `/v1/symbols/{symbol}/book|trades|candles`, from Market Data.
async fn market(
    State(gw): Gw,
    ip: ClientIp,
    Path((symbol, view)): Path<(String, String)>,
    uri: axum::http::Uri,
) -> Reply {
    if !matches!(view.as_str(), "book" | "trades" | "candles") {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "no such route",
        ));
    }
    // Only a plain symbol goes into the upstream path.
    if symbol.is_empty() || symbol.len() > 8 || !symbol.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return Err(ApiError::invalid("symbol must be 1 to 8 letters or digits"));
    }
    gw.limit(Limit::Public, &ip.0).await?;
    let query = uri.query().map_or(String::new(), |q| format!("?{q}"));
    let path = format!("/v1/symbols/{symbol}/{view}{query}");
    proxy::forward(
        &gw.http,
        &gw.market_data,
        Method::GET,
        &path,
        None,
        Bytes::new(),
    )
    .await
}

// --- Notifications -------------------------------------------------------

async fn notifications(State(gw): Gw, user: User, uri: axum::http::Uri) -> Reply {
    gw.limit(Limit::Authenticated, &user.id.to_string()).await?;
    let query = uri.query().map_or(String::new(), |q| format!("?{q}"));
    let path = format!("/v1/notifications{query}");
    proxy::forward(
        &gw.http,
        &gw.notifications,
        Method::GET,
        &path,
        Some(user.id),
        Bytes::new(),
    )
    .await
}

/// A 30-second, one-time ticket for `/ws/private` (browsers cannot set the
/// Authorization header on a WebSocket).
async fn ws_ticket(State(gw): Gw, user: User) -> Reply {
    gw.limit(Limit::Authenticated, &user.id.to_string()).await?;
    proxy::forward(
        &gw.http,
        &gw.notifications,
        Method::POST,
        "/v1/ws-ticket",
        Some(user.id),
        Bytes::new(),
    )
    .await
}
