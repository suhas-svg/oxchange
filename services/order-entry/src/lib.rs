//! Phase 1 order endpoint (spec build plan: "Kafka loop and a basic order
//! endpoint").
//!
//! **Development stand-in only.** It turns REST requests into commands on
//! `orders.commands` so orders can flow end to end before the Gateway and
//! Risk exist. It has no login (the user comes from an `X-User-Id` header),
//! reserves no funds, does not deduplicate `client_order_id`, and refuses
//! market orders because there is no reference price yet. Phase 2 and 3
//! replace it.
//!
//! Requests and errors follow the spec's REST API shapes, so clients written
//! against it keep working when the real Gateway arrives.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use domain::{
    CommandId, MAX_ORDER_VALUE_CENTS, OrderId, OrderType, Price, Qty, Side, Symbol, TimeInForce,
    UserId,
};
use order_book::{CancelOrder, Command, CommandBody, NewOrder};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

/// Kafka record header that carries the W3C trace context.
pub const TRACEPARENT: &str = "traceparent";
/// Request bodies over this size are refused (spec: "Input").
pub const MAX_BODY_BYTES: usize = 16 * 1024;

/// Where commands go. The real one is Kafka; tests use memory.
pub trait Publisher: Send + Sync + 'static {
    /// Resolves once the command is durably stored, or fails.
    fn publish(
        &self,
        symbol: &Symbol,
        payload: Vec<u8>,
        headers: Vec<(String, Vec<u8>)>,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

pub fn router<P: Publisher>(publisher: Arc<P>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/orders", post(place_order::<P>))
        .route("/v1/orders/{order_id}", delete(cancel_order::<P>))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(publisher)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaceOrder {
    pub client_order_id: String,
    pub symbol: String,
    pub side: String,
    #[serde(rename = "type")]
    pub order_type: String,
    pub time_in_force: String,
    pub price_cents: Option<i64>,
    pub quantity: i64,
}

#[derive(Debug, Serialize)]
struct Accepted {
    order_id: String,
    client_order_id: String,
    status: &'static str,
    filled_quantity: i64,
    accepted_at_ns: i64,
    trace_id: String,
}

#[derive(Debug, Deserialize)]
struct CancelQuery {
    symbol: String,
}

/// A spec-shaped error: `{"error": {"code", "message", "trace_id"}}`.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    trace_id: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({ "error": {
            "code": self.code, "message": self.message, "trace_id": self.trace_id,
        }});
        (self.status, Json(body)).into_response()
    }
}

fn error(
    status: StatusCode,
    code: &'static str,
    message: impl Into<String>,
    trace: &str,
) -> ApiError {
    ApiError {
        status,
        code,
        message: message.into(),
        trace_id: trace.to_owned(),
    }
}

fn invalid(message: impl Into<String>, trace: &str) -> ApiError {
    error(StatusCode::BAD_REQUEST, "VALIDATION_FAILED", message, trace)
}

fn user_from(headers: &HeaderMap, trace: &str) -> Result<UserId, ApiError> {
    headers
        .get("x-user-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| Uuid::parse_str(v).ok())
        .map(UserId)
        .ok_or_else(|| {
            error(
                StatusCode::UNAUTHORIZED,
                "UNAUTHENTICATED",
                "X-User-Id must be a UUID (development stand-in for login)",
                trace,
            )
        })
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// A new trace ID and the `traceparent` header that starts the trace.
fn new_trace() -> (String, (String, Vec<u8>)) {
    let trace_id = Uuid::new_v4().simple().to_string();
    let span_id = &Uuid::new_v4().simple().to_string()[..16];
    let header = format!("00-{trace_id}-{span_id}-01");
    (trace_id, (TRACEPARENT.to_owned(), header.into_bytes()))
}

/// Validates a request and builds its command. Pure, so it is unit-tested.
pub fn new_order_command(
    req: &PlaceOrder,
    user_id: UserId,
    order_id: OrderId,
    command_id: CommandId,
    accepted_at_ns: i64,
    trace: &str,
) -> Result<(Symbol, Command), ApiError> {
    if req.client_order_id.is_empty() || req.client_order_id.len() > 64 {
        return Err(invalid("client_order_id must be 1 to 64 characters", trace));
    }
    let symbol = Symbol::new(&req.symbol).map_err(|e| invalid(e.to_string(), trace))?;
    let side = match req.side.as_str() {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        _ => return Err(invalid("side must be BUY or SELL", trace)),
    };
    match req.order_type.as_str() {
        "LIMIT" => {}
        "MARKET" => {
            return Err(error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "SYMBOL_NOT_TRADING",
                "market orders need a reference price, which arrives with Risk",
                trace,
            ));
        }
        _ => return Err(invalid("type must be LIMIT or MARKET", trace)),
    }
    let time_in_force = match req.time_in_force.as_str() {
        "GTC" => TimeInForce::Gtc,
        "IOC" => TimeInForce::Ioc,
        _ => return Err(invalid("time_in_force must be GTC or IOC", trace)),
    };
    let cents = req
        .price_cents
        .ok_or_else(|| invalid("price_cents is required for limit orders", trace))?;
    let price = Price::new(cents).map_err(|e| invalid(e.to_string(), trace))?;
    let qty = Qty::new_order(req.quantity).map_err(|e| invalid(e.to_string(), trace))?;
    let value = price
        .notional(qty)
        .map_err(|e| invalid(e.to_string(), trace))?;
    if value.get() > MAX_ORDER_VALUE_CENTS {
        return Err(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "RISK_LIMIT_EXCEEDED",
            "order value is above $1,000,000",
            trace,
        ));
    }
    Ok((
        symbol,
        Command {
            command_id,
            accepted_at_ns,
            body: CommandBody::NewOrder(NewOrder {
                order_id,
                user_id,
                side,
                order_type: OrderType::Limit,
                time_in_force,
                price_cents: price.cents(),
                quantity: qty.shares(),
            }),
        },
    ))
}

async fn place_order<P: Publisher>(
    State(publisher): State<Arc<P>>,
    headers: HeaderMap,
    body: Result<Json<PlaceOrder>, axum::extract::rejection::JsonRejection>,
) -> Result<(StatusCode, Json<Accepted>), ApiError> {
    let (trace_id, traceparent) = new_trace();
    let user_id = user_from(&headers, &trace_id)?;
    let Json(req) = body.map_err(|e| {
        let mut err = invalid(e.body_text(), &trace_id);
        // Keep 413 for oversized bodies; everything else is a plain 400.
        if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
            err.status = StatusCode::PAYLOAD_TOO_LARGE;
        }
        err
    })?;
    let order_id = OrderId::new_v7();
    let accepted_at_ns = now_ns();
    let (symbol, cmd) = new_order_command(
        &req,
        user_id,
        order_id,
        CommandId::new_v7(),
        accepted_at_ns,
        &trace_id,
    )?;
    publish(&*publisher, &symbol, &cmd, traceparent, &trace_id).await?;
    tracing::info!(%order_id, %symbol, %trace_id, "order accepted for matching");
    Ok((
        StatusCode::ACCEPTED,
        Json(Accepted {
            order_id: order_id.to_string(),
            client_order_id: req.client_order_id,
            status: "ACCEPTED",
            filled_quantity: 0,
            accepted_at_ns,
            trace_id,
        }),
    ))
}

async fn cancel_order<P: Publisher>(
    State(publisher): State<Arc<P>>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
    query: Result<Query<CancelQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let (trace_id, traceparent) = new_trace();
    let user_id = user_from(&headers, &trace_id)?;
    let order_id = Uuid::parse_str(&order_id).map(OrderId).map_err(|_| {
        error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "order_id is not a UUID",
            &trace_id,
        )
    })?;
    // Without Risk there is no order store to look the stock up in, so the
    // caller names it; the cancel must reach the partition holding the order.
    let Query(q) = query.map_err(|_| invalid("?symbol= is required", &trace_id))?;
    let symbol = Symbol::new(&q.symbol).map_err(|e| invalid(e.to_string(), &trace_id))?;
    let cmd = Command {
        command_id: CommandId::new_v7(),
        accepted_at_ns: now_ns(),
        body: CommandBody::CancelOrder(CancelOrder { order_id, user_id }),
    };
    publish(&*publisher, &symbol, &cmd, traceparent, &trace_id).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            json!({ "order_id": order_id.to_string(), "status": "CANCEL_REQUESTED", "trace_id": trace_id }),
        ),
    ))
}

async fn publish<P: Publisher>(
    publisher: &P,
    symbol: &Symbol,
    cmd: &Command,
    traceparent: (String, Vec<u8>),
    trace_id: &str,
) -> Result<(), ApiError> {
    let payload = matching::codec::encode_command(symbol, cmd);
    publisher
        .publish(symbol, payload, vec![traceparent])
        .await
        .map_err(|e| {
            tracing::error!(error = %e, %trace_id, "publishing command failed");
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "SERVICE_UNAVAILABLE",
                "the order could not be queued; retry",
                trace_id,
            )
        })
}

#[cfg(feature = "kafka")]
pub mod kafka {
    use std::time::Duration;

    use domain::Symbol;
    use rdkafka::ClientConfig;
    use rdkafka::message::{Header, OwnedHeaders};
    use rdkafka::producer::{FutureProducer, FutureRecord};

    use super::Publisher;

    pub const COMMANDS_TOPIC: &str = "orders.commands";

    pub struct KafkaPublisher {
        producer: FutureProducer,
    }

    impl KafkaPublisher {
        pub fn connect(brokers: &str) -> Result<Self, String> {
            let producer = ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("acks", "all")
                .set("enable.idempotence", "true")
                // Every producer of commands must pick partitions the same way,
                // so one stock's commands always share a partition.
                .set("partitioner", "murmur2_random")
                .set("linger.ms", "1")
                .create()
                .map_err(|e| e.to_string())?;
            Ok(Self { producer })
        }
    }

    impl Publisher for KafkaPublisher {
        async fn publish(
            &self,
            symbol: &Symbol,
            payload: Vec<u8>,
            headers: Vec<(String, Vec<u8>)>,
        ) -> Result<(), String> {
            let hs = headers.iter().fold(OwnedHeaders::new(), |hs, (k, v)| {
                hs.insert(Header {
                    key: k,
                    value: Some(v.as_slice()),
                })
            });
            let record = FutureRecord::to(COMMANDS_TOPIC)
                .key(symbol.as_str())
                .payload(&payload)
                .headers(hs);
            self.producer
                .send(record, Duration::from_secs(5))
                .await
                .map(|_| ())
                .map_err(|(e, _)| e.to_string())
        }
    }
}
