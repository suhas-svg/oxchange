//! Market Data's HTTP surface. The Gateway forwards the public routes here.
//!
//! ```text
//! GET /v1/symbols/{symbol}/book                         current depth
//! GET /v1/symbols/{symbol}/trades?before_seq=&limit=    newest first
//! GET /v1/symbols/{symbol}/candles?before_ms=&limit=    newest first
//! GET /ws/public                                        live stream
//! GET /healthz, /readyz
//! ```
//!
//! Lists use cursor paging with at most 100 items per page (spec: "REST
//! endpoints"). Times are Unix milliseconds.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use time::OffsetDateTime;

use crate::live::{BookSource, Channel, Live, Out, domain_symbol_ok};

pub const MAX_PAGE: i64 = 100;
const DEFAULT_PAGE: i64 = 50;
/// Largest message a client may send; subscribe requests are tiny.
const MAX_CLIENT_MESSAGE: usize = 4 * 1024;

pub struct App<B> {
    pub live: Arc<Live<B>>,
    pub pool: PgPool,
}

pub fn router<B: BookSource>(app: Arc<App<B>>) -> Router {
    Router::new()
        .route("/v1/symbols/{symbol}/book", get(book::<B>))
        .route("/v1/symbols/{symbol}/trades", get(trades::<B>))
        .route("/v1/symbols/{symbol}/candles", get(candles::<B>))
        .route("/ws/public", get(ws::<B>))
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/readyz", get(ready::<B>))
        .with_state(app)
}

fn error(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    let body = serde_json::json!({ "error": { "code": code, "message": message.into() } });
    (status, Json(body)).into_response()
}

fn internal(e: impl std::fmt::Display) -> Response {
    tracing::warn!(error = %e, "request failed");
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "SERVICE_UNAVAILABLE",
        "market data is unavailable; retry",
    )
}

// A ready response as the error keeps handlers short; it is built once
// per bad request, so its size does not matter.
#[allow(clippy::result_large_err)]
fn check_symbol(s: &str) -> Result<(), Response> {
    domain_symbol_ok(s).map_err(|m| error(StatusCode::BAD_REQUEST, "VALIDATION_FAILED", m))
}

// A ready response as the error keeps handlers short; it is built once
// per bad request, so its size does not matter.
#[allow(clippy::result_large_err)]
fn page(limit: Option<i64>) -> Result<i64, Response> {
    match limit {
        None => Ok(DEFAULT_PAGE),
        Some(n) if (1..=MAX_PAGE).contains(&n) => Ok(n),
        Some(_) => Err(error(
            StatusCode::BAD_REQUEST,
            "VALIDATION_FAILED",
            format!("limit must be 1 to {MAX_PAGE}"),
        )),
    }
}

fn ms(t: OffsetDateTime) -> i64 {
    i64::try_from(t.unix_timestamp_nanos() / 1_000_000).unwrap_or(i64::MAX)
}

async fn ready<B: BookSource>(State(app): State<Arc<App<B>>>) -> StatusCode {
    match sqlx::query("SELECT 1").execute(&app.pool).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn book<B: BookSource>(
    State(app): State<Arc<App<B>>>,
    Path(symbol): Path<String>,
) -> Response {
    if let Err(r) = check_symbol(&symbol) {
        return r;
    }
    match app.live.book(&symbol).await {
        Ok(b) => Json(b).into_response(),
        Err(e) => internal(e),
    }
}

#[derive(Deserialize)]
struct TradesQuery {
    before_seq: Option<i64>,
    limit: Option<i64>,
}

#[derive(Serialize, sqlx::FromRow)]
struct TradeRow {
    seq: i64,
    price_cents: i64,
    quantity: i64,
    aggressor_side: String,
    #[serde(skip)]
    executed_at: OffsetDateTime,
}

#[derive(Serialize)]
struct TradeOut {
    #[serde(flatten)]
    row: TradeRow,
    executed_at_ms: i64,
}

async fn trades<B: BookSource>(
    State(app): State<Arc<App<B>>>,
    Path(symbol): Path<String>,
    Query(q): Query<TradesQuery>,
) -> Response {
    if let Err(r) = check_symbol(&symbol) {
        return r;
    }
    let limit = match page(q.limit) {
        Ok(n) => n,
        Err(r) => return r,
    };
    let rows: Vec<TradeRow> = match sqlx::query_as(
        "SELECT seq, price_cents, quantity, aggressor_side, executed_at FROM market_trades
         WHERE symbol = $1 AND seq < $2 ORDER BY seq DESC LIMIT $3",
    )
    .bind(&symbol)
    .bind(q.before_seq.unwrap_or(i64::MAX))
    .bind(limit)
    .fetch_all(&app.pool)
    .await
    {
        Ok(r) => r,
        Err(e) => return internal(e),
    };
    let next = (rows.len() as i64 == limit).then(|| rows.last().map(|r| r.seq));
    let trades: Vec<TradeOut> = rows
        .into_iter()
        .map(|row| TradeOut {
            executed_at_ms: ms(row.executed_at),
            row,
        })
        .collect();
    Json(serde_json::json!({ "symbol": symbol, "trades": trades, "next_before_seq": next.flatten() }))
        .into_response()
}

#[derive(Deserialize)]
struct CandlesQuery {
    before_ms: Option<i64>,
    limit: Option<i64>,
}

#[derive(Serialize)]
struct CandleOut {
    bucket_start_ms: i64,
    open_cents: i64,
    high_cents: i64,
    low_cents: i64,
    close_cents: i64,
    volume: i64,
}

async fn candles<B: BookSource>(
    State(app): State<Arc<App<B>>>,
    Path(symbol): Path<String>,
    Query(q): Query<CandlesQuery>,
) -> Response {
    if let Err(r) = check_symbol(&symbol) {
        return r;
    }
    let limit = match page(q.limit) {
        Ok(n) => n,
        Err(r) => return r,
    };
    let before = match q.before_ms {
        None => None,
        Some(m) => match OffsetDateTime::from_unix_timestamp_nanos(i128::from(m) * 1_000_000) {
            Ok(t) => Some(t),
            Err(_) => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "VALIDATION_FAILED",
                    "before_ms is out of range",
                );
            }
        },
    };
    let rows: Vec<(OffsetDateTime, i64, i64, i64, i64, i64)> = match sqlx::query_as(
        "SELECT bucket_start, open_cents, high_cents, low_cents, close_cents, volume
         FROM candles_1m WHERE symbol = $1 AND ($2::timestamptz IS NULL OR bucket_start < $2)
         ORDER BY bucket_start DESC LIMIT $3",
    )
    .bind(&symbol)
    .bind(before)
    .bind(limit)
    .fetch_all(&app.pool)
    .await
    {
        Ok(r) => r,
        Err(e) => return internal(e),
    };
    let candles: Vec<CandleOut> = rows
        .into_iter()
        .map(|(t, o, h, l, c, v)| CandleOut {
            bucket_start_ms: ms(t),
            open_cents: o,
            high_cents: h,
            low_cents: l,
            close_cents: c,
            volume: v,
        })
        .collect();
    let next = (candles.len() as i64 == limit).then(|| candles.last().map(|c| c.bucket_start_ms));
    Json(serde_json::json!({ "symbol": symbol, "candles": candles, "next_before_ms": next.flatten() }))
        .into_response()
}

/// `{ "op": "subscribe", "channel": "book", "symbol": "ACME" }`
#[derive(Deserialize)]
struct ClientOp {
    op: String,
    channel: Channel,
    symbol: String,
}

async fn ws<B: BookSource>(State(app): State<Arc<App<B>>>, upgrade: WebSocketUpgrade) -> Response {
    // Clients only send small subscribe requests, so a read buffer the size
    // of the largest one is enough. The default (128 KiB per socket, taken
    // up front) cost 750 MiB with 5,000 clients.
    upgrade
        .max_message_size(MAX_CLIENT_MESSAGE)
        .read_buffer_size(MAX_CLIENT_MESSAGE)
        .on_upgrade(move |socket| client(app, socket))
}

async fn client<B: BookSource>(app: Arc<App<B>>, socket: WebSocket) {
    let live = app.live.clone();
    drop(app);
    let conn = live.connect().await;
    let id = conn.id;
    let (mut sink, mut stream) = socket.split();

    // Writer: the client's queue, until it ends or the client falls behind.
    let mut messages = conn.messages;
    let mut dropped = conn.dropped;
    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                m = messages.recv() => match m {
                    Some(m) => {
                        if sink.send(Message::Text(m.as_ref().into())).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                },
                symbol = &mut dropped => {
                    if let Ok(symbol) = symbol {
                        let msg = Out::ResyncRequired { symbol }.json();
                        let _ = sink.send(Message::Text(msg.as_ref().into())).await;
                    }
                    let _ = sink.close().await;
                    return;
                }
            }
        }
    });

    // Reader: subscribe and unsubscribe requests.
    while let Some(Ok(msg)) = stream.next().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };
        let reply = match serde_json::from_str::<ClientOp>(&text) {
            Ok(op) if op.op == "subscribe" => {
                live.subscribe(id, op.channel, &op.symbol).await.err()
            }
            Ok(op) if op.op == "unsubscribe" => {
                live.unsubscribe(id, op.channel, &op.symbol).await;
                None
            }
            Ok(op) => Some(format!("unknown op {:?}", op.op)),
            Err(e) => Some(format!("bad request: {e}")),
        };
        if let Some(message) = reply {
            live.send_error(id, message).await;
        }
        if writer.is_finished() {
            break;
        }
    }
    live.disconnect(id).await;
    writer.abort();
}
