//! Notifications' HTTP surface. The Gateway forwards these routes here.
//!
//! ```text
//! GET  /v1/notifications?after=&limit=   stored messages, oldest first
//! POST /v1/ws-ticket                     a 30-second, one-time ticket
//! GET  /ws/private?ticket=               the private live stream
//! GET  /healthz, /readyz
//! ```
//!
//! The two `/v1` routes trust the `X-User-Id` header: the Gateway verifies
//! the login token and attaches the user ID to internal calls, and this
//! port is never exposed to the internet. `/ws/private` needs no header,
//! because browsers cannot set one on a WebSocket; the ticket names the
//! user instead.
//!
//! A client that reconnects opens the stream first, then asks
//! `GET /v1/notifications?after={last ID it has}` and ignores IDs it has
//! already seen, so nothing falls between the two.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use uuid::Uuid;

use crate::notifier::TICKET_SECONDS;
use crate::{Hub, Notifier};

pub const MAX_PAGE: i64 = 100;
const DEFAULT_PAGE: i64 = 50;
/// The private stream is one-way; anything a client sends is ignored.
const MAX_CLIENT_MESSAGE: usize = 1024;
/// A client that cannot take one message within this long is closed, so a
/// stuck browser never holds a task and a database slot forever.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
/// Wait before checking again after PostgreSQL fails mid-stream.
const RETRY_AFTER_ERROR: Duration = Duration::from_secs(1);

pub struct App {
    pub notifier: Arc<Notifier>,
    pub hub: Arc<Hub>,
}

pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/v1/notifications", get(list))
        .route("/v1/ws-ticket", post(ticket))
        .route("/ws/private", get(ws))
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/readyz", get(ready))
        .with_state(app)
}

fn error(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    let body = serde_json::json!({ "error": { "code": code, "message": message.into() } });
    (status, Json(body)).into_response()
}

fn unavailable(e: impl std::fmt::Display) -> Response {
    tracing::warn!(error = %e, "request failed");
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "SERVICE_UNAVAILABLE",
        "notifications are unavailable; retry",
    )
}

// A ready response as the error keeps handlers short; it is built once
// per bad request, so its size does not matter.
#[allow(clippy::result_large_err)]
fn user_from(headers: &HeaderMap) -> Result<Uuid, Response> {
    headers
        .get("x-user-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "UNAUTHENTICATED", "log in again"))
}

async fn ready(State(app): State<Arc<App>>) -> StatusCode {
    match sqlx::query("SELECT 1").execute(app.notifier.pool()).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

#[derive(Deserialize)]
struct ListQuery {
    after: Option<i64>,
    limit: Option<i64>,
}

/// With `after`: the messages after that ID, oldest first; `next_after` is
/// set while more remain. Without it: the newest page.
async fn list(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> Response {
    let user = match user_from(&headers) {
        Ok(u) => u,
        Err(r) => return r,
    };
    let limit = match q.limit {
        None => DEFAULT_PAGE,
        Some(n) if (1..=MAX_PAGE).contains(&n) => n,
        Some(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "VALIDATION_FAILED",
                format!("limit must be 1 to {MAX_PAGE}"),
            );
        }
    };
    let rows = match q.after {
        Some(after) => app.notifier.after(user, after, limit).await,
        None => app.notifier.latest(user, limit).await,
    };
    let rows = match rows {
        Ok(r) => r,
        Err(e) => return unavailable(e),
    };
    let next = (q.after.is_some() && rows.len() as i64 == limit)
        .then(|| rows.last().map(|n| n.id))
        .flatten();
    let items: Vec<_> = rows.iter().map(crate::Notification::json).collect();
    Json(serde_json::json!({ "notifications": items, "next_after": next })).into_response()
}

async fn ticket(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    let user = match user_from(&headers) {
        Ok(u) => u,
        Err(r) => return r,
    };
    match app.notifier.issue_ticket(user).await {
        Ok(ticket) => (
            StatusCode::CREATED,
            Json(serde_json::json!({ "ticket": ticket, "expires_in_s": TICKET_SECONDS })),
        )
            .into_response(),
        Err(e) => unavailable(e),
    }
}

#[derive(Deserialize)]
struct WsQuery {
    ticket: Option<String>,
}

async fn ws(
    State(app): State<Arc<App>>,
    Query(q): Query<WsQuery>,
    upgrade: WebSocketUpgrade,
) -> Response {
    // The ticket is checked, and used up, before the upgrade, so a bad one
    // gets a plain 401.
    let user = match app
        .notifier
        .redeem_ticket(q.ticket.as_deref().unwrap_or_default())
        .await
    {
        Ok(Some(user)) => user,
        Ok(None) => {
            return error(
                StatusCode::UNAUTHORIZED,
                "UNAUTHENTICATED",
                "the ticket is missing, expired or already used; get a new one",
            );
        }
        Err(e) => return unavailable(e),
    };
    upgrade
        .max_message_size(MAX_CLIENT_MESSAGE)
        .on_upgrade(move |socket| client(app, user, socket))
}

/// One private stream: `ready`, then every notification stored for this
/// user from now on, in ID order.
async fn client(app: Arc<App>, user: Uuid, socket: WebSocket) {
    // Register before reading the starting point: anything stored after
    // that read publishes a wake-up this socket is already listening for.
    let reg = app.hub.register(user);
    let (mut sink, mut stream) = socket.split();
    let mut last = match app.notifier.latest_id(user).await {
        Ok(id) => id,
        Err(e) => {
            tracing::warn!(error = %e, "private stream could not start");
            let _ = sink.close().await;
            return;
        }
    };
    // Tells the client where live delivery starts: it has everything once
    // it also has the stored messages up to `last_id`.
    let hello = serde_json::json!({ "type": "ready", "last_id": last }).to_string();
    if sink.send(Message::Text(hello.into())).await.is_err() {
        return;
    }

    loop {
        tokio::select! {
            () = reg.wake.notified() => {}
            msg = stream.next() => match msg {
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => continue,
            },
        }
        // Send everything new, a page at a time.
        loop {
            let rows = match app.notifier.after(user, last, MAX_PAGE).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(error = %e, "private stream read failed; retrying");
                    tokio::time::sleep(RETRY_AFTER_ERROR).await;
                    reg.wake.notify_one();
                    break;
                }
            };
            let full = rows.len() as i64 == MAX_PAGE;
            for n in rows {
                let text = n.json().to_string();
                match tokio::time::timeout(SEND_TIMEOUT, sink.send(Message::Text(text.into())))
                    .await
                {
                    Ok(Ok(())) => last = n.id,
                    Ok(Err(_)) => return,
                    Err(_) => {
                        app.hub.dropped_slow.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                }
            }
            if !full {
                break;
            }
        }
    }
    let _ = sink.close().await;
}
