//! WebSocket proxies: `/ws/public` to Market Data and `/ws/private` to
//! Notifications.
//!
//! Limits (spec: "Rate limits"):
//! - at most 5 open WebSockets at once per client address and stream;
//! - at most 50 subscriptions per public connection.
//!
//! The private stream's user is only known to Notifications (the one-time
//! ticket names it), so open connections are counted per address on both
//! streams, and on each Gateway copy separately: a crashed copy cannot
//! leave stale counts behind.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::tungstenite;

use crate::limits::Limit;
use crate::{ApiError, ClientIp, Gateway};

pub const MAX_SOCKETS: usize = 5;
pub const MAX_SUBSCRIPTIONS: usize = 50;
/// Clients send only small control messages.
const MAX_CLIENT_MESSAGE: usize = 4 * 1024;

/// Open sockets per (stream, address) on this copy.
#[derive(Default)]
pub struct Sockets {
    open: Mutex<HashMap<String, usize>>,
}

/// One counted socket; dropping it frees the slot.
pub struct Slot {
    sockets: Arc<Gateway>,
    key: String,
}

impl Sockets {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, usize>> {
        self.open.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Open sockets on this copy (`websocket_clients`).
    pub fn total(&self) -> usize {
        self.lock().values().sum()
    }
}

fn take_slot(gw: &Arc<Gateway>, key: String) -> Result<Slot, ApiError> {
    let mut open = gw.sockets.lock();
    let n = open.entry(key.clone()).or_insert(0);
    if *n >= MAX_SOCKETS {
        return Err(ApiError::rate_limited(5));
    }
    *n += 1;
    drop(open);
    Ok(Slot {
        sockets: gw.clone(),
        key,
    })
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut open = self.sockets.sockets.lock();
        if let Some(n) = open.get_mut(&self.key) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                open.remove(&self.key);
            }
        }
    }
}

pub fn router() -> Router<Arc<Gateway>> {
    Router::new()
        .route("/ws/public", get(public))
        .route("/ws/private", get(private))
}

fn ws_url(base: &str, path: &str) -> String {
    let rest = base
        .strip_prefix("http://")
        .map(|r| format!("ws://{r}"))
        .or_else(|| base.strip_prefix("https://").map(|r| format!("wss://{r}")))
        .unwrap_or_else(|| base.to_owned());
    format!("{rest}{path}")
}

async fn public(
    State(gw): State<Arc<Gateway>>,
    ip: ClientIp,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    gw.limit(Limit::Public, &ip.0).await?;
    let slot = take_slot(&gw, format!("public:{}", ip.0))?;
    let url = ws_url(&gw.market_data, "/ws/public");
    let (upstream, _) = connect(&url).await?;
    Ok(upgrade
        .max_message_size(MAX_CLIENT_MESSAGE)
        .on_upgrade(move |socket| async move {
            pump(socket, upstream, Some(MAX_SUBSCRIPTIONS)).await;
            drop(slot);
        }))
}

#[derive(Deserialize)]
struct Ticket {
    ticket: Option<String>,
}

async fn private(
    State(gw): State<Arc<Gateway>>,
    ip: ClientIp,
    Query(t): Query<Ticket>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    gw.limit(Limit::Public, &ip.0).await?;
    let ticket = t.ticket.unwrap_or_default();
    // Tickets are base64url; anything else could not be one.
    if ticket.is_empty()
        || ticket.len() > 64
        || !ticket
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(ApiError::unauthenticated(
            "the ticket is missing or malformed; get a new one",
        ));
    }
    let slot = take_slot(&gw, format!("private:{}", ip.0))?;
    // Notifications redeems the ticket during this handshake, so a bad one
    // is refused before the browser's upgrade is accepted.
    let url = ws_url(&gw.notifications, &format!("/ws/private?ticket={ticket}"));
    let (upstream, _) = connect(&url).await?;
    Ok(upgrade
        .max_message_size(MAX_CLIENT_MESSAGE)
        .on_upgrade(move |socket| async move {
            pump(socket, upstream, None).await;
            drop(slot);
        })
        .into_response())
}

type Upstream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(
    url: &str,
) -> Result<(Upstream, tungstenite::handshake::client::Response), ApiError> {
    match tokio::time::timeout(
        crate::UPSTREAM_TIMEOUT,
        tokio_tungstenite::connect_async(url),
    )
    .await
    {
        Ok(Ok(c)) => Ok(c),
        Ok(Err(tungstenite::Error::Http(resp))) if resp.status() == 401 => {
            Err(ApiError::unauthenticated(
                "the ticket is missing, expired or already used; get a new one",
            ))
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "upstream websocket failed");
            Err(ApiError::unavailable(
                "the live stream is unavailable; retry",
            ))
        }
        Err(_) => Err(ApiError::unavailable(
            "the live stream did not answer in time; retry",
        )),
    }
}

/// `{ "op": "subscribe", "channel": "book", "symbol": "ACME" }`
#[derive(Deserialize)]
struct Op {
    op: String,
    channel: String,
    symbol: String,
}

/// Copies messages both ways until either side closes. With
/// `max_subscriptions`, subscribe requests beyond it are answered here and
/// never reach the service.
async fn pump(client: WebSocket, upstream: Upstream, max_subscriptions: Option<usize>) {
    let (mut c_tx, mut c_rx) = client.split();
    let (mut u_tx, mut u_rx) = upstream.split();
    let (reply_tx, mut reply_rx) = tokio::sync::mpsc::channel::<String>(8);

    // Service → browser, plus the Gateway's own replies.
    let down = async move {
        loop {
            tokio::select! {
                m = u_rx.next() => {
                    let out = match m {
                        Some(Ok(tungstenite::Message::Text(t))) => Message::Text(t.as_str().into()),
                        Some(Ok(tungstenite::Message::Binary(b))) => Message::Binary(b),
                        Some(Ok(tungstenite::Message::Close(f))) => {
                            let frame = f.map(|f| CloseFrame {
                                code: f.code.into(),
                                reason: f.reason.as_str().into(),
                            });
                            let _ = c_tx.send(Message::Close(frame)).await;
                            return;
                        }
                        Some(Ok(_)) => continue,
                        Some(Err(_)) | None => {
                            let _ = c_tx.send(Message::Close(None)).await;
                            return;
                        }
                    };
                    if c_tx.send(out).await.is_err() {
                        return;
                    }
                }
                r = reply_rx.recv() => match r {
                    Some(text) => {
                        if c_tx.send(Message::Text(text.into())).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                },
            }
        }
    };

    // Browser → service.
    let up = async move {
        let mut subscribed: HashSet<(String, String)> = HashSet::new();
        while let Some(Ok(m)) = c_rx.next().await {
            let out = match m {
                Message::Text(t) => {
                    if let Some(max) = max_subscriptions
                        && let Ok(op) = serde_json::from_str::<Op>(&t)
                    {
                        let key = (op.channel, op.symbol);
                        match op.op.as_str() {
                            "subscribe"
                                if !subscribed.contains(&key) && subscribed.len() >= max =>
                            {
                                let msg = serde_json::json!({
                                    "type": "error",
                                    "message": format!("at most {max} subscriptions per connection"),
                                });
                                let _ = reply_tx.send(msg.to_string()).await;
                                continue;
                            }
                            "subscribe" => {
                                subscribed.insert(key);
                            }
                            "unsubscribe" => {
                                subscribed.remove(&key);
                            }
                            _ => {}
                        }
                    }
                    tungstenite::Message::Text(t.as_str().into())
                }
                Message::Binary(b) => tungstenite::Message::Binary(b),
                Message::Close(_) => break,
                _ => continue,
            };
            if u_tx.send(out).await.is_err() {
                break;
            }
        }
        let _ = u_tx.close().await;
    };

    // Either direction ending ends both.
    tokio::select! {
        () = down => {}
        () = up => {}
    }
}
