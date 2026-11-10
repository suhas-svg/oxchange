//! The WebSocket proxies and their limits.

mod common;

use common::{env, next, refused};
use futures_util::SinkExt;
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn the_public_stream_passes_messages_both_ways_and_caps_subscriptions() {
    let e = env().await;
    let mut ws = e.ws("/ws/public").await.unwrap();
    let sub =
        |i: usize| json!({ "op": "subscribe", "channel": "ticker", "symbol": format!("S{i}") });
    for i in 0..50 {
        ws.send(Message::Text(sub(i).to_string().into()))
            .await
            .unwrap();
        assert_eq!(next(&mut ws).await["type"], "echo");
    }
    // The 51st is answered by the Gateway and never reaches Market Data.
    ws.send(Message::Text(sub(50).to_string().into()))
        .await
        .unwrap();
    let r = next(&mut ws).await;
    assert_eq!(r["type"], "error");
    assert!(r["message"].as_str().unwrap().contains("50"));
    // Subscribing again to one already held is fine, and unsubscribing
    // frees a place.
    ws.send(Message::Text(sub(3).to_string().into()))
        .await
        .unwrap();
    assert_eq!(next(&mut ws).await["type"], "echo");
    let unsub = json!({ "op": "unsubscribe", "channel": "ticker", "symbol": "S3" });
    ws.send(Message::Text(unsub.to_string().into()))
        .await
        .unwrap();
    assert_eq!(next(&mut ws).await["type"], "echo");
    ws.send(Message::Text(sub(50).to_string().into()))
        .await
        .unwrap();
    assert_eq!(next(&mut ws).await["type"], "echo");
}

#[tokio::test]
async fn at_most_five_streams_per_address_and_closed_ones_free_their_place() {
    let e = env().await;
    let mut open = Vec::new();
    for _ in 0..5 {
        open.push(e.ws("/ws/public").await.unwrap());
    }
    assert_eq!(refused(e.ws("/ws/public").await), 429);
    // The private stream counts separately.
    assert!(e.ws("/ws/private?ticket=good-ticket").await.is_ok());

    // That private stream was dropped at once; now one public one too.
    drop(open.pop());
    let mut freed = false;
    for _ in 0..50 {
        if e.gateway.sockets.total() == 4 {
            freed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(freed, "the closed socket left the count");
    assert!(e.ws("/ws/public").await.is_ok());
}

#[tokio::test]
async fn the_private_stream_needs_a_good_ticket() {
    let e = env().await;
    let mut ws = e.ws("/ws/private?ticket=good-ticket").await.unwrap();
    assert_eq!(next(&mut ws).await["type"], "ready");
    // Refused before the browser's upgrade is accepted.
    assert_eq!(refused(e.ws("/ws/private?ticket=used-ticket").await), 401);
    assert_eq!(refused(e.ws("/ws/private").await), 401);
    assert_eq!(refused(e.ws("/ws/private?ticket=../../admin").await), 401);
}
