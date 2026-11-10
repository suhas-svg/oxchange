//! The live view: books, the public stream's messages, gaps, slow clients,
//! and an end-to-end run against a real engine over gRPC and WebSocket.

mod common;

#[path = "../../matching/benches/workload/mod.rs"]
mod workload;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use common::{Matcher, env, level, other, trade};
use futures_util::{SinkExt, StreamExt};
use market_data::live::{CLIENT_QUEUE, Channel, Connection, RELOAD_EVERY};
use market_data::{GrpcBooks, Live, MemBooks, Snapshot};
use proto_gen::events::v1::Side::{Buy, Sell};
use serde_json::{Value, json};

fn books(symbol: &str, snap: Snapshot) -> MemBooks {
    let b = MemBooks::default();
    b.0.lock().unwrap().insert(symbol.into(), snap);
    b
}

fn next(c: &mut Connection) -> Value {
    let m = c.messages.try_recv().expect("a message is waiting");
    serde_json::from_str(&m).unwrap()
}

fn none_waiting(c: &mut Connection) {
    if let Ok(m) = c.messages.try_recv() {
        panic!("unexpected message {m}");
    }
}

#[tokio::test]
async fn subscribers_get_a_snapshot_then_ordered_updates() {
    let mem = books(
        "ACME",
        Snapshot {
            as_of_seq: 3,
            bids: vec![(990, 10)],
            asks: vec![(1010, 5)],
        },
    );
    let live = Live::new(mem.clone(), None);
    let mut c = live.connect().await;
    live.subscribe(c.id, Channel::Book, "ACME").await.unwrap();
    assert_eq!(
        next(&mut c),
        json!({"type": "book_snapshot", "symbol": "ACME", "seq": 3,
               "bids": [{"price_cents": 990, "quantity": 10}],
               "asks": [{"price_cents": 1010, "quantity": 5}]})
    );

    // Already in the snapshot: ignored.
    live.on_event(0, &level("ACME", 2, Buy, 990, 99)).await;
    none_waiting(&mut c);
    live.on_event(0, &level("ACME", 4, Buy, 995, 7)).await;
    live.on_event(0, &other("ACME", 5)).await;
    live.on_event(0, &level("ACME", 6, Sell, 1010, 0)).await;
    assert_eq!(
        next(&mut c),
        json!({"type": "book_delta", "symbol": "ACME", "seq": 4, "prev_seq": 3,
               "side": "BUY", "price_cents": 995, "quantity": 7})
    );
    assert_eq!(
        next(&mut c),
        json!({"type": "book_delta", "symbol": "ACME", "seq": 6, "prev_seq": 4,
               "side": "SELL", "price_cents": 1010, "quantity": 0})
    );
    none_waiting(&mut c);

    live.subscribe(c.id, Channel::Ticker, "ACME").await.unwrap();
    live.subscribe(c.id, Channel::Trades, "ACME").await.unwrap();
    assert_eq!(
        next(&mut c),
        json!({"type": "ticker", "symbol": "ACME", "seq": 6, "last_price_cents": null, "day_volume": 0})
    );
    live.on_event(0, &trade("ACME", 7, common::T0, 1000, 4))
        .await;
    assert_eq!(next(&mut c)["type"], "trade");
    assert_eq!(
        next(&mut c),
        json!({"type": "ticker", "symbol": "ACME", "seq": 7, "last_price_cents": 1000, "day_volume": 4})
    );

    // A gap: the stock's state is discarded and fetched again.
    mem.0.lock().unwrap().insert(
        "ACME".into(),
        Snapshot {
            as_of_seq: 20,
            bids: vec![(1000, 1)],
            asks: vec![],
        },
    );
    tokio::time::sleep(RELOAD_EVERY + Duration::from_millis(50)).await;
    live.on_event(0, &level("ACME", 21, Sell, 1020, 3)).await;
    assert_eq!(next(&mut c)["type"], "book_snapshot");
    assert_eq!(next(&mut c)["type"], "ticker");
    assert_eq!(
        next(&mut c),
        json!({"type": "book_delta", "symbol": "ACME", "seq": 21, "prev_seq": 20,
               "side": "SELL", "price_cents": 1020, "quantity": 3})
    );
    assert_eq!(
        serde_json::to_value(live.book("ACME").await.unwrap()).unwrap(),
        json!({"type": "book_snapshot", "symbol": "ACME", "seq": 21,
               "bids": [{"price_cents": 1000, "quantity": 1}],
               "asks": [{"price_cents": 1020, "quantity": 3}]})
    );

    live.unsubscribe(c.id, Channel::Book, "ACME").await;
    live.on_event(0, &level("ACME", 22, Sell, 1020, 0)).await;
    none_waiting(&mut c);
}

/// Spec: a full queue means `resync_required` and a closed connection,
/// and the other clients are not delayed.
#[tokio::test]
async fn a_slow_client_is_dropped_and_the_others_keep_streaming() {
    let live = Live::new(books("ACME", Snapshot::default()), None);
    let mut slow = live.connect().await;
    let mut fast = live.connect().await;
    live.subscribe(slow.id, Channel::Book, "ACME")
        .await
        .unwrap();
    live.subscribe(fast.id, Channel::Book, "ACME")
        .await
        .unwrap();
    let mut fast_got = 0;
    for seq in 1..=(CLIENT_QUEUE as u64 + 10) {
        live.on_event(0, &level("ACME", seq, Buy, 1000, seq as i64))
            .await;
        while fast.messages.try_recv().is_ok() {
            fast_got += 1;
        }
    }
    assert_eq!(slow.dropped.try_recv().unwrap(), "ACME");
    assert_eq!(fast_got, CLIENT_QUEUE + 10 + 1, "snapshot plus every delta");
    assert_eq!(
        live.dropped_slow.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(live.clients().await, 1);
}

#[tokio::test]
async fn bad_symbols_are_refused() {
    let live = Live::new(MemBooks::default(), None);
    let c = live.connect().await;
    for s in ["", "acme", "TOOLONGNAME", "AC-ME", "1ACME"] {
        assert!(
            live.subscribe(c.id, Channel::Book, s).await.is_err(),
            "{s:?}"
        );
    }
}

/// The whole path: a real engine, `GetBook` over gRPC mid-stream, events
/// into the live view and the recorder, and a browser-style WebSocket
/// client rebuilding the book from snapshot plus deltas.
#[tokio::test]
async fn the_public_stream_matches_the_engine_book() {
    let e = env().await;
    let m = Matcher::start().await;
    let w = workload::generate(4_000, 11);
    let symbol = w.symbols[0].clone();
    let mut inputs = w.inputs.iter();

    // Trading before Market Data starts: only the recorder sees it. (It
    // records just the watched stock, to keep the test quick.)
    for i in inputs.by_ref().take(1_500) {
        for ev in m.process(&w.symbols[i.symbol], &i.cmd) {
            if ev.symbol == symbol.as_str() {
                e.recorder.apply_event(&ev).await.unwrap();
            }
        }
    }

    let live = Arc::new(Live::new(
        GrpcBooks::connect_lazy(std::slice::from_ref(&m.url)).unwrap(),
        Some(e.pool.clone()),
    ));
    let app = Arc::new(market_data::http::App {
        live: live.clone(),
        pool: e.pool.clone(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, market_data::http::router(app))
            .await
            .unwrap();
    });

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws/public"))
        .await
        .unwrap();
    let sub = json!({"op": "subscribe", "channel": "book", "symbol": symbol.as_str()});
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        sub.to_string().into(),
    ))
    .await
    .unwrap();
    // The client's view, built only from what the server sends.
    let mut bids = BTreeMap::new();
    let mut asks = BTreeMap::new();
    let mut last_seq = None;
    let mut apply = |msg: Value| {
        let levels = |v: &Value| -> BTreeMap<i64, i64> {
            v.as_array()
                .unwrap()
                .iter()
                .map(|l| {
                    (
                        l["price_cents"].as_i64().unwrap(),
                        l["quantity"].as_i64().unwrap(),
                    )
                })
                .collect()
        };
        match msg["type"].as_str().unwrap() {
            "book_snapshot" => {
                bids = levels(&msg["bids"]);
                asks = levels(&msg["asks"]);
                last_seq = msg["seq"].as_u64();
            }
            "book_delta" => {
                assert_eq!(msg["prev_seq"].as_u64(), last_seq, "no gap in the stream");
                let side = if msg["side"] == "BUY" {
                    &mut bids
                } else {
                    &mut asks
                };
                let (p, q) = (
                    msg["price_cents"].as_i64().unwrap(),
                    msg["quantity"].as_i64().unwrap(),
                );
                if q == 0 {
                    side.remove(&p);
                } else {
                    side.insert(p, q);
                }
                last_seq = msg["seq"].as_u64();
            }
            other => panic!("unexpected {other}: {msg}"),
        }
        (bids.clone(), asks.clone())
    };
    let first = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    apply(serde_json::from_str(first.to_text().unwrap()).unwrap());

    // Live trading: every event reaches both halves.
    for i in inputs {
        for ev in m.process(&w.symbols[i.symbol], &i.cmd) {
            live.on_event(0, &ev).await;
            if ev.symbol == symbol.as_str() {
                e.recorder.apply_event(&ev).await.unwrap();
            }
        }
    }
    let want = m.view(&symbol);
    let want_bids: BTreeMap<i64, i64> = want.bids.iter().copied().collect();
    let want_asks: BTreeMap<i64, i64> = want.asks.iter().copied().collect();
    let mut seen = (BTreeMap::new(), BTreeMap::new());
    while seen != (want_bids.clone(), want_asks.clone()) {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("the stream catches up")
            .unwrap()
            .unwrap();
        seen = apply(serde_json::from_str(msg.to_text().unwrap()).unwrap());
    }
    let server = serde_json::to_value(live.book(symbol.as_str()).await.unwrap()).unwrap();
    assert_eq!(server["seq"].as_u64(), Some(want.as_of_seq));

    // REST: the recorded trades and candles cover the whole run.
    let trades: i64 = sqlx::query_scalar("SELECT count(*) FROM market_trades WHERE symbol = $1")
        .bind(symbol.as_str())
        .fetch_one(&e.pool)
        .await
        .unwrap();
    assert!(trades > 5, "only {trades} trades for {symbol}");
    let volume: i64 = sqlx::query_scalar(
        "SELECT ((SELECT sum(volume) FROM candles_1m WHERE symbol = $1)
              - (SELECT sum(quantity) FROM market_trades WHERE symbol = $1))::bigint",
    )
    .bind(symbol.as_str())
    .fetch_one(&e.pool)
    .await
    .unwrap();
    assert_eq!(volume, 0, "candle volume equals traded quantity");
    let body = get(addr, &format!("/v1/symbols/{symbol}/trades?limit=100")).await;
    let page = body["trades"].as_array().unwrap();
    assert_eq!(page.len() as i64, trades.min(100));
    let seqs: Vec<i64> = page.iter().map(|t| t["seq"].as_i64().unwrap()).collect();
    assert!(seqs.windows(2).all(|w| w[0] > w[1]), "newest first");
    assert_eq!(
        get(addr, "/v1/symbols/acme/book").await["error"]["code"],
        "VALIDATION_FAILED"
    );
    let candles = get(addr, &format!("/v1/symbols/{symbol}/candles")).await;
    assert!(!candles["candles"].as_array().unwrap().is_empty());
}

async fn get(addr: std::net::SocketAddr, path: &str) -> Value {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(format!("GET {path} HTTP/1.0\r\nHost: x\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut buf = String::new();
    s.read_to_string(&mut buf).await.unwrap();
    let body = buf.split_once("\r\n\r\n").unwrap().1;
    serde_json::from_str(body).unwrap()
}
