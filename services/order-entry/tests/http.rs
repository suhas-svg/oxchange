use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use domain::Symbol;
use http_body_util::BodyExt;
use matching::codec::decode_command;
use order_book::CommandBody;
use order_entry::{Publisher, TRACEPARENT, router};
use serde_json::Value;
use tower::ServiceExt;

type Sent = (Symbol, Vec<u8>, Vec<(String, Vec<u8>)>);

#[derive(Default)]
struct MemPublisher {
    sent: Mutex<Vec<Sent>>,
    fail: bool,
}

impl Publisher for MemPublisher {
    async fn publish(
        &self,
        symbol: &Symbol,
        payload: Vec<u8>,
        headers: Vec<(String, Vec<u8>)>,
    ) -> Result<(), String> {
        if self.fail {
            return Err("broker down".into());
        }
        self.sent
            .lock()
            .unwrap()
            .push((symbol.clone(), payload, headers));
        Ok(())
    }
}

const USER: &str = "0194d5a2-7b3c-7e11-9a44-2f1c6d8e0b11";

fn order(body: &str) -> Request<Body> {
    Request::post("/v1/orders")
        .header("content-type", "application/json")
        .header("x-user-id", USER)
        .body(Body::from(body.to_owned()))
        .unwrap()
}

const SPEC_EXAMPLE: &str = r#"{
  "client_order_id": "web-7f3a-0001",
  "symbol": "ACME",
  "side": "BUY",
  "type": "LIMIT",
  "time_in_force": "GTC",
  "price_cents": 1001,
  "quantity": 120
}"#;

async fn call(p: &Arc<MemPublisher>, req: Request<Body>) -> (StatusCode, Value) {
    let res = router(p.clone()).oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn spec_example_order_is_published_and_accepted() {
    let p = Arc::new(MemPublisher::default());
    let (status, body) = call(&p, order(SPEC_EXAMPLE)).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body["status"], "ACCEPTED");
    assert_eq!(body["client_order_id"], "web-7f3a-0001");

    let sent = p.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    let (symbol, payload, headers) = &sent[0];
    assert_eq!(symbol.as_str(), "ACME");
    let (decoded_symbol, cmd) = decode_command(payload).unwrap();
    assert_eq!(&decoded_symbol, symbol);
    let CommandBody::NewOrder(o) = cmd.body else {
        panic!("expected a new order")
    };
    assert_eq!(o.order_id.to_string(), body["order_id"]);
    assert_eq!(o.user_id.to_string(), USER);
    assert_eq!((o.price_cents, o.quantity), (1001, 120));
    // The trace started here travels with the command.
    let tp = headers.iter().find(|(k, _)| k == TRACEPARENT).unwrap();
    let tp = String::from_utf8(tp.1.clone()).unwrap();
    assert!(tp.contains(body["trace_id"].as_str().unwrap()), "{tp}");
}

#[tokio::test]
async fn bad_requests_get_spec_error_codes_and_publish_nothing() {
    let p = Arc::new(MemPublisher::default());
    let cases = [
        (
            SPEC_EXAMPLE.replace("1001", "0"),
            StatusCode::BAD_REQUEST,
            "VALIDATION_FAILED",
        ),
        (
            SPEC_EXAMPLE.replace("\"BUY\"", "\"HOLD\""),
            StatusCode::BAD_REQUEST,
            "VALIDATION_FAILED",
        ),
        (
            SPEC_EXAMPLE.replace("ACME", "acme"),
            StatusCode::BAD_REQUEST,
            "VALIDATION_FAILED",
        ),
        (
            SPEC_EXAMPLE.replace("\"quantity\": 120", "\"quantity\": 120, \"extra\": 1"),
            StatusCode::BAD_REQUEST,
            "VALIDATION_FAILED",
        ),
        (
            SPEC_EXAMPLE
                .replace("\"quantity\": 120", "\"quantity\": 1000000")
                .replace("1001", "1000000"),
            StatusCode::UNPROCESSABLE_ENTITY,
            "RISK_LIMIT_EXCEEDED",
        ),
        (
            SPEC_EXAMPLE.replace("\"LIMIT\"", "\"MARKET\""),
            StatusCode::UNPROCESSABLE_ENTITY,
            "SYMBOL_NOT_TRADING",
        ),
    ];
    for (body, status, code) in cases {
        let (got, json) = call(&p, order(&body)).await;
        assert_eq!(got, status, "{body}");
        assert_eq!(json["error"]["code"], code, "{body}");
        assert_eq!(json["error"]["trace_id"].as_str().unwrap().len(), 32);
    }
    assert!(p.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn missing_user_is_unauthenticated() {
    let p = Arc::new(MemPublisher::default());
    let req = Request::post("/v1/orders")
        .header("content-type", "application/json")
        .body(Body::from(SPEC_EXAMPLE))
        .unwrap();
    let (status, json) = call(&p, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(json["error"]["code"], "UNAUTHENTICATED");
}

#[tokio::test]
async fn oversized_body_is_refused() {
    let p = Arc::new(MemPublisher::default());
    let big = format!(r#"{{"client_order_id": "{}"}}"#, "x".repeat(20_000));
    let res = router(p.clone()).oneshot(order(&big)).await.unwrap();
    assert_eq!(res.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn broker_failure_is_service_unavailable() {
    let p = Arc::new(MemPublisher {
        fail: true,
        ..Default::default()
    });
    let (status, json) = call(&p, order(SPEC_EXAMPLE)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json["error"]["code"], "SERVICE_UNAVAILABLE");
}

#[tokio::test]
async fn cancel_publishes_to_the_named_stock() {
    let p = Arc::new(MemPublisher::default());
    let id = "0194d5a2-7b3c-7e11-9a44-2f1c6d8e0b99";
    let req = Request::delete(format!("/v1/orders/{id}?symbol=ACME"))
        .header("x-user-id", USER)
        .body(Body::empty())
        .unwrap();
    let (status, json) = call(&p, req).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(json["status"], "CANCEL_REQUESTED");
    let sent = p.sent.lock().unwrap();
    let (_, cmd) = decode_command(&sent[0].1).unwrap();
    assert!(matches!(cmd.body, CommandBody::CancelOrder(c) if c.order_id.to_string() == id));
}
