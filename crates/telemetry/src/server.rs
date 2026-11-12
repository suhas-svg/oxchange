//! Request spans and the standard request metrics for HTTP (axum) and gRPC
//! (tonic) servers: one tower layer for both.
//!
//! | Metric | Labels |
//! |---|---|
//! | `http_requests_total` | `route`, `method`, `status` |
//! | `http_request_duration_seconds` | `route`, `method` |
//! | `grpc_requests_total` | `method`, `code` |
//! | `grpc_request_duration_seconds` | `method` |
//!
//! Health, readiness and WebSocket upgrades are left out: probes would
//! drown the real traffic, and a socket's "duration" is how long it was open.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use tracing::Instrument;
use tracing::field::Empty;

/// Which kind of server the layer wraps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Http,
    Grpc,
}

/// See the module docs.
#[derive(Debug, Clone, Copy)]
pub struct ServerLayer {
    kind: Kind,
    follow_callers: bool,
}

impl ServerLayer {
    /// For a gRPC server: continues the caller's trace.
    pub fn grpc() -> Self {
        Self {
            kind: Kind::Grpc,
            follow_callers: true,
        }
    }

    /// For an internal HTTP server: continues the caller's trace.
    pub fn http() -> Self {
        Self {
            kind: Kind::Http,
            follow_callers: true,
        }
    }

    /// For the public edge (the Gateway): every request starts a new trace,
    /// whatever `traceparent` the client sent.
    pub fn edge() -> Self {
        Self {
            kind: Kind::Http,
            follow_callers: false,
        }
    }
}

impl<S> tower::Layer<S> for ServerLayer {
    type Service = ServerService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        ServerService {
            inner,
            layer: *self,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerService<S> {
    inner: S,
    layer: ServerLayer,
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

impl<S, B, RB> tower::Service<http::Request<B>> for ServerService<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<RB>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        // The clone that was not polled ready stays behind (tower's rule).
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let path = req.uri().path();
        if is_untracked(path, req.headers()) {
            return Box::pin(inner.call(req));
        }
        let kind = self.layer.kind;
        let method = req.method().as_str().to_owned();
        let name = match kind {
            Kind::Grpc => path.to_owned(),
            Kind::Http => route(&req),
        };
        let span = tracing::info_span!(
            "request",
            otel.name = %match kind {
                Kind::Grpc => name.clone(),
                Kind::Http => format!("{method} {name}"),
            },
            otel.kind = "server",
            otel.status_code = Empty,
            trace_id = Empty,
            status = Empty,
        );
        let parent = if self.layer.follow_callers {
            req.headers()
                .get("traceparent")
                .and_then(|v| v.to_str().ok())
        } else {
            None
        };
        crate::follow(&span, parent);

        let started = Instant::now();
        let fut = {
            let _entered = span.enter();
            inner.call(req)
        };
        Box::pin(
            async move {
                let result = fut.await;
                let elapsed = started.elapsed().as_secs_f64();
                let span = tracing::Span::current();
                match (&result, kind) {
                    (Ok(res), Kind::Http) => {
                        let status = res.status().as_u16();
                        span.record("status", status);
                        if status >= 500 {
                            span.record("otel.status_code", "error");
                        }
                        metrics::counter!("http_requests_total",
                            "route" => name.clone(), "method" => method.clone(),
                            "status" => status.to_string())
                        .increment(1);
                        metrics::histogram!("http_request_duration_seconds",
                            "route" => name, "method" => method)
                        .record(elapsed);
                    }
                    (Ok(res), Kind::Grpc) => {
                        // An error is a trailers-only response, so its status
                        // is in the headers; a success's is in the trailers.
                        let code = res
                            .headers()
                            .get("grpc-status")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u8>().ok())
                            .unwrap_or(0);
                        span.record("status", code);
                        if is_server_fault(code) {
                            span.record("otel.status_code", "error");
                        }
                        metrics::counter!("grpc_requests_total",
                            "method" => name.clone(), "code" => grpc_code_name(code))
                        .increment(1);
                        metrics::histogram!("grpc_request_duration_seconds", "method" => name)
                            .record(elapsed);
                    }
                    (Err(_), _) => {
                        span.record("otel.status_code", "error");
                        let (counter, label) = match kind {
                            Kind::Http => ("http_requests_total", "route"),
                            Kind::Grpc => ("grpc_requests_total", "method"),
                        };
                        metrics::counter!(counter, label => name, "code" => "transport_error")
                            .increment(1);
                    }
                }
                result
            }
            .instrument(span),
        )
    }
}

fn is_untracked(path: &str, headers: &http::HeaderMap) -> bool {
    matches!(path, "/healthz" | "/readyz" | "/metrics")
        || headers.contains_key(http::header::UPGRADE)
}

/// The route template (`/v1/orders/{id}`), so IDs do not become labels.
fn route<B>(req: &http::Request<B>) -> String {
    #[cfg(feature = "axum")]
    if let Some(p) = req.extensions().get::<axum::extract::MatchedPath>() {
        return p.as_str().to_owned();
    }
    let _ = req;
    "unmatched".to_owned()
}

/// Codes that mean the server failed, not that the request was refused.
fn is_server_fault(code: u8) -> bool {
    // Unknown, DeadlineExceeded, Unimplemented, Internal, Unavailable, DataLoss.
    matches!(code, 2 | 4 | 12 | 13 | 14 | 15)
}

fn grpc_code_name(code: u8) -> &'static str {
    match code {
        0 => "OK",
        1 => "CANCELLED",
        2 => "UNKNOWN",
        3 => "INVALID_ARGUMENT",
        4 => "DEADLINE_EXCEEDED",
        5 => "NOT_FOUND",
        6 => "ALREADY_EXISTS",
        7 => "PERMISSION_DENIED",
        8 => "RESOURCE_EXHAUSTED",
        9 => "FAILED_PRECONDITION",
        10 => "ABORTED",
        11 => "OUT_OF_RANGE",
        12 => "UNIMPLEMENTED",
        13 => "INTERNAL",
        14 => "UNAVAILABLE",
        15 => "DATA_LOSS",
        16 => "UNAUTHENTICATED",
        _ => "OTHER",
    }
}

/// Counts sqlx's "slow statement" warnings as `db_slow_queries_total`.
pub(crate) struct SlowQueryCounter;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SlowQueryCounter {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let m = event.metadata();
        if m.target() == "sqlx::query" && *m.level() == tracing::Level::WARN {
            metrics::counter!("db_slow_queries_total").increment(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::{Layer, Service, ServiceExt};

    #[tokio::test]
    async fn passes_requests_through_and_reads_grpc_status() {
        let svc = tower::service_fn(|_req: http::Request<()>| async {
            Ok::<_, std::convert::Infallible>(
                http::Response::builder()
                    .header("grpc-status", "14")
                    .body(())
                    .unwrap(),
            )
        });
        let mut svc = ServerLayer::grpc().layer(svc);
        let req = http::Request::builder()
            .uri("/risk.v1.RiskService/PlaceOrder")
            .header(
                "traceparent",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .body(())
            .unwrap();
        let res = svc.ready().await.unwrap().call(req).await.unwrap();
        assert_eq!(res.headers()["grpc-status"], "14");
        assert!(is_server_fault(14));
        assert!(!is_server_fault(9));
        assert_eq!(grpc_code_name(14), "UNAVAILABLE");
    }

    #[test]
    fn probes_and_sockets_are_not_tracked() {
        let mut h = http::HeaderMap::new();
        assert!(is_untracked("/healthz", &h));
        assert!(!is_untracked("/v1/orders", &h));
        h.insert(http::header::UPGRADE, "websocket".parse().unwrap());
        assert!(is_untracked("/ws/public", &h));
    }
}
