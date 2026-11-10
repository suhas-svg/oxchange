//! Forwards REST calls to Market Data and Notifications.
//!
//! Only the method, the path and query, `Content-Type` and the body go
//! through. Everything else from the client, including any `X-User-Id` it
//! sends, is dropped; the Gateway sets `X-User-Id` itself from the verified
//! token.

use axum::body::Body;
use axum::http::{HeaderValue, Method, Request, header};
use axum::response::Response;
use bytes::Bytes;
use http_body_util::Full;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use uuid::Uuid;

use crate::error::{ApiError, trace_id};

pub type Client = hyper_util::client::legacy::Client<HttpConnector, Full<Bytes>>;

pub fn client() -> Client {
    let mut connector = HttpConnector::new();
    connector.set_connect_timeout(Some(crate::UPSTREAM_TIMEOUT));
    hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(connector)
}

/// Sends one request to `base` + `path_and_query` and returns the answer as
/// it came.
pub async fn forward(
    http: &Client,
    base: &str,
    method: Method,
    path_and_query: &str,
    user: Option<Uuid>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let mut req = Request::builder()
        .method(method)
        .uri(format!("{base}{path_and_query}"));
    if let Some(u) = user {
        req = req.header("x-user-id", u.to_string());
    }
    if !body.is_empty() {
        req = req.header(header::CONTENT_TYPE, "application/json");
    }
    let trace = trace_id();
    if !trace.is_empty() {
        req = req.header("x-trace-id", trace);
    }
    let req = req
        .body(Full::new(body))
        .map_err(|e| ApiError::invalid(format!("bad request: {e}")))?;
    let resp = match tokio::time::timeout(crate::UPSTREAM_TIMEOUT, http.request(req)).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            tracing::warn!(error = %e, %base, "upstream request failed");
            return Err(ApiError::unavailable("the service is unavailable; retry"));
        }
        Err(_) => {
            return Err(ApiError::unavailable(
                "the service did not answer in time; retry",
            ));
        }
    };
    let (parts, incoming) = resp.into_parts();
    let mut out = Response::new(Body::new(incoming));
    *out.status_mut() = parts.status;
    if let Some(ct) = parts.headers.get(header::CONTENT_TYPE) {
        out.headers_mut().insert(header::CONTENT_TYPE, ct.clone());
    }
    // Market data may be cached briefly by the browser; private data never.
    let cache = if user.is_some() {
        "no-store"
    } else {
        "no-cache"
    };
    out.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    Ok(out)
}
