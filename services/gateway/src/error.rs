//! The API's error body (spec: "External API → Errors"):
//!
//! ```json
//! { "error": { "code": "INSUFFICIENT_FUNDS", "message": "...", "trace_id": "4bf9..." } }
//! ```
//!
//! Every response carries its trace ID in `X-Trace-Id`; errors repeat it in
//! the body. The ID is set per request by [`crate::trace`] and read here, so
//! handlers never pass it around.

use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use tonic::Code;

tokio::task_local! {
    /// The current request's trace ID.
    pub static TRACE_ID: String;
}

pub fn trace_id() -> String {
    TRACE_ID.try_with(Clone::clone).unwrap_or_default()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: String,
    pub message: String,
    /// Seconds, for `Retry-After` on 429.
    pub retry_after: Option<u64>,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.to_owned(),
            message: message.into(),
            retry_after: None,
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "VALIDATION_FAILED", message)
    }

    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "UNAUTHENTICATED", message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "SERVICE_UNAVAILABLE",
            message,
        )
    }

    pub fn rate_limited(retry_after_secs: u64) -> Self {
        Self {
            retry_after: Some(retry_after_secs.max(1)),
            ..Self::new(
                StatusCode::TOO_MANY_REQUESTS,
                "RATE_LIMITED",
                "too many requests; wait for the Retry-After time",
            )
        }
    }

    /// Turns a service's gRPC error into the API error. Services start their
    /// messages with the spec's error code (`"INSUFFICIENT_FUNDS: ..."`).
    pub fn from_status(s: &tonic::Status) -> Self {
        let (code, message) = match s.message().split_once(": ") {
            Some((c, m))
                if !c.is_empty() && c.bytes().all(|b| b.is_ascii_uppercase() || b == b'_') =>
            {
                (c, m)
            }
            _ => ("", s.message()),
        };
        let status = match code {
            "VALIDATION_FAILED" => StatusCode::BAD_REQUEST,
            "UNAUTHENTICATED" => StatusCode::UNAUTHORIZED,
            "FORBIDDEN" => StatusCode::FORBIDDEN,
            "NOT_FOUND" => StatusCode::NOT_FOUND,
            "IDEMPOTENCY_CONFLICT" | "ALREADY_EXISTS" => StatusCode::CONFLICT,
            "INSUFFICIENT_FUNDS"
            | "RISK_LIMIT_EXCEEDED"
            | "SYMBOL_NOT_TRADING"
            | "ORDER_REJECTED" => StatusCode::UNPROCESSABLE_ENTITY,
            "SERVICE_UNAVAILABLE" => StatusCode::SERVICE_UNAVAILABLE,
            _ => match s.code() {
                Code::InvalidArgument | Code::OutOfRange => StatusCode::BAD_REQUEST,
                Code::Unauthenticated => StatusCode::UNAUTHORIZED,
                Code::PermissionDenied => StatusCode::FORBIDDEN,
                Code::NotFound => StatusCode::NOT_FOUND,
                Code::AlreadyExists => StatusCode::CONFLICT,
                Code::FailedPrecondition => StatusCode::UNPROCESSABLE_ENTITY,
                Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled => {
                    StatusCode::SERVICE_UNAVAILABLE
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        };
        match status {
            StatusCode::SERVICE_UNAVAILABLE => {
                tracing::warn!(grpc = ?s.code(), error = %s.message(), "upstream unavailable");
                Self::unavailable("the service is unavailable; retry with the same request")
            }
            StatusCode::INTERNAL_SERVER_ERROR => {
                tracing::error!(grpc = ?s.code(), error = %s.message(), "upstream failed");
                Self::new(status, "INTERNAL", "something went wrong; retry later")
            }
            _ => {
                let code = if code.is_empty() {
                    match status {
                        StatusCode::BAD_REQUEST => "VALIDATION_FAILED",
                        StatusCode::UNAUTHORIZED => "UNAUTHENTICATED",
                        StatusCode::FORBIDDEN => "FORBIDDEN",
                        StatusCode::NOT_FOUND => "NOT_FOUND",
                        StatusCode::CONFLICT => "CONFLICT",
                        _ => "REJECTED",
                    }
                } else {
                    code
                };
                Self::new(status, code, message)
            }
        }
    }
}

impl From<tonic::Status> for ApiError {
    fn from(s: tonic::Status) -> Self {
        Self::from_status(&s)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({
            "error": { "code": self.code, "message": self.message, "trace_id": trace_id() }
        });
        let mut r = (self.status, Json(body)).into_response();
        if let Some(secs) = self.retry_after {
            r.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(secs));
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_codes_map_to_the_spec_statuses() {
        let e = ApiError::from_status(&tonic::Status::failed_precondition(
            "INSUFFICIENT_FUNDS: available balance is too low",
        ));
        assert_eq!(
            (e.status, e.code.as_str(), e.message.as_str()),
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                "INSUFFICIENT_FUNDS",
                "available balance is too low"
            )
        );
        let e = ApiError::from_status(&tonic::Status::already_exists(
            "ALREADY_EXISTS: that email is registered",
        ));
        assert_eq!(e.status, StatusCode::CONFLICT);
        // Internal details never reach the client.
        let e = ApiError::from_status(&tonic::Status::internal("INVARIANT_VIOLATED: ledger off"));
        assert_eq!(
            (e.status, e.code.as_str()),
            (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL")
        );
        assert!(!e.message.contains("ledger"));
        let e = ApiError::from_status(&tonic::Status::unavailable("connection refused"));
        assert_eq!(e.code, "SERVICE_UNAVAILABLE");
        // A message without a code still gets one.
        let e = ApiError::from_status(&tonic::Status::not_found("no such thing"));
        assert_eq!(
            (e.code.as_str(), e.message.as_str()),
            ("NOT_FOUND", "no such thing")
        );
    }
}
