//! The two Accounts calls Risk makes, behind a trait so tests can lose a
//! reply on purpose (spec failure test: "Lost reservation reply").

use std::future::Future;
use std::time::Duration;

use proto_gen::accounts::v1 as pb;
use proto_gen::accounts::v1::accounts_service_client::AccountsServiceClient;
use tonic::transport::Channel;
use tonic::{Code, Status};

/// Spec: "Risk to Accounts, Reserve: 500 ms; no automatic retry; a timeout
/// starts the undo path".
pub const RESERVE_TIMEOUT: Duration = Duration::from_millis(500);
/// Spec: "Risk to Accounts, Release: 500 ms; retried until it succeeds".
pub const RELEASE_TIMEOUT: Duration = Duration::from_millis(500);

pub trait AccountsApi: Send + Sync + 'static {
    fn reserve(
        &self,
        r: pb::ReserveRequest,
    ) -> impl Future<Output = Result<pb::ReserveResponse, Status>> + Send;
    fn release(
        &self,
        r: pb::ReleaseRequest,
    ) -> impl Future<Output = Result<pb::ReleaseResponse, Status>> + Send;
}

#[derive(Clone)]
pub struct GrpcAccounts {
    client: AccountsServiceClient<Channel>,
}

impl GrpcAccounts {
    /// Connects lazily, so Risk starts even while Accounts is down.
    pub fn connect_lazy(url: &str) -> Result<Self, String> {
        let channel = Channel::from_shared(url.to_owned())
            .map_err(|e| format!("bad Accounts URL {url:?}: {e}"))?
            .connect_timeout(Duration::from_secs(2))
            .connect_lazy();
        Ok(Self {
            client: AccountsServiceClient::new(channel),
        })
    }
}

async fn with_timeout<T>(
    limit: Duration,
    call: impl Future<Output = Result<tonic::Response<T>, Status>>,
) -> Result<T, Status> {
    match tokio::time::timeout(limit, call).await {
        Ok(r) => r.map(tonic::Response::into_inner),
        Err(_) => Err(Status::deadline_exceeded("accounts did not answer in time")),
    }
}

impl AccountsApi for GrpcAccounts {
    async fn reserve(&self, r: pb::ReserveRequest) -> Result<pb::ReserveResponse, Status> {
        let mut c = self.client.clone();
        with_timeout(RESERVE_TIMEOUT, c.reserve(telemetry::grpc_request(r))).await
    }

    async fn release(&self, r: pb::ReleaseRequest) -> Result<pb::ReleaseResponse, Status> {
        let mut c = self.client.clone();
        with_timeout(RELEASE_TIMEOUT, c.release(telemetry::grpc_request(r))).await
    }
}

/// The spec error code at the start of an Accounts status message.
pub(crate) fn error_code(s: &Status) -> &str {
    s.message().split(':').next().unwrap_or_default()
}

/// Whether the reservation's outcome is unknown: the request may or may not
/// have been applied, so the void path must run.
pub(crate) fn outcome_unknown(s: &Status) -> bool {
    matches!(
        s.code(),
        Code::DeadlineExceeded
            | Code::Unavailable
            | Code::Unknown
            | Code::Cancelled
            | Code::Aborted
            | Code::ResourceExhausted
            | Code::Internal
    )
}
