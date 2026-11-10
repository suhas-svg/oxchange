//! The one Accounts call Settlement makes, behind a trait so tests can lose
//! replies or take Accounts down on purpose.

use std::future::Future;
use std::time::Duration;

use proto_gen::accounts::v1 as pb;
use proto_gen::accounts::v1::accounts_service_client::AccountsServiceClient;
use tonic::transport::Channel;
use tonic::{Code, Status};

/// Spec: "Settlement to Accounts, SettleTrade: 1 s; retried until it
/// succeeds or halts".
pub const SETTLE_TIMEOUT: Duration = Duration::from_secs(1);

pub trait AccountsApi: Send + Sync + 'static {
    fn settle_trade(
        &self,
        r: pb::SettleTradeRequest,
    ) -> impl Future<Output = Result<pb::SettleTradeResponse, Status>> + Send;
}

#[derive(Clone)]
pub struct GrpcAccounts {
    client: AccountsServiceClient<Channel>,
}

impl GrpcAccounts {
    /// Connects lazily, so Settlement starts even while Accounts is down.
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

impl AccountsApi for GrpcAccounts {
    async fn settle_trade(
        &self,
        r: pb::SettleTradeRequest,
    ) -> Result<pb::SettleTradeResponse, Status> {
        let mut c = self.client.clone();
        match tokio::time::timeout(SETTLE_TIMEOUT, c.settle_trade(r)).await {
            Ok(r) => r.map(tonic::Response::into_inner),
            Err(_) => Err(Status::deadline_exceeded("accounts did not answer in time")),
        }
    }
}

/// Whether the call may simply be repeated: Accounts was unreachable, slow,
/// or failed internally. Anything else is a definite refusal.
pub fn outcome_unknown(s: &Status) -> bool {
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
