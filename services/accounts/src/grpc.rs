//! The `accounts.v1.AccountsService` gRPC server.
//!
//! Each error becomes a gRPC status whose message starts with the spec's
//! error code (for example `INSUFFICIENT_FUNDS: ...`), so the Gateway can map
//! it to the HTTP status in the API table.

use std::sync::Arc;

use proto_gen::accounts::v1 as pb;
use proto_gen::accounts::v1::accounts_service_server::{AccountsService, AccountsServiceServer};
use tonic::{Code, Request, Response, Status};

use crate::{Accounts, AccountsError};

pub fn status(e: &AccountsError) -> Status {
    let code = match e {
        AccountsError::Validation(_) => Code::InvalidArgument,
        AccountsError::InsufficientFunds | AccountsError::ReservationVoided => {
            Code::FailedPrecondition
        }
        AccountsError::NotFound(_) => Code::NotFound,
        AccountsError::AlreadyExists(_) | AccountsError::IdempotencyConflict => Code::AlreadyExists,
        AccountsError::Unauthenticated => Code::Unauthenticated,
        AccountsError::Invariant(_) => Code::Internal,
        AccountsError::Db(_) => Code::Unavailable,
    };
    match e {
        AccountsError::Invariant(_) => tracing::error!(error = %e, "invariant violated"),
        AccountsError::Db(_) => tracing::warn!(error = %e, "database error"),
        _ => {}
    }
    // Database details stay in the logs, not in responses.
    let message = match e {
        AccountsError::Db(_) => "database unavailable".to_owned(),
        other => other.to_string(),
    };
    Status::new(code, format!("{}: {message}", e.code()))
}

type Reply<T> = Result<Response<T>, Status>;

fn reply<T>(r: crate::Result<T>) -> Reply<T> {
    r.map(Response::new).map_err(|e| status(&e))
}

pub struct Service(pub Arc<Accounts>);

impl Service {
    pub fn server(accounts: Arc<Accounts>) -> AccountsServiceServer<Self> {
        AccountsServiceServer::new(Self(accounts))
    }
}

#[tonic::async_trait]
impl AccountsService for Service {
    async fn register(&self, r: Request<pb::RegisterRequest>) -> Reply<pb::RegisterResponse> {
        let r = r.into_inner();
        reply(self.0.register(&r.email, &r.password).await)
    }

    async fn login(&self, r: Request<pb::LoginRequest>) -> Reply<pb::LoginResponse> {
        let r = r.into_inner();
        reply(self.0.login(&r.email, &r.password).await)
    }

    async fn refresh(&self, r: Request<pb::RefreshRequest>) -> Reply<pb::RefreshResponse> {
        reply(self.0.refresh(&r.into_inner().refresh_token).await)
    }

    async fn logout(&self, r: Request<pb::LogoutRequest>) -> Reply<pb::LogoutResponse> {
        let r = r.into_inner();
        reply(self.0.logout(&r.user_id, &r.refresh_token).await)
    }

    async fn deposit(&self, r: Request<pb::DepositRequest>) -> Reply<pb::DepositResponse> {
        let r = r.into_inner();
        reply(
            self.0
                .deposit(&r.user_id, &r.idempotency_key, &r.asset, r.amount_cents)
                .await,
        )
    }

    async fn withdraw(&self, r: Request<pb::WithdrawRequest>) -> Reply<pb::WithdrawResponse> {
        let r = r.into_inner();
        reply(
            self.0
                .withdraw(&r.user_id, &r.idempotency_key, &r.asset, r.amount_cents)
                .await,
        )
    }

    async fn get_balances(
        &self,
        r: Request<pb::GetBalancesRequest>,
    ) -> Reply<pb::GetBalancesResponse> {
        reply(self.0.balances(&r.into_inner().user_id).await)
    }

    async fn list_ledger(
        &self,
        r: Request<pb::ListLedgerRequest>,
    ) -> Reply<pb::ListLedgerResponse> {
        let r = r.into_inner();
        reply(self.0.ledger(&r.user_id, &r.cursor, r.page_size).await)
    }

    async fn reserve(&self, r: Request<pb::ReserveRequest>) -> Reply<pb::ReserveResponse> {
        reply(self.0.reserve(&r.into_inner()).await)
    }

    async fn release(&self, r: Request<pb::ReleaseRequest>) -> Reply<pb::ReleaseResponse> {
        reply(self.0.release(&r.into_inner()).await)
    }

    async fn settle_trade(
        &self,
        r: Request<pb::SettleTradeRequest>,
    ) -> Reply<pb::SettleTradeResponse> {
        reply(self.0.settle_trade(&r.into_inner()).await)
    }
}
