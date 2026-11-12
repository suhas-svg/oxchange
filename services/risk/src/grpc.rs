//! The `risk.v1.RiskService` gRPC server. Error messages start with the
//! spec's error code, as in Accounts.

use std::sync::Arc;

use proto_gen::risk::v1 as pb;
use proto_gen::risk::v1::risk_service_server::{RiskService, RiskServiceServer};
use tonic::{Code, Request, Response, Status};

use crate::{AccountsApi, Risk, RiskError};

pub fn status(e: &RiskError) -> Status {
    let code = match e {
        RiskError::Validation(_) => Code::InvalidArgument,
        RiskError::SymbolNotTrading
        | RiskError::RiskLimit(_)
        | RiskError::InsufficientFunds
        | RiskError::Rejected(_) => Code::FailedPrecondition,
        RiskError::NotFound => Code::NotFound,
        RiskError::Unavailable(_) | RiskError::Db(_) => Code::Unavailable,
    };
    let message = match e {
        RiskError::Db(err) => {
            tracing::warn!(error = %err, "database error");
            "database unavailable".to_owned()
        }
        other => other.to_string(),
    };
    Status::new(code, format!("{}: {message}", e.code()))
}

type Reply<T> = Result<Response<T>, Status>;

fn reply<T>(r: crate::Result<T>) -> Reply<T> {
    r.map(Response::new).map_err(|e| status(&e))
}

/// The `result` label of `orders_received_total`.
fn order_result(r: &crate::Result<pb::PlaceOrderResponse>) -> &'static str {
    use crate::RiskError as E;
    match r {
        Ok(o) if o.existing => "repeat",
        Ok(_) => "accepted",
        Err(E::Validation(_) | E::SymbolNotTrading) => "invalid",
        Err(E::RiskLimit(_)) => "risk_limit",
        Err(E::InsufficientFunds) => "insufficient_funds",
        Err(E::Rejected(_)) => "rejected",
        Err(E::Unavailable(_)) => "unavailable",
        Err(_) => "error",
    }
}

pub struct Service<A>(pub Arc<Risk<A>>);

impl<A: AccountsApi> Service<A> {
    pub fn server(risk: Arc<Risk<A>>) -> RiskServiceServer<Self> {
        RiskServiceServer::new(Self(risk))
    }
}

#[tonic::async_trait]
impl<A: AccountsApi> RiskService for Service<A> {
    async fn place_order(
        &self,
        r: Request<pb::PlaceOrderRequest>,
    ) -> Reply<pb::PlaceOrderResponse> {
        let result = self.0.place_order(&r.into_inner()).await;
        metrics::counter!("orders_received_total", "result" => order_result(&result)).increment(1);
        reply(result)
    }

    async fn cancel_order(
        &self,
        r: Request<pb::CancelOrderRequest>,
    ) -> Reply<pb::CancelOrderResponse> {
        let r = r.into_inner();
        reply(self.0.cancel_order(&r.user_id, &r.order_id).await)
    }

    async fn get_order(&self, r: Request<pb::GetOrderRequest>) -> Reply<pb::GetOrderResponse> {
        let r = r.into_inner();
        reply(self.0.get_order(&r.user_id, &r.order_id).await)
    }

    async fn list_orders(
        &self,
        r: Request<pb::ListOrdersRequest>,
    ) -> Reply<pb::ListOrdersResponse> {
        reply(self.0.list_orders(&r.into_inner()).await)
    }

    async fn list_symbols(
        &self,
        _r: Request<pb::ListSymbolsRequest>,
    ) -> Reply<pb::ListSymbolsResponse> {
        reply(self.0.list_symbols().await)
    }
}
