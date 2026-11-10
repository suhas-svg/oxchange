//! `MatchingService.GetBook` (spec: "The matcher answers between commands").
//!
//! The handler hands a [`BookQuery`] to the matcher thread and waits for the
//! reply on a blocking thread, so the async runtime never stalls.

use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::time::Duration;

use domain::Symbol;
use proto_gen::matching::v1 as pb;
use tonic::{Request, Response, Status};

use crate::pipeline::{BookQuery, BookView};

/// How long a query may wait for the matcher.
pub const REPLY_TIMEOUT: Duration = Duration::from_secs(1);

pub struct Service {
    queries: SyncSender<BookQuery>,
}

impl Service {
    pub fn new(queries: SyncSender<BookQuery>) -> Self {
        Self { queries }
    }

    pub fn server(
        queries: SyncSender<BookQuery>,
    ) -> pb::matching_service_server::MatchingServiceServer<Self> {
        pb::matching_service_server::MatchingServiceServer::new(Self::new(queries))
    }
}

pub fn to_proto(symbol: &str, v: BookView) -> pb::GetBookResponse {
    let levels = |l: Vec<(i64, i64)>| {
        l.into_iter()
            .map(|(price_cents, quantity)| pb::PriceLevel {
                price_cents,
                quantity,
            })
            .collect()
    };
    pb::GetBookResponse {
        symbol: symbol.to_owned(),
        as_of_seq: v.as_of_seq,
        bids: levels(v.bids),
        asks: levels(v.asks),
    }
}

#[tonic::async_trait]
impl pb::matching_service_server::MatchingService for Service {
    async fn get_book(
        &self,
        req: Request<pb::GetBookRequest>,
    ) -> Result<Response<pb::GetBookResponse>, Status> {
        let name = req.into_inner().symbol;
        let symbol =
            Symbol::new(&name).map_err(|e| Status::invalid_argument(format!("symbol: {e}")))?;
        let (reply, answer) = sync_channel(1);
        match self.queries.try_send(BookQuery { symbol, reply }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                return Err(Status::unavailable("too many book queries waiting"));
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err(Status::unavailable("the matcher is not running"));
            }
        }
        let view = tokio::task::spawn_blocking(move || answer.recv_timeout(REPLY_TIMEOUT))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|_| Status::unavailable("the matcher did not answer in time"))?;
        Ok(Response::new(to_proto(&name, view)))
    }
}
