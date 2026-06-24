//! Distributed execution over Arrow Flight (MVP, single-stage).
//!
//! A [`Worker`] is an Arrow Flight server that executes a query carried in the `do_get` ticket
//! on its local [`weft_loom::Engine`] and streams the Arrow result back. [`query_worker`] is the
//! driver side: connect, send the ticket, decode the Flight stream into record batches.
//!
//! This proves the driver/worker topology + the Flight data plane on a single stage. Shipping
//! serialized plan *fragments* (via `datafusion-proto`) and a shuffle stage between workers is
//! the next increment.

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};
use weft_common::{Error, Result};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

/// A Flight worker that runs queries on its local engine.
pub struct Worker {
    engine: Arc<Engine>,
}

impl Worker {
    /// Wrap an engine as a worker.
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
}

type FlightStream<T> =
    Pin<Box<dyn futures::Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

fn unimpl<T>(what: &str) -> std::result::Result<Response<T>, Status> {
    Err(Status::unimplemented(format!(
        "flight {what} not implemented"
    )))
}

#[tonic::async_trait]
impl FlightService for Worker {
    type HandshakeStream = FlightStream<HandshakeResponse>;
    type ListFlightsStream = FlightStream<FlightInfo>;
    type DoGetStream = FlightStream<FlightData>;
    type DoPutStream = FlightStream<PutResult>;
    type DoActionStream = FlightStream<arrow_flight::Result>;
    type ListActionsStream = FlightStream<ActionType>;
    type DoExchangeStream = FlightStream<FlightData>;

    /// Execute the SQL carried in the ticket and stream the Arrow result back.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let sql = String::from_utf8(request.into_inner().ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("ticket utf8: {e}")))?;
        let batches = self
            .engine
            .sql(&sql)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => Arc::new(weft_loom::arrow::datatypes::Schema::empty()),
        };
        let input = futures::stream::iter(batches.into_iter().map(Ok::<_, FlightError>));
        let flight = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(input)
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(flight.boxed()))
    }

    async fn handshake(
        &self,
        _r: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        unimpl("handshake")
    }
    async fn list_flights(
        &self,
        _r: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        unimpl("list_flights")
    }
    async fn get_flight_info(
        &self,
        _r: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        unimpl("get_flight_info")
    }
    async fn poll_flight_info(
        &self,
        _r: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        unimpl("poll_flight_info")
    }
    async fn get_schema(
        &self,
        _r: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        unimpl("get_schema")
    }
    async fn do_put(
        &self,
        _r: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        unimpl("do_put")
    }
    async fn do_action(
        &self,
        _r: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        unimpl("do_action")
    }
    async fn list_actions(
        &self,
        _r: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        unimpl("list_actions")
    }
    async fn do_exchange(
        &self,
        _r: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        unimpl("do_exchange")
    }
}

/// Serve a worker on `0.0.0.0:port` until the process exits.
pub async fn serve_worker(port: u16, engine: Arc<Engine>) -> Result<()> {
    let addr = format!("0.0.0.0:{port}")
        .parse()
        .map_err(|e| Error::Io(format!("bad worker addr: {e}")))?;
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(Worker::new(engine)))
        .serve(addr)
        .await
        .map_err(|e| Error::Io(format!("worker serve: {e}")))?;
    Ok(())
}

/// Driver: send `sql` to a worker over Flight and collect the Arrow result.
pub async fn query_worker(endpoint: String, sql: &str) -> Result<Vec<RecordBatch>> {
    // Build the channel via tonic directly (arrow-flight's generated client has no `connect`).
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .map_err(|e| Error::Io(format!("endpoint: {e}")))?
        .connect()
        .await
        .map_err(|e| Error::Io(format!("connect worker: {e}")))?;
    let mut client = FlightServiceClient::new(channel);
    let ticket = Ticket {
        ticket: sql.as_bytes().to_vec().into(),
    };
    let stream = client
        .do_get(ticket)
        .await
        .map_err(|e| Error::Execution(format!("do_get: {}", e.message())))?
        .into_inner();

    // Decode the Flight stream back into record batches.
    let mut rb = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(|s| FlightError::Tonic(Box::new(s))),
    );
    let mut out = Vec::new();
    while let Some(batch) = rb.next().await {
        out.push(batch.map_err(|e| Error::Execution(format!("flight decode: {e}")))?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_loom::arrow::array::Int64Array;

    #[tokio::test]
    async fn distributed_single_stage_roundtrip() {
        let port = 50561;
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
        // Retry until the worker is up and the distributed query returns.
        let endpoint = format!("http://127.0.0.1:{port}");
        let mut batches = None;
        for _ in 0..50 {
            match query_worker(endpoint.clone(), "SELECT 21 + 21 AS answer").await {
                Ok(b) => {
                    batches = Some(b);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        let batches = batches.expect("worker did not become ready / query failed");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
        let v = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64")
            .value(0);
        assert_eq!(v, 42);
    }
}
