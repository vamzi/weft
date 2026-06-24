//! Distributed execution over Arrow Flight.
//!
//! A [`Worker`] is an Arrow Flight server. Its `do_get` ticket is one of three things
//! (see [`crate::shuffle::protocol`]):
//!
//! - a legacy raw-SQL string — run it and stream the result (the single-stage MVP);
//! - a [`StageTicket`] — run a stage. A *leaf* stage (no upstreams) runs its SQL on local
//!   data, hash-partitions the output into per-downstream buckets, caches them, and returns an
//!   empty stream; a *consumer* stage (with upstreams) pulls its bucket from every upstream,
//!   registers it as `shuffle_input`, runs its SQL, and streams the result back;
//! - a [`ShuffleReadTicket`] — stream one cached bucket of a prior stage's output.
//!
//! This is the two-stage `partial-agg → hash shuffle → final-agg` shape; the driver in
//! [`crate::driver`] orchestrates it.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

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
use weft_loom::arrow::datatypes::Schema;
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

use crate::shuffle::protocol::{self, ShuffleReadTicket, StageTicket};
use crate::shuffle::{hash_partition, SHUFFLE_INPUT_TABLE};

/// Per-stage cached output, partitioned into buckets (one per downstream worker).
type StageCache = Arc<Mutex<HashMap<u32, Vec<Vec<RecordBatch>>>>>;

/// A Flight worker that runs stages on its local engine and serves shuffle buckets.
pub struct Worker {
    engine: Arc<Engine>,
    stage_outputs: StageCache,
}

impl Worker {
    /// Wrap an engine as a worker.
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            stage_outputs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

type FlightStream<T> =
    Pin<Box<dyn futures::Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

fn unimpl<T>(what: &str) -> std::result::Result<Response<T>, Status> {
    Err(Status::unimplemented(format!(
        "flight {what} not implemented"
    )))
}

/// Build a Flight `do_get` response stream from a set of record batches.
fn batches_to_stream(batches: Vec<RecordBatch>) -> FlightStream<FlightData> {
    let schema = match batches.first() {
        Some(b) => b.schema(),
        None => Arc::new(Schema::empty()),
    };
    let input = futures::stream::iter(batches.into_iter().map(Ok::<_, FlightError>));
    FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(input)
        .map_err(|e| Status::internal(e.to_string()))
        .boxed()
}

impl Worker {
    /// Run a [`StageTicket`]: leaf stages partition + cache + return empty; consumer stages
    /// pull their input bucket from upstreams, register it, run, and return the result.
    async fn run_stage(&self, t: StageTicket) -> std::result::Result<Vec<RecordBatch>, Status> {
        if t.upstream_endpoints.is_empty() {
            // Leaf (producer) stage: run on local data, hash-partition, cache for downstreams.
            let batches = self
                .engine
                .sql(&t.stage_sql)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            let key_cols: Vec<usize> = t.hash_key_cols.iter().map(|&c| c as usize).collect();
            let buckets = hash_partition(&batches, &key_cols, t.num_partitions as usize)
                .map_err(|e| Status::internal(e.to_string()))?;
            self.stage_outputs
                .lock()
                .expect("stage cache poisoned")
                .insert(t.stage_id, buckets);
            Ok(Vec::new())
        } else {
            // Consumer stage: pull this worker's bucket from every upstream's prior stage.
            let upstream_stage = t.stage_id.saturating_sub(1);
            let mut input = Vec::new();
            for ep in &t.upstream_endpoints {
                let part = pull_bucket(ep.clone(), upstream_stage, t.partition_id)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                input.extend(part);
            }
            self.engine
                .register_batches(SHUFFLE_INPUT_TABLE, input)
                .map_err(|e| Status::internal(e.to_string()))?;
            self.engine
                .sql(&t.stage_sql)
                .await
                .map_err(|e| Status::internal(e.to_string()))
        }
    }

    /// Serve one cached shuffle bucket (or an empty result if absent).
    fn read_shuffle(&self, r: ShuffleReadTicket) -> Vec<RecordBatch> {
        self.stage_outputs
            .lock()
            .expect("stage cache poisoned")
            .get(&r.stage_id)
            .and_then(|buckets| buckets.get(r.target_partition as usize).cloned())
            .unwrap_or_default()
    }
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

    /// Dispatch on the ticket kind: legacy SQL, a stage, or a shuffle-bucket read.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let bytes = request.into_inner().ticket.to_vec();
        let ticket = protocol::decode_ticket(&bytes)
            .map_err(|e| Status::invalid_argument(format!("decode ticket: {e}")))?;
        let batches = match ticket {
            protocol::Ticket::Sql(sql) => self
                .engine
                .sql(&sql)
                .await
                .map_err(|e| Status::internal(e.to_string()))?,
            protocol::Ticket::Stage(t) => self.run_stage(t).await?,
            protocol::Ticket::ShuffleRead(r) => self.read_shuffle(r),
        };
        Ok(Response::new(batches_to_stream(batches)))
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
    /// The streaming shuffle exchange — left as a documented stub. The MVP uses the simpler
    /// pull-based `do_get(ShuffleReadTicket)` path instead of a `do_exchange` handshake.
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

/// Connect to a worker and run one `do_get` with the given ticket bytes, decoding the Flight
/// stream into record batches. The shared transport for every driver→worker call.
async fn do_get_batches(endpoint: String, ticket_bytes: Vec<u8>) -> Result<Vec<RecordBatch>> {
    // Build the channel via tonic directly (arrow-flight's generated client has no `connect`).
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .map_err(|e| Error::Io(format!("endpoint: {e}")))?
        .connect()
        .await
        .map_err(|e| Error::Io(format!("connect worker: {e}")))?;
    let mut client = FlightServiceClient::new(channel);
    let ticket = Ticket {
        ticket: ticket_bytes.into(),
    };
    let stream = client
        .do_get(ticket)
        .await
        .map_err(|e| Error::Execution(format!("do_get: {}", e.message())))?
        .into_inner();

    let mut rb = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(|s| FlightError::Tonic(Box::new(s))),
    );
    let mut out = Vec::new();
    while let Some(batch) = rb.next().await {
        out.push(batch.map_err(|e| Error::Execution(format!("flight decode: {e}")))?);
    }
    Ok(out)
}

/// Driver: send raw `sql` to a worker over Flight and collect the result (single-stage path).
pub async fn query_worker(endpoint: String, sql: &str) -> Result<Vec<RecordBatch>> {
    do_get_batches(endpoint, sql.as_bytes().to_vec()).await
}

/// Driver: run a [`StageTicket`] on a worker and collect whatever it streams back.
pub async fn run_stage_on_worker(
    endpoint: String,
    ticket: StageTicket,
) -> Result<Vec<RecordBatch>> {
    do_get_batches(endpoint, ticket.to_ticket_bytes()).await
}

/// Pull one shuffle bucket (`target_partition`) of `stage_id` from a worker.
pub async fn pull_bucket(
    endpoint: String,
    stage_id: u32,
    target_partition: u32,
) -> Result<Vec<RecordBatch>> {
    let ticket = ShuffleReadTicket {
        stage_id,
        target_partition,
    };
    do_get_batches(endpoint, ticket.to_ticket_bytes()).await
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

    #[tokio::test]
    async fn stage_ticket_runs_as_leaf() {
        let port = 50562;
        let engine = Arc::new(Engine::new());
        tokio::spawn(async move {
            let _ = serve_worker(port, engine).await;
        });
        let endpoint = format!("http://127.0.0.1:{port}");
        // A leaf stage caches and returns empty; assert it doesn't error and returns 0 rows.
        let ticket = StageTicket {
            stage_id: 0,
            partition_id: 0,
            num_partitions: 1,
            upstream_endpoints: vec![],
            stage_sql: "SELECT 1 AS k, 2 AS v".into(),
            plan_fragment: vec![],
            hash_key_cols: vec![0],
        };
        let mut out = None;
        for _ in 0..50 {
            match run_stage_on_worker(endpoint.clone(), ticket.clone()).await {
                Ok(b) => {
                    out = Some(b);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        let out = out.expect("worker not ready");
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 0);

        // The cached bucket 0 should now be pullable and contain the row.
        let pulled = pull_bucket(endpoint, 0, 0).await.unwrap();
        assert_eq!(pulled.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }
}
