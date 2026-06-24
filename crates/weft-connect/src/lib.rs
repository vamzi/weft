//! `weft-connect` — the Spark Connect gRPC front-end.
//!
//! Implements `SparkConnectService` so an unmodified PySpark / Spark SQL client connects
//! via `sc://host:port`. Phase 0 wires the load-bearing slice end to end:
//! `ExecutePlan` for a SQL relation/command → DataFusion ([`weft_loom::Engine`]) → Arrow
//! IPC batches + a terminal `ResultComplete`. `AnalyzePlan(SparkVersion)` and `Config` are
//! handled enough for session bootstrap; the rest return `unimplemented` for now.
//!
//! Request path: gRPC `Plan` → SQL string → [`weft_loom`] → Arrow IPC back out.

use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use sc::spark_connect_service_server::{SparkConnectService, SparkConnectServiceServer};
use weft_common::{Error, Result};
use weft_loom::arrow::ipc::writer::StreamWriter;
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;
use weft_proto::spark::connect as sc;

/// Max gRPC message size (Spark Connect defaults to 128 MB; we allow 256 MB headroom).
const MAX_MSG: usize = 256 * 1024 * 1024;
/// Rows per Arrow result chunk, so a single gRPC message never carries an oversized batch.
const CHUNK_ROWS: usize = 8192;

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// TCP port. Sail uses 50051; Spark's own server defaults to 15002.
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { port: 50051 }
    }
}

/// The Spark Connect service implementation.
pub struct WeftService {
    engine: Arc<Engine>,
    /// Server-side session idempotency key (per server lifetime).
    server_session_id: String,
}

impl Default for WeftService {
    fn default() -> Self {
        Self::new()
    }
}

impl WeftService {
    /// Build a service with a fresh DataFusion-backed engine.
    pub fn new() -> Self {
        Self {
            engine: Arc::new(Engine::new()),
            server_session_id: Uuid::new_v4().to_string(),
        }
    }

    fn response(
        &self,
        session_id: &str,
        operation_id: &str,
        response_type: sc::execute_plan_response::ResponseType,
    ) -> sc::ExecutePlanResponse {
        sc::ExecutePlanResponse {
            session_id: session_id.to_string(),
            server_side_session_id: self.server_session_id.clone(),
            operation_id: operation_id.to_string(),
            response_id: Uuid::new_v4().to_string(),
            response_type: Some(response_type),
            ..Default::default()
        }
    }
}

type RespStream =
    Pin<Box<dyn Stream<Item = std::result::Result<sc::ExecutePlanResponse, Status>> + Send>>;

#[tonic::async_trait]
impl SparkConnectService for WeftService {
    type ExecutePlanStream = RespStream;
    type ReattachExecuteStream = RespStream;

    async fn execute_plan(
        &self,
        request: Request<sc::ExecutePlanRequest>,
    ) -> std::result::Result<Response<Self::ExecutePlanStream>, Status> {
        let req = request.into_inner();
        let session_id = req.session_id.clone();
        let operation_id = req
            .operation_id
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let sql = extract_sql(&req.plan)
            .ok_or_else(|| Status::unimplemented("Phase 0 supports only SQL relations/commands"))?;

        let batches = self.engine.sql(&sql).await.map_err(err_to_status)?;

        let mut responses = Vec::new();
        for batch in &batches {
            // Slice wide results into row-chunks so no single gRPC message is oversized
            // (Spark Connect's ArrowBatch chunking model).
            let n = batch.num_rows();
            let mut off = 0;
            while off < n {
                let len = (n - off).min(CHUNK_ROWS);
                let slice = batch.slice(off, len);
                let data = encode_ipc_stream(&slice)
                    .map_err(|e| Status::internal(format!("arrow ipc encode: {e}")))?;
                responses.push(self.response(
                    &session_id,
                    &operation_id,
                    sc::execute_plan_response::ResponseType::ArrowBatch(
                        sc::execute_plan_response::ArrowBatch {
                            row_count: len as i64,
                            data,
                            ..Default::default()
                        },
                    ),
                ));
                off += len;
            }
        }
        // Terminal marker — reattachable execution (PySpark 3.5+) expects it.
        responses.push(self.response(
            &session_id,
            &operation_id,
            sc::execute_plan_response::ResponseType::ResultComplete(
                sc::execute_plan_response::ResultComplete {},
            ),
        ));

        let stream = tokio_stream::iter(responses.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn analyze_plan(
        &self,
        request: Request<sc::AnalyzePlanRequest>,
    ) -> std::result::Result<Response<sc::AnalyzePlanResponse>, Status> {
        use sc::analyze_plan_request::Analyze;
        use sc::analyze_plan_response as out;

        let req = request.into_inner();
        let result = match req.analyze {
            Some(Analyze::SparkVersion(_)) => Some(out::Result::SparkVersion(out::SparkVersion {
                // Advertise the protocol version we vendored protos from.
                version: "4.0.0".to_string(),
            })),
            other => {
                return Err(Status::unimplemented(format!(
                    "AnalyzePlan variant not implemented in Phase 0: {other:?}"
                )))
            }
        };
        Ok(Response::new(sc::AnalyzePlanResponse {
            session_id: req.session_id,
            server_side_session_id: self.server_session_id.clone(),
            result,
        }))
    }

    async fn config(
        &self,
        request: Request<sc::ConfigRequest>,
    ) -> std::result::Result<Response<sc::ConfigResponse>, Status> {
        let req = request.into_inner();
        // Phase 0: accept everything, return no values. Enough for session bootstrap.
        Ok(Response::new(sc::ConfigResponse {
            session_id: req.session_id,
            server_side_session_id: self.server_session_id.clone(),
            ..Default::default()
        }))
    }

    async fn add_artifacts(
        &self,
        _request: Request<tonic::Streaming<sc::AddArtifactsRequest>>,
    ) -> std::result::Result<Response<sc::AddArtifactsResponse>, Status> {
        Err(Status::unimplemented(
            "AddArtifacts (Python UDFs) lands in Phase 1",
        ))
    }

    async fn artifact_status(
        &self,
        _request: Request<sc::ArtifactStatusesRequest>,
    ) -> std::result::Result<Response<sc::ArtifactStatusesResponse>, Status> {
        Ok(Response::new(sc::ArtifactStatusesResponse::default()))
    }

    async fn interrupt(
        &self,
        request: Request<sc::InterruptRequest>,
    ) -> std::result::Result<Response<sc::InterruptResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(sc::InterruptResponse {
            session_id: req.session_id,
            server_side_session_id: self.server_session_id.clone(),
            ..Default::default()
        }))
    }

    async fn reattach_execute(
        &self,
        _request: Request<sc::ReattachExecuteRequest>,
    ) -> std::result::Result<Response<Self::ReattachExecuteStream>, Status> {
        // Phase 0 buffers nothing; a reattach just reports completion.
        Err(Status::unimplemented(
            "ReattachExecute buffer not implemented in Phase 0",
        ))
    }

    async fn release_execute(
        &self,
        request: Request<sc::ReleaseExecuteRequest>,
    ) -> std::result::Result<Response<sc::ReleaseExecuteResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(sc::ReleaseExecuteResponse {
            session_id: req.session_id,
            server_side_session_id: self.server_session_id.clone(),
            operation_id: Some(req.operation_id),
        }))
    }

    async fn release_session(
        &self,
        request: Request<sc::ReleaseSessionRequest>,
    ) -> std::result::Result<Response<sc::ReleaseSessionResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(sc::ReleaseSessionResponse {
            session_id: req.session_id,
            server_side_session_id: self.server_session_id.clone(),
        }))
    }

    async fn fetch_error_details(
        &self,
        request: Request<sc::FetchErrorDetailsRequest>,
    ) -> std::result::Result<Response<sc::FetchErrorDetailsResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(sc::FetchErrorDetailsResponse {
            session_id: req.session_id,
            server_side_session_id: self.server_session_id.clone(),
            ..Default::default()
        }))
    }

    async fn clone_session(
        &self,
        _request: Request<sc::CloneSessionRequest>,
    ) -> std::result::Result<Response<sc::CloneSessionResponse>, Status> {
        Ok(Response::new(sc::CloneSessionResponse::default()))
    }

    async fn get_status(
        &self,
        _request: Request<sc::GetStatusRequest>,
    ) -> std::result::Result<Response<sc::GetStatusResponse>, Status> {
        Ok(Response::new(sc::GetStatusResponse::default()))
    }
}

/// Pull a SQL string out of a Spark Connect plan (either a `Sql` relation or a
/// `SqlCommand`). Returns `None` for any other plan shape.
fn extract_sql(plan: &Option<sc::Plan>) -> Option<String> {
    match plan.as_ref()?.op_type.as_ref()? {
        sc::plan::OpType::Root(rel) => match rel.rel_type.as_ref()? {
            sc::relation::RelType::Sql(sql) => Some(sql.query.clone()),
            _ => None,
        },
        sc::plan::OpType::Command(cmd) => match cmd.command_type.as_ref()? {
            #[allow(deprecated)]
            sc::command::CommandType::SqlCommand(c) if !c.sql.is_empty() => Some(c.sql.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// Encode one record batch as a self-contained Arrow IPC stream (schema + batch), which is
/// exactly what `ExecutePlanResponse.arrow_batch.data` carries.
fn encode_ipc_stream(
    batch: &RecordBatch,
) -> std::result::Result<Vec<u8>, weft_loom::arrow::error::ArrowError> {
    let mut buf = Vec::new();
    let schema = batch.schema();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

fn err_to_status(e: Error) -> Status {
    let msg = e.to_string();
    match e {
        Error::Plan(_) => Status::invalid_argument(msg),
        Error::Unsupported(_) => Status::unimplemented(msg),
        Error::Execution(_) | Error::Io(_) => Status::internal(msg),
    }
}

/// Start the Spark Connect server and serve until the process is killed.
pub async fn serve(config: ServerConfig) -> Result<()> {
    let addr = format!("0.0.0.0:{}", config.port)
        .parse()
        .map_err(|e| Error::Io(format!("bad listen addr: {e}")))?;
    let service = SparkConnectServiceServer::new(WeftService::new())
        .max_decoding_message_size(MAX_MSG)
        .max_encoding_message_size(MAX_MSG);
    tonic::transport::Server::builder()
        .add_service(service)
        .serve(addr)
        .await
        .map_err(|e| Error::Io(format!("server error: {e}")))?;
    Ok(())
}
