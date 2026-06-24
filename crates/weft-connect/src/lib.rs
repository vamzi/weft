//! `weft-connect` — the Spark Connect gRPC front-end.
//!
//! Implements `SparkConnectService` so an unmodified PySpark / Spark SQL client connects
//! via `sc://host:port`. `ExecutePlan` runs a SQL relation, a PySpark `SqlCommand`
//! (`spark.sql(...)` — a query returns a lazy `SqlCommandResult` relation handle; a DDL/DML
//! command runs eagerly and returns its result as a `LocalRelation`), or a `LocalRelation`
//! → DataFusion ([`weft_loom::Engine`]) → Arrow IPC + `ResultComplete`. `AnalyzePlan` answers
//! `SparkVersion` and `Schema` (with Arrow→Spark type conversion in [`types`]); `Config` is
//! handled enough for session bootstrap.
//!
//! Request path: gRPC `Plan` → SQL / relation → [`weft_loom`] → Arrow IPC back out.

// Every handler returns `Result<_, tonic::Status>` per the gRPC contract; `Status` is a large
// Err type, but it's fixed by the service trait, so boxing it buys nothing.
#![allow(clippy::result_large_err)]

use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use sc::spark_connect_service_server::{SparkConnectService, SparkConnectServiceServer};
use weft_common::{Error, Result};
use weft_loom::arrow::ipc::reader::StreamReader;
use weft_loom::arrow::ipc::writer::StreamWriter;
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;
use weft_proto::spark::connect as sc;

mod types;

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

    /// Stream `batches` back as chunked Arrow IPC `ArrowBatch` responses + a terminal
    /// `ResultComplete` (the shape PySpark 3.5+ reattachable execution expects).
    fn stream_batches(
        &self,
        session_id: &str,
        operation_id: &str,
        batches: &[RecordBatch],
    ) -> std::result::Result<Vec<sc::ExecutePlanResponse>, Status> {
        let mut responses = Vec::new();
        for batch in batches {
            // Slice wide results into row-chunks so no single gRPC message is oversized.
            let n = batch.num_rows();
            let mut off = 0;
            while off < n {
                let len = (n - off).min(CHUNK_ROWS);
                let slice = batch.slice(off, len);
                let data = encode_ipc_stream(&slice)
                    .map_err(|e| Status::internal(format!("arrow ipc encode: {e}")))?;
                responses.push(self.response(
                    session_id,
                    operation_id,
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
        responses.push(self.response(
            session_id,
            operation_id,
            sc::execute_plan_response::ResponseType::ResultComplete(
                sc::execute_plan_response::ResultComplete {},
            ),
        ));
        Ok(responses)
    }

    /// Handle a PySpark `SqlCommand`. A query stays lazy — we return a `SqlCommandResult` whose
    /// relation is the `Sql` plan, so the client's `DataFrame` re-executes it on `collect`/`show`
    /// (no large result embedded here). A command (DDL/DML) runs eagerly for its side effect and
    /// its result rows come back inline as a `LocalRelation`.
    async fn run_sql_command(
        &self,
        session_id: &str,
        operation_id: &str,
        sql: &str,
    ) -> std::result::Result<Vec<sc::ExecutePlanResponse>, Status> {
        let relation = if is_query(sql) {
            sql_relation(sql)
        } else {
            let batches = self.engine.sql(sql).await.map_err(err_to_status)?;
            let data = encode_ipc_multi(&batches)
                .map_err(|e| Status::internal(format!("arrow ipc encode: {e}")))?;
            sc::Relation {
                common: None,
                rel_type: Some(sc::relation::RelType::LocalRelation(sc::LocalRelation {
                    data: Some(data),
                    schema: None,
                })),
            }
        };
        Ok(vec![
            self.response(
                session_id,
                operation_id,
                sc::execute_plan_response::ResponseType::SqlCommandResult(
                    sc::execute_plan_response::SqlCommandResult {
                        relation: Some(relation),
                    },
                ),
            ),
            self.response(
                session_id,
                operation_id,
                sc::execute_plan_response::ResponseType::ResultComplete(
                    sc::execute_plan_response::ResultComplete {},
                ),
            ),
        ])
    }

    /// Resolve the result schema of a plan for `AnalyzePlan(Schema)`.
    async fn plan_schema(
        &self,
        plan: &Option<sc::Plan>,
    ) -> std::result::Result<weft_loom::arrow::datatypes::SchemaRef, Status> {
        match classify_exec(plan) {
            Some(Exec::Sql(sql)) | Some(Exec::SqlCommand(sql)) => {
                self.engine.schema(&sql).await.map_err(err_to_status)
            }
            Some(Exec::Local(data)) => {
                let reader = StreamReader::try_new(Cursor::new(data), None)
                    .map_err(|e| Status::internal(format!("decode local relation: {e}")))?;
                Ok(reader.schema())
            }
            None => Err(Status::unimplemented(
                "AnalyzePlan(Schema): unsupported plan shape",
            )),
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

        let exec = classify_exec(&req.plan).ok_or_else(|| {
            Status::unimplemented("unsupported plan; only SQL / SqlCommand / LocalRelation")
        })?;

        let responses = match exec {
            // PySpark `spark.sql(...)`: queries return a lazy relation handle; commands (DDL/DML)
            // run eagerly and return their result as a LocalRelation.
            Exec::SqlCommand(sql) => {
                self.run_sql_command(&session_id, &operation_id, &sql)
                    .await?
            }
            // Raw SQL relation/command (our Rust client + `.show()` re-execution): run + stream.
            Exec::Sql(sql) => {
                let batches = self.engine.sql(&sql).await.map_err(err_to_status)?;
                self.stream_batches(&session_id, &operation_id, &batches)?
            }
            // A cached LocalRelation (PySpark `.show()` over an eager command's result): echo it.
            Exec::Local(data) => {
                let batches = decode_ipc(&data)
                    .map_err(|e| Status::internal(format!("decode local relation: {e}")))?;
                self.stream_batches(&session_id, &operation_id, &batches)?
            }
        };

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
            // PySpark `df.schema` / `df.columns` / `printSchema()` — resolve the result schema
            // without executing and return it as a Spark struct `DataType`.
            Some(Analyze::Schema(s)) => {
                let schema = self.plan_schema(&s.plan).await?;
                Some(out::Result::Schema(out::Schema {
                    schema: Some(types::schema_to_spark(&schema)),
                }))
            }
            other => {
                return Err(Status::unimplemented(format!(
                    "AnalyzePlan variant not implemented: {other:?}"
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

/// How an `ExecutePlan` request should be run.
enum Exec {
    /// A PySpark `SqlCommand` — query stays lazy, command runs eagerly (see [`WeftService::run_sql_command`]).
    SqlCommand(String),
    /// A raw SQL relation/command — execute and stream the result.
    Sql(String),
    /// A `LocalRelation` carrying Arrow IPC — decode and stream it back.
    Local(Vec<u8>),
}

/// Classify a Spark Connect plan into how to run it. `None` for unsupported shapes.
fn classify_exec(plan: &Option<sc::Plan>) -> Option<Exec> {
    match plan.as_ref()?.op_type.as_ref()? {
        sc::plan::OpType::Root(rel) => match rel.rel_type.as_ref()? {
            sc::relation::RelType::Sql(sql) => Some(Exec::Sql(sql.query.clone())),
            sc::relation::RelType::LocalRelation(lr) => lr.data.clone().map(Exec::Local),
            _ => None,
        },
        sc::plan::OpType::Command(cmd) => match cmd.command_type.as_ref()? {
            sc::command::CommandType::SqlCommand(c) => sql_command_text(c).map(Exec::SqlCommand),
            _ => None,
        },
        _ => None,
    }
}

/// Pull the SQL text out of a `SqlCommand`: prefer the modern `input` relation (a `Sql` relation,
/// PySpark 4.x), fall back to the deprecated top-level `sql` string.
fn sql_command_text(c: &sc::SqlCommand) -> Option<String> {
    if let Some(input) = c.input.as_ref() {
        if let Some(sc::relation::RelType::Sql(sql)) = input.rel_type.as_ref() {
            if !sql.query.is_empty() {
                return Some(sql.query.clone());
            }
        }
    }
    #[allow(deprecated)]
    if !c.sql.is_empty() {
        return Some(c.sql.clone());
    }
    None
}

/// A bare `Sql` relation wrapping `query` (the lazy handle returned for a `SqlCommand` query).
fn sql_relation(query: &str) -> sc::Relation {
    sc::Relation {
        common: None,
        rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
            query: query.to_string(),
            ..Default::default()
        })),
    }
}

/// Does this SQL produce a result set (lazy), as opposed to running for a side effect (eager)?
/// First-keyword heuristic over the SQL surface Weft supports.
fn is_query(sql: &str) -> bool {
    let kw = sql
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    matches!(
        kw.as_str(),
        "SELECT" | "WITH" | "VALUES" | "TABLE" | "FROM" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN"
    )
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

/// Encode many record batches into one self-contained Arrow IPC stream (schema + batches), for a
/// `LocalRelation`. An empty result encodes just an empty schema.
fn encode_ipc_multi(
    batches: &[RecordBatch],
) -> std::result::Result<Vec<u8>, weft_loom::arrow::error::ArrowError> {
    use weft_loom::arrow::datatypes::Schema;
    let mut buf = Vec::new();
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(Schema::empty()));
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())?;
        for b in batches {
            writer.write(b)?;
        }
        writer.finish()?;
    }
    Ok(buf)
}

/// Decode an Arrow IPC stream (a `LocalRelation`'s `data`) back into record batches.
fn decode_ipc(
    data: &[u8],
) -> std::result::Result<Vec<RecordBatch>, weft_loom::arrow::error::ArrowError> {
    let reader = StreamReader::try_new(Cursor::new(data.to_vec()), None)?;
    reader.collect()
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
