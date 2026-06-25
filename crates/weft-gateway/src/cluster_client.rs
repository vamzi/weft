//! A minimal Spark Connect gRPC **client**: routes a SQL query to a cluster's `weft spark server`
//! endpoint and decodes the Arrow-IPC response back into record batches.
//!
//! This is the data-plane hop the control/data-plane split is built around — the browser talks
//! REST to the gateway, and the gateway (the only Spark Connect client) forwards execution to the
//! chosen cluster's `sc://host:port` endpoint. When no cluster is selected the gateway runs the
//! query on its own embedded engine instead (see `run_sql`).

use std::io::Cursor;

use weft_loom::arrow::ipc::reader::StreamReader;
use weft_loom::arrow::record_batch::RecordBatch;
use weft_proto::spark::connect as sc;

use sc::spark_connect_service_client::SparkConnectServiceClient;

/// Match the server / Spark Connect max message size (large Arrow batches).
const MAX_MSG: usize = 256 * 1024 * 1024;

/// Run `sql` on the cluster at `endpoint` (an `sc://host:port` or `http://host:port` address) over
/// Spark Connect, returning the decoded result batches. Stops reading once `max_rows` have been
/// collected (dropping the stream cancels the RPC) so an unbounded `SELECT *` can't OOM the gateway.
/// Errors surface the gRPC/engine message.
pub async fn run_sql_on_cluster(
    endpoint: &str,
    sql: &str,
    max_rows: usize,
) -> Result<Vec<RecordBatch>, String> {
    let url = match endpoint.strip_prefix("sc://") {
        Some(host) => format!("http://{host}"),
        None => endpoint.to_string(),
    };
    let mut client = SparkConnectServiceClient::connect(url)
        .await
        .map_err(|e| format!("connect to cluster: {e}"))?
        .max_decoding_message_size(MAX_MSG)
        .max_encoding_message_size(MAX_MSG);

    let request = sc::ExecutePlanRequest {
        session_id: "00112233-4455-6677-8899-aabbccddeeff".to_string(),
        plan: Some(sc::Plan {
            op_type: Some(sc::plan::OpType::Root(sc::Relation {
                common: None,
                rel_type: Some(sc::relation::RelType::Sql(sc::Sql {
                    query: sql.to_string(),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    };

    let mut stream = client
        .execute_plan(request)
        .await
        .map_err(|e| e.message().to_string())?
        .into_inner();

    let mut out = Vec::new();
    let mut total = 0usize;
    while let Some(msg) = stream
        .message()
        .await
        .map_err(|e| e.message().to_string())?
    {
        if let Some(sc::execute_plan_response::ResponseType::ArrowBatch(b)) = msg.response_type {
            if b.data.is_empty() {
                continue;
            }
            let reader =
                StreamReader::try_new(Cursor::new(b.data), None).map_err(|e| e.to_string())?;
            for rb in reader {
                let rb = rb.map_err(|e| e.to_string())?;
                total += rb.num_rows();
                out.push(rb);
            }
            if total >= max_rows {
                break; // enough for the UI — dropping `stream` cancels the rest of the RPC
            }
        }
    }
    Ok(out)
}
