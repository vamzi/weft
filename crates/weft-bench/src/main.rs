//! `weft-bench` — benchmark harness for ClickBench coverage.
//!
//! Two modes, both on a **synthetic `hits` table** (the real 14 GB run happens on a
//! `c6a.4xlarge`; these are dev/CI coverage, timings NOT comparable to Sail's absolutes):
//!
//! - `clickbench`      — engine-direct: runs the 43 queries straight through
//!   [`weft_loom::Engine`] (DataFusion) over an in-memory table. Fast; the CI coverage gate.
//! - `clickbench-grpc` — live-server: writes synthetic `hits.parquet`, boots the real
//!   `weft-connect` Spark Connect server, and drives `CREATE EXTERNAL TABLE` + the 43 queries
//!   **over gRPC** (Arrow IPC round-trip) via the generated client. Exercises the full
//!   production transport — the same path the official PySpark harness uses.
//!
//! Usage: `cargo run -p weft-bench [--release] -- {clickbench|clickbench-grpc} [--rows N]`

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{
    ArrayRef, Date32Array, Int16Array, Int32Array, Int64Array, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::parquet::arrow::ArrowWriter;
use sc::spark_connect_service_client::SparkConnectServiceClient;
use tonic::transport::Channel;
use weft_connect::{serve, ServerConfig};
use weft_loom::Engine;
use weft_proto::spark::connect as sc;

mod tpch;

const HITS_SCHEMA_TSV: &str = include_str!("../../../bench/clickbench/hits_schema.tsv");
const CLICKBENCH_QUERIES: &str = include_str!("../../../bench/clickbench/queries.sql");

/// Days since the Unix epoch for 2013-07-01 (the ClickBench filter window in Q37–Q40).
const JULY_2013: i32 = 15887;
const DEFAULT_ROWS: usize = 50_000;
const GRPC_PORT: u16 = 50552;

#[derive(Clone, Copy)]
enum Kind {
    I16,
    I32,
    I64,
    Str,
    Date,
    /// EventTime & friends are epoch seconds (the DataFusion queries call
    /// `to_timestamp_seconds` on them), so we store them as i64.
    Ts,
}

fn parse_kind(s: &str) -> Kind {
    match s.trim() {
        "i16" => Kind::I16,
        "i32" => Kind::I32,
        "i64" | "f64" => Kind::I64,
        "date" => Kind::Date,
        "ts" => Kind::Ts,
        _ => Kind::Str,
    }
}

fn arrow_type(k: Kind) -> DataType {
    match k {
        Kind::I16 => DataType::Int16,
        Kind::I32 => DataType::Int32,
        Kind::I64 | Kind::Ts => DataType::Int64,
        Kind::Date => DataType::Date32,
        Kind::Str => DataType::Utf8,
    }
}

fn columns() -> Vec<(String, Kind)> {
    HITS_SCHEMA_TSV
        .lines()
        .filter_map(|l| {
            let mut it = l.split('\t');
            let name = it.next()?.trim();
            let kind = it.next()?;
            if name.is_empty() {
                return None;
            }
            Some((name.to_string(), parse_kind(kind)))
        })
        .collect()
}

/// Deterministic synthetic value generation, with a few name-aware tweaks so that
/// filters/LIKE/GROUP BY in the queries actually match some rows.
fn gen_array(name: &str, kind: Kind, n: usize) -> ArrayRef {
    let i32_at = |i: usize| -> i32 {
        match name {
            "CounterID" => (i % 100) as i32, // 62 appears (Q37–Q40 filter)
            _ => (i % 1000) as i32,
        }
    };
    let i16_at = |i: usize| -> i16 {
        match name {
            // small enums incl. 0 so `<> 0` / `= 0` filters split the data
            "AdvEngineID" | "SearchEngineID" | "IsRefresh" | "DontCountHits" | "IsLink"
            | "IsDownload" | "TraficSourceID" => (i % 3) as i16,
            "ResolutionWidth" => (1000 + (i % 920)) as i16,
            _ => (i % 50) as i16,
        }
    };
    let i64_at = |i: usize| -> i64 {
        match name {
            "WatchID" => i as i64,         // near-unique (high-card GROUP BY Q32/Q33)
            "UserID" => (i % 5000) as i64, // medium cardinality
            _ => (i % 1000) as i64,
        }
    };
    let str_at = |i: usize| -> String {
        match name {
            "URL" if i % 7 == 0 => "http://google.com/search".to_string(),
            "URL" => format!("http://example{}.com/page{}", i % 50, i % 200),
            "Referer" if i % 5 == 0 => "http://www.google.com/path".to_string(),
            "Referer" => format!("http://ref{}.com/q{}", i % 80, i % 300),
            "Title" if i % 6 == 0 => "Google News Today".to_string(),
            "Title" => format!("Title {}", i % 150),
            // many empty — queries filter `SearchPhrase <> ''`
            "SearchPhrase" => {
                if i % 2 == 0 {
                    String::new()
                } else {
                    format!("query {}", i % 120)
                }
            }
            "MobilePhoneModel" => {
                if i % 3 == 0 {
                    String::new()
                } else {
                    format!("model{}", i % 20)
                }
            }
            _ => format!("{}{}", name, i % 100),
        }
    };

    match kind {
        Kind::I16 => Arc::new(Int16Array::from_iter_values((0..n).map(i16_at))),
        Kind::I32 => Arc::new(Int32Array::from_iter_values((0..n).map(i32_at))),
        Kind::I64 => Arc::new(Int64Array::from_iter_values((0..n).map(i64_at))),
        Kind::Ts => Arc::new(Int64Array::from_iter_values(
            (0..n).map(|i| 1_372_636_800_i64 + (i as i64)), // 2013-07-01 00:00:00 + i sec
        )),
        Kind::Date => Arc::new(Date32Array::from_iter_values(
            (0..n).map(|i| JULY_2013 + (i % 31) as i32),
        )),
        Kind::Str => Arc::new(StringArray::from_iter_values((0..n).map(str_at))),
    }
}

fn build_batch(rows: usize) -> (SchemaRef, RecordBatch) {
    let cols = columns();
    let fields: Vec<Field> = cols
        .iter()
        .map(|(name, k)| Field::new(name, arrow_type(*k), false))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let arrays: Vec<ArrayRef> = cols
        .iter()
        .map(|(name, k)| gen_array(name, *k, rows))
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).expect("build record batch");
    (schema, batch)
}

fn load_queries() -> Vec<String> {
    CLICKBENCH_QUERIES
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("--"))
        .map(str::to_string)
        .collect()
}

/// ClickBench-format results + summary; exits non-zero if any query failed.
#[allow(clippy::too_many_arguments)]
fn summarize(
    label: &str,
    out_path: &str,
    queries: usize,
    load_secs: f64,
    data_size: usize,
    results: Vec<serde_json::Value>,
    failures: Vec<(usize, String)>,
    hot_total: f64,
) {
    let passed = queries - failures.len();
    let out = serde_json::json!({
        "system": format!("Weft ({label}, synthetic)"),
        "date": "local-synthetic",
        "machine": "local",
        "cluster_size": 1,
        "proprietary": "no",
        "hardware": "cpu",
        "tuned": "no",
        "tags": ["Rust", "DataFusion", "synthetic-data"],
        "load_time": load_secs,
        "data_size": data_size,
        "result": results,
    });
    std::fs::create_dir_all("bench/clickbench/results").ok();
    std::fs::write(out_path, serde_json::to_string_pretty(&out).unwrap()).expect("write results");
    eprintln!(
        "\n=== ClickBench [{label}] (synthetic): {passed}/{queries} passed; \
         hot total (passing) = {hot_total:.3}s ===",
    );
    eprintln!("wrote {out_path}");
    if !failures.is_empty() {
        eprintln!(
            "failures: {:?}",
            failures.iter().map(|(i, _)| i).collect::<Vec<_>>()
        );
        std::process::exit(1);
    }
}

fn hot_of(tries: &[serde_json::Value]) -> f64 {
    match (
        tries.get(1).and_then(|v| v.as_f64()),
        tries.get(2).and_then(|v| v.as_f64()),
    ) {
        (Some(a), Some(b)) => a.min(b),
        (Some(a), None) | (None, Some(a)) => a,
        _ => 0.0,
    }
}

/// Engine-direct: run the 43 queries straight through DataFusion over an in-memory table.
async fn run_clickbench(rows: usize) {
    eprintln!("[engine-direct] generating synthetic `hits`: {rows} rows × 105 cols …");
    let gen_start = Instant::now();
    let (schema, batch) = build_batch(rows);
    let load_secs = gen_start.elapsed().as_secs_f64();

    let engine = Engine::new();
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("memtable");
    engine
        .ctx()
        .register_table("hits", Arc::new(table))
        .expect("register hits");

    let queries = load_queries();
    eprintln!("running {} queries × 3 tries …\n", queries.len());

    let mut results = Vec::with_capacity(queries.len());
    let mut failures = Vec::new();
    let mut hot_total = 0.0;

    for (idx, q) in queries.iter().enumerate() {
        let mut tries = Vec::with_capacity(3);
        let mut err: Option<String> = None;
        for _ in 0..3 {
            let t = Instant::now();
            match engine.sql(q).await {
                Ok(_) => tries.push(serde_json::json!(t.elapsed().as_secs_f64())),
                Err(e) => {
                    tries.push(serde_json::Value::Null);
                    err.get_or_insert_with(|| e.to_string());
                }
            }
        }
        report(idx, &tries, err, &mut failures, &mut hot_total);
        results.push(serde_json::Value::Array(tries));
    }
    summarize(
        "engine-direct",
        "bench/clickbench/results/local-synthetic.json",
        queries.len(),
        load_secs,
        rows,
        results,
        failures,
        hot_total,
    );
}

/// Live-server: write parquet, boot weft-connect, run everything over gRPC.
async fn run_clickbench_grpc(rows: usize) {
    eprintln!("[live-server] generating synthetic `hits.parquet`: {rows} rows × 105 cols …");
    let gen_start = Instant::now();
    let (_schema, batch) = build_batch(rows);
    let dir = std::env::temp_dir().join("weft-bench");
    std::fs::create_dir_all(&dir).ok();
    let parquet = dir.join("hits.parquet");
    write_parquet(&parquet, &batch);
    let parquet_abs = parquet.canonicalize().expect("canonicalize parquet path");
    let load_secs = gen_start.elapsed().as_secs_f64();

    // Boot the real Spark Connect server in-process.
    tokio::spawn(async move {
        let _ = serve(ServerConfig { port: GRPC_PORT }).await;
    });
    let endpoint = format!("http://127.0.0.1:{GRPC_PORT}");
    let mut client = connect_retry(&endpoint).await;
    eprintln!("connected to live server at {endpoint}");

    let ddl = format!(
        "CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION '{}'",
        parquet_abs.display()
    );
    exec_sql_grpc(&mut client, &ddl)
        .await
        .expect("CREATE EXTERNAL TABLE over gRPC failed");

    let queries = load_queries();
    eprintln!("running {} queries × 3 tries over gRPC …\n", queries.len());

    let mut results = Vec::with_capacity(queries.len());
    let mut failures = Vec::new();
    let mut hot_total = 0.0;

    for (idx, q) in queries.iter().enumerate() {
        let mut tries = Vec::with_capacity(3);
        let mut err: Option<String> = None;
        for _ in 0..3 {
            let t = Instant::now();
            match exec_sql_grpc(&mut client, q).await {
                Ok(_) => tries.push(serde_json::json!(t.elapsed().as_secs_f64())),
                Err(e) => {
                    tries.push(serde_json::Value::Null);
                    err.get_or_insert(e);
                }
            }
        }
        report(idx, &tries, err, &mut failures, &mut hot_total);
        results.push(serde_json::Value::Array(tries));
    }
    summarize(
        "live-server gRPC",
        "bench/clickbench/results/local-grpc.json",
        queries.len(),
        load_secs,
        rows,
        results,
        failures,
        hot_total,
    );
}

fn report(
    idx: usize,
    tries: &[serde_json::Value],
    err: Option<String>,
    failures: &mut Vec<(usize, String)>,
    hot_total: &mut f64,
) {
    if let Some(e) = err {
        let msg = e.lines().next().unwrap_or("").to_string();
        eprintln!("Q{idx:<2} FAIL  {msg}");
        failures.push((idx, msg));
    } else {
        let hot = hot_of(tries);
        *hot_total += hot;
        eprintln!("Q{idx:<2} ok    hot={hot:.4}s");
    }
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close parquet");
}

async fn connect_retry(endpoint: &str) -> SparkConnectServiceClient<Channel> {
    for _ in 0..50 {
        if let Ok(c) = SparkConnectServiceClient::connect(endpoint.to_string()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server did not become ready at {endpoint}");
}

/// Run one SQL statement over gRPC and drain the response stream; returns row count.
async fn exec_sql_grpc(
    client: &mut SparkConnectServiceClient<Channel>,
    sql: &str,
) -> Result<usize, String> {
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
    let mut rows = 0usize;
    while let Some(msg) = stream
        .message()
        .await
        .map_err(|e| e.message().to_string())?
    {
        if let Some(sc::execute_plan_response::ResponseType::ArrowBatch(b)) = msg.response_type {
            rows += b.row_count as usize;
        }
    }
    Ok(rows)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rows = args
        .iter()
        .position(|a| a == "--rows")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_ROWS);

    match args.get(1).map(String::as_str) {
        Some("clickbench") | None => run_clickbench(rows).await,
        Some("clickbench-grpc") => run_clickbench_grpc(rows).await,
        Some("tpch") => tpch::run().await,
        Some(other) => {
            eprintln!("unknown subcommand: {other}; try `clickbench` or `clickbench-grpc`");
            std::process::exit(2);
        }
    }
}
