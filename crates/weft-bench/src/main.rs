//! `weft-bench` — engine-direct benchmark harness.
//!
//! Runs the real ClickBench queries through [`weft_loom::Engine`] (DataFusion) against a
//! **synthetic `hits` table** so we can prove all 43 queries run to completion locally and
//! produce a ClickBench-format `results.json`. The real 14 GB run happens on a
//! `c6a.4xlarge` via the shell harness in `bench/clickbench/` (driven by a Spark Connect
//! client against the live server). This local harness is for dev/CI coverage, not for
//! comparison against Sail's absolute numbers (synthetic data, debug builds).
//!
//! Usage: `cargo run -p weft-bench [--release] -- clickbench [--rows N]`

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{
    ArrayRef, Date32Array, Int16Array, Int32Array, Int64Array, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use weft_loom::Engine;

/// Embedded so the binary needs no working-directory assumptions.
const HITS_SCHEMA_TSV: &str = include_str!("../../../bench/clickbench/hits_schema.tsv");
const CLICKBENCH_QUERIES: &str = include_str!("../../../bench/clickbench/queries.sql");

/// Days since the Unix epoch for 2013-07-01 (the ClickBench filter window in Q37–Q40).
const JULY_2013: i32 = 15887;
const DEFAULT_ROWS: usize = 50_000;

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
/// filters/joins/LIKE/GROUP BY in the queries actually match some rows.
fn gen_array(name: &str, kind: Kind, n: usize) -> ArrayRef {
    let i32_at = |i: usize| -> i32 {
        match name {
            "CounterID" => (i % 100) as i32, // 62 appears (Q37–Q40 filter)
            "ClientIP" => (i % 1000) as i32,
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
            "URL" => {
                if i % 7 == 0 {
                    "http://google.com/search".to_string()
                } else {
                    format!("http://example{}.com/page{}", i % 50, i % 200)
                }
            }
            "Referer" => {
                if i % 5 == 0 {
                    "http://www.google.com/path".to_string()
                } else {
                    format!("http://ref{}.com/q{}", i % 80, i % 300)
                }
            }
            "Title" => {
                if i % 6 == 0 {
                    "Google News Today".to_string()
                } else {
                    format!("Title {}", i % 150)
                }
            }
            "SearchPhrase" => {
                if i % 2 == 0 {
                    String::new() // many empty — queries filter `SearchPhrase <> ''`
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

async fn run_clickbench(rows: usize) {
    let cols = columns();
    eprintln!(
        "generating synthetic `hits`: {} rows × {} columns …",
        rows,
        cols.len()
    );

    let gen_start = Instant::now();
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
    let load_secs = gen_start.elapsed().as_secs_f64();

    let engine = Engine::new();
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("memtable");
    engine
        .ctx()
        .register_table("hits", Arc::new(table))
        .expect("register hits");

    let queries: Vec<&str> = CLICKBENCH_QUERIES
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("--"))
        .collect();
    eprintln!("running {} queries × 3 tries …\n", queries.len());

    let mut results: Vec<serde_json::Value> = Vec::with_capacity(queries.len());
    let mut failures: Vec<(usize, String)> = Vec::new();
    let mut hot_total = 0.0_f64;
    let mut passed = 0usize;

    for (idx, q) in queries.iter().enumerate() {
        let mut tries: Vec<serde_json::Value> = Vec::with_capacity(3);
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
        if let Some(e) = err {
            failures.push((idx, e.lines().next().unwrap_or("").to_string()));
            eprintln!("Q{:<2} FAIL  {}", idx, failures.last().unwrap().1);
        } else {
            passed += 1;
            // hot = min of try 2 and 3 (ClickBench rule)
            let hot = match (tries[1].as_f64(), tries[2].as_f64()) {
                (Some(a), Some(b)) => a.min(b),
                (Some(a), None) | (None, Some(a)) => a,
                _ => 0.0,
            };
            hot_total += hot;
            eprintln!("Q{:<2} ok    hot={:.4}s", idx, hot);
        }
        results.push(serde_json::Value::Array(tries));
    }

    let out = serde_json::json!({
        "system": "Weft (Loom/DataFusion, synthetic)",
        "date": "local-synthetic",
        "machine": "local",
        "cluster_size": 1,
        "proprietary": "no",
        "hardware": "cpu",
        "tuned": "no",
        "tags": ["Rust", "DataFusion", "synthetic-data"],
        "load_time": load_secs,
        "data_size": rows,
        "result": results,
    });
    let dir = "bench/clickbench/results";
    std::fs::create_dir_all(dir).ok();
    let path = format!("{dir}/local-synthetic.json");
    std::fs::write(&path, serde_json::to_string_pretty(&out).unwrap()).expect("write results");

    eprintln!(
        "\n=== ClickBench (synthetic): {}/{} queries passed; hot total (passing) = {:.3}s ===",
        passed,
        queries.len(),
        hot_total
    );
    eprintln!("wrote {path}");
    if !failures.is_empty() {
        eprintln!(
            "failures: {:?}",
            failures.iter().map(|(i, _)| i).collect::<Vec<_>>()
        );
        // Non-zero exit so CI notices missing coverage.
        std::process::exit(1);
    }
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
        Some("tpch") => {
            eprintln!("tpch harness: TODO (issue #2)");
            std::process::exit(2);
        }
        Some(other) => {
            eprintln!("unknown subcommand: {other}; try `clickbench`");
            std::process::exit(2);
        }
    }
}
