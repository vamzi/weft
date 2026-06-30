//! Smoke test for the `weft worker` / `weft driver` CLI subprocess path: spawn two workers,
//! register in-memory data via Parquet files, and assert the distributed GROUP BY matches
//! single-node execution.
//!
//! Lives in `weft-cli` so Cargo sets `CARGO_BIN_EXE_weft` when the test binary is built.

use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use weft_loom::arrow::array::Int64Array;
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
}

fn make_batch(start: i64, end: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let ks: Vec<i64> = (start..end).map(|i| i % 5).collect();
    let vs: Vec<i64> = (start..end).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ks)),
            Arc::new(Int64Array::from(vs)),
        ],
    )
    .unwrap()
}

fn write_parquet(path: &std::path::Path, batch: &RecordBatch) {
    use datafusion::parquet::arrow::ArrowWriter;
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn weft_bin() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_weft") {
        return std::path::PathBuf::from(p);
    }
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "debug".into());
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();
    if let Ok(td) = std::env::var("CARGO_TARGET_DIR") {
        candidates.push(std::path::PathBuf::from(td).join(&profile).join("weft"));
    }
    let workspace_target =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target");
    candidates.push(workspace_target.join(&profile).join("weft"));
    // `cargo llvm-cov` uses a separate target dir; CI pre-builds weft-cli there.
    candidates.push(
        workspace_target
            .join("llvm-cov-target")
            .join(&profile)
            .join("weft"),
    );
    for c in &candidates {
        if c.exists() {
            return c.clone();
        }
    }
    candidates
        .into_iter()
        .next()
        .unwrap_or_else(|| workspace_target.join(&profile).join("weft"))
}

#[tokio::test]
async fn cli_driver_worker_matches_single_node() {
    const N: i64 = 50;
    let query = "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k";

    let single = Engine::new();
    single
        .register_batches("t", vec![make_batch(0, N)])
        .unwrap();
    let expected_rows: usize = single
        .sql(query)
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();

    let dir = TempDir::new().unwrap();
    let p0_path = dir.path().join("half0.parquet");
    let p1_path = dir.path().join("half1.parquet");
    write_parquet(&p0_path, &make_batch(0, N / 2));
    write_parquet(&p1_path, &make_batch(N / 2, N));

    let weft = weft_bin();
    assert!(
        weft.exists(),
        "weft binary not found at {}; run `cargo build -p weft-cli` first",
        weft.display()
    );

    let p0 = pick_port();
    let p1 = pick_port();

    let mut w0 = Command::new(&weft)
        .args([
            "worker",
            "--port",
            &p0.to_string(),
            "--data",
            p0_path.to_str().unwrap(),
            "--table",
            "t",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn worker 0");
    let mut w1 = Command::new(&weft)
        .args([
            "worker",
            "--port",
            &p1.to_string(),
            "--data",
            p1_path.to_str().unwrap(),
            "--table",
            "t",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn worker 1");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut driver_ok = false;
    for _ in 0..30 {
        let out = Command::new(&weft)
            .args([
                "driver",
                "--workers",
                &format!("127.0.0.1:{p0},127.0.0.1:{p1}"),
                "--partial-sql",
                "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k",
                "--final-sql",
                "SELECT k, SUM(c) AS c, SUM(s) AS s FROM shuffle_input GROUP BY k",
                "--hash-keys",
                "0",
            ])
            .output()
            .expect("run driver");
        if out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            if stderr.contains(&format!("distributed result: {expected_rows} rows")) {
                driver_ok = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let _ = w0.kill();
    let _ = w1.kill();
    let _ = w0.wait();
    let _ = w1.wait();

    assert!(
        driver_ok,
        "weft driver subprocess must return {expected_rows} rows matching single-node"
    );
}
