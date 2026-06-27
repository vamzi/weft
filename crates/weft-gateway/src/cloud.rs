//! Durable control-plane persistence via the AWS CLI (no SDK dependency — mirrors the Glue
//! approach; the EC2 instance role provides credentials via IMDS).
//!
//! - **Workspace** (notebooks, saved queries) → an S3 object per collection.
//! - **Clusters + catalog connections** → a DynamoDB table, one item per collection (`pk` = the
//!   collection name, `body` = the JSON blob). Last-write-wins.
//!
//! Each collection is stored as a single JSON blob (not row-per-item) so the existing in-memory
//! `Vec<T>` model maps directly and writes stay atomic per collection. Writes are best-effort and
//! run off the request path (the callers `tokio::spawn` them); loads happen once at startup.

use std::process::Stdio;

use tokio::io::AsyncWriteExt;

fn aws_bin() -> String {
    std::env::var("WEFT_AWS_BIN").unwrap_or_else(|_| "aws".to_string())
}
fn region() -> String {
    std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string())
}

/// Run `aws <args> --region <r>`, optionally feeding `stdin`. Returns stdout on success.
async fn aws(args: &[&str], stdin: Option<Vec<u8>>) -> Result<String, String> {
    let mut cmd = tokio::process::Command::new(aws_bin());
    cmd.args(args).args(["--region", &region()]);
    if stdin.is_some() {
        cmd.stdin(Stdio::piped());
    }
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("spawn aws: {e}"))?;
    if let Some(bytes) = stdin {
        if let Some(mut si) = child.stdin.take() {
            si.write_all(&bytes)
                .await
                .map_err(|e| format!("write stdin: {e}"))?;
            si.shutdown().await.ok();
        }
    }
    let out = child
        .wait_with_output()
        .await
        .map_err(|e| format!("wait aws: {e}"))?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

// ───────────────────────────────── S3 (workspace blobs) ─────────────────────────────────

/// Read an S3 object's body (`aws s3 cp <uri> -`). `None` if missing/unreadable.
pub async fn s3_get(uri: &str) -> Option<String> {
    aws(&["s3", "cp", uri, "-"], None).await.ok()
}

/// Write an S3 object from `body` (`aws s3 cp - <uri>`).
pub async fn s3_put(uri: String, body: String) {
    if let Err(e) = aws(&["s3", "cp", "-", &uri], Some(body.into_bytes())).await {
        eprintln!("warn: S3 persist to {uri} failed: {e}");
    }
}

// ───────────────────────────────── DynamoDB (collections) ─────────────────────────────────

/// Read the `body` blob of item `pk` from `table`. `None` if the item is absent.
pub async fn ddb_get(table: &str, pk: &str) -> Option<String> {
    let key = format!(r#"{{"pk":{{"S":"{pk}"}}}}"#);
    let out = aws(
        &[
            "dynamodb",
            "get-item",
            "--table-name",
            table,
            "--key",
            &key,
            "--query",
            "Item.body.S",
            "--output",
            "text",
        ],
        None,
    )
    .await
    .ok()?;
    let t = out.trim();
    if t.is_empty() || t == "None" {
        None
    } else {
        Some(t.to_string())
    }
}

/// Upsert item `pk` in `table` with `body` as its JSON blob.
pub async fn ddb_put(table: String, pk: String, body: String) {
    // Build the item with serde so the JSON-string escaping is correct, then pass via a temp file
    // (the blob can be large and contains quotes — inline `--item` is fragile).
    let item = serde_json::json!({ "pk": { "S": pk }, "body": { "S": body } }).to_string();
    let path = std::env::temp_dir().join(format!("weft-ddb-{}.json", sanitize(&item)));
    if let Err(e) = std::fs::write(&path, item) {
        eprintln!("warn: DynamoDB temp write failed: {e}");
        return;
    }
    let file_arg = format!("file://{}", path.display());
    let res = aws(
        &[
            "dynamodb",
            "put-item",
            "--table-name",
            &table,
            "--item",
            &file_arg,
        ],
        None,
    )
    .await;
    let _ = std::fs::remove_file(&path);
    if let Err(e) = res {
        eprintln!("warn: DynamoDB persist of `{table}` failed: {e}");
    }
}

/// A short stable-ish suffix for the temp filename (avoids `Math.random`; derived from the payload).
fn sanitize(s: &str) -> String {
    let mut h: u64 = 1469598103934665603;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    format!("{h:x}")
}
