//! `weft-datasource` — turn storage into Arrow record batches.
//!
//! Phase 0: Parquet/CSV/JSON. Phase 1: lakehouse reads via the engine-agnostic kernels —
//! **Delta** through `delta-kernel-rs` (handles transaction-log replay, deletion vectors,
//! and column mapping; plain `delta-rs` historically did not) and **Iceberg** through
//! `iceberg-rust` (metadata.json → manifest list → manifests → data files, with position
//! and equality deletes). Scan pushdown (projection, predicate, limit) lives here because
//! late materialization is decisive for the heavy ClickBench queries (Q24-class).

use weft_common::{Error, Result};

/// A read request against a source: which columns, what filter, optional row limit.
#[derive(Debug, Clone, Default)]
pub struct ScanRequest {
    /// Projected column names; empty = all.
    pub projection: Vec<String>,
    /// Pushed-down filter as a SQL fragment (placeholder; becomes a typed predicate).
    pub filter: Option<String>,
    /// Optional `LIMIT` for top-N / sample pushdown.
    pub limit: Option<usize>,
}

/// Open a source and produce Arrow batches. Implemented in Phase 0/1.
pub fn scan(_uri: &str, _req: &ScanRequest) -> Result<()> {
    Ok(())
}

/// Resolve a Delta Lake table to its active data-file paths by replaying the JSON transaction
/// log (`_delta_log/*.json`): `add` actions introduce files, `remove` actions retire them.
///
/// This is the version-safe Phase-1 read path — it yields plain Parquet paths that the
/// engine's native reader consumes, so it does not couple Weft to a Delta crate's DataFusion
/// version. Limitations (v1): JSON commits only (no checkpoint Parquet), and no deletion
/// vectors / column mapping — those arrive with a `delta-kernel` integration later.
pub fn delta_active_files(table_path: &str) -> Result<Vec<std::path::PathBuf>> {
    use std::collections::HashSet;
    use std::path::Path;

    let base = Path::new(table_path);
    let log_dir = base.join("_delta_log");
    let mut commits: Vec<std::path::PathBuf> = std::fs::read_dir(&log_dir)
        .map_err(|e| Error::Io(format!("reading {}: {e}", log_dir.display())))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    commits.sort(); // 000…0.json, 000…1.json, … apply in version order

    if commits.is_empty() {
        return Err(Error::Io(format!(
            "no _delta_log/*.json under {} (checkpoint-only tables not supported yet)",
            table_path
        )));
    }

    let mut order: Vec<String> = Vec::new();
    let mut present: HashSet<String> = HashSet::new();
    for commit in &commits {
        let content = std::fs::read_to_string(commit)
            .map_err(|e| Error::Io(format!("reading {}: {e}", commit.display())))?;
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let v: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| Error::Io(format!("delta log json: {e}")))?;
            if let Some(p) = v
                .get("add")
                .and_then(|a| a.get("path"))
                .and_then(|p| p.as_str())
            {
                if present.insert(p.to_string()) {
                    order.push(p.to_string());
                }
            } else if let Some(p) = v
                .get("remove")
                .and_then(|r| r.get("path"))
                .and_then(|p| p.as_str())
            {
                if present.remove(p) {
                    order.retain(|x| x != p);
                }
            }
        }
    }
    Ok(order.into_iter().map(|p| base.join(p)).collect())
}
