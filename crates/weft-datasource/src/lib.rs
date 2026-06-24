//! `weft-datasource` — turn storage into Arrow record batches.
//!
//! Phase 0: Parquet/CSV/JSON. Phase 1: lakehouse reads via the engine-agnostic kernels —
//! **Delta** through `delta-kernel-rs` (handles transaction-log replay, deletion vectors,
//! and column mapping; plain `delta-rs` historically did not) and **Iceberg** through
//! `iceberg-rust` (metadata.json → manifest list → manifests → data files, with position
//! and equality deletes). Scan pushdown (projection, predicate, limit) lives here because
//! late materialization is decisive for the heavy ClickBench queries (Q24-class).

use weft_common::Result;

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
