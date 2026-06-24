//! `weft-catalog` — table/namespace resolution across catalog providers.
//!
//! Phase 1→2 providers: in-memory, Hive Metastore, AWS Glue, and **Unity Catalog**
//! (resolve via the Iceberg REST Catalog API, then call
//! `POST /api/2.1/unity-catalog/temporary-table-credentials` for short-lived storage
//! tokens; tables must advertise `HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT`).

use weft_common::Result;

/// A resolved table handle: the storage location plus the format needed to read it.
#[derive(Debug, Clone)]
pub struct TableHandle {
    /// Fully-qualified name, e.g. `main.sales.orders`.
    pub name: String,
    /// Storage URI (e.g. `s3://bucket/path`).
    pub location: String,
    /// Table format.
    pub format: TableFormat,
}

/// Physical table format Weft can read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormat {
    /// Parquet directory / single file.
    Parquet,
    /// Delta Lake.
    Delta,
    /// Apache Iceberg.
    Iceberg,
}

/// A catalog that can resolve a name to a [`TableHandle`].
pub trait Catalog: Send + Sync {
    /// Resolve a (possibly multi-part) identifier to a table handle.
    fn resolve(&self, identifier: &str) -> Result<TableHandle>;
}
