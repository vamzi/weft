//! `weft-connect` — the Spark Connect gRPC front-end.
//!
//! Implements `SparkConnectService` so an unmodified PySpark / Spark SQL client connects
//! via `sc://host:port`. Minimum surface to be a true drop-in (architecture §2):
//!
//! - **Config** (Set/Get/GetAll/Unset/IsModifiable);
//! - **AnalyzePlan** (`Schema`, `Explain`, `SparkVersion`, `DDLParse`);
//! - **ExecutePlan** streaming Arrow IPC batches + a terminal `ResultComplete`;
//! - **ReattachExecute`/`ReleaseExecute** (PySpark 3.5+ sets `reattachable=true`, so the
//!   server buffers responses and resumes a broken stream by `last_response_id`);
//! - **AddArtifacts`/`ArtifactStatus** (cloudpickled Python UDFs, once UDFs land);
//! - error responses carrying `google.rpc.ErrorInfo` metadata via
//!   [`weft_common::Error::spark_error_class`].
//!
//! The request path: gRPC `Plan` → [`weft_plan`] (or [`weft_sql`]) → [`weft_analyzer`] →
//! [`weft_optimizer`] → [`weft_physical`] → [`weft_execution`] → Arrow batches back out.

use weft_common::Result;

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

/// Start the Spark Connect server. Implemented in Phase 0 (issue #1).
pub fn serve(_config: ServerConfig) -> Result<()> {
    Err(weft_common::Error::Unsupported(
        "weft-connect server not implemented yet — see docs/ISSUES.md issue #1".into(),
    ))
}
