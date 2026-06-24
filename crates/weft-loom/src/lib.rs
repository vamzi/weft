//! `weft-loom` — the vectorized CPU engine and Weft's workhorse.
//!
//! **This is what beats Sail on ClickBench.** Phase 0 embeds DataFusion behind the warp
//! IR to reach correctness + a credible benchmark entry fast. Phase 1 carves out native
//! operators for the handful of queries that dominate the total runtime:
//!
//! - high-cardinality `GROUP BY` (Q31–Q35): adaptive, radix-partitioned, open-addressing
//!   hash table with an inline hash salt; spill partitions independently;
//! - sort / top-N (Q23–Q26 and every `… ORDER BY c DESC LIMIT 10`): late-materialized
//!   top-N heap that decodes only the surviving rows;
//! - string `LIKE`/regex (Q20–Q23, Q28): SIMD substring + vectorized regex;
//! - `COUNT(DISTINCT)` (Q4/Q5 + per-group): HyperLogLog sketches.
//!
//! The strategy: tie Sail on the ~33 cheap queries (DataFusion parity), beat it 1.5–2× on
//! the ~10 expensive ones. Winning those *is* winning the total.

use datafusion::prelude::SessionContext;
use weft_common::{Error, Result};

/// Re-export of the exact `arrow` DataFusion uses, so every crate in the workspace encodes
/// Arrow IPC against one version (no cross-crate `arrow` mismatch).
pub use datafusion::arrow;

use arrow::record_batch::RecordBatch;

/// Backend identifier reported in `EXPLAIN`.
pub const NAME: &str = "loom";

/// The CPU execution engine: a DataFusion [`SessionContext`] today, growing native
/// operators behind the same surface in Phase 1.
pub struct Engine {
    ctx: SessionContext,
}

impl Engine {
    /// Create a fresh engine with default session state.
    ///
    /// If `WEFT_MEMORY_LIMIT_BYTES` is set, the engine runs with a bounded spill pool of
    /// that size (DataFusion spills aggregations/sorts to disk instead of OOM-killing the
    /// process) — important when running ClickBench on a memory-constrained box. Unset
    /// (the default) keeps the unbounded pool, so local/test behavior is unchanged.
    pub fn new() -> Self {
        use datafusion::prelude::SessionConfig;

        let mut config = SessionConfig::new();
        if let Some(p) = std::env::var("WEFT_TARGET_PARTITIONS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            config = config.with_target_partitions(p);
        }

        let ctx = match std::env::var("WEFT_MEMORY_LIMIT_BYTES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(bytes) => {
                use datafusion::execution::memory_pool::FairSpillPool;
                use datafusion::execution::runtime_env::RuntimeEnvBuilder;
                use std::sync::Arc;
                let env = RuntimeEnvBuilder::new()
                    .with_memory_pool(Arc::new(FairSpillPool::new(bytes)))
                    .build_arc()
                    .expect("runtime env");
                SessionContext::new_with_config_rt(config, env)
            }
            None => SessionContext::new_with_config(config),
        };
        Self { ctx }
    }

    /// Run a SQL string and collect the result as Arrow record batches.
    ///
    /// Errors are mapped onto the Weft error model: a planning/analysis failure becomes
    /// [`Error::Plan`] (→ Spark `AnalysisException`), an execution failure [`Error::Execution`].
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let df = self
            .ctx
            .sql(query)
            .await
            .map_err(|e| Error::Plan(e.to_string()))?;
        df.collect()
            .await
            .map_err(|e| Error::Execution(e.to_string()))
    }

    /// Access the underlying DataFusion context (e.g. to register tables/Parquet).
    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn select_one() {
        let engine = Engine::new();
        let batches = engine.sql("SELECT 1 AS x").await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[tokio::test]
    async fn select_arithmetic() {
        let engine = Engine::new();
        let batches = engine.sql("SELECT 40 + 2 AS answer").await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }
}
