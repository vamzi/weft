//! Route distributable SQL through the driver/worker cluster.

use weft_common::{Error, Result};
use weft_execution::driver::{run_stages, Cluster};
use weft_execution::flight::sync_udfs_to_worker;
use weft_execution::membership::resolve_membership;
use weft_execution::plan::plan_distributed;
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

/// If workers or K8s service discovery is configured and `sql` is auto-splittable, run distributed.
/// Returns `Ok(None)` when the query should fall back to single-node execution.
pub async fn try_run_distributed(
    engine: &Engine,
    workers: &[String],
    sql: &str,
    replicated: &[&str],
    udf_json: Option<&str>,
) -> Result<Option<Vec<RecordBatch>>> {
    let membership = resolve_membership(workers);
    let endpoints = membership.endpoints();
    if endpoints.is_empty() {
        return Ok(None);
    }

    let dq = match plan_distributed(engine, sql, replicated).await {
        Ok(d) => d,
        Err(Error::Unsupported(_)) => return Ok(None),
        Err(e) => return Err(e),
    };

    if let Some(json) = udf_json.filter(|s| !s.is_empty() && *s != "[]") {
        for ep in &endpoints {
            sync_udfs_to_worker(ep.clone(), json).await?;
        }
    }

    let cluster = Cluster::from_membership(membership);
    let mut batches = run_stages(&cluster, &dq.stages).await?;

    if let Some(finalize) = dq.finalize_sql {
        engine
            .register_batches("result", batches.clone())
            .map_err(|e| Error::Execution(e.to_string()))?;
        batches = engine.sql(&finalize).await?;
    }

    Ok(Some(batches))
}

/// Parse `spark.weft.workers` or `WEFT_WORKERS` (comma-separated `host:port` list).
pub fn parse_worker_list(config_value: Option<&str>) -> Vec<String> {
    let env_workers = std::env::var("WEFT_WORKERS").ok();
    let raw = config_value
        .filter(|s| !s.is_empty())
        .or(env_workers.as_deref())
        .unwrap_or("");
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|ep| {
            if ep.starts_with("http://") || ep.starts_with("https://") {
                ep.to_string()
            } else {
                format!("http://{ep}")
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_worker_list() {
        let w = parse_worker_list(Some("127.0.0.1:50561,127.0.0.1:50562"));
        assert_eq!(w.len(), 2);
        assert!(w[0].starts_with("http://"));
    }
}
