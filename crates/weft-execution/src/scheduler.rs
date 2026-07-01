//! Task scheduling with fault tolerance: retries, alternate workers, health checks.

use std::sync::Arc;
use std::time::Duration;

use weft_common::{Error, Result};
use weft_loom::arrow::record_batch::RecordBatch;

use crate::flight::{health_check_worker, run_stage_on_worker};
use crate::membership::ClusterMembership;
use crate::shuffle::protocol::StageTicket;

/// Max task attempts per endpoint before trying alternates (env: `WEFT_TASK_MAX_RETRIES`, default 3).
pub fn task_max_retries() -> u32 {
    std::env::var("WEFT_TASK_MAX_RETRIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &u32| n > 0)
        .unwrap_or(3)
}

/// Whether an execution error is worth retrying on another worker.
pub fn is_retryable(err: &Error) -> bool {
    let s = err.to_string().to_ascii_lowercase();
    s.contains("connect worker")
        || s.contains("do_get:")
        || s.contains("unavailable")
        || s.contains("connection")
        || s.contains("deadline")
        || s.contains("reset")
        || s.contains("broken pipe")
        || s.contains("health check failed")
}

/// Run a stage ticket on `primary`, retrying on transient errors and falling back to alternate
/// workers from `membership` when the primary is unreachable.
pub async fn run_stage_with_retry(
    membership: &Arc<dyn ClusterMembership>,
    primary: String,
    ticket: StageTicket,
) -> Result<Vec<RecordBatch>> {
    let max = task_max_retries();
    let mut tried = vec![primary.clone()];
    let mut last_err = None;

    for attempt in 0..max {
        match run_stage_on_worker(primary.clone(), ticket.clone()).await {
            Ok(b) => return Ok(b),
            Err(e) if is_retryable(&e) && attempt + 1 < max => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                continue;
            }
            Err(e) if is_retryable(&e) => last_err = Some(e),
            Err(e) => return Err(e),
        }
    }

    // Try alternate healthy workers not yet attempted.
    for alt in membership.endpoints() {
        if tried.contains(&alt) {
            continue;
        }
        if health_check_worker(alt.clone()).await.is_err() {
            continue;
        }
        tried.push(alt.clone());
        match run_stage_on_worker(alt, ticket.clone()).await {
            Ok(b) => return Ok(b),
            Err(e) if is_retryable(&e) => last_err = Some(e),
            Err(e) => return Err(e),
        }
    }

    Err(last_err.unwrap_or_else(|| Error::Execution("stage task failed on all workers".into())))
}

/// Filter endpoints to those that respond to a health check.
pub async fn healthy_endpoints(endpoints: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for ep in endpoints {
        if health_check_worker(ep.clone()).await.is_ok() {
            out.push(ep.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_errors_match_transport_failures() {
        assert!(is_retryable(&Error::Io("connect worker: refused".into())));
        assert!(is_retryable(&Error::Execution("do_get: Unavailable".into())));
        assert!(!is_retryable(&Error::Plan("bad sql".into())));
    }
}
