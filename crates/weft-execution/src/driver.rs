//! The distributed driver: orchestrate a two-stage `partial-agg → hash shuffle → final-agg`
//! query across a static set of workers.
//!
//! The MVP does not auto-decompose SQL; the caller supplies a [`DistributedPlan`] with the
//! partial and final SQL plus the shuffle key. This proves the shuffle mechanism end-to-end;
//! automatic aggregate decomposition (and AVG / `COUNT(DISTINCT)`) is deferred.

use weft_common::Result;
use weft_loom::arrow::record_batch::RecordBatch;

use crate::flight::run_stage_on_worker;
use crate::shuffle::protocol::StageTicket;

/// A static cluster of worker Flight endpoints (e.g. `http://127.0.0.1:50561`).
#[derive(Debug, Clone)]
pub struct Cluster {
    /// Worker endpoints. Partition `i` is owned by `workers[i]`.
    pub workers: Vec<String>,
}

impl Cluster {
    /// Build a cluster from a list of endpoints.
    pub fn new(workers: Vec<String>) -> Self {
        Self { workers }
    }
}

/// A two-stage distributed aggregation plan.
#[derive(Debug, Clone)]
pub struct DistributedPlan {
    /// Stage-0 SQL, run per worker over its local table(s); its output is shuffled.
    pub partial_sql: String,
    /// Stage-1 SQL, run per worker over the `shuffle_input` table built from pulled buckets.
    pub final_sql: String,
    /// Output column indices of the stage-0 result to hash-partition on (the group key).
    pub hash_key_cols: Vec<u32>,
}

/// Run `plan` across `cluster` and return the concatenated final result.
///
/// Stage 0: every worker computes its partial aggregate, hash-partitions it into one bucket
/// per worker, and caches the buckets (barrier — all must finish). Stage 1: worker `p` pulls
/// bucket `p` from every worker, registers it as `shuffle_input`, runs `final_sql`, and
/// streams its slice of the answer back.
pub async fn run_distributed(
    cluster: &Cluster,
    plan: &DistributedPlan,
) -> Result<Vec<RecordBatch>> {
    let w = cluster.workers.len() as u32;

    // Stage 0 — compute + cache partials on every worker (barrier before stage 1).
    let mut stage0 = Vec::new();
    for (i, endpoint) in cluster.workers.iter().enumerate() {
        let ticket = StageTicket {
            stage_id: 0,
            partition_id: i as u32,
            num_partitions: w,
            upstream_endpoints: vec![],
            stage_sql: plan.partial_sql.clone(),
            plan_fragment: vec![],
            hash_key_cols: plan.hash_key_cols.clone(),
        };
        stage0.push(run_stage_on_worker(endpoint.clone(), ticket));
    }
    for r in futures::future::join_all(stage0).await {
        r?; // surface any stage-0 failure
    }

    // Stage 1 — each worker pulls its bucket from all upstreams and finalizes.
    let mut stage1 = Vec::new();
    for (p, endpoint) in cluster.workers.iter().enumerate() {
        let ticket = StageTicket {
            stage_id: 1,
            partition_id: p as u32,
            num_partitions: w,
            upstream_endpoints: cluster.workers.clone(),
            stage_sql: plan.final_sql.clone(),
            plan_fragment: vec![],
            hash_key_cols: plan.hash_key_cols.clone(),
        };
        stage1.push(run_stage_on_worker(endpoint.clone(), ticket));
    }

    let mut out = Vec::new();
    for r in futures::future::join_all(stage1).await {
        out.extend(r?);
    }
    Ok(out)
}
