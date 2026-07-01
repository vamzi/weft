//! The distributed driver: orchestrate a stage DAG across workers.
//!
//! A query is expressed as a topologically-ordered list of [`StageDef`]s. Each *producer* stage
//! runs once per shuffle partition on the worker that owns that partition (rendezvous hashing),
//! hash-partitions its output, and caches the buckets. The output stage runs on every partition
//! owner, pulling upstream buckets and returning results.
//!
//! Shuffle partition count defaults to worker count but can be overridden via
//! `WEFT_SHUFFLE_PARTITIONS` (like `spark.sql.shuffle.partitions`).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use weft_common::{Error, Result};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_observability::{ExecutionEvent, SharedStore, StageStatus, TaskStatus, now_ms};

use crate::aqe::{aqe_enabled, coalesced_partitions};
use crate::flight::{clear_worker_stages, pull_bucket_with_retry};
use crate::lineage::StageLineage;
use crate::membership::{ClusterMembership, StaticMembership};
use crate::scheduler::run_stage_with_retry;
use crate::shuffle::protocol::StageTicket;

/// Number of hash-shuffle partitions for the next query.
pub fn shuffle_partitions(worker_count: usize) -> u32 {
    std::env::var("WEFT_SHUFFLE_PARTITIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &u32| n > 0)
        .unwrap_or(worker_count.max(1) as u32)
}

/// A cluster snapshot for one query: workers + stable partition→owner mapping.
#[derive(Clone)]
pub struct Cluster {
    /// Unique worker endpoints in this snapshot.
    pub workers: Vec<String>,
    /// Hash-shuffle partition count (may exceed worker count).
    pub num_partitions: u32,
    pub(crate) membership: Arc<dyn ClusterMembership>,
}

impl Cluster {
    /// Build a cluster from a fixed endpoint list (tests, CLI).
    pub fn new(workers: Vec<String>) -> Self {
        let membership = Arc::new(StaticMembership::new(workers.clone()));
        let num_partitions = shuffle_partitions(workers.len());
        Self {
            workers,
            num_partitions,
            membership,
        }
    }

    /// Snapshot from a live [`ClusterMembership`] provider (EKS DNS, static list, etc.).
    pub fn from_membership(membership: Arc<dyn ClusterMembership>) -> Self {
        let workers = membership.endpoints();
        let num_partitions = shuffle_partitions(workers.len());
        Self {
            workers,
            num_partitions,
            membership,
        }
    }

    /// Wrap an existing trait object reference (preserves live membership for DNS refresh).
    pub fn from_membership_ref(membership: &dyn ClusterMembership) -> Self {
        // Caller should prefer `from_membership(Arc<...>)`; this path clones endpoints once.
        Self::from_membership(Arc::new(StaticMembership::new(membership.endpoints())))
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// The Flight endpoint that owns shuffle partition `p`.
    pub fn owner_endpoint(&self, partition: u32) -> Result<String> {
        self.membership
            .owner_of(partition, self.num_partitions)
            .ok_or_else(|| Error::Execution(format!("no owner for partition {partition}")))
    }
}

/// One stage of a distributed query.
#[derive(Debug, Clone)]
pub struct StageDef {
    pub stage_id: u32,
    pub sql: String,
    pub upstream_stage_ids: Vec<u32>,
    pub hash_key_cols: Vec<u32>,
}

/// A two-stage distributed aggregation plan: `partial-agg → hash shuffle → final-agg`.
#[derive(Debug, Clone)]
pub struct DistributedPlan {
    pub partial_sql: String,
    pub final_sql: String,
    pub hash_key_cols: Vec<u32>,
}

impl DistributedPlan {
    pub fn into_stages(&self) -> Vec<StageDef> {
        vec![
            StageDef {
                stage_id: 0,
                sql: self.partial_sql.clone(),
                upstream_stage_ids: vec![],
                hash_key_cols: self.hash_key_cols.clone(),
            },
            StageDef {
                stage_id: 1,
                sql: self.final_sql.clone(),
                upstream_stage_ids: vec![0],
                hash_key_cols: vec![],
            },
        ]
    }
}

pub async fn run_distributed(
    cluster: &Cluster,
    plan: &DistributedPlan,
) -> Result<Vec<RecordBatch>> {
    run_stages_obs(cluster, &plan.into_stages(), None, None).await
}

pub async fn run_distributed_with_membership(
    membership: Arc<dyn ClusterMembership>,
    plan: &DistributedPlan,
) -> Result<Vec<RecordBatch>> {
    run_stages_obs(
        &Cluster::from_membership(membership),
        &plan.into_stages(),
        None,
        None,
    )
    .await
}

pub async fn run_stages_with_membership(
    membership: Arc<dyn ClusterMembership>,
    stages: &[StageDef],
) -> Result<Vec<RecordBatch>> {
    run_stages_obs(&Cluster::from_membership(membership), stages, None, None).await
}

pub async fn run_stages(cluster: &Cluster, stages: &[StageDef]) -> Result<Vec<RecordBatch>> {
    run_stages_obs(cluster, stages, None, None).await
}

pub async fn run_stages_obs(
    cluster: &Cluster,
    stages: &[StageDef],
    store: Option<SharedStore>,
    operation_id: Option<String>,
) -> Result<Vec<RecordBatch>> {
    let lineage = Arc::new(StageLineage::new());
    let stage_map: HashMap<u32, StageDef> =
        stages.iter().map(|s| (s.stage_id, s.clone())).collect();
    let mut cluster = cluster.clone();
    let consumed: HashSet<u32> = stages
        .iter()
        .flat_map(|s| s.upstream_stage_ids.iter().copied())
        .collect();

    let outputs: Vec<&StageDef> = stages
        .iter()
        .filter(|s| !consumed.contains(&s.stage_id))
        .collect();
    let output = match outputs.as_slice() {
        [o] => *o,
        _ => {
            return Err(Error::Plan(format!(
                "distributed plan must have exactly one output stage, found {}",
                outputs.len()
            )))
        }
    };

    // Producer / intermediate stages: one invocation per worker endpoint (each runs local SQL
    // and hash-partitions into `num_partitions` buckets). Rendezvous hashing applies to the
    // output stage only.
    for stage in stages.iter().filter(|s| s.stage_id != output.stage_id) {
        refresh_cluster_workers(&mut cluster);
        let np = cluster.num_partitions;
        let mut futs = Vec::new();
        for (i, endpoint) in cluster.workers.iter().enumerate() {
            let ticket = stage_ticket(stage, i as u32, np, &cluster, true);
            let membership = cluster.membership.clone();
            let ep = endpoint.clone();
            let host = ep
                .trim_start_matches("http://")
                .trim_start_matches("https://")
                .to_string();
            let lineage = lineage.clone();
            let stage_map = stage_map.clone();
            let store_c = store.clone();
            let op_c = operation_id.clone();
            let stage_id = stage.stage_id as i32;
            let task_id = store
                .as_ref()
                .map(|s| s.alloc_task_id())
                .unwrap_or(i as i64);
            if let (Some(ref s), Some(ref op)) = (&store_c, &op_c) {
                s.emit(ExecutionEvent::TaskStarted {
                    operation_id: op.clone(),
                    stage_id,
                    task_id,
                    executor_id: host.to_string(),
                    launch_time_ms: now_ms(),
                });
            }
            futs.push(async move {
                let start = std::time::Instant::now();
                let result = run_stage_with_retry(&membership, ep, ticket, &lineage, &stage_map).await;
                if let (Some(s), Some(op)) = (store_c, op_c) {
                    match &result {
                        Ok(batches) => {
                            let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
                            s.emit(ExecutionEvent::TaskFinished {
                                operation_id: op,
                                stage_id,
                                task_id,
                                executor_id: host.clone(),
                                status: TaskStatus::Success,
                                duration_ms: start.elapsed().as_millis() as i64,
                                shuffle_read_bytes: 0,
                                shuffle_write_bytes: rows * 8,
                                output_rows: rows,
                            });
                        }
                        Err(_) => {
                            s.emit(ExecutionEvent::TaskFinished {
                                operation_id: op,
                                stage_id,
                                task_id,
                                executor_id: host.clone(),
                                status: TaskStatus::Failed,
                                duration_ms: start.elapsed().as_millis() as i64,
                                shuffle_read_bytes: 0,
                                shuffle_write_bytes: 0,
                                output_rows: 0,
                            });
                        }
                    }
                }
                result
            });
        }
        for r in futures::future::join_all(futs).await {
            r?;
        }
        if let (Some(ref s), Some(ref op)) = (&store, &operation_id) {
            s.emit(ExecutionEvent::StageFinished {
                operation_id: op.clone(),
                stage_id: stage.stage_id as i32,
                status: StageStatus::Complete,
                completion_time_ms: now_ms(),
                shuffle_read_bytes: 0,
                shuffle_write_bytes: 0,
                input_rows: 0,
                output_rows: 0,
            });
        }
        // AQE: sample bucket row counts after producer stage when enabled.
        if aqe_enabled() {
            let mut counts = vec![0usize; np as usize];
            for p in 0..np {
                if let Ok(ep) = cluster.owner_endpoint(p) {
                    if let Ok(batches) = pull_bucket_with_retry(ep, stage.stage_id, p).await {
                        counts[p as usize] = batches.iter().map(|b| b.num_rows()).sum();
                    }
                }
            }
            if let Ok(new_p) = coalesced_partitions(cluster.worker_count(), np, &counts) {
                if new_p < cluster.num_partitions {
                    if let (Some(ref s), Some(ref op)) = (&store, &operation_id) {
                        s.emit(ExecutionEvent::AqeCoalesced {
                            operation_id: op.clone(),
                            stage_id: stage.stage_id as i32,
                            old_partitions: cluster.num_partitions,
                            new_partitions: new_p,
                        });
                    }
                    cluster.num_partitions = new_p;
                }
            }
        }
    }

    // Output stage: per-worker scatter (global agg) or per-partition rendezvous shuffle.
    let scatter_output = output.upstream_stage_ids.is_empty() && output.hash_key_cols.is_empty();
    let mut out = Vec::new();
    refresh_cluster_workers(&mut cluster);
    let w = cluster.num_partitions;
    if scatter_output {
        let mut futs = Vec::new();
        for (i, endpoint) in cluster.workers.iter().enumerate() {
            let ticket = stage_ticket(output, i as u32, w, &cluster, false);
            let membership = cluster.membership.clone();
            let ep = endpoint.clone();
            let lineage = lineage.clone();
            let stage_map = stage_map.clone();
            futs.push(async move {
                run_stage_with_retry(&membership, ep, ticket, &lineage, &stage_map).await
            });
        }
        for r in futures::future::join_all(futs).await {
            out.extend(r?);
        }
    } else {
        // Group partitions by rendezvous owner so concurrent tasks on the same worker do not
        // race on the shared `shuffle_input` registration table.
        let mut by_endpoint: std::collections::BTreeMap<String, Vec<u32>> =
            std::collections::BTreeMap::new();
        for p in 0..w {
            let endpoint = cluster.owner_endpoint(p)?;
            by_endpoint.entry(endpoint).or_default().push(p);
        }
        let mut ep_futs = Vec::new();
        for (endpoint, parts) in by_endpoint {
            let membership = cluster.membership.clone();
            let output = output.clone();
            let cluster = cluster.clone();
            let lineage = lineage.clone();
            let stage_map = stage_map.clone();
            ep_futs.push(async move {
                let mut local = Vec::new();
                for p in parts {
                    let ticket = stage_ticket(&output, p, w, &cluster, false);
                    local.extend(
                        run_stage_with_retry(
                            &membership,
                            endpoint.clone(),
                            ticket,
                            &lineage,
                            &stage_map,
                        )
                        .await?,
                    );
                }
                Ok::<_, Error>(local)
            });
        }
        for r in futures::future::join_all(ep_futs).await {
            out.extend(r?);
        }
    }

    // Evict stage caches on all workers after the query completes.
    for ep in &cluster.workers {
        let _ = clear_worker_stages(ep.clone()).await;
    }

    let data: Vec<RecordBatch> = out.iter().filter(|b| b.num_rows() > 0).cloned().collect();
    let out = if data.is_empty() {
        out.into_iter().take(1).collect()
    } else {
        data
    };
    Ok(unify_schema(out))
}

/// Refresh worker list from live membership (autoscaling between stage barriers).
fn refresh_cluster_workers(cluster: &mut Cluster) {
    let fresh = cluster.membership.endpoints();
    if !fresh.is_empty() && fresh != cluster.workers {
        cluster.workers = fresh;
        cluster.num_partitions = shuffle_partitions(cluster.workers.len());
    }
}

fn unify_schema(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    use std::sync::Arc;
    use weft_loom::arrow::datatypes::{Field, Schema};
    let Some(first) = batches.first() else {
        return batches;
    };
    let fields: Vec<Field> = first
        .schema()
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), f.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    batches
        .into_iter()
        .filter_map(|b| RecordBatch::try_new(schema.clone(), b.columns().to_vec()).ok())
        .collect()
}

fn stage_ticket(
    stage: &StageDef,
    partition_id: u32,
    num_partitions: u32,
    cluster: &Cluster,
    produce: bool,
) -> StageTicket {
    StageTicket {
        stage_id: stage.stage_id,
        partition_id,
        num_partitions,
        upstream_endpoints: if stage.upstream_stage_ids.is_empty() {
            vec![]
        } else {
            cluster.workers.clone()
        },
        stage_sql: stage.sql.clone(),
        plan_fragment: vec![],
        hash_key_cols: stage.hash_key_cols.clone(),
        upstream_stage_ids: stage.upstream_stage_ids.clone(),
        produce,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_snapshots_membership_at_scheduling_time() {
        let membership = Arc::new(StaticMembership::new(vec![
            "a:50561".into(),
            "b:50561".into(),
        ]));
        let cluster = Cluster::from_membership(membership);
        assert_eq!(cluster.worker_count(), 2);
        assert!(cluster.num_partitions >= 2);
    }

    #[test]
    fn owner_endpoint_uses_rendezvous() {
        let cluster = Cluster::new(vec!["a:1".into(), "b:1".into()]);
        let o0 = cluster.owner_endpoint(0).unwrap();
        let o1 = cluster.owner_endpoint(1).unwrap();
        assert!(o0 == "a:1" || o0 == "b:1");
        assert!(o1 == "a:1" || o1 == "b:1");
    }
}
