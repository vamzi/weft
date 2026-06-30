//! The distributed driver: orchestrate a stage DAG across workers.
//!
//! A query is expressed as a topologically-ordered list of [`StageDef`]s. Each *producer* stage
//! runs once per shuffle partition on the worker that owns that partition (rendezvous hashing),
//! hash-partitions its output, and caches the buckets. The output stage runs on every partition
//! owner, pulling upstream buckets and returning results.
//!
//! Shuffle partition count defaults to worker count but can be overridden via
//! `WEFT_SHUFFLE_PARTITIONS` (like `spark.sql.shuffle.partitions`).

use std::collections::HashSet;
use std::sync::Arc;

use weft_common::{Error, Result};
use weft_loom::arrow::record_batch::RecordBatch;

use crate::flight::{clear_worker_stages, run_stage_on_worker};
use crate::membership::{ClusterMembership, StaticMembership};
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
    membership: Arc<dyn ClusterMembership>,
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

    /// Wrap an existing trait object reference.
    pub fn from_membership_ref(membership: &dyn ClusterMembership) -> Self {
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
    run_stages(cluster, &plan.into_stages()).await
}

pub async fn run_distributed_with_membership(
    membership: Arc<dyn ClusterMembership>,
    plan: &DistributedPlan,
) -> Result<Vec<RecordBatch>> {
    run_stages(&Cluster::from_membership(membership), &plan.into_stages()).await
}

pub async fn run_stages_with_membership(
    membership: Arc<dyn ClusterMembership>,
    stages: &[StageDef],
) -> Result<Vec<RecordBatch>> {
    run_stages(&Cluster::from_membership(membership), stages).await
}

pub async fn run_stages(cluster: &Cluster, stages: &[StageDef]) -> Result<Vec<RecordBatch>> {
    let w = cluster.num_partitions;
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
        let mut futs = Vec::new();
        for (i, endpoint) in cluster.workers.iter().enumerate() {
            futs.push(run_stage_on_worker(
                endpoint.clone(),
                stage_ticket(stage, i as u32, w, cluster, true),
            ));
        }
        for r in futures::future::join_all(futs).await {
            r?;
        }
    }

    // Output stage: one invocation per shuffle partition on its rendezvous owner.
    let mut out = Vec::new();
    for p in 0..w {
        let endpoint = cluster.owner_endpoint(p)?;
        let part = run_stage_on_worker(
            endpoint,
            stage_ticket(output, p, w, cluster, false),
        )
        .await?;
        out.extend(part);
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
