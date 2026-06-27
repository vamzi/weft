//! `weft-orchestrator` — the seam between the control plane and a cluster's real compute.
//!
//! The gateway provisions per-user "clusters" through a [`ClusterBackend`]. The Kubernetes backend
//! ([`K8sBackend`]) builds **declarative manifests** ([`manifests`]) and applies them with `kubectl`
//! — there is no shell anywhere on the path, so a connection field can never inject a command (the
//! class of bug that made the EC2 `user_data` path an RCE). Pods are hardened (non-root, read-only
//! root fs, dropped caps, seccomp, no auto-mounted token), per-cluster-namespaced, network-isolated,
//! and quota-bounded; AWS access is per-cluster least-privilege IRSA.
//!
//! The backend is selected at runtime (e.g. `WEFT_ORCHESTRATOR=k8s`) so the existing process/EC2
//! paths keep working during migration and rollback is a flag flip.

pub mod manifests;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use manifests::{build_cluster_manifests, namespace_name};

/// A request to materialize one cluster's compute. All fields are typed; the only attacker-influenced
/// input, [`ClusterSpec::catalog_conf`], is a list of pairs that becomes an inert ConfigMap value —
/// never a shell token. The gateway derives `service_account` / `iam_role_arn` server-side (never
/// from a request body) so a caller can't bind another tenant's identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSpec {
    /// Stable cluster id (must be a DNS-1123 label; the namespace is `weft-cl-<id>`).
    pub id: String,
    /// Driver image running `weft spark server`.
    pub image: String,
    /// Worker image running `weft worker`.
    pub worker_image: String,
    /// AWS region for catalog/storage access.
    pub region: String,
    /// Spark Connect port (50051).
    pub port: u16,
    /// Worker autoscale floor.
    pub worker_min: u32,
    /// Worker autoscale ceiling.
    pub worker_max: u32,
    /// Per-pod CPU (an integer string, e.g. `"1"`).
    pub cpu: String,
    /// Per-pod memory (e.g. `"2Gi"`).
    pub memory: String,
    /// IRSA ServiceAccount name.
    pub service_account: String,
    /// IRSA role ARN annotation (server-derived; `None` in dev / no-IRSA).
    pub iam_role_arn: Option<String>,
    /// Catalog config as typed `(key, value)` pairs — inert; serialized into one ConfigMap value.
    pub catalog_conf: Vec<(String, String)>,
    /// Secrets Store CSI `SecretProviderClass` name for catalog credentials (mounted files), if any.
    pub secret_provider_class: Option<String>,
    /// NetworkPolicy egress CIDR allowlist (S3/Glue/HMS endpoints).
    pub egress_cidrs: Vec<String>,
}

impl ClusterSpec {
    /// The in-cluster Spark Connect endpoint for this cluster's driver Service (set once the pod is
    /// ready). Stable DNS — no IP interpolation.
    pub fn endpoint(&self) -> String {
        let ns = namespace_name(&self.id);
        format!(
            "sc://weft-cl-{}.{ns}.svc.cluster.local:{}",
            self.id, self.port
        )
    }
}

/// A pluggable compute backend. Implementations must never feed `spec` fields to a shell.
#[async_trait]
pub trait ClusterBackend: Send + Sync {
    /// Provision (or converge) the cluster's compute. Idempotent where the backend allows it.
    async fn provision(&self, spec: &ClusterSpec) -> Result<(), String>;
    /// Tear the cluster's compute down by id.
    async fn terminate(&self, id: &str) -> Result<(), String>;
}

mod k8s;
pub use k8s::K8sBackend;
