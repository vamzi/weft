//! `weft-clustermgr` — the EKS cluster lifecycle operator.
//!
//! Owns the `WeftCluster` custom resource. For each one, the reconcile loop materializes a Spark
//! Connect **driver** Deployment, an N-replica Arrow Flight **worker** StatefulSet, a headless
//! Service for worker discovery (consumed by `weft-execution`'s `K8sMembership`), a ClusterIP
//! Service for the Connect endpoint, an HPA, and a per-cluster ServiceAccount (IRSA) for scoped S3.
//!
//! The gateway creates/patches `WeftCluster` objects; the operator does the rest. This module
//! freezes the **spec/status contract** (the CRD shape) and the **state machine**, ahead of the
//! `kube-rs` implementation.

/// The desired state of a Weft compute cluster — the `spec` of the `WeftCluster` CRD.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeftClusterSpec {
    /// Display name.
    pub name: String,
    /// Minimum worker replicas (autoscale floor; also a warm-pool size).
    pub worker_min: u32,
    /// Maximum worker replicas (autoscale ceiling).
    pub worker_max: u32,
    /// Pod size class (drives CPU/memory requests + `WEFT_*` tuning env).
    pub worker_size: String,
    /// Idle minutes before auto-stop (0 = never).
    pub idle_timeout_s: u32,
    /// IAM role ARN to bind to the cluster's ServiceAccount via IRSA (scoped S3 access).
    pub iam_role_arn: Option<String>,
    /// Ephemeral job cluster — torn down after its run completes.
    pub job_cluster: bool,
}

/// The lifecycle phase of a cluster — the operator's state machine and the `status.phase` of the CRD.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Accepted, not yet acted on.
    Pending,
    /// Pods/Services being created; waiting for readiness.
    Provisioning,
    /// Driver Connect endpoint is ready; serving queries.
    Running,
    /// Tearing down resources.
    Terminating,
    /// All resources removed.
    Terminated,
    /// Reconcile failed.
    Error,
}

impl Phase {
    /// Stable string used in the CRD status and the `clusters.state` column (mirrors
    /// [`weft_meta::cluster_state`]).
    pub fn as_str(&self) -> &'static str {
        match self {
            Phase::Pending => "PENDING",
            Phase::Provisioning => "PROVISIONING",
            Phase::Running => "RUNNING",
            Phase::Terminating => "TERMINATING",
            Phase::Terminated => "TERMINATED",
            Phase::Error => "ERROR",
        }
    }

    /// The legal next phases from this one (the operator only transitions along these edges).
    pub fn allowed_next(&self) -> &'static [Phase] {
        match self {
            Phase::Pending => &[Phase::Provisioning, Phase::Error],
            Phase::Provisioning => &[Phase::Running, Phase::Error, Phase::Terminating],
            Phase::Running => &[Phase::Terminating, Phase::Error],
            Phase::Terminating => &[Phase::Terminated, Phase::Error],
            Phase::Terminated => &[],
            Phase::Error => &[Phase::Terminating, Phase::Terminated],
        }
    }

    /// Whether `next` is a legal transition from `self`.
    pub fn can_transition_to(&self, next: Phase) -> bool {
        self.allowed_next().contains(&next)
    }
}

/// The observed state of a cluster — the `status` of the `WeftCluster` CRD.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeftClusterStatus {
    /// Current lifecycle phase.
    pub phase: Phase,
    /// Ready worker replicas.
    pub ready_workers: u32,
    /// In-cluster Spark Connect endpoint (e.g. `weft-cluster-<id>.weft-data.svc:50051`), set once Running.
    pub connect_endpoint: Option<String>,
    /// Last human-readable status/error message.
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path_transitions_are_legal() {
        assert!(Phase::Pending.can_transition_to(Phase::Provisioning));
        assert!(Phase::Provisioning.can_transition_to(Phase::Running));
        assert!(Phase::Running.can_transition_to(Phase::Terminating));
        assert!(Phase::Terminating.can_transition_to(Phase::Terminated));
    }

    #[test]
    fn illegal_transitions_rejected() {
        assert!(!Phase::Pending.can_transition_to(Phase::Running));
        assert!(!Phase::Terminated.can_transition_to(Phase::Running));
        assert!(Phase::Terminated.allowed_next().is_empty());
    }

    #[test]
    fn phase_string_matches_meta() {
        assert_eq!(Phase::Running.as_str(), weft_meta::cluster_state::RUNNING);
        assert_eq!(
            Phase::Terminated.as_str(),
            weft_meta::cluster_state::TERMINATED
        );
    }
}
