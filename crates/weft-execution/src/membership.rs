//! Cluster membership: where the driver finds its workers.
//!
//! Today the driver takes a **static** `Vec<String>` of worker endpoints ([`Role::Driver`]).
//! On EKS, worker pods come and go (autoscaling, restarts), so the static list must become a
//! live view. This module introduces the [`ClusterMembership`] seam the distributed driver reads
//! at stage-scheduling time:
//!
//! - [`StaticMembership`] — the current behavior, kept for tests and local runs.
//! - `K8sMembership` (Wave 1, behind a `kube` dependency) — resolves the worker headless Service
//!   via DNS-SRV or an EndpointSlice watch.
//!
//! Crucially, membership also defines a **stable partition→worker assignment** (consistent
//! hashing) so a worker restart doesn't reshuffle ownership mid-query. The driver's `run_stages`
//! is refactored to consume this in the engine workstream; the trait is frozen here first.
//!
//! [`Role::Driver`]: crate::Role

#[cfg(feature = "k8s")]
pub mod k8s;

#[cfg(feature = "k8s")]
pub use k8s::K8sMembership;

/// A worker endpoint (`host:port`) the driver can dial over Arrow Flight.
pub type WorkerEndpoint = String;

/// A live view of the worker set backing a distributed cluster.
pub trait ClusterMembership: Send + Sync {
    /// Snapshot the current worker endpoints. Called at the start of stage scheduling so the
    /// partition count tracks live workers.
    fn endpoints(&self) -> Vec<WorkerEndpoint>;

    /// The endpoint that owns `partition` out of `num_partitions`, using a **stable** assignment
    /// so a membership change doesn't reshuffle existing ownership. Default: rendezvous
    /// (highest-random-weight) hashing over the current endpoints.
    fn owner_of(&self, partition: u32, num_partitions: u32) -> Option<WorkerEndpoint> {
        let eps = self.endpoints();
        if eps.is_empty() || num_partitions == 0 {
            return None;
        }
        // Rendezvous hashing: assign the partition to the endpoint with the highest combined hash.
        eps.into_iter()
            .max_by_key(|ep| rendezvous_weight(ep, partition))
    }
}

/// FNV-1a over `(endpoint, partition)` — cheap, dependency-free weight for rendezvous hashing.
fn rendezvous_weight(endpoint: &str, partition: u32) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for b in endpoint.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(PRIME);
    }
    for b in partition.to_le_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h
}

/// A fixed worker list — the pre-EKS behavior, kept for tests and single-host runs.
pub struct StaticMembership {
    endpoints: Vec<WorkerEndpoint>,
}

impl StaticMembership {
    /// Wrap a fixed list of worker endpoints.
    pub fn new(endpoints: Vec<WorkerEndpoint>) -> Self {
        Self { endpoints }
    }
}

impl ClusterMembership for StaticMembership {
    fn endpoints(&self) -> Vec<WorkerEndpoint> {
        self.endpoints.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_membership_returns_endpoints() {
        let m = StaticMembership::new(vec!["a:1".into(), "b:1".into()]);
        assert_eq!(m.endpoints(), vec!["a:1".to_string(), "b:1".to_string()]);
    }

    #[test]
    fn assignment_is_stable_under_membership_change() {
        let full = StaticMembership::new(vec!["a:1".into(), "b:1".into(), "c:1".into()]);
        let n = 12u32;
        let before: Vec<_> = (0..n).map(|p| full.owner_of(p, n)).collect();

        // Remove one worker; every partition that did NOT belong to the removed worker keeps its
        // owner (rendezvous hashing's stability property — no global reshuffle).
        let reduced = StaticMembership::new(vec!["a:1".into(), "c:1".into()]);
        for p in 0..n {
            let owner_before = before[p as usize].clone().unwrap();
            let owner_after = reduced.owner_of(p, n).unwrap();
            if owner_before != "b:1" {
                assert_eq!(
                    owner_before, owner_after,
                    "partition {p} reshuffled unexpectedly"
                );
            } else {
                assert_ne!(owner_after, "b:1"); // reassigned away from the removed node
            }
        }
    }

    #[test]
    fn empty_membership_has_no_owner() {
        let m = StaticMembership::new(vec![]);
        assert_eq!(m.owner_of(0, 4), None);
    }
}
