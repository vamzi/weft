//! The Kubernetes [`ClusterBackend`]: apply declarative manifests via `kubectl`.
//!
//! We deliberately shell out to `kubectl apply -f -` with the manifests as JSON on **stdin** (argv
//! exec, no `sh -c`) rather than embedding a large in-process Kubernetes client. The manifests are
//! built as typed JSON in [`crate::manifests`], so attacker-influenced values are inert data, never
//! command tokens. `kubectl` resolves cluster credentials from its in-pod ServiceAccount /
//! kubeconfig the usual way.

use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::manifests::{build_cluster_manifests, namespace_name};
use crate::{ClusterBackend, ClusterSpec};
use async_trait::async_trait;
use serde_json::{json, Value};

/// Applies cluster manifests with `kubectl`. The binary path is operator-controlled (never from a
/// request); the namespace prefix bounds what this backend may delete.
pub struct K8sBackend {
    kubectl: String,
}

impl Default for K8sBackend {
    fn default() -> Self {
        Self::from_env()
    }
}

impl K8sBackend {
    /// Build from the environment: `WEFT_KUBECTL` (default `kubectl` on `$PATH`).
    pub fn from_env() -> Self {
        Self {
            kubectl: std::env::var("WEFT_KUBECTL").unwrap_or_else(|_| "kubectl".to_string()),
        }
    }

    /// Run `kubectl <args>` with optional stdin, returning stdout or a combined error string. No
    /// shell is involved — args are passed as an argv vector.
    async fn run(&self, args: &[&str], stdin: Option<&[u8]>) -> Result<String, String> {
        let mut cmd = Command::new(&self.kubectl);
        cmd.args(args);
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        if stdin.is_some() {
            cmd.stdin(std::process::Stdio::piped());
        }
        let mut child = cmd.spawn().map_err(|e| format!("spawn kubectl: {e}"))?;
        if let Some(bytes) = stdin {
            let mut si = child.stdin.take().ok_or("kubectl stdin unavailable")?;
            si.write_all(bytes)
                .await
                .map_err(|e| format!("write kubectl stdin: {e}"))?;
            si.shutdown()
                .await
                .map_err(|e| format!("close kubectl stdin: {e}"))?;
        }
        let out = child
            .wait_with_output()
            .await
            .map_err(|e| format!("wait kubectl: {e}"))?;
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).into_owned())
        } else {
            Err(format!(
                "kubectl {}: {}",
                args.first().copied().unwrap_or(""),
                String::from_utf8_lossy(&out.stderr).trim()
            ))
        }
    }
}

#[async_trait]
impl ClusterBackend for K8sBackend {
    async fn provision(&self, spec: &ClusterSpec) -> Result<(), String> {
        let items = build_cluster_manifests(spec);
        // Wrap as a v1 List so a single `kubectl apply` is one atomic-ish server-side apply pass.
        let list: Value = json!({ "apiVersion": "v1", "kind": "List", "items": items });
        let body = serde_json::to_vec(&list).map_err(|e| format!("serialize manifests: {e}"))?;
        self.run(
            &["apply", "--server-side", "--field-manager=weft-gateway", "-f", "-"],
            Some(&body),
        )
        .await
        .map(|_| ())
    }

    async fn terminate(&self, id: &str) -> Result<(), String> {
        // Delete only the cluster's own namespace; the `weft-cl-` prefix is enforced here and by a
        // cluster-side admission policy, so this can't touch anything else. Cascade GC (owner refs)
        // tears down the workload.
        let ns = namespace_name(id);
        debug_assert!(ns.starts_with("weft-cl-"));
        self.run(
            &[
                "delete",
                "namespace",
                &ns,
                "--ignore-not-found",
                "--wait=false",
            ],
            None,
        )
        .await
        .map(|_| ())
    }
}
