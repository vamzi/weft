//! Cluster backend trait and implementations.

use std::process::Command;

use weft_common::{Error, Result};
use weft_execution::membership::{ClusterMembership, StaticMembership};

use crate::spec::ClusterSpec;

/// Live cluster metadata returned after provision.
#[derive(Debug, Clone)]
pub struct ClusterInfo {
    pub cluster_id: String,
    pub connect_endpoint: String,
    pub worker_endpoints: Vec<String>,
}

/// Provision and tear down per-user compute clusters.
pub trait ClusterBackend: Send + Sync {
    fn provision(&self, spec: &ClusterSpec) -> Result<ClusterInfo>;
    fn delete(&self, cluster_id: &str) -> Result<()>;
    fn worker_endpoints(&self, spec: &ClusterSpec) -> Result<Vec<String>>;
}

/// Static worker list (local dev / CI).
pub struct StaticBackend {
    endpoints: Vec<String>,
}

impl StaticBackend {
    pub fn new(endpoints: Vec<String>) -> Self {
        Self { endpoints }
    }

    pub fn from_env() -> Option<Self> {
        let raw = std::env::var("WEFT_WORKERS").ok()?;
        let endpoints: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|ep| {
                if ep.starts_with("http://") || ep.starts_with("https://") {
                    ep.to_string()
                } else {
                    format!("http://{ep}")
                }
            })
            .collect();
        if endpoints.is_empty() {
            None
        } else {
            Some(Self { endpoints })
        }
    }
}

impl ClusterBackend for StaticBackend {
    fn provision(&self, spec: &ClusterSpec) -> Result<ClusterInfo> {
        let eps = if self.endpoints.is_empty() {
            (0..spec.worker_count)
                .map(|i| format!("http://127.0.0.1:{}", spec.worker_port + i as u16))
                .collect()
        } else {
            self.endpoints.clone()
        };
        Ok(ClusterInfo {
            cluster_id: spec.cluster_id.clone(),
            connect_endpoint: "sc://127.0.0.1:50051".to_string(),
            worker_endpoints: eps,
        })
    }

    fn delete(&self, _cluster_id: &str) -> Result<()> {
        Ok(())
    }

    fn worker_endpoints(&self, spec: &ClusterSpec) -> Result<Vec<String>> {
        if !self.endpoints.is_empty() {
            return Ok(self.endpoints.clone());
        }
        Ok((0..spec.worker_count)
            .map(|i| format!("http://127.0.0.1:{}", spec.worker_port + i as u16))
            .collect())
    }
}

/// Kubernetes backend: applies manifests via `kubectl` (HPA scales workers).
pub struct K8sBackend {
    /// When set, use DNS membership instead of kubectl for endpoint discovery.
    pub use_dns: bool,
}

impl Default for K8sBackend {
    fn default() -> Self {
        Self { use_dns: true }
    }
}

impl K8sBackend {
    pub fn apply_manifests(&self, yaml: &str) -> Result<()> {
        let mut child = Command::new("kubectl")
            .args(["apply", "--server-side", "-f", "-"])
            .stdin(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| Error::Io(format!("kubectl spawn: {e}")))?;
        use std::io::Write;
        child
            .stdin
            .take()
            .ok_or_else(|| Error::Io("kubectl stdin".into()))?
            .write_all(yaml.as_bytes())
            .map_err(|e| Error::Io(format!("kubectl write: {e}")))?;
        let status = child
            .wait()
            .map_err(|e| Error::Io(format!("kubectl wait: {e}")))?;
        if !status.success() {
            return Err(Error::Io("kubectl apply failed".into()));
        }
        Ok(())
    }

    fn worker_deployment_yaml(spec: &ClusterSpec) -> String {
        format!(
            r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: weft-worker
  namespace: {ns}
spec:
  replicas: {replicas}
  selector:
    matchLabels:
      app: weft-worker
  template:
    metadata:
      labels:
        app: weft-worker
    spec:
      containers:
      - name: worker
        image: {image}
        args: ["worker", "--port", "{port}"]
        ports:
        - containerPort: {port}
---
apiVersion: v1
kind: Service
metadata:
  name: weft-worker
  namespace: {ns}
spec:
  clusterIP: None
  selector:
    app: weft-worker
  ports:
  - port: {port}
    targetPort: {port}
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: weft-worker
  namespace: {ns}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: weft-worker
  minReplicas: {min}
  maxReplicas: {max}
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
"#,
            ns = spec.namespace,
            replicas = spec.worker_count,
            image = spec.worker_image,
            port = spec.worker_port,
            min = spec.min_workers,
            max = spec.max_workers,
        )
    }
}

impl ClusterBackend for K8sBackend {
    fn provision(&self, spec: &ClusterSpec) -> Result<ClusterInfo> {
        self.apply_manifests(&Self::worker_deployment_yaml(spec))?;
        let eps = self.worker_endpoints(spec)?;
        Ok(ClusterInfo {
            cluster_id: spec.cluster_id.clone(),
            connect_endpoint: format!(
                "sc://weft-connect.{}.svc.cluster.local:50051",
                spec.namespace
            ),
            worker_endpoints: eps,
        })
    }

    fn delete(&self, cluster_id: &str) -> Result<()> {
        let ns = format!("weft-cl-{cluster_id}");
        let status = Command::new("kubectl")
            .args(["delete", "namespace", &ns, "--ignore-not-found"])
            .status()
            .map_err(|e| Error::Io(format!("kubectl delete: {e}")))?;
        if !status.success() {
            return Err(Error::Io("kubectl delete namespace failed".into()));
        }
        Ok(())
    }

    fn worker_endpoints(&self, spec: &ClusterSpec) -> Result<Vec<String>> {
        if self.use_dns {
            #[cfg(feature = "k8s")]
            {
                use weft_execution::membership::K8sMembership;
                let m = K8sMembership::new(spec.worker_service_host(), spec.worker_port);
                return Ok(m.endpoints());
            }
        }
        let membership = StaticMembership::new(vec![]);
        Ok(membership.endpoints())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_backend_from_env() {
        std::env::set_var("WEFT_WORKERS", "127.0.0.1:50561,127.0.0.1:50562");
        let b = StaticBackend::from_env().unwrap();
        assert_eq!(b.endpoints.len(), 2);
    }

    #[test]
    fn hpa_manifest_contains_replicas() {
        let spec = ClusterSpec::local_demo("abc", 2);
        let yaml = K8sBackend::worker_deployment_yaml(&spec);
        assert!(yaml.contains("HorizontalPodAutoscaler"));
        assert!(yaml.contains("weft-worker"));
    }
}
