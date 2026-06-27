# Weft control-plane gateway — Helm chart

A production Helm chart for `weft-gateway`, the single public entrypoint of the Weft platform. The
gateway is an axum REST/WebSocket service that authenticates users and **provisions per-user compute
"clusters" at runtime** by applying hardened manifests with `kubectl` (in-cluster credentials).

Because the gateway talks to the Kubernetes API directly, the security boundary is RBAC + admission
control, not a separate operator. This chart ships that boundary.

> The skeleton `Chart.yaml` already present in this directory is intentionally left untouched by this
> change set. Everything else in the chart is new. See **Chart metadata** at the bottom.

---

## What this chart installs

| Object | File | Purpose |
| --- | --- | --- |
| `Deployment` (gateway) | `templates/gateway-deployment.yaml` | Hardened, non-root, read-only rootfs; `kubectl` staged by an init container |
| `ServiceAccount` (gateway) | `templates/gateway-serviceaccount.yaml` | IRSA-annotated; token mounted (the gateway calls the API server) |
| `ClusterRole` ×2 + `ClusterRoleBinding` | `templates/gateway-rbac.yaml` | Scoped, least-privilege RBAC (details below) |
| `Service` (gateway) | `templates/gateway-service.yaml` | ClusterIP on `:8080` |
| `Secret` (optional) | `templates/gateway-secret.yaml` | JWT key — **referenced, never inlined** |
| `ValidatingAdmissionPolicy` + binding | `templates/namespace-admission-policy.yaml` | Pins gateway namespace create/delete to the `weft-cl-` prefix |
| Reference cluster graph | `templates/cluster-reference.yaml` (gated) and `examples/cluster-namespace.yaml` (static) | The exact per-cluster objects the gateway applies at runtime |

The per-cluster objects (`weft-cl-<id>` namespace, driver `Deployment`, worker `StatefulSet`,
`Service`s, `NetworkPolicy`s, `ResourceQuota`, `LimitRange`, `SecretProviderClass`) are **created by
the gateway at runtime, not by `helm install`**. They are included here only as reference material so
operators can audit the exact, fully-hardened shape.

---

## Security model

### 1. Per-cluster namespaces are isolated and hardened
Each cluster lives in its own `weft-cl-<id>` namespace labeled
`pod-security.kubernetes.io/enforce=restricted`. Every pod runs:

- `runAsNonRoot: true`, `runAsUser: 65532`
- `readOnlyRootFilesystem: true` (writable `emptyDir`s for `/tmp` and spill only)
- `capabilities: { drop: [ALL] }`
- `seccompProfile: { type: RuntimeDefault }`
- `allowPrivilegeEscalation: false`
- `automountServiceAccountToken: false`

### 2. The gateway's RBAC is scoped — and never reads Secrets cluster-wide
Two ClusterRoles (`templates/gateway-rbac.yaml`):

- **`<release>-namespaces`** (the only standing `ClusterRoleBinding`): `create/get/list/watch/update/patch/delete`
  on **Namespaces**, plus the minimum to *bootstrap* a fresh namespace — create `RoleBindings`,
  `ResourceQuotas`, `LimitRanges` — and `bind` on **exactly one** ClusterRole
  (`<release>-cluster-manage`, via `resourceNames`). The `bind` restriction means the gateway can
  create RoleBindings but can **never escalate** to `admin`/`cluster-admin`.
- **`<release>-cluster-manage`** (NOT bound cluster-wide): the namespaced workload surface —
  `Deployments`, `StatefulSets`, `Services`, `ConfigMaps`, `ServiceAccounts`, `RoleBindings`,
  `NetworkPolicies`, `ResourceQuotas`, `LimitRanges`, `SecretProviderClasses`, plus read-only
  `pods`/`pods/log` and `events`.

When the gateway provisions a cluster it creates a `RoleBinding` **inside that namespace** binding its
own SA to `<release>-cluster-manage`. So the gateway has **zero standing access** to any workload
namespace until it explicitly, per-namespace, grants itself a scoped role. Crucially, **no role grants
`get/list/watch` on `secrets` anywhere** — there is no cluster-wide Secret read.

### 3. Admission control pins the namespace name
RBAC can grant "create Namespaces" but cannot constrain the *name* on `CREATE`. A
`ValidatingAdmissionPolicy` (`templates/namespace-admission-policy.yaml`) closes the gap: a
`matchCondition` scopes evaluation to the gateway SA (`system:serviceaccount:<ns>:<sa>`), and a CEL
`validation` requires `metadata.name` to start with `weft-cl-` on both `CREATE` and `DELETE`. Other
principals are unaffected. Requires **Kubernetes ≥ 1.30**. `failurePolicy: Fail` (fail closed).

### 4. The JWT signing key is referenced, never inlined
`templates/gateway-secret.yaml` does **not** put a key in the chart. In production, set
`gateway.jwt.existingSecret` to a Secret you created out-of-band; the Deployment reads it via
`secretKeyRef`. A break-glass `--set-string gateway.jwt.value=...` path exists for bootstrapping, and
only then is a Secret materialised (with `helm.sh/resource-policy: keep`). Never commit a real value to
`values.yaml`.

```sh
kubectl -n weft-system create secret generic weft-gateway-jwt \
  --from-literal=jwt-secret="$(openssl rand -hex 32)"
helm install weft deploy/helm/weft -n weft-system \
  --set gateway.jwt.existingSecret=weft-gateway-jwt
```

### 5. Egress is default-deny + allowlist
Each cluster namespace gets a `default-deny` `NetworkPolicy` and a narrow allow policy: ingress only
from the gateway namespace on the Spark Connect port (`50051`); egress only to DNS (CoreDNS), S3, Glue,
the Hive Metastore, and intra-namespace (driver ↔ workers). CIDRs are configurable under
`cluster.egress.*`.

### 6. Catalog credentials via Secrets Store CSI, mounted 0400
The cluster `SecretProviderClass` (`secrets-store.csi.x-k8s.io/v1`) pulls catalog credentials from AWS
Secrets Manager and `secretObjects`-syncs them to a Secret, which the driver mounts read-only with
`defaultMode: 256` (octal `0400`). IRSA on the per-cluster ServiceAccount scopes which secrets/data the
tenant can reach.

---

## Installing

```sh
# 1. Create the gateway namespace and JWT secret.
kubectl create namespace weft-system
kubectl -n weft-system create secret generic weft-gateway-jwt \
  --from-literal=jwt-secret="$(openssl rand -hex 32)"

# 2. Install (override registry + IRSA ARNs for your account).
helm install weft deploy/helm/weft -n weft-system \
  --set image.registry=123456789012.dkr.ecr.us-west-2.amazonaws.com \
  --set gateway.jwt.existingSecret=weft-gateway-jwt \
  --set gateway.serviceAccount.irsaRoleArn=arn:aws:iam::123456789012:role/weft-gateway \
  --set cluster.serviceAccount.irsaRoleArn=arn:aws:iam::123456789012:role/weft-cluster

# 3. Audit the exact per-cluster shape the gateway will apply (renders nothing into the cluster).
helm template weft deploy/helm/weft \
  --set clusterReference.render=true -s templates/cluster-reference.yaml
```

Prerequisites: Kubernetes ≥ 1.30 (admission policy), the Secrets Store CSI driver + AWS provider, and
the EKS Pod Identity / IRSA webhook.

---

## Values reference

### Top level
| Key | Default | Description |
| --- | --- | --- |
| `nameOverride` / `fullnameOverride` | `""` | Resource-name overrides |
| `region` | `us-west-2` | AWS region (`AWS_REGION`) for control plane + clusters |
| `image.registry` | ECR placeholder | Registry prepended to every image repository |
| `image.pullPolicy` | `IfNotPresent` | Pull policy for all containers |
| `image.pullSecrets` | `[]` | Image pull secrets |

### Gateway
| Key | Default | Description |
| --- | --- | --- |
| `gateway.replicaCount` | `2` | Stateless replicas |
| `gateway.image.repository` / `.tag` | `weft/gateway` / `""` (→ appVersion) | Gateway image |
| `gateway.kubectl.enabled` / `.image.*` | `true` / `bitnami/kubectl:1.30` | Stage `kubectl` via init container |
| `gateway.service.type` / `.port` | `ClusterIP` / `8080` | Service |
| `gateway.containerPort` | `8080` | `WEFT_GATEWAY_ADDR` bind port |
| `gateway.serviceAccount.irsaRoleArn` | placeholder | IRSA role for the gateway |
| `gateway.serviceAccount.name` / `.annotations` | `""` / `{}` | SA name / extra annotations |
| `gateway.jwt.existingSecret` | `""` | **Required**: Secret holding the JWT key |
| `gateway.jwt.secretKey` | `jwt-secret` | Key within that Secret |
| `gateway.jwt.value` | `""` | Break-glass only; pass via `--set-string`, never commit |
| `gateway.admin.user` / `.passwordSecretKey` | `admin` / `admin-password` | Bootstrap admin |
| `gateway.config.workspaceS3` | `s3://weft-workspace` | `WEFT_WORKSPACE_S3` |
| `gateway.config.ddbTable` | `weft-control-plane` | `WEFT_DDB_TABLE` (blank for RDS builds) |
| `gateway.config.clusterNamespacePrefix` | `weft-cl-` | Must match `admissionPolicy.namespacePrefix` |
| `gateway.config.clusterIdleSecs` | `1800` | Idle teardown |
| `gateway.config.extraEnv` | `[]` | Extra env vars |
| `gateway.resources` | 250m/256Mi → 2/1Gi | Requests/limits |
| `gateway.podSecurityContext` / `gateway.securityContext` | restricted | Hardening (override with care) |
| `gateway.nodeSelector` / `tolerations` / `affinity` / `podAnnotations` | `{}`/`[]` | Scheduling |

### RBAC & admission
| Key | Default | Description |
| --- | --- | --- |
| `rbac.create` | `true` | Create the gateway ClusterRoles + binding |
| `rbac.clusterManageRoleName` | `""` (→ `<release>-cluster-manage`) | Name of the per-ns role |
| `admissionPolicy.enabled` | `true` | Install the namespace-prefix policy (needs k8s ≥ 1.30) |
| `admissionPolicy.namespacePrefix` | `weft-cl-` | Allowed namespace prefix |
| `admissionPolicy.failurePolicy` | `Fail` | Fail closed |

### Per-cluster defaults (`cluster.*`) — applied by the gateway at runtime
| Key | Default | Description |
| --- | --- | --- |
| `cluster.namespacePrefix` | `weft-cl-` | Namespace prefix |
| `cluster.podSecurityStandard` | `restricted` | PSS enforce label |
| `cluster.sparkConnectPort` | `50051` | Driver port |
| `cluster.image.connectServer` / `.worker` | `weft/connect-server` / `weft/worker` | Cluster images |
| `cluster.serviceAccount.irsaRoleArn` | placeholder | IRSA role for cluster pods |
| `cluster.egress.dnsNamespace` | `kube-system` | Namespace allowed for DNS egress |
| `cluster.egress.s3Cidrs` / `glueCidrs` / `hmsCidrs` / `hmsPort` / `extraCidrs` | see `values.yaml` | Egress allowlist |
| `cluster.resourceQuota.*` | 8/32Gi → 16/64Gi, 20 pods | `ResourceQuota` |
| `cluster.limitRange.*` | see `values.yaml` | `LimitRange` |
| `cluster.sizeClasses.{small,medium,large}` | see `values.yaml` | Worker replicas + resources per size class |
| `cluster.csi.*` | AWS, `weft-catalog-creds`, mode `0400` | Secrets Store CSI for catalog creds |

### Reference rendering
| Key | Default | Description |
| --- | --- | --- |
| `clusterReference.render` | `false` | Render `templates/cluster-reference.yaml` for auditing only |
| `clusterReference.id` | `demo` | Cluster id → `weft-cl-<id>` |
| `clusterReference.sizeClass` | `small` | Size class to render |

---

## Chart metadata

The `Chart.yaml` in this directory predates this change set and is left unmodified per the change
scope (it still carries `version: 0.0.0` and an operator-centric description). When cutting a release,
bump `version`/`appVersion` and align the description with this gateway-applies-manifests design. The
chart name is `weft`; all templates derive their names from it via `templates/_helpers.tpl`.
