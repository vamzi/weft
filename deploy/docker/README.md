# Weft container images

Production Dockerfiles for the Weft platform images. Everything builds from the Cargo
workspace at the repository root; the **build context for every image is the repo
root**, e.g. `docker build -f deploy/docker/<name>.Dockerfile -t <ref> .`.

These images target the Kubernetes secure-redesign, where each per-user "cluster"
is a hardened pod running the Spark Connect server, and the gateway provisions those
pods by applying declarative manifests with `kubectl` (see
`crates/weft-orchestrator/`).

| File | Image | Binary / crate | Entry | Port |
|------|-------|----------------|-------|------|
| `connect-server.Dockerfile` | `connect-server` | `weft` (crate `weft-cli`) | `weft spark server --port 50051` | 50051 (gRPC) |
| `worker.Dockerfile` | `worker` | `weft` (same binary, rebased on `connect-server`) | `weft worker` | 50561 (Flight) |
| `gateway.Dockerfile` | `gateway` | `weft-gateway` (crate `weft-gateway`) + bundled `kubectl` + SPA | `weft-gateway` | 8080 (REST/WS) |

> The `worker` and `connect-server` images are the **same `weft` binary** — `worker`
> is `connect-server` with a different default command, so they share every layer.
> The control plane drives this with `WEFT_CLUSTER_IMAGE` (driver) and
> `WEFT_WORKER_IMAGE` (worker); you may point both at `connect-server` and override
> the worker command instead of shipping a separate `worker` image.

## Build

```sh
# from the repo root
TAG=$(git rev-parse --short HEAD)

# 1) Spark Connect driver (also the source image for the worker)
docker build -f deploy/docker/connect-server.Dockerfile \
  -t weft/connect-server:$TAG .

# 2) Worker — rebases connect-server, no recompile
docker build -f deploy/docker/worker.Dockerfile \
  --build-arg CONNECT_IMAGE=weft/connect-server:$TAG \
  -t weft/worker:$TAG .

# 3) Control-plane gateway (bakes the web SPA + pinned kubectl)
docker build -f deploy/docker/gateway.Dockerfile \
  -t weft/gateway:$TAG .
```

BuildKit is required (the `# syntax=` directive + the per-Dockerfile
`*.dockerignore` files in this directory). Use `docker buildx` for multi-arch
(`--platform linux/amd64,linux/arm64`); `kubectl` is selected per `TARGETARCH` with
both checksums pinned.

### How the build stages work

- **Rust:** `rust:1.90-bookworm` (matches `rust-toolchain.toml`) + `cargo-chef` for
  dependency-layer caching across source edits. No `protoc` is needed — `weft-proto`
  compiles the vendored Spark Connect protos with pure-Rust `protox`. No
  `libssl-dev`/`pkg-config` either — TLS is `rustls`/`ring`, and `zstd`/`brotli` are
  vendored C/Rust statically linked into the binary.
- **Runtime:** `gcr.io/distroless/cc-debian12:nonroot` — glibc + libgcc only, no
  shell, no package manager. The `nonroot` user is uid/gid **65532**.
- **Gateway SPA:** a `node:20` stage runs `npm ci && npm run build` (`web/`) and the
  result is copied to `/usr/local/share/weft/web` (`WEFT_WEB_DIR`).
- **Gateway kubectl:** downloaded from `dl.k8s.io` at a pinned version and verified
  against the official SHA-256 before it is copied in.

## Security posture

The images are built to satisfy PodSecurity **`restricted`** admission — the exact
shape the orchestrator stamps onto every cluster pod
(`crates/weft-orchestrator/src/manifests.rs`):

| Control | How the image satisfies it |
|---------|----------------------------|
| `runAsNonRoot` / `runAsUser: 65532` | `USER 65532:65532`; binary at `/usr/local/bin/weft[-gateway]`, owned by root, world-readable/executable |
| `readOnlyRootFilesystem: true` | nothing writes to the rootfs; all scratch is redirected to mounts (below) |
| `capabilities.drop: ["ALL"]` | plain TCP listener; no privileged syscalls, no `setcap` |
| `allowPrivilegeEscalation: false` | no setuid binaries; distroless base |
| `seccompProfile: RuntimeDefault` | no blocked syscalls in the default profile |
| `automountServiceAccountToken: false` (cluster pods) | pods reach AWS via IRSA, not the K8s API |
| No baked credentials | catalog/storage auth is per-cluster least-privilege **IRSA**; the AWS CLI is intentionally **not** in `connect-server`/`worker` |

### Read-only rootfs ⇒ emptyDir scratch is mandatory

The engine stages sort/aggregation **spill** and Delta/catalog scratch under
`std::env::temp_dir()` (i.e. `$TMPDIR`). With a read-only rootfs the process cannot
write anywhere except mounted volumes, so the pod **must** provide:

```yaml
# connect-server / worker pods (the orchestrator already emits this)
securityContext:                 # pod
  runAsNonRoot: true
  runAsUser: 65532
  runAsGroup: 65532
  fsGroup: 65532
  seccompProfile: { type: RuntimeDefault }
containers:
- name: connect
  securityContext:               # container
    readOnlyRootFilesystem: true
    allowPrivilegeEscalation: false
    capabilities: { drop: ["ALL"] }
    seccompProfile: { type: RuntimeDefault }
  volumeMounts:
  - { name: tmp,   mountPath: /tmp }                 # $TMPDIR — scratch + actual spill
  - { name: spill, mountPath: /var/lib/weft/spill }  # WEFT_SPILL_DIR (dedicated volume)
volumes:
- { name: tmp,   emptyDir: {} }
- { name: spill, emptyDir: {} }
```

The image sets `TMPDIR=/tmp`, `HOME=/tmp`, and `WEFT_SPILL_DIR=/var/lib/weft/spill`
to match. (Today the engine spills to `$TMPDIR`; `WEFT_SPILL_DIR` is set for parity
with the manifest and forward-compatibility. Set `WEFT_MEMORY_LIMIT_BYTES` from the
pod memory limit so large aggregations spill to that emptyDir instead of OOM-killing.)

The **gateway** needs the same `/tmp` emptyDir: `kubectl` writes a discovery/HTTP
cache under `$HOME/.kube` (`HOME=/tmp`), and the gateway's embedded engine spills
under `$TMPDIR`. It authenticates to the K8s API with its in-pod ServiceAccount
token, so the provisioner ServiceAccount needs RBAC to create namespaces,
ServiceAccounts, ConfigMaps, NetworkPolicies, ResourceQuotas, LimitRanges,
Services, Deployments, and StatefulSets.

Run standalone (outside K8s) with the same posture:

```sh
docker run --read-only --tmpfs /tmp --tmpfs /var/lib/weft/spill \
  -p 50051:50051 weft/connect-server:$TAG

docker run --read-only --tmpfs /tmp -p 8080:8080 \
  -e WEFT_JWT_SECRET=... -e WEFT_ADMIN_PASSWORD=... weft/gateway:$TAG
```

## Gateway configuration

`weft-gateway` reads its config from the environment (see
`weft-gateway::server::serve`). Production-required (the binary **refuses to boot**
with break-glass defaults unless `WEFT_DEV_MODE=1`):

- `WEFT_JWT_SECRET` — session-JWT signing key, **≥ 32 bytes**, from a K8s Secret.
- `WEFT_ADMIN_PASSWORD` — break-glass admin password (must differ from `admin`).

Commonly set:

- `WEFT_GATEWAY_ADDR` (default `0.0.0.0:8080`, set in the image), `WEFT_ADMIN_USER`
  (default `admin`), `WEFT_WEB_DIR` (baked at `/usr/local/share/weft/web`),
  `WEFT_PUBLIC_HOST`.

Cluster orchestration (Kubernetes redesign):

- `WEFT_ORCHESTRATOR=k8s` selects the kubectl backend (set in the image).
- `WEFT_KUBECTL` — kubectl path (set to the bundled, pinned binary).
- `WEFT_CLUSTER_IMAGE` → the `connect-server` image ref; `WEFT_WORKER_IMAGE` → the
  `worker` image ref.
- `WEFT_CLUSTER_IRSA_ROLE_PREFIX`, `WEFT_CLUSTER_SECRET_CLASS`,
  `WEFT_CLUSTER_EGRESS_CIDRS`, `WEFT_CLUSTER_IDLE_SECS`.
- `WEFT_BIN` — local-process fallback when not on K8s.

Catalog/AWS: `AWS_REGION`, `WEFT_CATALOG_CONF`, `WEFT_HMS_URI`. Legacy EC2 backend
(`WEFT_CLUSTER_AMI/_SG/_WEFT_URL/...`, `WEFT_AWS_BIN`) applies only when
`WEFT_ORCHESTRATOR != k8s`. The legacy AWS-CLI persistence path (`WEFT_DDB_TABLE`,
`WEFT_WORKSPACE_S3`, `WEFT_*_FILE`) is best-effort and degrades to in-memory if the
`aws` binary is absent — it is **not** bundled here; prefer RDS in the redesign.

The Spark Connect server (`connect-server`/`worker`) reads `WEFT_CATALOG_CONF`
(also via `--catalog-conf`), `AWS_REGION`, `WEFT_SPILL_DIR`/`TMPDIR`, and the engine
tunables `WEFT_MEMORY_LIMIT_BYTES`, `WEFT_TARGET_PARTITIONS`, `WEFT_BATCH_SIZE`,
`WEFT_COALESCE_BATCHES`, `WEFT_REPARTITION_AGGREGATIONS`.

## Mapping to Helm values (`deploy/helm/weft`)

The chart's control-plane Deployments and the `WeftCluster`/orchestrator templates
consume these images through `image` values. Expected shape:

```yaml
gateway:
  image: { repository: <registry>/weft/gateway, tag: <git-sha> }
  env:
    WEFT_ORCHESTRATOR: k8s
    WEFT_CLUSTER_IMAGE: <registry>/weft/connect-server:<git-sha>
    WEFT_WORKER_IMAGE:  <registry>/weft/worker:<git-sha>
  secrets:                      # -> K8s Secret -> env
    WEFT_JWT_SECRET, WEFT_ADMIN_PASSWORD

# Compute images the gateway hands to the orchestrator (no static Deployment;
# pods are materialized from WeftCluster specs):
connectServer:
  image: { repository: <registry>/weft/connect-server, tag: <git-sha> }
worker:
  image: { repository: <registry>/weft/worker, tag: <git-sha> }
```

The hardened securityContext + emptyDir scratch documented above are produced by the
orchestrator for cluster pods; apply the equivalent `securityContext` and a `/tmp`
emptyDir to the **gateway** Deployment template in the chart.

---

`clustermgr`, `scheduler`, and `pyworker` images (listed in the platform plan) are
not part of this set — add them as their crates leave skeleton state.
