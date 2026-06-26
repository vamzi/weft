# syntax=docker/dockerfile:1.7
###############################################################################
# Weft control-plane gateway  —  `weft-gateway`
#
# The single public entry point: REST/WS API + the web SPA + the cluster
# provisioner. In the Kubernetes redesign the gateway provisions per-user clusters
# by APPLYING DECLARATIVE MANIFESTS WITH kubectl (crates/weft-orchestrator: argv
# `kubectl apply --server-side -f -`, manifests on stdin — no shell, so a catalog
# value can never become a command). Hence this image bundles `kubectl`, pinned and
# checksum-verified.
#
# Runs non-root (uid 65532) and tolerates a read-only root filesystem the same way
# the cluster pods do (writable scratch via an emptyDir at /tmp; HOME=/tmp so
# kubectl's discovery cache has somewhere to live).
#
# Build context is the repository root:
#   docker build -f deploy/docker/gateway.Dockerfile -t weft/gateway:<tag> .
###############################################################################

# ---- web: build the React/Vite SPA the gateway serves (WEFT_WEB_DIR) ---------
FROM node:20-bookworm-slim AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build          # tsc --noEmit && vite build  ->  /web/dist

# ---- chef: toolchain + cargo-chef -------------------------------------------
FROM rust:1.90-bookworm AS chef
WORKDIR /build
RUN cargo install cargo-chef --locked --version ^0.1

# ---- planner ----------------------------------------------------------------
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- builder: cook deps, then build `weft-gateway` --------------------------
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
# weft-gateway pulls in weft-orchestrator (the kubectl K8s backend), weft-catalog-*,
# datafusion, tonic, axum — all pure-Rust at build time (no protoc, no openssl-dev).
RUN cargo build --release --locked -p weft-gateway --bin weft-gateway \
 && strip target/release/weft-gateway

# ---- kubectl: pinned version, official SHA-256 verified ---------------------
FROM debian:bookworm-slim AS kubectl
ARG KUBECTL_VERSION=v1.30.5
# TARGETARCH is provided automatically by BuildKit (amd64 / arm64).
ARG TARGETARCH=amd64
RUN set -eux; \
    apt-get update; apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    # Checksums published at dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/<arch>/kubectl.sha256
    case "${TARGETARCH}" in \
      amd64) sha=b8aa921a580c3d8ba473236815de5ce5173d6fbfa2ccff453fa5eef46cc5ee7a ;; \
      arm64) sha=efc594857f9255fc33bcda9409b8862a3b47ce5f4e09d51c3427b85dd769b9b9 ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSLo /kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl"; \
    echo "${sha}  /kubectl" | sha256sum --check --strict; \
    chmod 0555 /kubectl; \
    /kubectl version --client=true >/dev/null

# ---- runtime ----------------------------------------------------------------
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

LABEL org.opencontainers.image.title="weft-gateway" \
      org.opencontainers.image.description="Weft control-plane API gateway + SPA + cluster provisioner" \
      org.opencontainers.image.source="https://gitlab.com/weftlabs/weft"

# REST/WS API (WEFT_GATEWAY_ADDR default 0.0.0.0:8080).
EXPOSE 8080

USER 65532:65532

# Read-only rootfs: kubectl writes its discovery/HTTP cache under $HOME/.kube, and
# the gateway's embedded engine spills under $TMPDIR — both must resolve to a
# writable emptyDir. The gateway authenticates to the K8s API with its in-pod
# ServiceAccount token (mounted by K8s), not a kubeconfig.
ENV TMPDIR=/tmp \
    HOME=/tmp \
    WEFT_GATEWAY_ADDR=0.0.0.0:8080 \
    WEFT_WEB_DIR=/usr/local/share/weft/web \
    WEFT_ORCHESTRATOR=k8s \
    WEFT_KUBECTL=/usr/local/bin/kubectl \
    RUST_BACKTRACE=1

COPY --from=builder /build/target/release/weft-gateway /usr/local/bin/weft-gateway
COPY --from=kubectl /kubectl /usr/local/bin/kubectl
COPY --from=web /web/dist /usr/local/share/weft/web

# Bind address comes from WEFT_GATEWAY_ADDR; `--routes` prints the frozen API surface.
ENTRYPOINT ["/usr/local/bin/weft-gateway"]

###############################################################################
# Runtime configuration (read by weft-gateway::server::serve and AppState):
#
#   REQUIRED in production (the binary refuses to boot with break-glass defaults
#   unless WEFT_DEV_MODE=1):
#     WEFT_JWT_SECRET       session-JWT signing key, >= 32 bytes (from a Secret)
#     WEFT_ADMIN_PASSWORD   break-glass admin password (must differ from "admin")
#   Common:
#     WEFT_GATEWAY_ADDR     listen addr (default 0.0.0.0:8080; set above)
#     WEFT_ADMIN_USER       break-glass admin username (default "admin")
#     WEFT_WEB_DIR          SPA dir (baked here at /usr/local/share/weft/web)
#     WEFT_PUBLIC_HOST      host advertised in a cluster's connect endpoint
#   Cluster orchestration (Kubernetes redesign):
#     WEFT_ORCHESTRATOR     "k8s" selects the kubectl backend (set above)
#     WEFT_KUBECTL          kubectl path (set above to the bundled, pinned binary)
#     WEFT_CLUSTER_IMAGE    driver image  (-> connect-server:<tag>)
#     WEFT_WORKER_IMAGE     worker image  (-> worker:<tag>)
#     WEFT_CLUSTER_IRSA_ROLE_PREFIX / WEFT_CLUSTER_SECRET_CLASS /
#     WEFT_CLUSTER_EGRESS_CIDRS / WEFT_CLUSTER_IDLE_SECS
#     WEFT_BIN              local-process fallback (the `weft` binary path)
#   Catalog + AWS:
#     AWS_REGION, WEFT_CATALOG_CONF, WEFT_HMS_URI
#   Legacy EC2 backend (only if WEFT_ORCHESTRATOR != k8s):
#     WEFT_CLUSTER_AMI / _SG / _WEFT_URL / _SUBNET / _KEY / _PRIVATE /
#     _INSTANCE_PROFILE, WEFT_AWS_BIN
#   Legacy durable persistence via the AWS CLI (best-effort; degrades to in-memory
#   if the `aws` binary is absent, which it is here — prefer RDS in the redesign):
#     WEFT_DDB_TABLE, WEFT_WORKSPACE_S3, WEFT_CONNECTIONS_FILE,
#     WEFT_NOTEBOOKS_FILE, WEFT_QUERIES_FILE
#
# Read-only rootfs — required writable mount:
#     volumes:      - name: tmp  emptyDir: {}
#     volumeMounts: - name: tmp  mountPath: /tmp     # $TMPDIR + $HOME/.kube cache
#
# Standalone:
#   docker run --read-only --tmpfs /tmp -p 8080:8080 \
#     -e WEFT_JWT_SECRET=... -e WEFT_ADMIN_PASSWORD=... weft/gateway:<tag>
###############################################################################
