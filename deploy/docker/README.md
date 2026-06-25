# Weft container images

Dockerfiles for the platform images, built and pushed to ECR by `deploy/terraform` + CI:

- `connect-server` — the `weft spark server` Spark Connect driver (from `weft-connect`/`weft-cli`).
- `worker` — the `weft worker` Arrow Flight worker (from `weft-execution`/`weft-cli`).
- `gateway` — the control-plane REST/WS API (`weft-gateway`).
- `clustermgr` — the `WeftCluster` operator (`weft-clustermgr`).
- `scheduler` — the job/workflow engine (`weft-scheduler`).
- `pyworker` — the Python UDF sidecar (`weft-pyworker` + the `runtime/` Python image).

All Rust images share a cargo-chef build stage off the workspace; the pyworker image layers the
Python runtime (`runtime/`). Dockerfiles land with the Wave-1 infra agent.
