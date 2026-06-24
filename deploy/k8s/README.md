# Kubernetes deployment (Phase 2)

Cloud-native deployment of Weft: a Helm chart + operator for the driver/worker topology
(`weft-execution`), with the Spark Connect server fronted by a Service.

Planned:
- `helm install weft ./chart` → one driver + N stateless workers (start in seconds, MB idle).
- Arrow Flight data plane between workers for shuffle.
- Multi-tenant: one warm server, many concurrent Spark Connect sessions, shared scheduler —
  the lane Sail leaves open (its ClickBench is single-process-per-query).

Not started; tracked under the Phase 2 milestone.
