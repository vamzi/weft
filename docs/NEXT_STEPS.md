# Weft — Resume Guide & Next Steps

Paused 2026-06-24. This doc is the single place to pick up from a fresh session. For the full
design rationale see [`architecture.md`](architecture.md); for issue-level progress see
[`ISSUES.md`](ISSUES.md).

## TL;DR — where it stands

**Weft is a working drop-in Spark replacement that beats Sail's published ClickBench on real
hardware.** Spark Connect server → DataFusion 54 engine (`Loom`), validated on the real
14.78 GB / 100 M-row ClickBench on a real `c6a.4xlarge`: **43/43 queries, hot total 45.51 s vs
Sail's 56.3 s (~19% faster)**, same hardware/dataset/methodology.

| Phase 1 item | Status |
|---|---|
| 1.1 Beat Sail (DataFusion tuning) | ✅ 45.51 s vs 56.3 s |
| 1.2 Reproducible ClickBench entry | ✅ `bench/clickbench/{install,benchmark.sh}` |
| 1.3 Delta + Iceberg reads | ✅ version-safe resolvers, tested |
| 1.5a Distributed MVP (single-stage Flight) | ✅ driver/worker over Arrow Flight, tested |
| **1.4 Push the margin** | ⬜ NEXT (see below) |
| **1.5b Distributed shuffle** | ⬜ NEXT |
| Phase 2 (streaming / Unity / K8s / HVM2 gate) | ⬜ later |

All committed (~18 commits), all gates green: `cargo build/test`, `clippy -D warnings`, `fmt`.

## Resume quickstart (local)

Requires **Rust 1.90** (pinned in `rust-toolchain.toml`) — `protoc` NOT needed (we use `protox`).
```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check
# local benchmark harnesses (synthetic data; fast):
cargo run -p weft-bench -- clickbench       # 43/43 engine-direct
cargo run -p weft-bench -- clickbench-grpc   # 43/43 through the live server over gRPC
cargo run -p weft-bench -- correctness       # gRPC == engine-direct + ground-truth anchors
cargo run -p weft-bench -- tpch              # TPC-H Q1/Q3/Q5/Q6/Q10
# run the server, then point any PySpark client at sc://localhost:50051:
cargo run --bin weft -- spark server --port 50051
```

## The real-data benchmark on AWS

The instance is **terminated** (we don't pay EBS while paused). To re-run the real ClickBench:
```sh
scratchpad/c6a.sh up      # provision c6a.4xlarge, install Rust, download 14.78 GB, build, run
scratchpad/c6a.sh run     # after edits: start (if stopped), sync source, incremental build, re-run
scratchpad/c6a.sh stop    # pause compute (EBS-only ~$1.66/day) between runs
scratchpad/c6a.sh down    # terminate + delete SG/keypair
```
- AWS account **810738286322**, region us-west-2, on-demand $0.612/hr (+ EBS). Budget ceiling $50;
  ~$0.90 spent so far. `c6a.sh` writes state to `scratchpad/c6a.state`.
- `c6a.sh` runs `bench/clickbench/benchmark.sh` semantics: `weft-bench clickbench-grpc --data <hits.parquet>`
  with `WEFT_MEMORY_LIMIT_BYTES=26000000000` (spill pool) and `WEFT_TARGET_PARTITIONS`.
- **Note:** `scratchpad/` is session-scoped; if `c6a.sh` is gone on resume, it's reconstructable from
  this doc + git history (it's a thin wrapper around `aws ec2` + scp + `benchmark.sh`).

## Next steps (in priority order)

### 1.4 — Push the margin (needs paid c6a runs)
Current bottlenecks at 45.51 s: **Q32 8.08 s** (`GROUP BY WatchID, ClientIP` — WatchID near-unique,
~100 M groups), **Q28 4.0 s** (REGEXP_REPLACE), **Q33/Q34 ~3.6 s** (high-card GROUP BY).
1. **Cheap first (one c6a run):** try DataFusion knobs in `weft-loom::Engine::new` —
   `WEFT_TARGET_PARTITIONS` sweep, larger `execution.batch_size`, `aggregate` settings, and check
   whether `schema_force_view_types` already covers the string keys. Measure; commit what helps.
2. **If config plateaus:** assess a native/strategy operator for the ~100 M-group aggregation
   (adaptive cardinality, two-phase bypass). Honest expectation: DataFusion 54's hash-agg is
   already strong, so ROI is uncertain — this may confirm we're near the DataFusion ceiling, in
   which case the durable margin comes from the **HVM2 GPU path (Phase 2)**, not CPU operators.
3. Re-benchmark with `c6a.sh run`; update `bench/clickbench/results` + `ISSUES.md`.

### 1.5b — Distributed shuffle (no AWS cost; pure local Rust)
Build on `crates/weft-execution/src/flight.rs` (single-stage driver/worker already works):
1. Add `datafusion-proto = "54"` to ship **serialized physical-plan fragments** (not SQL strings)
   in the Flight ticket — `PhysicalPlanNode::try_from_physical_plan` / `try_into_physical_plan`.
2. Split a plan at shuffle boundaries (driver): stage graph; partition the data; workers exchange
   partitions via Flight `do_get`/`do_exchange` (the shuffle data plane).
3. Test: a 2-worker GROUP BY with a shuffle, asserting the same result as single-node.
4. Wire `weft-execution::run(Mode::Distributed)` and a `weft spark server --cluster` path.

### Phase 1 exit loose ends
- Compute **median per-query speedup vs Spark** (need a Spark baseline run or use Sail's published
  per-query Spark numbers) — exit criterion is > 8.4×.
- **PySpark parity** for the *official* harness (today the live-server bench uses our Rust gRPC
  client): handle `SqlCommand.input` (PySpark's `spark.sql` path) + `AnalyzePlan(Schema)` with
  Arrow→Spark type conversion, then validate with stock `pyspark-client`.
- Open the upstream PR: copy `results/<date>/c6a.4xlarge.json` + `template.json` under
  `ClickHouse/ClickBench/weft/`.

### Phase 2 (per architecture.md §4)
Structured Streaming + Kafka source; Unity Catalog (Iceberg REST + temp credentials); K8s deploy;
and the **HVM2/Bend go/no-go gate** — `weft-hvm` is scaffolded + feature-gated off; the gate is
≥2× over Loom on a bounded parallel workload, or shelve as research. (HVM2 wins 0/43 ClickBench by
design — its moat is a *separate* benchmark.)

## Key learnings / gotchas (don't re-discover these)

- **DataFusion 54, not 43** — DF43 had a `group_column.rs` panic on high-card string GROUP BY that
  corrupted the gRPC stream. `Engine::new` uses `RuntimeEnvBuilder` (DF54 API) + ClickBench tuning
  (`pushdown_filters`/`reorder_filters`/`binary_as_string`/`schema_force_view_types`).
- **gRPC must allow 128 MB+** and **chunk** result batches (`weft-connect`: `MAX_MSG`/`CHUNK_ROWS`);
  the default 4 MB limit fails real results.
- **`WEFT_MEMORY_LIMIT_BYTES`** sets a DataFusion spill pool — required so heavy queries spill
  instead of OOM-killing on a 32 GB box.
- **tonic versions split on purpose:** `weft-connect`/`weft-proto` use **0.12** (Spark Connect);
  `weft-execution` uses **0.14** (matches `arrow-flight 58`). They don't exchange tonic types.
  `arrow_flight::error::FlightError::Tonic` wraps `Box<Status>`.
- **Lakehouse:** the `deltalake`/`iceberg` crates pin DataFusion 53 → can't compose with our 54.
  We instead resolve tables to their active Parquet file list (`weft_datasource::delta_active_files`
  / `iceberg_active_files`) and use DataFusion 54's native reader. v1 limits: no deletion vectors /
  MoR deletes / partition pruning / checkpoint Parquet.
- **`protox`** compiles the vendored Spark protos (no `protoc`); `build.rs` skips macOS AppleDouble
  `._*.proto` sidecars (a careless `tar` adds them and breaks the build — use `COPYFILE_DISABLE=1`).
- **Results JSON is gitignored** (`bench/**/results/*.json`) — don't let a stale one get committed
  and shipped to the instance (it produced a misleading "result" once).

## Map of the code
`crates/`: `weft-connect` (Spark Connect gRPC server) · `weft-proto` (generated protos via protox) ·
`weft-loom` (DataFusion 54 engine + lakehouse register) · `weft-datasource` (Delta/Iceberg file
resolvers) · `weft-execution` (Flight driver/worker) · `weft-bench` (ClickBench/TPC-H/correctness
harness) · `weft-{plan,analyzer,optimizer,physical,catalog,hvm,common,cli}` (mostly scaffold).
`bench/clickbench/` (the entry) · `scratchpad/c6a.sh` (AWS runner) · full plan:
`~/.claude/plans/you-are-a-principal-floofy-donut.md`.
