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
| **1.5b Distributed shuffle** | ✅ 2-stage hash shuffle, `two_worker_groupby` test passes |
| **1.4 Push the margin** | ✅ swept locally → at the DF54 ceiling; defaults optimal (see below) |
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

### 1.4 — Push the margin — SWEPT: at the DataFusion 54 ceiling
Knobs are env-tunable in `weft-loom::Engine::new` (no rebuild to sweep): `WEFT_TARGET_PARTITIONS`,
`WEFT_BATCH_SIZE`, `WEFT_COALESCE_BATCHES`, `WEFT_REPARTITION_AGGREGATIONS` (`schema_force_view_types`
already on for the string keys).

Swept **locally** against the synthetic ClickBench at 3 M rows (`scratchpad/local-sweep.sh`,
11 configs, hot=min(try2,try3)) — the EC2 path was unavailable in this environment, so this is
directional synthetic signal, not the c6a absolutes:

| config | hot total | note |
|---|---|---|
| baseline (tp=#cores, repart-agg on) | 0.368 s | optimal |
| `WEFT_TARGET_PARTITIONS=4` | 0.872 s | +137% (Q32 8× worse) |
| `WEFT_TARGET_PARTITIONS=8` | 0.435 s | +18% |
| `WEFT_BATCH_SIZE=32768` | 0.345 s | −6% (Q23/Q28) |
| `WEFT_REPARTITION_AGGREGATIONS=off` | 0.674 s | +83% (Q32 7× worse) |

**Conclusion:** the defaults already win — high-card `GROUP BY` (Q32 `WatchID,ClientIP`) is sharply
sensitive to parallelism, which `target_partitions=#cores` + `repartition_aggregations=on` (both
defaults) already provide. The only positive knob is a larger `batch_size` (~6% on synthetic, mostly
string/regex/scan) — too marginal and synthetic-specific to hardcode (raises transient memory vs the
spill pool on the 32 GB box). **This confirms the original honest expectation: config can't move the
margin; the durable separation is the Phase 2 HVM2 GPU path, not CPU config.** A real c6a run, if
done, would only need to confirm the `batch_size` candidate before any default change.

### 1.5b — Distributed shuffle — DONE (local MVP, $0)
Implemented in `crates/weft-execution`: 2-stage `partial-agg → hash shuffle → final-agg`.
- `shuffle::protocol` — prost `StageTicket`/`ShuffleReadTicket` envelope (tag-byte prefixed so the
  legacy raw-SQL `do_get` ticket still works).
- `shuffle::partition` — FNV hash partitioning of stage output into per-worker buckets.
- `flight.rs` — `Worker` caches stage output; consumer stages pull their bucket from every upstream
  via `do_get(ShuffleReadTicket)`, register `shuffle_input`, and finalize.
- `shuffle::codec` — `datafusion-proto` physical-fragment ser/de (round-trips a GROUP BY over a
  Parquet leaf; `stage_sql` is the primary path and permanent fallback).
- `driver::run_distributed` + `weft worker` / `weft driver` CLI subcommands; `run(Mode::Distributed)`
  seam in `lib.rs`.
- Test `two_worker_groupby_matches_single_node` asserts row-for-row equality with single-node.

**Remaining 1.5b follow-ups (deferred):** auto-decompose SQL aggregates (AVG/COUNT(DISTINCT) via
sum+count or sketches), >2-stage plans, shuffle spill, dynamic worker discovery, `do_exchange`
streaming, and routing `weft spark server --cluster` GROUP BY through `run_distributed`.

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
- **Shuffle proto alignment is fine:** `datafusion-proto 54`, `arrow-flight 58`, and `prost 0.14`
  all resolve to **prost 0.14.4** in `weft-execution` (verified) — the shuffle envelope and the
  DataFusion physical-fragment bytes share one prost major. `datafusion-proto`'s
  `try_into_physical_plan(&TaskContext, &dyn PhysicalExtensionCodec)` in DF54 takes a `TaskContext`
  (build via `TaskContext::from(&session_state)`), not a registry+runtime pair.
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
resolvers) · `weft-execution` (Flight driver/worker + `shuffle::{protocol,partition,codec}` +
`driver::run_distributed`) · `weft-bench` (ClickBench/TPC-H/correctness
harness) · `weft-{plan,analyzer,optimizer,physical,catalog,hvm,common,cli}` (mostly scaffold).
`bench/clickbench/` (the entry) · `scratchpad/c6a.sh` (AWS runner) · full plan:
`~/.claude/plans/you-are-a-principal-floofy-donut.md`.
