# First issues

The three issues that bootstrap Phase 0. Open these on GitLab in order; #1 and #3 can run in
parallel, #2 depends on #1.

## ✅ Phase 0 EXIT MET (2026-06-24)

All 43 ClickBench queries run to completion on the **real 14.78 GB / 100 M-row dataset**, on a
real `c6a.4xlarge` (us-west-2), driven through the live `weft-connect` Spark Connect server over
gRPC — **43/43 passing, hot total 52.851 s** (Sail's published baseline: 56.3 s; same hardware,
same dataset, 3 tries, hot = min(try2,try3)).

Honest framing: this is *parity-class*, not a Weft-engineered win — it rides on DataFusion 54
(newer than Sail's pinned build) + a warm reused server. Beating Sail with a real margin is
**Phase 1's** job (native heavy operators). Getting here required: DataFusion 43→54 (fixed the
high-card `GROUP BY` `group_column` panic), gRPC 128 MB + Arrow chunking (fixed the oversized-
message failures), and a bounded spill pool (`WEFT_MEMORY_LIMIT_BYTES`) so the heavy queries
spill instead of OOM-killing on 32 GB. Heaviest queries today: Q23 8.98 s (`SELECT *`+LIKE+sort),
Q32 8.07 s and Q33/Q34 ~3.5 s (high-card GROUP BY), Q28 4.0 s (regex) — the Phase 1 targets.

## Progress

- **#1 — DONE (core slice).** A real tonic `SparkConnectService` is live: vendored protos
  compiled with `protox` (no `protoc`), `ExecutePlan(SQL)` runs through DataFusion and streams
  Arrow IPC + `ResultComplete`; `AnalyzePlan(SparkVersion)` + `Config` handle session
  bootstrap. Validated end-to-end by `crates/weft-connect/tests/select_one.rs` (boots the
  server, runs `SELECT 1` over gRPC, decodes Arrow, asserts `1`). **The full 43-query
  ClickBench suite also runs over this live server** via `weft-bench clickbench-grpc`
  (`CREATE EXTERNAL TABLE` + queries -> Parquet scan -> Arrow IPC, **43/43**).
  **Remaining for full PySpark parity:** the `SqlCommand` `input` path (PySpark's `spark.sql`
  uses it over the deprecated `sql` field), `AnalyzePlan(Schema)` with Arrow→Spark type
  conversion, real `Config` get/set, and reattach buffering. Validated with a Rust gRPC client
  (not PySpark) to avoid the local Python 3.14 / pyarrow wheel risk.
- **#2 — DONE (subset).** DataFusion embedded in `weft-loom`; `weft-bench tpch` runs the
  Q1/Q3/Q5/Q6/Q10 subset on synthetic tables — **5/5 pass** with structurally-correct row
  counts (Q1's 6 returnflag×linestatus groups, Q5's 6-table ASIA-region join). Gated in CI
  (`tpch-coverage`). Oracle-diff correctness (vs DuckDB) still to add.
- **#3 — local coverage DONE.** `weft-bench` runs all **43/43** ClickBench queries through
  Loom/DataFusion on a synthetic `hits` table (`cargo run -p weft-bench -- clickbench`),
  emitting a ClickBench-format `results.json`; gated in CI (`clickbench-coverage`). The
  official `c6a.4xlarge` run (real 14 GB via the Spark Connect client) is still to wire.

## #1 — `weft-connect`: Spark Connect gRPC skeleton + session + `ExecutePlan(SQL)→Arrow`

Stand up the tonic gRPC server. `weft-proto` compiles vendored `apache/spark` protos via
`protox` (no `protoc` needed). Implement:
- `Config` (Set/Get/GetAll/Unset/IsModifiable);
- `AnalyzePlan` (`SparkVersion`, `Schema`);
- `ExecutePlan` for the `Sql` relation, returning Arrow IPC batches + `ResultComplete`;
- `ReattachExecute`/`ReleaseExecute` (PySpark 3.5+ sets `reattachable=true`).

**Definition of done:** with unmodified PySpark,
```python
SparkSession.builder.remote("sc://localhost:50051").getOrCreate().sql("SELECT 1").show()
```
returns `1`.

## #2 — `weft-loom`: embed DataFusion behind the warp IR; pass a 10-query TPC-H subset

Lower `weft-plan` → DataFusion `LogicalPlan`; wire `weft-datasource` Parquet reads.
**Definition of done:** TPC-H Q1/Q3/Q5/Q6/Q10 on SF1 return results matching a Spark oracle.
**Blocked by:** #1.

## #3 — `bench/clickbench`: reproducible harness that runs all 43 queries and emits `results.json`

Port the ClickBench shared-driver contract (3 tries/query; hot = min of try 2/3). Produce
`results/<date>/c6a.4xlarge.json` and print the total-vs-Sail delta.
**Definition of done:** a nightly run on a c6a.4xlarge produces a valid results JSON — the
scoreboard exists from day one, even before we win.
