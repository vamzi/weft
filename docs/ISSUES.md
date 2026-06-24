# First issues

The three issues that bootstrap Phase 0. Open these on GitLab in order; #1 and #3 can run in
parallel, #2 depends on #1.

## Progress

- **#1 — DONE (core slice).** A real tonic `SparkConnectService` is live: vendored protos
  compiled with `protox` (no `protoc`), `ExecutePlan(SQL)` runs through DataFusion and streams
  Arrow IPC + `ResultComplete`; `AnalyzePlan(SparkVersion)` + `Config` handle session
  bootstrap. Validated end-to-end by `crates/weft-connect/tests/select_one.rs` (boots the
  server, runs `SELECT 1` over gRPC, decodes Arrow, asserts `1`).
  **Remaining for full PySpark parity:** the `SqlCommand` `input` path (PySpark's `spark.sql`
  uses it over the deprecated `sql` field), `AnalyzePlan(Schema)` with Arrow→Spark type
  conversion, real `Config` get/set, and reattach buffering. Validated with a Rust gRPC client
  (not PySpark) to avoid the local Python 3.14 / pyarrow wheel risk.
- **#2 — STARTED.** DataFusion is embedded in `weft-loom` (`Engine::sql`); TPC-H subset
  correctness harness still to come.
- **#3 — scaffolded** (`bench/clickbench`), not yet wired to the live server.

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
