# Multi-engine ClickBench harness (Weft · Sail · Spark · Spark+Gluten/Velox)

A single, fair, reproducible head-to-head on one machine. Every engine here speaks the **Spark
Connect** protocol, so **one stock PySpark client drives all four** — identical SQL text,
identical dataset, identical client, identical hardware. The only variable is the engine behind
`sc://`. This is also a direct test of Weft's core claim: a *drop-in* Spark replacement should
answer the exact same Spark SQL a real Spark cluster does.

## What runs

| Engine | Server | Port | Client | Notes |
|--------|--------|-----:|--------|-------|
| **Weft** | `weft spark server` (this repo, release build) | 50051 | PySpark Connect | the engine under test |
| **Sail** | `pysail` (LakeSail OSS, Rust) | 50052 | PySpark Connect | the published baseline, re-measured fresh |
| **Spark** | Apache Spark 3.5 Connect server | 15002 | PySpark Connect | vanilla JVM baseline |
| **Spark + Gluten/Velox** | Spark 3.5 + Gluten bundle JAR | 15002 | PySpark Connect | Spark with a native vectorized backend |

Each engine runs **alone** (others' servers stopped) so every run gets the full box.

## Methodology (fairness)

- **Dataset**: the real `hits.parquet` — 99,997,497 rows, 14.78 GB — downloaded once, shared.
- **Queries**: the 43 upstream ClickBench queries in **Spark SQL** (`queries.spark.sql`),
  byte-for-byte identical across all four engines.
- **Timing**: 3 tries/query; **hot = min(try2, try3)**; total = sum of per-query hot. Matches the
  ClickBench contract and `../assert-beats-sail.sh`.
- **Registration** is the one engine-specific bit (DDL, not timed): Spark-family registers the
  parquet with `CREATE OR REPLACE TEMPORARY VIEW hits USING parquet`; Weft uses its DataFusion
  DDL (`CREATE EXTERNAL TABLE … STORED AS PARQUET`) plus the `EventDate` int→date view that
  `weft-bench` already uses. Both expose the same logical `hits` table.
- **Honesty**: a query that errors on an engine is recorded as `null` (a visible gap on the
  site), never silently dropped. Engines that fail to install/boot stay `pending` rather than
  being faked. Per-engine tuning is limited to documented, defensible defaults (shuffle
  partitions = vCPUs; off-heap sized for Gluten) — this is a fair first run, not a tuning war.

## Run it (c6a.4xlarge, Ubuntu 24.04)

```sh
git clone https://github.com/vamzi/weft && cd weft
bash bench/clickbench/multi/bootstrap.sh      # deps + JDK + Rust + install all engines
bash bench/clickbench/multi/run-all.sh        # download 14.78 GB once, run 43×3×4
python3 bench/clickbench/multi/to-site.py     # → site/src/data/benchmarks.json
```

Subset / re-run a single engine:

```sh
ENGINES="weft sail" bash bench/clickbench/multi/run-all.sh
bash bench/clickbench/multi/run-engine.sh weft
```

## Files

| file | role |
|------|------|
| `bootstrap.sh` | system deps + JDK 17 + Rust, then every `install-*.sh` |
| `install-spark.sh` | Apache Spark 3.5 + PySpark client venv |
| `install-gluten.sh` | Gluten/Velox bundle JAR (layered on the Spark install) |
| `install-sail.sh` | `pysail` + PySpark client venv |
| `install-weft.sh` | `cargo build --release -p weft-cli` + PySpark client venv |
| `run-engine.sh` | boot one engine, register `hits`, run 43×3, write `results/<engine>.json` |
| `run-all.sh` | download data, run all four sequentially |
| `runner.py` | engine-agnostic Spark Connect benchmark client |
| `queries.spark.sql` | the 43 ClickBench queries, Spark SQL dialect |
| `to-site.py` | merge `results/*.json` → `site/src/data/benchmarks.json` |

`results/*.json` are git-ignored (intermediate); the committed, published artifact is the site's
`benchmarks.json`.

## Version/compat notes (resolve on the live box)

- Gluten publishes per-(Spark, OS, arch) bundle JARs; the asset name moves per release. Override
  `GLUTEN_JAR` / `GLUTEN_URL` / `GLUTEN_VERSION` if the default 1.3.0 / Spark 3.5 / x86_64 asset
  is renamed.
- `pysail`'s server CLI invocation may differ by version; `run-engine.sh sail` tries both the
  `sail` console script and `python -m pysail`.
- Weft's live Spark Connect server is young; if a specific query can't yet be parsed/executed
  over Connect it is recorded as `null` (honest gap) rather than excluded.
