# ClickBench harness for Weft

Weft's north-star benchmark. Mirrors the [ClickBench](https://github.com/ClickHouse/ClickBench)
entry contract so results are independently reproducible, and so we can submit an entry
under `ClickHouse/ClickBench/weft/`.

## Contract (from ClickBench)

- Dataset: single `hits` table, 99,997,497 rows × 105 cols, `hits.parquet` ≈ 14.78 GB.
- Each of the 43 queries runs **3 times**; the **hot** number is `min(try2, try3)`; the
  **cold** number is try 1 (caches dropped before it).
- Default hardware: AWS `c6a.4xlarge` (16 vCPU), 500 GB gp2, Ubuntu.
- `query` contract: read SQL on stdin → result to stdout → fractional **seconds on the last
  line of stderr** → non-zero exit on error.

## Files

| file | role |
|------|------|
| `install` | install the `weft` server + a PySpark client into a venv |
| `benchmark.sh` | top-level driver: download → start server → run 43 queries ×3 → emit JSON |
| `query` | run one SQL statement against `sc://localhost:50051`, time it |
| `queries.sql` | the 43 queries (vendored from upstream; a few included here as anchors) |
| `assert-beats-sail.sh` | CI gate: fail if total hot runtime > Sail's baseline |
| `run-correctness.sh` | run the 43 queries and diff results vs a DuckDB/Spark oracle |
| `template.json` | static system metadata for the results file |

## Sail baseline to beat (c6a.4xlarge, 2026-05-11)

Total hot ≈ **56.3 s**; the heavy hitters are Q24 (~10.2 s), Q34/Q35 (~5 s each).
See `../../docs/architecture.md` for the per-query target table.
