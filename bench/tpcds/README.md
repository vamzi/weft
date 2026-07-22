# TPC-DS harness

Runs the full TPC-DS suite (Q1–Q99) through Weft for correctness and timing.

Data is generated with DuckDB’s `tpcds` extension (`CALL dsdgen(sf = …)`) and exported as
**Parquet** so the same CLI can scale from CI (`sf=0.01`) to large factors on bigger hardware.
Fixed substitution parameters match DuckDB’s `tpcds_queries()` (qualification-style binds).

When `duckdb` is on `PATH`, every result is cross-checked against DuckDB over the same Parquet
files (3-significant-figure cell normalize). CI enforces a pass-set ratchet in
[`baseline.json`](baseline.json) — coverage can only hold or rise.

## Requirements

- `duckdb` CLI (data gen + oracle). Install from [duckdb.org](https://duckdb.org/docs/installation/)
  or the GitHub release zip (`duckdb_cli-linux-amd64.zip`).

## Usage

```bash
# CI / local smoke (default --sf 0.01)
cargo run -p weft-bench -- tpcds
cargo run -p weft-bench -- tpcds --sf 0.01 --data /tmp/weft-tpcds-sf0.01

# Single query debug
WEFT_TPCDS_ONLY=Q66 WEFT_TPCDS_DEBUG=1 cargo run -p weft-bench -- tpcds --sf 0.01
```

### Large scale factors (external hardware)

The CLI accepts any DuckDB-supported scale factor. Parquet stays on disk under `--data`;
plan for disk ≈ raw TPC-DS size (SF100 ~100 GB, SF1000 ~1 TB) plus headroom, and enough
RAM/workers for the joins. Not a CI target on GitHub-hosted runners.

```bash
cargo run -p weft-bench --release -- tpcds --sf 100  --data /data/tpcds-sf100
cargo run -p weft-bench --release -- tpcds --sf 500  --data /data/tpcds-sf500
cargo run -p weft-bench --release -- tpcds --sf 1000 --data /data/tpcds-sf1000
```

Generation is idempotent when `store_sales.parquet` exists and `scale_factor.txt` matches `--sf`.

## Ratchet

If a PR improves the pass set, re-run the suite and copy the printed `passed_json=…` list into
`baseline.json` (keep numeric order). The gate fails if any previously green query regresses.

## Notes

- Engineering harness (timing + oracle), not an audited TPC Fair Use publication.
- Distributed TPC-DS is out of scope for this harness (see TPC-H `tpch-distributed` for the pattern).
