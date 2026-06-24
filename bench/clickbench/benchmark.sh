#!/usr/bin/env bash
# Reproducible Weft ClickBench entry — run on a prepared Linux box (run ./install first).
# Builds Weft, ensures the 14.78 GB hits.parquet is present, then runs all 43 queries through
# the live weft-connect Spark Connect server over gRPC (3 tries each, hot = min of try 2/3) and
# writes a ClickBench-format results/<machine>.json.
#
# Env: BENCH_DATA (parquet path), WEFT_MEMORY_LIMIT_BYTES (spill-pool size; default 26 GB),
#      WEFT_TARGET_PARTITIONS (default = vCPUs).
set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root

DATA="${BENCH_DATA:-$PWD/bench/clickbench/hits.parquet}"
export WEFT_MEMORY_LIMIT_BYTES="${WEFT_MEMORY_LIMIT_BYTES:-26000000000}"

echo "[bench] building weft-bench (release) …"
cargo build --release -p weft-bench

if [ ! -f "$DATA" ]; then
  echo "[bench] downloading hits.parquet (~14.78 GB) → $DATA"
  curl -sL -o "$DATA" https://datasets.clickhouse.com/hits_compatible/athena/hits.parquet
fi
echo "[bench] data: $(ls -la "$DATA" | awk '{print $5}') bytes"

./target/release/weft-bench clickbench-grpc --data "$DATA"
echo "[bench] results written to bench/clickbench/results/c6a.4xlarge.json"
