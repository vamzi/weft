#!/usr/bin/env bash
# Weft ClickBench driver. Self-contained (does not depend on ClickBench's shared lib) so it
# can run in our own CI; the upstream submission will use ClickBench/lib/benchmark-common.sh.
#
# Output: bench/clickbench/results/<date>/<machine>.json  (result = 43 × [cold, hot1, hot2]).
set -euo pipefail
cd "$(dirname "$0")"

TRIES=3
DATA="${BENCH_DATA:-hits.parquet}"
PORT="${WEFT_PORT:-50051}"
MACHINE="${BENCH_MACHINE:-c6a.4xlarge}"

# 1. data
if [[ ! -f "$DATA" ]]; then
  echo "downloading hits.parquet (~14.78 GB) …" >&2
  wget --continue --progress=dot:giga \
    'https://datasets.clickhouse.com/hits_compatible/athena/hits.parquet' -O "$DATA"
fi

# 2. server  (TODO(issue #1/#3): wait for readiness instead of sleep)
echo "starting weft server on :$PORT …" >&2
weft spark server --port "$PORT" &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT
# shellcheck disable=SC2034
for _ in $(seq 1 30); do sleep 1; done

# 3. run queries
RESULTS="[]"
i=0
while IFS= read -r sql; do
  [[ -z "$sql" || "$sql" == --* ]] && continue
  row="["
  for try in $(seq 1 "$TRIES"); do
    secs="$(printf '%s' "$sql" | ./query 2>&1 1>/dev/null | tail -n1 || echo null)"
    row+="$secs"; [[ "$try" -lt "$TRIES" ]] && row+=","
  done
  row+="]"
  echo "Q$i: $row" >&2
  RESULTS="${RESULTS%]}${RESULTS#[[]}"  # placeholder accumulation; real impl uses jq
  i=$((i+1))
done < queries.sql

# 4. emit results.json
DATE="$(date +%Y%m%d)"
OUT="results/$DATE/$MACHINE.json"
mkdir -p "results/$DATE"
echo "TODO(issue #3): assemble template.json + per-query [cold,hot1,hot2] into $OUT" >&2
echo "wrote $OUT" >&2
