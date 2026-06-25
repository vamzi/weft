#!/usr/bin/env bash
# Boot ONE engine's Spark Connect server, register `hits` (engine-specific DDL), run the 43
# queries × 3 via the stock PySpark client, write results/<engine>.json, then stop the server.
#
#   bash run-engine.sh <weft|sail|spark|gluten>
#
# Env: BENCH_DATA = path to hits.parquet (default: bench/clickbench/hits.parquet)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
ENGINE="${1:?usage: run-engine.sh <weft|sail|spark|gluten>}"
DATA="${BENCH_DATA:-$REPO/bench/clickbench/hits.parquet}"
RESULTS="$HERE/results"; mkdir -p "$RESULTS"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
SPARK_VERSION="${SPARK_VERSION:-3.5.3}"
SPARK_HOME="${SPARK_HOME:-$HOME/spark-${SPARK_VERSION}-bin-hadoop3}"
NPROC="$(nproc)"

[ -f "$DATA" ] || { echo "[run] dataset missing: $DATA (run download in run-all.sh)"; exit 1; }

SERVER_PID=""
cleanup() {
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
  # Spark's start-connect-server forks a daemon — stop it too.
  [ -n "${SPARK_HOME:-}" ] && "$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_port() {  # host port timeout_s
  local h="$1" p="$2" t="${3:-120}" i=0
  while ! (exec 3<>"/dev/tcp/$h/$p") 2>/dev/null; do
    i=$((i+1)); [ "$i" -ge "$t" ] && { echo "[run] port $h:$p never opened"; return 1; }
    sleep 1
  done
  exec 3>&- 3<&- 2>/dev/null || true
}

REG="$HERE/.register-$ENGINE.sql"
case "$ENGINE" in
  weft)
    VENV="$HERE/.venv-weft"; REMOTE="sc://localhost:50051"; HOSTPORT="localhost 50051"
    # Weft reads the date column as int32 days → cast to DATE (mirrors weft-bench's own setup).
    cat > "$REG" <<SQL
CREATE EXTERNAL TABLE hits_raw STORED AS PARQUET LOCATION '$DATA' OPTIONS ('binary_as_string' 'true')
CREATE VIEW hits AS SELECT * EXCEPT ("EventDate"), CAST(CAST("EventDate" AS INTEGER) AS DATE) AS "EventDate" FROM hits_raw
SQL
    echo "[run] starting weft server …"
    "$REPO/target/release/weft" spark server --port 50051 >"$LOGS/weft.log" 2>&1 &
    SERVER_PID=$!
    ;;
  sail)
    VENV="$HERE/.venv-sail"; REMOTE="sc://localhost:50052"; HOSTPORT="localhost 50052"
    echo "CREATE OR REPLACE TEMPORARY VIEW hits USING parquet OPTIONS (path '$DATA')" > "$REG"
    echo "[run] starting sail server …"
    # pysail exposes a CLI; fall back to the module entrypoint if the name differs.
    ( "$VENV/bin/sail" spark server --ip 0.0.0.0 --port 50052 \
      || "$VENV/bin/python" -m pysail spark server --ip 0.0.0.0 --port 50052 ) \
      >"$LOGS/sail.log" 2>&1 &
    SERVER_PID=$!
    ;;
  spark)
    VENV="$HERE/.venv-spark"; REMOTE="sc://localhost:15002"; HOSTPORT="localhost 15002"
    echo "CREATE OR REPLACE TEMPORARY VIEW hits USING parquet OPTIONS (path '$DATA')" > "$REG"
    echo "[run] starting spark connect server …"
    "$SPARK_HOME/sbin/start-connect-server.sh" \
      --packages "org.apache.spark:spark-connect_2.12:$SPARK_VERSION" \
      --conf spark.driver.memory=24g \
      --conf spark.sql.shuffle.partitions="$NPROC" \
      --conf spark.driver.bindAddress=0.0.0.0 >"$LOGS/spark.log" 2>&1
    SERVER_PID=""   # start-connect-server daemonizes; cleanup() stops it
    ;;
  gluten)
    VENV="$HERE/.venv-spark"; REMOTE="sc://localhost:15002"; HOSTPORT="localhost 15002"
    GLUTEN_JAR="$(ls "$HERE"/jars/gluten-velox-bundle-*.jar 2>/dev/null | head -1 || true)"
    [ -n "$GLUTEN_JAR" ] || { echo "[run] gluten jar missing (install-gluten.sh)"; exit 1; }
    echo "CREATE OR REPLACE TEMPORARY VIEW hits USING parquet OPTIONS (path '$DATA')" > "$REG"
    echo "[run] starting spark+gluten/velox connect server …"
    "$SPARK_HOME/sbin/start-connect-server.sh" \
      --packages "org.apache.spark:spark-connect_2.12:$SPARK_VERSION" \
      --jars "$GLUTEN_JAR" \
      --conf spark.plugins=org.apache.gluten.GlutenPlugin \
      --conf spark.memory.offHeap.enabled=true \
      --conf spark.memory.offHeap.size=20g \
      --conf spark.gluten.sql.columnar.backend.lib=velox \
      --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
      --conf spark.driver.memory=8g \
      --conf spark.sql.shuffle.partitions="$NPROC" \
      --conf spark.driver.bindAddress=0.0.0.0 >"$LOGS/gluten.log" 2>&1
    SERVER_PID=""
    ;;
  *) echo "unknown engine: $ENGINE"; exit 2 ;;
esac

echo "[run] waiting for $REMOTE …"
# shellcheck disable=SC2086
wait_for_port $HOSTPORT 180

"$VENV/bin/python" "$HERE/runner.py" \
  --remote "$REMOTE" \
  --queries "$HERE/queries.spark.sql" \
  --register-file "$REG" \
  --engine "$ENGINE" \
  --out "$RESULTS/$ENGINE.json" \
  --data-size "$(stat -c%s "$DATA" 2>/dev/null || echo 0)"

echo "[run] $ENGINE done → $RESULTS/$ENGINE.json"
