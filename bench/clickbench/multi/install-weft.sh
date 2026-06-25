#!/usr/bin/env bash
# Weft — build the `weft` binary (weft-cli) in release mode; it serves Spark Connect via
# `weft spark server --port 50051`. A stock PySpark client drives it like any other engine.
# weft-proto targets Spark 4.x, so the client venv pins PySpark 4.0 to match the protocol.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
PYSPARK_FOR_WEFT="${PYSPARK_FOR_WEFT:-3.5.3}"   # 3.5 client works for the basic Connect ops; override to 4.0.0 if needed

# shellcheck disable=SC1091
[ -f "$HOME/.cargo/env" ] && source "$HOME/.cargo/env"
echo "[weft] building release binary (this takes a while) …"
( cd "$REPO" && cargo build --release -p weft-cli )
echo "WEFT_BIN=$REPO/target/release/weft"

VENV="$HERE/.venv-weft"
if [ ! -d "$VENV" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet --upgrade pip
  "$VENV/bin/pip" install --quiet \
    "pyspark[connect]==${PYSPARK_FOR_WEFT}" pandas pyarrow grpcio grpcio-status protobuf
fi
echo "[weft] ready: bin=$REPO/target/release/weft  client=$VENV"
