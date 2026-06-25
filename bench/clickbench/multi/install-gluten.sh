#!/usr/bin/env bash
# Gluten + Velox backend for Spark 3.5 — a single prebuilt "bundle" JAR layered onto the same
# Spark install from install-spark.sh. Gluten offloads Spark SQL operators to the native Velox
# engine, so this is the "Spark made fast with a native vectorized backend" data point.
#
# Apache Gluten publishes per-(Spark, OS, arch) bundle jars on its releases page. The exact asset
# name moves with each release, so it is overridable via env. Defaults target Spark 3.5 / x86_64.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
GLUTEN_VERSION="${GLUTEN_VERSION:-1.3.0}"
# e.g. gluten-velox-bundle-spark3.5_2.12-ubuntu_22.04_x86_64-1.3.0.jar
GLUTEN_JAR="${GLUTEN_JAR:-gluten-velox-bundle-spark3.5_2.12-ubuntu_22.04_x86_64-${GLUTEN_VERSION}.jar}"
GLUTEN_URL="${GLUTEN_URL:-https://github.com/apache/incubator-gluten/releases/download/v${GLUTEN_VERSION}/${GLUTEN_JAR}}"

DEST="$HERE/jars"
mkdir -p "$DEST"
if [ ! -f "$DEST/$GLUTEN_JAR" ]; then
  echo "[gluten] downloading $GLUTEN_JAR …"
  curl -fSL -o "$DEST/$GLUTEN_JAR" "$GLUTEN_URL"
fi
# Gluten needs libnuma at runtime.
sudo apt-get install -y --no-install-recommends libnuma1 >/dev/null 2>&1 || true
echo "GLUTEN_JAR_PATH=$DEST/$GLUTEN_JAR"
echo "[gluten] ready (reuses the Spark install + client from install-spark.sh)"
