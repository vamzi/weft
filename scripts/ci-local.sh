#!/usr/bin/env bash
# Run the same Rust CI gates as .github/workflows/ci.yml (subset that fits a dev laptop).
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> rustfmt"
cargo fmt --all -- --check

echo "==> clippy"
cargo clippy --workspace --all-targets -- -D warnings

echo "==> build weft CLI (required by weft-cli integration tests)"
cargo build -p weft-cli

echo "==> test"
cargo test --workspace

echo "==> tpch"
cargo run -p weft-bench -- tpch

echo "==> tpch-distributed"
cargo run -p weft-bench -- tpch-distributed --sf 0.01 --workers 2

echo "==> clickbench (engine-direct)"
cargo run -p weft-bench -- clickbench --rows 20000

echo "==> clickbench-grpc"
cargo run -p weft-bench -- clickbench-grpc --rows 20000

echo "==> correctness"
cargo run -p weft-bench -- correctness --rows 5000

echo "==> parity ratchet"
cargo build -p weft-spark-compat --bin weft-parity
./target/debug/weft-parity ratchet --baseline parity/baseline.json --out-dir parity

echo "All local CI gates passed."
