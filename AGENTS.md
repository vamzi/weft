# AGENTS.md

## Cursor Cloud specific instructions

Weft is a Rust workspace implementing a drop-in Apache Spark replacement that speaks the
Spark Connect gRPC protocol. There are two runnable products in this repo:

1. **Weft engine** (Rust) — the `weft` binary (`crates/weft-cli`) starts a Spark Connect
   server that real PySpark / Spark SQL clients connect to. This is the core product.
2. **Showcase site** (`site/`) — a React 18 + Vite 5 + TypeScript marketing/benchmark site.

The README's "stubbed deps / does not yet execute queries" note is outdated: the workspace
pulls in real DataFusion/Arrow/tonic and the engine executes SQL end-to-end.

### Toolchains / build notes
- Rust toolchain is pinned by `rust-toolchain.toml` (1.90) and auto-installs via rustup.
- `protoc` is **not** required — `crates/weft-proto/build.rs` compiles the vendored Spark
  Connect protos with the pure-Rust `protox` crate.
- A clean `cargo build --workspace` takes a few minutes the first time.

### Standard commands (see `CONTRIBUTING.md` / `site/README.md`)
- Rust: `cargo build --workspace`, `cargo test --workspace`, `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`.
- Bench/coverage gates (CI, runnable locally): `cargo run -p weft-bench -- clickbench --rows 20000`,
  `cargo run -p weft-bench -- clickbench-grpc --rows 20000`, `cargo run -p weft-bench -- tpch`.
- Spark-SQL parity gate: `cargo build -p weft-spark-compat --bin weft-parity` then
  `./target/debug/weft-parity ratchet --baseline parity/baseline.json --out-dir parity`.
- Site: `npm install` then `npm run dev` (serves http://localhost:5174/weft/),
  `npm run typecheck`, `npm run build` — all run from inside `site/`.

### Running the engine + a hello-world query
- Start the server: `./target/debug/weft spark server --port 50051`
  (build first with `cargo build -p weft-cli`). It listens on `sc://0.0.0.0:50051`.
- To drive it with a real client, install the stock PySpark Connect client:
  `pip install "pyspark-client>=4.0"` (pure-Python, no JVM needed), then:
  ```python
  from pyspark.sql import SparkSession
  spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
  spark.sql("SELECT 1 AS hello").show()
  ```
- Engine gotcha (pre-alpha): SQL like `range(5)` returns a column named `range().value`
  rather than Spark's conventional `id`, so `SELECT id FROM range(5)` errors. Use explicit
  `VALUES (...) AS t(...)` tables or aliased projections in smoke tests.

### Distributed mode (optional)
The `weft` binary also has `worker` and `driver` subcommands for a Flight-based
driver/worker cluster (`weft worker --port ...`, `weft driver --workers h:p,... --partial-sql ... --final-sql ...`).
Not needed for the basic single-server flow.
