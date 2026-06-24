# Contributing to Weft

## Build

```sh
cargo build --workspace
cargo test  --workspace
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

The stub workspace builds on Rust 1.72+. The runtime crates (DataFusion/Arrow/tonic) will
require **Rust ≥ 1.80** and **protoc**; their dependencies are stubbed out today and noted
as `TODO(deps)` in each crate's `Cargo.toml`.

## Layout

See [`docs/architecture.md`](docs/architecture.md). Crates live in `crates/weft-*`;
benchmarks in `bench/`; the Python helper package in `python/pyweft`.

## Ground rules (non-negotiable, from the architecture)

1. **Arrow is the currency between operators.** Don't invent a second in-memory format.
2. **The columnar hot loop stays in `weft-loom`.** Never lift columns into HVM2 per-row.
3. **`weft-hvm` is off by default and off the critical path.** The engine must be correct
   and competitive on `weft-loom` alone.
4. **Every claim is measured.** Performance changes ride with a ClickBench/TPC-H number.

## Commit / MR conventions

- Conventional-commit style subjects (`feat(loom): …`, `fix(connect): …`).
- An MR that changes execution must include a benchmark delta.
