# weft-spark-compat — Apache Spark parity harness

Turns "Weft is a drop-in Spark replacement" from a slogan into a **measured, gated number**.

It replays Apache Spark's *own* golden SQL tests (`sql/core/src/test/resources/sql-tests/`,
vendored under `spark-tests/` from Spark **v4.0.0**) through Weft, formats the results exactly the
way Spark's `SQLQueryTestSuite` does, and diffs them against Spark's committed `.sql.out` golden
outputs. Every mismatch is filed into a triage bucket so the result is an actionable backlog.

## Run it

```bash
# Full corpus → parity/{parity.json, report.md, parity.html, scoreboard.json} + headline.
cargo run -p weft-spark-compat --bin weft-parity -- golden

# CI gate: fail if parity dropped below parity/baseline.json (the ratchet).
cargo run -p weft-spark-compat --bin weft-parity -- ratchet --baseline parity/baseline.json

# Debug one file's per-block verdicts.
cargo run -p weft-spark-compat --bin weft-parity -- file group-by.sql.out

# Tests (fast wiring tests + the #[ignore]d full-corpus ratchet).
cargo test -p weft-spark-compat            # fast
cargo test -p weft-spark-compat -- --ignored   # full corpus, ~7s
```

## Two honest numbers

- **strict** — byte-for-byte identical to Spark's golden (schema line + rows).
- **semantic** — right answer / right rejection, crediting two benign divergences: column-name
  spelling in the `struct<...>` schema line, and "both engines reject this query" (different JVM
  vs Rust error text). The fair reading for a drop-in replacement.

## Current state (Spark v4.0.0, 12,641 queries across 303 files)

| metric | first baseline | now | Δ |
|---|---|---|---|
| strict parity | 2.2% (274) | **3.9% (496)** | +222 |
| semantic parity | 25.5% (3,223) | **38.9% (4,923)** | +1,700 |

The jump came from the first parity fix: **`CREATE [OR REPLACE] [GLOBAL] TEMPORARY VIEW` support**
in Weft's engine (`weft_loom::normalize_spark_sql`). DataFusion's `create_view` rejects *only* the
`temporary` flag, so the rewrite drops the keyword (Spark temp views and DataFusion
session-catalog views are both session-scoped — semantically equivalent). That cleared 2,438
cascade failures; ~1,500 of those queries now run and return the right values.

### Triage backlog — failures by bucket (current)

| bucket | count | what it means |
|---|---:|---|
| `missing-relation` | 3,131 | remaining setup cascades (`CREATE TABLE` variants, `INSERT`, …) |
| `error-parity` | 2,464 | ✓ both engines reject (counts as semantic pass) |
| `schema-only` | 1,963 | ✓ right values, divergent column name (semantic pass; blocks *strict*) |
| `function-missing` | 1,639 | functions Weft/DataFusion lacks or names differently |
| `parser-unsupported` | 1,304 | Spark SQL syntax DataFusion's parser rejects |
| `exec-error` | 840 | miscellaneous execution failures |
| `feature-unsupported` | 346 | `PIVOT`, `USE db`, `SHOW CREATE TABLE`, … |
| `correctness` | 179 | **genuine wrong answers — top priority** |
| `decimal-precision` | 129 | precision/scale/rounding diverged |
| `missing-error` | 110 | Weft accepted a query Spark rejects (too lenient) |
| `null-semantics` | 21 | three-valued-logic divergence |
| `ordering` | 17 | right multiset, wrong row order |
| `datetime` | 1 | calendar/timezone/format |
| `engine-panic` | 1 | DataFusion `panic!` (multi-arg `COUNT(DISTINCT a,b)`) — isolated, not fatal |

### Next levers

- **`schema-only` (1,963)** — the biggest lever for *strict* parity: these queries already return
  the right rows, but the `struct<...>` column names differ from Spark's analyzer (`count(a)` vs
  `count(t.a)`). Aligning generated column names converts ~1,900 semantic passes into strict ones.
- **`missing-relation` (3,131)** — the remaining setup cascades; chase the next failing
  `CREATE`/`INSERT` forms the way temp views were chased.
- **`function-missing` (1,639)** + **`parser-unsupported` (1,304)** — the long tails.
- **`correctness` (179)** — fewest, but highest-trust: genuine wrong answers.

## How it works

`golden.rs` parses `.sql.out` into `(sql, schema, output)` blocks → one `weft_loom::Engine` per
file replays them in order (so `CREATE … VIEW` setup persists) → `format.rs` renders Weft's Arrow
output Spark-style → `normalize.rs` applies Spark's row-sorting for unordered queries →
`classify.rs` buckets the result → `report.rs` aggregates the scoreboard. The golden file is the
authoritative statement list, so we never re-implement Spark's `.sql` splitter.

Files needing machinery Weft lacks (registered UDFs, `--IMPORT` chains) are **skipped with a
recorded reason**, never silently dropped — see the "Skipped files" section of `report.md`.

## Re-baselining

When Weft improves, lock the gain in: `weft-parity golden` then commit the regenerated
`parity/baseline.json`. The CI `spark-parity` job and the `#[ignore]`d ratchet test both read it.

The scoreboard is published to the site at `site/public/parity.{html,json}`.
