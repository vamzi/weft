# Spark-parity work — handoff for a fresh session

> Read this first, then `ROADMAP.md` (the adversarially-verified per-cluster plan) and the
> committed example UDFs. Everything here is on branch `feat/spark-parity-harness`.

## TL;DR

Weft is a drop-in Apache Spark replacement on DataFusion 54. We **measure** Spark compatibility by
replaying Apache Spark v4.0.0's *own* golden SQL tests through weft and diffing against Spark's
committed `.sql.out` outputs, with a CI ratchet so parity can only rise.

**Current parity (deterministic): semantic 41.9% (5,297 / 12,641), strict 4.8% (611).**
Up from 25.5% / 2.2% at the start. To continue, the single best next move is the **session-timezone
quick win** (§6.1); the biggest structural lever is the **column-naming pass** (§6.4).

Reproduce in ~10s:
```bash
cargo run -p weft-spark-compat --bin weft-parity -- golden   # writes parity/{parity.json,report.md,parity.html,scoreboard.json}
```

## 1. The mission and the honest framing

"100% Spark parity" is pursued as a **measured pass-rate against Spark's portable corpora**, NOT by
porting Spark's ~30k Scala/JVM-internal suites (those test Catalyst/codegen/RDD internals weft
doesn't have — they'd validate DataFusion, not Spark).

Two honest numbers (see `report.rs`):
- **strict** — byte-for-byte identical to Spark's golden (schema line + rows). The hard claim.
- **semantic** — right answer / right rejection, crediting benign divergences: column-name spelling
  (`schema-only`), "both engines reject" (`error-parity`), and same-rows-different-tie-order
  (`ordering`). The fair claim for a drop-in replacement.

**Realistic ceilings (from `ROADMAP.md`): ~85–95% semantic, ~55–75% strict.** 100% strict is not
reachable without trading against faithfulness — error-text matching and the column-naming long tail
are structural. Do not chase strict at the cost of correctness.

### The load-bearing rule: FAITHFULNESS
Anything that runs in `Engine::sql` is on the **production path**, not just tests. A change must not
alter results/semantics for real users. Examples:
- ✅ Faithful: dropping `TEMPORARY` from `CREATE TEMPORARY VIEW` (Spark temp views ≡ DataFusion
  session views); registering a Spark function name as an alias of an identical DataFusion builtin.
- ❌ Forbidden: stripping `USING parquet` from `CREATE TABLE` (silently turns a persistent table
  into an in-memory one). If the only way to pass a query is a lossy rewrite, it is **needs-feature**,
  not a shortcut. The harness measures reality; never inflate the number with a lossy hack.

## 2. What exists

**`crates/weft-spark-compat/`** — the harness (read its `README.md`).
- `spark-tests/{inputs,results}` — vendored Spark v4.0.0 golden corpus (304 files, 12,641 queries).
- `golden.rs` parse `.sql.out` → blocks; `format.rs` render weft Arrow output Spark-style
  (`hiveResultString`); `normalize.rs` row-sort for unordered queries; `classify.rs` triage
  taxonomy; `runner.rs` replay (one `Engine` per file, panic-isolated per block); `report.rs`
  scoreboard (JSON + markdown + HTML + the ratchet JSON).
- `bin/parity.rs` — `weft-parity {golden|ratchet|file}`.
- `tests/golden_sql.rs` — fast wiring tests + the `#[ignore]`d full-corpus ratchet test.

**Engine changes in `crates/weft-loom/src/`** (all faithful):
- `lib.rs::normalize_spark_sql` — rewrites `CREATE [OR REPLACE] [GLOBAL] {TEMPORARY|TEMP} VIEW` →
  `CREATE … VIEW` (keyword-only, the body is preserved verbatim).
- `lib.rs::register_spark_function_aliases` — Wave-A aliases (Spark name → identical DataFusion
  builtin: `startswith`→`starts_with`, `len`→`length`, `any`→`bool_or`, …).
- `lib.rs::Engine::new` sets `datafusion.sql_parser.dialect = Databricks` (Spark SQL: `"..."` is a
  STRING literal, backticks quote identifiers).
- `spark_functions/` — additive module of Spark-only UDFs (see §7). Modules: `try_arithmetic`,
  `spark_strings`, `spark_encoding`, `spark_datetime`, `spark_convert`, `spark_regex_misc`,
  `spark_datetime2`, `spark_json`, `spark_aggregates`, plus `typeof` in `mod.rs` (the template).

**CI**: `.gitlab-ci.yml` job `spark-parity` runs `weft-parity ratchet` against
`parity/baseline.json`. **Scoreboard**: `site/public/parity.{html,json}`.

## 3. How to run things
```bash
# Measure + write artifacts (parity/):
cargo run -p weft-spark-compat --bin weft-parity -- golden
# CI gate — fails if below parity/baseline.json:
cargo run -p weft-spark-compat --bin weft-parity -- ratchet --baseline parity/baseline.json
# Debug one file's per-block verdicts:
cargo run -p weft-spark-compat --bin weft-parity -- file group-by.sql.out
# Tests (fast) and full-corpus ratchet test:
cargo test -p weft-loom -p weft-spark-compat
cargo test -p weft-spark-compat -- --ignored
```
After any improvement: re-run `golden`, then **re-baseline** by copying the new headline+buckets into
`parity/baseline.json` (strict has a ±1 tie-flake — baseline strict to the *minimum* over 3 runs),
and refresh `site/public/parity.{html,json}` from the run's `parity.html`/`scoreboard.json`.

## 4. Current state (Spark v4.0.0, 12,641 queries)

| bucket | count | meaning / where the work is |
|---|---:|---|
| `missing-relation` | 2,726 | cascade from a failed setup stmt (mostly `CREATE TABLE … USING` — §6.5) |
| `error-parity` | 2,463 | ✓ both engines reject (semantic pass) |
| `schema-only` | 2,194 | ✓ right values, divergent column name — **the strict lever (§6.4)** |
| `function-missing` | 1,340 | functions still unimplemented (§6.3) |
| `parser-unsupported` | 1,336 | Spark syntax DataFusion rejects (`CREATE TABLE … USING`, PIVOT, USE) |
| `exec-error` | 1,031 | misc execution failures |
| `pass` | 611 | ✓ strict |
| `feature-unsupported` | 405 | PIVOT, `USE db`, SHOW CREATE TABLE, … |
| `correctness` | 209 | **genuine wrong answers — highest trust priority** |
| `decimal-precision` | 137 | precision/scale/rounding |
| `missing-error` | 111 | weft too lenient (Spark rejects, weft accepts) |
| `null-semantics` | 41 | three-valued-logic |
| `ordering` | 29 | ✓ counted semantic |
| `datetime` | 4 | (most tz-blocked queries land in other buckets — §6.1) |
| `nondeterministic` | 3 | rand/uuid/shuffle — excluded from scoring by design |
| `engine-panic` | 1 | DataFusion `panic!` on `COUNT(DISTINCT a,b)` (§6.6) |

## 5. Mine the backlog (how to pick the next target)
```bash
# Cluster the most common weft errors driving function-missing/exec/parser:
python3 - <<'PY'
import json, collections, re
r=json.load(open("parity/parity.json"))
c=collections.Counter()
for f in r["files"]:
    for x in f.get("failures",[]):
        d=re.sub(r"'[^']*'","'X'",x["detail"]); d=re.sub(r'"[^"]*"','"X"',d); d=re.sub(r'\d+','N',d)
        c[(x["bucket"], d[:90])]+=1
for (b,m),n in c.most_common(30): print(f"{n:4} [{b}] {m}")
PY
```
Note: per-file `failures` are capped at 20; bucket *totals* are exact.

## 6. Ranked next steps

### 6.1 Session timezone — BEST QUICK WIN (likely dialect-sized, low risk)
Spark generated the datetime/timestamp goldens in **`America/Los_Angeles`**; weft's session tz is
UTC, so faithful timestamp values render at the wrong wall-clock and fail (spread across
`schema-only`/`correctness`/`exec-error`, not just the small `datetime` bucket). Set weft's default
session timezone to match the golden-gen tz and re-measure.
- Where: `Engine::new` — `config.options_mut().execution.time_zone = "America/Los_Angeles".into()`
  (verify the exact field name in DataFusion 54's `ConfigOptions`; it governs timestamp rendering).
- Verify it's faithful: Spark's own session tz for these tests is fixed; matching it is correct, not
  a hack. Measure with `weft-parity file datetime.sql.out` / `timestamp.sql.out` before/after.
- Risk: could shift other timestamp outputs — let the ratchet adjudicate net.

### 6.2 `array(...)` / type-constructor function syntax (parser/alias layer)
Spark uses `array(1,2,3)` (DataFusion: `make_array`), and cast-constructors `int(x)`/`double(x)`/
`string(x)` (= `CAST(x AS …)`). These hit `function-missing`/`parser-unsupported` and block
`to_json(array(...))` etc. `array`→`make_array` is an alias (add to `register_spark_function_aliases`).
The cast-constructors need an `ExprPlanner` (recognize `TYPE(expr)` → `CAST`) — bigger; see ROADMAP
"type-and-cast" / function-registration "OUT OF THIS CLUSTER".

### 6.3 More function waves (diminishing but steady; use the swarm — §7)
Remaining backlog (`ROADMAP.md` → function-registration notes, Waves B–F): UDAFs
`listagg`/`percentile_cont`/`percentile_disc`/`histogram_numeric`; `to_number`/`to_char` format
coverage; `mask` variants; `regexp_replace`/`regexp_substr`; `from_csv`/`to_csv`. Per-wave yield is
now ~+30–60 semantic — worthwhile but no longer the dominant lever.

### 6.4 Column-naming pass — THE biggest STRICT lever (large, structural)
`schema-only` (2,194) = right rows, wrong output column NAME. DataFusion emits `Utf8("hello")`,
`count(testdata.a)`, unparenthesized `a = 1`; Spark emits `hello`, `count(a)`, `(a = 1)`. Converting
these to strict passes requires reproducing Spark's `Expression.sql`/`prettyName` output-naming
algorithm as a **plan-output naming pass** (walk the analyzed projection list, rename columns) — NOT
a string hack, and correctness-sensitive (column resolution depends on names). See ROADMAP §1d/§2
(stage-3 "project_spark_names"). Build it incrementally; each naming rule flips a sub-bucket.

### 6.5 `CREATE TABLE … USING <format>` — biggest cascade (needs a real feature)
~120 direct `parser-unsupported` + thousands of downstream `missing-relation`. **Do NOT** shim by
stripping `USING` (lossy — see §1). The faithful fix is a Spark-DDL front end that lowers
`CREATE TABLE … USING fmt [OPTIONS/PARTITIONED BY/AS SELECT]` to a real format-backed table
(`CREATE EXTERNAL TABLE … STORED AS fmt LOCATION <managed-warehouse-path>`; write CTAS results
first). This belongs in the planned `weft-sql` dialect layer, not `normalize_spark_sql`. ROADMAP
"create-table-using" has the full spec.

### 6.6 Correctness + robustness (small count, high trust)
`correctness` (209) = wrong answers — triage these directly (`weft-parity file <f>`), they matter most
for trust. `engine-panic` (1): DataFusion panics on multi-arg `COUNT(DISTINCT a,b)` — implement a
real multi-arg distinct or special-case it.

### 6.7 The other parity pillars (not yet started — original plan Wave 3)
- **PySpark Connect suite**: run Spark's own `python/pyspark/sql/tests/connect/` unmodified against a
  live weft Spark Connect server — the strongest drop-in proof. Harness dir: `bench/spark-compat/`.
- **TPC-H/DS correctness** vs DuckDB oracle (wire `bench/{tpch,clickbench}/run-correctness.sh`).
- **Type/Arrow conformance matrix** (Spark↔Arrow DataType round-trips).

## 7. How to add a Spark UDF (the proven pattern)

Each function is additive: a new file `spark_functions/<name>.rs` with a `pub fn register(ctx)`, plus
one `mod` line and one `register` call in `spark_functions/mod.rs`. **Templates to copy:**
`spark_functions/mod.rs` (`typeof`, minimal scalar), `spark_encoding.rs` (array/per-row scalar),
`try_arithmetic.rs` (numeric, NULL-on-error), `spark_aggregates.rs` (AggregateUDF).

**DataFusion 54 ScalarUDFImpl gotchas (already in the templates):**
- `#[derive(Debug, PartialEq, Eq, Hash)]` on the struct (the trait requires Eq+Hash).
- Exactly four methods: `name`, `signature`, `return_type`, `invoke_with_args` — **no `as_any`**.
- Materialize args: `args.args[i].clone().into_array(args.number_rows)?` then downcast.
- **MSRV is 1.72**: no `Arc::unwrap_or_clone` (use `(*arc).clone()`), no other >1.72 APIs.
- Tests: DataFusion's parser rejects Spark literal suffixes (`1L`,`1.0D`); use `CAST(...)`.
- When unsure of exact Spark output, READ THE GOLDEN: grep the fn in `spark-tests/inputs/`, read the
  matching `spark-tests/results/*.sql.out`, match it byte-for-byte.

**Running an implementation swarm** (optional; needs explicit opt-in / "ultracode"): see the saved
scripts in the session's `workflows/scripts/weft-spark-udf-swarm*.js`. Pattern: one agent per batch,
`isolation: 'worktree'` so each compiles against real DataFusion, returns the verified file source as
text; you integrate into the main tree and gate through the ratchet. Worktrees branch from **committed
HEAD**, so commit any new template before launching. Watch for: JSON round-trip can double-escape
quotes in returned source (`\"` → check it compiles; `spark_datetime.rs` needed an unescape once);
agents may add deps (`regex`, `serde_json`) — declare them in `weft-loom/Cargo.toml`.

## 8. Guardrails (do not skip)
- **Faithfulness** (§1) over the score. A faithful 70% beats a lossy 95%.
- **Ratchet is the arbiter.** Integrate a change only if the full corpus holds/raises. Re-baseline to
  lock gains (strict to the 3-run minimum — there's a ±1 tie-order flake on `postgreSQL/union.sql`).
- **Determinism.** `rand/uuid/shuffle` → `nondeterministic` bucket (excluded). If you see the score
  flake, find the unstable query and either exclude it or fix the comparison — never ship a flaky gate.
- **Concurrent work on this branch.** Another session is actively committing the platform/gateway
  control plane (catalog `schema_adapt`, OIDC/SCIM — commits `39a55e9`, `2aa9723`, `ed2dc1b`, …). Keep
  parity commits limited to `crates/weft-loom/src/{lib.rs,spark_functions/**}`, `crates/weft-spark-compat/**`,
  `parity/`, `site/public/parity.*`. Never stage their files (`schema_adapt.rs`, `catalog_bridge.rs`,
  gateway/*). If a build error points at a file you didn't touch, it's likely their in-progress WIP —
  confirm all errors are outside your files (the compiler lists every error) before worrying.

## 9. Pointers
- `crates/weft-spark-compat/ROADMAP.md` — the per-cluster verdicts + dialect-layer architecture.
- `crates/weft-spark-compat/README.md` — harness internals + how to run.
- Memory: `~/.claude/.../memory/spark-parity-harness.md`.
- My parity commits: `c9a6dd6`, `1c4694f`, `f927cbe`, `cb81580`, `f0c1947`, `55b4c54`, `8458824`,
  `070429b` (interleaved with the concurrent platform commits).
