# Spark-parity work — handoff for a fresh session

> Read this first, then `ROADMAP.md` (the adversarially-verified per-cluster plan) and the
> committed example UDFs. Everything here is on branch `feat/spark-parity-harness`.

## TL;DR

Weft is a drop-in Apache Spark replacement on DataFusion 54. We **measure** Spark compatibility by
replaying Apache Spark v4.0.0's *own* golden SQL tests through weft and diffing against Spark's
committed `.sql.out` outputs, with a CI ratchet so parity can only rise.

**Current parity (deterministic): semantic 44.3% (5,599 / 12,641), strict 5.3% (669 floor).**
Up from 25.5% / 2.2% at the start. To continue, the biggest structural lever is the **column-naming
pass** (§6.4) — `schema-only` is now 2,456, all semantic-passing but strict-failing; another
**function wave** (§6.3) is the steady low-risk option. (The "session-timezone quick win" was tested
and disproven — see §6.1.)

**Wave 4b (2026-06-25): Spark typed-literal parser.** `normalize_spark_sql` now rewrites Spark's
suffixed numeric literals — `1L`/`2Y`/`3S`/`1.0F`/`1.0D`/`1.0BD` — into `CAST(<n> AS <type>)`
(`lib.rs::rewrite_spark_typed_literals` + `decimal_ps` for BigDecimal precision/scale). DataFusion's
lexer read the suffixed forms as identifiers (`No field named "1l"`); the cast is exactly Spark's
semantics, so it's faithful. The scanner is string-/identifier-/comment-aware (never touches `'…'`,
`"…"`, `` `…` ``, `--`/`/* */`, `col1`, `0x1F`, `1e5`). Net: exec-error 1069→955, semantic +168,
strict +44, zero new wrong answers (verified: no typed-literal query is in `correctness`; the small
`correctness` rise is cascade — setup statements that now parse expose pre-existing downstream gaps).

**Last wave (2026-06-25): function wave 4 + Spark binary rendering.** A 4-agent worktree swarm added
`spark_array.rs` (array_size, sort_array, map_contains_key, try_element_at + `array`→`make_array`
alias), `spark_datetime3.rs` (make_timestamp[_ntz], to_timestamp_ntz, try_to_timestamp, unix_*,
date_from_unix_date, date_add), `spark_misc.rs` (hex/unhex/to_binary/try_to_binary, current_*,
assert_true, 2-arg replace), `spark_aggregates2.rs` (try_sum, try_avg, skewness). Then `format.rs`
learned Spark's binary rendering (`new String(bytes, UTF_8)` ≈ `from_utf8_lossy`, not Arrow hex).
Net: function-missing 1340→1099; semantic +134, strict +15. One real wrong-answer bug found and
fixed during integration: `map_contains_key` needed least-common-type key coercion (Spark widens
int↔double, rejects string↔numeric). **Lessons for the next swarm:** (1) always re-run the full
corpus and watch the `correctness` bucket — a newly-registered fn that returns a *wrong* answer is
worse than a missing one (faithfulness); survey `correctness`/`exec-error` for the new fn names and
fix or drop. (2) Some Spark calls are **parser-blocked**, not UDF-able: `timestampdiff(MONTH,…)`
(bare unit keyword parses as a column ref), `percentile_disc/approx` and `listagg` (`WITHIN GROUP`),
and **typed literal suffixes** `1L`/`1.0D`/`2Y`/`2S`/`98…BD` (parsed as identifiers — this blocks
the try_sum/try_avg overflow goldens and many `array(2Y,…)` rows). A Spark typed-literal parser pass
is now a clean, high-value next target. (3) JSON/CSV/XML (`from_json`/`from_csv`/`from_xml`) still
need a Spark DDL/DataType schema-string parser (Wave F) — deferred, substantial.

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
| `missing-relation` | 2,572 | cascade from a failed setup stmt (mostly `CREATE TABLE … USING` — §6.5) |
| `schema-only` | 2,456 | ✓ right values, divergent column name — **the strict lever (§6.4)** |
| `error-parity` | 2,443 | ✓ both engines reject (semantic pass) |
| `parser-unsupported` | 1,348 | Spark syntax DataFusion rejects (`CREATE TABLE … USING`, PIVOT, USE) |
| `function-missing` | 1,133 | functions still unimplemented (§6.3) |
| `exec-error` | 955 | misc execution failures |
| `pass` | 669 | ✓ strict |
| `feature-unsupported` | 459 | PIVOT, `USE db`, SHOW CREATE TABLE, … |
| `correctness` | 244 | **genuine wrong answers — highest trust priority** (mostly cascade-unblocked rows hitting pre-existing gaps, not new-code bugs) |
| `decimal-precision` | 143 | precision/scale/rounding |
| `missing-error` | 131 | weft too lenient (Spark rejects, weft accepts) |
| `null-semantics` | 47 | three-valued-logic |
| `ordering` | 31 | ✓ counted semantic |
| `datetime` | 6 | tz-naive TIMESTAMP gap — not a quick win (§6.1) |
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

### 6.1 Session timezone — DISPROVEN (2026-06-25): NOT a quick win on DataFusion 54
Hypothesis was: Spark generated the goldens in `America/Los_Angeles`, weft renders in UTC, so
setting the session tz would flip a batch of timestamp renders. **Tested and false.** Setting
`opts.execution.time_zone = Some("America/Los_Angeles")` *does* take effect (verified: `now()`'s
type flips to `Timestamp(ns, Some("America/Los_Angeles"))`), but produces **zero parity movement**
because DataFusion 54 produces bare `TIMESTAMP` literals, `CAST(x AS timestamp)`, `to_timestamp`,
`from_unixtime` as `Timestamp(_, None)` — **timezone-naive (NTZ)**. The session tz only governs
`now()`/`current_*` (nondeterministic → excluded from scoring), so the deterministic corpus values
render identically regardless of session tz. `cast(0 as timestamp)` → `1970-01-01 00:00:00` in both
UTC and LA; Spark (LTZ) would give `1969-12-31 16:00:00`.

The *actual* gap is **Spark `TIMESTAMP` ≡ timestamp-with-local-time-zone (LTZ)** vs DataFusion
`TIMESTAMP` ≡ NTZ. Closing it is a real type-semantics feature (coerce literals/casts to
`Timestamp(_, Some(session_tz))` on the production path; affects comparisons/joins/storage), **not** a
config flip — and its yield is small: across `timestamp/date/interval/timestamp-ntz` the failures are
dominated by `function-missing`/`schema-only`/`parser-unsupported`/`exec-error`, with the
tz-sensitive `datetime` bucket only ~3 in `timestamp.sql` and ~0 elsewhere. Skip unless doing a
dedicated LTZ-correctness pass. The real levers in these files are §6.3 (functions: `unix_seconds`,
`unix_millis`, `make_timestamp`, `make_timestamp_ltz`, `date_add`, `convert_timezone`) and §6.4
(column-naming).

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

### 6.4 Column-naming pass — THE biggest STRICT lever (large, structural)  ·  **see `COLUMN_NAMING_PASS.md`**
`schema-only` (now **2,456**) = right rows, wrong output column NAME. DataFusion emits `Utf8("hello")`,
`count(testdata.a)`, `make_array(…)`, unparenthesized `a = 1`; Spark emits `hello`, `count(a)`,
`array(…)`, `(a = 1)`. Converting these to strict passes requires reproducing Spark's
`Expression.sql`/`prettyName` output-naming as a **plan-output naming pass** (re-alias the top output
projection only) — NOT a harness/string hack, and correctness-sensitive (column resolution depends on
names). **A full, data-grounded plan for this pass — the prioritized naming rules with measured
frequencies, the engine hook points, the hard correctness constraints, and an implementation sketch —
is in `crates/weft-spark-compat/COLUMN_NAMING_PASS.md`. Start there.** See also ROADMAP §1d/§2
(stage-3 "project_spark_names").

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
- **`crates/weft-spark-compat/COLUMN_NAMING_PASS.md` — the next pass (output column-naming, the
  biggest strict lever). Start here.**
- `crates/weft-spark-compat/ROADMAP.md` — the per-cluster verdicts + dialect-layer architecture.
- `crates/weft-spark-compat/README.md` — harness internals + how to run.
- Memory: `~/.claude/.../memory/spark-parity-harness.md`.
- My parity commits: `c9a6dd6`, `1c4694f`, `f927cbe`, `cb81580`, `f0c1947`, `55b4c54`, `8458824`,
  `070429b`, `e8057e3` (UDF wave 4 + binary rendering), `57e7aa5` (typed literals) — interleaved
  with the concurrent platform commits.
