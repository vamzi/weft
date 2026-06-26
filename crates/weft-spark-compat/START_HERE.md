# START HERE — Spark-parity push, fresh-session handoff

> Read this first. It is the launchpad: current state, how to run, the next moves, the orchestration
> recipe, and the gotchas that will bite you. For depth, follow the **Doc map** at the bottom.
> Everything is on branch `feat/spark-parity-harness` (pushed to `github.com/vamzi/weft`, remote name
> `github`). Last commit at this handoff: `b72afd3`.

---

## 1. What this is

weft is a drop-in Apache Spark replacement on DataFusion 54. We measure Spark compatibility by replaying
**Apache Spark v4.0.0's own golden SQL corpus** (`crates/weft-spark-compat/spark-tests/{inputs,results}`,
304 files / **12,641 queries**, vendored verbatim from `sql/core/src/test/resources/sql-tests`) through
`weft_loom::Engine`, formatting output Spark-style, and diffing against Spark's committed `.sql.out`. A CI
ratchet (`parity/baseline.json`) means parity can only rise. This *is* the faithful way to "run Spark's
actual unit tests" — it's Spark's `SQLQueryTestSuite` corpus. (The Scala/JVM-internal suites test
Catalyst/codegen/RDD internals weft doesn't have — they'd validate DataFusion, not parity.)

Two numbers: **strict** = byte-for-byte identical to the golden (schema line + rows); **semantic** =
right answer / right rejection, crediting benign column-name / error-text / tie-order divergence.

## 2. Current state (deterministic)

**strict 22.1% (2,793 / 12,641) · semantic 58.5% (7,397 / 12,641).** Up from 2.2% / 25.5% at the start.

| bucket | n | | bucket | n |
|---|--:|---|---|--:|
| **pass** (strict) | 2,793 | | exec-error | 1,093 |
| error-parity (sem) | 2,406 | | missing-relation | 900 |
| schema-only (sem*) | 2,147 | | function-missing | 721 |
| parser-unsupported | 1,238 | | feature-unsupported | 482 |
| correctness | 277 | | missing-error | 166 |
| decimal-precision | 189 | | requires-udf-registration (excluded) | 87 |
| null-semantics | 71 | | ordering (sem) | 51 |
| datetime | 13 | | engine-panic | 3 |
| nondeterministic (excluded) | 4 | | | |

Trajectory: column-naming-w1 (7.8/44.3) → iter-1 six levers (10.5/45.6) → iter-2 CREATE TABLE USING +
cast-constructors (**22.1/58.5**). See `HANDOFF.md` for the per-iteration changelog.

## 3. How to run (≈10s build incremental, golden ≈30–40s now that CTU writes real files)

```bash
cargo build -p weft-spark-compat --bin weft-parity
./target/debug/weft-parity golden     # measure; writes parity/{parity.json,report.md,parity.html,scoreboard.json}
./target/debug/weft-parity file group-by.sql.out   # uncapped per-block verdicts for ONE file
./target/debug/weft-parity ratchet --baseline parity/baseline.json   # CI gate (strict & semantic must not drop)
cargo test -p weft-loom -p weft-spark-compat        # 177 + 23 unit tests, all green
```

## 4. The coordinator loop (the whole job)

`MEASURE → MINE → FAN OUT (swarm) → INTEGRATE → GATE (ratchet) → RE-BASELINE → REPEAT until dry.`
Full recipe + the faithfulness contract every agent inherits: **`PARITY_SWARM_PLAYBOOK.md`**. MINE script
(cluster failing signatures into a ranked work-list): `HANDOFF.md` §5 / playbook §3.

## 5. THE non-negotiables (these are what keep a 30-agent swarm from wrecking the engine)

1. **FAITHFULNESS.** Anything in `Engine::sql` is on the production path for real users. ✅ alias a Spark
   fn to an identical DF builtin; lower Spark syntax to an *equivalent* DF plan; emit Spark-compatible
   names. ❌ lossy rewrites — the canonical sin is stripping `USING parquet` (turns a persistent table
   into an in-memory MemTable). If the only way to pass is lossy, it's **needs-feature** → report, don't
   ship. A faithful 70% beats a lossy 95%.
2. **The real regression gate is NOT "no bad bucket rose" — it's "no file lost a strict pass."** Unblocking
   a cascade (CREATE TABLE USING, a function wave) makes previously-unrunnable rows execute and hit
   *pre-existing* downstream gaps, so correctness/exec-error/decimal/etc. **rise — that is honest
   unmasking, not regression.** Verify the real line with the **stash audit** (§7). Iter-2 rose correctness
   169→277 etc. and was correct to ship because **zero files lost a strict (byte-correct) pass.**
3. **The ratchet only gates strict + semantic + blocks_total** (`bin/parity.rs`). Both must not drop; bad
   buckets are not gated. Agents propose, the ratchet + your stash-audit dispose.
4. **Stay in lane.** Edit only `crates/weft-loom/src/{lib.rs,spark_functions/**,spark_names.rs,spark_int_literals.rs,spark_create_table.rs}`,
   `crates/weft-loom/Cargo.toml`, `crates/weft-spark-compat/**`, `parity/`, `site/public/parity.*`. NEVER
   touch `schema_adapt.rs`, `catalog_bridge.rs`, `gateway/*` — a **concurrent session** owns the platform
   control plane on this same branch (its worktree: `/Users/vamsi/projects/weft-platform-k8s`).

## 6. Next moves (ranked — this is where to point the next swarm)

The iter-2 cascade surfaced its own backlog. Highest leverage first:

1. **Decimal-precision pass (189).** Spark types unsuffixed `1.5` as `decimal(2,1)` (not double) and has
   exact decimal-arithmetic precision/scale rules (DataFusion 54 *copies* them — `avg→Decimal(min(38,p+4),
   min(38,s+4))`, `sum→Decimal(min(38,p+10),s)`). Iter-2 had TWO agents do this (`decimalArithmeticOperations`
   3/3-verified, narrow; `window_part4` higher-yield but corpus-wide) — **they conflict in
   `lib.rs::rewrite_spark_typed_literals`.** Reconcile into ONE gated literal-typing pass. Their verified
   artifacts are described in the iter-2 workflow output (transcript dir under `subagents/workflows/`).
2. **Column-naming wave 2 — aggregate output names (chunk of schema-only 2,147).** `count(*)`→`count(1)`,
   `count(testdata.a)`→`count(a)`, `max(t.c)`→`max(c)`. Blocked in w1 because `SELECT k, count(*) … GROUP BY k`
   plans as `Projection → Aggregate` and the projection references the aggregate as a bare `Column`. Fix in
   `spark_names.rs`: when a top-projection output is a bare `Column` resolving to an `Aggregate` output,
   look up that aggregate's expr in the child `Aggregate` node and render *it* Spark-style. (Iter-2's agent
   for this **died on an API stall** — re-queue it.) Pure output-shaping: semantic flat, strict up.
3. **Unmasked correctness (277) + missing-error (166) + null-semantics (71).** Now-visible pre-existing
   gaps, concentrated in `collations.sql`, `postgreSQL/numeric.sql`, `window.sql`, `charvarchar.sql`,
   `postgreSQL/int4/int8.sql`. Diagnose→fix→**adversarially refute** (the highest-trust swarm; only ship
   refutation survivors). missing-error = weft too lenient now that tables exist (accepts queries Spark
   rejects) — needs analyzer validations.
4. **Function wave (function-missing 721):** `listagg` (needs `WITHIN GROUP` plan support), `from_xml`/
   `from_csv`/`to_csv` (extend the `spark_from_json.rs` schema-string parser), `percentile_disc`,
   `grouping_id`, `to_timestamp_ltz`. (uniform/randn = nondeterministic, excluded; udaf/foo*/udtf already
   excluded as test fixtures.)
5. **CREATE TABLE USING deferred follow-ons** (`CREATE_TABLE_USING_DESIGN.md`): CTAS (`USING fmt AS SELECT`,
   needs COPY-then-CREATE-EXTERNAL materialization), `PARTITIONED BY`, `OPTIONS`/`LOCATION`, exotic column
   types (varchar(n)/timestamp_ntz/nested struct). Each currently returns `None` → fails as before.
6. **Structural residual (the honest distance to 100%, per `ROADMAP.md` §0/§4):** exact Spark error-text
   (`error-parity`→strict — low value, brittle, partly anti-faithful), and Spark-internal behaviors weft
   legitimately differs on. Faithful ceiling ≈ 85–95% semantic / 55–75% strict. Don't chase strict at the
   cost of correctness; present the residual as an itemized opt-in list.

## 7. The stash audit (run this to prove faithfulness after any cascade-unblocking change)

```bash
cp parity/parity.json /tmp/after.json                 # your built tree's result
git stash && cargo build -q -p weft-spark-compat --bin weft-parity && ./target/debug/weft-parity golden
cp parity/parity.json /tmp/before.json && git stash pop && cargo build -q -p weft-spark-compat --bin weft-parity
# then per-file: assert no file's `pass` (strict) count dropped before→after; confirm every bad-bucket
# rise sits on a missing-relation/function-missing drop in the SAME file (= unmasking, not regression).
```
(After `git stash` the new untracked `spark_*.rs` files orphan harmlessly — their `mod` lines are stashed.)

## 8. Integration mechanics (coordinator replays agent artifacts into the main tree)

Worktree agents return structured artifacts: `new_files` (full content) + `edits` ({path, old_str, new_str})
+ `added_deps`. You replay them: Write new files, apply edits with exact-match verification, then build +
ratchet. Gotchas learned the hard way:
- Agent JSON may be **HTML-escaped** (`&gt;`/`&amp;`) — unescape before writing or it won't compile.
- `spark_functions/mod.rs` **register-call anchors displace** when multiple agents add a `mod`+`register`
  line — apply those manually. Same for any shared file (`lib.rs`, `spark_names.rs`, `format.rs`).
- Each agent compiled in isolation from HEAD; the **combined** tree is the truth — build once, run the
  full golden, run the stash audit. Don't trust summed isolated deltas.
- Re-baseline strict to the **3-run minimum** (±1 `postgreSQL/union.sql` tie-flake); semantic is stable.
  Then `cp parity/scoreboard.json site/public/parity.json; cp parity/parity.html site/public/parity.html`.

## 9. Orchestration recipe (how to launch the next swarm — needs "ultracode"/explicit opt-in)

Use the `Workflow` tool. Two iteration scripts are saved at (session scratchpad — copy out if you want them):
`parity_iteration.js`, `parity_iteration2.js`. Shape that worked:
- Each impl agent runs `isolation: 'worktree'` and **self-verifies via the harness** (build →
  `./target/debug/weft-parity golden` → compare buckets to `parity/baseline.json` → confirm no bad bucket
  rose / report honestly) before returning its artifact. Gold standard.
- Risky levers (correctness, decimal, type changes): `diagnose → fix → 3-lens adversarial refute`; ship
  only refutation survivors. A refuter's abstract doubt loses to the harness golden-diff ground truth, but
  read its objection — it catches out-of-corpus gaps (e.g. divide-by-zero → Inf vs Spark NULL).
- Big features (CREATE TABLE USING): dedicated `impl → 3-lens verify` pipeline, `effort: 'high'`.

## 10. OPS gotchas (will bite you)

- **Disk fills up.** Each worktree agent does a cold full build (~5 GB). A swarm leaves
  `.claude/worktrees/wf_*` — once **121 GB**, which failed a link with `errno 28`. After extracting
  artifacts: `git worktree list --porcelain | awk '/^worktree /{print $2}' | grep '/.claude/worktrees/wf_' |
  while read w; do git worktree remove --force "$w"; done; git worktree prune`. (Leave `weft-platform-k8s`.)
- **Spend limit** killed agents mid-swarm in iter-1 (cast-constructors, refuters). If agents fail with
  "monthly spend limit," raise it (claude.ai/settings/usage) before launching a full swarm — or
  coordinator-verify the survivors yourself.
- CTU makes `golden` slower (writes real files to a per-engine temp warehouse, torn down on `Engine` drop);
  a single golden run can exceed a 2-min Bash timeout — give it room or run in background.

## 11. Doc map

- **`PARITY_SWARM_PLAYBOOK.md`** — the coordinator playbook (the loop, faithfulness contract, swarm waves,
  Workflow patterns). Run the campaign from here.
- **`HANDOFF.md`** — detailed per-iteration changelog + current bucket table + ranked next steps + the
  "how to add a Spark UDF" template (§7).
- **`ROADMAP.md`** — per-cluster verdicts, the weft-sql dialect-layer architecture, the honest-ceiling §0/§4.
- **`CREATE_TABLE_USING_DESIGN.md`** — CTU spec; non-CTAS subset LANDED, follow-ons specced.
- **`COLUMN_NAMING_PASS.md`** — output column-naming deep-dive (w1 landed; aggregate-names is move #2 above).
- **`README.md`** — harness internals.
- Memory: `~/.claude/projects/-Users-vamsi-projects-weft/memory/spark-parity-harness.md`.
