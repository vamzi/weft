//! `weft-loom` — the vectorized CPU engine and Weft's workhorse.
//!
//! **This is what beats Sail on ClickBench.** Phase 0 embeds DataFusion behind the warp
//! IR to reach correctness + a credible benchmark entry fast. Phase 1 carves out native
//! operators for the handful of queries that dominate the total runtime:
//!
//! - high-cardinality `GROUP BY` (Q31–Q35): adaptive, radix-partitioned, open-addressing
//!   hash table with an inline hash salt; spill partitions independently;
//! - sort / top-N (Q23–Q26 and every `… ORDER BY c DESC LIMIT 10`): late-materialized
//!   top-N heap that decodes only the surviving rows;
//! - string `LIKE`/regex (Q20–Q23, Q28): SIMD substring + vectorized regex;
//! - `COUNT(DISTINCT)` (Q4/Q5 + per-group): HyperLogLog sketches.
//!
//! The strategy: tie Sail on the ~33 cheap queries (DataFusion parity), beat it 1.5–2× on
//! the ~10 expensive ones. Winning those *is* winning the total.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use weft_common::{Error, Result};

pub mod catalog_bridge;

/// Case-insensitive file→table column matching for catalog-declared schemas (Glue/Hive parity).
mod schema_adapt;

/// Spark-only scalar functions (DataFusion `ScalarUDF`s) registered into every [`Engine`].
pub mod spark_functions;

/// Spark-compatible output column naming for the top result projection (drop-in `df.columns`
/// parity). See [`spark_names::project_spark_names`].
mod spark_names;

/// Spark-compatible integer-literal typing (`INT` vs `BIGINT` default). See
/// [`spark_int_literals::downcast_int_literals`].
mod spark_int_literals;

/// Re-export of the exact `arrow` DataFusion uses, so every crate in the workspace encodes
/// Arrow IPC against one version (no cross-crate `arrow` mismatch).
pub use datafusion::arrow;

use arrow::record_batch::RecordBatch;

/// Native operators (Phase-1 carve-outs) that replace DataFusion's generic physical operators
/// on the heavy ClickBench queries. See [`ops`] for status and scope.
pub mod ops;

/// Backend identifier reported in `EXPLAIN`.
pub const NAME: &str = "loom";

/// Parse a `usize` tuning knob from the environment (absent / unparseable → `None`).
fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok().and_then(|s| s.parse().ok())
}

/// Parse a boolean tuning knob from the environment. Accepts `1/0`, `true/false`, `on/off`
/// (case-insensitive); absent / unrecognized → `None`.
fn env_bool(key: &str) -> Option<bool> {
    match std::env::var(key)
        .ok()?
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "true" | "on" | "yes" => Some(true),
        "0" | "false" | "off" | "no" => Some(false),
        _ => None,
    }
}

/// Adapt Spark-dialect SQL that DataFusion's planner rejects verbatim but supports once a
/// dialect-only keyword is dropped. The rewrite only touches the leading DDL keywords and leaves
/// the statement body byte-for-byte intact.
///
/// Today it handles `CREATE [OR REPLACE] [GLOBAL] TEMPORARY VIEW … ` → `CREATE [OR REPLACE]
/// VIEW … `. Spark temporary views are *session*-scoped; a DataFusion session-catalog view is
/// too, so dropping `TEMPORARY`/`GLOBAL` preserves the semantics within a session while letting
/// DataFusion register the view (its `create_view` rejects `temporary` and nothing else). This is
/// the single biggest Spark-parity unlock — almost every Spark SQL test opens with
/// `CREATE OR REPLACE TEMPORARY VIEW testData AS …`.
///
/// This is a stopgap living in the engine; it will migrate into the `weft-sql` Spark-dialect
/// front end when that lands.
pub fn normalize_spark_sql(query: &str) -> std::borrow::Cow<'_, str> {
    // First the leading-keyword DDL rewrite, then the typed-literal rewrite over the result.
    let stripped = strip_temporary_view(query);
    let base = stripped.as_deref().unwrap_or(query);
    match rewrite_spark_typed_literals(base) {
        Some(rewritten) => std::borrow::Cow::Owned(rewritten),
        None => match stripped {
            Some(s) => std::borrow::Cow::Owned(s),
            None => std::borrow::Cow::Borrowed(query),
        },
    }
}

/// Byte length of the UTF-8 char starting with leading byte `lead`.
fn utf8_len(lead: u8) -> usize {
    if lead < 0x80 {
        1
    } else if lead < 0xE0 {
        2
    } else if lead < 0xF0 {
        3
    } else {
        4
    }
}

/// Derive Spark's `DECIMAL(precision, scale)` for a `…BD` literal from its digit text (no sign, no
/// exponent), matching `java.math.BigDecimal`: scale = fractional digits; precision = significant
/// digits (leading zeros stripped, min 1), widened so `precision >= scale`. Returns `None` if it
/// would exceed Spark's 38-digit decimal range (leave the literal untouched).
fn decimal_ps(num: &str) -> Option<(u8, u8)> {
    let (int_part, frac_part) = num.split_once('.').unwrap_or((num, ""));
    let scale = frac_part.len();
    let sig_digits: String = format!("{int_part}{frac_part}");
    let trimmed = sig_digits.trim_start_matches('0');
    let sig = if trimmed.is_empty() { 1 } else { trimmed.len() };
    let precision = sig.max(scale).max(1);
    if precision > 38 {
        return None;
    }
    Some((precision as u8, scale as u8))
}

/// Rewrite Spark's typed numeric literals — `1L`, `2Y`, `3S`, `1.0F`, `1.0D`, `1.0BD` — into the
/// equivalent `CAST(<num> AS <type>)`. DataFusion's lexer reads the suffixed forms as identifiers
/// (failing with `No field named "1l"`), so Spark SQL that uses typed literals — pervasive in the
/// corpus — won't plan. The cast is exactly Spark's semantics (`1L` *is* a bigint `1`), so the
/// rewrite is faithful, not lossy.
///
/// The scan is string-/identifier-/comment-aware: single- and double-quoted strings (`"…"` is a
/// string literal under the Databricks dialect), backtick-quoted identifiers, and `--`/`/* */`
/// comments are copied through verbatim, so a literal like `'1L'` or a column `` `2Y` `` is never
/// touched. A numeric token is only rewritten when it sits in code position (the preceding char is
/// not an identifier char or `.`) and the suffix is followed by a non-identifier boundary, so
/// `col1`, `0x1F`, `1e5`, and `3.14desc` are all left intact. Returns `None` when nothing changed
/// (so the caller keeps the borrowed fast-path).
fn rewrite_spark_typed_literals(sql: &str) -> Option<String> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = String::with_capacity(n + 16);
    let mut i = 0;
    let mut changed = false;

    while i < n {
        let c = b[i];

        // Quoted string ('…', "…") or identifier (`…`) — copy verbatim, honoring doubled quotes.
        if c == b'\'' || c == b'"' || c == b'`' {
            let start = i;
            i += 1;
            while i < n {
                if b[i] == c {
                    if i + 1 < n && b[i + 1] == c {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Line comment.
        if c == b'-' && i + 1 < n && b[i + 1] == b'-' {
            let start = i;
            while i < n && b[i] != b'\n' {
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Block comment.
        if c == b'/' && i + 1 < n && b[i + 1] == b'*' {
            let start = i;
            i += 2;
            while i < n && !(b[i] == b'*' && i + 1 < n && b[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(n);
            out.push_str(&sql[start..i]);
            continue;
        }

        // Numeric literal candidate: a digit in code position (not part of an identifier or a
        // fractional tail).
        let prev = if i == 0 { 0 } else { b[i - 1] };
        let prev_blocks = prev.is_ascii_alphanumeric() || prev == b'_' || prev == b'.';
        if c.is_ascii_digit() && !prev_blocks {
            let num_start = i;
            while i < n && b[i].is_ascii_digit() {
                i += 1;
            }
            // Fraction (only when a digit follows the dot — otherwise the dot isn't ours).
            if i + 1 < n && b[i] == b'.' && b[i + 1].is_ascii_digit() {
                i += 1;
                while i < n && b[i].is_ascii_digit() {
                    i += 1;
                }
            }
            // Exponent.
            let mut has_exp = false;
            if i < n && (b[i] == b'e' || b[i] == b'E') {
                let mut j = i + 1;
                if j < n && (b[j] == b'+' || b[j] == b'-') {
                    j += 1;
                }
                if j < n && b[j].is_ascii_digit() {
                    i = j;
                    while i < n && b[i].is_ascii_digit() {
                        i += 1;
                    }
                    has_exp = true;
                }
            }
            let num = &sql[num_start..i];
            let after_ok =
                |k: usize| k >= n || !(b[k].is_ascii_alphanumeric() || b[k] == b'_');

            // `BD` → DECIMAL (only without an exponent, where precision/scale are well-defined).
            if i + 1 < n
                && (b[i] == b'b' || b[i] == b'B')
                && (b[i + 1] == b'd' || b[i + 1] == b'D')
                && after_ok(i + 2)
            {
                if !has_exp {
                    if let Some((p, s)) = decimal_ps(num) {
                        out.push_str(&format!("CAST({num} AS DECIMAL({p},{s}))"));
                        i += 2;
                        changed = true;
                        continue;
                    }
                }
                out.push_str(num);
                continue;
            }
            // Single-letter type suffix.
            if i < n && after_ok(i + 1) {
                let ty = match b[i] {
                    b'y' | b'Y' => Some("TINYINT"),
                    b's' | b'S' => Some("SMALLINT"),
                    b'l' | b'L' => Some("BIGINT"),
                    b'f' | b'F' => Some("FLOAT"),
                    b'd' | b'D' => Some("DOUBLE"),
                    _ => None,
                };
                if let Some(ty) = ty {
                    out.push_str(&format!("CAST({num} AS {ty})"));
                    i += 1;
                    changed = true;
                    continue;
                }
            }
            // A plain number with no type suffix — copy as-is.
            out.push_str(num);
            continue;
        }

        // Any other char — copy one UTF-8 char.
        let len = utf8_len(c).min(n - i);
        out.push_str(&sql[i..i + len]);
        i += len;
    }

    changed.then_some(out)
}

/// Read the next whitespace-delimited token from `s` starting at `*cur`, returning its byte span
/// and advancing `*cur` past it. `None` at end of input.
fn next_token(s: &str, cur: &mut usize) -> Option<(usize, usize)> {
    let b = s.as_bytes();
    while *cur < b.len() && b[*cur].is_ascii_whitespace() {
        *cur += 1;
    }
    let start = *cur;
    while *cur < b.len() && !b[*cur].is_ascii_whitespace() {
        *cur += 1;
    }
    (start < *cur).then_some((start, *cur))
}

/// If `query` begins with `CREATE [OR REPLACE] [GLOBAL] TEMPORARY VIEW`, return the same
/// statement with `GLOBAL TEMPORARY` removed; otherwise `None` (leave the query untouched).
fn strip_temporary_view(query: &str) -> Option<String> {
    let lead = query.len() - query.trim_start().len();
    let (ws, rest) = query.split_at(lead);
    let eq = |span: (usize, usize), kw: &str| rest[span.0..span.1].eq_ignore_ascii_case(kw);

    let mut cur = 0;
    if !eq(next_token(rest, &mut cur)?, "create") {
        return None;
    }
    let mut or_replace = false;
    let mut tok = next_token(rest, &mut cur)?;
    if eq(tok, "or") {
        if !eq(next_token(rest, &mut cur)?, "replace") {
            return None;
        }
        or_replace = true;
        tok = next_token(rest, &mut cur)?;
    }
    if eq(tok, "global") {
        tok = next_token(rest, &mut cur)?;
    }
    // Only rewrite when the temp keyword is present (otherwise DataFusion already copes). Spark
    // accepts both `TEMPORARY` and the `TEMP` abbreviation.
    if !eq(tok, "temporary") && !eq(tok, "temp") {
        return None;
    }
    if !eq(next_token(rest, &mut cur)?, "view") {
        return None;
    }
    // The statement body (view name onward) is preserved verbatim from just after `VIEW`.
    let head = if or_replace {
        "CREATE OR REPLACE VIEW"
    } else {
        "CREATE VIEW"
    };
    Some(format!("{ws}{head}{}", &rest[cur..]))
}

/// Register Spark function names that DataFusion already implements under a *different* name, as
/// faithful aliases — same implementation, extra invocation name. Purely additive and zero-risk:
/// it can only make more Spark SQL resolve, never change an existing result (DataFusion's
/// `with_aliases` merges, so no built-in alias is dropped). This is "Wave A" of the Spark function
/// backlog (aliases for functions with identical semantics under another name); real UDF
/// implementations for Spark-only functions are a separate, larger effort.
fn register_spark_function_aliases(ctx: &SessionContext) {
    use datafusion::execution::FunctionRegistry;

    // (Spark name, DataFusion builtin with identical semantics).
    const SCALAR_ALIASES: &[(&str, &str)] = &[
        ("startswith", "starts_with"),
        ("endswith", "ends_with"),
        ("len", "length"),
        ("ucase", "upper"),
        ("lcase", "lower"),
        ("sign", "signum"),
        ("char", "chr"),
        // Spark `array(e1, …)` constructs an array — identical to DataFusion's `make_array`.
        ("array", "make_array"),
    ];
    const AGG_ALIASES: &[(&str, &str)] = &[
        ("variance", "var_samp"),
        ("approx_count_distinct", "approx_distinct"),
        ("any", "bool_or"),
        ("some", "bool_or"),
        ("every", "bool_and"),
    ];

    let state = ctx.state();
    for (alias, target) in SCALAR_ALIASES {
        // If the target isn't registered (name drift across DataFusion versions), skip silently —
        // never panic the engine over an alias.
        if let Ok(udf) = state.udf(target) {
            // `(*udf).clone()` (not `Arc::unwrap_or_clone`, which needs Rust 1.76 > our 1.72 MSRV).
            ctx.register_udf((*udf).clone().with_aliases([*alias]));
        }
    }
    for (alias, target) in AGG_ALIASES {
        if let Ok(udaf) = state.udaf(target) {
            ctx.register_udaf((*udaf).clone().with_aliases([*alias]));
        }
    }
}

/// A custom [`ExprPlanner`] that lowers Spark's `/` operator to true (double-precision) division
/// whenever both operands are integral, matching Spark's documented `Divide` contract.
///
/// Spark's `/` always evaluates in `DOUBLE` for non-decimal operands — `cast(1 as int) / cast(1 as
/// int)` is the double `1.0`, `7 / 2` is `3.5`. DataFusion's default [`Operator::Divide`], by
/// contrast, performs *truncating integer* division and yields an integer type when both operands
/// are integral (`7 / 2` → `3`, `5 / 2` → `2`). Relative to Spark that is genuine data corruption
/// of both the value and the result type, not a formatting nit.
///
/// This is a faithful, EQUIVALENT-plan lowering (explicitly allowed by the parity contract:
/// "lowering Spark syntax to an equivalent DataFusion plan" matching Spark's documented `/`
/// contract), never a lossy rewrite: when both operand types are integral we rebuild the binary op
/// as `CAST(left AS DOUBLE) / CAST(right AS DOUBLE)`, so DataFusion evaluates it in double
/// precision and returns the Spark value/type. The output column name is unaffected — Spark (and
/// `spark_names::render`) omit coercion casts from a column's name, so the operands still render as
/// before.
///
/// Scope is deliberately narrow so no sibling row (in `division.sql` or elsewhere) regresses:
/// - only `Operator::Divide` (`/`); Spark integer division (`DIV`) is `Operator::IntegerDivide`,
///   a different operator, and is never matched;
/// - only when *both* operands are integral (signed/unsigned `Int*`). `DECIMAL` operands keep
///   Spark's decimal-division precision rules; `FLOAT`/`DOUBLE` operands are already double;
///   string/binary/boolean/date/timestamp/interval/null operands aren't integral, so the existing
///   error / exec parity for those rows is untouched;
/// - a *literal-zero* divisor is left to DataFusion's integer divide, which raises `DIVIDE_BY_ZERO`
///   exactly as Spark's ANSI `/` does. Lowering it to IEEE double division would instead yield a
///   non-erroring `Infinity` and silently drop a Spark error (`SELECT 5 / 0`), so we don't.
#[derive(Debug)]
struct SparkDividePlanner;

impl datafusion::logical_expr::planner::ExprPlanner for SparkDividePlanner {
    fn plan_binary_op(
        &self,
        expr: datafusion::logical_expr::planner::RawBinaryExpr,
        schema: &datafusion::common::DFSchema,
    ) -> datafusion::common::Result<
        datafusion::logical_expr::planner::PlannerResult<
            datafusion::logical_expr::planner::RawBinaryExpr,
        >,
    > {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::logical_expr::planner::PlannerResult;
        use datafusion::logical_expr::{cast, BinaryExpr, Expr, ExprSchemable, Operator};
        use datafusion::sql::sqlparser::ast::BinaryOperator;

        // Spark `/` only. (Spark integer division `DIV` is `Operator::IntegerDivide`, never `/`.)
        if !matches!(expr.op, BinaryOperator::Divide) {
            return Ok(PlannerResult::Original(expr));
        }
        // Resolve operand types against the input schema; if either is unresolvable (e.g. a bare
        // placeholder), defer to the default planner unchanged.
        let (Ok(left_ty), Ok(right_ty)) =
            (expr.left.get_type(schema), expr.right.get_type(schema))
        else {
            return Ok(PlannerResult::Original(expr));
        };
        // Both operands must be integral. Anything else is left exactly as DataFusion/Spark handle
        // it (decimal precision, already-double float, string/binary/bool/date/timestamp errors).
        if !is_integral(&left_ty) || !is_integral(&right_ty) {
            return Ok(PlannerResult::Original(expr));
        }
        // Preserve Spark's ANSI divide-by-zero error: a literal-zero divisor stays integer division
        // (DataFusion raises DIVIDE_BY_ZERO); double division would yield Infinity and drop the error.
        if is_literal_zero(&expr.right) {
            return Ok(PlannerResult::Original(expr));
        }

        let planned = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(cast(expr.left, DataType::Float64)),
            Operator::Divide,
            Box::new(cast(expr.right, DataType::Float64)),
        ));
        Ok(PlannerResult::Planned(planned))
    }
}

/// Whether `t` is one of Spark's integral types (the signed/unsigned fixed-width integers). Decimal,
/// float, and double are intentionally excluded — only these need Spark's true-division lowering.
fn is_integral(t: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::DataType::{
        Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64, UInt8,
    };
    matches!(
        t,
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
    )
}

/// Whether `e` is (a cast wrapper around) an integer literal `0`. Used to keep a literal-zero
/// divisor on DataFusion's integer-divide path, which raises `DIVIDE_BY_ZERO` like Spark ANSI `/`.
fn is_literal_zero(e: &datafusion::logical_expr::Expr) -> bool {
    use datafusion::common::ScalarValue::{
        Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64, UInt8,
    };
    use datafusion::logical_expr::Expr;
    match e {
        Expr::Cast(c) => is_literal_zero(&c.expr),
        Expr::TryCast(c) => is_literal_zero(&c.expr),
        Expr::Literal(v, _) => matches!(
            v,
            Int8(Some(0))
                | Int16(Some(0))
                | Int32(Some(0))
                | Int64(Some(0))
                | UInt8(Some(0))
                | UInt16(Some(0))
                | UInt32(Some(0))
                | UInt64(Some(0))
        ),
        _ => false,
    }
}

/// The CPU execution engine: a DataFusion [`SessionContext`] today, growing native
/// operators behind the same surface in Phase 1.
pub struct Engine {
    ctx: Arc<SessionContext>,
}

impl Engine {
    /// Create a fresh engine with default session state.
    ///
    /// If `WEFT_MEMORY_LIMIT_BYTES` is set, the engine runs with a bounded spill pool of
    /// that size (DataFusion spills aggregations/sorts to disk instead of OOM-killing the
    /// process) — important when running ClickBench on a memory-constrained box. Unset
    /// (the default) keeps the unbounded pool, so local/test behavior is unchanged.
    ///
    /// Phase 1.4 margin-push knobs, each applied only when its env var is set (so the default
    /// behavior is unchanged and the values can be swept on a benchmark box without a rebuild):
    /// - `WEFT_TARGET_PARTITIONS` (usize) — scan/aggregation parallelism (default = vCPUs).
    /// - `WEFT_BATCH_SIZE` (usize) — vectorized batch size (default 8192).
    /// - `WEFT_COALESCE_BATCHES` (bool) — coalesce small batches after filtering.
    /// - `WEFT_REPARTITION_AGGREGATIONS` (bool) — repartition before aggregation for parallelism
    ///   (the lever most likely to move the high-card `GROUP BY` queries Q32–Q34).
    pub fn new() -> Self {
        use datafusion::prelude::SessionConfig;

        let mut config = SessionConfig::new();
        if let Some(p) = env_usize("WEFT_TARGET_PARTITIONS") {
            config = config.with_target_partitions(p);
        }
        if let Some(n) = env_usize("WEFT_BATCH_SIZE") {
            config = config.with_batch_size(n);
        }
        // ClickBench-winning scan settings (mirrors DataFusion's published entry + what Sail
        // tunes): push filters into the Parquet decoder, reorder them by selectivity, read
        // binary columns as strings, and use Arrow StringView for big string columns (URL,
        // Title, Referer) — decisive for the string/scan-heavy queries (Q20–Q28, Q34/Q35).
        {
            let opts = config.options_mut();
            // Parse SQL the Spark way: the Databricks dialect (Databricks SQL *is* Spark SQL) uses
            // backticks for identifiers and treats `"..."` as a STRING LITERAL — Spark's default
            // (`spark.sql.ansi.double_quoted_identifiers=false`). DataFusion's Generic dialect treats
            // `"..."` as an identifier, which mis-parses Spark string literals like
            // `next_day("2015-07-23", "Mon")`.
            opts.sql_parser.dialect = datafusion::common::config::Dialect::Databricks;
            opts.execution.parquet.pushdown_filters = true;
            opts.execution.parquet.reorder_filters = true;
            opts.execution.parquet.binary_as_string = true;
            opts.execution.parquet.schema_force_view_types = true;
            if let Some(b) = env_bool("WEFT_COALESCE_BATCHES") {
                opts.execution.coalesce_batches = b;
            }
            if let Some(b) = env_bool("WEFT_REPARTITION_AGGREGATIONS") {
                opts.optimizer.repartition_aggregations = b;
            }
        }

        let mut ctx = match std::env::var("WEFT_MEMORY_LIMIT_BYTES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(bytes) => {
                use datafusion::execution::memory_pool::FairSpillPool;
                use datafusion::execution::runtime_env::RuntimeEnvBuilder;
                use std::sync::Arc;
                let env = RuntimeEnvBuilder::new()
                    .with_memory_pool(Arc::new(FairSpillPool::new(bytes)))
                    .build_arc()
                    .expect("runtime env");
                SessionContext::new_with_config_rt(config, env)
            }
            None => SessionContext::new_with_config(config),
        };
        register_spark_function_aliases(&ctx);
        spark_functions::register(&ctx);
        // Spark's `/` is true (double) division for non-decimal operands; lower integral `/` to a
        // double divide so it returns Spark's value/type instead of DataFusion's truncating integer
        // division. Additive: only Divide between two integral operands is rewritten (see
        // `SparkDividePlanner`); registration only appends a planner and cannot fail.
        {
            use datafusion::execution::FunctionRegistry;
            let _ = ctx.register_expr_planner(Arc::new(SparkDividePlanner));
        }
        Self { ctx: Arc::new(ctx) }
    }

    /// Run a SQL string and collect the result as Arrow record batches.
    ///
    /// Errors are mapped onto the Weft error model: a planning/analysis failure becomes
    /// [`Error::Plan`] (→ Spark `AnalysisException`), an execution failure [`Error::Execution`].
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let df = self.plan_spark(query).await?;
        df.collect()
            .await
            .map_err(|e| Error::Execution(e.to_string()))
    }

    /// Resolve the result schema of `query` without executing it — the logical-plan schema.
    /// Used by Spark Connect `AnalyzePlan(Schema)` (PySpark `df.schema` / `printSchema`).
    pub async fn schema(&self, query: &str) -> Result<arrow::datatypes::SchemaRef> {
        let df = self.plan_spark(query).await?;
        Ok(std::sync::Arc::new(df.schema().as_arrow().clone()))
    }

    /// Plan `query` and rewrite its top output projection to use Spark-compatible column names, so
    /// the executed result and `df.schema` both expose the same column names Spark would. Shared by
    /// [`Engine::sql`] and [`Engine::schema`] so the two never disagree.
    async fn plan_spark(&self, query: &str) -> Result<datafusion::dataframe::DataFrame> {
        let query = normalize_spark_sql(query);
        // Plan WITHOUT executing. `ctx.sql()` eagerly runs DDL (e.g. `CREATE VIEW`) inside its
        // call, registering the view *before* we could retype its body — so we go one level down:
        // `create_logical_plan` returns the raw, un-analyzed plan, we (1) retype in-range integer
        // literals to Int32 (Spark's `INT` default vs DataFusion's `BIGINT`) and (2) apply Spark
        // output column names, then hand the rewritten plan to `execute_logical_plan` (which runs
        // any DDL / builds the lazy DataFrame). Under the default `SQLOptions` `ctx.sql()` uses,
        // all statement kinds are allowed, so this is behavior-equivalent plus the two rewrites.
        let plan = self
            .ctx
            .state()
            .create_logical_plan(query.as_ref())
            .await
            .map_err(|e| Error::Plan(e.to_string()))?;
        // Order is load-bearing. `project_spark_names` runs FIRST, on the raw plan, so it sees the
        // bare (un-aliased) anonymous literal columns and renames them to their Spark names — its
        // outer projection then references the inner columns by their original DataFusion names.
        // `downcast_int_literals` runs SECOND and *preserves* exactly those names while retyping
        // Int64→Int32, so the Spark-name projection (and every other by-name reference) keeps
        // resolving. Reversing the order would hide the literals behind name-preserving aliases and
        // defeat the Spark-name pass.
        let plan = spark_names::project_spark_names(plan);
        let plan = spark_int_literals::downcast_int_literals(plan);
        self.ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| Error::Plan(e.to_string()))
    }

    /// Build the optimized DataFusion physical plan for `query`. The driver side of
    /// distributed execution uses this to obtain a serializable plan to split into stages.
    pub async fn physical_plan(
        &self,
        query: &str,
    ) -> Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let df = self
            .ctx
            .sql(query)
            .await
            .map_err(|e| Error::Plan(e.to_string()))?;
        df.create_physical_plan()
            .await
            .map_err(|e| Error::Execution(e.to_string()))
    }

    /// Build the (unoptimized) logical plan for a SQL query, without executing it.
    /// Used by Spark Connect `AnalyzePlan(Explain)` for a `spark.sql(...)` command.
    pub async fn logical_plan(&self, query: &str) -> Result<datafusion::logical_expr::LogicalPlan> {
        self.ctx
            .state()
            .create_logical_plan(query)
            .await
            .map_err(|e| Error::Plan(e.to_string()))
    }

    /// Render a Spark-style `EXPLAIN` string for a logical plan, for Spark Connect
    /// `AnalyzePlan(Explain)` (PySpark `df.explain()`). `extended` mirrors Spark's EXTENDED mode:
    /// it prepends the parsed + optimized logical plans; otherwise only the physical plan is shown
    /// (Spark's SIMPLE mode). Running the optimizer here also exercises the same passes (predicate
    /// / projection pushdown) the execution path applies, so the output reflects what will run.
    pub async fn explain(
        &self,
        plan: &datafusion::logical_expr::LogicalPlan,
        extended: bool,
    ) -> Result<String> {
        use std::fmt::Write as _;
        let mut out = String::new();
        if extended {
            let _ = write!(
                out,
                "== Parsed Logical Plan ==\n{}\n",
                plan.display_indent()
            );
        }
        let optimized = self
            .ctx
            .state()
            .optimize(plan)
            .map_err(|e| Error::Plan(e.to_string()))?;
        if extended {
            let _ = write!(
                out,
                "== Optimized Logical Plan ==\n{}\n",
                optimized.display_indent()
            );
        }
        let physical = self
            .ctx
            .state()
            .create_physical_plan(&optimized)
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;
        let _ = write!(
            out,
            "== Physical Plan ==\n{}",
            datafusion::physical_plan::displayable(physical.as_ref()).indent(false)
        );
        Ok(out)
    }

    /// Execute a DataFusion logical plan to record batches — the seam the Spark Connect relation
    /// translator uses to run lowered `DataFrame` plans.
    pub async fn execute_logical_plan(
        &self,
        plan: datafusion::logical_expr::LogicalPlan,
    ) -> Result<Vec<RecordBatch>> {
        self.ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| Error::Plan(e.to_string()))?
            .collect()
            .await
            .map_err(|e| Error::Execution(e.to_string()))
    }

    /// Execute an already-built physical plan to record batches (the worker side of a stage).
    pub async fn execute_plan(
        &self,
        plan: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>> {
        datafusion::physical_plan::collect(plan, self.ctx.task_ctx())
            .await
            .map_err(|e| Error::Execution(e.to_string()))
    }

    /// Register an in-memory table of `batches` under `name` — the worker-side landing zone
    /// for shuffle input, so a downstream stage can read it as an ordinary table. Idempotent: any
    /// existing table of the same name is replaced (a worker reuses its engine across queries, so
    /// `shuffle_input` is re-registered each time).
    pub fn register_batches(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        use datafusion::datasource::MemTable;
        use std::sync::Arc;

        let schema = match batches.first() {
            Some(b) => b.schema(),
            None => return Err(Error::Plan(format!("register `{name}`: no batches"))),
        };
        let table = MemTable::try_new(schema, vec![batches])
            .map_err(|e| Error::Execution(format!("mem table `{name}`: {e}")))?;
        // Drop any prior registration so re-registering the same name doesn't error.
        let _ = self.ctx.deregister_table(name);
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(|e| Error::Execution(format!("register `{name}`: {e}")))?;
        Ok(())
    }

    /// Snapshot of the session state, for building a `FunctionRegistry`/codec when
    /// deserializing physical-plan fragments shipped from the driver.
    pub fn session_state(&self) -> datafusion::execution::context::SessionState {
        self.ctx.state()
    }

    /// Register a Parquet file or directory under `name` (a thin wrapper over DataFusion's
    /// reader, so callers needn't depend on DataFusion's option types).
    pub async fn register_parquet(&self, name: &str, path: &str) -> Result<()> {
        use datafusion::prelude::ParquetReadOptions;
        self.ctx
            .register_parquet(name, path, ParquetReadOptions::default())
            .await
            .map_err(|e| Error::Execution(format!("register parquet `{name}`: {e}")))
    }

    /// Register a Delta Lake table directory under `name` — resolves active files from the
    /// `_delta_log` (via [`weft_datasource::delta_active_files`]), then the native reader.
    pub async fn register_delta(&self, name: &str, table_path: &str) -> Result<()> {
        let files = weft_datasource::delta_active_files(table_path)?;
        self.register_parquet_files(name, table_path, files).await
    }

    /// Register an Iceberg table directory under `name` — resolves data files from the current
    /// snapshot's manifests (via [`weft_datasource::iceberg_active_files`]), then the reader.
    pub async fn register_iceberg(&self, name: &str, table_path: &str) -> Result<()> {
        let files = weft_datasource::iceberg_active_files(table_path)?;
        self.register_parquet_files(name, table_path, files).await
    }

    /// Expose a set of Parquet files as a DataFusion listing table — the version-safe seam both
    /// lakehouse readers share (resolve the format to files, then use DataFusion 54's reader).
    async fn register_parquet_files(
        &self,
        name: &str,
        table_path: &str,
        files: Vec<std::path::PathBuf>,
    ) -> Result<()> {
        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};

        if files.is_empty() {
            return Err(Error::Plan(format!(
                "table `{table_path}` has no active data files"
            )));
        }
        let urls = files
            .iter()
            .map(|p| {
                ListingTableUrl::parse(p.to_string_lossy())
                    .map_err(|e| Error::Io(format!("bad file path {}: {e}", p.display())))
            })
            .collect::<Result<Vec<_>>>()?;
        let opts = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let table = build_listing_table(&self.ctx.state(), urls, opts, None).await?;
        self.ctx
            .register_table(name, table)
            .map_err(|e| Error::Execution(format!("register `{name}`: {e}")))?;
        Ok(())
    }

    /// Register an external catalog under `name`, bridging it into DataFusion's catalog API so
    /// `SELECT … FROM {name}.namespace.table` (and `spark.read.table("{name}.ns.t")`) resolve
    /// **lazily** — the catalog is hit only when a query first references one of its tables.
    pub fn register_catalog(&self, name: &str, provider: Arc<dyn weft_catalog::CatalogProvider>) {
        let bridge = Arc::new(catalog_bridge::WeftCatalogProvider::new(
            provider,
            self.ctx.clone(),
        ));
        self.ctx.register_catalog(name, bridge);
    }

    /// Access the underlying DataFusion context (e.g. to register tables/Parquet).
    pub fn ctx(&self) -> &SessionContext {
        self.ctx.as_ref()
    }

    /// Schema (database) names in the built-in in-process catalog — backs `listDatabases` for the
    /// default `spark_catalog` (the catalog holding temp views and ad-hoc registered tables).
    pub fn builtin_namespaces(&self) -> Vec<String> {
        let default = self.default_catalog_name();
        match self.ctx.catalog(&default) {
            Some(cat) => cat.schema_names(),
            None => Vec::new(),
        }
    }

    /// Table names in `schema` of the built-in catalog — backs `listTables` for `spark_catalog`.
    pub fn builtin_table_names(&self, schema: &str) -> Vec<String> {
        let default = self.default_catalog_name();
        self.ctx
            .catalog(&default)
            .and_then(|c| c.schema(schema))
            .map(|s| s.table_names())
            .unwrap_or_default()
    }

    fn default_catalog_name(&self) -> String {
        self.ctx
            .state()
            .config()
            .options()
            .catalog
            .default_catalog
            .clone()
    }
}

/// Build a DataFusion [`ListingTable`] over `urls` — the one place the Parquet/Delta/Iceberg
/// readers and the catalog bridge converge. Infers the schema from the data files unless `schema`
/// is supplied (a catalog that already knows the schema passes it, avoiding a metadata read and
/// handling empty tables). Returned as a `TableProvider` so callers can register it or hand it to
/// the bridge.
pub(crate) async fn build_listing_table(
    state: &datafusion::execution::context::SessionState,
    urls: Vec<datafusion::datasource::listing::ListingTableUrl>,
    options: datafusion::datasource::listing::ListingOptions,
    schema: Option<arrow::datatypes::SchemaRef>,
) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
    use datafusion::datasource::listing::{ListingTable, ListingTableConfig};

    let config = ListingTableConfig::new_with_multi_paths(urls).with_listing_options(options);
    let config = match schema {
        // Declared-schema path: read files *against* the catalog schema. Install a
        // case-insensitive physical-expression adapter so a lowercase catalog column (Glue's
        // `vendorid`) binds to a mixed-case file column (`VendorID`) — then DataFusion's default
        // adapter casts types as usual. Inference path (below) is left untouched.
        Some(s) => config
            .with_schema(s)
            .with_expr_adapter_factory(Arc::new(schema_adapt::CaseInsensitiveExprAdapterFactory)),
        None => config
            .infer_schema(state)
            .await
            .map_err(|e| Error::Execution(format!("infer schema: {e}")))?,
    };
    let table = ListingTable::try_new(config)
        .map_err(|e| Error::Execution(format!("listing table: {e}")))?;
    Ok(Arc::new(table))
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn select_one() {
        let engine = Engine::new();
        let batches = engine.sql("SELECT 1 AS x").await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[tokio::test]
    async fn select_arithmetic() {
        let engine = Engine::new();
        let batches = engine.sql("SELECT 40 + 2 AS answer").await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn normalize_strips_temporary_view() {
        // The four Spark spellings collapse to plain CREATE [OR REPLACE] VIEW, body untouched.
        assert_eq!(
            normalize_spark_sql("CREATE TEMPORARY VIEW t AS SELECT 1 a"),
            "CREATE VIEW t AS SELECT 1 a"
        );
        assert_eq!(
            normalize_spark_sql("CREATE OR REPLACE TEMPORARY VIEW t AS SELECT 1 a"),
            "CREATE OR REPLACE VIEW t AS SELECT 1 a"
        );
        assert_eq!(
            normalize_spark_sql("create global temporary view t as select 1"),
            "CREATE VIEW t as select 1"
        );
        // `TEMP` is Spark's accepted abbreviation for `TEMPORARY`.
        assert_eq!(
            normalize_spark_sql("CREATE TEMP VIEW df AS SELECT 1"),
            "CREATE VIEW df AS SELECT 1"
        );
        assert_eq!(
            normalize_spark_sql("CREATE GLOBAL TEMP VIEW v(a,b) AS VALUES (1,2)"),
            "CREATE VIEW v(a,b) AS VALUES (1,2)"
        );
        // Case-insensitive keywords, leading whitespace preserved.
        assert_eq!(
            normalize_spark_sql("  Create Temporary View v As Select 2"),
            "  CREATE VIEW v As Select 2"
        );
    }

    #[test]
    fn normalize_leaves_other_statements_untouched() {
        for q in [
            "SELECT * FROM t",
            "CREATE VIEW v AS SELECT 1",
            "CREATE TABLE t(a INT)",
            "CREATE TEMPORARY FUNCTION f AS 'x'",
            "INSERT INTO t VALUES (1)",
        ] {
            assert_eq!(normalize_spark_sql(q), q, "should not rewrite: {q}");
        }
    }

    #[test]
    fn normalize_rewrites_typed_literals() {
        // Each Spark suffix maps to the matching CAST.
        assert_eq!(
            normalize_spark_sql("SELECT 1Y, 2S, 3L, 4F, 5D"),
            "SELECT CAST(1 AS TINYINT), CAST(2 AS SMALLINT), CAST(3 AS BIGINT), \
             CAST(4 AS FLOAT), CAST(5 AS DOUBLE)"
        );
        // Fractions and exponents are part of the number; case-insensitive suffix.
        assert_eq!(
            normalize_spark_sql("VALUES (1.0d), (2.5e3D)"),
            "VALUES (CAST(1.0 AS DOUBLE)), (CAST(2.5e3 AS DOUBLE))"
        );
        // BD → DECIMAL with BigDecimal precision/scale.
        assert_eq!(
            normalize_spark_sql("SELECT 1.0BD, 0.1BD, 123BD, 0.001BD"),
            "SELECT CAST(1.0 AS DECIMAL(2,1)), CAST(0.1 AS DECIMAL(1,1)), \
             CAST(123 AS DECIMAL(3,0)), CAST(0.001 AS DECIMAL(3,3))"
        );
        // Protected contexts: string literals ('…' and Databricks "…"), backtick identifiers,
        // comments, ordinary identifiers, hex, and plain numbers are all left untouched.
        for q in [
            "SELECT '1L' AS s",
            "SELECT \"2Y\" AS s",
            "SELECT `3S` FROM t",
            "SELECT 1 -- a 4L comment\n",
            "SELECT /* 5D */ 1",
            "SELECT col1, a2d, x1L FROM t",
            "SELECT 0x1F, 1e5, 3.14, 42",
        ] {
            assert_eq!(normalize_spark_sql(q), q, "should not rewrite: {q}");
        }
    }

    #[tokio::test]
    async fn typed_literals_plan_and_eval() {
        let engine = Engine::new();
        // bigint literal resolves and computes (would otherwise be `No field named "3l"`).
        let b = engine.sql("SELECT 3L + 4L AS x").await.unwrap();
        let got = crate::arrow::util::pretty::pretty_format_batches(&b)
            .unwrap()
            .to_string();
        assert!(got.contains("7"), "got: {got}");
        // decimal literal keeps scale.
        let b = engine.sql("SELECT 1.0BD AS x").await.unwrap();
        let got = crate::arrow::util::pretty::pretty_format_batches(&b)
            .unwrap()
            .to_string();
        assert!(got.contains("1.0"), "got: {got}");
    }

    #[tokio::test]
    async fn spark_function_aliases_resolve() {
        let engine = Engine::new();
        // Scalar aliases delegate to the DataFusion builtin with identical semantics.
        for (q, want) in [
            ("SELECT startswith('hello', 'he') AS x", "true"),
            ("SELECT endswith('hello', 'lo') AS x", "true"),
            ("SELECT len('hello') AS x", "5"),
            ("SELECT ucase('abc') AS x", "ABC"),
            ("SELECT lcase('ABC') AS x", "abc"),
            ("SELECT sign(-3) AS x", "-1"),
        ] {
            let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
            let got = crate::arrow::util::pretty::pretty_format_batches(&batches)
                .unwrap()
                .to_string();
            assert!(got.contains(want), "{q} -> expected {want}, got:\n{got}");
        }
        // Aggregate aliases too.
        for q in [
            "SELECT variance(c) FROM (VALUES (1.0),(2.0),(3.0)) AS t(c)",
            "SELECT any(c) FROM (VALUES (true),(false)) AS t(c)",
            "SELECT every(c) FROM (VALUES (true),(false)) AS t(c)",
            "SELECT approx_count_distinct(c) FROM (VALUES (1),(2),(2)) AS t(c)",
        ] {
            engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        }
    }

    #[tokio::test]
    async fn temporary_view_then_query_roundtrips() {
        // The whole point: a Spark-style temp view registers and is queryable afterwards.
        let engine = Engine::new();
        engine
            .sql("CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES (1,2),(3,4) AS t(a,b)")
            .await
            .expect("temp view should register");
        let batches = engine
            .sql("SELECT COUNT(*) AS n FROM testData")
            .await
            .expect("query against temp view");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    #[tokio::test]
    async fn physical_plan_round_trips_through_execute() {
        let engine = Engine::new();
        let plan = engine.physical_plan("SELECT 1 AS x").await.unwrap();
        let batches = engine.execute_plan(plan).await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    #[tokio::test]
    async fn register_batches_is_queryable() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10, 20, 30]))])
                .unwrap();
        let engine = Engine::new();
        engine.register_batches("t", vec![batch]).unwrap();
        let out = engine.sql("SELECT SUM(v) AS s FROM t").await.unwrap();
        let s = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(s, 60);
    }

    #[tokio::test]
    async fn reads_a_delta_table() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        // Build a minimal Delta table: one Parquet data file + a single JSON commit that
        // `add`s it.
        let dir = std::env::temp_dir().join(format!("weft-delta-{}", std::process::id()));
        let log = dir.join("_delta_log");
        std::fs::create_dir_all(&log).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        {
            let f = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
            let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let commit = concat!(
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            "\n",
            r#"{"metaData":{"id":"t","format":{"provider":"parquet"},"schemaString":"{}","partitionColumns":[]}}"#,
            "\n",
            r#"{"add":{"path":"part-0.parquet","partitionValues":{},"size":1,"modificationTime":0,"dataChange":true}}"#,
            "\n",
        );
        std::fs::write(log.join("00000000000000000000.json"), commit).unwrap();

        let engine = Engine::new();
        engine
            .register_delta("t", dir.to_str().unwrap())
            .await
            .unwrap();
        let batches = engine
            .sql("SELECT COUNT(*) AS c, SUM(x) AS s FROM t")
            .await
            .unwrap();
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let s = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!((c, s), (4, 10));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
