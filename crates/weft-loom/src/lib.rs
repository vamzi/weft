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
    match strip_temporary_view(query) {
        Some(rewritten) => std::borrow::Cow::Owned(rewritten),
        None => std::borrow::Cow::Borrowed(query),
    }
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

        let ctx = match std::env::var("WEFT_MEMORY_LIMIT_BYTES")
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
        Self { ctx: Arc::new(ctx) }
    }

    /// Run a SQL string and collect the result as Arrow record batches.
    ///
    /// Errors are mapped onto the Weft error model: a planning/analysis failure becomes
    /// [`Error::Plan`] (→ Spark `AnalysisException`), an execution failure [`Error::Execution`].
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let query = normalize_spark_sql(query);
        let df = self
            .ctx
            .sql(query.as_ref())
            .await
            .map_err(|e| Error::Plan(e.to_string()))?;
        df.collect()
            .await
            .map_err(|e| Error::Execution(e.to_string()))
    }

    /// Resolve the result schema of `query` without executing it — the logical-plan schema.
    /// Used by Spark Connect `AnalyzePlan(Schema)` (PySpark `df.schema` / `printSchema`).
    pub async fn schema(&self, query: &str) -> Result<arrow::datatypes::SchemaRef> {
        let query = normalize_spark_sql(query);
        let df = self
            .ctx
            .sql(query.as_ref())
            .await
            .map_err(|e| Error::Plan(e.to_string()))?;
        Ok(std::sync::Arc::new(df.schema().as_arrow().clone()))
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
