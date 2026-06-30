//! Derive a distributed [`StageDef`] DAG automatically from a SQL query.
//!
//! ## Supported shape (v1)
//!
//! A single **grouped aggregation** over one table (optionally filtered, sorted, limited):
//!
//! ```sql
//! SELECT <group cols>, <aggregates> FROM t [WHERE ...] GROUP BY <cols> [ORDER BY ...] [LIMIT n]
//! ```
//!
//! It lowers to the canonical two stages — *partial aggregate per worker → hash shuffle by the
//! group key → final combine*:
//!
//! - re-combinable aggregates lower directly (`SUM→SUM`, `COUNT→SUM`, `MIN→MIN`, `MAX→MAX`);
//! - `AVG(x)` is split into `SUM(x)`/`COUNT(x)` partials and recombined as `Σsum / Σcount`;
//! - `COUNT(DISTINCT x)` (and any other `DISTINCT` aggregate) can't pre-aggregate, so the partial
//!   stage instead *projects* the grouping + argument columns and shuffles the raw rows by the
//!   group key; the final stage runs the original aggregate over the co-located rows (exact,
//!   because every group lands wholly on one worker).
//!
//! ## Joins (broadcast)
//!
//! A join is auto-derived when every base table but one is **replicated** (passed in `replicated` —
//! present in full on every worker): the join then runs locally per worker over the single sharded
//! table's shard, so it folds straight into the partial stage's FROM tail with no extra shuffle.
//! This covers star schemas (a sharded fact + replicated dimensions). Joins between two *sharded*
//! tables need an explicit shuffle-join plan (see `tests/distributed_join.rs`); auto-deriving those
//! is a follow-up.
//!
//! Anything else (ungrouped/global aggregates, two+ sharded tables, `HAVING`, window functions, set
//! operations, nested subqueries) returns [`Error::Unsupported`] so the caller falls back to
//! single-node execution.

use std::collections::HashMap;

use datafusion::logical_expr::{Aggregate, Expr, LogicalPlan};
use datafusion::sql::unparser::Unparser;
use weft_common::{Error, Result};
use weft_loom::Engine;

use crate::driver::StageDef;

/// A query lowered to a distributed [`StageDef`] DAG.
#[derive(Debug, Clone)]
pub struct DistributedQuery {
    /// Topologically-ordered stages; the last is the output stage. Its result is the grouped
    /// aggregation, **unordered** — a global `ORDER BY` / `LIMIT` can't be applied per-worker.
    pub stages: Vec<StageDef>,
    /// Optional global finalize to run on the *gathered* result (registered as table `result`):
    /// the query's `ORDER BY` / `LIMIT`, which must run once over all workers' output, not per
    /// worker. `None` when the query has neither.
    pub finalize_sql: Option<String>,
}

/// Derive a distributed plan for `sql`, or [`Error::Unsupported`] if its shape isn't handled yet.
///
/// `replicated` names base tables that are present in **full** on every worker (small dimension
/// tables). A join is auto-derived as a **broadcast join** — it runs locally per worker — as long as
/// every table but one is replicated (so exactly one table is sharded). Joins between two *sharded*
/// tables need an explicit shuffle-join plan (see `tests/distributed_join.rs`); auto-deriving those
/// is a follow-up.
pub async fn plan_distributed(
    engine: &Engine,
    sql: &str,
    replicated: &[&str],
) -> Result<DistributedQuery> {
    let lp = engine.logical_plan(sql).await?;
    let peeled = peel(&lp)?;
    aggregation_stages(&peeled, replicated)
}

/// The top of the plan above the aggregate: the output projection (if any) plus the trailing
/// `ORDER BY` / `LIMIT`, which the final stage must reproduce.
struct Peeled<'a> {
    /// Output projection exprs (the SELECT list), if the plan has a `Projection` over the aggregate.
    projection: Option<&'a [Expr]>,
    /// `ORDER BY` exprs to apply on the final output, if any.
    sort: Option<&'a [datafusion::logical_expr::SortExpr]>,
    /// `LIMIT` fetch count, if any.
    limit: Option<usize>,
    /// The aggregate node itself.
    agg: &'a Aggregate,
}

/// Strip an optional `Limit` / `Sort` / `Projection` off the top and require an `Aggregate` under
/// them. Rejects anything else (the caller falls back to single-node).
fn peel(lp: &LogicalPlan) -> Result<Peeled<'_>> {
    let mut limit = None;
    let mut sort = None;
    let mut projection = None;
    let mut node = lp;
    loop {
        match node {
            LogicalPlan::Limit(l) => {
                // Only a plain `LIMIT n` (no OFFSET) is supported; fetch is an Expr in DF54.
                if let Some(Expr::Literal(scalar, _)) = l.fetch.as_deref() {
                    limit = scalar_as_usize(scalar);
                }
                node = &l.input;
            }
            LogicalPlan::Sort(s) => {
                sort = Some(s.expr.as_slice());
                node = &s.input;
            }
            LogicalPlan::Projection(p) => {
                projection = Some(p.expr.as_slice());
                node = &p.input;
            }
            LogicalPlan::Aggregate(agg) => {
                return Ok(Peeled {
                    projection,
                    sort,
                    limit,
                    agg,
                })
            }
            other => {
                return Err(Error::Unsupported(format!(
                    "auto-distribute: unsupported top-level plan node `{}`",
                    other.display().to_string().lines().next().unwrap_or("")
                )))
            }
        }
    }
}

/// Build the two-stage partial→final plan for a grouped aggregation.
fn aggregation_stages(p: &Peeled, replicated: &[&str]) -> Result<DistributedQuery> {
    let agg = p.agg;
    if agg.group_expr.is_empty() {
        return Err(Error::Unsupported(
            "auto-distribute: ungrouped/global aggregation not yet supported".into(),
        ));
    }
    // Broadcast-join safety: the partial stage runs the join locally per worker, so exactly one base
    // table may be sharded; every other must be replicated in full on every worker. (Zero sharded
    // tables would duplicate the fully-replicated result across workers; two+ need a shuffle join.)
    let tables = base_tables(&agg.input);
    let sharded: Vec<&String> = tables
        .iter()
        .filter(|t| !replicated.contains(&t.as_str()))
        .collect();
    if sharded.len() != 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: need exactly one sharded base table (others replicated), \
             found {} sharded among {tables:?}",
            sharded.len()
        )));
    }
    // The aggregate's input must unparse to a plain `SELECT * FROM …` so we can splice our own
    // SELECT list onto its FROM/WHERE tail without losing column qualifiers.
    let input_sql = Unparser::default()
        .plan_to_sql(&agg.input)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse input: {e}")))?
        .to_string();
    let tail = input_sql
        .strip_prefix("SELECT * ")
        .ok_or_else(|| Error::Unsupported("auto-distribute: non-trivial aggregate input".into()))?;
    let tail = sanitize_generated_sql(tail);

    // Broadcast is only correct if the sharded table is *scanned* exactly once (the driving fact).
    // A second scan — a self-join or a correlated EXISTS/IN subquery over it — would see only the
    // local shard per worker and silently lose cross-shard rows, so reject it. (`base_tables` counts
    // the plan-input scan only; subquery scans live in expressions, so descend into those too.)
    let sharded_name = sharded[0].as_str();
    let scans = count_table_scans(&agg.input, sharded_name);
    if scans > 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: sharded table `{sharded_name}` scanned {scans}× \
             (self-join / subquery) — not broadcast-safe"
        )));
    }

    let up = Unparser::default();
    let group_sql: Vec<String> = agg
        .group_expr
        .iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<_>>()?;

    let aggs = agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    let distinct = aggs.iter().any(|a| a.distinct);

    // remap: original output column name -> safe name (`g{j}` group, `r{i}` aggregate result).
    let mut remap: HashMap<String, String> = HashMap::new();
    for (j, g) in agg.group_expr.iter().enumerate() {
        remap.insert(g.schema_name().to_string(), format!("g{j}"));
    }
    for (i, a) in agg.aggr_expr.iter().enumerate() {
        remap.insert(a.schema_name().to_string(), format!("r{i}"));
    }

    let (partial_sql, final_sql) = if distinct {
        distinct_stage_sql(&up, p, &group_sql, &aggs, &tail, &remap)?
    } else {
        recombine_stage_sql(p, &group_sql, &aggs, &tail, &remap)?
    };

    let hash_key_cols: Vec<u32> = (0..group_sql.len() as u32).collect();
    Ok(DistributedQuery {
        stages: vec![
            StageDef {
                stage_id: 0,
                sql: partial_sql,
                upstream_stage_ids: vec![],
                hash_key_cols,
            },
            StageDef {
                stage_id: 1,
                sql: final_sql,
                upstream_stage_ids: vec![0],
                hash_key_cols: vec![],
            },
        ],
        finalize_sql: build_finalize(p)?,
    })
}

/// Build the global finalize query (`ORDER BY` / `LIMIT` over the gathered `result` table), or
/// `None` when the query has neither. Sort exprs reference output columns; `result` carries those
/// under their unqualified output names, so column refs are unqualified (e.g. `lineitem.l_returnflag`
/// → `l_returnflag`, matching `wrap_output`'s aliasing) before unparsing.
fn build_finalize(p: &Peeled) -> Result<Option<String>> {
    if p.sort.is_none() && p.limit.is_none() {
        return Ok(None);
    }
    let up = Unparser::default();
    let mut sql = String::from("SELECT * FROM result");
    if let Some(sorts) = p.sort {
        let parts = sorts
            .iter()
            .map(|s| {
                let dir = if s.asc { "ASC" } else { "DESC" };
                let nulls = if s.nulls_first {
                    "NULLS FIRST"
                } else {
                    "NULLS LAST"
                };
                Ok(format!(
                    "{} {dir} {nulls}",
                    expr_sql(&up, &unqualify(&s.expr))?
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        if !parts.is_empty() {
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }
    }
    if let Some(n) = p.limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    Ok(Some(sql))
}

/// One aggregate in the SELECT list, classified for partial/final decomposition.
struct AggSpec {
    /// Lowercased function name (`sum`/`count`/`min`/`max`/`avg`).
    func: String,
    /// SQL of the (single) argument, e.g. `t.v` (or `1` for `count(*)`).
    arg_sql: String,
    /// Whether the aggregate is `DISTINCT`.
    distinct: bool,
}

impl AggSpec {
    fn classify(e: &Expr) -> Result<AggSpec> {
        let Expr::AggregateFunction(af) = e else {
            return Err(Error::Unsupported(format!(
                "auto-distribute: non-aggregate in aggregate list: {e}"
            )));
        };
        let func = af.func.name().to_ascii_lowercase();
        let up = Unparser::default();
        let arg_sql = match af.params.args.first() {
            Some(a) => expr_sql(&up, a)?,
            None => "1".to_string(), // count(*) carries no arg
        };
        Ok(AggSpec {
            func,
            arg_sql,
            distinct: af.params.distinct,
        })
    }
}

/// Re-combinable path (no DISTINCT): partial aggregates per worker, final recombines.
fn recombine_stage_sql(
    p: &Peeled,
    group_sql: &[String],
    aggs: &[AggSpec],
    tail: &str,
    remap: &HashMap<String, String>,
) -> Result<(String, String)> {
    // Partial SELECT list: group cols as g{j}, then per-aggregate partial state.
    let mut psel: Vec<String> = group_sql
        .iter()
        .enumerate()
        .map(|(j, g)| format!("{g} AS g{j}"))
        .collect();
    // Final combine SELECT list (over `shuffle_input`): g{j} group cols + recombined aggregates.
    let mut combine: Vec<String> = (0..group_sql.len()).map(|j| format!("g{j}")).collect();

    for (i, a) in aggs.iter().enumerate() {
        match a.func.as_str() {
            "sum" => {
                psel.push(format!("sum({}) AS a{i}", a.arg_sql));
                combine.push(format!("sum(a{i}) AS r{i}"));
            }
            "count" => {
                psel.push(format!("count({}) AS a{i}", a.arg_sql));
                combine.push(format!("sum(a{i}) AS r{i}")); // counts recombine by summing
            }
            "min" => {
                psel.push(format!("min({}) AS a{i}", a.arg_sql));
                combine.push(format!("min(a{i}) AS r{i}"));
            }
            "max" => {
                psel.push(format!("max({}) AS a{i}", a.arg_sql));
                combine.push(format!("max(a{i}) AS r{i}"));
            }
            "avg" => {
                psel.push(format!(
                    "sum({}) AS a{i}s, count({}) AS a{i}c",
                    a.arg_sql, a.arg_sql
                ));
                combine.push(format!(
                    "(CAST(sum(a{i}s) AS DOUBLE) / NULLIF(sum(a{i}c), 0)) AS r{i}"
                ));
            }
            other => {
                return Err(Error::Unsupported(format!(
                    "auto-distribute: aggregate `{other}` not supported"
                )))
            }
        }
    }

    let group_by = group_sql.join(", ");
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} {tail} GROUP BY {group_by}",
        psel.join(", ")
    ));
    let inner = format!(
        "SELECT {} FROM shuffle_input GROUP BY {}",
        combine.join(", "),
        (0..group_sql.len())
            .map(|j| format!("g{j}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let final_sql = wrap_output(p, &inner, remap)?;
    Ok((partial_sql, final_sql))
}

/// DISTINCT path: shuffle the raw grouping + argument columns by group key, run the original
/// aggregate in the final stage (exact, since each group is co-located on one worker).
fn distinct_stage_sql(
    _up: &Unparser,
    p: &Peeled,
    group_sql: &[String],
    aggs: &[AggSpec],
    tail: &str,
    remap: &HashMap<String, String>,
) -> Result<(String, String)> {
    // Partial: project group cols (g{j}) and each aggregate's argument column (c{i}); no aggregation.
    let mut psel: Vec<String> = group_sql
        .iter()
        .enumerate()
        .map(|(j, g)| format!("{g} AS g{j}"))
        .collect();
    for (i, a) in aggs.iter().enumerate() {
        psel.push(format!("{} AS c{i}", a.arg_sql));
    }
    let partial_sql = sanitize_generated_sql(&format!("SELECT {} {tail}", psel.join(", ")));

    // Final: re-run each aggregate over the projected columns, grouped by g{j}.
    let mut combine: Vec<String> = (0..group_sql.len()).map(|j| format!("g{j}")).collect();
    for (i, a) in aggs.iter().enumerate() {
        let d = if a.distinct { "DISTINCT " } else { "" };
        combine.push(format!("{}({d}c{i}) AS r{i}", a.func));
    }
    let inner = format!(
        "SELECT {} FROM shuffle_input GROUP BY {}",
        combine.join(", "),
        (0..group_sql.len())
            .map(|j| format!("g{j}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let final_sql = wrap_output(p, &inner, remap)?;
    Ok((partial_sql, final_sql))
}

/// Wrap the combined inner query so the final stage's output matches the original query's columns:
/// re-apply the output projection with aggregate/group columns remapped to `r{i}`/`g{j}`, each
/// item explicitly aliased back to its original output name (so a bare `t.k` stays column `k`, and
/// downstream `ORDER BY` over those names resolves). `ORDER BY` / `LIMIT` are *not* applied here —
/// they're global and run in [`build_finalize`].
fn wrap_output(p: &Peeled, inner: &str, remap: &HashMap<String, String>) -> Result<String> {
    let up = Unparser::default();
    let select = match p.projection {
        Some(exprs) => exprs
            .iter()
            .map(|e| {
                let name = output_name(e);
                let sql = expr_sql(&up, &remap_columns(strip_alias(e), remap))?;
                Ok(format!("{sql} AS \"{name}\""))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", "),
        None => "*".to_string(),
    };
    Ok(format!("SELECT {select} FROM ({inner}) AS combined"))
}

/// The output column name an expr produces: the alias if present, the unqualified column name for
/// a bare column reference (so `t.k` stays `k`, matching non-distributed output), else its schema
/// name.
fn output_name(e: &Expr) -> String {
    match e {
        Expr::Alias(a) => a.name.clone(),
        Expr::Column(c) => c.name.clone(),
        other => other.schema_name().to_string(),
    }
}

/// The expr without its top-level alias (so we can re-alias after remapping).
fn strip_alias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => &a.expr,
        other => other,
    }
}

/// Drop the table qualifier from every column reference (e.g. `lineitem.l_returnflag` →
/// `l_returnflag`), so a sort over the gathered `result` table resolves against its unqualified
/// output column names.
fn unqualify(e: &Expr) -> Expr {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    e.clone()
        .transform(|node| {
            if let Expr::Column(c) = &node {
                return Ok(Transformed::yes(datafusion::prelude::col(c.name.clone())));
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .unwrap_or(e.clone())
}

/// Replace any column reference whose flat name is in `remap` with the safe-named column.
fn remap_columns(e: &Expr, remap: &HashMap<String, String>) -> Expr {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    e.clone()
        .transform(|node| {
            if let Expr::Column(c) = &node {
                if let Some(safe) = remap.get(&c.flat_name()) {
                    return Ok(Transformed::yes(datafusion::prelude::col(safe)));
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .unwrap_or(e.clone())
}

/// Unparse an expr to SQL text.
fn expr_sql(up: &Unparser, e: &Expr) -> Result<String> {
    up.expr_to_sql(e)
        .map(|ast| sanitize_generated_sql(&ast.to_string()))
        .map_err(|err| Error::Unsupported(format!("auto-distribute: unparse expr: {err}")))
}

/// Fix SQL fragments from DataFusion's Unparser that the Databricks-dialect re-parser rejects.
///
/// Two common failure modes when generated stage SQL is sent to workers:
/// - `alias."col"` — dot access with a double-quoted column name;
/// - `"table".col` — dot access on a double-quoted table name (e.g. reserved `part`).
fn sanitize_generated_sql(sql: &str) -> String {
    fix_quoted_column_after_dot(&fix_quoted_table_dot_access(sql))
}

/// `"table".col` → `` `table`.col `` so dot access parses under the Databricks dialect.
fn fix_quoted_table_dot_access(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let bytes = sql.as_bytes();
    while i < bytes.len() {
        if bytes[i] == b'"' {
            let start = i;
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                i += 1;
            }
            if i < bytes.len() {
                let ident = &sql[start + 1..i];
                i += 1; // closing quote
                if i < bytes.len() && bytes[i] == b'.' && is_simple_ident(ident) {
                    out.push('`');
                    out.push_str(ident);
                    out.push('`');
                    out.push('.');
                    i += 1;
                    continue;
                }
                out.push_str(&sql[start..i]);
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// `alias."col"` → `alias.col` when `col` is a plain identifier.
fn fix_quoted_column_after_dot(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let bytes = sql.as_bytes();
    while i < bytes.len() {
        let start = i;
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b'.' && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                let qstart = i + 2;
                let mut j = qstart;
                while j < bytes.len() && bytes[j] != b'"' {
                    j += 1;
                }
                if j < bytes.len() {
                    let ident = &sql[qstart..j];
                    if is_simple_ident(ident) {
                        out.push_str(&sql[start..=i]);
                        out.push_str(ident);
                        i = j + 1;
                        continue;
                    }
                }
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn is_simple_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Extract a non-negative integer `LIMIT` value from a literal scalar.
fn scalar_as_usize(s: &datafusion::scalar::ScalarValue) -> Option<usize> {
    use datafusion::scalar::ScalarValue::*;
    match s {
        Int64(Some(v)) if *v >= 0 => Some(*v as usize),
        Int32(Some(v)) if *v >= 0 => Some(*v as usize),
        UInt64(Some(v)) => Some(*v as usize),
        UInt32(Some(v)) => Some(*v as usize),
        _ => None,
    }
}

/// Count scans of table `name` anywhere in `lp` — across plan inputs **and** subquery plans nested
/// in expressions (EXISTS / IN / scalar subqueries), so a correlated subquery over the table counts.
fn count_table_scans(lp: &LogicalPlan, name: &str) -> usize {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut n = match lp {
        LogicalPlan::TableScan(s) if s.table_name.table() == name => 1,
        _ => 0,
    };
    for c in lp.inputs() {
        n += count_table_scans(c, name);
    }
    for e in lp.expressions() {
        let _ = e.apply(|node| {
            let sub = match node {
                Expr::Exists(ex) => Some(&ex.subquery.subquery),
                Expr::InSubquery(iq) => Some(&iq.subquery.subquery),
                Expr::ScalarSubquery(sq) => Some(&sq.subquery),
                _ => None,
            };
            if let Some(plan) = sub {
                n += count_table_scans(plan, name);
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
    n
}

/// Collect the base (scanned) table names referenced anywhere in `lp`.
fn base_tables(lp: &LogicalPlan) -> Vec<String> {
    let mut out = Vec::new();
    collect_tables(lp, &mut out);
    out
}

fn collect_tables(lp: &LogicalPlan, out: &mut Vec<String>) {
    if let LogicalPlan::TableScan(s) = lp {
        out.push(s.table_name.table().to_string());
    }
    for c in lp.inputs() {
        collect_tables(c, out);
    }
}

#[cfg(test)]
mod sanitize_tests {
    use super::{fix_quoted_column_after_dot, fix_quoted_table_dot_access, sanitize_generated_sql};

    #[test]
    fn quoted_column_after_dot_becomes_unquoted() {
        let sql = r#"sum(shipping."volume")"#;
        assert_eq!(fix_quoted_column_after_dot(sql), "sum(shipping.volume)");
    }

    #[test]
    fn quoted_table_dot_access_uses_backticks() {
        let sql = r#""part".p_partkey = lineitem.l_partkey"#;
        assert_eq!(
            fix_quoted_table_dot_access(sql),
            "`part`.p_partkey = lineitem.l_partkey"
        );
    }

    #[test]
    fn sanitize_composes_both_fixes() {
        let sql = r#"SELECT sum(shipping."volume") FROM "part" WHERE "part".p_partkey = 1"#;
        let got = sanitize_generated_sql(sql);
        assert!(got.contains("shipping.volume"));
        assert!(got.contains("`part`.p_partkey"));
        assert!(!got.contains(r#""volume""#));
    }
}
