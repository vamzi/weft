//! Spark-compatible output column naming for the top result projection.
//!
//! A drop-in Spark replacement must return the same *result column names* as Spark — BI tools,
//! `df.columns`, and `CREATE TABLE AS` all depend on them. DataFusion's auto-generated names for
//! anonymous (un-aliased) projection expressions diverge from Spark's `Expression.sql` /
//! `toPrettySQL` output in cosmetic-but-load-bearing ways:
//!
//! | divergence                         | DataFusion           | Spark            |
//! |------------------------------------|----------------------|------------------|
//! | string / numeric / bool literals   | `Utf8("x")`,`Int64(1)`| `x`, `1`, `true`|
//! | qualified column refs              | `t.a`                | `a`              |
//! | the array constructor              | `make_array(…)`      | `array(…)`       |
//! | binary operations                  | `a = 1`              | `(a = 1)`        |
//! | function argument separator        | `f(a,b)`             | `f(a, b)`        |
//! | binary / date literals             | `Binary("…")`        | `X'…'`, `DATE '…'`|
//!
//! [`project_spark_names`] reads the **top** projection's expressions (descending through the
//! result-shaping wrappers `Sort`/`Limit`/`Distinct` that preserve their input's columns) and, when
//! any column's Spark name differs from DataFusion's, wraps the whole plan in one **outer**
//! renaming projection: `SELECT col0 AS spark0, col1 AS spark1, … FROM (original plan)`.
//!
//! Wrapping rather than mutating the inner projection is the load-bearing correctness choice: a
//! `Sort`/`Filter`/CTE above the projection references its output columns *by name*, so renaming
//! those columns in place breaks resolution (`ORDER BY 1`, `GROUP BY ALL`, window plans all fail).
//! The outer projection leaves the inner plan byte-identical and only renames the final output, so
//! nothing internal can break. Explicit user aliases (`SELECT a AS foo`) are passed through
//! unchanged. Only names change; types and row order are untouched (projection is 1:1, order-
//! preserving). If the rename would produce duplicate output names (`SELECT 1, 1` → two `1`
//! columns, which DataFusion projections forbid) we leave the plan as-is — those rows simply keep
//! DataFusion's names rather than regress.
//!
//! This runs on the production `Engine::sql` / `Engine::schema` path (not a comparison-time hack),
//! so the Spark-parity gain is a real drop-in-compatibility improvement, not a measurement artifact.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Distinct, Expr, LogicalPlan, Projection};

/// Wrap `plan` in an outer projection that renames anonymous output columns to their Spark names.
/// Returns the plan unchanged when there's nothing to rename, when the root isn't a recognized
/// projection-bearing shape, or when the rename would collide on output names (all safe no-ops).
pub fn project_spark_names(plan: LogicalPlan) -> LogicalPlan {
    let Some(proj) = top_projection(&plan) else {
        return plan;
    };
    let schema = plan.schema();
    // The wrappers above the projection preserve its schema, so the root's output fields line up
    // 1:1 with the projection's expressions. If they somehow don't, bail rather than mis-map.
    if schema.fields().len() != proj.expr.len() {
        return plan;
    }

    let mut outer: Vec<Expr> = Vec::with_capacity(proj.expr.len());
    let mut seen: HashSet<String> = HashSet::with_capacity(proj.expr.len());
    let mut changed = false;
    for (i, pe) in proj.expr.iter().enumerate() {
        let (qualifier, field) = schema.qualified_field(i);
        let col = Expr::Column(Column::new(qualifier.cloned(), field.name()));
        // Reference the existing output column; rename it only if it's anonymous and its Spark
        // name differs. User-aliased columns keep their (already correct) name.
        let out_name = if matches!(pe, Expr::Alias(_)) {
            field.name().to_string()
        } else {
            let spark = spark_expr_name(pe);
            if spark != *field.name() {
                changed = true;
            }
            spark
        };
        // Duplicate output names would make `Projection::try_new` reject the plan — Spark permits
        // them but DataFusion doesn't, so bail and keep DataFusion's (distinct) names instead.
        if !seen.insert(out_name.clone()) {
            return plan;
        }
        outer.push(if out_name == *field.name() {
            col
        } else {
            col.alias(out_name)
        });
    }

    if !changed {
        return plan;
    }
    let input = Arc::new(plan);
    match Projection::try_new(outer, Arc::clone(&input)) {
        Ok(p) => LogicalPlan::Projection(p),
        // Unreachable in practice (columns come from the input schema and names are unique), but
        // never panic the engine: fall back to the original plan.
        Err(_) => (*input).clone(),
    }
}

/// The first `Projection` reached from the plan root by descending only through result-shaping
/// wrappers that pass their input's columns through unchanged. `None` if the root isn't such a
/// shape (e.g. a bare `Aggregate`, `Union`, `SubqueryAlias`, or `Distinct::On`).
fn top_projection(plan: &LogicalPlan) -> Option<&Projection> {
    match plan {
        LogicalPlan::Projection(p) => Some(p),
        LogicalPlan::Sort(s) => top_projection(&s.input),
        LogicalPlan::Limit(l) => top_projection(&l.input),
        LogicalPlan::Distinct(Distinct::All(input)) => top_projection(input),
        _ => None,
    }
}

/// Spark's auto-generated output name for a top-projection expression. Pure function over an `Expr`.
pub fn spark_expr_name(e: &Expr) -> String {
    render(e)
}

/// Recursive Spark-`prettyName` renderer. Variants we model explicitly are rendered Spark-style;
/// anything else falls back to DataFusion's own schema name (safe — that column simply keeps its
/// current name and stays in the `schema-only` bucket rather than regressing).
fn render(e: &Expr) -> String {
    match e {
        // An `Alias` reached *inside* an expression is an internal artifact (e.g. the
        // `array`→`make_array` alias wraps its call), never a user alias — render the inner expr.
        Expr::Alias(a) => render(&a.expr),
        // Spark prints the unqualified column name.
        Expr::Column(c) => c.name.clone(),
        Expr::Literal(v, _) => render_literal(v),
        // Spark fully parenthesizes binary operations, including nested ones.
        Expr::BinaryExpr(b) => format!("({} {} {})", render(&b.left), b.op, render(&b.right)),
        // Spark (like Postgres) omits coercion casts from the column name.
        Expr::Cast(c) => render(&c.expr),
        Expr::TryCast(c) => render(&c.expr),
        Expr::Negative(x) => format!("(- {})", render(x)),
        Expr::Not(x) => format!("NOT {}", render(x)),
        Expr::IsNull(x) => format!("{} IS NULL", render(x)),
        Expr::IsNotNull(x) => format!("{} IS NOT NULL", render(x)),
        Expr::ScalarFunction(sf) => render_scalar_fn(sf),
        _ => e.schema_name().to_string(),
    }
}

/// Render a scalar function call Spark-style: `prettyName(arg, arg, …)` with comma-**space**
/// separators (DataFusion uses no space) and the `make_array`→`array` constructor rename.
fn render_scalar_fn(sf: &ScalarFunction) -> String {
    let name = match sf.func.name() {
        "make_array" => "array",
        other => other,
    };
    let args = sf
        .args
        .iter()
        .map(render)
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({args})")
}

/// Render a literal the way Spark names it: bare value text, full-length `X'…'` for binary,
/// `DATE '…'` for dates. (Typed-null `CAST(NULL AS T)` spelling is deferred to a later increment;
/// a bare `NULL` here can only leave such columns in `schema-only`, never regress a passing one.)
fn render_literal(v: &ScalarValue) -> String {
    if v.is_null() {
        return "NULL".to_string();
    }
    match v {
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => spark_hex(b),
        ScalarValue::Date32(Some(_)) | ScalarValue::Date64(Some(_)) => {
            // `ScalarValue`'s `Display` already renders the ISO date; Spark wraps it in `DATE '…'`.
            format!("DATE '{v}'")
        }
        // For the common scalars (`Utf8`, `Int*`, `Float*`, `Boolean`, …) `ScalarValue`'s `Display`
        // is already the bare Spark form (`x`, `11`, `2.1`, `true`); the `Utf8("x")` wrapper weft
        // emits today comes from `Expr`'s debug-style display, which we bypass here.
        _ => v.to_string(),
    }
}

/// Uppercase `X'…'` hex spelling of a binary literal (Spark's `Literal.sql` for `BinaryType`).
/// Unlike `ScalarValue`'s `Display`, this renders the full byte string (Display truncates to 10).
fn spark_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2 + 3);
    s.push_str("X'");
    for b in bytes {
        s.push_str(&format!("{b:02X}"));
    }
    s.push('\'');
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit, Operator};

    fn binary(l: Expr, op: Operator, r: Expr) -> Expr {
        Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(l),
            op,
            Box::new(r),
        ))
    }

    #[test]
    fn strips_literal_type_wrappers() {
        assert_eq!(spark_expr_name(&lit("x")), "x");
        assert_eq!(spark_expr_name(&lit(11i64)), "11");
        assert_eq!(spark_expr_name(&lit(true)), "true");
    }

    #[test]
    fn unqualifies_columns() {
        assert_eq!(spark_expr_name(&col("testdata.a")), "a");
        assert_eq!(spark_expr_name(&col("a")), "a");
    }

    #[test]
    fn parenthesizes_binary_ops() {
        assert_eq!(
            spark_expr_name(&binary(col("a"), Operator::Eq, lit(1i64))),
            "(a = 1)"
        );
        // nested: (a + (b / 2))
        let inner = binary(col("b"), Operator::Divide, lit(2i64));
        let outer = binary(col("a"), Operator::Plus, inner);
        assert_eq!(spark_expr_name(&outer), "(a + (b / 2))");
    }

    #[test]
    fn renders_binary_literal_full_hex() {
        let v = ScalarValue::Binary(Some(vec![0x45, 0x61, 0x00, 0xFF]));
        assert_eq!(render_literal(&v), "X'456100FF'");
    }
}
