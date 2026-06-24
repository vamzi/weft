//! Lower a Spark Connect `Expression` to a DataFusion [`Expr`].
//!
//! Covers the surface the DataFrame API leans on: literals, column references, functions
//! (operators dispatched specially, named scalar/aggregate functions resolved through the session
//! registry), aliases, casts, and `expr("…")` strings. Unsupported nodes return `unimplemented`.

use datafusion::common::Column;
use datafusion::logical_expr::{expr::AggregateFunction, lit, BinaryExpr, Cast, Expr, Operator};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use tonic::Status;
use weft_proto::spark::connect as sc;

use super::inval;
use crate::types::spark_to_arrow;

/// Maps a Spark relation `plan_id` to the DataFusion qualifier its output was aliased with —
/// lets `df.col` (carrying `df`'s plan id) bind to the right side of a join.
pub type Ids = std::collections::HashMap<i64, String>;

/// Translate one Spark Connect expression. `ids` resolves `plan_id`-qualified column references
/// (pass `None` outside multi-input contexts like join conditions).
pub fn to_expr(
    ctx: &SessionContext,
    e: &sc::Expression,
    ids: Option<&Ids>,
) -> Result<Expr, Status> {
    use sc::expression::ExprType;
    match e
        .expr_type
        .as_ref()
        .ok_or_else(|| inval("empty expression"))?
    {
        ExprType::Literal(l) => literal(l),
        ExprType::UnresolvedAttribute(a) => Ok(attribute(a, ids)),
        ExprType::UnresolvedFunction(f) => function(ctx, f, ids),
        ExprType::Alias(a) => {
            let inner = to_expr(
                ctx,
                a.expr.as_deref().ok_or_else(|| inval("alias.expr"))?,
                ids,
            )?;
            let name = a.name.first().ok_or_else(|| inval("alias.name"))?;
            Ok(inner.alias(name))
        }
        ExprType::Cast(c) => cast(ctx, c, ids),
        ExprType::UnresolvedStar(_) => Ok(wildcard()),
        ExprType::ExpressionString(s) => ctx
            .parse_sql_expr(&s.expression, &datafusion::common::DFSchema::empty())
            .map_err(|e| inval(format!("parse expr `{}`: {e}", s.expression))),
        other => Err(Status::unimplemented(format!("expression: {other:?}"))),
    }
}

/// Resolve a column reference, honoring `plan_id` when a join (etc.) provided a qualifier map.
fn attribute(a: &sc::expression::UnresolvedAttribute, ids: Option<&Ids>) -> Expr {
    if let (Some(pid), Some(map)) = (a.plan_id, ids) {
        if let Some(q) = map.get(&pid) {
            return Expr::Column(Column::new(Some(q.clone()), &a.unparsed_identifier));
        }
    }
    Expr::Column(Column::from_qualified_name(&a.unparsed_identifier))
}

/// A `*` wildcard projection expression.
pub fn wildcard() -> Expr {
    #[allow(deprecated)]
    Expr::Wildcard {
        qualifier: None,
        options: Box::new(datafusion::logical_expr::expr::WildcardOptions::default()),
    }
}

/// Is this expression a `*` wildcard?
pub fn is_wildcard(e: &sc::Expression) -> bool {
    matches!(
        e.expr_type.as_ref(),
        Some(sc::expression::ExprType::UnresolvedStar(_))
    )
}

/// Does this expression tree reference any column by `plan_id`? (vs. by qualified name). Joins use
/// this to decide whether to auto-alias their inputs for plan-id resolution.
pub fn uses_plan_id(e: &sc::Expression) -> bool {
    use sc::expression::ExprType;
    match e.expr_type.as_ref() {
        Some(ExprType::UnresolvedAttribute(a)) => a.plan_id.is_some(),
        Some(ExprType::UnresolvedFunction(f)) => f.arguments.iter().any(uses_plan_id),
        Some(ExprType::Alias(a)) => a.expr.as_deref().is_some_and(uses_plan_id),
        Some(ExprType::Cast(c)) => c.expr.as_deref().is_some_and(uses_plan_id),
        _ => false,
    }
}

fn literal(l: &sc::expression::Literal) -> Result<Expr, Status> {
    use sc::expression::literal::LiteralType as L;
    let sv = match l
        .literal_type
        .as_ref()
        .ok_or_else(|| inval("empty literal"))?
    {
        L::Null(_) => ScalarValue::Null,
        L::Boolean(b) => ScalarValue::Boolean(Some(*b)),
        L::Byte(v) => ScalarValue::Int8(Some(*v as i8)),
        L::Short(v) => ScalarValue::Int16(Some(*v as i16)),
        L::Integer(v) => ScalarValue::Int32(Some(*v)),
        L::Long(v) => ScalarValue::Int64(Some(*v)),
        L::Float(v) => ScalarValue::Float32(Some(*v)),
        L::Double(v) => ScalarValue::Float64(Some(*v)),
        L::String(s) => ScalarValue::Utf8(Some(s.clone())),
        L::Binary(b) => ScalarValue::Binary(Some(b.clone())),
        L::Date(d) => ScalarValue::Date32(Some(*d)),
        L::TimestampNtz(t) => ScalarValue::TimestampMicrosecond(Some(*t), None),
        L::Timestamp(t) => ScalarValue::TimestampMicrosecond(Some(*t), Some("UTC".into())),
        other => return Err(Status::unimplemented(format!("literal: {other:?}"))),
    };
    Ok(lit(sv))
}

/// Map a Spark operator name to a DataFusion binary [`Operator`].
fn binary_operator(name: &str) -> Option<Operator> {
    Some(match name {
        "+" => Operator::Plus,
        "-" => Operator::Minus,
        "*" => Operator::Multiply,
        "/" => Operator::Divide,
        "%" | "mod" => Operator::Modulo,
        "=" | "==" => Operator::Eq,
        "!=" | "<>" => Operator::NotEq,
        "<" => Operator::Lt,
        "<=" => Operator::LtEq,
        ">" => Operator::Gt,
        ">=" => Operator::GtEq,
        "and" => Operator::And,
        "or" => Operator::Or,
        _ => return None,
    })
}

fn function(
    ctx: &SessionContext,
    f: &sc::expression::UnresolvedFunction,
    ids: Option<&Ids>,
) -> Result<Expr, Status> {
    let name = f.function_name.as_str();
    // A `*` argument only appears in `count(*)`; lower it to the literal `1` so it counts rows.
    let args = f
        .arguments
        .iter()
        .map(|a| {
            if is_wildcard(a) {
                Ok(lit(ScalarValue::Int64(Some(1))))
            } else {
                to_expr(ctx, a, ids)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Binary operators.
    if let Some(op) = binary_operator(name) {
        if args.len() != 2 {
            return Err(inval(format!("operator `{name}` needs 2 args")));
        }
        return Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(args[0].clone()),
            op,
            Box::new(args[1].clone()),
        )));
    }

    // Common unary / special forms.
    let arg0 = || {
        args.first()
            .cloned()
            .ok_or_else(|| inval(format!("`{name}` needs an arg")))
    };
    match name {
        "not" | "!" => return Ok(!arg0()?),
        "isnull" => return Ok(arg0()?.is_null()),
        "isnotnull" => return Ok(arg0()?.is_not_null()),
        "negative" | "negate" => return Ok(Expr::Negative(Box::new(arg0()?))),
        // `F.when(c1,v1).when(c2,v2).otherwise(e)` → CASE WHEN. Args are condition/value pairs,
        // with an optional trailing else value.
        "when" => {
            let else_expr = (args.len() % 2 == 1).then(|| Box::new(args[args.len() - 1].clone()));
            let when_then = args
                .chunks_exact(2)
                .map(|p| (Box::new(p[0].clone()), Box::new(p[1].clone())))
                .collect();
            return Ok(Expr::Case(datafusion::logical_expr::Case::new(
                None, when_then, else_expr,
            )));
        }
        // `col.isin(a, b, …)` → `col IN (a, b, …)`.
        "in" => {
            let (target, list) = args.split_first().ok_or_else(|| inval("`in` needs args"))?;
            return Ok(target.clone().in_list(list.to_vec(), false));
        }
        "like" | "ilike" => {
            if args.len() != 2 {
                return Err(inval("`like` needs 2 args"));
            }
            return Ok(Expr::Like(datafusion::logical_expr::expr::Like::new(
                false,
                Box::new(args[0].clone()),
                Box::new(args[1].clone()),
                None,
                name == "ilike",
            )));
        }
        _ => {}
    }

    // Named functions: aggregate first (sum/avg/count/…), then scalar (upper/abs/…).
    let state = ctx.state();
    let lname = name.to_ascii_lowercase();
    use datafusion::execution::FunctionRegistry;
    if let Ok(udaf) = state.udaf(&lname) {
        return Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
            udaf,
            args,
            f.is_distinct,
            None,
            vec![],
            None,
        )));
    }
    if let Ok(udf) = state.udf(&lname) {
        return Ok(udf.call(args));
    }
    Err(Status::unimplemented(format!("function `{name}`")))
}

fn cast(ctx: &SessionContext, c: &sc::expression::Cast, ids: Option<&Ids>) -> Result<Expr, Status> {
    use sc::expression::cast::CastToType;
    let inner = to_expr(
        ctx,
        c.expr.as_deref().ok_or_else(|| inval("cast.expr"))?,
        ids,
    )?;
    let dt = match c.cast_to_type.as_ref().ok_or_else(|| inval("cast.type"))? {
        CastToType::Type(t) => spark_to_arrow(t)?,
        CastToType::TypeStr(s) => parse_type_str(s)?,
    };
    Ok(Expr::Cast(Cast::new(Box::new(inner), dt)))
}

/// Parse a Spark DDL type string (e.g. `int`, `string`, `double`) to an Arrow type.
fn parse_type_str(s: &str) -> Result<datafusion::arrow::datatypes::DataType, Status> {
    use datafusion::arrow::datatypes::DataType;
    Ok(match s.trim().to_ascii_lowercase().as_str() {
        "boolean" | "bool" => DataType::Boolean,
        "tinyint" | "byte" => DataType::Int8,
        "smallint" | "short" => DataType::Int16,
        "int" | "integer" => DataType::Int32,
        "bigint" | "long" => DataType::Int64,
        "float" | "real" => DataType::Float32,
        "double" => DataType::Float64,
        "string" => DataType::Utf8,
        "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Microsecond,
            Some("UTC".into()),
        ),
        other => return Err(Status::unimplemented(format!("cast type `{other}`"))),
    })
}
