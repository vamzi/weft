//! `weft-plan` (codename **warp**) — the plan intermediate representation.
//!
//! Both entry paths converge here: the Spark Connect `Relation`/`Expression` protobuf
//! tree (`weft-connect`) and parsed Spark SQL (`weft-sql`) both lower into this single
//! *unresolved* IR. `weft-analyzer` then resolves names/types against the catalog to
//! produce a resolved logical plan that `weft-optimizer` and `weft-physical` consume.
//!
//! This mirrors Sail's "Sail spec" stage — a deliberate, known-good decomposition.

use weft_common::Result;

/// A node in the unresolved logical plan. Variants will map 1:1 onto the Spark Connect
/// `Relation.rel_type` oneof (read/project/filter/join/aggregate/sort/limit/sql/…).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum LogicalPlan {
    /// `SELECT 1`-style placeholder so downstream crates have something to match on.
    Empty,
}

/// An unresolved scalar expression. Variants will map onto `Expression.expr_type`
/// (literal/unresolved_attribute/unresolved_function/alias/cast/sort_order/window/…).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Expr {
    /// Placeholder literal.
    Null,
}

/// Lower a Spark Connect relation (or SQL) into the warp IR. Implemented in Phase 0.
pub fn lower_placeholder() -> Result<LogicalPlan> {
    Ok(LogicalPlan::Empty)
}
