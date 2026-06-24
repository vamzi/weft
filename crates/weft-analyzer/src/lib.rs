//! `weft-analyzer` — name/type/function resolution.
//!
//! Turns the *unresolved* warp IR into a *resolved* logical plan: binds
//! `UnresolvedAttribute`/`UnresolvedFunction`/`UnresolvedStar` against the catalog and a
//! function registry, computes output schemas, and inserts implicit casts following
//! Spark type-coercion rules. This (plus behavioral parity) is the bulk of the real
//! engineering effort — not the gRPC transport.

use weft_catalog::Catalog;
use weft_common::Result;
use weft_plan::LogicalPlan;

/// Resolve a plan against a catalog. Implemented in Phase 0.
pub fn resolve(plan: LogicalPlan, _catalog: &dyn Catalog) -> Result<LogicalPlan> {
    Ok(plan)
}
