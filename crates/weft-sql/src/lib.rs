//! `weft-sql` — the Spark SQL dialect front-end.
//!
//! Parses SQL text (from the Spark Connect `Sql` relation / `SqlCommand`, and raw
//! `ExpressionString` fragments) into [`weft_plan`] IR. Spark dialect quirks live here:
//! backtick identifiers, `LIKE`/`RLIKE`, `DATE_TRUNC`, lateral views, etc.

use weft_common::Result;
use weft_plan::LogicalPlan;

/// Parse a Spark SQL statement into the warp IR. Implemented in Phase 0.
pub fn parse(_sql: &str) -> Result<LogicalPlan> {
    weft_plan::lower_placeholder()
}
