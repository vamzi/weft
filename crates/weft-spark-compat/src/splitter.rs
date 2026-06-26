//! `.sql` input-file helpers.
//!
//! The golden `.sql.out` files already enumerate every statement, so the *comparison* path
//! (see [`crate::runner`]) drives off the golden blocks and never needs to re-implement
//! Spark's statement splitter. This module exists for the secondary concerns the golden file
//! can't express on its own: which input files we must **skip** (because they need machinery
//! weft doesn't have yet — registered UDFs, `--IMPORT` chains we don't resolve), and surfacing
//! the directives so skips are explicit and counted, never silent.

/// Why an input file is skipped for now (recorded in the report, not dropped silently).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// Uses `udf(...)` wrappers that require a registered Python/Scala/Java UDF.
    RequiresUdf,
    /// Pulls in another file via `--IMPORT` (shared setup we don't yet inline).
    UsesImport,
}

impl SkipReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            SkipReason::RequiresUdf => "requires-udf-registration",
            SkipReason::UsesImport => "uses-import-directive",
        }
    }
}

/// Decide whether an input file is runnable by the golden-replay path today. Returns the first
/// blocking reason, or `None` when the file can be replayed directly.
///
/// `udf` is checked before `--IMPORT` because UDF files are the harder, more fundamental gap.
pub fn skip_reason(input_sql: &str) -> Option<SkipReason> {
    // `udf(` wrappers (the `inputs/udf/*` and `inputs/udaf/*` ports) need a real UDF registry.
    if input_sql.lines().any(|l| {
        let t = l.trim_start();
        !t.starts_with("--") && l.contains("udf(")
    }) {
        return Some(SkipReason::RequiresUdf);
    }
    if input_sql
        .lines()
        .any(|l| l.trim_start().starts_with("--IMPORT"))
    {
        return Some(SkipReason::UsesImport);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_udf_files() {
        let sql = "-- a comment\nSELECT udf(a) FROM t;";
        assert_eq!(skip_reason(sql), Some(SkipReason::RequiresUdf));
    }

    #[test]
    fn detects_import_files() {
        let sql = "--IMPORT subquery/setup.sql\nSELECT 1;";
        assert_eq!(skip_reason(sql), Some(SkipReason::UsesImport));
    }

    #[test]
    fn plain_files_run() {
        assert_eq!(skip_reason("SELECT 1; SELECT 2;"), None);
    }

    #[test]
    fn udf_in_comment_does_not_trip() {
        assert_eq!(skip_reason("-- mentions udf( in prose\nSELECT 1;"), None);
    }
}
