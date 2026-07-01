//! An AWS Glue Data Catalog [`CatalogProvider`].
//!
//! Implements the catalog SPI by shelling out to the `aws glue` CLI (so we avoid pulling the large
//! AWS Rust SDK; an EC2 instance role provides credentials via IMDS). `list_namespaces` →
//! `get-databases`, `list_tables` → `get-tables`, `load_table` → `get-table` resolved to the
//! table's storage location + format. Once registered via `Engine::register_catalog`, Glue tables
//! resolve and query lazily through the DataFusion bridge — a genuine external catalog.
//!
//! Shared by the control-plane gateway (`POST /api/connections` with `kind=glue`) and the
//! cluster-side Spark Connect server (`spark.sql.catalog.<name>.type=glue`), so an attached Glue
//! catalog resolves identically whether a query runs on the gateway engine or on a cluster.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use weft_catalog::hive_types::columns_to_schema;
use weft_catalog::{CatalogProvider, Error, Result, TableFormat, TableMetadata};

/// A Glue catalog connection, addressed by its registered `name` and AWS `region`.
pub struct GlueCatalog {
    name: String,
    region: String,
    aws_bin: String,
}

impl GlueCatalog {
    /// Build a Glue catalog provider. `aws_bin` is the path to the AWS CLI (default `aws`).
    pub fn new(
        name: impl Into<String>,
        region: impl Into<String>,
        aws_bin: Option<String>,
    ) -> Self {
        Self {
            name: name.into(),
            region: region.into(),
            aws_bin: aws_bin.unwrap_or_else(|| "aws".to_string()),
        }
    }

    /// Build from a flat options map (`region` only) — the shape used by both the gateway connection
    /// request and the `spark.sql.catalog.<name>.*` startup config. `region` defaults to `us-west-2`.
    ///
    /// SECURITY: the AWS CLI path is **never** taken from `options` (which can be attacker-supplied
    /// via `POST /api/connections`). It is sourced only from the operator-controlled `WEFT_AWS_BIN`
    /// env var, defaulting to `aws` on `$PATH`. Honoring a request-supplied `aws_bin` here was an
    /// arbitrary-executable RCE (`Command::new(options["aws_bin"])`) on the gateway host.
    pub fn from_config(name: &str, options: &HashMap<String, String>) -> Self {
        let region = options
            .get("region")
            .cloned()
            .unwrap_or_else(|| "us-west-2".to_string());
        let aws_bin = std::env::var("WEFT_AWS_BIN").ok();
        Self::new(name, region, aws_bin)
    }

    /// Run `aws glue <args> --region <region> --output json` and return stdout.
    async fn glue(&self, args: &[&str]) -> Result<String> {
        let out = tokio::process::Command::new(&self.aws_bin)
            .arg("glue")
            .args(args)
            .args(["--region", &self.region, "--output", "json"])
            .output()
            .await
            .map_err(|e| Error::Io(format!("exec aws glue: {e}")))?;
        if !out.status.success() {
            let action = args.first().copied().unwrap_or("");
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(classify_glue_failure(action, stderr.trim()));
        }
        Ok(String::from_utf8_lossy(&out.stdout).to_string())
    }
}

/// Classify a failed `aws glue <action>` invocation's stderr.
///
/// The AWS CLI reports a missing database/table as `EntityNotFoundException` — an expected
/// "doesn't exist" signal (e.g. probed by CTAS to decide whether to create vs. fail), not a
/// genuine failure. That case maps to [`Error::Plan`], which `weft-loom`'s catalog bridge (and
/// `CatalogProvider::table_exists`'s default impl) already treats as "not found" rather than a
/// hard error. Every other failure (auth, network, throttling, missing binary output, ...) keeps
/// mapping to [`Error::Io`] so it still surfaces as a real error instead of being silently
/// swallowed as "table missing".
fn classify_glue_failure(action: &str, stderr: &str) -> Error {
    if stderr.contains("EntityNotFoundException") {
        Error::Plan(format!("aws glue {action}: {stderr}"))
    } else {
        Error::Io(format!("aws glue {action}: {stderr}"))
    }
}

#[async_trait]
impl CatalogProvider for GlueCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    async fn list_namespaces(&self, parent: &[String]) -> Result<Vec<Vec<String>>> {
        // Glue databases are flat — no nesting below a database.
        if !parent.is_empty() {
            return Ok(vec![]);
        }
        let out = self
            .glue(&["get-databases", "--query", "DatabaseList[].Name"])
            .await?;
        let names: Vec<String> = serde_json::from_str(&out)
            .map_err(|e| Error::Io(format!("parse get-databases: {e}")))?;
        Ok(names.into_iter().map(|d| vec![d]).collect())
    }

    async fn list_tables(&self, namespace: &[String]) -> Result<Vec<String>> {
        let db = single_db(namespace)?;
        let out = self
            .glue(&[
                "get-tables",
                "--database-name",
                db,
                "--query",
                "TableList[].Name",
            ])
            .await?;
        serde_json::from_str(&out).map_err(|e| Error::Io(format!("parse get-tables: {e}")))
    }

    async fn load_table(&self, namespace: &[String], table: &str) -> Result<TableMetadata> {
        let db = single_db(namespace)?;
        let out = self
            .glue(&["get-table", "--database-name", db, "--name", table])
            .await?;
        let v: serde_json::Value =
            serde_json::from_str(&out).map_err(|e| Error::Io(format!("parse get-table: {e}")))?;
        let t = &v["Table"];
        let location = t["StorageDescriptor"]["Location"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::Plan(format!("glue table `{db}.{table}` has no location")))?;
        // Format from the `classification` table parameter (Glue/Athena convention), default parquet.
        let classification = t["Parameters"]["classification"]
            .as_str()
            .unwrap_or("parquet");
        let format = TableFormat::from_provider(classification).unwrap_or(TableFormat::Parquet);

        // The Glue-declared schema is the *authoritative* table schema: data columns
        // (`StorageDescriptor.Columns`) followed by partition columns (`PartitionKeys`). When it is
        // present and fully mappable we attach it so the engine reads files *against* it — files
        // whose physical types differ (a common case across monthly Parquet dumps) are cast to the
        // declared types by DataFusion's scan-time expression adapter, rather than failing schema
        // inference's strict "merge" check. If the columns are absent/empty, or *any* column has a
        // type we can't faithfully map, we leave `schema = None` and fall back to Parquet inference
        // (preserving today's behavior — never guessing a type that could silently corrupt a read).
        let data_cols = t["StorageDescriptor"]["Columns"].as_array();
        let part_cols = t["PartitionKeys"].as_array();
        let schema = columns_to_schema(glue_column_pairs(data_cols, part_cols));

        let md = TableMetadata::new(
            format!("{}.{db}.{table}", self.name),
            location.to_string(),
            format,
        );
        Ok(match schema {
            Some(s) => md.with_schema(Arc::new(s)),
            None => md,
        })
    }
}

/// Flatten a Glue table's `StorageDescriptor.Columns` (data columns) and `PartitionKeys` (partition
/// columns) — each a JSON array of `{"Name": .., "Type": ..}` — into ordered `(name, type)` pairs,
/// data columns first. Feeds [`columns_to_schema`], which decides schema-vs-inference.
///
/// A column missing a string `Name`/`Type` yields an empty type string, which is unmappable — so
/// `columns_to_schema` returns `None` (whole-table inference). This is the conservative,
/// all-or-nothing behavior: never build a partial schema that could shift column positions.
fn glue_column_pairs(
    data_cols: Option<&Vec<serde_json::Value>>,
    part_cols: Option<&Vec<serde_json::Value>>,
) -> Vec<(String, String)> {
    data_cols
        .into_iter()
        .flatten()
        .chain(part_cols.into_iter().flatten())
        .map(|col| {
            let name = col["Name"].as_str().unwrap_or("").to_string();
            let ty = col["Type"].as_str().unwrap_or("").to_string();
            (name, ty)
        })
        .collect()
}

fn single_db(namespace: &[String]) -> Result<&str> {
    match namespace {
        [db] => Ok(db.as_str()),
        [] => Err(Error::Plan(
            "a Glue table reference needs a database, e.g. `catalog.database.table`".into(),
        )),
        _ => Err(Error::Plan(format!(
            "Glue namespaces are a single database; got `{}`",
            namespace.join(".")
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use weft_catalog::arrow::datatypes::DataType;

    // The pure Hive-type→Arrow mapping is unit-tested in `weft_catalog::hive_types`; these tests
    // cover Glue's JSON `{Name,Type}` → `(name, type)` flattening and its integration with
    // `columns_to_schema` (data columns then partition keys, with the all-or-nothing fallback).

    #[test]
    fn schema_from_columns_includes_partition_keys() {
        let data = json!([
            {"Name": "vendor_id", "Type": "bigint"},
            {"Name": "fare", "Type": "decimal(10,2)"},
        ]);
        let parts = json!([{"Name": "month", "Type": "string"}]);
        let schema = columns_to_schema(glue_column_pairs(data.as_array(), parts.as_array()))
            .expect("schema");
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "vendor_id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Decimal128(10, 2));
        // Partition column appended after data columns.
        assert_eq!(schema.field(2).name(), "month");
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert!(schema.field(0).is_nullable());
    }

    #[test]
    fn empty_or_absent_columns_fall_back_to_inference() {
        // Empty Columns (the existing-table case) → None, preserving today's inference behavior.
        let empty = json!([]);
        assert_eq!(
            columns_to_schema(glue_column_pairs(empty.as_array(), None)),
            None
        );
        // Absent Columns → None.
        assert_eq!(columns_to_schema(glue_column_pairs(None, None)), None);
    }

    #[test]
    fn any_unmappable_column_falls_back_to_inference() {
        // One complex column poisons the whole schema → infer rather than shift positions.
        let data = json!([
            {"Name": "id", "Type": "bigint"},
            {"Name": "tags", "Type": "array<string>"},
        ]);
        assert_eq!(
            columns_to_schema(glue_column_pairs(data.as_array(), None)),
            None
        );
    }

    #[test]
    fn column_missing_name_or_type_falls_back() {
        // A malformed Glue column (no `Type`) yields an empty type string → unmappable → None.
        let data = json!([{"Name": "id"}]);
        assert_eq!(
            columns_to_schema(glue_column_pairs(data.as_array(), None)),
            None
        );
    }

    // `classify_glue_failure` is what lets a CTAS's "does the target table already exist?" probe
    // (`get-table`) tell "doesn't exist yet, go ahead and create it" (EntityNotFoundException)
    // apart from a genuine failure that must still surface as an error.

    #[test]
    fn entity_not_found_classifies_as_not_found() {
        let stderr = "An error occurred (EntityNotFoundException) when calling the GetTable \
                       operation: Entity Not Found";
        match classify_glue_failure("get-table", stderr) {
            Error::Plan(msg) => assert!(msg.contains("EntityNotFoundException")),
            other => panic!("expected Error::Plan, got {other:?}"),
        }
    }

    #[test]
    fn access_denied_classifies_as_io_error() {
        let stderr = "An error occurred (AccessDeniedException) when calling the GetTable \
                       operation: User is not authorized";
        match classify_glue_failure("get-table", stderr) {
            Error::Io(msg) => assert!(msg.contains("AccessDeniedException")),
            other => panic!("expected Error::Io, got {other:?}"),
        }
    }

    #[test]
    fn generic_failure_classifies_as_io_error() {
        let stderr = "Could not connect to the endpoint URL";
        match classify_glue_failure("get-table", stderr) {
            Error::Io(msg) => assert!(msg.contains("Could not connect")),
            other => panic!("expected Error::Io, got {other:?}"),
        }
    }
}
