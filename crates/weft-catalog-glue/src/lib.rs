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

use async_trait::async_trait;
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

    /// Build from a flat options map (`region`, optional `aws_bin`) — the shape used by both the
    /// gateway connection request and the `spark.sql.catalog.<name>.*` startup config. `region`
    /// defaults to `us-west-2`.
    pub fn from_config(name: &str, options: &HashMap<String, String>) -> Self {
        let region = options
            .get("region")
            .cloned()
            .unwrap_or_else(|| "us-west-2".to_string());
        let aws_bin = options.get("aws_bin").cloned();
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
            return Err(Error::Io(format!(
                "aws glue {}: {}",
                args.first().copied().unwrap_or(""),
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(String::from_utf8_lossy(&out.stdout).to_string())
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
        Ok(TableMetadata::new(
            format!("{}.{db}.{table}", self.name),
            location.to_string(),
            format,
        ))
    }
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
