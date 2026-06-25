//! `weft-catalog-hive` — a [`CatalogProvider`] backed by a Hive Metastore.
//!
//! Resolves Spark three-part names `<catalog>.<database>.<table>` against a Hive Metastore over
//! Thrift: the catalog is the registered name, the database is the (single-level) namespace, and
//! `load_table` returns the table's storage location + inferred format. Read-only in v1 (no DDL).
//!
//! Configure it the Spark way — `spark.sql.catalog.<name>.type=hive` plus
//! `spark.sql.catalog.<name>.uri=thrift://host:9083` — which `weft-connect`'s catalog factory turns
//! into [`HiveCatalog::from_config`].

mod thrift;

use std::collections::HashMap;

use async_trait::async_trait;
use weft_catalog::{CatalogProvider, Error, Result, TableFormat, TableMetadata};

use thrift::{HiveTable, MetastoreClient};

/// The default Hive Metastore Thrift port.
const DEFAULT_PORT: u16 = 9083;

/// A Hive Metastore catalog.
pub struct HiveCatalog {
    name: String,
    host: String,
    port: u16,
}

impl HiveCatalog {
    /// Build from a `thrift://host:port` URI (port defaults to 9083).
    pub fn from_uri(name: &str, uri: &str) -> Result<Self> {
        let (host, port) = parse_thrift_uri(uri)?;
        Ok(Self {
            name: name.to_string(),
            host,
            port,
        })
    }

    /// Build from `spark.sql.catalog.<name>.*` options (the `spark.sql.catalog.<name>.` prefix
    /// already stripped, so keys are `type`, `uri`, …). Requires a `uri` (or `host`[+`port`]).
    pub fn from_config(name: &str, options: &HashMap<String, String>) -> Result<Self> {
        if let Some(uri) = options.get("uri").or_else(|| options.get("thrift.uri")) {
            return Self::from_uri(name, uri);
        }
        if let Some(host) = options.get("host") {
            let port = options
                .get("port")
                .and_then(|p| p.parse().ok())
                .unwrap_or(DEFAULT_PORT);
            return Ok(Self {
                name: name.to_string(),
                host: host.clone(),
                port,
            });
        }
        Err(Error::Plan(format!(
            "hive catalog `{name}` requires `spark.sql.catalog.{name}.uri` (e.g. thrift://host:9083)"
        )))
    }

    async fn connect(&self) -> Result<MetastoreClient> {
        MetastoreClient::connect(&self.host, self.port).await
    }
}

#[async_trait]
impl CatalogProvider for HiveCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    async fn list_namespaces(&self, parent: &[String]) -> Result<Vec<Vec<String>>> {
        // Hive databases are flat: there are no nested namespaces under a database.
        if !parent.is_empty() {
            return Ok(vec![]);
        }
        let dbs = self.connect().await?.get_all_databases().await?;
        Ok(dbs.into_iter().map(|d| vec![d]).collect())
    }

    async fn list_tables(&self, namespace: &[String]) -> Result<Vec<String>> {
        let db = single_database(namespace)?;
        self.connect().await?.get_all_tables(db).await
    }

    async fn load_table(&self, namespace: &[String], table: &str) -> Result<TableMetadata> {
        let db = single_database(namespace)?;
        let t = self.connect().await?.get_table(db, table).await?;
        let location = t.location.clone().ok_or_else(|| {
            Error::Plan(format!("hive table `{db}.{table}` has no storage location"))
        })?;
        let format = detect_format(&t)?;
        Ok(
            TableMetadata::new(format!("{}.{db}.{table}", self.name), location, format)
                .with_partition_columns(t.partition_columns.clone()),
        )
    }
}

/// Hive namespaces are a single database name. Reject empty / nested namespaces with a clear error.
fn single_database(namespace: &[String]) -> Result<&str> {
    match namespace {
        [db] => Ok(db.as_str()),
        [] => Err(Error::Plan(
            "a Hive table reference needs a database, e.g. `catalog.database.table`".to_string(),
        )),
        _ => Err(Error::Plan(format!(
            "Hive namespaces are a single database; got `{}`",
            namespace.join(".")
        ))),
    }
}

/// Infer the readable file format from a Hive table's properties and storage descriptor.
///
/// Order of signals (most authoritative first): Iceberg `table_type` property, an explicit Spark
/// `provider` property, then the input format / serde class name. Falls back to Parquet — the
/// common lakehouse default — when no signal is conclusive.
fn detect_format(t: &HiveTable) -> Result<TableFormat> {
    if t.param("table_type")
        .is_some_and(|v| v.eq_ignore_ascii_case("iceberg"))
    {
        return Ok(TableFormat::Iceberg);
    }
    for key in ["spark.sql.sources.provider", "provider"] {
        if let Some(f) = t.param(key).and_then(TableFormat::from_provider) {
            return Ok(f);
        }
    }
    let probe = format!(
        "{} {}",
        t.input_format.as_deref().unwrap_or(""),
        t.serde_lib.as_deref().unwrap_or("")
    )
    .to_lowercase();
    if probe.contains("parquet") {
        Ok(TableFormat::Parquet)
    } else if probe.contains("orc") {
        Err(Error::Unsupported(
            "ORC tables are not supported yet".to_string(),
        ))
    } else if probe.contains("avro") {
        Err(Error::Unsupported(
            "Avro tables are not supported yet".to_string(),
        ))
    } else if probe.contains("json") {
        Ok(TableFormat::Json)
    } else if probe.contains("csv") || probe.contains("opencsv") || probe.contains("lazysimple") {
        Ok(TableFormat::Csv)
    } else {
        // No conclusive signal — assume Parquet (the dominant lakehouse format).
        Ok(TableFormat::Parquet)
    }
}

/// Parse `thrift://host:port` (scheme optional, port defaults to 9083).
fn parse_thrift_uri(uri: &str) -> Result<(String, u16)> {
    let rest = uri
        .strip_prefix("thrift://")
        .unwrap_or_else(|| uri.strip_prefix("//").unwrap_or(uri));
    let rest = rest.trim_end_matches('/');
    if rest.is_empty() {
        return Err(Error::Plan(format!("empty Hive Metastore URI: `{uri}`")));
    }
    match rest.rsplit_once(':') {
        Some((host, port)) => {
            let port = port
                .parse()
                .map_err(|_| Error::Plan(format!("bad port in Hive URI `{uri}`")))?;
            Ok((host.to_string(), port))
        }
        None => Ok((rest.to_string(), DEFAULT_PORT)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_parsing() {
        assert_eq!(
            parse_thrift_uri("thrift://hms.internal:9083").unwrap(),
            ("hms.internal".to_string(), 9083)
        );
        assert_eq!(
            parse_thrift_uri("localhost").unwrap(),
            ("localhost".to_string(), 9083)
        );
        assert_eq!(
            parse_thrift_uri("thrift://10.0.0.5:9999").unwrap(),
            ("10.0.0.5".to_string(), 9999)
        );
        assert!(parse_thrift_uri("thrift://").is_err());
    }

    #[test]
    fn format_detection() {
        let mut t = HiveTable {
            input_format: Some(
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat".to_string(),
            ),
            ..Default::default()
        };
        assert_eq!(detect_format(&t).unwrap(), TableFormat::Parquet);

        t.parameters = vec![("table_type".to_string(), "ICEBERG".to_string())];
        assert_eq!(detect_format(&t).unwrap(), TableFormat::Iceberg);

        let delta = HiveTable {
            parameters: vec![(
                "spark.sql.sources.provider".to_string(),
                "delta".to_string(),
            )],
            ..Default::default()
        };
        assert_eq!(detect_format(&delta).unwrap(), TableFormat::Delta);

        let orc = HiveTable {
            input_format: Some("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat".to_string()),
            ..Default::default()
        };
        assert!(matches!(detect_format(&orc), Err(Error::Unsupported(_))));
    }

    #[test]
    fn config_requires_uri() {
        let opts = HashMap::new();
        assert!(HiveCatalog::from_config("prod", &opts).is_err());
        let mut opts = HashMap::new();
        opts.insert("uri".to_string(), "thrift://h:9083".to_string());
        let c = HiveCatalog::from_config("prod", &opts).unwrap();
        assert_eq!(c.name(), "prod");
    }
}
