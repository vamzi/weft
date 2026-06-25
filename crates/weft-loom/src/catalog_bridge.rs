//! Bridge a [`weft_catalog::CatalogProvider`] into DataFusion's catalog API.
//!
//! DataFusion already resolves three-part names (`catalog.schema.table`) and loads tables
//! **lazily and asynchronously** through [`SchemaProvider::table`]. This module adapts a weft
//! catalog onto that model so an external metastore plugs straight into query resolution: the
//! catalog is hit only when a query first references one of its tables, and the resolved
//! [`TableMetadata`] is turned into a `TableProvider` via the engine's shared listing-table
//! builder (so Parquet/Delta/Iceberg all read through the same version-safe path).
//!
//! Mapping to DataFusion's fixed three-level model: a weft *namespace* is the middle level
//! (DataFusion's "schema"), so it is single-part here — covering Hive (`database`) and Unity /
//! Iceberg-REST (`schema`). The sync `schema_names`/`table_names`/`table_exist` methods are
//! best-effort (a cached snapshot); authoritative listing for the `spark.catalog.*` RPC goes
//! straight to the weft provider in `weft-connect`, not through these.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::prelude::SessionContext;
use weft_catalog::{CatalogProvider as WeftCatalog, TableFormat, TableMetadata};
use weft_common::Error;

/// DataFusion `CatalogProvider` backed by a weft [`WeftCatalog`].
pub struct WeftCatalogProvider {
    catalog: Arc<dyn WeftCatalog>,
    ctx: Arc<SessionContext>,
    /// Lazily-created per-namespace schema providers (cheap wrappers; cached so repeated
    /// references to the same namespace share a table cache).
    schemas: Mutex<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl WeftCatalogProvider {
    /// Wrap a weft catalog. `ctx` supplies the session state used to infer schemas / read files.
    pub fn new(catalog: Arc<dyn WeftCatalog>, ctx: Arc<SessionContext>) -> Self {
        Self {
            catalog,
            ctx,
            schemas: Mutex::new(HashMap::new()),
        }
    }
}

impl fmt::Debug for WeftCatalogProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeftCatalogProvider")
            .field("catalog", &self.catalog.name())
            .finish()
    }
}

impl CatalogProvider for WeftCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        // Best-effort: the namespaces we've already materialized a provider for. Authoritative
        // listing is the `spark.catalog.listDatabases` RPC, which queries the weft provider.
        self.schemas
            .lock()
            .expect("schemas poisoned")
            .keys()
            .cloned()
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Always hand back a provider (without a sync existence check); a non-existent table
        // surfaces as `Ok(None)` from the async `table()` below — DataFusion's normal "table not
        // found" path.
        let mut schemas = self.schemas.lock().expect("schemas poisoned");
        let provider = schemas.entry(name.to_string()).or_insert_with(|| {
            Arc::new(WeftSchemaProvider::new(
                self.catalog.clone(),
                vec![name.to_string()],
                self.ctx.clone(),
            ))
        });
        Some(provider.clone())
    }
}

/// DataFusion `SchemaProvider` for one namespace of a weft catalog.
struct WeftSchemaProvider {
    catalog: Arc<dyn WeftCatalog>,
    namespace: Vec<String>,
    ctx: Arc<SessionContext>,
    /// Resolved tables, cached so a table referenced repeatedly in a query is loaded once.
    tables: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
}

impl WeftSchemaProvider {
    fn new(
        catalog: Arc<dyn WeftCatalog>,
        namespace: Vec<String>,
        ctx: Arc<SessionContext>,
    ) -> Self {
        Self {
            catalog,
            namespace,
            ctx,
            tables: Mutex::new(HashMap::new()),
        }
    }
}

impl fmt::Debug for WeftSchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeftSchemaProvider")
            .field("catalog", &self.catalog.name())
            .field("namespace", &self.namespace)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for WeftSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        // Best-effort: already-resolved tables. `spark.catalog.listTables` uses the weft provider.
        self.tables
            .lock()
            .expect("tables poisoned")
            .keys()
            .cloned()
            .collect()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        if let Some(t) = self.tables.lock().expect("tables poisoned").get(name) {
            return Ok(Some(t.clone()));
        }
        let metadata = match self.catalog.load_table(&self.namespace, name).await {
            Ok(md) => md,
            // A "no such table" (analysis) error → DataFusion's standard not-found path.
            Err(Error::Plan(_)) => return Ok(None),
            // A storage / connection / unsupported failure is a real error — surface it.
            Err(e) => return Err(weft_to_df(e)),
        };
        let provider = metadata_to_provider(&self.ctx.state(), &metadata).await?;
        self.tables
            .lock()
            .expect("tables poisoned")
            .insert(name.to_string(), provider.clone());
        Ok(Some(provider))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables
            .lock()
            .expect("tables poisoned")
            .contains_key(name)
    }
}

/// Turn resolved table metadata into a readable DataFusion `TableProvider`.
async fn metadata_to_provider(
    state: &SessionState,
    md: &TableMetadata,
) -> DfResult<Arc<dyn TableProvider>> {
    use datafusion::datasource::file_format::csv::CsvFormat;
    use datafusion::datasource::file_format::json::JsonFormat;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};

    match md.format {
        TableFormat::Parquet => {
            let url = ListingTableUrl::parse(&md.location).map_err(loc_err(md))?;
            ensure_remote_store(state, &url);
            let opts = ListingOptions::new(Arc::new(ParquetFormat::default()))
                .with_file_extension(".parquet");
            crate::build_listing_table(state, vec![url], opts, md.schema.clone())
                .await
                .map_err(weft_to_df)
        }
        TableFormat::Csv => {
            let url = ListingTableUrl::parse(&md.location).map_err(loc_err(md))?;
            ensure_remote_store(state, &url);
            let opts =
                ListingOptions::new(Arc::new(CsvFormat::default())).with_file_extension(".csv");
            crate::build_listing_table(state, vec![url], opts, md.schema.clone())
                .await
                .map_err(weft_to_df)
        }
        TableFormat::Json => {
            let url = ListingTableUrl::parse(&md.location).map_err(loc_err(md))?;
            ensure_remote_store(state, &url);
            let opts =
                ListingOptions::new(Arc::new(JsonFormat::default())).with_file_extension(".json");
            crate::build_listing_table(state, vec![url], opts, md.schema.clone())
                .await
                .map_err(weft_to_df)
        }
        // Lakehouse formats resolve to their active Parquet files (version-safe), then the
        // Parquet reader. v1 reads from the local filesystem — remote object stores for Delta /
        // Iceberg follow once the resolver registers an `object_store` for the location's scheme.
        TableFormat::Delta => {
            let path = local_path(&md.location)?;
            let files = weft_datasource::delta_active_files(&path).map_err(weft_to_df)?;
            parquet_files_provider(state, &md.location, files, md.schema.clone()).await
        }
        TableFormat::Iceberg => {
            let path = local_path(&md.location)?;
            let files = weft_datasource::iceberg_active_files(&path).map_err(weft_to_df)?;
            parquet_files_provider(state, &md.location, files, md.schema.clone()).await
        }
    }
}

/// Ensure an object store is registered on the session's runtime for a remote table location so
/// DataFusion can read it. Currently handles `s3://` — credentials come from the environment or the
/// EC2 instance role (IMDS) via object_store's default provider; no static keys. Registering on the
/// shared runtime is idempotent and persists for the session, so query-time resolution finds it.
/// `file://` and bare paths need nothing and are skipped.
fn ensure_remote_store(state: &SessionState, url: &datafusion::datasource::listing::ListingTableUrl) {
    if url.scheme() != "s3" {
        return;
    }
    let os_url = url.object_store(); // canonical `s3://bucket` key
    if state.runtime_env().object_store(&os_url).is_ok() {
        return; // already registered for this bucket
    }
    // `os_url` is the canonical `s3://bucket/` — pull the bucket from the authority.
    let bucket = os_url
        .as_str()
        .strip_prefix("s3://")
        .and_then(|r| r.split('/').next())
        .unwrap_or("")
        .to_string();
    if bucket.is_empty() {
        return;
    }
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
    match object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .with_region(region)
        .build()
    {
        Ok(store) => {
            state
                .runtime_env()
                .register_object_store(os_url.as_ref(), Arc::new(store));
        }
        Err(e) => eprintln!("warn: could not register S3 object store for `{bucket}`: {e}"),
    }
}

/// Build a Parquet listing table over an explicit set of files (the Delta/Iceberg seam).
async fn parquet_files_provider(
    state: &SessionState,
    location: &str,
    files: Vec<std::path::PathBuf>,
    schema: Option<datafusion::arrow::datatypes::SchemaRef>,
) -> DfResult<Arc<dyn TableProvider>> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};

    if files.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "table `{location}` has no active data files"
        )));
    }
    let urls = files
        .iter()
        .map(|p| {
            ListingTableUrl::parse(p.to_string_lossy())
                .map_err(|e| DataFusionError::Plan(format!("bad file path {}: {e}", p.display())))
        })
        .collect::<DfResult<Vec<_>>>()?;
    let opts = ListingOptions::new(Arc::new(ParquetFormat::default()));
    crate::build_listing_table(state, urls, opts, schema)
        .await
        .map_err(weft_to_df)
}

/// Convert a storage URI to a local filesystem path, or error for a scheme v1 can't read locally.
///
/// Handles both `file:///abs` (RFC form) and Hive's `file:/abs` (single-slash, as the Metastore
/// returns it), as well as bare paths. Non-`file` schemes (`s3://`, `hdfs://`, …) are not local.
fn local_path(location: &str) -> DfResult<String> {
    if let Some(rest) = location.strip_prefix("file://") {
        return Ok(rest.to_string());
    }
    if let Some(rest) = location.strip_prefix("file:") {
        return Ok(rest.to_string());
    }
    if location.contains("://") {
        let scheme = location.split("://").next().unwrap_or("");
        return Err(DataFusionError::NotImplemented(format!(
            "reading Delta/Iceberg from `{scheme}://` is not supported yet (local filesystem only)"
        )));
    }
    Ok(location.to_string())
}

fn loc_err(md: &TableMetadata) -> impl Fn(DataFusionError) -> DataFusionError + '_ {
    move |e| DataFusionError::Plan(format!("bad table location `{}`: {e}", md.location))
}

/// Map a weft error onto DataFusion's error type, preserving the failure class.
fn weft_to_df(e: Error) -> DataFusionError {
    match e {
        Error::Plan(m) => DataFusionError::Plan(m),
        Error::Unsupported(m) => DataFusionError::NotImplemented(m),
        Error::Execution(m) | Error::Io(m) => DataFusionError::Execution(m),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use weft_catalog::{Result as CatResult, TableMetadata};

    /// A fake catalog whose single namespace `ns` has one table `orders` at a fixed location.
    struct FakeCatalog {
        location: String,
    }

    #[async_trait]
    impl WeftCatalog for FakeCatalog {
        fn name(&self) -> &str {
            "fake"
        }
        async fn list_namespaces(&self, _parent: &[String]) -> CatResult<Vec<Vec<String>>> {
            Ok(vec![vec!["ns".to_string()]])
        }
        async fn list_tables(&self, _ns: &[String]) -> CatResult<Vec<String>> {
            Ok(vec!["orders".to_string()])
        }
        async fn load_table(&self, ns: &[String], table: &str) -> CatResult<TableMetadata> {
            if ns == ["ns"] && table == "orders" {
                Ok(TableMetadata::new(
                    "fake.ns.orders",
                    self.location.clone(),
                    TableFormat::Parquet,
                ))
            } else {
                Err(Error::Plan(format!(
                    "no such table: {}.{table}",
                    ns.join(".")
                )))
            }
        }
    }

    fn write_parquet_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("weft-cat-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        dir
    }

    #[tokio::test]
    async fn lazy_resolution_through_registered_catalog() {
        let dir = write_parquet_dir();
        let location = format!("file://{}", dir.to_string_lossy());

        let engine = crate::Engine::new();
        engine.register_catalog("fake", Arc::new(FakeCatalog { location }));

        // Never pre-registered the table — it resolves lazily via the bridge's async `table()`.
        let batches = engine
            .sql("SELECT COUNT(*) AS c, SUM(x) AS s FROM fake.ns.orders")
            .await
            .unwrap();
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let s = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!((c, s), (4, 10));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn missing_table_is_a_clean_not_found() {
        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(FakeCatalog {
                location: "file:///nonexistent".to_string(),
            }),
        );
        let err = engine
            .sql("SELECT * FROM fake.ns.missing")
            .await
            .unwrap_err();
        // DataFusion's table-not-found analysis error, not a panic / internal error.
        assert!(format!("{err}").to_lowercase().contains("not"));
    }
}
