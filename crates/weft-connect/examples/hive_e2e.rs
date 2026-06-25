//! End-to-end check against a live Hive Metastore.
//!
//! Two modes:
//!   cargo run -p weft-connect --example hive_e2e -- gen <dir>   # write orders parquet into <dir>
//!   cargo run -p weft-connect --example hive_e2e                # connect, list, load, query
//!
//! The query mode connects to `$WEFT_HMS_URI` (default `thrift://localhost:9083`), lists the
//! `sales` database + its tables, loads `sales.orders`, registers the Hive catalog into a Weft
//! engine, and runs a real SQL query that resolves the table **lazily** through the catalog bridge.

use std::sync::Arc;

use weft_catalog::CatalogProvider;
use weft_catalog_hive::HiveCatalog;
use weft_loom::arrow::array::{Float64Array, Int64Array};
use weft_loom::arrow::datatypes::{DataType, Field, Schema};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("gen") {
        let dir = args.get(2).expect("usage: hive_e2e gen <dir>");
        gen_orders(dir);
        return;
    }
    if let Err(e) = query().await {
        eprintln!("E2E FAILED: {e}");
        std::process::exit(1);
    }
}

/// Write `orders(id BIGINT, amount DOUBLE)` = (1,10),(2,20),(3,30) as a parquet file into `dir`.
fn gen_orders(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
        ],
    )
    .expect("batch");
    let path = format!("{}/part-0.parquet", dir.trim_end_matches('/'));
    let f = std::fs::File::create(&path).expect("create parquet");
    let mut w = datafusion::parquet::arrow::ArrowWriter::try_new(f, schema, None).expect("writer");
    w.write(&batch).expect("write");
    w.close().expect("close");
    println!("wrote {path} (3 rows)");
}

async fn query() -> Result<(), Box<dyn std::error::Error>> {
    let uri = std::env::var("WEFT_HMS_URI").unwrap_or_else(|_| "thrift://localhost:9083".to_string());
    println!("== connecting to Hive Metastore at {uri} ==");
    let catalog = HiveCatalog::from_uri("hive", &uri)?;

    let namespaces = catalog.list_namespaces(&[]).await?;
    println!("namespaces: {namespaces:?}");

    let tables = catalog.list_tables(&["sales".to_string()]).await?;
    println!("tables in `sales`: {tables:?}");

    let md = catalog.load_table(&["sales".to_string()], "orders").await?;
    println!(
        "loaded sales.orders -> location={} format={:?} partitions={:?}",
        md.location, md.format, md.partition_columns
    );

    let exists = catalog.table_exists(&["sales".to_string()], "orders").await?;
    let ghost = catalog.table_exists(&["sales".to_string()], "ghost").await?;
    println!("tableExists orders={exists} ghost={ghost}");

    // Register the catalog and run a query that was NEVER pre-registered — it resolves lazily.
    let engine = Engine::new();
    engine.register_catalog("hive", Arc::new(catalog));
    let batches = engine
        .sql("SELECT COUNT(*) AS c, SUM(amount) AS s FROM hive.sales.orders")
        .await?;
    let c = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let s = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    println!("query result: count={c} sum={s}");

    // A filtered + projected query — exercises predicate/projection through the bridge.
    let filtered = engine
        .sql("SELECT id, amount FROM hive.sales.orders WHERE amount >= 20 ORDER BY id")
        .await?;
    let ids: Vec<i64> = filtered
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    println!("filtered ids (amount>=20): {ids:?}");

    assert_eq!(c, 3, "expected 3 rows");
    assert_eq!(s, 60.0, "expected sum(amount)=60");
    assert_eq!(ids, vec![2, 3], "expected ids [2,3] for amount>=20");
    assert!(exists && !ghost, "tableExists wrong");
    println!("\n✅ E2E PASSED: live HMS → weft-catalog-hive → bridge → lazy query");
    Ok(())
}
