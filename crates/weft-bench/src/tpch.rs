//! TPC-H Phase-0 subset (Q1/Q3/Q5/Q6/Q10) on small synthetic tables.
//!
//! Proves the engine runs the canonical aggregation (Q1/Q6) and multi-table join
//! (Q3/Q5/Q10) shapes end to end. Synthetic data with consistent foreign keys and date
//! ranges so the queries match rows; this is coverage, not TPC-H-spec validation.

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{
    ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use weft_loom::Engine;

/// Day numbers (since the Unix epoch) for the date windows the queries filter on.
const D_1993_10_01: i32 = 8674;

fn f(name: &str, t: DataType) -> Field {
    Field::new(name, t, false)
}

fn register(engine: &Engine, name: &str, fields: Vec<Field>, arrays: Vec<ArrayRef>) {
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).expect("tpch batch");
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("tpch memtable");
    engine
        .ctx()
        .register_table(name, Arc::new(table))
        .expect("register tpch table");
}

fn i32a(it: impl Iterator<Item = i32>) -> ArrayRef {
    Arc::new(Int32Array::from_iter_values(it))
}
fn i64a(it: impl Iterator<Item = i64>) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(it))
}
fn f64a(it: impl Iterator<Item = f64>) -> ArrayRef {
    Arc::new(Float64Array::from_iter_values(it))
}
fn da(it: impl Iterator<Item = i32>) -> ArrayRef {
    Arc::new(Date32Array::from_iter_values(it))
}
fn sa(it: impl Iterator<Item = String>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(it))
}

fn build_tables(engine: &Engine) {
    let regions = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];
    let segments = [
        "BUILDING",
        "AUTOMOBILE",
        "MACHINERY",
        "HOUSEHOLD",
        "FURNITURE",
    ];

    // region (5)
    register(
        engine,
        "region",
        vec![
            f("r_regionkey", DataType::Int32),
            f("r_name", DataType::Utf8),
        ],
        vec![i32a(0..5), sa(regions.iter().map(|s| s.to_string()))],
    );

    // nation (25): n_regionkey = key % 5
    let n = 25usize;
    register(
        engine,
        "nation",
        vec![
            f("n_nationkey", DataType::Int32),
            f("n_name", DataType::Utf8),
            f("n_regionkey", DataType::Int32),
        ],
        vec![
            i32a(0..n as i32),
            sa((0..n).map(|k| format!("NATION{k}"))),
            i32a((0..n).map(|k| (k % 5) as i32)),
        ],
    );

    // supplier (100): s_nationkey = key % 25
    let s = 100usize;
    register(
        engine,
        "supplier",
        vec![
            f("s_suppkey", DataType::Int32),
            f("s_nationkey", DataType::Int32),
        ],
        vec![i32a(0..s as i32), i32a((0..s).map(|k| (k % 25) as i32))],
    );

    // customer (1000)
    let c = 1000usize;
    register(
        engine,
        "customer",
        vec![
            f("c_custkey", DataType::Int32),
            f("c_name", DataType::Utf8),
            f("c_acctbal", DataType::Float64),
            f("c_phone", DataType::Utf8),
            f("c_nationkey", DataType::Int32),
            f("c_address", DataType::Utf8),
            f("c_comment", DataType::Utf8),
            f("c_mktsegment", DataType::Utf8),
        ],
        vec![
            i32a(0..c as i32),
            sa((0..c).map(|k| format!("Customer{k}"))),
            f64a((0..c).map(|k| (k % 9000) as f64 + 100.0)),
            sa((0..c).map(|k| format!("phone{k}"))),
            i32a((0..c).map(|k| (k % 25) as i32)),
            sa((0..c).map(|k| format!("addr{k}"))),
            sa((0..c).map(|k| format!("comment{k}"))),
            sa((0..c).map(|k| segments[k % 5].to_string())),
        ],
    );

    // orders (4000): o_custkey = key % customers; dates 1993-10-01 .. ~1995-03
    let o = 4000usize;
    register(
        engine,
        "orders",
        vec![
            f("o_orderkey", DataType::Int64),
            f("o_custkey", DataType::Int32),
            f("o_orderdate", DataType::Date32),
            f("o_shippriority", DataType::Int32),
        ],
        vec![
            i64a((0..o).map(|k| k as i64)),
            i32a((0..o).map(|k| (k % c) as i32)),
            da((0..o).map(|k| D_1993_10_01 + (k % 530) as i32)),
            i32a((0..o).map(|_| 0)),
        ],
    );

    // lineitem (20000): l_orderkey FK to orders; shipdate 1993-10 .. ~1995-05
    let l = 20_000usize;
    let flags = ["A", "N", "R"];
    let status = ["O", "F"];
    register(
        engine,
        "lineitem",
        vec![
            f("l_orderkey", DataType::Int64),
            f("l_suppkey", DataType::Int32),
            f("l_quantity", DataType::Float64),
            f("l_extendedprice", DataType::Float64),
            f("l_discount", DataType::Float64),
            f("l_tax", DataType::Float64),
            f("l_returnflag", DataType::Utf8),
            f("l_linestatus", DataType::Utf8),
            f("l_shipdate", DataType::Date32),
        ],
        vec![
            i64a((0..l).map(|k| (k % o) as i64)),
            i32a((0..l).map(|k| (k % s) as i32)),
            f64a((0..l).map(|k| (k % 50) as f64)),
            f64a((0..l).map(|k| 1000.0 + (k % 1000) as f64)),
            f64a((0..l).map(|k| (k % 11) as f64 / 100.0)),
            f64a((0..l).map(|k| (k % 8) as f64 / 100.0)),
            sa((0..l).map(|k| flags[k % 3].to_string())),
            sa((0..l).map(|k| status[k % 2].to_string())),
            da((0..l).map(|k| D_1993_10_01 + (k % 600) as i32)),
        ],
    );
}

fn queries() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "Q1",
            "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, \
            sum(l_extendedprice) AS sum_base_price, \
            sum(l_extendedprice*(1-l_discount)) AS sum_disc_price, \
            sum(l_extendedprice*(1-l_discount)*(1+l_tax)) AS sum_charge, \
            avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, \
            avg(l_discount) AS avg_disc, count(*) AS count_order \
            FROM lineitem WHERE l_shipdate <= date '1998-09-02' \
            GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus",
        ),
        (
            "Q3",
            "SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) AS revenue, \
            o_orderdate, o_shippriority \
            FROM customer, orders, lineitem \
            WHERE c_mktsegment='BUILDING' AND c_custkey=o_custkey AND l_orderkey=o_orderkey \
            AND o_orderdate < date '1995-03-15' AND l_shipdate > date '1995-03-15' \
            GROUP BY l_orderkey, o_orderdate, o_shippriority \
            ORDER BY revenue DESC, o_orderdate LIMIT 10",
        ),
        (
            "Q5",
            "SELECT n_name, sum(l_extendedprice*(1-l_discount)) AS revenue \
            FROM customer, orders, lineitem, supplier, nation, region \
            WHERE c_custkey=o_custkey AND l_orderkey=o_orderkey AND l_suppkey=s_suppkey \
            AND c_nationkey=s_nationkey AND s_nationkey=n_nationkey AND n_regionkey=r_regionkey \
            AND r_name='ASIA' AND o_orderdate >= date '1994-01-01' \
            AND o_orderdate < date '1995-01-01' GROUP BY n_name ORDER BY revenue DESC",
        ),
        (
            "Q6",
            "SELECT sum(l_extendedprice*l_discount) AS revenue FROM lineitem \
            WHERE l_shipdate >= date '1994-01-01' AND l_shipdate < date '1995-01-01' \
            AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24",
        ),
        (
            "Q10",
            "SELECT c_custkey, c_name, sum(l_extendedprice*(1-l_discount)) AS revenue, \
            c_acctbal, n_name, c_address, c_phone, c_comment \
            FROM customer, orders, lineitem, nation \
            WHERE c_custkey=o_custkey AND l_orderkey=o_orderkey AND c_nationkey=n_nationkey \
            AND o_orderdate >= date '1993-10-01' AND o_orderdate < date '1994-01-01' \
            AND l_returnflag='R' \
            GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment \
            ORDER BY revenue DESC LIMIT 20",
        ),
    ]
}

pub async fn run() {
    eprintln!(
        "[tpch] building synthetic tables (region/nation/supplier/customer/orders/lineitem) …\n"
    );
    let engine = Engine::new();
    build_tables(&engine);

    let mut failed = 0usize;
    for (name, sql) in queries() {
        let t = Instant::now();
        match engine.sql(sql).await {
            Ok(batches) => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                eprintln!(
                    "{name:<4} ok    {:.4}s  ({rows} result rows)",
                    t.elapsed().as_secs_f64()
                );
            }
            Err(e) => {
                failed += 1;
                eprintln!(
                    "{name:<4} FAIL  {}",
                    e.to_string().lines().next().unwrap_or("")
                );
            }
        }
    }

    eprintln!("\n=== TPC-H subset: {}/5 queries passed ===", 5 - failed);
    if failed > 0 {
        std::process::exit(1);
    }
}
