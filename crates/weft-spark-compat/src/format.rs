//! Render weft's Arrow output the way Spark's `SQLQueryTestSuite` renders golden output.
//!
//! Two pieces must match Spark byte-for-byte:
//!
//! 1. **Schema line** — `struct<name:sparktype,...>`. Types use Spark's spelling
//!    (`bigint`, `int`, `double`, `array<int>`, `decimal(10,2)`, …), names are whatever the
//!    analyzer produced (column-name divergence is a real, *measured* parity gap → bucketed
//!    `schema-only`, not hidden).
//! 2. **Rows** — each row is its cells joined by `\t`, rows joined by `\n`. `NULL` for nulls,
//!    `[1,2,3]` for arrays (no spaces), `{1,2}` for structs, `{k:v}` for maps, Java-style
//!    float rendering (`1.0`, `NaN`, `Infinity`).
//!
//! Where Arrow's own display already matches Spark (ints, decimals, dates, strings) we lean on
//! [`ArrayFormatter`]; floats, timestamps and the container types are rendered by hand because
//! Arrow and Spark disagree on spacing / delimiters / trailing zeros.

use datafusion::arrow::array::{
    Array, Float32Array, Float64Array, ListArray, MapArray, StructArray,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};

/// A Spark-formatted result: the `struct<...>` schema line plus rendered rows.
pub struct Formatted {
    pub schema: String,
    pub rows: Vec<String>,
}

impl Formatted {
    /// The rows joined the way the golden `output` block stores them.
    pub fn output(&self) -> String {
        self.rows.join("\n")
    }
}

/// Format a result set (schema + batches) into Spark golden form.
pub fn format_result(schema: &SchemaRef, batches: &[RecordBatch]) -> Formatted {
    let schema_str = format!(
        "struct<{}>",
        schema
            .fields()
            .iter()
            .map(|f| format!("{}:{}", f.name(), spark_type(f.data_type())))
            .collect::<Vec<_>>()
            .join(",")
    );

    let mut rows = Vec::new();
    for batch in batches {
        for r in 0..batch.num_rows() {
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(|c| fmt_value(c.as_ref(), r))
                .collect();
            rows.push(cells.join("\t"));
        }
    }
    Formatted {
        schema: schema_str,
        rows,
    }
}

/// Spark's spelling of an Arrow [`DataType`] for the `struct<...>` schema line.
pub fn spark_type(dt: &DataType) -> String {
    match dt {
        DataType::Null => "void".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "tinyint".into(),
        DataType::Int16 => "smallint".into(),
        DataType::Int32 => "int".into(),
        DataType::Int64 => "bigint".into(),
        // Spark has no unsigned ints; map to the smallest signed type that holds the range.
        DataType::UInt8 => "smallint".into(),
        DataType::UInt16 => "int".into(),
        DataType::UInt32 => "bigint".into(),
        DataType::UInt64 => "decimal(20,0)".into(),
        DataType::Float16 | DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".into(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "binary".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Timestamp(_, Some(_)) => "timestamp".into(),
        DataType::Timestamp(_, None) => "timestamp_ntz".into(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => format!("decimal({p},{s})"),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            format!("array<{}>", spark_type(f.data_type()))
        }
        DataType::Struct(fields) => format!(
            "struct<{}>",
            fields
                .iter()
                .map(|f| format!("{}:{}", f.name(), spark_type(f.data_type())))
                .collect::<Vec<_>>()
                .join(",")
        ),
        DataType::Map(field, _) => {
            // The map entries are a Struct<key, value>.
            if let DataType::Struct(kv) = field.data_type() {
                format!(
                    "map<{},{}>",
                    spark_type(kv[0].data_type()),
                    spark_type(kv[1].data_type())
                )
            } else {
                "map<string,string>".into()
            }
        }
        other => format!("{other:?}").to_lowercase(),
    }
}

/// Render a single cell (`array[row]`) Spark-style.
pub fn fmt_value(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "NULL".into();
    }
    match array.data_type() {
        DataType::Float32 => {
            let v = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row);
            fmt_f64(v as f64)
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            fmt_f64(v)
        }
        DataType::Timestamp(_, _) => {
            // Arrow renders `2013-07-01T00:00:00`; Spark uses a space separator.
            leaf(array, row).replacen('T', " ", 1)
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            let child = array
                .as_any()
                .downcast_ref::<ListArray>()
                .map(|l| l.value(row))
                .or_else(|| {
                    array
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::LargeListArray>()
                        .map(|l| l.value(row))
                });
            match child {
                Some(elems) => {
                    let parts: Vec<String> = (0..elems.len())
                        .map(|k| fmt_value(elems.as_ref(), k))
                        .collect();
                    format!("[{}]", parts.join(","))
                }
                None => leaf(array, row),
            }
        }
        DataType::Struct(_) => {
            let s = array.as_any().downcast_ref::<StructArray>().unwrap();
            let parts: Vec<String> = s
                .columns()
                .iter()
                .map(|c| fmt_value(c.as_ref(), row))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        DataType::Map(_, _) => {
            let m = array.as_any().downcast_ref::<MapArray>().unwrap();
            let entries = m.value(row);
            let keys = entries.column(0);
            let vals = entries.column(1);
            let parts: Vec<String> = (0..entries.len())
                .map(|k| {
                    format!(
                        "{}:{}",
                        fmt_value(keys.as_ref(), k),
                        fmt_value(vals.as_ref(), k)
                    )
                })
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        _ => leaf(array, row),
    }
}

/// Leaf rendering via Arrow's display (with `NULL` for nulls) — used for the scalar types
/// where Arrow already matches Spark (ints, decimals, dates, strings, booleans, binary).
fn leaf(array: &dyn Array, row: usize) -> String {
    let opts = FormatOptions::default().with_null("NULL");
    match ArrayFormatter::try_new(array, &opts) {
        Ok(f) => f.value(row).to_string(),
        Err(_) => "NULL".into(),
    }
}

/// Java `Double.toString`-style rendering: finite integral values keep a trailing `.0`,
/// non-finite values use Java's spellings. Rust's `{}` already does shortest round-trip,
/// so for non-integral finite values it matches Spark in the common case.
fn fmt_f64(v: f64) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        };
    }
    let s = format!("{v}");
    if s.contains('.') || s.contains('e') || s.contains('E') {
        s
    } else {
        format!("{s}.0")
    }
}

/// Map a [`TimeUnit`] to the number of sub-second digits Spark prints. (Kept for the
/// timestamp path; Spark trims trailing zeros, handled by Arrow's formatter.)
#[allow(dead_code)]
fn ts_digits(u: &TimeUnit) -> usize {
    match u {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn float_rendering_matches_java() {
        assert_eq!(fmt_f64(1.0), "1.0");
        assert_eq!(fmt_f64(1.5), "1.5");
        assert_eq!(fmt_f64(f64::NAN), "NaN");
        assert_eq!(fmt_f64(f64::INFINITY), "Infinity");
        assert_eq!(fmt_f64(f64::NEG_INFINITY), "-Infinity");
    }

    #[test]
    fn spark_type_spelling() {
        assert_eq!(spark_type(&DataType::Int64), "bigint");
        assert_eq!(spark_type(&DataType::Int32), "int");
        assert_eq!(spark_type(&DataType::Float64), "double");
        assert_eq!(spark_type(&DataType::Utf8), "string");
        assert_eq!(spark_type(&DataType::Decimal128(10, 2)), "decimal(10,2)");
        let list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        assert_eq!(spark_type(&list), "array<int>");
    }

    #[test]
    fn rows_and_nulls_and_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("count(a)", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
            ],
        )
        .unwrap();
        let out = format_result(&schema, &[batch]);
        assert_eq!(out.schema, "struct<count(a):bigint,s:string>");
        assert_eq!(out.rows, vec!["1\tx".to_string(), "NULL\ty".to_string()]);
    }
}
