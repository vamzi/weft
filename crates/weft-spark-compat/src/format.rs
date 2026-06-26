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
    Array, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, Float32Array, Float64Array,
    IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray,
    ListArray, MapArray, StructArray,
};
use datafusion::arrow::datatypes::{DataType, IntervalUnit, SchemaRef, TimeUnit};
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
        // Spark spells the legacy `CalendarInterval` type simply `interval` (Arrow's `Debug`
        // would give `Interval(MonthDayNano)`).
        DataType::Interval(_) => "interval".into(),
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

/// Render a single cell (`array[row]`) Spark-style for a **top-level** column.
pub fn fmt_value(array: &dyn Array, row: usize) -> String {
    fmt_cell(array, row, false)
}

/// Render `array[row]` Spark-style. `nested` mirrors Spark's `HiveResult.toHiveString(value,
/// nested = true)`: when the value is an *element* of a container (array / struct / map), Spark
/// (a) double-quotes string leaves (`["1","2"]`, not `[1,2]`), (b) renders a NULL as lowercase
/// `null` (a *top-level* NULL stays `NULL`), and (c) keeps recursing for nested containers
/// (`[["h"]]`). Non-string scalars (ints, decimals, dates, timestamps, booleans, binary) render
/// identically whether nested or not. This is **harness display only** — it never runs on the
/// engine path, so it just aligns weft's rendering with Spark's golden rendering.
fn fmt_cell(array: &dyn Array, row: usize, nested: bool) -> String {
    if array.is_null(row) {
        return if nested { "null".into() } else { "NULL".into() };
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
                    // Elements are nested: Spark quotes string leaves and recurses.
                    let parts: Vec<String> = (0..elems.len())
                        .map(|k| fmt_cell(elems.as_ref(), k, true))
                        .collect();
                    format!("[{}]", parts.join(","))
                }
                None => leaf(array, row),
            }
        }
        DataType::Struct(fields) => {
            // Spark 4.0's `HiveResult.toHiveString` renders a struct value as a JSON-style object
            // `{"field":value,...}`: every field is emitted (including NULL fields, as
            // `"field":null`), in schema order, with the value rendered in nested mode (string
            // leaves quoted, containers recursed). This is harness display only — it never runs on
            // the engine path; it just aligns weft's rendering with the Spark golden, which quotes
            // the field name and prefixes each value.
            let s = array.as_any().downcast_ref::<StructArray>().unwrap();
            let parts: Vec<String> = fields
                .iter()
                .zip(s.columns().iter())
                .map(|(f, c)| {
                    let mut p = String::new();
                    push_json_field_name(f.name(), &mut p);
                    p.push(':');
                    p.push_str(&fmt_cell(c.as_ref(), row, true));
                    p
                })
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
                        fmt_cell(keys.as_ref(), k, true),
                        fmt_cell(vals.as_ref(), k, true)
                    )
                })
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        // Spark double-quotes a string only when it is nested inside a container; a top-level
        // string column renders bare (it falls through to the `_ => leaf` arm below).
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View if nested => {
            format!("\"{}\"", leaf(array, row))
        }
        // Spark's `hiveResultString` renders BinaryType as `new String(bytes, UTF_8)` — the bytes
        // decoded as UTF-8 with U+FFFD for invalid sequences — NOT Arrow's hex dump. (e.g.
        // `to_binary('737472696E67','hex')` prints `string`.) `from_utf8_lossy` matches Java's
        // substitution behavior exactly.
        DataType::Binary => {
            let v = array.as_any().downcast_ref::<BinaryArray>().unwrap().value(row);
            String::from_utf8_lossy(v).into_owned()
        }
        DataType::LargeBinary => {
            let v = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(row);
            String::from_utf8_lossy(v).into_owned()
        }
        DataType::BinaryView => {
            let v = array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .unwrap()
                .value(row);
            String::from_utf8_lossy(v).into_owned()
        }
        DataType::FixedSizeBinary(_) => {
            let v = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(row);
            String::from_utf8_lossy(v).into_owned()
        }
        // Spark renders the legacy CalendarInterval (which DataFusion stores as an Arrow
        // interval) via `CalendarInterval.toString` / `IntervalUtils.toMultiUnitsString`: full
        // *plural* unit words, months normalized into years + months, and seconds at microsecond
        // resolution with trailing zeros stripped — NOT Arrow's abbreviated `999 mons` /
        // `16 mins 39.000000000 secs`. The stored Arrow value is identical; only the string
        // rendering differs, so this is a pure parity-oracle fix (never on Engine::sql's path).
        DataType::Interval(unit) => fmt_interval(array, row, unit),
        _ => leaf(array, row),
    }
}

/// Render an Arrow interval cell the way Spark renders a legacy `CalendarInterval`
/// (`org.apache.spark.unsafe.types.CalendarInterval.toString`). All three Arrow interval layouts
/// collapse to Spark's `(months, days, microseconds)` triple before formatting.
fn fmt_interval(array: &dyn Array, row: usize, unit: &IntervalUnit) -> String {
    let (months, days, micros) = match unit {
        IntervalUnit::YearMonth => {
            let v = array
                .as_any()
                .downcast_ref::<IntervalYearMonthArray>()
                .unwrap()
                .value(row);
            (v as i64, 0i64, 0i64)
        }
        IntervalUnit::DayTime => {
            let v = array
                .as_any()
                .downcast_ref::<IntervalDayTimeArray>()
                .unwrap()
                .value(row);
            (0i64, v.days as i64, v.milliseconds as i64 * 1_000)
        }
        IntervalUnit::MonthDayNano => {
            let v = array
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .unwrap()
                .value(row);
            // Spark's CalendarInterval carries microseconds, so drop sub-microsecond nanos
            // (truncating toward zero, matching Java integer division).
            (v.months as i64, v.days as i64, v.nanoseconds / 1_000)
        }
    };
    fmt_calendar_interval(months, days, micros)
}

/// Spark's multi-units interval string from `(months, days, microseconds)`. Mirrors
/// `CalendarInterval.toString`: only non-zero components are emitted, each `"<n> <plural-unit>"`,
/// space-joined; an all-zero interval renders `"0 seconds"`.
fn fmt_calendar_interval(months: i64, days: i64, micros: i64) -> String {
    const MICROS_PER_HOUR: i64 = 3_600_000_000;
    const MICROS_PER_MINUTE: i64 = 60_000_000;
    if months == 0 && days == 0 && micros == 0 {
        return "0 seconds".into();
    }
    let mut parts: Vec<String> = Vec::new();
    if months != 0 {
        let years = months / 12;
        let mons = months % 12;
        if years != 0 {
            parts.push(format!("{years} years"));
        }
        if mons != 0 {
            parts.push(format!("{mons} months"));
        }
    }
    if days != 0 {
        parts.push(format!("{days} days"));
    }
    if micros != 0 {
        let mut rest = micros;
        let hours = rest / MICROS_PER_HOUR;
        rest %= MICROS_PER_HOUR;
        let minutes = rest / MICROS_PER_MINUTE;
        rest %= MICROS_PER_MINUTE;
        if hours != 0 {
            parts.push(format!("{hours} hours"));
        }
        if minutes != 0 {
            parts.push(format!("{minutes} minutes"));
        }
        if rest != 0 {
            parts.push(format!("{} seconds", fmt_interval_seconds(rest)));
        }
    }
    parts.join(" ")
}

/// Render the fractional-second component (`micros`) like Java
/// `BigDecimal.valueOf(micros, 6).stripTrailingZeros().toPlainString()`: integral values print
/// with no decimal point, otherwise up to six fraction digits with trailing zeros stripped.
/// Negative values carry a leading `-`.
fn fmt_interval_seconds(micros: i64) -> String {
    let neg = micros < 0;
    let abs = micros.unsigned_abs();
    let whole = abs / 1_000_000;
    let frac = abs % 1_000_000;
    let mut s = String::new();
    if neg {
        s.push('-');
    }
    if frac == 0 {
        s.push_str(&whole.to_string());
    } else {
        let frac_str = format!("{frac:06}");
        s.push_str(&format!("{whole}.{}", frac_str.trim_end_matches('0')));
    }
    s
}

/// Append a struct field name as a JSON-quoted key (matching Spark's `{"name":...}` rendering).
/// Field names in the corpus are plain identifiers, but quote-escape defensively so an odd name
/// can never break the surrounding JSON shape.
fn push_json_field_name(name: &str, out: &mut String) {
    out.push('"');
    for c in name.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c => out.push(c),
        }
    }
    out.push('"');
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
    fn interval_renders_spark_multi_units() {
        // Goldens from postgreSQL/interval.sql (`SELECT interval '999' second`, etc.).
        assert_eq!(
            fmt_calendar_interval(0, 0, 999_000_000),
            "16 minutes 39 seconds"
        );
        assert_eq!(
            fmt_calendar_interval(0, 0, 59_940_000_000),
            "16 hours 39 minutes"
        );
        assert_eq!(fmt_calendar_interval(0, 0, 3_596_400_000_000), "999 hours");
        assert_eq!(fmt_calendar_interval(0, 999, 0), "999 days");
        assert_eq!(fmt_calendar_interval(999, 0, 0), "83 years 3 months");
        assert_eq!(fmt_calendar_interval(12, 0, 0), "1 years");
        assert_eq!(fmt_calendar_interval(2, 0, 0), "2 months");
        assert_eq!(fmt_calendar_interval(0, 3, 0), "3 days");
        assert_eq!(fmt_calendar_interval(0, 0, 14_400_000_000), "4 hours");
        assert_eq!(fmt_calendar_interval(0, 0, 300_000_000), "5 minutes");
        assert_eq!(fmt_calendar_interval(0, 0, 6_000_000), "6 seconds");
        assert_eq!(fmt_calendar_interval(14, 0, 0), "1 years 2 months");
        // All-zero, sub-second precision, and negative components.
        assert_eq!(fmt_calendar_interval(0, 0, 0), "0 seconds");
        assert_eq!(
            fmt_calendar_interval(14, 1, 7_008_009),
            "1 years 2 months 1 days 7.008009 seconds"
        );
        assert_eq!(fmt_interval_seconds(7_008_009), "7.008009");
        assert_eq!(fmt_interval_seconds(6_000_000), "6");
        assert_eq!(fmt_interval_seconds(-6_500_000), "-6.5");
        assert_eq!(fmt_interval_seconds(39_000), "0.039");
        assert_eq!(spark_type(&DataType::Interval(IntervalUnit::MonthDayNano)), "interval");
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

    #[test]
    fn nested_strings_are_quoted_ints_are_not_nulls_lowercase() {
        use datafusion::arrow::array::{Int32Builder, ListBuilder, StringBuilder};
        // array<string> with a non-participating (NULL) middle element → `["a",null,"f"]`.
        let mut sb = ListBuilder::new(StringBuilder::new());
        sb.values().append_value("a");
        sb.values().append_null();
        sb.values().append_value("f");
        sb.append(true);
        let str_list = sb.finish();
        assert_eq!(fmt_value(&str_list, 0), r#"["a",null,"f"]"#);
        // A bare top-level string stays unquoted.
        let s = StringArray::from(vec![Some("a")]);
        assert_eq!(fmt_value(&s, 0), "a");
        // array<int> stays unquoted, nested NULL still lowercase → `[1,2,null]`.
        let mut ib = ListBuilder::new(Int32Builder::new());
        ib.values().append_value(1);
        ib.values().append_value(2);
        ib.values().append_null();
        ib.append(true);
        let int_list = ib.finish();
        assert_eq!(fmt_value(&int_list, 0), "[1,2,null]");
    }
}
