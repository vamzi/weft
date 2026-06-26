//! Hive/Glue type-string → Arrow type mapping, shared by the Hive Metastore and AWS Glue catalog
//! providers (both speak the same Hive type-string vocabulary).
//!
//! The point of attaching a catalog-declared schema to a [`TableMetadata`](crate::TableMetadata) is
//! to let the engine read files *against* the authoritative schema: files whose physical types
//! differ (e.g. one monthly Parquet dump stores a column as `int`, another as `bigint`) are cast to
//! the declared types at scan time, instead of failing DataFusion's strict schema-inference
//! "merge" check.
//!
//! Mapping policy is deliberately **all-or-nothing** ([`columns_to_schema`]): if any column's type
//! string is one we don't faithfully model (complex `array<…>` / `struct<…>` / `map<…>`, or
//! anything unrecognized), we return `None` so the caller falls back to inferring the *whole*
//! table from the data files. A partial schema would shift column positions and silently corrupt
//! reads, so inferring everything is the safe choice.

use crate::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

/// Build an Arrow [`Schema`] from an ordered list of `(column_name, hive_type_string)` pairs
/// (data columns first, then partition columns).
///
/// Returns `None` — meaning "fall back to data-file inference" — when the list is empty or any
/// column's type string is unmappable (see the module docs for why this is all-or-nothing).
pub fn columns_to_schema(columns: impl IntoIterator<Item = (String, String)>) -> Option<Schema> {
    let mut fields: Vec<Field> = Vec::new();
    for (name, ty) in columns {
        let dt = hive_type_to_arrow(&ty)?;
        // Catalog columns are nullable: Hive/Glue don't carry per-column NOT NULL, external data
        // lakes routinely contain nulls, and this matches what Parquet inference would produce.
        fields.push(Field::new(name, dt, true));
    }
    if fields.is_empty() {
        return None;
    }
    Some(Schema::new(fields))
}

/// Map a Hive/Glue type string to an Arrow [`DataType`].
///
/// Covers the common scalar types and the parameterized `decimal(p,s)` / `varchar(n)` / `char(n)`.
/// Returns `None` for complex types (`array<…>`, `struct<…>`, `map<…>`, `uniontype<…>`) and any
/// unrecognized string, so the caller can fall back to inferring the whole table's schema from the
/// data files rather than risk a wrong mapping. Whitespace- and case-insensitive.
pub fn hive_type_to_arrow(ty: &str) -> Option<DataType> {
    let t = ty.trim().to_ascii_lowercase();
    Some(match t.as_str() {
        "bigint" | "long" => DataType::Int64,
        "int" | "integer" => DataType::Int32,
        "smallint" | "short" => DataType::Int16,
        "tinyint" | "byte" => DataType::Int8,
        "double" => DataType::Float64,
        "float" | "real" => DataType::Float32,
        // Hive/Spark `string` and unparameterized `varchar`/`char` map to Arrow Utf8.
        "string" | "varchar" | "char" | "text" => DataType::Utf8,
        "boolean" | "bool" => DataType::Boolean,
        "date" => DataType::Date32,
        // Hive `timestamp` is microsecond, no timezone — the standard Hive/Spark→Arrow mapping and
        // what Spark-written Parquet stores (logical TIMESTAMP_MICROS / INT96 reads as micros).
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "binary" => DataType::Binary,
        // Parameterized: varchar(n) / char(n) → Utf8 (length is a constraint, not an Arrow type);
        // decimal(p,s) → Decimal128(p,s). Anything else falls through to `None`.
        _ => {
            if t.starts_with("varchar(") || t.starts_with("char(") {
                DataType::Utf8
            } else if let Some(args) = t
                .strip_prefix("decimal(")
                .or_else(|| t.strip_prefix("numeric("))
                .and_then(|r| r.strip_suffix(')'))
            {
                let (p, s) = parse_decimal_args(args)?;
                DataType::Decimal128(p, s)
            } else {
                // Bare `decimal`/`numeric` default to Hive's (10, 0).
                match t.as_str() {
                    "decimal" | "numeric" => DataType::Decimal128(10, 0),
                    _ => return None,
                }
            }
        }
    })
}

/// Parse the `p,s` (or just `p`) inside `decimal(...)`. Scale defaults to 0. Returns `None` on a
/// malformed spec so the column falls back to inference.
fn parse_decimal_args(args: &str) -> Option<(u8, i8)> {
    let mut it = args.split(',');
    let p: u8 = it.next()?.trim().parse().ok()?;
    let s: i8 = match it.next() {
        Some(s) => s.trim().parse().ok()?,
        None => 0,
    };
    if it.next().is_some() || p == 0 {
        return None;
    }
    Some((p, s))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_type_mapping() {
        let cases = [
            ("bigint", DataType::Int64),
            ("INT", DataType::Int32),
            ("integer", DataType::Int32),
            ("smallint", DataType::Int16),
            ("tinyint", DataType::Int8),
            ("double", DataType::Float64),
            ("float", DataType::Float32),
            ("string", DataType::Utf8),
            (" Boolean ", DataType::Boolean),
            ("date", DataType::Date32),
            ("binary", DataType::Binary),
            (
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
        ];
        for (hive, arrow) in cases {
            assert_eq!(hive_type_to_arrow(hive), Some(arrow), "type `{hive}`");
        }
    }

    #[test]
    fn parameterized_type_mapping() {
        assert_eq!(hive_type_to_arrow("varchar(50)"), Some(DataType::Utf8));
        assert_eq!(hive_type_to_arrow("char(10)"), Some(DataType::Utf8));
        assert_eq!(
            hive_type_to_arrow("decimal(15,2)"),
            Some(DataType::Decimal128(15, 2))
        );
        // Spaces inside the parens are tolerated.
        assert_eq!(
            hive_type_to_arrow("decimal( 38 , 10 )"),
            Some(DataType::Decimal128(38, 10))
        );
        // Precision-only decimal → scale 0; bare `decimal` → Hive default (10, 0).
        assert_eq!(
            hive_type_to_arrow("decimal(9)"),
            Some(DataType::Decimal128(9, 0))
        );
        assert_eq!(
            hive_type_to_arrow("decimal"),
            Some(DataType::Decimal128(10, 0))
        );
    }

    #[test]
    fn unknown_and_complex_types_fall_back() {
        // Complex types we don't model → None (whole-table inference fallback).
        assert_eq!(hive_type_to_arrow("array<int>"), None);
        assert_eq!(hive_type_to_arrow("struct<a:int,b:string>"), None);
        assert_eq!(hive_type_to_arrow("map<string,int>"), None);
        assert_eq!(hive_type_to_arrow("uniontype<int,string>"), None);
        // Unrecognized / malformed.
        assert_eq!(hive_type_to_arrow("frobnicate"), None);
        assert_eq!(hive_type_to_arrow("decimal(x,2)"), None);
        assert_eq!(hive_type_to_arrow("decimal(1,2,3)"), None);
    }

    #[test]
    fn schema_from_columns_includes_partition_keys() {
        let cols = [
            ("vendor_id".to_string(), "bigint".to_string()),
            ("fare".to_string(), "decimal(10,2)".to_string()),
            // Partition column appended after data columns by the caller.
            ("month".to_string(), "string".to_string()),
        ];
        let schema = columns_to_schema(cols).expect("schema built");
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "vendor_id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Decimal128(10, 2));
        assert_eq!(schema.field(2).name(), "month");
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        // Catalog columns are nullable.
        assert!(schema.field(0).is_nullable());
    }

    #[test]
    fn empty_columns_fall_back_to_inference() {
        let empty: Vec<(String, String)> = Vec::new();
        assert_eq!(columns_to_schema(empty), None);
    }

    #[test]
    fn any_unmappable_column_falls_back_to_inference() {
        // One complex column poisons the whole schema → infer rather than shift positions.
        let cols = [
            ("id".to_string(), "bigint".to_string()),
            ("tags".to_string(), "array<string>".to_string()),
        ];
        assert_eq!(columns_to_schema(cols), None);
    }
}
