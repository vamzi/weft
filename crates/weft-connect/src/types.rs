//! Arrow → Spark Connect `DataType` conversion, for `AnalyzePlan(Schema)` and for stamping
//! result-relation schemas. Covers the scalar types Weft produces today (all of ClickBench) plus
//! best-effort nested types; anything unmapped falls back to `string` rather than failing.

use weft_loom::arrow::datatypes::{DataType, Fields, Schema};
use weft_proto::spark::connect as sc;

use sc::data_type as d;
use sc::data_type::Kind;

fn wrap(kind: Kind) -> sc::DataType {
    sc::DataType { kind: Some(kind) }
}

/// Convert one Arrow [`DataType`] to a Spark Connect [`sc::DataType`].
pub fn arrow_to_spark(t: &DataType) -> sc::DataType {
    let kind = match t {
        DataType::Null => Kind::Null(d::Null::default()),
        DataType::Boolean => Kind::Boolean(d::Boolean::default()),
        DataType::Int8 | DataType::UInt8 => Kind::Byte(d::Byte::default()),
        DataType::Int16 | DataType::UInt16 => Kind::Short(d::Short::default()),
        DataType::Int32 | DataType::UInt32 => Kind::Integer(d::Integer::default()),
        DataType::Int64 | DataType::UInt64 => Kind::Long(d::Long::default()),
        DataType::Float16 | DataType::Float32 => Kind::Float(d::Float::default()),
        DataType::Float64 => Kind::Double(d::Double::default()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Kind::String(d::String::default())
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Kind::Binary(d::Binary::default()),
        DataType::Date32 | DataType::Date64 => Kind::Date(d::Date::default()),
        DataType::Timestamp(_, Some(_)) => Kind::Timestamp(d::Timestamp::default()),
        DataType::Timestamp(_, None) => Kind::TimestampNtz(d::TimestampNtz::default()),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => Kind::Decimal(d::Decimal {
            precision: Some(*p as i32),
            scale: Some(*s as i32),
            type_variation_reference: 0,
        }),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _) => Kind::Array(Box::new(d::Array {
            element_type: Some(Box::new(arrow_to_spark(f.data_type()))),
            contains_null: f.is_nullable(),
            type_variation_reference: 0,
        })),
        DataType::Struct(fields) => Kind::Struct(struct_of(fields)),
        DataType::Map(entry, _) => {
            // The Map field's child is a Struct{key, value}.
            if let DataType::Struct(kv) = entry.data_type() {
                let key = kv.first().map(|f| arrow_to_spark(f.data_type()));
                let val = kv.get(1);
                Kind::Map(Box::new(d::Map {
                    key_type: key.map(Box::new),
                    value_type: val.map(|f| Box::new(arrow_to_spark(f.data_type()))),
                    value_contains_null: val.map(|f| f.is_nullable()).unwrap_or(true),
                    type_variation_reference: 0,
                }))
            } else {
                Kind::String(d::String::default())
            }
        }
        // Intervals, time, dictionary, union, etc. — not produced by Weft yet; be lenient.
        _ => Kind::String(d::String::default()),
    };
    wrap(kind)
}

/// Build a Spark Connect `Struct` `DataType` from Arrow [`Fields`].
fn struct_of(fields: &Fields) -> d::Struct {
    d::Struct {
        fields: fields
            .iter()
            .map(|f| d::StructField {
                name: f.name().clone(),
                data_type: Some(arrow_to_spark(f.data_type())),
                nullable: f.is_nullable(),
                metadata: None,
            })
            .collect(),
        type_variation_reference: 0,
    }
}

/// Convert an Arrow [`Schema`] to a Spark Connect struct `DataType` (a row schema).
pub fn schema_to_spark(schema: &Schema) -> sc::DataType {
    wrap(Kind::Struct(struct_of(schema.fields())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use weft_loom::arrow::datatypes::{Field, Schema};

    #[test]
    fn maps_clickbench_scalars() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, false),
            Field::new("d", DataType::Date32, false),
            Field::new("e", DataType::Int16, false),
        ]);
        let st = schema_to_spark(&schema);
        let Some(Kind::Struct(s)) = st.kind else {
            panic!("expected struct")
        };
        assert_eq!(s.fields.len(), 5);
        assert!(matches!(
            s.fields[0].data_type.as_ref().unwrap().kind,
            Some(Kind::Long(_))
        ));
        assert!(matches!(
            s.fields[1].data_type.as_ref().unwrap().kind,
            Some(Kind::String(_))
        ));
        assert!(matches!(
            s.fields[2].data_type.as_ref().unwrap().kind,
            Some(Kind::Double(_))
        ));
        assert!(matches!(
            s.fields[3].data_type.as_ref().unwrap().kind,
            Some(Kind::Date(_))
        ));
        assert!(matches!(
            s.fields[4].data_type.as_ref().unwrap().kind,
            Some(Kind::Short(_))
        ));
        assert!(s.fields[1].nullable);
        assert!(!s.fields[0].nullable);
    }

    #[test]
    fn nested_list_and_struct() {
        let inner = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let st = arrow_to_spark(&inner);
        assert!(matches!(st.kind, Some(Kind::Array(_))));
    }
}
