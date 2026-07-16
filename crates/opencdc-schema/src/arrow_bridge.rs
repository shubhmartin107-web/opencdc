use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;

use opencdc_core::arrow::DebeziumArrowMapper;
use opencdc_core::schema::{DebeziumField, DebeziumSchema, DebeziumSchemaType};

pub struct SchemaBridge;

impl SchemaBridge {
    pub fn debezium_to_arrow(schema: &DebeziumSchema) -> Schema {
        DebeziumArrowMapper::debezium_schema_to_arrow(schema)
    }

    pub fn arrow_to_debezium(schema: &Schema) -> DebeziumSchema {
        let fields: Vec<DebeziumField> = schema
            .fields()
            .iter()
            .map(|f| Self::arrow_field_to_debezium(f))
            .collect();

        DebeziumSchema {
            schema_type: DebeziumSchemaType::Struct,
            fields: Some(fields),
            name: schema
                .metadata()
                .get("debezium.name")
                .cloned(),
            ..Default::default()
        }
    }

    fn arrow_type_to_debezium(dt: &DataType) -> DebeziumSchemaType {
        match dt {
            DataType::Int8 => DebeziumSchemaType::Int8,
            DataType::Int16 => DebeziumSchemaType::Int16,
            DataType::Int32 => DebeziumSchemaType::Int32,
            DataType::Int64 => DebeziumSchemaType::Int64,
            DataType::UInt8 => DebeziumSchemaType::Int16,
            DataType::UInt16 => DebeziumSchemaType::Int32,
            DataType::UInt32 => DebeziumSchemaType::Int64,
            DataType::UInt64 => DebeziumSchemaType::Int64,
            DataType::Float16 | DataType::Float32 => DebeziumSchemaType::Float,
            DataType::Float64 => DebeziumSchemaType::Double,
            DataType::Boolean => DebeziumSchemaType::Boolean,
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                DebeziumSchemaType::String
            }
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                DebeziumSchemaType::Bytes
            }
            DataType::Date32 | DataType::Date64 => DebeziumSchemaType::Date,
            DataType::Time32(_) | DataType::Time64(TimeUnit::Millisecond) => {
                DebeziumSchemaType::Time
            }
            DataType::Time64(TimeUnit::Microsecond) => DebeziumSchemaType::MicroTime,
            DataType::Time64(TimeUnit::Nanosecond) => DebeziumSchemaType::NanoTime,
            DataType::Timestamp(TimeUnit::Millisecond, _)
            | DataType::Timestamp(TimeUnit::Second, _) => DebeziumSchemaType::Timestamp,
            DataType::Timestamp(TimeUnit::Microsecond, _) => DebeziumSchemaType::MicroTimestamp,
            DataType::Timestamp(TimeUnit::Nanosecond, _) => DebeziumSchemaType::NanoTimestamp,
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
                DebeziumSchemaType::Decimal
            }
            DataType::Struct(_) => DebeziumSchemaType::Struct,
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                DebeziumSchemaType::Array
            }
            DataType::Map(_, _) => DebeziumSchemaType::Map,
            _ => DebeziumSchemaType::String,
        }
    }

    fn arrow_field_to_debezium(field: &Field) -> DebeziumField {
        let debezium_type = Self::arrow_type_to_debezium(field.data_type());
        let nullable = field.is_nullable();

        let parameters = if let DataType::Decimal128(precision, scale) = field.data_type() {
            Some(HashMap::from([
                ("precision".to_string(), precision.to_string()),
                ("scale".to_string(), scale.to_string()),
            ]))
        } else {
            None
        };

        DebeziumField {
            field_name: field.name().clone(),
            field_type: serde_json::Value::String(debezium_type.as_str().to_string()),
            fields: match field.data_type() {
                DataType::Struct(inner_fields) => {
                    Some(
                        inner_fields
                            .iter()
                            .map(|f| Self::arrow_field_to_debezium(f))
                            .collect(),
                    )
                }
                _ => None,
            },
            optional: Some(nullable),
            name: None,
            version: None,
            doc: field
                .metadata()
                .get("doc")
                .cloned(),
            parameters,
            default: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arrow_to_debezium_primitive() {
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]);

        let debezium = SchemaBridge::arrow_to_debezium(&arrow_schema);
        let fields = debezium.fields.as_ref().unwrap();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].field_name, "id");
        assert_eq!(fields[0].resolve_type(), DebeziumSchemaType::Int32);
        assert_eq!(fields[0].optional, Some(false));
        assert_eq!(fields[1].field_name, "name");
        assert_eq!(fields[1].resolve_type(), DebeziumSchemaType::String);
        assert_eq!(fields[1].optional, Some(true));
    }

    #[test]
    fn test_roundtrip() {
        let original = DebeziumSchema {
            schema_type: DebeziumSchemaType::Struct,
            fields: Some(vec![
                DebeziumField::int32("id"),
                DebeziumField::string("name").optional(),
            ]),
            name: Some("io.opencdc.test.public.users.Envelope".to_string()),
            ..Default::default()
        };

        let arrow = SchemaBridge::debezium_to_arrow(&original);
        let roundtripped = SchemaBridge::arrow_to_debezium(&arrow);

        let orig_fields = original.fields.as_ref().unwrap();
        let rt_fields = roundtripped.fields.as_ref().unwrap();
        assert_eq!(orig_fields.len(), rt_fields.len());
        assert_eq!(orig_fields[0].field_name, rt_fields[0].field_name);
        assert_eq!(
            orig_fields[0].resolve_type(),
            rt_fields[0].resolve_type()
        );
    }
}
