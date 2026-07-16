use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::collections::HashMap;

use crate::schema::{DebeziumField, DebeziumSchema, DebeziumSchemaType};

pub struct DebeziumArrowMapper;

impl DebeziumArrowMapper {
    pub fn debezium_type_to_arrow(
        debezium_type: &DebeziumSchemaType,
        parameters: Option<&HashMap<String, String>>,
    ) -> DataType {
        match debezium_type {
            DebeziumSchemaType::Int8 => DataType::Int8,
            DebeziumSchemaType::Int16 => DataType::Int16,
            DebeziumSchemaType::Int32 => DataType::Int32,
            DebeziumSchemaType::Int64 => DataType::Int64,
            DebeziumSchemaType::Float => DataType::Float32,
            DebeziumSchemaType::Double => DataType::Float64,
            DebeziumSchemaType::Boolean => DataType::Boolean,
            DebeziumSchemaType::String => DataType::Utf8,
            DebeziumSchemaType::Bytes => DataType::Binary,
            DebeziumSchemaType::Decimal => {
                let precision = parameters
                    .and_then(|p| p.get("precision"))
                    .and_then(|v| v.parse::<u8>().ok())
                    .unwrap_or(38);
                let scale = parameters
                    .and_then(|p| p.get("scale"))
                    .and_then(|v| v.parse::<i8>().ok())
                    .unwrap_or(0);
                DataType::Decimal128(precision, scale)
            }
            DebeziumSchemaType::Timestamp | DebeziumSchemaType::MicroTimestamp => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            DebeziumSchemaType::NanoTimestamp => DataType::Timestamp(TimeUnit::Nanosecond, None),
            DebeziumSchemaType::Date => DataType::Date64,
            DebeziumSchemaType::Time | DebeziumSchemaType::MicroTime => {
                DataType::Time64(TimeUnit::Microsecond)
            }
            DebeziumSchemaType::NanoTime => DataType::Time64(TimeUnit::Nanosecond),
            DebeziumSchemaType::Json => DataType::Utf8,
            DebeziumSchemaType::Struct | DebeziumSchemaType::Map | DebeziumSchemaType::Array => {
                DataType::Utf8
            }
            DebeziumSchemaType::Unknown(_) => DataType::Utf8,
        }
    }

    pub fn debezium_field_to_arrow(field: &DebeziumField) -> Field {
        let dt = field.resolve_type();
        let arrow_type = Self::debezium_type_to_arrow(&dt, field.parameters.as_ref());
        let nullable = field.optional.unwrap_or(true);
        let mut arrow_field = Field::new(&field.field_name, arrow_type, nullable);

        if let Some(doc) = &field.doc {
            arrow_field = arrow_field.with_metadata([("doc".to_string(), doc.to_string())].into());
        }

        arrow_field
    }

    pub fn debezium_schema_to_arrow(schema: &DebeziumSchema) -> Schema {
        let fields: Vec<Field> = schema
            .fields
            .as_ref()
            .map(|fields| fields.iter().map(Self::debezium_field_to_arrow).collect())
            .unwrap_or_default();

        let mut metadata = HashMap::new();
        if let Some(name) = &schema.name {
            metadata.insert("debezium.name".to_string(), name.clone());
        }
        if let Some(doc) = &schema.doc {
            metadata.insert("debezium.doc".to_string(), doc.clone());
        }

        Schema::new_with_metadata(fields, metadata)
    }

    pub fn table_fields_to_arrow_schema(name: &str, fields: &[DebeziumField]) -> Schema {
        let arrow_fields: Vec<Field> = fields.iter().map(Self::debezium_field_to_arrow).collect();
        Schema::new_with_metadata(
            arrow_fields,
            [("debezium.name".to_string(), name.to_string())].into(),
        )
    }

    pub fn make_envelope_schema(
        namespace: &str,
        table: &str,
        columns: &[DebeziumField],
    ) -> DebeziumSchema {
        let table_schema_name = format!("{}.{}", namespace, table);
        let envelope_name = format!("{}.Envelope", table_schema_name);

        let before_fields: Vec<DebeziumField> = columns
            .iter()
            .map(|f| {
                let mut field = f.clone();
                field.optional = Some(true);
                field.name = Some(format!("{}.{}", table_schema_name, f.field_name));
                field
            })
            .collect();

        let after_fields: Vec<DebeziumField> = columns
            .iter()
            .map(|f| {
                let mut field = f.clone();
                field.optional = Some(true);
                field.name = Some(format!("{}.{}", table_schema_name, f.field_name));
                field
            })
            .collect();

        DebeziumSchema {
            schema_type: DebeziumSchemaType::Struct,
            fields: Some(vec![
                DebeziumField {
                    field_name: "before".to_string(),
                    field_type: serde_json::Value::String("struct".to_string()),
                    fields: Some(before_fields),
                    optional: Some(true),
                    name: Some(table_schema_name.clone()),
                    ..Default::default()
                },
                DebeziumField {
                    field_name: "after".to_string(),
                    field_type: serde_json::Value::String("struct".to_string()),
                    fields: Some(after_fields),
                    optional: Some(true),
                    name: Some(table_schema_name.clone()),
                    ..Default::default()
                },
                DebeziumField {
                    field_name: "source".to_string(),
                    field_type: serde_json::Value::String("struct".to_string()),
                    fields: Some(make_source_schema_fields()),
                    optional: Some(false),
                    name: Some(format!(
                        "io.debezium.connector.{}.Source",
                        namespace.split('.').next().unwrap_or("common")
                    )),
                    ..Default::default()
                },
                DebeziumField {
                    field_name: "op".to_string(),
                    field_type: serde_json::Value::String("string".to_string()),
                    optional: Some(false),
                    ..Default::default()
                },
                DebeziumField {
                    field_name: "ts_ms".to_string(),
                    field_type: serde_json::Value::String("int64".to_string()),
                    optional: Some(true),
                    ..Default::default()
                },
            ]),
            optional: Some(false),
            name: Some(envelope_name),
            version: Some(1),
            ..Default::default()
        }
    }

    pub fn schema_name_for_table(namespace: &str, table: &str) -> String {
        format!("{}.{}", namespace, table)
    }

    pub fn envelope_name_for_table(namespace: &str, table: &str) -> String {
        format!("{}.{}.Envelope", namespace, table)
    }

    pub fn key_schema_name_for_table(namespace: &str, table: &str) -> String {
        format!("{}.{}.Key", namespace, table)
    }
}

fn make_source_schema_fields() -> Vec<DebeziumField> {
    vec![
        DebeziumField {
            field_name: "version".to_string(),
            field_type: serde_json::Value::String("string".to_string()),
            optional: Some(false),
            ..DebeziumField::new("", DebeziumSchemaType::String)
        },
        DebeziumField {
            field_name: "connector".to_string(),
            field_type: serde_json::Value::String("string".to_string()),
            optional: Some(false),
            ..DebeziumField::new("", DebeziumSchemaType::String)
        },
        DebeziumField {
            field_name: "name".to_string(),
            field_type: serde_json::Value::String("string".to_string()),
            optional: Some(false),
            ..DebeziumField::new("", DebeziumSchemaType::String)
        },
        DebeziumField {
            field_name: "ts_ms".to_string(),
            field_type: serde_json::Value::String("int64".to_string()),
            optional: Some(false),
            ..DebeziumField::new("", DebeziumSchemaType::Int64)
        },
        DebeziumField {
            field_name: "snapshot".to_string(),
            field_type: serde_json::Value::String("string".to_string()),
            optional: Some(true),
            ..DebeziumField::new("", DebeziumSchemaType::String)
        },
        DebeziumField {
            field_name: "db".to_string(),
            field_type: serde_json::Value::String("string".to_string()),
            optional: Some(false),
            ..DebeziumField::new("", DebeziumSchemaType::String)
        },
        DebeziumField {
            field_name: "schema".to_string(),
            field_type: serde_json::Value::String("string".to_string()),
            optional: Some(true),
            ..DebeziumField::new("", DebeziumSchemaType::String)
        },
        DebeziumField {
            field_name: "table".to_string(),
            field_type: serde_json::Value::String("string".to_string()),
            optional: Some(false),
            ..DebeziumField::new("", DebeziumSchemaType::String)
        },
        DebeziumField {
            field_name: "txId".to_string(),
            field_type: serde_json::Value::String("int64".to_string()),
            optional: Some(true),
            ..DebeziumField::new("", DebeziumSchemaType::Int64)
        },
        DebeziumField {
            field_name: "lsn".to_string(),
            field_type: serde_json::Value::String("int64".to_string()),
            optional: Some(true),
            ..DebeziumField::new("", DebeziumSchemaType::Int64)
        },
        DebeziumField {
            field_name: "xmin".to_string(),
            field_type: serde_json::Value::String("int64".to_string()),
            optional: Some(true),
            ..DebeziumField::new("", DebeziumSchemaType::Int64)
        },
        DebeziumField {
            field_name: "file".to_string(),
            field_type: serde_json::Value::String("string".to_string()),
            optional: Some(true),
            ..DebeziumField::new("", DebeziumSchemaType::String)
        },
        DebeziumField {
            field_name: "pos".to_string(),
            field_type: serde_json::Value::String("int64".to_string()),
            optional: Some(true),
            ..DebeziumField::new("", DebeziumSchemaType::Int64)
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_mapping() {
        assert_eq!(
            DebeziumArrowMapper::debezium_type_to_arrow(&DebeziumSchemaType::Int32, None),
            DataType::Int32
        );
        assert_eq!(
            DebeziumArrowMapper::debezium_type_to_arrow(&DebeziumSchemaType::String, None),
            DataType::Utf8
        );
        assert_eq!(
            DebeziumArrowMapper::debezium_type_to_arrow(&DebeziumSchemaType::Boolean, None),
            DataType::Boolean
        );
        assert_eq!(
            DebeziumArrowMapper::debezium_type_to_arrow(&DebeziumSchemaType::Double, None),
            DataType::Float64
        );
    }

    #[test]
    fn test_decimal_mapping() {
        let mut params = HashMap::new();
        params.insert("precision".to_string(), "10".to_string());
        params.insert("scale".to_string(), "2".to_string());
        let arrow_type = DebeziumArrowMapper::debezium_type_to_arrow(
            &DebeziumSchemaType::Decimal,
            Some(&params),
        );
        assert_eq!(arrow_type, DataType::Decimal128(10, 2));
    }

    #[test]
    fn test_field_conversion() {
        let field = DebeziumField::string("name").optional();
        let arrow_field = DebeziumArrowMapper::debezium_field_to_arrow(&field);
        assert_eq!(arrow_field.name(), "name");
        assert_eq!(arrow_field.data_type(), &DataType::Utf8);
        assert!(arrow_field.is_nullable());
    }

    #[test]
    fn test_make_envelope_schema() {
        let columns = vec![
            DebeziumField::int32("id"),
            DebeziumField::string("name").optional(),
        ];

        let envelope =
            DebeziumArrowMapper::make_envelope_schema("opencdc.testdb.public", "users", &columns);

        assert_eq!(
            envelope.name.as_deref(),
            Some("opencdc.testdb.public.users.Envelope")
        );
        let fields = envelope.fields.as_ref().unwrap();
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[0].field_name, "before");
        assert_eq!(fields[1].field_name, "after");
        assert_eq!(fields[2].field_name, "source");
        assert_eq!(fields[3].field_name, "op");
        assert_eq!(fields[4].field_name, "ts_ms");
    }

    #[test]
    fn test_schema_name_generation() {
        assert_eq!(
            DebeziumArrowMapper::schema_name_for_table("opencdc.testdb.public", "users"),
            "opencdc.testdb.public.users"
        );
        assert_eq!(
            DebeziumArrowMapper::envelope_name_for_table("opencdc.testdb.public", "users"),
            "opencdc.testdb.public.users.Envelope"
        );
        assert_eq!(
            DebeziumArrowMapper::key_schema_name_for_table("opencdc.testdb.public", "users"),
            "opencdc.testdb.public.users.Key"
        );
    }

    #[test]
    fn test_debezium_schema_to_arrow() {
        let debezium_schema = DebeziumSchema {
            schema_type: DebeziumSchemaType::Struct,
            fields: Some(vec![
                DebeziumField::int32("id"),
                DebeziumField::string("name").optional(),
            ]),
            name: Some("io.opencdc.test.public.users.Envelope".to_string()),
            ..Default::default()
        };
        let arrow_schema = DebeziumArrowMapper::debezium_schema_to_arrow(&debezium_schema);
        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(arrow_schema.field(1).name(), "name");
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Utf8);
        assert!(arrow_schema.field(1).is_nullable());
        assert_eq!(
            arrow_schema.metadata().get("debezium.name").unwrap(),
            "io.opencdc.test.public.users.Envelope"
        );
    }
}
