use opencdc_core::error::{Error, Result};
use opencdc_core::schema::{DebeziumField, DebeziumSchema, DebeziumSchemaType};

pub struct AvroSchemaGenerator;

impl AvroSchemaGenerator {
    pub fn debezium_schema_to_avro_json(schema: &DebeziumSchema) -> Result<serde_json::Value> {
        Self::convert_schema(schema, false)
    }

    pub fn table_fields_to_avro_json(
        name: &str,
        fields: &[DebeziumField],
        namespace: Option<&str>,
    ) -> Result<serde_json::Value> {
        let avro_fields: Vec<serde_json::Value> = fields
            .iter()
            .map(Self::convert_field_to_avro)
            .collect::<Result<Vec<_>>>()?;

        let mut record = serde_json::json!({
            "type": "record",
            "name": name,
            "fields": avro_fields,
        });

        if let Some(ns) = namespace {
            record["namespace"] = serde_json::Value::String(ns.to_string());
        }

        Ok(record)
    }

    pub fn make_envelope_avro_schema(
        namespace: &str,
        table: &str,
        columns: &[DebeziumField],
    ) -> Result<serde_json::Value> {
        let table_full_name = format!("{}.{}", namespace, table);
        let column_avro_fields: Vec<serde_json::Value> = columns
            .iter()
            .map(Self::convert_field_to_avro)
            .collect::<Result<Vec<_>>>()?;

        let table_schema = serde_json::json!({
            "type": "record",
            "name": table,
            "namespace": namespace,
            "fields": column_avro_fields,
        });

        let source_fields: Vec<serde_json::Value> = vec![
            serde_json::json!({"field": "version", "type": "string"}),
            serde_json::json!({"field": "connector", "type": "string"}),
            serde_json::json!({"field": "name", "type": "string"}),
            serde_json::json!({"field": "ts_ms", "type": "long"}),
            serde_json::json!({"field": "snapshot", "type": ["null", "string"], "default": null}),
            serde_json::json!({"field": "db", "type": "string"}),
            serde_json::json!({"field": "schema", "type": ["null", "string"], "default": null}),
            serde_json::json!({"field": "table", "type": "string"}),
            serde_json::json!({"field": "txId", "type": ["null", "long"], "default": null}),
            serde_json::json!({"field": "lsn", "type": ["null", "long"], "default": null}),
        ];

        let envelope_fields: Vec<serde_json::Value> = vec![
            serde_json::json!({"field": "before", "type": ["null", table_schema.clone()], "default": null}),
            serde_json::json!({"field": "after", "type": ["null", table_schema], "default": null}),
            serde_json::json!({"field": "source", "type": {
                "type": "record",
                "name": "Source",
                "namespace": format!("io.debezium.connector.{}", namespace.split('.').next().unwrap_or("common")),
                "fields": source_fields,
            }}),
            serde_json::json!({"field": "op", "type": "string"}),
            serde_json::json!({"field": "ts_ms", "type": ["null", "long"], "default": null}),
            serde_json::json!({"field": "transaction", "type": ["null", {
                "type": "record",
                "name": "Transaction",
                "namespace": table_full_name,
                "fields": [
                    {"field": "id", "type": "string"},
                    {"field": "total_order", "type": "long"},
                    {"field": "data_collections_order", "type": "long"}
                ]
            }], "default": null}),
        ];

        Ok(serde_json::json!({
            "type": "record",
            "name": "Envelope",
            "namespace": table_full_name,
            "fields": envelope_fields,
        }))
    }

    fn convert_schema(schema: &DebeziumSchema, force_nullable: bool) -> Result<serde_json::Value> {
        match schema.schema_type {
            DebeziumSchemaType::Struct => {
                let fields = schema.fields.as_ref().ok_or_else(|| {
                    Error::Schema("struct schema has no fields".to_string())
                })?;
                let avro_fields: Vec<serde_json::Value> = fields
                    .iter()
                    .map(Self::convert_field_to_avro)
                    .collect::<Result<Vec<_>>>()?;

                let mut avro_type = serde_json::json!({
                    "type": "record",
                    "fields": avro_fields,
                });
                if let Some(name) = &schema.name {
                    avro_type["name"] = serde_json::Value::String(name.clone());
                }
                Ok(avro_type)
            }
            DebeziumSchemaType::Array => {
                let items = schema.fields.as_ref().and_then(|f| f.first()).ok_or_else(|| {
                    Error::Schema("array schema has no item field".to_string())
                })?;
                let item_type = Self::convert_field_to_avro(items)?;
                Ok(serde_json::json!({"type": "array", "items": item_type}))
            }
            DebeziumSchemaType::Map => {
                Ok(serde_json::json!({"type": "map", "values": "string"}))
            }
            ref t => Ok(Self::primitive_type_to_avro(t, force_nullable)),
        }
    }

    fn convert_field_to_avro(field: &DebeziumField) -> Result<serde_json::Value> {
        let dt = field.resolve_type();
        let optional = field.optional.unwrap_or(false);

        let avro_type = match dt {
            DebeziumSchemaType::Struct => {
                let nested_fields = field.fields.as_ref().ok_or_else(|| {
                    Error::Schema(format!("struct field '{}' has no fields", field.field_name))
                })?;
                let avro_nested: Vec<serde_json::Value> = nested_fields
                    .iter()
                    .map(Self::convert_field_to_avro)
                    .collect::<Result<Vec<_>>>()?;
                let mut type_val = serde_json::json!({
                    "type": "record",
                    "fields": avro_nested,
                });
                if let Some(name) = &field.name {
                    type_val["name"] = serde_json::Value::String(name.clone());
                }
                type_val
            }
            DebeziumSchemaType::Array => {
                serde_json::json!({"type": "array", "items": "string"})
            }
            DebeziumSchemaType::Map => {
                serde_json::json!({"type": "map", "values": "string"})
            }
            DebeziumSchemaType::Decimal => {
                let mut avro_type = serde_json::json!({
                    "type": "bytes",
                    "logicalType": "decimal",
                });
                if let Some(params) = &field.parameters {
                    if let Some(precision) = params.get("precision") {
                        avro_type["precision"] =
                            serde_json::Value::String(precision.clone());
                    }
                    if let Some(scale) = params.get("scale") {
                        avro_type["scale"] = serde_json::Value::String(scale.clone());
                    }
                }
                avro_type
            }
            DebeziumSchemaType::Timestamp | DebeziumSchemaType::MicroTimestamp => {
                serde_json::json!({"type": "long", "logicalType": "timestamp-micros"})
            }
            DebeziumSchemaType::NanoTimestamp => {
                serde_json::json!({"type": "long", "logicalType": "timestamp-nanos"})
            }
            DebeziumSchemaType::Date => {
                serde_json::json!({"type": "int", "logicalType": "date"})
            }
            DebeziumSchemaType::Time | DebeziumSchemaType::MicroTime => {
                serde_json::json!({"type": "long", "logicalType": "time-micros"})
            }
            DebeziumSchemaType::NanoTime => {
                serde_json::json!({"type": "long", "logicalType": "time-nanos"})
            }
            _ => Self::primitive_type_to_avro(&dt, false),
        };

        if optional {
            Ok(serde_json::json!([avro_type, "null"]))
        } else {
            Ok(serde_json::json!({"field": field.field_name, "type": avro_type}))
        }
    }

    fn primitive_type_to_avro(t: &DebeziumSchemaType, _force_nullable: bool) -> serde_json::Value {
        match t {
            DebeziumSchemaType::String | DebeziumSchemaType::Json => {
                serde_json::Value::String("string".to_string())
            }
            DebeziumSchemaType::Bytes => serde_json::Value::String("bytes".to_string()),
            DebeziumSchemaType::Int8 | DebeziumSchemaType::Int16 | DebeziumSchemaType::Int32 => {
                serde_json::Value::String("int".to_string())
            }
            DebeziumSchemaType::Int64 => serde_json::Value::String("long".to_string()),
            DebeziumSchemaType::Float => serde_json::Value::String("float".to_string()),
            DebeziumSchemaType::Double => serde_json::Value::String("double".to_string()),
            DebeziumSchemaType::Boolean => serde_json::Value::String("boolean".to_string()),
            _ => serde_json::Value::String("string".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debezium_schema_to_avro_json() {
        let schema = opencdc_core::schema::DebeziumSchema {
            schema_type: opencdc_core::schema::DebeziumSchemaType::Struct,
            optional: Some(false),
            name: Some("io.opencdc.testdb.public.users.Envelope".to_string()),
            fields: Some(vec![
                opencdc_core::schema::DebeziumField::int32("id"),
                opencdc_core::schema::DebeziumField::string("name").optional(),
            ]),
            ..Default::default()
        };
        let avro = AvroSchemaGenerator::debezium_schema_to_avro_json(&schema).unwrap();
        assert_eq!(avro["type"], "record");
        assert_eq!(avro["name"], "io.opencdc.testdb.public.users.Envelope");
        assert!(avro["fields"].is_array());
    }

    #[test]
    fn test_debezium_schema_to_avro_json_primitive() {
        let schema = opencdc_core::schema::DebeziumSchema {
            schema_type: opencdc_core::schema::DebeziumSchemaType::Int32,
            ..Default::default()
        };
        let avro = AvroSchemaGenerator::debezium_schema_to_avro_json(&schema).unwrap();
        // A primitive Int32 schema maps to "int" string
        assert_eq!(avro, "int");
    }

    #[test]
    fn test_primitive_field_to_avro() {
        let fields = vec![DebeziumField::int32("id"), DebeziumField::string("name")];
        let avro = AvroSchemaGenerator::table_fields_to_avro_json(
            "users",
            &fields,
            Some("io.opencdc.testdb.public"),
        )
        .unwrap();
        assert_eq!(avro["name"], "users");
        assert_eq!(avro["namespace"], "io.opencdc.testdb.public");
        assert_eq!(avro["fields"][0]["field"], "id");
    }

    #[test]
    fn test_envelope_avro_schema() {
        let columns = vec![
            DebeziumField::int32("id"),
            DebeziumField::string("name").optional(),
        ];
        let avro = AvroSchemaGenerator::make_envelope_avro_schema(
            "io.opencdc.testdb.public",
            "users",
            &columns,
        )
        .unwrap();
        assert_eq!(avro["name"], "Envelope");
        assert_eq!(avro["namespace"], "io.opencdc.testdb.public.users");
        let fields = avro["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 6);
        assert_eq!(fields[0]["field"], "before");
        assert_eq!(fields[3]["field"], "op");
    }
}
