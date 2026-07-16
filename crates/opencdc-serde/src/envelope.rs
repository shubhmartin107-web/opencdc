use opencdc_core::change_event::{ChangeEvent, ChangePayload};
use opencdc_core::schema::{DebeziumField, DebeziumSchema, DebeziumSchemaType};

pub struct EnvelopeBuilder;

impl EnvelopeBuilder {
    pub fn new_event(payload: ChangePayload) -> ChangeEvent {
        ChangeEvent::new(payload)
    }

    pub fn build_envelope_schema(
        connector_name: &str,
        catalog: &str,
        schema: Option<&str>,
        table: &str,
        columns: &[DebeziumField],
    ) -> DebeziumSchema {
        let namespace = match schema {
            Some(s) => format!("{}.{}.{}", connector_name, catalog, s),
            None => format!("{}.{}", connector_name, catalog),
        };
        let table_schema_name = format!("{}.{}", namespace, table);

        let mk_fields = |cols: &[DebeziumField]| -> Vec<DebeziumField> {
            cols.iter()
                .map(|f| {
                    let mut field = f.clone();
                    field.optional = Some(true);
                    field.name = Some(format!("{}.{}", table_schema_name, f.field_name));
                    field
                })
                .collect()
        };

        DebeziumSchema {
            schema_type: DebeziumSchemaType::Struct,
            fields: Some(vec![
                DebeziumField {
                    field_name: "before".to_string(),
                    field_type: serde_json::Value::String("struct".to_string()),
                    fields: Some(mk_fields(columns)),
                    optional: Some(true),
                    name: Some(table_schema_name.clone()),
                    ..Default::default()
                },
                DebeziumField {
                    field_name: "after".to_string(),
                    field_type: serde_json::Value::String("struct".to_string()),
                    fields: Some(mk_fields(columns)),
                    optional: Some(true),
                    name: Some(table_schema_name.clone()),
                    ..Default::default()
                },
                DebeziumField {
                    field_name: "source".to_string(),
                    field_type: serde_json::Value::String("struct".to_string()),
                    fields: Some(build_source_schema_fields()),
                    optional: Some(false),
                    name: Some(format!(
                        "io.debezium.connector.{}.Source",
                        connector_name
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
                DebeziumField {
                    field_name: "transaction".to_string(),
                    field_type: serde_json::Value::String("struct".to_string()),
                    fields: Some(vec![
                        DebeziumField::string("id"),
                        DebeziumField::int64("total_order"),
                        DebeziumField::int64("data_collections_order"),
                    ]),
                    optional: Some(true),
                    name: Some(format!("{}.Envelope", table_schema_name)),
                    ..Default::default()
                },
            ]),
            optional: Some(false),
            name: Some(format!("{}.Envelope", table_schema_name)),
            version: Some(1),
            ..Default::default()
        }
    }
}

fn build_source_schema_fields() -> Vec<DebeziumField> {
    vec![
        DebeziumField::string("version"),
        DebeziumField::string("connector"),
        DebeziumField::string("name"),
        DebeziumField::int64("ts_ms"),
        DebeziumField::string("snapshot").optional(),
        DebeziumField::string("db"),
        DebeziumField::string("schema").optional(),
        DebeziumField::string("table"),
        DebeziumField::int64("txId").optional(),
        DebeziumField::int64("lsn").optional(),
        DebeziumField::string("file").optional(),
        DebeziumField::int64("pos").optional(),
        DebeziumField::string("gtid").optional(),
        DebeziumField::string("resume_token").optional(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_event_creates_change_event() {
        let payload = opencdc_core::change_event::ChangePayload {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            source: opencdc_core::source_info::SourceInfo::new(
                &opencdc_core::ConnectorType::Postgres,
                "db",
                Some("public"),
                "t1",
            ),
            op: opencdc_core::operation::Operation::Create,
            ts_ms: None,
            transaction: None,
        };
        let event = EnvelopeBuilder::new_event(payload);
        assert_eq!(event.payload.op, opencdc_core::operation::Operation::Create);
        assert_eq!(event.payload.after, Some(serde_json::json!({"id": 1})));
    }

    #[test]
    fn test_build_envelope_schema() {
        let columns = vec![
            DebeziumField::int32("id"),
            DebeziumField::string("name").optional(),
        ];
        let schema = EnvelopeBuilder::build_envelope_schema(
            "opencdc", "testdb", Some("public"), "users", &columns,
        );
        assert_eq!(
            schema.name.as_deref(),
            Some("opencdc.testdb.public.users.Envelope")
        );
        let fields = schema.fields.as_ref().unwrap();
        assert_eq!(fields.len(), 6);
        assert_eq!(fields[0].field_name, "before");
        assert_eq!(fields[1].field_name, "after");
        assert_eq!(fields[2].field_name, "source");
        assert_eq!(fields[3].field_name, "op");
        assert_eq!(fields[4].field_name, "ts_ms");
        assert_eq!(fields[5].field_name, "transaction");
    }
}
