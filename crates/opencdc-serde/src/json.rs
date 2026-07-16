use opencdc_core::change_event::ChangeEvent;
use opencdc_core::error::{Error, Result};
use opencdc_core::schema::DebeziumSchema;
use opencdc_core::source_info::SourceInfo;

pub struct DebeziumJsonSerde;

impl DebeziumJsonSerde {
    pub fn serialize(event: &ChangeEvent) -> Result<serde_json::Value> {
        serde_json::to_value(event).map_err(|e| Error::Serialization(e.to_string()))
    }

    pub fn serialize_to_string(event: &ChangeEvent) -> Result<String> {
        serde_json::to_string(event).map_err(|e| Error::Serialization(e.to_string()))
    }

    pub fn serialize_to_string_pretty(event: &ChangeEvent) -> Result<String> {
        serde_json::to_string_pretty(event).map_err(|e| Error::Serialization(e.to_string()))
    }

    pub fn deserialize(value: serde_json::Value) -> Result<ChangeEvent> {
        serde_json::from_value(value).map_err(|e| Error::Deserialization(e.to_string()))
    }

    pub fn deserialize_from_str(s: &str) -> Result<ChangeEvent> {
        serde_json::from_str(s).map_err(|e| Error::Deserialization(e.to_string()))
    }
}

pub struct DebeziumEventBuilder;

impl DebeziumEventBuilder {
    pub fn create_event(
        after: serde_json::Value,
        source: SourceInfo,
        schema: Option<DebeziumSchema>,
    ) -> ChangeEvent {
        let event = ChangeEvent::create(after, source);
        match schema {
            Some(s) => event.with_schema(s),
            None => event,
        }
    }

    pub fn update_event(
        before: Option<serde_json::Value>,
        after: serde_json::Value,
        source: SourceInfo,
        schema: Option<DebeziumSchema>,
    ) -> ChangeEvent {
        let event = ChangeEvent::update(before, after, source);
        match schema {
            Some(s) => event.with_schema(s),
            None => event,
        }
    }

    pub fn delete_event(
        before: serde_json::Value,
        source: SourceInfo,
        schema: Option<DebeziumSchema>,
    ) -> ChangeEvent {
        let event = ChangeEvent::delete(before, source);
        match schema {
            Some(s) => event.with_schema(s),
            None => event,
        }
    }

    pub fn snapshot_event(
        after: serde_json::Value,
        source: SourceInfo,
        schema: Option<DebeziumSchema>,
    ) -> ChangeEvent {
        let event = ChangeEvent::snapshot(after, source);
        match schema {
            Some(s) => event.with_schema(s),
            None => event,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opencdc_core::operation::Operation;
    use opencdc_core::schema::{DebeziumField, DebeziumSchemaType};
    use opencdc_core::ConnectorType;

    #[test]
    fn test_json_serialize_to_string_pretty() {
        let source = SourceInfo::new(&ConnectorType::Postgres, "testdb", Some("public"), "users");
        let after = serde_json::json!({"id": 1});
        let event = ChangeEvent::create(after, source);
        let pretty = DebeziumJsonSerde::serialize_to_string_pretty(&event).unwrap();
        assert!(pretty.contains('\n'));
        assert!(pretty.contains("op"));
        assert!(pretty.contains("postgresql"));
    }

    #[test]
    fn test_json_roundtrip() {
        let source = SourceInfo::new(&ConnectorType::Postgres, "testdb", Some("public"), "users");
        let after = serde_json::json!({"id": 1, "name": "Alice"});
        let event = ChangeEvent::create(after, source);

        let json_str = DebeziumJsonSerde::serialize_to_string(&event).unwrap();
        let deserialized = DebeziumJsonSerde::deserialize_from_str(&json_str).unwrap();

        assert_eq!(event.payload.op, deserialized.payload.op);
        assert_eq!(event.payload.after, deserialized.payload.after);
        assert_eq!(event.payload.source.db, deserialized.payload.source.db);
    }

    #[test]
    fn test_json_with_schema() {
        let source = SourceInfo::new(&ConnectorType::Postgres, "testdb", Some("public"), "users");
        let after = serde_json::json!({"id": 1, "name": "Alice"});
        let schema = DebeziumSchema {
            schema_type: DebeziumSchemaType::Struct,
            fields: Some(vec![
                DebeziumField::int32("id"),
                DebeziumField::string("name").optional(),
            ]),
            name: Some("io.opencdc.testdb.public.users.Envelope".to_string()),
            ..Default::default()
        };
        let event = ChangeEvent::create(after, source).with_schema(schema);

        let json = DebeziumJsonSerde::serialize(&event).unwrap();
        assert!(json.get("schema").is_some());
        assert!(json.get("payload").is_some());
        assert_eq!(json["payload"]["op"], "c");
    }

    #[test]
    fn test_debezium_envelope_format() {
        let source = SourceInfo::new(&ConnectorType::Mysql, "mydb", None::<&str>, "orders")
            .with_lsn(99999);
        let after = serde_json::json!({"order_id": 42, "amount": 100.50});
        let event = DebeziumEventBuilder::create_event(after, source, None);

        let json = DebeziumJsonSerde::serialize(&event).unwrap();
        assert_eq!(json["payload"]["op"], "c");
        assert_eq!(json["payload"]["source"]["connector"], "mysql");
        assert_eq!(json["payload"]["source"]["db"], "mydb");
        assert_eq!(json["payload"]["source"]["table"], "orders");
        assert_eq!(json["payload"]["after"]["order_id"], 42);
        assert!(json.get("schema").is_none());
    }

    #[test]
    fn test_builder_methods() {
        let source = SourceInfo::new(&ConnectorType::Postgres, "db", Some("public"), "t");

        let create = DebeziumEventBuilder::create_event(
            serde_json::json!({"id": 1}),
            source.clone(),
            None,
        );
        assert_eq!(create.payload.op, Operation::Create);

        let update = DebeziumEventBuilder::update_event(
            Some(serde_json::json!({"id": 1})),
            serde_json::json!({"id": 1, "name": "updated"}),
            source.clone(),
            None,
        );
        assert_eq!(update.payload.op, Operation::Update);

        let delete = DebeziumEventBuilder::delete_event(
            serde_json::json!({"id": 1}),
            source.clone(),
            None,
        );
        assert_eq!(delete.payload.op, Operation::Delete);

        let snap = DebeziumEventBuilder::snapshot_event(
            serde_json::json!({"id": 1}),
            source,
            None,
        );
        assert_eq!(snap.payload.op, Operation::Read);
    }
}
