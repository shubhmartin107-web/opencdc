use serde::{Deserialize, Serialize};

use crate::operation::Operation;
use crate::schema::DebeziumSchema;
use crate::source_info::SourceInfo;
use crate::transaction::TransactionInfo;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangeEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<DebeziumSchema>,

    pub payload: ChangePayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangePayload {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub before: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub after: Option<serde_json::Value>,

    pub source: SourceInfo,

    pub op: Operation,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_ms: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction: Option<TransactionInfo>,
}

impl ChangeEvent {
    pub fn new(payload: ChangePayload) -> Self {
        Self {
            schema: None,
            payload,
        }
    }

    pub fn with_schema(mut self, schema: DebeziumSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn create(after: serde_json::Value, source: SourceInfo) -> Self {
        Self::new(ChangePayload {
            before: None,
            after: Some(after),
            source,
            op: Operation::Create,
            ts_ms: Some(chrono::Utc::now().timestamp_millis()),
            transaction: None,
        })
    }

    pub fn update(
        before: Option<serde_json::Value>,
        after: serde_json::Value,
        source: SourceInfo,
    ) -> Self {
        Self::new(ChangePayload {
            before,
            after: Some(after),
            source,
            op: Operation::Update,
            ts_ms: Some(chrono::Utc::now().timestamp_millis()),
            transaction: None,
        })
    }

    pub fn delete(before: serde_json::Value, source: SourceInfo) -> Self {
        Self::new(ChangePayload {
            before: Some(before),
            after: None,
            source,
            op: Operation::Delete,
            ts_ms: Some(chrono::Utc::now().timestamp_millis()),
            transaction: None,
        })
    }

    pub fn snapshot(after: serde_json::Value, source: SourceInfo) -> Self {
        Self::new(ChangePayload {
            before: None,
            after: Some(after),
            source,
            op: Operation::Read,
            ts_ms: Some(chrono::Utc::now().timestamp_millis()),
            transaction: None,
        })
    }

    pub fn table_name(&self) -> &str {
        &self.payload.source.table
    }

    pub fn db_name(&self) -> &str {
        &self.payload.source.db
    }

    pub fn schema_name(&self) -> Option<&str> {
        self.payload.source.schema.as_deref()
    }

    pub fn fully_qualified_table(&self) -> String {
        match &self.payload.source.schema {
            Some(schema) => format!(
                "{}.{}.{}",
                self.payload.source.db, schema, self.payload.source.table
            ),
            None => format!("{}.{}", self.payload.source.db, self.payload.source.table),
        }
    }

    pub fn topic_name(&self, prefix: &str) -> String {
        match &self.payload.source.schema {
            Some(schema) => format!("{}.{}.{}", prefix, schema, self.payload.source.table),
            None => format!("{}.{}", prefix, self.payload.source.table),
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.payload.op == Operation::Delete && self.payload.after.is_none()
    }

    pub fn is_heartbeat(&self) -> bool {
        self.payload.source.table.is_empty() || self.payload.source.table == "__heartbeat__"
    }

    pub fn event_timestamp_ms(&self) -> i64 {
        self.payload
            .ts_ms
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis())
    }
}

impl ChangePayload {
    pub fn new(op: Operation, source: SourceInfo) -> Self {
        Self {
            before: None,
            after: None,
            source,
            op,
            ts_ms: Some(chrono::Utc::now().timestamp_millis()),
            transaction: None,
        }
    }

    pub fn with_before(mut self, before: serde_json::Value) -> Self {
        self.before = Some(before);
        self
    }

    pub fn with_after(mut self, after: serde_json::Value) -> Self {
        self.after = Some(after);
        self
    }

    pub fn with_ts_ms(mut self, ts_ms: i64) -> Self {
        self.ts_ms = Some(ts_ms);
        self
    }

    pub fn with_transaction(mut self, txn: TransactionInfo) -> Self {
        self.transaction = Some(txn);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConnectorType;
    use crate::SnapshotPhase;

    fn make_source() -> SourceInfo {
        SourceInfo::new(&ConnectorType::Postgres, "testdb", Some("public"), "users")
            .with_snapshot(SnapshotPhase::False)
            .with_lsn(12345)
    }

    #[test]
    fn test_create_event() {
        let after = serde_json::json!({"id": 1, "name": "Alice"});
        let event = ChangeEvent::create(after.clone(), make_source());
        assert_eq!(event.payload.op, Operation::Create);
        assert!(event.payload.before.is_none());
        assert_eq!(event.payload.after, Some(after));
        assert!(!event.is_tombstone());
    }

    #[test]
    fn test_update_event() {
        let before = serde_json::json!({"id": 1, "name": "Alice"});
        let after = serde_json::json!({"id": 1, "name": "Bob"});
        let event = ChangeEvent::update(Some(before.clone()), after.clone(), make_source());
        assert_eq!(event.payload.op, Operation::Update);
        assert_eq!(event.payload.before, Some(before));
        assert_eq!(event.payload.after, Some(after));
    }

    #[test]
    fn test_delete_event() {
        let before = serde_json::json!({"id": 1, "name": "Alice"});
        let event = ChangeEvent::delete(before.clone(), make_source());
        assert_eq!(event.payload.op, Operation::Delete);
        assert!(event.payload.after.is_none());
        assert!(event.is_tombstone());
    }

    #[test]
    fn test_snapshot_event() {
        let after = serde_json::json!({"id": 1, "name": "Alice"});
        let source = make_source().with_snapshot(SnapshotPhase::True);
        let event = ChangeEvent::snapshot(after, source);
        assert_eq!(event.payload.op, Operation::Read);
        assert_eq!(event.payload.source.snapshot, SnapshotPhase::True);
    }

    #[test]
    fn test_topic_name() {
        let event = ChangeEvent::create(serde_json::json!({"id": 1}), make_source());
        assert_eq!(event.topic_name("opencdc"), "opencdc.public.users");
    }

    #[test]
    fn test_fully_qualified_table() {
        let event = ChangeEvent::create(serde_json::json!({"id": 1}), make_source());
        assert_eq!(event.fully_qualified_table(), "testdb.public.users");
    }

    #[test]
    fn test_event_roundtrip() {
        let after = serde_json::json!({"id": 1, "name": "Alice", "email": "alice@example.com"});
        let event = ChangeEvent::create(after, make_source());
        let json = serde_json::to_value(&event).unwrap();
        let deserialized: ChangeEvent = serde_json::from_value(json).unwrap();
        assert_eq!(event.payload.op, deserialized.payload.op);
        assert_eq!(event.payload.after, deserialized.payload.after);
        assert_eq!(event.payload.source.db, deserialized.payload.source.db);
        assert_eq!(event.payload.source.lsn, deserialized.payload.source.lsn);
    }

    #[test]
    fn test_event_with_schema() {
        let after = serde_json::json!({"id": 1, "name": "Alice"});
        let schema = crate::DebeziumSchema {
            schema_type: crate::DebeziumSchemaType::Struct,
            fields: Some(vec![
                crate::DebeziumField::int32("id"),
                crate::DebeziumField::string("name").optional(),
            ]),
            name: Some("io.opencdc.testdb.public.users.Envelope".to_string()),
            ..Default::default()
        };
        let event = ChangeEvent::create(after, make_source()).with_schema(schema);
        assert!(event.schema.is_some());
        let json = serde_json::to_value(&event).unwrap();
        assert!(json.get("schema").is_some());
        assert!(json.get("payload").is_some());
    }
}
