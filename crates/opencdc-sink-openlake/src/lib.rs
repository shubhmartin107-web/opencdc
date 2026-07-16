pub mod persistent;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use opencdc_core::change_event::ChangeEvent;
use opencdc_pipeline::error::PipelineResult;
use opencdc_pipeline::sink::Sink;

use openlake_core::types::TableIdentifier;
use openlake_query::store::TableStore;

pub struct OpenLakeSink {
    name: String,
    store: TableStore,
    registered: Arc<RwLock<HashMap<String, bool>>>,
}

impl OpenLakeSink {
    pub fn new(name: impl Into<String>, store: TableStore) -> Self {
        Self {
            name: name.into(),
            store,
            registered: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn table_key(event: &ChangeEvent) -> String {
        format!("{}.{}", event.payload.source.db, event.payload.source.table)
    }

    fn make_table_id(event: &ChangeEvent) -> TableIdentifier {
        TableIdentifier::new(
            &event.payload.source.db,
            &event.payload.source.table,
        )
    }

    fn make_schema(_event: &ChangeEvent) -> Schema {
        Schema::new(vec![
            Field::new("op", DataType::Utf8, false),
            Field::new("ts_ms", DataType::Int64, true),
            Field::new("db", DataType::Utf8, false),
            Field::new("schema", DataType::Utf8, true),
            Field::new("table", DataType::Utf8, false),
            Field::new("connector", DataType::Utf8, false),
            Field::new("lsn", DataType::Int64, true),
            Field::new("txId", DataType::Int64, true),
            Field::new("snapshot", DataType::Utf8, false),
            Field::new("before", DataType::Utf8, true),
            Field::new("after", DataType::Utf8, true),
        ])
    }

    fn events_to_batch(events: &[ChangeEvent]) -> RecordBatch {
        let n = events.len();
        let schema = Self::make_schema(&events[0]);

        let mut op_col = Vec::with_capacity(n);
        let mut ts_ms_col = Vec::with_capacity(n);
        let mut db_col = Vec::with_capacity(n);
        let mut schema_col = Vec::with_capacity(n);
        let mut table_col = Vec::with_capacity(n);
        let mut connector_col = Vec::with_capacity(n);
        let mut lsn_col = Vec::with_capacity(n);
        let mut tx_id_col = Vec::with_capacity(n);
        let mut snapshot_col = Vec::with_capacity(n);
        let mut before_col = Vec::with_capacity(n);
        let mut after_col = Vec::with_capacity(n);

        for event in events {
            op_col.push(Some(event.payload.op.as_str()));
            ts_ms_col.push(event.payload.ts_ms);
            db_col.push(Some(event.payload.source.db.as_str()));
            schema_col.push(event.payload.source.schema.as_deref());
            table_col.push(Some(event.payload.source.table.as_str()));
            connector_col.push(Some(event.payload.source.connector.as_str()));
            lsn_col.push(event.payload.source.lsn);
            tx_id_col.push(event.payload.source.tx_id);
            snapshot_col.push(Some(event.payload.source.snapshot.as_str()));

            before_col.push(
                event
                    .payload
                    .before
                    .as_ref()
                    .map(|v| serde_json::to_string(v).unwrap_or_else(|e| {
                        tracing::warn!("failed to serialize before value: {}", e);
                        String::new()
                    })),
            );
            after_col.push(
                event
                    .payload
                    .after
                    .as_ref()
                    .map(|v| serde_json::to_string(v).unwrap_or_else(|e| {
                        tracing::warn!("failed to serialize after value: {}", e);
                        String::new()
                    })),
            );
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(op_col)),
            Arc::new(Int64Array::from(ts_ms_col)),
            Arc::new(StringArray::from(db_col)),
            Arc::new(StringArray::from(schema_col)),
            Arc::new(StringArray::from(table_col)),
            Arc::new(StringArray::from(connector_col)),
            Arc::new(Int64Array::from(lsn_col)),
            Arc::new(Int64Array::from(tx_id_col)),
            Arc::new(StringArray::from(snapshot_col)),
            Arc::new(StringArray::from(before_col)),
            Arc::new(StringArray::from(after_col)),
        ];

        RecordBatch::try_new(Arc::new(schema), columns)
            .expect("OpenLakeSink: RecordBatch creation failed")
    }

    fn ensure_table(&self, event: &ChangeEvent) {
        let key = Self::table_key(event);
        let mut reg = self.registered.write().expect("lock poisoned");
        if reg.contains_key(&key) {
            return;
        }
        let schema = Self::make_schema(event);
        let table_id = Self::make_table_id(event);
        self.store.register(&table_id, Arc::new(schema));
        reg.insert(key, true);
    }
}

#[async_trait]
impl Sink for OpenLakeSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn write(&mut self, events: &[ChangeEvent]) -> PipelineResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let key = Self::table_key(&events[0]);
        self.ensure_table(&events[0]);

        let batch = Self::events_to_batch(events);
        let table_id = Self::make_table_id(&events[0]);

        self.store.append(&table_id, vec![batch]).map_err(|e| {
            opencdc_pipeline::error::PipelineError::Sink(format!(
                "OpenLake append failed for {key}: {e}"
            ))
        })?;

        tracing::debug!(table = %key, count = events.len(), "wrote events to OpenLake");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opencdc_core::source_info::SourceInfo;
    use opencdc_core::ConnectorType;

    #[tokio::test]
    async fn test_openlake_sink_writes_events() {
        let store = TableStore::new();
        let mut sink = OpenLakeSink::new("test", store.clone());

        let source = SourceInfo::new(&ConnectorType::Postgres, "testdb", Some("public"), "users");
        let events = vec![
            ChangeEvent::create(serde_json::json!({"id": 1, "name": "Alice"}), source.clone()),
            ChangeEvent::create(serde_json::json!({"id": 2, "name": "Bob"}), source),
        ];

        sink.write(&events).await.unwrap();

        let table_id = TableIdentifier::new("testdb", "users");
        let entry = store.get(&table_id).unwrap();
        assert_eq!(entry.batches.len(), 1);
        assert_eq!(entry.batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_openlake_sink_handles_empty_events() {
        let store = TableStore::new();
        let mut sink = OpenLakeSink::new("test", store.clone());
        sink.write(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn test_openlake_sink_multiple_tables() {
        let store = TableStore::new();
        let mut sink = OpenLakeSink::new("test", store.clone());

        let users_source =
            SourceInfo::new(&ConnectorType::Postgres, "testdb", Some("public"), "users");
        let orders_source =
            SourceInfo::new(&ConnectorType::Postgres, "testdb", Some("public"), "orders");

        sink.write(&[ChangeEvent::create(
            serde_json::json!({"id": 1}),
            users_source,
        )])
        .await
        .unwrap();

        sink.write(&[ChangeEvent::create(
            serde_json::json!({"id": 100}),
            orders_source,
        )])
        .await
        .unwrap();

        let users_id = TableIdentifier::new("testdb", "users");
        let orders_id = TableIdentifier::new("testdb", "orders");

        assert_eq!(store.get(&users_id).unwrap().batches[0].num_rows(), 1);
        assert_eq!(store.get(&orders_id).unwrap().batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_openlake_sink_roundtrip_before_after() {
        let store = TableStore::new();
        let mut sink = OpenLakeSink::new("test", store.clone());

        let source = SourceInfo::new(&ConnectorType::Postgres, "db", Some("public"), "t");

        let update_event = ChangeEvent::update(
            Some(serde_json::json!({"id": 1, "old_val": "old"})),
            serde_json::json!({"id": 1, "new_val": "new"}),
            source,
        );

        sink.write(&[update_event]).await.unwrap();

        let table_id = TableIdentifier::new("db", "t");
        let entry = store.get(&table_id).unwrap();
        let batch = &entry.batches[0];

        let before_col = batch
            .column_by_name("before")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let after_col = batch
            .column_by_name("after")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert!(before_col.value(0).contains("old_val"));
        assert!(after_col.value(0).contains("new_val"));
    }

    #[tokio::test]
    async fn test_openlake_sink_schema_registration() {
        let store = TableStore::new();
        let mut sink = OpenLakeSink::new("test", store.clone());

        let source = SourceInfo::new(&ConnectorType::Postgres, "db", None::<&str>, "t");
        sink.write(&[ChangeEvent::create(
            serde_json::json!({"x": 1}),
            source,
        )])
        .await
        .unwrap();

        let table_id = TableIdentifier::new("db", "t");
        let entry = store.get(&table_id).unwrap();
        let field_names: Vec<&str> = entry
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert!(field_names.contains(&"op"));
        assert!(field_names.contains(&"before"));
        assert!(field_names.contains(&"after"));
        assert!(field_names.contains(&"ts_ms"));
    }
}
