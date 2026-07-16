use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use opencdc_core::change_event::ChangeEvent;
use opencdc_core::error::{Error, Result};
use opencdc_core::operation::Operation;

pub struct BatchConverter;

impl BatchConverter {
    pub fn events_to_batch(events: &[ChangeEvent], schema: &Schema) -> Result<RecordBatch> {
        let num_rows = events.len();
        if num_rows == 0 {
            return Err(Error::Conversion("empty events slice".to_string()));
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        for field in schema.fields() {
            let col_data: Vec<Option<String>> = events
                .iter()
                .map(|event| match field.name().as_str() {
                    "op" => Some(event.payload.op.as_str().to_string()),
                    "ts_ms" => event.payload.ts_ms.map(|v| v.to_string()),
                    "db" => Some(event.payload.source.db.clone()),
                    "schema" => event.payload.source.schema.clone(),
                    "table" => Some(event.payload.source.table.clone()),
                    "connector" => Some(event.payload.source.connector.clone()),
                    "lsn" => event.payload.source.lsn.map(|v| v.to_string()),
                    "txId" => event.payload.source.tx_id.map(|v| v.to_string()),
                    "snapshot" => Some(event.payload.source.snapshot.as_str().to_string()),
                    _ => None,
                })
                .collect();

            let array: ArrayRef = Arc::new(
                col_data.iter().map(|v| v.as_deref()).collect::<StringArray>(),
            );
            columns.push(array);
        }

        RecordBatch::try_new(Arc::new(schema.clone()), columns)
            .map_err(Error::Arrow)
    }

    pub fn batch_to_events(batch: &RecordBatch) -> Result<Vec<ChangeEvent>> {
        let num_rows = batch.num_rows();
        let mut events = Vec::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            let op = batch
                .column_by_name("op")
                .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                .and_then(|arr| Operation::from_str(arr.value(row_idx)))
                .unwrap_or(Operation::Read);

            let ts_ms = batch
                .column_by_name("ts_ms")
                .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
                .map(|arr| arr.value(row_idx));

            let source = opencdc_core::source_info::SourceInfo {
                db: extract_string_col(batch, "db", row_idx)
                    .unwrap_or_default(),
                schema: extract_string_col(batch, "schema", row_idx),
                table: extract_string_col(batch, "table", row_idx)
                    .unwrap_or_default(),
                connector: extract_string_col(batch, "connector", row_idx)
                    .unwrap_or_default(),
                lsn: extract_int_col(batch, "lsn", row_idx),
                tx_id: extract_int_col(batch, "txId", row_idx),
                ..Default::default()
            };

            events.push(ChangeEvent::new(opencdc_core::change_event::ChangePayload {
                before: None,
                after: None,
                source,
                op,
                ts_ms,
                transaction: None,
            }));
        }

        Ok(events)
    }
}

fn extract_string_col(batch: &RecordBatch, name: &str, row: usize) -> Option<String> {
    batch
        .column_by_name(name)
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .map(|arr| arr.value(row).to_string())
}

fn extract_int_col(batch: &RecordBatch, name: &str, row: usize) -> Option<i64> {
    batch
        .column_by_name(name)
        .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
        .map(|arr| arr.value(row))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use opencdc_core::ConnectorType;

    #[test]
    fn test_events_to_batch_roundtrip() {
        let source = opencdc_core::source_info::SourceInfo::new(
            &ConnectorType::Postgres,
            "testdb",
            Some("public"),
            "users",
        );
        let events = vec![
            ChangeEvent::create(serde_json::json!({"id": 1, "name": "Alice"}), source.clone()),
            ChangeEvent::create(serde_json::json!({"id": 2, "name": "Bob"}), source),
        ];

        let fields = vec![
            Field::new("op", DataType::Utf8, false),
            Field::new("db", DataType::Utf8, false),
            Field::new("table", DataType::Utf8, false),
        ];
        let schema = Schema::new(fields);

        let batch = BatchConverter::events_to_batch(&events, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let recovered = BatchConverter::batch_to_events(&batch).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].payload.op, Operation::Create);
        assert_eq!(recovered[0].payload.source.db, "testdb");
    }
}
