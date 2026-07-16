use async_trait::async_trait;

use opencdc_core::change_event::ChangeEvent;
use opencdc_core::operation::Operation;

use crate::error::PipelineResult;

#[async_trait]
pub trait Transform: Send + Sync {
    fn name(&self) -> &str;

    async fn transform(&self, event: ChangeEvent) -> PipelineResult<Option<ChangeEvent>>;
}

pub struct FilterTransform {
    name: String,
    include_ops: Vec<Operation>,
}

impl FilterTransform {
    pub fn new(name: impl Into<String>, include_ops: Vec<Operation>) -> Self {
        Self {
            name: name.into(),
            include_ops,
        }
    }

    pub fn only_create() -> Self {
        Self::new("only_create", vec![Operation::Create])
    }

    pub fn only_dml() -> Self {
        Self::new(
            "only_dml",
            vec![Operation::Create, Operation::Update, Operation::Delete],
        )
    }

    pub fn exclude_snapshot() -> Self {
        Self::new(
            "exclude_snapshot",
            vec![
                Operation::Create,
                Operation::Update,
                Operation::Delete,
                Operation::Truncate,
                Operation::Message,
            ],
        )
    }
}

#[async_trait]
impl Transform for FilterTransform {
    fn name(&self) -> &str {
        &self.name
    }

    async fn transform(&self, event: ChangeEvent) -> PipelineResult<Option<ChangeEvent>> {
        if self.include_ops.contains(&event.payload.op) {
            Ok(Some(event))
        } else {
            Ok(None)
        }
    }
}

pub struct RenameTransform {
    name: String,
    table_map: Vec<(String, String)>,
    db_map: Vec<(String, String)>,
}

impl RenameTransform {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table_map: Vec::new(),
            db_map: Vec::new(),
        }
    }

    pub fn remap_table(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.table_map.push((from.into(), to.into()));
        self
    }

    pub fn remap_database(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.db_map.push((from.into(), to.into()));
        self
    }
}

#[async_trait]
impl Transform for RenameTransform {
    fn name(&self) -> &str {
        &self.name
    }

    async fn transform(&self, mut event: ChangeEvent) -> PipelineResult<Option<ChangeEvent>> {
        for (from, to) in &self.table_map {
            if &event.payload.source.table == from {
                event.payload.source.table = to.clone();
            }
        }
        for (from, to) in &self.db_map {
            if event.payload.source.db == *from {
                event.payload.source.db = to.clone();
            }
        }
        Ok(Some(event))
    }
}

pub struct LogTransform {
    name: String,
}

impl LogTransform {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
        }
    }
}

#[async_trait]
impl Transform for LogTransform {
    fn name(&self) -> &str {
        &self.name
    }

    async fn transform(&self, event: ChangeEvent) -> PipelineResult<Option<ChangeEvent>> {
        tracing::debug!(
            "event: op={:?} table={} db={}",
            event.payload.op,
            event.payload.source.table,
            event.payload.source.db,
        );
        Ok(Some(event))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opencdc_core::source_info::SourceInfo;
    use opencdc_core::ConnectorType;

    fn make_event(op: Operation, table: &str, db: &str) -> ChangeEvent {
        let source = SourceInfo::new(&ConnectorType::Postgres, db, Some("public"), table);
        ChangeEvent::new(opencdc_core::change_event::ChangePayload {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            source,
            op,
            ts_ms: None,
            transaction: None,
        })
    }

    #[tokio::test]
    async fn test_filter_keeps_matching_ops() {
        let transform = FilterTransform::only_dml();
        let create = make_event(Operation::Create, "t", "db");
        let read = make_event(Operation::Read, "t", "db");

        assert!(transform.transform(create).await.unwrap().is_some());
        assert!(transform.transform(read).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_filter_exclude_snapshot() {
        let transform = FilterTransform::exclude_snapshot();
        assert!(transform
            .transform(make_event(Operation::Create, "t", "db"))
            .await
            .unwrap()
            .is_some());
        assert!(transform
            .transform(make_event(Operation::Read, "t", "db"))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_rename_transform_remaps_table() {
        let transform = RenameTransform::new("rename").remap_table("old_t", "new_t");
        let event = make_event(Operation::Create, "old_t", "db");

        let result = transform.transform(event).await.unwrap().unwrap();
        assert_eq!(result.payload.source.table, "new_t");
        assert_eq!(result.payload.source.db, "db");
    }

    #[tokio::test]
    async fn test_rename_transform_remaps_database() {
        let transform = RenameTransform::new("rename").remap_database("old_db", "new_db");
        let event = make_event(Operation::Update, "t", "old_db");

        let result = transform.transform(event).await.unwrap().unwrap();
        assert_eq!(result.payload.source.db, "new_db");
    }

    #[tokio::test]
    async fn test_rename_no_match_passes_through() {
        let transform = RenameTransform::new("rename").remap_table("a", "b");
        let event = make_event(Operation::Delete, "c", "db");

        let result = transform.transform(event).await.unwrap().unwrap();
        assert_eq!(result.payload.source.table, "c");
    }

    #[tokio::test]
    async fn test_log_transform_does_not_drop_events() {
        let transform = LogTransform::new("logger");
        let event = make_event(Operation::Create, "t", "db");

        let result = transform.transform(event).await.unwrap();
        assert!(result.is_some());
    }
}
