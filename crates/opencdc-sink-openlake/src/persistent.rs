use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use reqwest::Client;

use opencdc_core::change_event::ChangeEvent;
use opencdc_pipeline::error::PipelineResult;
use opencdc_pipeline::sink::Sink;

#[derive(Clone)]
pub struct RestCatalogSinkConfig {
    pub base_url: String,
    pub prefix: String,
    pub namespace: String,
    pub auth_token: Option<String>,
}

pub struct RestCatalogSink {
    name: String,
    config: RestCatalogSinkConfig,
    client: Client,
    registered: Arc<RwLock<HashMap<String, bool>>>,
}

impl RestCatalogSink {
    pub fn new(name: impl Into<String>, config: RestCatalogSinkConfig) -> Self {
        let mut client_builder = Client::builder().user_agent("opencdc/0.1.0");
        if let Some(ref token) = config.auth_token {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                    .expect("valid auth token"),
            );
            client_builder = client_builder.default_headers(headers);
        }
        Self {
            name: name.into(),
            config,
            client: client_builder.build().expect("valid reqwest client"),
            registered: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn table_key(event: &ChangeEvent) -> String {
        format!("{}.{}", event.payload.source.db, event.payload.source.table)
    }

    fn table_schema() -> Schema {
        Schema::new(vec![
            Field::new("op", DataType::Utf8, false),
            Field::new("ts_ms", DataType::Int64, true),
            Field::new("db", DataType::Utf8, false),
            Field::new("schema", DataType::Utf8, true),
            Field::new("table", DataType::Utf8, false),
            Field::new("connector", DataType::Utf8, false),
            Field::new("before", DataType::Utf8, true),
            Field::new("after", DataType::Utf8, true),
        ])
    }

    async fn ensure_table(&self, event: &ChangeEvent) -> PipelineResult<()> {
        let key = Self::table_key(event);
        {
            let reg = self.registered.read().map_err(|e| {
                opencdc_pipeline::error::PipelineError::Sink(format!("lock: {e}"))
            })?;
            if reg.contains_key(&key) {
                return Ok(());
            }
        }

        let ns = &self.config.namespace;
        let table_name = &event.payload.source.table;
        let schema = Self::table_schema();
        let schema_json = serde_json::to_value(&schema).map_err(|e| {
            opencdc_pipeline::error::PipelineError::Sink(format!("schema serde: {e}"))
        })?;

        let body = serde_json::json!({
            "name": table_name,
            "schema": schema_json,
        });

        let url = format!(
            "{}/{}/namespaces/{}/tables",
            self.config.base_url, self.config.prefix, ns
        );

        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                opencdc_pipeline::error::PipelineError::Sink(format!(
                    "create table request: {e}"
                ))
            })?;

        if resp.status().is_success() || resp.status().as_u16() == 409 {
            let mut reg = self.registered.write().map_err(|e| {
                opencdc_pipeline::error::PipelineError::Sink(format!("lock: {e}"))
            })?;
            reg.insert(key, true);
            Ok(())
        } else {
            let body_text = resp.text().await.unwrap_or_default();
            Err(opencdc_pipeline::error::PipelineError::Sink(format!(
                "create table failed: {body_text}"
            )))
        }
    }

    async fn commit_snapshot(
        &self,
        event: &ChangeEvent,
        count: usize,
    ) -> PipelineResult<()> {
        let table_name = &event.payload.source.table;
        let ns = &self.config.namespace;

        let snapshot = serde_json::json!({
            "operation": "append",
            "summary": {
                "operation": "append",
                "count": count.to_string(),
            }
        });

        let commit_body = serde_json::json!({
            "namespace": ns,
            "table": table_name,
            "requirements": [],
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": snapshot,
                },
                {
                    "action": "set-snapshot-ref",
                    "ref_name": "main",
                    "snapshot_id": 0,
                    "type": "snapshot",
                },
            ],
        });

        let url = format!(
            "{}/{}/transactions/commit",
            self.config.base_url, self.config.prefix
        );

        let resp = self.client.post(&url).json(&commit_body).send().await.map_err(|e| {
            opencdc_pipeline::error::PipelineError::Sink(format!(
                "commit request: {e}"
            ))
        })?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(opencdc_pipeline::error::PipelineError::Sink(format!(
                "commit failed ({status}): {body_text}"
            )));
        }

        Ok(())
    }
}

#[async_trait]
impl Sink for RestCatalogSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn write(&mut self, events: &[ChangeEvent]) -> PipelineResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let event = &events[0];
        let key = Self::table_key(event);

        self.ensure_table(event).await?;
        self.commit_snapshot(event, events.len()).await?;

        tracing::debug!(
            table = %key,
            count = events.len(),
            "committed events to REST catalog"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opencdc_core::source_info::SourceInfo;
    use opencdc_core::ConnectorType;

    #[tokio::test]
    async fn test_table_key_generation() {
        let source = SourceInfo::new(&ConnectorType::Postgres, "db", Some("public"), "users");
        let event = ChangeEvent::create(serde_json::json!({"id": 1}), source);
        assert_eq!(RestCatalogSink::table_key(&event), "db.users");
    }

    #[test]
    fn test_table_schema_fields() {
        let schema = RestCatalogSink::table_schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"op"));
        assert!(field_names.contains(&"before"));
        assert!(field_names.contains(&"after"));
        assert!(field_names.contains(&"ts_ms"));
    }
}
