pub mod config;
pub mod snapshot;
pub mod stream;

use async_trait::async_trait;

use opencdc_core::ConnectorType;
use opencdc_core::change_event::ChangeEvent;
use opencdc_core::error::{Error, Result};
use opencdc_core::offset::ConnectorOffset;

use crate::config::{ConnectorConfig, SnapshotContext, StreamContext};
use crate::r#trait::Connector;

use mongodb::change_stream::event::ResumeToken;

use self::config::MongoDbConnectorConfig;
use self::snapshot::MongoDbSnapshotter;

pub struct MongoDbConnector {
    config: Option<MongoDbConnectorConfig>,
}

impl MongoDbConnector {
    pub fn new() -> Self {
        Self { config: None }
    }

    pub fn with_config(config: MongoDbConnectorConfig) -> Self {
        Self {
            config: Some(config),
        }
    }

    async fn connect_with_retry(config: &MongoDbConnectorConfig) -> Result<mongodb::Client> {
        let max_attempts = config.max_reconnect_attempts.max(1);
        let mut last_error = None;

        for attempt in 0..max_attempts {
            match mongodb::Client::with_uri_str(&config.connection_string).await {
                Ok(client) => {
                    // Ping to verify
                    match client
                        .database(&config.database)
                        .run_command(mongodb::bson::doc! { "ping": 1 })
                        .await
                    {
                        Ok(_) => return Ok(client),
                        Err(e) => {
                            last_error = Some(Error::Other(format!(
                                "mongodb ping attempt {}/{} failed: {}",
                                attempt + 1,
                                max_attempts,
                                e
                            )));
                        }
                    }
                }
                Err(e) => {
                    last_error = Some(Error::Other(format!(
                        "mongodb connect attempt {}/{} failed: {}",
                        attempt + 1,
                        max_attempts,
                        e
                    )));
                }
            }

            if attempt + 1 < max_attempts {
                let delay = std::time::Duration::from_millis(500 * (attempt as u64 + 1));
                tracing::warn!(
                    "mongodb connection attempt {} failed, retrying in {:?}...",
                    attempt + 1,
                    delay
                );
                tokio::time::sleep(delay).await;
            }
        }

        Err(last_error.unwrap_or_else(|| Error::Other("mongodb connection failed".to_string())))
    }
}

impl Default for MongoDbConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Connector for MongoDbConnector {
    fn name(&self) -> &str {
        self.config
            .as_ref()
            .map(|c| c.database.as_str())
            .unwrap_or("mongodb")
    }

    fn connector_type(&self) -> ConnectorType {
        ConnectorType::Mongodb
    }

    async fn start(&mut self, _config: ConnectorConfig) -> Result<()> {
        let mongo_config = self
            .config
            .as_ref()
            .ok_or_else(|| Error::Other("MongoDbConnectorConfig not set".to_string()))?;

        let _client = Self::connect_with_retry(mongo_config).await?;

        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        Ok(())
    }

    async fn snapshot(
        &mut self,
        ctx: SnapshotContext,
        sink: tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> Result<ConnectorOffset> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| Error::Other("config not set".to_string()))?;

        MongoDbSnapshotter::run(config, &ctx.tables, &sink).await
    }

    async fn stream(
        &mut self,
        ctx: StreamContext,
        sink: tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> Result<()> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| Error::Other("config not set".to_string()))?;

        let _client = Self::connect_with_retry(config).await?;

        let resume_token = ctx
            .offset
            .as_ref()
            .and_then(|o| o.resume_token.as_ref())
            .and_then(|s| serde_json::from_str::<ResumeToken>(s).ok());

        stream::MongoDbStreamer::run(config, resume_token, &sink).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::r#trait::Connector;

    #[test]
    fn test_mongodb_connector_default() {
        let connector = MongoDbConnector::new();
        assert_eq!(connector.name(), "mongodb");
        assert_eq!(connector.connector_type(), ConnectorType::Mongodb);
    }

    #[test]
    fn test_mongodb_connector_with_config() {
        let config = MongoDbConnectorConfig::new("mongodb://localhost:27017", "test_db");
        let connector = MongoDbConnector::with_config(config);
        assert_eq!(connector.name(), "test_db");
        assert_eq!(connector.connector_type(), ConnectorType::Mongodb);
    }

    #[test]
    fn test_mongodb_connector_default_impl() {
        let connector = MongoDbConnector::default();
        assert_eq!(connector.name(), "mongodb");
    }

    #[test]
    fn test_mongodb_connector_stop_does_not_panic() {
        let mut connector = MongoDbConnector::new();
        // stop is a no-op, should not panic
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(connector.stop());
        assert!(result.is_ok());
    }
}
