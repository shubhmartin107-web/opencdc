pub mod config;
pub mod decoder;
pub mod replication;
pub mod snapshot;
pub mod types;

use async_trait::async_trait;
use tokio_postgres::NoTls;

use opencdc_core::change_event::ChangeEvent;
use opencdc_core::error::{Error, Result};
use opencdc_core::offset::ConnectorOffset;
use opencdc_core::ConnectorType;

use crate::config::{ConnectorConfig, SnapshotContext, StreamContext};
use crate::r#trait::Connector;

use self::config::PostgresConnectorConfig;
use self::replication::Streamer;
use self::snapshot::Snapshotter;

pub struct PostgresConnector {
    config: Option<PostgresConnectorConfig>,
    client: Option<tokio_postgres::Client>,
    connection_handle: Option<tokio::task::JoinHandle<()>>,
}

impl PostgresConnector {
    pub fn new() -> Self {
        Self {
            config: None,
            client: None,
            connection_handle: None,
        }
    }

    pub fn with_config(config: PostgresConnectorConfig) -> Self {
        Self {
            config: Some(config),
            client: None,
            connection_handle: None,
        }
    }

    async fn connect_with_retry(config: &PostgresConnectorConfig) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>)> {
        let max_attempts = config.max_reconnect_attempts.max(1);
        let mut last_error = None;

        for attempt in 0..max_attempts {
            let conn_str = config.connection_string();
            match tokio_postgres::connect(&conn_str, NoTls).await {
                Ok((client, connection)) => {
                    let handle = tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            tracing::error!("postgres connection error: {}", e);
                        }
                    });
                    return Ok((client, handle));
                }
                Err(e) => {
                    last_error = Some(Error::Other(format!("postgres connect attempt {}/{} failed: {}", attempt + 1, max_attempts, e)));
                    if attempt + 1 < max_attempts {
                        let delay = std::time::Duration::from_millis(500 * (attempt as u64 + 1));
                        tracing::warn!("postgres connection attempt {} failed, retrying in {:?}...", attempt + 1, delay);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| Error::Other("postgres connection failed".to_string())))
    }
}

impl Default for PostgresConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Connector for PostgresConnector {
    fn name(&self) -> &str {
        self.config
            .as_ref()
            .map(|c| c.database.as_str())
            .unwrap_or("postgres")
    }

    fn connector_type(&self) -> ConnectorType {
        ConnectorType::Postgres
    }

    async fn start(&mut self, _config: ConnectorConfig) -> Result<()> {
        let pg_config = self
            .config
            .as_ref()
            .ok_or_else(|| Error::Other("PostgresConnectorConfig not set".to_string()))?;

        let (client, connection_handle) = Self::connect_with_retry(pg_config).await?;
        self.connection_handle = Some(connection_handle);

        // Ensure publication exists
        let pub_name = &pg_config.publication;
        client
            .execute(
                &format!(
                    "CREATE PUBLICATION IF NOT EXISTS \"{}\" FOR ALL TABLES",
                    pub_name
                ),
                &[],
            )
            .await
            .map_err(|e| {
                Error::Other(format!("failed to create publication: {}", e))
            })?;

        // Ensure replication slot exists
        let slot_name = &pg_config.slot_name;
        let _ = client
            .execute(
                &format!(
                    "SELECT * FROM pg_create_logical_replication_slot('{}', 'pgoutput')",
                    slot_name
                ),
                &[],
            )
            .await;

        self.client = Some(client);
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.client.take();
        if let Some(handle) = self.connection_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    async fn snapshot(
        &mut self,
        ctx: SnapshotContext,
        sink: tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> Result<ConnectorOffset> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Other("connector not started".to_string()))?;
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| Error::Other("config not set".to_string()))?;

        let snapshotter = Snapshotter::new(config, client);
        snapshotter.run(&ctx.tables, &sink).await
    }

    async fn stream(
        &mut self,
        ctx: StreamContext,
        sink: tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> Result<()> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Other("connector not started".to_string()))?;
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| Error::Other("config not set".to_string()))?;

        let mut streamer = Streamer::new(config, client);
        streamer.run(ctx.offset, &sink).await
    }
}
