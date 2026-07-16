pub mod binlog;
pub mod column;
pub mod config;
pub mod snapshot;

use async_trait::async_trait;

use opencdc_core::ConnectorType;
use opencdc_core::change_event::ChangeEvent;
use opencdc_core::error::{Error, Result};
use opencdc_core::offset::ConnectorOffset;
use opencdc_core::source_info::SourceInfo;

use crate::config::{ConnectorConfig, SnapshotContext, StreamContext};
use crate::r#trait::Connector;

use self::binlog::BinlogClient;
use self::config::MySqlConnectorConfig;
use self::snapshot::MySqlSnapshotter;

pub struct MySqlConnector {
    config: Option<MySqlConnectorConfig>,
    binlog_client: Option<BinlogClient>,
}

impl MySqlConnector {
    pub fn new() -> Self {
        Self {
            config: None,
            binlog_client: None,
        }
    }

    pub fn with_config(config: MySqlConnectorConfig) -> Self {
        Self {
            config: Some(config),
            binlog_client: None,
        }
    }

    async fn connect_with_retry(config: &MySqlConnectorConfig) -> Result<BinlogClient> {
        let max_attempts = config.max_reconnect_attempts.max(1);
        let mut last_error = None;

        for attempt in 0..max_attempts {
            match BinlogClient::connect(config.clone()).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    last_error = Some(Error::Other(format!(
                        "mysql connect attempt {}/{} failed: {}",
                        attempt + 1,
                        max_attempts,
                        e
                    )));
                    if attempt + 1 < max_attempts {
                        let delay = std::time::Duration::from_millis(500 * (attempt as u64 + 1));
                        tracing::warn!(
                            "mysql connection attempt {} failed, retrying in {:?}...",
                            attempt + 1,
                            delay
                        );
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| Error::Other("mysql connection failed".to_string())))
    }
}

impl Default for MySqlConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Connector for MySqlConnector {
    fn name(&self) -> &str {
        self.config
            .as_ref()
            .map(|c| c.database.as_str())
            .unwrap_or("mysql")
    }

    fn connector_type(&self) -> ConnectorType {
        ConnectorType::Mysql
    }

    async fn start(&mut self, _config: ConnectorConfig) -> Result<()> {
        let mysql_config = self
            .config
            .as_ref()
            .ok_or_else(|| Error::Other("MySqlConnectorConfig not set".to_string()))?;

        let client = Self::connect_with_retry(mysql_config).await?;

        self.binlog_client = Some(client);
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.binlog_client.take();
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

        MySqlSnapshotter::run(config, &ctx.tables, &sink).await
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

        let mut client = Self::connect_with_retry(config).await?;

        client
            .register_slave()
            .await
            .map_err(|e| Error::Other(format!("register slave: {}", e)))?;

        let gtid_set = ctx
            .offset
            .as_ref()
            .and_then(|o| o.gtid.as_deref())
            .unwrap_or("");

        if !gtid_set.is_empty() {
            client
                .dump_gtid(gtid_set)
                .await
                .map_err(|e| Error::Other(format!("dump gtid: {}", e)))?;
        } else {
            let filename = ctx
                .offset
                .as_ref()
                .and_then(|o| o.file.as_deref())
                .unwrap_or("");
            let pos = ctx.offset.as_ref().and_then(|o| o.pos).unwrap_or(4) as u32;

            client
                .dump_binlog(filename, pos)
                .await
                .map_err(|e| Error::Other(format!("dump binlog: {}", e)))?;
        }

        loop {
            let result = client
                .next_event()
                .await
                .map_err(|e| Error::Other(format!("read event: {}", e)))?;

            match result {
                None | Some((_, binlog::BinlogEvent::Heartbeat)) => {
                    continue;
                }
                Some((header, binlog::BinlogEvent::Gtid(uuid, gno))) => {
                    let gtid_str = format!("{}:{}", uuid, gno);
                    let offset = ConnectorOffset::from_gtid(&gtid_str)
                        .with_ts_ms(header.timestamp as i64 * 1000);
                    // Store the offset for reference
                    let _ = offset;
                }
                Some((_header, binlog::BinlogEvent::WriteRows { table_id, rows })) => {
                    if let Some(tm) = client.tables.get(&table_id) {
                        for row in &rows {
                            let mut obj = serde_json::Map::new();
                            for (i, _col) in tm.column_types.iter().enumerate() {
                                if i < row.columns.len() {
                                    let key = format!("col_{}", i);
                                    obj.insert(key, row.columns[i].clone());
                                }
                            }
                            let source = SourceInfo::new(
                                &ConnectorType::Mysql,
                                &tm.database,
                                None::<&str>,
                                &tm.table,
                            );
                            let event = ChangeEvent::create(serde_json::Value::Object(obj), source);
                            if sink.send(event).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                Some((_header, binlog::BinlogEvent::UpdateRows { table_id, rows })) => {
                    if let Some(tm) = client.tables.get(&table_id) {
                        for (before, after) in &rows {
                            let mut before_obj = serde_json::Map::new();
                            let mut after_obj = serde_json::Map::new();
                            for (i, _col) in tm.column_types.iter().enumerate() {
                                if i < before.columns.len() {
                                    before_obj
                                        .insert(format!("col_{}", i), before.columns[i].clone());
                                }
                                if i < after.columns.len() {
                                    after_obj
                                        .insert(format!("col_{}", i), after.columns[i].clone());
                                }
                            }
                            let source = SourceInfo::new(
                                &ConnectorType::Mysql,
                                &tm.database,
                                None::<&str>,
                                &tm.table,
                            );
                            let event = ChangeEvent::update(
                                Some(serde_json::Value::Object(before_obj)),
                                serde_json::Value::Object(after_obj),
                                source,
                            );
                            if sink.send(event).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                Some((_header, binlog::BinlogEvent::DeleteRows { table_id, rows })) => {
                    if let Some(tm) = client.tables.get(&table_id) {
                        for row in &rows {
                            let mut obj = serde_json::Map::new();
                            for (i, _col) in tm.column_types.iter().enumerate() {
                                if i < row.columns.len() {
                                    obj.insert(format!("col_{}", i), row.columns[i].clone());
                                }
                            }
                            let source = SourceInfo::new(
                                &ConnectorType::Mysql,
                                &tm.database,
                                None::<&str>,
                                &tm.table,
                            );
                            let event = ChangeEvent::delete(serde_json::Value::Object(obj), source);
                            if sink.send(event).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                Some((_header, binlog::BinlogEvent::Xid(_))) => {}
                Some((_header, binlog::BinlogEvent::Rotate(_, _))) => {}
                Some((_header, binlog::BinlogEvent::FormatDescription)) => {}
                Some((_header, binlog::BinlogEvent::TableMap(_))) => {}
                Some((_header, binlog::BinlogEvent::Unknown(ty))) => {
                    tracing::debug!(event_type = ty, "skipping unknown binlog event");
                }
            }
        }
    }
}
