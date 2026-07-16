use tokio_postgres::Client;

use opencdc_core::change_event::ChangeEvent;
use opencdc_core::error::{Error, Result};
use opencdc_core::offset::ConnectorOffset;
use opencdc_core::source_info::SnapshotPhase;
use opencdc_core::source_info::SourceInfo;

use super::config::PostgresConnectorConfig;

pub struct Snapshotter<'a> {
    config: &'a PostgresConnectorConfig,
    client: &'a Client,
}

impl<'a> Snapshotter<'a> {
    pub fn new(config: &'a PostgresConnectorConfig, client: &'a Client) -> Self {
        Self { config, client }
    }

    pub async fn run(
        &self,
        tables: &[String],
        sink: &tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> Result<ConnectorOffset> {
        let snapshot_lsn = self.get_snapshot_lsn().await?;

        let table_list = if tables.is_empty() {
            self.get_public_tables().await?
        } else {
            tables.to_vec()
        };

        for table in &table_list {
            self.snapshot_table(table, sink).await?;
        }

        Ok(ConnectorOffset::from_lsn(snapshot_lsn))
    }

    async fn get_snapshot_lsn(&self) -> Result<i64> {
        let row = self
            .client
            .query_one(
                "SELECT pg_current_wal_lsn()::text,
                        pg_current_wal_lsn() - '0/0'::pg_lsn AS lsn_bytes",
                &[],
            )
            .await
            .map_err(|e| {
                Error::Other(format!("failed to get current WAL LSN: {}", e))
            })?;

        let lsn_str: String = row.get(0);
        let parts: Vec<&str> = lsn_str.split('/').collect();
        if parts.len() == 2 {
            let high = u32::from_str_radix(parts[0], 16).map_err(|e| {
                Error::Other(format!("invalid LSN high part '{}': {}", parts[0], e))
            })?;
            let low = u32::from_str_radix(parts[1], 16).map_err(|e| {
                Error::Other(format!("invalid LSN low part '{}': {}", parts[1], e))
            })?;
            Ok(((high as i64) << 32) | (low as i64))
        } else {
            Err(Error::Other(format!("invalid WAL LSN format: {}", lsn_str)))
        }
    }

    async fn get_public_tables(&self) -> Result<Vec<String>> {
        let rows = self
            .client
            .query(
                "SELECT tablename FROM pg_catalog.pg_tables
                 WHERE schemaname = 'public' AND tableowner = CURRENT_USER
                 ORDER BY tablename",
                &[],
            )
            .await
            .map_err(|e| Error::Other(format!("failed to list tables: {}", e)))?;

        Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
    }

    async fn snapshot_table(
        &self,
        table: &str,
        sink: &tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> Result<()> {
        let columns = self.get_column_info(table).await?;

        let col_names: Vec<&str> = columns.iter().map(|(n, _)| n.as_str()).collect();
        let query = if col_names.is_empty() {
            format!("SELECT * FROM \"{}\"", table)
        } else {
            format!(
                "SELECT {} FROM \"{}\"",
                col_names.join(", "),
                table
            )
        };

        let rows = self
            .client
            .query(&query, &[])
            .await
            .map_err(|e| Error::Other(format!("failed to snapshot table '{}': {}", table, e)))?;

        for row in &rows {
            let row_json = self.row_to_json(row, &columns);

            let source = SourceInfo::new(
                &opencdc_core::ConnectorType::Postgres,
                &self.config.database,
                Some("public"),
                table,
            )
            .with_snapshot(SnapshotPhase::True);

            let event = ChangeEvent::snapshot(row_json, source);
            if sink.send(event).await.is_err() {
                return Err(Error::Other("snapshot sink closed".to_string()));
            }
        }

        Ok(())
    }

    async fn get_column_info(&self, table: &str) -> Result<Vec<(String, u32)>> {
        let rows = self
            .client
            .query(
                "SELECT a.attname::text, a.atttypid
                 FROM pg_catalog.pg_attribute a
                 JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                 WHERE c.relname = $1
                   AND a.attnum > 0
                   AND NOT a.attisdropped
                 ORDER BY a.attnum",
                &[&table],
            )
            .await
            .map_err(|e| {
                Error::Other(format!("failed to get columns for '{}': {}", table, e))
            })?;

        Ok(rows
            .iter()
            .map(|r| {
                let name: String = r.get(0);
                let oid: u32 = r.get(1);
                (name, oid)
            })
            .collect())
    }

    fn row_to_json(
        &self,
        row: &tokio_postgres::Row,
        columns: &[(String, u32)],
    ) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        for (name, oid) in columns {
            let val = self.pg_value_to_json(row, name, *oid);
            map.insert(name.clone(), val);
        }
        serde_json::Value::Object(map)
    }

    fn pg_value_to_json(
        &self,
        row: &tokio_postgres::Row,
        col: &str,
        type_oid: u32,
    ) -> serde_json::Value {
        let val: Option<String> = row.get(col);
        match val {
            Some(s) => self::super::types::pg_type_to_json(&bytes::Bytes::from(s), type_oid),
            None => serde_json::Value::Null,
        }
    }
}
