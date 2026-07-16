use tokio_postgres::Client;

use opencdc_core::ConnectorType;
use opencdc_core::change_event::ChangeEvent;
use opencdc_core::error::{Error, Result};
use opencdc_core::offset::ConnectorOffset;
use opencdc_core::source_info::SourceInfo;

use super::config::PostgresConnectorConfig;
use super::decoder::{PgOutputDecoder, PgOutputMessage};
use super::types::pg_type_to_json;

pub struct Streamer<'a> {
    config: &'a PostgresConnectorConfig,
    client: &'a Client,
    decoder: PgOutputDecoder,
}

impl<'a> Streamer<'a> {
    pub fn new(config: &'a PostgresConnectorConfig, client: &'a Client) -> Self {
        Self {
            config,
            client,
            decoder: PgOutputDecoder::new(),
        }
    }

    pub async fn run(
        &mut self,
        offset: Option<ConnectorOffset>,
        sink: &tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> Result<()> {
        let start_lsn = offset
            .as_ref()
            .and_then(|o| o.lsn)
            .map(|lsn| format!("{}/{}", (lsn >> 32) as u32, (lsn & 0xFFFF_FFFF) as u32))
            .unwrap_or_else(|| "0/0".to_string());

        let protocol = format!(
            "START_REPLICATION SLOT \"{}\" LOGICAL {} (proto_version '1', publication_names \"{}\")",
            self.config.slot_name, start_lsn, self.config.publication,
        );

        let stream = self
            .client
            .copy_out(&protocol)
            .await
            .map_err(|e| Error::Other(format!("replication start failed: {}", e)))?;

        self.process_stream(Box::pin(stream), sink).await
    }

    async fn process_stream(
        &mut self,
        mut stream: std::pin::Pin<Box<tokio_postgres::CopyOutStream>>,
        sink: &tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> Result<()> {
        use futures::StreamExt;

        let mut wal_lsn: Option<i64> = None;

        loop {
            let chunk = stream.next().await;
            match chunk {
                Some(Ok(data)) => {
                    self.process_chunk(&data, sink, &mut wal_lsn).await?;
                }
                Some(Err(e)) => {
                    return Err(Error::Other(format!("replication stream error: {}", e)));
                }
                None => break,
            }
        }

        Ok(())
    }

    async fn process_chunk(
        &mut self,
        data: &[u8],
        sink: &tokio::sync::mpsc::Sender<ChangeEvent>,
        wal_lsn: &mut Option<i64>,
    ) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if data[0] == b'k' {
            return self.process_keepalive(data, wal_lsn);
        }

        match self.decoder.decode(data) {
            Ok(Some(PgOutputMessage::Begin(begin))) => {
                *wal_lsn = Some(begin.lsn);
            }
            Ok(Some(PgOutputMessage::Commit(commit))) => {
                *wal_lsn = Some(commit.end_lsn);
            }
            Ok(Some(PgOutputMessage::Relation(_))) => {}
            Ok(Some(PgOutputMessage::Insert(insert))) => {
                if let Some(rel) = self.decoder.get_relation(insert.relation_oid) {
                    let source = SourceInfo::new(
                        &ConnectorType::Postgres,
                        &self.config.database,
                        Some(&rel.schema),
                        &rel.name,
                    );
                    let after = columns_to_json(&rel.columns, &insert.new_tuple);
                    let event = ChangeEvent::create(after, source);
                    let _ = sink.send(event).await;
                }
            }
            Ok(Some(PgOutputMessage::Update(update))) => {
                if let Some(rel) = self.decoder.get_relation(update.relation_oid) {
                    let source = SourceInfo::new(
                        &ConnectorType::Postgres,
                        &self.config.database,
                        Some(&rel.schema),
                        &rel.name,
                    );
                    let before = update
                        .old_tuple
                        .as_ref()
                        .map(|t| columns_to_json(&rel.columns, t));
                    let after = columns_to_json(&rel.columns, &update.new_tuple);
                    let event = ChangeEvent::update(before, after, source);
                    let _ = sink.send(event).await;
                }
            }
            Ok(Some(PgOutputMessage::Delete(delete))) => {
                if let Some(rel) = self.decoder.get_relation(delete.relation_oid) {
                    let source = SourceInfo::new(
                        &ConnectorType::Postgres,
                        &self.config.database,
                        Some(&rel.schema),
                        &rel.name,
                    );
                    let before = columns_to_json(&rel.columns, &delete.old_tuple);
                    let event = ChangeEvent::delete(before, source);
                    let _ = sink.send(event).await;
                }
            }
            Ok(Some(PgOutputMessage::Truncate(_))) => {}
            Ok(None) => {}
            Err(e) => {
                return Err(Error::Other(format!("pgoutput decode error: {}", e)));
            }
        }

        Ok(())
    }

    fn process_keepalive(&self, data: &[u8], wal_lsn: &mut Option<i64>) -> Result<()> {
        if data.len() >= 9 {
            let lsn_bytes: [u8; 8] = data[1..9].try_into().unwrap_or([0; 8]);
            let lsn = i64::from_be_bytes(lsn_bytes);
            *wal_lsn = Some(lsn);
        }
        Ok(())
    }
}

fn columns_to_json(
    columns: &[super::decoder::RelationColumn],
    tuple: &[super::decoder::TupleColumn],
) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (i, col) in columns.iter().enumerate() {
        if i >= tuple.len() {
            break;
        }
        let val = if tuple[i].is_null || tuple[i].is_unchanged_toast {
            serde_json::Value::Null
        } else if let Some(ref raw) = tuple[i].value {
            pg_type_to_json(raw, col.type_oid)
        } else {
            serde_json::Value::Null
        };
        map.insert(col.name.clone(), val);
    }
    serde_json::Value::Object(map)
}
