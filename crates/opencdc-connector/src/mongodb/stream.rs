use futures::TryStreamExt;
use mongodb::Client;
use mongodb::change_stream::event::{OperationType, ResumeToken};

use opencdc_core::ConnectorType;
use opencdc_core::change_event::ChangeEvent;
use opencdc_core::error::{Error, Result};
use opencdc_core::source_info::SourceInfo;

use super::config::MongoDbConnectorConfig;
use super::snapshot::bson_doc_to_json;

pub struct MongoDbStreamer;

impl MongoDbStreamer {
    pub async fn run(
        config: &MongoDbConnectorConfig,
        resume_token: Option<ResumeToken>,
        sink: &tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> Result<()> {
        let client = Client::with_uri_str(&config.connection_string)
            .await
            .map_err(|e| Error::Other(format!("mongodb connect: {}", e)))?;

        let db = client.database(&config.database);

        let mut watch = db.watch();
        if config.collections.is_empty() {
            watch = watch.pipeline(Vec::<mongodb::bson::Document>::new());
        }
        if let Some(token) = resume_token {
            watch = watch.resume_after(token);
        }
        let mut cursor = watch
            .await
            .map_err(|e| Error::Other(format!("change stream: {}", e)))?;

        while let Some(event) = cursor
            .try_next()
            .await
            .map_err(|e| Error::Other(format!("change stream event: {}", e)))?
        {
            let (db_name, coll_name) = match event.ns {
                Some(ref ns) => {
                    let coll = match ns.coll {
                        Some(ref c) => c.clone(),
                        None => continue,
                    };
                    (ns.db.clone(), coll)
                }
                None => continue,
            };

            if !config.collections.is_empty() && !config.collections.contains(&coll_name) {
                continue;
            }

            let source =
                SourceInfo::new(&ConnectorType::Mongodb, &db_name, None::<&str>, &coll_name);

            match event.operation_type {
                OperationType::Insert => {
                    if let Some(ref doc) = event.full_document {
                        let after = bson_doc_to_json(doc);
                        if sink.send(ChangeEvent::create(after, source)).await.is_err() {
                            break;
                        }
                    }
                }
                OperationType::Update | OperationType::Replace => {
                    let after = event.full_document.as_ref().map(bson_doc_to_json);
                    let before = event
                        .full_document_before_change
                        .as_ref()
                        .map(bson_doc_to_json);
                    let after_val = after.unwrap_or(serde_json::Value::Null);
                    if sink
                        .send(ChangeEvent::update(before, after_val, source))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                OperationType::Delete => {
                    let before = event
                        .full_document_before_change
                        .as_ref()
                        .map(bson_doc_to_json)
                        .unwrap_or_else(|| {
                            event
                                .document_key
                                .as_ref()
                                .map(bson_doc_to_json)
                                .unwrap_or(serde_json::Value::Null)
                        });
                    if sink
                        .send(ChangeEvent::delete(before, source))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }
}
