use futures::TryStreamExt;
use mongodb::Client;
use mongodb::bson::Document;

use opencdc_core::ConnectorType;
use opencdc_core::change_event::ChangeEvent;
use opencdc_core::error::{Error, Result};
use opencdc_core::offset::ConnectorOffset;
use opencdc_core::source_info::SourceInfo;

use super::config::MongoDbConnectorConfig;

pub struct MongoDbSnapshotter;

impl MongoDbSnapshotter {
    pub async fn run(
        config: &MongoDbConnectorConfig,
        tables: &[String],
        sink: &tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> Result<ConnectorOffset> {
        let client = Client::with_uri_str(&config.connection_string)
            .await
            .map_err(|e| Error::Other(format!("mongodb connect: {}", e)))?;

        let db = client.database(&config.database);

        let collections = if tables.is_empty() {
            let all_names = db
                .list_collection_names()
                .await
                .map_err(|e| Error::Other(format!("list collections: {}", e)))?;
            all_names
                .into_iter()
                .filter(|n| !n.starts_with("system."))
                .collect::<Vec<_>>()
        } else {
            tables.to_vec()
        };

        for collection_name in &collections {
            if sink.is_closed() {
                break;
            }

            let coll = db.collection::<Document>(collection_name);
            let mut cursor = coll
                .find(mongodb::bson::doc! {})
                .await
                .map_err(|e| Error::Other(format!("find {}: {}", collection_name, e)))?;

            while let Some(doc) = cursor
                .try_next()
                .await
                .map_err(|e| Error::Other(format!("find cursor {}: {}", collection_name, e)))?
            {
                let json_val = bson_doc_to_json(&doc);
                let source = SourceInfo::new(
                    &ConnectorType::Mongodb,
                    &config.database,
                    None::<&str>,
                    collection_name,
                );
                let event = ChangeEvent::snapshot(json_val, source);
                if sink.send(event).await.is_err() {
                    return Err(Error::Other("snapshot sink closed".to_string()));
                }
            }
        }

        Ok(ConnectorOffset::snapshot_done())
    }
}

pub fn bson_doc_to_json(doc: &Document) -> serde_json::Value {
    let mut map = serde_json::Map::with_capacity(doc.len());
    for (key, bv) in doc {
        map.insert(key.clone(), bson_value_to_json(bv));
    }
    serde_json::Value::Object(map)
}

fn bson_value_to_json(bv: &mongodb::bson::Bson) -> serde_json::Value {
    match bv {
        mongodb::bson::Bson::Double(v) => serde_json::json!(v),
        mongodb::bson::Bson::String(v) => serde_json::Value::String(v.clone()),
        mongodb::bson::Bson::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(bson_value_to_json).collect())
        }
        mongodb::bson::Bson::Document(doc) => bson_doc_to_json(doc),
        mongodb::bson::Bson::Boolean(v) => serde_json::Value::Bool(*v),
        mongodb::bson::Bson::Null => serde_json::Value::Null,
        mongodb::bson::Bson::Int32(v) => serde_json::json!(v),
        mongodb::bson::Bson::Int64(v) => serde_json::json!(v),
        mongodb::bson::Bson::DateTime(dt) => serde_json::Value::String(dt.to_string()),
        mongodb::bson::Bson::ObjectId(oid) => serde_json::Value::String(oid.to_hex()),
        mongodb::bson::Bson::Binary(bin) => serde_json::Value::String(hex::encode(&bin.bytes)),
        _ => serde_json::Value::String(format!("{:?}", bv)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{doc, oid::ObjectId};

    #[test]
    fn test_bson_doc_to_json_simple() {
        let doc = doc! {
            "name": "Alice",
            "age": 30,
            "active": true,
        };
        let json = bson_doc_to_json(&doc);
        assert_eq!(json["name"], "Alice");
        assert_eq!(json["age"], 30);
        assert_eq!(json["active"], true);
    }

    #[test]
    fn test_bson_doc_to_json_nested() {
        let doc = doc! {
            "address": {
                "city": "NYC",
                "zip": 10001,
            }
        };
        let json = bson_doc_to_json(&doc);
        assert_eq!(json["address"]["city"], "NYC");
        assert_eq!(json["address"]["zip"], 10001);
    }

    #[test]
    fn test_bson_doc_to_json_array() {
        let doc = doc! {
            "tags": ["a", "b", "c"],
            "scores": [1, 2, 3],
        };
        let json = bson_doc_to_json(&doc);
        assert_eq!(json["tags"][0], "a");
        assert_eq!(json["scores"][2], 3);
    }

    #[test]
    fn test_bson_doc_to_json_object_id() {
        let oid = ObjectId::new();
        let hex = oid.to_hex();
        let doc = doc! { "_id": oid };
        let json = bson_doc_to_json(&doc);
        assert_eq!(json["_id"], hex);
    }

    #[test]
    fn test_bson_value_to_json_null() {
        assert_eq!(
            super::bson_value_to_json(&mongodb::bson::Bson::Null),
            serde_json::Value::Null
        );
    }

    #[test]
    fn test_bson_value_to_json_double() {
        let result = super::bson_value_to_json(&mongodb::bson::Bson::Double(3.14));
        assert_eq!(result, serde_json::json!(3.14));
    }

    #[test]
    fn test_bson_doc_to_json_with_datetime() {
        let now = mongodb::bson::DateTime::now();
        let doc = doc! { "created_at": now };
        let json = bson_doc_to_json(&doc);
        let val = json["created_at"].as_str().unwrap();
        assert!(val.contains('-') || val.is_empty());
    }

    #[test]
    fn test_bson_doc_to_json_binary() {
        use mongodb::bson::Binary;
        let bin = Binary {
            bytes: vec![0x01, 0x02, 0x03],
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
        };
        let doc = doc! { "data": mongodb::bson::Bson::Binary(bin) };

        let json = bson_doc_to_json(&doc);
        assert_eq!(json["data"], "010203");
    }

    #[test]
    fn test_bson_doc_to_json_empty_doc() {
        let doc = Document::new();
        let json = bson_doc_to_json(&doc);
        assert_eq!(json, serde_json::json!({}));
    }

    #[test]
    fn test_mongodb_config_defaults() {
        let config = super::super::config::MongoDbConnectorConfig::new(
            "mongodb://localhost:27017",
            "test_db",
        );
        assert_eq!(config.connection_string, "mongodb://localhost:27017");
        assert_eq!(config.database, "test_db");
        assert!(config.collections.is_empty());
    }

    #[test]
    fn test_mongodb_config_with_collections() {
        let config = super::super::config::MongoDbConnectorConfig::new(
            "mongodb://localhost:27017",
            "test_db",
        )
        .with_collections(vec!["users", "orders"]);
        assert_eq!(config.collections, vec!["users", "orders"]);
    }
}
