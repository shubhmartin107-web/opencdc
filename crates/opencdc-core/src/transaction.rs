use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransactionInfo {
    pub id: String,

    #[serde(default = "default_total_order")]
    pub total_order: i64,

    #[serde(default = "default_data_collections_order")]
    pub data_collections_order: i64,
}

fn default_total_order() -> i64 {
    0
}

fn default_data_collections_order() -> i64 {
    0
}

impl TransactionInfo {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            total_order: 0,
            data_collections_order: 0,
        }
    }

    pub fn with_order(mut self, total_order: i64, data_collections_order: i64) -> Self {
        self.total_order = total_order;
        self.data_collections_order = data_collections_order;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransactionMetadata {
    pub id: String,
    pub status: TransactionStatus,
    pub event_count: Option<i64>,
    pub data_collections: Option<Vec<DataCollection>>,
    pub ts_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TransactionStatus {
    Begin,
    End,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataCollection {
    pub data_collection: String,
    pub event_count: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_info() {
        let txn = TransactionInfo::new("tx-123").with_order(1, 42);
        assert_eq!(txn.id, "tx-123");
        assert_eq!(txn.total_order, 1);
        assert_eq!(txn.data_collections_order, 42);
    }

    #[test]
    fn test_transaction_roundtrip() {
        let txn = TransactionInfo::new("tx-123");
        let json = serde_json::to_string(&txn).unwrap();
        let deserialized: TransactionInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(txn, deserialized);
    }

    #[test]
    fn test_transaction_metadata_begin() {
        let meta = TransactionMetadata {
            id: "tx-1".to_string(),
            status: TransactionStatus::Begin,
            event_count: None,
            data_collections: None,
            ts_ms: Some(1710000000000),
        };
        assert_eq!(meta.id, "tx-1");
        assert_eq!(meta.status, TransactionStatus::Begin);
        assert!(meta.data_collections.is_none());
    }

    #[test]
    fn test_transaction_metadata_end() {
        let meta = TransactionMetadata {
            id: "tx-2".to_string(),
            status: TransactionStatus::End,
            event_count: Some(5),
            data_collections: Some(vec![
                DataCollection {
                    data_collection: "public.users".to_string(),
                    event_count: 3,
                },
            ]),
            ts_ms: Some(1710000000001),
        };
        assert_eq!(meta.status, TransactionStatus::End);
        assert_eq!(meta.event_count, Some(5));
        assert_eq!(meta.data_collections.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_transaction_metadata_roundtrip() {
        let meta = TransactionMetadata {
            id: "tx-3".to_string(),
            status: TransactionStatus::Begin,
            event_count: Some(10),
            data_collections: Some(vec![
                DataCollection {
                    data_collection: "public.orders".to_string(),
                    event_count: 7,
                },
                DataCollection {
                    data_collection: "public.users".to_string(),
                    event_count: 3,
                },
            ]),
            ts_ms: Some(1710000000002),
        };
        let json = serde_json::to_value(&meta).unwrap();
        assert_eq!(json["id"], "tx-3");
        assert_eq!(json["status"], "begin");
        assert_eq!(json["data_collections"][0]["data_collection"], "public.orders");

        let deserialized: TransactionMetadata = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized.id, meta.id);
        assert_eq!(deserialized.status, meta.status);
        assert_eq!(deserialized.event_count, meta.event_count);
    }

    #[test]
    fn test_transaction_status_roundtrip() {
        for status in &[TransactionStatus::Begin, TransactionStatus::End] {
            let json = serde_json::to_string(status).unwrap();
            let deserialized: TransactionStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(*status, deserialized);
        }
    }

    #[test]
    fn test_data_collection_roundtrip() {
        let dc = DataCollection {
            data_collection: "public.users".to_string(),
            event_count: 42,
        };
        let json = serde_json::to_value(&dc).unwrap();
        assert_eq!(json["data_collection"], "public.users");
        assert_eq!(json["event_count"], 42);

        let deserialized: DataCollection = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized.data_collection, dc.data_collection);
        assert_eq!(deserialized.event_count, dc.event_count);
    }
}
