use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ConnectorOffset {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lsn: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_id: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_ms: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_completed: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub pos: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtid: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume_token: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_partition: Option<String>,

    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub extra: Option<HashMap<String, serde_json::Value>>,
}

impl ConnectorOffset {
    pub fn from_lsn(lsn: i64) -> Self {
        Self {
            lsn: Some(lsn),
            ..Default::default()
        }
    }

    pub fn from_gtid(gtid: impl Into<String>) -> Self {
        Self {
            gtid: Some(gtid.into()),
            ..Default::default()
        }
    }

    pub fn from_binlog_position(file: impl Into<String>, pos: i64) -> Self {
        Self {
            file: Some(file.into()),
            pos: Some(pos),
            ..Default::default()
        }
    }

    pub fn from_resume_token(token: impl Into<String>) -> Self {
        Self {
            resume_token: Some(token.into()),
            ..Default::default()
        }
    }

    pub fn snapshot_start() -> Self {
        Self {
            snapshot: Some(true),
            snapshot_completed: Some(false),
            ..Default::default()
        }
    }

    pub fn snapshot_done() -> Self {
        Self {
            snapshot: Some(false),
            snapshot_completed: Some(true),
            ..Default::default()
        }
    }

    pub fn is_snapshot(&self) -> bool {
        self.snapshot.unwrap_or(false)
    }

    pub fn is_snapshot_completed(&self) -> bool {
        self.snapshot_completed.unwrap_or(false)
    }

    pub fn with_ts_ms(mut self, ts_ms: i64) -> Self {
        self.ts_ms = Some(ts_ms);
        self
    }

    pub fn with_tx_id(mut self, tx_id: i64) -> Self {
        self.tx_id = Some(tx_id);
        self
    }

    pub fn to_cursor_map(&self) -> HashMap<String, serde_json::Value> {
        let mut map = HashMap::new();
        if let Some(lsn) = self.lsn {
            map.insert("lsn".to_string(), serde_json::Value::String(format!("{:#X}", lsn)));
        }
        if let Some(file) = &self.file {
            map.insert("file".to_string(), serde_json::Value::String(file.clone()));
        }
        if let Some(pos) = self.pos {
            map.insert("pos".to_string(), serde_json::Value::Number(pos.into()));
        }
        if let Some(gtid) = &self.gtid {
            map.insert("gtid".to_string(), serde_json::Value::String(gtid.clone()));
        }
        if let Some(token) = &self.resume_token {
            map.insert("resume_token".to_string(), serde_json::Value::String(token.clone()));
        }
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_from_lsn() {
        let offset = ConnectorOffset::from_lsn(1234567890);
        assert_eq!(offset.lsn, Some(1234567890));
        assert!(offset.gtid.is_none());
    }

    #[test]
    fn test_offset_snapshot() {
        let start = ConnectorOffset::snapshot_start();
        assert!(start.is_snapshot());
        assert!(!start.is_snapshot_completed());

        let done = ConnectorOffset::snapshot_done();
        assert!(!done.is_snapshot());
        assert!(done.is_snapshot_completed());
    }

    #[test]
    fn test_offset_to_cursor_map() {
        let offset = ConnectorOffset::from_lsn(1234567890).with_tx_id(42);
        let map = offset.to_cursor_map();
        assert_eq!(map.get("lsn").unwrap().as_str().unwrap(), "0x499602D2");
        assert!(map.get("tx_id").is_none());
    }

    #[test]
    fn test_offset_from_gtid() {
        let offset = ConnectorOffset::from_gtid("1234-5678-9abc");
        assert_eq!(offset.gtid, Some("1234-5678-9abc".to_string()));
        assert!(offset.lsn.is_none());
    }

    #[test]
    fn test_offset_from_binlog_position() {
        let offset = ConnectorOffset::from_binlog_position("mysql-bin.000001", 1234);
        assert_eq!(offset.file, Some("mysql-bin.000001".to_string()));
        assert_eq!(offset.pos, Some(1234));
    }

    #[test]
    fn test_offset_from_resume_token() {
        let offset = ConnectorOffset::from_resume_token("{\"_data\": \"abc123\"}");
        assert_eq!(
            offset.resume_token,
            Some("{\"_data\": \"abc123\"}".to_string())
        );
    }

    #[test]
    fn test_offset_with_ts_ms() {
        let offset = ConnectorOffset::from_lsn(100).with_ts_ms(1710000000000);
        assert_eq!(offset.lsn, Some(100));
        assert_eq!(offset.ts_ms, Some(1710000000000));
    }

    #[test]
    fn test_offset_with_tx_id() {
        let offset = ConnectorOffset::from_lsn(100).with_tx_id(42);
        assert_eq!(offset.lsn, Some(100));
        assert_eq!(offset.tx_id, Some(42));
    }

    #[test]
    fn test_offset_chaining() {
        let offset = ConnectorOffset::from_gtid("abc:123")
            .with_ts_ms(1710000000000)
            .with_tx_id(99);
        assert_eq!(offset.gtid, Some("abc:123".to_string()));
        assert_eq!(offset.ts_ms, Some(1710000000000));
        assert_eq!(offset.tx_id, Some(99));
    }

    #[test]
    fn test_offset_to_cursor_map_with_all_fields() {
        let offset = ConnectorOffset::from_binlog_position("bin.001", 456)
            .with_ts_ms(1710000000000);
        let map = offset.to_cursor_map();
        assert_eq!(map.get("file").unwrap().as_str().unwrap(), "bin.001");
        assert_eq!(map.get("pos").unwrap().as_u64().unwrap(), 456);
    }

    #[test]
    fn test_offset_to_cursor_map_with_gtid() {
        let offset = ConnectorOffset::from_gtid("abc:123");
        let map = offset.to_cursor_map();
        assert_eq!(map.get("gtid").unwrap().as_str().unwrap(), "abc:123");
    }

    #[test]
    fn test_offset_to_cursor_map_with_resume_token() {
        let offset = ConnectorOffset::from_resume_token("tok_123");
        let map = offset.to_cursor_map();
        assert_eq!(map.get("resume_token").unwrap().as_str().unwrap(), "tok_123");
    }

    #[test]
    fn test_offset_default() {
        let offset = ConnectorOffset::default();
        assert!(offset.lsn.is_none());
        assert!(offset.gtid.is_none());
        assert!(offset.file.is_none());
        assert!(offset.pos.is_none());
        assert!(offset.ts_ms.is_none());
        assert!(offset.tx_id.is_none());
        assert!(offset.resume_token.is_none());
    }

    #[test]
    fn test_offset_roundtrip() {
        let offset = ConnectorOffset::from_binlog_position("mysql-bin.000001", 1234)
            .with_ts_ms(1710000000000);
        let json = serde_json::to_string(&offset).unwrap();
        let deserialized: ConnectorOffset = serde_json::from_str(&json).unwrap();
        assert_eq!(offset.lsn, deserialized.lsn);
        assert_eq!(offset.tx_id, deserialized.tx_id);
        assert_eq!(offset.ts_ms, deserialized.ts_ms);
        assert_eq!(offset.file, deserialized.file);
        assert_eq!(offset.pos, deserialized.pos);
        assert_eq!(offset.gtid, deserialized.gtid);
    }
}
