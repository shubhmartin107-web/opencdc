use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceInfo {
    #[serde(default = "default_version")]
    pub version: String,

    #[serde(default = "default_connector")]
    pub connector: String,

    #[serde(default = "default_name")]
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_ms: Option<i64>,

    #[serde(default)]
    pub snapshot: SnapshotPhase,

    #[serde(default)]
    pub db: String,

    #[serde(default = "default_schema", skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    #[serde(default)]
    pub table: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_id: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub lsn: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub xmin: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub pos: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtid: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub rs_id: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume_token: Option<String>,

    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub extra: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotPhase {
    True,
    #[default]
    False,
    Last,
    Incremental,
}

impl SnapshotPhase {
    pub fn is_snapshot(&self) -> bool {
        !matches!(self, SnapshotPhase::False)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            SnapshotPhase::True => "true",
            SnapshotPhase::False => "false",
            SnapshotPhase::Last => "last",
            SnapshotPhase::Incremental => "incremental",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "true" => Some(SnapshotPhase::True),
            "false" => Some(SnapshotPhase::False),
            "last" => Some(SnapshotPhase::Last),
            "incremental" => Some(SnapshotPhase::Incremental),
            _ => None,
        }
    }
}

fn default_version() -> String {
    "2.7.2.Final".to_string()
}

fn default_connector() -> String {
    "opencdc".to_string()
}

fn default_name() -> String {
    "opencdc".to_string()
}

fn default_schema() -> Option<String> {
    Some("public".to_string())
}

impl SourceInfo {
    pub fn new(
        connector_type: &crate::ConnectorType,
        db: impl Into<String>,
        schema: Option<impl Into<String>>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            version: connector_type.debezium_version().to_string(),
            connector: connector_type.as_str().to_string(),
            name: "opencdc".to_string(),
            ts_ms: Some(chrono::Utc::now().timestamp_millis()),
            snapshot: SnapshotPhase::False,
            db: db.into(),
            schema: schema.map(|s| s.into()),
            table: table.into(),
            tx_id: None,
            lsn: None,
            xmin: None,
            file: None,
            pos: None,
            gtid: None,
            sequence: None,
            rs_id: None,
            resume_token: None,
            extra: None,
        }
    }

    pub fn with_snapshot(mut self, phase: SnapshotPhase) -> Self {
        self.snapshot = phase;
        self
    }

    pub fn with_lsn(mut self, lsn: i64) -> Self {
        self.lsn = Some(lsn);
        self
    }

    pub fn with_tx_id(mut self, tx_id: i64) -> Self {
        self.tx_id = Some(tx_id);
        self
    }

    pub fn with_ts_ms(mut self, ts_ms: i64) -> Self {
        self.ts_ms = Some(ts_ms);
        self
    }

    pub fn with_gtid(mut self, gtid: impl Into<String>) -> Self {
        self.gtid = Some(gtid.into());
        self
    }

    pub fn with_resume_token(mut self, token: impl Into<String>) -> Self {
        self.resume_token = Some(token.into());
        self
    }

    pub fn with_extra(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.extra.get_or_insert_default().insert(key.into(), value);
        self
    }
}

impl Default for SourceInfo {
    fn default() -> Self {
        Self {
            version: default_version(),
            connector: default_connector(),
            name: default_name(),
            ts_ms: Some(chrono::Utc::now().timestamp_millis()),
            snapshot: SnapshotPhase::False,
            db: String::new(),
            schema: default_schema(),
            table: String::new(),
            tx_id: None,
            lsn: None,
            xmin: None,
            file: None,
            pos: None,
            gtid: None,
            sequence: None,
            rs_id: None,
            resume_token: None,
            extra: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConnectorType;

    #[test]
    fn test_source_info_creation() {
        let source = SourceInfo::new(
            &ConnectorType::Postgres,
            "mydb",
            Some("public"),
            "users",
        );
        assert_eq!(source.db, "mydb");
        assert_eq!(source.schema, Some("public".to_string()));
        assert_eq!(source.table, "users");
        assert_eq!(source.connector, "postgresql");
        assert_eq!(source.snapshot, SnapshotPhase::False);
        assert!(source.ts_ms.is_some());
    }

    #[test]
    fn test_source_info_with_snapshot() {
        let source = SourceInfo::new(&ConnectorType::Mysql, "mydb", None::<&str>, "orders")
            .with_snapshot(SnapshotPhase::True)
            .with_lsn(12345)
            .with_tx_id(67890);
        assert_eq!(source.snapshot, SnapshotPhase::True);
        assert_eq!(source.lsn, Some(12345));
        assert_eq!(source.tx_id, Some(67890));
        assert_eq!(source.schema, None);
    }

    #[test]
    fn test_source_info_with_gtid() {
        let source = SourceInfo::new(&ConnectorType::Mysql, "mydb", None::<&str>, "orders")
            .with_gtid("abc:123");
        assert_eq!(source.gtid, Some("abc:123".to_string()));
    }

    #[test]
    fn test_source_info_with_resume_token() {
        let source = SourceInfo::new(&ConnectorType::Mongodb, "mydb", None::<&str>, "users")
            .with_resume_token("tok_123");
        assert_eq!(source.resume_token, Some("tok_123".to_string()));
    }

    #[test]
    fn test_source_info_with_extra() {
        let source = SourceInfo::new(&ConnectorType::Postgres, "mydb", Some("public"), "users")
            .with_extra("custom_field", serde_json::json!("custom_value"));
        let extra = source.extra.unwrap();
        assert_eq!(extra.get("custom_field").unwrap(), "custom_value");
    }

    #[test]
    fn test_source_info_default() {
        let source = SourceInfo::default();
        assert_eq!(source.version, "2.7.2.Final");
        assert_eq!(source.connector, "opencdc");
        assert_eq!(source.snapshot, SnapshotPhase::False);
        assert!(source.ts_ms.is_some());
        assert!(source.db.is_empty());
        assert!(source.table.is_empty());
    }

    #[test]
    fn test_source_info_with_ts_ms() {
        let source = SourceInfo::new(&ConnectorType::Postgres, "db", None::<&str>, "t")
            .with_ts_ms(1710000000000);
        assert_eq!(source.ts_ms, Some(1710000000000));
    }

    #[test]
    fn test_source_info_serialization() {
        let source = SourceInfo::new(&ConnectorType::Postgres, "mydb", Some("public"), "users")
            .with_lsn(1234567890)
            .with_snapshot(SnapshotPhase::True);
        let json = serde_json::to_value(&source).unwrap();
        assert_eq!(json["db"], "mydb");
        assert_eq!(json["schema"], "public");
        assert_eq!(json["table"], "users");
        assert_eq!(json["snapshot"], "true");
        assert_eq!(json["lsn"], 1234567890);
        assert_eq!(json["connector"], "postgresql");
        assert!(json.get("ts_ms").is_some());
    }

    #[test]
    fn test_snapshot_roundtrip() {
        for phase in &[
            SnapshotPhase::True,
            SnapshotPhase::False,
            SnapshotPhase::Last,
            SnapshotPhase::Incremental,
        ] {
            let json = serde_json::to_string(phase).unwrap();
            let deserialized: SnapshotPhase = serde_json::from_str(&json).unwrap();
            assert_eq!(*phase, deserialized);
            assert_eq!(
                SnapshotPhase::from_str(phase.as_str()),
                Some(*phase)
            );
        }
    }
}
