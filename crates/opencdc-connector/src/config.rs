use opencdc_core::offset::ConnectorOffset;
use opencdc_core::ConnectorType;

#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    pub connector_type: ConnectorType,
    pub name: String,
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        Self {
            connector_type: ConnectorType::Postgres,
            name: "opencdc".to_string(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotContext {
    pub tables: Vec<String>,
}

impl SnapshotContext {
    pub fn all_tables() -> Self {
        Self { tables: Vec::new() }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StreamContext {
    pub offset: Option<ConnectorOffset>,
    pub tables: Vec<String>,
}

impl StreamContext {
    pub fn from_offset(offset: ConnectorOffset) -> Self {
        Self {
            offset: Some(offset),
            tables: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_config_default() {
        let config = ConnectorConfig::default();
        assert_eq!(config.name, "opencdc");
        assert_eq!(config.connector_type, ConnectorType::Postgres);
    }

    #[test]
    fn test_snapshot_context_default() {
        let ctx = SnapshotContext::default();
        assert!(ctx.tables.is_empty());
        assert!(SnapshotContext::all_tables().tables.is_empty());
    }

    #[test]
    fn test_stream_context_default() {
        let ctx = StreamContext::default();
        assert!(ctx.offset.is_none());
        assert!(ctx.tables.is_empty());
    }

    #[test]
    fn test_stream_context_from_offset() {
        let offset = ConnectorOffset::from_lsn(12345);
        let ctx = StreamContext::from_offset(offset);
        assert!(ctx.offset.is_some());
        assert_eq!(ctx.offset.unwrap().lsn, Some(12345));
    }
}
