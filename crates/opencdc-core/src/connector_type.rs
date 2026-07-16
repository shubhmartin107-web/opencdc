use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorType {
    Postgres,
    Mysql,
    Mongodb,
    SqlServer,
    Oracle,
    Db2,
    Cassandra,
    Vitess,
}

impl ConnectorType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConnectorType::Postgres => "postgresql",
            ConnectorType::Mysql => "mysql",
            ConnectorType::Mongodb => "mongodb",
            ConnectorType::SqlServer => "sqlserver",
            ConnectorType::Oracle => "oracle",
            ConnectorType::Db2 => "db2",
            ConnectorType::Cassandra => "cassandra",
            ConnectorType::Vitess => "vitess",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "postgresql" | "postgres" => Some(ConnectorType::Postgres),
            "mysql" | "mariadb" => Some(ConnectorType::Mysql),
            "mongodb" | "mongo" => Some(ConnectorType::Mongodb),
            "sqlserver" | "sql_server" => Some(ConnectorType::SqlServer),
            "oracle" => Some(ConnectorType::Oracle),
            "db2" => Some(ConnectorType::Db2),
            "cassandra" => Some(ConnectorType::Cassandra),
            "vitess" => Some(ConnectorType::Vitess),
            _ => None,
        }
    }

    pub fn debezium_class_name(&self) -> &'static str {
        match self {
            ConnectorType::Postgres => "io.debezium.connector.postgresql.PostgresConnector",
            ConnectorType::Mysql => "io.debezium.connector.mysql.MySqlConnector",
            ConnectorType::Mongodb => "io.debezium.connector.mongodb.MongoDbConnector",
            ConnectorType::SqlServer => "io.debezium.connector.sqlserver.SqlServerConnector",
            ConnectorType::Oracle => "io.debezium.connector.oracle.OracleConnector",
            ConnectorType::Db2 => "io.debezium.connector.db2.Db2Connector",
            ConnectorType::Cassandra => "io.debezium.connector.cassandra.CassandraConnector",
            ConnectorType::Vitess => "io.debezium.connector.vitess.VitessConnector",
        }
    }

    pub fn debezium_version(&self) -> &'static str {
        "2.7.2.Final"
    }
}

impl fmt::Display for ConnectorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_type_roundtrip() {
        for ct in &[
            ConnectorType::Postgres,
            ConnectorType::Mysql,
            ConnectorType::Mongodb,
        ] {
            let json = serde_json::to_string(ct).unwrap();
            let deserialized: ConnectorType = serde_json::from_str(&json).unwrap();
            assert_eq!(*ct, deserialized);
            assert_eq!(
                ct.as_str(),
                ConnectorType::from_str(ct.as_str()).unwrap().as_str()
            );
        }
    }

    #[test]
    fn test_debezium_class_names() {
        assert_eq!(
            ConnectorType::Postgres.debezium_class_name(),
            "io.debezium.connector.postgresql.PostgresConnector"
        );
        assert_eq!(
            ConnectorType::Mysql.debezium_class_name(),
            "io.debezium.connector.mysql.MySqlConnector"
        );
    }

    #[test]
    fn test_connector_type_aliases() {
        // Test all aliases
        assert_eq!(
            ConnectorType::from_str("postgres"),
            Some(ConnectorType::Postgres)
        );
        assert_eq!(
            ConnectorType::from_str("mariadb"),
            Some(ConnectorType::Mysql)
        );
        assert_eq!(
            ConnectorType::from_str("mongo"),
            Some(ConnectorType::Mongodb)
        );
        assert_eq!(
            ConnectorType::from_str("sql_server"),
            Some(ConnectorType::SqlServer)
        );

        // Test all primary names
        assert_eq!(
            ConnectorType::from_str("oracle"),
            Some(ConnectorType::Oracle)
        );
        assert_eq!(ConnectorType::from_str("db2"), Some(ConnectorType::Db2));
        assert_eq!(
            ConnectorType::from_str("cassandra"),
            Some(ConnectorType::Cassandra)
        );
        assert_eq!(
            ConnectorType::from_str("vitess"),
            Some(ConnectorType::Vitess)
        );
    }

    #[test]
    fn test_connector_type_invalid() {
        assert_eq!(ConnectorType::from_str("invalid"), None);
        assert_eq!(ConnectorType::from_str(""), None);
    }

    #[test]
    fn test_connector_type_display() {
        assert_eq!(format!("{}", ConnectorType::Postgres), "postgresql");
        assert_eq!(format!("{}", ConnectorType::Mysql), "mysql");
        assert_eq!(format!("{}", ConnectorType::Mongodb), "mongodb");
    }

    #[test]
    fn test_debezium_version() {
        assert_eq!(ConnectorType::Postgres.debezium_version(), "2.7.2.Final");
        assert_eq!(ConnectorType::Mysql.debezium_version(), "2.7.2.Final");
        assert_eq!(ConnectorType::Mongodb.debezium_version(), "2.7.2.Final");
    }

    #[test]
    fn test_all_connector_types_class_names() {
        let types = [
            (
                ConnectorType::Postgres,
                "io.debezium.connector.postgresql.PostgresConnector",
            ),
            (
                ConnectorType::Mysql,
                "io.debezium.connector.mysql.MySqlConnector",
            ),
            (
                ConnectorType::Mongodb,
                "io.debezium.connector.mongodb.MongoDbConnector",
            ),
            (
                ConnectorType::SqlServer,
                "io.debezium.connector.sqlserver.SqlServerConnector",
            ),
            (
                ConnectorType::Oracle,
                "io.debezium.connector.oracle.OracleConnector",
            ),
            (ConnectorType::Db2, "io.debezium.connector.db2.Db2Connector"),
            (
                ConnectorType::Cassandra,
                "io.debezium.connector.cassandra.CassandraConnector",
            ),
            (
                ConnectorType::Vitess,
                "io.debezium.connector.vitess.VitessConnector",
            ),
        ];
        for (ct, expected) in &types {
            assert_eq!(ct.debezium_class_name(), *expected);
        }
    }

    #[test]
    fn test_all_as_str_and_back() {
        let types = [
            ConnectorType::Postgres,
            ConnectorType::Mysql,
            ConnectorType::Mongodb,
            ConnectorType::SqlServer,
            ConnectorType::Oracle,
            ConnectorType::Db2,
            ConnectorType::Cassandra,
            ConnectorType::Vitess,
        ];
        for ct in &types {
            let s = ct.as_str();
            let back = ConnectorType::from_str(s).unwrap();
            assert_eq!(*ct, back);
        }
    }
}
