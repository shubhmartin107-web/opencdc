use std::time::Duration;

#[derive(Debug, Clone)]
pub struct PostgresConnectorConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    pub slot_name: String,
    pub publication: String,
    pub table_include: Vec<String>,
    pub heartbeat_interval: Duration,
    pub max_reconnect_attempts: u32,
}

impl Default for PostgresConnectorConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            database: "postgres".to_string(),
            username: "postgres".to_string(),
            password: String::new(),
            slot_name: "opencdc_slot".to_string(),
            publication: "opencdc_publication".to_string(),
            table_include: Vec::new(),
            heartbeat_interval: Duration::from_secs(10),
            max_reconnect_attempts: 5,
        }
    }
}

impl PostgresConnectorConfig {
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={}",
            self.host, self.port, self.database, self.username, self.password
        )
    }

    pub fn replication_connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={} replication=database",
            self.host, self.port, self.database, self.username, self.password
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_config_defaults() {
        let config = PostgresConnectorConfig::default();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.database, "postgres");
        assert_eq!(config.username, "postgres");
        assert_eq!(config.slot_name, "opencdc_slot");
        assert_eq!(config.publication, "opencdc_publication");
        assert!(config.table_include.is_empty());
        assert_eq!(config.heartbeat_interval, Duration::from_secs(10));
        assert_eq!(config.max_reconnect_attempts, 5);
    }

    #[test]
    fn test_postgres_connection_string() {
        let config = PostgresConnectorConfig {
            host: "pg.example.com".to_string(),
            port: 5432,
            database: "mydb".to_string(),
            username: "user1".to_string(),
            password: "pass1".to_string(),
            ..Default::default()
        };
        let conn_str = config.connection_string();
        assert!(conn_str.contains("host=pg.example.com"));
        assert!(conn_str.contains("port=5432"));
        assert!(conn_str.contains("dbname=mydb"));
        assert!(conn_str.contains("user=user1"));
        assert!(conn_str.contains("password=pass1"));
    }

    #[test]
    fn test_postgres_replication_connection_string() {
        let config = PostgresConnectorConfig::default();
        let conn_str = config.replication_connection_string();
        assert!(conn_str.contains("replication=database"));
    }
}
