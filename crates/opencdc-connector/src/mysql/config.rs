use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MySqlConnectorConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub server_id: u32,
    pub tables: Vec<String>,
    pub heartbeat_interval: Duration,
    pub max_reconnect_attempts: u32,
}

impl MySqlConnectorConfig {
    pub fn new(
        host: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        password: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            user: user.into(),
            password: password.into(),
            database: database.into(),
            server_id: 1001,
            tables: Vec::new(),
            heartbeat_interval: Duration::from_secs(30),
            max_reconnect_attempts: 5,
        }
    }

    pub fn with_server_id(mut self, id: u32) -> Self {
        self.server_id = id;
        self
    }

    pub fn with_tables(mut self, tables: Vec<impl Into<String>>) -> Self {
        self.tables = tables.into_iter().map(|t| t.into()).collect();
        self
    }

    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    pub fn with_max_reconnect_attempts(mut self, attempts: u32) -> Self {
        self.max_reconnect_attempts = attempts;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_config_defaults() {
        let config = MySqlConnectorConfig::new("localhost", 3306, "root", "pass", "mydb");
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 3306);
        assert_eq!(config.user, "root");
        assert_eq!(config.password, "pass");
        assert_eq!(config.database, "mydb");
        assert_eq!(config.server_id, 1001);
        assert!(config.tables.is_empty());
        assert_eq!(config.heartbeat_interval, Duration::from_secs(30));
        assert_eq!(config.max_reconnect_attempts, 5);
    }

    #[test]
    fn test_mysql_config_builder() {
        let config = MySqlConnectorConfig::new("host1", 3307, "admin", "secret", "testdb")
            .with_server_id(2001)
            .with_tables(vec!["users", "orders"])
            .with_heartbeat_interval(Duration::from_secs(15))
            .with_max_reconnect_attempts(3);
        assert_eq!(config.server_id, 2001);
        assert_eq!(config.tables, vec!["users", "orders"]);
        assert_eq!(config.heartbeat_interval, Duration::from_secs(15));
        assert_eq!(config.max_reconnect_attempts, 3);
    }
}
