#[derive(Debug, Clone)]
pub struct MongoDbConnectorConfig {
    pub connection_string: String,
    pub database: String,
    pub collections: Vec<String>,
    pub max_reconnect_attempts: u32,
}

impl MongoDbConnectorConfig {
    pub fn new(connection_string: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            database: database.into(),
            collections: Vec::new(),
            max_reconnect_attempts: 5,
        }
    }

    pub fn with_collections(mut self, collections: Vec<impl Into<String>>) -> Self {
        self.collections = collections.into_iter().map(|c| c.into()).collect();
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
    fn test_mongodb_config_defaults() {
        let config = MongoDbConnectorConfig::new("mongodb://localhost:27017", "test_db");
        assert_eq!(config.connection_string, "mongodb://localhost:27017");
        assert_eq!(config.database, "test_db");
        assert!(config.collections.is_empty());
        assert_eq!(config.max_reconnect_attempts, 5);
    }

    #[test]
    fn test_mongodb_config_with_collections() {
        let config = MongoDbConnectorConfig::new("mongodb://mongo:27017", "mydb")
            .with_collections(vec!["users", "orders"])
            .with_max_reconnect_attempts(3);
        assert_eq!(config.collections, vec!["users", "orders"]);
        assert_eq!(config.max_reconnect_attempts, 3);
    }
}
