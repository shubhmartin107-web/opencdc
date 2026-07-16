use opencdc_core::error::{Error, Result};

#[derive(Debug, Clone)]
pub enum SourceConfig {
    Postgres(PostgresSourceConfig),
    MySql(MySqlSourceConfig),
    MongoDb(MongoDbSourceConfig),
}

impl SourceConfig {
    pub fn connector_type(&self) -> opencdc_core::ConnectorType {
        match self {
            Self::Postgres(_) => opencdc_core::ConnectorType::Postgres,
            Self::MySql(_) => opencdc_core::ConnectorType::Mysql,
            Self::MongoDb(_) => opencdc_core::ConnectorType::Mongodb,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Postgres(c) => &c.name,
            Self::MySql(c) => &c.name,
            Self::MongoDb(c) => &c.name,
        }
    }

    pub fn from_raw(value: toml::Value) -> Result<Self> {
        let table = value.as_table().ok_or_else(|| {
            Error::Other("connector config must be a table".to_string())
        })?;

        let connector_type = table.get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Other("connector config missing 'type' field".to_string()))?;

        match connector_type {
            "postgres" | "postgresql" => {
                let cfg: PostgresSourceConfig = value.try_into().map_err(|e| {
                    Error::Other(format!("invalid postgres config: {}", e))
                })?;
                Ok(Self::Postgres(cfg))
            }
            "mysql" | "mariadb" => {
                let cfg: MySqlSourceConfig = value.try_into().map_err(|e| {
                    Error::Other(format!("invalid mysql config: {}", e))
                })?;
                Ok(Self::MySql(cfg))
            }
            "mongodb" | "mongo" => {
                let cfg: MongoDbSourceConfig = value.try_into().map_err(|e| {
                    Error::Other(format!("invalid mongodb config: {}", e))
                })?;
                Ok(Self::MongoDb(cfg))
            }
            other => Err(Error::Other(format!(
                "unsupported connector type '{}'", other
            ))),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PostgresSourceConfig {
    pub name: String,
    #[serde(default = "default_pg_host")]
    pub host: String,
    #[serde(default = "default_pg_port")]
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    #[serde(default = "default_slot_name")]
    pub slot_name: String,
    #[serde(default = "default_publication")]
    pub publication: String,
    #[serde(default)]
    pub table_include: Vec<String>,
}

fn default_pg_host() -> String { "localhost".to_string() }
fn default_pg_port() -> u16 { 5432 }
fn default_slot_name() -> String { "opencdc_slot".to_string() }
fn default_publication() -> String { "opencdc_publication".to_string() }

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MySqlSourceConfig {
    pub name: String,
    #[serde(default = "default_mysql_host")]
    pub host: String,
    #[serde(default = "default_mysql_port")]
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    #[serde(default = "default_server_id")]
    pub server_id: u32,
    #[serde(default)]
    pub tables: Vec<String>,
}

fn default_mysql_host() -> String { "localhost".to_string() }
fn default_mysql_port() -> u16 { 3306 }
fn default_server_id() -> u32 { 1001 }

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MongoDbSourceConfig {
    pub name: String,
    pub connection_string: String,
    pub database: String,
    #[serde(default)]
    pub collections: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_config_postgres() {
        let toml_str = r#"
type = "postgres"
name = "pg1"
database = "mydb"
username = "user"
password = "pass"
"#;
        let value: toml::Value = toml::from_str(toml_str).unwrap();
        let config = SourceConfig::from_raw(value).unwrap();
        assert_eq!(config.name(), "pg1");
        assert_eq!(config.connector_type(), opencdc_core::ConnectorType::Postgres);
        match &config {
            SourceConfig::Postgres(c) => {
                assert_eq!(c.host, "localhost");
                assert_eq!(c.port, 5432);
                assert_eq!(c.database, "mydb");
                assert_eq!(c.slot_name, "opencdc_slot");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_source_config_mysql() {
        let toml_str = r#"
type = "mysql"
name = "mysql1"
database = "testdb"
user = "root"
password = "secret"
"#;
        let value: toml::Value = toml::from_str(toml_str).unwrap();
        let config = SourceConfig::from_raw(value).unwrap();
        assert_eq!(config.name(), "mysql1");
        assert_eq!(config.connector_type(), opencdc_core::ConnectorType::Mysql);
        match &config {
            SourceConfig::MySql(c) => {
                assert_eq!(c.host, "localhost");
                assert_eq!(c.port, 3306);
                assert_eq!(c.server_id, 1001);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_source_config_mongodb() {
        let toml_str = r#"
type = "mongodb"
name = "mongo1"
connection_string = "mongodb://localhost:27017"
database = "mydb"
collections = ["users", "orders"]
"#;
        let value: toml::Value = toml::from_str(toml_str).unwrap();
        let config = SourceConfig::from_raw(value).unwrap();
        assert_eq!(config.name(), "mongo1");
        assert_eq!(config.connector_type(), opencdc_core::ConnectorType::Mongodb);
        match &config {
            SourceConfig::MongoDb(c) => {
                assert_eq!(c.collections, vec!["users", "orders"]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_source_config_invalid_type() {
        let toml_str = r#"
type = "invalid"
name = "x"
"#;
        let value: toml::Value = toml::from_str(toml_str).unwrap();
        let result = SourceConfig::from_raw(value);
        assert!(result.is_err());
    }

    #[test]
    fn test_source_config_missing_type() {
        let toml_str = r#"
name = "x"
"#;
        let value: toml::Value = toml::from_str(toml_str).unwrap();
        let result = SourceConfig::from_raw(value);
        assert!(result.is_err());
    }

    #[test]
    fn test_postgres_config_override_host_port() {
        let toml_str = r#"
type = "postgres"
name = "pg-remote"
host = "pg.example.com"
port = 5433
database = "analytics"
username = "admin"
password = "pass123"
slot_name = "my_slot"
publication = "my_pub"
table_include = ["public.users", "public.orders"]
"#;
        let value: toml::Value = toml::from_str(toml_str).unwrap();
        let config = SourceConfig::from_raw(value).unwrap();
        match &config {
            SourceConfig::Postgres(c) => {
                assert_eq!(c.host, "pg.example.com");
                assert_eq!(c.port, 5433);
                assert_eq!(c.slot_name, "my_slot");
                assert_eq!(c.publication, "my_pub");
                assert_eq!(c.table_include, vec!["public.users", "public.orders"]);
            }
            _ => panic!("wrong variant"),
        }
    }
}
