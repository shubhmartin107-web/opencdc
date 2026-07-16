use opencdc_core::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub transforms: Vec<TransformConfig>,
    pub sinks: Vec<SinkConfig>,
}

impl PipelineConfig {
    pub fn from_raw(value: toml::Value) -> Result<Self> {
        let table = value
            .as_table()
            .ok_or_else(|| Error::Other("pipeline config must be a table".to_string()))?;

        let transforms = match table.get("transforms") {
            Some(v) => {
                let arr = v
                    .as_array()
                    .ok_or_else(|| Error::Other("transforms must be an array".to_string()))?;
                arr.iter()
                    .map(|v| TransformConfig::from_raw(v.clone()))
                    .collect::<Result<Vec<_>>>()?
            }
            None => Vec::new(),
        };

        let sinks = match table.get("sinks") {
            Some(v) => {
                let arr = v
                    .as_array()
                    .ok_or_else(|| Error::Other("sinks must be an array".to_string()))?;
                arr.iter()
                    .map(|v| SinkConfig::from_raw(v.clone()))
                    .collect::<Result<Vec<_>>>()?
            }
            None => Vec::new(),
        };

        Ok(Self { transforms, sinks })
    }
}

#[derive(Debug, Clone)]
pub enum TransformConfig {
    Log,
    Filter(FilterTransformConfig),
    Rename(RenameTransformConfig),
}

impl TransformConfig {
    pub fn from_raw(value: toml::Value) -> Result<Self> {
        let table = value
            .as_table()
            .ok_or_else(|| Error::Other("transform config must be a table".to_string()))?;
        let ty = table
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Other("transform missing 'type'".to_string()))?;
        match ty {
            "log" => Ok(Self::Log),
            "filter" => {
                let cfg: FilterTransformConfig = value
                    .try_into()
                    .map_err(|e| Error::Other(format!("invalid filter config: {}", e)))?;
                Ok(Self::Filter(cfg))
            }
            "rename" => {
                let cfg: RenameTransformConfig = value
                    .try_into()
                    .map_err(|e| Error::Other(format!("invalid rename config: {}", e)))?;
                Ok(Self::Rename(cfg))
            }
            other => Err(Error::Other(format!("unknown transform type '{}'", other))),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct FilterTransformConfig {
    #[serde(default)]
    pub operations: Vec<String>,
    #[serde(default)]
    pub exclude_operations: Vec<String>,
    #[serde(default)]
    pub tables: Vec<String>,
    #[serde(default)]
    pub exclude_snapshot: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RenameTransformConfig {
    #[serde(default)]
    pub table_remap: std::collections::HashMap<String, String>,
    #[serde(default)]
    pub database_remap: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum SinkConfig {
    Stdout,
    Null,
    OpenLake(OpenLakeSinkConfig),
}

impl SinkConfig {
    pub fn from_raw(value: toml::Value) -> Result<Self> {
        let table = value
            .as_table()
            .ok_or_else(|| Error::Other("sink config must be a table".to_string()))?;
        let ty = table
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Other("sink missing 'type'".to_string()))?;
        match ty {
            "stdout" => Ok(Self::Stdout),
            "null" => Ok(Self::Null),
            "openlake" => {
                let cfg: OpenLakeSinkConfig = value
                    .try_into()
                    .map_err(|e| Error::Other(format!("invalid openlake config: {}", e)))?;
                Ok(Self::OpenLake(cfg))
            }
            other => Err(Error::Other(format!("unknown sink type '{}'", other))),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct OpenLakeSinkConfig {
    #[serde(default)]
    pub namespace: String,
    #[serde(default = "default_catalog_url")]
    pub catalog_url: String,
    #[serde(default)]
    pub auth_token: Option<String>,
}

fn default_catalog_url() -> String {
    "http://localhost:8181".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_config_empty() {
        let toml_str = "transforms = []\nsinks = []\n";
        let value: toml::Value = toml::from_str(toml_str).unwrap();
        let config = PipelineConfig::from_raw(value).unwrap();
        assert!(config.transforms.is_empty());
        assert!(config.sinks.is_empty());
    }

    #[test]
    fn test_pipeline_config_with_transforms_sinks() {
        let toml_str = r#"
transforms = [
    { type = "log" },
    { type = "filter", exclude_snapshot = true },
]
sinks = [
    { type = "stdout" },
    { type = "openlake", namespace = "cdc" },
]
"#;
        let value: toml::Value = toml::from_str(toml_str).unwrap();
        let config = PipelineConfig::from_raw(value).unwrap();
        assert_eq!(config.transforms.len(), 2);
        assert_eq!(config.sinks.len(), 2);
        assert!(matches!(config.transforms[0], TransformConfig::Log));
        assert!(matches!(config.transforms[1], TransformConfig::Filter(_)));
        assert!(matches!(config.sinks[0], SinkConfig::Stdout));
        assert!(matches!(config.sinks[1], SinkConfig::OpenLake(_)));
    }

    #[test]
    fn test_rename_transform_config() {
        let toml_str = r#"
type = "rename"
table_remap = { users = "cdc_users", orders = "cdc_orders" }
database_remap = { prod = "cdc" }
"#;
        let value: toml::Value = toml::from_str(toml_str).unwrap();
        let config = TransformConfig::from_raw(value).unwrap();
        match config {
            TransformConfig::Rename(c) => {
                assert_eq!(c.table_remap.get("users").unwrap(), "cdc_users");
                assert_eq!(c.table_remap.get("orders").unwrap(), "cdc_orders");
                assert_eq!(c.database_remap.get("prod").unwrap(), "cdc");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_unknown_transform_type() {
        let toml_str = r#"
type = "unknown"
"#;
        let value: toml::Value = toml::from_str(toml_str).unwrap();
        let result = TransformConfig::from_raw(value);
        assert!(result.is_err());
    }

    #[test]
    fn test_pipeline_no_transforms_sinks() {
        let value: toml::Value = toml::from_str("").unwrap();
        let config = PipelineConfig::from_raw(value).unwrap();
        assert!(config.transforms.is_empty());
        assert!(config.sinks.is_empty());
    }
}
