mod pipeline;
mod source;

pub use pipeline::*;
pub use source::*;

use opencdc_core::error::Result;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub connector: SourceConfig,
    pub pipeline: Option<PipelineConfig>,
}

impl AppConfig {
    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            opencdc_core::error::Error::Other(format!(
                "failed to read config file '{}': {}",
                path.as_ref().display(),
                e
            ))
        })?;
        Self::from_toml(&content)
    }

    pub fn from_toml(toml_str: &str) -> Result<Self> {
        let raw: RawConfig = toml::from_str(toml_str).map_err(|e| {
            opencdc_core::error::Error::Other(format!("failed to parse TOML config: {}", e))
        })?;
        Self::from_raw(raw)
    }

    fn from_raw(raw: RawConfig) -> Result<Self> {
        let connector = SourceConfig::from_raw(raw.connector)?;
        let pipeline = raw.pipeline.map(PipelineConfig::from_raw).transpose()?;
        Ok(Self {
            connector,
            pipeline,
        })
    }
}

#[derive(serde::Deserialize)]
struct RawConfig {
    connector: toml::Value,
    pipeline: Option<toml::Value>,
}
