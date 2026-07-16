use opencdc_core::error::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRegistryConfig {
    pub url: String,
    pub auth_token: Option<String>,
    pub timeout_secs: u64,
}

impl Default for SchemaRegistryConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:8081".to_string(),
            auth_token: None,
            timeout_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredSchema {
    pub id: u32,
    pub version: u32,
    pub subject: String,
    pub schema_type: String,
    pub schema: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityConfig {
    pub level: CompatibilityLevel,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum CompatibilityLevel {
    None,
    #[default]
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
}

#[derive(Debug, Clone)]
pub struct SchemaRegistryClient {
    config: SchemaRegistryConfig,
    client: reqwest::Client,
}

impl SchemaRegistryClient {
    pub fn new(config: SchemaRegistryConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| {
                opencdc_core::error::Error::Other(format!(
                    "failed to create schema registry http client: {}",
                    e
                ))
            })?;

        Ok(Self { config, client })
    }

    pub fn url(&self) -> &str {
        &self.config.url
    }

    pub async fn register_subject(
        &self,
        subject: &str,
        schema_type: &str,
        schema: &str,
    ) -> Result<u32> {
        let url = format!("{}/subjects/{}/versions", self.config.url, subject);
        let body = serde_json::json!({
            "schemaType": schema_type,
            "schema": schema,
        });

        let response = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                opencdc_core::error::Error::Other(format!(
                    "schema registry request failed: {}",
                    e
                ))
            })?;

        if !response.status().is_success() {
            return Err(opencdc_core::error::Error::Other(format!(
                "schema registry returned {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let result: serde_json::Value = response.json().await.map_err(|e| {
            opencdc_core::error::Error::Deserialization(format!(
                "failed to parse schema registry response: {}",
                e
            ))
        })?;

        result["id"]
            .as_u64()
            .map(|id| id as u32)
            .ok_or_else(|| {
                opencdc_core::error::Error::Deserialization(
                    "schema registry response missing 'id'".to_string(),
                )
            })
    }

    pub async fn get_schema_by_id(&self, id: u32) -> Result<RegisteredSchema> {
        let url = format!("{}/schemas/ids/{}", self.config.url, id);

        let response = self.client.get(&url).send().await.map_err(|e| {
            opencdc_core::error::Error::Other(format!(
                "schema registry request failed: {}",
                e
            ))
        })?;

        if !response.status().is_success() {
            return Err(opencdc_core::error::Error::Other(format!(
                "schema registry returned {} for schema id {}",
                response.status(),
                id
            )));
        }

        let result: serde_json::Value = response.json().await.map_err(|e| {
            opencdc_core::error::Error::Deserialization(format!(
                "failed to parse schema registry response: {}",
                e
            ))
        })?;

        let id = result["id"].as_u64().ok_or_else(|| {
            opencdc_core::error::Error::Deserialization(
                "schema registry response missing 'id'".to_string(),
            )
        })? as u32;
        let version = result["version"].as_u64().ok_or_else(|| {
            opencdc_core::error::Error::Deserialization(
                "schema registry response missing 'version'".to_string(),
            )
        })? as u32;
        let subject = result["subject"].as_str().ok_or_else(|| {
            opencdc_core::error::Error::Deserialization(
                "schema registry response missing 'subject'".to_string(),
            )
        })?;
        let schema_type = result["schemaType"].as_str().unwrap_or("AVRO");
        let schema = result["schema"].as_str().ok_or_else(|| {
            opencdc_core::error::Error::Deserialization(
                "schema registry response missing 'schema'".to_string(),
            )
        })?;

        Ok(RegisteredSchema {
            id,
            version,
            subject: subject.to_string(),
            schema_type: schema_type.to_string(),
            schema: schema.to_string(),
        })
    }

    pub async fn get_latest_schema(
        &self,
        subject: &str,
    ) -> Result<RegisteredSchema> {
        let url = format!(
            "{}/subjects/{}/versions/latest",
            self.config.url, subject
        );

        let response = self.client.get(&url).send().await.map_err(|e| {
            opencdc_core::error::Error::Other(format!(
                "schema registry request failed: {}",
                e
            ))
        })?;

        if !response.status().is_success() {
            return Err(opencdc_core::error::Error::Other(format!(
                "schema registry returned {} for subject {}",
                response.status(),
                subject
            )));
        }

        let result: serde_json::Value = response.json().await.map_err(|e| {
            opencdc_core::error::Error::Deserialization(format!(
                "failed to parse schema registry response: {}",
                e
            ))
        })?;

        let id = result["id"].as_u64().ok_or_else(|| {
            opencdc_core::error::Error::Deserialization(
                "schema registry response missing 'id'".to_string(),
            )
        })? as u32;
        let version = result["version"].as_u64().ok_or_else(|| {
            opencdc_core::error::Error::Deserialization(
                "schema registry response missing 'version'".to_string(),
            )
        })? as u32;
        let subject = result["subject"].as_str().ok_or_else(|| {
            opencdc_core::error::Error::Deserialization(
                "schema registry response missing 'subject'".to_string(),
            )
        })?;
        let schema_type = result["schemaType"].as_str().unwrap_or("AVRO");
        let schema = result["schema"].as_str().ok_or_else(|| {
            opencdc_core::error::Error::Deserialization(
                "schema registry response missing 'schema'".to_string(),
            )
        })?;

        Ok(RegisteredSchema {
            id,
            version,
            subject: subject.to_string(),
            schema_type: schema_type.to_string(),
            schema: schema.to_string(),
        })
    }

    pub async fn subjects(&self) -> Result<Vec<String>> {
        let url = format!("{}/subjects", self.config.url);
        let response = self.client.get(&url).send().await.map_err(|e| {
            opencdc_core::error::Error::Other(format!(
                "schema registry request failed: {}",
                e
            ))
        })?;

        if !response.status().is_success() {
            return Err(opencdc_core::error::Error::Other(format!(
                "schema registry returned {}",
                response.status()
            )));
        }

        let result: Vec<String> = response.json().await.map_err(|e| {
            opencdc_core::error::Error::Deserialization(format!(
                "failed to parse schema registry response: {}",
                e
            ))
        })?;

        Ok(result)
    }

    pub async fn delete_subject(&self, subject: &str) -> Result<()> {
        let url = format!("{}/subjects/{}", self.config.url, subject);
        let response = self.client.delete(&url).send().await.map_err(|e| {
            opencdc_core::error::Error::Other(format!(
                "schema registry request failed: {}",
                e
            ))
        })?;

        if !response.status().is_success() {
            return Err(opencdc_core::error::Error::Other(format!(
                "schema registry returned {} for subject {}",
                response.status(),
                subject
            )));
        }

        Ok(())
    }

    pub async fn check_compatibility(
        &self,
        subject: &str,
        schema_type: &str,
        schema: &str,
    ) -> Result<bool> {
        let url = format!(
            "{}/compatibility/subjects/{}/versions/latest",
            self.config.url, subject
        );
        let body = serde_json::json!({
            "schemaType": schema_type,
            "schema": schema,
        });

        let response = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                opencdc_core::error::Error::Other(format!(
                    "schema registry request failed: {}",
                    e
                ))
            })?;

        if !response.status().is_success() {
            return Err(opencdc_core::error::Error::Other(format!(
                "schema registry returned {}",
                response.status()
            )));
        }

        let result: serde_json::Value = response.json().await.map_err(|e| {
            opencdc_core::error::Error::Deserialization(format!(
                "failed to parse schema registry response: {}",
                e
            ))
        })?;

        Ok(result["is_compatible"].as_bool().unwrap_or(false))
    }

    pub async fn config(&self) -> Result<CompatibilityConfig> {
        let url = format!("{}/config", self.config.url);
        let response = self.client.get(&url).send().await.map_err(|e| {
            opencdc_core::error::Error::Other(format!(
                "schema registry request failed: {}",
                e
            ))
        })?;

        if !response.status().is_success() {
            return Err(opencdc_core::error::Error::Other(format!(
                "schema registry returned {}",
                response.status()
            )));
        }

        let result: serde_json::Value = response.json().await.map_err(|e| {
            opencdc_core::error::Error::Deserialization(format!(
                "failed to parse schema registry response: {}",
                e
            ))
        })?;

        let level = result["compatibilityLevel"]
            .as_str()
            .and_then(|s| match s.to_uppercase().as_str() {
                "NONE" => Some(CompatibilityLevel::None),
                "BACKWARD" => Some(CompatibilityLevel::Backward),
                "BACKWARD_TRANSITIVE" => Some(CompatibilityLevel::BackwardTransitive),
                "FORWARD" => Some(CompatibilityLevel::Forward),
                "FORWARD_TRANSITIVE" => Some(CompatibilityLevel::ForwardTransitive),
                "FULL" => Some(CompatibilityLevel::Full),
                "FULL_TRANSITIVE" => Some(CompatibilityLevel::FullTransitive),
                _ => None,
            })
            .unwrap_or(CompatibilityLevel::Backward);

        Ok(CompatibilityConfig { level })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_config_default() {
        let config = SchemaRegistryConfig::default();
        assert_eq!(config.url, "http://localhost:8081");
        assert_eq!(config.timeout_secs, 30);
        assert!(config.auth_token.is_none());
    }

    #[test]
    fn test_compatibility_level_default() {
        assert_eq!(CompatibilityLevel::default(), CompatibilityLevel::Backward);
    }

    #[test]
    fn test_compatibility_level_roundtrip() {
        let levels = [
            CompatibilityLevel::None,
            CompatibilityLevel::Backward,
            CompatibilityLevel::BackwardTransitive,
            CompatibilityLevel::Forward,
            CompatibilityLevel::ForwardTransitive,
            CompatibilityLevel::Full,
            CompatibilityLevel::FullTransitive,
        ];
        for level in &levels {
            let json = serde_json::to_string(level).unwrap();
            let deserialized: CompatibilityLevel = serde_json::from_str(&json).unwrap();
            assert_eq!(*level, deserialized);
        }
    }

    #[test]
    fn test_compatibility_config_roundtrip() {
        let config = CompatibilityConfig {
            level: CompatibilityLevel::ForwardTransitive,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: CompatibilityConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.level, CompatibilityLevel::ForwardTransitive);
    }

    #[test]
    fn test_registered_schema_roundtrip() {
        let schema = RegisteredSchema {
            id: 42,
            version: 3,
            subject: "test-value".to_string(),
            schema_type: "AVRO".to_string(),
            schema: r#"{"type":"record","name":"Test"}"#.to_string(),
        };
        let json = serde_json::to_value(&schema).unwrap();
        assert_eq!(json["id"], 42);
        assert_eq!(json["version"], 3);
        assert_eq!(json["subject"], "test-value");
        assert_eq!(json["schema_type"], "AVRO");

        let deserialized: RegisteredSchema = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized.id, schema.id);
        assert_eq!(deserialized.version, schema.version);
        assert_eq!(deserialized.subject, schema.subject);
        assert_eq!(deserialized.schema_type, schema.schema_type);
        assert_eq!(deserialized.schema, schema.schema);
    }

    #[test]
    fn test_schema_registry_config_with_auth() {
        let config = SchemaRegistryConfig {
            url: "https://sr.example.com".to_string(),
            auth_token: Some("token123".to_string()),
            timeout_secs: 60,
        };
        assert_eq!(config.auth_token.as_deref(), Some("token123"));
        assert_eq!(config.url, "https://sr.example.com");
        assert_eq!(config.timeout_secs, 60);
    }

    #[test]
    fn test_registry_client_new() {
        let config = SchemaRegistryConfig::default();
        let client = SchemaRegistryClient::new(config);
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.url(), "http://localhost:8081");
    }
}
