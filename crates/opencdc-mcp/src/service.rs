use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    handler::server::wrapper::Parameters,
    model::*,
    schemars,
    service::RequestContext,
    tool, tool_handler, tool_router,
};
use serde_json::json;

use crate::state::{AppState, ConnectorStatus, ManagedConnector};

#[derive(Clone)]
pub struct McpService {
    state: std::sync::Arc<AppState>,
}

impl std::fmt::Debug for McpService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpService").finish_non_exhaustive()
    }
}

#[tool_router]
impl McpService {
    pub fn new(state: std::sync::Arc<AppState>) -> Self {
        Self { state }
    }

    fn connector_info_json(
        name: &str,
        mc: &ManagedConnector,
    ) -> serde_json::Value {
        json!({
            "name": name,
            "status": format!("{:?}", mc.status),
            "events_received": mc.events_received,
            "error": mc.error,
        })
    }

    #[tool(description = "List all registered CDC connectors and their status")]
    async fn connector_list(&self) -> Result<CallToolResult, McpError> {
        let _permit = self.state.acquire_tool_permit().await.ok_or_else(|| {
            McpError::internal_error("rate_limited", Some(json!({"detail": "server is shutting down"})))
        })?;
        let list = self.state.list_connectors().await;
        let connectors: Vec<serde_json::Value> = list
            .iter()
            .map(|(name, mc)| Self::connector_info_json(name, mc))
            .collect();

        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&connectors).unwrap_or_default(),
        )]))
    }

    #[tool(description = "Register a new CDC connector")]
    async fn connector_register(
        &self,
        Parameters(args): Parameters<ConnectorRegisterArgs>,
    ) -> Result<CallToolResult, McpError> {
        let _permit = self.state.acquire_tool_permit().await.ok_or_else(|| {
            McpError::internal_error("rate_limited", Some(json!({"detail": "server is shutting down"})))
        })?;
        self.state.register_connector(&args.name).await;

        let list = self.state.list_connectors().await;
        let names: Vec<&str> = list.iter().map(|(n, _)| n.as_str()).collect();
        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::to_string_pretty(&json!({
                "registered": args.name,
                "all_connectors": names
            }))
            .unwrap_or_default(),
        )]))
    }

    #[tool(description = "Remove a registered CDC connector")]
    async fn connector_remove(
        &self,
        Parameters(args): Parameters<ConnectorNameArgs>,
    ) -> Result<CallToolResult, McpError> {
        let _permit = self.state.acquire_tool_permit().await.ok_or_else(|| {
            McpError::internal_error("rate_limited", Some(json!({"detail": "server is shutting down"})))
        })?;
        {
            let mut connectors = self.state.connectors.write().await;
            connectors.remove(&args.name);
        }

        Ok(CallToolResult::success(vec![ContentBlock::text(
            json!({"removed": args.name}).to_string(),
        )]))
    }

    #[tool(description = "Get detailed status of a specific connector")]
    async fn connector_status(
        &self,
        Parameters(args): Parameters<ConnectorNameArgs>,
    ) -> Result<CallToolResult, McpError> {
        let _permit = self.state.acquire_tool_permit().await.ok_or_else(|| {
            McpError::internal_error("rate_limited", Some(json!({"detail": "server is shutting down"})))
        })?;
        let connectors = self.state.connectors.read().await;
        match connectors.get(&args.name) {
            Some(mc) => Ok(CallToolResult::success(vec![ContentBlock::text(
                serde_json::to_string_pretty(&Self::connector_info_json(&args.name, mc))
                    .unwrap_or_default(),
            )])),
            None => Err(McpError::invalid_params(
                "connector_not_found",
                Some(json!({"connector": args.name})),
            )),
        }
    }

    #[tool(description = "Run a snapshot for a connector, capturing current table data")]
    async fn snapshot_start(
        &self,
        Parameters(args): Parameters<SnapshotStartArgs>,
    ) -> Result<CallToolResult, McpError> {
        let _permit = self.state.acquire_tool_permit().await.ok_or_else(|| {
            McpError::internal_error("rate_limited", Some(json!({"detail": "server is shutting down"})))
        })?;
        let status = {
            let connectors = self.state.connectors.read().await;
            connectors.get(&args.connector).map(|c| c.status.clone())
        };

        match status {
            Some(ConnectorStatus::Running) | Some(ConnectorStatus::Stopped) => {}
            Some(s) => {
                return Err(McpError::invalid_params(
                    "invalid_state",
                    Some(json!({
                        "connector": args.connector,
                        "status": format!("{:?}", s),
                        "expected": "Running or Stopped"
                    })),
                ));
            }
            None => {
                return Err(McpError::invalid_params(
                    "connector_not_found",
                    Some(json!({"connector": args.connector})),
                ));
            }
        }

        let tables = args.tables.unwrap_or_default();
        AppState::run_snapshot(&self.state, &args.connector, tables).await;

        Ok(CallToolResult::success(vec![ContentBlock::text(
            json!({
                "connector": args.connector,
                "action": "snapshot_started",
                "note": "Snapshot running in background task. Use connector_status to monitor progress."
            })
            .to_string(),
        )]))
    }

    #[tool(description = "List subjects in the Schema Registry")]
    async fn schema_registry_subjects(&self) -> Result<CallToolResult, McpError> {
        let _permit = self.state.acquire_tool_permit().await.ok_or_else(|| {
            McpError::internal_error("rate_limited", Some(json!({"detail": "server is shutting down"})))
        })?;
        match &self.state.schema_registry_client {
            Some(client) => match client.subjects().await {
                Ok(subjects) => Ok(CallToolResult::success(vec![ContentBlock::text(
                    serde_json::to_string_pretty(&json!({"subjects": subjects})).unwrap_or_default(),
                )])),
                Err(e) => Err(McpError::internal_error(
                    "schema_registry_error",
                    Some(json!({"detail": format!("{e}")})),
                )),
            },
            None => Err(McpError::invalid_params(
                "schema_registry_not_configured",
                Some(json!({"hint": "Set OPENCDC_SCHEMA_REGISTRY_URL env var"})),
            )),
        }
    }

    #[tool(
        description = "Get a registered schema from the Schema Registry by its global ID"
    )]
    async fn schema_registry_get(
        &self,
        Parameters(args): Parameters<SchemaGetArgs>,
    ) -> Result<CallToolResult, McpError> {
        let _permit = self.state.acquire_tool_permit().await.ok_or_else(|| {
            McpError::internal_error("rate_limited", Some(json!({"detail": "server is shutting down"})))
        })?;
        match &self.state.schema_registry_client {
            Some(client) => match client.get_schema_by_id(args.id).await {
                Ok(registered) => {
                    let result = json!({
                        "id": registered.id,
                        "schema": registered.schema,
                        "schemaType": registered.schema_type,
                        "subject": registered.subject,
                        "version": registered.version,
                    });
                    Ok(CallToolResult::success(vec![ContentBlock::text(
                        serde_json::to_string_pretty(&result).unwrap_or_default(),
                    )]))
                }
                Err(e) => Err(McpError::internal_error(
                    "schema_registry_error",
                    Some(json!({"detail": format!("{e}")})),
                )),
            },
            None => Err(McpError::invalid_params(
                "schema_registry_not_configured",
                Some(json!({"hint": "Set OPENCDC_SCHEMA_REGISTRY_URL env var"})),
            )),
        }
    }
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ConnectorRegisterArgs {
    pub name: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ConnectorNameArgs {
    pub name: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SnapshotStartArgs {
    pub connector: String,
    pub tables: Option<Vec<String>>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SchemaGetArgs {
    pub id: u32,
}

#[tool_handler]
impl ServerHandler for McpService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .build(),
        )
        .with_server_info(Implementation::from_build_env())
        .with_protocol_version(ProtocolVersion::V_2024_11_05)
        .with_instructions(
            "OpenCDC MCP Server - Change Data Capture management.\n\
             Tools:\n\
             - connector_list: List all connectors\n\
             - connector_register: Register a new connector\n\
             - connector_remove: Remove a connector\n\
             - connector_status: Get connector status\n\
             - snapshot_start: Run a table snapshot\n\
             - schema_registry_subjects: List schema registry subjects\n\
             - schema_registry_get: Get a schema by ID"
            .to_string(),
        )
    }

    async fn initialize(
        &self,
        _request: InitializeRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, McpError> {
        Ok(self.get_info())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::ConnectorStatus;

    #[test]
    fn test_connector_info_json() {
        let mc = ManagedConnector {
            config: None,
            status: ConnectorStatus::Running,
            offset: None,
            events_received: 42,
            error: None,
        };
        let json = McpService::connector_info_json("test", &mc);
        assert_eq!(json["name"], "test");
        assert_eq!(json["status"], "Running");
        assert_eq!(json["events_received"], 42);
    }

    #[tokio::test]
    async fn test_connector_register_and_list() {
        let state = std::sync::Arc::new(AppState::new());
        let _service = McpService::new(state.clone());

        state.register_connector("pg1").await;
        state.register_connector("pg2").await;

        let list = state.list_connectors().await;
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].0, "pg1");
        assert_eq!(list[1].0, "pg2");
    }

    #[tokio::test]
    async fn test_connector_register_and_remove() {
        let state = std::sync::Arc::new(AppState::new());
        state.register_connector("test1").await;
        assert_eq!(state.list_connectors().await.len(), 1);

        {
            let mut connectors = state.connectors.write().await;
            connectors.remove("test1");
        }
        assert_eq!(state.list_connectors().await.len(), 0);
    }

    #[tokio::test]
    async fn test_connector_status_update() {
        let state = std::sync::Arc::new(AppState::new());
        state.register_connector("pg").await;
        state
            .update_status("pg", ConnectorStatus::Snapshotting)
            .await;

        let connectors = state.connectors.read().await;
        let mc = connectors.get("pg").unwrap();
        assert_eq!(mc.status, ConnectorStatus::Snapshotting);
    }

    #[tokio::test]
    async fn test_connector_list_empty() {
        let state = std::sync::Arc::new(AppState::new());
        let list = state.list_connectors().await;
        assert!(list.is_empty());
    }

    #[test]
    fn test_connector_register_args_deserialize() {
        let json = serde_json::json!({"name": "test_connector"});
        let args: ConnectorRegisterArgs = serde_json::from_value(json).unwrap();
        assert_eq!(args.name, "test_connector");
    }

    #[test]
    fn test_snapshot_start_args_deserialize() {
        let json = serde_json::json!({
            "connector": "pg1",
            "tables": ["users", "orders"]
        });
        let args: SnapshotStartArgs = serde_json::from_value(json).unwrap();
        assert_eq!(args.connector, "pg1");
        assert_eq!(args.tables.unwrap(), vec!["users", "orders"]);
    }

    #[test]
    fn test_connector_name_args_deserialize() {
        let json = serde_json::json!({"name": "my_connector"});
        let args: ConnectorNameArgs = serde_json::from_value(json).unwrap();
        assert_eq!(args.name, "my_connector");
    }
}
