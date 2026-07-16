use std::sync::Arc;

use rmcp::{ServiceExt, transport::stdio};
use tokio::signal;
use tracing_subscriber::EnvFilter;

use opencdc_mcp::service::McpService;
use opencdc_mcp::state::AppState;

mod health;

fn init_tracing() {
    let is_json = std::env::var("OPENCDC_LOG_FORMAT").as_deref() == Ok("json");

    let builder = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .with_writer(std::io::stderr)
        .with_ansi(false);

    if is_json {
        builder.json().init();
        tracing::info!("JSON structured logging enabled");
    } else {
        builder.init();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    tracing::info!("Starting OpenCDC MCP server");

    let state = Arc::new(AppState::new());
    let service = McpService::new(state.clone());

    let health_port: u16 = std::env::var("OPENCDC_HEALTH_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    if health_port > 0 {
        let health_state = state.clone();
        tokio::spawn(async move {
            health::serve_health(health_port, health_state).await;
        });
        tracing::info!("Health endpoint listening on port {}", health_port);
    } else {
        tracing::info!("Health endpoint disabled (set OPENCDC_HEALTH_PORT to enable)");
    }

    let transport = stdio();
    let server = service.serve(transport).await.inspect_err(|e| {
        tracing::error!("MCP server error: {:?}", e);
    })?;

    tracing::info!("OpenCDC MCP server running on stdio");

    tokio::select! {
        _ = server.waiting() => {}
        _ = signal::ctrl_c() => {
            tracing::info!("Received SIGINT, initiating graceful shutdown...");
        }
    }

    state.shutdown_all().await;
    tracing::info!("OpenCDC MCP server stopped");

    Ok(())
}
