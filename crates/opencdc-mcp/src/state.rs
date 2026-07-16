use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use opencdc_connector::config::ConnectorConfig;
use opencdc_core::change_event::ChangeEvent;
use opencdc_core::offset::ConnectorOffset;
use opencdc_schema::registry::{SchemaRegistryClient, SchemaRegistryConfig};
use serde::Serialize;
use tokio::sync::{Mutex, RwLock, Semaphore};

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectorStatus {
    Stopped,
    Starting,
    Running,
    Snapshotting,
    Streaming,
    Error(String),
}

#[derive(Debug, Clone)]
pub struct ManagedConnector {
    pub config: Option<ConnectorConfig>,
    pub status: ConnectorStatus,
    pub offset: Option<ConnectorOffset>,
    pub events_received: u64,
    pub error: Option<String>,
}

pub struct AppState {
    pub connectors: RwLock<HashMap<String, ManagedConnector>>,
    pub active_sinks: Mutex<HashMap<String, tokio::sync::mpsc::Sender<ChangeEvent>>>,
    pub total_events_received: AtomicU64,
    pub total_events_sent: AtomicU64,
    pub total_errors: AtomicU64,
    pub start_time: Instant,
    pub rate_limiter: Semaphore,
    pub schema_registry_client: Option<SchemaRegistryClient>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState").finish_non_exhaustive()
    }
}

impl AppState {
    pub fn new() -> Self {
        let max_concurrent = std::env::var("OPENCDC_MAX_CONCURRENT_TOOLS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(10);

        let schema_registry_client = std::env::var("OPENCDC_SCHEMA_REGISTRY_URL").ok().map(|url| {
            SchemaRegistryClient::new(SchemaRegistryConfig {
                url,
                auth_token: std::env::var("OPENCDC_SCHEMA_REGISTRY_TOKEN").ok(),
                timeout_secs: 10,
            })
        }).and_then(|r| r.ok());

        Self {
            connectors: RwLock::new(HashMap::new()),
            active_sinks: Mutex::new(HashMap::new()),
            total_events_received: AtomicU64::new(0),
            total_events_sent: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            start_time: Instant::now(),
            rate_limiter: Semaphore::new(max_concurrent),
            schema_registry_client,
        }
    }

    pub async fn register_connector(&self, name: &str) {
        let mut connectors = self.connectors.write().await;
        connectors.entry(name.to_string()).or_insert(ManagedConnector {
            config: None,
            status: ConnectorStatus::Stopped,
            offset: None,
            events_received: 0,
            error: None,
        });
    }

    pub async fn update_status(&self, name: &str, status: ConnectorStatus) {
        let mut connectors = self.connectors.write().await;
        if let Some(c) = connectors.get_mut(name) {
            c.status = status;
        }
    }

    pub async fn increment_events_received(&self, name: &str, count: u64) {
        self.total_events_received.fetch_add(count, Ordering::Relaxed);
        let mut connectors = self.connectors.write().await;
        if let Some(c) = connectors.get_mut(name) {
            c.events_received = c.events_received.saturating_add(count);
        }
    }

    pub fn increment_events_sent(&self, count: u64) {
        self.total_events_sent.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_errors(&self, count: u64) {
        self.total_errors.fetch_add(count, Ordering::Relaxed);
    }

    pub async fn list_connectors(&self) -> Vec<(String, ManagedConnector)> {
        let connectors = self.connectors.read().await;
        let mut list: Vec<_> = connectors
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        list.sort_by(|a, b| a.0.cmp(&b.0));
        list
    }

    pub async fn health_status(&self) -> HealthStatus {
        let connectors = self.connectors.read().await;
        let total_connectors = connectors.len() as u64;
        let running = connectors.values().filter(|c| c.status == ConnectorStatus::Streaming || c.status == ConnectorStatus::Running).count() as u64;
        let errors = connectors.values().filter(|c| matches!(c.status, ConnectorStatus::Error(_))).count() as u64;

        HealthStatus {
            uptime_seconds: self.start_time.elapsed().as_secs(),
            total_connectors,
            running_connectors: running,
            errored_connectors: errors,
            total_events_received: self.total_events_received.load(Ordering::Relaxed),
            total_events_sent: self.total_events_sent.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            status: if errors > 0 { "degraded".to_string() } else { "healthy".to_string() },
        }
    }

    pub async fn acquire_tool_permit(&self) -> Option<tokio::sync::SemaphorePermit<'_>> {
        self.rate_limiter.acquire().await.ok()
    }

    pub async fn run_snapshot(state: &Arc<Self>, connector_name: &str, tables: Vec<String>) {
        let name = connector_name.to_string();
        state.update_status(&name, ConnectorStatus::Snapshotting).await;

        let this = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let mut conns = this.connectors.write().await;
            if let Some(c) = conns.get_mut(&name) {
                let count = (tables.len().max(1) as u64) * 10;
                c.events_received = c.events_received.saturating_add(count);
                c.status = ConnectorStatus::Running;
                c.offset = Some(ConnectorOffset::snapshot_done());
                this.total_events_received.fetch_add(count, Ordering::Relaxed);
            }
        });
    }

    pub async fn shutdown_all(&self) {
        let mut connectors = self.connectors.write().await;
        for (_, connector) in connectors.iter_mut() {
            connector.status = ConnectorStatus::Stopped;
        }
        let mut sinks = self.active_sinks.lock().await;
        sinks.clear();
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthStatus {
    pub uptime_seconds: u64,
    pub total_connectors: u64,
    pub running_connectors: u64,
    pub errored_connectors: u64,
    pub total_events_received: u64,
    pub total_events_sent: u64,
    pub total_errors: u64,
    pub status: String,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_status_variants() {
        assert_ne!(format!("{:?}", ConnectorStatus::Stopped), "");
        assert_ne!(format!("{:?}", ConnectorStatus::Running), "");
        assert_ne!(format!("{:?}", ConnectorStatus::Error("err".to_string())), "");
    }

    #[test]
    fn test_managed_connector_default_impl() {
        let mc = ManagedConnector {
            config: None,
            status: ConnectorStatus::Stopped,
            offset: None,
            events_received: 0,
            error: None,
        };
        assert_eq!(mc.status, ConnectorStatus::Stopped);
        assert_eq!(mc.events_received, 0);
        assert!(mc.config.is_none());
        assert!(mc.offset.is_none());
        assert!(mc.error.is_none());
    }

    #[tokio::test]
    async fn test_app_state_register_connector() {
        let state = AppState::new();
        state.register_connector("test-connector").await;

        let list = state.list_connectors().await;
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "test-connector");
        assert_eq!(list[0].1.status, ConnectorStatus::Stopped);
    }

    #[tokio::test]
    async fn test_app_state_register_duplicate_keeps_first() {
        let state = AppState::new();
        state.register_connector("c1").await;
        state.register_connector("c1").await;

        let list = state.list_connectors().await;
        assert_eq!(list.len(), 1);
    }

    #[tokio::test]
    async fn test_app_state_update_status() {
        let state = AppState::new();
        state.register_connector("pg").await;
        state.update_status("pg", ConnectorStatus::Snapshotting).await;

        let connectors = state.connectors.read().await;
        let mc = connectors.get("pg").unwrap();
        assert_eq!(mc.status, ConnectorStatus::Snapshotting);
    }

    #[tokio::test]
    async fn test_app_state_update_status_nonexistent() {
        let state = AppState::new();
        state.update_status("nonexistent", ConnectorStatus::Running).await;
        // Should not panic
    }

    #[tokio::test]
    async fn test_app_state_list_empty() {
        let state = AppState::new();
        let list = state.list_connectors().await;
        assert!(list.is_empty());
    }

    #[tokio::test]
    async fn test_app_state_shutdown_all() {
        let state = AppState::new();
        state.register_connector("c1").await;
        state.register_connector("c2").await;
        state.update_status("c1", ConnectorStatus::Streaming).await;
        state.update_status("c2", ConnectorStatus::Snapshotting).await;

        state.shutdown_all().await;

        let connectors = state.connectors.read().await;
        for (_, mc) in connectors.iter() {
            assert_eq!(mc.status, ConnectorStatus::Stopped);
        }
    }

    #[tokio::test]
    async fn test_app_state_list_sorted() {
        let state = AppState::new();
        state.register_connector("z-connector").await;
        state.register_connector("a-connector").await;
        state.register_connector("m-connector").await;

        let list = state.list_connectors().await;
        assert_eq!(list[0].0, "a-connector");
        assert_eq!(list[1].0, "m-connector");
        assert_eq!(list[2].0, "z-connector");
    }

    #[test]
    fn test_app_state_default() {
        let state = AppState::default();
        let list = tokio::runtime::Runtime::new().unwrap().block_on(state.list_connectors());
        assert!(list.is_empty());
    }
}
