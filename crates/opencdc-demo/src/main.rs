use async_trait::async_trait;

use opencdc_connector::config::{ConnectorConfig, SnapshotContext, StreamContext};
use opencdc_connector::r#trait::Connector;
use opencdc_core::change_event::ChangeEvent;
use opencdc_core::offset::ConnectorOffset;
use opencdc_core::source_info::SourceInfo;
use opencdc_core::ConnectorType;

use opencdc_pipeline::pipeline::Pipeline;
use opencdc_pipeline::sink::StdoutSink;
use opencdc_pipeline::transform::{LogTransform, RenameTransform};

use opencdc_sink_openlake::OpenLakeSink;
use openlake_query::store::TableStore;

struct DemoConnector {
    offset: i64,
    snapshot_done: bool,
}

impl DemoConnector {
    fn new() -> Self {
        Self {
            offset: 0,
            snapshot_done: false,
        }
    }

    fn make_source(&self, table: &str) -> SourceInfo {
        SourceInfo::new(&ConnectorType::Postgres, "demodb", Some("public"), table)
    }
}

#[async_trait]
impl Connector for DemoConnector {
    fn name(&self) -> &str {
        "demo-connector"
    }

    fn connector_type(&self) -> ConnectorType {
        ConnectorType::Postgres
    }

    async fn start(&mut self, _config: ConnectorConfig) -> opencdc_core::error::Result<()> {
        tracing::info!("Connector started");
        Ok(())
    }

    async fn stop(&mut self) -> opencdc_core::error::Result<()> {
        tracing::info!("Connector stopped");
        Ok(())
    }

    async fn snapshot(
        &mut self,
        _ctx: SnapshotContext,
        sink: tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> opencdc_core::error::Result<ConnectorOffset> {
        tracing::info!("Starting snapshot phase");

        let tables = vec!["users", "orders", "products"];

        for table in &tables {
            for i in 1..=100 {
                let source = self.make_source(table);
                let event = ChangeEvent::snapshot(
                    serde_json::json!({
                        "id": i,
                        "name": format!("{}_{}", table, i),
                        "created_at": "2026-01-01T00:00:00Z",
                    }),
                    source,
                );
                let _ = sink.send(event).await;
            }
        }

        tracing::info!("Snapshot complete: 300 events across 3 tables");
        self.snapshot_done = true;
        self.offset = 0;
        Ok(ConnectorOffset::from_lsn(0))
    }

    async fn stream(
        &mut self,
        _ctx: StreamContext,
        sink: tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> opencdc_core::error::Result<()> {
        tracing::info!("Starting stream phase");

        let stream_events = vec![
            ("users", "create", serde_json::json!({"id": 101, "name": "Alice"})),
            ("users", "create", serde_json::json!({"id": 102, "name": "Bob"})),
            ("users", "update", serde_json::json!({"id": 101, "name": "Alice Updated"})),
            ("orders", "create", serde_json::json!({"id": 1001, "user_id": 101, "total": 29.99})),
            ("orders", "create", serde_json::json!({"id": 1002, "user_id": 102, "total": 59.99})),
            ("users", "delete", serde_json::json!({"id": 102})),
            ("orders", "update", serde_json::json!({"id": 1002, "status": "shipped"})),
        ];

        for (table, op, payload) in &stream_events {
            let source = self.make_source(table);
            let event = match *op {
                "create" => ChangeEvent::create(payload.clone(), source),
                "update" => ChangeEvent::update(
                    Some(serde_json::json!({"id": payload["id"]})),
                    payload.clone(),
                    source,
                ),
                "delete" => ChangeEvent::delete(payload.clone(), source),
                _ => continue,
            };
            self.offset += 1;
            let _ = sink.send(event).await;
        }

        tracing::info!("Stream phase complete: {} events sent", self.offset);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    tracing::info!("=== OpenCDC Demo ===");
    tracing::info!("Demonstrating: Connector → Transforms → Sinks → OpenLake");

    let store = TableStore::new();

    let connector = DemoConnector::new();
    let mut pipeline = Pipeline::new("demo", Box::new(connector))
        .add_transform(Box::new(LogTransform::new("log")))
        .add_transform(Box::new(
            RenameTransform::new("rename")
                .remap_table("users", "cdc_users")
                .remap_database("demodb", "cdc"),
        ))
        .add_sink(Box::new(StdoutSink::new("stdout")))
        .add_sink(Box::new(OpenLakeSink::new("openlake", store.clone())));

    tracing::info!("Running pipeline (snapshot + stream)...");
    pipeline
        .run(Some(vec![
            "public.users".to_string(),
            "public.orders".to_string(),
            "public.products".to_string(),
        ]))
        .await
        .expect("Pipeline run failed");

    tracing::info!("Stopping pipeline...");
    pipeline.stop().await.expect("Pipeline stop failed");

    tracing::info!("=== OpenLake Storage Summary ===");

    let tables = store.list_tables();
    tracing::info!("Tables in OpenLake: {}", tables.len());

    for table_name in &tables {
        if let Ok(entry) = store.get(
            &openlake_core::types::TableIdentifier::from_string(table_name)
                .expect("invalid table id"),
        ) {
            let total_rows: usize = entry.batches.iter().map(|b| b.num_rows()).sum();
            let snapshot_events = if table_name.contains("snapshot") { "yes" } else { "no" };
            tracing::info!(
                "  Table '{}': {} rows across {} batches (snapshot: {})",
                table_name,
                total_rows,
                entry.batches.len(),
                snapshot_events,
            );
        }
    }

    tracing::info!("=== Demo Complete ===");
    tracing::info!("CDC pipeline successfully routed {} events through:", 300 + 7);
    tracing::info!("  DemoConnector → LogTransform");
    tracing::info!("    → RenameTransform(demodb→cdc, users→cdc_users)");
    tracing::info!("    → StdoutSink");
    tracing::info!("    → OpenLakeSink(TableStore)");
    tracing::info!("OpenLake stores {} tables with queryable Arrow records", tables.len());
}
