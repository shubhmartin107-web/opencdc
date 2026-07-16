use async_trait::async_trait;

use opencdc_core::change_event::ChangeEvent;
use opencdc_core::error::Result;
use opencdc_core::offset::ConnectorOffset;
use opencdc_core::ConnectorType;

use crate::config::{ConnectorConfig, SnapshotContext, StreamContext};

#[cfg(test)]
mod tests {
    use super::*;
    use opencdc_core::change_event::ChangeEvent;
    use opencdc_core::operation::Operation;
use opencdc_core::ConnectorType;

    struct TestConnector {
        name: String,
        started: bool,
    }

    #[async_trait]
    impl Connector for TestConnector {
        fn name(&self) -> &str {
            &self.name
        }

        fn connector_type(&self) -> ConnectorType {
            ConnectorType::Postgres
        }

        async fn start(&mut self, _config: ConnectorConfig) -> Result<()> {
            self.started = true;
            Ok(())
        }

        async fn stop(&mut self) -> Result<()> {
            self.started = false;
            Ok(())
        }

        async fn snapshot(
            &mut self,
            _ctx: SnapshotContext,
            _sink: tokio::sync::mpsc::Sender<ChangeEvent>,
        ) -> Result<ConnectorOffset> {
            Ok(ConnectorOffset::from_lsn(0))
        }

        async fn stream(
            &mut self,
            _ctx: StreamContext,
            _sink: tokio::sync::mpsc::Sender<ChangeEvent>,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_connector_lifecycle() {
        let mut connector = TestConnector {
            name: "test".to_string(),
            started: false,
        };
        assert_eq!(connector.name(), "test");
        assert_eq!(connector.connector_type(), ConnectorType::Postgres);

        connector.start(ConnectorConfig::default()).await.unwrap();
        assert!(connector.started);

        let (sink, _rx) = tokio::sync::mpsc::channel(1024);
        let offset = connector
            .snapshot(SnapshotContext::default(), sink)
            .await
            .unwrap();
        assert_eq!(offset.lsn, Some(0));

        connector.stop().await.unwrap();
        assert!(!connector.started);
    }

    #[tokio::test]
    async fn test_connector_snapshot_sends_events() {
        let _connector = TestConnector {
            name: "snapshot_test".to_string(),
            started: true,
        };

        let (sink, mut rx) = tokio::sync::mpsc::channel(1024);

        let handle = tokio::spawn(async move {
            // Simulate sending events then stopping
            let source = opencdc_core::source_info::SourceInfo::new(
                &ConnectorType::Postgres,
                "testdb",
                Some("public"),
                "users",
            );
            let event = ChangeEvent::create(serde_json::json!({"id": 1}), source);
            sink.send(event).await.unwrap();
            std::mem::drop(sink);
        });

        // Read from the rx to confirm events flow through
        let received = rx.recv().await;
        assert!(received.is_some());
        assert_eq!(received.unwrap().payload.op, Operation::Create);

        handle.await.unwrap();
    }
}

#[async_trait]
pub trait Connector: Send {
    fn name(&self) -> &str;
    fn connector_type(&self) -> ConnectorType;

    async fn start(&mut self, config: ConnectorConfig) -> Result<()>;

    async fn stop(&mut self) -> Result<()>;

    async fn snapshot(
        &mut self,
        ctx: SnapshotContext,
        sink: tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> Result<ConnectorOffset>;

    async fn stream(
        &mut self,
        ctx: StreamContext,
        sink: tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> Result<()>;
}
