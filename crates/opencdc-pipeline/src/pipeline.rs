use std::sync::Arc;

use tokio::sync::Mutex;

use opencdc_connector::config::{SnapshotContext, StreamContext};
use opencdc_connector::r#trait::Connector;
use opencdc_core::change_event::ChangeEvent;
use opencdc_core::offset::ConnectorOffset;

use crate::error::{PipelineError, PipelineResult};
use crate::sink::Sink;
use crate::transform::Transform;

pub struct Pipeline {
    name: String,
    connector: Arc<Mutex<Box<dyn Connector>>>,
    transforms: Vec<Box<dyn Transform>>,
    sinks: Vec<Arc<Mutex<Box<dyn Sink>>>>,
    running: bool,
}

impl Pipeline {
    pub fn new(name: impl Into<String>, connector: Box<dyn Connector>) -> Self {
        Self {
            name: name.into(),
            connector: Arc::new(Mutex::new(connector)),
            transforms: Vec::new(),
            sinks: Vec::new(),
            running: false,
        }
    }

    pub fn add_transform(mut self, transform: Box<dyn Transform>) -> Self {
        self.transforms.push(transform);
        self
    }

    pub fn add_sink(mut self, sink: Box<dyn Sink>) -> Self {
        self.sinks.push(Arc::new(Mutex::new(sink)));
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn is_running(&self) -> bool {
        self.running
    }

    pub async fn run(
        &mut self,
        snapshot_tables: Option<Vec<String>>,
    ) -> PipelineResult<()> {
        if self.running {
            return Err(PipelineError::Stopped(
                "pipeline is already running".to_string(),
            ));
        }
        self.running = true;

        let connector = Arc::clone(&self.connector);

        // Step 1: Run snapshot if tables requested
        if let Some(tables) = snapshot_tables {
            let (snap_sink, mut snap_stream) =
                tokio::sync::mpsc::channel::<ChangeEvent>(1024);

            let snap_ctx = SnapshotContext { tables };
            let mut conn = connector.lock().await;

            let offset = conn.snapshot(snap_ctx, snap_sink).await.map_err(|e| {
                self.running = false;
                PipelineError::Source(format!("snapshot failed: {}", e))
            })?;

            std::mem::drop(conn);

            // Process snapshot events through transforms and sinks
            while let Some(event) = snap_stream.recv().await {
                self.process_event(event).await?;
            }

            // Step 2: Stream from snapshot offset
            self.stream_from_offset(Some(offset)).await?;
        } else {
            self.stream_from_offset(None).await?;
        }

        Ok(())
    }

    async fn stream_from_offset(&mut self, offset: Option<ConnectorOffset>) -> PipelineResult<()> {
        let (stream_sink, mut stream_rx) =
            tokio::sync::mpsc::channel::<ChangeEvent>(1024);

        let stream_ctx = StreamContext {
            offset,
            ..Default::default()
        };

        let mut conn = self.connector.lock().await;
        conn.stream(stream_ctx, stream_sink)
            .await
            .map_err(|e| PipelineError::Source(format!("stream failed: {}", e)))?;
        std::mem::drop(conn);

        while let Some(event) = stream_rx.recv().await {
            self.process_event(event).await?;
        }

        Ok(())
    }

    async fn process_event(&self, event: ChangeEvent) -> PipelineResult<()> {
        let mut event = Some(event);

        // Apply transforms in order
        for transform in &self.transforms {
            match event {
                Some(e) => {
                    event = transform
                        .transform(e)
                        .await
                        .map_err(|e| PipelineError::Transform(format!("{}", e)))?;
                }
                None => break,
            }
        }

        // Dispatch to all sinks
        if let Some(ref event) = event {
            for sink in &self.sinks {
                let mut sink = sink.lock().await;
                let slice = std::slice::from_ref(event);
                sink.write(slice)
                    .await
                    .map_err(|e| PipelineError::Sink(format!("{}", e)))?;
            }
        }

        Ok(())
    }

    pub async fn stop(&mut self) -> PipelineResult<()> {
        self.running = false;

        for sink in &self.sinks {
            let mut sink = sink.lock().await;
            sink.close().await.map_err(|e| {
                PipelineError::Sink(format!("close error: {}", e))
            })?;
        }

        let mut conn = self.connector.lock().await;
        conn.stop().await.map_err(|e| {
            PipelineError::Source(format!("stop error: {}", e))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use opencdc_connector::config::ConnectorConfig;
    use opencdc_connector::r#trait::Connector;
    use opencdc_core::change_event::ChangeEvent;
    use opencdc_core::offset::ConnectorOffset;
    use opencdc_core::operation::Operation;
    use opencdc_core::source_info::SourceInfo;
    use opencdc_core::ConnectorType;

    use crate::transform::FilterTransform;

    struct TestConnector {
        events: Vec<ChangeEvent>,
    }

    #[async_trait]
    impl Connector for TestConnector {
        fn name(&self) -> &str {
            "test"
        }

        fn connector_type(&self) -> ConnectorType {
            ConnectorType::Postgres
        }

        async fn start(&mut self, _config: ConnectorConfig) -> opencdc_core::error::Result<()> {
            Ok(())
        }

        async fn stop(&mut self) -> opencdc_core::error::Result<()> {
            Ok(())
        }

        async fn snapshot(
            &mut self,
            _ctx: SnapshotContext,
            _sink: tokio::sync::mpsc::Sender<ChangeEvent>,
        ) -> opencdc_core::error::Result<ConnectorOffset> {
            Ok(ConnectorOffset::from_lsn(0))
        }

        async fn stream(
            &mut self,
            _ctx: StreamContext,
            sink: tokio::sync::mpsc::Sender<ChangeEvent>,
        ) -> opencdc_core::error::Result<()> {
            let events = std::mem::take(&mut self.events);
            for event in events {
                let _ = sink.send(event).await;
            }
            Ok(())
        }
    }

    fn make_event(table: &str, op: Operation) -> ChangeEvent {
        let source = SourceInfo::new(&ConnectorType::Postgres, "db", Some("public"), table);
        ChangeEvent::new(opencdc_core::change_event::ChangePayload {
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            source,
            op,
            ts_ms: None,
            transaction: None,
        })
    }

    struct CountSink {
        name: String,
        count: Arc<Mutex<u64>>,
    }

    #[async_trait]
    impl Sink for CountSink {
        fn name(&self) -> &str {
            &self.name
        }

        async fn write(&mut self, events: &[ChangeEvent]) -> PipelineResult<()> {
            let mut count = self.count.lock().await;
            *count += events.len() as u64;
            Ok(())
        }
    }

    fn make_count_sink() -> (CountSink, Arc<Mutex<u64>>) {
        let count = Arc::new(Mutex::new(0));
        (
            CountSink {
                name: "count".to_string(),
                count: Arc::clone(&count),
            },
            count,
        )
    }

    #[tokio::test]
    async fn test_pipeline_routes_events_to_sink() {
        let connector = TestConnector {
            events: vec![
                make_event("t1", Operation::Create),
                make_event("t1", Operation::Update),
            ],
        };

        let (sink, count) = make_count_sink();
        let mut pipeline = Pipeline::new("test", Box::new(connector))
            .add_sink(Box::new(sink));

        pipeline.run(None).await.unwrap();
        assert_eq!(*count.lock().await, 2);
    }

    #[tokio::test]
    async fn test_pipeline_with_transform_filters_events() {
        let connector = TestConnector {
            events: vec![
                make_event("t1", Operation::Create),
                make_event("t1", Operation::Read),
                make_event("t1", Operation::Update),
            ],
        };

        let (sink, count) = make_count_sink();
        let mut pipeline = Pipeline::new("test", Box::new(connector))
            .add_transform(Box::new(FilterTransform::only_dml()))
            .add_sink(Box::new(sink));

        pipeline.run(None).await.unwrap();
        assert_eq!(*count.lock().await, 2);
    }

    #[tokio::test]
    async fn test_pipeline_stop_works() {
        let connector = TestConnector {
            events: Vec::new(),
        };

        let (sink, _count) = make_count_sink();
        let mut pipeline = Pipeline::new("test", Box::new(connector))
            .add_sink(Box::new(sink));

        pipeline.run(None).await.unwrap();
        pipeline.stop().await.unwrap();
        assert!(!pipeline.running);
    }

    #[tokio::test]
    async fn test_pipeline_rejects_double_run() {
        let connector = TestConnector {
            events: Vec::new(),
        };

        let (sink, _count) = make_count_sink();
        let mut pipeline = Pipeline::new("test", Box::new(connector))
            .add_sink(Box::new(sink));

        pipeline.run(None).await.unwrap();
        let result = pipeline.run(None).await;
        assert!(result.is_err());
    }
}
