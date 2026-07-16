use async_trait::async_trait;

use opencdc_core::change_event::ChangeEvent;

use crate::error::PipelineResult;

#[async_trait]
pub trait Sink: Send + Sync {
    fn name(&self) -> &str;

    async fn write(&mut self, events: &[ChangeEvent]) -> PipelineResult<()>;

    async fn flush(&mut self) -> PipelineResult<()> {
        Ok(())
    }

    async fn close(&mut self) -> PipelineResult<()> {
        self.flush().await
    }
}

pub struct StdoutSink {
    name: String,
    pretty: bool,
}

impl StdoutSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            pretty: false,
        }
    }

    pub fn with_pretty(mut self) -> Self {
        self.pretty = true;
        self
    }
}

#[async_trait]
impl Sink for StdoutSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn write(&mut self, events: &[ChangeEvent]) -> PipelineResult<()> {
        for event in events {
            let line = if self.pretty {
                serde_json::to_string_pretty(event).unwrap_or_else(|_| "{}".to_string())
            } else {
                serde_json::to_string(event).unwrap_or_else(|_| "{}".to_string())
            };
            println!("{}", line);
        }
        Ok(())
    }
}

pub struct NullSink {
    name: String,
    count: u64,
}

impl NullSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            count: 0,
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }
}

#[async_trait]
impl Sink for NullSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn write(&mut self, events: &[ChangeEvent]) -> PipelineResult<()> {
        self.count += events.len() as u64;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opencdc_core::ConnectorType;
    use opencdc_core::source_info::SourceInfo;

    #[tokio::test]
    async fn test_null_sink_counts_events() {
        let mut sink = NullSink::new("test");
        let source = SourceInfo::new(&ConnectorType::Postgres, "db", None::<&str>, "t");
        let events = vec![
            ChangeEvent::create(serde_json::json!({"id": 1}), source.clone()),
            ChangeEvent::create(serde_json::json!({"id": 2}), source),
        ];

        sink.write(&events).await.unwrap();
        assert_eq!(sink.count(), 2);

        sink.write(&events).await.unwrap();
        assert_eq!(sink.count(), 4);

        assert_eq!(sink.name(), "test");
    }

    #[tokio::test]
    async fn test_stdout_sink_doesnt_crash() {
        let mut sink = StdoutSink::new("stdout_test");
        let source = SourceInfo::new(&ConnectorType::Postgres, "db", None::<&str>, "t");
        let events = vec![ChangeEvent::create(
            serde_json::json!({"id": 1, "name": "test"}),
            source,
        )];

        // Just verify no panic
        sink.write(&events).await.unwrap();
        sink.flush().await.unwrap();
        sink.close().await.unwrap();
        assert_eq!(sink.name(), "stdout_test");
    }
}
