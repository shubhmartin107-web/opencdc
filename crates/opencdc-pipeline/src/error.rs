use opencdc_core::error::Error as CoreError;

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("Sink error: {0}")]
    Sink(String),

    #[error("Transform error: {0}")]
    Transform(String),

    #[error("Source error: {0}")]
    Source(String),

    #[error("Pipeline stopped: {0}")]
    Stopped(String),

    #[error("Core error: {0}")]
    Core(#[from] CoreError),
}

pub type PipelineResult<T> = std::result::Result<T, PipelineError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_error_sink() {
        let err = PipelineError::Sink("write failed".to_string());
        assert_eq!(err.to_string(), "Sink error: write failed");
    }

    #[test]
    fn test_pipeline_error_transform() {
        let err = PipelineError::Transform("filter error".to_string());
        assert_eq!(err.to_string(), "Transform error: filter error");
    }

    #[test]
    fn test_pipeline_error_source() {
        let err = PipelineError::Source("connection lost".to_string());
        assert_eq!(err.to_string(), "Source error: connection lost");
    }

    #[test]
    fn test_pipeline_error_stopped() {
        let err = PipelineError::Stopped("already stopped".to_string());
        assert_eq!(err.to_string(), "Pipeline stopped: already stopped");
    }

    #[test]
    fn test_pipeline_error_from_core() {
        let core_err = CoreError::Other("core issue".to_string());
        let err: PipelineError = core_err.into();
        assert!(matches!(err, PipelineError::Core(_)));
        assert_eq!(err.to_string(), "Core error: core issue");
    }

    #[test]
    fn test_pipeline_result_type() {
        let ok: PipelineResult<i32> = Ok(42);
        assert_eq!(ok.unwrap(), 42);

        let err: PipelineResult<i32> = Err(PipelineError::Sink("fail".to_string()));
        assert!(err.is_err());
    }
}
