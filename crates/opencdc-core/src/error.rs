#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),

    #[error("Invalid offset state: {0}")]
    InvalidOffset(String),

    #[error("Invalid message format: {0}")]
    InvalidMessageFormat(String),

    #[error("Conversion error: {0}")]
    Conversion(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Other(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Other(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_serialization_variant() {
        let err = Error::Serialization("encode failed".to_string());
        assert_eq!(err.to_string(), "Serialization error: encode failed");
    }

    #[test]
    fn test_error_deserialization_variant() {
        let err = Error::Deserialization("bad data".to_string());
        assert_eq!(err.to_string(), "Deserialization error: bad data");
    }

    #[test]
    fn test_error_schema_variant() {
        let err = Error::Schema("invalid".to_string());
        assert_eq!(err.to_string(), "Schema error: invalid");
    }

    #[test]
    fn test_error_unsupported_operation_variant() {
        let err = Error::UnsupportedOperation("truncate".to_string());
        assert_eq!(err.to_string(), "Unsupported operation: truncate");
    }

    #[test]
    fn test_error_invalid_offset_variant() {
        let err = Error::InvalidOffset("missing lsn".to_string());
        assert_eq!(err.to_string(), "Invalid offset state: missing lsn");
    }

    #[test]
    fn test_error_invalid_message_format_variant() {
        let err = Error::InvalidMessageFormat("bad magic byte".to_string());
        assert_eq!(err.to_string(), "Invalid message format: bad magic byte");
    }

    #[test]
    fn test_error_conversion_variant() {
        let err = Error::Conversion("type mismatch".to_string());
        assert_eq!(err.to_string(), "Conversion error: type mismatch");
    }

    #[test]
    fn test_error_transaction_variant() {
        let err = Error::Transaction("rollback".to_string());
        assert_eq!(err.to_string(), "Transaction error: rollback");
    }

    #[test]
    fn test_error_other_variant() {
        let err = Error::Other("something went wrong".to_string());
        assert_eq!(err.to_string(), "something went wrong");
    }

    #[test]
    fn test_error_from_string() {
        let err: Error = "custom error".to_string().into();
        assert!(matches!(err, Error::Other(_)));
        assert_eq!(err.to_string(), "custom error");
    }

    #[test]
    fn test_error_from_str() {
        let err: Error = "custom error".into();
        assert!(matches!(err, Error::Other(_)));
        assert_eq!(err.to_string(), "custom error");
    }

    #[test]
    fn test_error_from_arrow() {
        let arrow_err = arrow::error::ArrowError::IpcError("test".to_string());
        let err: Error = arrow_err.into();
        assert!(matches!(err, Error::Arrow(_)));
    }

    #[test]
    fn test_error_from_json() {
        let json_err = serde_json::from_str::<()>("invalid").unwrap_err();
        let err: Error = json_err.into();
        assert!(matches!(err, Error::Json(_)));
    }

    #[test]
    fn test_result_type_alias() {
        let ok: Result<i32> = Ok(42);
        assert_eq!(ok.unwrap(), 42);

        let err: Result<i32> = Err(Error::Other("fail".to_string()));
        assert!(err.is_err());
    }
}
