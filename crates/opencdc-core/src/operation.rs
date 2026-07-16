use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    Create,
    Update,
    Delete,
    Read,
    Truncate,
    Message,
}

impl Operation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Operation::Create => "c",
            Operation::Update => "u",
            Operation::Delete => "d",
            Operation::Read => "r",
            Operation::Truncate => "t",
            Operation::Message => "m",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "c" => Some(Operation::Create),
            "u" => Some(Operation::Update),
            "d" => Some(Operation::Delete),
            "r" => Some(Operation::Read),
            "t" => Some(Operation::Truncate),
            "m" => Some(Operation::Message),
            _ => None,
        }
    }

    pub fn is_dml(&self) -> bool {
        matches!(self, Operation::Create | Operation::Update | Operation::Delete)
    }

    pub fn is_snapshot(&self) -> bool {
        matches!(self, Operation::Read)
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Operation::Delete)
    }
}

impl Serialize for Operation {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        self.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Operation::from_str(&s).ok_or_else(|| serde::de::Error::custom(format!(
            "invalid operation '{}', expected one of: c, u, d, r, t, m", s
        )))
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

use std::fmt;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_roundtrip() {
        for op in &[
            Operation::Create,
            Operation::Update,
            Operation::Delete,
            Operation::Read,
            Operation::Truncate,
            Operation::Message,
        ] {
            let json = serde_json::to_string(op).unwrap();
            let deserialized: Operation = serde_json::from_str(&json).unwrap();
            assert_eq!(*op, deserialized);
        }
    }

    #[test]
    fn test_operation_from_str() {
        assert_eq!(Operation::from_str("c"), Some(Operation::Create));
        assert_eq!(Operation::from_str("u"), Some(Operation::Update));
        assert_eq!(Operation::from_str("d"), Some(Operation::Delete));
        assert_eq!(Operation::from_str("r"), Some(Operation::Read));
        assert_eq!(Operation::from_str("t"), Some(Operation::Truncate));
        assert_eq!(Operation::from_str("m"), Some(Operation::Message));
        assert_eq!(Operation::from_str("x"), None);
    }

    #[test]
    fn test_operation_display() {
        assert_eq!(format!("{}", Operation::Create), "c");
        assert_eq!(format!("{}", Operation::Update), "u");
        assert_eq!(format!("{}", Operation::Delete), "d");
        assert_eq!(format!("{}", Operation::Read), "r");
        assert_eq!(format!("{}", Operation::Truncate), "t");
        assert_eq!(format!("{}", Operation::Message), "m");
    }

    #[test]
    fn test_operation_as_str() {
        assert_eq!(Operation::Create.as_str(), "c");
        assert_eq!(Operation::Update.as_str(), "u");
        assert_eq!(Operation::Delete.as_str(), "d");
        assert_eq!(Operation::Read.as_str(), "r");
        assert_eq!(Operation::Truncate.as_str(), "t");
        assert_eq!(Operation::Message.as_str(), "m");
    }

    #[test]
    fn test_operation_message_predicate() {
        assert!(!Operation::Message.is_dml());
        assert!(!Operation::Message.is_snapshot());
        assert!(!Operation::Message.is_delete());
    }

    #[test]
    fn test_operation_truncate_predicate() {
        assert!(!Operation::Truncate.is_delete());
    }

    #[test]
    fn test_operation_predicates() {
        assert!(Operation::Create.is_dml());
        assert!(Operation::Update.is_dml());
        assert!(Operation::Delete.is_dml());
        assert!(!Operation::Read.is_dml());
        assert!(!Operation::Truncate.is_dml());

        assert!(Operation::Read.is_snapshot());
        assert!(!Operation::Create.is_snapshot());

        assert!(Operation::Delete.is_delete());
        assert!(!Operation::Create.is_delete());
    }
}
