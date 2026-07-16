use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DebeziumSchema {
    #[serde(rename = "type")]
    pub schema_type: DebeziumSchemaType,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<DebeziumField>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DebeziumField {
    /// The column name in the source (serialized as "field" in JSON)
    #[serde(rename = "field")]
    pub field_name: String,

    /// The Debezium type (also can be an array like ["string", "null"])
    #[serde(rename = "type")]
    pub field_type: serde_json::Value,

    /// Nested fields for struct/array/map types
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<DebeziumField>>,

    /// Whether the field is optional
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,

    /// The Debezium schema name for named types (e.g., "io.debezium.connector.postgresql.Source")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Schema version
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,

    /// Documentation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,

    /// Type-specific parameters (e.g., precision, scale for decimal)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<HashMap<String, String>>,

    /// Default value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
}

impl Default for DebeziumField {
    fn default() -> Self {
        Self {
            field_name: String::new(),
            field_type: serde_json::Value::String("string".to_string()),
            fields: None,
            optional: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
            default: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DebeziumSchemaType {
    Struct,
    Array,
    Map,
    String,
    Bytes,
    Int8,
    Int16,
    Int32,
    Int64,
    Float,
    Double,
    Boolean,
    Decimal,
    Timestamp,
    Date,
    Time,
    MicroTimestamp,
    MicroTime,
    NanoTimestamp,
    NanoTime,
    Json,
    #[serde(untagged)]
    Unknown(String),
}

impl DebeziumSchemaType {
    pub fn as_str(&self) -> &str {
        match self {
            DebeziumSchemaType::Struct => "struct",
            DebeziumSchemaType::Array => "array",
            DebeziumSchemaType::Map => "map",
            DebeziumSchemaType::String => "string",
            DebeziumSchemaType::Bytes => "bytes",
            DebeziumSchemaType::Int8 => "int8",
            DebeziumSchemaType::Int16 => "int16",
            DebeziumSchemaType::Int32 => "int32",
            DebeziumSchemaType::Int64 => "int64",
            DebeziumSchemaType::Float => "float",
            DebeziumSchemaType::Double => "double",
            DebeziumSchemaType::Boolean => "boolean",
            DebeziumSchemaType::Decimal => "decimal",
            DebeziumSchemaType::Timestamp => "timestamp",
            DebeziumSchemaType::Date => "date",
            DebeziumSchemaType::Time => "time",
            DebeziumSchemaType::MicroTimestamp => "microtimestamp",
            DebeziumSchemaType::MicroTime => "microtime",
            DebeziumSchemaType::NanoTimestamp => "nanotimestamp",
            DebeziumSchemaType::NanoTime => "nanotime",
            DebeziumSchemaType::Json => "json",
            DebeziumSchemaType::Unknown(s) => s.as_str(),
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "struct" => DebeziumSchemaType::Struct,
            "array" => DebeziumSchemaType::Array,
            "map" => DebeziumSchemaType::Map,
            "string" => DebeziumSchemaType::String,
            "bytes" => DebeziumSchemaType::Bytes,
            "int8" => DebeziumSchemaType::Int8,
            "int16" => DebeziumSchemaType::Int16,
            "int32" => DebeziumSchemaType::Int32,
            "int64" => DebeziumSchemaType::Int64,
            "float" => DebeziumSchemaType::Float,
            "double" => DebeziumSchemaType::Double,
            "boolean" => DebeziumSchemaType::Boolean,
            "decimal" => DebeziumSchemaType::Decimal,
            "timestamp" => DebeziumSchemaType::Timestamp,
            "date" => DebeziumSchemaType::Date,
            "time" => DebeziumSchemaType::Time,
            "microtimestamp" => DebeziumSchemaType::MicroTimestamp,
            "microtime" => DebeziumSchemaType::MicroTime,
            "nanotimestamp" => DebeziumSchemaType::NanoTimestamp,
            "nanotime" => DebeziumSchemaType::NanoTime,
            "json" => DebeziumSchemaType::Json,
            other => DebeziumSchemaType::Unknown(other.to_string()),
        }
    }

    pub fn is_primitive(&self) -> bool {
        !matches!(
            self,
            DebeziumSchemaType::Struct | DebeziumSchemaType::Array | DebeziumSchemaType::Map
        )
    }
}

impl DebeziumField {
    pub fn new(field_name: impl Into<String>, field_type: DebeziumSchemaType) -> Self {
        Self {
            field_name: field_name.into(),
            field_type: serde_json::Value::String(field_type.as_str().to_string()),
            fields: None,
            optional: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
            default: None,
        }
    }

    pub fn string(name: impl Into<String>) -> Self {
        Self::new(name, DebeziumSchemaType::String)
    }

    pub fn int32(name: impl Into<String>) -> Self {
        Self::new(name, DebeziumSchemaType::Int32)
    }

    pub fn int64(name: impl Into<String>) -> Self {
        Self::new(name, DebeziumSchemaType::Int64)
    }

    pub fn boolean(name: impl Into<String>) -> Self {
        Self::new(name, DebeziumSchemaType::Boolean)
    }

    pub fn float64(name: impl Into<String>) -> Self {
        Self::new(name, DebeziumSchemaType::Double)
    }

    pub fn struct_field(name: impl Into<String>, fields: Vec<DebeziumField>) -> Self {
        Self {
            field_name: name.into(),
            field_type: serde_json::Value::String("struct".to_string()),
            fields: Some(fields),
            optional: None,
            name: None,
            version: None,
            doc: None,
            parameters: None,
            default: None,
        }
    }

    pub fn optional(mut self) -> Self {
        self.optional = Some(true);
        self
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }

    pub fn with_doc(mut self, doc: impl Into<String>) -> Self {
        self.doc = Some(doc.into());
        self
    }

    pub fn with_parameter(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters
            .get_or_insert_default()
            .insert(key.into(), value.into());
        self
    }

    pub fn resolve_type(&self) -> DebeziumSchemaType {
        match &self.field_type {
            serde_json::Value::String(s) => DebeziumSchemaType::from_str(s),
            serde_json::Value::Array(types) => types
                .first()
                .and_then(|t| t.as_str())
                .map(DebeziumSchemaType::from_str)
                .unwrap_or(DebeziumSchemaType::Unknown("unresolved".to_string())),
            _ => DebeziumSchemaType::Unknown("unresolved".to_string()),
        }
    }

    pub fn is_optional(&self) -> bool {
        match &self.field_type {
            serde_json::Value::Array(types) => types.iter().any(|t| t == "null"),
            _ => self.optional.unwrap_or(false),
        }
    }
}

impl Default for DebeziumSchema {
    fn default() -> Self {
        Self {
            schema_type: DebeziumSchemaType::Struct,
            fields: None,
            optional: None,
            name: None,
            version: None,
            doc: None,
            field: None,
            default: None,
            parameters: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_type_from_str() {
        assert_eq!(
            DebeziumSchemaType::from_str("struct"),
            DebeziumSchemaType::Struct
        );
        assert_eq!(
            DebeziumSchemaType::from_str("int32"),
            DebeziumSchemaType::Int32
        );
        assert_eq!(
            DebeziumSchemaType::from_str("unknown_type"),
            DebeziumSchemaType::Unknown("unknown_type".to_string())
        );
    }

    #[test]
    fn test_all_schema_types_from_str() {
        let cases = [
            ("struct", DebeziumSchemaType::Struct),
            ("array", DebeziumSchemaType::Array),
            ("map", DebeziumSchemaType::Map),
            ("string", DebeziumSchemaType::String),
            ("bytes", DebeziumSchemaType::Bytes),
            ("int8", DebeziumSchemaType::Int8),
            ("int16", DebeziumSchemaType::Int16),
            ("int32", DebeziumSchemaType::Int32),
            ("int64", DebeziumSchemaType::Int64),
            ("float", DebeziumSchemaType::Float),
            ("double", DebeziumSchemaType::Double),
            ("boolean", DebeziumSchemaType::Boolean),
            ("decimal", DebeziumSchemaType::Decimal),
            ("timestamp", DebeziumSchemaType::Timestamp),
            ("date", DebeziumSchemaType::Date),
            ("time", DebeziumSchemaType::Time),
            ("microtimestamp", DebeziumSchemaType::MicroTimestamp),
            ("microtime", DebeziumSchemaType::MicroTime),
            ("nanotimestamp", DebeziumSchemaType::NanoTimestamp),
            ("nanotime", DebeziumSchemaType::NanoTime),
            ("json", DebeziumSchemaType::Json),
        ];
        for (s, expected) in &cases {
            assert_eq!(
                DebeziumSchemaType::from_str(s),
                *expected,
                "failed for '{}'",
                s
            );
        }
    }

    #[test]
    fn test_schema_types_as_str_roundtrip() {
        let types = [
            DebeziumSchemaType::Struct,
            DebeziumSchemaType::Array,
            DebeziumSchemaType::Map,
            DebeziumSchemaType::String,
            DebeziumSchemaType::Bytes,
            DebeziumSchemaType::Int8,
            DebeziumSchemaType::Int16,
            DebeziumSchemaType::Int32,
            DebeziumSchemaType::Int64,
            DebeziumSchemaType::Float,
            DebeziumSchemaType::Double,
            DebeziumSchemaType::Boolean,
            DebeziumSchemaType::Decimal,
            DebeziumSchemaType::Timestamp,
            DebeziumSchemaType::Date,
            DebeziumSchemaType::Time,
            DebeziumSchemaType::MicroTimestamp,
            DebeziumSchemaType::MicroTime,
            DebeziumSchemaType::NanoTimestamp,
            DebeziumSchemaType::NanoTime,
            DebeziumSchemaType::Json,
        ];
        for ty in &types {
            let s = ty.as_str();
            let back = DebeziumSchemaType::from_str(s);
            assert_eq!(*ty, back, "failed roundtrip for '{}'", s);
        }
    }

    #[test]
    fn test_schema_type_as_str() {
        assert_eq!(DebeziumSchemaType::Int32.as_str(), "int32");
        assert_eq!(DebeziumSchemaType::Struct.as_str(), "struct");
        assert_eq!(
            DebeziumSchemaType::Unknown("custom".to_string()).as_str(),
            "custom"
        );
    }

    #[test]
    fn test_schema_type_unknown_as_str() {
        let ty = DebeziumSchemaType::Unknown("my_custom_type".to_string());
        assert_eq!(ty.as_str(), "my_custom_type");
    }

    #[test]
    fn test_field_int64_constructor() {
        let field = DebeziumField::int64("big_id");
        assert_eq!(field.field_name, "big_id");
        assert_eq!(field.resolve_type(), DebeziumSchemaType::Int64);
    }

    #[test]
    fn test_field_boolean_constructor() {
        let field = DebeziumField::boolean("is_active");
        assert_eq!(field.field_name, "is_active");
        assert_eq!(field.resolve_type(), DebeziumSchemaType::Boolean);
    }

    #[test]
    fn test_field_float64_constructor() {
        let field = DebeziumField::float64("price");
        assert_eq!(field.field_name, "price");
        assert_eq!(field.resolve_type(), DebeziumSchemaType::Double);
    }

    #[test]
    fn test_field_struct_constructor() {
        let inner = vec![DebeziumField::int32("x"), DebeziumField::string("y")];
        let field = DebeziumField::struct_field("point", inner);
        assert_eq!(field.field_name, "point");
        assert_eq!(field.resolve_type(), DebeziumSchemaType::Struct);
        assert!(field.fields.is_some());
        assert_eq!(field.fields.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_field_with_version() {
        let field = DebeziumField::string("name").with_version(3);
        assert_eq!(field.version, Some(3));
    }

    #[test]
    fn test_field_builder() {
        let field = DebeziumField::string("name")
            .optional()
            .named("io.opencdc.public.users.name")
            .with_doc("The user's display name")
            .with_parameter("version", "2");
        assert_eq!(field.field_name, "name");
        assert_eq!(field.optional, Some(true));
        assert_eq!(field.name.as_deref(), Some("io.opencdc.public.users.name"));
        assert_eq!(field.doc.as_deref(), Some("The user's display name"));
        assert_eq!(
            field.parameters.as_ref().unwrap().get("version").unwrap(),
            "2"
        );
    }

    #[test]
    fn test_field_resolve_type_with_array_type() {
        let mut field = DebeziumField::new("col", DebeziumSchemaType::String);
        field.field_type = serde_json::json!(["string", "null"]);
        assert_eq!(field.resolve_type(), DebeziumSchemaType::String);
    }

    #[test]
    fn test_field_resolve_type_with_empty_array() {
        let mut field = DebeziumField::new("col", DebeziumSchemaType::String);
        field.field_type = serde_json::json!([]);
        assert_eq!(
            field.resolve_type(),
            DebeziumSchemaType::Unknown("unresolved".to_string())
        );
    }

    #[test]
    fn test_field_resolve_type() {
        let field = DebeziumField::new("col", DebeziumSchemaType::Int64);
        assert_eq!(field.resolve_type(), DebeziumSchemaType::Int64);
    }

    #[test]
    fn test_field_serialization() {
        let field = DebeziumField::int32("id").named("io.opencdc.public.users.id");
        let json = serde_json::to_value(&field).unwrap();
        assert_eq!(json["field"], "id");
        assert_eq!(json["type"], "int32");
        assert_eq!(json["name"], "io.opencdc.public.users.id");
    }

    #[test]
    fn test_field_deserialization() {
        let json = serde_json::json!({
            "field": "id",
            "type": "int32",
            "optional": true,
            "name": "io.opencdc.public.users.id"
        });
        let field: DebeziumField = serde_json::from_value(json).unwrap();
        assert_eq!(field.field_name, "id");
        assert_eq!(field.resolve_type(), DebeziumSchemaType::Int32);
        assert_eq!(field.optional, Some(true));
        assert_eq!(field.name.as_deref(), Some("io.opencdc.public.users.id"));
    }

    #[test]
    fn test_schema_roundtrip() {
        let schema = DebeziumSchema {
            schema_type: DebeziumSchemaType::Struct,
            fields: Some(vec![
                DebeziumField::int32("id"),
                DebeziumField::string("name").optional(),
            ]),
            optional: Some(false),
            name: Some("io.opencdc.public.users.Envelope".to_string()),
            version: Some(1),
            ..Default::default()
        };
        let json = serde_json::to_value(&schema).unwrap();
        assert_eq!(json["type"], "struct");
        assert_eq!(json["name"], "io.opencdc.public.users.Envelope");
        assert_eq!(json["fields"][0]["field"], "id");
        assert_eq!(json["fields"][0]["type"], "int32");
        let deserialized: DebeziumSchema = serde_json::from_value(json).unwrap();
        assert_eq!(schema, deserialized);
    }

    #[test]
    fn test_primitive_check() {
        assert!(DebeziumSchemaType::String.is_primitive());
        assert!(!DebeziumSchemaType::Struct.is_primitive());
        assert!(!DebeziumSchemaType::Array.is_primitive());
    }

    #[test]
    fn test_debezium_field_json_rename() {
        let field = DebeziumField::string("email").optional();
        let json = serde_json::to_value(&field).unwrap();
        assert_eq!(json["field"], "email");
        assert_eq!(json["type"], "string");
        assert_eq!(json["optional"], true);
        assert!(json.get("field_name").is_none());
    }

    #[test]
    fn test_is_optional_with_array_type() {
        let mut field = DebeziumField::string("col");
        field.field_type = serde_json::json!(["string", "null"]);
        assert!(field.is_optional());

        let mut field2 = DebeziumField::string("col");
        field2.field_type = serde_json::json!(["string"]);
        assert!(!field2.is_optional());
    }
}
