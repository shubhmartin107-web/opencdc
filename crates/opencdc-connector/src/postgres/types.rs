use bytes::Bytes;

/// Common PostgreSQL type OIDs
const BOOLOID: u32 = 16;
const INT2OID: u32 = 21;
const INT4OID: u32 = 23;
const INT8OID: u32 = 20;
const FLOAT4OID: u32 = 700;
const FLOAT8OID: u32 = 701;
const NUMERICOID: u32 = 1700;
const TEXTOID: u32 = 25;
const VARCHAROID: u32 = 1043;
const BPCHAROID: u32 = 1042;
const JSONOID: u32 = 114;
const JSONBOID: u32 = 3802;
const UUIDOID: u32 = 2950;
const TIMESTAMPOID: u32 = 1114;
const TIMESTAMPTZOID: u32 = 1184;
const DATEOID: u32 = 1082;
const TIMEOID: u32 = 1083;
const TIMETZOID: u32 = 1266;
const BYTEAOID: u32 = 17;
const XIDOID: u32 = 28;
const OIDOID: u32 = 26;

pub fn pg_type_to_json(raw: &Bytes, type_oid: u32) -> serde_json::Value {
    let s = String::from_utf8_lossy(raw);

    match type_oid {
        BOOLOID => {
            serde_json::Value::Bool(s == "t" || s == "true")
        }
        INT2OID | INT4OID => {
            s.parse::<i32>()
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null)
        }
        INT8OID | XIDOID => {
            s.parse::<i64>()
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null)
        }
        FLOAT4OID => {
            s.parse::<f32>()
                .map(|v| serde_json::json!(v))
                .unwrap_or(serde_json::Value::Null)
        }
        FLOAT8OID => {
            s.parse::<f64>()
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null)
        }
        NUMERICOID => {
            serde_json::Value::String(s.to_string())
        }
        TEXTOID | VARCHAROID | BPCHAROID | UUIDOID => {
            serde_json::Value::String(s.to_string())
        }
        JSONOID | JSONBOID => {
            let s = s.into_owned();
            serde_json::from_str(&s).unwrap_or(serde_json::Value::String(s))
        }
        TIMESTAMPOID | TIMESTAMPTZOID => {
            serde_json::Value::String(s.to_string())
        }
        DATEOID => {
            serde_json::Value::String(s.to_string())
        }
        TIMEOID | TIMETZOID => {
            serde_json::Value::String(s.to_string())
        }
        BYTEAOID => {
            serde_json::Value::String(s.to_string())
        }
        OIDOID => {
            let s = s.as_ref();
            s.parse::<u32>()
                .map(|v| serde_json::json!(v))
                .unwrap_or(serde_json::Value::Null)
        }
        _ => {
            serde_json::Value::String(s.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_int4_conversion() {
        assert_eq!(
            pg_type_to_json(&Bytes::from("42"), INT4OID),
            serde_json::json!(42)
        );
        assert_eq!(
            pg_type_to_json(&Bytes::from("-1"), INT4OID),
            serde_json::json!(-1)
        );
    }

    #[test]
    fn test_bool_conversion() {
        assert_eq!(
            pg_type_to_json(&Bytes::from("t"), BOOLOID),
            serde_json::json!(true)
        );
        assert_eq!(
            pg_type_to_json(&Bytes::from("f"), BOOLOID),
            serde_json::json!(false)
        );
    }

    #[test]
    fn test_text_conversion() {
        assert_eq!(
            pg_type_to_json(&Bytes::from("hello"), TEXTOID),
            serde_json::json!("hello")
        );
    }

    #[test]
    fn test_json_conversion() {
        let raw = Bytes::from(r#"{"key": "value"}"#);
        let result = pg_type_to_json(&raw, JSONBOID);
        assert_eq!(result["key"], "value");
    }

    #[test]
    fn test_unknown_type_falls_back_to_string() {
        assert_eq!(
            pg_type_to_json(&Bytes::from("anything"), 9999),
            serde_json::json!("anything")
        );
    }
}
