use serde_json::Value;

pub const MYSQL_TYPE_DECIMAL: u8 = 0;
pub const MYSQL_TYPE_TINY: u8 = 1;
pub const MYSQL_TYPE_SHORT: u8 = 2;
pub const MYSQL_TYPE_LONG: u8 = 3;
pub const MYSQL_TYPE_FLOAT: u8 = 4;
pub const MYSQL_TYPE_DOUBLE: u8 = 5;
pub const MYSQL_TYPE_NULL: u8 = 6;
pub const MYSQL_TYPE_TIMESTAMP: u8 = 7;
pub const MYSQL_TYPE_LONGLONG: u8 = 8;
pub const MYSQL_TYPE_INT24: u8 = 9;
pub const MYSQL_TYPE_DATE: u8 = 10;
pub const MYSQL_TYPE_TIME: u8 = 11;
pub const MYSQL_TYPE_DATETIME: u8 = 12;
pub const MYSQL_TYPE_YEAR: u8 = 13;
pub const MYSQL_TYPE_NEWDATE: u8 = 14;
pub const MYSQL_TYPE_VARCHAR: u8 = 15;
pub const MYSQL_TYPE_BIT: u8 = 16;
pub const MYSQL_TYPE_TIMESTAMP2: u8 = 17;
pub const MYSQL_TYPE_DATETIME2: u8 = 18;
pub const MYSQL_TYPE_TIME2: u8 = 19;
pub const MYSQL_TYPE_JSON: u8 = 245;
pub const MYSQL_TYPE_NEWDECIMAL: u8 = 246;
pub const MYSQL_TYPE_ENUM: u8 = 247;
pub const MYSQL_TYPE_SET: u8 = 248;
pub const MYSQL_TYPE_TINY_BLOB: u8 = 249;
pub const MYSQL_TYPE_MEDIUM_BLOB: u8 = 250;
pub const MYSQL_TYPE_LONG_BLOB: u8 = 251;
pub const MYSQL_TYPE_BLOB: u8 = 252;
pub const MYSQL_TYPE_VAR_STRING: u8 = 253;
pub const MYSQL_TYPE_STRING: u8 = 254;
pub const MYSQL_TYPE_GEOMETRY: u8 = 255;

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub col_type: u8,
    pub meta: u16,
    pub is_signed: bool,
    pub charset: u16,
}

pub fn mysql_type_name(col_type: u8) -> &'static str {
    match col_type {
        MYSQL_TYPE_DECIMAL => "DECIMAL",
        MYSQL_TYPE_TINY => "TINYINT",
        MYSQL_TYPE_SHORT => "SMALLINT",
        MYSQL_TYPE_LONG => "INT",
        MYSQL_TYPE_FLOAT => "FLOAT",
        MYSQL_TYPE_DOUBLE => "DOUBLE",
        MYSQL_TYPE_NULL => "NULL",
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => "TIMESTAMP",
        MYSQL_TYPE_LONGLONG => "BIGINT",
        MYSQL_TYPE_INT24 => "MEDIUMINT",
        MYSQL_TYPE_DATE => "DATE",
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => "TIME",
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => "DATETIME",
        MYSQL_TYPE_YEAR => "YEAR",
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING => "VARCHAR",
        MYSQL_TYPE_BIT => "BIT",
        MYSQL_TYPE_JSON => "JSON",
        MYSQL_TYPE_NEWDECIMAL => "DECIMAL",
        MYSQL_TYPE_ENUM => "ENUM",
        MYSQL_TYPE_SET => "SET",
        MYSQL_TYPE_BLOB | MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB => {
            "BLOB"
        }
        MYSQL_TYPE_STRING => "CHAR",
        MYSQL_TYPE_GEOMETRY => "GEOMETRY",
        _ => "UNKNOWN",
    }
}

pub fn decode_binlog_value(data: &[u8], col_type: u8, meta: u16) -> Value {
    match col_type {
        MYSQL_TYPE_TINY => {
            if meta & 0x80 != 0 {
                Value::Number(serde_json::Number::from(data[0] as i8 as i64))
            } else {
                Value::Number(serde_json::Number::from(data[0]))
            }
        }
        MYSQL_TYPE_SHORT => {
            let val = u16::from_le_bytes([data[0], data[1]]);
            if meta & 0x80 != 0 {
                Value::Number(serde_json::Number::from(val as i16 as i64))
            } else {
                Value::Number(serde_json::Number::from(val))
            }
        }
        MYSQL_TYPE_LONG | MYSQL_TYPE_INT24 => {
            let val = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            Value::Number(serde_json::Number::from(val as i64))
        }
        MYSQL_TYPE_LONGLONG => {
            let val = u64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            if val > i64::MAX as u64 {
                Value::String(val.to_string())
            } else {
                Value::Number(serde_json::Number::from(val as i64))
            }
        }
        MYSQL_TYPE_FLOAT => {
            let val = f32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            serde_json::json!(val)
        }
        MYSQL_TYPE_DOUBLE => {
            let val = f64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            serde_json::json!(val)
        }
        MYSQL_TYPE_DATE => {
            let days = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            if days == 0 {
                return Value::String("0000-00-00".to_string());
            }
            let d = days as i64 - 365 - 1;
            let year = (d * 100 + 50) / 36525 + 1970;
            let yday = d - ((year - 1970) * 365 + (year - 1969) / 4);
            Value::String(format!(
                "{}-{:02}-{:02}",
                year,
                yday / 30 + 1,
                yday % 30 + 1
            ))
        }
        MYSQL_TYPE_DATETIME => decode_datetime(data, false),
        MYSQL_TYPE_DATETIME2 => {
            let frac = (meta & 0xff) as u8;
            decode_datetime2(data, frac)
        }
        MYSQL_TYPE_TIMESTAMP => {
            let secs = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            Value::Number(serde_json::Number::from(secs as i64))
        }
        MYSQL_TYPE_TIMESTAMP2 => decode_timestamp2(data, meta),
        MYSQL_TYPE_STRING | MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING => {
            let (_, s) = decode_length_encoded_string(data);
            Value::String(s)
        }
        MYSQL_TYPE_BLOB | MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB => {
            Value::String(hex::encode(data))
        }
        MYSQL_TYPE_BIT => {
            let bit_len = (meta >> 8) * 8 + (meta & 0xff);
            let byte_len = (bit_len as usize).div_ceil(8);
            let mut val = 0u64;
            for (i, &b) in data.iter().enumerate().take(byte_len) {
                val |= (b as u64) << (i * 8);
            }
            if val > i64::MAX as u64 {
                Value::String(val.to_string())
            } else {
                Value::Number(serde_json::Number::from(val as i64))
            }
        }
        MYSQL_TYPE_JSON => {
            let json_str = String::from_utf8_lossy(data).into_owned();
            serde_json::from_str(&json_str).unwrap_or_else(|_| Value::String(json_str))
        }
        MYSQL_TYPE_ENUM | MYSQL_TYPE_SET => Value::Number(serde_json::Number::from(data[0])),
        MYSQL_TYPE_NEWDECIMAL => decode_decimal(data, meta),
        MYSQL_TYPE_YEAR => Value::Number(serde_json::Number::from(data[0] as i64 + 1900)),
        _ => Value::String(hex::encode(data)),
    }
}

pub fn decode_length_encoded_string(data: &[u8]) -> (usize, String) {
    if data.is_empty() {
        return (1, String::new());
    }
    let len = data[0] as usize;
    if 1 + len > data.len() {
        return (1, String::new());
    }
    let s = String::from_utf8_lossy(&data[1..1 + len]).to_string();
    (1 + len, s)
}

fn decode_datetime(data: &[u8], _is_date2: bool) -> Value {
    if data.len() < 8 {
        return Value::String("0000-00-00 00:00:00".to_string());
    }
    let packed = i64::from_le_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]);
    if packed == 0 {
        return Value::String("0000-00-00 00:00:00".to_string());
    }
    let d = packed / 1000000;
    let t = packed % 1000000;
    let year = (d / 10000) as i32;
    let month = ((d % 10000) / 100) as i32;
    let day = (d % 100) as i32;
    let hour = (t / 10000) as i32;
    let minute = ((t % 10000) / 100) as i32;
    let second = (t % 100) as i32;
    Value::String(format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        year, month, day, hour, minute, second
    ))
}

fn decode_datetime2(data: &[u8], frac_sec: u8) -> Value {
    if data.len() < 5 {
        return Value::String("0000-00-00 00:00:00".to_string());
    }
    let packed = i64::from(data[0]) << 32
        | i64::from(data[1]) << 24
        | i64::from(data[2]) << 16
        | i64::from(data[3]) << 8
        | i64::from(data[4]);
    let d = packed >> 17;
    let t = (packed >> 5) & 0xfff;
    let year = (d / 13 / 32) as i32;
    let month = ((d / 32) % 13) as i32;
    let day = (d % 32) as i32;
    let hour = (t >> 7) as i32;
    let minute = ((t >> 1) & 0x3f) as i32;
    let second = ((t << 3) & 0x38 | ((packed & 0x1f) >> 2)) as i32;

    if frac_sec > 0 {
        let frac_start = 5;
        let frac_bytes = frac_sec.div_ceil(2);
        if data.len() >= frac_start + frac_bytes as usize {
            let mut frac = 0u64;
            for i in 0..frac_bytes as usize {
                frac = (frac << 8) | u64::from(data[frac_start + i]);
            }
            Value::String(format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                year, month, day, hour, minute, second, frac
            ))
        } else {
            Value::String(format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                year, month, day, hour, minute, second
            ))
        }
    } else {
        Value::String(format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            year, month, day, hour, minute, second
        ))
    }
}

fn decode_timestamp2(data: &[u8], _meta: u16) -> Value {
    if data.len() < 4 {
        return Value::Number(serde_json::Number::from(0));
    }
    let secs = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    Value::Number(serde_json::Number::from(secs as i64))
}

fn decode_decimal(data: &[u8], _meta: u16) -> Value {
    if data.is_empty() {
        return Value::Number(serde_json::Number::from(0));
    }
    let str_val = String::from_utf8_lossy(data).into_owned();
    serde_json::from_str::<f64>(&str_val)
        .map(|v| serde_json::json!(v))
        .unwrap_or_else(|_| Value::String(str_val))
}
