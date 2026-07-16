use opencdc_core::error::Result;

pub const SCHEMA_REGISTRY_MAGIC_BYTE: u8 = 0x00;

pub struct SchemaRegistryWireFormat;

impl SchemaRegistryWireFormat {
    pub fn encode(schema_id: u32, payload: &[u8]) -> Vec<u8> {
        let mut result = Vec::with_capacity(5 + payload.len());
        result.push(SCHEMA_REGISTRY_MAGIC_BYTE);
        result.extend_from_slice(&schema_id.to_be_bytes());
        result.extend_from_slice(payload);
        result
    }

    pub fn decode(data: &[u8]) -> Result<(u32, &[u8])> {
        if data.len() < 5 {
            return Err(opencdc_core::error::Error::Deserialization(format!(
                "schema registry data too short: {} bytes",
                data.len()
            )));
        }
        if data[0] != SCHEMA_REGISTRY_MAGIC_BYTE {
            return Err(opencdc_core::error::Error::Deserialization(format!(
                "invalid magic byte: {:#x}",
                data[0]
            )));
        }
        let schema_id = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
        Ok((schema_id, &data[5..]))
    }
}

pub struct SchemaRegistryPayload;

impl SchemaRegistryPayload {
    pub fn encode_json(schema_id: u32, json_str: &str) -> Vec<u8> {
        SchemaRegistryWireFormat::encode(schema_id, json_str.as_bytes())
    }

    pub fn decode_json(data: &[u8]) -> Result<(u32, String)> {
        let (schema_id, payload) = SchemaRegistryWireFormat::decode(data)?;
        let json_str = String::from_utf8(payload.to_vec()).map_err(|e| {
            opencdc_core::error::Error::Deserialization(format!(
                "invalid utf-8 in schema registry payload: {}",
                e
            ))
        })?;
        Ok((schema_id, json_str))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let payload = b"{\"type\": \"record\", \"name\": \"test\"}";
        let encoded = SchemaRegistryWireFormat::encode(42, payload);
        assert_eq!(encoded.len(), 5 + payload.len());
        assert_eq!(encoded[0], 0x00);

        let (schema_id, decoded) = SchemaRegistryWireFormat::decode(&encoded).unwrap();
        assert_eq!(schema_id, 42);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_invalid_magic_byte() {
        let data = &[0x01, 0x00, 0x00, 0x00, 0x01];
        assert!(SchemaRegistryWireFormat::decode(data).is_err());
    }

    #[test]
    fn test_json_payload_roundtrip() {
        let json = "{\"type\": \"string\"}";
        let encoded = SchemaRegistryPayload::encode_json(99, json);
        let (id, decoded) = SchemaRegistryPayload::decode_json(&encoded).unwrap();
        assert_eq!(id, 99);
        assert_eq!(decoded, json);
    }
}
