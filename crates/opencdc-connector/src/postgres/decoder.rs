use bytes::Bytes;

use opencdc_core::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct RelationColumn {
    pub flags: i8,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

#[derive(Debug, Clone)]
pub struct RelationMessage {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub columns: Vec<RelationColumn>,
}

#[derive(Debug, Clone)]
pub struct BeginMessage {
    pub lsn: i64,
    pub timestamp: i64,
    pub xid: u32,
}

#[derive(Debug, Clone)]
pub struct CommitMessage {
    pub flags: i8,
    pub lsn: i64,
    pub end_lsn: i64,
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct TupleColumn {
    pub is_null: bool,
    pub is_unchanged_toast: bool,
    pub value: Option<Bytes>,
}

#[derive(Debug, Clone)]
pub struct InsertMessage {
    pub relation_oid: u32,
    pub new_tuple: Vec<TupleColumn>,
}

#[derive(Debug, Clone)]
pub struct DeleteMessage {
    pub relation_oid: u32,
    pub old_tuple: Vec<TupleColumn>,
}

#[derive(Debug, Clone)]
pub struct UpdateMessage {
    pub relation_oid: u32,
    pub old_tuple: Option<Vec<TupleColumn>>,
    pub new_tuple: Vec<TupleColumn>,
}

#[derive(Debug, Clone)]
pub struct TruncateMessage {
    pub relation_oids: Vec<u32>,
    pub options: i8,
}

#[derive(Debug, Clone)]
pub enum PgOutputMessage {
    Begin(BeginMessage),
    Commit(CommitMessage),
    Relation(RelationMessage),
    Insert(InsertMessage),
    Update(UpdateMessage),
    Delete(DeleteMessage),
    Truncate(TruncateMessage),
}

#[derive(Default)]
pub struct PgOutputDecoder {
    relations: Vec<RelationMessage>,
}

impl PgOutputDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_relation(&self, oid: u32) -> Option<&RelationMessage> {
        self.relations.iter().find(|r| r.oid == oid)
    }

    pub fn decode(&mut self, data: &[u8]) -> Result<Option<PgOutputMessage>> {
        if data.is_empty() {
            return Ok(None);
        }

        let msg_type = data[0] as char;
        let rest = &data[1..];

        match msg_type {
            'B' => {
                let (msg, _) = self.decode_begin(rest)?;
                Ok(Some(PgOutputMessage::Begin(msg)))
            }
            'C' => {
                let (msg, _) = self.decode_commit(rest)?;
                Ok(Some(PgOutputMessage::Commit(msg)))
            }
            'R' => {
                let (msg, _) = self.decode_relation(rest)?;
                self.relations.push(msg.clone());
                Ok(Some(PgOutputMessage::Relation(msg)))
            }
            'I' => {
                let (msg, _) = self.decode_insert(rest)?;
                Ok(Some(PgOutputMessage::Insert(msg)))
            }
            'U' => {
                let (msg, _) = self.decode_update(rest)?;
                Ok(Some(PgOutputMessage::Update(msg)))
            }
            'D' => {
                let (msg, _) = self.decode_delete(rest)?;
                Ok(Some(PgOutputMessage::Delete(msg)))
            }
            'T' => {
                let (msg, _) = self.decode_truncate(rest)?;
                Ok(Some(PgOutputMessage::Truncate(msg)))
            }
            _ => Ok(None),
        }
    }

    fn decode_begin(&self, data: &[u8]) -> Result<(BeginMessage, usize)> {
        let mut offset = 0;
        let lsn = read_i64_be(data, &mut offset);
        let timestamp = read_i64_be(data, &mut offset);
        let xid = read_i32_be(data, &mut offset) as u32;
        Ok((
            BeginMessage {
                lsn,
                timestamp,
                xid,
            },
            offset,
        ))
    }

    fn decode_commit(&self, data: &[u8]) -> Result<(CommitMessage, usize)> {
        let mut offset = 0;
        let flags = data[offset] as i8;
        offset += 1;
        let lsn = read_i64_be(data, &mut offset);
        let end_lsn = read_i64_be(data, &mut offset);
        let timestamp = read_i64_be(data, &mut offset);
        Ok((
            CommitMessage {
                flags,
                lsn,
                end_lsn,
                timestamp,
            },
            offset,
        ))
    }

    fn decode_relation(&self, data: &[u8]) -> Result<(RelationMessage, usize)> {
        let mut offset = 0;
        let oid = read_i32_be(data, &mut offset) as u32;
        let schema = read_string(data, &mut offset);
        let name = read_string(data, &mut offset);
        let _replica_identity = read_i16_be(data, &mut offset);
        let col_count = read_i16_be(data, &mut offset) as u16;

        let mut columns = Vec::with_capacity(col_count as usize);
        for _ in 0..col_count {
            let flags = data[offset] as i8;
            offset += 1;
            let col_name = read_string(data, &mut offset);
            let type_oid = read_i32_be(data, &mut offset) as u32;
            let type_modifier = read_i32_be(data, &mut offset);
            columns.push(RelationColumn {
                flags,
                name: col_name,
                type_oid,
                type_modifier,
            });
        }

        Ok((
            RelationMessage {
                oid,
                schema,
                name,
                columns,
            },
            offset,
        ))
    }

    fn decode_insert(&self, data: &[u8]) -> Result<(InsertMessage, usize)> {
        let mut offset = 0;
        let relation_oid = read_i32_be(data, &mut offset) as u32;
        if offset >= data.len() {
            return Err(Error::InvalidMessageFormat(
                "truncated insert message".to_string(),
            ));
        }
        let tuple_type = data[offset] as char;
        offset += 1;
        if tuple_type != 'N' {
            return Err(Error::InvalidMessageFormat(format!(
                "expected 'N' tuple type in insert, got '{}'",
                tuple_type
            )));
        }
        let (new_tuple, consumed) = decode_tuple_data(&data[offset..])?;
        offset += consumed;
        Ok((
            InsertMessage {
                relation_oid,
                new_tuple,
            },
            offset,
        ))
    }

    fn decode_update(&self, data: &[u8]) -> Result<(UpdateMessage, usize)> {
        let mut offset = 0;
        let relation_oid = read_i32_be(data, &mut offset) as u32;
        if offset >= data.len() {
            return Err(Error::InvalidMessageFormat(
                "truncated update message".to_string(),
            ));
        }
        let tuple_type = data[offset] as char;
        offset += 1;

        let (old_tuple, new_tuple) = match tuple_type {
            'K' => {
                let (key_tuple, consumed) = decode_tuple_data(&data[offset..])?;
                offset += consumed;
                if offset >= data.len() {
                    return Err(Error::InvalidMessageFormat(
                        "truncated update: expected 'N' after 'K'".to_string(),
                    ));
                }
                if data[offset] as char != 'N' {
                    return Err(Error::InvalidMessageFormat(format!(
                        "expected 'N' after 'K' in update, got '{}'",
                        data[offset] as char
                    )));
                }
                offset += 1;
                let (new_tup, consumed2) = decode_tuple_data(&data[offset..])?;
                offset += consumed2;
                (Some(key_tuple), new_tup)
            }
            'O' => {
                let (old_tup, consumed) = decode_tuple_data(&data[offset..])?;
                offset += consumed;
                if offset >= data.len() {
                    return Err(Error::InvalidMessageFormat(
                        "truncated update: expected 'N' after 'O'".to_string(),
                    ));
                }
                if data[offset] as char != 'N' {
                    return Err(Error::InvalidMessageFormat(format!(
                        "expected 'N' after 'O' in update, got '{}'",
                        data[offset] as char
                    )));
                }
                offset += 1;
                let (new_tup, consumed2) = decode_tuple_data(&data[offset..])?;
                offset += consumed2;
                (Some(old_tup), new_tup)
            }
            'N' => {
                let (new_tup, consumed) = decode_tuple_data(&data[offset..])?;
                offset += consumed;
                (None, new_tup)
            }
            _ => {
                return Err(Error::InvalidMessageFormat(format!(
                    "unknown tuple type '{}' in update",
                    tuple_type
                )));
            }
        };

        Ok((
            UpdateMessage {
                relation_oid,
                old_tuple,
                new_tuple,
            },
            offset,
        ))
    }

    fn decode_delete(&self, data: &[u8]) -> Result<(DeleteMessage, usize)> {
        let mut offset = 0;
        let relation_oid = read_i32_be(data, &mut offset) as u32;
        if offset >= data.len() {
            return Err(Error::InvalidMessageFormat(
                "truncated delete message".to_string(),
            ));
        }
        let tuple_type = data[offset] as char;
        offset += 1;
        let old_tuple = match tuple_type {
            'K' | 'O' => {
                let (tup, consumed) = decode_tuple_data(&data[offset..])?;
                offset += consumed;
                tup
            }
            _ => {
                return Err(Error::InvalidMessageFormat(format!(
                    "unknown tuple type '{}' in delete",
                    tuple_type
                )));
            }
        };

        Ok((
            DeleteMessage {
                relation_oid,
                old_tuple,
            },
            offset,
        ))
    }

    fn decode_truncate(&self, data: &[u8]) -> Result<(TruncateMessage, usize)> {
        let mut offset = 0;
        let num_rels = read_i32_be(data, &mut offset) as u32;
        let options = data[offset] as i8;
        offset += 1;
        let mut relation_oids = Vec::with_capacity(num_rels as usize);
        for _ in 0..num_rels {
            let oid = read_i32_be(data, &mut offset) as u32;
            relation_oids.push(oid);
        }
        Ok((
            TruncateMessage {
                relation_oids,
                options,
            },
            offset,
        ))
    }
}

fn decode_tuple_data(data: &[u8]) -> Result<(Vec<TupleColumn>, usize)> {
    let mut offset = 0;
    let col_count = read_i16_be(data, &mut offset) as u16;
    let mut columns = Vec::with_capacity(col_count as usize);

    for _ in 0..col_count {
        if offset >= data.len() {
            return Err(Error::InvalidMessageFormat(
                "truncated tuple data".to_string(),
            ));
        }
        let kind = data[offset] as char;
        offset += 1;
        match kind {
            'n' => {
                columns.push(TupleColumn {
                    is_null: true,
                    is_unchanged_toast: false,
                    value: None,
                });
            }
            'u' => {
                columns.push(TupleColumn {
                    is_null: false,
                    is_unchanged_toast: true,
                    value: None,
                });
            }
            't' => {
                let raw_len = read_i32_be(data, &mut offset);
                if raw_len < 0 {
                    return Err(Error::InvalidMessageFormat(
                        "negative tuple value length".to_string(),
                    ));
                }
                let len = raw_len as usize;
                if offset + len > data.len() {
                    return Err(Error::InvalidMessageFormat(
                        "tuple value exceeds data length".to_string(),
                    ));
                }
                let value = Bytes::copy_from_slice(&data[offset..offset + len]);
                offset += len;
                columns.push(TupleColumn {
                    is_null: false,
                    is_unchanged_toast: false,
                    value: Some(value),
                });
            }
            _ => {
                return Err(Error::InvalidMessageFormat(format!(
                    "unknown column kind '{}'",
                    kind
                )));
            }
        }
    }

    Ok((columns, offset))
}

fn read_i16_be(data: &[u8], offset: &mut usize) -> i16 {
    let val = i16::from_be_bytes([data[*offset], data[*offset + 1]]);
    *offset += 2;
    val
}

fn read_i32_be(data: &[u8], offset: &mut usize) -> i32 {
    let val = i32::from_be_bytes([
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
    ]);
    *offset += 4;
    val
}

fn read_i64_be(data: &[u8], offset: &mut usize) -> i64 {
    let val = i64::from_be_bytes([
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
        data[*offset + 4],
        data[*offset + 5],
        data[*offset + 6],
        data[*offset + 7],
    ]);
    *offset += 8;
    val
}

fn read_string(data: &[u8], offset: &mut usize) -> String {
    let start = *offset;
    while *offset < data.len() && data[*offset] != 0 {
        *offset += 1;
    }
    let s = String::from_utf8_lossy(&data[start..*offset]).to_string();
    *offset += 1;
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_begin_packet(lsn: i64, ts: i64, xid: u32) -> Vec<u8> {
        let mut buf = vec![b'B'];
        buf.extend_from_slice(&lsn.to_be_bytes());
        buf.extend_from_slice(&ts.to_be_bytes());
        buf.extend_from_slice(&(xid as i32).to_be_bytes());
        buf
    }

    fn build_relation_packet(
        oid: u32,
        schema: &str,
        table: &str,
        cols: &[(&str, u32, i32)],
    ) -> Vec<u8> {
        let mut buf = vec![b'R'];
        buf.extend_from_slice(&(oid as i32).to_be_bytes());
        buf.extend_from_slice(schema.as_bytes());
        buf.push(0);
        buf.extend_from_slice(table.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&1i16.to_be_bytes()); // replica identity: index
        buf.extend_from_slice(&(cols.len() as i16).to_be_bytes());
        for (name, type_oid, type_mod) in cols {
            buf.push(0); // flags
            buf.extend_from_slice(name.as_bytes());
            buf.push(0);
            buf.extend_from_slice(&(*type_oid as i32).to_be_bytes());
            buf.extend_from_slice(&type_mod.to_be_bytes());
        }
        buf
    }

    fn build_insert_packet(oid: u32, values: &[&str]) -> Vec<u8> {
        let mut buf = vec![b'I'];
        buf.extend_from_slice(&(oid as i32).to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for val in values {
            let val_bytes = val.as_bytes();
            buf.push(b't');
            buf.extend_from_slice(&(val_bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(val_bytes);
        }
        buf
    }

    #[test]
    fn test_decode_begin() {
        let mut decoder = PgOutputDecoder::new();
        let packet = build_begin_packet(12345, 1710000000000, 42);
        let msg = decoder.decode(&packet).unwrap().unwrap();
        match msg {
            PgOutputMessage::Begin(b) => {
                assert_eq!(b.lsn, 12345);
                assert_eq!(b.timestamp, 1710000000000);
                assert_eq!(b.xid, 42);
            }
            _ => panic!("expected begin"),
        }
    }

    #[test]
    fn test_decode_relation() {
        let mut decoder = PgOutputDecoder::new();
        let packet = build_relation_packet(
            16384,
            "public",
            "users",
            &[("id", 23i32 as u32, -1), ("name", 25i32 as u32, -1)],
        );
        let msg = decoder.decode(&packet).unwrap().unwrap();
        match msg {
            PgOutputMessage::Relation(r) => {
                assert_eq!(r.oid, 16384);
                assert_eq!(r.schema, "public");
                assert_eq!(r.name, "users");
                assert_eq!(r.columns.len(), 2);
                assert_eq!(r.columns[0].name, "id");
                assert_eq!(r.columns[1].name, "name");
            }
            _ => panic!("expected relation"),
        }
    }

    #[test]
    fn test_decode_insert() {
        let mut decoder = PgOutputDecoder::new();
        let rel_packet = build_relation_packet(
            16384,
            "public",
            "users",
            &[("id", 23, -1), ("name", 25, -1)],
        );
        decoder.decode(&rel_packet).unwrap();

        let ins_packet = build_insert_packet(16384, &["1", "Alice"]);
        let msg = decoder.decode(&ins_packet).unwrap().unwrap();
        match msg {
            PgOutputMessage::Insert(i) => {
                assert_eq!(i.relation_oid, 16384);
                assert_eq!(i.new_tuple.len(), 2);
                assert_eq!(i.new_tuple[0].value.as_ref().unwrap(), &Bytes::from("1"));
                assert_eq!(
                    i.new_tuple[1].value.as_ref().unwrap(),
                    &Bytes::from("Alice")
                );
            }
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn test_decode_commit() {
        let mut decoder = PgOutputDecoder::new();
        let mut packet = vec![b'C', 0]; // flags
        packet.extend_from_slice(&100i64.to_be_bytes()); // lsn
        packet.extend_from_slice(&200i64.to_be_bytes()); // end_lsn
        packet.extend_from_slice(&1710000000000i64.to_be_bytes()); // timestamp

        let msg = decoder.decode(&packet).unwrap().unwrap();
        match msg {
            PgOutputMessage::Commit(c) => {
                assert_eq!(c.lsn, 100);
                assert_eq!(c.end_lsn, 200);
                assert_eq!(c.timestamp, 1710000000000);
            }
            _ => panic!("expected commit"),
        }
    }

    #[test]
    fn test_decode_multiple_messages() {
        let mut decoder = PgOutputDecoder::new();
        let begin = build_begin_packet(100, 1710000000000, 42);
        let rel = build_relation_packet(16384, "public", "users", &[("id", 23, -1)]);
        let ins = build_insert_packet(16384, &["1"]);

        // Feed messages sequentially (as they'd come from the WAL stream)
        let m1 = decoder.decode(&begin).unwrap();
        assert!(m1.is_some());
        let m2 = decoder.decode(&rel).unwrap();
        assert!(m2.is_some());
        let m3 = decoder.decode(&ins).unwrap();
        assert!(m3.is_some());

        // Verify relation was cached
        assert!(decoder.get_relation(16384).is_some());
        assert_eq!(decoder.get_relation(16384).unwrap().name, "users");
    }
}
