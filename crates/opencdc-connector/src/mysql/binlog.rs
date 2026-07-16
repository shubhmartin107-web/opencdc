use std::collections::HashMap;

use bytes::BytesMut;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::column::*;
use super::config::MySqlConnectorConfig;

#[derive(Debug, Clone)]
pub struct EventHeader {
    pub timestamp: u32,
    pub event_type: u8,
    pub server_id: u32,
    pub event_size: u32,
    pub log_pos: u32,
    pub flags: u16,
}

#[derive(Debug, Clone)]
pub struct TableMapEvent {
    pub table_id: u64,
    pub database: String,
    pub table: String,
    pub column_types: Vec<u8>,
    pub column_metas: Vec<u16>,
    pub null_bits: Vec<bool>,
}

#[derive(Debug, Clone)]
pub struct RowEvent {
    pub columns: Vec<Value>,
}

#[derive(Debug, Clone)]
pub enum BinlogEvent {
    Rotate(String, u64),
    FormatDescription,
    TableMap(TableMapEvent),
    Gtid(uuid::Uuid, u64),
    Xid(u64),
    WriteRows {
        table_id: u64,
        rows: Vec<RowEvent>,
    },
    UpdateRows {
        table_id: u64,
        rows: Vec<(RowEvent, RowEvent)>,
    },
    DeleteRows {
        table_id: u64,
        rows: Vec<RowEvent>,
    },
    Heartbeat,
    Unknown(u8),
}

pub struct BinlogClient {
    stream: TcpStream,
    #[allow(dead_code)]
    buf: BytesMut,
    config: MySqlConnectorConfig,
    pub current_gtid: Option<(uuid::Uuid, u64)>,
    pub tables: HashMap<u64, TableMapEvent>,
}

impl BinlogClient {
    pub async fn connect(config: MySqlConnectorConfig) -> Result<Self, String> {
        let addr = format!("{}:{}", config.host, config.port);
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| format!("connect failed: {}", e))?;

        let mut client = Self {
            stream,
            buf: BytesMut::with_capacity(65536),
            config,
            current_gtid: None,
            tables: HashMap::new(),
        };

        client.handshake().await?;

        Ok(client)
    }

    async fn read_packet(&mut self) -> Result<BytesMut, String> {
        let mut header = [0u8; 4];
        self.stream
            .read_exact(&mut header)
            .await
            .map_err(|e| format!("read packet header failed: {}", e))?;

        let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        let _seq = header[3];

        let mut data = vec![0u8; len];
        if len > 0 {
            self.stream
                .read_exact(&mut data)
                .await
                .map_err(|e| format!("read packet body failed: {}", e))?;
        }

        Ok(BytesMut::from(&data[..]))
    }

    async fn write_command(&mut self, command: u8, args: &[u8]) -> Result<(), String> {
        let mut packet = Vec::with_capacity(1 + args.len());
        packet.push(command);
        packet.extend_from_slice(args);

        let len = packet.len() as u32;
        let header = [(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, 0];
        self.stream
            .write_all(&header)
            .await
            .map_err(|e| format!("write command header failed: {}", e))?;
        self.stream
            .write_all(&packet)
            .await
            .map_err(|e| format!("write command body failed: {}", e))?;

        Ok(())
    }

    async fn handshake(&mut self) -> Result<(), String> {
        let greeting = self.read_packet().await?;
        if greeting[0] == 0xff {
            let err_msg = String::from_utf8_lossy(&greeting[3..]);
            return Err(format!("mysql handshake error: {}", err_msg));
        }

        let protocol_version = greeting[0];
        if protocol_version != 10 {
            return Err(format!("unsupported protocol version: {}", protocol_version));
        }

        let mut pos = 1;
        while pos < greeting.len() && greeting[pos] != 0 {
            pos += 1;
        }
        pos += 1;

        let _connection_id =
            u32::from_le_bytes([greeting[pos], greeting[pos + 1], greeting[pos + 2], greeting[pos + 3]]);
        pos += 4;

        let auth_plugin_data_part1 = &greeting[pos..pos + 8];
        pos += 8;

        let _filler = greeting[pos];
        pos += 1;

        let _capability_flags_1 =
            u16::from_le_bytes([greeting[pos], greeting[pos + 1]]);
        pos += 2;

        let _character_set = greeting[pos];
        pos += 1;

        let _status_flags =
            u16::from_le_bytes([greeting[pos], greeting[pos + 1]]);
        pos += 2;

        let capability_flags_2 =
            u16::from_le_bytes([greeting[pos], greeting[pos + 1]]);
        pos += 2;

        let auth_data_len = if greeting.len() > pos {
            let len = greeting[pos] as usize;
            pos += 1;
            len
        } else {
            0
        };

        pos += 10;

        let auth_plugin_data_part2 = if auth_data_len > 8 {
            let len = (auth_data_len - 8) as usize;
            let end = (pos + len).min(greeting.len());
            let part2 = &greeting[pos..end];
            pos = end;
            part2.to_vec()
        } else {
            Vec::new()
        };

        let mut scramble = Vec::with_capacity(20);
        scramble.extend_from_slice(auth_plugin_data_part1);
        scramble.extend_from_slice(&auth_plugin_data_part2);

        let _auth_plugin_name: Vec<u8> = if capability_flags_2 & 0x8000 != 0 {
            let _ = &pos;
            pos += 1;
            Vec::new()
        } else {
            Vec::new()
        };

        let _ = pos;
        self.send_auth(&scramble).await?;
        self.read_ok_response().await?;

        Ok(())
    }

    fn mysql_native_password(password: &str, scramble: &[u8]) -> Vec<u8> {
        use sha1::{Digest, Sha1};

        let mut hasher = Sha1::new();
        hasher.update(password.as_bytes());
        let hash1 = hasher.finalize();

        let mut hasher = Sha1::new();
        hasher.update(hash1);
        let hash2 = hasher.finalize();

        let mut hasher = Sha1::new();
        hasher.update(scramble);
        hasher.update(hash2);
        let hash3 = hasher.finalize();

        let mut result = Vec::with_capacity(20);
        for (a, b) in hash1.iter().zip(hash3.iter()) {
            result.push(a ^ b);
        }
        result
    }

    async fn send_auth(&mut self, scramble: &[u8]) -> Result<(), String> {
        let auth_response = Self::mysql_native_password(&self.config.password, scramble);

        let user_bytes = self.config.user.as_bytes();
        let db_bytes = self.config.database.as_bytes();

        let cap = 0x00a085 | 0x000008 | 0x000200;

        let mut packet = Vec::with_capacity(256);
        packet.extend_from_slice(&(cap as u32).to_le_bytes());
        packet.extend_from_slice(&(16777215u32).to_le_bytes());
        packet.push(33);
        packet.extend_from_slice(&[0u8; 23]);
        packet.extend_from_slice(user_bytes);
        packet.push(0);
        packet.push(auth_response.len() as u8);
        packet.extend_from_slice(&auth_response);
        packet.extend_from_slice(db_bytes);
        packet.push(0);
        packet.push(b"mysql_native_password".len() as u8);
        packet.extend_from_slice(b"mysql_native_password");

        let len = packet.len() as u32;
        let header = [(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, 1];
        self.stream
            .write_all(&header)
            .await
            .map_err(|e| format!("send auth header failed: {}", e))?;
        self.stream
            .write_all(&packet)
            .await
            .map_err(|e| format!("send auth body failed: {}", e))?;

        Ok(())
    }

    async fn read_ok_response(&mut self) -> Result<(), String> {
        let resp = self.read_packet().await?;
        if resp.is_empty() {
            return Err("empty response".to_string());
        }
        match resp[0] {
            0x00 | 0xfe => Ok(()),
            0xff => {
                let err_msg = String::from_utf8_lossy(&resp[3..]);
                Err(format!("mysql error: {}", err_msg))
            }
            _ => Err(format!("unexpected response: {:02x}", resp[0])),
        }
    }

    pub async fn register_slave(&mut self) -> Result<(), String> {
        let host_bytes = self.config.host.as_bytes();
        let user_bytes = self.config.user.as_bytes();
        let pass_bytes = self.config.password.as_bytes();

        let mut args = Vec::with_capacity(128);
        args.extend_from_slice(&self.config.server_id.to_le_bytes());
        args.push(host_bytes.len() as u8);
        args.extend_from_slice(host_bytes);
        args.push(user_bytes.len() as u8);
        args.extend_from_slice(user_bytes);
        args.push(pass_bytes.len() as u8);
        args.extend_from_slice(pass_bytes);
        args.extend_from_slice(&self.config.port.to_le_bytes());
        args.extend_from_slice(&0u32.to_le_bytes());
        args.extend_from_slice(&0u32.to_le_bytes());

        self.write_command(0x15, &args).await?;
        self.read_ok_response().await
    }

    pub async fn dump_gtid(&mut self, gtid_set: &str) -> Result<(), String> {
        let mut args = Vec::with_capacity(256);
        args.extend_from_slice(&0u16.to_le_bytes());
        args.extend_from_slice(&self.config.server_id.to_le_bytes());
        args.push(0);
        args.extend_from_slice(&4u64.to_le_bytes());
        args.extend_from_slice(&0u32.to_le_bytes());

        let gtid_bytes = gtid_set.as_bytes();
        args.extend_from_slice(&(gtid_bytes.len() as u32).to_le_bytes());
        args.extend_from_slice(gtid_bytes);

        self.write_command(0x1e, &args).await
    }

    pub async fn dump_binlog(&mut self, filename: &str, pos: u32) -> Result<(), String> {
        let mut args = Vec::with_capacity(128);
        args.extend_from_slice(&pos.to_le_bytes());
        args.extend_from_slice(&0u16.to_le_bytes());
        args.extend_from_slice(&self.config.server_id.to_le_bytes());
        args.extend_from_slice(filename.as_bytes());
        args.push(0);

        self.write_command(0x12, &args).await?;
        self.read_ok_response().await
    }

    pub async fn next_event(&mut self) -> Result<Option<(EventHeader, BinlogEvent)>, String> {
        loop {
            let packet = self.read_packet().await?;
            if packet.is_empty() {
                return Ok(None);
            }

            if packet[0] == 0x00 {
                if packet.len() < 2 {
                    continue;
                }
                if packet[1] == 0x00 {
                    continue;
                }
            }

            let mut pos = 0;
            while pos + 19 <= packet.len() {
                let header = EventHeader {
                    timestamp: u32::from_le_bytes([packet[pos], packet[pos + 1], packet[pos + 2], packet[pos + 3]]),
                    event_type: packet[pos + 4],
                    server_id: u32::from_le_bytes([packet[pos + 5], packet[pos + 6], packet[pos + 7], packet[pos + 8]]),
                    event_size: u32::from_le_bytes([packet[pos + 9], packet[pos + 10], packet[pos + 11], packet[pos + 12]]),
                    log_pos: u32::from_le_bytes([packet[pos + 13], packet[pos + 14], packet[pos + 15], packet[pos + 16]]),
                    flags: u16::from_le_bytes([packet[pos + 17], packet[pos + 18]]),
                };

                let event_size = header.event_size as usize;
                if pos + event_size > packet.len() {
                    break;
                }

                let event_data = &packet[pos + 19..pos + event_size];

                let event = self.parse_event(&header, event_data)?;

                if let Some(ref evt) = event {
                    match evt {
                        BinlogEvent::Rotate(_, _) | BinlogEvent::FormatDescription => {}
                        _ => {
                            return Ok(Some((header, evt.clone())));
                        }
                    }
                }

                pos += event_size;
            }
        }
    }

    fn parse_event(&mut self, header: &EventHeader, data: &[u8]) -> Result<Option<BinlogEvent>, String> {
        match header.event_type {
            0x0f => self.parse_format_description(data),
            0x04 => self.parse_rotate(data),
            0x13 => self.parse_table_map(data),
            0x17 => self.parse_write_rows(header, data, 1),
            0x18 => self.parse_update_rows(header, data, 1),
            0x19 => self.parse_delete_rows(header, data, 1),
            0x1e => self.parse_write_rows(header, data, 2),
            0x1f => self.parse_update_rows(header, data, 2),
            0x20 => self.parse_delete_rows(header, data, 2),
            0x21 => self.parse_gtid(data),
            0x10 => self.parse_xid(data),
            0x1b => Ok(Some(BinlogEvent::Heartbeat)),
            _ => Ok(Some(BinlogEvent::Unknown(header.event_type))),
        }
    }

    fn parse_format_description(&mut self, _data: &[u8]) -> Result<Option<BinlogEvent>, String> {
        Ok(Some(BinlogEvent::FormatDescription))
    }

    fn parse_rotate(&mut self, data: &[u8]) -> Result<Option<BinlogEvent>, String> {
        if data.len() < 8 {
            return Ok(Some(BinlogEvent::Rotate(String::new(), 0)));
        }
        let pos = u64::from_le_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]);
        let filename = String::from_utf8_lossy(&data[8..]).trim_end_matches('\0').to_string();
        Ok(Some(BinlogEvent::Rotate(filename, pos)))
    }

    fn parse_table_map(&mut self, data: &[u8]) -> Result<Option<BinlogEvent>, String> {
        let (table_id, pos) = read_packed_u48(data);
        let _flags = u16::from_le_bytes([data[pos], data[pos + 1]]);
        let mut p = pos + 2;

        let db_end = data[p..].iter().position(|&b| b == 0).ok_or_else(|| {
            format!("missing null terminator for database name in table map at offset {}", p)
        })?;
        let database = String::from_utf8_lossy(&data[p..p + db_end]).to_string();
        p += db_end + 1;

        let table_end = data[p..].iter().position(|&b| b == 0).ok_or_else(|| {
            format!("missing null terminator for table name in table map at offset {}", p)
        })?;
        let table = String::from_utf8_lossy(&data[p..p + table_end]).to_string();
        p += table_end + 1;

        let (column_count, n) = read_packed_int(&data[p..]);
        p += n;

        let column_types = data[p..p + column_count as usize].to_vec();
        p += column_count as usize;

        let (_, meta_n) = read_packed_int(&data[p..]);
        p += meta_n;

        let mut column_metas = Vec::with_capacity(column_count as usize);
        for &ct in &column_types {
            let meta_len = match ct {
                0 | 246 => 2,
                1 | 2 | 3 | 4 | 5 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | 17 | 18 | 19 | 245 | 247 | 248 => 0,
                16 => {
                    if p < data.len() {
                        2
                    } else {
                        0
                    }
                }
                249..=255 => 1,
                _ => 0,
            };
            if meta_len == 0 {
                column_metas.push(0);
            } else if p + meta_len <= data.len() {
                match meta_len {
                    1 => column_metas.push(data[p] as u16),
                    2 => column_metas.push(u16::from_le_bytes([data[p], data[p + 1]])),
                    _ => column_metas.push(0),
                }
                p += meta_len;
            } else {
                column_metas.push(0);
            }
        }

        let null_bytes = (column_count as usize).div_ceil(8);
        let null_bits: Vec<bool> = if p + null_bytes <= data.len() {
            data[p..p + null_bytes]
                .iter()
                .flat_map(|&b| (0..8).map(move |i| (b >> i) & 1 == 1))
                .take(column_count as usize)
                .collect()
        } else {
            vec![false; column_count as usize]
        };

        let event = TableMapEvent {
            table_id,
            database,
            table,
            column_types,
            column_metas,
            null_bits,
        };

        self.tables.insert(table_id, event.clone());
        Ok(Some(BinlogEvent::TableMap(event)))
    }

    fn parse_write_rows(&mut self, _header: &EventHeader, data: &[u8], version: u8) -> Result<Option<BinlogEvent>, String> {
        let (table_id, mut pos) = read_packed_u48(data);
        let _flags = u16::from_le_bytes([data[pos], data[pos + 1]]);
        pos += 2;

        if version == 2 {
            pos += 2;
        }

        let (_, _col_count, n) = read_bitset(&data[pos..]);
        pos += n;

        let table = self.tables.get(&table_id).cloned();
        let column_types = table.as_ref().map(|t| t.column_types.clone()).unwrap_or_default();
        let column_metas = table.as_ref().map(|t| t.column_metas.clone()).unwrap_or_default();

        let mut rows = Vec::new();
        while pos < data.len() {
            let (row, n) = read_row(&data[pos..], &column_types, &column_metas);
            if n == 0 {
                break;
            }
            rows.push(RowEvent { columns: row });
            pos += n;
        }

        Ok(Some(BinlogEvent::WriteRows { table_id, rows }))
    }

    fn parse_update_rows(&mut self, _header: &EventHeader, data: &[u8], version: u8) -> Result<Option<BinlogEvent>, String> {
        let (table_id, mut pos) = read_packed_u48(data);
        let _flags = u16::from_le_bytes([data[pos], data[pos + 1]]);
        pos += 2;

        if version == 2 {
            pos += 2;
        }

        let (_, _col_count, n) = read_bitset(&data[pos..]);
        pos += n;

        let table = self.tables.get(&table_id).cloned();
        let column_types = table.as_ref().map(|t| t.column_types.clone()).unwrap_or_default();
        let column_metas = table.as_ref().map(|t| t.column_metas.clone()).unwrap_or_default();

        let mut rows = Vec::new();
        while pos < data.len() {
            let (before, n1) = read_row(&data[pos..], &column_types, &column_metas);
            if n1 == 0 {
                break;
            }
            pos += n1;
            let (after, n2) = read_row(&data[pos..], &column_types, &column_metas);
            if n2 == 0 {
                break;
            }
            rows.push((RowEvent { columns: before }, RowEvent { columns: after }));
            pos += n2;
        }

        Ok(Some(BinlogEvent::UpdateRows { table_id, rows }))
    }

    fn parse_delete_rows(&mut self, _header: &EventHeader, data: &[u8], version: u8) -> Result<Option<BinlogEvent>, String> {
        let (table_id, mut pos) = read_packed_u48(data);
        let _flags = u16::from_le_bytes([data[pos], data[pos + 1]]);
        pos += 2;

        if version == 2 {
            pos += 2;
        }

        let (_, _col_count, n) = read_bitset(&data[pos..]);
        pos += n;

        let table = self.tables.get(&table_id).cloned();
        let column_types = table.as_ref().map(|t| t.column_types.clone()).unwrap_or_default();
        let column_metas = table.as_ref().map(|t| t.column_metas.clone()).unwrap_or_default();

        let mut rows = Vec::new();
        while pos < data.len() {
            let (row, n) = read_row(&data[pos..], &column_types, &column_metas);
            if n == 0 {
                break;
            }
            rows.push(RowEvent { columns: row });
            pos += n;
        }

        Ok(Some(BinlogEvent::DeleteRows { table_id, rows }))
    }

    fn parse_xid(&mut self, data: &[u8]) -> Result<Option<BinlogEvent>, String> {
        if data.len() < 8 {
            return Ok(Some(BinlogEvent::Xid(0)));
        }
        let xid = u64::from_le_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]);
        Ok(Some(BinlogEvent::Xid(xid)))
    }

    fn parse_gtid(&mut self, data: &[u8]) -> Result<Option<BinlogEvent>, String> {
        if data.len() < 20 {
            return Ok(None);
        }
        let _flags = data[0];
        let uuid_bytes = &data[1..17];
        let uuid_val = uuid::Uuid::from_slice(uuid_bytes).map_err(|e| format!("invalid uuid: {}", e))?;
        let gno = u64::from_le_bytes([
            data[17], data[18], data[19], data[20], data[21], data[22], data[23], data[24],
        ]);

        self.current_gtid = Some((uuid_val, gno));
        Ok(Some(BinlogEvent::Gtid(uuid_val, gno)))
    }
}

fn read_packed_u48(data: &[u8]) -> (u64, usize) {
    if data.len() < 6 {
        return (0, data.len());
    }
    let val = u64::from(data[0])
        | (u64::from(data[1]) << 8)
        | (u64::from(data[2]) << 16)
        | (u64::from(data[3]) << 24)
        | (u64::from(data[4]) << 32)
        | (u64::from(data[5]) << 40);
    (val, 6)
}

fn read_packed_int(data: &[u8]) -> (u64, usize) {
    if data.is_empty() {
        return (0, 0);
    }
    match data[0] {
        b if b < 251 => (b as u64, 1),
        251 => (0, 1),
        252 => {
            let val = u16::from_le_bytes([data[1], data[2]]);
            (val as u64, 3)
        }
        253 => {
            let val = u32::from_le_bytes([data[1], data[2], data[3], 0]);
            (val as u64, 4)
        }
        254 => {
            let val = u64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]);
            (val, 9)
        }
        _ => (0, 0),
    }
}

fn read_bitset(data: &[u8]) -> (Vec<bool>, u64, usize) {
    let (col_count, n) = read_packed_int(data);
    if col_count == 0 || n == 0 {
        return (Vec::new(), 0, 0);
    }
    let byte_count = (col_count as usize).div_ceil(8);
    let total = n + byte_count;
    if total > data.len() {
        return (Vec::new(), col_count, total);
    }
    let bits: Vec<bool> = data[n..total]
        .iter()
        .flat_map(|&b| (0..8).map(move |i| (b >> i) & 1 == 1))
        .take(col_count as usize)
        .collect();
    (bits, col_count, total)
}

fn read_row(data: &[u8], column_types: &[u8], column_metas: &[u16]) -> (Vec<Value>, usize) {
    if data.is_empty() || column_types.is_empty() {
        return (Vec::new(), 0);
    }

    let null_bytes = column_types.len().div_ceil(8);
    if data.len() < null_bytes {
        return (Vec::new(), 0);
    }

    let null_bits: Vec<bool> = data[..null_bytes]
        .iter()
        .flat_map(|&b| (0..8).map(move |i| (b >> i) & 1 == 1))
        .take(column_types.len())
        .collect();

    let mut pos = null_bytes;
    let mut values = Vec::with_capacity(column_types.len());

    for (i, &col_type) in column_types.iter().enumerate() {
        if i < null_bits.len() && null_bits[i] {
            values.push(Value::Null);
            continue;
        }

        let meta = column_metas.get(i).copied().unwrap_or(0);
        let byte_len = column_data_length(col_type, meta);

        if byte_len > 0 && pos + byte_len <= data.len() {
            let val = decode_binlog_value(&data[pos..pos + byte_len], col_type, meta);
            values.push(val);
            pos += byte_len;
        } else if byte_len == 0 && pos < data.len() {
            let n;
            let val: Value;
            if col_type == MYSQL_TYPE_VARCHAR || col_type == MYSQL_TYPE_VAR_STRING || col_type == MYSQL_TYPE_STRING {
                let (len, s) = decode_length_encoded_string(&data[pos..]);
                if len > 0 {
                    val = serde_json::Value::String(s);
                    n = len;
                } else {
                    val = Value::Null;
                    n = 1;
                }
            } else if col_type == MYSQL_TYPE_BLOB || col_type == MYSQL_TYPE_TINY_BLOB
                || col_type == MYSQL_TYPE_MEDIUM_BLOB || col_type == MYSQL_TYPE_LONG_BLOB
            {
                let blob_len = match col_type {
                    MYSQL_TYPE_TINY_BLOB => 1,
                    MYSQL_TYPE_BLOB => 2,
                    MYSQL_TYPE_MEDIUM_BLOB => 3,
                    MYSQL_TYPE_LONG_BLOB => 4,
                    _ => 2,
                };
                if pos + blob_len <= data.len() {
                    let size = match blob_len {
                        1 => data[pos] as usize,
                        2 => u16::from_le_bytes([data[pos], data[pos + 1]]) as usize,
                        3 => u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], 0]) as usize,
                        _ => 0,
                    };
                    let start = pos + blob_len;
                    if start + size <= data.len() {
                        let s = String::from_utf8_lossy(&data[start..start + size]).to_string();
                        val = Value::String(s);
                        n = blob_len + size;
                    } else {
                        val = Value::Null;
                        n = blob_len;
                    }
                } else {
                    val = Value::Null;
                    n = blob_len;
                }
            } else {
                val = Value::Null;
                n = 1;
            }
            values.push(val);
            pos += n;
        } else {
            values.push(Value::Null);
        }
    }

    (values, pos)
}

fn column_data_length(col_type: u8, meta: u16) -> usize {
    match col_type {
        MYSQL_TYPE_TINY => 1,
        MYSQL_TYPE_SHORT | MYSQL_TYPE_YEAR => 2,
        MYSQL_TYPE_LONG | MYSQL_TYPE_INT24 | MYSQL_TYPE_FLOAT | MYSQL_TYPE_DATE
        | MYSQL_TYPE_TIME | MYSQL_TYPE_TIMESTAMP => 4,
        MYSQL_TYPE_LONGLONG | MYSQL_TYPE_DOUBLE | MYSQL_TYPE_DATETIME => 8,
        MYSQL_TYPE_BIT => ((meta >> 8) * 8 + (meta & 0xff) + 7) as usize / 8,
        MYSQL_TYPE_NEWDECIMAL => (meta >> 8) as usize + 4,
        MYSQL_TYPE_TIMESTAMP2 | MYSQL_TYPE_DATETIME2 | MYSQL_TYPE_TIME2 => {
            let frac = meta & 0xff;
            if frac > 0 {
                5 + frac.div_ceil(2) as usize
            } else {
                5
            }
        }
        _ => 0,
    }
}
