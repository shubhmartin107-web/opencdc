use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use opencdc_core::ConnectorType;
use opencdc_core::change_event::ChangeEvent;
use opencdc_core::error::{Error, Result};
use opencdc_core::offset::ConnectorOffset;
use opencdc_core::source_info::SourceInfo;

use super::config::MySqlConnectorConfig;

pub struct MySqlSnapshotter;

impl MySqlSnapshotter {
    pub async fn run(
        config: &MySqlConnectorConfig,
        tables: &[String],
        sink: &tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> Result<ConnectorOffset> {
        let table_list = if tables.is_empty() {
            Self::discover_tables(config).await?
        } else {
            tables.to_vec()
        };

        let stream = TcpStream::connect(format!("{}:{}", config.host, config.port))
            .await
            .map_err(|e| Error::Other(format!("snapshot connect failed: {}", e)))?;

        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);

        let handshake = read_mysql_packet(&mut reader).await?;
        let auth_data = parse_greeting(&handshake)?;

        send_auth_packet(&mut writer, config, &auth_data).await?;
        read_ok_packet(&mut reader).await?;

        for table in &table_list {
            if sink.is_closed() {
                break;
            }
            let parts: Vec<&str> = table.splitn(2, '.').collect();
            let (db, tbl) = if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                (config.database.as_str(), parts[0])
            };

            let query = format!("SELECT * FROM `{}`.`{}`", db, tbl);
            let rows = Self::query_table(&mut reader, &mut writer, &query).await?;

            for row in &rows {
                let source = SourceInfo::new(&ConnectorType::Mysql, db, None::<&str>, tbl);
                let event = ChangeEvent::snapshot(serde_json::Value::Object(row.clone()), source);
                if sink.send(event).await.is_err() {
                    return Err(Error::Other("snapshot sink closed".to_string()));
                }
            }
        }

        Ok(ConnectorOffset::snapshot_done())
    }

    async fn discover_tables(config: &MySqlConnectorConfig) -> Result<Vec<String>> {
        let stream = TcpStream::connect(format!("{}:{}", config.host, config.port))
            .await
            .map_err(|e| Error::Other(format!("discover connect failed: {}", e)))?;

        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);

        let handshake = read_mysql_packet(&mut reader).await?;
        let auth_data = parse_greeting(&handshake)?;

        send_auth_packet(&mut writer, config, &auth_data).await?;
        read_ok_packet(&mut reader).await?;

        let query = format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = '{}' AND TABLE_TYPE = 'BASE TABLE'",
            config.database
        );

        let rows = Self::query_table(&mut reader, &mut writer, &query).await?;
        let tables: Vec<String> = rows
            .iter()
            .filter_map(|row| {
                let schema = row.get("TABLE_SCHEMA").and_then(|v| v.as_str())?;
                let name = row.get("TABLE_NAME").and_then(|v| v.as_str())?;
                Some(format!("{}.{}", schema, name))
            })
            .collect();

        Ok(tables)
    }

    async fn query_table(
        reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
        writer: &mut tokio::net::tcp::OwnedWriteHalf,
        query: &str,
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>> {
        let q = query.as_bytes().to_vec();
        let len = q.len() as u32;
        let header = [
            (len & 0xff) as u8,
            ((len >> 8) & 0xff) as u8,
            ((len >> 16) & 0xff) as u8,
            0,
        ];
        writer
            .write_all(&header)
            .await
            .map_err(|e| Error::Other(format!("write query header: {}", e)))?;
        writer
            .write_all(&q)
            .await
            .map_err(|e| Error::Other(format!("write query body: {}", e)))?;

        let resp = read_mysql_packet(reader).await?;
        if resp.is_empty() {
            return Ok(Vec::new());
        }
        if resp[0] == 0xff {
            let err = String::from_utf8_lossy(&resp[3..]);
            return Err(Error::Other(format!("mysql query error: {}", err)));
        }

        let col_count = resp[0] as usize;
        for _ in 0..col_count {
            read_mysql_packet(reader).await?;
        }

        read_mysql_packet(reader).await?;

        let mut results = Vec::new();
        loop {
            let row_packet = read_mysql_packet(reader).await?;
            if row_packet.is_empty() || row_packet[0] == 0xfe {
                break;
            }

            let mut row = serde_json::Map::new();
            let mut pos = 0;
            for i in 0..col_count {
                if pos >= row_packet.len() {
                    break;
                }
                if row_packet[pos] == 0xfb {
                    row.insert(format!("col_{}", i), serde_json::Value::Null);
                    pos += 1;
                } else {
                    let (n, s) = decode_length_str(&row_packet[pos..]);
                    pos += n;
                    row.insert(format!("col_{}", i), serde_json::Value::String(s));
                }
            }
            results.push(row);
        }

        Ok(results)
    }
}

async fn read_mysql_packet(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;

    let mut header = [0u8; 4];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|e| Error::Other(format!("read packet header: {}", e)))?;

    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    if len == 0 {
        return Ok(Vec::new());
    }

    let mut data = vec![0u8; len];
    reader
        .read_exact(&mut data)
        .await
        .map_err(|e| Error::Other(format!("read packet body: {}", e)))?;

    Ok(data)
}

fn parse_greeting(data: &[u8]) -> Result<Vec<u8>> {
    if data.is_empty() {
        return Err(Error::Other("empty greeting".to_string()));
    }
    if data[0] == 0xff {
        let err = String::from_utf8_lossy(&data[3..]);
        return Err(Error::Other(format!("mysql greeting error: {}", err)));
    }
    if data[0] != 10 {
        return Err(Error::Other(format!("unsupported protocol: {}", data[0])));
    }

    let mut pos = 1;
    while pos < data.len() && data[pos] != 0 {
        pos += 1;
    }
    pos += 1;
    pos += 4;
    let auth1 = &data[pos..pos + 8];
    pos += 8 + 1 + 2 + 1 + 2;
    let _cap2 = u16::from_le_bytes([data[pos], data[pos + 1]]);
    pos += 2;
    let _ = pos;
    let auth_len = if pos < data.len() {
        data[pos] as usize
    } else {
        0
    };
    pos += 1;

    let mut auth = auth1.to_vec();
    if auth_len > 8 && pos + (auth_len - 8) <= data.len() {
        auth.extend_from_slice(&data[pos..pos + (auth_len - 8)]);
    }

    Ok(auth)
}

async fn send_auth_packet(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    config: &MySqlConnectorConfig,
    scramble: &[u8],
) -> Result<()> {
    use sha1::{Digest, Sha1};

    let mut hasher = Sha1::new();
    hasher.update(config.password.as_bytes());
    let hash1 = hasher.finalize();

    let mut hasher = Sha1::new();
    hasher.update(hash1);
    let hash2 = hasher.finalize();

    let mut hasher = Sha1::new();
    hasher.update(scramble);
    hasher.update(hash2);
    let hash3 = hasher.finalize();

    let mut auth_response = Vec::with_capacity(20);
    for (a, b) in hash1.iter().zip(hash3.iter()) {
        auth_response.push(a ^ b);
    }

    let cap: u32 = 0x00a085 | 0x000008 | 0x000200;
    let mut packet = Vec::with_capacity(256);
    packet.extend_from_slice(&cap.to_le_bytes());
    packet.extend_from_slice(&16777215u32.to_le_bytes());
    packet.push(33);
    packet.extend_from_slice(&[0u8; 23]);
    packet.extend_from_slice(config.user.as_bytes());
    packet.push(0);
    packet.push(auth_response.len() as u8);
    packet.extend_from_slice(&auth_response);
    packet.extend_from_slice(config.database.as_bytes());
    packet.push(0);

    let len = packet.len() as u32;
    let header = [
        (len & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        ((len >> 16) & 0xff) as u8,
        1,
    ];
    writer
        .write_all(&header)
        .await
        .map_err(|e| Error::Other(format!("auth header: {}", e)))?;
    writer
        .write_all(&packet)
        .await
        .map_err(|e| Error::Other(format!("auth body: {}", e)))?;

    Ok(())
}

async fn read_ok_packet(reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> Result<()> {
    let resp = read_mysql_packet(reader).await?;
    if resp.is_empty() {
        return Err(Error::Other("empty auth response".to_string()));
    }
    match resp[0] {
        0x00 | 0xfe => Ok(()),
        0xff => {
            let err = String::from_utf8_lossy(&resp[3..]);
            Err(Error::Other(format!("auth failed: {}", err)))
        }
        b => Err(Error::Other(format!("unexpected auth response: {:02x}", b))),
    }
}

fn decode_length_str(data: &[u8]) -> (usize, String) {
    if data.is_empty() {
        return (0, String::new());
    }
    let len = data[0] as usize;
    if len < data.len() {
        let s = String::from_utf8_lossy(&data[1..1 + len]).to_string();
        (1 + len, s)
    } else {
        (1, String::new())
    }
}
