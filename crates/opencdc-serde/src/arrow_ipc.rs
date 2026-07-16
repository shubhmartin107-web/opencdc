use arrow::ipc::reader::{FileReader, StreamReader};
use arrow::ipc::writer::{FileWriter, StreamWriter};
use arrow::record_batch::RecordBatch;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use opencdc_core::error::{Error, Result};

pub struct ArrowIpcSerde;

impl ArrowIpcSerde {
    pub fn write_file(batch: &RecordBatch, path: impl AsRef<Path>) -> Result<()> {
        let file = File::create(path.as_ref()).map_err(|e| {
            Error::Serialization(format!("failed to create arrow file: {}", e))
        })?;
        let writer = BufWriter::new(file);
        let mut arrow_writer =
            FileWriter::try_new(writer, batch.schema().as_ref()).map_err(Error::Arrow)?;
        arrow_writer.write(batch).map_err(Error::Arrow)?;
        arrow_writer.finish().map_err(Error::Arrow)?;
        Ok(())
    }

    pub fn read_file(path: impl AsRef<Path>) -> Result<Vec<RecordBatch>> {
        let file = File::open(path.as_ref()).map_err(|e| {
            Error::Deserialization(format!("failed to open arrow file: {}", e))
        })?;
        let reader = BufReader::new(file);
        let arrow_reader =
            FileReader::try_new(reader, None).map_err(Error::Arrow)?;
        let batches: Vec<RecordBatch> = arrow_reader
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        Ok(batches)
    }

    pub fn write_stream(batches: &[RecordBatch], writer: &mut dyn Write) -> Result<()> {
        let schema = batches.first().map(|b| b.schema()).unwrap_or(Arc::new(
            arrow::datatypes::Schema::empty(),
        ));
        let mut stream_writer =
            StreamWriter::try_new(writer, &schema).map_err(Error::Arrow)?;
        for batch in batches {
            stream_writer.write(batch).map_err(Error::Arrow)?;
        }
        stream_writer.finish().map_err(Error::Arrow)?;
        Ok(())
    }

    pub fn read_stream(data: &[u8]) -> Result<Vec<RecordBatch>> {
        let reader =
            StreamReader::try_new(data, None).map_err(Error::Arrow)?;
        let batches: Vec<RecordBatch> = reader.into_iter().filter_map(|r| r.ok()).collect();
        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_ipc_file_roundtrip() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let path = std::env::temp_dir().join("test_opencdc_arrow.ipc");
        ArrowIpcSerde::write_file(&batch, &path).unwrap();
        let batches = ArrowIpcSerde::read_file(&path).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_ipc_file_invalid_path() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let result = ArrowIpcSerde::write_file(&batch, "/nonexistent/dir/file.arrow");
        assert!(result.is_err());
    }

    #[test]
    fn test_ipc_read_invalid_file() {
        let result = ArrowIpcSerde::read_file("/nonexistent/file.arrow");
        assert!(result.is_err());
    }

    #[test]
    fn test_ipc_stream_roundtrip() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let mut buf: Vec<u8> = Vec::new();
        ArrowIpcSerde::write_stream(&[batch.clone()], &mut buf).unwrap();
        let batches = ArrowIpcSerde::read_stream(&buf).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }
}
