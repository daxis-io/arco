#![allow(clippy::redundant_pub_crate)]

use std::sync::Arc;

use arrow::record_batch::RecordBatchReader;
use bytes::Bytes;
use datafusion::datasource::MemTable;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::ApiError;

pub(crate) fn parquet_bytes_to_mem_table(bytes: Bytes) -> Result<Arc<MemTable>, ApiError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|err| ApiError::internal(format!("failed to read parquet bytes: {err}")))?;
    let reader = builder
        .build()
        .map_err(|err| ApiError::internal(format!("failed to build parquet reader: {err}")))?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    for batch in reader {
        let batch = batch
            .map_err(|err| ApiError::internal(format!("failed to decode parquet batch: {err}")))?;
        batches.push(batch);
    }

    let table = MemTable::try_new(schema, vec![batches])
        .map_err(|err| ApiError::internal(format!("failed to register table: {err}")))?;
    Ok(Arc::new(table))
}
