//! Parquet encoding/decoding helpers for Tier-1 catalog snapshots.
//!
//! This module defines the canonical Parquet schemas for Tier-1 snapshot files:
//! - `namespaces.parquet`
//! - `tables.parquet`
//! - `columns.parquet`
//! - `lineage_edges.parquet`
//!
//! The schemas here are the contract for browser reads (DuckDB-WASM) and API
//! clients. Keep changes backwards-compatible and gated by snapshot versioning.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Array as _, BooleanArray, Float32Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use parquet::format::KeyValue;
use serde::{Deserialize, Serialize};

use crate::error::{CatalogError, Result};

/// Record stored in `namespaces.parquet`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceRecord {
    /// Namespace ID (UUID v7).
    pub id: String,
    /// Namespace name.
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Creation timestamp (ms since epoch).
    pub created_at: i64,
    /// Update timestamp (ms since epoch).
    pub updated_at: i64,
}

/// Record stored in `tables.parquet`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRecord {
    /// Table ID (UUID v7).
    pub id: String,
    /// Namespace ID (UUID v7).
    pub namespace_id: String,
    /// Table name.
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Optional storage location (URI).
    pub location: Option<String>,
    /// Optional file format (e.g., "parquet", "iceberg").
    pub format: Option<String>,
    /// Creation timestamp (ms since epoch).
    pub created_at: i64,
    /// Update timestamp (ms since epoch).
    pub updated_at: i64,
}

/// Record stored in `columns.parquet`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnRecord {
    /// Column ID (UUID v7).
    pub id: String,
    /// Parent table ID.
    pub table_id: String,
    /// Column name.
    pub name: String,
    /// Data type (stringified).
    pub data_type: String,
    /// Whether column is nullable.
    pub is_nullable: bool,
    /// Ordinal position (0-indexed).
    pub ordinal: i32,
    /// Optional description.
    pub description: Option<String>,
}

/// Record stored in `lineage_edges.parquet`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageEdgeRecord {
    /// Edge ID (ULID).
    pub id: String,
    /// Source entity ID.
    pub source_id: String,
    /// Target entity ID.
    pub target_id: String,
    /// Edge type.
    pub edge_type: String,
    /// Optional run ID.
    pub run_id: Option<String>,
    /// Creation timestamp (ms since epoch).
    pub created_at: i64,
}

/// Record stored in `token_postings.parquet`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchPostingRecord {
    /// Token as indexed.
    pub token: String,
    /// Normalized token (lowercase).
    pub token_norm: String,
    /// Document type (namespace/table/column).
    pub doc_type: String,
    /// Document identifier.
    pub doc_id: String,
    /// Field the token came from (name/description).
    pub field: String,
    /// Relevance score for ranking.
    pub score: f32,
}

fn namespaces_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("created_at", DataType::Int64, false),
        Field::new("updated_at", DataType::Int64, false),
    ]))
}

fn tables_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("namespace_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("location", DataType::Utf8, true),
        Field::new("format", DataType::Utf8, true),
        Field::new("created_at", DataType::Int64, false),
        Field::new("updated_at", DataType::Int64, false),
    ]))
}

fn columns_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("table_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("is_nullable", DataType::Boolean, false),
        Field::new("ordinal", DataType::Int32, false),
        Field::new("description", DataType::Utf8, true),
    ]))
}

fn lineage_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("source_id", DataType::Utf8, false),
        Field::new("target_id", DataType::Utf8, false),
        Field::new("edge_type", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, true),
        Field::new("created_at", DataType::Int64, false),
    ]))
}

fn search_postings_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("token", DataType::Utf8, false),
        Field::new("token_norm", DataType::Utf8, false),
        Field::new("doc_type", DataType::Utf8, false),
        Field::new("doc_id", DataType::Utf8, false),
        Field::new("field", DataType::Utf8, false),
        Field::new("score", DataType::Float32, false),
    ]))
}

// ============================================================================
// Public Schema Accessors (for tests and external consumers)
// ============================================================================

/// Returns the namespace schema for golden file comparison.
#[must_use]
pub fn namespace_schema() -> Schema {
    (*namespaces_schema()).clone()
}

/// Returns the table schema for golden file comparison.
#[must_use]
pub fn table_schema() -> Schema {
    (*tables_schema()).clone()
}

/// Returns the column schema for golden file comparison.
#[must_use]
pub fn column_schema() -> Schema {
    (*columns_schema()).clone()
}

/// Returns the lineage edge schema for golden file comparison.
#[must_use]
pub fn lineage_edge_schema() -> Schema {
    (*lineage_schema()).clone()
}

/// Returns the search posting schema for golden file comparison.
#[must_use]
pub fn search_posting_schema() -> Schema {
    (*search_postings_schema()).clone()
}

fn writer_properties() -> WriterProperties {
    // Keep properties minimal and widely compatible with DuckDB readers.
    let created_by = KeyValue {
        key: "created_by".to_string(),
        value: Some("arco-catalog".to_string()),
    };
    WriterProperties::builder()
        .set_key_value_metadata(Some(vec![created_by]))
        .build()
}

fn write_single_batch(schema: Arc<Schema>, batch: &RecordBatch) -> Result<Bytes> {
    let mut cursor = Cursor::new(Vec::<u8>::new());
    let props = writer_properties();
    let mut writer = ArrowWriter::try_new(&mut cursor, schema, Some(props)).map_err(|e| {
        CatalogError::Parquet {
            message: format!("parquet writer init failed: {e}"),
        }
    })?;
    writer.write(batch).map_err(|e| CatalogError::Parquet {
        message: format!("parquet write failed: {e}"),
    })?;
    writer.close().map_err(|e| CatalogError::Parquet {
        message: format!("parquet close failed: {e}"),
    })?;
    Ok(Bytes::from(cursor.into_inner()))
}

/// Writes `namespaces.parquet`.
///
/// # Errors
///
/// Returns an error if the record batch cannot be built or the Parquet write
/// fails.
pub fn write_namespaces(rows: &[NamespaceRecord]) -> Result<Bytes> {
    let schema = namespaces_schema();

    let ids = StringArray::from(rows.iter().map(|r| Some(r.id.as_str())).collect::<Vec<_>>());
    let names = StringArray::from(
        rows.iter()
            .map(|r| Some(r.name.as_str()))
            .collect::<Vec<_>>(),
    );
    let descriptions = StringArray::from(
        rows.iter()
            .map(|r| r.description.as_deref())
            .collect::<Vec<_>>(),
    );
    let created_at = Int64Array::from(rows.iter().map(|r| r.created_at).collect::<Vec<_>>());
    let updated_at = Int64Array::from(rows.iter().map(|r| r.updated_at).collect::<Vec<_>>());

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ids),
            Arc::new(names),
            Arc::new(descriptions),
            Arc::new(created_at),
            Arc::new(updated_at),
        ],
    )
    .map_err(|e| CatalogError::Parquet {
        message: format!("record batch build failed: {e}"),
    })?;

    write_single_batch(schema, &batch)
}

/// Writes `tables.parquet`.
///
/// # Errors
///
/// Returns an error if the record batch cannot be built or the Parquet write
/// fails.
pub fn write_tables(rows: &[TableRecord]) -> Result<Bytes> {
    let schema = tables_schema();

    let ids = StringArray::from(rows.iter().map(|r| Some(r.id.as_str())).collect::<Vec<_>>());
    let namespace_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.namespace_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let names = StringArray::from(
        rows.iter()
            .map(|r| Some(r.name.as_str()))
            .collect::<Vec<_>>(),
    );
    let descriptions = StringArray::from(
        rows.iter()
            .map(|r| r.description.as_deref())
            .collect::<Vec<_>>(),
    );
    let locations = StringArray::from(
        rows.iter()
            .map(|r| r.location.as_deref())
            .collect::<Vec<_>>(),
    );
    let formats = StringArray::from(rows.iter().map(|r| r.format.as_deref()).collect::<Vec<_>>());
    let created_at = Int64Array::from(rows.iter().map(|r| r.created_at).collect::<Vec<_>>());
    let updated_at = Int64Array::from(rows.iter().map(|r| r.updated_at).collect::<Vec<_>>());

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ids),
            Arc::new(namespace_ids),
            Arc::new(names),
            Arc::new(descriptions),
            Arc::new(locations),
            Arc::new(formats),
            Arc::new(created_at),
            Arc::new(updated_at),
        ],
    )
    .map_err(|e| CatalogError::Parquet {
        message: format!("record batch build failed: {e}"),
    })?;

    write_single_batch(schema, &batch)
}

/// Writes `columns.parquet`.
///
/// # Errors
///
/// Returns an error if the record batch cannot be built or the Parquet write
/// fails.
pub fn write_columns(rows: &[ColumnRecord]) -> Result<Bytes> {
    let schema = columns_schema();

    let ids = StringArray::from(rows.iter().map(|r| Some(r.id.as_str())).collect::<Vec<_>>());
    let table_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.table_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let names = StringArray::from(
        rows.iter()
            .map(|r| Some(r.name.as_str()))
            .collect::<Vec<_>>(),
    );
    let data_types = StringArray::from(
        rows.iter()
            .map(|r| Some(r.data_type.as_str()))
            .collect::<Vec<_>>(),
    );
    let is_nullable = BooleanArray::from(rows.iter().map(|r| r.is_nullable).collect::<Vec<_>>());
    let ordinal = Int32Array::from(rows.iter().map(|r| r.ordinal).collect::<Vec<_>>());
    let descriptions = StringArray::from(
        rows.iter()
            .map(|r| r.description.as_deref())
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ids),
            Arc::new(table_ids),
            Arc::new(names),
            Arc::new(data_types),
            Arc::new(is_nullable),
            Arc::new(ordinal),
            Arc::new(descriptions),
        ],
    )
    .map_err(|e| CatalogError::Parquet {
        message: format!("record batch build failed: {e}"),
    })?;

    write_single_batch(schema, &batch)
}

/// Writes `lineage_edges.parquet`.
///
/// # Errors
///
/// Returns an error if the record batch cannot be built or the Parquet write
/// fails.
pub fn write_lineage_edges(rows: &[LineageEdgeRecord]) -> Result<Bytes> {
    let schema = lineage_schema();

    let ids = StringArray::from(rows.iter().map(|r| Some(r.id.as_str())).collect::<Vec<_>>());
    let source_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.source_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let target_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.target_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let edge_types = StringArray::from(
        rows.iter()
            .map(|r| Some(r.edge_type.as_str()))
            .collect::<Vec<_>>(),
    );
    let run_ids = StringArray::from(rows.iter().map(|r| r.run_id.as_deref()).collect::<Vec<_>>());
    let created_at = Int64Array::from(rows.iter().map(|r| r.created_at).collect::<Vec<_>>());

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ids),
            Arc::new(source_ids),
            Arc::new(target_ids),
            Arc::new(edge_types),
            Arc::new(run_ids),
            Arc::new(created_at),
        ],
    )
    .map_err(|e| CatalogError::Parquet {
        message: format!("record batch build failed: {e}"),
    })?;

    write_single_batch(schema, &batch)
}

/// Writes `token_postings.parquet`.
///
/// # Errors
/// Returns an error if record batch construction or parquet serialization fails.
pub fn write_search_postings(rows: &[SearchPostingRecord]) -> Result<Bytes> {
    let schema = search_postings_schema();

    let tokens = StringArray::from(
        rows.iter()
            .map(|r| Some(r.token.as_str()))
            .collect::<Vec<_>>(),
    );
    let token_norms = StringArray::from(
        rows.iter()
            .map(|r| Some(r.token_norm.as_str()))
            .collect::<Vec<_>>(),
    );
    let doc_types = StringArray::from(
        rows.iter()
            .map(|r| Some(r.doc_type.as_str()))
            .collect::<Vec<_>>(),
    );
    let doc_ids = StringArray::from(
        rows.iter()
            .map(|r| Some(r.doc_id.as_str()))
            .collect::<Vec<_>>(),
    );
    let fields = StringArray::from(
        rows.iter()
            .map(|r| Some(r.field.as_str()))
            .collect::<Vec<_>>(),
    );
    let scores = Float32Array::from(rows.iter().map(|r| r.score).collect::<Vec<_>>());

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(tokens),
            Arc::new(token_norms),
            Arc::new(doc_types),
            Arc::new(doc_ids),
            Arc::new(fields),
            Arc::new(scores),
        ],
    )
    .map_err(|e| CatalogError::Parquet {
        message: format!("record batch build failed: {e}"),
    })?;

    write_single_batch(schema, &batch)
}

fn read_batches(bytes: &Bytes) -> Result<Vec<RecordBatch>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .map_err(|e| CatalogError::Parquet {
            message: format!("parquet reader init failed: {e}"),
        })?
        .build()
        .map_err(|e| CatalogError::Parquet {
            message: format!("parquet reader build failed: {e}"),
        })?;

    let mut batches = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| CatalogError::Parquet {
            message: format!("parquet read batch failed: {e}"),
        })?;
        batches.push(batch);
    }
    Ok(batches)
}

fn col_string<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|e| CatalogError::InvariantViolation {
            message: format!("missing column '{name}': {e}"),
        })?;

    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| CatalogError::InvariantViolation {
            message: format!("column '{name}' is not StringArray"),
        })
}

fn col_string_optional<'a>(batch: &'a RecordBatch, name: &str) -> Result<Option<&'a StringArray>> {
    let idx = match batch.schema().index_of(name) {
        Ok(idx) => idx,
        Err(_) => return Ok(None),
    };

    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .map(Some)
        .ok_or_else(|| CatalogError::InvariantViolation {
            message: format!("column '{name}' is not StringArray"),
        })
}

fn col_i64<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|e| CatalogError::InvariantViolation {
            message: format!("missing column '{name}': {e}"),
        })?;

    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| CatalogError::InvariantViolation {
            message: format!("column '{name}' is not Int64Array"),
        })
}

fn col_i32<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int32Array> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|e| CatalogError::InvariantViolation {
            message: format!("missing column '{name}': {e}"),
        })?;

    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| CatalogError::InvariantViolation {
            message: format!("column '{name}' is not Int32Array"),
        })
}

fn col_f32<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Float32Array> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|e| CatalogError::InvariantViolation {
            message: format!("missing column '{name}': {e}"),
        })?;

    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| CatalogError::InvariantViolation {
            message: format!("column '{name}' is not Float32Array"),
        })
}

fn col_bool<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a BooleanArray> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|e| CatalogError::InvariantViolation {
            message: format!("missing column '{name}': {e}"),
        })?;

    batch
        .column(idx)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| CatalogError::InvariantViolation {
            message: format!("column '{name}' is not BooleanArray"),
        })
}

/// Reads `namespaces.parquet`.
///
/// # Errors
///
/// Returns an error if the Parquet payload is invalid or required columns are
/// missing.
pub fn read_namespaces(bytes: &Bytes) -> Result<Vec<NamespaceRecord>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let id = col_string(&batch, "id")?;
        let name = col_string(&batch, "name")?;
        let description = col_string_optional(&batch, "description")?;
        let created_at = col_i64(&batch, "created_at")?;
        let updated_at = col_i64(&batch, "updated_at")?;

        for row in 0..batch.num_rows() {
            out.push(NamespaceRecord {
                id: id.value(row).to_string(),
                name: name.value(row).to_string(),
                description: description.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                created_at: created_at.value(row),
                updated_at: updated_at.value(row),
            });
        }
    }
    Ok(out)
}

/// Reads `tables.parquet`.
///
/// # Errors
///
/// Returns an error if the Parquet payload is invalid or required columns are
/// missing.
pub fn read_tables(bytes: &Bytes) -> Result<Vec<TableRecord>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let id = col_string(&batch, "id")?;
        let namespace_id = col_string(&batch, "namespace_id")?;
        let name = col_string(&batch, "name")?;
        let description = col_string_optional(&batch, "description")?;
        let location = col_string_optional(&batch, "location")?;
        let format = col_string_optional(&batch, "format")?;
        let created_at = col_i64(&batch, "created_at")?;
        let updated_at = col_i64(&batch, "updated_at")?;

        for row in 0..batch.num_rows() {
            out.push(TableRecord {
                id: id.value(row).to_string(),
                namespace_id: namespace_id.value(row).to_string(),
                name: name.value(row).to_string(),
                description: description.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                location: location.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                format: format.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                created_at: created_at.value(row),
                updated_at: updated_at.value(row),
            });
        }
    }
    Ok(out)
}

/// Reads `columns.parquet`.
///
/// # Errors
///
/// Returns an error if the Parquet payload is invalid or required columns are
/// missing.
pub fn read_columns(bytes: &Bytes) -> Result<Vec<ColumnRecord>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let id = col_string(&batch, "id")?;
        let table_id = col_string(&batch, "table_id")?;
        let name = col_string(&batch, "name")?;
        let data_type = col_string(&batch, "data_type")?;
        let is_nullable = col_bool(&batch, "is_nullable")?;
        let ordinal = col_i32(&batch, "ordinal")?;
        let description = col_string_optional(&batch, "description")?;

        for row in 0..batch.num_rows() {
            out.push(ColumnRecord {
                id: id.value(row).to_string(),
                table_id: table_id.value(row).to_string(),
                name: name.value(row).to_string(),
                data_type: data_type.value(row).to_string(),
                is_nullable: is_nullable.value(row),
                ordinal: ordinal.value(row),
                description: description.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
            });
        }
    }
    Ok(out)
}

/// Reads `lineage_edges.parquet`.
///
/// # Errors
///
/// Returns an error if the Parquet payload is invalid or required columns are
/// missing.
pub fn read_lineage_edges(bytes: &Bytes) -> Result<Vec<LineageEdgeRecord>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let id = col_string(&batch, "id")?;
        let source_id = col_string(&batch, "source_id")?;
        let target_id = col_string(&batch, "target_id")?;
        let edge_type = col_string(&batch, "edge_type")?;
        let run_id = col_string_optional(&batch, "run_id")?;
        let created_at = col_i64(&batch, "created_at")?;

        for row in 0..batch.num_rows() {
            out.push(LineageEdgeRecord {
                id: id.value(row).to_string(),
                source_id: source_id.value(row).to_string(),
                target_id: target_id.value(row).to_string(),
                edge_type: edge_type.value(row).to_string(),
                run_id: run_id.and_then(|col| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(col.value(row).to_string())
                    }
                }),
                created_at: created_at.value(row),
            });
        }
    }
    Ok(out)
}

/// Reads `token_postings.parquet`.
///
/// # Errors
/// Returns an error if parquet deserialization or column extraction fails.
pub fn read_search_postings(bytes: &Bytes) -> Result<Vec<SearchPostingRecord>> {
    let mut out = Vec::new();
    for batch in read_batches(bytes)? {
        let token = col_string(&batch, "token")?;
        let token_norm = col_string(&batch, "token_norm")?;
        let doc_type = col_string(&batch, "doc_type")?;
        let doc_id = col_string(&batch, "doc_id")?;
        let field = col_string(&batch, "field")?;
        let score = col_f32(&batch, "score")?;

        for row in 0..batch.num_rows() {
            out.push(SearchPostingRecord {
                token: token.value(row).to_string(),
                token_norm: token_norm.value(row).to_string(),
                doc_type: doc_type.value(row).to_string(),
                doc_id: doc_id.value(row).to_string(),
                field: field.value(row).to_string(),
                score: score.value(row),
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_namespaces_tolerates_missing_optional_columns() {
        // Simulate an "old" snapshot schema that predates optional columns.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            // NOTE: "description" intentionally omitted.
            Field::new("created_at", DataType::Int64, false),
            Field::new("updated_at", DataType::Int64, false),
        ]));

        let ids = StringArray::from(vec![Some("ns_1")]);
        let names = StringArray::from(vec![Some("default")]);
        let created_at = Int64Array::from(vec![123_i64]);
        let updated_at = Int64Array::from(vec![123_i64]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ids),
                Arc::new(names),
                Arc::new(created_at),
                Arc::new(updated_at),
            ],
        )
        .expect("record batch build");

        let bytes = write_single_batch(schema, &batch).expect("write parquet");
        let rows = read_namespaces(&bytes).expect("read namespaces");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "ns_1");
        assert_eq!(rows[0].name, "default");
        assert_eq!(rows[0].description, None);
    }
}
