//! Compactor for Tier 2 event consolidation.
//!
//! INVARIANT 2: Compactor is the SOLE writer of Parquet state files.
//! INVARIANT 3: Compaction is idempotent (dedupe by `idempotency_key`, primary key upsert).
//! INVARIANT 4: Publish is atomic (manifest CAS).
//!
//! It reads from `ledger/`, writes to `state/`, and updates the watermark atomically via CAS.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};

use arco_core::scoped_storage::ScopedStorage;
use arco_core::storage::WritePrecondition;

use crate::error::{CatalogError, Result};
use crate::manifest::ExecutionManifest;

/// Result of a compaction operation.
#[derive(Debug, Clone)]
pub struct CompactionResult {
    /// Number of events processed from ledger.
    pub events_processed: usize,
    /// Number of Parquet files written.
    pub parquet_files_written: usize,
    /// New watermark position (event ID).
    pub new_watermark: String,
}

/// Compacts Tier 2 ledger events into Parquet snapshots.
///
/// INVARIANT: Compactor is the SOLE writer of `state/` Parquet files.
pub struct Compactor {
    storage: ScopedStorage,
}

impl Compactor {
    /// Creates a new compactor.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self { storage }
    }

    /// Compacts a domain, reading events since watermark and writing Parquet.
    ///
    /// ARCHITECTURE: Uses CAS for atomic manifest publish. The sequence is:
    /// 1. Read manifest + version (head)
    /// 2. Write Parquet snapshot
    /// 3. CAS-update manifest with `MatchesVersion`
    /// 4. Only if CAS succeeds: compaction is visible
    ///
    /// # Errors
    ///
    /// Returns an error if reading ledger or writing state fails.
    /// Returns `CatalogError::CasFailed` if another compactor won the race.
    #[allow(clippy::too_many_lines)]
    pub async fn compact_domain(&self, domain: &str) -> Result<CompactionResult> {
        // 1. Read current manifest + its version for CAS
        let (manifest, current_version) = self.read_manifest_with_version().await?;

        let watermark_event_id = manifest.watermark_event_id.clone();

        // 2. List ledger events
        let prefix = format!("ledger/{domain}/");
        let scoped_paths = self
            .storage
            .list(&prefix)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to list events: {e}"),
            })?;

        // Convert ScopedPath to path strings for processing
        let mut files: Vec<String> = scoped_paths.into_iter().map(|p| p.to_string()).collect();

        // CRITICAL: Sort explicitly - object store list() order is NOT guaranteed
        files.sort();

        // 3. Filter to events after watermark
        // Watermark is stored as exact filename (including .json) for correct comparison
        let events_to_process: Vec<_> = files
            .into_iter()
            .filter(|path| {
                // Compare exact filenames (both include .json)
                let filename = path.rsplit('/').next().unwrap_or("");
                // If no watermark, process all events; otherwise, only events after watermark
                #[allow(clippy::option_if_let_else)]
                match &watermark_event_id {
                    Some(watermark) => filename > watermark.as_str(),
                    None => true,
                }
            })
            .collect();

        if events_to_process.is_empty() {
            return Ok(CompactionResult {
                events_processed: 0,
                parquet_files_written: 0,
                new_watermark: watermark_event_id.unwrap_or_default(),
            });
        }

        // 4. Read, parse, and DEDUPE events by idempotency_key
        // Architecture: "Compactor dedupes by idempotency_key during fold"
        // Use HashMap keyed by primary key (materialization_id) for upsert semantics
        let mut records_by_key: HashMap<String, MaterializationRecord> = HashMap::new();
        let mut last_event_file = String::new();

        for file_path in &events_to_process {
            let filename = file_path.rsplit('/').next().unwrap_or("").to_string();

            let data =
                self.storage
                    .get_raw(file_path)
                    .await
                    .map_err(|e| CatalogError::Storage {
                        message: format!("failed to read event: {e}"),
                    })?;

            let event: MaterializationRecord =
                serde_json::from_slice(&data).map_err(|e| CatalogError::Serialization {
                    message: format!("failed to parse event '{file_path}': {e}"),
                })?;
            // DEDUPE: Use materialization_id as primary key (upsert semantics)
            // Later events overwrite earlier ones for the same key
            records_by_key.insert(event.materialization_id.clone(), event);

            // Track watermark as the last processed file name
            if filename > last_event_file {
                last_event_file = filename;
            }
        }

        // Convert deduped map to vec for Parquet write (sort for determinism)
        let mut records: Vec<_> = records_by_key.into_values().collect();
        records.sort_by(|a, b| a.materialization_id.cmp(&b.materialization_id));
        // CRITICAL: Store exact filename (including .json) as watermark
        let last_event_id = last_event_file;

        // 5. Write Parquet file
        // INVARIANT 4: Parquet is written BEFORE manifest update (atomic publish)
        let (parquet_files_written, new_snapshot_path, new_snapshot_version) = if records.is_empty()
        {
            (0, manifest.snapshot_path.clone(), manifest.snapshot_version)
        } else {
            let snapshot_version = manifest.snapshot_version + 1;
            let snapshot_id = last_event_id
                .strip_suffix(".json")
                .unwrap_or(&last_event_id);
            let path = format!("state/{domain}/snapshot_v{snapshot_version}_{snapshot_id}.parquet");

            let wrote_new = self.write_parquet(&path, &records).await?;
            let files_written = usize::from(wrote_new);

            (files_written, Some(path), snapshot_version)
        };

        // 6. Update watermark in manifest with CAS (ARCHITECTURE-CRITICAL)
        // INVARIANT 4: snapshot_path makes the new Parquet visible atomically
        let new_watermark_version = manifest.watermark_version + 1;

        let mut new_manifest = manifest.clone();
        new_manifest.watermark_version = new_watermark_version;
        new_manifest.watermark_event_id = Some(last_event_id.clone());
        new_manifest.snapshot_path = new_snapshot_path; // Atomic visibility gate
        new_manifest.snapshot_version = new_snapshot_version;
        new_manifest.last_compaction_at = Some(Utc::now());
        new_manifest.compaction.total_events_compacted += events_to_process.len() as u64;
        new_manifest.compaction.total_files_written += parquet_files_written as u64;
        new_manifest.updated_at = Utc::now();

        let execution_json =
            serde_json::to_vec_pretty(&new_manifest).map_err(|e| CatalogError::Serialization {
                message: format!("failed to serialize manifest: {e}"),
            })?;

        // CRITICAL: Use CAS to ensure atomic publish semantics
        // If another compactor updated the manifest, this will fail and we retry
        let result = self
            .storage
            .put_raw(
                "manifests/execution.manifest.json",
                Bytes::from(execution_json),
                WritePrecondition::MatchesVersion(current_version),
            )
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to write manifest: {e}"),
            })?;

        // Check if CAS succeeded
        match result {
            arco_core::storage::WriteResult::Success { .. } => {
                // Success - compaction is now visible
            }
            arco_core::storage::WriteResult::PreconditionFailed { .. } => {
                // Another compactor won the race
                return Err(CatalogError::CasFailed {
                    message: "manifest updated by another compactor".to_string(),
                });
            }
        }

        Ok(CompactionResult {
            events_processed: events_to_process.len(),
            parquet_files_written,
            new_watermark: last_event_id,
        })
    }

    /// Writes records to a Parquet snapshot file.
    ///
    /// Returns `true` if a new file was written, or `false` if the file already existed.
    async fn write_parquet(&self, path: &str, records: &[MaterializationRecord]) -> Result<bool> {
        let materialization_ids: Vec<_> = records
            .iter()
            .map(|r| r.materialization_id.as_str())
            .collect();
        let asset_ids: Vec<_> = records.iter().map(|r| r.asset_id.as_str()).collect();
        let row_counts: Vec<_> = records.iter().map(|r| r.row_count).collect();
        let byte_sizes: Vec<_> = records.iter().map(|r| r.byte_size).collect();

        let schema = Schema::new(vec![
            Field::new("materialization_id", DataType::Utf8, false),
            Field::new("asset_id", DataType::Utf8, false),
            Field::new("row_count", DataType::Int64, false),
            Field::new("byte_size", DataType::Int64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(StringArray::from(materialization_ids)),
                Arc::new(StringArray::from(asset_ids)),
                Arc::new(Int64Array::from(row_counts)),
                Arc::new(Int64Array::from(byte_sizes)),
            ],
        )
        .map_err(|e| CatalogError::Serialization {
            message: format!("failed to create record batch: {e}"),
        })?;

        let mut buffer = Cursor::new(Vec::new());
        let props = WriterProperties::builder().build();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, Arc::new(schema), Some(props)).map_err(|e| {
                CatalogError::Serialization {
                    message: format!("failed to create Parquet writer: {e}"),
                }
            })?;

        writer
            .write(&batch)
            .map_err(|e| CatalogError::Serialization {
                message: format!("failed to write batch: {e}"),
            })?;
        writer.close().map_err(|e| CatalogError::Serialization {
            message: format!("failed to close writer: {e}"),
        })?;

        let result = self
            .storage
            .put_raw(
                path,
                Bytes::from(buffer.into_inner()),
                WritePrecondition::DoesNotExist,
            )
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to write Parquet: {e}"),
            })?;

        match result {
            arco_core::storage::WriteResult::Success { .. } => Ok(true),
            arco_core::storage::WriteResult::PreconditionFailed { .. } => Ok(false),
        }
    }

    /// Reads the execution manifest with its current version for CAS.
    async fn read_manifest_with_version(&self) -> Result<(ExecutionManifest, String)> {
        let manifest_path = "manifests/execution.manifest.json";

        // First, get metadata to retrieve version
        let meta =
            self.storage
                .head_raw(manifest_path)
                .await
                .map_err(|e| CatalogError::Storage {
                    message: format!("failed to read manifest metadata: {e}"),
                })?;

        // If manifest doesn't exist, return default with version "0"
        let version = meta
            .as_ref()
            .map_or_else(|| "0".to_string(), |m| m.version.clone());

        // Read the content
        let data =
            self.storage
                .get_raw(manifest_path)
                .await
                .map_err(|e| CatalogError::Storage {
                    message: format!("failed to read manifest: {e}"),
                })?;

        let manifest: ExecutionManifest =
            serde_json::from_slice(&data).map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse manifest: {e}"),
            })?;

        Ok((manifest, version))
    }
}

/// Record structure for materialization events (matches `EventWriter` format).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationRecord {
    /// Unique materialization ID.
    pub materialization_id: String,
    /// Asset ID this materialization belongs to.
    pub asset_id: String,
    /// Number of rows produced.
    #[serde(default)]
    pub row_count: i64,
    /// Size in bytes.
    #[serde(default)]
    pub byte_size: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventWriter;
    use crate::Tier1Writer;
    use arco_core::storage::MemoryBackend;

    #[tokio::test]
    async fn compact_empty_ledger_is_noop() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await.expect("init");

        let compactor = Compactor::new(storage);
        let result = compactor
            .compact_domain("execution")
            .await
            .expect("compact");

        assert_eq!(result.events_processed, 0);
        assert_eq!(result.parquet_files_written, 0);
    }

    #[tokio::test]
    async fn compact_processes_ledger_events() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let tier1_writer = Tier1Writer::new(storage.clone());
        tier1_writer.initialize().await.expect("init");

        let event_writer = EventWriter::new(storage.clone());
        for i in 0..5 {
            let event = MaterializationRecord {
                materialization_id: format!("mat_{i:03}"),
                asset_id: format!("asset_{i:03}"),
                row_count: 1000,
                byte_size: 50000,
            };
            event_writer
                .append("execution", &event)
                .await
                .expect("append");
        }

        let compactor = Compactor::new(storage);
        let result = compactor
            .compact_domain("execution")
            .await
            .expect("compact");

        assert_eq!(result.events_processed, 5);
        assert!(result.parquet_files_written > 0);
    }

    #[tokio::test]
    async fn second_compaction_only_processes_new_events() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let tier1_writer = Tier1Writer::new(storage.clone());
        tier1_writer.initialize().await.expect("init");

        let event_writer = EventWriter::new(storage.clone());
        let compactor = Compactor::new(storage.clone());

        // Write 3 events and compact
        for i in 0..3 {
            let event = MaterializationRecord {
                materialization_id: format!("mat_{i:03}"),
                asset_id: format!("asset_{i:03}"),
                row_count: 1000,
                byte_size: 50000,
            };
            event_writer
                .append("execution", &event)
                .await
                .expect("append");
        }
        let result1 = compactor
            .compact_domain("execution")
            .await
            .expect("compact1");
        assert_eq!(result1.events_processed, 3);

        // Write 2 more events and compact again
        for i in 3..5 {
            let event = MaterializationRecord {
                materialization_id: format!("mat_{i:03}"),
                asset_id: format!("asset_{i:03}"),
                row_count: 1000,
                byte_size: 50000,
            };
            event_writer
                .append("execution", &event)
                .await
                .expect("append");
        }
        let result2 = compactor
            .compact_domain("execution")
            .await
            .expect("compact2");

        assert_eq!(
            result2.events_processed, 2,
            "only new events since watermark"
        );
    }
}
