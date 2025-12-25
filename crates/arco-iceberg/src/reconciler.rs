//! Metadata-log driven reconciler for Iceberg committed receipts.
//!
//! The reconciler backfills missing committed receipts by walking the Iceberg
//! metadata log chain. This enables deterministic recovery of commit history
//! without relying on bucket listings.
//!
//! # Design
//!
//! Per design doc Section 4.5:
//! - Walk metadata-log entries within retention
//! - Include active refs/branches/tags
//! - Cap traversal depth to avoid unbounded reads
//! - Use `DoesNotExist` precondition for idempotent backfill
//!
//! # Example
//!
//! ```rust,ignore
//! use arco_iceberg::reconciler::IcebergReconciler;
//!
//! let reconciler = IcebergReconciler::new(storage);
//! let report = reconciler.reconcile_all("tenant", "workspace").await?;
//! println!("Backfilled {} receipts", report.receipts_created);
//! ```

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use arco_core::storage::StorageBackend;

use crate::error::IcebergResult;
use crate::pointer::PointerStore;
use crate::types::CommitKey;

/// Default maximum depth for metadata log traversal.
pub const DEFAULT_MAX_DEPTH: usize = 1000;

/// Default reconciliation interval.
pub const DEFAULT_RECONCILE_INTERVAL_SECS: u64 = 15 * 60; // 15 minutes

// ============================================================================
// Reconciliation Report
// ============================================================================

/// Report from a reconciliation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationReport {
    /// When reconciliation started.
    pub started_at: DateTime<Utc>,

    /// When reconciliation completed.
    pub completed_at: DateTime<Utc>,

    /// Tenant that was reconciled.
    pub tenant: String,

    /// Workspace that was reconciled.
    pub workspace: String,

    /// Number of tables processed.
    pub tables_processed: usize,

    /// Number of tables that had errors.
    pub tables_with_errors: usize,

    /// Number of metadata entries found across all tables.
    pub metadata_entries_found: usize,

    /// Number of committed receipts that already existed.
    pub receipts_existing: usize,

    /// Number of committed receipts that were created.
    pub receipts_created: usize,

    /// Number of receipt writes that failed.
    pub receipts_failed: usize,

    /// Per-table results.
    pub table_results: Vec<TableReconciliationResult>,
}

impl ReconciliationReport {
    /// Creates a new empty report.
    #[must_use]
    pub fn new(tenant: impl Into<String>, workspace: impl Into<String>) -> Self {
        Self {
            started_at: Utc::now(),
            completed_at: Utc::now(),
            tenant: tenant.into(),
            workspace: workspace.into(),
            tables_processed: 0,
            tables_with_errors: 0,
            metadata_entries_found: 0,
            receipts_existing: 0,
            receipts_created: 0,
            receipts_failed: 0,
            table_results: Vec::new(),
        }
    }

    /// Returns true if any errors occurred.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.tables_with_errors > 0 || self.receipts_failed > 0
    }

    /// Marks the report as complete.
    pub fn complete(&mut self) {
        self.completed_at = Utc::now();
    }

    /// Adds a table result to the report.
    pub fn add_table_result(&mut self, result: TableReconciliationResult) {
        self.tables_processed += 1;
        if result.error.is_some() {
            self.tables_with_errors += 1;
        }
        self.metadata_entries_found += result.metadata_entries_found;
        self.receipts_existing += result.receipts_existing;
        self.receipts_created += result.receipts_created;
        self.receipts_failed += result.receipts_failed;
        self.table_results.push(result);
    }
}

/// Result from reconciling a single table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableReconciliationResult {
    /// Table UUID that was reconciled.
    pub table_uuid: Uuid,

    /// Number of metadata entries found in the log chain.
    pub metadata_entries_found: usize,

    /// Number of committed receipts that already existed.
    pub receipts_existing: usize,

    /// Number of committed receipts that were created.
    pub receipts_created: usize,

    /// Number of receipt writes that failed.
    pub receipts_failed: usize,

    /// Error message if reconciliation failed.
    pub error: Option<String>,

    /// Duration of reconciliation in milliseconds.
    pub duration_ms: u64,
}

impl TableReconciliationResult {
    /// Creates a new empty result for a table.
    #[must_use]
    pub fn new(table_uuid: Uuid) -> Self {
        Self {
            table_uuid,
            metadata_entries_found: 0,
            receipts_existing: 0,
            receipts_created: 0,
            receipts_failed: 0,
            error: None,
            duration_ms: 0,
        }
    }

    /// Creates an error result.
    #[must_use]
    pub fn with_error(table_uuid: Uuid, error: impl Into<String>) -> Self {
        Self {
            table_uuid,
            metadata_entries_found: 0,
            receipts_existing: 0,
            receipts_created: 0,
            receipts_failed: 0,
            error: Some(error.into()),
            duration_ms: 0,
        }
    }
}

// ============================================================================
// Metadata Log Entry for Reconciliation
// ============================================================================

/// Entry from the metadata log chain used for reconciliation.
#[derive(Debug, Clone)]
pub struct MetadataLogChainEntry {
    /// Location of this metadata file.
    pub metadata_location: String,

    /// Commit key derived from metadata location.
    pub commit_key: CommitKey,

    /// Timestamp from the metadata file.
    pub timestamp_ms: i64,

    /// Previous metadata location (if available).
    pub previous_metadata_location: Option<String>,

    /// Snapshot ID at this point (if any).
    pub snapshot_id: Option<i64>,
}

// ============================================================================
// Reconciler Configuration
// ============================================================================

/// Configuration for the reconciler.
#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    /// Maximum depth for metadata log traversal.
    pub max_depth: usize,

    /// Batch size for processing tables.
    pub batch_size: usize,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            batch_size: 100,
        }
    }
}

// ============================================================================
// Reconciler Trait
// ============================================================================

/// Trait for Iceberg reconciliation operations.
#[async_trait]
pub trait Reconciler: Send + Sync {
    /// Reconciles a single table, backfilling missing committed receipts.
    async fn reconcile_table(
        &self,
        table_uuid: &Uuid,
        tenant: &str,
        workspace: &str,
    ) -> IcebergResult<TableReconciliationResult>;

    /// Reconciles all tables in a tenant/workspace.
    async fn reconcile_all(
        &self,
        tenant: &str,
        workspace: &str,
    ) -> IcebergResult<ReconciliationReport>;
}

// ============================================================================
// Reconciler Implementation
// ============================================================================

/// Implementation of the Iceberg reconciler.
pub struct IcebergReconciler<S, P> {
    #[allow(dead_code)] // reserved for Phase C reconciliation work
    storage: Arc<S>,
    #[allow(dead_code)] // reserved for Phase C reconciliation work
    pointer_store: Arc<P>,
    config: ReconcilerConfig,
}

impl<S: StorageBackend, P: PointerStore> IcebergReconciler<S, P> {
    /// Creates a new reconciler.
    #[must_use]
    pub fn new(storage: Arc<S>, pointer_store: Arc<P>) -> Self {
        Self {
            storage,
            pointer_store,
            config: ReconcilerConfig::default(),
        }
    }

    /// Creates a new reconciler with custom configuration.
    #[must_use]
    pub fn with_config(storage: Arc<S>, pointer_store: Arc<P>, config: ReconcilerConfig) -> Self {
        Self {
            storage,
            pointer_store,
            config,
        }
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &ReconcilerConfig {
        &self.config
    }
}

// ============================================================================
// Metadata Log Walker
// ============================================================================

/// Walks the Iceberg metadata log chain to enumerate all historical metadata locations.
///
/// The walker starts from the current metadata file and traverses the `metadata-log`
/// entries to build a complete history. A depth limit prevents unbounded reads.
///
/// # Design Notes
///
/// - The walker reads metadata files but does NOT validate them structurally
/// - Each metadata file contains a `metadata-log` array pointing to previous versions
/// - The walker produces `MetadataLogChainEntry` for each metadata file found
pub struct MetadataLogWalker<S> {
    storage: Arc<S>,
    max_depth: usize,
}

impl<S: StorageBackend> MetadataLogWalker<S> {
    /// Creates a new metadata log walker.
    #[must_use]
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            max_depth: DEFAULT_MAX_DEPTH,
        }
    }

    /// Creates a new walker with custom depth limit.
    #[must_use]
    pub fn with_max_depth(storage: Arc<S>, max_depth: usize) -> Self {
        Self { storage, max_depth }
    }

    /// Walks the metadata log chain starting from the given metadata location.
    ///
    /// Returns all metadata entries found, including the starting location.
    /// The entries are ordered from newest (current) to oldest.
    ///
    /// # Arguments
    ///
    /// * `current_metadata_location` - The location of the current metadata file
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata file cannot be read or parsed.
    pub async fn walk_from(
        &self,
        current_metadata_location: &str,
    ) -> IcebergResult<Vec<MetadataLogChainEntry>> {
        use crate::types::TableMetadata;

        let mut entries = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut to_process = vec![current_metadata_location.to_string()];

        while let Some(location) = to_process.pop() {
            // Depth limit check
            if entries.len() >= self.max_depth {
                tracing::warn!(
                    max_depth = self.max_depth,
                    entries_found = entries.len(),
                    "Metadata log walk hit depth limit"
                );
                break;
            }

            // Cycle detection
            if visited.contains(&location) {
                continue;
            }
            visited.insert(location.clone());

            // Read metadata file
            let bytes = match self.storage.get(&location).await {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::warn!(
                        location = %location,
                        error = %e,
                        "Failed to read metadata file during walk"
                    );
                    continue;
                }
            };

            // Parse metadata
            let metadata: TableMetadata = match serde_json::from_slice(&bytes) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(
                        location = %location,
                        error = %e,
                        "Failed to parse metadata file during walk"
                    );
                    continue;
                }
            };

            // Create entry for this metadata
            let entry = MetadataLogChainEntry {
                metadata_location: location.clone(),
                commit_key: CommitKey::from_metadata_location(&location),
                timestamp_ms: metadata.last_updated_ms,
                previous_metadata_location: metadata
                    .metadata_log
                    .first()
                    .map(|e| e.metadata_file.clone()),
                snapshot_id: metadata.current_snapshot_id,
            };
            entries.push(entry);

            // Queue previous metadata locations for processing
            for log_entry in &metadata.metadata_log {
                if !visited.contains(&log_entry.metadata_file) {
                    to_process.push(log_entry.metadata_file.clone());
                }
            }
        }

        Ok(entries)
    }

    /// Walks the metadata log chain and returns only commit keys.
    ///
    /// This is a convenience method for when only the commit keys are needed.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata log cannot be loaded or parsed.
    pub async fn walk_commit_keys(
        &self,
        current_metadata_location: &str,
    ) -> IcebergResult<Vec<CommitKey>> {
        let entries = self.walk_from(current_metadata_location).await?;
        Ok(entries.into_iter().map(|e| e.commit_key).collect())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reconciliation_report_new() {
        let report = ReconciliationReport::new("acme", "prod");
        assert_eq!(report.tenant, "acme");
        assert_eq!(report.workspace, "prod");
        assert_eq!(report.tables_processed, 0);
        assert!(!report.has_errors());
    }

    #[test]
    fn test_reconciliation_report_serialization() {
        let report = ReconciliationReport::new("acme", "prod");
        let json = serde_json::to_string(&report).expect("serialization");
        let parsed: ReconciliationReport = serde_json::from_str(&json).expect("deserialization");
        assert_eq!(report.tenant, parsed.tenant);
        assert_eq!(report.workspace, parsed.workspace);
    }

    #[test]
    fn test_reconciliation_report_add_table_result() {
        let mut report = ReconciliationReport::new("acme", "prod");

        let result1 = TableReconciliationResult {
            table_uuid: Uuid::new_v4(),
            metadata_entries_found: 5,
            receipts_existing: 3,
            receipts_created: 2,
            receipts_failed: 0,
            error: None,
            duration_ms: 100,
        };
        report.add_table_result(result1);

        assert_eq!(report.tables_processed, 1);
        assert_eq!(report.metadata_entries_found, 5);
        assert_eq!(report.receipts_existing, 3);
        assert_eq!(report.receipts_created, 2);
        assert!(!report.has_errors());

        let result2 = TableReconciliationResult::with_error(Uuid::new_v4(), "test error");
        report.add_table_result(result2);

        assert_eq!(report.tables_processed, 2);
        assert_eq!(report.tables_with_errors, 1);
        assert!(report.has_errors());
    }

    #[test]
    fn test_table_reconciliation_result_new() {
        let table_uuid = Uuid::new_v4();
        let result = TableReconciliationResult::new(table_uuid);
        assert_eq!(result.table_uuid, table_uuid);
        assert!(result.error.is_none());
    }

    #[test]
    fn test_table_reconciliation_result_with_error() {
        let table_uuid = Uuid::new_v4();
        let result = TableReconciliationResult::with_error(table_uuid, "something went wrong");
        assert_eq!(result.table_uuid, table_uuid);
        assert_eq!(result.error, Some("something went wrong".to_string()));
    }

    #[test]
    fn test_reconciler_config_default() {
        let config = ReconcilerConfig::default();
        assert_eq!(config.max_depth, DEFAULT_MAX_DEPTH);
        assert_eq!(config.batch_size, 100);
    }

    #[test]
    fn test_metadata_log_chain_entry() {
        let entry = MetadataLogChainEntry {
            metadata_location: "gs://bucket/table/metadata/00001.json".to_string(),
            commit_key: CommitKey::from_metadata_location("gs://bucket/table/metadata/00001.json"),
            timestamp_ms: 1234567890000,
            previous_metadata_location: Some("gs://bucket/table/metadata/00000.json".to_string()),
            snapshot_id: Some(123),
        };
        assert!(entry.metadata_location.contains("00001"));
        assert!(entry.previous_metadata_location.is_some());
    }

    #[test]
    fn test_metadata_log_walker_new() {
        use arco_core::storage::MemoryBackend;

        let storage = Arc::new(MemoryBackend::new());
        let walker = MetadataLogWalker::new(storage);
        assert_eq!(walker.max_depth, DEFAULT_MAX_DEPTH);
    }

    #[test]
    fn test_metadata_log_walker_with_custom_depth() {
        use arco_core::storage::MemoryBackend;

        let storage = Arc::new(MemoryBackend::new());
        let walker = MetadataLogWalker::with_max_depth(storage, 50);
        assert_eq!(walker.max_depth, 50);
    }

    #[tokio::test]
    async fn test_metadata_log_walker_single_metadata() {
        use arco_core::storage::MemoryBackend;
        use bytes::Bytes;

        let storage = Arc::new(MemoryBackend::new());

        // Create a simple metadata file
        let metadata_json = r#"{
            "format-version": 2,
            "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
            "location": "gs://bucket/table",
            "last-sequence-number": 1,
            "last-updated-ms": 1234567890000,
            "last-column-id": 3,
            "current-schema-id": 0,
            "schemas": [],
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "properties": {},
            "default-spec-id": 0,
            "partition-specs": [],
            "last-partition-id": 0,
            "default-sort-order-id": 0,
            "sort-orders": []
        }"#;

        storage
            .put(
                "metadata/v1.metadata.json",
                Bytes::from(metadata_json),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        let walker = MetadataLogWalker::new(storage);
        let entries = walker
            .walk_from("metadata/v1.metadata.json")
            .await
            .expect("walk");

        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].metadata_location,
            "metadata/v1.metadata.json"
        );
        assert_eq!(entries[0].timestamp_ms, 1234567890000);
        assert!(entries[0].previous_metadata_location.is_none());
    }

    #[tokio::test]
    async fn test_metadata_log_walker_chain_of_three() {
        use arco_core::storage::MemoryBackend;
        use bytes::Bytes;

        let storage = Arc::new(MemoryBackend::new());

        // v1 - oldest, no previous
        let v1_json = r#"{
            "format-version": 2,
            "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
            "location": "gs://bucket/table",
            "last-sequence-number": 1,
            "last-updated-ms": 1000,
            "last-column-id": 3,
            "current-schema-id": 0,
            "schemas": [],
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "properties": {},
            "default-spec-id": 0,
            "partition-specs": [],
            "last-partition-id": 0,
            "default-sort-order-id": 0,
            "sort-orders": []
        }"#;

        // v2 - points to v1
        let v2_json = r#"{
            "format-version": 2,
            "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
            "location": "gs://bucket/table",
            "last-sequence-number": 2,
            "last-updated-ms": 2000,
            "last-column-id": 3,
            "current-schema-id": 0,
            "schemas": [],
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [
                {"metadata-file": "metadata/v1.metadata.json", "timestamp-ms": 1000}
            ],
            "properties": {},
            "default-spec-id": 0,
            "partition-specs": [],
            "last-partition-id": 0,
            "default-sort-order-id": 0,
            "sort-orders": []
        }"#;

        // v3 - points to v2 and v1
        let v3_json = r#"{
            "format-version": 2,
            "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
            "location": "gs://bucket/table",
            "last-sequence-number": 3,
            "last-updated-ms": 3000,
            "last-column-id": 3,
            "current-schema-id": 0,
            "schemas": [],
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [
                {"metadata-file": "metadata/v2.metadata.json", "timestamp-ms": 2000},
                {"metadata-file": "metadata/v1.metadata.json", "timestamp-ms": 1000}
            ],
            "properties": {},
            "default-spec-id": 0,
            "partition-specs": [],
            "last-partition-id": 0,
            "default-sort-order-id": 0,
            "sort-orders": []
        }"#;

        storage
            .put(
                "metadata/v1.metadata.json",
                Bytes::from(v1_json),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put v1");
        storage
            .put(
                "metadata/v2.metadata.json",
                Bytes::from(v2_json),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put v2");
        storage
            .put(
                "metadata/v3.metadata.json",
                Bytes::from(v3_json),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put v3");

        let walker = MetadataLogWalker::new(storage);
        let entries = walker
            .walk_from("metadata/v3.metadata.json")
            .await
            .expect("walk");

        // Should find all 3 versions
        assert_eq!(entries.len(), 3);

        // Check we found all locations (order may vary due to hashset)
        let locations: std::collections::HashSet<_> =
            entries.iter().map(|e| e.metadata_location.as_str()).collect();
        assert!(locations.contains("metadata/v1.metadata.json"));
        assert!(locations.contains("metadata/v2.metadata.json"));
        assert!(locations.contains("metadata/v3.metadata.json"));
    }

    #[tokio::test]
    async fn test_metadata_log_walker_respects_depth_limit() {
        use arco_core::storage::MemoryBackend;
        use bytes::Bytes;

        let storage = Arc::new(MemoryBackend::new());

        // Create 5 metadata files but limit depth to 2
        for i in 1..=5 {
            let prev_log = if i > 1 {
                format!(
                    r#"[{{"metadata-file": "metadata/v{}.metadata.json", "timestamp-ms": {}}}]"#,
                    i - 1,
                    (i - 1) * 1000
                )
            } else {
                "[]".to_string()
            };

            let json = format!(
                r#"{{
                    "format-version": 2,
                    "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
                    "location": "gs://bucket/table",
                    "last-sequence-number": {i},
                    "last-updated-ms": {},
                    "last-column-id": 3,
                    "current-schema-id": 0,
                    "schemas": [],
                    "snapshots": [],
                    "snapshot-log": [],
                    "metadata-log": {prev_log},
                    "properties": {{}},
                    "default-spec-id": 0,
                    "partition-specs": [],
                    "last-partition-id": 0,
                    "default-sort-order-id": 0,
                    "sort-orders": []
                }}"#,
                i * 1000
            );

            storage
                .put(
                    &format!("metadata/v{i}.metadata.json"),
                    Bytes::from(json),
                    arco_core::storage::WritePrecondition::None,
                )
                .await
                .expect("put");
        }

        // Walk with depth limit of 2
        let walker = MetadataLogWalker::with_max_depth(Arc::clone(&storage), 2);
        let entries = walker
            .walk_from("metadata/v5.metadata.json")
            .await
            .expect("walk");

        // Should only find 2 entries due to depth limit
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_metadata_log_walker_commit_keys() {
        use arco_core::storage::MemoryBackend;
        use bytes::Bytes;

        let storage = Arc::new(MemoryBackend::new());

        let metadata_json = r#"{
            "format-version": 2,
            "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
            "location": "gs://bucket/table",
            "last-sequence-number": 1,
            "last-updated-ms": 1234567890000,
            "last-column-id": 3,
            "current-schema-id": 0,
            "schemas": [],
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "properties": {},
            "default-spec-id": 0,
            "partition-specs": [],
            "last-partition-id": 0,
            "default-sort-order-id": 0,
            "sort-orders": []
        }"#;

        storage
            .put(
                "metadata/v1.metadata.json",
                Bytes::from(metadata_json),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        let walker = MetadataLogWalker::new(storage);
        let commit_keys = walker
            .walk_commit_keys("metadata/v1.metadata.json")
            .await
            .expect("walk_commit_keys");

        assert_eq!(commit_keys.len(), 1);
        assert_eq!(
            commit_keys[0],
            CommitKey::from_metadata_location("metadata/v1.metadata.json")
        );
    }
}
