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
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use arco_core::storage::StorageBackend;

use crate::error::IcebergResult;
use crate::paths::resolve_metadata_path;
use crate::pointer::PointerStore;
use crate::types::{CommitKey, SnapshotRefMetadata};

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

    /// Number of metadata files that failed to read.
    #[serde(default)]
    pub metadata_read_errors: usize,

    /// Number of metadata files that failed to parse.
    #[serde(default)]
    pub metadata_parse_errors: usize,

    /// Number of tables that hit the metadata depth limit.
    #[serde(default)]
    pub tables_depth_limited: usize,

    /// Per-table results.
    pub table_results: Vec<TableReconciliationResult>,
}

/// Overall reconciliation outcome.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReconciliationResult {
    /// All tables reconciled without errors.
    Success,
    /// Some tables reconciled, but errors occurred.
    PartialSuccess,
    /// Reconciliation failed entirely.
    Failed,
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
            metadata_read_errors: 0,
            metadata_parse_errors: 0,
            tables_depth_limited: 0,
            table_results: Vec::new(),
        }
    }

    /// Returns true if any errors occurred.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.tables_with_errors > 0
            || self.receipts_failed > 0
            || self.metadata_read_errors > 0
            || self.metadata_parse_errors > 0
            || self.tables_depth_limited > 0
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
        self.metadata_read_errors += result.metadata_read_errors;
        self.metadata_parse_errors += result.metadata_parse_errors;
        if result.depth_limit_reached {
            self.tables_depth_limited += 1;
        }
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

    /// Number of metadata files that failed to read.
    #[serde(default)]
    pub metadata_read_errors: usize,

    /// Number of metadata files that failed to parse.
    #[serde(default)]
    pub metadata_parse_errors: usize,

    /// Whether the metadata walk hit the depth limit.
    #[serde(default)]
    pub depth_limit_reached: bool,

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
            metadata_read_errors: 0,
            metadata_parse_errors: 0,
            depth_limit_reached: false,
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
            metadata_read_errors: 0,
            metadata_parse_errors: 0,
            depth_limit_reached: false,
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

    /// Active snapshot refs (branches and tags).
    pub refs: HashMap<String, SnapshotRefMetadata>,
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
/// - Metadata locations are resolved relative to tenant/workspace storage scope
/// - Active refs are captured per entry for downstream reconciliation/GC
/// - Read/parse failures are skipped but recorded in walk statistics
pub struct MetadataLogWalker<S> {
    storage: Arc<S>,
    tenant: String,
    workspace: String,
    max_depth: usize,
}

/// Statistics from a metadata log walk.
#[derive(Debug, Clone, Default)]
pub struct MetadataLogWalkStats {
    /// Number of metadata reads that failed.
    pub read_errors: usize,
    /// Number of metadata files that failed to parse.
    pub parse_errors: usize,
    /// Whether the depth limit was reached.
    pub depth_limit_reached: bool,
}

/// Result of walking the metadata log chain.
#[derive(Debug, Clone, Default)]
pub struct MetadataLogWalkResult {
    /// Entries discovered during the walk.
    pub entries: Vec<MetadataLogChainEntry>,
    /// Aggregate statistics for the walk.
    pub stats: MetadataLogWalkStats,
}

impl<S: StorageBackend> MetadataLogWalker<S> {
    /// Creates a new metadata log walker.
    #[must_use]
    pub fn new(storage: Arc<S>, tenant: impl Into<String>, workspace: impl Into<String>) -> Self {
        Self {
            storage,
            tenant: tenant.into(),
            workspace: workspace.into(),
            max_depth: DEFAULT_MAX_DEPTH,
        }
    }

    /// Creates a new walker with custom depth limit.
    #[must_use]
    pub fn with_max_depth(
        storage: Arc<S>,
        tenant: impl Into<String>,
        workspace: impl Into<String>,
        max_depth: usize,
    ) -> Self {
        Self {
            storage,
            tenant: tenant.into(),
            workspace: workspace.into(),
            max_depth,
        }
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
    /// Read, parse, and resolution errors are recorded in the walk result and skipped.
    /// Use `walk_with_report` to inspect these statistics.
    pub async fn walk_from(
        &self,
        current_metadata_location: &str,
    ) -> IcebergResult<Vec<MetadataLogChainEntry>> {
        let result = self.walk_with_report(current_metadata_location).await?;
        Ok(result.entries)
    }

    /// Walks the metadata log chain and returns entries with walk statistics.
    pub async fn walk_with_report(
        &self,
        current_metadata_location: &str,
    ) -> IcebergResult<MetadataLogWalkResult> {
        use crate::types::TableMetadata;

        let mut result = MetadataLogWalkResult::default();
        let mut visited = std::collections::HashSet::new();
        let mut to_process = vec![current_metadata_location.to_string()];

        while let Some(location) = to_process.pop() {
            // Depth limit check
            if result.entries.len() >= self.max_depth {
                result.stats.depth_limit_reached = true;
                tracing::warn!(
                    max_depth = self.max_depth,
                    entries_found = result.entries.len(),
                    "Metadata log walk hit depth limit"
                );
                break;
            }

            // Cycle detection
            if !visited.insert(location.clone()) {
                continue;
            }

            let path = match resolve_metadata_path(&location, &self.tenant, &self.workspace) {
                Ok(path) => path,
                Err(e) => {
                    result.stats.read_errors += 1;
                    tracing::warn!(
                        location = %location,
                        error = %e,
                        "Failed to resolve metadata location during walk"
                    );
                    continue;
                }
            };

            // Read metadata file
            let bytes = match self.storage.get(&path).await {
                Ok(bytes) => bytes,
                Err(e) => {
                    result.stats.read_errors += 1;
                    tracing::warn!(
                        location = %location,
                        resolved_path = %path,
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
                    result.stats.parse_errors += 1;
                    tracing::warn!(
                        location = %location,
                        resolved_path = %path,
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
                    .last()
                    .map(|e| e.metadata_file.clone()),
                snapshot_id: metadata.current_snapshot_id,
                refs: metadata.refs.clone(),
            };
            result.entries.push(entry);

            // Queue previous metadata locations for processing
            for log_entry in &metadata.metadata_log {
                if !visited.contains(&log_entry.metadata_file) {
                    to_process.push(log_entry.metadata_file.clone());
                }
            }
        }

        result.entries.sort_by(|a, b| {
            b.timestamp_ms
                .cmp(&a.timestamp_ms)
                .then_with(|| a.metadata_location.cmp(&b.metadata_location))
        });

        Ok(result)
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
        assert_eq!(report.metadata_read_errors, 0);
        assert_eq!(report.metadata_parse_errors, 0);
        assert_eq!(report.tables_depth_limited, 0);
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
            metadata_read_errors: 0,
            metadata_parse_errors: 0,
            depth_limit_reached: false,
            error: None,
            duration_ms: 100,
        };
        report.add_table_result(result1);

        assert_eq!(report.tables_processed, 1);
        assert_eq!(report.metadata_entries_found, 5);
        assert_eq!(report.receipts_existing, 3);
        assert_eq!(report.receipts_created, 2);
        assert!(!report.has_errors());

        let result2 = TableReconciliationResult {
            table_uuid: Uuid::new_v4(),
            metadata_entries_found: 0,
            receipts_existing: 0,
            receipts_created: 0,
            receipts_failed: 0,
            metadata_read_errors: 1,
            metadata_parse_errors: 2,
            depth_limit_reached: true,
            error: Some("test error".to_string()),
            duration_ms: 0,
        };
        report.add_table_result(result2);

        assert_eq!(report.tables_processed, 2);
        assert_eq!(report.tables_with_errors, 1);
        assert_eq!(report.metadata_read_errors, 1);
        assert_eq!(report.metadata_parse_errors, 2);
        assert_eq!(report.tables_depth_limited, 1);
        assert!(report.has_errors());
    }

    #[test]
    fn test_table_reconciliation_result_new() {
        let table_uuid = Uuid::new_v4();
        let result = TableReconciliationResult::new(table_uuid);
        assert_eq!(result.table_uuid, table_uuid);
        assert!(result.error.is_none());
        assert_eq!(result.metadata_read_errors, 0);
        assert_eq!(result.metadata_parse_errors, 0);
        assert!(!result.depth_limit_reached);
    }

    #[test]
    fn test_table_reconciliation_result_with_error() {
        let table_uuid = Uuid::new_v4();
        let result = TableReconciliationResult::with_error(table_uuid, "something went wrong");
        assert_eq!(result.table_uuid, table_uuid);
        assert_eq!(result.error, Some("something went wrong".to_string()));
        assert_eq!(result.metadata_read_errors, 0);
        assert_eq!(result.metadata_parse_errors, 0);
        assert!(!result.depth_limit_reached);
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
            refs: HashMap::new(),
        };
        assert!(entry.metadata_location.contains("00001"));
        assert!(entry.previous_metadata_location.is_some());
    }

    #[test]
    fn test_metadata_log_walker_new() {
        use arco_core::storage::MemoryBackend;

        let storage = Arc::new(MemoryBackend::new());
        let walker = MetadataLogWalker::new(storage, "acme", "prod");
        assert_eq!(walker.max_depth, DEFAULT_MAX_DEPTH);
    }

    #[test]
    fn test_metadata_log_walker_with_custom_depth() {
        use arco_core::storage::MemoryBackend;

        let storage = Arc::new(MemoryBackend::new());
        let walker = MetadataLogWalker::with_max_depth(storage, "acme", "prod", 50);
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

        let walker = MetadataLogWalker::new(storage, "acme", "prod");
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
    async fn test_metadata_log_walker_includes_refs() {
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
            "refs": {
                "main": { "snapshot-id": 42, "type": "branch" },
                "release": { "snapshot-id": 84, "type": "tag" }
            },
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

        let walker = MetadataLogWalker::new(storage, "acme", "prod");
        let entries = walker
            .walk_from("metadata/v1.metadata.json")
            .await
            .expect("walk");

        assert_eq!(entries.len(), 1);
        let refs = &entries[0].refs;
        assert_eq!(refs.len(), 2);
        assert_eq!(refs.get("main").map(|r| r.snapshot_id), Some(42));
        assert_eq!(refs.get("main").map(|r| r.ref_type.as_str()), Some("branch"));
        assert_eq!(refs.get("release").map(|r| r.snapshot_id), Some(84));
        assert_eq!(refs.get("release").map(|r| r.ref_type.as_str()), Some("tag"));
    }

    #[tokio::test]
    async fn test_metadata_log_walker_resolves_scoped_uri() {
        use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition};
        use arco_core::ScopedStorage;
        use bytes::Bytes;

        let tenant = "acme";
        let workspace = "prod";
        let backend = Arc::new(MemoryBackend::new());
        let storage_backend: Arc<dyn StorageBackend> = backend.clone();
        let storage = ScopedStorage::new(storage_backend, tenant, workspace).expect("scoped");
        let storage = Arc::new(storage);

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
                "warehouse/table/metadata/v1.metadata.json",
                Bytes::from(metadata_json),
                WritePrecondition::None,
            )
            .await
            .expect("put");

        let location = format!(
            "gs://bucket/tenant={tenant}/workspace={workspace}/warehouse/table/metadata/v1.metadata.json"
        );
        let walker = MetadataLogWalker::new(storage, tenant, workspace);
        let entries = walker.walk_from(&location).await.expect("walk");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].metadata_location, location);
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

        // v3 - points to v1 and v2 (oldest to newest)
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
                {"metadata-file": "metadata/v1.metadata.json", "timestamp-ms": 1000},
                {"metadata-file": "metadata/v2.metadata.json", "timestamp-ms": 2000}
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

        let walker = MetadataLogWalker::new(storage, "acme", "prod");
        let entries = walker
            .walk_from("metadata/v3.metadata.json")
            .await
            .expect("walk");

        // Should find all 3 versions in newest-to-oldest order
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].metadata_location, "metadata/v3.metadata.json");
        assert_eq!(entries[1].metadata_location, "metadata/v2.metadata.json");
        assert_eq!(entries[2].metadata_location, "metadata/v1.metadata.json");
        assert_eq!(
            entries[0].previous_metadata_location.as_deref(),
            Some("metadata/v2.metadata.json")
        );
    }

    #[tokio::test]
    async fn test_metadata_log_walker_skips_corrupt_metadata() {
        use arco_core::storage::MemoryBackend;
        use bytes::Bytes;

        let storage = Arc::new(MemoryBackend::new());

        let v1_json = r#"{
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
                {"metadata-file": "metadata/v2.metadata.json", "timestamp-ms": 1000}
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
                Bytes::from("not-json"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put v2");

        let walker = MetadataLogWalker::new(storage, "acme", "prod");
        let result = walker
            .walk_with_report("metadata/v1.metadata.json")
            .await
            .expect("walk");

        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.stats.parse_errors, 1);
        assert_eq!(result.stats.read_errors, 0);
        assert!(!result.stats.depth_limit_reached);
    }

    #[tokio::test]
    async fn test_metadata_log_walker_cycle_detection() {
        use arco_core::storage::MemoryBackend;
        use bytes::Bytes;

        let storage = Arc::new(MemoryBackend::new());

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
            "metadata-log": [
                {"metadata-file": "metadata/v2.metadata.json", "timestamp-ms": 2000}
            ],
            "properties": {},
            "default-spec-id": 0,
            "partition-specs": [],
            "last-partition-id": 0,
            "default-sort-order-id": 0,
            "sort-orders": []
        }"#;

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

        let walker = MetadataLogWalker::new(storage, "acme", "prod");
        let result = walker
            .walk_with_report("metadata/v1.metadata.json")
            .await
            .expect("walk");

        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.stats.read_errors, 0);
        assert_eq!(result.stats.parse_errors, 0);
        assert!(!result.stats.depth_limit_reached);
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
        let walker = MetadataLogWalker::with_max_depth(Arc::clone(&storage), "acme", "prod", 2);
        let result = walker
            .walk_with_report("metadata/v5.metadata.json")
            .await
            .expect("walk");

        // Should only find 2 entries due to depth limit
        assert_eq!(result.entries.len(), 2);
        assert!(result.stats.depth_limit_reached);
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

        let walker = MetadataLogWalker::new(storage, "acme", "prod");
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
