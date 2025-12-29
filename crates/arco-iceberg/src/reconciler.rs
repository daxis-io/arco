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
use std::time::Instant;
use uuid::Uuid;

use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};
use bytes::Bytes;

use crate::error::IcebergResult;
use crate::events::CommittedReceipt;
use crate::metrics;
use crate::paths::resolve_metadata_path;
use crate::pointer::{PointerStore, UpdateSource};
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
    storage: Arc<S>,
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

#[async_trait]
impl<S: StorageBackend, P: PointerStore> Reconciler for IcebergReconciler<S, P> {
    #[allow(clippy::cast_possible_truncation)]
    #[tracing::instrument(skip(self), fields(table_uuid = %table_uuid, tenant = %tenant, workspace = %workspace))]
    async fn reconcile_table(
        &self,
        table_uuid: &Uuid,
        tenant: &str,
        workspace: &str,
    ) -> IcebergResult<TableReconciliationResult> {
        let start = Instant::now();
        let mut result = TableReconciliationResult::new(*table_uuid);

        // Load the table pointer
        let Some((pointer, _version)) = self.pointer_store.load(table_uuid).await? else {
            result.error = Some(format!("Table pointer not found: {table_uuid}"));
            result.duration_ms = start.elapsed().as_millis() as u64;
            return Ok(result);
        };

        // Walk the metadata log
        let walker = MetadataLogWalker::with_max_depth(
            Arc::clone(&self.storage),
            tenant,
            workspace,
            self.config.max_depth,
        );

        let walk_result = walker
            .walk_with_report(&pointer.current_metadata_location)
            .await?;

        result.metadata_entries_found = walk_result.entries.len();
        result.metadata_read_errors = walk_result.stats.read_errors;
        result.metadata_parse_errors = walk_result.stats.parse_errors;
        result.depth_limit_reached = walk_result.stats.depth_limit_reached;

        // Backfill missing receipts
        let backfiller = ReceiptBackfiller::new(Arc::clone(&self.storage));
        let backfill_result = backfiller.backfill(table_uuid, &walk_result.entries).await;

        result.receipts_existing = backfill_result.existing;
        result.receipts_created = backfill_result.created;
        result.receipts_failed = backfill_result.failed;
        let duration = start.elapsed();
        result.duration_ms = duration.as_millis() as u64;

        tracing::info!(
            table_uuid = %table_uuid,
            metadata_entries = result.metadata_entries_found,
            receipts_created = result.receipts_created,
            receipts_existing = result.receipts_existing,
            receipts_failed = result.receipts_failed,
            duration_ms = result.duration_ms,
            "Reconciled table"
        );

        metrics::record_reconciler_table(tenant, workspace);
        metrics::record_reconciler_receipts(tenant, workspace, result.receipts_created as u64);

        Ok(result)
    }

    async fn reconcile_all(
        &self,
        tenant: &str,
        workspace: &str,
    ) -> IcebergResult<ReconciliationReport> {
        let run_start = Instant::now();
        let mut report = ReconciliationReport::new(tenant, workspace);

        // List all table pointers
        let table_uuids = self.pointer_store.list_all().await?;

        tracing::info!(
            tenant = %tenant,
            workspace = %workspace,
            table_count = table_uuids.len(),
            "Starting reconciliation"
        );

        // Process tables in batches
        for chunk in table_uuids.chunks(self.config.batch_size) {
            for table_uuid in chunk {
                let result = self.reconcile_table(table_uuid, tenant, workspace).await?;
                report.add_table_result(result);
            }
        }

        report.complete();

        tracing::info!(
            tenant = %tenant,
            workspace = %workspace,
            tables_processed = report.tables_processed,
            receipts_created = report.receipts_created,
            receipts_existing = report.receipts_existing,
            receipts_failed = report.receipts_failed,
            duration_ms = (report.completed_at - report.started_at).num_milliseconds(),
            "Completed reconciliation"
        );

        metrics::record_reconciler_duration(tenant, workspace, run_start.elapsed().as_secs_f64());

        Ok(report)
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
    ///
    /// # Errors
    ///
    /// Returns errors for unrecoverable issues; read/parse errors are recorded
    /// in the walk result statistics and skipped.
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
// Receipt Backfiller
// ============================================================================

/// Result of backfilling committed receipts for a table.
#[derive(Debug, Clone, Default)]
pub struct BackfillResult {
    /// Number of receipts that already existed.
    pub existing: usize,
    /// Number of receipts that were created.
    pub created: usize,
    /// Number of receipt writes that failed.
    pub failed: usize,
}

/// Backfills missing committed receipts from metadata log entries.
///
/// For each metadata file in the log chain, this creates a committed receipt
/// using `DoesNotExist` precondition for idempotent writes.
///
/// # Design Notes
///
/// - Uses `DoesNotExist` precondition so concurrent backfills are safe
/// - Creates synthetic receipts with `source=Reconciliation`
/// - Derives date from metadata timestamp for path organization
pub struct ReceiptBackfiller<S> {
    storage: Arc<S>,
}

impl<S: StorageBackend> ReceiptBackfiller<S> {
    /// Creates a new receipt backfiller.
    #[must_use]
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// Backfills missing committed receipts from metadata log entries.
    ///
    /// # Arguments
    ///
    /// * `table_uuid` - The table these metadata entries belong to
    /// * `entries` - Metadata log entries to process
    ///
    /// # Returns
    ///
    /// Statistics about the backfill operation.
    pub async fn backfill(
        &self,
        table_uuid: &Uuid,
        entries: &[MetadataLogChainEntry],
    ) -> BackfillResult {
        let mut result = BackfillResult::default();

        for entry in entries {
            match self.backfill_entry(table_uuid, entry).await {
                BackfillEntryResult::Existing => result.existing += 1,
                BackfillEntryResult::Created => result.created += 1,
                BackfillEntryResult::Failed => result.failed += 1,
            }
        }

        result
    }

    /// Backfills a single metadata entry.
    async fn backfill_entry(
        &self,
        table_uuid: &Uuid,
        entry: &MetadataLogChainEntry,
    ) -> BackfillEntryResult {
        // Derive date from metadata timestamp
        let date = DateTime::from_timestamp_millis(entry.timestamp_ms)
            .map_or_else(|| Utc::now().date_naive(), |dt| dt.date_naive());

        // Build the receipt path
        let path = CommittedReceipt::storage_path(date, &entry.commit_key);

        // Create the backfill receipt
        let receipt = CommittedReceipt {
            table_uuid: *table_uuid,
            commit_key: entry.commit_key.clone(),
            event_id: ulid::Ulid::new(),
            metadata_location: entry.metadata_location.clone(),
            snapshot_id: entry.snapshot_id,
            previous_metadata_location: entry.previous_metadata_location.clone(),
            source: UpdateSource::AdminApi {
                user_id: "reconciler".to_string(),
            },
            committed_at: DateTime::from_timestamp_millis(entry.timestamp_ms)
                .unwrap_or_else(Utc::now),
        };

        // Serialize the receipt
        let bytes = match serde_json::to_vec(&receipt) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    table_uuid = %table_uuid,
                    commit_key = %entry.commit_key,
                    error = %e,
                    "Failed to serialize receipt during backfill"
                );
                return BackfillEntryResult::Failed;
            }
        };

        // Write with DoesNotExist precondition for idempotency
        match self
            .storage
            .put(&path, Bytes::from(bytes), WritePrecondition::DoesNotExist)
            .await
        {
            Ok(WriteResult::Success { .. }) => {
                tracing::debug!(
                    table_uuid = %table_uuid,
                    commit_key = %entry.commit_key,
                    path = %path,
                    "Backfilled committed receipt"
                );
                BackfillEntryResult::Created
            }
            Ok(WriteResult::PreconditionFailed { .. }) => {
                // Receipt already exists - this is expected and fine
                BackfillEntryResult::Existing
            }
            Err(e) => {
                tracing::warn!(
                    table_uuid = %table_uuid,
                    commit_key = %entry.commit_key,
                    path = %path,
                    error = %e,
                    "Failed to write receipt during backfill"
                );
                BackfillEntryResult::Failed
            }
        }
    }
}

/// Result of backfilling a single entry.
enum BackfillEntryResult {
    /// Receipt already existed.
    Existing,
    /// Receipt was created.
    Created,
    /// Failed to create receipt.
    Failed,
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
                WritePrecondition::None,
            )
            .await
            .expect("put");

        let walker = MetadataLogWalker::new(storage, "acme", "prod");
        let entries = walker
            .walk_from("metadata/v1.metadata.json")
            .await
            .expect("walk");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].metadata_location, "metadata/v1.metadata.json");
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
                WritePrecondition::None,
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
        assert_eq!(
            refs.get("main").map(|r| r.ref_type.as_str()),
            Some("branch")
        );
        assert_eq!(refs.get("release").map(|r| r.snapshot_id), Some(84));
        assert_eq!(
            refs.get("release").map(|r| r.ref_type.as_str()),
            Some("tag")
        );
    }

    #[tokio::test]
    async fn test_metadata_log_walker_resolves_scoped_uri() {
        use arco_core::ScopedStorage;
        use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition};
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
                WritePrecondition::None,
            )
            .await
            .expect("put v1");
        storage
            .put(
                "metadata/v2.metadata.json",
                Bytes::from(v2_json),
                WritePrecondition::None,
            )
            .await
            .expect("put v2");
        storage
            .put(
                "metadata/v3.metadata.json",
                Bytes::from(v3_json),
                WritePrecondition::None,
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
                WritePrecondition::None,
            )
            .await
            .expect("put v1");
        storage
            .put(
                "metadata/v2.metadata.json",
                Bytes::from("not-json"),
                WritePrecondition::None,
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
                WritePrecondition::None,
            )
            .await
            .expect("put v1");
        storage
            .put(
                "metadata/v2.metadata.json",
                Bytes::from(v2_json),
                WritePrecondition::None,
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
                    WritePrecondition::None,
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
                WritePrecondition::None,
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

    // ========================================================================
    // Receipt Backfiller Tests
    // ========================================================================

    #[tokio::test]
    async fn test_receipt_backfiller_creates_missing_receipts() {
        use arco_core::storage::MemoryBackend;

        let storage = Arc::new(MemoryBackend::new());
        let backfiller = ReceiptBackfiller::new(Arc::clone(&storage));

        let table_uuid = Uuid::new_v4();
        let entries = vec![
            MetadataLogChainEntry {
                metadata_location: "metadata/v1.json".to_string(),
                commit_key: CommitKey::from_metadata_location("metadata/v1.json"),
                timestamp_ms: 1704067200000, // 2024-01-01 00:00:00 UTC
                previous_metadata_location: None,
                snapshot_id: Some(1),
                refs: HashMap::new(),
            },
            MetadataLogChainEntry {
                metadata_location: "metadata/v2.json".to_string(),
                commit_key: CommitKey::from_metadata_location("metadata/v2.json"),
                timestamp_ms: 1704153600000, // 2024-01-02 00:00:00 UTC
                previous_metadata_location: Some("metadata/v1.json".to_string()),
                snapshot_id: Some(2),
                refs: HashMap::new(),
            },
        ];

        let result = backfiller.backfill(&table_uuid, &entries).await;

        assert_eq!(result.created, 2);
        assert_eq!(result.existing, 0);
        assert_eq!(result.failed, 0);
    }

    #[tokio::test]
    async fn test_receipt_backfiller_skips_existing_receipts() {
        use arco_core::storage::MemoryBackend;

        let storage = Arc::new(MemoryBackend::new());
        let backfiller = ReceiptBackfiller::new(Arc::clone(&storage));

        let table_uuid = Uuid::new_v4();
        let entries = vec![MetadataLogChainEntry {
            metadata_location: "metadata/v1.json".to_string(),
            commit_key: CommitKey::from_metadata_location("metadata/v1.json"),
            timestamp_ms: 1704067200000,
            previous_metadata_location: None,
            snapshot_id: Some(1),
            refs: HashMap::new(),
        }];

        // First backfill creates the receipt
        let result1 = backfiller.backfill(&table_uuid, &entries).await;
        assert_eq!(result1.created, 1);
        assert_eq!(result1.existing, 0);

        // Second backfill finds it existing
        let result2 = backfiller.backfill(&table_uuid, &entries).await;
        assert_eq!(result2.created, 0);
        assert_eq!(result2.existing, 1);
    }

    #[tokio::test]
    async fn test_receipt_backfiller_uses_correct_date_path() {
        use arco_core::storage::MemoryBackend;

        let storage = Arc::new(MemoryBackend::new());
        let backfiller = ReceiptBackfiller::new(Arc::clone(&storage));

        let table_uuid = Uuid::new_v4();
        let commit_key = CommitKey::from_metadata_location("metadata/v1.json");
        let entries = vec![MetadataLogChainEntry {
            metadata_location: "metadata/v1.json".to_string(),
            commit_key: commit_key.clone(),
            timestamp_ms: 1704067200000, // 2024-01-01 00:00:00 UTC
            previous_metadata_location: None,
            snapshot_id: Some(1),
            refs: HashMap::new(),
        }];

        backfiller.backfill(&table_uuid, &entries).await;

        // Verify the receipt was written to the correct path
        let expected_path = format!("events/2024-01-01/iceberg/committed/{}.json", commit_key);
        let result = storage.head(&expected_path).await.expect("head");
        assert!(
            result.is_some(),
            "Receipt should exist at {}",
            expected_path
        );
    }

    #[tokio::test]
    async fn test_receipt_backfiller_content_is_valid() {
        use arco_core::storage::MemoryBackend;

        let storage = Arc::new(MemoryBackend::new());
        let backfiller = ReceiptBackfiller::new(Arc::clone(&storage));

        let table_uuid = Uuid::new_v4();
        let commit_key = CommitKey::from_metadata_location("metadata/v1.json");
        let entries = vec![MetadataLogChainEntry {
            metadata_location: "metadata/v1.json".to_string(),
            commit_key: commit_key.clone(),
            timestamp_ms: 1704067200000,
            previous_metadata_location: Some("metadata/v0.json".to_string()),
            snapshot_id: Some(42),
            refs: HashMap::new(),
        }];

        backfiller.backfill(&table_uuid, &entries).await;

        // Read and verify the receipt content
        let path = format!("events/2024-01-01/iceberg/committed/{}.json", commit_key);
        let bytes = storage.get(&path).await.expect("get");
        let receipt: CommittedReceipt =
            serde_json::from_slice(&bytes).expect("deserialize receipt");

        assert_eq!(receipt.table_uuid, table_uuid);
        assert_eq!(receipt.commit_key, commit_key);
        assert_eq!(receipt.metadata_location, "metadata/v1.json");
        assert_eq!(receipt.snapshot_id, Some(42));
        assert_eq!(
            receipt.previous_metadata_location,
            Some("metadata/v0.json".to_string())
        );
    }

    // ========================================================================
    // Full Reconciler Tests
    // ========================================================================

    #[tokio::test]
    async fn test_reconciler_reconcile_table_with_missing_pointer() {
        use crate::pointer::PointerStoreImpl;
        use arco_core::storage::MemoryBackend;

        let storage = Arc::new(MemoryBackend::new());
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));
        let reconciler = IcebergReconciler::new(storage, pointer_store);

        let table_uuid = Uuid::new_v4();
        let result = reconciler
            .reconcile_table(&table_uuid, "acme", "prod")
            .await
            .expect("reconcile");

        assert!(result.error.is_some());
        assert!(result.error.as_ref().unwrap().contains("not found"));
    }

    #[tokio::test]
    async fn test_reconciler_reconcile_table_success() {
        use crate::pointer::{CasResult, IcebergTablePointer, PointerStoreImpl};
        use arco_core::storage::MemoryBackend;
        use bytes::Bytes;

        let storage = Arc::new(MemoryBackend::new());
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));

        // Create metadata file
        let table_uuid = Uuid::new_v4();
        let metadata_json = format!(
            r#"{{
            "format-version": 2,
            "table-uuid": "{}",
            "location": "gs://bucket/table",
            "last-sequence-number": 1,
            "last-updated-ms": 1704067200000,
            "last-column-id": 3,
            "current-schema-id": 0,
            "schemas": [],
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "properties": {{}},
            "default-spec-id": 0,
            "partition-specs": [],
            "last-partition-id": 0,
            "default-sort-order-id": 0,
            "sort-orders": []
        }}"#,
            table_uuid
        );

        storage
            .put(
                "metadata/v1.metadata.json",
                Bytes::from(metadata_json),
                WritePrecondition::None,
            )
            .await
            .expect("put metadata");

        // Create pointer
        let pointer = IcebergTablePointer::new(table_uuid, "metadata/v1.metadata.json".to_string());
        let result = pointer_store
            .create(&table_uuid, &pointer)
            .await
            .expect("create pointer");
        assert!(matches!(result, CasResult::Success { .. }));

        // Reconcile
        let reconciler = IcebergReconciler::new(Arc::clone(&storage), pointer_store);
        let result = reconciler
            .reconcile_table(&table_uuid, "acme", "prod")
            .await
            .expect("reconcile");

        assert!(result.error.is_none());
        assert_eq!(result.metadata_entries_found, 1);
        assert_eq!(result.receipts_created, 1);
        assert_eq!(result.receipts_existing, 0);
        assert_eq!(result.receipts_failed, 0);
    }

    #[tokio::test]
    async fn test_reconciler_reconcile_all_multiple_tables() {
        use crate::pointer::{CasResult, IcebergTablePointer, PointerStoreImpl};
        use arco_core::storage::MemoryBackend;
        use bytes::Bytes;

        let storage = Arc::new(MemoryBackend::new());
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));

        // Create two tables
        let table1 = Uuid::new_v4();
        let table2 = Uuid::new_v4();

        for (i, table_uuid) in [table1, table2].iter().enumerate() {
            let metadata_json = format!(
                r#"{{
                "format-version": 2,
                "table-uuid": "{}",
                "location": "gs://bucket/table{}",
                "last-sequence-number": 1,
                "last-updated-ms": {},
                "last-column-id": 3,
                "current-schema-id": 0,
                "schemas": [],
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
                "properties": {{}},
                "default-spec-id": 0,
                "partition-specs": [],
                "last-partition-id": 0,
                "default-sort-order-id": 0,
                "sort-orders": []
            }}"#,
                table_uuid,
                i,
                1704067200000_i64 + (i as i64 * 86400000)
            );

            let path = format!("metadata/table{}/v1.json", i);
            storage
                .put(&path, Bytes::from(metadata_json), WritePrecondition::None)
                .await
                .expect("put metadata");

            let pointer = IcebergTablePointer::new(*table_uuid, path);
            let result = pointer_store
                .create(table_uuid, &pointer)
                .await
                .expect("create pointer");
            assert!(matches!(result, CasResult::Success { .. }));
        }

        // Reconcile all
        let reconciler = IcebergReconciler::new(Arc::clone(&storage), pointer_store);
        let report = reconciler
            .reconcile_all("acme", "prod")
            .await
            .expect("reconcile_all");

        assert_eq!(report.tables_processed, 2);
        assert_eq!(report.tables_with_errors, 0);
        assert_eq!(report.metadata_entries_found, 2);
        assert_eq!(report.receipts_created, 2);
        assert!(!report.has_errors());
    }

    #[tokio::test]
    async fn test_reconciler_reconcile_all_empty() {
        use crate::pointer::PointerStoreImpl;
        use arco_core::storage::MemoryBackend;

        let storage = Arc::new(MemoryBackend::new());
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));
        let reconciler = IcebergReconciler::new(storage, pointer_store);

        let report = reconciler
            .reconcile_all("acme", "prod")
            .await
            .expect("reconcile_all");

        assert_eq!(report.tables_processed, 0);
        assert_eq!(report.receipts_created, 0);
        assert!(!report.has_errors());
    }
}
