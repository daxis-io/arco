//! Garbage collection for Iceberg catalog artifacts.
//!
//! This module implements GC for:
//! - Idempotency markers (Section 9.2 of design doc)
//! - Orphan metadata files
//! - Event receipts
//!
//! GC rules:
//! - `Committed`/`Failed` markers: delete after `lifetime + 24h` grace
//! - `InProgress` markers older than `timeout + 24h`: verify pointer, then delete or mark Failed
//! - Pending receipts: delete after 24h
//! - Committed receipts: retain 90 days or until compaction

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

use arco_core::storage::StorageBackend;

use crate::error::{IcebergError, IcebergResult};
use crate::idempotency::{
    FinalizeResult, IdempotencyMarker, IdempotencyStatus, IdempotencyStore, IdempotencyStoreImpl,
};
use crate::metrics;
use crate::paths::resolve_metadata_path;
use crate::pointer::PointerStore;
use crate::types::{ObjectVersion, TableMetadata};

/// Default grace period added to marker lifetime before deletion.
pub const DEFAULT_GRACE_PERIOD_HOURS: i64 = 24;

/// Default in-progress timeout (10 minutes).
pub const DEFAULT_IN_PROGRESS_TIMEOUT_MINS: i64 = 10;

/// Default event receipt retention (90 days).
pub const DEFAULT_RECEIPT_RETENTION_DAYS: i64 = 90;

/// Default pending receipt expiry (24 hours).
pub const DEFAULT_PENDING_RECEIPT_EXPIRY_HOURS: i64 = 24;

/// Configuration for idempotency marker GC.
#[derive(Debug, Clone)]
pub struct IdempotencyGcConfig {
    /// Grace period added to marker lifetime before deletion.
    pub grace_period: Duration,
    /// Timeout for in-progress markers.
    pub in_progress_timeout: Duration,
    /// Total lifetime for committed/failed markers.
    pub marker_lifetime: Duration,
}

impl Default for IdempotencyGcConfig {
    fn default() -> Self {
        Self {
            grace_period: Duration::hours(DEFAULT_GRACE_PERIOD_HOURS),
            in_progress_timeout: Duration::minutes(DEFAULT_IN_PROGRESS_TIMEOUT_MINS),
            marker_lifetime: Duration::hours(24), // Standard marker lifetime
        }
    }
}

impl IdempotencyGcConfig {
    /// Applies the configured idempotency key lifetime (ISO 8601) if provided.
    #[must_use]
    pub fn with_idempotency_key_lifetime(mut self, lifetime: Option<&str>) -> Self {
        if let Some(value) = lifetime {
            match parse_iso8601_duration(value) {
                Some(duration) => {
                    self.marker_lifetime = duration;
                }
                None => {
                    tracing::warn!(
                        value = value,
                        "Failed to parse idempotency key lifetime; using default"
                    );
                }
            }
        }
        self
    }
}

/// Result of listing markers for a table.
#[derive(Debug, Clone)]
pub struct MarkerListEntry {
    /// Path to the marker in storage.
    pub path: String,
    /// Object version for CAS operations.
    pub version: ObjectVersion,
    /// The marker itself.
    pub marker: IdempotencyMarker,
}

/// Result of listing all markers.
#[derive(Debug, Clone, Default)]
pub struct AllMarkersResult {
    /// Markers indexed by table UUID.
    pub by_table: std::collections::HashMap<Uuid, Vec<MarkerListEntry>>,
    /// Total number of markers found.
    pub total_count: usize,
    /// Number of markers that failed to parse.
    pub parse_errors: usize,
    /// Number of markers that failed to read.
    pub read_errors: usize,
    /// Number of markers whose path/table UUID mismatched.
    pub path_mismatches: usize,
}

/// Result of garbage collection for a table.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GcTableResult {
    /// Table that was processed.
    pub table_uuid: Uuid,
    /// Number of markers deleted.
    pub deleted: usize,
    /// Number of markers skipped (not eligible for GC).
    pub skipped: usize,
    /// Number of markers that failed to delete.
    pub failed: usize,
    /// Number of in-progress markers that were marked as Failed.
    pub marked_failed: usize,
}

/// Result of garbage collection across all tables.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GcReport {
    /// Results per table.
    pub tables: Vec<GcTableResult>,
    /// Total markers deleted.
    pub total_deleted: usize,
    /// Total markers skipped.
    pub total_skipped: usize,
    /// Total markers that failed to delete.
    pub total_failed: usize,
    /// Total markers that were marked as Failed.
    pub total_marked_failed: usize,
    /// Duration of the GC run in milliseconds.
    pub duration_ms: u64,
}

impl GcReport {
    /// Creates an empty report.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a table result to the report.
    pub fn add_table_result(&mut self, result: GcTableResult) {
        self.total_deleted += result.deleted;
        self.total_skipped += result.skipped;
        self.total_failed += result.failed;
        self.total_marked_failed += result.marked_failed;
        self.tables.push(result);
    }
}

/// Garbage collector for idempotency markers.
pub struct IdempotencyGarbageCollector<S, P> {
    storage: Arc<S>,
    pointer_store: Arc<P>,
    idempotency_store: IdempotencyStoreImpl<S>,
    config: IdempotencyGcConfig,
}

impl<S: StorageBackend, P: PointerStore> IdempotencyGarbageCollector<S, P> {
    /// Creates a new garbage collector with default configuration.
    #[must_use]
    pub fn new(storage: Arc<S>, pointer_store: Arc<P>) -> Self {
        let idempotency_store = IdempotencyStoreImpl::new(Arc::clone(&storage));
        Self {
            storage,
            pointer_store,
            idempotency_store,
            config: IdempotencyGcConfig::default(),
        }
    }

    /// Creates a new garbage collector with custom configuration.
    #[must_use]
    pub fn with_config(
        storage: Arc<S>,
        pointer_store: Arc<P>,
        config: IdempotencyGcConfig,
    ) -> Self {
        let idempotency_store = IdempotencyStoreImpl::new(Arc::clone(&storage));
        Self {
            storage,
            pointer_store,
            idempotency_store,
            config,
        }
    }

    /// Creates a new garbage collector with config and idempotency lifetime override.
    #[must_use]
    pub fn with_config_and_lifetime(
        storage: Arc<S>,
        pointer_store: Arc<P>,
        config: IdempotencyGcConfig,
        idempotency_key_lifetime: Option<&str>,
    ) -> Self {
        Self::with_config(
            storage,
            pointer_store,
            config.with_idempotency_key_lifetime(idempotency_key_lifetime),
        )
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &IdempotencyGcConfig {
        &self.config
    }

    /// Lists all markers for a specific table.
    ///
    /// Returns markers ordered by path for deterministic results.
    ///
    /// # Errors
    ///
    /// Returns error if storage listing or marker reading fails.
    pub async fn list_markers(&self, table_uuid: &Uuid) -> IcebergResult<Vec<MarkerListEntry>> {
        let prefix = format!("_catalog/iceberg_idempotency/{table_uuid}/");
        let result = self.list_markers_with_prefix(&prefix).await?;
        if result.stats.read_errors > 0
            || result.stats.parse_errors > 0
            || result.stats.path_mismatches > 0
        {
            tracing::warn!(
                table_uuid = %table_uuid,
                read_errors = result.stats.read_errors,
                parse_errors = result.stats.parse_errors,
                path_mismatches = result.stats.path_mismatches,
                "Marker listing encountered errors"
            );
        }
        Ok(result.entries)
    }

    /// Lists all markers across all tables.
    ///
    /// # Errors
    ///
    /// Returns error if storage listing fails.
    pub async fn list_all_markers(&self) -> IcebergResult<AllMarkersResult> {
        let prefix = "_catalog/iceberg_idempotency/";
        let list_result = self.list_markers_with_prefix(prefix).await?;
        let entries = list_result.entries;

        let mut by_table = std::collections::HashMap::new();
        for entry in &entries {
            by_table
                .entry(entry.marker.table_uuid)
                .or_insert_with(Vec::new)
                .push(entry.clone());
        }

        Ok(AllMarkersResult {
            total_count: entries.len(),
            parse_errors: list_result.stats.parse_errors,
            read_errors: list_result.stats.read_errors,
            path_mismatches: list_result.stats.path_mismatches,
            by_table,
        })
    }

    /// Internal helper to list and parse markers with a prefix.
    async fn list_markers_with_prefix(&self, prefix: &str) -> IcebergResult<MarkerListResult> {
        let objects = self
            .storage
            .list(prefix)
            .await
            .map_err(|e| IcebergError::Internal {
                message: format!("Failed to list markers: {e}"),
            })?;
        let mut entries = Vec::with_capacity(objects.len());
        let mut stats = MarkerListStats::default();

        for obj in objects {
            // Only process JSON files (case-sensitive is intentional for Iceberg markers)
            #[allow(clippy::case_sensitive_file_extension_comparisons)]
            if !obj.path.ends_with(".json") {
                continue;
            }

            let Some(path_table_uuid) = table_uuid_from_marker_path(&obj.path) else {
                stats.path_mismatches += 1;
                tracing::warn!(
                    path = %obj.path,
                    "Marker path did not contain a valid table UUID"
                );
                continue;
            };

            // Read and parse the marker
            match self.storage.get(&obj.path).await {
                Ok(bytes) => match serde_json::from_slice::<IdempotencyMarker>(&bytes) {
                    Ok(marker) => {
                        if marker.table_uuid != path_table_uuid {
                            stats.path_mismatches += 1;
                            tracing::warn!(
                                path = %obj.path,
                                marker_table_uuid = %marker.table_uuid,
                                path_table_uuid = %path_table_uuid,
                                "Marker table UUID did not match path"
                            );
                            continue;
                        }
                        entries.push(MarkerListEntry {
                            path: obj.path,
                            version: ObjectVersion::new(obj.version),
                            marker,
                        });
                    }
                    Err(e) => {
                        stats.parse_errors += 1;
                        tracing::warn!(path = %obj.path, error = %e, "Failed to parse marker");
                    }
                },
                Err(e) => {
                    stats.read_errors += 1;
                    tracing::warn!(path = %obj.path, error = %e, "Failed to read marker");
                }
            }
        }

        // Sort by path for deterministic ordering
        entries.sort_by(|a, b| a.path.cmp(&b.path));

        Ok(MarkerListResult { entries, stats })
    }
}

#[derive(Debug, Default)]
struct MarkerListStats {
    read_errors: usize,
    parse_errors: usize,
    path_mismatches: usize,
}

#[derive(Debug)]
struct MarkerListResult {
    entries: Vec<MarkerListEntry>,
    stats: MarkerListStats,
}

fn table_uuid_from_marker_path(path: &str) -> Option<Uuid> {
    let prefix = "_catalog/iceberg_idempotency/";
    let rest = path.strip_prefix(prefix)?;
    let table_part = rest.split('/').next()?;
    Uuid::parse_str(table_part).ok()
}

fn parse_iso8601_duration(value: &str) -> Option<Duration> {
    if !value.starts_with('P') {
        return None;
    }

    let mut days: i64 = 0;
    let mut hours: i64 = 0;
    let mut minutes: i64 = 0;
    let mut seconds: i64 = 0;

    let (date_part, time_part) = if let Some((date, time)) = value[1..].split_once('T') {
        (date, time)
    } else {
        (&value[1..], "")
    };

    if !date_part.is_empty() {
        let mut num = String::new();
        for ch in date_part.chars() {
            if ch.is_ascii_digit() {
                num.push(ch);
                continue;
            }
            if num.is_empty() {
                return None;
            }
            let parsed = num.parse::<i64>().ok()?;
            num.clear();
            match ch {
                'D' => days = parsed,
                _ => return None,
            }
        }
        if !num.is_empty() {
            return None;
        }
    }

    if !time_part.is_empty() {
        let mut num = String::new();
        for ch in time_part.chars() {
            if ch.is_ascii_digit() {
                num.push(ch);
                continue;
            }
            if num.is_empty() {
                return None;
            }
            let parsed = num.parse::<i64>().ok()?;
            num.clear();
            match ch {
                'H' => hours = parsed,
                'M' => minutes = parsed,
                'S' => seconds = parsed,
                _ => return None,
            }
        }
        if !num.is_empty() {
            return None;
        }
    }

    Some(
        Duration::days(days)
            + Duration::hours(hours)
            + Duration::minutes(minutes)
            + Duration::seconds(seconds),
    )
}

// ============================================================================
// GC Cleanup Logic (Task 7)
// ============================================================================

/// Action to take for an in-progress marker after pointer check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InProgressAction {
    /// Safe to delete - pointer moved past this marker.
    SafeToDelete,
    /// Pointer still references this marker's metadata - mark as failed first.
    MarkFailedThenDelete,
    /// Pointer not found - treat as orphan and delete.
    PointerNotFound,
}

impl<S: StorageBackend, P: PointerStore> IdempotencyGarbageCollector<S, P> {
    /// Checks if a marker is eligible for garbage collection.
    #[must_use]
    pub fn is_eligible_for_gc(&self, marker: &IdempotencyMarker, now: DateTime<Utc>) -> bool {
        match marker.status {
            IdempotencyStatus::Committed | IdempotencyStatus::Failed => {
                // Eligible after lifetime + grace period from finalization time
                let finalized_at = marker
                    .committed_at
                    .or(marker.failed_at)
                    .unwrap_or(marker.started_at);
                finalized_at + self.config.marker_lifetime + self.config.grace_period < now
            }
            IdempotencyStatus::InProgress => {
                // Eligible after timeout + grace period from start time
                marker.started_at + self.config.in_progress_timeout + self.config.grace_period < now
            }
        }
    }

    /// Determines the action to take for an in-progress marker.
    ///
    /// Per design doc Section 9.2:
    /// - If `pointer.metadata_location != marker.metadata_location`, safe to delete
    /// - Otherwise, mark as Failed before deleting
    ///
    /// # Errors
    ///
    /// Returns error if pointer loading fails.
    pub async fn check_in_progress_action(
        &self,
        marker: &IdempotencyMarker,
    ) -> IcebergResult<InProgressAction> {
        let pointer = self.pointer_store.load(&marker.table_uuid).await?;

        match pointer {
            None => {
                // Pointer not found - table may have been deleted
                Ok(InProgressAction::PointerNotFound)
            }
            Some((pointer, _version)) => {
                if pointer.current_metadata_location == marker.metadata_location {
                    // Pointer still points to this marker's metadata
                    // The commit may have succeeded but finalization failed
                    Ok(InProgressAction::MarkFailedThenDelete)
                } else {
                    // Pointer has moved past this marker
                    Ok(InProgressAction::SafeToDelete)
                }
            }
        }
    }

    /// Cleans up a single marker.
    ///
    /// Returns the action taken (deleted, skipped, or failed).
    async fn clean_marker(&self, entry: &MarkerListEntry, now: DateTime<Utc>) -> CleanupOutcome {
        if !self.is_eligible_for_gc(&entry.marker, now) {
            return CleanupOutcome::skipped();
        }

        match entry.marker.status {
            IdempotencyStatus::Committed | IdempotencyStatus::Failed => {
                // Simply delete
                self.delete_marker(&entry.path).await
            }
            IdempotencyStatus::InProgress => {
                // Check pointer before deleting
                match self.check_in_progress_action(&entry.marker).await {
                    Ok(InProgressAction::SafeToDelete | InProgressAction::PointerNotFound) => {
                        self.delete_marker(&entry.path).await
                    }
                    Ok(InProgressAction::MarkFailedThenDelete) => {
                        self.mark_failed_then_delete(entry).await
                    }
                    Err(e) => {
                        tracing::warn!(
                            path = %entry.path,
                            error = %e,
                            "Failed to check pointer for in-progress marker"
                        );
                        CleanupOutcome::failed()
                    }
                }
            }
        }
    }

    /// Deletes a marker file.
    async fn delete_marker(&self, path: &str) -> CleanupOutcome {
        match self.storage.delete(path).await {
            Ok(()) => {
                tracing::debug!(path = %path, "Deleted marker");
                CleanupOutcome::deleted()
            }
            Err(e) => {
                tracing::warn!(path = %path, error = %e, "Failed to delete marker");
                CleanupOutcome::failed()
            }
        }
    }

    async fn mark_failed_then_delete(&self, entry: &MarkerListEntry) -> CleanupOutcome {
        let error = IcebergError::commit_conflict(
            "Stale in-progress marker reclaimed by GC (pointer still matched)",
        );
        let error_response = crate::error::IcebergErrorResponse::from(&error);
        let failed_marker = entry
            .marker
            .clone()
            .finalize_failed(error.status_code().as_u16(), error_response);

        match self
            .idempotency_store
            .finalize(&failed_marker, &entry.version)
            .await
        {
            Ok(FinalizeResult::Success { .. }) => {
                let delete_outcome = self.delete_marker(&entry.path).await;
                CleanupOutcome {
                    deleted: delete_outcome.deleted,
                    failed: delete_outcome.failed,
                    skipped: 0,
                    marked_failed: 1,
                }
            }
            Ok(FinalizeResult::Conflict { .. }) => {
                tracing::warn!(
                    path = %entry.path,
                    table_uuid = %entry.marker.table_uuid,
                    "Marker changed during GC; skipping deletion"
                );
                CleanupOutcome::skipped()
            }
            Err(e) => {
                tracing::warn!(
                    path = %entry.path,
                    error = %e,
                    "Failed to finalize in-progress marker as failed"
                );
                CleanupOutcome::failed()
            }
        }
    }

    /// Cleans up all markers for a specific table.
    ///
    /// # Errors
    ///
    /// Returns error if marker listing fails.
    #[tracing::instrument(skip(self), fields(table_uuid = %table_uuid))]
    pub async fn clean_table(&self, table_uuid: &Uuid) -> IcebergResult<GcTableResult> {
        let start = Instant::now();
        let now = Utc::now();
        let entries = self.list_markers(table_uuid).await?;

        let mut result = GcTableResult {
            table_uuid: *table_uuid,
            ..Default::default()
        };

        for entry in &entries {
            let outcome = self.clean_marker(entry, now).await;
            result.deleted += outcome.deleted;
            result.skipped += outcome.skipped;
            result.failed += outcome.failed;
            result.marked_failed += outcome.marked_failed;
        }

        tracing::info!(
            table_uuid = %table_uuid,
            deleted = result.deleted,
            skipped = result.skipped,
            failed = result.failed,
            marked_failed = result.marked_failed,
            "Cleaned table markers"
        );

        metrics::record_gc_markers_deleted("total", result.deleted as u64);
        metrics::record_gc_markers_skipped(result.skipped as u64);
        metrics::record_gc_markers_failed(result.failed as u64);
        metrics::record_gc_duration("markers", start.elapsed().as_secs_f64());

        Ok(result)
    }

    /// Cleans up all markers across all tables.
    ///
    /// # Errors
    ///
    /// Returns error if marker listing fails.
    #[allow(clippy::cast_possible_truncation)]
    #[tracing::instrument(skip(self))]
    pub async fn clean_all(&self) -> IcebergResult<GcReport> {
        let start = Instant::now();
        let now = Utc::now();

        let all_markers = self.list_all_markers().await?;
        let mut report = GcReport::new();

        for (table_uuid, entries) in &all_markers.by_table {
            let mut table_result = GcTableResult {
                table_uuid: *table_uuid,
                ..Default::default()
            };

            for entry in entries {
                let outcome = self.clean_marker(entry, now).await;
                table_result.deleted += outcome.deleted;
                table_result.skipped += outcome.skipped;
                table_result.failed += outcome.failed;
                table_result.marked_failed += outcome.marked_failed;
            }

            report.add_table_result(table_result);
        }

        report.duration_ms = start.elapsed().as_millis() as u64;

        tracing::info!(
            tables = report.tables.len(),
            deleted = report.total_deleted,
            skipped = report.total_skipped,
            failed = report.total_failed,
            marked_failed = report.total_marked_failed,
            duration_ms = report.duration_ms,
            "Completed marker GC"
        );

        metrics::record_gc_markers_deleted("total", report.total_deleted as u64);
        metrics::record_gc_markers_skipped(report.total_skipped as u64);
        metrics::record_gc_markers_failed(report.total_failed as u64);
        #[allow(clippy::cast_precision_loss)]
        metrics::record_gc_duration("markers", (report.duration_ms as f64) / 1000.0);

        Ok(report)
    }
}

/// Result of cleaning a single marker.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CleanupOutcome {
    deleted: usize,
    skipped: usize,
    failed: usize,
    marked_failed: usize,
}

impl CleanupOutcome {
    fn deleted() -> Self {
        Self {
            deleted: 1,
            ..Default::default()
        }
    }

    fn skipped() -> Self {
        Self {
            skipped: 1,
            ..Default::default()
        }
    }

    fn failed() -> Self {
        Self {
            failed: 1,
            ..Default::default()
        }
    }
}

// ============================================================================
// Orphan Metadata Cleanup (Task 8)
// ============================================================================

/// Default minimum age for orphan deletion (24 hours).
pub const DEFAULT_ORPHAN_MIN_AGE_HOURS: i64 = 24;

/// Configuration for orphan metadata GC.
#[derive(Debug, Clone)]
pub struct OrphanGcConfig {
    /// Minimum age before an orphan can be deleted.
    pub min_age: Duration,
}

impl Default for OrphanGcConfig {
    fn default() -> Self {
        Self {
            min_age: Duration::hours(DEFAULT_ORPHAN_MIN_AGE_HOURS),
        }
    }
}

/// Result of orphan metadata cleanup for a table.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OrphanCleanupResult {
    /// Table that was processed.
    pub table_uuid: Uuid,
    /// Number of orphan files found.
    pub orphans_found: usize,
    /// Number of orphans deleted.
    pub orphans_deleted: usize,
    /// Number of orphans skipped (too recent).
    pub orphans_skipped: usize,
    /// Number of orphans that failed to delete.
    pub orphans_failed: usize,
    /// Total files scanned in metadata directory.
    pub files_scanned: usize,
}

/// Orphan metadata file cleaner.
///
/// Identifies and removes orphan metadata files that are:
/// 1. Not referenced by current pointer
/// 2. Not in the metadata log chain
/// 3. Older than the minimum age window
pub struct OrphanMetadataCleaner<S> {
    storage: Arc<S>,
    config: OrphanGcConfig,
}

impl<S: StorageBackend> OrphanMetadataCleaner<S> {
    /// Creates a new orphan cleaner with default configuration.
    #[must_use]
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            config: OrphanGcConfig::default(),
        }
    }

    /// Creates a new orphan cleaner with custom configuration.
    #[must_use]
    pub fn with_config(storage: Arc<S>, config: OrphanGcConfig) -> Self {
        Self { storage, config }
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &OrphanGcConfig {
        &self.config
    }

    /// Identifies orphan metadata files in a table's metadata directory.
    ///
    /// # Arguments
    ///
    /// * `metadata_prefix` - The storage prefix for metadata files (e.g., "metadata/")
    /// * `allowlist` - Set of valid metadata file paths to keep
    ///
    /// # Returns
    ///
    /// Scan result with orphan files and total files scanned.
    ///
    /// # Errors
    ///
    /// Returns error if storage listing fails.
    pub async fn find_orphans(
        &self,
        metadata_prefix: &str,
        allowlist: &HashSet<String>,
    ) -> IcebergResult<OrphanScanResult> {
        let objects =
            self.storage
                .list(metadata_prefix)
                .await
                .map_err(|e| IcebergError::Internal {
                    message: format!("Failed to list metadata files: {e}"),
                })?;

        let mut orphans = Vec::new();
        let mut files_scanned = 0;

        for obj in objects {
            files_scanned += 1;
            // Skip non-metadata files (case-sensitive is intentional for Iceberg files)
            #[allow(clippy::case_sensitive_file_extension_comparisons)]
            if !obj.path.ends_with(".json") && !obj.path.ends_with(".metadata.json") {
                continue;
            }

            // Check if this file is in the allowlist
            if !allowlist.contains(&obj.path) {
                orphans.push(OrphanFile {
                    path: obj.path,
                    last_modified: obj.last_modified.unwrap_or_else(Utc::now),
                    size_bytes: obj.size,
                });
            }
        }

        // Sort by path for deterministic ordering
        orphans.sort_by(|a, b| a.path.cmp(&b.path));

        Ok(OrphanScanResult {
            orphans,
            files_scanned,
        })
    }

    /// Cleans orphan metadata files for a table.
    ///
    /// # Arguments
    ///
    /// * `table_uuid` - The table to clean
    /// * `metadata_prefix` - The storage prefix for metadata files
    /// * `allowlist` - Set of valid metadata file paths to keep
    ///
    /// # Errors
    ///
    /// Returns error if orphan scanning fails.
    #[tracing::instrument(skip(self, allowlist))]
    pub async fn clean_orphans(
        &self,
        table_uuid: Uuid,
        metadata_prefix: &str,
        allowlist: &HashSet<String>,
    ) -> IcebergResult<OrphanCleanupResult> {
        let start = Instant::now();
        let now = Utc::now();
        let scan_result = self.find_orphans(metadata_prefix, allowlist).await?;
        let orphans = scan_result.orphans;

        let mut result = OrphanCleanupResult {
            table_uuid,
            orphans_found: orphans.len(),
            files_scanned: scan_result.files_scanned,
            ..Default::default()
        };

        for orphan in &orphans {
            // Check if orphan is old enough to delete
            if now - orphan.last_modified < self.config.min_age {
                tracing::debug!(
                    path = %orphan.path,
                    age_hours = (now - orphan.last_modified).num_hours(),
                    "Orphan too recent, skipping"
                );
                result.orphans_skipped += 1;
                continue;
            }

            // Delete the orphan
            match self.storage.delete(&orphan.path).await {
                Ok(()) => {
                    tracing::info!(
                        path = %orphan.path,
                        table_uuid = %table_uuid,
                        "Deleted orphan metadata file"
                    );
                    result.orphans_deleted += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        path = %orphan.path,
                        error = %e,
                        "Failed to delete orphan metadata file"
                    );
                    result.orphans_failed += 1;
                }
            }
        }

        metrics::record_gc_orphans_deleted(result.orphans_deleted as u64);
        metrics::record_gc_duration("metadata", start.elapsed().as_secs_f64());

        Ok(result)
    }
}

/// An orphan metadata file.
#[derive(Debug, Clone)]
pub struct OrphanFile {
    /// Path to the orphan file.
    pub path: String,
    /// When the file was last modified.
    pub last_modified: DateTime<Utc>,
    /// Size of the file in bytes.
    pub size_bytes: u64,
}

/// Result of scanning a metadata prefix for orphans.
#[derive(Debug, Clone)]
pub struct OrphanScanResult {
    /// Orphan metadata files discovered.
    pub orphans: Vec<OrphanFile>,
    /// Total files scanned under the prefix.
    pub files_scanned: usize,
}

// ============================================================================
// Metadata GC Planning (Task 8.1)
// ============================================================================

/// Default maximum number of previous metadata versions to retain.
pub const DEFAULT_PREVIOUS_VERSIONS_MAX: usize = 100;

const METADATA_PREVIOUS_VERSIONS_MAX_KEY: &str = "metadata.previous-versions-max";
const METADATA_DELETE_AFTER_COMMIT_KEY: &str = "metadata.delete-after-commit";

/// Parsed metadata retention policy from Iceberg table metadata.
#[derive(Debug, Clone)]
pub struct MetadataRetentionPolicy {
    /// Maximum number of previous metadata versions to retain.
    pub previous_versions_max: usize,
    /// Whether to delete metadata files after commit.
    pub delete_after_commit: bool,
}

impl MetadataRetentionPolicy {
    /// Parses retention policy from Iceberg table metadata properties.
    #[must_use]
    pub fn from_metadata(metadata: &TableMetadata) -> Self {
        let mut previous_versions_max = DEFAULT_PREVIOUS_VERSIONS_MAX;
        if let Some(value) = metadata.properties.get(METADATA_PREVIOUS_VERSIONS_MAX_KEY) {
            if let Some(parsed) = parse_usize(value) {
                previous_versions_max = parsed;
            } else {
                tracing::warn!(
                    value = value,
                    "Invalid metadata.previous-versions-max; using default"
                );
            }
        }

        let mut delete_after_commit = false;
        if let Some(value) = metadata.properties.get(METADATA_DELETE_AFTER_COMMIT_KEY) {
            if let Some(parsed) = parse_bool(value) {
                delete_after_commit = parsed;
            } else {
                tracing::warn!(
                    value = value,
                    "Invalid metadata.delete-after-commit; using default"
                );
            }
        }

        Self {
            previous_versions_max,
            delete_after_commit,
        }
    }
}

/// Planned metadata GC inputs for a table.
#[derive(Debug, Clone)]
pub struct MetadataGcPlan {
    /// Table that was planned.
    pub table_uuid: Uuid,
    /// Current metadata location (as stored in pointer).
    pub current_metadata_location: String,
    /// Prefix for metadata directory listing.
    pub metadata_prefix: String,
    /// Allowlist of metadata files to retain (storage-relative paths).
    pub allowlist: HashSet<String>,
    /// Retention policy applied.
    pub policy: MetadataRetentionPolicy,
}

/// Result of metadata GC execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetadataGcResult {
    /// Table that was processed.
    pub table_uuid: Uuid,
    /// Whether metadata deletion was enabled by policy.
    pub delete_after_commit: bool,
    /// Maximum previous versions retained.
    pub previous_versions_max: usize,
    /// Size of the allowlist.
    pub allowlist_len: usize,
    /// Whether GC was skipped due to policy.
    #[serde(default)]
    pub skipped_due_to_policy: bool,
    /// Orphan cleanup results, if GC ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orphan_result: Option<OrphanCleanupResult>,
}

/// Planner and executor for metadata GC.
pub struct MetadataGcPlanner<S, P> {
    storage: Arc<S>,
    pointer_store: Arc<P>,
    orphan_config: OrphanGcConfig,
}

impl<S: StorageBackend, P: PointerStore> MetadataGcPlanner<S, P> {
    /// Creates a new metadata GC planner with default configuration.
    #[must_use]
    pub fn new(storage: Arc<S>, pointer_store: Arc<P>) -> Self {
        Self {
            storage,
            pointer_store,
            orphan_config: OrphanGcConfig::default(),
        }
    }

    /// Creates a new metadata GC planner with custom orphan GC configuration.
    #[must_use]
    pub fn with_config(storage: Arc<S>, pointer_store: Arc<P>, config: OrphanGcConfig) -> Self {
        Self {
            storage,
            pointer_store,
            orphan_config: config,
        }
    }

    /// Builds a metadata GC plan for a table.
    ///
    /// # Errors
    ///
    /// Returns error if pointer load, metadata read, or metadata parse fails.
    #[tracing::instrument(skip(self), fields(table_uuid = %table_uuid, tenant = %tenant, workspace = %workspace))]
    pub async fn plan_for_table(
        &self,
        table_uuid: &Uuid,
        tenant: &str,
        workspace: &str,
    ) -> IcebergResult<MetadataGcPlan> {
        let Some((pointer, _)) = self.pointer_store.load(table_uuid).await? else {
            return Err(IcebergError::NotFound {
                message: format!("Table pointer not found: {table_uuid}"),
                error_type: "NoSuchTableException",
            });
        };

        let current_path =
            resolve_metadata_path(&pointer.current_metadata_location, tenant, workspace)?;
        let metadata_bytes =
            self.storage
                .get(&current_path)
                .await
                .map_err(|e| IcebergError::Internal {
                    message: format!("Failed to read metadata file: {e}"),
                })?;

        let metadata: TableMetadata =
            serde_json::from_slice(&metadata_bytes).map_err(|e| IcebergError::Internal {
                message: format!("Failed to parse table metadata: {e}"),
            })?;

        let policy = MetadataRetentionPolicy::from_metadata(&metadata);
        let metadata_prefix = metadata_directory(&current_path)?;

        let mut allowlist = HashSet::new();
        allowlist.insert(current_path);

        if let Some(previous) = pointer.previous_metadata_location.as_deref() {
            match resolve_metadata_path(previous, tenant, workspace) {
                Ok(path) => {
                    allowlist.insert(path);
                }
                Err(e) => tracing::warn!(
                    location = %previous,
                    error = %e,
                    "Failed to resolve previous metadata location for GC allowlist"
                ),
            }
        }

        let mut log_entries: Vec<_> = metadata.metadata_log.iter().collect();
        log_entries.sort_by_key(|entry| entry.timestamp_ms);

        let entries_to_keep = if policy.delete_after_commit {
            let start = log_entries
                .len()
                .saturating_sub(policy.previous_versions_max);
            // start is always <= log_entries.len() due to saturating_sub
            log_entries.get(start..).unwrap_or(&[])
        } else {
            &log_entries[..]
        };

        for entry in entries_to_keep {
            match resolve_metadata_path(&entry.metadata_file, tenant, workspace) {
                Ok(path) => {
                    allowlist.insert(path);
                }
                Err(e) => tracing::warn!(
                    location = %entry.metadata_file,
                    error = %e,
                    "Failed to resolve metadata-log entry for GC allowlist"
                ),
            }
        }

        Ok(MetadataGcPlan {
            table_uuid: *table_uuid,
            current_metadata_location: pointer.current_metadata_location,
            metadata_prefix,
            allowlist,
            policy,
        })
    }

    /// Executes metadata GC for a table based on the plan.
    ///
    /// # Errors
    ///
    /// Returns error if planning or metadata file deletion fails.
    #[tracing::instrument(skip(self), fields(table_uuid = %table_uuid, tenant = %tenant, workspace = %workspace))]
    pub async fn clean_table(
        &self,
        table_uuid: &Uuid,
        tenant: &str,
        workspace: &str,
    ) -> IcebergResult<MetadataGcResult> {
        let plan = self.plan_for_table(table_uuid, tenant, workspace).await?;

        if !plan.policy.delete_after_commit {
            tracing::info!(
                table_uuid = %table_uuid,
                "Skipping metadata GC (metadata.delete-after-commit=false)"
            );
            return Ok(MetadataGcResult {
                table_uuid: *table_uuid,
                delete_after_commit: false,
                previous_versions_max: plan.policy.previous_versions_max,
                allowlist_len: plan.allowlist.len(),
                skipped_due_to_policy: true,
                orphan_result: None,
            });
        }

        let cleaner = OrphanMetadataCleaner::with_config(
            Arc::clone(&self.storage),
            self.orphan_config.clone(),
        );
        let orphan_result = cleaner
            .clean_orphans(*table_uuid, &plan.metadata_prefix, &plan.allowlist)
            .await?;

        Ok(MetadataGcResult {
            table_uuid: *table_uuid,
            delete_after_commit: plan.policy.delete_after_commit,
            previous_versions_max: plan.policy.previous_versions_max,
            allowlist_len: plan.allowlist.len(),
            skipped_due_to_policy: false,
            orphan_result: Some(orphan_result),
        })
    }
}

fn metadata_directory(path: &str) -> IcebergResult<String> {
    let Some((prefix, _)) = path.rsplit_once('/') else {
        return Err(IcebergError::Internal {
            message: format!("Invalid metadata path: {path}"),
        });
    };
    Ok(format!("{prefix}/"))
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

fn parse_usize(value: &str) -> Option<usize> {
    value.trim().parse::<usize>().ok()
}

// ============================================================================
// Event Receipt GC (Task 9)
// ============================================================================

/// Default pending receipt retention (24 hours).
pub const DEFAULT_PENDING_RECEIPT_RETENTION_HOURS: i64 = 24;

/// Configuration for event receipt GC.
#[derive(Debug, Clone)]
pub struct EventReceiptGcConfig {
    /// Minimum age before pending receipts can be deleted.
    pub pending_retention: Duration,
    /// Minimum age before committed receipts can be deleted.
    pub committed_retention: Duration,
}

impl Default for EventReceiptGcConfig {
    fn default() -> Self {
        Self {
            pending_retention: Duration::hours(DEFAULT_PENDING_RECEIPT_RETENTION_HOURS),
            committed_retention: Duration::days(DEFAULT_RECEIPT_RETENTION_DAYS),
        }
    }
}

/// Result of event receipt cleanup.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventReceiptGcResult {
    /// Number of pending receipts deleted.
    pub pending_deleted: usize,
    /// Number of pending receipts skipped (too recent).
    pub pending_skipped: usize,
    /// Number of pending receipts that failed to delete.
    pub pending_failed: usize,
    /// Number of committed receipts deleted.
    pub committed_deleted: usize,
    /// Number of committed receipts skipped (too recent).
    pub committed_skipped: usize,
    /// Number of committed receipts that failed to delete.
    pub committed_failed: usize,
    /// Total files scanned.
    pub files_scanned: usize,
    /// Duration in milliseconds.
    pub duration_ms: u64,
}

/// Garbage collector for event receipts.
///
/// GC rules per design doc Section 9.2:
/// - Pending receipts: delete after 24h
/// - Committed receipts: retain 90 days or until compaction
pub struct EventReceiptGarbageCollector<S> {
    storage: Arc<S>,
    config: EventReceiptGcConfig,
}

impl<S: StorageBackend> EventReceiptGarbageCollector<S> {
    /// Creates a new event receipt GC with default configuration.
    #[must_use]
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            config: EventReceiptGcConfig::default(),
        }
    }

    /// Creates a new event receipt GC with custom configuration.
    #[must_use]
    pub fn with_config(storage: Arc<S>, config: EventReceiptGcConfig) -> Self {
        Self { storage, config }
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &EventReceiptGcConfig {
        &self.config
    }

    /// Cleans pending receipts older than the retention period.
    ///
    /// Pending receipts are at: `events/YYYY-MM-DD/iceberg/pending/`
    ///
    /// # Errors
    ///
    /// Returns error if storage listing or deletion fails.
    pub async fn clean_pending_receipts(
        &self,
        date: chrono::NaiveDate,
    ) -> IcebergResult<ReceiptCleanupResult> {
        let prefix = format!("events/{}/iceberg/pending/", date.format("%Y-%m-%d"));
        self.clean_receipts_in_prefix(&prefix, self.config.pending_retention)
            .await
    }

    /// Cleans committed receipts older than the retention period.
    ///
    /// Committed receipts are at: `events/YYYY-MM-DD/iceberg/committed/`
    ///
    /// # Errors
    ///
    /// Returns error if storage listing or deletion fails.
    pub async fn clean_committed_receipts(
        &self,
        date: chrono::NaiveDate,
    ) -> IcebergResult<ReceiptCleanupResult> {
        let prefix = format!("events/{}/iceberg/committed/", date.format("%Y-%m-%d"));
        self.clean_receipts_in_prefix(&prefix, self.config.committed_retention)
            .await
    }

    /// Cleans all eligible receipts for a specific date.
    ///
    /// Returns combined results for both pending and committed receipts.
    ///
    /// # Errors
    ///
    /// Returns error if receipt cleaning fails.
    #[allow(clippy::cast_possible_truncation)]
    #[tracing::instrument(skip(self), fields(date = %date))]
    pub async fn clean_receipts_for_date(
        &self,
        date: chrono::NaiveDate,
    ) -> IcebergResult<EventReceiptGcResult> {
        let start = Instant::now();

        let pending_result = self.clean_pending_receipts(date).await?;
        let committed_result = self.clean_committed_receipts(date).await?;

        let result = EventReceiptGcResult {
            pending_deleted: pending_result.deleted,
            pending_skipped: pending_result.skipped,
            pending_failed: pending_result.failed,
            committed_deleted: committed_result.deleted,
            committed_skipped: committed_result.skipped,
            committed_failed: committed_result.failed,
            files_scanned: pending_result.files_scanned + committed_result.files_scanned,
            duration_ms: start.elapsed().as_millis() as u64,
        };

        metrics::record_gc_receipts_deleted("pending", result.pending_deleted as u64);
        metrics::record_gc_receipts_deleted("committed", result.committed_deleted as u64);
        #[allow(clippy::cast_precision_loss)]
        metrics::record_gc_duration("receipts", (result.duration_ms as f64) / 1000.0);

        Ok(result)
    }

    /// Cleans all eligible receipts for a range of dates.
    ///
    /// Typically called with dates older than the retention period.
    ///
    /// # Errors
    ///
    /// Returns error if receipt cleaning fails.
    #[allow(clippy::cast_possible_truncation)]
    #[tracing::instrument(skip(self), fields(start_date = %start_date, end_date = %end_date))]
    pub async fn clean_receipts_for_date_range(
        &self,
        start_date: chrono::NaiveDate,
        end_date: chrono::NaiveDate,
    ) -> IcebergResult<EventReceiptGcResult> {
        let start = Instant::now();
        let mut result = EventReceiptGcResult::default();

        let mut current = start_date;
        loop {
            let day_result = self.clean_receipts_for_date(current).await?;
            result.pending_deleted += day_result.pending_deleted;
            result.pending_skipped += day_result.pending_skipped;
            result.pending_failed += day_result.pending_failed;
            result.committed_deleted += day_result.committed_deleted;
            result.committed_skipped += day_result.committed_skipped;
            result.committed_failed += day_result.committed_failed;
            result.files_scanned += day_result.files_scanned;

            if current >= end_date {
                break;
            }

            // Move to next day, break if we can't advance (at max date)
            match current.succ_opt() {
                Some(next) => current = next,
                None => break,
            }
        }

        result.duration_ms = start.elapsed().as_millis() as u64;

        tracing::info!(
            pending_deleted = result.pending_deleted,
            committed_deleted = result.committed_deleted,
            files_scanned = result.files_scanned,
            duration_ms = result.duration_ms,
            "Completed event receipt GC"
        );

        Ok(result)
    }

    /// Internal helper to clean receipts in a specific prefix.
    async fn clean_receipts_in_prefix(
        &self,
        prefix: &str,
        retention: Duration,
    ) -> IcebergResult<ReceiptCleanupResult> {
        let now = Utc::now();
        let objects = self
            .storage
            .list(prefix)
            .await
            .map_err(|e| IcebergError::Internal {
                message: format!("Failed to list receipts at {prefix}: {e}"),
            })?;

        let mut result = ReceiptCleanupResult {
            files_scanned: objects.len(),
            ..Default::default()
        };

        for obj in objects {
            // Only process JSON files (case-sensitive is intentional)
            #[allow(clippy::case_sensitive_file_extension_comparisons)]
            if !obj.path.ends_with(".json") {
                continue;
            }

            // Check age
            let last_modified = obj.last_modified.unwrap_or_else(Utc::now);
            if now - last_modified < retention {
                result.skipped += 1;
                continue;
            }

            // Delete
            match self.storage.delete(&obj.path).await {
                Ok(()) => {
                    tracing::debug!(path = %obj.path, "Deleted expired receipt");
                    result.deleted += 1;
                }
                Err(e) => {
                    tracing::warn!(path = %obj.path, error = %e, "Failed to delete receipt");
                    result.failed += 1;
                }
            }
        }

        Ok(result)
    }
}

/// Result of cleaning receipts in a single prefix.
#[derive(Debug, Clone, Default)]
pub struct ReceiptCleanupResult {
    /// Number of receipts deleted.
    pub deleted: usize,
    /// Number of receipts skipped (too recent).
    pub skipped: usize,
    /// Number of receipts that failed to delete.
    pub failed: usize,
    /// Total files scanned.
    pub files_scanned: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::storage::MemoryBackend;
    use async_trait::async_trait;
    use bytes::Bytes;

    // Mock PointerStore for testing
    struct MockPointerStore;

    #[async_trait]
    impl PointerStore for MockPointerStore {
        async fn load(
            &self,
            _table_uuid: &Uuid,
        ) -> IcebergResult<Option<(crate::pointer::IcebergTablePointer, ObjectVersion)>> {
            Ok(None)
        }

        async fn create(
            &self,
            _table_uuid: &Uuid,
            _pointer: &crate::pointer::IcebergTablePointer,
        ) -> IcebergResult<crate::pointer::CasResult> {
            unimplemented!("not needed for listing tests")
        }

        async fn compare_and_swap(
            &self,
            _table_uuid: &Uuid,
            _expected_version: &ObjectVersion,
            _new_pointer: &crate::pointer::IcebergTablePointer,
        ) -> IcebergResult<crate::pointer::CasResult> {
            unimplemented!("not needed for listing tests")
        }

        async fn delete(
            &self,
            _table_uuid: &Uuid,
            _expected_version: &ObjectVersion,
        ) -> IcebergResult<crate::pointer::CasResult> {
            unimplemented!("not needed for listing tests")
        }

        async fn list_all(&self) -> IcebergResult<Vec<Uuid>> {
            Ok(vec![])
        }
    }

    // ========================================================================
    // Task 6: Marker Listing Tests
    // ========================================================================

    #[tokio::test]
    async fn test_list_markers_empty() {
        let storage = Arc::new(MemoryBackend::new());
        let pointer_store = Arc::new(MockPointerStore);
        let gc = IdempotencyGarbageCollector::new(storage, pointer_store);

        let table_uuid = Uuid::new_v4();
        let entries = gc.list_markers(&table_uuid).await.expect("list");

        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn test_list_markers_finds_all_markers() {
        let storage = Arc::new(MemoryBackend::new());
        let table_uuid = Uuid::new_v4();

        // Create 3 markers with different statuses
        for i in 0..3 {
            let key = format!("key_{i}");
            let key_hash = IdempotencyMarker::hash_key(&key);
            let path = IdempotencyMarker::storage_path(&table_uuid, &key_hash);

            let marker = IdempotencyMarker::new_in_progress(
                key,
                table_uuid,
                format!("request_hash_{i}"),
                "base.json".to_string(),
                format!("new_{i}.json"),
            );

            let bytes = serde_json::to_vec(&marker).expect("serialize");
            storage
                .put(
                    &path,
                    Bytes::from(bytes),
                    arco_core::storage::WritePrecondition::None,
                )
                .await
                .expect("put");
        }

        let pointer_store = Arc::new(MockPointerStore);
        let gc = IdempotencyGarbageCollector::new(Arc::clone(&storage), pointer_store);

        let entries = gc.list_markers(&table_uuid).await.expect("list");

        assert_eq!(entries.len(), 3);
        // Verify ordering is deterministic (by path)
        for i in 0..2 {
            assert!(entries[i].path < entries[i + 1].path);
        }
    }

    #[tokio::test]
    async fn test_list_all_markers_groups_by_table() {
        let storage = Arc::new(MemoryBackend::new());
        let table1 = Uuid::new_v4();
        let table2 = Uuid::new_v4();

        // Create 2 markers for table1
        for i in 0..2 {
            let key = format!("key_t1_{i}");
            let key_hash = IdempotencyMarker::hash_key(&key);
            let path = IdempotencyMarker::storage_path(&table1, &key_hash);

            let marker = IdempotencyMarker::new_in_progress(
                key,
                table1,
                format!("hash_{i}"),
                "base.json".to_string(),
                format!("new_{i}.json"),
            );

            let bytes = serde_json::to_vec(&marker).expect("serialize");
            storage
                .put(
                    &path,
                    Bytes::from(bytes),
                    arco_core::storage::WritePrecondition::None,
                )
                .await
                .expect("put");
        }

        // Create 3 markers for table2
        for i in 0..3 {
            let key = format!("key_t2_{i}");
            let key_hash = IdempotencyMarker::hash_key(&key);
            let path = IdempotencyMarker::storage_path(&table2, &key_hash);

            let marker = IdempotencyMarker::new_in_progress(
                key,
                table2,
                format!("hash_{i}"),
                "base.json".to_string(),
                format!("new_{i}.json"),
            );

            let bytes = serde_json::to_vec(&marker).expect("serialize");
            storage
                .put(
                    &path,
                    Bytes::from(bytes),
                    arco_core::storage::WritePrecondition::None,
                )
                .await
                .expect("put");
        }

        let pointer_store = Arc::new(MockPointerStore);
        let gc = IdempotencyGarbageCollector::new(Arc::clone(&storage), pointer_store);

        let result = gc.list_all_markers().await.expect("list_all");

        assert_eq!(result.total_count, 5);
        assert_eq!(result.by_table.len(), 2);
        assert_eq!(
            result.by_table.get(&table1).map(|v| v.len()).unwrap_or(0),
            2
        );
        assert_eq!(
            result.by_table.get(&table2).map(|v| v.len()).unwrap_or(0),
            3
        );
    }

    #[tokio::test]
    async fn test_list_markers_ignores_non_json_files() {
        let storage = Arc::new(MemoryBackend::new());
        let table_uuid = Uuid::new_v4();

        // Create a valid marker
        let key = "valid_key";
        let key_hash = IdempotencyMarker::hash_key(key);
        let path = IdempotencyMarker::storage_path(&table_uuid, &key_hash);

        let marker = IdempotencyMarker::new_in_progress(
            key.to_string(),
            table_uuid,
            "hash".to_string(),
            "base.json".to_string(),
            "new.json".to_string(),
        );

        let bytes = serde_json::to_vec(&marker).expect("serialize");
        storage
            .put(
                &path,
                Bytes::from(bytes),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        // Create a non-JSON file in the same directory
        storage
            .put(
                &format!("_catalog/iceberg_idempotency/{table_uuid}/ab/some_file.txt"),
                Bytes::from("not a marker"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        let pointer_store = Arc::new(MockPointerStore);
        let gc = IdempotencyGarbageCollector::new(Arc::clone(&storage), pointer_store);

        let entries = gc.list_markers(&table_uuid).await.expect("list");

        // Should only find the JSON marker
        assert_eq!(entries.len(), 1);
        assert!(entries[0].path.ends_with(".json"));
    }

    #[tokio::test]
    async fn test_list_markers_handles_parse_errors_gracefully() {
        let storage = Arc::new(MemoryBackend::new());
        let table_uuid = Uuid::new_v4();

        // Create a valid marker
        let key = "valid_key";
        let key_hash = IdempotencyMarker::hash_key(key);
        let valid_path = IdempotencyMarker::storage_path(&table_uuid, &key_hash);

        let marker = IdempotencyMarker::new_in_progress(
            key.to_string(),
            table_uuid,
            "hash".to_string(),
            "base.json".to_string(),
            "new.json".to_string(),
        );

        let bytes = serde_json::to_vec(&marker).expect("serialize");
        storage
            .put(
                &valid_path,
                Bytes::from(bytes),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        // Create an invalid JSON file
        storage
            .put(
                &format!("_catalog/iceberg_idempotency/{table_uuid}/ba/invalid.json"),
                Bytes::from("not valid json"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        let pointer_store = Arc::new(MockPointerStore);
        let gc = IdempotencyGarbageCollector::new(Arc::clone(&storage), pointer_store);

        let entries = gc.list_markers(&table_uuid).await.expect("list");

        // Should only return the valid marker
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].marker.idempotency_key, "valid_key");

        let all_markers = gc.list_all_markers().await.expect("list_all");
        assert_eq!(all_markers.parse_errors, 1);
    }

    #[tokio::test]
    async fn test_list_all_markers_detects_path_mismatch() {
        let storage = Arc::new(MemoryBackend::new());
        let table_uuid = Uuid::new_v4();
        let other_table = Uuid::new_v4();

        let key = "valid_key";
        let key_hash = IdempotencyMarker::hash_key(key);
        let path = IdempotencyMarker::storage_path(&other_table, &key_hash);

        let marker = IdempotencyMarker::new_in_progress(
            key.to_string(),
            table_uuid,
            "hash".to_string(),
            "base.json".to_string(),
            "new.json".to_string(),
        );

        let bytes = serde_json::to_vec(&marker).expect("serialize");
        storage
            .put(
                &path,
                Bytes::from(bytes),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        let pointer_store = Arc::new(MockPointerStore);
        let gc = IdempotencyGarbageCollector::new(Arc::clone(&storage), pointer_store);

        let result = gc.list_all_markers().await.expect("list_all");
        assert_eq!(result.total_count, 0);
        assert_eq!(result.path_mismatches, 1);
    }

    // ========================================================================
    // GC Eligibility Tests (prerequisite for Task 7)
    // ========================================================================

    #[test]
    fn test_idempotency_gc_config_parses_lifetime() {
        let config = IdempotencyGcConfig::default().with_idempotency_key_lifetime(Some("PT2H"));
        assert_eq!(config.marker_lifetime, Duration::hours(2));

        let config = IdempotencyGcConfig::default().with_idempotency_key_lifetime(Some("P1DT1H"));
        assert_eq!(config.marker_lifetime, Duration::hours(25));

        let config = IdempotencyGcConfig::default().with_idempotency_key_lifetime(Some("P1M"));
        assert_eq!(config.marker_lifetime, Duration::hours(24));
    }

    #[test]
    fn test_committed_marker_eligible_after_lifetime_plus_grace() {
        let storage = Arc::new(MemoryBackend::new());
        let pointer_store = Arc::new(MockPointerStore);
        let gc = IdempotencyGarbageCollector::new(storage, pointer_store);

        let mut marker = IdempotencyMarker::new_in_progress(
            "key".to_string(),
            Uuid::new_v4(),
            "hash".to_string(),
            "base.json".to_string(),
            "new.json".to_string(),
        );
        marker = marker.finalize_committed("new.json".to_string());

        // Set committed_at to 49 hours ago (past 48h threshold = 24h lifetime + 24h grace)
        marker.committed_at = Some(Utc::now() - Duration::hours(49));

        // With default lifetime (24h) + grace (24h) = 48h, this should be eligible
        assert!(gc.is_eligible_for_gc(&marker, Utc::now()));

        // Set committed_at to 1 day ago - not yet eligible
        marker.committed_at = Some(Utc::now() - Duration::hours(24));
        assert!(!gc.is_eligible_for_gc(&marker, Utc::now()));
    }

    #[test]
    fn test_in_progress_marker_eligible_after_timeout_plus_grace() {
        let storage = Arc::new(MemoryBackend::new());
        let pointer_store = Arc::new(MockPointerStore);
        let gc = IdempotencyGarbageCollector::new(storage, pointer_store);

        let mut marker = IdempotencyMarker::new_in_progress(
            "key".to_string(),
            Uuid::new_v4(),
            "hash".to_string(),
            "base.json".to_string(),
            "new.json".to_string(),
        );

        // Set started_at to 25 hours ago (timeout 10min + grace 24h = 24h10m)
        marker.started_at = Utc::now() - Duration::hours(25);
        assert!(gc.is_eligible_for_gc(&marker, Utc::now()));

        // Set started_at to 1 hour ago - not yet eligible
        marker.started_at = Utc::now() - Duration::hours(1);
        assert!(!gc.is_eligible_for_gc(&marker, Utc::now()));
    }

    #[test]
    fn test_failed_marker_eligible_after_lifetime_plus_grace() {
        use crate::error::IcebergErrorResponse;

        let storage = Arc::new(MemoryBackend::new());
        let pointer_store = Arc::new(MockPointerStore);
        let gc = IdempotencyGarbageCollector::new(storage, pointer_store);

        let mut marker = IdempotencyMarker::new_in_progress(
            "key".to_string(),
            Uuid::new_v4(),
            "hash".to_string(),
            "base.json".to_string(),
            "new.json".to_string(),
        );

        let error = IcebergErrorResponse {
            error: crate::error::IcebergErrorDetail {
                message: "conflict".to_string(),
                error_type: "CommitFailedException".to_string(),
                code: 409,
            },
        };
        marker = marker.finalize_failed(409, error);

        // Set failed_at to 49 hours ago (past 48h threshold = 24h lifetime + 24h grace)
        marker.failed_at = Some(Utc::now() - Duration::hours(49));

        assert!(gc.is_eligible_for_gc(&marker, Utc::now()));
    }

    // ========================================================================
    // Task 7: GC Cleanup Tests
    // ========================================================================

    /// Helper to create an expired committed marker.
    async fn create_expired_committed_marker(
        storage: &MemoryBackend,
        table_uuid: Uuid,
        key: &str,
    ) -> String {
        let key_hash = IdempotencyMarker::hash_key(key);
        let path = IdempotencyMarker::storage_path(&table_uuid, &key_hash);

        let mut marker = IdempotencyMarker::new_in_progress(
            key.to_string(),
            table_uuid,
            "hash".to_string(),
            "base.json".to_string(),
            "new.json".to_string(),
        );
        marker = marker.finalize_committed("new.json".to_string());
        // Set committed_at to 3 days ago (past 48h expiry)
        marker.committed_at = Some(Utc::now() - Duration::hours(72));

        let bytes = serde_json::to_vec(&marker).expect("serialize");
        storage
            .put(
                &path,
                Bytes::from(bytes),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        path
    }

    /// Helper to create a fresh committed marker (not yet eligible for GC).
    async fn create_fresh_committed_marker(
        storage: &MemoryBackend,
        table_uuid: Uuid,
        key: &str,
    ) -> String {
        let key_hash = IdempotencyMarker::hash_key(key);
        let path = IdempotencyMarker::storage_path(&table_uuid, &key_hash);

        let mut marker = IdempotencyMarker::new_in_progress(
            key.to_string(),
            table_uuid,
            "hash".to_string(),
            "base.json".to_string(),
            "new.json".to_string(),
        );
        marker = marker.finalize_committed("new.json".to_string());
        // Leave committed_at at now (not expired)

        let bytes = serde_json::to_vec(&marker).expect("serialize");
        storage
            .put(
                &path,
                Bytes::from(bytes),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        path
    }

    #[tokio::test]
    async fn test_clean_table_deletes_expired_markers() {
        let storage = Arc::new(MemoryBackend::new());
        let table_uuid = Uuid::new_v4();

        // Create 2 expired markers
        let path1 = create_expired_committed_marker(&storage, table_uuid, "expired_1").await;
        let path2 = create_expired_committed_marker(&storage, table_uuid, "expired_2").await;

        // Verify they exist
        assert!(storage.head(&path1).await.expect("head").is_some());
        assert!(storage.head(&path2).await.expect("head").is_some());

        let pointer_store = Arc::new(MockPointerStore);
        let gc = IdempotencyGarbageCollector::new(Arc::clone(&storage), pointer_store);

        let result = gc.clean_table(&table_uuid).await.expect("clean_table");

        assert_eq!(result.deleted, 2);
        assert_eq!(result.skipped, 0);
        assert_eq!(result.failed, 0);

        // Verify markers are deleted
        assert!(storage.head(&path1).await.expect("head").is_none());
        assert!(storage.head(&path2).await.expect("head").is_none());
    }

    #[tokio::test]
    async fn test_clean_table_skips_fresh_markers() {
        let storage = Arc::new(MemoryBackend::new());
        let table_uuid = Uuid::new_v4();

        // Create 1 expired and 1 fresh marker
        let expired_path = create_expired_committed_marker(&storage, table_uuid, "expired").await;
        let fresh_path = create_fresh_committed_marker(&storage, table_uuid, "fresh").await;

        let pointer_store = Arc::new(MockPointerStore);
        let gc = IdempotencyGarbageCollector::new(Arc::clone(&storage), pointer_store);

        let result = gc.clean_table(&table_uuid).await.expect("clean_table");

        assert_eq!(result.deleted, 1);
        assert_eq!(result.skipped, 1);
        assert_eq!(result.failed, 0);

        // Expired marker is deleted, fresh marker remains
        assert!(storage.head(&expired_path).await.expect("head").is_none());
        assert!(storage.head(&fresh_path).await.expect("head").is_some());
    }

    #[tokio::test]
    async fn test_clean_all_handles_multiple_tables() {
        let storage = Arc::new(MemoryBackend::new());
        let table1 = Uuid::new_v4();
        let table2 = Uuid::new_v4();

        // Create expired markers in both tables
        create_expired_committed_marker(&storage, table1, "t1_expired").await;
        create_expired_committed_marker(&storage, table2, "t2_expired_1").await;
        create_expired_committed_marker(&storage, table2, "t2_expired_2").await;

        // Create fresh marker in table1
        create_fresh_committed_marker(&storage, table1, "t1_fresh").await;

        let pointer_store = Arc::new(MockPointerStore);
        let gc = IdempotencyGarbageCollector::new(Arc::clone(&storage), pointer_store);

        let report = gc.clean_all().await.expect("clean_all");

        assert_eq!(report.tables.len(), 2);
        assert_eq!(report.total_deleted, 3);
        assert_eq!(report.total_skipped, 1);
        assert_eq!(report.total_failed, 0);
    }

    #[tokio::test]
    async fn test_clean_in_progress_marker_with_pointer_not_found() {
        let storage = Arc::new(MemoryBackend::new());
        let table_uuid = Uuid::new_v4();

        // Create an expired in-progress marker
        let key = "in_progress_expired";
        let key_hash = IdempotencyMarker::hash_key(key);
        let path = IdempotencyMarker::storage_path(&table_uuid, &key_hash);

        let mut marker = IdempotencyMarker::new_in_progress(
            key.to_string(),
            table_uuid,
            "hash".to_string(),
            "base.json".to_string(),
            "new.json".to_string(),
        );
        // Set started_at to 2 days ago (past timeout + grace)
        marker.started_at = Utc::now() - Duration::hours(48);

        let bytes = serde_json::to_vec(&marker).expect("serialize");
        storage
            .put(
                &path,
                Bytes::from(bytes),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        // MockPointerStore returns None (pointer not found)
        let pointer_store = Arc::new(MockPointerStore);
        let gc = IdempotencyGarbageCollector::new(Arc::clone(&storage), pointer_store);

        let result = gc.clean_table(&table_uuid).await.expect("clean_table");

        // Should delete the marker since pointer is not found
        assert_eq!(result.deleted, 1);
        assert_eq!(result.skipped, 0);
        assert!(storage.head(&path).await.expect("head").is_none());
    }

    /// Mock PointerStore that returns a specific pointer.
    struct MockPointerStoreWithPointer {
        table_uuid: Uuid,
        metadata_location: String,
    }

    #[async_trait]
    impl PointerStore for MockPointerStoreWithPointer {
        async fn load(
            &self,
            table_uuid: &Uuid,
        ) -> IcebergResult<Option<(crate::pointer::IcebergTablePointer, ObjectVersion)>> {
            if *table_uuid == self.table_uuid {
                let pointer = crate::pointer::IcebergTablePointer::new(
                    self.table_uuid,
                    self.metadata_location.clone(),
                );
                Ok(Some((pointer, ObjectVersion::new("v1"))))
            } else {
                Ok(None)
            }
        }

        async fn create(
            &self,
            _table_uuid: &Uuid,
            _pointer: &crate::pointer::IcebergTablePointer,
        ) -> IcebergResult<crate::pointer::CasResult> {
            unimplemented!()
        }

        async fn compare_and_swap(
            &self,
            _table_uuid: &Uuid,
            _expected_version: &ObjectVersion,
            _new_pointer: &crate::pointer::IcebergTablePointer,
        ) -> IcebergResult<crate::pointer::CasResult> {
            unimplemented!()
        }

        async fn delete(
            &self,
            _table_uuid: &Uuid,
            _expected_version: &ObjectVersion,
        ) -> IcebergResult<crate::pointer::CasResult> {
            unimplemented!()
        }

        async fn list_all(&self) -> IcebergResult<Vec<Uuid>> {
            Ok(vec![self.table_uuid])
        }
    }

    #[tokio::test]
    async fn test_check_in_progress_action_safe_to_delete() {
        let storage = Arc::new(MemoryBackend::new());
        let table_uuid = Uuid::new_v4();

        let marker = IdempotencyMarker::new_in_progress(
            "key".to_string(),
            table_uuid,
            "hash".to_string(),
            "base.json".to_string(),
            "old_metadata.json".to_string(), // Different from pointer
        );

        // Pointer points to different metadata
        let pointer_store = Arc::new(MockPointerStoreWithPointer {
            table_uuid,
            metadata_location: "new_metadata.json".to_string(),
        });

        let gc = IdempotencyGarbageCollector::new(storage, pointer_store);
        let action = gc.check_in_progress_action(&marker).await.expect("check");

        assert_eq!(action, InProgressAction::SafeToDelete);
    }

    #[tokio::test]
    async fn test_check_in_progress_action_mark_failed() {
        let storage = Arc::new(MemoryBackend::new());
        let table_uuid = Uuid::new_v4();

        let marker = IdempotencyMarker::new_in_progress(
            "key".to_string(),
            table_uuid,
            "hash".to_string(),
            "base.json".to_string(),
            "same_metadata.json".to_string(), // Same as pointer
        );

        // Pointer points to same metadata as marker
        let pointer_store = Arc::new(MockPointerStoreWithPointer {
            table_uuid,
            metadata_location: "same_metadata.json".to_string(),
        });

        let gc = IdempotencyGarbageCollector::new(storage, pointer_store);
        let action = gc.check_in_progress_action(&marker).await.expect("check");

        assert_eq!(action, InProgressAction::MarkFailedThenDelete);
    }

    #[tokio::test]
    async fn test_clean_in_progress_with_pointer_moved() {
        let storage = Arc::new(MemoryBackend::new());
        let table_uuid = Uuid::new_v4();

        // Create an expired in-progress marker
        let key = "in_progress_expired";
        let key_hash = IdempotencyMarker::hash_key(key);
        let path = IdempotencyMarker::storage_path(&table_uuid, &key_hash);

        let mut marker = IdempotencyMarker::new_in_progress(
            key.to_string(),
            table_uuid,
            "hash".to_string(),
            "base.json".to_string(),
            "old_metadata.json".to_string(),
        );
        marker.started_at = Utc::now() - Duration::hours(48);

        let bytes = serde_json::to_vec(&marker).expect("serialize");
        storage
            .put(
                &path,
                Bytes::from(bytes),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        // Pointer has moved to different metadata
        let pointer_store = Arc::new(MockPointerStoreWithPointer {
            table_uuid,
            metadata_location: "new_metadata.json".to_string(),
        });

        let gc = IdempotencyGarbageCollector::new(Arc::clone(&storage), pointer_store);
        let result = gc.clean_table(&table_uuid).await.expect("clean_table");

        assert_eq!(result.deleted, 1);
        assert!(storage.head(&path).await.expect("head").is_none());
    }

    #[tokio::test]
    async fn test_clean_in_progress_marker_marks_failed_then_deletes() {
        let storage = Arc::new(MemoryBackend::new());
        let table_uuid = Uuid::new_v4();

        let key = "in_progress_stale";
        let key_hash = IdempotencyMarker::hash_key(key);
        let path = IdempotencyMarker::storage_path(&table_uuid, &key_hash);

        let mut marker = IdempotencyMarker::new_in_progress(
            key.to_string(),
            table_uuid,
            "hash".to_string(),
            "base.json".to_string(),
            "same_metadata.json".to_string(),
        );
        marker.started_at = Utc::now() - Duration::hours(48);

        let bytes = serde_json::to_vec(&marker).expect("serialize");
        storage
            .put(
                &path,
                Bytes::from(bytes),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        let pointer_store = Arc::new(MockPointerStoreWithPointer {
            table_uuid,
            metadata_location: "same_metadata.json".to_string(),
        });

        let gc = IdempotencyGarbageCollector::new(Arc::clone(&storage), pointer_store);
        let result = gc.clean_table(&table_uuid).await.expect("clean_table");

        assert_eq!(result.deleted, 1);
        assert_eq!(result.skipped, 0);
        assert_eq!(result.failed, 0);
        assert_eq!(result.marked_failed, 1);
        assert!(storage.head(&path).await.expect("head").is_none());
    }

    // ========================================================================
    // Task 8: Orphan Metadata Cleanup Tests
    // ========================================================================

    #[tokio::test]
    async fn test_find_orphans_empty_directory() {
        let storage = Arc::new(MemoryBackend::new());
        let cleaner = OrphanMetadataCleaner::new(Arc::clone(&storage));

        let allowlist = HashSet::new();
        let scan = cleaner
            .find_orphans("metadata/", &allowlist)
            .await
            .expect("find_orphans");

        assert!(scan.orphans.is_empty());
        assert_eq!(scan.files_scanned, 0);
    }

    #[tokio::test]
    async fn test_find_orphans_all_in_allowlist() {
        let storage = Arc::new(MemoryBackend::new());

        // Create metadata files
        storage
            .put(
                "metadata/v1.metadata.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");
        storage
            .put(
                "metadata/v2.metadata.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        let cleaner = OrphanMetadataCleaner::new(Arc::clone(&storage));

        let mut allowlist = HashSet::new();
        allowlist.insert("metadata/v1.metadata.json".to_string());
        allowlist.insert("metadata/v2.metadata.json".to_string());

        let scan = cleaner
            .find_orphans("metadata/", &allowlist)
            .await
            .expect("find_orphans");

        assert!(scan.orphans.is_empty());
        assert_eq!(scan.files_scanned, 2);
    }

    #[tokio::test]
    async fn test_find_orphans_detects_orphan() {
        let storage = Arc::new(MemoryBackend::new());

        // Create metadata files
        storage
            .put(
                "metadata/v1.metadata.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");
        storage
            .put(
                "metadata/orphan.metadata.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        let cleaner = OrphanMetadataCleaner::new(Arc::clone(&storage));

        // Only v1 is in the allowlist
        let mut allowlist = HashSet::new();
        allowlist.insert("metadata/v1.metadata.json".to_string());

        let scan = cleaner
            .find_orphans("metadata/", &allowlist)
            .await
            .expect("find_orphans");

        assert_eq!(scan.orphans.len(), 1);
        assert_eq!(scan.orphans[0].path, "metadata/orphan.metadata.json");
    }

    #[tokio::test]
    async fn test_find_orphans_ignores_non_metadata_files() {
        let storage = Arc::new(MemoryBackend::new());

        // Create metadata file and non-metadata file
        storage
            .put(
                "metadata/v1.metadata.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");
        storage
            .put(
                "metadata/manifest.avro",
                Bytes::from("avro data"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        let cleaner = OrphanMetadataCleaner::new(Arc::clone(&storage));

        // Only v1 is in allowlist
        let mut allowlist = HashSet::new();
        allowlist.insert("metadata/v1.metadata.json".to_string());

        let scan = cleaner
            .find_orphans("metadata/", &allowlist)
            .await
            .expect("find_orphans");

        // The avro file should not be counted as an orphan
        assert!(scan.orphans.is_empty());
        assert_eq!(scan.files_scanned, 2);
    }

    #[tokio::test]
    async fn test_clean_orphans_deletes_old_orphans() {
        let storage = Arc::new(MemoryBackend::new());
        let table_uuid = Uuid::new_v4();

        // Create a valid metadata file
        storage
            .put(
                "metadata/v1.metadata.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        // Create an orphan (it will have a recent timestamp from MemoryBackend)
        storage
            .put(
                "metadata/orphan.metadata.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        // Use a config with no minimum age (delete immediately)
        let config = OrphanGcConfig {
            min_age: Duration::zero(),
        };
        let cleaner = OrphanMetadataCleaner::with_config(Arc::clone(&storage), config);

        let mut allowlist = HashSet::new();
        allowlist.insert("metadata/v1.metadata.json".to_string());

        let result = cleaner
            .clean_orphans(table_uuid, "metadata/", &allowlist)
            .await
            .expect("clean_orphans");

        assert_eq!(result.orphans_found, 1);
        assert_eq!(result.orphans_deleted, 1);
        assert_eq!(result.orphans_skipped, 0);

        // Verify orphan is deleted but valid file remains
        assert!(
            storage
                .head("metadata/v1.metadata.json")
                .await
                .expect("head")
                .is_some()
        );
        assert!(
            storage
                .head("metadata/orphan.metadata.json")
                .await
                .expect("head")
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_clean_orphans_skips_recent_orphans() {
        let storage = Arc::new(MemoryBackend::new());
        let table_uuid = Uuid::new_v4();

        // Create an orphan (it will have a recent timestamp)
        storage
            .put(
                "metadata/orphan.metadata.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        // Use default config which requires 24h minimum age
        let cleaner = OrphanMetadataCleaner::new(Arc::clone(&storage));

        let allowlist = HashSet::new();

        let result = cleaner
            .clean_orphans(table_uuid, "metadata/", &allowlist)
            .await
            .expect("clean_orphans");

        // Should find the orphan but skip it (too recent)
        assert_eq!(result.orphans_found, 1);
        assert_eq!(result.orphans_deleted, 0);
        assert_eq!(result.orphans_skipped, 1);

        // Orphan should still exist
        assert!(
            storage
                .head("metadata/orphan.metadata.json")
                .await
                .expect("head")
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_clean_orphans_multiple_files() {
        let storage = Arc::new(MemoryBackend::new());
        let table_uuid = Uuid::new_v4();

        // Create valid and orphan files
        for i in 1..=3 {
            storage
                .put(
                    &format!("metadata/v{i}.metadata.json"),
                    Bytes::from("{}"),
                    arco_core::storage::WritePrecondition::None,
                )
                .await
                .expect("put");
        }
        for i in 1..=2 {
            storage
                .put(
                    &format!("metadata/orphan{i}.metadata.json"),
                    Bytes::from("{}"),
                    arco_core::storage::WritePrecondition::None,
                )
                .await
                .expect("put");
        }

        let config = OrphanGcConfig {
            min_age: Duration::zero(),
        };
        let cleaner = OrphanMetadataCleaner::with_config(Arc::clone(&storage), config);

        // Only v1-v3 are in allowlist
        let mut allowlist = HashSet::new();
        for i in 1..=3 {
            allowlist.insert(format!("metadata/v{i}.metadata.json"));
        }

        let result = cleaner
            .clean_orphans(table_uuid, "metadata/", &allowlist)
            .await
            .expect("clean_orphans");

        assert_eq!(result.orphans_found, 2);
        assert_eq!(result.orphans_deleted, 2);
        assert_eq!(result.files_scanned, 5);

        // Valid files remain
        for i in 1..=3 {
            assert!(
                storage
                    .head(&format!("metadata/v{i}.metadata.json"))
                    .await
                    .expect("head")
                    .is_some()
            );
        }
        // Orphans are deleted
        for i in 1..=2 {
            assert!(
                storage
                    .head(&format!("metadata/orphan{i}.metadata.json"))
                    .await
                    .expect("head")
                    .is_none()
            );
        }
    }

    // ========================================================================
    // Task 8.1: Metadata GC Planner Tests
    // ========================================================================

    fn metadata_json(table_uuid: Uuid, metadata_log: &str, properties: &str) -> String {
        format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "{table_uuid}",
                "location": "gs://bucket/warehouse/table",
                "last-sequence-number": 3,
                "last-updated-ms": 1704067200000,
                "last-column-id": 3,
                "current-schema-id": 0,
                "schemas": [],
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": {metadata_log},
                "properties": {properties},
                "default-spec-id": 0,
                "partition-specs": [],
                "last-partition-id": 0,
                "default-sort-order-id": 0,
                "sort-orders": []
            }}"#
        )
    }

    #[test]
    fn test_metadata_retention_policy_parses_properties() {
        let table_uuid = Uuid::new_v4();
        let json = metadata_json(
            table_uuid,
            "[]",
            r#"{"metadata.previous-versions-max":"5","metadata.delete-after-commit":"true"}"#,
        );
        let metadata: TableMetadata = serde_json::from_str(&json).expect("parse metadata");

        let policy = MetadataRetentionPolicy::from_metadata(&metadata);
        assert_eq!(policy.previous_versions_max, 5);
        assert!(policy.delete_after_commit);

        let json = metadata_json(
            table_uuid,
            "[]",
            r#"{"metadata.previous-versions-max":"nope","metadata.delete-after-commit":"maybe"}"#,
        );
        let metadata: TableMetadata = serde_json::from_str(&json).expect("parse metadata");
        let policy = MetadataRetentionPolicy::from_metadata(&metadata);
        assert_eq!(policy.previous_versions_max, DEFAULT_PREVIOUS_VERSIONS_MAX);
        assert!(!policy.delete_after_commit);
    }

    #[tokio::test]
    async fn test_metadata_gc_plan_respects_previous_versions_max() {
        use crate::pointer::PointerStoreImpl;

        let storage = Arc::new(MemoryBackend::new());
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));

        let table_uuid = Uuid::new_v4();
        let current_location = "warehouse/table/metadata/v3.metadata.json";

        let metadata_log = r#"[
            {"metadata-file": "warehouse/table/metadata/v1.metadata.json", "timestamp-ms": 1},
            {"metadata-file": "warehouse/table/metadata/v2.metadata.json", "timestamp-ms": 2}
        ]"#;
        let properties =
            r#"{"metadata.previous-versions-max":"1","metadata.delete-after-commit":"true"}"#;
        let json = metadata_json(table_uuid, metadata_log, properties);

        storage
            .put(
                current_location,
                Bytes::from(json),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put metadata");

        let pointer =
            crate::pointer::IcebergTablePointer::new(table_uuid, current_location.to_string());
        pointer_store
            .create(&table_uuid, &pointer)
            .await
            .expect("create pointer");

        let planner = MetadataGcPlanner::new(storage, pointer_store);
        let plan = planner
            .plan_for_table(&table_uuid, "acme", "prod")
            .await
            .expect("plan");

        assert!(plan.allowlist.contains(current_location));
        assert!(
            plan.allowlist
                .contains("warehouse/table/metadata/v2.metadata.json")
        );
        assert!(
            !plan
                .allowlist
                .contains("warehouse/table/metadata/v1.metadata.json")
        );
        assert_eq!(plan.metadata_prefix, "warehouse/table/metadata/");
    }

    #[tokio::test]
    async fn test_metadata_gc_clean_table_skips_when_delete_after_commit_false() {
        use crate::pointer::PointerStoreImpl;

        let storage = Arc::new(MemoryBackend::new());
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));

        let table_uuid = Uuid::new_v4();
        let current_location = "warehouse/table/metadata/v1.metadata.json";
        let properties = r#"{"metadata.delete-after-commit":"false"}"#;
        let json = metadata_json(table_uuid, "[]", properties);

        storage
            .put(
                current_location,
                Bytes::from(json),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put metadata");

        let pointer =
            crate::pointer::IcebergTablePointer::new(table_uuid, current_location.to_string());
        pointer_store
            .create(&table_uuid, &pointer)
            .await
            .expect("create pointer");

        let planner = MetadataGcPlanner::new(storage, pointer_store);
        let result = planner
            .clean_table(&table_uuid, "acme", "prod")
            .await
            .expect("clean table");

        assert!(result.skipped_due_to_policy);
        assert!(result.orphan_result.is_none());
        assert!(!result.delete_after_commit);
    }

    #[tokio::test]
    async fn test_metadata_gc_clean_table_deletes_orphans() {
        use crate::pointer::PointerStoreImpl;

        let storage = Arc::new(MemoryBackend::new());
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));

        let table_uuid = Uuid::new_v4();
        let current_location = "warehouse/table/metadata/v2.metadata.json";
        let metadata_log = r#"[
            {"metadata-file": "warehouse/table/metadata/v1.metadata.json", "timestamp-ms": 1}
        ]"#;
        let properties = r#"{"metadata.delete-after-commit":"true"}"#;
        let json = metadata_json(table_uuid, metadata_log, properties);

        storage
            .put(
                current_location,
                Bytes::from(json),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put metadata");
        storage
            .put(
                "warehouse/table/metadata/v1.metadata.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put v1");
        storage
            .put(
                "warehouse/table/metadata/orphan.metadata.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put orphan");

        let pointer =
            crate::pointer::IcebergTablePointer::new(table_uuid, current_location.to_string());
        pointer_store
            .create(&table_uuid, &pointer)
            .await
            .expect("create pointer");

        let config = OrphanGcConfig {
            min_age: Duration::zero(),
        };
        let planner = MetadataGcPlanner::with_config(Arc::clone(&storage), pointer_store, config);
        let result = planner
            .clean_table(&table_uuid, "acme", "prod")
            .await
            .expect("clean table");

        let orphan_result = result.orphan_result.expect("orphan result");
        assert_eq!(orphan_result.orphans_deleted, 1);
        assert!(
            storage
                .head("warehouse/table/metadata/orphan.metadata.json")
                .await
                .expect("head")
                .is_none()
        );
        assert!(
            storage
                .head(current_location)
                .await
                .expect("head")
                .is_some()
        );
    }

    // ========================================================================
    // Task 9: Event Receipt GC Tests
    // ========================================================================

    #[tokio::test]
    async fn test_event_receipt_gc_config_defaults() {
        let config = EventReceiptGcConfig::default();
        assert_eq!(config.pending_retention, Duration::hours(24));
        assert_eq!(config.committed_retention, Duration::days(90));
    }

    #[tokio::test]
    async fn test_clean_pending_receipts_empty() {
        let storage = Arc::new(MemoryBackend::new());
        let gc = EventReceiptGarbageCollector::new(Arc::clone(&storage));

        let date = chrono::NaiveDate::from_ymd_opt(2025, 1, 15).expect("valid date");
        let result = gc.clean_pending_receipts(date).await.expect("clean");

        assert_eq!(result.deleted, 0);
        assert_eq!(result.skipped, 0);
        assert_eq!(result.files_scanned, 0);
    }

    #[tokio::test]
    async fn test_clean_pending_receipts_skips_recent() {
        let storage = Arc::new(MemoryBackend::new());

        // Create a pending receipt (will have recent timestamp)
        storage
            .put(
                "events/2025-01-15/iceberg/pending/abc123.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        let gc = EventReceiptGarbageCollector::new(Arc::clone(&storage));
        let date = chrono::NaiveDate::from_ymd_opt(2025, 1, 15).expect("valid date");
        let result = gc.clean_pending_receipts(date).await.expect("clean");

        // Should skip (too recent)
        assert_eq!(result.deleted, 0);
        assert_eq!(result.skipped, 1);
        assert_eq!(result.files_scanned, 1);

        // File should still exist
        assert!(
            storage
                .head("events/2025-01-15/iceberg/pending/abc123.json")
                .await
                .expect("head")
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_clean_pending_receipts_deletes_old() {
        let storage = Arc::new(MemoryBackend::new());

        // Create a pending receipt
        storage
            .put(
                "events/2025-01-15/iceberg/pending/abc123.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        // Use zero retention for testing
        let config = EventReceiptGcConfig {
            pending_retention: Duration::zero(),
            committed_retention: Duration::zero(),
        };
        let gc = EventReceiptGarbageCollector::with_config(Arc::clone(&storage), config);

        let date = chrono::NaiveDate::from_ymd_opt(2025, 1, 15).expect("valid date");
        let result = gc.clean_pending_receipts(date).await.expect("clean");

        assert_eq!(result.deleted, 1);
        assert_eq!(result.skipped, 0);

        // File should be deleted
        assert!(
            storage
                .head("events/2025-01-15/iceberg/pending/abc123.json")
                .await
                .expect("head")
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_clean_committed_receipts_respects_90_day_retention() {
        let storage = Arc::new(MemoryBackend::new());

        // Create committed receipts
        storage
            .put(
                "events/2025-01-15/iceberg/committed/abc123.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        let gc = EventReceiptGarbageCollector::new(Arc::clone(&storage));
        let date = chrono::NaiveDate::from_ymd_opt(2025, 1, 15).expect("valid date");
        let result = gc.clean_committed_receipts(date).await.expect("clean");

        // Should skip (too recent, less than 90 days)
        assert_eq!(result.deleted, 0);
        assert_eq!(result.skipped, 1);
    }

    #[tokio::test]
    async fn test_clean_receipts_for_date_combines_results() {
        let storage = Arc::new(MemoryBackend::new());

        // Create pending and committed receipts
        storage
            .put(
                "events/2025-01-15/iceberg/pending/pending1.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");
        storage
            .put(
                "events/2025-01-15/iceberg/committed/committed1.json",
                Bytes::from("{}"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        // Zero retention deletes both
        let config = EventReceiptGcConfig {
            pending_retention: Duration::zero(),
            committed_retention: Duration::zero(),
        };
        let gc = EventReceiptGarbageCollector::with_config(Arc::clone(&storage), config);

        let date = chrono::NaiveDate::from_ymd_opt(2025, 1, 15).expect("valid date");
        let result = gc.clean_receipts_for_date(date).await.expect("clean");

        assert_eq!(result.pending_deleted, 1);
        assert_eq!(result.committed_deleted, 1);
        assert_eq!(result.files_scanned, 2);
    }

    #[tokio::test]
    async fn test_clean_receipts_for_date_range() {
        let storage = Arc::new(MemoryBackend::new());

        // Create receipts for multiple days
        for day in 15..=17 {
            storage
                .put(
                    &format!("events/2025-01-{day:02}/iceberg/pending/receipt.json"),
                    Bytes::from("{}"),
                    arco_core::storage::WritePrecondition::None,
                )
                .await
                .expect("put");
        }

        let config = EventReceiptGcConfig {
            pending_retention: Duration::zero(),
            committed_retention: Duration::zero(),
        };
        let gc = EventReceiptGarbageCollector::with_config(Arc::clone(&storage), config);

        let start = chrono::NaiveDate::from_ymd_opt(2025, 1, 15).expect("valid date");
        let end = chrono::NaiveDate::from_ymd_opt(2025, 1, 17).expect("valid date");
        let result = gc
            .clean_receipts_for_date_range(start, end)
            .await
            .expect("clean");

        assert_eq!(result.pending_deleted, 3);
        assert_eq!(result.files_scanned, 3);
    }

    #[tokio::test]
    async fn test_event_receipt_gc_ignores_non_json() {
        let storage = Arc::new(MemoryBackend::new());

        // Create non-JSON file
        storage
            .put(
                "events/2025-01-15/iceberg/pending/readme.txt",
                Bytes::from("text"),
                arco_core::storage::WritePrecondition::None,
            )
            .await
            .expect("put");

        let config = EventReceiptGcConfig {
            pending_retention: Duration::zero(),
            committed_retention: Duration::zero(),
        };
        let gc = EventReceiptGarbageCollector::with_config(Arc::clone(&storage), config);

        let date = chrono::NaiveDate::from_ymd_opt(2025, 1, 15).expect("valid date");
        let result = gc.clean_pending_receipts(date).await.expect("clean");

        // Should scan but not delete (not a JSON file)
        assert_eq!(result.deleted, 0);
        assert_eq!(result.skipped, 0);
        assert_eq!(result.files_scanned, 1);

        // File should still exist
        assert!(
            storage
                .head("events/2025-01-15/iceberg/pending/readme.txt")
                .await
                .expect("head")
                .is_some()
        );
    }
}
