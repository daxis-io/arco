//! Garbage collector implementation.

use std::collections::HashSet;
use std::future::Future;
use std::time::Instant;

use chrono::{DateTime, Duration, Utc};

use arco_core::scoped_storage::ScopedStorage;
use arco_core::{CatalogDomain, CatalogPaths};

use crate::error::{CatalogError, Result};
use crate::gc::RetentionPolicy;
use crate::manifest::{
    CatalogDomainManifest, DomainManifestPointer, ExecutionsManifest, LineageManifest,
    RootManifest, SearchManifest,
};

// =========================================================================
// Metrics (emitted via structured logging)
//
// These metrics follow the Prometheus naming conventions and can be
// scraped from logs using a log-to-metrics pipeline.
//
// Counters:
// - arco_gc_objects_deleted_total{phase}
// - arco_gc_bytes_reclaimed_total{phase}
// - arco_gc_errors_total{phase}
//
// Histograms:
// - arco_gc_run_duration_seconds{phase}
// =========================================================================

/// Result of a garbage collection run.
#[derive(Debug, Clone, Default)]
pub struct GcResult {
    /// Number of objects deleted.
    pub objects_deleted: u64,
    /// Total bytes reclaimed.
    pub bytes_reclaimed: u64,
    /// Number of orphaned snapshots deleted.
    pub orphaned_snapshots_deleted: u64,
    /// Number of old ledger events deleted.
    pub ledger_events_deleted: u64,
    /// Number of old snapshot versions deleted.
    pub old_snapshots_deleted: u64,
    /// Errors encountered (GC continues on non-fatal errors).
    pub errors: Vec<String>,
}

impl GcResult {
    /// Merges another result into this one.
    pub fn merge(&mut self, other: Self) {
        self.objects_deleted += other.objects_deleted;
        self.bytes_reclaimed += other.bytes_reclaimed;
        self.orphaned_snapshots_deleted += other.orphaned_snapshots_deleted;
        self.ledger_events_deleted += other.ledger_events_deleted;
        self.old_snapshots_deleted += other.old_snapshots_deleted;
        self.errors.extend(other.errors);
    }

    /// Returns true if any errors were encountered.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

/// Dry-run report showing what would be collected.
#[derive(Debug, Clone, Default)]
pub struct GcReport {
    /// Total objects that would be deleted.
    pub objects_to_delete: u64,
    /// Estimated bytes to reclaim.
    pub bytes_to_reclaim: u64,
    /// Orphaned snapshots found.
    pub orphaned_snapshots: Vec<String>,
    /// Old ledger events found.
    pub old_ledger_events: Vec<String>,
    /// Old snapshot versions found.
    pub old_snapshot_versions: Vec<String>,
}

/// Garbage collector for catalog artifacts.
///
/// Collects:
/// - Orphaned snapshots: Not referenced by any manifest
/// - Old ledger events: Compacted beyond retention window
/// - Old snapshot versions: Beyond retention count
///
/// # Example
///
/// ```rust,ignore
/// let collector = GarbageCollector::new(storage, RetentionPolicy::default());
///
/// // Dry run first
/// let report = collector.collect_dry_run().await?;
///
/// // Actually collect
/// let result = collector.collect().await?;
/// ```
pub struct GarbageCollector {
    storage: ScopedStorage,
    policy: RetentionPolicy,
}

impl GarbageCollector {
    /// Creates a new garbage collector.
    #[must_use]
    pub fn new(storage: ScopedStorage, policy: RetentionPolicy) -> Self {
        Self { storage, policy }
    }

    /// Runs garbage collection (dry run - no deletions).
    ///
    /// Returns a report showing what would be deleted without actually deleting.
    ///
    /// # Errors
    ///
    /// Returns an error if listing catalog artifacts or reading manifests fails.
    pub async fn collect_dry_run(&self) -> Result<GcReport> {
        let mut report = GcReport::default();

        // 1. Find orphaned snapshots
        let orphaned = self.find_orphaned_snapshots().await?;
        report.orphaned_snapshots = orphaned;
        report.objects_to_delete += report.orphaned_snapshots.len() as u64;

        // 2. Find old ledger events
        let old_events = self.find_old_ledger_events().await?;
        report.old_ledger_events = old_events;
        report.objects_to_delete += report.old_ledger_events.len() as u64;

        // 3. Find old snapshot versions
        let old_versions = self.find_old_snapshot_versions().await?;
        report.old_snapshot_versions = old_versions;
        report.objects_to_delete += report.old_snapshot_versions.len() as u64;

        Ok(report)
    }

    /// Runs garbage collection (actually deletes artifacts).
    ///
    /// # Errors
    ///
    /// Returns an error if critical operations fail. Non-fatal errors are
    /// collected in the result's `errors` field without aborting the run.
    pub async fn collect(&self) -> Result<GcResult> {
        let start = Instant::now();
        let mut result = GcResult::default();

        tracing::info!(
            tenant = %self.storage.tenant_id(),
            workspace = %self.storage.workspace_id(),
            keep_snapshots = self.policy.keep_snapshots,
            delay_hours = self.policy.delay_hours,
            ledger_retention_hours = self.policy.ledger_retention_hours,
            max_age_days = self.policy.max_age_days,
            metric = "arco_gc_run_started",
            "starting garbage collection"
        );

        self.run_phase(
            "orphaned_snapshots",
            "orphaned snapshots",
            || self.gc_orphaned_snapshots(),
            &mut result,
        )
        .await;
        self.run_phase(
            "compacted_ledger",
            "compacted ledger",
            || self.gc_compacted_ledger(),
            &mut result,
        )
        .await;
        self.run_phase(
            "old_snapshots",
            "old snapshots",
            || self.gc_old_snapshots(),
            &mut result,
        )
        .await;

        let total_duration_secs = start.elapsed().as_secs_f64();
        tracing::info!(
            tenant = %self.storage.tenant_id(),
            workspace = %self.storage.workspace_id(),
            objects_deleted = result.objects_deleted,
            bytes_reclaimed = result.bytes_reclaimed,
            orphaned_snapshots_deleted = result.orphaned_snapshots_deleted,
            ledger_events_deleted = result.ledger_events_deleted,
            old_snapshots_deleted = result.old_snapshots_deleted,
            errors_count = result.errors.len(),
            duration_secs = total_duration_secs,
            metric = "arco_gc_run_completed",
            "garbage collection completed"
        );

        Ok(result)
    }

    async fn run_phase<F, Fut>(
        &self,
        phase: &'static str,
        error_context: &'static str,
        f: F,
        result: &mut GcResult,
    ) where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<GcResult>>,
    {
        let phase_start = Instant::now();
        match f().await {
            Ok(phase_result) => {
                let duration_secs = phase_start.elapsed().as_secs_f64();
                tracing::info!(
                    phase,
                    objects_deleted = phase_result.objects_deleted,
                    bytes_reclaimed = phase_result.bytes_reclaimed,
                    duration_secs,
                    metric = "arco_gc_phase_completed",
                    "GC phase completed"
                );

                // Record metrics
                crate::metrics::record_gc_completion(
                    phase,
                    phase_result.objects_deleted,
                    phase_result.bytes_reclaimed,
                    duration_secs,
                );

                result.merge(phase_result);
            }
            Err(e) => {
                tracing::error!(
                    phase,
                    error = %e,
                    metric = "arco_gc_errors_total",
                    "GC phase failed"
                );

                // Record error metric
                crate::metrics::record_gc_error(phase);

                result.errors.push(format!("{error_context}: {e}"));
            }
        }
    }

    // =========================================================================
    // Phase 1: Orphaned Snapshots
    // =========================================================================

    /// Finds snapshot directories not referenced by any manifest.
    async fn find_orphaned_snapshots(&self) -> Result<Vec<String>> {
        let referenced = self.get_referenced_snapshots().await?;
        let mut orphaned = Vec::new();

        // Check each domain's snapshot directory
        for domain in CatalogDomain::all() {
            let prefix = format!("snapshots/{}/", domain.as_str());
            let entries = self
                .storage
                .list(&prefix)
                .await
                .map_err(|e| CatalogError::Storage {
                    message: format!("failed to list snapshots for {domain}: {e}"),
                })?;

            for entry in entries {
                let path = entry.as_str();
                // Extract the version directory (e.g., "snapshots/catalog/v1/")
                if let Some(version_dir) = extract_snapshot_version_dir(path) {
                    if !referenced.contains(&version_dir) {
                        orphaned.push(version_dir);
                    }
                }
            }
        }

        // Dedupe (multiple files in same dir)
        orphaned.sort();
        orphaned.dedup();

        Ok(orphaned)
    }

    /// Deletes orphaned snapshot directories.
    async fn gc_orphaned_snapshots(&self) -> Result<GcResult> {
        let mut result = GcResult::default();
        let orphaned = self.find_orphaned_snapshots().await?;
        let cutoff = Utc::now() - Duration::hours(i64::from(self.policy.delay_hours));

        for dir in orphaned {
            // Check age before deletion
            if let Ok(Some(meta)) = self.storage.head_raw(&dir).await {
                if let Some(last_modified) = meta.last_modified {
                    if last_modified >= cutoff {
                        tracing::debug!(
                            path = %dir,
                            last_modified = %last_modified,
                            "skipping orphan (too recent)"
                        );
                        continue;
                    }
                }

                // Delete all files under the directory
                match self.delete_prefix(&dir).await {
                    Ok((count, bytes)) => {
                        tracing::info!(path = %dir, count, bytes, "deleted orphaned snapshot");
                        result.objects_deleted += count;
                        result.bytes_reclaimed += bytes;
                        result.orphaned_snapshots_deleted += 1;
                    }
                    Err(e) => {
                        result.errors.push(format!("delete {dir}: {e}"));
                    }
                }
            }
        }

        Ok(result)
    }

    // =========================================================================
    // Phase 2: Compacted Ledger Events
    // =========================================================================

    /// Finds ledger events older than the retention window.
    async fn find_old_ledger_events(&self) -> Result<Vec<String>> {
        let mut old_events = Vec::new();

        for domain in CatalogDomain::all() {
            // Read watermark for this domain
            let watermark_ts = self.get_domain_watermark_timestamp(*domain).await?;

            if let Some(watermark_ts) = watermark_ts {
                let cutoff =
                    watermark_ts - Duration::hours(i64::from(self.policy.ledger_retention_hours));

                let prefix = CatalogPaths::ledger_dir(*domain);
                let entries =
                    self.storage
                        .list_meta(&prefix)
                        .await
                        .map_err(|e| CatalogError::Storage {
                            message: format!("failed to list ledger for {domain}: {e}"),
                        })?;

                let objects = u64::try_from(entries.len()).unwrap_or(u64::MAX);
                let bytes: u64 = entries.iter().map(|meta| meta.size).sum();
                crate::metrics::record_storage_inventory(
                    &format!("ledger/{}", domain.as_str()),
                    objects,
                    bytes,
                );

                for meta in entries {
                    if let Some(last_modified) = meta.last_modified {
                        if last_modified < cutoff {
                            old_events.push(meta.path.to_string());
                        }
                    }
                }
            }
        }

        Ok(old_events)
    }

    /// Deletes ledger events older than the retention window.
    async fn gc_compacted_ledger(&self) -> Result<GcResult> {
        let mut result = GcResult::default();
        let old_events = self.find_old_ledger_events().await?;

        for path in old_events {
            match self.storage.delete(&path).await {
                Ok(()) => {
                    result.objects_deleted += 1;
                    result.ledger_events_deleted += 1;
                }
                Err(e) => {
                    result.errors.push(format!("delete {path}: {e}"));
                }
            }
        }

        Ok(result)
    }

    // =========================================================================
    // Phase 3: Old Snapshot Versions
    // =========================================================================

    /// Finds snapshot versions beyond the retention count.
    async fn find_old_snapshot_versions(&self) -> Result<Vec<String>> {
        let mut old_versions = Vec::new();
        let cutoff = Utc::now() - Duration::hours(i64::from(self.policy.delay_hours));
        let referenced = self.get_referenced_snapshots().await?;

        for domain in CatalogDomain::all() {
            let prefix = format!("snapshots/{}/", domain.as_str());
            let entries =
                self.storage
                    .list_meta(&prefix)
                    .await
                    .map_err(|e| CatalogError::Storage {
                        message: format!("failed to list snapshots for {domain}: {e}"),
                    })?;

            let objects = u64::try_from(entries.len()).unwrap_or(u64::MAX);
            let bytes: u64 = entries.iter().map(|meta| meta.size).sum();
            crate::metrics::record_storage_inventory(
                &format!("snapshots/{}", domain.as_str()),
                objects,
                bytes,
            );

            // Group by version directory and get max timestamp for each
            let mut version_dirs: Vec<(String, u64, DateTime<Utc>)> = Vec::new();

            for meta in entries {
                let path = meta.path.to_string();
                if let Some(version_dir) = extract_snapshot_version_dir(&path) {
                    if let Some(version) = extract_version_number(&version_dir) {
                        let ts = meta.last_modified.unwrap_or_else(Utc::now);

                        // Update or insert
                        if let Some(existing) =
                            version_dirs.iter_mut().find(|(d, _, _)| *d == version_dir)
                        {
                            if ts > existing.2 {
                                existing.2 = ts;
                            }
                        } else {
                            version_dirs.push((version_dir, version, ts));
                        }
                    }
                }
            }

            // Sort by version descending
            version_dirs.sort_by(|a, b| b.1.cmp(&a.1));

            // Skip the first N (keep_snapshots), mark the rest for deletion if old enough
            for (dir, _version, last_modified) in version_dirs
                .into_iter()
                .skip(self.policy.keep_snapshots as usize)
            {
                if referenced.contains(&dir) {
                    continue;
                }
                if last_modified < cutoff {
                    old_versions.push(dir);
                }
            }
        }

        Ok(old_versions)
    }

    /// Deletes snapshot versions beyond the retention count.
    async fn gc_old_snapshots(&self) -> Result<GcResult> {
        let mut result = GcResult::default();
        let old_versions = self.find_old_snapshot_versions().await?;

        for dir in old_versions {
            match self.delete_prefix(&dir).await {
                Ok((count, bytes)) => {
                    tracing::info!(path = %dir, count, bytes, "deleted old snapshot version");
                    result.objects_deleted += count;
                    result.bytes_reclaimed += bytes;
                    result.old_snapshots_deleted += 1;
                }
                Err(e) => {
                    result.errors.push(format!("delete {dir}: {e}"));
                }
            }
        }

        Ok(result)
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /// Gets all snapshot paths referenced by current manifests.
    async fn get_referenced_snapshots(&self) -> Result<HashSet<String>> {
        let mut referenced = HashSet::new();

        // Read root manifest to find domain manifests
        let root = self.read_root_manifest().await?;
        let catalog_manifest_path = self
            .resolve_domain_manifest_path(CatalogDomain::Catalog, &root.catalog_manifest_path)
            .await?;
        let lineage_manifest_path = self
            .resolve_domain_manifest_path(CatalogDomain::Lineage, &root.lineage_manifest_path)
            .await?;
        let executions_manifest_path = self
            .resolve_domain_manifest_path(CatalogDomain::Executions, &root.executions_manifest_path)
            .await?;
        let search_manifest_path = self
            .resolve_domain_manifest_path(CatalogDomain::Search, &root.search_manifest_path)
            .await?;

        // Catalog domain
        if let Ok(manifest) = self.read_catalog_manifest(&catalog_manifest_path).await {
            if !manifest.snapshot_path.is_empty() {
                referenced.insert(normalize_directory_path(&manifest.snapshot_path));
            }
        }

        // Lineage domain
        if let Ok(manifest) = self.read_lineage_manifest(&lineage_manifest_path).await {
            if !manifest.edges_path.is_empty() {
                referenced.insert(normalize_directory_path(&manifest.edges_path));
            }
        }

        // Executions domain
        if let Ok(manifest) = self
            .read_executions_manifest(&executions_manifest_path)
            .await
        {
            if let Some(path) = manifest.snapshot_path {
                // For state snapshots, extract the directory portion
                if let Some(dir) = path.rsplit('/').nth(0).and_then(|_| {
                    // state/executions/snapshot_v1_xxx.parquet -> state/executions/
                    path.rsplit_once('/')
                        .map(|(prefix, _)| format!("{prefix}/"))
                }) {
                    referenced.insert(dir);
                }
                referenced.insert(path);
            }
        }

        // Search domain
        if let Ok(manifest) = self.read_search_manifest(&search_manifest_path).await {
            if !manifest.base_path.is_empty() {
                referenced.insert(normalize_directory_path(&manifest.base_path));
            }
        }

        Ok(referenced)
    }

    /// Gets the watermark timestamp for a domain (used for ledger retention).
    async fn get_domain_watermark_timestamp(
        &self,
        domain: CatalogDomain,
    ) -> Result<Option<DateTime<Utc>>> {
        // Only executions domain has a watermark currently
        if domain != CatalogDomain::Executions {
            return Ok(None);
        }

        let root = self.read_root_manifest().await?;
        let executions_manifest_path = self
            .resolve_domain_manifest_path(CatalogDomain::Executions, &root.executions_manifest_path)
            .await?;

        if let Ok(manifest) = self
            .read_executions_manifest(&executions_manifest_path)
            .await
        {
            Ok(manifest.last_compaction_at)
        } else {
            Ok(None)
        }
    }

    /// Deletes all objects under a prefix.
    async fn delete_prefix(&self, prefix: &str) -> Result<(u64, u64)> {
        let entries = self
            .storage
            .list_meta(prefix)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to list {prefix}: {e}"),
            })?;

        let mut count = 0u64;
        let mut bytes = 0u64;

        for meta in entries {
            let path = meta.path.to_string();
            let size = meta.size;

            self.storage
                .delete(&path)
                .await
                .map_err(|e| CatalogError::Storage {
                    message: format!("failed to delete {path}: {e}"),
                })?;

            count += 1;
            bytes += size;
        }

        Ok((count, bytes))
    }

    // =========================================================================
    // Manifest Readers
    // =========================================================================

    async fn read_root_manifest(&self) -> Result<RootManifest> {
        let data = self
            .storage
            .get_raw(CatalogPaths::ROOT_MANIFEST)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to read root manifest: {e}"),
            })?;

        let mut manifest: RootManifest =
            serde_json::from_slice(&data).map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse root manifest: {e}"),
            })?;

        manifest.normalize_paths();
        Ok(manifest)
    }

    async fn read_catalog_manifest(&self, path: &str) -> Result<CatalogDomainManifest> {
        let data = self
            .storage
            .get_raw(path)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to read catalog manifest: {e}"),
            })?;

        serde_json::from_slice(&data).map_err(|e| CatalogError::Serialization {
            message: format!("failed to parse catalog manifest: {e}"),
        })
    }

    async fn read_lineage_manifest(&self, path: &str) -> Result<LineageManifest> {
        let data = self
            .storage
            .get_raw(path)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to read lineage manifest: {e}"),
            })?;

        serde_json::from_slice(&data).map_err(|e| CatalogError::Serialization {
            message: format!("failed to parse lineage manifest: {e}"),
        })
    }

    async fn read_executions_manifest(&self, path: &str) -> Result<ExecutionsManifest> {
        let data = self
            .storage
            .get_raw(path)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to read executions manifest: {e}"),
            })?;

        serde_json::from_slice(&data).map_err(|e| CatalogError::Serialization {
            message: format!("failed to parse executions manifest: {e}"),
        })
    }

    async fn read_search_manifest(&self, path: &str) -> Result<SearchManifest> {
        let data = self
            .storage
            .get_raw(path)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to read search manifest: {e}"),
            })?;

        serde_json::from_slice(&data).map_err(|e| CatalogError::Serialization {
            message: format!("failed to parse search manifest: {e}"),
        })
    }

    async fn resolve_domain_manifest_path(
        &self,
        domain: CatalogDomain,
        legacy_path: &str,
    ) -> Result<String> {
        let pointer_path = CatalogPaths::domain_manifest_pointer(domain);
        match self.storage.get_raw(&pointer_path).await {
            Ok(pointer_bytes) => {
                let pointer: DomainManifestPointer = serde_json::from_slice(&pointer_bytes)
                    .map_err(|e| CatalogError::Serialization {
                        message: format!("failed to parse JSON at {pointer_path}: {e}"),
                    })?;
                match self.storage.head_raw(&pointer.manifest_path).await {
                    Ok(Some(_)) => Ok(pointer.manifest_path),
                    Ok(None) => Ok(legacy_path.to_string()),
                    Err(e) => Err(CatalogError::from(e)),
                }
            }
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                Ok(legacy_path.to_string())
            }
            Err(e) => Err(CatalogError::from(e)),
        }
    }
}

// =========================================================================
// Helper Functions
// =========================================================================

/// Extracts the version directory from a snapshot path.
///
/// `snapshots/catalog/v42/namespaces.parquet` -> `snapshots/catalog/v42/`
fn extract_snapshot_version_dir(path: &str) -> Option<String> {
    // Find the version segment (v followed by digits)
    let parts: Vec<&str> = path.split('/').collect();

    for (i, part) in parts.iter().enumerate() {
        if part.starts_with('v') && part.len() > 1 && part[1..].chars().all(|c| c.is_ascii_digit())
        {
            // Reconstruct path up to and including version directory
            let Some(prefix) = parts.get(..=i) else {
                continue;
            };
            let dir = prefix.join("/");
            return Some(format!("{dir}/"));
        }
    }

    None
}

/// Extracts the version number from a version directory path.
///
/// `snapshots/catalog/v42/` -> `42`
fn extract_version_number(path: &str) -> Option<u64> {
    path.split('/')
        .find(|part| part.starts_with('v') && part.len() > 1)
        .and_then(|part| part[1..].parse().ok())
}

fn normalize_directory_path(path: &str) -> String {
    if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::storage::{MemoryBackend, WritePrecondition};
    use bytes::Bytes;
    use chrono::Utc;
    use std::sync::Arc;

    #[test]
    fn test_extract_snapshot_version_dir() {
        assert_eq!(
            extract_snapshot_version_dir("snapshots/catalog/v42/namespaces.parquet"),
            Some("snapshots/catalog/v42/".to_string())
        );
        assert_eq!(
            extract_snapshot_version_dir("snapshots/lineage/v1/edges.parquet"),
            Some("snapshots/lineage/v1/".to_string())
        );
        assert_eq!(extract_snapshot_version_dir("manifests/root.json"), None);
        assert_eq!(
            extract_snapshot_version_dir("ledger/executions/abc.json"),
            None
        );
    }

    #[test]
    fn test_extract_version_number() {
        assert_eq!(extract_version_number("snapshots/catalog/v42/"), Some(42));
        assert_eq!(extract_version_number("snapshots/lineage/v1/"), Some(1));
        assert_eq!(extract_version_number("snapshots/catalog/invalid/"), None);
    }

    #[test]
    fn test_gc_result_merge() {
        let mut r1 = GcResult {
            objects_deleted: 5,
            bytes_reclaimed: 1000,
            orphaned_snapshots_deleted: 2,
            ledger_events_deleted: 3,
            old_snapshots_deleted: 0,
            errors: vec!["error1".to_string()],
        };

        let r2 = GcResult {
            objects_deleted: 3,
            bytes_reclaimed: 500,
            orphaned_snapshots_deleted: 1,
            ledger_events_deleted: 1,
            old_snapshots_deleted: 1,
            errors: vec!["error2".to_string()],
        };

        r1.merge(r2);

        assert_eq!(r1.objects_deleted, 8);
        assert_eq!(r1.bytes_reclaimed, 1500);
        assert_eq!(r1.orphaned_snapshots_deleted, 3);
        assert_eq!(r1.ledger_events_deleted, 4);
        assert_eq!(r1.old_snapshots_deleted, 1);
        assert_eq!(r1.errors.len(), 2);
    }

    #[tokio::test]
    async fn test_gc_empty_storage() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        // Initialize minimal manifests
        let tier1_writer = crate::Tier1Writer::new(storage.clone());
        tier1_writer.initialize().await.expect("init");

        let collector = GarbageCollector::new(storage, RetentionPolicy::default());
        let result = collector.collect().await.expect("gc");

        assert_eq!(result.objects_deleted, 0);
        assert_eq!(result.bytes_reclaimed, 0);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_gc_dry_run() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let tier1_writer = crate::Tier1Writer::new(storage.clone());
        tier1_writer.initialize().await.expect("init");

        let collector = GarbageCollector::new(storage, RetentionPolicy::default());
        let report = collector.collect_dry_run().await.expect("dry run");

        // Should report what would be deleted without deleting
        assert!(report.orphaned_snapshots.is_empty());
        assert!(report.old_ledger_events.is_empty());
        assert!(report.old_snapshot_versions.is_empty());
    }

    fn catalog_manifest_for_version(
        version: u64,
        manifest_id: u64,
        file_name: &str,
        epoch: u64,
    ) -> CatalogDomainManifest {
        let snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, version);
        let mut snapshot = crate::manifest::SnapshotInfo::new(version, snapshot_path.clone());
        snapshot.add_file(crate::manifest::SnapshotFile {
            path: file_name.to_string(),
            checksum_sha256: "11".repeat(32),
            byte_size: 1,
            row_count: 1,
            position_range: None,
        });

        CatalogDomainManifest {
            manifest_id: crate::manifest::format_manifest_id(manifest_id),
            epoch,
            previous_manifest_path: None,
            writer_session_id: Some(format!("gc-test-{manifest_id}")),
            snapshot_version: version,
            snapshot_path,
            snapshot: Some(snapshot),
            watermark_event_id: None,
            last_commit_id: None,
            fencing_token: Some(epoch),
            commit_ulid: None,
            parent_hash: None,
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn dry_run_does_not_flag_pointer_target_snapshot_as_orphan() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");
        let tier1_writer = crate::Tier1Writer::new(storage.clone());
        tier1_writer.initialize().await.expect("init");

        let root_bytes = storage
            .get_raw(CatalogPaths::ROOT_MANIFEST)
            .await
            .expect("read root");
        let mut root: RootManifest = serde_json::from_slice(&root_bytes).expect("parse root");
        root.normalize_paths();

        let legacy_manifest = catalog_manifest_for_version(1, 1, "legacy.parquet", 1);
        storage
            .put_raw(
                &root.catalog_manifest_path,
                Bytes::from(serde_json::to_vec(&legacy_manifest).expect("serialize legacy")),
                WritePrecondition::None,
            )
            .await
            .expect("write legacy");

        let pointed_manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, "00000000000000000002");
        let pointed_manifest = catalog_manifest_for_version(2, 2, "current.parquet", 2);
        storage
            .put_raw(
                &pointed_manifest_path,
                Bytes::from(serde_json::to_vec(&pointed_manifest).expect("serialize pointed")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write pointed");

        let pointer = DomainManifestPointer {
            manifest_id: "00000000000000000002".to_string(),
            manifest_path: pointed_manifest_path,
            epoch: 2,
            parent_pointer_hash: None,
            updated_at: Utc::now(),
        };
        storage
            .put_raw(
                &CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog),
                Bytes::from(serde_json::to_vec(&pointer).expect("serialize pointer")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write pointer");

        storage
            .put_raw(
                &CatalogPaths::snapshot_file(CatalogDomain::Catalog, 2, "current.parquet"),
                Bytes::from_static(b"ok"),
                WritePrecondition::None,
            )
            .await
            .expect("write pointed file");

        let collector = GarbageCollector::new(storage, RetentionPolicy::default());
        let report = collector.collect_dry_run().await.expect("dry run");
        let pointed_dir = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 2);
        assert!(
            !report.orphaned_snapshots.contains(&pointed_dir),
            "pointer-targeted snapshot directory must never be treated as orphaned"
        );
    }

    #[tokio::test]
    async fn old_snapshot_gc_never_selects_pointer_target_manifest_version() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");
        let tier1_writer = crate::Tier1Writer::new(storage.clone());
        tier1_writer.initialize().await.expect("init");

        let root_bytes = storage
            .get_raw(CatalogPaths::ROOT_MANIFEST)
            .await
            .expect("read root");
        let mut root: RootManifest = serde_json::from_slice(&root_bytes).expect("parse root");
        root.normalize_paths();

        let legacy_manifest = catalog_manifest_for_version(1, 1, "legacy.parquet", 1);
        storage
            .put_raw(
                &root.catalog_manifest_path,
                Bytes::from(serde_json::to_vec(&legacy_manifest).expect("serialize legacy")),
                WritePrecondition::None,
            )
            .await
            .expect("write legacy");

        let pointed_manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, "00000000000000000002");
        let pointed_manifest = catalog_manifest_for_version(2, 2, "current.parquet", 2);
        storage
            .put_raw(
                &pointed_manifest_path,
                Bytes::from(serde_json::to_vec(&pointed_manifest).expect("serialize pointed")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write pointed");

        let pointer = DomainManifestPointer {
            manifest_id: "00000000000000000002".to_string(),
            manifest_path: pointed_manifest_path,
            epoch: 2,
            parent_pointer_hash: None,
            updated_at: Utc::now(),
        };
        storage
            .put_raw(
                &CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog),
                Bytes::from(serde_json::to_vec(&pointer).expect("serialize pointer")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write pointer");

        storage
            .put_raw(
                &CatalogPaths::snapshot_file(CatalogDomain::Catalog, 1, "legacy.parquet"),
                Bytes::from_static(b"old"),
                WritePrecondition::None,
            )
            .await
            .expect("write legacy file");
        storage
            .put_raw(
                &CatalogPaths::snapshot_file(CatalogDomain::Catalog, 2, "current.parquet"),
                Bytes::from_static(b"new"),
                WritePrecondition::None,
            )
            .await
            .expect("write current file");

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let collector = GarbageCollector::new(
            storage,
            RetentionPolicy {
                keep_snapshots: 0,
                delay_hours: 0,
                ledger_retention_hours: 1,
                max_age_days: 1,
            },
        );
        let report = collector.collect_dry_run().await.expect("dry run");
        let pointed_dir = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 2);
        assert!(
            !report.old_snapshot_versions.contains(&pointed_dir),
            "pointer-targeted snapshot directory must never be selected as old snapshot GC candidate"
        );
    }
}
