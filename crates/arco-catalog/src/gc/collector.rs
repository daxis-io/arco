//! Garbage collector implementation.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Duration, Utc};

use arco_core::lock::DistributedLock;
use arco_core::scoped_storage::ScopedStorage;
use arco_core::{CatalogDomain, CatalogPaths};

use crate::error::{CatalogError, Result};
use crate::gc::RetentionPolicy;
#[cfg(test)]
use crate::gc::reachability::sha256_digest;
use crate::gc::reachability::{
    ProtectionSet, ReachabilityInventory, build_protection_set, export_record_path,
    load_selected_retention_pin, snapshot_record_path, validate_inventory_path,
};
#[cfg(test)]
use crate::gc::reachability::{pin_latest_path, pin_revision_path};
use crate::manifest::{
    CatalogDomainManifest, DomainManifestPointer, ExecutionsManifest, LineageManifest,
    RootManifest, SearchManifest,
};
use crate::retention_coordination::{RetentionMutationEpoch, RetentionMutationKind};
use crate::workspace_snapshot::{
    RETENTION_GC_LOCK_MAX_RETRIES, RETENTION_GC_LOCK_PATH, RETENTION_GC_LOCK_TTL, RetentionStatus,
    RetentionTarget, decode_export_manifest, decode_workspace_snapshot,
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
        let protection = self.load_protection_set(Utc::now()).await?;
        let mut report = GcReport::default();

        // 1. Find orphaned snapshots
        let orphaned = self.find_orphaned_snapshots(&protection).await?;
        report.orphaned_snapshots = orphaned;
        report.objects_to_delete += report.orphaned_snapshots.len() as u64;

        // 2. Find old ledger events
        let old_events = self.find_old_ledger_events(&protection).await?;
        report.old_ledger_events = old_events;
        report.objects_to_delete += report.old_ledger_events.len() as u64;

        // 3. Find old snapshot versions
        let old_versions = self.find_old_snapshot_versions(&protection).await?;
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
        let mut guard =
            DistributedLock::new(Arc::new(self.storage.clone()), RETENTION_GC_LOCK_PATH)
                .acquire_with_operation(
                    RETENTION_GC_LOCK_TTL,
                    RETENTION_GC_LOCK_MAX_RETRIES,
                    Some("catalog-gc".to_string()),
                )
                .await
                .map_err(CatalogError::from)?;
        let operation_id = guard.holder_id().to_string();
        let mut epoch = match RetentionMutationEpoch::claim(
            self.storage.clone(),
            &mut guard,
            RetentionMutationKind::CatalogGc,
            operation_id,
        )
        .await
        {
            Ok(epoch) => epoch,
            Err(error) => {
                let _ = guard.release().await;
                return Err(error);
            }
        };
        let collection = self.collect_while_coordinated(&mut epoch).await;
        let settlement = epoch.settle().await;
        let release = guard.release().await.map_err(CatalogError::from);
        match (collection, settlement, release) {
            (Ok(result), Ok(()), Ok(())) => Ok(result),
            (Err(error), _, _) | (Ok(_), Err(error), _) | (Ok(_), Ok(()), Err(error)) => Err(error),
        }
    }

    async fn collect_while_coordinated(
        &self,
        epoch: &mut RetentionMutationEpoch,
    ) -> Result<GcResult> {
        let start = Instant::now();
        let mut result = GcResult::default();
        let protection = self.load_protection_set(Utc::now()).await?;

        // Complete every inventory pass before the first mutation. A corrupt
        // later root or listing must never be discovered after an earlier delete.
        let orphaned = self.find_orphaned_snapshots(&protection).await?;
        let old_events = self.find_old_ledger_events(&protection).await?;
        let old_versions = self.find_old_snapshot_versions(&protection).await?;

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
            || self.gc_orphaned_snapshots(orphaned, &protection, epoch),
            &mut result,
        )
        .await?;
        self.run_phase(
            "compacted_ledger",
            "compacted ledger",
            || self.gc_compacted_ledger(old_events, &protection, epoch),
            &mut result,
        )
        .await?;
        self.run_phase(
            "old_snapshots",
            "old snapshots",
            || self.gc_old_snapshots(old_versions, &protection, epoch),
            &mut result,
        )
        .await?;

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
    ) -> Result<()>
    where
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
            Err(error @ CatalogError::CasFailed { .. }) => return Err(error),
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
        Ok(())
    }

    // =========================================================================
    // Phase 1: Orphaned Snapshots
    // =========================================================================

    /// Finds snapshot directories not referenced by any manifest.
    async fn find_orphaned_snapshots(&self, protection: &ProtectionSet) -> Result<Vec<String>> {
        let referenced = self.get_referenced_snapshots().await?;
        let mut orphaned = Vec::new();

        // Check each domain's snapshot directory
        for domain in CatalogDomain::all() {
            let prefix = format!("snapshots/{}/", domain.as_str());
            let mut entries =
                self.storage
                    .list(&prefix)
                    .await
                    .map_err(|e| CatalogError::Storage {
                        message: format!("failed to list snapshots for {domain}: {e}"),
                    })?;
            entries.sort_by(|left, right| left.as_str().cmp(right.as_str()));

            for entry in entries {
                let path = entry.as_str();
                // Extract the version directory (e.g., "snapshots/catalog/v1/")
                if let Some(version_dir) = extract_snapshot_version_dir(path) {
                    if !referenced.contains(&version_dir)
                        && !protection.protects_prefix(&version_dir)
                    {
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
    async fn gc_orphaned_snapshots(
        &self,
        orphaned: Vec<String>,
        protection: &ProtectionSet,
        epoch: &mut RetentionMutationEpoch,
    ) -> Result<GcResult> {
        let mut result = GcResult::default();
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
                match self.delete_prefix(&dir, protection, epoch).await {
                    Ok((count, bytes)) => {
                        tracing::info!(path = %dir, count, bytes, "deleted orphaned snapshot");
                        result.objects_deleted += count;
                        result.bytes_reclaimed += bytes;
                        result.orphaned_snapshots_deleted += 1;
                    }
                    Err(error @ CatalogError::CasFailed { .. }) => return Err(error),
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
    async fn find_old_ledger_events(&self, protection: &ProtectionSet) -> Result<Vec<String>> {
        let mut old_events = Vec::new();

        for domain in CatalogDomain::all() {
            // Read watermark for this domain
            let watermark_ts = self.get_domain_watermark_timestamp(*domain).await?;

            if let Some(watermark_ts) = watermark_ts {
                let cutoff =
                    watermark_ts - Duration::hours(i64::from(self.policy.ledger_retention_hours));

                let prefix = CatalogPaths::ledger_dir(*domain);
                let mut entries =
                    self.storage
                        .list_meta(&prefix)
                        .await
                        .map_err(|e| CatalogError::Storage {
                            message: format!("failed to list ledger for {domain}: {e}"),
                        })?;
                entries.sort_by(|left, right| left.path.as_str().cmp(right.path.as_str()));

                let objects = u64::try_from(entries.len()).unwrap_or(u64::MAX);
                let bytes: u64 = entries.iter().map(|meta| meta.size).sum();
                crate::metrics::record_storage_inventory(
                    &format!("ledger/{}", domain.as_str()),
                    objects,
                    bytes,
                );

                for meta in entries {
                    if let Some(last_modified) = meta.last_modified {
                        if last_modified < cutoff && !protection.protects_object(meta.path.as_str())
                        {
                            old_events.push(meta.path.to_string());
                        }
                    }
                }
            }
        }

        Ok(old_events)
    }

    /// Deletes ledger events older than the retention window.
    async fn gc_compacted_ledger(
        &self,
        old_events: Vec<String>,
        protection: &ProtectionSet,
        epoch: &mut RetentionMutationEpoch,
    ) -> Result<GcResult> {
        let mut result = GcResult::default();

        for path in old_events {
            if protection.protects_object(&path) {
                continue;
            }
            match epoch.delete(&path).await {
                Ok(()) => {
                    result.objects_deleted += 1;
                    result.ledger_events_deleted += 1;
                }
                Err(error @ CatalogError::CasFailed { .. }) => return Err(error),
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
    async fn find_old_snapshot_versions(&self, protection: &ProtectionSet) -> Result<Vec<String>> {
        let mut old_versions = Vec::new();
        let cutoff = Utc::now() - Duration::hours(i64::from(self.policy.delay_hours));
        let referenced = self.get_referenced_snapshots().await?;

        for domain in CatalogDomain::all() {
            let prefix = format!("snapshots/{}/", domain.as_str());
            let mut entries =
                self.storage
                    .list_meta(&prefix)
                    .await
                    .map_err(|e| CatalogError::Storage {
                        message: format!("failed to list snapshots for {domain}: {e}"),
                    })?;
            entries.sort_by(|left, right| left.path.as_str().cmp(right.path.as_str()));

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
                if referenced.contains(&dir) || protection.protects_prefix(&dir) {
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
    async fn gc_old_snapshots(
        &self,
        old_versions: Vec<String>,
        protection: &ProtectionSet,
        epoch: &mut RetentionMutationEpoch,
    ) -> Result<GcResult> {
        let mut result = GcResult::default();

        for dir in old_versions {
            match self.delete_prefix(&dir, protection, epoch).await {
                Ok((count, bytes)) => {
                    tracing::info!(path = %dir, count, bytes, "deleted old snapshot version");
                    result.objects_deleted += count;
                    result.bytes_reclaimed += bytes;
                    result.old_snapshots_deleted += 1;
                }
                Err(error @ CatalogError::CasFailed { .. }) => return Err(error),
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

    async fn load_protection_set(&self, now: DateTime<Utc>) -> Result<ProtectionSet> {
        let current_heads = self.get_referenced_snapshots().await?.into_iter().collect();
        let mut pin_objects = self
            .storage
            .list_meta("retention/pins/")
            .await
            .map_err(|error| CatalogError::Storage {
                message: format!("failed to list retention pin selectors: {error}"),
            })?;
        pin_objects.sort_by(|left, right| left.path.as_str().cmp(right.path.as_str()));

        let mut selected_pins = Vec::new();
        for object in pin_objects {
            let path = object.path.as_str();
            if !path.ends_with("/latest.json") {
                continue;
            }
            let Some(pin_id) = path
                .strip_prefix("retention/pins/")
                .and_then(|value| value.strip_suffix("/latest.json"))
            else {
                return Err(CatalogError::Validation {
                    message: "retention pin selector path is not canonical".to_string(),
                });
            };
            selected_pins.push(load_selected_retention_pin(&self.storage, pin_id).await?);
        }

        // No retained target is read until every selected pin has passed full
        // selector, raw-byte digest, predecessor-chain, and transition validation.
        let mut snapshots = Vec::new();
        let mut exports = Vec::new();
        let mut loaded_targets = BTreeSet::new();
        for selected in &selected_pins {
            let target = selected.latest_revision()?.target().clone();
            if selected.status_at(now)? != RetentionStatus::Active
                || !loaded_targets.insert(target.id().to_string())
            {
                continue;
            }
            match &target {
                RetentionTarget::Snapshot(snapshot_id) => {
                    let snapshot_path = snapshot_record_path(snapshot_id)?;
                    let bytes = self.storage.get_raw(&snapshot_path).await?;
                    snapshots.push(decode_workspace_snapshot(&bytes)?);
                }
                RetentionTarget::Export(export_id) => {
                    let export_path = export_record_path(export_id)?;
                    let bytes = self.storage.get_raw(&export_path).await?;
                    exports.push(decode_export_manifest(&bytes)?);
                }
            }
        }

        build_protection_set(
            now,
            ReachabilityInventory {
                current_heads,
                snapshots,
                exports,
                selected_pins,
            },
        )
    }

    /// Gets all snapshot paths referenced by current manifests.
    async fn get_referenced_snapshots(&self) -> Result<BTreeSet<String>> {
        let mut referenced = BTreeSet::new();

        // Read root manifest to find domain manifests
        let root = self.read_root_manifest().await?;
        let catalog_manifest_path = self
            .current_tier1_manifest_path(CatalogDomain::Catalog)
            .await?;
        let lineage_manifest_path = self
            .current_tier1_manifest_path(CatalogDomain::Lineage)
            .await?;
        let executions_manifest_path = self
            .resolve_domain_manifest_path(CatalogDomain::Executions, &root.executions_manifest_path)
            .await?;
        let search_manifest_path = self
            .current_tier1_manifest_path(CatalogDomain::Search)
            .await?;

        // Catalog domain
        let manifest = self.read_catalog_manifest(&catalog_manifest_path).await?;
        if let Some(path) = selected_manifest_snapshot_path(
            "catalog",
            &manifest.snapshot_path,
            manifest.snapshot_version,
            manifest.snapshot.as_ref(),
        )? {
            referenced.insert(normalize_directory_path(&path));
        }

        // Lineage domain
        let manifest = self.read_lineage_manifest(&lineage_manifest_path).await?;
        if let Some(path) = selected_manifest_snapshot_path(
            "lineage",
            &manifest.edges_path,
            manifest.snapshot_version,
            manifest.snapshot.as_ref(),
        )? {
            referenced.insert(normalize_directory_path(&path));
        }

        // Executions domain
        let manifest = self
            .read_executions_manifest(&executions_manifest_path)
            .await?;
        if let Some(path) = manifest.snapshot_path {
            validate_inventory_path(&path)?;
            // For state snapshots, extract the directory portion.
            if let Some((prefix, _)) = path.rsplit_once('/') {
                referenced.insert(format!("{prefix}/"));
            }
            referenced.insert(path);
        }

        // Search domain
        let manifest = self.read_search_manifest(&search_manifest_path).await?;
        if let Some(path) = selected_manifest_snapshot_path(
            "search",
            &manifest.base_path,
            manifest.snapshot_version,
            manifest.snapshot.as_ref(),
        )? {
            referenced.insert(normalize_directory_path(&path));
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
    async fn delete_prefix(
        &self,
        prefix: &str,
        protection: &ProtectionSet,
        epoch: &mut RetentionMutationEpoch,
    ) -> Result<(u64, u64)> {
        if protection.protects_prefix(prefix) {
            return Ok((0, 0));
        }
        if self.snapshot_path_is_currently_referenced(prefix).await? {
            tracing::warn!(
                path = %prefix,
                "skipping delete for currently referenced snapshot prefix"
            );
            return Ok((0, 0));
        }

        let mut entries =
            self.storage
                .list_meta(prefix)
                .await
                .map_err(|e| CatalogError::Storage {
                    message: format!("failed to list {prefix}: {e}"),
                })?;
        entries.sort_by(|left, right| left.path.as_str().cmp(right.path.as_str()));

        let mut count = 0u64;
        let mut bytes = 0u64;

        for meta in entries {
            let path = meta.path.to_string();
            let size = meta.size;

            if protection.protects_object(&path)
                || self.snapshot_path_is_currently_referenced(&path).await?
            {
                tracing::warn!(
                    path = %path,
                    "skipping delete for currently referenced snapshot path"
                );
                continue;
            }

            epoch.delete(&path).await?;

            count += 1;
            bytes += size;
        }

        Ok((count, bytes))
    }

    async fn snapshot_path_is_currently_referenced(&self, path: &str) -> Result<bool> {
        let Some(version_dir) = extract_snapshot_version_dir(path) else {
            return Ok(false);
        };
        let referenced = self.get_referenced_snapshots().await?;
        Ok(referenced.contains(&version_dir))
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
        if domain == CatalogDomain::Executions {
            Ok(legacy_path.to_string())
        } else {
            self.current_tier1_manifest_path(domain).await
        }
    }

    async fn current_tier1_manifest_path(&self, domain: CatalogDomain) -> Result<String> {
        let pointer_path = CatalogPaths::domain_manifest_pointer(domain);
        let pointer_bytes = self.storage.get_raw(&pointer_path).await?;
        let pointer: DomainManifestPointer =
            serde_json::from_slice(&pointer_bytes).map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse JSON at {pointer_path}: {e}"),
            })?;
        Ok(pointer.manifest_path)
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

fn selected_manifest_snapshot_path(
    domain: &str,
    legacy_path: &str,
    legacy_version: u64,
    enhanced: Option<&crate::manifest::SnapshotInfo>,
) -> Result<Option<String>> {
    let path = if let Some(enhanced) = enhanced {
        if legacy_path != enhanced.path || legacy_version != enhanced.version {
            return Err(CatalogError::Validation {
                message: format!("{domain} manifest legacy and enhanced snapshot fields disagree"),
            });
        }
        enhanced.path.as_str()
    } else {
        legacy_path
    };
    if path.is_empty() {
        return Ok(None);
    }
    validate_inventory_path(path)?;
    Ok(Some(path.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::storage::{
        MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::{Duration as ChronoDuration, Utc};
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration as StdDuration;

    use crate::state_store::{PersistedAuthorityKind, PersistedAuthorityReference, StateScope};
    use crate::workspace_snapshot::{
        DomainAuthorityReference, DomainEventArchive, RequiredObject, RequiredObjectKind,
        RetentionPinLatest, RetentionPinRevision, RetentionTarget, WorkspaceScope,
        WorkspaceSnapshot, encode_retention_pin_latest, encode_retention_pin_revision,
        encode_workspace_snapshot,
    };

    const TEST_SNAPSHOT_ID: &str = "snap_01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const TEST_PIN_ID: &str = "pin_01ARZ3NDEKTSV4RRFFQ69G5FAY";

    #[derive(Debug)]
    struct RecordingBackend {
        inner: MemoryBackend,
        get_paths: Mutex<Vec<String>>,
        delete_calls: AtomicUsize,
        reverse_lists: bool,
    }

    impl RecordingBackend {
        fn new(reverse_lists: bool) -> Self {
            Self {
                inner: MemoryBackend::new(),
                get_paths: Mutex::new(Vec::new()),
                delete_calls: AtomicUsize::new(0),
                reverse_lists,
            }
        }

        fn read_path_suffix(&self, suffix: &str) -> bool {
            self.get_paths
                .lock()
                .expect("get paths lock")
                .iter()
                .any(|path| path.ends_with(suffix))
        }

        fn delete_calls(&self) -> usize {
            self.delete_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl StorageBackend for RecordingBackend {
        async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
            self.get_paths
                .lock()
                .expect("get paths lock")
                .push(path.to_string());
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::Result<WriteResult> {
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.delete_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            let mut objects = self.inner.list(prefix).await?;
            if self.reverse_lists {
                objects.reverse();
            }
            Ok(objects)
        }

        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: StdDuration) -> arco_core::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

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

        let legacy_manifest = catalog_manifest_for_version(1, 1, "legacy.parquet", 1);
        storage
            .put_raw(
                &CatalogPaths::domain_manifest(CatalogDomain::Catalog),
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
                WritePrecondition::None,
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

        let legacy_manifest = catalog_manifest_for_version(1, 1, "legacy.parquet", 1);
        storage
            .put_raw(
                &CatalogPaths::domain_manifest(CatalogDomain::Catalog),
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
                WritePrecondition::None,
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

    async fn seed_old_and_current_catalog_snapshots(storage: &ScopedStorage) -> String {
        let legacy_manifest = catalog_manifest_for_version(1, 1, "legacy.parquet", 1);
        storage
            .put_raw(
                &CatalogPaths::domain_manifest(CatalogDomain::Catalog),
                Bytes::from(serde_json::to_vec(&legacy_manifest).expect("serialize legacy")),
                WritePrecondition::None,
            )
            .await
            .expect("write legacy manifest");

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
            .expect("write pointed manifest");
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
                WritePrecondition::None,
            )
            .await
            .expect("write pointer");

        let old_path = CatalogPaths::snapshot_file(CatalogDomain::Catalog, 1, "legacy.parquet");
        storage
            .put_raw(
                &old_path,
                Bytes::from_static(b"old"),
                WritePrecondition::None,
            )
            .await
            .expect("write old object");
        storage
            .put_raw(
                &CatalogPaths::snapshot_file(CatalogDomain::Catalog, 2, "current.parquet"),
                Bytes::from_static(b"new"),
                WritePrecondition::None,
            )
            .await
            .expect("write current object");
        old_path
    }

    async fn seed_retained_snapshot_record(
        storage: &ScopedStorage,
        required_objects: Vec<RequiredObject>,
    ) -> (DateTime<Utc>, DateTime<Utc>) {
        let created_at = Utc::now() - ChronoDuration::hours(1);
        let deadline = Utc::now() + ChronoDuration::days(1);
        let digest = format!("sha256:{}", "1".repeat(64));
        let workspace_scope = WorkspaceScope::new(storage.tenant_id(), storage.workspace_id())
            .expect("workspace scope");
        let authority = PersistedAuthorityReference::new(
            "arco-state-control-mvp",
            StateScope::new(storage.tenant_id(), storage.workspace_id(), "catalog"),
            PersistedAuthorityKind::StateToken,
            "manifest-1",
            1,
            "state-store/control-mvp/catalog/manifests/manifest-1.json",
            &digest,
            None,
            None,
            deadline,
        )
        .expect("authority reference");
        let snapshot = WorkspaceSnapshot::new(
            TEST_SNAPSHOT_ID,
            TEST_PIN_ID,
            workspace_scope.clone(),
            created_at,
            deadline,
            None,
            vec![
                DomainAuthorityReference::new("catalog", workspace_scope, authority)
                    .expect("domain authority"),
            ],
            vec![],
            vec![DomainEventArchive::empty("catalog").expect("archive")],
            required_objects,
            vec![],
        )
        .expect("snapshot");
        storage
            .put_raw(
                &snapshot_record_path(TEST_SNAPSHOT_ID).expect("snapshot record path"),
                Bytes::from(encode_workspace_snapshot(&snapshot).expect("snapshot bytes")),
                WritePrecondition::None,
            )
            .await
            .expect("write snapshot record");
        (created_at, deadline)
    }

    async fn seed_active_snapshot_pin(storage: &ScopedStorage, required_paths: &[(&str, u64)]) {
        let digest = format!("sha256:{}", "1".repeat(64));
        let required_objects = required_paths
            .iter()
            .map(|(path, size)| {
                RequiredObject::new(*path, *size, RequiredObjectKind::Other, &digest)
                    .expect("required retained object")
            })
            .collect();
        let (created_at, deadline) = seed_retained_snapshot_record(storage, required_objects).await;

        let revision = RetentionPinRevision::new(
            TEST_PIN_ID,
            1,
            RetentionTarget::snapshot(TEST_SNAPSHOT_ID).expect("target"),
            created_at,
            deadline,
            None,
        )
        .expect("pin revision");
        let revision_bytes = encode_retention_pin_revision(&revision).expect("revision bytes");
        let revision_digest = sha256_digest(&revision_bytes);
        let revision_path = pin_revision_path(TEST_PIN_ID, 1).expect("pin revision path");
        storage
            .put_raw(
                &revision_path,
                Bytes::from(revision_bytes),
                WritePrecondition::None,
            )
            .await
            .expect("write revision");
        let selector = RetentionPinLatest::new(TEST_PIN_ID, 1, revision_path, revision_digest)
            .expect("selector");
        storage
            .put_raw(
                &pin_latest_path(TEST_PIN_ID).expect("pin latest path"),
                Bytes::from(encode_retention_pin_latest(&selector).expect("selector bytes")),
                WritePrecondition::None,
            )
            .await
            .expect("write selector");
    }

    async fn seed_old_execution_ledger_event(storage: &ScopedStorage) -> String {
        let root: RootManifest = serde_json::from_slice(
            &storage
                .get_raw(CatalogPaths::ROOT_MANIFEST)
                .await
                .expect("root manifest bytes"),
        )
        .expect("root manifest");
        let mut executions: ExecutionsManifest = serde_json::from_slice(
            &storage
                .get_raw(&root.executions_manifest_path)
                .await
                .expect("executions manifest bytes"),
        )
        .expect("executions manifest");
        executions.last_compaction_at = Some(Utc::now() + ChronoDuration::days(1));
        storage
            .put_raw(
                &root.executions_manifest_path,
                Bytes::from(serde_json::to_vec(&executions).expect("executions manifest json")),
                WritePrecondition::None,
            )
            .await
            .expect("advance executions watermark");

        let path = CatalogPaths::ledger_event(CatalogDomain::Executions, "retained-old-event");
        storage
            .put_raw(
                &path,
                Bytes::from_static(b"old-event"),
                WritePrecondition::None,
            )
            .await
            .expect("write old ledger event");
        path
    }

    async fn move_current_catalog_head_to_version_one(storage: &ScopedStorage) {
        let manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, "00000000000000000003");
        let manifest = catalog_manifest_for_version(1, 3, "legacy.parquet", 3);
        storage
            .put_raw(
                &manifest_path,
                Bytes::from(serde_json::to_vec(&manifest).expect("moved manifest json")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write moved manifest");
        let pointer = DomainManifestPointer {
            manifest_id: "00000000000000000003".to_string(),
            manifest_path,
            epoch: 3,
            parent_pointer_hash: None,
            updated_at: Utc::now(),
        };
        storage
            .put_raw(
                &CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog),
                Bytes::from(serde_json::to_vec(&pointer).expect("moved pointer json")),
                WritePrecondition::None,
            )
            .await
            .expect("move current catalog head");
    }

    #[tokio::test]
    async fn malformed_retention_inventory_aborts_gc_before_any_delete() {
        let backend = Arc::new(RecordingBackend::new(false));
        let storage = ScopedStorage::new(backend.clone(), "acme", "prod").expect("storage");
        crate::Tier1Writer::new(storage.clone())
            .initialize()
            .await
            .expect("init");
        let old_path = seed_old_and_current_catalog_snapshots(&storage).await;
        let pin_id = "pin_01ARZ3NDEKTSV4RRFFQ69G5FAY";
        storage
            .put_raw(
                &format!("retention/pins/{pin_id}/latest.json"),
                Bytes::from_static(
                    br#"{"record_type":"retention_pin_latest","version":2,"pin_id":"pin_01ARZ3NDEKTSV4RRFFQ69G5FAY"}"#,
                ),
                WritePrecondition::None,
            )
            .await
            .expect("write malformed pin");

        let collector = GarbageCollector::new(
            storage.clone(),
            RetentionPolicy {
                keep_snapshots: 0,
                delay_hours: 0,
                ledger_retention_hours: 0,
                max_age_days: 0,
            },
        );
        assert!(collector.collect().await.is_err());
        assert_eq!(0, backend.delete_calls());
        assert_eq!(
            Bytes::from_static(b"old"),
            storage
                .get_raw(&old_path)
                .await
                .expect("old object retained")
        );
    }

    async fn write_selected_pin(
        storage: &ScopedStorage,
        revision: &RetentionPinRevision,
        revision_bytes: Vec<u8>,
    ) {
        let revision_path =
            pin_revision_path(revision.pin_id(), revision.revision()).expect("pin revision path");
        let digest = sha256_digest(&revision_bytes);
        storage
            .put_raw(
                &revision_path,
                Bytes::from(revision_bytes),
                WritePrecondition::None,
            )
            .await
            .expect("write selected revision");
        let selector = RetentionPinLatest::new(
            revision.pin_id(),
            revision.revision(),
            revision_path,
            digest,
        )
        .expect("selector");
        storage
            .put_raw(
                &pin_latest_path(revision.pin_id()).expect("pin latest path"),
                Bytes::from(encode_retention_pin_latest(&selector).expect("selector bytes")),
                WritePrecondition::None,
            )
            .await
            .expect("write selector");
    }

    #[tokio::test]
    async fn future_dated_release_keeps_snapshot_protected_until_effective_time() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");
        crate::Tier1Writer::new(storage.clone())
            .initialize()
            .await
            .expect("init");
        let old_path = seed_old_and_current_catalog_snapshots(&storage).await;
        let digest = format!("sha256:{}", "1".repeat(64));
        let (created_at, deadline) = seed_retained_snapshot_record(
            &storage,
            vec![
                RequiredObject::new(&old_path, 3, RequiredObjectKind::Other, &digest)
                    .expect("required object"),
            ],
        )
        .await;
        let initial = RetentionPinRevision::new(
            TEST_PIN_ID,
            1,
            RetentionTarget::snapshot(TEST_SNAPSHOT_ID).expect("target"),
            created_at,
            deadline,
            None,
        )
        .expect("initial revision");
        let initial_bytes = encode_retention_pin_revision(&initial).expect("initial bytes");
        storage
            .put_raw(
                &pin_revision_path(TEST_PIN_ID, 1).expect("pin revision path"),
                Bytes::from(initial_bytes),
                WritePrecondition::None,
            )
            .await
            .expect("write initial revision");
        let scheduled = initial
            .release(2, Utc::now() + ChronoDuration::hours(1))
            .expect("schedule active release");
        let scheduled_bytes =
            encode_retention_pin_revision(&scheduled).expect("scheduled release bytes");
        write_selected_pin(&storage, &scheduled, scheduled_bytes).await;

        let collector = GarbageCollector::new(
            storage,
            RetentionPolicy {
                keep_snapshots: 0,
                delay_hours: 0,
                ledger_retention_hours: 0,
                max_age_days: 0,
            },
        );
        let report = collector.collect_dry_run().await.expect("dry run");
        assert!(
            !report
                .old_snapshot_versions
                .contains(&CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1))
        );
    }

    #[tokio::test]
    async fn additive_v1_pin_fields_preserve_raw_selector_digest_semantics() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");
        crate::Tier1Writer::new(storage.clone())
            .initialize()
            .await
            .expect("init");
        let old_path = seed_old_and_current_catalog_snapshots(&storage).await;
        let digest = format!("sha256:{}", "1".repeat(64));
        let (created_at, deadline) = seed_retained_snapshot_record(
            &storage,
            vec![
                RequiredObject::new(&old_path, 3, RequiredObjectKind::Other, &digest)
                    .expect("required object"),
            ],
        )
        .await;
        let revision = RetentionPinRevision::new(
            TEST_PIN_ID,
            1,
            RetentionTarget::snapshot(TEST_SNAPSHOT_ID).expect("target"),
            created_at,
            deadline,
            None,
        )
        .expect("revision");
        let mut value = serde_json::to_value(&revision).expect("revision json");
        value.as_object_mut().expect("revision json object").insert(
            "future_v1_hint".to_string(),
            serde_json::Value::String("preserved in raw bytes".to_string()),
        );
        let raw = serde_jcs::to_vec(&value).expect("canonical additive revision");
        write_selected_pin(&storage, &revision, raw).await;

        let collector = GarbageCollector::new(
            storage,
            RetentionPolicy {
                keep_snapshots: 0,
                delay_hours: 0,
                ledger_retention_hours: 0,
                max_age_days: 0,
            },
        );
        let report = collector.collect_dry_run().await.expect("additive v1 pin");
        assert!(
            !report
                .old_snapshot_versions
                .contains(&CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1))
        );
    }

    #[tokio::test]
    async fn selector_validation_precedes_target_read_and_malformed_gc_never_deletes() {
        let backend = Arc::new(RecordingBackend::new(false));
        let storage = ScopedStorage::new(backend.clone(), "acme", "prod").expect("storage");
        crate::Tier1Writer::new(storage.clone())
            .initialize()
            .await
            .expect("init");
        seed_old_and_current_catalog_snapshots(&storage).await;
        let (created_at, deadline) = seed_retained_snapshot_record(&storage, vec![]).await;
        let earlier_valid = RetentionPinRevision::new(
            "pin_01ARZ3NDEKTSV4RRFFQ69G5FAX",
            1,
            RetentionTarget::snapshot(TEST_SNAPSHOT_ID).expect("valid target"),
            created_at,
            deadline,
            None,
        )
        .expect("earlier valid pin");
        write_selected_pin(
            &storage,
            &earlier_valid,
            encode_retention_pin_revision(&earlier_valid).expect("earlier revision bytes"),
        )
        .await;
        let revision = RetentionPinRevision::new(
            TEST_PIN_ID,
            1,
            RetentionTarget::snapshot(TEST_SNAPSHOT_ID).expect("target"),
            created_at,
            deadline,
            None,
        )
        .expect("revision");
        let revision_bytes = encode_retention_pin_revision(&revision).expect("revision bytes");
        let revision_path = pin_revision_path(TEST_PIN_ID, 1).expect("pin revision path");
        storage
            .put_raw(
                &revision_path,
                Bytes::from(revision_bytes),
                WritePrecondition::None,
            )
            .await
            .expect("write revision");
        let corrupt_selector = RetentionPinLatest::new(
            TEST_PIN_ID,
            1,
            revision_path,
            format!("sha256:{}", "f".repeat(64)),
        )
        .expect("selector shape");
        storage
            .put_raw(
                &pin_latest_path(TEST_PIN_ID).expect("pin latest path"),
                Bytes::from(
                    encode_retention_pin_latest(&corrupt_selector).expect("selector bytes"),
                ),
                WritePrecondition::None,
            )
            .await
            .expect("write corrupt selector");

        let collector = GarbageCollector::new(
            storage,
            RetentionPolicy {
                keep_snapshots: 0,
                delay_hours: 0,
                ledger_retention_hours: 0,
                max_age_days: 0,
            },
        );
        assert!(collector.collect().await.is_err());
        assert_eq!(0, backend.delete_calls());
        assert!(
            !backend.read_path_suffix(
                &snapshot_record_path(TEST_SNAPSHOT_ID).expect("snapshot record path")
            ),
            "no target may be read until every selected pin validates"
        );
    }

    async fn corrupt_selected_enhanced_snapshot_path(
        storage: &ScopedStorage,
        domain: CatalogDomain,
    ) {
        let pointer_path = CatalogPaths::domain_manifest_pointer(domain);
        let pointer: DomainManifestPointer =
            serde_json::from_slice(&storage.get_raw(&pointer_path).await.expect("pointer bytes"))
                .expect("pointer json");
        let bytes = storage
            .get_raw(&pointer.manifest_path)
            .await
            .expect("manifest bytes");
        let corrupt = match domain {
            CatalogDomain::Catalog => {
                let mut manifest: CatalogDomainManifest =
                    serde_json::from_slice(&bytes).expect("catalog manifest");
                manifest.snapshot = Some(crate::manifest::SnapshotInfo::new(
                    manifest.snapshot_version,
                    "snapshots/catalog/v99/".to_string(),
                ));
                serde_json::to_vec(&manifest).expect("catalog json")
            }
            CatalogDomain::Lineage => {
                let mut manifest: LineageManifest =
                    serde_json::from_slice(&bytes).expect("lineage manifest");
                manifest.snapshot = Some(crate::manifest::SnapshotInfo::new(
                    manifest.snapshot_version,
                    "snapshots/lineage/v99/".to_string(),
                ));
                serde_json::to_vec(&manifest).expect("lineage json")
            }
            CatalogDomain::Search => {
                let mut manifest: SearchManifest =
                    serde_json::from_slice(&bytes).expect("search manifest");
                manifest.snapshot = Some(crate::manifest::SnapshotInfo::new(
                    manifest.snapshot_version,
                    "snapshots/search/v99/".to_string(),
                ));
                serde_json::to_vec(&manifest).expect("search json")
            }
            CatalogDomain::Executions => panic!("executions has no enhanced snapshot path"),
        };
        storage
            .put_raw(
                &pointer.manifest_path,
                Bytes::from(corrupt),
                WritePrecondition::None,
            )
            .await
            .expect("overwrite selected manifest");
    }

    #[tokio::test]
    async fn mixed_legacy_and_enhanced_current_head_paths_abort_gc() {
        for domain in [
            CatalogDomain::Catalog,
            CatalogDomain::Lineage,
            CatalogDomain::Search,
        ] {
            let backend = Arc::new(MemoryBackend::new());
            let storage = ScopedStorage::new(backend, "acme", domain.as_str()).expect("storage");
            crate::Tier1Writer::new(storage.clone())
                .initialize()
                .await
                .expect("init");
            corrupt_selected_enhanced_snapshot_path(&storage, domain).await;
            let collector = GarbageCollector::new(storage, RetentionPolicy::default());
            assert!(
                collector.collect_dry_run().await.is_err(),
                "mixed {domain} manifest must fail closed"
            );
        }
    }

    #[tokio::test]
    async fn active_retention_root_protects_candidates_in_dry_run_and_mutation_mode() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");
        crate::Tier1Writer::new(storage.clone())
            .initialize()
            .await
            .expect("init");
        let old_path = seed_old_and_current_catalog_snapshots(&storage).await;
        let old_ledger_path = seed_old_execution_ledger_event(&storage).await;
        seed_active_snapshot_pin(
            &storage,
            &[
                (&old_path, 3),
                (
                    &old_ledger_path,
                    u64::try_from(b"old-event".len()).expect("event size"),
                ),
            ],
        )
        .await;

        let collector = GarbageCollector::new(
            storage.clone(),
            RetentionPolicy {
                keep_snapshots: 0,
                delay_hours: 0,
                ledger_retention_hours: 0,
                max_age_days: 0,
            },
        );
        let report = collector.collect_dry_run().await.expect("dry run");
        assert!(
            !report
                .old_snapshot_versions
                .contains(&CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1))
        );
        assert!(
            !report
                .orphaned_snapshots
                .contains(&CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1))
        );
        assert!(!report.old_ledger_events.contains(&old_ledger_path));
        collector.collect().await.expect("collect");
        assert_eq!(
            Bytes::from_static(b"old"),
            storage
                .get_raw(&old_path)
                .await
                .expect("protected old object")
        );
        assert_eq!(
            Bytes::from_static(b"old-event"),
            storage
                .get_raw(&old_ledger_path)
                .await
                .expect("protected ledger event")
        );
    }

    async fn report_with_list_order(reverse_lists: bool) -> GcReport {
        let backend = Arc::new(RecordingBackend::new(reverse_lists));
        let storage = ScopedStorage::new(backend, "acme", "list-order").expect("storage");
        crate::Tier1Writer::new(storage.clone())
            .initialize()
            .await
            .expect("init");
        let old_path = seed_old_and_current_catalog_snapshots(&storage).await;
        seed_active_snapshot_pin(&storage, &[(&old_path, 3)]).await;
        storage
            .put_raw(
                &CatalogPaths::snapshot_file(CatalogDomain::Catalog, 3, "orphan.parquet"),
                Bytes::from_static(b"orphan"),
                WritePrecondition::None,
            )
            .await
            .expect("write unprotected candidate");
        GarbageCollector::new(
            storage,
            RetentionPolicy {
                keep_snapshots: 0,
                delay_hours: 0,
                ledger_retention_hours: 0,
                max_age_days: 0,
            },
        )
        .collect_dry_run()
        .await
        .expect("dry run")
    }

    #[tokio::test]
    async fn protection_and_candidates_are_invariant_to_backend_list_order() {
        let normal = report_with_list_order(false).await;
        let reversed = report_with_list_order(true).await;
        assert!(!normal.orphaned_snapshots.is_empty());
        assert!(!normal.old_snapshot_versions.is_empty());
        assert_eq!(normal.orphaned_snapshots, reversed.orphaned_snapshots);
        assert_eq!(normal.old_ledger_events, reversed.old_ledger_events);
        assert_eq!(normal.old_snapshot_versions, reversed.old_snapshot_versions);
        assert_eq!(normal.objects_to_delete, reversed.objects_to_delete);
    }

    #[tokio::test]
    async fn moving_current_head_is_rechecked_before_snapshot_deletion() {
        let backend = Arc::new(RecordingBackend::new(false));
        let storage = ScopedStorage::new(backend.clone(), "acme", "moving-head").expect("storage");
        crate::Tier1Writer::new(storage.clone())
            .initialize()
            .await
            .expect("init");
        let old_path = seed_old_and_current_catalog_snapshots(&storage).await;
        let collector = GarbageCollector::new(
            storage.clone(),
            RetentionPolicy {
                keep_snapshots: 0,
                delay_hours: 0,
                ledger_retention_hours: 0,
                max_age_days: 0,
            },
        );
        let protection = collector
            .load_protection_set(Utc::now())
            .await
            .expect("initial protection");
        let candidates = collector
            .find_old_snapshot_versions(&protection)
            .await
            .expect("old snapshot candidates");
        let old_dir = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1);
        assert!(candidates.contains(&old_dir));

        move_current_catalog_head_to_version_one(&storage).await;
        let mut guard = DistributedLock::new(Arc::new(storage.clone()), RETENTION_GC_LOCK_PATH)
            .acquire(RETENTION_GC_LOCK_TTL, 1)
            .await
            .expect("retention coordination");
        let operation_id = guard.holder_id().to_string();
        let mut epoch = RetentionMutationEpoch::claim(
            storage.clone(),
            &mut guard,
            RetentionMutationKind::CatalogGc,
            operation_id,
        )
        .await
        .expect("durable retention epoch");
        collector
            .gc_old_snapshots(candidates, &protection, &mut epoch)
            .await
            .expect("delete pass");
        epoch.settle().await.expect("settle durable epoch");
        guard.release().await.expect("release coordination");

        assert_eq!(0, backend.delete_calls());
        assert_eq!(
            Bytes::from_static(b"old"),
            storage
                .get_raw(&old_path)
                .await
                .expect("newly current object must survive")
        );
    }
}
