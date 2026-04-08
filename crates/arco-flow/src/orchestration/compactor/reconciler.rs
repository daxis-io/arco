//! Orchestration snapshot reconciliation and orphan cleanup.

use std::collections::BTreeSet;

use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use metrics::counter;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use arco_core::ScopedStorage;

use crate::error::{Error, Result};
use crate::metrics::{labels as metric_labels, names as metric_names};
use crate::paths::{
    orchestration_base_snapshot_dir, orchestration_l0_dir, orchestration_manifest_path,
    orchestration_manifest_pointer_path,
};

use super::manifest::{OrchestrationManifest, OrchestrationManifestPointer};

const ORCHESTRATION_BASE_PREFIX: &str = "state/orchestration/base";
const ORCHESTRATION_L0_PREFIX: &str = "state/orchestration/l0";
const ORCHESTRATION_MANIFEST_PREFIX: &str = "state/orchestration/manifests";
const DEFAULT_MINIMUM_REPORT_AGE_BEFORE_DELETE_SECS: i64 = 60 * 60;
const MAX_CURRENT_HEAD_REPAIR_ATTEMPTS: usize = 4;

/// Why the current pointer-target legacy manifest requires repair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentHeadLegacyManifestIssue {
    /// The legacy manifest path is absent.
    Missing,
    /// The legacy manifest path exists but does not match the current pointer target.
    Stale,
}

enum CurrentHeadRepairOutcome {
    Repaired,
    Skipped,
}

/// Reconciliation report for immutable orchestration artifacts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrationReconciliationReport {
    /// When the report was generated. Repair uses this as the start of the delete quarantine.
    pub checked_at: DateTime<Utc>,
    /// Current immutable manifest snapshot protected by the pointer, when present.
    pub current_manifest_path: Option<String>,
    /// Legacy manifest mirror repair target for the current pointer-target head.
    pub missing_current_head_legacy_manifest_path: Option<String>,
    /// Why the current-head legacy manifest requires repair.
    pub current_head_legacy_manifest_issue: Option<CurrentHeadLegacyManifestIssue>,
    /// Immutable manifest snapshots not targeted by the current pointer.
    pub orphan_manifest_snapshots: Vec<String>,
    /// Base snapshot directories not referenced by the current manifest.
    pub orphan_base_dirs: Vec<String>,
    /// L0 delta directories not referenced by the current manifest.
    pub orphan_l0_dirs: Vec<String>,
}

impl OrchestrationReconciliationReport {
    /// Returns true when any orphaned storage was found.
    #[must_use]
    pub fn has_issues(&self) -> bool {
        self.current_head_legacy_manifest_issue.is_some()
            || !(self.orphan_manifest_snapshots.is_empty()
                && self.orphan_base_dirs.is_empty()
                && self.orphan_l0_dirs.is_empty())
    }
}

/// Result of deleting orphaned orchestration artifacts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OrchestrationRepairResult {
    /// Number of repaired post-commit side-effect paths.
    pub repaired_paths: u64,
    /// Number of repair attempts that failed.
    pub failed_repairs: u64,
    /// Number of deleted objects.
    pub deleted_objects: u64,
    /// Number of skipped protected paths from a stale report.
    pub skipped_paths: u64,
    /// Number of orphan paths deferred until the report ages past the delete quarantine.
    pub deferred_paths: u64,
    /// Total deleted bytes.
    pub deleted_bytes: u64,
}

/// Scope of orchestration repair actions to apply from a reconciliation report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrchestrationRepairScope {
    /// Only repair current-head side effects for the visible pointer target.
    CurrentHeadOnly,
    /// Apply all repairable items, including generic cleanup work such as orphan deletion.
    Full,
}

impl OrchestrationRepairScope {
    const fn allows_generic_cleanup(self) -> bool {
        matches!(self, Self::Full)
    }
}

/// Policy controlling destructive orchestration reconciliation behavior.
#[derive(Debug, Clone)]
pub struct OrchestrationReconciliationPolicy {
    /// Minimum age of a reconciliation report before `repair()` deletes discovered orphan paths.
    pub minimum_report_age_before_delete: Duration,
}

impl Default for OrchestrationReconciliationPolicy {
    fn default() -> Self {
        Self {
            minimum_report_age_before_delete: Duration::seconds(
                DEFAULT_MINIMUM_REPORT_AGE_BEFORE_DELETE_SECS,
            ),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ProtectedReferences {
    current_manifest_path: Option<String>,
    manifest_paths: BTreeSet<String>,
    base_dirs: BTreeSet<String>,
    l0_dirs: BTreeSet<String>,
}

/// Reconciles immutable orchestration state against current manifest references.
#[derive(Clone)]
pub struct OrchestrationReconciler {
    storage: ScopedStorage,
    policy: OrchestrationReconciliationPolicy,
}

impl OrchestrationReconciler {
    /// Creates a new orchestration reconciler.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self::with_policy(storage, OrchestrationReconciliationPolicy::default())
    }

    /// Creates a new orchestration reconciler with an explicit repair policy.
    #[must_use]
    pub fn with_policy(
        storage: ScopedStorage,
        mut policy: OrchestrationReconciliationPolicy,
    ) -> Self {
        if policy.minimum_report_age_before_delete < Duration::zero() {
            policy.minimum_report_age_before_delete = Duration::zero();
        }

        Self { storage, policy }
    }

    /// Detects orphan immutable manifest snapshots and data directories.
    ///
    /// # Errors
    ///
    /// Returns an error if manifests or storage metadata cannot be read.
    pub async fn check(&self) -> Result<OrchestrationReconciliationReport> {
        let protected = self.current_references().await?;
        let (missing_current_head_legacy_manifest_path, current_head_legacy_manifest_issue) =
            match self.read_current_manifest_snapshot_state().await? {
                Some((current_manifest_bytes, _pointer_version)) => {
                    match self.storage.get_raw(orchestration_manifest_path()).await {
                        Ok(legacy_bytes) if legacy_bytes != current_manifest_bytes => {
                            counter!(
                                metric_names::ORCH_RECONCILER_REPAIR_ISSUES_TOTAL,
                                metric_labels::REASON => "legacy_manifest".to_string(),
                            )
                            .increment(1);
                            (
                                Some(orchestration_manifest_path().to_string()),
                                Some(CurrentHeadLegacyManifestIssue::Stale),
                            )
                        }
                        Ok(_) => (None, None),
                        Err(
                            arco_core::Error::NotFound(_)
                            | arco_core::Error::ResourceNotFound { .. },
                        ) => {
                            counter!(
                                metric_names::ORCH_RECONCILER_REPAIR_ISSUES_TOTAL,
                                metric_labels::REASON => "legacy_manifest".to_string(),
                            )
                            .increment(1);
                            (
                                Some(orchestration_manifest_path().to_string()),
                                Some(CurrentHeadLegacyManifestIssue::Missing),
                            )
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                None => (None, None),
            };
        let manifest_paths = self.list_paths(ORCHESTRATION_MANIFEST_PREFIX).await?;
        let base_dirs = self.list_top_level_dirs(ORCHESTRATION_BASE_PREFIX).await?;
        let l0_dirs = self.list_top_level_dirs(ORCHESTRATION_L0_PREFIX).await?;

        let mut orphan_manifest_snapshots: Vec<String> = manifest_paths
            .into_iter()
            .filter(|path| !protected.manifest_paths.contains(path))
            .collect();
        let mut orphan_base_dirs: Vec<String> = base_dirs
            .into_iter()
            .filter(|path| !protected.base_dirs.contains(path))
            .collect();
        let mut orphan_l0_dirs: Vec<String> = l0_dirs
            .into_iter()
            .filter(|path| !protected.l0_dirs.contains(path))
            .collect();

        orphan_manifest_snapshots.sort();
        orphan_base_dirs.sort();
        orphan_l0_dirs.sort();

        counter!(
            metric_names::ORCH_RECONCILER_ORPHANS_TOTAL,
            metric_labels::REASON => "manifest_snapshot".to_string(),
        )
        .increment(u64::try_from(orphan_manifest_snapshots.len()).unwrap_or(u64::MAX));
        counter!(
            metric_names::ORCH_RECONCILER_ORPHANS_TOTAL,
            metric_labels::REASON => "base_dir".to_string(),
        )
        .increment(u64::try_from(orphan_base_dirs.len()).unwrap_or(u64::MAX));
        counter!(
            metric_names::ORCH_RECONCILER_ORPHANS_TOTAL,
            metric_labels::REASON => "l0_dir".to_string(),
        )
        .increment(u64::try_from(orphan_l0_dirs.len()).unwrap_or(u64::MAX));

        Ok(OrchestrationReconciliationReport {
            checked_at: Utc::now(),
            current_manifest_path: protected.current_manifest_path,
            missing_current_head_legacy_manifest_path,
            current_head_legacy_manifest_issue,
            orphan_manifest_snapshots,
            orphan_base_dirs,
            orphan_l0_dirs,
        })
    }

    /// Deletes orphan immutable manifest snapshots and data directories.
    ///
    /// # Errors
    ///
    /// Returns an error if storage reads or deletes fail.
    #[allow(clippy::too_many_lines)]
    pub async fn repair(
        &self,
        report: &OrchestrationReconciliationReport,
    ) -> Result<OrchestrationRepairResult> {
        self.repair_with_scope(report, OrchestrationRepairScope::Full)
            .await
    }

    /// Repairs orchestration issues found in a reconciliation report using an explicit scope.
    ///
    /// # Errors
    ///
    /// Returns an error if storage reads or deletes fail.
    #[allow(clippy::too_many_lines)]
    pub async fn repair_with_scope(
        &self,
        report: &OrchestrationReconciliationReport,
        scope: OrchestrationRepairScope,
    ) -> Result<OrchestrationRepairResult> {
        let protected = self.current_references().await?;
        let report_ready_for_delete = self.report_ready_for_delete(report);
        let mut result = OrchestrationRepairResult::default();

        if report.current_head_legacy_manifest_issue.is_some() {
            if let Some(legacy_path) = &report.missing_current_head_legacy_manifest_path {
                match self.repair_current_head_legacy_manifest(legacy_path).await {
                    Ok(CurrentHeadRepairOutcome::Repaired) => {
                        result.repaired_paths += 1;
                        counter!(
                            metric_names::ORCH_RECONCILER_REPAIRS_TOTAL,
                            metric_labels::REASON => "legacy_manifest".to_string(),
                            metric_labels::STATUS => "repaired".to_string(),
                        )
                        .increment(1);
                    }
                    Ok(CurrentHeadRepairOutcome::Skipped) => {
                        result.skipped_paths += 1;
                        counter!(
                            metric_names::ORCH_RECONCILER_REPAIRS_TOTAL,
                            metric_labels::REASON => "legacy_manifest".to_string(),
                            metric_labels::STATUS => "skipped".to_string(),
                        )
                        .increment(1);
                    }
                    Err(error) => {
                        result.failed_repairs += 1;
                        counter!(
                            metric_names::ORCH_RECONCILER_REPAIRS_TOTAL,
                            metric_labels::REASON => "legacy_manifest".to_string(),
                            metric_labels::STATUS => "failed".to_string(),
                        )
                        .increment(1);
                        tracing::error!(
                            path = %legacy_path,
                            error = %error,
                            "failed to repair current-head orchestration legacy manifest"
                        );
                    }
                }
            }
        }

        for manifest_path in &report.orphan_manifest_snapshots {
            if !scope.allows_generic_cleanup() {
                result.skipped_paths += 1;
                counter!(
                    metric_names::ORCH_RECONCILER_REPAIRS_TOTAL,
                    metric_labels::REASON => "manifest_snapshot".to_string(),
                    metric_labels::STATUS => "skipped".to_string(),
                )
                .increment(1);
                continue;
            }
            if protected.manifest_paths.contains(manifest_path) {
                result.skipped_paths += 1;
                counter!(
                    metric_names::ORCH_RECONCILER_SKIPPED_PATHS_TOTAL,
                    metric_labels::REASON => "manifest_snapshot".to_string(),
                )
                .increment(1);
                continue;
            }
            if !report_ready_for_delete {
                result.deferred_paths += 1;
                counter!(
                    metric_names::ORCH_RECONCILER_DEFERRED_PATHS_TOTAL,
                    metric_labels::REASON => "manifest_snapshot".to_string(),
                )
                .increment(1);
                continue;
            }

            if let Some(meta) = self.storage.head_raw(manifest_path).await? {
                self.storage.delete(manifest_path).await?;
                result.deleted_objects += 1;
                result.deleted_bytes += meta.size;
                counter!(
                    metric_names::ORCH_RECONCILER_DELETES_TOTAL,
                    metric_labels::REASON => "manifest_snapshot".to_string(),
                )
                .increment(1);
                counter!(
                    metric_names::ORCH_RECONCILER_DELETED_BYTES_TOTAL,
                    metric_labels::REASON => "manifest_snapshot".to_string(),
                )
                .increment(meta.size);
            }
        }

        for prefix in &report.orphan_base_dirs {
            if !scope.allows_generic_cleanup() {
                result.skipped_paths += 1;
                counter!(
                    metric_names::ORCH_RECONCILER_REPAIRS_TOTAL,
                    metric_labels::REASON => "base_dir".to_string(),
                    metric_labels::STATUS => "skipped".to_string(),
                )
                .increment(1);
                continue;
            }
            if protected.base_dirs.contains(prefix) {
                result.skipped_paths += 1;
                counter!(
                    metric_names::ORCH_RECONCILER_SKIPPED_PATHS_TOTAL,
                    metric_labels::REASON => "base_dir".to_string(),
                )
                .increment(1);
                continue;
            }
            if !report_ready_for_delete {
                result.deferred_paths += 1;
                counter!(
                    metric_names::ORCH_RECONCILER_DEFERRED_PATHS_TOTAL,
                    metric_labels::REASON => "base_dir".to_string(),
                )
                .increment(1);
                continue;
            }

            let (deleted_objects, deleted_bytes) = self.delete_prefix(prefix).await?;
            result.deleted_objects += deleted_objects;
            result.deleted_bytes += deleted_bytes;
            if deleted_objects > 0 {
                counter!(
                    metric_names::ORCH_RECONCILER_DELETES_TOTAL,
                    metric_labels::REASON => "base_dir".to_string(),
                )
                .increment(deleted_objects);
            }
            if deleted_bytes > 0 {
                counter!(
                    metric_names::ORCH_RECONCILER_DELETED_BYTES_TOTAL,
                    metric_labels::REASON => "base_dir".to_string(),
                )
                .increment(deleted_bytes);
            }
        }

        for prefix in &report.orphan_l0_dirs {
            if !scope.allows_generic_cleanup() {
                result.skipped_paths += 1;
                counter!(
                    metric_names::ORCH_RECONCILER_REPAIRS_TOTAL,
                    metric_labels::REASON => "l0_dir".to_string(),
                    metric_labels::STATUS => "skipped".to_string(),
                )
                .increment(1);
                continue;
            }
            if protected.l0_dirs.contains(prefix) {
                result.skipped_paths += 1;
                counter!(
                    metric_names::ORCH_RECONCILER_SKIPPED_PATHS_TOTAL,
                    metric_labels::REASON => "l0_dir".to_string(),
                )
                .increment(1);
                continue;
            }
            if !report_ready_for_delete {
                result.deferred_paths += 1;
                counter!(
                    metric_names::ORCH_RECONCILER_DEFERRED_PATHS_TOTAL,
                    metric_labels::REASON => "l0_dir".to_string(),
                )
                .increment(1);
                continue;
            }

            let (deleted_objects, deleted_bytes) = self.delete_prefix(prefix).await?;
            result.deleted_objects += deleted_objects;
            result.deleted_bytes += deleted_bytes;
            if deleted_objects > 0 {
                counter!(
                    metric_names::ORCH_RECONCILER_DELETES_TOTAL,
                    metric_labels::REASON => "l0_dir".to_string(),
                )
                .increment(deleted_objects);
            }
            if deleted_bytes > 0 {
                counter!(
                    metric_names::ORCH_RECONCILER_DELETED_BYTES_TOTAL,
                    metric_labels::REASON => "l0_dir".to_string(),
                )
                .increment(deleted_bytes);
            }
        }

        Ok(result)
    }

    fn report_ready_for_delete(&self, report: &OrchestrationReconciliationReport) -> bool {
        Utc::now().signed_duration_since(report.checked_at)
            >= self.policy.minimum_report_age_before_delete
    }

    async fn current_references(&self) -> Result<ProtectedReferences> {
        let (current_manifest_path, manifest) = self.read_current_manifest().await?;
        let mut protected = ProtectedReferences {
            current_manifest_path: current_manifest_path.clone(),
            ..ProtectedReferences::default()
        };
        if let Some(path) = current_manifest_path.clone() {
            protected.manifest_paths.insert(path);
        }

        Self::protect_manifest_artifacts(&manifest, &mut protected);

        let mut visited_paths = BTreeSet::new();
        if let Some(path) = current_manifest_path {
            visited_paths.insert(path);
        }
        let mut previous_manifest_path = manifest.previous_manifest_path.clone();
        while let Some(path) = previous_manifest_path.clone() {
            if !visited_paths.insert(path.clone()) {
                return Err(Error::storage(format!(
                    "cycle detected in orchestration manifest chain at {path}"
                )));
            }

            let manifest = self.read_manifest_at(&path).await?;
            if path.starts_with(&format!("{ORCHESTRATION_MANIFEST_PREFIX}/")) {
                protected.manifest_paths.insert(path.clone());
            }
            Self::protect_manifest_artifacts(&manifest, &mut protected);
            previous_manifest_path.clone_from(&manifest.previous_manifest_path);
        }

        Ok(protected)
    }

    fn protect_manifest_artifacts(
        manifest: &OrchestrationManifest,
        protected: &mut ProtectedReferences,
    ) {
        if let Some(snapshot_id) = &manifest.base_snapshot.snapshot_id {
            protected
                .base_dirs
                .insert(orchestration_base_snapshot_dir(snapshot_id));
        }

        for delta in &manifest.l0_deltas {
            protected
                .l0_dirs
                .insert(orchestration_l0_dir(&delta.delta_id));
        }
    }

    async fn read_manifest_at(&self, path: &str) -> Result<OrchestrationManifest> {
        let manifest_bytes = self.storage.get_raw(path).await?;
        serde_json::from_slice(&manifest_bytes).map_err(|e| Error::Serialization {
            message: format!("failed to parse manifest at {path}: {e}"),
        })
    }

    async fn read_current_manifest(&self) -> Result<(Option<String>, OrchestrationManifest)> {
        let pointer_path = orchestration_manifest_pointer_path();
        match self.storage.get_raw(pointer_path).await {
            Ok(pointer_bytes) => {
                let pointer: OrchestrationManifestPointer = serde_json::from_slice(&pointer_bytes)
                    .map_err(|e| Error::Serialization {
                        message: format!("failed to parse manifest pointer: {e}"),
                    })?;

                match self.storage.get_raw(&pointer.manifest_path).await {
                    Ok(manifest_bytes) => {
                        let mut manifest: OrchestrationManifest =
                            serde_json::from_slice(&manifest_bytes).map_err(|e| {
                                Error::Serialization {
                                    message: format!("failed to parse manifest: {e}"),
                                }
                            })?;
                        manifest.manifest_id.clone_from(&pointer.manifest_id);
                        manifest.epoch = manifest.epoch.max(pointer.epoch);
                        Ok((Some(pointer.manifest_path), manifest))
                    }
                    Err(
                        arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. },
                    ) => self.read_legacy_manifest().await,
                    Err(error) => Err(error.into()),
                }
            }
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                self.read_legacy_manifest().await
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn read_current_manifest_snapshot_state(&self) -> Result<Option<(Bytes, String)>> {
        let pointer_path = orchestration_manifest_pointer_path();
        let Some(pointer_meta) = self.storage.head_raw(pointer_path).await? else {
            return Ok(None);
        };
        match self.storage.get_raw(pointer_path).await {
            Ok(pointer_bytes) => {
                let pointer: OrchestrationManifestPointer = serde_json::from_slice(&pointer_bytes)
                    .map_err(|e| Error::Serialization {
                        message: format!("failed to parse manifest pointer: {e}"),
                    })?;
                match self.storage.get_raw(&pointer.manifest_path).await {
                    Ok(manifest_bytes) => Ok(Some((manifest_bytes, pointer_meta.version))),
                    Err(
                        arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. },
                    ) => Ok(None),
                    Err(error) => Err(error.into()),
                }
            }
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn repair_current_head_legacy_manifest(
        &self,
        legacy_path: &str,
    ) -> Result<CurrentHeadRepairOutcome> {
        for attempt in 1..=MAX_CURRENT_HEAD_REPAIR_ATTEMPTS {
            let Some((current_manifest_bytes, pointer_version)) =
                self.read_current_manifest_snapshot_state().await?
            else {
                return Ok(CurrentHeadRepairOutcome::Skipped);
            };

            let legacy_in_sync = match self.storage.get_raw(legacy_path).await {
                Ok(legacy_bytes) => legacy_bytes == current_manifest_bytes,
                Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                    false
                }
                Err(error) => return Err(error.into()),
            };
            if legacy_in_sync {
                return Ok(CurrentHeadRepairOutcome::Skipped);
            }

            self.storage
                .put_raw(
                    legacy_path,
                    current_manifest_bytes.clone(),
                    arco_core::WritePrecondition::None,
                )
                .await?;

            let Some((latest_manifest_bytes, latest_pointer_version)) =
                self.read_current_manifest_snapshot_state().await?
            else {
                return Ok(CurrentHeadRepairOutcome::Repaired);
            };
            if latest_pointer_version == pointer_version
                || latest_manifest_bytes == current_manifest_bytes
            {
                return Ok(CurrentHeadRepairOutcome::Repaired);
            }
            if attempt == MAX_CURRENT_HEAD_REPAIR_ATTEMPTS {
                return Err(Error::Core(arco_core::Error::PreconditionFailed {
                    message: "orchestration head advanced while repairing legacy manifest"
                        .to_string(),
                }));
            }
        }

        Ok(CurrentHeadRepairOutcome::Skipped)
    }

    async fn read_legacy_manifest(&self) -> Result<(Option<String>, OrchestrationManifest)> {
        match self.storage.get_raw(orchestration_manifest_path()).await {
            Ok(manifest_bytes) => {
                let manifest: OrchestrationManifest = serde_json::from_slice(&manifest_bytes)
                    .map_err(|e| Error::Serialization {
                        message: format!("failed to parse manifest: {e}"),
                    })?;
                Ok((None, manifest))
            }
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                Ok((None, OrchestrationManifest::new(Ulid::new().to_string())))
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn list_paths(&self, prefix: &str) -> Result<Vec<String>> {
        let metas = self.storage.list_meta(prefix).await?;
        Ok(metas
            .into_iter()
            .map(|meta| meta.path.to_string())
            .collect())
    }

    async fn list_top_level_dirs(&self, prefix: &str) -> Result<Vec<String>> {
        let metas = self.storage.list_meta(prefix).await?;
        let mut dirs = BTreeSet::new();
        let scoped_prefix = format!("{prefix}/");

        for meta in metas {
            let path = meta.path.to_string();
            if let Some(rest) = path.strip_prefix(&scoped_prefix) {
                if let Some((dir, _)) = rest.split_once('/') {
                    dirs.insert(format!("{prefix}/{dir}"));
                }
            }
        }

        Ok(dirs.into_iter().collect())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(u64, u64)> {
        let entries = self.storage.list_meta(prefix).await?;
        let mut deleted_objects = 0;
        let mut deleted_bytes = 0;

        for entry in entries {
            let path = entry.path.to_string();
            self.storage.delete(&path).await?;
            deleted_objects += 1;
            deleted_bytes += entry.size;
        }

        Ok((deleted_objects, deleted_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicBool, Ordering};

    use arco_core::WritePrecondition;
    use arco_core::storage::{MemoryBackend, ObjectMeta, StorageBackend};
    use async_trait::async_trait;
    use bytes::Bytes;
    use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

    use crate::paths::orchestration_manifest_snapshot_path;

    use super::super::manifest::{BaseSnapshot, L0Delta, RowCounts, TableArtifact, TablePaths};

    fn artifact(path: &str) -> TableArtifact {
        TableArtifact::new(path, "sha256:abcd", 4)
    }

    async fn create_storage() -> Result<ScopedStorage> {
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "acme", "prod").map_err(Error::from)
    }

    fn init_metrics() -> PrometheusHandle {
        static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

        PROMETHEUS_HANDLE
            .get_or_init(|| {
                PrometheusBuilder::new()
                    .install_recorder()
                    .expect("install prometheus recorder for reconciler tests")
            })
            .clone()
    }

    fn assert_metric_lines_contain(metrics: &str, name: &str, needle: &str) {
        let lines: Vec<&str> = metrics
            .lines()
            .filter(|line| line.starts_with(name))
            .collect();
        assert!(!lines.is_empty(), "missing metric {name} in {metrics}");
        assert!(
            lines.iter().any(|line| line.contains(needle)),
            "missing {needle} on metric {name} in {metrics}"
        );
    }

    #[derive(Debug)]
    struct OrchestrationRepairRaceBackend {
        inner: MemoryBackend,
        legacy_path: String,
        pointer_path: String,
        advanced_pointer_bytes: Bytes,
        inject_once: AtomicBool,
    }

    impl OrchestrationRepairRaceBackend {
        fn new(
            legacy_path: impl Into<String>,
            pointer_path: impl Into<String>,
            advanced_pointer_bytes: Bytes,
        ) -> Self {
            Self {
                inner: MemoryBackend::new(),
                legacy_path: legacy_path.into(),
                pointer_path: pointer_path.into(),
                advanced_pointer_bytes,
                inject_once: AtomicBool::new(true),
            }
        }
    }

    #[async_trait]
    impl StorageBackend for OrchestrationRepairRaceBackend {
        async fn get(&self, path: &str) -> arco_core::error::Result<Bytes> {
            self.inner.get(path).await
        }

        async fn get_range(
            &self,
            path: &str,
            range: Range<u64>,
        ) -> arco_core::error::Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::error::Result<arco_core::WriteResult> {
            if path.ends_with(&self.legacy_path)
                && matches!(precondition, WritePrecondition::None)
                && self.inject_once.swap(false, Ordering::SeqCst)
            {
                let scope_prefix = path
                    .strip_suffix(&self.legacy_path)
                    .expect("legacy suffix must match");
                let scoped_pointer_path = format!("{scope_prefix}{}", self.pointer_path);
                self.inner
                    .put(
                        &scoped_pointer_path,
                        self.advanced_pointer_bytes.clone(),
                        WritePrecondition::None,
                    )
                    .await?;
            }

            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::error::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::error::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> arco_core::error::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(
            &self,
            path: &str,
            expiry: std::time::Duration,
        ) -> arco_core::error::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    async fn write_manifest_snapshot(
        storage: &ScopedStorage,
        manifest_id: &str,
        previous_manifest_path: Option<String>,
        base_snapshot_id: &str,
        delta_id: &str,
        artifact_suffix: &str,
    ) -> Result<(String, String, String)> {
        let manifest_path = orchestration_manifest_snapshot_path(manifest_id);
        let base_dir = orchestration_base_snapshot_dir(base_snapshot_id);
        let l0_dir = orchestration_l0_dir(delta_id);
        let base_path = format!("{base_dir}/runs.{artifact_suffix}.parquet");
        let l0_path = format!("{l0_dir}/runs.{artifact_suffix}.parquet");
        let manifest = OrchestrationManifest {
            manifest_id: manifest_id.to_string(),
            epoch: 1,
            previous_manifest_path,
            schema_version: 1,
            revision_ulid: "01HQXYZ123REV".to_string(),
            published_at: Utc::now(),
            watermarks: Default::default(),
            base_snapshot: BaseSnapshot {
                snapshot_id: Some(base_snapshot_id.to_string()),
                published_at: Utc::now(),
                tables: TablePaths {
                    runs: Some(artifact(&base_path)),
                    ..Default::default()
                },
            },
            l0_deltas: vec![L0Delta {
                delta_id: delta_id.to_string(),
                created_at: Utc::now(),
                event_range: super::super::manifest::EventRange {
                    from_event: "evt-01".to_string(),
                    to_event: "evt-01".to_string(),
                    event_count: 1,
                },
                tables: TablePaths {
                    runs: Some(artifact(&l0_path)),
                    ..Default::default()
                },
                row_counts: RowCounts {
                    runs: 1,
                    ..Default::default()
                },
            }],
            l0_count: 1,
            l0_limits: Default::default(),
        };

        storage
            .put_raw(
                &manifest_path,
                Bytes::from(serde_json::to_vec(&manifest).expect("serialize manifest")),
                WritePrecondition::DoesNotExist,
            )
            .await?;

        storage
            .put_raw(
                &base_path,
                Bytes::from_static(b"base"),
                WritePrecondition::None,
            )
            .await?;
        storage
            .put_raw(&l0_path, Bytes::from_static(b"l0"), WritePrecondition::None)
            .await?;

        Ok((manifest_path, base_dir, l0_dir))
    }

    async fn seed_current_manifest(storage: &ScopedStorage) -> Result<(String, String, String)> {
        let (manifest_path, base_dir, l0_dir) = write_manifest_snapshot(
            storage,
            "00000000000000000001",
            None,
            "base-current",
            "delta-current",
            "current",
        )
        .await?;

        let pointer = OrchestrationManifestPointer {
            manifest_id: "00000000000000000001".to_string(),
            manifest_path: manifest_path.clone(),
            epoch: 1,
            parent_pointer_hash: None,
            updated_at: Utc::now(),
        };
        storage
            .put_raw(
                orchestration_manifest_pointer_path(),
                Bytes::from(serde_json::to_vec(&pointer).expect("serialize pointer")),
                WritePrecondition::DoesNotExist,
            )
            .await?;

        Ok((manifest_path, base_dir, l0_dir))
    }

    async fn seed_manifest_history(
        storage: &ScopedStorage,
    ) -> Result<((String, String, String), (String, String, String))> {
        let history = write_manifest_snapshot(
            storage,
            "00000000000000000001",
            None,
            "base-history",
            "delta-history",
            "history",
        )
        .await?;
        let current = write_manifest_snapshot(
            storage,
            "00000000000000000002",
            Some(history.0.clone()),
            "base-current",
            "delta-current",
            "current",
        )
        .await?;

        let pointer = OrchestrationManifestPointer {
            manifest_id: "00000000000000000002".to_string(),
            manifest_path: current.0.clone(),
            epoch: 1,
            parent_pointer_hash: None,
            updated_at: Utc::now(),
        };
        storage
            .put_raw(
                orchestration_manifest_pointer_path(),
                Bytes::from(serde_json::to_vec(&pointer).expect("serialize pointer")),
                WritePrecondition::DoesNotExist,
            )
            .await?;

        Ok((current, history))
    }

    #[tokio::test]
    async fn check_reports_only_orphaned_orchestration_artifacts() -> Result<()> {
        let storage = create_storage().await?;
        let (current_manifest_path, current_base_dir, current_l0_dir) =
            seed_current_manifest(&storage).await?;

        let orphan_manifest_path = orchestration_manifest_snapshot_path("00000000000000000002");
        storage
            .put_raw(
                &orphan_manifest_path,
                Bytes::from_static(b"{}"),
                WritePrecondition::DoesNotExist,
            )
            .await?;

        let orphan_base_dir = orchestration_base_snapshot_dir("base-orphan");
        storage
            .put_raw(
                &format!("{orphan_base_dir}/runs.old.parquet"),
                Bytes::from_static(b"orphan-base"),
                WritePrecondition::None,
            )
            .await?;

        let orphan_l0_dir = orchestration_l0_dir("delta-orphan");
        storage
            .put_raw(
                &format!("{orphan_l0_dir}/runs.old.parquet"),
                Bytes::from_static(b"orphan-l0"),
                WritePrecondition::None,
            )
            .await?;

        let report = OrchestrationReconciler::new(storage).check().await?;

        assert_eq!(
            report.current_manifest_path,
            Some(current_manifest_path.clone())
        );
        assert_eq!(report.orphan_manifest_snapshots, vec![orphan_manifest_path]);
        assert_eq!(report.orphan_base_dirs, vec![orphan_base_dir]);
        assert_eq!(report.orphan_l0_dirs, vec![orphan_l0_dir]);
        assert!(!report.orphan_base_dirs.contains(&current_base_dir));
        assert!(!report.orphan_l0_dirs.contains(&current_l0_dir));

        Ok(())
    }

    #[tokio::test]
    async fn repair_restores_missing_current_head_legacy_manifest() -> Result<()> {
        let storage = create_storage().await?;
        let (current_manifest_path, _current_base_dir, _current_l0_dir) =
            seed_current_manifest(&storage).await?;
        storage.delete(orchestration_manifest_path()).await?;

        let reconciler = OrchestrationReconciler::new(storage.clone());
        let report = reconciler.check().await?;
        assert_eq!(
            report.missing_current_head_legacy_manifest_path.as_deref(),
            Some(orchestration_manifest_path())
        );
        assert_eq!(
            report.current_head_legacy_manifest_issue,
            Some(CurrentHeadLegacyManifestIssue::Missing)
        );

        let result = reconciler.repair(&report).await?;
        assert_eq!(result.repaired_paths, 1);
        assert_eq!(result.failed_repairs, 0);

        let legacy_bytes = storage.get_raw(orchestration_manifest_path()).await?;
        let current_bytes = storage.get_raw(&current_manifest_path).await?;
        assert_eq!(legacy_bytes, current_bytes);

        let post_repair = reconciler.check().await?;
        assert_eq!(post_repair.missing_current_head_legacy_manifest_path, None);
        assert_eq!(post_repair.current_head_legacy_manifest_issue, None);

        Ok(())
    }

    #[tokio::test]
    async fn repair_restores_stale_current_head_legacy_manifest_after_pointer_advances()
    -> Result<()> {
        let advanced_manifest_path = orchestration_manifest_snapshot_path("00000000000000000002");
        let advanced_pointer = OrchestrationManifestPointer {
            manifest_id: "00000000000000000002".to_string(),
            manifest_path: advanced_manifest_path.clone(),
            epoch: 2,
            parent_pointer_hash: None,
            updated_at: Utc::now(),
        };
        let backend = Arc::new(OrchestrationRepairRaceBackend::new(
            orchestration_manifest_path(),
            orchestration_manifest_pointer_path(),
            Bytes::from(serde_json::to_vec(&advanced_pointer).expect("serialize advanced pointer")),
        ));
        let storage = ScopedStorage::new(backend, "acme", "prod").map_err(Error::from)?;
        let (current_manifest_path, _current_base_dir, _current_l0_dir) =
            seed_current_manifest(&storage).await?;

        let (history_manifest_path, _history_base_dir, _history_l0_dir) = write_manifest_snapshot(
            &storage,
            "00000000000000000000",
            None,
            "base-history",
            "delta-history",
            "history",
        )
        .await?;
        let history_bytes = storage.get_raw(&history_manifest_path).await?;
        storage
            .put_raw(
                orchestration_manifest_path(),
                history_bytes,
                WritePrecondition::None,
            )
            .await?;

        let (_advanced_manifest_path, _advanced_base_dir, _advanced_l0_dir) =
            write_manifest_snapshot(
                &storage,
                "00000000000000000002",
                Some(current_manifest_path),
                "base-advanced",
                "delta-advanced",
                "advanced",
            )
            .await?;

        let reconciler = OrchestrationReconciler::new(storage.clone());
        let report = reconciler.check().await?;
        assert_eq!(
            report.missing_current_head_legacy_manifest_path.as_deref(),
            Some(orchestration_manifest_path())
        );
        assert_eq!(
            report.current_head_legacy_manifest_issue,
            Some(CurrentHeadLegacyManifestIssue::Stale)
        );

        let result = reconciler.repair(&report).await?;
        assert_eq!(result.failed_repairs, 0);

        let legacy_bytes = storage.get_raw(orchestration_manifest_path()).await?;
        let advanced_bytes = storage.get_raw(&advanced_manifest_path).await?;
        assert_eq!(
            legacy_bytes, advanced_bytes,
            "repair must converge to the latest pointer target even if the pointer advances mid-repair"
        );

        let post_repair = reconciler.check().await?;
        assert_eq!(post_repair.missing_current_head_legacy_manifest_path, None);
        assert_eq!(post_repair.current_head_legacy_manifest_issue, None);

        Ok(())
    }

    #[tokio::test]
    async fn repair_defers_fresh_orphans_until_report_ages_out() -> Result<()> {
        let storage = create_storage().await?;
        let (_current_manifest_path, _current_base_dir, _current_l0_dir) =
            seed_current_manifest(&storage).await?;

        let orphan_manifest_path = orchestration_manifest_snapshot_path("00000000000000000002");
        let orphan_base_path = format!(
            "{}/runs.old.parquet",
            orchestration_base_snapshot_dir("base-orphan")
        );
        let orphan_l0_path = format!("{}/runs.old.parquet", orchestration_l0_dir("delta-orphan"));
        storage
            .put_raw(
                &orphan_manifest_path,
                Bytes::from_static(b"{}"),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        storage
            .put_raw(
                &orphan_base_path,
                Bytes::from_static(b"orphan-base"),
                WritePrecondition::None,
            )
            .await?;
        storage
            .put_raw(
                &orphan_l0_path,
                Bytes::from_static(b"orphan-l0"),
                WritePrecondition::None,
            )
            .await?;

        let reconciler = OrchestrationReconciler::new(storage.clone());
        let report = reconciler.check().await?;
        let result = reconciler.repair(&report).await?;

        assert_eq!(result.deleted_objects, 0);
        assert_eq!(result.deferred_paths, 3);
        assert!(storage.head_raw(&orphan_manifest_path).await?.is_some());
        assert!(storage.head_raw(&orphan_base_path).await?.is_some());
        assert!(storage.head_raw(&orphan_l0_path).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn repair_deletes_aged_orphans_without_touching_current_targets() -> Result<()> {
        let storage = create_storage().await?;
        let (current_manifest_path, current_base_dir, current_l0_dir) =
            seed_current_manifest(&storage).await?;

        let orphan_manifest_path = orchestration_manifest_snapshot_path("00000000000000000002");
        storage
            .put_raw(
                &orphan_manifest_path,
                Bytes::from_static(b"{}"),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        let orphan_base_path = format!(
            "{}/runs.old.parquet",
            orchestration_base_snapshot_dir("base-orphan")
        );
        let orphan_l0_path = format!("{}/runs.old.parquet", orchestration_l0_dir("delta-orphan"));
        storage
            .put_raw(
                &orphan_base_path,
                Bytes::from_static(b"orphan-base"),
                WritePrecondition::None,
            )
            .await?;
        storage
            .put_raw(
                &orphan_l0_path,
                Bytes::from_static(b"orphan-l0"),
                WritePrecondition::None,
            )
            .await?;

        let reconciler = OrchestrationReconciler::new(storage.clone());
        let mut report = reconciler.check().await?;
        report.checked_at -= Duration::hours(2);
        let result = reconciler.repair(&report).await?;

        assert!(result.deleted_objects >= 3);
        assert_eq!(result.deferred_paths, 0);
        assert!(storage.head_raw(&orphan_manifest_path).await?.is_none());
        assert!(storage.head_raw(&orphan_base_path).await?.is_none());
        assert!(storage.head_raw(&orphan_l0_path).await?.is_none());
        assert!(storage.head_raw(&current_manifest_path).await?.is_some());
        assert!(
            storage
                .list_meta(&current_base_dir)
                .await?
                .iter()
                .any(|meta| meta.path.as_str().ends_with(".parquet"))
        );
        assert!(
            storage
                .list_meta(&current_l0_dir)
                .await?
                .iter()
                .any(|meta| meta.path.as_str().ends_with(".parquet"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn repair_with_current_head_only_scope_skips_aged_generic_cleanup_items() -> Result<()> {
        let storage = create_storage().await?;
        let (_current_manifest_path, _current_base_dir, _current_l0_dir) =
            seed_current_manifest(&storage).await?;

        let orphan_manifest_path = orchestration_manifest_snapshot_path("00000000000000000002");
        storage
            .put_raw(
                &orphan_manifest_path,
                Bytes::from_static(b"{}"),
                WritePrecondition::DoesNotExist,
            )
            .await?;

        let reconciler = OrchestrationReconciler::new(storage.clone());
        let mut report = reconciler.check().await?;
        report.checked_at -= Duration::hours(2);

        let result = reconciler
            .repair_with_scope(&report, OrchestrationRepairScope::CurrentHeadOnly)
            .await?;

        assert_eq!(result.deleted_objects, 0);
        assert_eq!(result.deferred_paths, 0);
        assert_eq!(result.skipped_paths, 1);
        assert!(
            storage.head_raw(&orphan_manifest_path).await?.is_some(),
            "current-head-only repair must not delete aged generic cleanup candidates"
        );

        Ok(())
    }

    #[tokio::test]
    async fn repair_skips_stale_report_for_current_pointer_targets() -> Result<()> {
        let storage = create_storage().await?;
        let (current_manifest_path, current_base_dir, current_l0_dir) =
            seed_current_manifest(&storage).await?;

        let report = OrchestrationReconciliationReport {
            checked_at: Utc::now(),
            current_manifest_path: Some(current_manifest_path.clone()),
            missing_current_head_legacy_manifest_path: None,
            current_head_legacy_manifest_issue: None,
            orphan_manifest_snapshots: vec![current_manifest_path.clone()],
            orphan_base_dirs: vec![current_base_dir.clone()],
            orphan_l0_dirs: vec![current_l0_dir.clone()],
        };

        let result = OrchestrationReconciler::new(storage.clone())
            .repair(&report)
            .await?;

        assert_eq!(result.deleted_objects, 0);
        assert_eq!(result.skipped_paths, 3);
        assert_eq!(result.deferred_paths, 0);
        assert!(storage.head_raw(&current_manifest_path).await?.is_some());
        assert!(!storage.list_meta(&current_base_dir).await?.is_empty());
        assert!(!storage.list_meta(&current_l0_dir).await?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn check_preserves_reachable_manifest_history_and_artifacts() -> Result<()> {
        let storage = create_storage().await?;
        let (
            (_current_manifest_path, current_base_dir, current_l0_dir),
            (history_manifest_path, history_base_dir, history_l0_dir),
        ) = seed_manifest_history(&storage).await?;

        let orphan_manifest_path = orchestration_manifest_snapshot_path("00000000000000000003");
        storage
            .put_raw(
                &orphan_manifest_path,
                Bytes::from_static(b"{}"),
                WritePrecondition::DoesNotExist,
            )
            .await?;

        let orphan_base_dir = orchestration_base_snapshot_dir("base-orphan");
        let orphan_l0_dir = orchestration_l0_dir("delta-orphan");
        storage
            .put_raw(
                &format!("{orphan_base_dir}/runs.old.parquet"),
                Bytes::from_static(b"orphan-base"),
                WritePrecondition::None,
            )
            .await?;
        storage
            .put_raw(
                &format!("{orphan_l0_dir}/runs.old.parquet"),
                Bytes::from_static(b"orphan-l0"),
                WritePrecondition::None,
            )
            .await?;

        let report = OrchestrationReconciler::new(storage).check().await?;

        assert!(
            !report
                .orphan_manifest_snapshots
                .contains(&history_manifest_path),
            "reachable manifest history must stay protected"
        );
        assert!(
            !report.orphan_base_dirs.contains(&history_base_dir),
            "base dir referenced by reachable history must stay protected"
        );
        assert!(
            !report.orphan_l0_dirs.contains(&history_l0_dir),
            "l0 dir referenced by reachable history must stay protected"
        );
        assert!(
            !report.orphan_base_dirs.contains(&current_base_dir)
                && !report.orphan_l0_dirs.contains(&current_l0_dir),
            "reachable current artifacts must stay protected: {report:?}"
        );
        assert_eq!(report.orphan_manifest_snapshots, vec![orphan_manifest_path]);
        assert_eq!(report.orphan_base_dirs, vec![orphan_base_dir]);
        assert_eq!(report.orphan_l0_dirs, vec![orphan_l0_dir]);

        Ok(())
    }

    #[tokio::test]
    async fn repair_keeps_reachable_manifest_history_and_artifacts() -> Result<()> {
        let storage = create_storage().await?;
        let (
            (_current_manifest_path, _current_base_dir, _current_l0_dir),
            (history_manifest_path, history_base_dir, history_l0_dir),
        ) = seed_manifest_history(&storage).await?;

        let history_base_path = format!("{history_base_dir}/runs.history.parquet");
        let history_l0_path = format!("{history_l0_dir}/runs.history.parquet");
        let orphan_manifest_path = orchestration_manifest_snapshot_path("00000000000000000003");
        let orphan_base_path = format!(
            "{}/runs.old.parquet",
            orchestration_base_snapshot_dir("base-orphan")
        );
        let orphan_l0_path = format!("{}/runs.old.parquet", orchestration_l0_dir("delta-orphan"));

        storage
            .put_raw(
                &orphan_manifest_path,
                Bytes::from_static(b"{}"),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        storage
            .put_raw(
                &orphan_base_path,
                Bytes::from_static(b"orphan-base"),
                WritePrecondition::None,
            )
            .await?;
        storage
            .put_raw(
                &orphan_l0_path,
                Bytes::from_static(b"orphan-l0"),
                WritePrecondition::None,
            )
            .await?;

        let reconciler = OrchestrationReconciler::new(storage.clone());
        let mut report = reconciler.check().await?;
        report.checked_at -= Duration::hours(2);
        let result = reconciler.repair(&report).await?;

        assert_eq!(result.deferred_paths, 0);
        assert!(storage.head_raw(&history_manifest_path).await?.is_some());
        assert!(storage.head_raw(&history_base_path).await?.is_some());
        assert!(storage.head_raw(&history_l0_path).await?.is_some());
        assert!(storage.head_raw(&orphan_manifest_path).await?.is_none());
        assert!(storage.head_raw(&orphan_base_path).await?.is_none());
        assert!(storage.head_raw(&orphan_l0_path).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn repair_records_reconciliation_metrics() -> Result<()> {
        let handle = init_metrics();
        let storage = create_storage().await?;
        let (current_manifest_path, current_base_dir, current_l0_dir) =
            seed_current_manifest(&storage).await?;

        let orphan_manifest_path = orchestration_manifest_snapshot_path("00000000000000000003");
        let orphan_base_path = format!(
            "{}/runs.old.parquet",
            orchestration_base_snapshot_dir("base-orphan-metrics")
        );
        let orphan_l0_path = format!(
            "{}/runs.old.parquet",
            orchestration_l0_dir("delta-orphan-metrics")
        );
        storage
            .put_raw(
                &orphan_manifest_path,
                Bytes::from_static(b"{}"),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        storage
            .put_raw(
                &orphan_base_path,
                Bytes::from_static(b"orphan-base"),
                WritePrecondition::None,
            )
            .await?;
        storage
            .put_raw(
                &orphan_l0_path,
                Bytes::from_static(b"orphan-l0"),
                WritePrecondition::None,
            )
            .await?;

        let reconciler = OrchestrationReconciler::new(storage);
        let report = reconciler.check().await?;
        let deferred_result = reconciler.repair(&report).await?;
        assert_eq!(deferred_result.deferred_paths, 3);

        let mut aged_report = report;
        aged_report.checked_at -= Duration::hours(2);
        let deleted_result = reconciler.repair(&aged_report).await?;
        assert!(deleted_result.deleted_objects >= 3);

        let stale_report = OrchestrationReconciliationReport {
            checked_at: Utc::now(),
            current_manifest_path: Some(current_manifest_path.clone()),
            missing_current_head_legacy_manifest_path: None,
            current_head_legacy_manifest_issue: None,
            orphan_manifest_snapshots: vec![current_manifest_path],
            orphan_base_dirs: vec![current_base_dir],
            orphan_l0_dirs: vec![current_l0_dir],
        };
        let skipped_result = reconciler.repair(&stale_report).await?;
        assert_eq!(skipped_result.skipped_paths, 3);

        let metrics = handle.render();
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_ORPHANS_TOTAL,
            "reason=\"manifest_snapshot\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_ORPHANS_TOTAL,
            "reason=\"base_dir\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_ORPHANS_TOTAL,
            "reason=\"l0_dir\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_REPAIR_ISSUES_TOTAL,
            "reason=\"legacy_manifest\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_DEFERRED_PATHS_TOTAL,
            "reason=\"manifest_snapshot\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_DEFERRED_PATHS_TOTAL,
            "reason=\"base_dir\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_DEFERRED_PATHS_TOTAL,
            "reason=\"l0_dir\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_DELETES_TOTAL,
            "reason=\"manifest_snapshot\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_REPAIRS_TOTAL,
            "reason=\"legacy_manifest\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_REPAIRS_TOTAL,
            "status=\"repaired\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_DELETES_TOTAL,
            "reason=\"base_dir\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_DELETES_TOTAL,
            "reason=\"l0_dir\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_DELETED_BYTES_TOTAL,
            "reason=\"manifest_snapshot\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_DELETED_BYTES_TOTAL,
            "reason=\"base_dir\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_DELETED_BYTES_TOTAL,
            "reason=\"l0_dir\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_SKIPPED_PATHS_TOTAL,
            "reason=\"manifest_snapshot\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_SKIPPED_PATHS_TOTAL,
            "reason=\"base_dir\"",
        );
        assert_metric_lines_contain(
            &metrics,
            metric_names::ORCH_RECONCILER_SKIPPED_PATHS_TOTAL,
            "reason=\"l0_dir\"",
        );

        Ok(())
    }
}
