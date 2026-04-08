//! Anti-entropy reconciler for catalog state verification and repair.
//!
//! The reconciler detects inconsistencies between manifest state and object storage.
//! It is explicitly an **anti-entropy** tool: bucket/object listing is used here for
//! verification and repair only. The normal read path remains manifest-driven.
//!
//! # Security / Isolation
//!
//! This reconciler operates on [`arco_core::ScopedStorage`], meaning all reads/writes
//! are constrained to a tenant/workspace scope. This prevents cross-tenant listing
//! or deletion when used correctly.

use std::collections::HashSet;

use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{CatalogError, Result};
use crate::manifest::{
    CatalogDomainManifest, DomainManifestPointer, ExecutionsManifest, LineageManifest, RootManifest,
};

// ============================================================================
// Reconciliation Report
// ============================================================================

/// Report from a reconciliation check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationReport {
    /// Domain that was checked.
    pub domain: String,

    /// When the check was performed.
    pub checked_at: DateTime<Utc>,

    /// Current manifest snapshot version (domain-specific).
    pub manifest_snapshot_version: u64,

    /// Snapshot/state files referenced by manifest.
    pub manifest_snapshot_count: usize,

    /// Snapshot/state files found in storage (under the domain prefix).
    pub storage_snapshot_count: usize,

    /// Ledger events found in storage.
    pub ledger_event_count: usize,

    /// Issues found during reconciliation.
    pub issues: Vec<ReconciliationIssue>,
}

impl ReconciliationReport {
    /// Returns true if any issues were found.
    #[must_use]
    pub fn has_issues(&self) -> bool {
        !self.issues.is_empty()
    }

    /// Returns issues of a specific type.
    #[must_use]
    pub fn issues_of_type(&self, issue_type: IssueType) -> Vec<&ReconciliationIssue> {
        self.issues
            .iter()
            .filter(|i| i.issue_type == issue_type)
            .collect()
    }
}

/// A specific reconciliation issue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationIssue {
    /// Type of issue.
    pub issue_type: IssueType,

    /// Affected scope-relative path.
    pub path: String,

    /// Human-readable description.
    pub description: String,

    /// Severity level.
    pub severity: Severity,

    /// Whether this issue is auto-repairable.
    pub repairable: bool,
}

/// Type of reconciliation issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueType {
    /// Snapshot/state file not referenced by the manifest.
    OrphanedSnapshot,
    /// Manifest references a missing snapshot/state file.
    MissingSnapshot,
    /// Manifest file is missing.
    MissingManifest,
    /// Old snapshot version (GC candidate).
    OldSnapshotVersion,
    /// Pointer-target current head is visible but the legacy mutable mirror is missing.
    MissingCurrentHeadLegacyMirror,
    /// Pointer-target current head is visible but the legacy mutable mirror is stale.
    StaleCurrentHeadLegacyMirror,
    /// Pointer-target current head is visible but its commit record is missing.
    MissingCurrentHeadCommitRecord,
}

impl IssueType {
    /// Returns a stable metric/log label for this issue type.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OrphanedSnapshot => "orphaned_snapshot",
            Self::MissingSnapshot => "missing_snapshot",
            Self::MissingManifest => "missing_manifest",
            Self::OldSnapshotVersion => "old_snapshot_version",
            Self::MissingCurrentHeadLegacyMirror => "missing_current_head_legacy_mirror",
            Self::StaleCurrentHeadLegacyMirror => "stale_current_head_legacy_mirror",
            Self::MissingCurrentHeadCommitRecord => "missing_current_head_commit_record",
        }
    }
}

/// Scope of repairs to apply from a reconciliation report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairScope {
    /// Only repair current-head side effects for the visible pointer target.
    CurrentHeadOnly,
    /// Apply all repairable items, including generic cleanup work such as snapshot deletion.
    Full,
}

impl RepairScope {
    const fn allows_issue(self, issue_type: IssueType) -> bool {
        match self {
            Self::CurrentHeadOnly => matches!(
                issue_type,
                IssueType::MissingCurrentHeadLegacyMirror
                    | IssueType::StaleCurrentHeadLegacyMirror
                    | IssueType::MissingCurrentHeadCommitRecord
            ),
            Self::Full => true,
        }
    }
}

struct CurrentPointerHead<T> {
    manifest: T,
}

/// Severity of an issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Informational - no action required.
    Info,
    /// Warning - should be investigated.
    Warning,
    /// Error - requires attention.
    Error,
    /// Critical - immediate action required.
    Critical,
}

// ============================================================================
// Reconciler
// ============================================================================

/// Anti-entropy reconciler for catalog state.
///
/// Uses object listing to detect inconsistencies. This is the ONLY place
/// where bucket listing is used for correctness verification.
#[derive(Clone)]
pub struct Reconciler {
    storage: ScopedStorage,
}

impl std::fmt::Debug for Reconciler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reconciler")
            .field("storage", &"ScopedStorage")
            .finish()
    }
}

impl Reconciler {
    /// Creates a new reconciler.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self { storage }
    }

    /// Checks a domain for inconsistencies.
    ///
    /// This performs a scan of:
    /// - Root + domain manifest (expected paths)
    /// - Storage listing (actual objects under the domain prefixes)
    ///
    /// # Errors
    ///
    /// Returns an error if listing fails. Missing manifests are reported as issues
    /// in the returned report (no error).
    pub async fn check(&self, domain: CatalogDomain) -> Result<ReconciliationReport> {
        tracing::info!(
            tenant = %self.storage.tenant_id(),
            workspace = %self.storage.workspace_id(),
            domain = %domain,
            "Starting reconciliation check"
        );

        let mut report = ReconciliationReport {
            domain: domain.as_str().to_string(),
            checked_at: Utc::now(),
            manifest_snapshot_version: 0,
            manifest_snapshot_count: 0,
            storage_snapshot_count: 0,
            ledger_event_count: 0,
            issues: Vec::new(),
        };

        let Some((manifest_version, expected_paths, snapshot_prefix)) =
            self.load_expected_paths(domain).await?
        else {
            report.issues.push(ReconciliationIssue {
                issue_type: IssueType::MissingManifest,
                path: Self::head_locator_path(domain),
                description: "Domain manifest not found".to_string(),
                severity: Severity::Critical,
                repairable: false,
            });
            return Ok(report);
        };

        report.manifest_snapshot_version = manifest_version;
        report.manifest_snapshot_count = expected_paths.len();

        // List actual snapshot/state files for this domain
        let actual_paths = self.list_files(&snapshot_prefix).await?;
        report.storage_snapshot_count = actual_paths.len();

        // List ledger events
        let ledger_prefix = CatalogPaths::ledger_dir(domain);
        let ledger_events = self.list_files(&ledger_prefix).await?;
        report.ledger_event_count = ledger_events.len();

        // Orphaned files: in storage but not referenced by manifest
        let expected_set: HashSet<&String> = expected_paths.iter().collect();
        for actual in &actual_paths {
            if expected_set.contains(actual) {
                continue;
            }

            let (issue_type, severity) = match Self::extract_snapshot_version(actual) {
                Some(v) if v < manifest_version => (IssueType::OldSnapshotVersion, Severity::Info),
                _ => (IssueType::OrphanedSnapshot, Severity::Warning),
            };

            report.issues.push(ReconciliationIssue {
                issue_type,
                path: actual.clone(),
                description: match issue_type {
                    IssueType::OldSnapshotVersion => {
                        format!("Snapshot from old version (current: v{manifest_version})")
                    }
                    IssueType::OrphanedSnapshot => {
                        "Snapshot/state file not referenced by manifest".to_string()
                    }
                    _ => "unexpected issue type".to_string(),
                },
                severity,
                repairable: matches!(
                    issue_type,
                    IssueType::OrphanedSnapshot | IssueType::OldSnapshotVersion
                ),
            });
        }

        // Missing files: referenced by manifest but not in storage
        let actual_set: HashSet<&String> = actual_paths.iter().collect();
        for expected in &expected_paths {
            if !actual_set.contains(expected) {
                report.issues.push(ReconciliationIssue {
                    issue_type: IssueType::MissingSnapshot,
                    path: expected.clone(),
                    description: "Manifest references missing snapshot/state file".to_string(),
                    severity: Severity::Critical,
                    repairable: false,
                });
            }
        }

        tracing::info!(
            tenant = %self.storage.tenant_id(),
            workspace = %self.storage.workspace_id(),
            domain = %domain,
            manifest_version,
            manifest_paths = report.manifest_snapshot_count,
            storage_paths = report.storage_snapshot_count,
            ledger_events = report.ledger_event_count,
            issues = report.issues.len(),
            "Reconciliation check complete"
        );

        for issue in &report.issues {
            crate::metrics::record_reconciler_issue(
                domain,
                issue.issue_type.as_str(),
                issue.repairable,
            );
        }

        Ok(report)
    }

    /// Repairs all repairable issues found in a reconciliation report.
    ///
    /// Only issues marked as `repairable: true` will be addressed.
    ///
    /// # Errors
    ///
    /// Returns an error if storage operations fail while attempting repairs.
    pub async fn repair(&self, report: &ReconciliationReport) -> Result<RepairResult> {
        self.repair_with_scope(report, RepairScope::Full).await
    }

    /// Repairs issues found in a reconciliation report within the requested scope.
    ///
    /// Only issues marked as `repairable: true` and allowed by `scope` will be addressed.
    ///
    /// # Errors
    ///
    /// Returns an error if storage operations fail while attempting repairs.
    #[allow(clippy::too_many_lines)]
    pub async fn repair_with_scope(
        &self,
        report: &ReconciliationReport,
        scope: RepairScope,
    ) -> Result<RepairResult> {
        let mut result = RepairResult {
            domain: report.domain.clone(),
            repaired_at: Utc::now(),
            repaired_count: 0,
            skipped_count: 0,
            failed_count: 0,
        };

        let protected_paths: HashSet<String> =
            if let Some(domain) = Self::parse_domain(&report.domain) {
                self.load_expected_paths(domain)
                    .await?
                    .map_or_else(HashSet::new, |(_, expected, _)| {
                        expected.into_iter().collect()
                    })
            } else {
                HashSet::new()
            };

        for issue in &report.issues {
            if !issue.repairable {
                result.skipped_count += 1;
                continue;
            }
            if !scope.allows_issue(issue.issue_type) {
                result.skipped_count += 1;
                if let Some(domain) = Self::parse_domain(&report.domain) {
                    crate::metrics::record_reconciler_repair(
                        domain,
                        issue.issue_type.as_str(),
                        "skipped",
                    );
                }
                continue;
            }

            match issue.issue_type {
                IssueType::OrphanedSnapshot | IssueType::OldSnapshotVersion => {
                    if protected_paths.contains(&issue.path) {
                        tracing::warn!(
                            path = %issue.path,
                            domain = %report.domain,
                            "skipping repair delete for currently referenced snapshot path"
                        );
                        result.skipped_count += 1;
                        if let Some(domain) = Self::parse_domain(&report.domain) {
                            crate::metrics::record_reconciler_repair(
                                domain,
                                issue.issue_type.as_str(),
                                "skipped",
                            );
                        }
                        continue;
                    }
                    match self.storage.delete(&issue.path).await {
                        Ok(()) => {
                            result.repaired_count += 1;
                            if let Some(domain) = Self::parse_domain(&report.domain) {
                                crate::metrics::record_reconciler_repair(
                                    domain,
                                    issue.issue_type.as_str(),
                                    "repaired",
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!(path = %issue.path, error = %e, "Failed to delete orphaned/old snapshot");
                            result.failed_count += 1;
                            if let Some(domain) = Self::parse_domain(&report.domain) {
                                crate::metrics::record_reconciler_repair(
                                    domain,
                                    issue.issue_type.as_str(),
                                    "failed",
                                );
                            }
                        }
                    }
                }
                IssueType::MissingCurrentHeadLegacyMirror
                | IssueType::StaleCurrentHeadLegacyMirror
                | IssueType::MissingCurrentHeadCommitRecord => {
                    result.skipped_count += 1;
                    if let Some(domain) = Self::parse_domain(&report.domain) {
                        crate::metrics::record_reconciler_repair(
                            domain,
                            issue.issue_type.as_str(),
                            "skipped",
                        );
                    }
                }
                _ => {
                    result.skipped_count += 1;
                    if let Some(domain) = Self::parse_domain(&report.domain) {
                        crate::metrics::record_reconciler_repair(
                            domain,
                            issue.issue_type.as_str(),
                            "skipped",
                        );
                    }
                }
            }
        }

        Ok(result)
    }

    #[allow(clippy::too_many_lines)]
    async fn load_expected_paths(
        &self,
        domain: CatalogDomain,
    ) -> Result<Option<(u64, Vec<String>, String)>> {
        match domain {
            CatalogDomain::Catalog => {
                let Some(CurrentPointerHead { manifest, .. }) = self
                    .current_pointer_head_manifest::<CatalogDomainManifest>(domain)
                    .await?
                else {
                    return Ok(None);
                };

                let version = manifest.snapshot_version;
                let expected = expected_from_snapshot_info(manifest.snapshot.as_ref());

                let expected = if !expected.is_empty() {
                    expected
                } else if version == 0 {
                    Vec::new()
                } else {
                    vec![
                        CatalogPaths::snapshot_file(
                            CatalogDomain::Catalog,
                            version,
                            "namespaces.parquet",
                        ),
                        CatalogPaths::snapshot_file(
                            CatalogDomain::Catalog,
                            version,
                            "tables.parquet",
                        ),
                        CatalogPaths::snapshot_file(
                            CatalogDomain::Catalog,
                            version,
                            "columns.parquet",
                        ),
                    ]
                };

                let snapshot_prefix = format!("snapshots/{}/", domain.as_str());
                Ok(Some((version, expected, snapshot_prefix)))
            }
            CatalogDomain::Lineage => {
                let Some(CurrentPointerHead { manifest, .. }) = self
                    .current_pointer_head_manifest::<LineageManifest>(domain)
                    .await?
                else {
                    return Ok(None);
                };

                let version = manifest.snapshot_version;
                let expected = expected_from_snapshot_info(manifest.snapshot.as_ref());

                let expected = if !expected.is_empty() {
                    expected
                } else if version == 0 {
                    Vec::new()
                } else {
                    vec![CatalogPaths::snapshot_file(
                        CatalogDomain::Lineage,
                        version,
                        "lineage_edges.parquet",
                    )]
                };

                let snapshot_prefix = format!("snapshots/{}/", domain.as_str());
                Ok(Some((version, expected, snapshot_prefix)))
            }
            CatalogDomain::Executions => {
                let Some(mut root) = self
                    .get_json::<RootManifest>(CatalogPaths::ROOT_MANIFEST)
                    .await?
                else {
                    return Ok(None);
                };
                root.normalize_paths();
                let domain_path = root.executions_manifest_path;
                let Some(manifest) = self.get_json::<ExecutionsManifest>(&domain_path).await?
                else {
                    return Ok(None);
                };

                let version = manifest.snapshot_version;
                let mut expected = Vec::new();

                // Tier-2 invariants: checkpoint is required; snapshot_path is the visibility gate.
                expected.push(manifest.checkpoint_path);
                if let Some(snapshot) = manifest.snapshot_path {
                    expected.push(snapshot);
                }

                let snapshot_prefix = CatalogPaths::state_dir(CatalogDomain::Executions);
                Ok(Some((version, expected, snapshot_prefix)))
            }
            CatalogDomain::Search => {
                let Some(CurrentPointerHead { manifest, .. }) = self
                    .current_pointer_head_manifest::<crate::manifest::SearchManifest>(domain)
                    .await?
                else {
                    return Ok(None);
                };

                let version = manifest.snapshot_version;
                let expected = expected_from_snapshot_info(manifest.snapshot.as_ref());

                let expected = if !expected.is_empty() {
                    expected
                } else if version == 0 {
                    Vec::new()
                } else {
                    vec![CatalogPaths::snapshot_file(
                        CatalogDomain::Search,
                        version,
                        "token_postings.parquet",
                    )]
                };

                let snapshot_prefix = format!("snapshots/{}/", domain.as_str());
                Ok(Some((version, expected, snapshot_prefix)))
            }
        }
    }

    fn head_locator_path(domain: CatalogDomain) -> String {
        match domain {
            CatalogDomain::Executions => CatalogPaths::domain_manifest(domain),
            CatalogDomain::Catalog | CatalogDomain::Lineage | CatalogDomain::Search => {
                CatalogPaths::domain_manifest_pointer(domain)
            }
        }
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<Option<T>> {
        let bytes = match self.storage.get_raw(path).await {
            Ok(b) => b,
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                return Ok(None);
            }
            Err(e) => return Err(CatalogError::from(e)),
        };

        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse JSON at '{path}': {e}"),
            })
    }

    async fn current_pointer_head_manifest<T>(
        &self,
        domain: CatalogDomain,
    ) -> Result<Option<CurrentPointerHead<T>>>
    where
        T: DeserializeOwned,
    {
        let pointer_path = CatalogPaths::domain_manifest_pointer(domain);
        let Some(_) = self.storage.head_raw(&pointer_path).await? else {
            return Ok(None);
        };

        let pointer_bytes = self.storage.get_raw(&pointer_path).await?;
        let pointer: DomainManifestPointer =
            serde_json::from_slice(&pointer_bytes).map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse JSON at '{pointer_path}': {e}"),
            })?;
        if self
            .storage
            .head_raw(&pointer.manifest_path)
            .await?
            .is_none()
        {
            return Ok(None);
        }

        let bytes = self.storage.get_raw(&pointer.manifest_path).await?;
        let manifest = serde_json::from_slice(&bytes).map_err(|e| CatalogError::Serialization {
            message: format!("failed to parse JSON at '{}': {e}", pointer.manifest_path),
        })?;
        Ok(Some(CurrentPointerHead { manifest }))
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<String>> {
        let paths = self.storage.list(prefix).await?;
        Ok(paths.into_iter().map(|p| p.to_string()).collect())
    }

    fn parse_domain(value: &str) -> Option<CatalogDomain> {
        match value {
            "catalog" | "core" => Some(CatalogDomain::Catalog),
            "lineage" => Some(CatalogDomain::Lineage),
            "executions" => Some(CatalogDomain::Executions),
            "search" | "governance" => Some(CatalogDomain::Search),
            _ => None,
        }
    }

    /// Attempts to extract a snapshot version number from a scope-relative path.
    ///
    /// Supports:
    /// - Tier-1 snapshots: `.../v{N}/...`
    /// - Tier-2 state snapshots: `.../snapshot_v{N}_{ulid}.parquet`
    fn extract_snapshot_version(path: &str) -> Option<u64> {
        for segment in path.split('/') {
            if let Some(v) = segment.strip_prefix('v') {
                if let Ok(n) = v.parse::<u64>() {
                    return Some(n);
                }
            }

            if let Some(rest) = segment.strip_prefix("snapshot_v") {
                if let Some((num, _)) = rest.split_once('_') {
                    if let Ok(n) = num.parse::<u64>() {
                        return Some(n);
                    }
                }
            }
        }
        None
    }
}

fn join_snapshot_path(dir: &str, file: &str) -> String {
    if dir.ends_with('/') {
        format!("{dir}{file}")
    } else {
        format!("{dir}/{file}")
    }
}

fn expected_from_snapshot_info(info: Option<&crate::manifest::SnapshotInfo>) -> Vec<String> {
    let Some(info) = info else {
        return Vec::new();
    };
    info.files
        .iter()
        .map(|f| join_snapshot_path(&info.path, &f.path))
        .collect()
}

/// Result of a repair operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairResult {
    /// Domain that was repaired.
    pub domain: String,

    /// When the repair was performed.
    pub repaired_at: DateTime<Utc>,

    /// Number of issues successfully repaired.
    pub repaired_count: usize,

    /// Number of issues skipped (not repairable).
    pub skipped_count: usize,

    /// Number of repair attempts that failed.
    pub failed_count: usize,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::manifest::DomainManifestPointer;
    use arco_core::storage::{MemoryBackend, WritePrecondition};
    use bytes::Bytes;
    use chrono::Utc;

    async fn write_json<T: Serialize>(
        storage: &ScopedStorage,
        path: &str,
        value: &T,
        precondition: WritePrecondition,
    ) {
        storage
            .put_raw(
                path,
                Bytes::from(serde_json::to_vec(value).expect("serialize json")),
                precondition,
            )
            .await
            .expect("write json");
    }

    #[test]
    fn extract_snapshot_version_parses_tier1_and_tier2() {
        assert_eq!(
            Reconciler::extract_snapshot_version("snapshots/catalog/v1/tables.parquet"),
            Some(1)
        );
        assert_eq!(
            Reconciler::extract_snapshot_version("state/executions/snapshot_v42_01J123.parquet"),
            Some(42)
        );
        assert_eq!(
            Reconciler::extract_snapshot_version("snapshots/catalog/invalid/file.parquet"),
            None
        );
    }

    #[tokio::test]
    async fn check_uses_scope_relative_paths_and_joins_snapshot_files() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").unwrap();

        // Root manifest
        let root = RootManifest::new();
        storage
            .put_raw(
                CatalogPaths::ROOT_MANIFEST,
                Bytes::from(serde_json::to_vec(&root).unwrap()),
                WritePrecondition::None,
            )
            .await
            .unwrap();

        // Catalog domain manifest with snapshot metadata
        let snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1);
        let mut snapshot = crate::manifest::SnapshotInfo::new(1, snapshot_path.clone());
        snapshot.add_file(crate::manifest::SnapshotFile {
            path: "a.parquet".to_string(),
            checksum_sha256: "00".repeat(32),
            byte_size: 1,
            row_count: 0,
            position_range: None,
        });
        snapshot.add_file(crate::manifest::SnapshotFile {
            path: "c.parquet".to_string(),
            checksum_sha256: "11".repeat(32),
            byte_size: 1,
            row_count: 0,
            position_range: None,
        });

        let manifest = CatalogDomainManifest {
            manifest_id: crate::manifest::format_manifest_id(1),
            epoch: 0,
            previous_manifest_path: Some("manifests/catalog/00000000000000000000.json".to_string()),
            writer_session_id: Some("reconciler-test".to_string()),
            snapshot_version: 1,
            snapshot_path: snapshot_path.clone(),
            snapshot: Some(snapshot),
            watermark_event_id: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: None,
            parent_hash: None,
            updated_at: Utc::now(),
        };
        let manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, &manifest.manifest_id);
        storage
            .put_raw(
                &manifest_path,
                Bytes::from(serde_json::to_vec(&manifest).unwrap()),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        storage
            .put_raw(
                &CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog),
                Bytes::from(
                    serde_json::to_vec(&DomainManifestPointer {
                        manifest_id: manifest.manifest_id.clone(),
                        manifest_path,
                        epoch: 0,
                        parent_pointer_hash: None,
                        updated_at: Utc::now(),
                    })
                    .expect("serialize pointer"),
                ),
                WritePrecondition::None,
            )
            .await
            .unwrap();

        // Only write a.parquet, leave c.parquet missing.
        storage
            .put_raw(
                &CatalogPaths::snapshot_file(CatalogDomain::Catalog, 1, "a.parquet"),
                Bytes::from_static(b"x"),
                WritePrecondition::None,
            )
            .await
            .unwrap();

        // Write an orphan file in the snapshot dir.
        storage
            .put_raw(
                &CatalogPaths::snapshot_file(CatalogDomain::Catalog, 1, "b.parquet"),
                Bytes::from_static(b"x"),
                WritePrecondition::None,
            )
            .await
            .unwrap();

        let reconciler = Reconciler::new(storage);
        let report = reconciler.check(CatalogDomain::Catalog).await.unwrap();

        assert!(report.has_issues());
        assert_eq!(report.issues_of_type(IssueType::MissingSnapshot).len(), 1);
        assert_eq!(report.issues_of_type(IssueType::OrphanedSnapshot).len(), 1);

        // Paths in the report are scope-relative (no tenant/workspace prefix).
        for issue in &report.issues {
            assert!(
                !issue.path.starts_with("tenant="),
                "issue path must be scope-relative"
            );
        }
    }

    #[tokio::test]
    async fn check_prefers_pointer_manifest_over_legacy_manifest() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");

        let mut root = RootManifest::new();
        root.normalize_paths();
        storage
            .put_raw(
                CatalogPaths::ROOT_MANIFEST,
                Bytes::from(serde_json::to_vec(&root).expect("serialize root")),
                WritePrecondition::None,
            )
            .await
            .expect("write root");

        let mut legacy_snapshot = crate::manifest::SnapshotInfo::new(
            1,
            CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1),
        );
        legacy_snapshot.add_file(crate::manifest::SnapshotFile {
            path: "legacy.parquet".to_string(),
            checksum_sha256: "00".repeat(32),
            byte_size: 1,
            row_count: 0,
            position_range: None,
        });
        let legacy = CatalogDomainManifest {
            manifest_id: crate::manifest::format_manifest_id(1),
            epoch: 1,
            previous_manifest_path: Some("manifests/catalog/00000000000000000000.json".to_string()),
            writer_session_id: Some("legacy-session".to_string()),
            snapshot_version: 1,
            snapshot_path: CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1),
            snapshot: Some(legacy_snapshot),
            watermark_event_id: None,
            last_commit_id: None,
            fencing_token: Some(1),
            commit_ulid: None,
            parent_hash: None,
            updated_at: Utc::now(),
        };
        storage
            .put_raw(
                &CatalogPaths::domain_manifest(CatalogDomain::Catalog),
                Bytes::from(serde_json::to_vec(&legacy).expect("serialize legacy")),
                WritePrecondition::None,
            )
            .await
            .expect("write legacy");

        let pointed_manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, "00000000000000000002");
        let mut pointed_snapshot = crate::manifest::SnapshotInfo::new(
            2,
            CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 2),
        );
        pointed_snapshot.add_file(crate::manifest::SnapshotFile {
            path: "current.parquet".to_string(),
            checksum_sha256: "11".repeat(32),
            byte_size: 1,
            row_count: 0,
            position_range: None,
        });
        let pointed = CatalogDomainManifest {
            manifest_id: crate::manifest::format_manifest_id(2),
            epoch: 2,
            previous_manifest_path: Some("manifests/catalog/00000000000000000001.json".to_string()),
            writer_session_id: Some("pointer-session".to_string()),
            snapshot_version: 2,
            snapshot_path: CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 2),
            snapshot: Some(pointed_snapshot),
            watermark_event_id: None,
            last_commit_id: None,
            fencing_token: Some(2),
            commit_ulid: None,
            parent_hash: None,
            updated_at: Utc::now(),
        };
        storage
            .put_raw(
                &pointed_manifest_path,
                Bytes::from(serde_json::to_vec(&pointed).expect("serialize pointed")),
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

        let reconciler = Reconciler::new(storage);
        let report = reconciler
            .check(CatalogDomain::Catalog)
            .await
            .expect("check");

        assert_eq!(report.manifest_snapshot_version, 2);
        assert!(
            report
                .issues_of_type(IssueType::StaleCurrentHeadLegacyMirror)
                .is_empty()
        );
        assert!(
            report
                .issues_of_type(IssueType::MissingCurrentHeadCommitRecord)
                .is_empty()
        );
        assert!(
            report
                .issues
                .iter()
                .all(|issue| !issue.path.contains("/v2/current.parquet")),
            "current pointer-targeted snapshot file must not be flagged"
        );
    }

    #[tokio::test]
    async fn repair_skips_pointer_targeted_snapshot_paths() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");

        let mut root = RootManifest::new();
        root.normalize_paths();
        storage
            .put_raw(
                CatalogPaths::ROOT_MANIFEST,
                Bytes::from(serde_json::to_vec(&root).expect("serialize root")),
                WritePrecondition::None,
            )
            .await
            .expect("write root");

        let pointed_manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, "00000000000000000002");
        let mut pointed_snapshot = crate::manifest::SnapshotInfo::new(
            2,
            CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 2),
        );
        pointed_snapshot.add_file(crate::manifest::SnapshotFile {
            path: "current.parquet".to_string(),
            checksum_sha256: "22".repeat(32),
            byte_size: 1,
            row_count: 0,
            position_range: None,
        });
        let pointed = CatalogDomainManifest {
            manifest_id: crate::manifest::format_manifest_id(2),
            epoch: 2,
            previous_manifest_path: Some("manifests/catalog/00000000000000000001.json".to_string()),
            writer_session_id: Some("pointer-session".to_string()),
            snapshot_version: 2,
            snapshot_path: CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 2),
            snapshot: Some(pointed_snapshot),
            watermark_event_id: None,
            last_commit_id: None,
            fencing_token: Some(2),
            commit_ulid: None,
            parent_hash: None,
            updated_at: Utc::now(),
        };
        storage
            .put_raw(
                &pointed_manifest_path,
                Bytes::from(serde_json::to_vec(&pointed).expect("serialize pointed")),
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

        let protected_file =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, 2, "current.parquet");
        storage
            .put_raw(
                &protected_file,
                Bytes::from_static(b"ok"),
                WritePrecondition::None,
            )
            .await
            .expect("write protected file");

        let report = ReconciliationReport {
            domain: CatalogDomain::Catalog.as_str().to_string(),
            checked_at: Utc::now(),
            manifest_snapshot_version: 2,
            manifest_snapshot_count: 1,
            storage_snapshot_count: 1,
            ledger_event_count: 0,
            issues: vec![ReconciliationIssue {
                issue_type: IssueType::OrphanedSnapshot,
                path: protected_file.clone(),
                description: "stale report".to_string(),
                severity: Severity::Warning,
                repairable: true,
            }],
        };

        let reconciler = Reconciler::new(storage.clone());
        let result = reconciler.repair(&report).await.expect("repair");

        assert_eq!(result.repaired_count, 0);
        assert_eq!(result.skipped_count, 1);
        storage
            .get_raw(&protected_file)
            .await
            .expect("protected file must remain");
    }

    #[tokio::test]
    async fn repair_with_current_head_scope_skips_generic_cleanup_issues() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");

        let mut root = RootManifest::new();
        root.normalize_paths();
        write_json(
            &storage,
            CatalogPaths::ROOT_MANIFEST,
            &root,
            WritePrecondition::None,
        )
        .await;

        let snapshot_dir = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1);
        let mut snapshot = crate::manifest::SnapshotInfo::new(1, snapshot_dir);
        snapshot.add_file(crate::manifest::SnapshotFile {
            path: "current.parquet".to_string(),
            checksum_sha256: "aa".repeat(32),
            byte_size: 1,
            row_count: 1,
            position_range: None,
        });

        let manifest = CatalogDomainManifest {
            manifest_id: crate::manifest::format_manifest_id(1),
            epoch: 1,
            previous_manifest_path: None,
            writer_session_id: Some("scope-test".to_string()),
            snapshot_version: 1,
            snapshot_path: CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1),
            snapshot: Some(snapshot),
            watermark_event_id: None,
            last_commit_id: None,
            fencing_token: Some(1),
            commit_ulid: None,
            parent_hash: None,
            updated_at: Utc::now(),
        };
        let manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, &manifest.manifest_id);
        write_json(
            &storage,
            &manifest_path,
            &manifest,
            WritePrecondition::DoesNotExist,
        )
        .await;
        write_json(
            &storage,
            &CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog),
            &DomainManifestPointer {
                manifest_id: manifest.manifest_id.clone(),
                manifest_path,
                epoch: 1,
                parent_pointer_hash: None,
                updated_at: Utc::now(),
            },
            WritePrecondition::None,
        )
        .await;
        storage
            .put_raw(
                &CatalogPaths::snapshot_file(CatalogDomain::Catalog, 1, "current.parquet"),
                Bytes::from_static(b"current"),
                WritePrecondition::None,
            )
            .await
            .expect("write current snapshot");

        let old_snapshot =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, 0, "stale-current-head.parquet");
        storage
            .put_raw(
                &old_snapshot,
                Bytes::from_static(b"stale"),
                WritePrecondition::None,
            )
            .await
            .expect("write old snapshot");

        let reconciler = Reconciler::new(storage.clone());
        let report = reconciler
            .check(CatalogDomain::Catalog)
            .await
            .expect("check report");
        assert_eq!(
            report.issues_of_type(IssueType::OldSnapshotVersion).len(),
            1,
            "expected a generic cleanup issue in the report"
        );

        let repair = reconciler
            .repair_with_scope(&report, RepairScope::CurrentHeadOnly)
            .await
            .expect("repair with current-head scope");
        assert_eq!(repair.repaired_count, 0);
        assert!(repair.skipped_count >= 1);

        storage
            .get_raw(&old_snapshot)
            .await
            .expect("current-head-only repair must not delete generic cleanup candidates");
    }
}
