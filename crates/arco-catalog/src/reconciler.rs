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
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{CatalogError, Result};
use crate::manifest::{
    CatalogDomainManifest, CommitRecord, DomainManifestPointer, ExecutionsManifest,
    LineageManifest, RootManifest,
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

const MAX_CURRENT_HEAD_REPAIR_ATTEMPTS: usize = 4;

struct CurrentPointerHead<T> {
    manifest: T,
    bytes: Bytes,
    pointer_version: String,
}

enum CurrentHeadRepairOutcome {
    Repaired,
    Skipped,
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
                path: CatalogPaths::domain_manifest(domain),
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

        self.add_current_head_side_effect_issues(domain, &mut report)
            .await?;

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
                IssueType::MissingCurrentHeadLegacyMirror => {
                    let Some(domain) = Self::parse_domain(&report.domain) else {
                        result.skipped_count += 1;
                        continue;
                    };
                    match self.repair_current_head_legacy_mirror(domain).await {
                        Ok(CurrentHeadRepairOutcome::Repaired) => {
                            result.repaired_count += 1;
                            crate::metrics::record_reconciler_repair(
                                domain,
                                issue.issue_type.as_str(),
                                "repaired",
                            );
                        }
                        Ok(CurrentHeadRepairOutcome::Skipped) => {
                            result.skipped_count += 1;
                            crate::metrics::record_reconciler_repair(
                                domain,
                                issue.issue_type.as_str(),
                                "skipped",
                            );
                        }
                        Err(error) => {
                            tracing::error!(
                                path = %issue.path,
                                domain = %report.domain,
                                error = %error,
                                "failed to repair current-head legacy mirror"
                            );
                            result.failed_count += 1;
                            crate::metrics::record_reconciler_repair(
                                domain,
                                issue.issue_type.as_str(),
                                "failed",
                            );
                        }
                    }
                }
                IssueType::StaleCurrentHeadLegacyMirror => {
                    let Some(domain) = Self::parse_domain(&report.domain) else {
                        result.skipped_count += 1;
                        continue;
                    };
                    match self.repair_current_head_legacy_mirror(domain).await {
                        Ok(CurrentHeadRepairOutcome::Repaired) => {
                            result.repaired_count += 1;
                            crate::metrics::record_reconciler_repair(
                                domain,
                                issue.issue_type.as_str(),
                                "repaired",
                            );
                        }
                        Ok(CurrentHeadRepairOutcome::Skipped) => {
                            result.skipped_count += 1;
                            crate::metrics::record_reconciler_repair(
                                domain,
                                issue.issue_type.as_str(),
                                "skipped",
                            );
                        }
                        Err(error) => {
                            tracing::error!(
                                path = %issue.path,
                                domain = %report.domain,
                                error = %error,
                                "failed to repair stale current-head legacy mirror"
                            );
                            result.failed_count += 1;
                            crate::metrics::record_reconciler_repair(
                                domain,
                                issue.issue_type.as_str(),
                                "failed",
                            );
                        }
                    }
                }
                IssueType::MissingCurrentHeadCommitRecord => {
                    let Some(domain) = Self::parse_domain(&report.domain) else {
                        result.skipped_count += 1;
                        continue;
                    };
                    if self.storage.head_raw(&issue.path).await?.is_some() {
                        result.skipped_count += 1;
                        crate::metrics::record_reconciler_repair(
                            domain,
                            issue.issue_type.as_str(),
                            "skipped",
                        );
                        continue;
                    }
                    match self.repair_current_head_commit_record(domain).await {
                        Ok(()) => {
                            result.repaired_count += 1;
                            crate::metrics::record_reconciler_repair(
                                domain,
                                issue.issue_type.as_str(),
                                "repaired",
                            );
                        }
                        Err(error) => {
                            tracing::error!(
                                path = %issue.path,
                                domain = %report.domain,
                                error = %error,
                                "failed to repair current-head commit record"
                            );
                            result.failed_count += 1;
                            crate::metrics::record_reconciler_repair(
                                domain,
                                issue.issue_type.as_str(),
                                "failed",
                            );
                        }
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

    async fn add_current_head_side_effect_issues(
        &self,
        domain: CatalogDomain,
        report: &mut ReconciliationReport,
    ) -> Result<()> {
        match domain {
            CatalogDomain::Catalog => {
                if let Some(head) = self
                    .current_pointer_head_manifest::<CatalogDomainManifest>(domain)
                    .await?
                {
                    self.push_current_head_side_effect_issues(
                        domain,
                        head.manifest.last_commit_id.as_deref(),
                        &head.bytes,
                        report,
                    )
                    .await?;
                }
            }
            CatalogDomain::Lineage => {
                if let Some(head) = self
                    .current_pointer_head_manifest::<LineageManifest>(domain)
                    .await?
                {
                    self.push_current_head_side_effect_issues(
                        domain,
                        head.manifest.last_commit_id.as_deref(),
                        &head.bytes,
                        report,
                    )
                    .await?;
                }
            }
            CatalogDomain::Search => {
                if let Some(head) = self
                    .current_pointer_head_manifest::<crate::manifest::SearchManifest>(domain)
                    .await?
                {
                    self.push_current_head_side_effect_issues(
                        domain,
                        head.manifest.last_commit_id.as_deref(),
                        &head.bytes,
                        report,
                    )
                    .await?;
                }
            }
            CatalogDomain::Executions => {}
        }

        Ok(())
    }

    async fn push_current_head_side_effect_issues(
        &self,
        domain: CatalogDomain,
        last_commit_id: Option<&str>,
        current_head_bytes: &Bytes,
        report: &mut ReconciliationReport,
    ) -> Result<()> {
        let legacy_path = CatalogPaths::domain_manifest(domain);
        match self.storage.get_raw(&legacy_path).await {
            Ok(legacy_bytes) => {
                if legacy_bytes != *current_head_bytes {
                    report.issues.push(ReconciliationIssue {
                        issue_type: IssueType::StaleCurrentHeadLegacyMirror,
                        path: legacy_path,
                        description:
                            "Pointer-target current head differs from its legacy manifest mirror"
                                .to_string(),
                        severity: Severity::Warning,
                        repairable: true,
                    });
                }
            }
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                report.issues.push(ReconciliationIssue {
                    issue_type: IssueType::MissingCurrentHeadLegacyMirror,
                    path: legacy_path,
                    description:
                        "Pointer-target current head is missing its legacy manifest mirror"
                            .to_string(),
                    severity: Severity::Warning,
                    repairable: true,
                });
            }
            Err(error) => return Err(CatalogError::from(error)),
        }

        if let Some(commit_id) = last_commit_id {
            let commit_path = CatalogPaths::commit(domain, commit_id);
            if self.storage.head_raw(&commit_path).await?.is_none() {
                report.issues.push(ReconciliationIssue {
                    issue_type: IssueType::MissingCurrentHeadCommitRecord,
                    path: commit_path,
                    description: "Pointer-target current head is missing its commit record"
                        .to_string(),
                    severity: Severity::Warning,
                    repairable: true,
                });
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn load_expected_paths(
        &self,
        domain: CatalogDomain,
    ) -> Result<Option<(u64, Vec<String>, String)>> {
        let Some(mut root) = self
            .get_json::<RootManifest>(CatalogPaths::ROOT_MANIFEST)
            .await?
        else {
            return Ok(None);
        };
        root.normalize_paths();

        let legacy_domain_path = match domain {
            CatalogDomain::Catalog => root.catalog_manifest_path,
            CatalogDomain::Lineage => root.lineage_manifest_path,
            CatalogDomain::Executions => root.executions_manifest_path,
            CatalogDomain::Search => root.search_manifest_path,
        };
        let domain_path = self
            .resolve_domain_manifest_path(domain, &legacy_domain_path)
            .await?;

        match domain {
            CatalogDomain::Catalog => {
                let Some(manifest) = self.get_json::<CatalogDomainManifest>(&domain_path).await?
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
                let Some(manifest) = self.get_json::<LineageManifest>(&domain_path).await? else {
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
                let Some(manifest) = self
                    .get_json::<crate::manifest::SearchManifest>(&domain_path)
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
        let Some(pointer_meta) = self.storage.head_raw(&pointer_path).await? else {
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
        Ok(Some(CurrentPointerHead {
            manifest,
            bytes,
            pointer_version: pointer_meta.version,
        }))
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<String>> {
        let paths = self.storage.list(prefix).await?;
        Ok(paths.into_iter().map(|p| p.to_string()).collect())
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
                        message: format!("failed to parse JSON at '{pointer_path}': {e}"),
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

    async fn repair_current_head_legacy_mirror(
        &self,
        domain: CatalogDomain,
    ) -> Result<CurrentHeadRepairOutcome> {
        let legacy_path = CatalogPaths::domain_manifest(domain);

        for attempt in 1..=MAX_CURRENT_HEAD_REPAIR_ATTEMPTS {
            let Some((current_bytes, pointer_version)) =
                self.current_pointer_head_bytes(domain).await?
            else {
                return Ok(CurrentHeadRepairOutcome::Skipped);
            };

            let legacy_in_sync = match self.storage.get_raw(&legacy_path).await {
                Ok(legacy_bytes) => legacy_bytes == current_bytes,
                Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                    false
                }
                Err(error) => return Err(CatalogError::from(error)),
            };
            if legacy_in_sync {
                return Ok(CurrentHeadRepairOutcome::Skipped);
            }

            self.storage
                .put_raw(
                    &legacy_path,
                    current_bytes.clone(),
                    arco_core::WritePrecondition::None,
                )
                .await?;

            let Some((latest_bytes, latest_pointer_version)) =
                self.current_pointer_head_bytes(domain).await?
            else {
                return Ok(CurrentHeadRepairOutcome::Repaired);
            };
            if latest_pointer_version == pointer_version || latest_bytes == current_bytes {
                return Ok(CurrentHeadRepairOutcome::Repaired);
            }
            if attempt == MAX_CURRENT_HEAD_REPAIR_ATTEMPTS {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "current {domain} head advanced while repairing legacy manifest mirror"
                    ),
                });
            }
        }

        Ok(CurrentHeadRepairOutcome::Skipped)
    }

    async fn repair_current_head_commit_record(&self, domain: CatalogDomain) -> Result<()> {
        match domain {
            CatalogDomain::Catalog => {
                let Some(CurrentPointerHead { manifest, .. }) = self
                    .current_pointer_head_manifest::<CatalogDomainManifest>(domain)
                    .await?
                else {
                    return Ok(());
                };
                let commit = self
                    .build_catalog_commit_record_for_repair(&manifest)
                    .await?;
                self.write_commit_record(domain, &commit).await?;
            }
            CatalogDomain::Lineage => {
                let Some(CurrentPointerHead { manifest, .. }) = self
                    .current_pointer_head_manifest::<LineageManifest>(domain)
                    .await?
                else {
                    return Ok(());
                };
                let commit = self
                    .build_lineage_commit_record_for_repair(&manifest)
                    .await?;
                self.write_commit_record(domain, &commit).await?;
            }
            CatalogDomain::Search => {
                let Some(CurrentPointerHead { manifest, .. }) = self
                    .current_pointer_head_manifest::<crate::manifest::SearchManifest>(domain)
                    .await?
                else {
                    return Ok(());
                };
                let commit = self
                    .build_search_commit_record_for_repair(&manifest)
                    .await?;
                self.write_commit_record(domain, &commit).await?;
            }
            CatalogDomain::Executions => {}
        }

        Ok(())
    }

    async fn current_pointer_head_bytes(
        &self,
        domain: CatalogDomain,
    ) -> Result<Option<(Bytes, String)>> {
        match domain {
            CatalogDomain::Catalog => Ok(self
                .current_pointer_head_manifest::<CatalogDomainManifest>(domain)
                .await?
                .map(|head| (head.bytes, head.pointer_version))),
            CatalogDomain::Lineage => Ok(self
                .current_pointer_head_manifest::<LineageManifest>(domain)
                .await?
                .map(|head| (head.bytes, head.pointer_version))),
            CatalogDomain::Search => Ok(self
                .current_pointer_head_manifest::<crate::manifest::SearchManifest>(domain)
                .await?
                .map(|head| (head.bytes, head.pointer_version))),
            CatalogDomain::Executions => Ok(None),
        }
    }

    async fn write_commit_record(
        &self,
        domain: CatalogDomain,
        commit: &CommitRecord,
    ) -> Result<()> {
        match self
            .storage
            .put_raw(
                &CatalogPaths::commit(domain, &commit.commit_id),
                Bytes::from(serde_json::to_vec(commit).map_err(|e| {
                    CatalogError::Serialization {
                        message: format!("failed to serialize commit record: {e}"),
                    }
                })?),
                arco_core::WritePrecondition::DoesNotExist,
            )
            .await?
        {
            arco_core::WriteResult::Success { .. }
            | arco_core::WriteResult::PreconditionFailed { .. } => Ok(()),
        }
    }

    async fn build_catalog_commit_record_for_repair(
        &self,
        manifest: &CatalogDomainManifest,
    ) -> Result<CommitRecord> {
        let payload_hash = sha256_prefixed(&serde_json::to_vec(manifest).map_err(|e| {
            CatalogError::Serialization {
                message: format!("failed to serialize current catalog manifest: {e}"),
            }
        })?);
        let prev_commit_id = self
            .load_previous_manifest_commit_id::<CatalogDomainManifest>(
                manifest.previous_manifest_path.as_deref(),
            )
            .await?;
        self.build_repaired_commit_record(
            manifest.last_commit_id.clone(),
            payload_hash,
            CatalogDomain::Catalog,
            prev_commit_id,
        )
        .await
    }

    async fn build_lineage_commit_record_for_repair(
        &self,
        manifest: &LineageManifest,
    ) -> Result<CommitRecord> {
        let payload_hash = sha256_prefixed(&serde_json::to_vec(manifest).map_err(|e| {
            CatalogError::Serialization {
                message: format!("failed to serialize current lineage manifest: {e}"),
            }
        })?);
        let prev_commit_id = self
            .load_previous_manifest_commit_id::<LineageManifest>(
                manifest.previous_manifest_path.as_deref(),
            )
            .await?;
        self.build_repaired_commit_record(
            manifest.last_commit_id.clone(),
            payload_hash,
            CatalogDomain::Lineage,
            prev_commit_id,
        )
        .await
    }

    async fn build_search_commit_record_for_repair(
        &self,
        manifest: &crate::manifest::SearchManifest,
    ) -> Result<CommitRecord> {
        let payload_hash = sha256_prefixed(&serde_json::to_vec(manifest).map_err(|e| {
            CatalogError::Serialization {
                message: format!("failed to serialize current search manifest: {e}"),
            }
        })?);
        let prev_commit_id = self
            .load_previous_manifest_commit_id::<crate::manifest::SearchManifest>(
                manifest.previous_manifest_path.as_deref(),
            )
            .await?;
        self.build_repaired_commit_record(
            manifest.last_commit_id.clone(),
            payload_hash,
            CatalogDomain::Search,
            prev_commit_id,
        )
        .await
    }

    async fn load_previous_manifest_commit_id<T>(
        &self,
        previous_manifest_path: Option<&str>,
    ) -> Result<Option<String>>
    where
        T: DeserializeOwned + PreviousCommitAccessor,
    {
        let Some(previous_manifest_path) = previous_manifest_path else {
            return Ok(None);
        };
        Ok(self
            .get_json::<T>(previous_manifest_path)
            .await?
            .and_then(|manifest| manifest.previous_commit_id().map(ToOwned::to_owned)))
    }

    async fn build_repaired_commit_record(
        &self,
        commit_id: Option<String>,
        payload_hash: String,
        domain: CatalogDomain,
        prev_commit_id: Option<String>,
    ) -> Result<CommitRecord> {
        let commit_id = commit_id.ok_or_else(|| CatalogError::Validation {
            message: format!("current {domain} head is missing last_commit_id"),
        })?;
        let prev_commit_hash = self
            .load_commit_hash(domain, prev_commit_id.as_deref())
            .await?;
        Ok(CommitRecord {
            commit_id,
            prev_commit_id,
            prev_commit_hash,
            operation: "SyncCompact".to_string(),
            payload_hash,
            created_at: Utc::now(),
        })
    }

    async fn load_commit_hash(
        &self,
        domain: CatalogDomain,
        commit_id: Option<&str>,
    ) -> Result<Option<String>> {
        let Some(commit_id) = commit_id else {
            return Ok(None);
        };
        let path = CatalogPaths::commit(domain, commit_id);
        match self.storage.get_raw(&path).await {
            Ok(bytes) => {
                let record: CommitRecord =
                    serde_json::from_slice(&bytes).map_err(|e| CatalogError::Serialization {
                        message: format!("failed to parse JSON at '{path}': {e}"),
                    })?;
                Ok(Some(record.compute_hash()))
            }
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                Ok(None)
            }
            Err(error) => Err(CatalogError::from(error)),
        }
    }
}

trait PreviousCommitAccessor {
    fn previous_commit_id(&self) -> Option<&str>;
}

impl PreviousCommitAccessor for CatalogDomainManifest {
    fn previous_commit_id(&self) -> Option<&str> {
        self.last_commit_id.as_deref()
    }
}

impl PreviousCommitAccessor for LineageManifest {
    fn previous_commit_id(&self) -> Option<&str> {
        self.last_commit_id.as_deref()
    }
}

impl PreviousCommitAccessor for crate::manifest::SearchManifest {
    fn previous_commit_id(&self) -> Option<&str> {
        self.last_commit_id.as_deref()
    }
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let hash = Sha256::digest(bytes);
    format!("sha256:{}", hex::encode(hash))
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
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::manifest::{CommitRecord, DomainManifestPointer};
    use arco_core::storage::{MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition};
    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::Utc;
    use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

    fn init_metrics() -> PrometheusHandle {
        static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

        PROMETHEUS_HANDLE
            .get_or_init(|| {
                PrometheusBuilder::new()
                    .install_recorder()
                    .expect("install prometheus recorder for catalog reconciler tests")
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
    struct CatalogRepairRaceBackend {
        inner: MemoryBackend,
        legacy_path: String,
        pointer_path: String,
        advanced_pointer_bytes: Bytes,
        inject_once: AtomicBool,
    }

    impl CatalogRepairRaceBackend {
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
    impl StorageBackend for CatalogRepairRaceBackend {
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

    async fn seed_catalog_pointer_head_with_missing_side_effects(
        storage: &ScopedStorage,
    ) -> (
        CatalogDomainManifest,
        CatalogDomainManifest,
        CommitRecord,
        String,
    ) {
        let mut root = RootManifest::new();
        root.normalize_paths();
        write_json(
            storage,
            CatalogPaths::ROOT_MANIFEST,
            &root,
            WritePrecondition::None,
        )
        .await;

        let previous_snapshot_dir = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1);
        let mut previous_snapshot = crate::manifest::SnapshotInfo::new(1, previous_snapshot_dir);
        previous_snapshot.add_file(crate::manifest::SnapshotFile {
            path: "previous.parquet".to_string(),
            checksum_sha256: "11".repeat(32),
            byte_size: 1,
            row_count: 1,
            position_range: None,
        });
        let previous_manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, "00000000000000000001");
        let previous_manifest = CatalogDomainManifest {
            manifest_id: crate::manifest::format_manifest_id(1),
            epoch: 1,
            previous_manifest_path: None,
            writer_session_id: Some("reconciler-prev".to_string()),
            snapshot_version: 1,
            snapshot_path: CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1),
            snapshot: Some(previous_snapshot),
            watermark_event_id: Some("evt_prev".to_string()),
            last_commit_id: Some("commit_01".to_string()),
            fencing_token: Some(1),
            commit_ulid: Some("commit_01".to_string()),
            parent_hash: None,
            updated_at: Utc::now(),
        };
        write_json(
            storage,
            &previous_manifest_path,
            &previous_manifest,
            WritePrecondition::DoesNotExist,
        )
        .await;
        storage
            .put_raw(
                &CatalogPaths::snapshot_file(CatalogDomain::Catalog, 1, "previous.parquet"),
                Bytes::from_static(b"prev"),
                WritePrecondition::None,
            )
            .await
            .expect("write previous snapshot");

        let previous_commit = CommitRecord {
            commit_id: "commit_01".to_string(),
            prev_commit_id: None,
            prev_commit_hash: None,
            operation: "SyncCompact".to_string(),
            payload_hash: "sha256:prev".to_string(),
            created_at: Utc::now(),
        };
        write_json(
            storage,
            &CatalogPaths::commit(CatalogDomain::Catalog, &previous_commit.commit_id),
            &previous_commit,
            WritePrecondition::DoesNotExist,
        )
        .await;

        let current_snapshot_dir = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 2);
        let mut current_snapshot = crate::manifest::SnapshotInfo::new(2, current_snapshot_dir);
        current_snapshot.add_file(crate::manifest::SnapshotFile {
            path: "current.parquet".to_string(),
            checksum_sha256: "22".repeat(32),
            byte_size: 1,
            row_count: 1,
            position_range: None,
        });
        let current_manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, "00000000000000000002");
        let current_manifest = CatalogDomainManifest {
            manifest_id: crate::manifest::format_manifest_id(2),
            epoch: 2,
            previous_manifest_path: Some(previous_manifest_path),
            writer_session_id: Some("reconciler-current".to_string()),
            snapshot_version: 2,
            snapshot_path: CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 2),
            snapshot: Some(current_snapshot),
            watermark_event_id: Some("evt_current".to_string()),
            last_commit_id: Some("commit_02".to_string()),
            fencing_token: Some(2),
            commit_ulid: Some("commit_02".to_string()),
            parent_hash: None,
            updated_at: Utc::now(),
        };
        write_json(
            storage,
            &current_manifest_path,
            &current_manifest,
            WritePrecondition::DoesNotExist,
        )
        .await;
        storage
            .put_raw(
                &CatalogPaths::snapshot_file(CatalogDomain::Catalog, 2, "current.parquet"),
                Bytes::from_static(b"current"),
                WritePrecondition::None,
            )
            .await
            .expect("write current snapshot");

        let pointer = DomainManifestPointer {
            manifest_id: current_manifest.manifest_id.clone(),
            manifest_path: current_manifest_path.clone(),
            epoch: 2,
            parent_pointer_hash: None,
            updated_at: Utc::now(),
        };
        write_json(
            storage,
            &CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog),
            &pointer,
            WritePrecondition::DoesNotExist,
        )
        .await;

        (
            previous_manifest,
            current_manifest,
            previous_commit,
            current_manifest_path,
        )
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
        storage
            .put_raw(
                &root.catalog_manifest_path,
                Bytes::from(serde_json::to_vec(&manifest).unwrap()),
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
                &root.catalog_manifest_path,
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

        let reconciler = Reconciler::new(storage);
        let report = reconciler
            .check(CatalogDomain::Catalog)
            .await
            .expect("check");

        assert_eq!(report.manifest_snapshot_version, 2);
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
        write_json(
            &storage,
            &root.catalog_manifest_path,
            &manifest,
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

    #[tokio::test]
    async fn repair_restores_missing_current_head_catalog_side_effects() {
        let handle = init_metrics();
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");
        let (_previous_manifest, current_manifest, previous_commit, current_manifest_path) =
            seed_catalog_pointer_head_with_missing_side_effects(&storage).await;

        let reconciler = Reconciler::new(storage.clone());
        let report = reconciler
            .check(CatalogDomain::Catalog)
            .await
            .expect("check report");

        assert_eq!(
            report
                .issues_of_type(IssueType::MissingCurrentHeadLegacyMirror)
                .len(),
            1
        );
        assert_eq!(
            report
                .issues_of_type(IssueType::MissingCurrentHeadCommitRecord)
                .len(),
            1
        );

        let repair = reconciler.repair(&report).await.expect("repair");
        assert!(repair.repaired_count >= 2);
        assert_eq!(repair.failed_count, 0);

        let legacy_bytes = storage
            .get_raw(&CatalogPaths::domain_manifest(CatalogDomain::Catalog))
            .await
            .expect("legacy mirror restored");
        let current_bytes = storage
            .get_raw(&current_manifest_path)
            .await
            .expect("current head exists");
        assert_eq!(legacy_bytes, current_bytes);

        let commit_bytes = storage
            .get_raw(&CatalogPaths::commit(CatalogDomain::Catalog, "commit_02"))
            .await
            .expect("current commit restored");
        let current_commit: CommitRecord =
            serde_json::from_slice(&commit_bytes).expect("parse repaired commit");
        assert_eq!(current_commit.commit_id, "commit_02");
        assert_eq!(current_commit.prev_commit_id.as_deref(), Some("commit_01"));
        assert_eq!(
            current_commit.prev_commit_hash.as_deref(),
            Some(previous_commit.compute_hash().as_str())
        );
        assert_eq!(current_commit.operation, "SyncCompact");

        let post_repair = reconciler
            .check(CatalogDomain::Catalog)
            .await
            .expect("recheck report");
        assert!(
            post_repair
                .issues_of_type(IssueType::MissingCurrentHeadLegacyMirror)
                .is_empty()
        );
        assert!(
            post_repair
                .issues_of_type(IssueType::MissingCurrentHeadCommitRecord)
                .is_empty()
        );
        assert_eq!(
            post_repair.manifest_snapshot_version,
            current_manifest.snapshot_version
        );

        let metrics = handle.render();
        assert_metric_lines_contain(
            &metrics,
            crate::metrics::RECONCILER_ISSUES,
            "issue_type=\"missing_current_head_legacy_mirror\"",
        );
        assert_metric_lines_contain(
            &metrics,
            crate::metrics::RECONCILER_ISSUES,
            "issue_type=\"missing_current_head_commit_record\"",
        );
        assert_metric_lines_contain(
            &metrics,
            crate::metrics::RECONCILER_REPAIRS,
            "issue_type=\"missing_current_head_legacy_mirror\"",
        );
        assert_metric_lines_contain(
            &metrics,
            crate::metrics::RECONCILER_REPAIRS,
            "status=\"repaired\"",
        );
    }

    #[tokio::test]
    async fn repair_restores_stale_current_head_legacy_mirror_after_pointer_advances() {
        let legacy_path = CatalogPaths::domain_manifest(CatalogDomain::Catalog);
        let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
        let advanced_manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, "00000000000000000003");
        let advanced_pointer = DomainManifestPointer {
            manifest_id: crate::manifest::format_manifest_id(3),
            manifest_path: advanced_manifest_path.clone(),
            epoch: 3,
            parent_pointer_hash: None,
            updated_at: Utc::now(),
        };
        let backend = Arc::new(CatalogRepairRaceBackend::new(
            legacy_path.clone(),
            pointer_path.clone(),
            Bytes::from(serde_json::to_vec(&advanced_pointer).expect("serialize advanced pointer")),
        ));
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");
        let (previous_manifest, current_manifest, previous_commit, current_manifest_path) =
            seed_catalog_pointer_head_with_missing_side_effects(&storage).await;

        write_json(
            &storage,
            &legacy_path,
            &previous_manifest,
            WritePrecondition::None,
        )
        .await;
        write_json(
            &storage,
            &CatalogPaths::commit(CatalogDomain::Catalog, "commit_02"),
            &CommitRecord {
                commit_id: "commit_02".to_string(),
                prev_commit_id: Some(previous_commit.commit_id.clone()),
                prev_commit_hash: Some(previous_commit.compute_hash()),
                operation: "SyncCompact".to_string(),
                payload_hash: "sha256:current".to_string(),
                created_at: Utc::now(),
            },
            WritePrecondition::DoesNotExist,
        )
        .await;

        let mut advanced_manifest = current_manifest.clone();
        let mut advanced_snapshot = crate::manifest::SnapshotInfo::new(
            3,
            CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 3),
        );
        advanced_snapshot.add_file(crate::manifest::SnapshotFile {
            path: "advanced.parquet".to_string(),
            checksum_sha256: "33".repeat(32),
            byte_size: 1,
            row_count: 1,
            position_range: None,
        });
        advanced_manifest.manifest_id = crate::manifest::format_manifest_id(3);
        advanced_manifest.epoch = 3;
        advanced_manifest.previous_manifest_path = Some(current_manifest_path);
        advanced_manifest.snapshot_version = 3;
        advanced_manifest.snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 3);
        advanced_manifest.snapshot = Some(advanced_snapshot);
        advanced_manifest.watermark_event_id = Some("evt_advanced".to_string());
        advanced_manifest.last_commit_id = Some("commit_03".to_string());
        advanced_manifest.fencing_token = Some(3);
        advanced_manifest.commit_ulid = Some("commit_03".to_string());
        advanced_manifest.updated_at = Utc::now();
        write_json(
            &storage,
            &advanced_manifest_path,
            &advanced_manifest,
            WritePrecondition::DoesNotExist,
        )
        .await;
        storage
            .put_raw(
                &CatalogPaths::snapshot_file(CatalogDomain::Catalog, 3, "advanced.parquet"),
                Bytes::from_static(b"advanced"),
                WritePrecondition::None,
            )
            .await
            .expect("write advanced snapshot");
        write_json(
            &storage,
            &CatalogPaths::commit(CatalogDomain::Catalog, "commit_03"),
            &CommitRecord {
                commit_id: "commit_03".to_string(),
                prev_commit_id: Some("commit_02".to_string()),
                prev_commit_hash: Some("sha256:commit02".to_string()),
                operation: "SyncCompact".to_string(),
                payload_hash: "sha256:advanced".to_string(),
                created_at: Utc::now(),
            },
            WritePrecondition::DoesNotExist,
        )
        .await;

        let reconciler = Reconciler::new(storage.clone());
        let report = reconciler
            .check(CatalogDomain::Catalog)
            .await
            .expect("check stale legacy mirror");

        assert_eq!(
            report
                .issues_of_type(IssueType::StaleCurrentHeadLegacyMirror)
                .len(),
            1
        );
        assert!(
            report
                .issues_of_type(IssueType::MissingCurrentHeadCommitRecord)
                .is_empty(),
            "commit record should already exist for the current head"
        );

        let repair = reconciler.repair(&report).await.expect("repair");
        assert_eq!(repair.failed_count, 0);

        let legacy_bytes = storage
            .get_raw(&CatalogPaths::domain_manifest(CatalogDomain::Catalog))
            .await
            .expect("legacy mirror exists");
        let advanced_bytes = storage
            .get_raw(&advanced_manifest_path)
            .await
            .expect("advanced head exists");
        assert_eq!(
            legacy_bytes, advanced_bytes,
            "repair must converge to the latest pointer target even if the pointer advances mid-repair"
        );

        let post_repair = reconciler
            .check(CatalogDomain::Catalog)
            .await
            .expect("recheck after repair");
        assert!(
            post_repair
                .issues_of_type(IssueType::StaleCurrentHeadLegacyMirror)
                .is_empty()
        );
    }
}
