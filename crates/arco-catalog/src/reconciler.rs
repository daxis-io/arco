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
use crate::manifest::{CatalogDomainManifest, ExecutionsManifest, LineageManifest, RootManifest};

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

        Ok(report)
    }

    /// Repairs issues found in a reconciliation report.
    ///
    /// Only issues marked as `repairable: true` will be addressed.
    ///
    /// # Errors
    ///
    /// Returns an error if storage operations fail while attempting repairs.
    pub async fn repair(&self, report: &ReconciliationReport) -> Result<RepairResult> {
        let mut result = RepairResult {
            domain: report.domain.clone(),
            repaired_at: Utc::now(),
            repaired_count: 0,
            skipped_count: 0,
            failed_count: 0,
        };

        for issue in &report.issues {
            if !issue.repairable {
                result.skipped_count += 1;
                continue;
            }

            match issue.issue_type {
                IssueType::OrphanedSnapshot | IssueType::OldSnapshotVersion => {
                    match self.storage.delete(&issue.path).await {
                        Ok(()) => result.repaired_count += 1,
                        Err(e) => {
                            tracing::error!(path = %issue.path, error = %e, "Failed to delete orphaned/old snapshot");
                            result.failed_count += 1;
                        }
                    }
                }
                _ => result.skipped_count += 1,
            }
        }

        Ok(result)
    }

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

        let domain_path = match domain {
            CatalogDomain::Catalog => root.catalog_manifest_path,
            CatalogDomain::Lineage => root.lineage_manifest_path,
            CatalogDomain::Executions => root.executions_manifest_path,
            CatalogDomain::Search => root.search_manifest_path,
        };

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
                // Search snapshots are not yet implemented. We can still validate that the
                // manifest exists, but we don't have a manifest-driven allowlist of files.
                let Some(manifest) = self
                    .get_json::<crate::manifest::SearchManifest>(&domain_path)
                    .await?
                else {
                    return Ok(None);
                };

                let version = manifest.snapshot_version;
                let expected = Vec::new();
                let snapshot_prefix = format!("snapshots/{}/", domain.as_str());
                Ok(Some((version, expected, snapshot_prefix)))
            }
        }
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<Option<T>> {
        let bytes = match self.storage.get_raw(path).await {
            Ok(b) => b,
            Err(arco_core::Error::NotFound(_)) => return Ok(None),
            Err(e) => return Err(CatalogError::from(e)),
        };

        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse JSON at '{path}': {e}"),
            })
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<String>> {
        let paths = self.storage.list(prefix).await?;
        Ok(paths.into_iter().map(|p| p.to_string()).collect())
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

    use arco_core::storage::{MemoryBackend, WritePrecondition};
    use bytes::Bytes;

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
            snapshot_version: 1,
            snapshot_path: snapshot_path.clone(),
            snapshot: Some(snapshot),
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
}
