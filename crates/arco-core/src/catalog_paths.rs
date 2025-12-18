//! Canonical storage paths for the Arco catalog.
//!
//! This module is the **single source of truth** for all catalog storage paths.
//! All writers must use these functions to construct paths. No hardcoded path
//! strings should exist outside this module.
//!
//! # Path Layout (per ADR-005)
//!
//! ```text
//! tenant={tenant}/workspace={workspace}/
//! ├── manifests/
//! │   ├── root.manifest.json
//! │   ├── catalog.manifest.json
//! │   ├── lineage.manifest.json
//! │   ├── executions.manifest.json
//! │   └── search.manifest.json
//! ├── locks/
//! │   ├── catalog.lock.json
//! │   ├── lineage.lock.json
//! │   ├── executions.lock.json
//! │   └── search.lock.json
//! ├── commits/
//! │   └── {domain}/
//! │       └── {commit_id}.json
//! ├── snapshots/
//! │   └── {domain}/
//! │       └── v{version}/
//! │           └── *.parquet
//! ├── ledger/
//! │   └── {domain}/
//! │       └── {event_id}.json
//! ├── sequence/                     # Tier-2 ingest sequencing (monotonic)
//! │   └── {domain}.sequence.json
//! ├── quarantine/
//! │   └── {domain}/
//! │       └── {event_id}.json
//! └── state/
//!     └── {domain}/
//!         └── snapshot_v{version}_{ulid}.parquet
//! ```
//!
//! # Domains (per ADR-003)
//!
//! - `catalog`: Namespaces, tables, columns (Tier-1, low frequency DDL)
//! - `lineage`: Lineage edges and graph (Tier-1, medium frequency)
//! - `executions`: Run/task execution state (Tier-2, high frequency)
//! - `search`: Token postings index (Tier-1, low frequency rebuild)

/// Canonical catalog domains per ADR-003.
///
/// Each domain has its own manifest, lock, and data paths to reduce contention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CatalogDomain {
    /// Catalog domain: namespaces, tables, columns (Tier-1)
    Catalog,
    /// Lineage domain: lineage edges and graph (Tier-1)
    Lineage,
    /// Executions domain: run/task state (Tier-2)
    Executions,
    /// Search domain: token postings index (Tier-1)
    Search,
}

impl CatalogDomain {
    /// Returns the string name for this domain.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Lineage => "lineage",
            Self::Executions => "executions",
            Self::Search => "search",
        }
    }

    /// Returns all domains.
    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[Self::Catalog, Self::Lineage, Self::Executions, Self::Search]
    }
}

impl std::fmt::Display for CatalogDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Canonical path generator for catalog storage.
///
/// All path generation goes through this struct to ensure consistency
/// with ADR-005 (Storage Layout).
///
/// # Example
///
/// ```
/// use arco_core::catalog_paths::{CatalogPaths, CatalogDomain};
///
/// assert_eq!(CatalogPaths::ROOT_MANIFEST, "manifests/root.manifest.json");
/// assert_eq!(
///     CatalogPaths::domain_manifest(CatalogDomain::Catalog),
///     "manifests/catalog.manifest.json"
/// );
/// ```
pub struct CatalogPaths;

impl CatalogPaths {
    // =========================================================================
    // Constants
    // =========================================================================

    /// Root manifest file path (entry point for all readers).
    pub const ROOT_MANIFEST: &'static str = "manifests/root.manifest.json";

    // =========================================================================
    // Manifest Paths
    // =========================================================================

    /// Returns the manifest path for a domain.
    #[must_use]
    pub fn domain_manifest(domain: CatalogDomain) -> String {
        format!("manifests/{}.manifest.json", domain.as_str())
    }

    /// Returns the manifest path for a domain by string name.
    ///
    /// Supports both canonical names (`catalog`) and legacy names (`core`).
    #[must_use]
    pub fn domain_manifest_str(domain: &str) -> String {
        let normalized = Self::normalize_domain_name(domain);
        format!("manifests/{normalized}.manifest.json")
    }

    // =========================================================================
    // Lock Paths
    // =========================================================================

    /// Returns the lock path for a domain.
    #[must_use]
    pub fn domain_lock(domain: CatalogDomain) -> String {
        format!("locks/{}.lock.json", domain.as_str())
    }

    /// Returns the lock path for a domain by string name.
    #[must_use]
    pub fn domain_lock_str(domain: &str) -> String {
        let normalized = Self::normalize_domain_name(domain);
        format!("locks/{normalized}.lock.json")
    }

    // =========================================================================
    // Commit Paths (Tier-1 audit chain)
    // =========================================================================

    /// Returns the commit record path for a domain.
    #[must_use]
    pub fn commit(domain: CatalogDomain, commit_id: &str) -> String {
        format!("commits/{}/{}.json", domain.as_str(), commit_id)
    }

    /// Returns the commits directory for a domain.
    #[must_use]
    pub fn commits_dir(domain: CatalogDomain) -> String {
        format!("commits/{}/", domain.as_str())
    }

    // =========================================================================
    // Snapshot Paths (Tier-1 Parquet output)
    // =========================================================================

    /// Returns the snapshot directory for a domain version.
    #[must_use]
    pub fn snapshot_dir(domain: CatalogDomain, version: u64) -> String {
        format!("snapshots/{}/v{version}/", domain.as_str())
    }

    /// Returns a specific snapshot file path.
    #[must_use]
    pub fn snapshot_file(domain: CatalogDomain, version: u64, filename: &str) -> String {
        format!("snapshots/{}/v{version}/{filename}", domain.as_str())
    }

    // =========================================================================
    // Ledger Paths (Tier-2 append-only events)
    // =========================================================================

    /// Returns the ledger event path.
    #[must_use]
    pub fn ledger_event(domain: CatalogDomain, event_id: &str) -> String {
        format!("ledger/{}/{}.json", domain.as_str(), event_id)
    }

    /// Returns the ledger directory for a domain.
    #[must_use]
    pub fn ledger_dir(domain: CatalogDomain) -> String {
        format!("ledger/{}/", domain.as_str())
    }

    // =========================================================================
    // Sequence Paths (Tier-2 ingest ordering)
    // =========================================================================

    /// Returns the monotonic ingest sequence counter path for a domain.
    ///
    /// This file is used by Tier-2 ingestion to assign `sequence_position` to events
    /// (ADR-004) so compaction can advance `watermark_position` without relying on
    /// event ID ordering.
    #[must_use]
    pub fn sequence_counter(domain: CatalogDomain) -> String {
        format!("sequence/{}.sequence.json", domain.as_str())
    }

    // =========================================================================
    // Quarantine Paths (dead-letter for operators)
    // =========================================================================

    /// Returns the quarantine directory for a domain.
    ///
    /// Used to store events that could not be compacted (unknown version/type)
    /// or that arrived late relative to the current watermark.
    #[must_use]
    pub fn quarantine_dir(domain: CatalogDomain) -> String {
        format!("quarantine/{}/", domain.as_str())
    }

    /// Returns the quarantine event path (expects a filename including `.json`).
    #[must_use]
    pub fn quarantine_event(domain: CatalogDomain, filename: &str) -> String {
        format!("quarantine/{}/{}", domain.as_str(), filename)
    }

    // =========================================================================
    // State Paths (Tier-2 compacted Parquet output)
    // =========================================================================

    /// Returns the state snapshot path for a domain.
    #[must_use]
    pub fn state_snapshot(domain: CatalogDomain, version: u64, ulid: &str) -> String {
        format!(
            "state/{}/snapshot_v{version}_{ulid}.parquet",
            domain.as_str()
        )
    }

    /// Returns the state directory for a domain.
    #[must_use]
    pub fn state_dir(domain: CatalogDomain) -> String {
        format!("state/{}/", domain.as_str())
    }

    // =========================================================================
    // Legacy Name Migration (per ADR-003)
    // =========================================================================

    /// Normalizes domain names for migration compatibility.
    ///
    /// Maps legacy names to canonical names:
    /// - `core` → `catalog`
    /// - `execution` → `executions`
    /// - `governance` → `search`
    ///
    /// **Migration visibility**: This function logs a warning when legacy names
    /// are encountered, enabling detection of code still using old naming conventions.
    /// Monitor `legacy_domain_name_normalized` log entries in staging to verify
    /// migration completion.
    #[must_use]
    pub fn normalize_domain_name(name: &str) -> &str {
        match name {
            // Legacy → Canonical mappings (log for migration visibility)
            "core" => {
                tracing::warn!(
                    from = "core",
                    to = "catalog",
                    "legacy_domain_name_normalized: legacy domain name detected, consider migrating to canonical name"
                );
                "catalog"
            }
            "execution" => {
                tracing::warn!(
                    from = "execution",
                    to = "executions",
                    "legacy_domain_name_normalized: legacy domain name detected, consider migrating to canonical name"
                );
                "executions"
            }
            "governance" => {
                tracing::warn!(
                    from = "governance",
                    to = "search",
                    "legacy_domain_name_normalized: legacy domain name detected, consider migrating to canonical name"
                );
                "search"
            }
            // Already canonical
            other => other,
        }
    }

    /// Returns true if this is a legacy domain name.
    #[must_use]
    pub fn is_legacy_domain_name(name: &str) -> bool {
        matches!(name, "core" | "execution" | "governance")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_manifest_path() {
        assert_eq!(CatalogPaths::ROOT_MANIFEST, "manifests/root.manifest.json");
    }

    #[test]
    fn test_domain_manifest_paths() {
        assert_eq!(
            CatalogPaths::domain_manifest(CatalogDomain::Catalog),
            "manifests/catalog.manifest.json"
        );
        assert_eq!(
            CatalogPaths::domain_manifest(CatalogDomain::Lineage),
            "manifests/lineage.manifest.json"
        );
        assert_eq!(
            CatalogPaths::domain_manifest(CatalogDomain::Executions),
            "manifests/executions.manifest.json"
        );
        assert_eq!(
            CatalogPaths::domain_manifest(CatalogDomain::Search),
            "manifests/search.manifest.json"
        );
    }

    #[test]
    fn test_domain_lock_paths() {
        assert_eq!(
            CatalogPaths::domain_lock(CatalogDomain::Catalog),
            "locks/catalog.lock.json"
        );
        assert_eq!(
            CatalogPaths::domain_lock(CatalogDomain::Lineage),
            "locks/lineage.lock.json"
        );
    }

    #[test]
    fn test_commit_paths() {
        assert_eq!(
            CatalogPaths::commit(CatalogDomain::Catalog, "01ARZ3NDEKTSV4RRFFQ69G5FAV"),
            "commits/catalog/01ARZ3NDEKTSV4RRFFQ69G5FAV.json"
        );
    }

    #[test]
    fn test_snapshot_paths() {
        assert_eq!(
            CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 42),
            "snapshots/catalog/v42/"
        );
        assert_eq!(
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, 42, "namespaces.parquet"),
            "snapshots/catalog/v42/namespaces.parquet"
        );
    }

    #[test]
    fn test_ledger_paths() {
        assert_eq!(
            CatalogPaths::ledger_event(CatalogDomain::Executions, "01ARZ3NDEK"),
            "ledger/executions/01ARZ3NDEK.json"
        );
        assert_eq!(
            CatalogPaths::ledger_dir(CatalogDomain::Executions),
            "ledger/executions/"
        );
    }

    #[test]
    fn test_state_paths() {
        assert_eq!(
            CatalogPaths::state_snapshot(CatalogDomain::Executions, 1, "01ARZ3NDEK"),
            "state/executions/snapshot_v1_01ARZ3NDEK.parquet"
        );
        assert_eq!(
            CatalogPaths::state_dir(CatalogDomain::Executions),
            "state/executions/"
        );
    }

    #[test]
    fn test_quarantine_paths() {
        assert_eq!(
            CatalogPaths::quarantine_dir(CatalogDomain::Executions),
            "quarantine/executions/"
        );
        assert_eq!(
            CatalogPaths::quarantine_event(CatalogDomain::Executions, "01ARZ3NDEK.json"),
            "quarantine/executions/01ARZ3NDEK.json"
        );
    }

    #[test]
    fn test_sequence_paths() {
        assert_eq!(
            CatalogPaths::sequence_counter(CatalogDomain::Executions),
            "sequence/executions.sequence.json"
        );
    }

    #[test]
    fn test_legacy_name_normalization() {
        // Legacy → Canonical
        assert_eq!(CatalogPaths::normalize_domain_name("core"), "catalog");
        assert_eq!(
            CatalogPaths::normalize_domain_name("execution"),
            "executions"
        );
        assert_eq!(CatalogPaths::normalize_domain_name("governance"), "search");

        // Already canonical (passthrough)
        assert_eq!(CatalogPaths::normalize_domain_name("catalog"), "catalog");
        assert_eq!(CatalogPaths::normalize_domain_name("lineage"), "lineage");
        assert_eq!(
            CatalogPaths::normalize_domain_name("executions"),
            "executions"
        );
        assert_eq!(CatalogPaths::normalize_domain_name("search"), "search");
    }

    #[test]
    fn test_legacy_domain_detection() {
        assert!(CatalogPaths::is_legacy_domain_name("core"));
        assert!(CatalogPaths::is_legacy_domain_name("execution"));
        assert!(CatalogPaths::is_legacy_domain_name("governance"));

        assert!(!CatalogPaths::is_legacy_domain_name("catalog"));
        assert!(!CatalogPaths::is_legacy_domain_name("lineage"));
        assert!(!CatalogPaths::is_legacy_domain_name("executions"));
        assert!(!CatalogPaths::is_legacy_domain_name("search"));
    }

    #[test]
    fn test_domain_manifest_str_with_legacy_names() {
        // Legacy names get normalized
        assert_eq!(
            CatalogPaths::domain_manifest_str("core"),
            "manifests/catalog.manifest.json"
        );
        assert_eq!(
            CatalogPaths::domain_manifest_str("execution"),
            "manifests/executions.manifest.json"
        );
        assert_eq!(
            CatalogPaths::domain_manifest_str("governance"),
            "manifests/search.manifest.json"
        );
    }

    #[test]
    fn test_all_domains() {
        let domains = CatalogDomain::all();
        assert_eq!(domains.len(), 4);
        assert!(domains.contains(&CatalogDomain::Catalog));
        assert!(domains.contains(&CatalogDomain::Lineage));
        assert!(domains.contains(&CatalogDomain::Executions));
        assert!(domains.contains(&CatalogDomain::Search));
    }
}
