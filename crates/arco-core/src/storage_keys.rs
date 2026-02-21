//! Typed storage keys for compile-time path safety (Gate 5).
//!
//! This module provides strongly-typed keys that encode storage path structure
//! and access permissions at the type level. Each key type corresponds to a
//! specific storage prefix with specific access semantics.
//!
//! # Design Philosophy
//!
//! - **Type safety**: Wrong paths cannot be constructed at compile time
//! - **Permission encoding**: Key types encode who can access what
//! - **No stringly-typed paths**: Use `LedgerKey`, `StateKey`, etc. instead of `&str`
//!
//! # Key Types
//!
//! | Key Type | Prefix | Who Writes | Who Reads |
//! |----------|--------|------------|-----------|
//! | `LedgerKey` | `ledger/` | API | API, Compactor |
//! | `StateKey` | `state/`, `snapshots/` | Compactor | API, Compactor |
//! | `ManifestKey` | `manifests/` | Compactor | API, Compactor |
//! | `LockKey` | `locks/` | API | API |
//! | `CommitKey` | `commits/` | API | API, Compactor |
//!
//! # Example
//!
//! ```rust
//! use arco_core::storage_keys::{LedgerKey, ManifestKey};
//! use arco_core::CatalogDomain;
//!
//! // Type-safe key construction
//! let ledger_key = LedgerKey::event(CatalogDomain::Catalog, "01JFXYZ");
//! let manifest_key = ManifestKey::domain(CatalogDomain::Catalog);
//!
//! // Keys implement AsRef<str> for storage operations
//! assert!(ledger_key.as_ref().starts_with("ledger/"));
//! assert!(manifest_key.as_ref().ends_with(".manifest.json"));
//! ```

use crate::CatalogDomain;

/// A typed storage key that encodes path structure.
///
/// All key types implement this trait to provide uniform access to the
/// underlying path string.
pub trait StorageKey: AsRef<str> {
    /// Returns the underlying path string.
    fn path(&self) -> &str {
        self.as_ref()
    }
}

// ============================================================================
// LedgerKey - API writes append-only events
// ============================================================================

/// A typed key for ledger event paths.
///
/// # Access
///
/// - **Write**: API only (via `LedgerPutStore`)
/// - **Read**: API, Compactor (via `ReadStore`)
///
/// # Path Format
///
/// `ledger/{domain}/{event_id}.json`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LedgerKey(String);

impl LedgerKey {
    /// Creates a ledger event key.
    ///
    /// # Arguments
    ///
    /// - `domain`: The catalog domain (catalog, lineage, executions, search)
    /// - `event_id`: The event ID (typically a ULID)
    #[must_use]
    pub fn event(domain: CatalogDomain, event_id: &str) -> Self {
        Self(format!("ledger/{}/{event_id}.json", domain.as_str()))
    }

    /// Creates a key for the ledger directory prefix.
    #[must_use]
    pub fn dir(domain: CatalogDomain) -> Self {
        Self(format!("ledger/{}/", domain.as_str()))
    }
}

impl AsRef<str> for LedgerKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl StorageKey for LedgerKey {}

impl std::fmt::Display for LedgerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ============================================================================
// StateKey - Compactor writes Parquet state
// ============================================================================

/// A typed key for state/snapshot paths.
///
/// # Access
///
/// - **Write**: Compactor only (via `StatePutStore`)
/// - **Read**: API, Compactor (via `ReadStore`)
///
/// # Path Formats
///
/// - Tier-1 snapshots: `snapshots/{domain}/v{version}/{file}.parquet`
/// - Tier-2 state: `state/{domain}/snapshot_v{version}_{ulid}.parquet`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StateKey(String);

impl StateKey {
    /// Creates a key for a Tier-1 snapshot file (e.g., namespaces.parquet).
    #[must_use]
    pub fn snapshot_file(domain: CatalogDomain, version: u64, filename: &str) -> Self {
        Self(format!(
            "snapshots/{}/v{version}/{filename}",
            domain.as_str()
        ))
    }

    /// Creates a key for the snapshot directory.
    #[must_use]
    pub fn snapshot_dir(domain: CatalogDomain, version: u64) -> Self {
        Self(format!("snapshots/{}/v{version}/", domain.as_str()))
    }

    /// Creates a key for a Tier-2 compacted state file.
    #[must_use]
    pub fn state_snapshot(domain: CatalogDomain, version: u64, ulid: &str) -> Self {
        Self(format!(
            "state/{}/snapshot_v{version}_{ulid}.parquet",
            domain.as_str()
        ))
    }

    /// Creates a key for the state directory prefix.
    #[must_use]
    pub fn state_dir(domain: CatalogDomain) -> Self {
        Self(format!("state/{}/", domain.as_str()))
    }
}

impl AsRef<str> for StateKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl StorageKey for StateKey {}

impl std::fmt::Display for StateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ============================================================================
// ManifestKey - Compactor CAS-updates manifests
// ============================================================================

/// A typed key for manifest paths.
///
/// # Access
///
/// - **Write**: Compactor only (via `CasStore`)
/// - **Read**: API, Compactor (via `ReadStore`)
///
/// # Path Format
///
/// - Root: `manifests/root.manifest.json`
/// - Domain: `manifests/{domain}.manifest.json`
/// - Domain pointer: `manifests/{domain}.pointer.json`
/// - Domain snapshot: `manifests/{domain}/{manifest_id}.json`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ManifestKey(String);

impl ManifestKey {
    /// The root manifest key (entry point for all readers).
    pub const ROOT: &'static str = "manifests/root.manifest.json";

    /// Creates the root manifest key.
    #[must_use]
    pub fn root() -> Self {
        Self(Self::ROOT.to_string())
    }

    /// Creates a domain manifest key.
    #[must_use]
    pub fn domain(domain: CatalogDomain) -> Self {
        Self(format!("manifests/{}.manifest.json", domain.as_str()))
    }

    /// Creates a domain manifest pointer key.
    #[must_use]
    pub fn domain_pointer(domain: CatalogDomain) -> Self {
        Self(format!("manifests/{}.pointer.json", domain.as_str()))
    }

    /// Creates an immutable domain manifest snapshot key.
    ///
    /// The manifest identifier is encoded as a fixed-width 20-digit decimal.
    #[must_use]
    pub fn domain_snapshot(domain: CatalogDomain, manifest_id: u64) -> Self {
        Self::domain_snapshot_id(domain, &format!("{manifest_id:020}"))
    }

    /// Creates an immutable domain manifest snapshot key from an explicit ID.
    #[must_use]
    pub fn domain_snapshot_id(domain: CatalogDomain, manifest_id: &str) -> Self {
        Self(format!(
            "manifests/{}/{}.json",
            domain.as_str(),
            manifest_id
        ))
    }
}

impl AsRef<str> for ManifestKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl StorageKey for ManifestKey {}

impl std::fmt::Display for ManifestKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ============================================================================
// LockKey - API manages distributed locks
// ============================================================================

/// A typed key for lock paths.
///
/// # Access
///
/// - **Write**: API only (via `LockPutStore`)
/// - **Read**: API (via `ReadStore`)
///
/// # Path Format
///
/// `locks/{domain}.lock.json`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LockKey(String);

impl LockKey {
    /// Creates a domain lock key.
    #[must_use]
    pub fn domain(domain: CatalogDomain) -> Self {
        Self(format!("locks/{}.lock.json", domain.as_str()))
    }

    /// Creates a custom lock key (for asset-level locks).
    #[must_use]
    pub fn custom(path: &str) -> Self {
        Self(format!("locks/{path}.lock.json"))
    }
}

impl AsRef<str> for LockKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl StorageKey for LockKey {}

impl std::fmt::Display for LockKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ============================================================================
// CommitKey - API writes audit trail
// ============================================================================

/// A typed key for commit record paths (audit trail).
///
/// # Access
///
/// - **Write**: API, Compactor (via `CommitPutStore`)
/// - **Read**: API, Compactor (via `ReadStore`)
///
/// # Path Format
///
/// `commits/{domain}/{commit_id}.json`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommitKey(String);

impl CommitKey {
    /// Creates a commit record key.
    #[must_use]
    pub fn record(domain: CatalogDomain, commit_id: &str) -> Self {
        Self(format!("commits/{}/{commit_id}.json", domain.as_str()))
    }

    /// Creates a key for the commits directory prefix.
    #[must_use]
    pub fn dir(domain: CatalogDomain) -> Self {
        Self(format!("commits/{}/", domain.as_str()))
    }
}

impl AsRef<str> for CommitKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl StorageKey for CommitKey {}

impl std::fmt::Display for CommitKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ============================================================================
// SequenceKey - API CAS-updates sequence counters
// ============================================================================

/// A typed key for sequence counter paths.
///
/// # Access
///
/// - **Write**: API (via CAS for atomicity)
/// - **Read**: API
///
/// # Path Format
///
/// `sequence/{domain}.sequence.json`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SequenceKey(String);

impl SequenceKey {
    /// Creates a sequence counter key.
    #[must_use]
    pub fn counter(domain: CatalogDomain) -> Self {
        Self(format!("sequence/{}.sequence.json", domain.as_str()))
    }
}

impl AsRef<str> for SequenceKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl StorageKey for SequenceKey {}

impl std::fmt::Display for SequenceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ============================================================================
// QuarantineKey - Compactor writes dead-letter events
// ============================================================================

/// A typed key for quarantine paths.
///
/// # Access
///
/// - **Write**: Compactor only
/// - **Read**: Operators (out-of-band)
///
/// # Path Format
///
/// `quarantine/{domain}/{filename}`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuarantineKey(String);

impl QuarantineKey {
    /// Creates a quarantine event key.
    #[must_use]
    pub fn event(domain: CatalogDomain, filename: &str) -> Self {
        Self(format!("quarantine/{}/{filename}", domain.as_str()))
    }

    /// Creates a key for the quarantine directory prefix.
    #[must_use]
    pub fn dir(domain: CatalogDomain) -> Self {
        Self(format!("quarantine/{}/", domain.as_str()))
    }
}

impl AsRef<str> for QuarantineKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl StorageKey for QuarantineKey {}

impl std::fmt::Display for QuarantineKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ledger_key_format() {
        let key = LedgerKey::event(CatalogDomain::Catalog, "01JFXYZ");
        assert_eq!(key.as_ref(), "ledger/catalog/01JFXYZ.json");

        let dir = LedgerKey::dir(CatalogDomain::Executions);
        assert_eq!(dir.as_ref(), "ledger/executions/");
    }

    #[test]
    fn test_state_key_format() {
        let snapshot = StateKey::snapshot_file(CatalogDomain::Catalog, 42, "namespaces.parquet");
        assert_eq!(
            snapshot.as_ref(),
            "snapshots/catalog/v42/namespaces.parquet"
        );

        let state = StateKey::state_snapshot(CatalogDomain::Executions, 1, "01JFXYZ");
        assert_eq!(
            state.as_ref(),
            "state/executions/snapshot_v1_01JFXYZ.parquet"
        );
    }

    #[test]
    fn test_manifest_key_format() {
        let root = ManifestKey::root();
        assert_eq!(root.as_ref(), "manifests/root.manifest.json");

        let domain = ManifestKey::domain(CatalogDomain::Catalog);
        assert_eq!(domain.as_ref(), "manifests/catalog.manifest.json");

        let pointer = ManifestKey::domain_pointer(CatalogDomain::Catalog);
        assert_eq!(pointer.as_ref(), "manifests/catalog.pointer.json");

        let snapshot = ManifestKey::domain_snapshot(CatalogDomain::Catalog, 42);
        assert_eq!(
            snapshot.as_ref(),
            "manifests/catalog/00000000000000000042.json"
        );

        let snapshot_id =
            ManifestKey::domain_snapshot_id(CatalogDomain::Search, "00000000000000012345");
        assert_eq!(
            snapshot_id.as_ref(),
            "manifests/search/00000000000000012345.json"
        );
    }

    #[test]
    fn test_lock_key_format() {
        let lock = LockKey::domain(CatalogDomain::Catalog);
        assert_eq!(lock.as_ref(), "locks/catalog.lock.json");

        let custom = LockKey::custom("assets/asset_123");
        assert_eq!(custom.as_ref(), "locks/assets/asset_123.lock.json");
    }

    #[test]
    fn test_commit_key_format() {
        let key = CommitKey::record(CatalogDomain::Catalog, "01JFXYZ");
        assert_eq!(key.as_ref(), "commits/catalog/01JFXYZ.json");
    }

    #[test]
    fn test_sequence_key_format() {
        let key = SequenceKey::counter(CatalogDomain::Executions);
        assert_eq!(key.as_ref(), "sequence/executions.sequence.json");
    }

    #[test]
    fn test_quarantine_key_format() {
        let key = QuarantineKey::event(CatalogDomain::Executions, "01JFXYZ.json");
        assert_eq!(key.as_ref(), "quarantine/executions/01JFXYZ.json");
    }

    #[test]
    fn test_keys_implement_display() {
        let key = LedgerKey::event(CatalogDomain::Catalog, "test");
        assert_eq!(format!("{key}"), "ledger/catalog/test.json");
    }
}
