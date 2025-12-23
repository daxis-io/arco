//! Physically multi-file manifest structure per architecture docs.
//!
//! The catalog uses separate manifest files to reduce contention:
//! - `root.manifest.json`: Pointers to domain manifests
//! - `catalog.manifest.json`: Tier 1 (namespaces, tables, columns) - locked writes
//! - `lineage.manifest.json`: Tier 1 (dependency edges) - locked writes
//! - `executions.manifest.json`: Tier 2 (execution state) - compactor writes
//! - `search.manifest.json`: Tier 1 (token postings) - locked writes
//!
//! Each domain manifest is a separate file that can be updated independently.
//!
//! # Storage Layout
//!
//! ```text
//! tenant={tenant}/workspace={workspace}/manifests/
//! ├── root.manifest.json        # Root pointer to domain manifests
//! ├── catalog.manifest.json     # Tier 1: Catalog state (locked writes)
//! ├── lineage.manifest.json     # Tier 1: Lineage state (locked writes)
//! ├── executions.manifest.json  # Tier 2: Execution state (compactor writes)
//! └── search.manifest.json      # Tier 1: Search state (locked writes)
//! ```

use arco_core::{CatalogDomain, CatalogPaths};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ============================================================================
// Raw Byte Hashing for Rollback Detection
// ============================================================================

/// Computes a cryptographic hash from raw manifest bytes.
///
/// **Critical:** This function takes the exact bytes read from storage,
/// NOT re-serialized JSON. Re-serialization may produce different bytes
/// due to JSON field ordering, whitespace, or other formatting differences.
///
/// # Arguments
///
/// - `raw_bytes`: The exact bytes read from storage
///
/// # Returns
///
/// A string in the format `sha256:{hex}` where `{hex}` is the lowercase
/// hex-encoded SHA-256 hash.
///
/// # Example
///
/// ```rust
/// use arco_catalog::manifest::compute_manifest_hash;
///
/// let raw_bytes = br#"{"snapshotVersion":1,"snapshotPath":"..."}"#;
/// let hash = compute_manifest_hash(raw_bytes);
/// assert!(hash.starts_with("sha256:"));
/// ```
#[must_use]
pub fn compute_manifest_hash(raw_bytes: &[u8]) -> String {
    let hash = Sha256::digest(raw_bytes);
    format!("sha256:{}", hex::encode(hash))
}

// ============================================================================
// Root Manifest (pointer to domain manifests)
// ============================================================================

/// Root manifest containing only paths to domain manifests.
///
/// This is the entry point - readers load this first, then fetch
/// domain manifests as needed. Critically, this contains NO embedded
/// content, just paths.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootManifest {
    /// Manifest schema version.
    pub version: u32,

    /// Path to catalog domain manifest.
    #[serde(alias = "coreManifestPath")]
    pub catalog_manifest_path: String,

    /// Path to lineage domain manifest.
    #[serde(default = "RootManifest::default_lineage_manifest_path")]
    pub lineage_manifest_path: String,

    /// Path to executions domain manifest.
    #[serde(alias = "executionManifestPath")]
    pub executions_manifest_path: String,

    /// Path to search domain manifest.
    #[serde(
        default = "RootManifest::default_search_manifest_path",
        alias = "governanceManifestPath"
    )]
    pub search_manifest_path: String,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl RootManifest {
    /// Creates a new root manifest with default paths.
    #[must_use]
    pub fn new() -> Self {
        Self {
            version: 1,
            catalog_manifest_path: CatalogPaths::domain_manifest(CatalogDomain::Catalog),
            lineage_manifest_path: Self::default_lineage_manifest_path(),
            executions_manifest_path: CatalogPaths::domain_manifest(CatalogDomain::Executions),
            search_manifest_path: Self::default_search_manifest_path(),
            updated_at: Utc::now(),
        }
    }

    fn default_lineage_manifest_path() -> String {
        CatalogPaths::domain_manifest(CatalogDomain::Lineage)
    }

    fn default_search_manifest_path() -> String {
        CatalogPaths::domain_manifest(CatalogDomain::Search)
    }

    /// Normalizes legacy manifest paths to canonical paths.
    ///
    /// Supports migrations from older domain names (e.g., `core` → `catalog`).
    pub fn normalize_paths(&mut self) {
        self.catalog_manifest_path = Self::normalize_manifest_path(&self.catalog_manifest_path);
        self.lineage_manifest_path = Self::normalize_manifest_path(&self.lineage_manifest_path);
        self.executions_manifest_path =
            Self::normalize_manifest_path(&self.executions_manifest_path);
        self.search_manifest_path = Self::normalize_manifest_path(&self.search_manifest_path);
    }

    fn normalize_manifest_path(path: &str) -> String {
        let Some(domain) = path
            .strip_prefix("manifests/")
            .and_then(|p| p.strip_suffix(".manifest.json"))
        else {
            return path.to_string();
        };

        CatalogPaths::domain_manifest_str(domain)
    }
}

impl Default for RootManifest {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Domain Manifests (each is a separate file)
// ============================================================================

/// Catalog domain manifest (Tier 1) - separate file.
///
/// Written via locking protocol. Contains assets, schemas.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogDomainManifest {
    /// Current snapshot version (monotonically increasing).
    pub snapshot_version: u64,

    /// Path to snapshot directory.
    pub snapshot_path: String,

    /// Optional enhanced snapshot metadata (per-file checksums, sizes, row counts).
    ///
    /// When present, this is the canonical allowlist for signed URL minting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SnapshotInfo>,

    /// Last commit ID for hash chain integrity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_commit_id: Option<String>,

    /// Fencing token for distributed lock validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,

    /// Commit ULID for audit correlation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_ulid: Option<String>,

    /// Parent manifest hash (sha256 of RAW stored bytes).
    ///
    /// **Critical:** This must be computed from the exact bytes read from
    /// storage, NOT from re-serializing the manifest struct. Re-serialization
    /// may produce different bytes (JSON field ordering, whitespace, etc.).
    ///
    /// Used for:
    /// - **Rollback detection**: Ensures version cannot regress
    /// - **Concurrent modification detection**: Hash mismatch means someone else modified
    /// - **Integrity verification**: Chain from current to previous manifest
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_hash: Option<String>,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl CatalogDomainManifest {
    /// Creates a new catalog domain manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot_version: 0,
            snapshot_path: CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 0),
            snapshot: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: None,
            parent_hash: None,
            updated_at: Utc::now(),
        }
    }

    /// Returns the next snapshot version.
    #[must_use]
    pub fn next_version(&self) -> u64 {
        self.snapshot_version + 1
    }

    /// Validates that this manifest can succeed the given previous manifest.
    ///
    /// # Arguments
    ///
    /// - `previous`: The previous manifest struct
    /// - `previous_raw_hash`: Hash computed from raw stored bytes of previous
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if succession is valid, or an error describing the violation.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `snapshot_version` decreased (rollback attempt)
    /// - `parent_hash` doesn't match `previous_raw_hash` (concurrent modification)
    pub fn validate_succession(
        &self,
        previous: &Self,
        previous_raw_hash: &str,
    ) -> Result<(), String> {
        // Version must not decrease (rollback detection)
        if self.snapshot_version < previous.snapshot_version {
            return Err(format!(
                "version regression: {} -> {} (rollback not allowed)",
                previous.snapshot_version, self.snapshot_version
            ));
        }

        // Parent hash must match raw stored bytes (concurrent modification detection)
        if let Some(ref parent_hash) = self.parent_hash {
            if parent_hash != previous_raw_hash {
                return Err(format!(
                    "parent hash mismatch: expected {previous_raw_hash}, got {parent_hash} (concurrent modification detected)"
                ));
            }
        }

        if let (Some(prev), Some(next)) = (previous.fencing_token, self.fencing_token) {
            if next < prev {
                return Err(format!(
                    "fencing token regression: {prev} -> {next} (stale holder)"
                ));
            }
        }

        if let (Some(prev), Some(next)) = (
            previous.commit_ulid.as_deref(),
            self.commit_ulid.as_deref(),
        ) {
            if next <= prev {
                return Err(format!(
                    "commit ulid regression: {prev} -> {next} (non-monotonic)"
                ));
            }
        }

        Ok(())
    }
}

impl Default for CatalogDomainManifest {
    fn default() -> Self {
        Self::new()
    }
}

/// Backwards-compatible alias for older naming.
pub type CoreManifest = CatalogDomainManifest;

/// Metadata about compaction operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionMetadata {
    /// Total events compacted.
    #[serde(default)]
    pub total_events_compacted: u64,
    /// Total Parquet files written.
    #[serde(default)]
    pub total_files_written: u64,
}

// ============================================================================
// Snapshot Info (per-file metadata for manifest-driven URL allowlist)
// ============================================================================

/// Metadata for a single Parquet file in a snapshot.
///
/// Used for:
/// - **Manifest-driven URL allowlist**: Only mint URLs for files in `SnapshotInfo`
/// - **Integrity verification**: Checksums detect corruption/tampering
/// - **Query optimization**: Row counts for cost estimation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotFile {
    /// Relative path within snapshot directory.
    pub path: String,

    /// SHA-256 checksum of file contents (hex encoded).
    pub checksum_sha256: String,

    /// File size in bytes.
    pub byte_size: u64,

    /// Number of rows in the Parquet file.
    pub row_count: u64,

    /// Optional min/max position range for L0 delta files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position_range: Option<(u64, u64)>,
}

/// Complete snapshot metadata stored in domain manifest.
///
/// The manifest stores a `SnapshotInfo` per domain, enabling:
/// - Readers to locate current snapshot files
/// - URL minting to validate requested paths
/// - Integrity verification via checksums
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotInfo {
    /// Snapshot version (monotonically increasing).
    pub version: u64,

    /// Snapshot directory path.
    pub path: String,

    /// List of files with checksums and metadata.
    pub files: Vec<SnapshotFile>,

    /// When this snapshot was published.
    pub published_at: DateTime<Utc>,

    /// Total row count across all files.
    pub total_rows: u64,

    /// Total size in bytes.
    pub total_bytes: u64,

    /// Optional tail commit range for incremental refresh.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tail: Option<TailRange>,
}

/// Tail range for incremental refresh (commit range since last full snapshot).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TailRange {
    /// First commit ID in the tail.
    pub from_commit: String,
    /// Last commit ID in the tail.
    pub to_commit: String,
    /// Number of commits in the tail.
    pub commit_count: u64,
}

impl SnapshotInfo {
    /// Creates a new empty snapshot info.
    #[must_use]
    pub fn new(version: u64, path: String) -> Self {
        Self {
            version,
            path,
            files: Vec::new(),
            published_at: Utc::now(),
            total_rows: 0,
            total_bytes: 0,
            tail: None,
        }
    }

    /// Adds a file to the snapshot and updates totals.
    pub fn add_file(&mut self, file: SnapshotFile) {
        self.total_rows += file.row_count;
        self.total_bytes += file.byte_size;
        self.files.push(file);
    }

    /// Returns the list of all file paths in this snapshot.
    #[must_use]
    pub fn file_paths(&self) -> Vec<&str> {
        self.files.iter().map(|f| f.path.as_str()).collect()
    }

    /// Checks if a path is allowed in this snapshot (for URL allowlist).
    #[must_use]
    pub fn contains_path(&self, path: &str) -> bool {
        self.files.iter().any(|f| f.path == path)
    }
}

/// Executions manifest (Tier 2) - separate file.
///
/// Written ONLY by compactor. Contains materializations, partitions.
///
/// INVARIANT 4 (Atomic Publish): Readers use `snapshot_path` to locate current
/// snapshot. A snapshot isn't visible until manifest CAS succeeds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionsManifest {
    /// Watermark version (last compacted event).
    pub watermark_version: u64,

    /// Path to compaction checkpoint.
    pub checkpoint_path: String,

    /// Path to the current published snapshot (atomic visibility gate).
    /// Readers MUST use this to locate current state, not list state/.
    #[serde(default)]
    pub snapshot_path: Option<String>,

    /// Version of the current snapshot (monotonically increasing).
    #[serde(default)]
    pub snapshot_version: u64,

    /// Last compacted event file name (for file-based watermark).
    /// Stored as exact filename including .json for correct comparison.
    #[serde(default)]
    pub watermark_event_id: Option<String>,

    /// Monotonic position watermark for clock-skew-resistant ordering.
    ///
    /// Planned: once events carry a monotonic `sequence_position` (assigned at ingest), Tier 2
    /// compaction should use this field as the primary watermark to avoid skipping late-arriving
    /// events under distributed writers and clock skew.
    #[serde(default)]
    pub watermark_position: Option<u64>,

    /// Last compaction cutoff timestamp.
    ///
    /// This is recorded at the start of a compaction run and used with object
    /// `last_modified` metadata to detect out-of-order events written during compaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_compaction_at: Option<DateTime<Utc>>,

    /// Compaction statistics.
    #[serde(default)]
    pub compaction: CompactionMetadata,

    /// Fencing token for distributed lock validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,

    /// Commit ULID for audit correlation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_ulid: Option<String>,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl ExecutionsManifest {
    /// Creates a new executions manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            watermark_version: 0,
            checkpoint_path: format!(
                "{}checkpoint.json",
                CatalogPaths::state_dir(CatalogDomain::Executions)
            ),
            snapshot_path: None,
            snapshot_version: 0,
            watermark_event_id: None,
            watermark_position: None,
            last_compaction_at: None,
            compaction: CompactionMetadata::default(),
            fencing_token: None,
            commit_ulid: None,
            updated_at: Utc::now(),
        }
    }
}

impl Default for ExecutionsManifest {
    fn default() -> Self {
        Self::new()
    }
}

/// Backwards-compatible alias for older naming.
pub type ExecutionManifest = ExecutionsManifest;

/// Lineage manifest (Tier 1) - separate file.
///
/// Contains dependency edges between assets.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LineageManifest {
    /// Snapshot version.
    pub snapshot_version: u64,

    /// Path to edges Parquet files.
    pub edges_path: String,

    /// Optional enhanced snapshot metadata (per-file checksums, sizes, row counts).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SnapshotInfo>,

    /// Hash of the previous raw manifest (tamper-evident chain).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_hash: Option<String>,

    /// Fencing token for distributed lock validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,

    /// Commit ULID for audit correlation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_ulid: Option<String>,

    /// Last commit ID for audit trail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_commit_id: Option<String>,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl LineageManifest {
    /// Creates a new lineage manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot_version: 0,
            edges_path: CatalogPaths::snapshot_dir(CatalogDomain::Lineage, 0),
            snapshot: None,
            parent_hash: None,
            fencing_token: None,
            commit_ulid: None,
            last_commit_id: None,
            updated_at: Utc::now(),
        }
    }

    /// Validates that this manifest can succeed the given previous manifest.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `snapshot_version` decreased (rollback attempt)
    /// - `parent_hash` doesn't match `previous_raw_hash` (concurrent modification)
    /// - `fencing_token` regresses (stale holder)
    /// - `commit_ulid` is non-monotonic
    pub fn validate_succession(
        &self,
        previous: &Self,
        previous_raw_hash: &str,
    ) -> Result<(), String> {
        if self.snapshot_version < previous.snapshot_version {
            return Err(format!(
                "version regression: {} -> {} (rollback not allowed)",
                previous.snapshot_version, self.snapshot_version
            ));
        }

        if let Some(ref parent_hash) = self.parent_hash {
            if parent_hash != previous_raw_hash {
                return Err(format!(
                    "parent hash mismatch: expected {previous_raw_hash}, got {parent_hash} (concurrent modification detected)"
                ));
            }
        }

        if let (Some(prev), Some(next)) = (previous.fencing_token, self.fencing_token) {
            if next < prev {
                return Err(format!(
                    "fencing token regression: {prev} -> {next} (stale holder)"
                ));
            }
        }

        if let (Some(prev), Some(next)) = (
            previous.commit_ulid.as_deref(),
            self.commit_ulid.as_deref(),
        ) {
            if next <= prev {
                return Err(format!(
                    "commit ulid regression: {prev} -> {next} (non-monotonic)"
                ));
            }
        }

        Ok(())
    }
}

impl Default for LineageManifest {
    fn default() -> Self {
        Self::new()
    }
}

/// Search manifest (Tier 1) - separate file.
///
/// Contains derived search state (e.g., token postings).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchManifest {
    /// Snapshot version.
    pub snapshot_version: u64,

    /// Path to search Parquet files.
    pub base_path: String,

    /// Fencing token for distributed lock validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,

    /// Commit ULID for audit correlation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_ulid: Option<String>,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl SearchManifest {
    /// Creates a new search manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot_version: 0,
            base_path: CatalogPaths::snapshot_dir(CatalogDomain::Search, 0),
            fencing_token: None,
            commit_ulid: None,
            updated_at: Utc::now(),
        }
    }
}

impl Default for SearchManifest {
    fn default() -> Self {
        Self::new()
    }
}

/// Backwards-compatible alias for older naming.
pub type GovernanceManifest = SearchManifest;

// ============================================================================
// Commit Record (hash chain for audit trail)
// ============================================================================

/// Commit record for hash chain integrity.
///
/// Per architecture: enables audit trail and rollback verification.
/// Each commit references the previous commit's hash, forming an
/// unbreakable chain that can be verified for tampering.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitRecord {
    /// Unique commit ID (ULID).
    pub commit_id: String,

    /// Previous commit ID (None for first commit).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_commit_id: Option<String>,

    /// Previous commit hash (for chain verification).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_commit_hash: Option<String>,

    /// Operation type (e.g., `CreateAsset`, `UpdateAsset`).
    pub operation: String,

    /// SHA256 hash of operation payload.
    pub payload_hash: String,

    /// Commit timestamp.
    pub created_at: DateTime<Utc>,
}

/// Canonical hash input for commit records.
///
/// Uses a separate struct to ensure stable, unambiguous serialization
/// for hash computation. JSON serialization with sorted keys provides
/// a canonical byte representation that avoids the ambiguity of
/// delimiter-based formats (e.g., colons appearing in sha256: values).
///
/// Note: `created_at` is serialized as RFC 3339 string for deterministic hashing.
#[derive(Serialize)]
struct CommitHashInput<'a> {
    commit_id: &'a str,
    prev_commit_id: Option<&'a str>,
    prev_commit_hash: Option<&'a str>,
    operation: &'a str,
    payload_hash: &'a str,
    created_at: String,
}

impl CommitRecord {
    /// Computes the hash of this commit for chain verification.
    ///
    /// The hash includes `prev_commit_hash` to form an unbreakable chain.
    /// Uses canonical JSON serialization to ensure unambiguous byte encoding.
    ///
    /// # Panics
    ///
    /// Panics if JSON serialization fails, which cannot occur for the internal
    /// `CommitHashInput` struct (contains only string references).
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn compute_hash(&self) -> String {
        let input = CommitHashInput {
            commit_id: &self.commit_id,
            prev_commit_id: self.prev_commit_id.as_deref(),
            prev_commit_hash: self.prev_commit_hash.as_deref(),
            operation: &self.operation,
            payload_hash: &self.payload_hash,
            created_at: self.created_at.to_rfc3339(),
        };
        // serde_json produces deterministic output for this simple struct
        let bytes = serde_json::to_vec(&input).expect("commit hash input is always serializable");
        let hash = Sha256::digest(&bytes);
        format!("sha256:{}", hex::encode(hash))
    }

    /// Creates a new commit as a successor to a previous commit.
    #[must_use]
    pub fn new_successor(
        prev: &Self,
        commit_id: String,
        operation: String,
        payload_hash: String,
    ) -> Self {
        Self {
            commit_id,
            prev_commit_id: Some(prev.commit_id.clone()),
            prev_commit_hash: Some(prev.compute_hash()),
            operation,
            payload_hash,
            created_at: Utc::now(),
        }
    }
}

// ============================================================================
// In-Memory Catalog Manifest (loaded from multiple files)
// ============================================================================

/// In-memory representation of the full catalog manifest.
///
/// This combines the loaded domain manifests for convenient access.
/// Note: This is for in-memory use only; each domain is stored separately.
#[derive(Debug, Clone)]
pub struct CatalogManifest {
    /// Manifest schema version.
    pub version: u32,

    /// Catalog domain (namespaces, tables, columns).
    pub catalog: CatalogDomainManifest,

    /// Executions domain (Tier 2 materializations and execution state).
    pub executions: ExecutionsManifest,

    /// Lineage domain.
    pub lineage: LineageManifest,

    /// Search domain.
    pub search: SearchManifest,

    /// Creation timestamp.
    pub created_at: DateTime<Utc>,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl CatalogManifest {
    /// Creates a new empty manifest.
    #[must_use]
    pub fn new() -> Self {
        let now = Utc::now();
        Self {
            version: 1,
            catalog: CatalogDomainManifest::new(),
            executions: ExecutionsManifest::new(),
            lineage: LineageManifest::new(),
            search: SearchManifest::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Returns the next catalog snapshot version.
    #[must_use]
    pub fn next_catalog_version(&self) -> u64 {
        self.catalog.snapshot_version + 1
    }

    /// Updates the manifest timestamp.
    pub fn touch(&mut self) {
        self.updated_at = Utc::now();
    }
}

impl Default for CatalogManifest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // === Root Manifest Tests (pointer to domain manifests) ===

    #[test]
    fn test_root_manifest_contains_only_pointers() {
        let root = RootManifest::new();

        // Root should only have paths, not embedded content
        assert_eq!(root.version, 1);
        assert_eq!(
            root.catalog_manifest_path,
            CatalogPaths::domain_manifest(CatalogDomain::Catalog)
        );
        assert_eq!(
            root.lineage_manifest_path,
            CatalogPaths::domain_manifest(CatalogDomain::Lineage)
        );
        assert_eq!(
            root.executions_manifest_path,
            CatalogPaths::domain_manifest(CatalogDomain::Executions)
        );
        assert_eq!(
            root.search_manifest_path,
            CatalogPaths::domain_manifest(CatalogDomain::Search)
        );
    }

    #[test]
    fn test_root_manifest_roundtrip() {
        let root = RootManifest {
            version: 1,
            catalog_manifest_path: CatalogPaths::domain_manifest(CatalogDomain::Catalog),
            lineage_manifest_path: CatalogPaths::domain_manifest(CatalogDomain::Lineage),
            executions_manifest_path: CatalogPaths::domain_manifest(CatalogDomain::Executions),
            search_manifest_path: CatalogPaths::domain_manifest(CatalogDomain::Search),
            updated_at: Utc::now(),
        };

        let json = serde_json::to_string_pretty(&root).expect("serialize");
        let parsed: RootManifest = serde_json::from_str(&json).expect("parse");

        assert_eq!(parsed.catalog_manifest_path, root.catalog_manifest_path);
    }

    #[test]
    fn test_root_manifest_accepts_legacy_fields() {
        let now = Utc::now();
        let legacy = serde_json::json!({
          "version": 1,
          "coreManifestPath": "manifests/core.manifest.json",
          "executionManifestPath": "manifests/execution.manifest.json",
          "updatedAt": now,
        });

        let parsed: RootManifest =
            serde_json::from_value(legacy).expect("legacy root manifest should parse");

        assert_eq!(
            parsed.catalog_manifest_path, "manifests/core.manifest.json",
            "legacy field should map to catalog_manifest_path"
        );
        assert_eq!(
            parsed.executions_manifest_path, "manifests/execution.manifest.json",
            "legacy field should map to executions_manifest_path"
        );
        assert_eq!(
            parsed.lineage_manifest_path,
            CatalogPaths::domain_manifest(CatalogDomain::Lineage),
            "missing lineage path should default"
        );
        assert_eq!(
            parsed.search_manifest_path,
            CatalogPaths::domain_manifest(CatalogDomain::Search),
            "missing search path should default"
        );
    }

    // === Domain Manifest Tests (each domain is separate file) ===

    #[test]
    fn test_core_manifest_structure() {
        let core = CoreManifest::new();

        assert_eq!(core.snapshot_version, 0);
        assert!(core.last_commit_id.is_none());
    }

    #[test]
    fn test_execution_manifest_structure() {
        let exec = ExecutionManifest::new();

        assert_eq!(exec.watermark_version, 0);
        assert!(exec.last_compaction_at.is_none());
    }

    #[test]
    fn test_domain_manifests_independent_serialization() {
        // Each domain manifest serializes independently
        let now = Utc::now();
        let core = CoreManifest {
            snapshot_version: 42,
            snapshot_path: CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 42),
            snapshot: None,
            last_commit_id: Some("commit_abc".into()),
            fencing_token: None,
            commit_ulid: None,
            parent_hash: None,
            updated_at: now,
        };

        let exec = ExecutionManifest {
            watermark_version: 100,
            checkpoint_path: format!(
                "{}checkpoint.json",
                CatalogPaths::state_dir(CatalogDomain::Executions)
            ),
            snapshot_path: Some(
                "state/executions/snapshot_v5_01ARZ3NDEKTSV4RRFFQ69G5FAV.parquet".into(),
            ),
            snapshot_version: 5,
            watermark_event_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV.json".into()),
            watermark_position: Some(42),
            last_compaction_at: Some(now),
            compaction: CompactionMetadata {
                total_events_compacted: 100,
                total_files_written: 5,
            },
            fencing_token: None,
            commit_ulid: None,
            updated_at: now,
        };

        // Each can be serialized/deserialized independently
        let core_json = serde_json::to_string(&core).expect("core");
        let exec_json = serde_json::to_string(&exec).expect("exec");

        // They are separate - updating one doesn't touch the other
        assert!(!core_json.contains("watermarkVersion"));
        assert!(!exec_json.contains("lastCommitId"));
    }

    // === Commit Record Tests (hash chain integrity) ===

    fn parse_timestamp(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s)
            .expect("valid timestamp")
            .with_timezone(&Utc)
    }

    #[test]
    fn test_commit_record_hash_chain() {
        let commit1 = CommitRecord {
            commit_id: "commit_001".into(),
            prev_commit_id: None,
            prev_commit_hash: None,
            operation: "InitializeCatalog".into(),
            payload_hash: "sha256:abc123".into(),
            created_at: parse_timestamp("2025-01-15T10:00:00Z"),
        };

        let hash1 = commit1.compute_hash();
        assert!(hash1.starts_with("sha256:"));

        // Hash is deterministic
        assert_eq!(hash1, commit1.compute_hash());

        // Next commit references previous
        let commit2 = CommitRecord {
            commit_id: "commit_002".into(),
            prev_commit_id: Some("commit_001".into()),
            prev_commit_hash: Some(hash1.clone()),
            operation: "CreateAsset".into(),
            payload_hash: "sha256:def456".into(),
            created_at: parse_timestamp("2025-01-15T10:01:00Z"),
        };

        let hash2 = commit2.compute_hash();
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_commit_record_hash_includes_prev() {
        // Two commits with same content but different prev should have different hashes
        let ts = parse_timestamp("2025-01-15T10:00:00Z");
        let commit_a = CommitRecord {
            commit_id: "commit_X".into(),
            prev_commit_id: Some("commit_A".into()),
            prev_commit_hash: Some("sha256:aaa".into()),
            operation: "CreateAsset".into(),
            payload_hash: "sha256:same".into(),
            created_at: ts,
        };

        let commit_b = CommitRecord {
            commit_id: "commit_X".into(),
            prev_commit_id: Some("commit_B".into()),
            prev_commit_hash: Some("sha256:bbb".into()),
            operation: "CreateAsset".into(),
            payload_hash: "sha256:same".into(),
            created_at: ts,
        };

        assert_ne!(commit_a.compute_hash(), commit_b.compute_hash());
    }

    #[test]
    fn test_next_version() {
        let mut manifest = CatalogManifest::new();
        assert_eq!(manifest.next_catalog_version(), 1);

        manifest.catalog.snapshot_version = 5;
        assert_eq!(manifest.next_catalog_version(), 6);
    }

    #[test]
    fn test_lineage_manifest_structure() {
        let lineage = LineageManifest::new();
        assert_eq!(lineage.snapshot_version, 0);
        assert_eq!(
            lineage.edges_path,
            CatalogPaths::snapshot_dir(CatalogDomain::Lineage, 0)
        );
    }

    #[test]
    fn test_governance_manifest_structure() {
        let gov = GovernanceManifest::new();
        assert_eq!(gov.snapshot_version, 0);
        assert_eq!(
            gov.base_path,
            CatalogPaths::snapshot_dir(CatalogDomain::Search, 0)
        );
    }

    #[test]
    fn test_commit_successor() {
        let commit1 = CommitRecord {
            commit_id: "commit_001".into(),
            prev_commit_id: None,
            prev_commit_hash: None,
            operation: "InitializeCatalog".into(),
            payload_hash: "sha256:abc123".into(),
            created_at: parse_timestamp("2025-01-15T10:00:00Z"),
        };

        let commit2 = CommitRecord::new_successor(
            &commit1,
            "commit_002".into(),
            "CreateAsset".into(),
            "sha256:def456".into(),
        );

        assert_eq!(commit2.commit_id, "commit_002");
        assert_eq!(commit2.prev_commit_id, Some("commit_001".into()));
        assert_eq!(commit2.prev_commit_hash, Some(commit1.compute_hash()));
        assert_eq!(commit2.operation, "CreateAsset");
    }

    // === Rollback Detection Tests (Gate 5) ===

    #[test]
    fn test_compute_manifest_hash_deterministic() {
        let raw_bytes = br#"{"snapshotVersion":1,"snapshotPath":"state/catalog/v1/"}"#;

        let hash1 = compute_manifest_hash(raw_bytes);
        let hash2 = compute_manifest_hash(raw_bytes);

        // Hash should be deterministic
        assert_eq!(hash1, hash2);
        assert!(hash1.starts_with("sha256:"));
    }

    #[test]
    fn test_compute_manifest_hash_different_bytes() {
        let bytes1 = br#"{"snapshotVersion":1}"#;
        let bytes2 = br#"{"snapshotVersion":2}"#;

        let hash1 = compute_manifest_hash(bytes1);
        let hash2 = compute_manifest_hash(bytes2);

        // Different bytes should produce different hashes
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_validate_succession_version_regression_rejected() {
        let now = Utc::now();
        let previous = CatalogDomainManifest {
            snapshot_version: 5,
            snapshot_path: "state/catalog/v5/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: None,
            parent_hash: None,
            updated_at: now,
        };

        // Attempt to regress version from 5 to 3
        let regressed = CatalogDomainManifest {
            snapshot_version: 3,
            snapshot_path: "state/catalog/v3/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: None,
            parent_hash: Some("sha256:abc123".into()),
            updated_at: now,
        };

        let previous_raw_hash = "sha256:abc123";
        let result = regressed.validate_succession(&previous, previous_raw_hash);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("regression"));
    }

    #[test]
    fn test_validate_succession_parent_hash_mismatch_rejected() {
        let now = Utc::now();
        let previous = CatalogDomainManifest {
            snapshot_version: 5,
            snapshot_path: "state/catalog/v5/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: None,
            parent_hash: None,
            updated_at: now,
        };

        // Attempt with wrong parent hash (concurrent modification)
        let concurrent_modified = CatalogDomainManifest {
            snapshot_version: 6,
            snapshot_path: "state/catalog/v6/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: None,
            parent_hash: Some("sha256:wrong_hash".into()),
            updated_at: now,
        };

        let actual_raw_hash = "sha256:actual_hash";
        let result = concurrent_modified.validate_succession(&previous, actual_raw_hash);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("mismatch"));
    }

    #[test]
    fn test_validate_succession_fencing_token_regression_rejected() {
        let now = Utc::now();
        let previous = CatalogDomainManifest {
            snapshot_version: 5,
            snapshot_path: "state/catalog/v5/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: Some(5),
            commit_ulid: None,
            parent_hash: None,
            updated_at: now,
        };

        let stale = CatalogDomainManifest {
            snapshot_version: 6,
            snapshot_path: "state/catalog/v6/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: Some(4),
            commit_ulid: None,
            parent_hash: None,
            updated_at: now,
        };

        let result = stale.validate_succession(&previous, "sha256:any");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("fencing token regression"));
    }

    #[test]
    fn test_validate_succession_commit_ulid_regression_rejected() {
        let now = Utc::now();
        let previous = CatalogDomainManifest {
            snapshot_version: 5,
            snapshot_path: "state/catalog/v5/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".into()),
            parent_hash: None,
            updated_at: now,
        };

        let stale = CatalogDomainManifest {
            snapshot_version: 6,
            snapshot_path: "state/catalog/v6/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: Some("01ARZ3NDEKTSV4RRFFQ69G5FAU".into()),
            parent_hash: None,
            updated_at: now,
        };

        let result = stale.validate_succession(&previous, "sha256:any");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("commit ulid regression"));
    }

    #[test]
    fn test_validate_succession_valid() {
        let now = Utc::now();
        let previous = CatalogDomainManifest {
            snapshot_version: 5,
            snapshot_path: "state/catalog/v5/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: None,
            parent_hash: None,
            updated_at: now,
        };

        // Valid succession: version increases, parent hash matches
        let valid_successor = CatalogDomainManifest {
            snapshot_version: 6,
            snapshot_path: "state/catalog/v6/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: None,
            parent_hash: Some("sha256:correct_hash".into()),
            updated_at: now,
        };

        let previous_raw_hash = "sha256:correct_hash";
        let result = valid_successor.validate_succession(&previous, previous_raw_hash);

        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_succession_same_version_allowed() {
        let now = Utc::now();
        let previous = CatalogDomainManifest {
            snapshot_version: 5,
            snapshot_path: "state/catalog/v5/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: None,
            parent_hash: None,
            updated_at: now,
        };

        // Same version is allowed (idempotent retry)
        let same_version = CatalogDomainManifest {
            snapshot_version: 5,
            snapshot_path: "state/catalog/v5/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: None,
            parent_hash: Some("sha256:hash".into()),
            updated_at: now,
        };

        let previous_raw_hash = "sha256:hash";
        let result = same_version.validate_succession(&previous, previous_raw_hash);

        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_succession_no_parent_hash_allowed() {
        let now = Utc::now();
        let previous = CatalogDomainManifest {
            snapshot_version: 5,
            snapshot_path: "state/catalog/v5/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: None,
            parent_hash: None,
            updated_at: now,
        };

        // No parent hash (backwards compatibility with old manifests)
        let no_parent_hash = CatalogDomainManifest {
            snapshot_version: 6,
            snapshot_path: "state/catalog/v6/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: None,
            parent_hash: None,
            updated_at: now,
        };

        let previous_raw_hash = "sha256:anything";
        let result = no_parent_hash.validate_succession(&previous, previous_raw_hash);

        // Should pass - no parent_hash means no hash check
        assert!(result.is_ok());
    }

    #[test]
    fn test_catalog_manifest_has_parent_hash_field() {
        // Verify the field exists and is serializable
        let manifest = CatalogDomainManifest {
            snapshot_version: 1,
            snapshot_path: "state/catalog/v1/".into(),
            snapshot: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: None,
            parent_hash: Some("sha256:abc123".into()),
            updated_at: Utc::now(),
        };

        let json = serde_json::to_string(&manifest).expect("serialize");
        assert!(json.contains("parentHash"));
        assert!(json.contains("sha256:abc123"));

        // Roundtrip
        let parsed: CatalogDomainManifest = serde_json::from_str(&json).expect("parse");
        assert_eq!(parsed.parent_hash, Some("sha256:abc123".into()));
    }
}
