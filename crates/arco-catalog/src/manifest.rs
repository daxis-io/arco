//! Physically multi-file manifest structure per architecture docs.
//!
//! The catalog uses separate manifest files to reduce contention:
//! - `root.manifest.json`: Pointers to domain manifests
//! - `core.manifest.json`: Tier 1 (assets, schemas) - locked writes
//! - `execution.manifest.json`: Tier 2 (materializations) - compactor writes
//! - `lineage.manifest.json`: Tier 2 (dependency edges)
//! - `governance.manifest.json`: Tier 2 (tags, owners)
//!
//! Each domain manifest is a separate file that can be updated independently.
//!
//! # Storage Layout
//!
//! ```text
//! tenant={tenant}/workspace={workspace}/manifests/
//! ├── root.manifest.json        # Root pointer to domain manifests
//! ├── core.manifest.json        # Tier 1: Assets, schemas (locked writes)
//! ├── execution.manifest.json   # Tier 2: Materializations (compactor writes)
//! ├── lineage.manifest.json     # Tier 2: Dependency edges
//! └── governance.manifest.json  # Tier 2: Tags, owners
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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

    /// Path to core domain manifest (required).
    pub core_manifest_path: String,

    /// Path to execution domain manifest (required).
    pub execution_manifest_path: String,

    /// Path to lineage domain manifest (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lineage_manifest_path: Option<String>,

    /// Path to governance domain manifest (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub governance_manifest_path: Option<String>,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl RootManifest {
    /// Creates a new root manifest with default paths.
    #[must_use]
    pub fn new() -> Self {
        Self {
            version: 1,
            core_manifest_path: "manifests/core.manifest.json".into(),
            execution_manifest_path: "manifests/execution.manifest.json".into(),
            lineage_manifest_path: None,
            governance_manifest_path: None,
            updated_at: Utc::now(),
        }
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

/// Core catalog manifest (Tier 1) - separate file.
///
/// Written via locking protocol. Contains assets, schemas.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoreManifest {
    /// Current snapshot version.
    pub snapshot_version: u64,

    /// Path to snapshot directory.
    pub snapshot_path: String,

    /// Last commit ID for hash chain integrity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_commit_id: Option<String>,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl CoreManifest {
    /// Creates a new core manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot_version: 0,
            snapshot_path: "core/snapshots/v0/".into(),
            last_commit_id: None,
            updated_at: Utc::now(),
        }
    }

    /// Returns the next snapshot version.
    #[must_use]
    pub fn next_version(&self) -> u64 {
        self.snapshot_version + 1
    }
}

impl Default for CoreManifest {
    fn default() -> Self {
        Self::new()
    }
}

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

/// Execution manifest (Tier 2) - separate file.
///
/// Written ONLY by compactor. Contains materializations, partitions.
///
/// INVARIANT 4 (Atomic Publish): Readers use `snapshot_path` to locate current
/// snapshot. A snapshot isn't visible until manifest CAS succeeds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionManifest {
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

    /// Last compaction timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_compaction_at: Option<DateTime<Utc>>,

    /// Compaction statistics.
    #[serde(default)]
    pub compaction: CompactionMetadata,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl ExecutionManifest {
    /// Creates a new execution manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            watermark_version: 0,
            checkpoint_path: "state/execution/checkpoint.json".into(),
            snapshot_path: None,
            snapshot_version: 0,
            watermark_event_id: None,
            watermark_position: None,
            last_compaction_at: None,
            compaction: CompactionMetadata::default(),
            updated_at: Utc::now(),
        }
    }
}

impl Default for ExecutionManifest {
    fn default() -> Self {
        Self::new()
    }
}

/// Lineage manifest (Tier 2) - separate file.
///
/// Contains dependency edges between assets.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LineageManifest {
    /// Snapshot version.
    pub snapshot_version: u64,

    /// Path to edges Parquet files.
    pub edges_path: String,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl LineageManifest {
    /// Creates a new lineage manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot_version: 0,
            edges_path: "state/lineage/edges/".into(),
            updated_at: Utc::now(),
        }
    }
}

impl Default for LineageManifest {
    fn default() -> Self {
        Self::new()
    }
}

/// Governance manifest (Tier 2) - separate file.
///
/// Contains tags, owners, access policies.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GovernanceManifest {
    /// Snapshot version.
    pub snapshot_version: u64,

    /// Path to governance Parquet files.
    pub base_path: String,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl GovernanceManifest {
    /// Creates a new governance manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot_version: 0,
            base_path: "governance/".into(),
            updated_at: Utc::now(),
        }
    }
}

impl Default for GovernanceManifest {
    fn default() -> Self {
        Self::new()
    }
}

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

    /// Core domain (assets, schemas).
    pub core: CoreManifest,

    /// Execution domain (materializations).
    pub execution: ExecutionManifest,

    /// Lineage domain (optional).
    pub lineage: Option<LineageManifest>,

    /// Governance domain (optional).
    pub governance: Option<GovernanceManifest>,

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
            core: CoreManifest::new(),
            execution: ExecutionManifest::new(),
            lineage: None,
            governance: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Returns the next core snapshot version.
    #[must_use]
    pub fn next_core_version(&self) -> u64 {
        self.core.snapshot_version + 1
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

// ============================================================================
// Manifest Paths
// ============================================================================

/// Standard manifest file names.
pub mod paths {
    /// Root manifest file name.
    pub const ROOT_MANIFEST: &str = "manifests/root.manifest.json";

    /// Core domain manifest file name.
    pub const CORE_MANIFEST: &str = "manifests/core.manifest.json";

    /// Execution domain manifest file name.
    pub const EXECUTION_MANIFEST: &str = "manifests/execution.manifest.json";

    /// Lineage domain manifest file name.
    pub const LINEAGE_MANIFEST: &str = "manifests/lineage.manifest.json";

    /// Governance domain manifest file name.
    pub const GOVERNANCE_MANIFEST: &str = "manifests/governance.manifest.json";
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
        assert_eq!(root.core_manifest_path, "manifests/core.manifest.json");
        assert_eq!(
            root.execution_manifest_path,
            "manifests/execution.manifest.json"
        );
        assert!(root.lineage_manifest_path.is_none());
        assert!(root.governance_manifest_path.is_none());
    }

    #[test]
    fn test_root_manifest_roundtrip() {
        let root = RootManifest {
            version: 1,
            core_manifest_path: "manifests/core.manifest.json".into(),
            execution_manifest_path: "manifests/execution.manifest.json".into(),
            lineage_manifest_path: Some("manifests/lineage.manifest.json".into()),
            governance_manifest_path: None,
            updated_at: Utc::now(),
        };

        let json = serde_json::to_string_pretty(&root).expect("serialize");
        let parsed: RootManifest = serde_json::from_str(&json).expect("parse");

        assert_eq!(parsed.core_manifest_path, root.core_manifest_path);
        assert!(parsed.lineage_manifest_path.is_some());
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
            snapshot_path: "core/snapshots/v42/".into(),
            last_commit_id: Some("commit_abc".into()),
            updated_at: now,
        };

        let exec = ExecutionManifest {
            watermark_version: 100,
            checkpoint_path: "state/execution/checkpoint.json".into(),
            snapshot_path: Some(
                "state/execution/snapshot_v5_01ARZ3NDEKTSV4RRFFQ69G5FAV.parquet".into(),
            ),
            snapshot_version: 5,
            watermark_event_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV.json".into()),
            watermark_position: Some(42),
            last_compaction_at: Some(now),
            compaction: CompactionMetadata {
                total_events_compacted: 100,
                total_files_written: 5,
            },
            updated_at: now,
        };

        // Each can be serialized/deserialized independently
        let core_json = serde_json::to_string(&core).expect("core");
        let exec_json = serde_json::to_string(&exec).expect("exec");

        // They are separate - updating one doesn't touch the other
        assert!(!core_json.contains("watermark"));
        assert!(!exec_json.contains("snapshot_version"));
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
        assert_eq!(manifest.next_core_version(), 1);

        manifest.core.snapshot_version = 5;
        assert_eq!(manifest.next_core_version(), 6);
    }

    #[test]
    fn test_manifest_paths() {
        assert_eq!(paths::ROOT_MANIFEST, "manifests/root.manifest.json");
        assert_eq!(paths::CORE_MANIFEST, "manifests/core.manifest.json");
        assert_eq!(
            paths::EXECUTION_MANIFEST,
            "manifests/execution.manifest.json"
        );
    }

    #[test]
    fn test_lineage_manifest_structure() {
        let lineage = LineageManifest::new();
        assert_eq!(lineage.snapshot_version, 0);
        assert_eq!(lineage.edges_path, "state/lineage/edges/");
    }

    #[test]
    fn test_governance_manifest_structure() {
        let gov = GovernanceManifest::new();
        assert_eq!(gov.snapshot_version, 0);
        assert_eq!(gov.base_path, "governance/");
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
}
