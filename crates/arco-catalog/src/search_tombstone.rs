//! Search index tombstones for delete/rename operations (Gate 5).
//!
//! When an asset is deleted or renamed, the search index needs to be updated
//! to remove stale entries. This module provides tombstone records that mark
//! assets for removal from search.
//!
//! # Architecture
//!
//! ```text
//! Asset Delete/Rename
//!         │
//!         ├── Write tombstone to ledger
//!         │
//!         ├── Compactor processes tombstone
//!         │
//!         └── Search index removes/updates entry
//! ```
//!
//! # Tombstone Lifecycle
//!
//! 1. **Creation**: When an asset is deleted/renamed, a tombstone is written
//! 2. **Processing**: Compactor reads tombstone and removes entry from search
//! 3. **Expiry**: After `expires_at`, tombstone can be garbage collected
//!
//! # Hot Token Handling
//!
//! Some tokens (like common words) appear in many assets. When such an asset
//! is deleted, the search index update must complete within latency budget.
//! The tombstone system supports batched processing for efficiency.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// Search index tombstone for deleted/renamed assets.
///
/// Tombstones mark assets for removal from the search index. They are
/// processed by the compactor during the next compaction pass.
///
/// # Example
///
/// ```rust
/// use arco_catalog::search_tombstone::{SearchTombstone, TombstoneReason};
///
/// // Asset "my-table" was deleted
/// let tombstone = SearchTombstone::deleted("asset-123", "my-namespace", "my-table");
/// assert!(tombstone.is_valid());
///
/// // Asset "old-name" was renamed to "new-name"
/// let tombstone = SearchTombstone::renamed(
///     "asset-456",
///     "my-namespace",
///     "old-name",
///     "new-name",
/// );
/// assert_eq!(tombstone.new_key(), Some("new-name"));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SearchTombstone {
    /// Unique tombstone ID (ULID).
    pub id: String,

    /// Asset ID being removed from search.
    pub asset_id: String,

    /// Namespace containing the asset.
    pub namespace: String,

    /// Asset key (name) being removed.
    pub asset_key: String,

    /// Reason for removal.
    pub reason: TombstoneReason,

    /// When the tombstone was created.
    pub created_at: DateTime<Utc>,

    /// Tombstone expires after this time (for garbage collection).
    ///
    /// Default expiry is 7 days after creation.
    pub expires_at: DateTime<Utc>,

    /// Optional correlation ID for audit trail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
}

/// Reason for search index removal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum TombstoneReason {
    /// Asset was deleted.
    Deleted,

    /// Asset was renamed.
    Renamed {
        /// New asset key after rename.
        new_key: String,
    },

    /// Asset was moved to a different namespace.
    Moved {
        /// New namespace.
        new_namespace: String,
        /// New key (may be same or different).
        new_key: String,
    },

    /// Asset was soft-deleted (archived).
    Archived,
}

/// Default tombstone expiry duration (7 days).
const DEFAULT_EXPIRY_DAYS: i64 = 7;

impl SearchTombstone {
    /// Creates a tombstone for a deleted asset.
    #[must_use]
    pub fn deleted(
        asset_id: impl Into<String>,
        namespace: impl Into<String>,
        asset_key: impl Into<String>,
    ) -> Self {
        Self::new(
            asset_id.into(),
            namespace.into(),
            asset_key.into(),
            TombstoneReason::Deleted,
        )
    }

    /// Creates a tombstone for a renamed asset.
    #[must_use]
    pub fn renamed(
        asset_id: impl Into<String>,
        namespace: impl Into<String>,
        old_key: impl Into<String>,
        new_key: impl Into<String>,
    ) -> Self {
        Self::new(
            asset_id.into(),
            namespace.into(),
            old_key.into(),
            TombstoneReason::Renamed {
                new_key: new_key.into(),
            },
        )
    }

    /// Creates a tombstone for a moved asset.
    #[must_use]
    pub fn moved(
        asset_id: impl Into<String>,
        old_namespace: impl Into<String>,
        old_key: impl Into<String>,
        new_namespace: impl Into<String>,
        new_key: impl Into<String>,
    ) -> Self {
        Self::new(
            asset_id.into(),
            old_namespace.into(),
            old_key.into(),
            TombstoneReason::Moved {
                new_namespace: new_namespace.into(),
                new_key: new_key.into(),
            },
        )
    }

    /// Creates a tombstone for an archived (soft-deleted) asset.
    #[must_use]
    pub fn archived(
        asset_id: impl Into<String>,
        namespace: impl Into<String>,
        asset_key: impl Into<String>,
    ) -> Self {
        Self::new(
            asset_id.into(),
            namespace.into(),
            asset_key.into(),
            TombstoneReason::Archived,
        )
    }

    /// Creates a new tombstone with the given reason.
    fn new(
        asset_id: String,
        namespace: String,
        asset_key: String,
        reason: TombstoneReason,
    ) -> Self {
        let now = Utc::now();
        let expires_at = now + Duration::days(DEFAULT_EXPIRY_DAYS);

        Self {
            id: ulid::Ulid::new().to_string(),
            asset_id,
            namespace,
            asset_key,
            reason,
            created_at: now,
            expires_at,
            correlation_id: None,
        }
    }

    /// Sets a custom expiry duration.
    #[must_use]
    pub fn with_expiry(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = expires_at;
        self
    }

    /// Sets a correlation ID for audit trail.
    #[must_use]
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Returns true if the tombstone is still valid (not expired).
    #[must_use]
    pub fn is_valid(&self) -> bool {
        Utc::now() < self.expires_at
    }

    /// Returns true if the tombstone has expired.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        Utc::now() >= self.expires_at
    }

    /// Returns the new asset key if this is a rename.
    #[must_use]
    pub fn new_key(&self) -> Option<&str> {
        match &self.reason {
            TombstoneReason::Moved { new_key, .. } | TombstoneReason::Renamed { new_key } => {
                Some(new_key)
            }
            _ => None,
        }
    }

    /// Returns the new namespace if this is a move.
    #[must_use]
    pub fn new_namespace(&self) -> Option<&str> {
        match &self.reason {
            TombstoneReason::Moved { new_namespace, .. } => Some(new_namespace),
            _ => None,
        }
    }

    /// Returns true if the asset should be re-indexed (rename/move).
    #[must_use]
    pub fn requires_reindex(&self) -> bool {
        matches!(
            self.reason,
            TombstoneReason::Renamed { .. } | TombstoneReason::Moved { .. }
        )
    }
}

/// Batch of tombstones for efficient processing.
///
/// Compactor processes tombstones in batches to minimize search index updates.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TombstoneBatch {
    /// Tombstones in this batch.
    pub tombstones: Vec<SearchTombstone>,

    /// When this batch was created.
    pub created_at: DateTime<Utc>,
}

impl TombstoneBatch {
    /// Creates a new empty batch.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tombstones: Vec::new(),
            created_at: Utc::now(),
        }
    }

    /// Adds a tombstone to the batch.
    pub fn add(&mut self, tombstone: SearchTombstone) {
        self.tombstones.push(tombstone);
    }

    /// Returns the number of tombstones in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tombstones.len()
    }

    /// Returns true if the batch is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tombstones.is_empty()
    }

    /// Returns only valid (non-expired) tombstones.
    pub fn valid_tombstones(&self) -> impl Iterator<Item = &SearchTombstone> {
        self.tombstones.iter().filter(|t| t.is_valid())
    }

    /// Returns tombstones grouped by namespace for efficient processing.
    pub fn by_namespace(&self) -> std::collections::HashMap<&str, Vec<&SearchTombstone>> {
        let mut map = std::collections::HashMap::new();
        for tombstone in &self.tombstones {
            map.entry(tombstone.namespace.as_str())
                .or_insert_with(Vec::new)
                .push(tombstone);
        }
        map
    }
}

/// Result of processing tombstones against the search index.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TombstoneProcessingResult {
    /// Number of entries removed from search.
    pub entries_removed: usize,

    /// Number of entries reindexed (for renames/moves).
    pub entries_reindexed: usize,

    /// Number of expired tombstones skipped.
    pub expired_skipped: usize,

    /// Errors encountered during processing.
    pub errors: Vec<TombstoneProcessingError>,
}

/// Error during tombstone processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneProcessingError {
    /// Tombstone ID that failed.
    pub tombstone_id: String,

    /// Error message.
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deleted_tombstone() {
        let tombstone = SearchTombstone::deleted("asset-1", "ns", "my-table");

        assert_eq!(tombstone.asset_id, "asset-1");
        assert_eq!(tombstone.namespace, "ns");
        assert_eq!(tombstone.asset_key, "my-table");
        assert!(matches!(tombstone.reason, TombstoneReason::Deleted));
        assert!(tombstone.is_valid());
        assert!(!tombstone.requires_reindex());
    }

    #[test]
    fn test_renamed_tombstone() {
        let tombstone = SearchTombstone::renamed("asset-1", "ns", "old-name", "new-name");

        assert_eq!(tombstone.asset_key, "old-name");
        assert_eq!(tombstone.new_key(), Some("new-name"));
        assert!(tombstone.requires_reindex());
    }

    #[test]
    fn test_moved_tombstone() {
        let tombstone = SearchTombstone::moved("asset-1", "ns1", "table", "ns2", "table");

        assert_eq!(tombstone.namespace, "ns1");
        assert_eq!(tombstone.new_namespace(), Some("ns2"));
        assert_eq!(tombstone.new_key(), Some("table"));
        assert!(tombstone.requires_reindex());
    }

    #[test]
    fn test_archived_tombstone() {
        let tombstone = SearchTombstone::archived("asset-1", "ns", "table");

        assert!(matches!(tombstone.reason, TombstoneReason::Archived));
        assert!(!tombstone.requires_reindex());
    }

    #[test]
    fn test_tombstone_expiry() {
        let tombstone = SearchTombstone::deleted("asset-1", "ns", "table");

        // Default expiry is 7 days in the future
        assert!(tombstone.is_valid());
        assert!(!tombstone.is_expired());

        // Test with expired timestamp
        let expired = tombstone.with_expiry(Utc::now() - Duration::hours(1));
        assert!(!expired.is_valid());
        assert!(expired.is_expired());
    }

    #[test]
    fn test_tombstone_batch() {
        let mut batch = TombstoneBatch::new();

        batch.add(SearchTombstone::deleted("a1", "ns1", "t1"));
        batch.add(SearchTombstone::deleted("a2", "ns1", "t2"));
        batch.add(SearchTombstone::deleted("a3", "ns2", "t3"));

        assert_eq!(batch.len(), 3);

        let by_ns = batch.by_namespace();
        assert_eq!(by_ns.get("ns1").unwrap().len(), 2);
        assert_eq!(by_ns.get("ns2").unwrap().len(), 1);
    }

    #[test]
    fn test_tombstone_serialization() {
        let tombstone =
            SearchTombstone::renamed("asset-1", "ns", "old", "new").with_correlation_id("corr-123");

        let json = serde_json::to_string(&tombstone).expect("serialize");
        let parsed: SearchTombstone = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(parsed.asset_id, "asset-1");
        assert_eq!(parsed.new_key(), Some("new"));
        assert_eq!(parsed.correlation_id, Some("corr-123".to_string()));
    }

    #[test]
    fn test_hot_token_skew_handling() {
        // This test documents the expected behavior for hot tokens:
        // - Tombstones are batched for efficiency
        // - Processing must complete within latency budget
        // - Batch processing reduces search index update overhead

        let mut batch = TombstoneBatch::new();

        // Add many tombstones (simulating hot token affecting many assets)
        for i in 0..1000 {
            batch.add(SearchTombstone::deleted(
                format!("asset-{i}"),
                "ns",
                format!("table-with-hot-token-{i}"),
            ));
        }

        assert_eq!(batch.len(), 1000);

        // Batch processing by namespace is efficient
        let by_ns = batch.by_namespace();
        assert_eq!(by_ns.len(), 1); // All in same namespace
        assert_eq!(by_ns.get("ns").unwrap().len(), 1000);
    }

    #[test]
    fn test_tombstone_reason_variants() {
        // Test all reason variants serialize correctly
        let deleted = TombstoneReason::Deleted;
        let renamed = TombstoneReason::Renamed {
            new_key: "new".to_string(),
        };
        let moved = TombstoneReason::Moved {
            new_namespace: "ns2".to_string(),
            new_key: "key".to_string(),
        };
        let archived = TombstoneReason::Archived;

        for reason in [deleted, renamed, moved, archived] {
            let json = serde_json::to_string(&reason).expect("serialize");
            let _parsed: TombstoneReason = serde_json::from_str(&json).expect("deserialize");
        }
    }
}
