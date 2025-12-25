//! Iceberg commit event receipts for reconciliation and lineage.
//!
//! Two immutable files per commit attempt:
//! - `events/YYYY-MM-DD/iceberg/pending/{commit_key}.json` - before pointer CAS
//! - `events/YYYY-MM-DD/iceberg/committed/{commit_key}.json` - after pointer CAS

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use uuid::Uuid;

use crate::pointer::UpdateSource;
use crate::types::CommitKey;

/// Pending commit receipt - written before pointer CAS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingReceipt {
    /// Table this commit targets.
    pub table_uuid: Uuid,
    /// Deterministic commit key.
    pub commit_key: CommitKey,
    /// Event ID for correlation.
    pub event_id: Ulid,
    /// New metadata file location.
    pub metadata_location: String,
    /// Base metadata location when commit started.
    pub base_metadata_location: String,
    /// Expected snapshot ID after commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    /// Source of this commit.
    pub source: UpdateSource,
    /// When the commit was started.
    pub started_at: DateTime<Utc>,
}

impl PendingReceipt {
    /// Returns the storage path for this receipt.
    ///
    /// Path: `events/YYYY-MM-DD/iceberg/pending/{commit_key}.json`
    #[must_use]
    pub fn storage_path(date: NaiveDate, commit_key: &CommitKey) -> String {
        format!(
            "events/{}/iceberg/pending/{}.json",
            date.format("%Y-%m-%d"),
            commit_key
        )
    }
}

/// Committed commit receipt - written after pointer CAS succeeds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedReceipt {
    /// Table this commit targeted.
    pub table_uuid: Uuid,
    /// Deterministic commit key.
    pub commit_key: CommitKey,
    /// Event ID for correlation.
    pub event_id: Ulid,
    /// New metadata file location.
    pub metadata_location: String,
    /// Snapshot ID after commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    /// Previous metadata location (for history).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_metadata_location: Option<String>,
    /// Source of this commit.
    pub source: UpdateSource,
    /// When the commit was confirmed.
    pub committed_at: DateTime<Utc>,
}

impl CommittedReceipt {
    /// Returns the storage path for this receipt.
    ///
    /// Path: `events/YYYY-MM-DD/iceberg/committed/{commit_key}.json`
    #[must_use]
    pub fn storage_path(date: NaiveDate, commit_key: &CommitKey) -> String {
        format!(
            "events/{}/iceberg/committed/{}.json",
            date.format("%Y-%m-%d"),
            commit_key
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_receipt_path() {
        let date = NaiveDate::from_ymd_opt(2025, 1, 15).expect("valid date");
        let commit_key = CommitKey::from_metadata_location("test/metadata.json");
        let path = PendingReceipt::storage_path(date, &commit_key);
        assert!(path.starts_with("events/2025-01-15/iceberg/pending/"));
        assert!(path.ends_with(".json"));
    }

    #[test]
    fn test_committed_receipt_path() {
        let date = NaiveDate::from_ymd_opt(2025, 1, 15).expect("valid date");
        let commit_key = CommitKey::from_metadata_location("test/metadata.json");
        let path = CommittedReceipt::storage_path(date, &commit_key);
        assert!(path.starts_with("events/2025-01-15/iceberg/committed/"));
        assert!(path.ends_with(".json"));
    }

    #[test]
    fn test_pending_receipt_serialization() {
        let receipt = PendingReceipt {
            table_uuid: Uuid::new_v4(),
            commit_key: CommitKey::from_metadata_location("test.json"),
            event_id: Ulid::new(),
            metadata_location: "new.json".to_string(),
            base_metadata_location: "base.json".to_string(),
            snapshot_id: Some(123),
            source: UpdateSource::IcebergRest {
                client_info: Some("spark/3.5".to_string()),
                principal: None,
            },
            started_at: Utc::now(),
        };

        let json = serde_json::to_string(&receipt).expect("serialize");
        let parsed: PendingReceipt = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(receipt.table_uuid, parsed.table_uuid);
    }

    #[test]
    fn test_committed_receipt_serialization() {
        let receipt = CommittedReceipt {
            table_uuid: Uuid::new_v4(),
            commit_key: CommitKey::from_metadata_location("test.json"),
            event_id: Ulid::new(),
            metadata_location: "new.json".to_string(),
            snapshot_id: Some(456),
            previous_metadata_location: Some("base.json".to_string()),
            source: UpdateSource::IcebergRest {
                client_info: Some("spark/3.5".to_string()),
                principal: Some("user@example.com".to_string()),
            },
            committed_at: Utc::now(),
        };

        let json = serde_json::to_string(&receipt).expect("serialize");
        let parsed: CommittedReceipt = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(receipt.table_uuid, parsed.table_uuid);
        assert_eq!(receipt.metadata_location, parsed.metadata_location);
    }
}
