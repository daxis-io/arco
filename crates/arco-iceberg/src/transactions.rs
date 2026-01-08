//! Multi-table transaction records for ICE-7 atomic commits.
//!
//! This module implements the transaction record storage that serves as the
//! atomic commit point for multi-table Iceberg transactions.
//!
//! # Protocol Overview
//!
//! The transaction protocol follows a prepare-commit-finalize pattern:
//!
//! 1. **Create**: Write `TransactionRecord` with `status=Preparing` (idempotency anchor)
//! 2. **Prepare**: Write metadata files and CAS pointers with `pending` state
//! 3. **Commit**: CAS transaction record to `Committed` (atomic visibility gate)
//! 4. **Finalize**: Best-effort clear `pending` from pointers
//!
//! # Atomic Visibility
//!
//! The `Committed` status on the transaction record is the single source of truth
//! for multi-table visibility. Readers use "effective pointer resolution" to check:
//! - If pointer has `pending` field pointing to a `Committed` transaction → use pending metadata
//! - Otherwise → use current metadata
//!
//! # Crash Recovery
//!
//! | Failure Point | Tables State | Visibility | Recovery |
//! |---------------|--------------|------------|----------|
//! | Before any prepare | No pending, tx=Preparing | None visible | Tx times out |
//! | After N prepares | N pending, tx=Preparing | None visible | Stale pending cleared |
//! | After commit | All pending, tx=Committed | All visible | Finalizer clears pending |
//!
//! # Storage Path
//!
//! Transaction records are stored at: `_catalog/iceberg_transactions/{tx_id}.json`
//!
//! See ADR-029 for the full design specification.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};

use crate::error::{IcebergError, IcebergResult};
use crate::paths::iceberg_transaction_record_path;
use crate::types::ObjectVersion;

/// Status of a multi-table transaction.
///
/// The state machine is:
/// ```text
/// (none) --create--> Preparing --commit--> Committed
///                        |
///                        +--abort--> Aborted
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransactionStatus {
    /// Transaction is being prepared - pointers may have `pending` state.
    ///
    /// Tables in this state are NOT visible to readers.
    Preparing,

    /// Transaction committed successfully - all tables are atomically visible.
    ///
    /// Tables with `pending` pointing to this transaction should be treated
    /// as having the pending metadata as current.
    Committed,

    /// Transaction was aborted - pending state should be cleaned up.
    ///
    /// Tables with `pending` pointing to this transaction should ignore
    /// the pending state and use current metadata.
    Aborted,
}

/// A single table's entry in a multi-table transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionTableEntry {
    /// The table UUID.
    pub table_uuid: Uuid,

    /// Namespace name (for debugging/logging).
    pub namespace: String,

    /// Table name (for debugging/logging).
    pub table_name: String,

    /// Pointer version at prepare time (for conflict detection).
    pub base_pointer_version: String,

    /// New metadata location written during prepare.
    pub new_metadata_location: String,

    /// New snapshot ID (if any snapshot was added).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_snapshot_id: Option<i64>,

    /// New last sequence number.
    pub new_last_sequence_number: i64,
}

/// Durable record for a multi-table transaction.
///
/// The transaction record serves as the atomic commit point. When its status
/// changes from `Preparing` to `Committed`, all tables in the transaction
/// become atomically visible.
///
/// Path: `_catalog/iceberg_transactions/{tx_id}.json`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionRecord {
    /// Transaction ID (same as Idempotency-Key, must be `UUIDv7`).
    pub tx_id: String,

    /// SHA256 hash of the canonical request body (RFC 8785 JCS).
    ///
    /// Used for idempotency conflict detection: same `tx_id` with different
    /// `request_hash` returns 409 Conflict.
    pub request_hash: String,

    /// Current transaction status.
    pub status: TransactionStatus,

    /// Tables included in this transaction.
    pub tables: Vec<TransactionTableEntry>,

    /// When the transaction was created.
    pub started_at: DateTime<Utc>,

    /// When the transaction was committed (only set for Committed status).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_at: Option<DateTime<Utc>>,

    /// When the transaction was aborted (only set for Aborted status).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aborted_at: Option<DateTime<Utc>>,

    /// Abort reason (only set for Aborted status).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abort_reason: Option<String>,
}

impl TransactionRecord {
    /// Creates a new transaction record in `Preparing` status.
    #[must_use]
    pub fn new_preparing(tx_id: String, request_hash: String) -> Self {
        Self {
            tx_id,
            request_hash,
            status: TransactionStatus::Preparing,
            tables: Vec::new(),
            started_at: Utc::now(),
            committed_at: None,
            aborted_at: None,
            abort_reason: None,
        }
    }

    /// Adds a table entry to the transaction.
    #[must_use]
    pub fn with_table(mut self, entry: TransactionTableEntry) -> Self {
        self.tables.push(entry);
        self
    }

    /// Adds multiple table entries to the transaction.
    #[must_use]
    pub fn with_tables(mut self, entries: impl IntoIterator<Item = TransactionTableEntry>) -> Self {
        self.tables.extend(entries);
        self
    }

    /// Returns a new record with status set to `Committed`.
    #[must_use]
    pub fn commit(mut self) -> Self {
        debug_assert_eq!(
            self.status,
            TransactionStatus::Preparing,
            "can only commit from Preparing"
        );
        self.status = TransactionStatus::Committed;
        self.committed_at = Some(Utc::now());
        self.aborted_at = None;
        self.abort_reason = None;
        self
    }

    /// Returns a new record with status set to `Aborted`.
    #[must_use]
    pub fn abort(mut self, reason: impl Into<String>) -> Self {
        debug_assert_eq!(
            self.status,
            TransactionStatus::Preparing,
            "can only abort from Preparing"
        );
        self.status = TransactionStatus::Aborted;
        self.aborted_at = Some(Utc::now());
        self.abort_reason = Some(reason.into());
        self.committed_at = None;
        self
    }

    /// Returns whether this transaction is stale (preparing for too long).
    #[must_use]
    pub fn is_stale(&self, timeout: Duration) -> bool {
        self.status == TransactionStatus::Preparing && self.started_at + timeout < Utc::now()
    }

    /// Returns the storage path for a transaction record.
    #[must_use]
    pub fn storage_path(tx_id: &str) -> String {
        iceberg_transaction_record_path(tx_id)
    }

    /// Checks if this transaction includes a specific table.
    #[must_use]
    pub fn includes_table(&self, table_uuid: &Uuid) -> bool {
        self.tables.iter().any(|t| &t.table_uuid == table_uuid)
    }

    /// Gets the entry for a specific table, if present.
    #[must_use]
    pub fn get_table_entry(&self, table_uuid: &Uuid) -> Option<&TransactionTableEntry> {
        self.tables.iter().find(|t| &t.table_uuid == table_uuid)
    }
}

// ============================================================================
// TransactionStore - Storage operations for transaction records
// ============================================================================

/// Result of attempting to create a transaction record.
#[derive(Debug, Clone)]
pub enum CreateTransactionResult {
    /// Successfully created the transaction (first create).
    Success {
        /// Version of the written record.
        version: ObjectVersion,
    },
    /// Transaction record already exists.
    Exists {
        /// The existing record (boxed to reduce enum size).
        record: Box<TransactionRecord>,
        /// Version of the existing record.
        version: ObjectVersion,
    },
}

/// Result of a CAS operation on a transaction record.
#[derive(Debug, Clone)]
pub enum TransactionCasResult {
    /// CAS succeeded.
    Success {
        /// New version after the write.
        version: ObjectVersion,
    },
    /// CAS failed due to version mismatch.
    Conflict {
        /// Current version that caused the conflict.
        current_version: ObjectVersion,
    },
}

/// Trait for transaction record storage operations.
#[async_trait]
pub trait TransactionStore: Send + Sync {
    /// Creates a new transaction record (fails if already exists).
    ///
    /// Returns `CreateTransactionResult::Success` if this is the first create.
    /// Returns `CreateTransactionResult::Exists` if the record already exists.
    async fn create(&self, record: &TransactionRecord) -> IcebergResult<CreateTransactionResult>;

    /// Loads an existing transaction record.
    async fn load(&self, tx_id: &str) -> IcebergResult<Option<(TransactionRecord, ObjectVersion)>>;

    /// Updates a transaction record with CAS (for status transitions).
    async fn compare_and_swap(
        &self,
        record: &TransactionRecord,
        expected_version: &ObjectVersion,
    ) -> IcebergResult<TransactionCasResult>;
}

/// Implementation of `TransactionStore` using `StorageBackend`.
pub struct TransactionStoreImpl<S> {
    storage: Arc<S>,
}

impl<S: StorageBackend> TransactionStoreImpl<S> {
    /// Creates a new transaction store.
    #[must_use]
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<S: StorageBackend> TransactionStore for TransactionStoreImpl<S> {
    async fn create(&self, record: &TransactionRecord) -> IcebergResult<CreateTransactionResult> {
        let path = TransactionRecord::storage_path(&record.tx_id);
        let bytes = serde_json::to_vec(record).map_err(|e| IcebergError::Internal {
            message: format!("Failed to serialize transaction record: {e}"),
        })?;

        match self
            .storage
            .put(&path, Bytes::from(bytes), WritePrecondition::DoesNotExist)
            .await
        {
            Ok(WriteResult::Success { version }) => Ok(CreateTransactionResult::Success {
                version: ObjectVersion::new(version),
            }),
            Ok(WriteResult::PreconditionFailed { current_version }) => {
                // Record exists - load it to return
                let existing = self.load(&record.tx_id).await?;
                match existing {
                    Some((existing_record, _)) => Ok(CreateTransactionResult::Exists {
                        record: Box::new(existing_record),
                        version: ObjectVersion::new(current_version),
                    }),
                    None => Err(IcebergError::Internal {
                        message: "Transaction record disappeared during create".to_string(),
                    }),
                }
            }
            Err(e) => Err(IcebergError::Internal {
                message: format!("Failed to create transaction record: {e}"),
            }),
        }
    }

    async fn load(&self, tx_id: &str) -> IcebergResult<Option<(TransactionRecord, ObjectVersion)>> {
        let path = TransactionRecord::storage_path(tx_id);

        let meta = self
            .storage
            .head(&path)
            .await
            .map_err(|e| IcebergError::Internal {
                message: format!("Failed to check transaction record existence: {e}"),
            })?;

        let Some(meta) = meta else {
            return Ok(None);
        };

        let bytes = self
            .storage
            .get(&path)
            .await
            .map_err(|e| IcebergError::Internal {
                message: format!("Failed to read transaction record: {e}"),
            })?;

        let record: TransactionRecord =
            serde_json::from_slice(&bytes).map_err(|e| IcebergError::Internal {
                message: format!("Failed to parse transaction record: {e}"),
            })?;

        Ok(Some((record, ObjectVersion::new(meta.version))))
    }

    async fn compare_and_swap(
        &self,
        record: &TransactionRecord,
        expected_version: &ObjectVersion,
    ) -> IcebergResult<TransactionCasResult> {
        let path = TransactionRecord::storage_path(&record.tx_id);
        let bytes = serde_json::to_vec(record).map_err(|e| IcebergError::Internal {
            message: format!("Failed to serialize transaction record: {e}"),
        })?;

        let precondition = WritePrecondition::MatchesVersion(expected_version.as_str().to_string());

        match self
            .storage
            .put(&path, Bytes::from(bytes), precondition)
            .await
        {
            Ok(WriteResult::Success { version }) => Ok(TransactionCasResult::Success {
                version: ObjectVersion::new(version),
            }),
            Ok(WriteResult::PreconditionFailed { current_version }) => {
                Ok(TransactionCasResult::Conflict {
                    current_version: ObjectVersion::new(current_version),
                })
            }
            Err(e) => Err(IcebergError::Internal {
                message: format!("Failed to CAS transaction record: {e}"),
            }),
        }
    }
}

/// Default timeout for considering a preparing transaction as stale.
pub const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::minutes(15);

/// Maximum number of tables allowed in a single transaction.
pub const MAX_TABLES_PER_TRANSACTION: usize = 10;

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::storage::MemoryBackend;

    fn make_tx_id() -> String {
        // Generate a valid UUIDv7-like string for testing
        "01941234-5678-7def-8abc-123456789abc".to_string()
    }

    #[test]
    fn test_transaction_record_serialization_roundtrip() {
        let record = TransactionRecord::new_preparing(make_tx_id(), "hash123".to_string())
            .with_table(TransactionTableEntry {
                table_uuid: Uuid::new_v4(),
                namespace: "sales".to_string(),
                table_name: "orders".to_string(),
                base_pointer_version: "v1".to_string(),
                new_metadata_location: "gs://bucket/metadata/00001.json".to_string(),
                new_snapshot_id: Some(12345),
                new_last_sequence_number: 1,
            });

        let json = serde_json::to_string_pretty(&record).expect("serialization");
        let parsed: TransactionRecord = serde_json::from_str(&json).expect("deserialization");

        assert_eq!(record.tx_id, parsed.tx_id);
        assert_eq!(record.request_hash, parsed.request_hash);
        assert_eq!(record.status, parsed.status);
        assert_eq!(record.tables.len(), parsed.tables.len());
    }

    #[test]
    fn test_transaction_status_transitions() {
        let record = TransactionRecord::new_preparing(make_tx_id(), "hash".to_string());
        assert_eq!(record.status, TransactionStatus::Preparing);
        assert!(record.committed_at.is_none());
        assert!(record.aborted_at.is_none());

        // Test commit transition
        let committed = record.clone().commit();
        assert_eq!(committed.status, TransactionStatus::Committed);
        assert!(committed.committed_at.is_some());
        assert!(committed.aborted_at.is_none());

        // Test abort transition
        let aborted = record.abort("test failure");
        assert_eq!(aborted.status, TransactionStatus::Aborted);
        assert!(aborted.aborted_at.is_some());
        assert!(aborted.committed_at.is_none());
        assert_eq!(aborted.abort_reason, Some("test failure".to_string()));
    }

    #[test]
    fn test_transaction_staleness() {
        let mut record = TransactionRecord::new_preparing(make_tx_id(), "hash".to_string());
        let timeout = Duration::minutes(10);

        // Fresh transaction is not stale
        assert!(!record.is_stale(timeout));

        // Old transaction is stale
        record.started_at = Utc::now() - Duration::minutes(15);
        assert!(record.is_stale(timeout));

        // Committed transaction is never stale (status check)
        let committed = record.clone().commit();
        assert!(!committed.is_stale(timeout));
    }

    #[test]
    fn test_transaction_table_lookup() {
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let uuid3 = Uuid::new_v4();

        let record = TransactionRecord::new_preparing(make_tx_id(), "hash".to_string())
            .with_table(TransactionTableEntry {
                table_uuid: uuid1,
                namespace: "ns1".to_string(),
                table_name: "table1".to_string(),
                base_pointer_version: "v1".to_string(),
                new_metadata_location: "loc1".to_string(),
                new_snapshot_id: None,
                new_last_sequence_number: 1,
            })
            .with_table(TransactionTableEntry {
                table_uuid: uuid2,
                namespace: "ns2".to_string(),
                table_name: "table2".to_string(),
                base_pointer_version: "v2".to_string(),
                new_metadata_location: "loc2".to_string(),
                new_snapshot_id: Some(100),
                new_last_sequence_number: 2,
            });

        assert!(record.includes_table(&uuid1));
        assert!(record.includes_table(&uuid2));
        assert!(!record.includes_table(&uuid3));

        let entry = record.get_table_entry(&uuid2).expect("entry exists");
        assert_eq!(entry.table_name, "table2");
        assert_eq!(entry.new_snapshot_id, Some(100));
    }

    #[test]
    fn test_storage_path() {
        let tx_id = "01941234-5678-7def-8abc-123456789abc";
        let path = TransactionRecord::storage_path(tx_id);
        assert_eq!(
            path,
            "_catalog/iceberg_transactions/01941234-5678-7def-8abc-123456789abc.json"
        );
    }

    #[tokio::test]
    async fn test_transaction_store_create() {
        let storage = Arc::new(MemoryBackend::new());
        let store = TransactionStoreImpl::new(storage);

        let record = TransactionRecord::new_preparing(make_tx_id(), "hash".to_string());

        // First create succeeds
        let result = store.create(&record).await.expect("create");
        assert!(matches!(result, CreateTransactionResult::Success { .. }));

        // Second create returns existing
        let result = store.create(&record).await.expect("create");
        assert!(matches!(result, CreateTransactionResult::Exists { .. }));
    }

    #[tokio::test]
    async fn test_transaction_store_load() {
        let storage = Arc::new(MemoryBackend::new());
        let store = TransactionStoreImpl::new(storage);

        let tx_id = make_tx_id();

        // Load non-existent returns None
        let result = store.load(&tx_id).await.expect("load");
        assert!(result.is_none());

        // Create and load
        let record = TransactionRecord::new_preparing(tx_id.clone(), "hash".to_string());
        store.create(&record).await.expect("create");

        let (loaded, _version) = store.load(&tx_id).await.expect("load").expect("exists");
        assert_eq!(loaded.tx_id, tx_id);
        assert_eq!(loaded.status, TransactionStatus::Preparing);
    }

    #[tokio::test]
    async fn test_transaction_store_cas() {
        let storage = Arc::new(MemoryBackend::new());
        let store = TransactionStoreImpl::new(storage);

        let tx_id = make_tx_id();
        let record = TransactionRecord::new_preparing(tx_id.clone(), "hash".to_string());

        // Create
        let result = store.create(&record).await.expect("create");
        let CreateTransactionResult::Success { version } = result else {
            panic!("expected success");
        };

        // CAS to commit
        let committed = record.commit();
        let result = store
            .compare_and_swap(&committed, &version)
            .await
            .expect("cas");
        assert!(matches!(result, TransactionCasResult::Success { .. }));

        // Verify committed
        let (loaded, _) = store.load(&tx_id).await.expect("load").expect("exists");
        assert_eq!(loaded.status, TransactionStatus::Committed);
        assert!(loaded.committed_at.is_some());

        // CAS with stale version fails
        let result = store
            .compare_and_swap(&committed, &version)
            .await
            .expect("cas");
        assert!(matches!(result, TransactionCasResult::Conflict { .. }));
    }
}
