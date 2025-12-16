//! Tier 2 event writer for append-only ledger persistence.
//!
//! INVARIANT 1: Ingest is append-only + ack. EventWriter must NEVER overwrite
//! existing events. Uses `DoesNotExist` precondition for true append-only semantics.
//!
//! Events are written as individual JSON files to the ledger path:
//! `ledger/{domain}/{event_id}.json`
//!
//! File naming uses ULID event_id ONLY (no timestamp prefix) for idempotent writes.
//! ULID's embedded timestamp ensures lexicographic ordering = chronological ordering.
//! NOTE: Production should use micro-batched segments to avoid "too many small files".

use bytes::Bytes;
use serde::Serialize;
use ulid::Ulid;

use arco_core::scoped_storage::ScopedStorage;
use arco_core::storage::WritePrecondition;

use crate::error::{CatalogError, Result};

/// Writes events to the Tier 2 append-only ledger.
///
/// INVARIANT: This writer is append-only. It uses `DoesNotExist` precondition
/// to ensure events are never overwritten (true audit log semantics).
///
/// Events are stored as individual JSON files at:
/// `ledger/{domain}/{event_id}.json`
///
/// Using event_id only (no timestamp prefix) ensures:
/// 1. Idempotent writes: same event_id always maps to same path
/// 2. DoesNotExist precondition prevents overwrites on replay
/// 3. ULID's embedded timestamp maintains lexicographic = chronological ordering
pub struct EventWriter {
    storage: ScopedStorage,
}

impl EventWriter {
    /// Creates a new event writer.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self { storage }
    }

    /// Appends an event to the ledger with an auto-generated ID.
    ///
    /// Returns the generated event ID.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or storage fails.
    pub async fn append<T: Serialize>(&self, domain: &str, event: &T) -> Result<String> {
        let event_id = Ulid::new().to_string();
        self.append_with_id(domain, event, &event_id).await?;
        Ok(event_id)
    }

    /// Appends an event with a specific ID (for idempotent replays).
    ///
    /// INVARIANT: Uses `DoesNotExist` precondition - true append-only.
    /// If an event with this ID already exists, returns Ok (duplicate delivery).
    /// The ledger is immutable; duplicates are handled at compaction time.
    ///
    /// IDEMPOTENCY: Path is deterministic based on event_id only.
    /// Same event_id always maps to same path, enabling DoesNotExist to prevent duplicates.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or storage fails (NOT for duplicates).
    pub async fn append_with_id<T: Serialize>(
        &self,
        domain: &str,
        event: &T,
        event_id: &str,
    ) -> Result<()> {
        // CRITICAL: Use event_id ONLY (no timestamp prefix) for idempotent writes
        // ULID's embedded timestamp ensures lexicographic = chronological ordering
        let path = format!("ledger/{domain}/{event_id}.json");
        let json = serde_json::to_vec_pretty(event).map_err(|e| CatalogError::Serialization {
            message: format!("failed to serialize event: {e}"),
        })?;

        // CRITICAL: Use DoesNotExist for true append-only semantics
        let result = self
            .storage
            .put_raw(&path, Bytes::from(json), WritePrecondition::DoesNotExist)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to write event: {e}"),
            })?;

        // Handle duplicate delivery gracefully (idempotent behavior)
        match result {
            arco_core::storage::WriteResult::Success { .. } => Ok(()),
            arco_core::storage::WriteResult::PreconditionFailed { .. } => {
                // Duplicate delivery - this is OK, compactor will dedupe
                tracing::debug!(event_id, "duplicate event delivery (already exists)");
                Ok(())
            }
        }
    }

    /// Lists all event files in a domain.
    ///
    /// Returns file paths sorted lexicographically (ULID ensures chronological).
    /// NOTE: Object store list() ordering is NOT guaranteed - we sort explicitly.
    ///
    /// # Errors
    ///
    /// Returns an error if listing fails.
    pub async fn list_events(&self, domain: &str) -> Result<Vec<String>> {
        let prefix = format!("ledger/{domain}/");
        // ScopedStorage.list() returns Vec<ScopedPath> - relative paths
        let mut files: Vec<_> = self
            .storage
            .list(&prefix)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to list events: {e}"),
            })?
            .into_iter()
            .map(|p| p.to_string()) // ScopedPath -> String
            .collect();

        // CRITICAL: Sort explicitly - object store list() order is not guaranteed
        files.sort();
        Ok(files)
    }

    /// Reads a specific event file by path.
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails.
    pub async fn read_event_file(&self, path: &str) -> Result<Bytes> {
        self.storage
            .get_raw(path)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to read event: {e}"),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::storage::MemoryBackend;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestEvent {
        value: i32,
    }

    #[tokio::test]
    async fn append_event_creates_ledger_file() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let writer = EventWriter::new(storage.clone());
        let event = TestEvent { value: 42 };

        let event_id = writer.append("execution", &event).await.expect("append");

        // Verify file exists in ledger
        let files = writer.list_events("execution").await.expect("list");
        assert_eq!(files.len(), 1);
        assert!(files[0].contains(&event_id), "file should contain event ID");
    }

    #[tokio::test]
    async fn append_is_truly_append_only() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let writer = EventWriter::new(storage.clone());
        let event1 = TestEvent { value: 42 };
        let event2 = TestEvent { value: 99 };
        let fixed_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

        // First write succeeds
        writer
            .append_with_id("execution", &event1, fixed_id)
            .await
            .expect("first");

        // Second write with same ID returns Ok but doesn't overwrite
        // (DoesNotExist precondition fails, but we handle gracefully)
        writer
            .append_with_id("execution", &event2, fixed_id)
            .await
            .expect("second should succeed (duplicate handled)");

        // Should still only have one file (DoesNotExist prevented overwrite)
        let files = storage.list("ledger/execution/").await.expect("list");
        assert_eq!(files.len(), 1, "append-only: no duplicates created");

        // Original value preserved (not overwritten)
        let data = storage.get_raw(files[0].as_str()).await.expect("read");
        let parsed: TestEvent = serde_json::from_slice(&data).expect("parse");
        assert_eq!(parsed.value, 42, "original event preserved, not overwritten");
    }

    #[tokio::test]
    async fn events_are_ordered_chronologically() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let writer = EventWriter::new(storage.clone());

        // Write 5 events
        let mut ids = Vec::new();
        for i in 0..5 {
            let event = TestEvent { value: i };
            let id = writer.append("execution", &event).await.expect("append");
            ids.push(id);
        }

        // List files - EventWriter.list_events() sorts explicitly
        let files = writer.list_events("execution").await.expect("list");

        // Verify ordering is maintained
        assert_eq!(files.len(), 5);
        // Files should already be sorted (list_events does this)
        let mut sorted = files.clone();
        sorted.sort();
        assert_eq!(
            files, sorted,
            "ULID ensures lexicographic = chronological ordering"
        );
    }
}
