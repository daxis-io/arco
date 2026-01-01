//! Tier 2 event writer for append-only ledger persistence.
//!
//! INVARIANT 1: Ingest is append-only + ack. `EventWriter` must NEVER overwrite
//! existing events. Uses `DoesNotExist` precondition for true append-only semantics.
//!
//! Events are written as individual JSON files to the ledger path:
//! `ledger/{domain}/{event_id}.json`
//!
//! File naming uses ULID `event_id` ONLY (no timestamp prefix) for idempotent writes.
//! ULID's embedded timestamp ensures lexicographic ordering = chronological ordering.
//! NOTE: Production should use micro-batched segments to avoid "too many small files".
//!
//! Ledger payloads are wrapped in the [`arco_core::CatalogEvent`] envelope (ADR-004) to
//! support schema evolution and deduplication during compaction.

use bytes::Bytes;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use arco_core::scoped_storage::ScopedStorage;
use arco_core::storage::WritePrecondition;
use arco_core::{CatalogDomain, CatalogEvent, CatalogEventPayload, CatalogPaths, EventId};

use crate::error::{CatalogError, Result};
use crate::metrics;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SequenceCounter {
    next: u64,
}

impl SequenceCounter {
    const fn new(next: u64) -> Self {
        Self { next }
    }

    fn allocate(&mut self) -> Result<u64> {
        let allocated = self.next;
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: "sequence counter overflowed u64".to_string(),
            })?;
        Ok(allocated)
    }
}

/// Writes events to the Tier 2 append-only ledger.
///
/// INVARIANT: This writer is append-only. It uses `DoesNotExist` precondition
/// to ensure events are never overwritten (true audit log semantics).
///
/// Events are stored as individual JSON files at:
/// `ledger/{domain}/{event_id}.json`
///
/// Using `event_id` only (no timestamp prefix) ensures:
/// 1. Idempotent writes: same `event_id` always maps to same path
/// 2. `DoesNotExist` precondition prevents overwrites on replay
/// 3. ULID's embedded timestamp maintains lexicographic = chronological ordering
pub struct EventWriter {
    storage: ScopedStorage,
    source: String,
}

impl EventWriter {
    const SEQUENCE_CAS_MAX_RETRIES: u32 = 10;

    /// Creates a new event writer.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self {
            storage,
            source: "arco-catalog".to_string(),
        }
    }

    /// Sets the `source` field written into the event envelope (ADR-004).
    #[must_use]
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = source.into();
        self
    }

    /// Appends an event to the ledger with an auto-generated ID.
    ///
    /// Returns the generated event ID.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or storage fails.
    pub async fn append<T: CatalogEventPayload + Serialize + Sync>(
        &self,
        domain: CatalogDomain,
        event: &T,
    ) -> Result<EventId> {
        let event_id = EventId::generate();
        self.append_with_id(domain, event, &event_id).await?;
        Ok(event_id)
    }

    /// Appends an event with a specific ID (for idempotent replays).
    ///
    /// INVARIANT: Uses `DoesNotExist` precondition - true append-only.
    /// If an event with this ID already exists, returns Ok (duplicate delivery).
    /// The ledger is immutable; duplicates are handled at compaction time.
    ///
    /// IDEMPOTENCY: Path is deterministic based on `event_id` only.
    /// Same `event_id` always maps to same path, enabling `DoesNotExist` to prevent duplicates.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or storage fails (NOT for duplicates).
    pub async fn append_with_id<T: CatalogEventPayload + Serialize + Sync>(
        &self,
        domain: CatalogDomain,
        event: &T,
        event_id: &EventId,
    ) -> Result<()> {
        // CRITICAL: Use event_id ONLY (no timestamp prefix) for idempotent writes
        // ULID's embedded timestamp ensures lexicographic = chronological ordering
        let path = CatalogPaths::ledger_event(domain, &event_id.to_string());

        // Fast-path duplicates to avoid wasting sequence positions.
        if self
            .storage
            .head_raw(&path)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to check event existence: {e}"),
            })?
            .is_some()
        {
            tracing::debug!(event_id = %event_id, "duplicate event delivery (already exists)");
            metrics::inc_event_writer_written(domain, "duplicate");
            return Ok(());
        }

        let event_type = T::EVENT_TYPE;
        let event_version = T::EVENT_VERSION;
        let idempotency_key =
            CatalogEvent::<()>::generate_idempotency_key(event_type, event_version, event)
                .map_err(|e| CatalogError::Serialization {
                    message: format!("failed to generate idempotency key: {e}"),
                })?;

        let sequence_position = self.allocate_sequence_position(domain).await?;

        let envelope = CatalogEvent {
            event_type: event_type.to_string(),
            event_version,
            idempotency_key,
            occurred_at: Utc::now(),
            source: self.source.clone(),
            trace_id: None,
            sequence_position: Some(sequence_position),
            payload: event,
        };

        envelope
            .validate()
            .map_err(|e| CatalogError::InvariantViolation {
                message: format!("invalid event envelope: {e}"),
            })?;

        let json =
            serde_json::to_vec_pretty(&envelope).map_err(|e| CatalogError::Serialization {
                message: format!("failed to serialize event: {e}"),
            })?;
        let json_len = u64::try_from(json.len()).unwrap_or(u64::MAX);

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
            arco_core::storage::WriteResult::Success { .. } => {
                metrics::inc_event_writer_written(domain, "success");
                metrics::add_event_writer_bytes_written(domain, json_len);
                Ok(())
            }
            arco_core::storage::WriteResult::PreconditionFailed { .. } => {
                // Duplicate delivery - this is OK, compactor will dedupe
                tracing::debug!(event_id = %event_id, "duplicate event delivery (already exists)");
                metrics::inc_event_writer_written(domain, "duplicate");
                metrics::inc_event_writer_sequence_allocation("wasted_duplicate");
                Ok(())
            }
        }
    }

    /// Lists all event files in a domain.
    ///
    /// Returns file paths sorted lexicographically (ULID ensures chronological).
    /// NOTE: Object store `list()` ordering is NOT guaranteed - we sort explicitly.
    ///
    /// # Errors
    ///
    /// Returns an error if listing fails.
    pub async fn list_events(&self, domain: CatalogDomain) -> Result<Vec<String>> {
        let prefix = CatalogPaths::ledger_dir(domain);
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

    async fn allocate_sequence_position(&self, domain: CatalogDomain) -> Result<u64> {
        let path = CatalogPaths::sequence_counter(domain);

        for attempt in 1..=Self::SEQUENCE_CAS_MAX_RETRIES {
            let meta = self
                .storage
                .head_raw(&path)
                .await
                .map_err(|e| CatalogError::Storage {
                    message: format!("failed to read sequence counter metadata: {e}"),
                })?;

            match meta {
                None => {
                    let counter = SequenceCounter::new(2);
                    let json = serde_json::to_vec_pretty(&counter).map_err(|e| {
                        CatalogError::Serialization {
                            message: format!("failed to serialize sequence counter: {e}"),
                        }
                    })?;

                    let result = self
                        .storage
                        .put_raw(&path, Bytes::from(json), WritePrecondition::DoesNotExist)
                        .await
                        .map_err(|e| CatalogError::Storage {
                            message: format!("failed to create sequence counter: {e}"),
                        })?;

                    match result {
                        arco_core::storage::WriteResult::Success { .. } => {
                            metrics::inc_event_writer_sequence_allocation("success");
                            return Ok(1);
                        }
                        arco_core::storage::WriteResult::PreconditionFailed { .. } => {
                            metrics::inc_event_writer_sequence_allocation("cas_retry");
                            metrics::record_cas_retry("sequence_counter");
                            if attempt == Self::SEQUENCE_CAS_MAX_RETRIES {
                                break;
                            }
                        }
                    }
                }
                Some(meta) => {
                    let data =
                        self.storage
                            .get_raw(&path)
                            .await
                            .map_err(|e| CatalogError::Storage {
                                message: format!("failed to read sequence counter: {e}"),
                            })?;

                    let mut counter: SequenceCounter =
                        serde_json::from_slice(&data).map_err(|e| CatalogError::Serialization {
                            message: format!("failed to parse sequence counter: {e}"),
                        })?;

                    let allocated = counter.allocate()?;
                    let json = serde_json::to_vec_pretty(&counter).map_err(|e| {
                        CatalogError::Serialization {
                            message: format!("failed to serialize sequence counter: {e}"),
                        }
                    })?;

                    let result = self
                        .storage
                        .put_raw(
                            &path,
                            Bytes::from(json),
                            WritePrecondition::MatchesVersion(meta.version.clone()),
                        )
                        .await
                        .map_err(|e| CatalogError::Storage {
                            message: format!("failed to update sequence counter: {e}"),
                        })?;

                    match result {
                        arco_core::storage::WriteResult::Success { .. } => {
                            metrics::inc_event_writer_sequence_allocation("success");
                            return Ok(allocated);
                        }
                        arco_core::storage::WriteResult::PreconditionFailed { .. } => {
                            metrics::inc_event_writer_sequence_allocation("cas_retry");
                            metrics::record_cas_retry("sequence_counter");
                            if attempt == Self::SEQUENCE_CAS_MAX_RETRIES {
                                break;
                            }
                        }
                    }
                }
            }
        }

        metrics::inc_event_writer_sequence_allocation("cas_exhausted");
        Err(CatalogError::CasFailed {
            message: format!(
                "sequence counter CAS failed after {} retries for domain '{domain}'",
                Self::SEQUENCE_CAS_MAX_RETRIES
            ),
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use arco_core::storage::MemoryBackend;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use tokio::sync::Barrier;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestEvent {
        value: i32,
    }

    impl CatalogEventPayload for TestEvent {
        const EVENT_TYPE: &'static str = "test.event";
        const EVENT_VERSION: u32 = 1;
    }

    #[tokio::test]
    async fn append_event_creates_ledger_file() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let writer = EventWriter::new(storage.clone());
        let event = TestEvent { value: 42 };

        let event_id = writer
            .append(CatalogDomain::Executions, &event)
            .await
            .expect("append");

        // Verify file exists in ledger
        let files = writer
            .list_events(CatalogDomain::Executions)
            .await
            .expect("list");
        assert_eq!(files.len(), 1);
        let file = files.first().expect("exactly one file");
        assert!(
            file.contains(&event_id.to_string()),
            "file should contain event ID"
        );

        let data = storage.get_raw(file).await.expect("read event");
        let stored: CatalogEvent<TestEvent> = serde_json::from_slice(&data).expect("parse");
        stored.validate().expect("valid envelope");
        assert_eq!(stored.event_type, "test.event");
        assert_eq!(stored.event_version, 1);
        assert!(!stored.idempotency_key.is_empty());
        assert_eq!(stored.source, "arco-catalog");
        assert!(
            stored.sequence_position.is_some(),
            "writer should assign sequence_position at ingest"
        );
        assert_eq!(stored.payload.value, 42);
    }

    #[tokio::test]
    async fn append_is_truly_append_only() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let writer = EventWriter::new(storage.clone());
        let event1 = TestEvent { value: 42 };
        let event2 = TestEvent { value: 99 };
        let fixed_id: EventId = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            .parse()
            .expect("valid event id");

        // First write succeeds
        writer
            .append_with_id(CatalogDomain::Executions, &event1, &fixed_id)
            .await
            .expect("first");

        // Second write with same ID returns Ok but doesn't overwrite
        // (DoesNotExist precondition fails, but we handle gracefully)
        writer
            .append_with_id(CatalogDomain::Executions, &event2, &fixed_id)
            .await
            .expect("second should succeed (duplicate handled)");

        // Should still only have one file (DoesNotExist prevented overwrite)
        let files = storage
            .list(&CatalogPaths::ledger_dir(CatalogDomain::Executions))
            .await
            .expect("list");
        assert_eq!(files.len(), 1, "append-only: no duplicates created");

        // Original value preserved (not overwritten)
        let file = files.first().expect("exactly one file");
        let data = storage.get_raw(file.as_str()).await.expect("read");
        let stored: CatalogEvent<TestEvent> = serde_json::from_slice(&data).expect("parse");
        assert_eq!(
            stored.payload.value, 42,
            "original event preserved, not overwritten"
        );
    }

    #[tokio::test]
    async fn events_are_ordered_chronologically() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let writer = EventWriter::new(storage.clone());

        // Write 5 events
        for i in 0..5 {
            let event = TestEvent { value: i };
            let _id = writer
                .append(CatalogDomain::Executions, &event)
                .await
                .expect("append");
        }

        // List files - EventWriter.list_events() sorts explicitly
        let files = writer
            .list_events(CatalogDomain::Executions)
            .await
            .expect("list");

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

    #[tokio::test]
    async fn sequence_positions_are_monotonic() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let writer = EventWriter::new(storage.clone());

        for i in 0..5 {
            let event = TestEvent { value: i };
            writer
                .append(CatalogDomain::Executions, &event)
                .await
                .expect("append");
        }

        let files = writer
            .list_events(CatalogDomain::Executions)
            .await
            .expect("list");

        let mut positions: Vec<u64> = Vec::new();
        for file in files {
            let data = storage.get_raw(file.as_str()).await.expect("read");
            let stored: CatalogEvent<TestEvent> = serde_json::from_slice(&data).expect("parse");
            positions.push(stored.sequence_position.expect("sequence"));
        }

        positions.sort_unstable();
        assert_eq!(positions, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn concurrent_appends_allocate_unique_sequence_positions() {
        const N: usize = 20;

        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let writer = Arc::new(EventWriter::new(storage.clone()));
        let barrier = Arc::new(Barrier::new(N));

        let mut handles = Vec::new();
        for i in 0..N {
            let writer = Arc::clone(&writer);
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                let value = i32::try_from(i).expect("i fits in i32");
                let event = TestEvent { value };
                writer
                    .append(CatalogDomain::Executions, &event)
                    .await
                    .expect("append");
            }));
        }

        for handle in handles {
            handle.await.expect("task");
        }

        let files = writer
            .list_events(CatalogDomain::Executions)
            .await
            .expect("list");
        assert_eq!(files.len(), N);

        let mut positions: Vec<u64> = Vec::new();
        for file in files {
            let data = storage.get_raw(file.as_str()).await.expect("read");
            let stored: CatalogEvent<TestEvent> = serde_json::from_slice(&data).expect("parse");
            positions.push(stored.sequence_position.expect("sequence"));
        }

        positions.sort_unstable();
        positions.dedup();
        assert_eq!(positions.len(), N, "sequence positions must be unique");
    }
}
