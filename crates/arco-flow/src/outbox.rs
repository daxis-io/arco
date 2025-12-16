//! Outbox and persistence helpers for execution events.
//!
//! The scheduler produces execution events describing run/task lifecycle changes.
//! These events should be persisted to Tier 2 (append-only ledger) and later
//! compacted into queryable state.

use bytes::Bytes;

use arco_core::{ScopedStorage, WritePrecondition, WriteResult};

use crate::error::{Error, Result};
use crate::events::EventEnvelope;

/// A sink for execution events emitted by orchestration operations.
///
/// This is intentionally synchronous: the scheduler can remain deterministic and
/// side-effect free, while callers decide when/how to persist events.
pub trait EventSink {
    /// Records an event for later persistence.
    fn push(&mut self, event: EventEnvelope);
}

/// In-memory outbox for collecting execution events.
#[derive(Debug, Default)]
pub struct InMemoryOutbox {
    events: Vec<EventEnvelope>,
}

impl InMemoryOutbox {
    /// Creates a new empty outbox.
    #[must_use]
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    /// Returns all collected events.
    #[must_use]
    pub fn events(&self) -> &[EventEnvelope] {
        &self.events
    }

    /// Drains the outbox, returning all events in insertion order.
    pub fn drain(&mut self) -> Vec<EventEnvelope> {
        std::mem::take(&mut self.events)
    }
}

impl EventSink for InMemoryOutbox {
    fn push(&mut self, event: EventEnvelope) {
        self.events.push(event);
    }
}

/// Writes execution events to the Tier 2 ledger (append-only JSON files).
#[derive(Clone)]
pub struct LedgerWriter {
    storage: ScopedStorage,
    domain: String,
}

impl LedgerWriter {
    /// Creates a new ledger writer for the given domain (e.g. "execution").
    #[must_use]
    pub fn new(storage: ScopedStorage, domain: impl Into<String>) -> Self {
        Self {
            storage,
            domain: domain.into(),
        }
    }

    /// Appends an event to the ledger.
    ///
    /// Events are written to `ledger/{domain}/runs/{run_id}/{sequence}.json` when a per-run
    /// `sequence` is present, falling back to an unsequenced path otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the storage write fails.
    pub async fn append(&self, event: EventEnvelope) -> Result<()> {
        let run_id = event.run_id().ok_or_else(|| Error::Serialization {
            message: "execution event is missing a run_id".into(),
        })?;

        let path = event.sequence.map_or_else(
            || {
                format!(
                    "ledger/{}/runs/{run_id}/unsequenced/{}.json",
                    self.domain, event.id
                )
            },
            |seq| format!("ledger/{}/runs/{run_id}/{seq:020}.json", self.domain),
        );

        let json = serde_json::to_string(&event).map_err(|e| Error::Serialization {
            message: format!("failed to serialize execution event: {e}"),
        })?;

        let result = self
            .storage
            .put_raw(&path, Bytes::from(json), WritePrecondition::DoesNotExist)
            .await?;

        match result {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { current_version } => Err(Error::storage(format!(
                "event already exists at {path} (version {current_version})"
            ))),
        }
    }

    /// Appends a batch of events to the ledger in order.
    ///
    /// # Errors
    ///
    /// Returns the first error encountered while writing the batch.
    pub async fn append_all(&self, events: impl IntoIterator<Item = EventEnvelope>) -> Result<()> {
        for event in events {
            self.append(event).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{EventBuilder, EventEnvelope};
    use arco_core::{MemoryBackend, RunId, ScopedStorage};
    use std::sync::Arc;

    #[tokio::test]
    async fn ledger_writer_writes_sequenced_events_under_run_prefix() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").unwrap();
        let writer = LedgerWriter::new(storage.clone(), "execution");

        let run_id = RunId::generate();
        let event =
            EventBuilder::run_started("tenant", "workspace", run_id, "plan-1").with_sequence(1);

        writer.append(event).await.unwrap();

        let path = format!("ledger/execution/runs/{run_id}/00000000000000000001.json");
        let data = storage.get_raw(&path).await.unwrap();
        let parsed: EventEnvelope = serde_json::from_slice(&data).unwrap();
        assert_eq!(parsed.sequence, Some(1));
        assert_eq!(parsed.run_id(), Some(&run_id));
    }
}
