//! Outbox and persistence helpers for execution events.
//!
//! The scheduler produces execution events describing run/task lifecycle changes.
//! These events should be persisted to Tier 2 (append-only ledger) and later
//! compacted into queryable state.

use bytes::Bytes;
use chrono::Utc;
use futures::TryStreamExt;
use futures::stream;
use ulid::Ulid;

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
    /// Creates a new ledger writer for the given domain (e.g. "executions").
    #[must_use]
    pub fn new(storage: ScopedStorage, domain: impl Into<String>) -> Self {
        Self {
            storage,
            domain: domain.into(),
        }
    }

    /// Appends an event to the ledger.
    ///
    /// Events are written to `ledger/flow/{domain}/{date}/{event_id}.json`, where:
    /// - `date` is `YYYY-MM-DD` (UTC), derived from the ULID timestamp in `event.id`
    ///
    /// The `flow/` namespace prevents collisions with catalog Tier 2 ledgers, which store
    /// materialization records under `ledger/{domain}/...`.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the storage write fails.
    #[tracing::instrument(
        skip(self, event),
        fields(
            domain = %self.domain,
            run_id = tracing::field::Empty,
            sequence = ?event.sequence,
            event_type = %event.event_type,
            path = tracing::field::Empty
        )
    )]
    pub async fn append(&self, event: EventEnvelope) -> Result<()> {
        let run_id = event.run_id().ok_or_else(|| Error::Serialization {
            message: "execution event is missing a run_id".into(),
        })?;
        tracing::Span::current().record("run_id", tracing::field::display(run_id));

        let event_ulid = Ulid::from_string(&event.id).map_err(|e| Error::Serialization {
            message: format!("execution event id is not a valid ULID: {e}"),
        })?;
        let ms_i64 = i64::try_from(event_ulid.timestamp_ms()).unwrap_or(i64::MAX);
        let event_time = chrono::DateTime::from_timestamp_millis(ms_i64).unwrap_or_else(Utc::now);
        let date = event_time.format("%Y-%m-%d").to_string();

        let path = format!("ledger/flow/{}/{}/{}.json", self.domain, date, event.id);
        tracing::Span::current().record("path", tracing::field::display(&path));

        let json = serde_json::to_string(&event).map_err(|e| Error::Serialization {
            message: format!("failed to serialize execution event: {e}"),
        })?;

        let result = self
            .storage
            .put_raw(&path, Bytes::from(json), WritePrecondition::DoesNotExist)
            .await?;

        match result {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { .. } => {
                tracing::debug!(event_id = %event.id, "duplicate execution event delivery");
                Ok(())
            }
        }
    }

    /// Appends a batch of events to the ledger.
    ///
    /// Writes are performed with bounded concurrency for throughput. Ordering for replay is
    /// provided by the per-run `sequence` field embedded in the event envelope.
    ///
    /// # Errors
    ///
    /// Returns the first error encountered while writing the batch.
    #[tracing::instrument(skip(self, events), fields(domain = %self.domain))]
    pub async fn append_all(&self, events: impl IntoIterator<Item = EventEnvelope>) -> Result<()> {
        const MAX_IN_FLIGHT_WRITES: usize = 16;

        let writer = self.clone();
        stream::iter(events.into_iter().map(Ok))
            .try_for_each_concurrent(MAX_IN_FLIGHT_WRITES, move |event| {
                let writer = writer.clone();
                async move { writer.append(event).await }
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{EventBuilder, EventEnvelope};
    use arco_core::{MemoryBackend, RunId, ScopedStorage};
    use std::sync::Arc;

    #[tokio::test]
    async fn ledger_writer_writes_events_under_domain_date_prefix() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
        let writer = LedgerWriter::new(storage.clone(), "executions");

        let run_id = RunId::generate();
        let mut event =
            EventBuilder::run_started("tenant", "workspace", run_id, "plan-1").with_sequence(1);
        event.id = "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string();

        writer.append(event).await?;

        let ulid = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("valid ULID");
        let ms_i64 = i64::try_from(ulid.timestamp_ms()).unwrap_or(i64::MAX);
        let event_time = chrono::DateTime::from_timestamp_millis(ms_i64).unwrap_or_else(Utc::now);
        let date = event_time.format("%Y-%m-%d").to_string();

        let path = format!("ledger/flow/executions/{date}/01ARZ3NDEKTSV4RRFFQ69G5FAV.json");
        let data = storage.get_raw(&path).await?;
        let parsed: EventEnvelope =
            serde_json::from_slice(&data).map_err(|e| Error::Serialization {
                message: format!("failed to parse stored envelope: {e}"),
            })?;
        assert_eq!(parsed.sequence, Some(1));
        assert_eq!(parsed.run_id(), Some(&run_id));

        Ok(())
    }
}
