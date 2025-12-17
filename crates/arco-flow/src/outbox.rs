//! Outbox and persistence helpers for execution events.
//!
//! The scheduler produces execution events describing run/task lifecycle changes.
//! These events should be persisted to Tier 2 (append-only ledger) and later
//! compacted into queryable state.

use bytes::Bytes;
use chrono::{SecondsFormat, Utc};
use futures::TryStreamExt;
use futures::stream;

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
    /// Events are written to `ledger/{domain}/{date}/{timestamp}-{event_id}.json`, where:
    /// - `date` is `YYYY-MM-DD` (UTC), used for partitioning/list efficiency
    /// - `timestamp` is RFC 3339 (UTC)
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

        let event_time = event.time.unwrap_or_else(Utc::now);
        let date = event_time.format("%Y-%m-%d").to_string();
        let timestamp = event_time.to_rfc3339_opts(SecondsFormat::Secs, true);

        let path = format!(
            "ledger/{}/{}/{}-{}.json",
            self.domain, date, timestamp, event.id
        );
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
            WriteResult::PreconditionFailed { current_version } => Err(Error::storage(format!(
                "event already exists at {path} (version {current_version})"
            ))),
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
    use chrono::TimeZone;
    use std::sync::Arc;

    #[tokio::test]
    async fn ledger_writer_writes_events_under_domain_date_prefix() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
        let writer = LedgerWriter::new(storage.clone(), "execution");

        let run_id = RunId::generate();
        let mut event =
            EventBuilder::run_started("tenant", "workspace", run_id, "plan-1").with_sequence(1);
        event.id = "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string();
        event.time = Some(
            Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0)
                .single()
                .ok_or_else(|| Error::TaskExecutionFailed {
                    message: "fixed test timestamp should be valid".into(),
                })?,
        );

        writer.append(event).await?;

        let path =
            "ledger/execution/2025-01-15/2025-01-15T10:00:00Z-01ARZ3NDEKTSV4RRFFQ69G5FAV.json";
        let data = storage.get_raw(path).await?;
        let parsed: EventEnvelope =
            serde_json::from_slice(&data).map_err(|e| Error::Serialization {
                message: format!("failed to parse stored envelope: {e}"),
            })?;
        assert_eq!(parsed.sequence, Some(1));
        assert_eq!(parsed.run_id(), Some(&run_id));

        Ok(())
    }
}
