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
use crate::paths::flow_event_path;

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

        let path = flow_event_path(&self.domain, &date, &event.id);
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
    use arco_core::storage::{ObjectMeta, StorageBackend};
    use arco_core::{Error as CoreError, FlowPaths, MemoryBackend, RunId, ScopedStorage};
    use async_trait::async_trait;
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Debug)]
    struct FailOnceNthPutBackend {
        inner: MemoryBackend,
        fail_on_put_call: usize,
        put_calls: AtomicUsize,
    }

    impl FailOnceNthPutBackend {
        fn new(fail_on_put_call: usize) -> Self {
            Self {
                inner: MemoryBackend::new(),
                fail_on_put_call,
                put_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl StorageBackend for FailOnceNthPutBackend {
        async fn get(&self, path: &str) -> arco_core::error::Result<Bytes> {
            self.inner.get(path).await
        }

        async fn get_range(
            &self,
            path: &str,
            range: Range<u64>,
        ) -> arco_core::error::Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::error::Result<WriteResult> {
            let call = self.put_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.fail_on_put_call {
                return Err(CoreError::Storage {
                    message: format!("injected partial-write failure at put call {call} ({path})"),
                    source: None,
                });
            }
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::error::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::error::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> arco_core::error::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(
            &self,
            path: &str,
            expiry: Duration,
        ) -> arco_core::error::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

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

        let path = flow_event_path("executions", &date, "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let data = storage.get_raw(&path).await?;
        let parsed: EventEnvelope =
            serde_json::from_slice(&data).map_err(|e| Error::Serialization {
                message: format!("failed to parse stored envelope: {e}"),
            })?;
        assert_eq!(parsed.sequence, Some(1));
        assert_eq!(parsed.run_id(), Some(&run_id));

        Ok(())
    }

    #[tokio::test]
    async fn ledger_writer_recovers_after_partial_batch_write_failure() -> Result<()> {
        let backend = Arc::new(FailOnceNthPutBackend::new(2));
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
        let writer = LedgerWriter::new(storage.clone(), "executions");

        let run_id = RunId::generate();

        let mut event1 =
            EventBuilder::run_started("tenant", "workspace", run_id, "plan-1").with_sequence(1);
        event1.id = "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string();

        let mut event2 =
            EventBuilder::run_started("tenant", "workspace", run_id, "plan-1").with_sequence(2);
        event2.id = "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_string();

        let batch = vec![event1.clone(), event2.clone()];

        // Injected failure creates a partial-write window for the first append attempt.
        let first_attempt = writer.append_all(batch.clone()).await;
        assert!(first_attempt.is_err(), "first append should fail");

        // Replay of the same batch must converge idempotently.
        writer.append_all(batch).await?;

        let date = "2016-07-30".to_string();
        let path1 = flow_event_path("executions", &date, &event1.id);
        let path2 = flow_event_path("executions", &date, &event2.id);

        assert!(storage.get_raw(&path1).await.is_ok());
        assert!(storage.get_raw(&path2).await.is_ok());

        let entries = storage
            .list(&format!(
                "{}/{}/",
                FlowPaths::FLOW_LEDGER_PREFIX,
                "executions"
            ))
            .await?;
        assert_eq!(
            entries.len(),
            2,
            "replay must not create duplicate ledger rows"
        );

        Ok(())
    }
}
