//! Orchestration ledger writer for appending events.
//!
//! Events are written to `ledger/orchestration/{date}/{event_id}.json` where:
//! - `date` is `YYYY-MM-DD` (UTC), derived from the event timestamp
//! - `event_id` is the event's ULID identifier
//!
//! The ledger is the source of truth. The compactor processes ledger events
//! and produces Parquet projections that controllers read.

use std::future::Future;

use bytes::Bytes;
use chrono::Utc;
use futures::TryStreamExt;
use futures::stream;
use ulid::Ulid;

use arco_core::{ScopedStorage, WritePrecondition, WriteResult};

use super::events::OrchestrationEvent;
use crate::error::{Error, Result};

/// Trait for writing orchestration events to the ledger.
///
/// This trait is used by callback handlers and other components that need
/// to emit events. The main implementation is [`LedgerWriter`].
pub trait OrchestrationLedgerWriter: Send + Sync {
    /// Writes an event to the ledger.
    fn write_event(
        &self,
        event: &OrchestrationEvent,
    ) -> impl Future<Output = std::result::Result<(), String>> + Send;
}

/// Writes orchestration events to the ledger (append-only JSON files).
///
/// Events are written idempotently - duplicate writes are no-ops.
#[derive(Clone)]
pub struct LedgerWriter {
    storage: ScopedStorage,
}

impl LedgerWriter {
    /// Creates a new ledger writer.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self { storage }
    }

    /// Appends an event to the ledger.
    ///
    /// Events are written to `ledger/orchestration/{date}/{event_id}.json`.
    /// Writes use `DoesNotExist` precondition for idempotency.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the storage write fails.
    #[tracing::instrument(
        skip(self, event),
        fields(
            event_id = %event.event_id,
            event_type = %event.event_type,
            tenant_id = %event.tenant_id,
            workspace_id = %event.workspace_id,
            correlation_id = ?event.correlation_id,
            path = tracing::field::Empty
        )
    )]
    pub async fn append(&self, event: OrchestrationEvent) -> Result<()> {
        // Derive date from event timestamp (or fallback to event_id ULID)
        let date = Ulid::from_string(&event.event_id).map_or_else(
            |_| event.timestamp.format("%Y-%m-%d").to_string(),
            |ulid| {
                let ms_i64 = i64::try_from(ulid.timestamp_ms()).unwrap_or(i64::MAX);
                chrono::DateTime::from_timestamp_millis(ms_i64)
                    .unwrap_or_else(Utc::now)
                    .format("%Y-%m-%d")
                    .to_string()
            },
        );

        let path = format!("ledger/orchestration/{}/{}.json", date, event.event_id);
        tracing::Span::current().record("path", tracing::field::display(&path));

        let json = serde_json::to_string(&event).map_err(|e| Error::Serialization {
            message: format!("failed to serialize orchestration event: {e}"),
        })?;

        let result = self
            .storage
            .put_raw(&path, Bytes::from(json), WritePrecondition::DoesNotExist)
            .await?;

        match result {
            WriteResult::Success { .. } => {
                tracing::debug!("orchestration event written");
                Ok(())
            }
            WriteResult::PreconditionFailed { .. } => {
                tracing::debug!("duplicate orchestration event delivery - no-op");
                Ok(())
            }
        }
    }

    /// Appends a batch of events to the ledger.
    ///
    /// Writes are performed with bounded concurrency for throughput.
    ///
    /// # Errors
    ///
    /// Returns the first error encountered while writing the batch.
    #[tracing::instrument(skip(self, events))]
    pub async fn append_all(
        &self,
        events: impl IntoIterator<Item = OrchestrationEvent>,
    ) -> Result<()> {
        const MAX_IN_FLIGHT_WRITES: usize = 16;

        let writer = self.clone();
        stream::iter(events.into_iter().map(Ok))
            .try_for_each_concurrent(MAX_IN_FLIGHT_WRITES, move |event| {
                let writer = writer.clone();
                async move { writer.append(event).await }
            })
            .await
    }

    /// Returns the path where an event would be written.
    ///
    /// Useful for testing and debugging.
    #[must_use]
    pub fn event_path(event: &OrchestrationEvent) -> String {
        let date = Ulid::from_string(&event.event_id).map_or_else(
            |_| event.timestamp.format("%Y-%m-%d").to_string(),
            |ulid| {
                let ms_i64 = i64::try_from(ulid.timestamp_ms()).unwrap_or(i64::MAX);
                chrono::DateTime::from_timestamp_millis(ms_i64)
                    .unwrap_or_else(Utc::now)
                    .format("%Y-%m-%d")
                    .to_string()
            },
        );
        format!("ledger/orchestration/{}/{}.json", date, event.event_id)
    }
}

impl OrchestrationLedgerWriter for LedgerWriter {
    async fn write_event(&self, event: &OrchestrationEvent) -> std::result::Result<(), String> {
        self.append(event.clone()).await.map_err(|e| format!("{e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::events::{OrchestrationEventData, TriggerInfo};
    use arco_core::MemoryBackend;
    use std::sync::Arc;

    fn make_test_event(tenant: &str, workspace: &str) -> OrchestrationEvent {
        OrchestrationEvent::new(
            tenant,
            workspace,
            OrchestrationEventData::RunTriggered {
                run_id: "run_01".to_string(),
                plan_id: "plan_01".to_string(),
                trigger: TriggerInfo::Manual {
                    user_id: "user@example.com".to_string(),
                },
                root_assets: vec!["analytics.daily_report".to_string()],
                run_key: None,
                labels: std::collections::HashMap::new(),
                code_version: None,
            },
        )
    }

    #[tokio::test]
    async fn writes_event_to_ledger_path() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace")?;
        let writer = LedgerWriter::new(storage.clone());

        let event = make_test_event("tenant", "workspace");
        let expected_path = LedgerWriter::event_path(&event);

        writer.append(event.clone()).await?;

        // Verify the event was written
        let data = storage.get_raw(&expected_path).await?;
        let parsed: OrchestrationEvent =
            serde_json::from_slice(&data).map_err(|e| Error::Serialization {
                message: format!("failed to parse stored event: {e}"),
            })?;

        assert_eq!(parsed.event_id, event.event_id);
        assert_eq!(parsed.event_type, "RunTriggered");
        assert_eq!(parsed.tenant_id, "tenant");
        assert_eq!(parsed.workspace_id, "workspace");

        Ok(())
    }

    #[tokio::test]
    async fn duplicate_writes_are_idempotent() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
        let writer = LedgerWriter::new(storage);

        let event = make_test_event("tenant", "workspace");

        // First write should succeed
        writer.append(event.clone()).await?;

        // Duplicate write should also succeed (no-op)
        writer.append(event).await?;

        Ok(())
    }

    #[tokio::test]
    async fn batch_writes_multiple_events() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
        let writer = LedgerWriter::new(storage);

        let events: Vec<_> = (0..5)
            .map(|_| make_test_event("tenant", "workspace"))
            .collect();

        writer.append_all(events.clone()).await?;

        Ok(())
    }
}
