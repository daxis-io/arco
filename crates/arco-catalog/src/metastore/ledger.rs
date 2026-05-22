//! Native metastore ledger persistence.

use bytes::Bytes;

use arco_core::ScopedStorage;
use arco_core::storage::{WritePrecondition, WriteResult};

use crate::error::{CatalogError, Result};

use super::events::MetastoreEvent;
use super::replay::{MetastoreState, replay_events};

const METASTORE_LEDGER_PREFIX: &str = "ledger/metastore/";
const METASTORE_LEDGER_SEQUENCE_PREFIX: &str = "ledger/metastore-sequences/";

/// Latest persisted metastore ledger watermark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetastoreLedgerWatermark {
    /// Latest metastore event ID that must be visible in a published projection.
    pub event_id: String,
    /// Latest metastore event sequence that must be visible in a published projection.
    pub sequence: u64,
}

/// Append-only native metastore ledger.
#[derive(Clone)]
pub struct MetastoreLedger {
    storage: ScopedStorage,
}

impl MetastoreLedger {
    /// Creates a metastore ledger over scoped storage.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self { storage }
    }

    /// Appends a metastore event at a deterministic event path.
    ///
    /// Duplicate event IDs are treated as idempotent delivery.
    ///
    /// # Errors
    ///
    /// Returns an error if the event is unscoped, serialization or storage
    /// fails, the same event ID already exists with different content, or the
    /// replay sequence is already reserved for a different event.
    pub async fn append_event(&self, event: &MetastoreEvent) -> Result<()> {
        self.validate_event_scope(event)?;
        let path = event_path(&event.event_id);
        let sequence_path = sequence_path(event.sequence);
        let bytes =
            serde_json::to_vec_pretty(event).map_err(|err| CatalogError::Serialization {
                message: format!("failed to serialize metastore event: {err}"),
            })?;

        let mut reserved_sequence = false;
        let sequence_result = self
            .storage
            .put_raw(
                &sequence_path,
                Bytes::copy_from_slice(event.event_id.as_bytes()),
                WritePrecondition::DoesNotExist,
            )
            .await
            .map_err(CatalogError::from)?;
        match sequence_result {
            WriteResult::Success { .. } => {
                reserved_sequence = true;
            }
            WriteResult::PreconditionFailed { .. } => {
                let existing_event_id = self
                    .storage
                    .get_raw(&sequence_path)
                    .await
                    .map_err(CatalogError::from)?;
                if existing_event_id.as_ref() != event.event_id.as_bytes() {
                    return Err(CatalogError::PreconditionFailed {
                        message: format!(
                            "metastore replay sequence {} is already reserved",
                            event.sequence
                        ),
                    });
                }
            }
        }

        let result = match self
            .storage
            .put_raw(&path, Bytes::from(bytes), WritePrecondition::DoesNotExist)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                if reserved_sequence {
                    self.storage
                        .delete(&sequence_path)
                        .await
                        .map_err(CatalogError::from)?;
                }
                return Err(CatalogError::from(err));
            }
        };

        match result {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { .. } => {
                let existing = self
                    .storage
                    .get_raw(&path)
                    .await
                    .map_err(CatalogError::from)?;
                let candidate = serde_json::to_vec_pretty(event).map_err(|err| {
                    CatalogError::Serialization {
                        message: format!("failed to serialize metastore event: {err}"),
                    }
                })?;
                if existing.as_ref() == candidate.as_slice() {
                    Ok(())
                } else {
                    if reserved_sequence {
                        self.storage
                            .delete(&sequence_path)
                            .await
                            .map_err(CatalogError::from)?;
                    }
                    Err(CatalogError::PreconditionFailed {
                        message: format!(
                            "metastore event '{}' already exists with different content",
                            event.event_id
                        ),
                    })
                }
            }
        }
    }

    /// Loads metastore events from the append-only ledger.
    ///
    /// # Errors
    ///
    /// Returns an error when listing, reading, or deserializing events fails.
    pub async fn load_events(&self) -> Result<Vec<MetastoreEvent>> {
        let mut paths = self
            .storage
            .list(METASTORE_LEDGER_PREFIX)
            .await
            .map_err(CatalogError::from)?
            .into_iter()
            .map(|path| path.to_string())
            .collect::<Vec<_>>();
        paths.sort();

        let mut events = Vec::with_capacity(paths.len());
        for path in paths {
            let bytes = self
                .storage
                .get_raw(&path)
                .await
                .map_err(CatalogError::from)?;
            let event = serde_json::from_slice::<MetastoreEvent>(&bytes).map_err(|err| {
                CatalogError::Serialization {
                    message: format!("failed to deserialize metastore event '{path}': {err}"),
                }
            })?;
            events.push(event);
        }
        Ok(events)
    }

    /// Replays all persisted metastore events into typed state.
    ///
    /// # Errors
    ///
    /// Returns an error if event loading or replay fails.
    pub async fn replay(&self) -> Result<MetastoreState> {
        let events = self.load_events().await?;
        replay_events(events.iter())
    }

    /// Returns the latest persisted metastore event that published projections
    /// must include, or `None` when the ledger is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if persisted event loading fails.
    pub async fn latest_watermark(&self) -> Result<Option<MetastoreLedgerWatermark>> {
        let latest = self
            .load_events()
            .await?
            .into_iter()
            .max_by_key(|event| event.sequence)
            .map(|event| MetastoreLedgerWatermark {
                event_id: event.event_id,
                sequence: event.sequence,
            });
        Ok(latest)
    }

    /// Returns the next sequence number after the current ledger contents.
    ///
    /// # Errors
    ///
    /// Returns an error if event loading or sequence reservation listing fails.
    pub async fn next_sequence(&self) -> Result<u64> {
        let events = self.load_events().await?;
        let event_max = events.iter().map(|event| event.sequence).max().unwrap_or(0);
        let reservation_max = self.max_reserved_sequence().await?.unwrap_or(0);
        Ok(event_max.max(reservation_max).saturating_add(1))
    }

    async fn max_reserved_sequence(&self) -> Result<Option<u64>> {
        let max = self
            .storage
            .list(METASTORE_LEDGER_SEQUENCE_PREFIX)
            .await
            .map_err(CatalogError::from)?
            .into_iter()
            .filter_map(|path| reserved_sequence_from_path(path.as_str()))
            .max();
        Ok(max)
    }

    fn validate_event_scope(&self, event: &MetastoreEvent) -> Result<()> {
        let Some(scope) = event.scope.as_ref() else {
            return Err(CatalogError::Validation {
                message: format!(
                    "metastore event '{}' is missing durable scope",
                    event.event_id
                ),
            });
        };
        if scope.tenant_id != self.storage.tenant_id() {
            return Err(CatalogError::Validation {
                message: format!(
                    "metastore event '{}' tenant scope does not match storage scope",
                    event.event_id
                ),
            });
        }
        if scope.workspace_id != self.storage.workspace_id() {
            return Err(CatalogError::Validation {
                message: format!(
                    "metastore event '{}' workspace scope does not match storage scope",
                    event.event_id
                ),
            });
        }
        Ok(())
    }
}

fn event_path(event_id: &str) -> String {
    format!("{METASTORE_LEDGER_PREFIX}{event_id}.json")
}

fn sequence_path(sequence: u64) -> String {
    format!("{METASTORE_LEDGER_SEQUENCE_PREFIX}{sequence:020}.event_id")
}

fn reserved_sequence_from_path(path: &str) -> Option<u64> {
    path.strip_prefix(METASTORE_LEDGER_SEQUENCE_PREFIX)?
        .strip_suffix(".event_id")?
        .parse::<u64>()
        .ok()
}
