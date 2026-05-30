//! Native metastore ledger persistence.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use arco_core::Error;
use arco_core::ScopedStorage;
use arco_core::storage::{WritePrecondition, WriteResult};

use crate::error::{CatalogError, Result};

use super::events::MetastoreEvent;
use super::replay::{MetastoreState, replay_events};

const METASTORE_LEDGER_PREFIX: &str = "ledger/metastore/";
const METASTORE_LEDGER_SEQUENCE_PREFIX: &str = "ledger/metastore-sequences/";
const METASTORE_LEDGER_LATEST_WATERMARK: &str = "ledger/metastore-latest/watermark.json";
const METASTORE_LEDGER_PENDING_WATERMARK: &str = "ledger/metastore-latest/pending.json";

/// Latest persisted metastore ledger watermark.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetastoreLedgerWatermark {
    /// Latest metastore event ID that must be visible in a published projection.
    pub event_id: String,
    /// Latest metastore event sequence that must be visible in a published projection.
    pub sequence: u64,
}

#[derive(Debug, Clone)]
struct LatestWatermarkMarker {
    watermark: MetastoreLedgerWatermark,
    version: String,
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
    #[allow(clippy::too_many_lines)]
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

        let candidate =
            serde_json::to_vec_pretty(event).map_err(|err| CatalogError::Serialization {
                message: format!("failed to serialize metastore event: {err}"),
            })?;
        if self
            .storage
            .head_raw(&path)
            .await
            .map_err(CatalogError::from)?
            .is_some()
        {
            let existing = self
                .storage
                .get_raw(&path)
                .await
                .map_err(CatalogError::from)?;
            if existing.as_ref() == candidate.as_slice() {
                return self.update_latest_watermark(event).await;
            }
            if reserved_sequence {
                self.storage
                    .delete(&sequence_path)
                    .await
                    .map_err(CatalogError::from)?;
            }
            return Err(CatalogError::PreconditionFailed {
                message: format!(
                    "metastore event '{}' already exists with different content",
                    event.event_id
                ),
            });
        }

        self.update_pending_watermark(event).await?;
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
        }?;

        self.update_latest_watermark(event).await
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
    /// Returns an error if sequence-marker listing or validation fails.
    pub async fn latest_watermark(&self) -> Result<Option<MetastoreLedgerWatermark>> {
        let pending = self
            .read_watermark_marker(METASTORE_LEDGER_PENDING_WATERMARK)
            .await?;
        if let Some(marker) = self.read_latest_watermark_marker().await? {
            if pending
                .as_ref()
                .is_some_and(|pending| pending.watermark.sequence > marker.watermark.sequence)
            {
                return Err(CatalogError::InvariantViolation {
                    message: "metastore latest watermark update is pending".to_string(),
                });
            }
            if self
                .storage
                .head_raw(&event_path(&marker.watermark.event_id))
                .await
                .map_err(CatalogError::from)?
                .is_some()
            {
                return Ok(Some(marker.watermark));
            }
            return Err(CatalogError::InvariantViolation {
                message: format!(
                    "latest metastore watermark '{}' points at a missing event",
                    marker.watermark.event_id
                ),
            });
        }
        if pending.is_some() {
            return Err(CatalogError::InvariantViolation {
                message: "metastore latest watermark update is pending".to_string(),
            });
        }
        self.latest_watermark_from_sequence_markers().await
    }

    async fn update_pending_watermark(&self, event: &MetastoreEvent) -> Result<()> {
        self.update_watermark_marker_allowing_stale_pending_replacement(
            METASTORE_LEDGER_PENDING_WATERMARK,
            MetastoreLedgerWatermark {
                event_id: event.event_id.clone(),
                sequence: event.sequence,
            },
        )
        .await
    }

    async fn update_latest_watermark(&self, event: &MetastoreEvent) -> Result<()> {
        let current = self.read_latest_watermark_marker().await?;
        let current_sequence = current
            .as_ref()
            .map_or(0, |current| current.watermark.sequence);
        let mut upper_bound = event.sequence;
        if let Some(pending) = self
            .read_watermark_marker(METASTORE_LEDGER_PENDING_WATERMARK)
            .await?
        {
            upper_bound = upper_bound.max(pending.watermark.sequence);
        }
        if current.is_none() {
            if let Some(discovered) = self.latest_watermark_from_sequence_markers().await? {
                upper_bound = upper_bound.max(discovered.sequence);
            }
        }
        if upper_bound <= current_sequence {
            return Ok(());
        }

        let Some(candidate) = self
            .contiguous_durable_watermark_after(current_sequence, upper_bound)
            .await?
        else {
            return Err(CatalogError::InvariantViolation {
                message: "metastore latest watermark update is pending".to_string(),
            });
        };

        if candidate.sequence < upper_bound {
            self.update_watermark_marker(METASTORE_LEDGER_LATEST_WATERMARK, candidate)
                .await?;
            return Err(CatalogError::InvariantViolation {
                message: "metastore latest watermark update is pending".to_string(),
            });
        }

        self.update_watermark_marker(METASTORE_LEDGER_LATEST_WATERMARK, candidate)
            .await
    }

    async fn contiguous_durable_watermark_after(
        &self,
        current_sequence: u64,
        upper_bound: u64,
    ) -> Result<Option<MetastoreLedgerWatermark>> {
        let mut latest = None;
        for sequence in current_sequence.saturating_add(1)..=upper_bound {
            let event_id_bytes = match self.storage.get_raw(&sequence_path(sequence)).await {
                Ok(bytes) => bytes,
                Err(Error::NotFound(_)) => continue,
                Err(err) => return Err(CatalogError::from(err)),
            };
            let event_id = String::from_utf8(event_id_bytes.to_vec()).map_err(|err| {
                CatalogError::Serialization {
                    message: format!(
                        "failed to deserialize metastore sequence marker {sequence}: {err}"
                    ),
                }
            })?;
            if self
                .storage
                .head_raw(&event_path(&event_id))
                .await
                .map_err(CatalogError::from)?
                .is_none()
            {
                break;
            }
            latest = Some(MetastoreLedgerWatermark { event_id, sequence });
        }
        Ok(latest)
    }

    async fn update_watermark_marker(
        &self,
        path: &str,
        candidate: MetastoreLedgerWatermark,
    ) -> Result<()> {
        self.update_watermark_marker_inner(path, candidate, false)
            .await
    }

    async fn update_watermark_marker_allowing_stale_pending_replacement(
        &self,
        path: &str,
        candidate: MetastoreLedgerWatermark,
    ) -> Result<()> {
        self.update_watermark_marker_inner(path, candidate, true)
            .await
    }

    async fn update_watermark_marker_inner(
        &self,
        path: &str,
        candidate: MetastoreLedgerWatermark,
        allow_stale_pending_replacement: bool,
    ) -> Result<()> {
        let bytes = Bytes::from(serde_json::to_vec_pretty(&candidate).map_err(|err| {
            CatalogError::Serialization {
                message: format!("failed to serialize metastore watermark marker: {err}"),
            }
        })?);

        for _ in 0..8 {
            let current = self.read_watermark_marker(path).await?;
            if let Some(current) = current.as_ref() {
                if current.watermark.sequence > candidate.sequence
                    || (current.watermark.sequence == candidate.sequence
                        && current.watermark.event_id == candidate.event_id)
                {
                    return Ok(());
                }
                if current.watermark.sequence == candidate.sequence
                    && (!allow_stale_pending_replacement
                        || !self
                            .can_replace_stale_pending_watermark(&current.watermark, &candidate)
                            .await?)
                {
                    return Err(CatalogError::PreconditionFailed {
                        message: format!(
                            "metastore latest watermark sequence {} already points at '{}'",
                            candidate.sequence, current.watermark.event_id
                        ),
                    });
                }
            }

            let precondition = current.map_or(WritePrecondition::DoesNotExist, |current| {
                WritePrecondition::MatchesVersion(current.version)
            });
            match self
                .storage
                .put_raw(path, bytes.clone(), precondition)
                .await
                .map_err(CatalogError::from)?
            {
                WriteResult::Success { .. } => return Ok(()),
                WriteResult::PreconditionFailed { .. } => {}
            }
        }

        Err(CatalogError::CasFailed {
            message: format!("failed to update metastore watermark marker '{path}' after retries"),
        })
    }

    async fn can_replace_stale_pending_watermark(
        &self,
        current: &MetastoreLedgerWatermark,
        candidate: &MetastoreLedgerWatermark,
    ) -> Result<bool> {
        if current.sequence != candidate.sequence || current.event_id == candidate.event_id {
            return Ok(false);
        }
        if self
            .storage
            .head_raw(&event_path(&current.event_id))
            .await
            .map_err(CatalogError::from)?
            .is_some()
        {
            return Ok(false);
        }
        let event_id_bytes = match self
            .storage
            .get_raw(&sequence_path(candidate.sequence))
            .await
        {
            Ok(bytes) => bytes,
            Err(Error::NotFound(_)) => return Ok(false),
            Err(err) => return Err(CatalogError::from(err)),
        };
        Ok(event_id_bytes.as_ref() == candidate.event_id.as_bytes())
    }

    async fn read_latest_watermark_marker(&self) -> Result<Option<LatestWatermarkMarker>> {
        self.read_watermark_marker(METASTORE_LEDGER_LATEST_WATERMARK)
            .await
    }

    async fn read_watermark_marker(&self, path: &str) -> Result<Option<LatestWatermarkMarker>> {
        let Some(meta) = self
            .storage
            .head_raw(path)
            .await
            .map_err(CatalogError::from)?
        else {
            return Ok(None);
        };
        let bytes = match self.storage.get_raw(path).await {
            Ok(bytes) => bytes,
            Err(Error::NotFound(_)) => return Ok(None),
            Err(err) => return Err(CatalogError::from(err)),
        };
        let watermark =
            serde_json::from_slice::<MetastoreLedgerWatermark>(&bytes).map_err(|err| {
                CatalogError::Serialization {
                    message: format!("failed to deserialize metastore watermark marker: {err}"),
                }
            })?;
        Ok(Some(LatestWatermarkMarker {
            watermark,
            version: meta.version,
        }))
    }

    async fn latest_watermark_from_sequence_markers(
        &self,
    ) -> Result<Option<MetastoreLedgerWatermark>> {
        let mut sequences = self
            .storage
            .list(METASTORE_LEDGER_SEQUENCE_PREFIX)
            .await
            .map_err(CatalogError::from)?
            .into_iter()
            .filter_map(|path| reserved_sequence_from_path(path.as_str()))
            .collect::<Vec<_>>();
        sequences.sort_unstable_by(|left, right| right.cmp(left));

        for sequence in sequences {
            let event_id_bytes = self
                .storage
                .get_raw(&sequence_path(sequence))
                .await
                .map_err(CatalogError::from)?;
            let event_id = String::from_utf8(event_id_bytes.to_vec()).map_err(|err| {
                CatalogError::Serialization {
                    message: format!(
                        "failed to deserialize metastore sequence marker {sequence}: {err}"
                    ),
                }
            })?;
            if self
                .storage
                .head_raw(&event_path(&event_id))
                .await
                .map_err(CatalogError::from)?
                .is_some()
            {
                return Ok(Some(MetastoreLedgerWatermark { event_id, sequence }));
            }
        }

        Ok(None)
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
