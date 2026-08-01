//! Phase 5 projection-outbox acknowledgement domain.
//!
//! INTERNAL / OPERATOR-ONLY WRITE SURFACE (roadmap Phase 5: "write APIs behind
//! internal or operator-only access"). Nothing here is a public compatibility
//! API: the writer and worker are exposed so internal services (the compactor's
//! operator endpoints) can construct them, not for tenant-facing routes.
//!
//! The domain records durable acknowledgements for consumed projection outbox
//! records. Acknowledgements carry the source record's `origin_sequence`, so
//! the latest acknowledged sequence per consumer IS the projection watermark —
//! no synthetic caller-supplied watermark is involved:
//!
//! - [`ProjectionOutboxAckWriter::latest_projected_sequence`] derives the
//!   watermark from committed ack records (pure KV scan of replayed state; no
//!   object-store listing).
//! - [`ProjectionOutboxWorker`] drains a source domain's outbox through a
//!   handler, acknowledges processed records, reports freshness/backlog, and
//!   trims fully-acknowledged records from the source domain so outbox bytes
//!   stop accumulating through state snapshots. Unacknowledged records are
//!   never trimmed.
//!
//! Metric names reserved for the deployed wiring (emitters live with the
//! operator endpoints): `arco_control_store_outbox_backlog_records`,
//! `arco_control_store_outbox_watermark_lag_sequences`,
//! `arco_control_store_outbox_drained_records_total`,
//! `arco_control_store_outbox_trimmed_records_total`.

use arco_core::ScopedStorage;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateTxn, ControlMvpProjectionOutboxRecord,
    ControlMvpStateStore, StateScope, StateToken, TxnOptions,
};
use crate::error::{CatalogError, Result};

/// State-store domain reserved for projection outbox acknowledgements.
pub const PROJECTION_OUTBOX_ACK_DOMAIN: &str = "projection-outbox-acks";

/// Internal/operator-only writer for projection outbox acknowledgements.
#[derive(Clone)]
pub struct ProjectionOutboxAckWriter {
    store: ControlMvpStateStore,
    scope: StateScope,
}

impl ProjectionOutboxAckWriter {
    /// Creates a writer bound to the acknowledgement domain.
    ///
    /// # Errors
    ///
    /// Returns a validation error when the scope names any other domain.
    pub fn new(storage: ScopedStorage, scope: StateScope) -> Result<Self> {
        if scope.domain() != PROJECTION_OUTBOX_ACK_DOMAIN {
            return Err(validation_failed(format!(
                "projection outbox acknowledgements require domain {PROJECTION_OUTBOX_ACK_DOMAIN}"
            )));
        }
        let store = ControlMvpStateStore::new(storage, scope.clone())?;
        Ok(Self { store, scope })
    }

    /// Durably acknowledges one consumed outbox record.
    ///
    /// Idempotent per `(consumer_id, record_id, source_sequence)`; a duplicate
    /// acknowledgement returns the existing committed token without a new
    /// sequence.
    ///
    /// # Errors
    ///
    /// Returns storage/CAS errors, or an invariant violation when the same
    /// `(consumer_id, record_id)` was acknowledged with a different source
    /// sequence.
    pub async fn acknowledge(
        &self,
        write: ProjectionOutboxAckWrite,
    ) -> Result<ProjectionOutboxAckReceipt> {
        let record = ProjectionOutboxAckRecord::from(write);
        let key = ack_key(record.consumer_id(), record.record_id());
        if let Some(receipt) = self.existing_receipt_for(&key, &record).await? {
            return Ok(receipt);
        }

        let mut txn = self
            .store
            .begin_control_txn(TxnOptions::new(Some(self.scope.clone())))
            .await?;
        txn.assert_absent(&key).await?;
        txn.put(&key, encode_ack_record(&record)?).await?;
        match txn.commit().await {
            Ok(token) => Ok(ProjectionOutboxAckReceipt { token, record }),
            Err(CatalogError::CasFailed { .. }) => {
                self.existing_receipt_for(&key, &record).await?.map_or_else(
                    || {
                        Err(CatalogError::CasFailed {
                            message: "projection outbox ack pointer CAS lost without a visible ack"
                                .to_string(),
                        })
                    },
                    Ok,
                )
            }
            Err(error) => Err(error),
        }
    }

    /// Reads a committed acknowledgement pinned at a state token.
    ///
    /// # Errors
    ///
    /// Returns storage errors or corrupt-artifact failures.
    pub async fn read_ack_at(
        &self,
        token: StateToken,
        consumer_id: &str,
        record_id: &str,
    ) -> Result<Option<ProjectionOutboxAckRecord>> {
        let key = ack_key(consumer_id, record_id);
        let reader = self.store.read_at(token).await?;
        let Some(bytes) = reader.get(&key).await? else {
            return Ok(None);
        };
        decode_ack_record(&bytes).map(Some)
    }

    /// Reads a committed acknowledgement, reporting retained-token loss as a
    /// typed status instead of an error.
    ///
    /// # Errors
    ///
    /// Returns storage errors other than a missing retained manifest.
    pub async fn read_ack_at_status(
        &self,
        token: StateToken,
        consumer_id: &str,
        record_id: &str,
    ) -> Result<ProjectionOutboxAckReadStatus> {
        let key = ack_key(consumer_id, record_id);
        let reader = match self.store.read_at(token.clone()).await {
            Ok(reader) => reader,
            Err(CatalogError::NotFound { .. }) => {
                return Ok(ProjectionOutboxAckReadStatus::TokenUnavailable {
                    manifest_id: token.authority_manifest_id().to_string(),
                    logical_sequence: token.logical_sequence(),
                });
            }
            Err(error) => return Err(error),
        };
        let Some(bytes) = reader.get(&key).await? else {
            return Ok(ProjectionOutboxAckReadStatus::Available(None));
        };
        decode_ack_record(&bytes)
            .map(|record| ProjectionOutboxAckReadStatus::Available(Some(record)))
    }

    /// Derives the projection watermark for a consumer from committed
    /// acknowledgements: the highest acknowledged source sequence, or `None`
    /// when the consumer has never acknowledged anything.
    ///
    /// This is a pure KV scan over replayed state — no object-store listing.
    ///
    /// # Errors
    ///
    /// Returns storage errors or corrupt-artifact failures.
    pub async fn latest_projected_sequence(&self, consumer_id: &str) -> Result<Option<u64>> {
        let prefix = consumer_prefix(consumer_id);
        let mut latest = None;
        for entry in self.store.scan_prefix(&prefix).await? {
            let record = decode_ack_record(entry.value().bytes())?;
            if latest.is_none_or(|current| record.source_sequence > current) {
                latest = Some(record.source_sequence);
            }
        }
        Ok(latest)
    }

    /// Returns all acknowledged record ids for a consumer.
    ///
    /// # Errors
    ///
    /// Returns storage errors or corrupt-artifact failures.
    pub async fn acknowledged_record_ids(&self, consumer_id: &str) -> Result<Vec<String>> {
        let prefix = consumer_prefix(consumer_id);
        let mut ids = Vec::new();
        for entry in self.store.scan_prefix(&prefix).await? {
            ids.push(decode_ack_record(entry.value().bytes())?.record_id);
        }
        Ok(ids)
    }

    /// Reports projection freshness for a source token against a consumer's
    /// derived watermark.
    ///
    /// # Errors
    ///
    /// Returns storage errors or corrupt-artifact failures.
    pub async fn projection_freshness(
        &self,
        source_token: &StateToken,
        consumer_id: &str,
    ) -> Result<ProjectionOutboxAckFreshness> {
        let latest = self.latest_projected_sequence(consumer_id).await?;
        Ok(Self::projection_freshness_for(source_token, latest))
    }

    /// Reports committed-vs-projected watermark lag for a source token against
    /// a consumer's derived watermark.
    ///
    /// # Errors
    ///
    /// Returns storage errors or corrupt-artifact failures.
    pub async fn projection_watermark_lag(
        &self,
        source_token: &StateToken,
        consumer_id: &str,
    ) -> Result<ProjectionOutboxAckWatermarkLag> {
        let latest = self.latest_projected_sequence(consumer_id).await?;
        Ok(Self::projection_watermark_lag_for(source_token, latest))
    }

    /// Pure freshness arithmetic over an explicit projected sequence.
    #[must_use]
    pub fn projection_freshness_for(
        token: &StateToken,
        latest_projected_sequence: Option<u64>,
    ) -> ProjectionOutboxAckFreshness {
        let committed_sequence = token.logical_sequence();
        let Some(latest_projected_sequence) = latest_projected_sequence else {
            return ProjectionOutboxAckFreshness::ProjectionUnavailable;
        };

        if latest_projected_sequence >= committed_sequence {
            ProjectionOutboxAckFreshness::Current {
                committed_sequence,
                latest_projected_sequence,
            }
        } else {
            ProjectionOutboxAckFreshness::StaleProjection {
                committed_sequence,
                latest_projected_sequence,
            }
        }
    }

    /// Pure watermark-lag arithmetic over an explicit projected sequence.
    #[must_use]
    pub fn projection_watermark_lag_for(
        token: &StateToken,
        latest_projected_sequence: Option<u64>,
    ) -> ProjectionOutboxAckWatermarkLag {
        let committed_sequence = token.logical_sequence();
        ProjectionOutboxAckWatermarkLag {
            committed_sequence,
            latest_projected_sequence,
            pending_sequences: latest_projected_sequence
                .map(|projected| committed_sequence.saturating_sub(projected)),
        }
    }

    async fn existing_receipt_for(
        &self,
        key: &[u8],
        expected: &ProjectionOutboxAckRecord,
    ) -> Result<Option<ProjectionOutboxAckReceipt>> {
        let Some(bytes) = self.store.get(key).await? else {
            return Ok(None);
        };
        let record = decode_ack_record(&bytes)?;
        if &record != expected {
            return Err(invariant_violation(
                "projection outbox ack key resolved to a different record",
            ));
        }
        let token = self.store.current_state_token().await?;
        Ok(Some(ProjectionOutboxAckReceipt { token, record }))
    }
}

/// Acknowledgement request for one consumed outbox record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionOutboxAckWrite {
    consumer_id: String,
    record_id: String,
    source_sequence: u64,
}

impl ProjectionOutboxAckWrite {
    /// Creates an acknowledgement for a record produced at `source_sequence`
    /// in the source domain.
    #[must_use]
    pub fn new(
        consumer_id: impl Into<String>,
        record_id: impl Into<String>,
        source_sequence: u64,
    ) -> Self {
        Self {
            consumer_id: consumer_id.into(),
            record_id: record_id.into(),
            source_sequence,
        }
    }
}

/// Committed acknowledgement record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionOutboxAckRecord {
    consumer_id: String,
    record_id: String,
    source_sequence: u64,
}

impl ProjectionOutboxAckRecord {
    /// Creates an acknowledgement record.
    #[must_use]
    pub fn new(
        consumer_id: impl Into<String>,
        record_id: impl Into<String>,
        source_sequence: u64,
    ) -> Self {
        Self {
            consumer_id: consumer_id.into(),
            record_id: record_id.into(),
            source_sequence,
        }
    }

    /// Returns the acknowledging consumer id.
    #[must_use]
    pub fn consumer_id(&self) -> &str {
        &self.consumer_id
    }

    /// Returns the acknowledged outbox record id.
    #[must_use]
    pub fn record_id(&self) -> &str {
        &self.record_id
    }

    /// Returns the source-domain logical sequence the record originated from.
    #[must_use]
    pub const fn source_sequence(&self) -> u64 {
        self.source_sequence
    }
}

impl From<ProjectionOutboxAckWrite> for ProjectionOutboxAckRecord {
    fn from(value: ProjectionOutboxAckWrite) -> Self {
        Self {
            consumer_id: value.consumer_id,
            record_id: value.record_id,
            source_sequence: value.source_sequence,
        }
    }
}

/// Receipt for a committed acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionOutboxAckReceipt {
    token: StateToken,
    record: ProjectionOutboxAckRecord,
}

impl ProjectionOutboxAckReceipt {
    /// Returns the state token pinning the acknowledgement.
    #[must_use]
    pub const fn token(&self) -> &StateToken {
        &self.token
    }

    /// Returns the committed acknowledgement record.
    #[must_use]
    pub const fn record(&self) -> &ProjectionOutboxAckRecord {
        &self.record
    }
}

/// Token-pinned acknowledgement read status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionOutboxAckReadStatus {
    /// The retained state was readable; the acknowledgement may be absent.
    Available(Option<ProjectionOutboxAckRecord>),
    /// The token's retained manifest is no longer available.
    TokenUnavailable {
        /// Manifest the token pinned.
        manifest_id: String,
        /// Logical sequence the token pinned.
        logical_sequence: u64,
    },
}

/// Diagnostic projection freshness relative to committed authority state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionOutboxAckFreshness {
    /// The projection covers the committed sequence.
    Current {
        /// Committed authority sequence.
        committed_sequence: u64,
        /// Latest acknowledged source sequence.
        latest_projected_sequence: u64,
    },
    /// The projection lags the committed sequence.
    StaleProjection {
        /// Committed authority sequence.
        committed_sequence: u64,
        /// Latest acknowledged source sequence.
        latest_projected_sequence: u64,
    },
    /// The consumer has never acknowledged anything.
    ProjectionUnavailable,
}

/// Committed-vs-projected watermark lag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionOutboxAckWatermarkLag {
    /// Committed authority sequence.
    pub committed_sequence: u64,
    /// Latest acknowledged source sequence, when any exists.
    pub latest_projected_sequence: Option<u64>,
    /// Sequences committed but not yet acknowledged.
    pub pending_sequences: Option<u64>,
}

/// Processes one outbox record during a drain pass.
#[async_trait]
pub trait ProjectionOutboxHandler: Send + Sync {
    /// Processes a record; an error aborts the drain before acknowledgement,
    /// so the record remains pending.
    async fn process(&self, record: &ControlMvpProjectionOutboxRecord) -> Result<()>;
}

/// Drain-only handler that performs no projection work.
///
/// OPERATOR TOOL: acknowledges records for backlog/watermark management
/// without materializing any projection. It must never be wired as a default
/// consumer — acknowledging implies the record no longer needs processing.
#[derive(Debug, Clone, Copy, Default)]
pub struct AckOnlyProjectionHandler;

#[async_trait]
impl ProjectionOutboxHandler for AckOnlyProjectionHandler {
    async fn process(&self, _record: &ControlMvpProjectionOutboxRecord) -> Result<()> {
        Ok(())
    }
}

/// Backlog summary for a source domain and consumer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectionOutboxBacklog {
    /// Committed source-domain sequence, when the domain has any state.
    pub committed_sequence: Option<u64>,
    /// Latest acknowledged source sequence for the consumer.
    pub latest_projected_sequence: Option<u64>,
    /// Outbox record ids not yet acknowledged, in replay order.
    pub pending_record_ids: Vec<String>,
}

/// Result of one drain pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectionOutboxDrainReport {
    /// Records processed and acknowledged by this pass.
    pub drained_record_ids: Vec<String>,
    /// Records skipped because they were already acknowledged.
    pub already_acknowledged: usize,
    /// Watermark after the pass.
    pub latest_projected_sequence: Option<u64>,
}

/// Result of one trim pass against the source domain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectionOutboxTrimReport {
    /// Record ids removed from the source domain's replayed outbox.
    pub trimmed_record_ids: Vec<String>,
    /// Source-domain sequence of the trim commit, when one was made.
    pub trim_sequence: Option<u64>,
}

/// Internal/operator-only consumer harness for a source domain's outbox.
///
/// Drains records through a handler, acknowledges them durably, and trims
/// fully-acknowledged records so outbox bytes stop accumulating through
/// state snapshots.
///
/// Trim commits are store-maintenance writes to the source domain executed
/// through the same pointer-CAS publish protocol as the domain writer;
/// contention resolves by CAS and the trim retries on the next pass.
pub struct ProjectionOutboxWorker {
    source: ControlMvpStateStore,
    source_scope: StateScope,
    acks: ProjectionOutboxAckWriter,
    consumer_id: String,
}

impl ProjectionOutboxWorker {
    /// Creates a worker for one source domain and consumer identity.
    ///
    /// # Errors
    ///
    /// Returns a validation error when the source domain is the ack domain
    /// itself or store construction fails.
    pub fn new(
        storage: ScopedStorage,
        source_domain: &str,
        consumer_id: impl Into<String>,
    ) -> Result<Self> {
        if source_domain == PROJECTION_OUTBOX_ACK_DOMAIN {
            return Err(validation_failed(
                "projection outbox worker source domain must not be the acknowledgement domain"
                    .to_string(),
            ));
        }
        let source_scope =
            StateScope::new(storage.tenant_id(), storage.workspace_id(), source_domain);
        let ack_scope = StateScope::new(
            storage.tenant_id(),
            storage.workspace_id(),
            PROJECTION_OUTBOX_ACK_DOMAIN,
        );
        let source = ControlMvpStateStore::new(storage.clone(), source_scope.clone())?;
        let acks = ProjectionOutboxAckWriter::new(storage, ack_scope)?;
        Ok(Self {
            source,
            source_scope,
            acks,
            consumer_id: consumer_id.into(),
        })
    }

    /// Returns the acknowledgement writer bound to this worker.
    #[must_use]
    pub const fn acks(&self) -> &ProjectionOutboxAckWriter {
        &self.acks
    }

    /// Reports the current backlog for this consumer.
    ///
    /// # Errors
    ///
    /// Returns storage errors or corrupt-artifact failures.
    pub async fn backlog(&self) -> Result<ProjectionOutboxBacklog> {
        let outbox = self.source.current_projection_outbox().await?;
        let committed_sequence = self.source_committed_sequence().await?;
        let acked = self.acks.acknowledged_record_ids(&self.consumer_id).await?;
        let pending_record_ids = outbox
            .iter()
            .filter(|record| !acked.contains(&record.record_id().to_string()))
            .map(|record| record.record_id().to_string())
            .collect();
        Ok(ProjectionOutboxBacklog {
            committed_sequence,
            latest_projected_sequence: self
                .acks
                .latest_projected_sequence(&self.consumer_id)
                .await?,
            pending_record_ids,
        })
    }

    /// Drains unacknowledged records through the handler in replay order.
    ///
    /// Each record is processed before it is acknowledged; a handler error
    /// aborts the pass with earlier acknowledgements already durable, so a
    /// retry resumes exactly where processing stopped.
    ///
    /// # Errors
    ///
    /// Returns handler, storage, or CAS errors.
    pub async fn drain(
        &self,
        handler: &dyn ProjectionOutboxHandler,
    ) -> Result<ProjectionOutboxDrainReport> {
        let outbox = self.source.current_projection_outbox().await?;
        let acked = self.acks.acknowledged_record_ids(&self.consumer_id).await?;
        let mut drained_record_ids = Vec::new();
        let mut already_acknowledged = 0usize;
        for record in &outbox {
            if acked.contains(&record.record_id().to_string()) {
                already_acknowledged += 1;
                continue;
            }
            let origin_sequence = record.origin_sequence().ok_or_else(|| {
                invariant_violation(
                    "projection outbox record read from state carries no origin sequence",
                )
            })?;
            handler.process(record).await?;
            self.acks
                .acknowledge(ProjectionOutboxAckWrite::new(
                    self.consumer_id.clone(),
                    record.record_id().to_string(),
                    origin_sequence,
                ))
                .await?;
            drained_record_ids.push(record.record_id().to_string());
        }
        Ok(ProjectionOutboxDrainReport {
            drained_record_ids,
            already_acknowledged,
            latest_projected_sequence: self
                .acks
                .latest_projected_sequence(&self.consumer_id)
                .await?,
        })
    }

    /// Trims acknowledged records from the source domain's replayed outbox.
    ///
    /// Only records this consumer has durably acknowledged are trimmed;
    /// unacknowledged records are never staged. Token-pinned reads of retained
    /// history continue to observe trimmed records.
    ///
    /// # Errors
    ///
    /// Returns storage or CAS errors.
    pub async fn trim_acked(&self) -> Result<ProjectionOutboxTrimReport> {
        let outbox = self.source.current_projection_outbox().await?;
        let acked = self.acks.acknowledged_record_ids(&self.consumer_id).await?;
        let trimmed_record_ids: Vec<String> = outbox
            .iter()
            .filter(|record| acked.contains(&record.record_id().to_string()))
            .map(|record| record.record_id().to_string())
            .collect();
        if trimmed_record_ids.is_empty() {
            return Ok(ProjectionOutboxTrimReport {
                trimmed_record_ids,
                trim_sequence: None,
            });
        }
        let mut txn = self
            .source
            .begin_control_txn(TxnOptions::new(Some(self.source_scope.clone())))
            .await?;
        txn.trim_projection_outbox(trimmed_record_ids.clone())?;
        let token = txn.commit().await?;
        Ok(ProjectionOutboxTrimReport {
            trimmed_record_ids,
            trim_sequence: Some(token.logical_sequence()),
        })
    }

    /// Reports freshness of this consumer's projection against the source
    /// domain's committed state.
    ///
    /// # Errors
    ///
    /// Returns storage errors or corrupt-artifact failures.
    pub async fn freshness(&self) -> Result<ProjectionOutboxAckFreshness> {
        let Some(committed_sequence) = self.source_committed_sequence().await? else {
            return Ok(ProjectionOutboxAckFreshness::ProjectionUnavailable);
        };
        let latest = self
            .acks
            .latest_projected_sequence(&self.consumer_id)
            .await?;
        let Some(latest_projected_sequence) = latest else {
            return Ok(ProjectionOutboxAckFreshness::ProjectionUnavailable);
        };
        if latest_projected_sequence >= committed_sequence {
            Ok(ProjectionOutboxAckFreshness::Current {
                committed_sequence,
                latest_projected_sequence,
            })
        } else {
            Ok(ProjectionOutboxAckFreshness::StaleProjection {
                committed_sequence,
                latest_projected_sequence,
            })
        }
    }

    async fn source_committed_sequence(&self) -> Result<Option<u64>> {
        match self.source.current_state_token().await {
            Ok(token) => Ok(Some(token.logical_sequence())),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

fn consumer_prefix(consumer_id: &str) -> Vec<u8> {
    let mut key = b"projection-outbox-acks/ack/".to_vec();
    push_length_prefixed(&mut key, consumer_id.as_bytes());
    key.push(b'/');
    key
}

fn ack_key(consumer_id: &str, record_id: &str) -> Vec<u8> {
    let mut key = consumer_prefix(consumer_id);
    push_length_prefixed(&mut key, record_id.as_bytes());
    key
}

fn push_length_prefixed(key: &mut Vec<u8>, value: &[u8]) {
    key.extend_from_slice(value.len().to_string().as_bytes());
    key.push(b':');
    key.extend_from_slice(value);
}

fn encode_ack_record(record: &ProjectionOutboxAckRecord) -> Result<Bytes> {
    serde_json::to_vec(record)
        .map(Bytes::from)
        .map_err(|error| serialization_failed(format!("projection ack record encode: {error}")))
}

fn decode_ack_record(bytes: &Bytes) -> Result<ProjectionOutboxAckRecord> {
    serde_json::from_slice(bytes)
        .map_err(|error| serialization_failed(format!("projection ack record decode: {error}")))
}

fn validation_failed(message: impl Into<String>) -> CatalogError {
    CatalogError::Validation {
        message: message.into(),
    }
}

fn serialization_failed(message: impl Into<String>) -> CatalogError {
    CatalogError::Serialization {
        message: message.into(),
    }
}

fn invariant_violation(message: impl Into<String>) -> CatalogError {
    CatalogError::InvariantViolation {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
    use arco_core::{MemoryBackend, ScopedStorage};
    use async_trait::async_trait;

    use super::*;
    use crate::error::CatalogError;
    use crate::state_store::{
        ArcoStateStore, ControlMvpPaths, CurrentStateStore, StateScope, TxnOptions,
    };

    const SOURCE_DOMAIN: &str = "phase5-source";

    fn ack_scope() -> StateScope {
        StateScope::new("tenant", "workspace", PROJECTION_OUTBOX_ACK_DOMAIN)
    }

    fn storage() -> ScopedStorage {
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scoped storage")
    }

    fn no_list_storage() -> (Arc<NoListBackend>, ScopedStorage) {
        let backend = Arc::new(NoListBackend::new(Arc::new(MemoryBackend::new())));
        let storage =
            ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scoped storage");
        (backend, storage)
    }

    fn writer(storage: ScopedStorage) -> ProjectionOutboxAckWriter {
        ProjectionOutboxAckWriter::new(storage, ack_scope()).expect("ack writer")
    }

    fn ack_write(record_id: &str, source_sequence: u64) -> ProjectionOutboxAckWrite {
        ProjectionOutboxAckWrite::new("consumer-a", record_id, source_sequence)
    }

    fn ack_record(record_id: &str, source_sequence: u64) -> ProjectionOutboxAckRecord {
        ProjectionOutboxAckRecord::new("consumer-a", record_id, source_sequence)
    }

    fn assert_unsupported<T>(result: Result<T>, expected: &str) {
        match result {
            Err(CatalogError::UnsupportedOperation { .. }) => {}
            Err(error) => panic!("expected UnsupportedOperation for {expected}, got {error:?}"),
            Ok(_) => panic!("expected UnsupportedOperation for {expected}"),
        }
    }

    async fn commit_source_record(storage: &ScopedStorage, record_id: &str) -> StateToken {
        let scope = StateScope::new("tenant", "workspace", SOURCE_DOMAIN);
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).expect("source");
        let mut txn = store
            .begin_control_txn(TxnOptions::new(Some(scope)))
            .await
            .expect("begin source txn");
        txn.put(
            format!("row/{record_id}").as_bytes(),
            Bytes::from_static(b"{}"),
        )
        .await
        .expect("stage row");
        txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            record_id.to_string(),
            Bytes::from_static(br#"{"projection":"phase5"}"#),
        ));
        txn.commit().await.expect("commit source record")
    }

    #[tokio::test]
    async fn successful_ack_write_returns_state_token() {
        let writer = writer(storage());

        let receipt = writer
            .acknowledge(ack_write("record-1", 1))
            .await
            .expect("ack write");

        assert_eq!(&ack_scope(), receipt.token().scope());
        assert_eq!(1, receipt.token().logical_sequence());
        assert!(!receipt.token().authority_manifest_id().is_empty());
        assert_eq!(&ack_record("record-1", 1), receipt.record());
    }

    #[tokio::test]
    async fn duplicate_ack_write_returns_existing_committed_token_without_new_sequence() {
        let writer = writer(storage());

        let first = writer
            .acknowledge(ack_write("record-1", 1))
            .await
            .expect("first ack");
        let duplicate = writer
            .acknowledge(ack_write("record-1", 1))
            .await
            .expect("duplicate ack is idempotent");

        assert_eq!(first.token(), duplicate.token());
        assert_eq!(first.record(), duplicate.record());
        assert_eq!(
            Some(ack_record("record-1", 1)),
            writer
                .read_ack_at(duplicate.token().clone(), "consumer-a", "record-1")
                .await
                .expect("duplicate token reads ack")
        );
    }

    #[tokio::test]
    async fn same_record_ack_with_different_source_sequence_fails_closed() {
        let writer = writer(storage());

        writer
            .acknowledge(ack_write("record-1", 1))
            .await
            .expect("first ack");
        let error = writer
            .acknowledge(ack_write("record-1", 9))
            .await
            .expect_err("conflicting source sequence must fail");

        assert!(
            matches!(error, CatalogError::InvariantViolation { .. }),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn read_ack_at_state_token_returns_committed_ack() {
        let writer = writer(storage());

        let first = writer
            .acknowledge(ack_write("record-1", 1))
            .await
            .expect("first ack");
        let second = writer
            .acknowledge(ack_write("record-2", 2))
            .await
            .expect("second ack");

        assert_eq!(
            Some(ack_record("record-1", 1)),
            writer
                .read_ack_at(first.token().clone(), "consumer-a", "record-1")
                .await
                .expect("read first token")
        );
        assert_eq!(
            None,
            writer
                .read_ack_at(first.token().clone(), "consumer-a", "record-2")
                .await
                .expect("first token does not include later ack")
        );
        assert_eq!(
            Some(ack_record("record-2", 2)),
            writer
                .read_ack_at(second.token().clone(), "consumer-a", "record-2")
                .await
                .expect("read second token")
        );
    }

    #[tokio::test]
    async fn state_token_read_status_marks_missing_retained_manifest_unavailable() {
        let storage = storage();
        let writer = writer(storage.clone());

        let receipt = writer
            .acknowledge(ack_write("record-1", 1))
            .await
            .expect("ack write");
        let token = receipt.token().clone();
        let manifest_id = token.authority_manifest_id().to_string();
        let logical_sequence = token.logical_sequence();
        let paths = ControlMvpPaths::new(PROJECTION_OUTBOX_ACK_DOMAIN);
        storage
            .delete(&paths.manifest_object(&manifest_id))
            .await
            .expect("expire retained manifest");

        assert_eq!(
            ProjectionOutboxAckReadStatus::TokenUnavailable {
                manifest_id,
                logical_sequence,
            },
            writer
                .read_ack_at_status(token, "consumer-a", "record-1")
                .await
                .expect("token status")
        );
    }

    #[tokio::test]
    async fn warm_ack_write_and_token_point_read_do_not_call_object_store_listing() {
        let (backend, storage) = no_list_storage();
        let writer = writer(storage);

        writer
            .acknowledge(ack_write("record-1", 1))
            .await
            .expect("seed ack");
        let receipt = writer
            .acknowledge(ack_write("record-2", 2))
            .await
            .expect("warm ack");

        assert_eq!(
            ProjectionOutboxAckReadStatus::Available(Some(ack_record("record-2", 2))),
            writer
                .read_ack_at_status(receipt.token().clone(), "consumer-a", "record-2")
                .await
                .expect("warm point read")
        );
        assert_eq!(0, backend.list_calls());
    }

    #[tokio::test]
    async fn bounded_replay_is_manifest_reachable_without_request_time_listing() {
        let (backend, storage) = no_list_storage();
        let writer = writer(storage);

        let first = writer
            .acknowledge(ack_write("record-1", 1))
            .await
            .expect("first ack");
        let second = writer
            .acknowledge(ack_write("record-2", 2))
            .await
            .expect("second ack");

        assert_eq!(
            ProjectionOutboxAckReadStatus::Available(Some(ack_record("record-1", 1))),
            writer
                .read_ack_at_status(first.token().clone(), "consumer-a", "record-1")
                .await
                .expect("first retained read")
        );
        assert_eq!(
            ProjectionOutboxAckReadStatus::Available(None),
            writer
                .read_ack_at_status(first.token().clone(), "consumer-a", "record-2")
                .await
                .expect("first retained read excludes later ack")
        );
        assert_eq!(
            ProjectionOutboxAckReadStatus::Available(Some(ack_record("record-2", 2))),
            writer
                .read_ack_at_status(second.token().clone(), "consumer-a", "record-2")
                .await
                .expect("second retained read")
        );
        assert_eq!(0, backend.list_calls());
    }

    #[tokio::test]
    async fn latest_projected_sequence_derives_watermark_from_committed_acks() {
        let writer = writer(storage());

        assert_eq!(
            None,
            writer
                .latest_projected_sequence("consumer-a")
                .await
                .expect("empty watermark")
        );

        writer
            .acknowledge(ack_write("record-1", 3))
            .await
            .expect("first ack");
        writer
            .acknowledge(ack_write("record-2", 7))
            .await
            .expect("second ack");
        writer
            .acknowledge(ack_write("record-3", 5))
            .await
            .expect("third ack");

        assert_eq!(
            Some(7),
            writer
                .latest_projected_sequence("consumer-a")
                .await
                .expect("derived watermark")
        );
        assert_eq!(
            None,
            writer
                .latest_projected_sequence("consumer-other")
                .await
                .expect("other consumer has no watermark")
        );
    }

    #[tokio::test]
    async fn instance_freshness_uses_derived_watermark_not_caller_input() {
        let storage = storage();
        let writer = writer(storage.clone());
        let source_token = commit_source_record(&storage, "record-1").await;

        assert_eq!(
            ProjectionOutboxAckFreshness::ProjectionUnavailable,
            writer
                .projection_freshness(&source_token, "consumer-a")
                .await
                .expect("freshness before any ack")
        );

        writer
            .acknowledge(ack_write("record-1", source_token.logical_sequence()))
            .await
            .expect("ack source record");

        assert_eq!(
            ProjectionOutboxAckFreshness::Current {
                committed_sequence: source_token.logical_sequence(),
                latest_projected_sequence: source_token.logical_sequence(),
            },
            writer
                .projection_freshness(&source_token, "consumer-a")
                .await
                .expect("freshness after ack")
        );
    }

    #[tokio::test]
    async fn projection_freshness_is_diagnostic_only_after_authority_commit() {
        let writer = writer(storage());

        let receipt = writer
            .acknowledge(ack_write("record-1", 1))
            .await
            .expect("ack commit");

        assert_eq!(
            ProjectionOutboxAckFreshness::ProjectionUnavailable,
            ProjectionOutboxAckWriter::projection_freshness_for(receipt.token(), None)
        );
        assert_eq!(
            Some(ack_record("record-1", 1)),
            writer
                .read_ack_at(receipt.token().clone(), "consumer-a", "record-1")
                .await
                .expect("committed ack remains readable")
        );
    }

    #[tokio::test]
    async fn stale_projection_watermark_status_is_visible() {
        let writer = writer(storage());

        let receipt = writer
            .acknowledge(ack_write("record-1", 1))
            .await
            .expect("ack commit");

        assert_eq!(
            ProjectionOutboxAckFreshness::StaleProjection {
                committed_sequence: receipt.token().logical_sequence(),
                latest_projected_sequence: 0,
            },
            ProjectionOutboxAckWriter::projection_freshness_for(receipt.token(), Some(0))
        );
    }

    #[tokio::test]
    async fn projection_watermark_lag_exposes_committed_and_projected_sequences() {
        let writer = writer(storage());

        let receipt = writer
            .acknowledge(ack_write("record-1", 1))
            .await
            .expect("ack commit");

        assert_eq!(
            ProjectionOutboxAckWatermarkLag {
                committed_sequence: receipt.token().logical_sequence(),
                latest_projected_sequence: Some(0),
                pending_sequences: Some(receipt.token().logical_sequence()),
            },
            ProjectionOutboxAckWriter::projection_watermark_lag_for(receipt.token(), Some(0))
        );
    }

    #[tokio::test]
    async fn current_projection_watermark_status_is_visible() {
        let writer = writer(storage());

        let receipt = writer
            .acknowledge(ack_write("record-1", 1))
            .await
            .expect("ack commit");

        assert_eq!(
            ProjectionOutboxAckFreshness::Current {
                committed_sequence: receipt.token().logical_sequence(),
                latest_projected_sequence: receipt.token().logical_sequence(),
            },
            ProjectionOutboxAckWriter::projection_freshness_for(
                receipt.token(),
                Some(receipt.token().logical_sequence())
            )
        );
    }

    #[tokio::test]
    async fn unavailable_projection_status_does_not_block_committed_writes() {
        let writer = writer(storage());

        let first = writer
            .acknowledge(ack_write("record-1", 1))
            .await
            .expect("first commit");
        let second = writer
            .acknowledge(ack_write("record-2", 2))
            .await
            .expect("second commit despite unavailable projection");

        assert_eq!(
            ProjectionOutboxAckFreshness::ProjectionUnavailable,
            ProjectionOutboxAckWriter::projection_freshness_for(first.token(), None)
        );
        assert_eq!(
            ProjectionOutboxAckWatermarkLag {
                committed_sequence: first.token().logical_sequence(),
                latest_projected_sequence: None,
                pending_sequences: None,
            },
            ProjectionOutboxAckWriter::projection_watermark_lag_for(first.token(), None)
        );
        assert_eq!(
            Some(ack_record("record-2", 2)),
            writer
                .read_ack_at(second.token().clone(), "consumer-a", "record-2")
                .await
                .expect("second committed ack")
        );
    }

    #[tokio::test]
    async fn current_state_store_rejects_selected_scope_while_control_store_accepts_it() {
        assert_unsupported(
            CurrentStateStore::new()
                .begin_txn(TxnOptions::new(Some(ack_scope())))
                .await,
            "current store selected-scope begin_txn",
        );

        let receipt = writer(storage())
            .acknowledge(ack_write("record-1", 1))
            .await
            .expect("control MVP writer accepts selected scope");

        assert_eq!(&ack_scope(), receipt.token().scope());
    }

    #[test]
    fn unsupported_domains_reject_phase5b_ack_writes() {
        for domain in [
            "catalog",
            "grants",
            "storage-governance",
            "projection-checkpoints",
            "projection-watermarks",
            "synthetic-non-selected-domain",
        ] {
            let unsupported_scope = StateScope::new("tenant", "workspace", domain);

            let error = match ProjectionOutboxAckWriter::new(storage(), unsupported_scope) {
                Err(error) => error,
                Ok(_) => panic!("unsupported scope {domain} must reject writer creation"),
            };

            assert!(
                matches!(error, CatalogError::Validation { .. }),
                "unexpected error for {domain}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn worker_drain_acknowledges_pending_records_in_order_and_is_idempotent() {
        let storage = storage();
        commit_source_record(&storage, "record-1").await;
        commit_source_record(&storage, "record-2").await;
        let worker =
            ProjectionOutboxWorker::new(storage, SOURCE_DOMAIN, "consumer-a").expect("worker");

        let report = worker
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("first drain");
        assert_eq!(
            vec!["record-1".to_string(), "record-2".to_string()],
            report.drained_record_ids
        );
        assert_eq!(0, report.already_acknowledged);
        assert_eq!(Some(2), report.latest_projected_sequence);

        let repeat = worker
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("repeat drain");
        assert!(repeat.drained_record_ids.is_empty());
        assert_eq!(2, repeat.already_acknowledged);
        assert_eq!(Some(2), repeat.latest_projected_sequence);
    }

    #[tokio::test]
    async fn worker_trim_removes_only_acknowledged_records_and_pinned_history_survives() {
        let storage = storage();
        let first_token = commit_source_record(&storage, "record-1").await;
        commit_source_record(&storage, "record-2").await;
        let worker = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
            .expect("worker");

        // Acknowledge only record-1; record-2 stays pending.
        worker
            .acks()
            .acknowledge(ProjectionOutboxAckWrite::new("consumer-a", "record-1", 1))
            .await
            .expect("ack record-1");

        let report = worker.trim_acked().await.expect("trim");
        assert_eq!(vec!["record-1".to_string()], report.trimmed_record_ids);
        assert_eq!(Some(3), report.trim_sequence);

        let scope = StateScope::new("tenant", "workspace", SOURCE_DOMAIN);
        let source = ControlMvpStateStore::new(storage, scope).expect("source store");
        let current = source
            .current_projection_outbox()
            .await
            .expect("current outbox");
        assert_eq!(1, current.len());
        assert_eq!("record-2", current[0].record_id());

        // Token-pinned read of retained history still observes the trimmed record.
        let pinned = source
            .projection_outbox_at(first_token)
            .await
            .expect("pinned outbox");
        assert_eq!(1, pinned.len());
        assert_eq!("record-1", pinned[0].record_id());

        // Nothing acknowledged remains, so a second trim is a no-op without a commit.
        let idle = worker.trim_acked().await.expect("idle trim");
        assert!(idle.trimmed_record_ids.is_empty());
        assert_eq!(None, idle.trim_sequence);
    }

    #[tokio::test]
    async fn trim_of_record_not_in_state_fails_closed() {
        let storage = storage();
        commit_source_record(&storage, "record-1").await;
        let scope = StateScope::new("tenant", "workspace", SOURCE_DOMAIN);
        let source = ControlMvpStateStore::new(storage, scope.clone()).expect("source store");
        let mut txn = source
            .begin_control_txn(TxnOptions::new(Some(scope)))
            .await
            .expect("begin");

        let error = txn
            .trim_projection_outbox(vec!["record-unknown".to_string()])
            .expect_err("trimming an unknown record must fail closed");
        assert!(
            matches!(error, CatalogError::PreconditionFailed { .. }),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn consumer_outage_degrades_freshness_and_recovery_drains_backlog() {
        // Non-vacuous Phase 5 outage evidence at the architecture's level: the
        // consumer (projection worker) is simply not running while source
        // commits continue. Commits succeed, freshness reports degrade
        // honestly, backlog is measurable, and a recovered consumer drains it.
        // Deployment-level evidence on a real provider (GCS/S3 CAS conformance,
        // #366 chain) remains outstanding and is tracked by the promotion gate.
        let storage = storage();
        let worker = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
            .expect("worker");

        // Consumer processes the first record, then "stops".
        let first = commit_source_record(&storage, "record-1").await;
        worker
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("initial drain");
        assert_eq!(
            ProjectionOutboxAckFreshness::Current {
                committed_sequence: first.logical_sequence(),
                latest_projected_sequence: first.logical_sequence(),
            },
            worker.freshness().await.expect("fresh after drain")
        );

        // Outage window: source commits keep succeeding with no consumer.
        commit_source_record(&storage, "record-2").await;
        let third = commit_source_record(&storage, "record-3").await;

        let freshness = worker.freshness().await.expect("degraded freshness");
        assert_eq!(
            ProjectionOutboxAckFreshness::StaleProjection {
                committed_sequence: third.logical_sequence(),
                latest_projected_sequence: first.logical_sequence(),
            },
            freshness
        );
        let backlog = worker.backlog().await.expect("backlog during outage");
        assert_eq!(
            vec!["record-2".to_string(), "record-3".to_string()],
            backlog.pending_record_ids
        );
        assert_eq!(Some(third.logical_sequence()), backlog.committed_sequence);

        // Recovery: the consumer drains the backlog and freshness returns.
        let recovered = worker
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("recovery drain");
        assert_eq!(
            vec!["record-2".to_string(), "record-3".to_string()],
            recovered.drained_record_ids
        );
        assert_eq!(
            ProjectionOutboxAckFreshness::Current {
                committed_sequence: third.logical_sequence(),
                latest_projected_sequence: third.logical_sequence(),
            },
            worker.freshness().await.expect("fresh after recovery")
        );
        assert!(
            worker
                .backlog()
                .await
                .expect("empty backlog")
                .pending_record_ids
                .is_empty()
        );
    }

    #[tokio::test]
    async fn worker_rejects_ack_domain_as_source() {
        let error = match ProjectionOutboxWorker::new(
            storage(),
            PROJECTION_OUTBOX_ACK_DOMAIN,
            "consumer-a",
        ) {
            Err(error) => error,
            Ok(_) => panic!("ack domain must be rejected as a worker source"),
        };
        assert!(
            matches!(error, CatalogError::Validation { .. }),
            "unexpected error: {error:?}"
        );
    }

    struct NoListBackend {
        inner: Arc<dyn StorageBackend>,
        list_calls: AtomicUsize,
    }

    impl NoListBackend {
        fn new(inner: Arc<dyn StorageBackend>) -> Self {
            Self {
                inner,
                list_calls: AtomicUsize::new(0),
            }
        }

        fn list_calls(&self) -> usize {
            self.list_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl StorageBackend for NoListBackend {
        async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::Result<WriteResult> {
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            Err(arco_core::Error::storage(format!(
                "list forbidden during projection outbox ack request path: {prefix}"
            )))
        }

        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }
}
