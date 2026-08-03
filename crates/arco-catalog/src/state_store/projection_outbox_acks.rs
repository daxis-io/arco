//! Phase 5 projection-outbox acknowledgement domain.
//!
//! INTERNAL / OPERATOR-ONLY WRITE SURFACE (roadmap Phase 5: "write APIs behind
//! internal or operator-only access"). Nothing here is a public compatibility
//! API: the writer and worker are exposed so internal services (arco-api's
//! operator endpoints, which is the service platform IAM makes the sole
//! writer of the `state-store/` prefix) can construct them, not for
//! tenant-facing routes.
//!
//! The domain records durable acknowledgements for consumed projection outbox
//! records. Acknowledgements carry the source record's `origin_sequence`, so
//! the latest acknowledged sequence per consumer IS the projection watermark —
//! no synthetic caller-supplied watermark is involved:
//!
//! - [`ProjectionOutboxAckWriter::latest_projected_sequence`] derives the
//!   watermark from committed ack records plus the per-consumer retired
//!   watermark (pure KV reads of replayed state; no object-store listing).
//! - [`ProjectionOutboxWorker`] drains a source domain's outbox through a
//!   handler, acknowledges processed records, reports freshness/backlog, and
//!   trims fully-acknowledged records from the source domain so outbox bytes
//!   stop accumulating through state snapshots. Unacknowledged records are
//!   never trimmed.
//!
//! # Delivery identity
//!
//! Acknowledgement and trim identity is an **incarnation-aware tuple**, never
//! the reusable business record id:
//!
//! ```text
//! (consumer_id, binding_incarnation, event_id)
//! event_id = evt-{origin_sequence:020}-{record_id}
//! ```
//!
//! - `event_id` is the source record's immutable *staging* incarnation. A
//!   record id re-staged after a trim gets a strictly greater origin sequence
//!   and therefore a different event id, so an acknowledgement of the consumed
//!   incarnation can never authorize skipping or trimming the fresh one.
//! - `binding_incarnation` is the source domain's *tenure* generation. Every
//!   transfer performed by [`ProjectionOutboxWorker::rebind_consumer`] mints a
//!   new incarnation, so acknowledgements written during a previous tenure —
//!   including a previous tenure of the same consumer id — are inert and can
//!   authorize nothing.
//!
//! Both components are part of the acknowledgement key and of the record body,
//! and both are matched on drain, retirement, and trim. Trims additionally
//! validate the exact `(record_id, origin_sequence)` identity *inside* the
//! source transaction that publishes them, so a stale observation fails closed
//! with a typed precondition instead of deleting whatever record currently
//! carries the id.
//!
//! # Delivery and trim contract
//!
//! Drain delivery is **at-least-once**: handlers must be idempotent. A trim
//! retires the consumer's acknowledgements in the ack domain **before** the
//! source-domain trim commit (cross-domain ordering: ack-domain commit first,
//! source-domain commit second). A crash between the two leaves the records
//! retained with no acknowledgement, so they are re-drained instead of being
//! lost.
//!
//! Because identity is incarnation-aware, an acknowledgement that outlives its
//! trimmed record — which a same-consumer drain running concurrently with a
//! trim can still produce, in the window between ack retirement and the source
//! commit — is inert residue rather than a hazard: it names an event id that no
//! future staging can reproduce, so it can never shadow a re-staged record. No
//! durable per-domain operation lease is therefore required to make drain,
//! trim, and rebind safe to overlap; the identity itself carries the exclusion
//! that consumer-name binding alone could not. The retired watermark record
//! preserves the consumer's projection watermark across ack retirement.
//!
//! Trim authority is **single-consumer** per source domain: the first
//! successful drain or trim durably binds the consumer id (at incarnation 1) in
//! the source domain, and later drains/trims by a different consumer fail
//! closed with a typed error naming the bound consumer. Operators transfer the
//! binding deliberately with [`ProjectionOutboxWorker::rebind_consumer`].
//!
//! Metric names reserved for the deployed wiring (emitters live with the
//! operator endpoints): `arco_control_store_outbox_backlog_records`,
//! `arco_control_store_outbox_watermark_lag_sequences`,
//! `arco_control_store_outbox_drained_records_total`,
//! `arco_control_store_outbox_trimmed_records_total`.

use std::collections::BTreeSet;

use arco_core::ScopedStorage;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateTxn, ControlMvpOutboxTrimTarget,
    ControlMvpProjectionOutboxRecord, ControlMvpStateStore, StateScope, StateToken, TxnOptions,
    control_mvp_outbox_event_id,
};
use crate::error::{CatalogError, Result};

/// State-store domain reserved for projection outbox acknowledgements.
pub const PROJECTION_OUTBOX_ACK_DOMAIN: &str = "projection-outbox-acks";

/// Reserved source-domain key recording the single consumer bound to
/// drain/trim authority for that domain's projection outbox, together with the
/// immutable incarnation of that binding's tenure.
pub const PROJECTION_OUTBOX_TRIM_BINDING_KEY: &[u8] = b"projection-outbox/trim-consumer-binding";

/// Incarnation assigned to the first binding a source domain ever registers,
/// and to bindings written before incarnations existed.
const FIRST_BINDING_INCARNATION: u64 = 1;

const BINDING_REGISTRATION_ATTEMPTS: usize = 4;

/// Acknowledgement key namespace. Version 2 keys carry the binding incarnation
/// and the immutable event id; version 1 keys (record-id-only) live under a
/// different prefix, so they are never scanned, decoded, or matched by this
/// revision and cannot authorize a skip or a trim.
const ACK_KEY_NAMESPACE: &[u8] = b"projection-outbox-acks/ack/v2/";

/// Internal/operator-only writer for projection outbox acknowledgements.
#[derive(Clone)]
pub struct ProjectionOutboxAckWriter {
    store: ControlMvpStateStore,
    scope: StateScope,
    explicit_epoch: Option<u64>,
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
        Ok(Self {
            store,
            scope,
            explicit_epoch: None,
        })
    }

    /// Pins ack-domain commits to an explicit writer epoch instead of the
    /// default cooperative resolution against the published pointer epoch.
    ///
    /// # Errors
    ///
    /// Returns a validation error for an epoch the store refuses to publish
    /// under (see [`ControlMvpStateStore::with_writer_epoch`]).
    pub fn with_writer_epoch(mut self, writer_epoch: u64) -> Result<Self> {
        self.store.clone().with_writer_epoch(writer_epoch)?;
        self.explicit_epoch = Some(writer_epoch);
        Ok(self)
    }

    /// Resolves the store this writer commits through: pinned to the explicit
    /// epoch when one was configured, otherwise cooperatively adopting the
    /// currently published ack-domain epoch so the writer survives epoch
    /// claims by other writers.
    async fn writer_store(&self) -> Result<ControlMvpStateStore> {
        match self.explicit_epoch {
            Some(epoch) => self.store.clone().with_writer_epoch(epoch),
            None => self.store.clone().at_current_writer_epoch().await,
        }
    }

    /// Durably acknowledges one consumed outbox event.
    ///
    /// Idempotent per delivery identity `(consumer_id, binding_incarnation,
    /// event_id)`; a duplicate acknowledgement returns the existing committed
    /// token without a new sequence. Distinct incarnations of the same record
    /// id are distinct events and are acknowledged independently.
    ///
    /// # Errors
    ///
    /// Returns storage/CAS errors, or an invariant violation when the ack key
    /// resolves to a record that is not the acknowledgement it names.
    pub async fn acknowledge(
        &self,
        delivery: &ProjectionOutboxDeliveryId,
    ) -> Result<ProjectionOutboxAckReceipt> {
        let record = ProjectionOutboxAckRecord::from(delivery);
        let key = delivery.ack_key();
        if let Some(receipt) = self.existing_receipt_for(&key, &record).await? {
            return Ok(receipt);
        }

        let mut txn = self
            .writer_store()
            .await?
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
        delivery: &ProjectionOutboxDeliveryId,
    ) -> Result<Option<ProjectionOutboxAckRecord>> {
        let reader = self.store.read_at(token).await?;
        let Some(bytes) = reader.get(&delivery.ack_key()).await? else {
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
        delivery: &ProjectionOutboxDeliveryId,
    ) -> Result<ProjectionOutboxAckReadStatus> {
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
        let Some(bytes) = reader.get(&delivery.ack_key()).await? else {
            return Ok(ProjectionOutboxAckReadStatus::Available(None));
        };
        decode_ack_record(&bytes)
            .map(|record| ProjectionOutboxAckReadStatus::Available(Some(record)))
    }

    /// Derives the projection watermark for a consumer from committed
    /// acknowledgements and the retired-acknowledgement watermark: the
    /// highest acknowledged source sequence, or `None` when the consumer has
    /// never acknowledged anything.
    ///
    /// The scan spans every incarnation of the consumer's binding, because the
    /// watermark answers "how far has this consumer ever projected", which
    /// must never regress across a rebind. Skip and trim decisions use the
    /// incarnation-scoped [`Self::acknowledged_event_ids`] instead.
    ///
    /// Retiring acknowledgements during a trim folds their high-water source
    /// sequence into a per-consumer watermark record, so the derived
    /// watermark never regresses when consumed records are garbage-collected.
    ///
    /// This is a pure KV read over replayed state — no object-store listing.
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
        if let Some(bytes) = self.store.get(&retired_watermark_key(consumer_id)).await? {
            let watermark = decode_retired_watermark(&bytes)?;
            if latest.is_none_or(|current| watermark.latest_source_sequence > current) {
                latest = Some(watermark.latest_source_sequence);
            }
        }
        Ok(latest)
    }

    /// Durably retires acknowledgements for events being trimmed from the
    /// source domain, folding their high-water source sequence into the
    /// per-consumer retired watermark so the projection watermark survives
    /// acknowledgement garbage collection.
    ///
    /// Cross-domain ordering contract: callers MUST commit this ack-domain
    /// retirement BEFORE the source-domain trim commit. A crash between the
    /// two leaves the records retained with no acknowledgement, so they are
    /// re-drained (at-least-once) instead of being lost.
    ///
    /// Idempotent per event: already-retired acknowledgements are skipped, so
    /// a caller retry after a partial failure converges.
    ///
    /// # Errors
    ///
    /// Returns storage/CAS errors, a validation error when the deliveries do
    /// not all belong to one consumer tenure, or an invariant violation when a
    /// retained acknowledgement does not describe the event it is keyed by.
    pub async fn retire_acknowledgements(
        &self,
        deliveries: &[ProjectionOutboxDeliveryId],
    ) -> Result<Option<StateToken>> {
        let Some(first) = deliveries.first() else {
            return Ok(None);
        };
        if deliveries.iter().any(|delivery| {
            delivery.consumer_id != first.consumer_id
                || delivery.binding_incarnation != first.binding_incarnation
        }) {
            return Err(validation_failed(
                "projection outbox ack retirement must name one consumer tenure",
            ));
        }
        let mut txn = self
            .writer_store()
            .await?
            .begin_control_txn(TxnOptions::new(Some(self.scope.clone())))
            .await?;
        let watermark_key = retired_watermark_key(&first.consumer_id);
        let mut watermark = match txn.get(&watermark_key).await? {
            Some(value) => Some(decode_retired_watermark(value.bytes())?.latest_source_sequence),
            None => None,
        };
        for delivery in deliveries {
            let key = delivery.ack_key();
            // An absent acknowledgement was already retired by an earlier
            // (partially failed) pass; retirement is idempotent per event.
            if let Some(value) = txn.get(&key).await? {
                let record = decode_ack_record(value.bytes())?;
                if record.source_sequence != delivery.origin_sequence
                    || record.event_id != delivery.event_id()
                    || record.binding_incarnation != delivery.binding_incarnation
                {
                    return Err(invariant_violation(format!(
                        "projection outbox ack keyed for event {} carries event {} at source sequence {}",
                        delivery.event_id(),
                        record.event_id,
                        record.source_sequence
                    )));
                }
                txn.delete(&key).await?;
            }
            if watermark.is_none_or(|current| delivery.origin_sequence > current) {
                watermark = Some(delivery.origin_sequence);
            }
        }
        let Some(watermark) = watermark else {
            return Ok(None);
        };
        txn.put(
            &watermark_key,
            encode_retired_watermark(&ProjectionOutboxRetiredWatermark {
                consumer_id: first.consumer_id.clone(),
                latest_source_sequence: watermark,
            })?,
        )
        .await?;
        txn.commit().await.map(Some)
    }

    /// Returns the event ids this consumer has acknowledged **within one
    /// binding incarnation**.
    ///
    /// This is the set that authorizes skipping a drain and staging a trim.
    /// Acknowledgements from any other tenure are deliberately excluded: they
    /// were written under an authority that has since been transferred away.
    ///
    /// # Errors
    ///
    /// Returns storage errors or corrupt-artifact failures.
    pub async fn acknowledged_event_ids(
        &self,
        consumer_id: &str,
        binding_incarnation: u64,
    ) -> Result<BTreeSet<String>> {
        let prefix = incarnation_prefix(consumer_id, binding_incarnation);
        let mut events = BTreeSet::new();
        for entry in self.store.scan_prefix(&prefix).await? {
            let record = decode_ack_record(entry.value().bytes())?;
            if record.consumer_id != consumer_id
                || record.binding_incarnation != binding_incarnation
            {
                return Err(invariant_violation(
                    "projection outbox ack record does not match the tenure it is keyed under",
                ));
            }
            events.insert(record.event_id);
        }
        Ok(events)
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

/// Incarnation-aware delivery identity for one consumed outbox event.
///
/// This is the unit acknowledgements are keyed by and trims are conditional
/// on. It deliberately combines three independently reusable components into
/// one identity that is unique for all time within a domain:
///
/// - `consumer_id` — who consumed it;
/// - `binding_incarnation` — under which tenure of the source domain's
///   single-consumer binding;
/// - `(record_id, origin_sequence)` — which *staging incarnation* of the
///   business record, encoded as [`Self::event_id`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionOutboxDeliveryId {
    consumer_id: String,
    binding_incarnation: u64,
    record_id: String,
    origin_sequence: u64,
}

impl ProjectionOutboxDeliveryId {
    /// Names one delivery of one outbox event to one consumer tenure.
    #[must_use]
    pub fn new(
        consumer_id: impl Into<String>,
        binding_incarnation: u64,
        record_id: impl Into<String>,
        origin_sequence: u64,
    ) -> Self {
        Self {
            consumer_id: consumer_id.into(),
            binding_incarnation,
            record_id: record_id.into(),
            origin_sequence,
        }
    }

    /// Returns the consuming consumer id.
    #[must_use]
    pub fn consumer_id(&self) -> &str {
        &self.consumer_id
    }

    /// Returns the source-domain binding tenure this delivery belongs to.
    #[must_use]
    pub const fn binding_incarnation(&self) -> u64 {
        self.binding_incarnation
    }

    /// Returns the business record id.
    #[must_use]
    pub fn record_id(&self) -> &str {
        &self.record_id
    }

    /// Returns the source-domain sequence that staged this event.
    #[must_use]
    pub const fn origin_sequence(&self) -> u64 {
        self.origin_sequence
    }

    /// Returns the immutable outbox-event id this delivery names.
    #[must_use]
    pub fn event_id(&self) -> String {
        control_mvp_outbox_event_id(self.origin_sequence, &self.record_id)
    }

    /// Returns the source-domain trim target naming exactly this event.
    #[must_use]
    pub fn trim_target(&self) -> ControlMvpOutboxTrimTarget {
        ControlMvpOutboxTrimTarget::new(self.record_id.clone(), self.origin_sequence)
    }

    fn ack_key(&self) -> Vec<u8> {
        let mut key = incarnation_prefix(&self.consumer_id, self.binding_incarnation);
        push_length_prefixed(&mut key, self.event_id().as_bytes());
        key
    }
}

/// Committed acknowledgement record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionOutboxAckRecord {
    consumer_id: String,
    binding_incarnation: u64,
    record_id: String,
    event_id: String,
    source_sequence: u64,
}

impl ProjectionOutboxAckRecord {
    /// Returns the acknowledging consumer id.
    #[must_use]
    pub fn consumer_id(&self) -> &str {
        &self.consumer_id
    }

    /// Returns the source-domain binding tenure that acknowledged the event.
    #[must_use]
    pub const fn binding_incarnation(&self) -> u64 {
        self.binding_incarnation
    }

    /// Returns the acknowledged outbox record id.
    #[must_use]
    pub fn record_id(&self) -> &str {
        &self.record_id
    }

    /// Returns the acknowledged immutable outbox-event id.
    #[must_use]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    /// Returns the source-domain logical sequence the record originated from.
    #[must_use]
    pub const fn source_sequence(&self) -> u64 {
        self.source_sequence
    }
}

impl From<&ProjectionOutboxDeliveryId> for ProjectionOutboxAckRecord {
    fn from(value: &ProjectionOutboxDeliveryId) -> Self {
        Self {
            consumer_id: value.consumer_id.clone(),
            binding_incarnation: value.binding_incarnation,
            record_id: value.record_id.clone(),
            event_id: value.event_id(),
            source_sequence: value.origin_sequence,
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
    /// Immutable event ids processed and acknowledged by this pass.
    pub drained_event_ids: Vec<String>,
    /// Records skipped because this tenure already acknowledged them.
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
    /// Immutable event ids removed from the source domain's replayed outbox.
    pub trimmed_event_ids: Vec<String>,
    /// Source-domain sequence of the trim commit, when one was made.
    pub trim_sequence: Option<u64>,
}

/// Result of a deliberate consumer-binding transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectionOutboxRebindReport {
    /// Consumer previously bound to the source domain, when any.
    pub previous_consumer: Option<String>,
    /// Binding incarnation in force before this call, when any.
    pub previous_incarnation: Option<u64>,
    /// Binding incarnation in force after this call.
    pub incarnation: u64,
    /// Source-domain sequence of the rebind commit, when one was made.
    pub rebind_sequence: Option<u64>,
}

/// Durable single-consumer binding stored in the source domain.
///
/// `incarnation` is the immutable generation of this binding's tenure. It is
/// minted at 1 on first registration and strictly increases on every transfer,
/// so acknowledgements written under an earlier tenure are permanently
/// unreachable from the current one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ProjectionOutboxConsumerBinding {
    consumer_id: String,
    /// Bindings written before incarnations existed carry no field; they are
    /// the domain's first tenure by definition, so they read as incarnation 1.
    #[serde(default = "first_binding_incarnation")]
    incarnation: u64,
}

const fn first_binding_incarnation() -> u64 {
    FIRST_BINDING_INCARNATION
}

/// Per-consumer watermark preserved when acknowledgements are retired.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ProjectionOutboxRetiredWatermark {
    consumer_id: String,
    latest_source_sequence: u64,
}

/// Internal/operator-only consumer harness for a source domain's outbox.
///
/// Drains records through a handler, acknowledges them durably, and trims
/// fully-acknowledged records so outbox bytes stop accumulating through
/// state snapshots.
///
/// # Delivery contract
///
/// Drain delivery is **at-least-once**; handlers must be idempotent. A trim
/// retires this consumer tenure's acknowledgements in the ack domain before
/// the source-domain trim commit, so a crash between the two commits re-drains
/// the affected records instead of losing them.
///
/// # Trim authority
///
/// Trim authority is single-consumer per source domain: the first successful
/// drain or trim registers this worker's consumer id (at incarnation 1) as a
/// KV record in the source domain, and later drains/trims by a different
/// consumer fail closed with a typed error naming the bound consumer. Use
/// [`Self::rebind_consumer`] to transfer the binding deliberately; the
/// transfer mints a new incarnation, which retires the previous tenure's
/// acknowledgement namespace without deleting anything.
///
/// # Fencing
///
/// Source- and ack-domain commits resolve their writer epoch cooperatively
/// against the published pointer epoch by default, so the worker keeps
/// functioning after another writer claims a higher epoch. Operators can pin
/// an explicit epoch with [`Self::with_writer_epoch`].
///
/// Trim commits are store-maintenance writes to the source domain executed
/// through the same pointer-CAS publish protocol as the domain writer;
/// contention resolves by CAS and the trim retries on the next pass.
pub struct ProjectionOutboxWorker {
    source: ControlMvpStateStore,
    source_scope: StateScope,
    acks: ProjectionOutboxAckWriter,
    consumer_id: String,
    explicit_epoch: Option<u64>,
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
            explicit_epoch: None,
        })
    }

    /// Pins source- and ack-domain commits to an explicit writer epoch
    /// instead of the default cooperative resolution against each domain's
    /// published pointer epoch.
    ///
    /// # Errors
    ///
    /// Returns a validation error for an epoch the store refuses to publish
    /// under (see [`ControlMvpStateStore::with_writer_epoch`]).
    pub fn with_writer_epoch(mut self, writer_epoch: u64) -> Result<Self> {
        self.acks = self.acks.with_writer_epoch(writer_epoch)?;
        self.explicit_epoch = Some(writer_epoch);
        Ok(self)
    }

    /// Returns the acknowledgement writer bound to this worker.
    #[must_use]
    pub const fn acks(&self) -> &ProjectionOutboxAckWriter {
        &self.acks
    }

    /// Resolves the store source-domain commits go through: pinned to the
    /// explicit epoch when one was configured, otherwise cooperatively
    /// adopting the currently published source-domain epoch.
    async fn source_writer(&self) -> Result<ControlMvpStateStore> {
        match self.explicit_epoch {
            Some(epoch) => self.source.clone().with_writer_epoch(epoch),
            None => self.source.clone().at_current_writer_epoch().await,
        }
    }

    /// Returns the consumer currently bound to this source domain's
    /// drain/trim authority, when any.
    ///
    /// # Errors
    ///
    /// Returns storage errors or corrupt-artifact failures.
    pub async fn bound_consumer(&self) -> Result<Option<String>> {
        Ok(self
            .consumer_binding()
            .await?
            .map(|binding| binding.consumer_id))
    }

    /// Returns the incarnation of this source domain's current binding tenure,
    /// when the domain is bound.
    ///
    /// # Errors
    ///
    /// Returns storage errors or corrupt-artifact failures.
    pub async fn bound_incarnation(&self) -> Result<Option<u64>> {
        Ok(self
            .consumer_binding()
            .await?
            .map(|binding| binding.incarnation))
    }

    async fn consumer_binding(&self) -> Result<Option<ProjectionOutboxConsumerBinding>> {
        // A source domain with no committed state replays to the default
        // empty state, so this reads as `None` without creating anything.
        (self.source.get(PROJECTION_OUTBOX_TRIM_BINDING_KEY).await?)
            .map_or(Ok(None), |bytes| decode_binding(&bytes).map(Some))
    }

    /// Returns the tenure incarnation a read-only pass should scope
    /// acknowledgements to: the bound tenure, or the first incarnation when
    /// the domain is not bound yet (in which case nothing is acknowledged).
    async fn observed_incarnation(&self) -> Result<u64> {
        Ok(self
            .consumer_binding()
            .await?
            .map_or(FIRST_BINDING_INCARNATION, |binding| binding.incarnation))
    }

    /// Enforces the single-consumer binding for destructive operations and
    /// registers this worker's consumer id when the domain is unbound,
    /// returning the incarnation this pass operates under.
    ///
    /// Registration is skipped while the source domain has no committed
    /// state, so probing an empty (or misspelled) domain never creates one.
    async fn ensure_binding(&self) -> Result<u64> {
        if self.source_committed_sequence().await?.is_none() {
            return Ok(FIRST_BINDING_INCARNATION);
        }
        for _ in 0..BINDING_REGISTRATION_ATTEMPTS {
            match self.consumer_binding().await? {
                Some(binding) if binding.consumer_id == self.consumer_id => {
                    return Ok(binding.incarnation);
                }
                Some(binding) => {
                    return Err(trim_consumer_conflict(
                        self.source_scope.domain(),
                        &binding.consumer_id,
                        &self.consumer_id,
                    ));
                }
                None => {}
            }
            let mut txn = self
                .source_writer()
                .await?
                .begin_control_txn(TxnOptions::new(Some(self.source_scope.clone())))
                .await?;
            match txn.assert_absent(PROJECTION_OUTBOX_TRIM_BINDING_KEY).await {
                Ok(()) => {}
                // Another worker registered between the read and this begin;
                // loop to re-read and compare consumer identities.
                Err(CatalogError::PreconditionFailed { .. }) => continue,
                Err(error) => return Err(error),
            }
            txn.put(
                PROJECTION_OUTBOX_TRIM_BINDING_KEY,
                encode_binding(&self.consumer_id, FIRST_BINDING_INCARNATION)?,
            )
            .await?;
            match txn.commit().await {
                Ok(_) => return Ok(FIRST_BINDING_INCARNATION),
                // Lost the pointer race; loop to re-read the (possibly
                // foreign) binding and either accept it or fail closed.
                Err(CatalogError::CasFailed { .. } | CatalogError::PreconditionFailed { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Err(CatalogError::CasFailed {
            message: "projection outbox consumer-binding registration lost repeated pointer races"
                .to_string(),
        })
    }

    /// Deliberately rebinds this source domain's drain/trim authority to this
    /// worker's consumer id, minting a new binding incarnation and reporting
    /// the previous binding.
    ///
    /// This is the operator escape hatch for the single-consumer trim
    /// semantics. Minting a new incarnation is what makes the transfer safe:
    /// every acknowledgement written by the previous tenure — including a
    /// previous tenure of the consumer being rebound *back* to — becomes
    /// unreachable, so it can neither make a drain skip a record nor authorize
    /// a trim. Nothing is deleted, so the projection watermark is preserved.
    ///
    /// # Errors
    ///
    /// Returns storage or CAS errors.
    pub async fn rebind_consumer(&self) -> Result<ProjectionOutboxRebindReport> {
        if self.source_committed_sequence().await?.is_none() {
            return Err(validation_failed(format!(
                "cannot rebind projection outbox consumer: source domain {} has no committed state",
                self.source_scope.domain()
            )));
        }
        let mut txn = self
            .source_writer()
            .await?
            .begin_control_txn(TxnOptions::new(Some(self.source_scope.clone())))
            .await?;
        let previous = match txn.get(PROJECTION_OUTBOX_TRIM_BINDING_KEY).await? {
            Some(value) => Some(decode_binding(value.bytes())?),
            None => None,
        };
        if let Some(previous) = &previous
            && previous.consumer_id == self.consumer_id
        {
            // Same consumer, same tenure: there is no authority to transfer,
            // so there is no stale acknowledgement namespace to retire.
            return Ok(ProjectionOutboxRebindReport {
                previous_consumer: Some(previous.consumer_id.clone()),
                previous_incarnation: Some(previous.incarnation),
                incarnation: previous.incarnation,
                rebind_sequence: None,
            });
        }
        let incarnation = match &previous {
            Some(previous) => previous.incarnation.checked_add(1).ok_or_else(|| {
                validation_failed("projection outbox binding incarnation overflow")
            })?,
            None => FIRST_BINDING_INCARNATION,
        };
        txn.put(
            PROJECTION_OUTBOX_TRIM_BINDING_KEY,
            encode_binding(&self.consumer_id, incarnation)?,
        )
        .await?;
        let token = txn.commit().await?;
        Ok(ProjectionOutboxRebindReport {
            previous_consumer: previous
                .as_ref()
                .map(|previous| previous.consumer_id.clone()),
            previous_incarnation: previous.as_ref().map(|previous| previous.incarnation),
            incarnation,
            rebind_sequence: Some(token.logical_sequence()),
        })
    }

    /// Reports the current backlog for this consumer's tenure.
    ///
    /// # Errors
    ///
    /// Returns storage errors or corrupt-artifact failures.
    pub async fn backlog(&self) -> Result<ProjectionOutboxBacklog> {
        let incarnation = self.observed_incarnation().await?;
        let outbox = self.source.current_projection_outbox().await?;
        let committed_sequence = self.source_committed_sequence().await?;
        let acked = self
            .acks
            .acknowledged_event_ids(&self.consumer_id, incarnation)
            .await?;
        let mut pending_record_ids = Vec::new();
        for record in &outbox {
            let event_id = Self::event_id_of(record)?;
            if !acked.contains(&event_id) {
                pending_record_ids.push(record.record_id().to_string());
            }
        }
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
    /// Delivery is at-least-once: a record whose acknowledgement was retired
    /// by an interrupted trim, or written under a superseded binding tenure,
    /// is redelivered, so handlers must be idempotent. Each record is
    /// processed before it is acknowledged; a handler error aborts the pass
    /// with earlier acknowledgements already durable, so a retry resumes
    /// exactly where processing stopped.
    ///
    /// Skips are decided by immutable event id within the current binding
    /// incarnation, never by record id, so a re-staged record id is always
    /// delivered as the fresh event it is.
    ///
    /// The first successful drain registers this worker's consumer id as the
    /// source domain's single drain/trim consumer; a drain by a different
    /// consumer fails closed naming the bound consumer.
    ///
    /// # Errors
    ///
    /// Returns handler, storage, CAS, or consumer-binding errors.
    pub async fn drain(
        &self,
        handler: &dyn ProjectionOutboxHandler,
    ) -> Result<ProjectionOutboxDrainReport> {
        let incarnation = self.ensure_binding().await?;
        let outbox = self.source.current_projection_outbox().await?;
        let acked = self
            .acks
            .acknowledged_event_ids(&self.consumer_id, incarnation)
            .await?;
        let mut drained_record_ids = Vec::new();
        let mut drained_event_ids = Vec::new();
        let mut already_acknowledged = 0usize;
        for record in &outbox {
            let event_id = Self::event_id_of(record)?;
            if acked.contains(&event_id) {
                already_acknowledged += 1;
                continue;
            }
            let origin_sequence = Self::origin_sequence_of(record)?;
            handler.process(record).await?;
            self.acks
                .acknowledge(&ProjectionOutboxDeliveryId::new(
                    self.consumer_id.clone(),
                    incarnation,
                    record.record_id().to_string(),
                    origin_sequence,
                ))
                .await?;
            drained_record_ids.push(record.record_id().to_string());
            drained_event_ids.push(event_id);
        }
        Ok(ProjectionOutboxDrainReport {
            drained_record_ids,
            drained_event_ids,
            already_acknowledged,
            latest_projected_sequence: self
                .acks
                .latest_projected_sequence(&self.consumer_id)
                .await?,
        })
    }

    /// Trims acknowledged events from the source domain's replayed outbox.
    ///
    /// Only events this consumer tenure has durably acknowledged are trimmed;
    /// unacknowledged records are never staged. Token-pinned reads of retained
    /// history continue to observe trimmed records.
    ///
    /// Cross-domain ordering: the tenure's acknowledgements for the trimmed
    /// events are retired first (ack-domain commit), then the records are
    /// trimmed (source-domain commit). A crash or CAS loss between the two
    /// leaves the records retained with no acknowledgement, so the next drain
    /// redelivers them (at-least-once) instead of losing them.
    ///
    /// The source commit is conditional on the exact event incarnations this
    /// pass observed and on the binding tenure it observed, both validated
    /// inside the transaction: a delayed pass whose observation was overtaken
    /// by a concurrent trim-and-re-stage cycle, or by a rebind, fails closed
    /// with a typed precondition instead of deleting the fresh record that
    /// inherited the id.
    ///
    /// The first successful trim registers this worker's consumer id as the
    /// source domain's single drain/trim consumer (within the trim commit
    /// itself); a trim by a different consumer fails closed naming the bound
    /// consumer.
    ///
    /// # Errors
    ///
    /// Returns storage, CAS, or consumer-binding errors.
    pub async fn trim_acked(&self) -> Result<ProjectionOutboxTrimReport> {
        let observed_binding = self.consumer_binding().await?;
        if let Some(binding) = &observed_binding
            && binding.consumer_id != self.consumer_id
        {
            return Err(trim_consumer_conflict(
                self.source_scope.domain(),
                &binding.consumer_id,
                &self.consumer_id,
            ));
        }
        let incarnation = observed_binding
            .as_ref()
            .map_or(FIRST_BINDING_INCARNATION, |binding| binding.incarnation);
        let outbox = self.source.current_projection_outbox().await?;
        let acked = self
            .acks
            .acknowledged_event_ids(&self.consumer_id, incarnation)
            .await?;
        let mut trimmed = Vec::new();
        for record in &outbox {
            let event_id = Self::event_id_of(record)?;
            if acked.contains(&event_id) {
                trimmed.push(ProjectionOutboxDeliveryId::new(
                    self.consumer_id.clone(),
                    incarnation,
                    record.record_id().to_string(),
                    Self::origin_sequence_of(record)?,
                ));
            }
        }
        let trimmed_record_ids: Vec<String> = trimmed
            .iter()
            .map(|delivery| delivery.record_id().to_string())
            .collect();
        let trimmed_event_ids: Vec<String> = trimmed
            .iter()
            .map(ProjectionOutboxDeliveryId::event_id)
            .collect();
        if trimmed.is_empty() {
            return Ok(ProjectionOutboxTrimReport {
                trimmed_record_ids,
                trimmed_event_ids,
                trim_sequence: None,
            });
        }
        // Ack-domain commit FIRST: retire the acknowledgements so no ack can
        // outlive its trimmed record (see the cross-domain ordering contract
        // in the method docs).
        self.acks.retire_acknowledgements(&trimmed).await?;
        // Source-domain commit SECOND: remove the records from replayed state.
        let mut txn = self
            .source_writer()
            .await?
            .begin_control_txn(TxnOptions::new(Some(self.source_scope.clone())))
            .await?;
        match txn.get(PROJECTION_OUTBOX_TRIM_BINDING_KEY).await? {
            Some(value) => {
                let binding = decode_binding(value.bytes())?;
                if binding.consumer_id != self.consumer_id {
                    return Err(trim_consumer_conflict(
                        self.source_scope.domain(),
                        &binding.consumer_id,
                        &self.consumer_id,
                    ));
                }
                if binding.incarnation != incarnation {
                    return Err(CatalogError::PreconditionFailed {
                        message: format!(
                            "projection outbox trim observed binding incarnation {incarnation} for \
                             consumer {} in domain {} but the committing transaction sees \
                             incarnation {}; the tenure was transferred mid-trim and the trim is \
                             refused",
                            self.consumer_id,
                            self.source_scope.domain(),
                            binding.incarnation
                        ),
                    });
                }
            }
            None => {
                txn.put(
                    PROJECTION_OUTBOX_TRIM_BINDING_KEY,
                    encode_binding(&self.consumer_id, incarnation)?,
                )
                .await?;
            }
        }
        txn.trim_projection_outbox(trimmed.iter().map(ProjectionOutboxDeliveryId::trim_target))?;
        let token = txn.commit().await?;
        Ok(ProjectionOutboxTrimReport {
            trimmed_record_ids,
            trimmed_event_ids,
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

    fn origin_sequence_of(record: &ControlMvpProjectionOutboxRecord) -> Result<u64> {
        record.origin_sequence().ok_or_else(|| {
            invariant_violation(
                "projection outbox record read from state carries no origin sequence",
            )
        })
    }

    fn event_id_of(record: &ControlMvpProjectionOutboxRecord) -> Result<String> {
        record.event_id().ok_or_else(|| {
            invariant_violation(
                "projection outbox record read from state carries no origin sequence",
            )
        })
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
    let mut key = ACK_KEY_NAMESPACE.to_vec();
    push_length_prefixed(&mut key, consumer_id.as_bytes());
    key.push(b'/');
    key
}

fn incarnation_prefix(consumer_id: &str, binding_incarnation: u64) -> Vec<u8> {
    let mut key = consumer_prefix(consumer_id);
    key.extend_from_slice(format!("{binding_incarnation:020}").as_bytes());
    key.push(b'/');
    key
}

fn retired_watermark_key(consumer_id: &str) -> Vec<u8> {
    let mut key = b"projection-outbox-acks/watermark/".to_vec();
    push_length_prefixed(&mut key, consumer_id.as_bytes());
    key
}

fn encode_binding(consumer_id: &str, incarnation: u64) -> Result<Bytes> {
    serde_json::to_vec(&ProjectionOutboxConsumerBinding {
        consumer_id: consumer_id.to_string(),
        incarnation,
    })
    .map(Bytes::from)
    .map_err(|error| serialization_failed(format!("projection consumer binding encode: {error}")))
}

fn decode_binding(bytes: &Bytes) -> Result<ProjectionOutboxConsumerBinding> {
    let binding: ProjectionOutboxConsumerBinding =
        serde_json::from_slice(bytes).map_err(|error| {
            serialization_failed(format!("projection consumer binding decode: {error}"))
        })?;
    if binding.incarnation == 0 {
        return Err(invariant_violation(
            "projection outbox consumer binding carries incarnation 0, which no tenure ever mints",
        ));
    }
    Ok(binding)
}

fn encode_retired_watermark(record: &ProjectionOutboxRetiredWatermark) -> Result<Bytes> {
    serde_json::to_vec(record)
        .map(Bytes::from)
        .map_err(|error| {
            serialization_failed(format!("projection retired watermark encode: {error}"))
        })
}

fn decode_retired_watermark(bytes: &Bytes) -> Result<ProjectionOutboxRetiredWatermark> {
    serde_json::from_slice(bytes).map_err(|error| {
        serialization_failed(format!("projection retired watermark decode: {error}"))
    })
}

fn trim_consumer_conflict(domain: &str, bound: &str, requested: &str) -> CatalogError {
    CatalogError::PreconditionFailed {
        message: format!(
            "projection outbox source domain {domain} is bound to consumer {bound}; \
             drain/trim by consumer {requested} is refused because trim authority is \
             single-consumer (a second consumer would silently lose records trimmed \
             before it acknowledged them); rebind deliberately with the force-rebind \
             option to transfer trim authority"
        ),
    }
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
// Advisory lint scope for test code (#331): the allowed pedantic/nursery
// lints conflict with test ergonomics here; production code keeps them active.
#[allow(clippy::manual_let_else)]
mod tests {
    use std::ops::Range;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
    use arco_core::{MemoryBackend, ScopedStorage};
    use async_trait::async_trait;
    use tokio::sync::oneshot;

    use super::*;
    use crate::error::CatalogError;
    use crate::state_store::{
        ArcoStateStore, ControlMvpPaths, CurrentStateStore, StateScope, TxnOptions,
    };

    const SOURCE_DOMAIN: &str = "phase5-source";
    const SOURCE_POINTER: &str = "/control-mvp/phase5-source/current.pointer.json";

    fn ack_scope() -> StateScope {
        StateScope::new("tenant", "workspace", PROJECTION_OUTBOX_ACK_DOMAIN)
    }

    fn source_scope() -> StateScope {
        StateScope::new("tenant", "workspace", SOURCE_DOMAIN)
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

    fn delivery(record_id: &str, source_sequence: u64) -> ProjectionOutboxDeliveryId {
        ProjectionOutboxDeliveryId::new(
            "consumer-a",
            FIRST_BINDING_INCARNATION,
            record_id,
            source_sequence,
        )
    }

    fn ack_record(record_id: &str, source_sequence: u64) -> ProjectionOutboxAckRecord {
        ProjectionOutboxAckRecord::from(&delivery(record_id, source_sequence))
    }

    fn assert_unsupported<T>(result: Result<T>, expected: &str) {
        match result {
            Err(CatalogError::UnsupportedOperation { .. }) => {}
            Err(error) => panic!("expected UnsupportedOperation for {expected}, got {error:?}"),
            Ok(_) => panic!("expected UnsupportedOperation for {expected}"),
        }
    }

    fn assert_precondition_failed<T: std::fmt::Debug>(result: Result<T>, needle: &str) {
        match result {
            Err(CatalogError::PreconditionFailed { message }) => assert!(
                message.contains(needle),
                "precondition message {message:?} does not mention {needle:?}"
            ),
            other => panic!("expected PreconditionFailed containing {needle:?}, got {other:?}"),
        }
    }

    async fn commit_source_record(storage: &ScopedStorage, record_id: &str) -> StateToken {
        commit_source_record_with_payload(storage, record_id, br#"{"projection":"phase5"}"#).await
    }

    async fn commit_source_record_with_payload(
        storage: &ScopedStorage,
        record_id: &str,
        payload: &'static [u8],
    ) -> StateToken {
        let scope = source_scope();
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
            Bytes::from_static(payload),
        ))
        .expect("stage outbox record");
        txn.commit().await.expect("commit source record")
    }

    async fn current_outbox(storage: &ScopedStorage) -> Vec<ControlMvpProjectionOutboxRecord> {
        ControlMvpStateStore::new(storage.clone(), source_scope())
            .expect("source store")
            .current_projection_outbox()
            .await
            .expect("current outbox")
    }

    async fn retained_event_id(storage: &ScopedStorage, record_id: &str) -> String {
        current_outbox(storage)
            .await
            .into_iter()
            .find(|record| record.record_id() == record_id)
            .and_then(|record| record.event_id())
            .expect("retained record carries an event id")
    }

    #[derive(Default)]
    struct RecordingProjectionHandler {
        payloads: Mutex<Vec<Bytes>>,
    }

    impl RecordingProjectionHandler {
        fn payloads(&self) -> Vec<Bytes> {
            self.payloads
                .lock()
                .expect("recording handler lock")
                .clone()
        }
    }

    #[async_trait]
    impl ProjectionOutboxHandler for RecordingProjectionHandler {
        async fn process(&self, record: &ControlMvpProjectionOutboxRecord) -> Result<()> {
            self.payloads
                .lock()
                .expect("recording handler lock")
                .push(record.payload().clone());
            Ok(())
        }
    }

    #[test]
    fn event_ids_separate_incarnations_of_one_reusable_record_id() {
        assert_eq!(
            "evt-00000000000000000001-record-r",
            control_mvp_outbox_event_id(1, "record-r")
        );
        assert_ne!(
            control_mvp_outbox_event_id(1, "record-r"),
            control_mvp_outbox_event_id(3, "record-r"),
            "a re-staged record id must not reuse the consumed incarnation's identity"
        );
        // The ack key is keyed by tenure *and* event, so neither a rebind nor a
        // re-stage can collide with an earlier acknowledgement.
        assert_ne!(
            ProjectionOutboxDeliveryId::new("consumer-a", 1, "record-r", 1).ack_key(),
            ProjectionOutboxDeliveryId::new("consumer-a", 2, "record-r", 1).ack_key()
        );
        assert_ne!(
            ProjectionOutboxDeliveryId::new("consumer-a", 1, "record-r", 1).ack_key(),
            ProjectionOutboxDeliveryId::new("consumer-a", 1, "record-r", 3).ack_key()
        );
    }

    #[tokio::test]
    async fn successful_ack_write_returns_state_token() {
        let writer = writer(storage());

        let receipt = writer
            .acknowledge(&delivery("record-1", 1))
            .await
            .expect("ack write");

        assert_eq!(&ack_scope(), receipt.token().scope());
        assert_eq!(1, receipt.token().logical_sequence());
        assert!(!receipt.token().authority_manifest_id().is_empty());
        assert_eq!(&ack_record("record-1", 1), receipt.record());
        assert_eq!(
            "evt-00000000000000000001-record-1",
            receipt.record().event_id()
        );
        assert_eq!(
            FIRST_BINDING_INCARNATION,
            receipt.record().binding_incarnation()
        );
    }

    #[tokio::test]
    async fn duplicate_ack_write_returns_existing_committed_token_without_new_sequence() {
        let writer = writer(storage());

        let first = writer
            .acknowledge(&delivery("record-1", 1))
            .await
            .expect("first ack");
        let duplicate = writer
            .acknowledge(&delivery("record-1", 1))
            .await
            .expect("duplicate ack is idempotent");

        assert_eq!(first.token(), duplicate.token());
        assert_eq!(first.record(), duplicate.record());
        assert_eq!(
            Some(ack_record("record-1", 1)),
            writer
                .read_ack_at(duplicate.token().clone(), &delivery("record-1", 1))
                .await
                .expect("duplicate token reads ack")
        );
    }

    #[tokio::test]
    async fn same_record_id_at_a_different_source_sequence_is_a_separate_event() {
        let writer = writer(storage());

        writer
            .acknowledge(&delivery("record-1", 1))
            .await
            .expect("first incarnation ack");
        writer
            .acknowledge(&delivery("record-1", 9))
            .await
            .expect("a later incarnation of the same record id is a distinct event");

        // Both acknowledgements coexist: the record id is reusable, the event
        // identity is not, so neither can be mistaken for the other.
        let acknowledged = writer
            .acknowledged_event_ids("consumer-a", FIRST_BINDING_INCARNATION)
            .await
            .expect("acknowledged events");
        assert_eq!(
            BTreeSet::from([
                control_mvp_outbox_event_id(1, "record-1"),
                control_mvp_outbox_event_id(9, "record-1"),
            ]),
            acknowledged
        );
        assert_eq!(
            Some(9),
            writer
                .latest_projected_sequence("consumer-a")
                .await
                .expect("watermark")
        );
    }

    #[tokio::test]
    async fn read_ack_at_state_token_returns_committed_ack() {
        let writer = writer(storage());

        let first = writer
            .acknowledge(&delivery("record-1", 1))
            .await
            .expect("first ack");
        let second = writer
            .acknowledge(&delivery("record-2", 2))
            .await
            .expect("second ack");

        assert_eq!(
            Some(ack_record("record-1", 1)),
            writer
                .read_ack_at(first.token().clone(), &delivery("record-1", 1))
                .await
                .expect("read first token")
        );
        assert_eq!(
            None,
            writer
                .read_ack_at(first.token().clone(), &delivery("record-2", 2))
                .await
                .expect("first token does not include later ack")
        );
        assert_eq!(
            Some(ack_record("record-2", 2)),
            writer
                .read_ack_at(second.token().clone(), &delivery("record-2", 2))
                .await
                .expect("read second token")
        );
    }

    #[tokio::test]
    async fn state_token_read_status_marks_missing_retained_manifest_unavailable() {
        let storage = storage();
        let writer = writer(storage.clone());

        let receipt = writer
            .acknowledge(&delivery("record-1", 1))
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
                .read_ack_at_status(token, &delivery("record-1", 1))
                .await
                .expect("token status")
        );
    }

    #[tokio::test]
    async fn warm_ack_write_and_token_point_read_do_not_call_object_store_listing() {
        let (backend, storage) = no_list_storage();
        let writer = writer(storage);

        writer
            .acknowledge(&delivery("record-1", 1))
            .await
            .expect("seed ack");
        let receipt = writer
            .acknowledge(&delivery("record-2", 2))
            .await
            .expect("warm ack");

        assert_eq!(
            ProjectionOutboxAckReadStatus::Available(Some(ack_record("record-2", 2))),
            writer
                .read_ack_at_status(receipt.token().clone(), &delivery("record-2", 2))
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
            .acknowledge(&delivery("record-1", 1))
            .await
            .expect("first ack");
        let second = writer
            .acknowledge(&delivery("record-2", 2))
            .await
            .expect("second ack");

        assert_eq!(
            ProjectionOutboxAckReadStatus::Available(Some(ack_record("record-1", 1))),
            writer
                .read_ack_at_status(first.token().clone(), &delivery("record-1", 1))
                .await
                .expect("first retained read")
        );
        assert_eq!(
            ProjectionOutboxAckReadStatus::Available(None),
            writer
                .read_ack_at_status(first.token().clone(), &delivery("record-2", 2))
                .await
                .expect("first retained read excludes later ack")
        );
        assert_eq!(
            ProjectionOutboxAckReadStatus::Available(Some(ack_record("record-2", 2))),
            writer
                .read_ack_at_status(second.token().clone(), &delivery("record-2", 2))
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
            .acknowledge(&delivery("record-1", 3))
            .await
            .expect("first ack");
        writer
            .acknowledge(&delivery("record-2", 7))
            .await
            .expect("second ack");
        writer
            .acknowledge(&delivery("record-3", 5))
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
    async fn watermark_spans_tenures_while_skip_authority_does_not() {
        let writer = writer(storage());

        writer
            .acknowledge(&ProjectionOutboxDeliveryId::new(
                "consumer-a",
                1,
                "record-1",
                4,
            ))
            .await
            .expect("first tenure ack");

        // The watermark answers "how far has this consumer ever projected", so
        // it must span tenures and never regress on a rebind...
        assert_eq!(
            Some(4),
            writer
                .latest_projected_sequence("consumer-a")
                .await
                .expect("watermark spans tenures")
        );
        // ...but skip/trim authority is tenure-scoped, so the new tenure sees
        // nothing it may act on.
        assert!(
            writer
                .acknowledged_event_ids("consumer-a", 2)
                .await
                .expect("second tenure acks")
                .is_empty()
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
            .acknowledge(&delivery("record-1", source_token.logical_sequence()))
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
            .acknowledge(&delivery("record-1", 1))
            .await
            .expect("ack commit");

        assert_eq!(
            ProjectionOutboxAckFreshness::ProjectionUnavailable,
            ProjectionOutboxAckWriter::projection_freshness_for(receipt.token(), None)
        );
        assert_eq!(
            Some(ack_record("record-1", 1)),
            writer
                .read_ack_at(receipt.token().clone(), &delivery("record-1", 1))
                .await
                .expect("committed ack remains readable")
        );
    }

    #[tokio::test]
    async fn stale_projection_watermark_status_is_visible() {
        let writer = writer(storage());

        let receipt = writer
            .acknowledge(&delivery("record-1", 1))
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
            .acknowledge(&delivery("record-1", 1))
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
            .acknowledge(&delivery("record-1", 1))
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
            .acknowledge(&delivery("record-1", 1))
            .await
            .expect("first commit");
        let second = writer
            .acknowledge(&delivery("record-2", 2))
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
                .read_ack_at(second.token().clone(), &delivery("record-2", 2))
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
            .acknowledge(&delivery("record-1", 1))
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
        assert_eq!(
            vec![
                control_mvp_outbox_event_id(1, "record-1"),
                control_mvp_outbox_event_id(2, "record-2"),
            ],
            report.drained_event_ids
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
            .acknowledge(&delivery("record-1", 1))
            .await
            .expect("ack record-1");

        let report = worker.trim_acked().await.expect("trim");
        assert_eq!(vec!["record-1".to_string()], report.trimmed_record_ids);
        assert_eq!(
            vec![control_mvp_outbox_event_id(1, "record-1")],
            report.trimmed_event_ids
        );
        assert_eq!(Some(3), report.trim_sequence);

        let source = ControlMvpStateStore::new(storage, source_scope()).expect("source store");
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
        let source = ControlMvpStateStore::new(storage, source_scope()).expect("source store");
        let mut txn = source
            .begin_control_txn(TxnOptions::new(Some(source_scope())))
            .await
            .expect("begin");

        assert_precondition_failed(
            txn.trim_projection_outbox(vec![ControlMvpOutboxTrimTarget::new("record-unknown", 1)]),
            "not present in current state",
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

        // Consumer processes the first record, then "stops". The first drain
        // also registers the single-consumer trim binding, which is one
        // source-domain maintenance commit, so ack-derived freshness honestly
        // reports that commit as not-yet-projected.
        let first = commit_source_record(&storage, "record-1").await;
        worker
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("initial drain");
        assert_eq!(
            ProjectionOutboxAckFreshness::StaleProjection {
                committed_sequence: first.logical_sequence() + 1,
                latest_projected_sequence: first.logical_sequence(),
            },
            worker.freshness().await.expect("freshness after drain")
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
    async fn restaged_record_id_after_trim_is_a_fresh_event_and_drains_normally() {
        let storage = storage();
        let worker = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
            .expect("worker");
        commit_source_record_with_payload(&storage, "record-r", br#"{"payload":"a"}"#).await;

        let first = worker
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("first drain");
        assert_eq!(vec!["record-r".to_string()], first.drained_record_ids);
        let trim = worker.trim_acked().await.expect("trim");
        assert_eq!(vec!["record-r".to_string()], trim.trimmed_record_ids);

        // H1 reproduction: the same record id staged again with a different
        // payload must be handed to the handler as a fresh event, not
        // swallowed as already-acknowledged and then trimmed unseen.
        commit_source_record_with_payload(&storage, "record-r", br#"{"payload":"b"}"#).await;
        let handler = RecordingProjectionHandler::default();
        let second = worker.drain(&handler).await.expect("second drain");
        assert_eq!(vec!["record-r".to_string()], second.drained_record_ids);
        assert_eq!(0, second.already_acknowledged);
        assert_eq!(
            vec![Bytes::from_static(br#"{"payload":"b"}"#)],
            handler.payloads()
        );

        // The re-staged record trims normally and the retired watermark never
        // regressed the derived projection watermark.
        let second_trim = worker.trim_acked().await.expect("second trim");
        assert_eq!(vec!["record-r".to_string()], second_trim.trimmed_record_ids);
        assert_eq!(
            Some(4),
            worker
                .acks()
                .latest_projected_sequence("consumer-a")
                .await
                .expect("watermark survives ack retirement")
        );
    }

    #[tokio::test]
    async fn ack_retirement_before_trim_crash_redelivers_instead_of_losing() {
        let backend = Arc::new(FailOncePutBackend::new(
            Arc::new(MemoryBackend::new()),
            SOURCE_POINTER,
        ));
        let storage =
            ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scoped storage");
        commit_source_record(&storage, "record-1").await;
        let worker = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
            .expect("worker");
        let handler = RecordingProjectionHandler::default();
        worker.drain(&handler).await.expect("first drain");

        // Crash window: the ack-domain retirement commits, then the
        // source-domain trim pointer CAS fails.
        backend.arm();
        let error = worker
            .trim_acked()
            .await
            .expect_err("injected crash after ack retirement must interrupt the trim");
        assert!(
            matches!(error, CatalogError::Storage { .. }),
            "unexpected error: {error:?}"
        );

        // At-least-once: the record is still retained, its acknowledgement is
        // retired, so it is redelivered rather than lost.
        let backlog = worker.backlog().await.expect("backlog after crash");
        assert_eq!(vec!["record-1".to_string()], backlog.pending_record_ids);
        let redelivery = worker.drain(&handler).await.expect("redelivery drain");
        assert_eq!(vec!["record-1".to_string()], redelivery.drained_record_ids);
        assert_eq!(2, handler.payloads().len());
        assert_eq!(
            Some(1),
            worker
                .acks()
                .latest_projected_sequence("consumer-a")
                .await
                .expect("watermark preserved across retirement")
        );

        // The retried trim converges.
        let trim = worker.trim_acked().await.expect("trim retry");
        assert_eq!(vec!["record-1".to_string()], trim.trimmed_record_ids);
        assert!(
            worker
                .backlog()
                .await
                .expect("final backlog")
                .pending_record_ids
                .is_empty()
        );
    }

    /// R3: the interleaving a single injected sequential pointer failure can
    /// never expose. A trim is paused *after* its ack-domain retirement has
    /// committed and before its source-domain trim commit; a concurrent drain
    /// with the same consumer id re-acknowledges the record in that window, so
    /// the fresh acknowledgement outlives the trimmed source record. With
    /// delivery keyed on the event incarnation, that surviving acknowledgement
    /// names an event id no future staging can reproduce, so it is inert: the
    /// re-staged record id is still delivered, and no trim can remove it
    /// unseen.
    #[tokio::test]
    async fn concurrent_same_consumer_drain_between_ack_retirement_and_trim_cannot_shadow_a_restage()
     {
        let backend = Arc::new(PauseOncePutBackend::new(
            Arc::new(MemoryBackend::new()),
            SOURCE_POINTER,
        ));
        let storage =
            ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scoped storage");
        commit_source_record_with_payload(&storage, "record-r", br#"{"payload":"a"}"#).await;
        let worker = Arc::new(
            ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
                .expect("worker"),
        );
        worker
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("initial drain binds the consumer and acknowledges record-r");

        // The trim retires the acknowledgement, then blocks on its source
        // pointer publish.
        let (reached, release) = backend.arm();
        let trimming = tokio::spawn({
            let worker = Arc::clone(&worker);
            async move { worker.trim_acked().await }
        });
        reached.await.expect("trim reached its source publish");

        // Concurrent same-consumer drain in the window: the source record is
        // still visible and its acknowledgement has been retired, so the drain
        // re-processes and re-acknowledges it.
        let racing = worker
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("concurrent drain in the retirement window");
        assert_eq!(vec!["record-r".to_string()], racing.drained_record_ids);

        release.send(()).expect("release the paused trim");
        let trim = trimming
            .await
            .expect("trim task")
            .expect("trim publishes after the racing drain");
        assert_eq!(vec!["record-r".to_string()], trim.trimmed_record_ids);

        // The racing acknowledgement now outlives its trimmed record. It names
        // event `evt-…-1-record-r`, which nothing will ever stage again.
        assert!(
            worker
                .acks()
                .acknowledged_event_ids("consumer-a", FIRST_BINDING_INCARNATION)
                .await
                .expect("surviving acks")
                .contains(&control_mvp_outbox_event_id(1, "record-r"))
        );

        // Re-staging the record id must therefore still deliver it.
        commit_source_record_with_payload(&storage, "record-r", br#"{"payload":"b"}"#).await;
        assert_eq!(
            vec!["record-r".to_string()],
            worker
                .backlog()
                .await
                .expect("backlog after restage")
                .pending_record_ids
        );
        let handler = RecordingProjectionHandler::default();
        let delivered = worker.drain(&handler).await.expect("drain after restage");
        assert_eq!(vec!["record-r".to_string()], delivered.drained_record_ids);
        assert_eq!(0, delivered.already_acknowledged);
        assert_eq!(
            vec![Bytes::from_static(br#"{"payload":"b"}"#)],
            handler.payloads()
        );
    }

    /// R2: two trim passes capture the same `(record_id, origin_sequence)`;
    /// one completes and the record id is re-staged behind it. The delayed
    /// pass reaches its ack retirement and *fresh* source transaction only
    /// afterwards, exactly as `trim_acked` sequences them, and must fail a
    /// typed precondition instead of deleting the new incarnation.
    #[tokio::test]
    async fn delayed_trim_observation_fails_closed_against_a_restaged_incarnation() {
        let storage = storage();
        let worker = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
            .expect("worker");
        commit_source_record_with_payload(&storage, "record-r", br#"{"payload":"a"}"#).await;
        worker
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("drain record-r");

        // Both passes observe record-r at origin sequence 1.
        let captured = vec![ProjectionOutboxDeliveryId::new(
            "consumer-a",
            FIRST_BINDING_INCARNATION,
            "record-r",
            1,
        )];
        assert_eq!(
            control_mvp_outbox_event_id(1, "record-r"),
            retained_event_id(&storage, "record-r").await
        );

        // One pass completes, and a producer re-stages the record id behind it.
        worker.trim_acked().await.expect("completing trim");
        commit_source_record_with_payload(&storage, "record-r", br#"{"payload":"b"}"#).await;
        let restaged_event = retained_event_id(&storage, "record-r").await;
        assert_ne!(control_mvp_outbox_event_id(1, "record-r"), restaged_event);

        // The delayed pass now runs its ack-domain retirement (which converges
        // idempotently) and opens a fresh source transaction against the new
        // state, which is where the identity predicate has to hold.
        worker
            .acks()
            .retire_acknowledgements(&captured)
            .await
            .expect("delayed retirement converges");
        let source =
            ControlMvpStateStore::new(storage.clone(), source_scope()).expect("source store");
        let mut txn = source
            .begin_control_txn(TxnOptions::new(Some(source_scope())))
            .await
            .expect("delayed trim opens a fresh source transaction");
        assert_precondition_failed(
            txn.trim_projection_outbox(
                captured.iter().map(ProjectionOutboxDeliveryId::trim_target),
            ),
            "a different incarnation of the same record id",
        );

        // The new incarnation survives untouched and is delivered normally.
        assert_eq!(
            restaged_event,
            retained_event_id(&storage, "record-r").await
        );
        let handler = RecordingProjectionHandler::default();
        let drained = worker.drain(&handler).await.expect("drain the new record");
        assert_eq!(vec!["record-r".to_string()], drained.drained_record_ids);
        assert_eq!(
            vec![Bytes::from_static(br#"{"payload":"b"}"#)],
            handler.payloads()
        );
    }

    #[tokio::test]
    async fn replay_rejects_a_forged_trim_naming_a_superseded_event_incarnation() {
        let storage = storage();
        commit_source_record(&storage, "record-1").await;
        let source =
            ControlMvpStateStore::new(storage.clone(), source_scope()).expect("source store");
        let mut txn = source
            .begin_control_txn(TxnOptions::new(Some(source_scope())))
            .await
            .expect("begin");

        assert_precondition_failed(
            txn.trim_projection_outbox(vec![ControlMvpOutboxTrimTarget::new("record-1", 99)]),
            "a different incarnation of the same record id",
        );
    }

    #[tokio::test]
    async fn second_consumer_drain_and_trim_fail_closed_on_bound_domain() {
        let storage = storage();
        commit_source_record(&storage, "record-1").await;
        let worker_a = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
            .expect("worker a");
        worker_a
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("first drain registers the binding");
        assert_eq!(
            Some("consumer-a".to_string()),
            worker_a.bound_consumer().await.expect("binding")
        );
        assert_eq!(
            Some(FIRST_BINDING_INCARNATION),
            worker_a.bound_incarnation().await.expect("incarnation")
        );

        let worker_b = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-b")
            .expect("worker b");
        let drain_error = worker_b
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect_err("second consumer drain must fail closed");
        assert!(
            matches!(&drain_error, CatalogError::PreconditionFailed { message }
                if message.contains("consumer-a") && message.contains("single-consumer")),
            "unexpected error: {drain_error:?}"
        );
        let trim_error = worker_b
            .trim_acked()
            .await
            .expect_err("second consumer trim must fail closed");
        assert!(
            matches!(&trim_error, CatalogError::PreconditionFailed { message }
                if message.contains("consumer-a")),
            "unexpected error: {trim_error:?}"
        );

        // The bound consumer keeps functioning.
        let trim = worker_a.trim_acked().await.expect("bound consumer trim");
        assert_eq!(vec!["record-1".to_string()], trim.trimmed_record_ids);
    }

    /// R1: a full rebind round trip. The previous test stopped once B had
    /// drained, which proves nothing about what A's *retained* acknowledgement
    /// can still authorize once authority comes back to it. Here A's original
    /// acknowledgement survives every step, the record id is re-staged with a
    /// new payload, and authority is rebound to A — whose old acknowledgement
    /// must be unable to skip the fresh record or authorize trimming it.
    #[tokio::test]
    async fn rebind_round_trip_denies_a_previous_tenure_ack_any_skip_or_trim_authority() {
        let storage = storage();
        commit_source_record_with_payload(&storage, "record-r", br#"{"payload":"a"}"#).await;
        let worker_a = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
            .expect("worker a");
        let worker_b = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-b")
            .expect("worker b");

        // A drains record-r in its first tenure.
        let drained_a = worker_a
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("consumer-a drains record-r");
        assert_eq!(vec!["record-r".to_string()], drained_a.drained_record_ids);
        let a_first_tenure_ack =
            ProjectionOutboxDeliveryId::new("consumer-a", FIRST_BINDING_INCARNATION, "record-r", 1);

        // Authority is transferred to B, which drains and trims record-r.
        let to_b = worker_b.rebind_consumer().await.expect("rebind to b");
        assert_eq!(Some("consumer-a".to_string()), to_b.previous_consumer);
        assert_eq!(2, to_b.incarnation);
        let drained_b = worker_b
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("consumer-b drains");
        assert_eq!(vec!["record-r".to_string()], drained_b.drained_record_ids);
        assert_eq!(0, drained_b.already_acknowledged);
        let trimmed_b = worker_b.trim_acked().await.expect("consumer-b trims");
        assert_eq!(vec!["record-r".to_string()], trimmed_b.trimmed_record_ids);
        let error = worker_a
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect_err("previous consumer must fail closed after rebind");
        assert!(
            matches!(&error, CatalogError::PreconditionFailed { message }
                if message.contains("consumer-b")),
            "unexpected error: {error:?}"
        );

        // A's first-tenure acknowledgement was never retired by B's trim: it
        // is still durably present, which is exactly what made the record-id
        // identity unsafe.
        assert_eq!(
            Some(1),
            worker_a
                .acks()
                .read_ack_at(
                    ControlMvpStateStore::new(storage.clone(), ack_scope())
                        .expect("ack store")
                        .current_state_token()
                        .await
                        .expect("ack token"),
                    &a_first_tenure_ack,
                )
                .await
                .expect("read a's first-tenure ack")
                .map(|record| record.source_sequence())
        );

        // The record id is re-staged with new content, and authority returns
        // to A.
        commit_source_record_with_payload(&storage, "record-r", br#"{"payload":"b"}"#).await;
        let back_to_a = worker_a.rebind_consumer().await.expect("rebind back to a");
        assert_eq!(Some("consumer-b".to_string()), back_to_a.previous_consumer);
        assert_eq!(Some(2), back_to_a.previous_incarnation);
        assert_eq!(
            3, back_to_a.incarnation,
            "returning authority must mint a new tenure, not resume the old one"
        );

        // Before draining anything in its new tenure, A must not be able to
        // trim the fresh record on the strength of its old acknowledgement.
        let premature_trim = worker_a
            .trim_acked()
            .await
            .expect("trim pass runs but must find nothing it may remove");
        assert!(
            premature_trim.trimmed_record_ids.is_empty(),
            "a previous tenure's acknowledgement authorized a trim: {:?}",
            premature_trim.trimmed_record_ids
        );
        assert_eq!(None, premature_trim.trim_sequence);
        assert_eq!(
            vec!["record-r".to_string()],
            worker_a
                .backlog()
                .await
                .expect("backlog in the new tenure")
                .pending_record_ids
        );

        // And the fresh record is delivered with its new payload.
        let handler = RecordingProjectionHandler::default();
        let drained = worker_a
            .drain(&handler)
            .await
            .expect("a drains the restage");
        assert_eq!(vec!["record-r".to_string()], drained.drained_record_ids);
        assert_eq!(0, drained.already_acknowledged);
        assert_eq!(
            vec![Bytes::from_static(br#"{"payload":"b"}"#)],
            handler.payloads()
        );

        // Rebinding to the already-bound consumer reports it without a commit
        // and without minting a tenure.
        let idempotent = worker_a.rebind_consumer().await.expect("idempotent rebind");
        assert_eq!(Some("consumer-a".to_string()), idempotent.previous_consumer);
        assert_eq!(3, idempotent.incarnation);
        assert_eq!(None, idempotent.rebind_sequence);
    }

    /// A rebind advertises a transfer of **trim** authority, not merely of
    /// drain behaviour. Stopping once the new consumer has drained would leave
    /// that claim unproven: it would still hold if trimming kept consulting
    /// the old ownership. So the new owner actually trims, the record is
    /// checked to be physically gone from the source, a fresh instance of the
    /// new owner sees an empty backlog, and the previous owner is refused.
    #[tokio::test]
    async fn rebind_transfers_trim_authority_and_refuses_the_previous_owner() {
        let storage = storage();
        commit_source_record(&storage, "record-1").await;
        let worker_a = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
            .expect("worker a");

        // A binds the domain and acknowledges record-1 in its tenure.
        let drained_a = worker_a
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("consumer-a drains record-1");
        assert_eq!(vec!["record-1".to_string()], drained_a.drained_record_ids);
        assert_eq!(
            BTreeSet::from([control_mvp_outbox_event_id(1, "record-1")]),
            worker_a
                .acks()
                .acknowledged_event_ids("consumer-a", FIRST_BINDING_INCARNATION)
                .await
                .expect("consumer-a acknowledgements")
        );

        // B takes over, drains and acknowledges as B, then trims.
        let worker_b = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-b")
            .expect("worker b");
        let rebind = worker_b.rebind_consumer().await.expect("rebind to b");
        assert_eq!(Some("consumer-a".to_string()), rebind.previous_consumer);
        assert_eq!(2, rebind.incarnation);
        let drained_b = worker_b
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("consumer-b drains record-1");
        assert_eq!(vec!["record-1".to_string()], drained_b.drained_record_ids);
        assert_eq!(
            BTreeSet::from([control_mvp_outbox_event_id(1, "record-1")]),
            worker_b
                .acks()
                .acknowledged_event_ids("consumer-b", 2)
                .await
                .expect("consumer-b acknowledgements")
        );
        let trimmed_b = worker_b
            .trim_acked()
            .await
            .expect("the rebound consumer holds trim authority");
        assert_eq!(vec!["record-1".to_string()], trimmed_b.trimmed_record_ids);
        assert!(trimmed_b.trim_sequence.is_some());

        // The record is physically gone from the source domain's replayed
        // outbox, not merely filtered out of one worker's view.
        assert!(
            current_outbox(&storage).await.is_empty(),
            "the transferred trim must remove the record from the source domain"
        );

        // A fresh instance of the new owner replays that from durable state.
        let fresh_b = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-b")
            .expect("fresh worker b");
        let backlog = fresh_b.backlog().await.expect("fresh backlog");
        assert!(backlog.pending_record_ids.is_empty());
        assert_eq!(Some(2), fresh_b.bound_incarnation().await.expect("tenure"));

        // The previous owner can no longer trim: the refusal is the typed
        // single-consumer conflict and names the consumer that holds authority.
        let error = worker_a
            .trim_acked()
            .await
            .expect_err("the previous owner must lose trim authority");
        assert!(
            matches!(&error, CatalogError::PreconditionFailed { message }
                if message.contains("consumer-b") && message.contains("single-consumer")),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn trim_fails_closed_when_the_binding_tenure_changes_mid_pass() {
        let backend = Arc::new(PauseOncePutBackend::new(
            Arc::new(MemoryBackend::new()),
            SOURCE_POINTER,
        ));
        let storage =
            ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scoped storage");
        commit_source_record(&storage, "record-1").await;
        let worker_a = Arc::new(
            ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
                .expect("worker a"),
        );
        worker_a
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("consumer-a drains");

        // Pause the trim at its source publish, then transfer authority away.
        let (reached, release) = backend.arm();
        let trimming = tokio::spawn({
            let worker = Arc::clone(&worker_a);
            async move { worker.trim_acked().await }
        });
        reached.await.expect("trim reached its source publish");
        ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-b")
            .expect("worker b")
            .rebind_consumer()
            .await
            .expect("transfer authority mid-trim");
        release.send(()).expect("release the paused trim");

        let error = trimming
            .await
            .expect("trim task")
            .expect_err("a trim whose tenure was transferred must fail closed");
        assert!(
            matches!(error, CatalogError::CasFailed { .. }),
            "unexpected error: {error:?}"
        );

        // The record is still retained, so nothing was lost.
        assert_eq!(
            vec!["record-1".to_string()],
            current_outbox(&storage)
                .await
                .into_iter()
                .map(|record| record.record_id().to_string())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn consumer_binding_survives_replay_in_fresh_store_instances() {
        let storage = storage();
        commit_source_record(&storage, "record-1").await;
        let worker_a = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
            .expect("worker a");
        worker_a
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("bind consumer-a");

        // Fresh worker instances replay the binding from durable state.
        let fresh_b = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-b")
            .expect("fresh worker b");
        let error = fresh_b
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect_err("binding must survive replay");
        assert!(
            matches!(&error, CatalogError::PreconditionFailed { message }
                if message.contains("consumer-a")),
            "unexpected error: {error:?}"
        );
        let fresh_a = ProjectionOutboxWorker::new(storage, SOURCE_DOMAIN, "consumer-a")
            .expect("fresh worker a");
        fresh_a
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("bound consumer functions after replay");
    }

    #[tokio::test]
    async fn bindings_written_before_incarnations_read_as_the_first_tenure() {
        let storage = storage();
        commit_source_record(&storage, "record-1").await;
        let source =
            ControlMvpStateStore::new(storage.clone(), source_scope()).expect("source store");
        let mut txn = source
            .begin_control_txn(TxnOptions::new(Some(source_scope())))
            .await
            .expect("begin");
        txn.put(
            PROJECTION_OUTBOX_TRIM_BINDING_KEY,
            Bytes::from_static(br#"{"consumer_id":"consumer-a"}"#),
        )
        .await
        .expect("stage pre-incarnation binding");
        txn.commit().await.expect("commit pre-incarnation binding");

        let worker = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
            .expect("worker");
        assert_eq!(
            Some(FIRST_BINDING_INCARNATION),
            worker.bound_incarnation().await.expect("incarnation")
        );
        worker
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("the pre-incarnation binding keeps functioning");

        // A binding claiming incarnation 0 is not something any tenure mints.
        let mut txn = source
            .begin_control_txn(TxnOptions::new(Some(source_scope())))
            .await
            .expect("begin");
        txn.put(
            PROJECTION_OUTBOX_TRIM_BINDING_KEY,
            Bytes::from_static(br#"{"consumer_id":"consumer-a","incarnation":0}"#),
        )
        .await
        .expect("stage impossible binding");
        txn.commit().await.expect("commit impossible binding");
        let error = worker
            .bound_incarnation()
            .await
            .expect_err("incarnation 0 must fail closed");
        assert!(
            matches!(error, CatalogError::InvariantViolation { .. }),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn worker_survives_writer_epoch_fencing_and_stale_explicit_epoch_fails_closed() {
        let storage = storage();
        commit_source_record(&storage, "record-1").await;

        // Another writer claims fencing authority over the source domain.
        let claimed = ControlMvpStateStore::new(storage.clone(), source_scope())
            .expect("source store")
            .claim_writer_authority()
            .await
            .expect("claim source epoch");
        assert_eq!(1, claimed.writer_epoch());

        // Cooperative default: the worker adopts the published epoch and
        // keeps draining and trimming.
        let worker = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
            .expect("worker");
        let report = worker
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("cooperative drain after source fencing");
        assert_eq!(vec!["record-1".to_string()], report.drained_record_ids);
        let trim = worker
            .trim_acked()
            .await
            .expect("cooperative trim after source fencing");
        assert_eq!(vec!["record-1".to_string()], trim.trimmed_record_ids);

        // The ack domain fences independently; the worker adopts its
        // published epoch cooperatively too.
        let ack_store = ControlMvpStateStore::new(storage.clone(), ack_scope())
            .expect("ack store")
            .claim_writer_authority()
            .await
            .expect("claim ack epoch");
        assert_eq!(1, ack_store.writer_epoch());
        let mut txn = claimed
            .begin_control_txn(TxnOptions::new(Some(source_scope())))
            .await
            .expect("begin record-2");
        txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "record-2",
            Bytes::from_static(b"{}"),
        ))
        .expect("stage record-2");
        txn.commit().await.expect("commit record-2");
        let report = worker
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect("cooperative drain after ack fencing");
        assert_eq!(vec!["record-2".to_string()], report.drained_record_ids);

        // An explicit epoch below the published one still fails closed with
        // the typed fencing error.
        let mut txn = claimed
            .begin_control_txn(TxnOptions::new(Some(source_scope())))
            .await
            .expect("begin record-3");
        txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "record-3",
            Bytes::from_static(b"{}"),
        ))
        .expect("stage record-3");
        txn.commit().await.expect("commit record-3");
        let pinned = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
            .expect("pinned worker")
            .with_writer_epoch(0)
            .expect("epoch 0 is a representable pin");
        let error = pinned
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect_err("stale explicit epoch must fail closed");
        assert!(
            matches!(error, CatalogError::StaleWriterEpoch { .. }),
            "unexpected error: {error:?}"
        );

        // An unclaimed future epoch is refused too: only a claim advances the
        // published epoch, so a pinned future epoch is never authority.
        let ahead = ProjectionOutboxWorker::new(storage.clone(), SOURCE_DOMAIN, "consumer-a")
            .expect("future-epoch worker")
            .with_writer_epoch(9)
            .expect("epoch 9 is a representable pin");
        let error = ahead
            .drain(&AckOnlyProjectionHandler)
            .await
            .expect_err("unclaimed future epoch must fail closed");
        assert!(
            matches!(&error, CatalogError::PreconditionFailed { message }
                if message.contains("never claimed")),
            "unexpected error: {error:?}"
        );

        // u64::MAX is refused at configuration time.
        let error = ProjectionOutboxWorker::new(storage, SOURCE_DOMAIN, "consumer-a")
            .expect("max-epoch worker")
            .with_writer_epoch(u64::MAX)
            .err()
            .expect("u64::MAX must be rejected");
        assert!(
            matches!(error, CatalogError::Validation { .. }),
            "unexpected error: {error:?}"
        );
    }

    struct FailOncePutBackend {
        inner: Arc<dyn StorageBackend>,
        needle: String,
        armed: AtomicBool,
    }

    impl FailOncePutBackend {
        fn new(inner: Arc<dyn StorageBackend>, needle: &str) -> Self {
            Self {
                inner,
                needle: needle.to_string(),
                armed: AtomicBool::new(false),
            }
        }

        fn arm(&self) {
            self.armed.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl StorageBackend for FailOncePutBackend {
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
            if path.contains(&self.needle) && self.armed.swap(false, Ordering::SeqCst) {
                return Err(arco_core::Error::storage("injected trim crash point"));
            }
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    /// Barrier backend: blocks the first armed write to a path so a test can
    /// interleave another operation at a precise point in a commit protocol.
    struct PauseOncePutBackend {
        inner: Arc<dyn StorageBackend>,
        needle: String,
        gate: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    }

    impl PauseOncePutBackend {
        fn new(inner: Arc<dyn StorageBackend>, needle: &str) -> Self {
            Self {
                inner,
                needle: needle.to_string(),
                gate: Mutex::new(None),
            }
        }

        fn arm(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
            let (reached_tx, reached_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();
            *self.gate.lock().expect("barrier gate") = Some((reached_tx, release_rx));
            (reached_rx, release_tx)
        }
    }

    #[async_trait]
    impl StorageBackend for PauseOncePutBackend {
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
            if path.contains(&self.needle) {
                let gate = self.gate.lock().expect("barrier gate").take();
                if let Some((reached, release)) = gate {
                    let _ = reached.send(());
                    let _ = release.await;
                }
            }
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
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
