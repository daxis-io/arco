use arco_core::ScopedStorage;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateTxn, ControlMvpStateStore, StateScope, StateToken,
    TxnOptions,
};
use crate::error::{CatalogError, Result};

#[allow(dead_code)]
pub(crate) const PROJECTION_OUTBOX_ACK_DOMAIN: &str = "projection-outbox-acks";

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct ProjectionOutboxAckWriter {
    store: ControlMvpStateStore,
    scope: StateScope,
}

#[allow(dead_code)]
impl ProjectionOutboxAckWriter {
    pub(crate) fn new(storage: ScopedStorage, scope: StateScope) -> Result<Self> {
        if scope.domain() != PROJECTION_OUTBOX_ACK_DOMAIN {
            return Err(validation_failed(format!(
                "projection outbox acknowledgements require domain {PROJECTION_OUTBOX_ACK_DOMAIN}"
            )));
        }
        let store = ControlMvpStateStore::new(storage, scope.clone())?;
        Ok(Self { store, scope })
    }

    pub(crate) async fn acknowledge(
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
                if let Some(receipt) = self.existing_receipt_for(&key, &record).await? {
                    Ok(receipt)
                } else {
                    Err(CatalogError::CasFailed {
                        message: "projection outbox ack pointer CAS lost without a visible ack"
                            .to_string(),
                    })
                }
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn read_ack_at(
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

    pub(crate) async fn read_ack_at_status(
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

    pub(crate) fn projection_freshness_for(
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

    pub(crate) fn projection_watermark_lag_for(
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

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionOutboxAckWrite {
    consumer_id: String,
    record_id: String,
}

#[allow(dead_code)]
impl ProjectionOutboxAckWrite {
    #[must_use]
    pub(crate) fn new(consumer_id: impl Into<String>, record_id: impl Into<String>) -> Self {
        Self {
            consumer_id: consumer_id.into(),
            record_id: record_id.into(),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProjectionOutboxAckRecord {
    consumer_id: String,
    record_id: String,
}

#[allow(dead_code)]
impl ProjectionOutboxAckRecord {
    #[must_use]
    pub(crate) fn new(consumer_id: impl Into<String>, record_id: impl Into<String>) -> Self {
        Self {
            consumer_id: consumer_id.into(),
            record_id: record_id.into(),
        }
    }

    #[must_use]
    pub(crate) fn consumer_id(&self) -> &str {
        &self.consumer_id
    }

    #[must_use]
    pub(crate) fn record_id(&self) -> &str {
        &self.record_id
    }
}

impl From<ProjectionOutboxAckWrite> for ProjectionOutboxAckRecord {
    fn from(value: ProjectionOutboxAckWrite) -> Self {
        Self {
            consumer_id: value.consumer_id,
            record_id: value.record_id,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionOutboxAckReceipt {
    token: StateToken,
    record: ProjectionOutboxAckRecord,
}

#[allow(dead_code)]
impl ProjectionOutboxAckReceipt {
    #[must_use]
    pub(crate) const fn token(&self) -> &StateToken {
        &self.token
    }

    #[must_use]
    pub(crate) const fn record(&self) -> &ProjectionOutboxAckRecord {
        &self.record
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProjectionOutboxAckReadStatus {
    Available(Option<ProjectionOutboxAckRecord>),
    TokenUnavailable {
        manifest_id: String,
        logical_sequence: u64,
    },
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProjectionOutboxAckFreshness {
    Current {
        committed_sequence: u64,
        latest_projected_sequence: u64,
    },
    StaleProjection {
        committed_sequence: u64,
        latest_projected_sequence: u64,
    },
    ProjectionUnavailable,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionOutboxAckWatermarkLag {
    committed_sequence: u64,
    latest_projected_sequence: Option<u64>,
    pending_sequences: Option<u64>,
}

#[allow(dead_code)]
fn ack_key(consumer_id: &str, record_id: &str) -> Vec<u8> {
    let mut key = b"projection-outbox-acks/ack/".to_vec();
    push_length_prefixed(&mut key, consumer_id.as_bytes());
    key.push(b'/');
    push_length_prefixed(&mut key, record_id.as_bytes());
    key
}

#[allow(dead_code)]
fn push_length_prefixed(key: &mut Vec<u8>, value: &[u8]) {
    key.extend_from_slice(value.len().to_string().as_bytes());
    key.push(b':');
    key.extend_from_slice(value);
}

#[allow(dead_code)]
fn encode_ack_record(record: &ProjectionOutboxAckRecord) -> Result<Bytes> {
    serde_json::to_vec(record)
        .map(Bytes::from)
        .map_err(|error| serialization_failed(format!("projection ack record encode: {error}")))
}

#[allow(dead_code)]
fn decode_ack_record(bytes: &Bytes) -> Result<ProjectionOutboxAckRecord> {
    serde_json::from_slice(bytes)
        .map_err(|error| serialization_failed(format!("projection ack record decode: {error}")))
}

#[allow(dead_code)]
fn validation_failed(message: impl Into<String>) -> CatalogError {
    CatalogError::Validation {
        message: message.into(),
    }
}

#[allow(dead_code)]
fn serialization_failed(message: impl Into<String>) -> CatalogError {
    CatalogError::Serialization {
        message: message.into(),
    }
}

#[allow(dead_code)]
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

    fn ack_write(record_id: &str) -> ProjectionOutboxAckWrite {
        ProjectionOutboxAckWrite::new("consumer-a", record_id)
    }

    fn ack_record(record_id: &str) -> ProjectionOutboxAckRecord {
        ProjectionOutboxAckRecord::new("consumer-a", record_id)
    }

    fn assert_unsupported<T>(result: Result<T>, expected: &str) {
        match result {
            Err(CatalogError::UnsupportedOperation { .. }) => {}
            Err(error) => panic!("expected UnsupportedOperation for {expected}, got {error:?}"),
            Ok(_) => panic!("expected UnsupportedOperation for {expected}"),
        }
    }

    #[tokio::test]
    async fn successful_ack_write_returns_state_token() {
        let writer = writer(storage());

        let receipt = writer
            .acknowledge(ack_write("record-1"))
            .await
            .expect("ack write");

        assert_eq!(&ack_scope(), receipt.token().scope());
        assert_eq!(1, receipt.token().logical_sequence());
        assert!(!receipt.token().authority_manifest_id().is_empty());
        assert_eq!(&ack_record("record-1"), receipt.record());
    }

    #[tokio::test]
    async fn duplicate_ack_write_returns_existing_committed_token_without_new_sequence() {
        let writer = writer(storage());

        let first = writer
            .acknowledge(ack_write("record-1"))
            .await
            .expect("first ack");
        let duplicate = writer
            .acknowledge(ack_write("record-1"))
            .await
            .expect("duplicate ack is idempotent");

        assert_eq!(first.token(), duplicate.token());
        assert_eq!(first.record(), duplicate.record());
        assert_eq!(
            Some(ack_record("record-1")),
            writer
                .read_ack_at(duplicate.token().clone(), "consumer-a", "record-1")
                .await
                .expect("duplicate token reads ack")
        );
    }

    #[tokio::test]
    async fn read_ack_at_state_token_returns_committed_ack() {
        let writer = writer(storage());

        let first = writer
            .acknowledge(ack_write("record-1"))
            .await
            .expect("first ack");
        let second = writer
            .acknowledge(ack_write("record-2"))
            .await
            .expect("second ack");

        assert_eq!(
            Some(ack_record("record-1")),
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
            Some(ack_record("record-2")),
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
            .acknowledge(ack_write("record-1"))
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
            .acknowledge(ack_write("record-1"))
            .await
            .expect("seed ack");
        let receipt = writer
            .acknowledge(ack_write("record-2"))
            .await
            .expect("warm ack");

        assert_eq!(
            ProjectionOutboxAckReadStatus::Available(Some(ack_record("record-2"))),
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
            .acknowledge(ack_write("record-1"))
            .await
            .expect("first ack");
        let second = writer
            .acknowledge(ack_write("record-2"))
            .await
            .expect("second ack");

        assert_eq!(
            ProjectionOutboxAckReadStatus::Available(Some(ack_record("record-1"))),
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
            ProjectionOutboxAckReadStatus::Available(Some(ack_record("record-2"))),
            writer
                .read_ack_at_status(second.token().clone(), "consumer-a", "record-2")
                .await
                .expect("second retained read")
        );
        assert_eq!(0, backend.list_calls());
    }

    #[tokio::test]
    async fn projection_freshness_is_diagnostic_only_after_authority_commit() {
        let writer = writer(storage());

        let receipt = writer
            .acknowledge(ack_write("record-1"))
            .await
            .expect("ack commit");

        assert_eq!(
            ProjectionOutboxAckFreshness::ProjectionUnavailable,
            ProjectionOutboxAckWriter::projection_freshness_for(receipt.token(), None)
        );
        assert_eq!(
            Some(ack_record("record-1")),
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
            .acknowledge(ack_write("record-1"))
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
            .acknowledge(ack_write("record-1"))
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
            .acknowledge(ack_write("record-1"))
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
            .acknowledge(ack_write("record-1"))
            .await
            .expect("first commit");
        let second = writer
            .acknowledge(ack_write("record-2"))
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
            Some(ack_record("record-2")),
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
            .acknowledge(ack_write("record-1"))
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
