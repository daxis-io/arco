#![allow(dead_code)]

use arco_core::ScopedStorage;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::{
    ArcoStateReader, ArcoStateTxn, ControlMvpStateStore, StateScope, StateToken, TxnOptions,
};
use crate::error::{CatalogError, Result};

pub(crate) const PROJECTION_OUTBOX_ACK_DOMAIN: &str = "projection-outbox-acks";

#[derive(Clone)]
pub(crate) struct ProjectionOutboxAckWriter {
    store: ControlMvpStateStore,
    scope: StateScope,
}

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
        let mut txn = self
            .store
            .begin_control_txn(TxnOptions::new(Some(self.scope.clone())))
            .await?;
        txn.assert_absent(&key).await?;
        txn.put(&key, encode_ack_record(&record)?).await?;
        let token = txn.commit().await?;
        Ok(ProjectionOutboxAckReceipt { token, record })
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionOutboxAckWrite {
    consumer_id: String,
    record_id: String,
}

impl ProjectionOutboxAckWrite {
    #[must_use]
    pub(crate) fn new(consumer_id: impl Into<String>, record_id: impl Into<String>) -> Self {
        Self {
            consumer_id: consumer_id.into(),
            record_id: record_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProjectionOutboxAckRecord {
    consumer_id: String,
    record_id: String,
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionOutboxAckReceipt {
    token: StateToken,
    record: ProjectionOutboxAckRecord,
}

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

fn ack_key(consumer_id: &str, record_id: &str) -> Vec<u8> {
    let mut key = b"projection-outbox-acks/ack/".to_vec();
    push_length_prefixed(&mut key, consumer_id.as_bytes());
    key.push(b'/');
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arco_core::{MemoryBackend, ScopedStorage};

    use super::*;
    use crate::error::CatalogError;
    use crate::state_store::{ArcoStateStore, CurrentStateStore, StateScope, TxnOptions};

    fn ack_scope() -> StateScope {
        StateScope::new("tenant", "workspace", PROJECTION_OUTBOX_ACK_DOMAIN)
    }

    fn storage() -> ScopedStorage {
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scoped storage")
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
    fn unsupported_domains_reject_phase5a_writes() {
        let unsupported_scope = StateScope::new("tenant", "workspace", "catalog");

        let error = match ProjectionOutboxAckWriter::new(storage(), unsupported_scope) {
            Err(error) => error,
            Ok(_) => panic!("unsupported scope must reject writer creation"),
        };

        assert!(matches!(error, CatalogError::Validation { .. }));
    }

    #[test]
    fn module_source_stays_inside_state_store_domain() {
        let source = include_str!("projection_outbox_acks.rs");
        let forbidden = [
            ["auth", "z"].concat(),
            ["cred", "ential"].concat(),
            ["system", "_table"].concat(),
            ["gra", "nts"].concat(),
        ];

        for term in forbidden {
            assert!(
                !source.contains(&term),
                "projection ack writer unexpectedly references {term}"
            );
        }
    }
}
