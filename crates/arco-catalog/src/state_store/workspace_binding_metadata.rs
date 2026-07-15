use std::collections::BTreeMap;

use arco_core::ScopedStorage;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::metadata_readiness::{self, CompiledStateStatus, ProjectionLag, TokenPinnedReadStatus};
use super::{
    ArcoStateReader, ArcoStateTxn, ControlMvpStateStore, StateScope, StateToken, TxnOptions,
    validate_metadata_timestamp, validate_required_metadata_field,
};
use crate::error::{CatalogError, Result};
use crate::metastore::events::LifecycleState;
use crate::state_store::path_governance_metadata::PATH_GOVERNANCE_METADATA_DOMAIN;

#[derive(Clone)]
pub struct WorkspaceMetastoreBindingMetadataWriter {
    store: ControlMvpStateStore,
    scope: StateScope,
}

impl WorkspaceMetastoreBindingMetadataWriter {
    pub(crate) fn new(storage: ScopedStorage, scope: StateScope) -> Result<Self> {
        if scope.domain() != PATH_GOVERNANCE_METADATA_DOMAIN {
            return Err(validation_failed(format!(
                "workspace binding metadata requires domain {PATH_GOVERNANCE_METADATA_DOMAIN}"
            )));
        }
        let store = ControlMvpStateStore::new(storage, scope.clone())?;
        Ok(Self { store, scope })
    }

    pub(crate) async fn bind_workspace(
        &self,
        input: WorkspaceMetastoreBindingMetadataInput,
    ) -> Result<WorkspaceMetastoreBindingMetadataReceipt> {
        input.validate()?;
        let record = WorkspaceMetastoreBindingMetadataRecord::from(input);
        if record.workspace_id() != self.scope.workspace_id() {
            return Err(validation_failed(
                "workspace binding metadata workspace_id must match state scope",
            ));
        }

        let binding_key = binding_key(record.binding_id());
        let pair_key = workspace_metastore_pair_key(record.workspace_id(), record.metastore_id());
        let mut txn = self
            .store
            .begin_control_txn(TxnOptions::new(Some(self.scope.clone())))
            .await?;

        if txn.get(&binding_key).await?.is_some() {
            return Err(CatalogError::AlreadyExists {
                entity: "workspace_metastore_binding_metadata".to_string(),
                name: record.binding_id().to_string(),
            });
        }
        if txn.get(&pair_key).await?.is_some() {
            return Err(precondition_failed(
                "workspace/metastore binding metadata pair already exists",
            ));
        }
        txn.assert_absent(&binding_key).await?;
        txn.assert_absent(&pair_key).await?;
        txn.put(&binding_key, encode_binding(&record)?).await?;
        txn.put(&pair_key, Bytes::from(record.binding_id().to_string()))
            .await?;

        let token = txn.commit().await?;
        Ok(WorkspaceMetastoreBindingMetadataReceipt { token, record })
    }

    pub(crate) async fn read_binding_at(
        &self,
        token: StateToken,
        binding_id: &str,
    ) -> Result<Option<WorkspaceMetastoreBindingMetadataRecord>> {
        let key = binding_key(binding_id);
        let reader = self.store.read_at(token).await?;
        let Some(bytes) = reader.get(&key).await? else {
            return Ok(None);
        };
        decode_binding(&bytes).map(Some)
    }

    pub(crate) async fn read_binding_at_status(
        &self,
        token: StateToken,
        binding_id: &str,
    ) -> Result<WorkspaceMetastoreBindingReadStatus> {
        let key = binding_key(binding_id);
        metadata_readiness::read_at_status(&self.store, token, &key, decode_binding).await
    }

    pub(crate) fn projection_lag_for(
        token: &StateToken,
        latest_projected_sequence: Option<u64>,
    ) -> WorkspaceMetastoreBindingProjectionLag {
        metadata_readiness::projection_lag_for(token, latest_projected_sequence)
    }

    pub(crate) fn compiled_state_status_for(
        token: &StateToken,
        compiled: Option<&CompiledWorkspaceMetastoreBindingMetadataState>,
    ) -> WorkspaceMetastoreBindingCompiledStateStatus {
        metadata_readiness::compiled_state_status_for(
            token,
            compiled.map(CompiledWorkspaceMetastoreBindingMetadataState::source_token),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceMetastoreBindingMetadataInput {
    binding_id: String,
    workspace_id: String,
    metastore_id: String,
    owner: String,
    lifecycle_state: LifecycleState,
    updated_at_ms: i64,
}

impl WorkspaceMetastoreBindingMetadataInput {
    #[must_use]
    pub(crate) fn active(
        binding_id: impl Into<String>,
        workspace_id: impl Into<String>,
        metastore_id: impl Into<String>,
        owner: impl Into<String>,
        updated_at_ms: i64,
    ) -> Self {
        Self {
            binding_id: binding_id.into(),
            workspace_id: workspace_id.into(),
            metastore_id: metastore_id.into(),
            owner: owner.into(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms,
        }
    }

    fn validate(&self) -> Result<()> {
        validate_required_metadata_field(&self.binding_id, "binding_id")?;
        validate_required_metadata_field(&self.workspace_id, "workspace_id")?;
        validate_required_metadata_field(&self.metastore_id, "metastore_id")?;
        validate_required_metadata_field(&self.owner, "owner")?;
        validate_metadata_timestamp(self.updated_at_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceMetastoreBindingMetadataRecord {
    binding_id: String,
    workspace_id: String,
    metastore_id: String,
    owner: String,
    lifecycle_state: LifecycleState,
    updated_at_ms: i64,
    properties: BTreeMap<String, String>,
}

impl WorkspaceMetastoreBindingMetadataRecord {
    #[must_use]
    pub(crate) fn binding_id(&self) -> &str {
        &self.binding_id
    }

    #[must_use]
    pub(crate) fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    #[must_use]
    pub(crate) fn metastore_id(&self) -> &str {
        &self.metastore_id
    }

    #[must_use]
    pub(crate) fn owner(&self) -> &str {
        &self.owner
    }

    #[must_use]
    pub(crate) const fn lifecycle_state(&self) -> LifecycleState {
        self.lifecycle_state
    }

    #[must_use]
    pub(crate) const fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }
}

impl From<WorkspaceMetastoreBindingMetadataInput> for WorkspaceMetastoreBindingMetadataRecord {
    fn from(value: WorkspaceMetastoreBindingMetadataInput) -> Self {
        Self {
            binding_id: value.binding_id,
            workspace_id: value.workspace_id,
            metastore_id: value.metastore_id,
            owner: value.owner,
            lifecycle_state: value.lifecycle_state,
            updated_at_ms: value.updated_at_ms,
            properties: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceMetastoreBindingMetadataReceipt {
    token: StateToken,
    record: WorkspaceMetastoreBindingMetadataRecord,
}

impl WorkspaceMetastoreBindingMetadataReceipt {
    #[must_use]
    pub(crate) const fn token(&self) -> &StateToken {
        &self.token
    }

    #[must_use]
    pub(crate) const fn record(&self) -> &WorkspaceMetastoreBindingMetadataRecord {
        &self.record
    }
}

pub type WorkspaceMetastoreBindingReadStatus =
    TokenPinnedReadStatus<WorkspaceMetastoreBindingMetadataRecord>;

pub type WorkspaceMetastoreBindingProjectionLag = ProjectionLag;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledWorkspaceMetastoreBindingMetadataState {
    source_token: StateToken,
}

impl CompiledWorkspaceMetastoreBindingMetadataState {
    #[must_use]
    pub(crate) const fn new(source_token: StateToken) -> Self {
        Self { source_token }
    }

    #[must_use]
    pub(crate) const fn source_token(&self) -> &StateToken {
        &self.source_token
    }
}

pub type WorkspaceMetastoreBindingCompiledStateStatus = CompiledStateStatus;

fn binding_key(binding_id: &str) -> Vec<u8> {
    let mut key = b"workspace-binding-metadata/bindings/".to_vec();
    push_length_prefixed(&mut key, binding_id.as_bytes());
    key
}

fn workspace_metastore_pair_key(workspace_id: &str, metastore_id: &str) -> Vec<u8> {
    let mut key = b"workspace-binding-metadata/by-workspace-metastore/".to_vec();
    push_length_prefixed(&mut key, workspace_id.as_bytes());
    push_length_prefixed(&mut key, metastore_id.as_bytes());
    key
}

fn push_length_prefixed(key: &mut Vec<u8>, value: &[u8]) {
    key.extend_from_slice(value.len().to_string().as_bytes());
    key.push(b':');
    key.extend_from_slice(value);
}

fn encode_binding(record: &WorkspaceMetastoreBindingMetadataRecord) -> Result<Bytes> {
    serde_json::to_vec(record)
        .map(Bytes::from)
        .map_err(|error| {
            serialization_failed(format!(
                "workspace metastore binding metadata record encode: {error}"
            ))
        })
}

fn decode_binding(bytes: &Bytes) -> Result<WorkspaceMetastoreBindingMetadataRecord> {
    serde_json::from_slice(bytes).map_err(|error| {
        serialization_failed(format!(
            "workspace metastore binding metadata record decode: {error}"
        ))
    })
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

fn precondition_failed(message: impl Into<String>) -> CatalogError {
    CatalogError::PreconditionFailed {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arco_core::{MemoryBackend, ScopedStorage};

    use super::*;
    use crate::error::CatalogError;
    use crate::state_store::path_governance_metadata::PATH_GOVERNANCE_METADATA_DOMAIN;
    use crate::state_store::test_support::{FirstPointerCasGateBackend, POINTER_CAS_GATE_TIMEOUT};
    use crate::state_store::{ControlMvpPaths, StateScope};

    fn metadata_scope() -> StateScope {
        StateScope::new("tenant", "workspace", PATH_GOVERNANCE_METADATA_DOMAIN)
    }

    fn storage() -> ScopedStorage {
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scoped storage")
    }

    fn gated_storage() -> (ScopedStorage, Arc<FirstPointerCasGateBackend>) {
        let backend = FirstPointerCasGateBackend::new(PATH_GOVERNANCE_METADATA_DOMAIN);
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace")
            .expect("gated scoped storage");
        (storage, backend)
    }

    fn writer(storage: ScopedStorage) -> WorkspaceMetastoreBindingMetadataWriter {
        WorkspaceMetastoreBindingMetadataWriter::new(storage, metadata_scope())
            .expect("workspace binding metadata writer")
    }

    fn binding_input(
        binding_id: &str,
        metastore_id: &str,
    ) -> WorkspaceMetastoreBindingMetadataInput {
        WorkspaceMetastoreBindingMetadataInput::active(
            binding_id,
            "workspace",
            metastore_id,
            "owner",
            300,
        )
    }

    fn assert_validation_contains<T>(result: Result<T>, expected: &str) {
        match result {
            Err(CatalogError::Validation { message }) => assert!(
                message.contains(expected),
                "expected validation message containing {expected:?}, got {message:?}"
            ),
            Err(error) => {
                panic!("expected validation failure containing {expected}, got {error:?}")
            }
            Ok(_) => panic!("expected validation failure containing {expected}"),
        }
    }

    #[tokio::test]
    async fn binding_write_returns_state_token_and_record() {
        let writer = writer(storage());

        let receipt = writer
            .bind_workspace(binding_input("binding_01", "metastore_01"))
            .await
            .expect("bind workspace");

        assert_eq!(&metadata_scope(), receipt.token().scope());
        assert_eq!(1, receipt.token().logical_sequence());
        assert!(!receipt.token().authority_manifest_id().is_empty());
        assert_eq!("binding_01", receipt.record().binding_id());
        assert_eq!("workspace", receipt.record().workspace_id());
        assert_eq!("metastore_01", receipt.record().metastore_id());
        assert_eq!("owner", receipt.record().owner());
        assert_eq!("active", receipt.record().lifecycle_state().as_str());
        assert_eq!(300, receipt.record().updated_at_ms());
        let encoded = serde_json::to_value(receipt.record()).expect("serialize binding record");
        assert_eq!(Some(&serde_json::json!({})), encoded.get("properties"));
    }

    #[tokio::test]
    async fn duplicate_binding_id_is_rejected() {
        let writer = writer(storage());
        writer
            .bind_workspace(binding_input("binding_01", "metastore_01"))
            .await
            .expect("first binding");

        match writer
            .bind_workspace(binding_input("binding_01", "metastore_02"))
            .await
        {
            Err(CatalogError::AlreadyExists { entity, name }) => {
                assert_eq!("workspace_metastore_binding_metadata", entity);
                assert_eq!("binding_01", name);
            }
            Err(error) => panic!("expected duplicate binding id, got {error:?}"),
            Ok(_) => panic!("duplicate binding id must fail"),
        }
    }

    #[tokio::test]
    async fn duplicate_workspace_metastore_pair_is_rejected() {
        let writer = writer(storage());
        writer
            .bind_workspace(binding_input("binding_01", "metastore_01"))
            .await
            .expect("first binding");

        match writer
            .bind_workspace(binding_input("binding_02", "metastore_01"))
            .await
        {
            Err(CatalogError::PreconditionFailed { message }) => assert!(
                message.contains("workspace/metastore binding metadata pair already exists"),
                "unexpected precondition message: {message:?}"
            ),
            Err(error) => panic!("expected duplicate workspace/metastore pair, got {error:?}"),
            Ok(_) => panic!("duplicate workspace/metastore pair must fail"),
        }
    }

    #[tokio::test]
    async fn concurrent_duplicate_pair_publishes_exactly_one_binding() {
        let (shared_storage, gate) = gated_storage();
        let writer = writer(shared_storage);

        gate.arm();
        let losing_writer = writer.clone();
        let losing_write = tokio::spawn(async move {
            losing_writer
                .bind_workspace(binding_input("binding_01", "metastore_01"))
                .await
        });
        gate.wait_until_blocked().await;

        let winner = writer
            .bind_workspace(binding_input("binding_02", "metastore_01"))
            .await
            .expect("publish duplicate-pair winner");
        gate.release();

        let losing_result = tokio::time::timeout(POINTER_CAS_GATE_TIMEOUT, losing_write)
            .await
            .expect("losing binding write did not finish before timeout")
            .expect("join losing binding write");
        assert!(
            matches!(losing_result, Err(CatalogError::CasFailed { .. })),
            "duplicate-pair pointer loser must report CAS failure: {losing_result:?}"
        );
        assert_eq!(
            None,
            writer
                .read_binding_at(winner.token().clone(), "binding_01")
                .await
                .expect("read losing binding")
        );
        assert_eq!(
            Some(winner.record().clone()),
            writer
                .read_binding_at(winner.token().clone(), "binding_02")
                .await
                .expect("read winning binding")
        );
        match writer
            .bind_workspace(binding_input("binding_03", "metastore_01"))
            .await
        {
            Err(CatalogError::PreconditionFailed { .. }) => {}
            Err(error) => panic!("expected committed pair precondition, got {error:?}"),
            Ok(_) => panic!("a third binding for the committed pair must fail"),
        }
    }

    #[tokio::test]
    async fn binding_workspace_must_match_scope_workspace() {
        let writer = writer(storage());

        match writer
            .bind_workspace(WorkspaceMetastoreBindingMetadataInput::active(
                "binding_01",
                "other_workspace",
                "metastore_01",
                "owner",
                300,
            ))
            .await
        {
            Err(CatalogError::Validation { message }) => assert!(
                message.contains("workspace binding metadata workspace_id must match state scope"),
                "unexpected validation message: {message:?}"
            ),
            Err(error) => panic!("expected workspace mismatch validation, got {error:?}"),
            Ok(_) => panic!("workspace mismatch must fail"),
        }
    }

    #[tokio::test]
    async fn workspace_binding_rejects_invalid_required_metadata() {
        let cases = [
            (
                "binding_id",
                WorkspaceMetastoreBindingMetadataInput::active(
                    " ",
                    "workspace",
                    "metastore_01",
                    "owner",
                    300,
                ),
            ),
            (
                "workspace_id",
                WorkspaceMetastoreBindingMetadataInput::active(
                    "binding_01",
                    " ",
                    "metastore_01",
                    "owner",
                    300,
                ),
            ),
            (
                "metastore_id",
                WorkspaceMetastoreBindingMetadataInput::active(
                    "binding_01",
                    "workspace",
                    " ",
                    "owner",
                    300,
                ),
            ),
            (
                "owner",
                WorkspaceMetastoreBindingMetadataInput::active(
                    "binding_01",
                    "workspace",
                    "metastore_01",
                    " ",
                    300,
                ),
            ),
            (
                "updated_at_ms",
                WorkspaceMetastoreBindingMetadataInput::active(
                    "binding_01",
                    "workspace",
                    "metastore_01",
                    "owner",
                    -1,
                ),
            ),
        ];

        for (field, input) in cases {
            assert_validation_contains(writer(storage()).bind_workspace(input).await, field);
        }
    }

    #[tokio::test]
    async fn read_binding_at_state_token_returns_committed_record() {
        let writer = writer(storage());

        let first = writer
            .bind_workspace(binding_input("binding_01", "metastore_01"))
            .await
            .expect("first binding");
        let second = writer
            .bind_workspace(binding_input("binding_02", "metastore_02"))
            .await
            .expect("second binding");

        assert_eq!(
            Some(first.record().clone()),
            writer
                .read_binding_at(first.token().clone(), "binding_01")
                .await
                .expect("read first token")
        );
        assert_eq!(
            None,
            writer
                .read_binding_at(first.token().clone(), "binding_02")
                .await
                .expect("first token excludes later binding")
        );
        assert_eq!(
            Some(second.record().clone()),
            writer
                .read_binding_at(second.token().clone(), "binding_02")
                .await
                .expect("read second token")
        );
    }

    #[tokio::test]
    async fn binding_status_marks_missing_retained_manifest_unavailable() {
        let shared_storage = storage();
        let writer = writer(shared_storage.clone());
        let receipt = writer
            .bind_workspace(binding_input("binding_01", "metastore_01"))
            .await
            .expect("bind workspace");
        let token = receipt.token().clone();
        let manifest_id = token.authority_manifest_id().to_string();
        let logical_sequence = token.logical_sequence();
        let paths = ControlMvpPaths::new(PATH_GOVERNANCE_METADATA_DOMAIN);
        shared_storage
            .delete(&paths.manifest_object(&manifest_id))
            .await
            .expect("expire retained manifest");

        assert_eq!(
            WorkspaceMetastoreBindingReadStatus::TokenUnavailable {
                manifest_id,
                logical_sequence,
            },
            writer
                .read_binding_at_status(token, "binding_01")
                .await
                .expect("token status")
        );
    }

    #[tokio::test]
    async fn missing_and_stale_compiled_state_deny_closed() {
        let writer = writer(storage());
        let receipt = writer
            .bind_workspace(binding_input("binding_01", "metastore_01"))
            .await
            .expect("bind workspace");
        let compiled = CompiledWorkspaceMetastoreBindingMetadataState::new(StateToken::for_test(
            metadata_scope(),
            0,
            "manifest-compiled",
        ));

        assert_eq!(
            WorkspaceMetastoreBindingCompiledStateStatus::DenyClosedMissing {
                required_sequence: receipt.token().logical_sequence(),
            },
            WorkspaceMetastoreBindingMetadataWriter::compiled_state_status_for(
                receipt.token(),
                None
            )
        );
        assert_eq!(
            WorkspaceMetastoreBindingCompiledStateStatus::DenyClosedStale {
                required_sequence: receipt.token().logical_sequence(),
                compiled_sequence: 0,
            },
            WorkspaceMetastoreBindingMetadataWriter::compiled_state_status_for(
                receipt.token(),
                Some(&compiled)
            )
        );
    }

    #[test]
    fn scope_mismatched_compiled_state_denies_closed() {
        let required_scope = metadata_scope();
        let required = StateToken::for_test(required_scope.clone(), 7, "manifest-required");
        let compiled_scope =
            StateScope::new("tenant", "other-workspace", PATH_GOVERNANCE_METADATA_DOMAIN);
        let compiled = CompiledWorkspaceMetastoreBindingMetadataState::new(StateToken::for_test(
            compiled_scope.clone(),
            7,
            "manifest-compiled",
        ));

        assert_eq!(
            WorkspaceMetastoreBindingCompiledStateStatus::DenyClosedScopeMismatch {
                required_scope,
                compiled_scope,
            },
            WorkspaceMetastoreBindingMetadataWriter::compiled_state_status_for(
                &required,
                Some(&compiled)
            )
        );
    }

    #[tokio::test]
    async fn projection_lag_is_diagnostic_only() {
        let writer = writer(storage());
        let receipt = writer
            .bind_workspace(binding_input("binding_01", "metastore_01"))
            .await
            .expect("bind workspace");
        let compiled = CompiledWorkspaceMetastoreBindingMetadataState::new(receipt.token().clone());

        assert_eq!(
            WorkspaceMetastoreBindingProjectionLag {
                committed_sequence: receipt.token().logical_sequence(),
                latest_projected_sequence: Some(0),
                pending_sequences: Some(receipt.token().logical_sequence()),
            },
            WorkspaceMetastoreBindingMetadataWriter::projection_lag_for(receipt.token(), Some(0))
        );
        assert_eq!(
            WorkspaceMetastoreBindingCompiledStateStatus::Ready {
                required_sequence: receipt.token().logical_sequence(),
                compiled_sequence: receipt.token().logical_sequence(),
            },
            WorkspaceMetastoreBindingMetadataWriter::compiled_state_status_for(
                receipt.token(),
                Some(&compiled)
            )
        );
    }

    #[test]
    fn unsupported_domains_reject_workspace_binding_metadata_writes() {
        for domain in [
            "catalog",
            "grants",
            "storage-governance",
            "credential-vending",
            "projection-outbox-acks",
        ] {
            let unsupported_scope = StateScope::new("tenant", "workspace", domain);

            let Err(error) =
                WorkspaceMetastoreBindingMetadataWriter::new(storage(), unsupported_scope)
            else {
                panic!("unsupported scope {domain} must reject writer creation");
            };

            assert!(
                matches!(error, CatalogError::Validation { .. }),
                "unexpected error for {domain}: {error:?}"
            );
        }
    }
}
