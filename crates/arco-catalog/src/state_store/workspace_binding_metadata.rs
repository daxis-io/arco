use std::collections::BTreeMap;

use arco_core::ScopedStorage;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::{
    ArcoStateReader, ArcoStateTxn, ControlMvpStateStore, StateScope, StateToken, TxnOptions,
};
use crate::error::{CatalogError, Result};
use crate::metastore::events::LifecycleState;
use crate::state_store::path_governance_metadata::PATH_GOVERNANCE_METADATA_DOMAIN;

#[allow(dead_code)]
#[derive(Clone)]
pub struct WorkspaceMetastoreBindingMetadataWriter {
    store: ControlMvpStateStore,
    scope: StateScope,
}

#[allow(dead_code)]
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
        let reader = match self.store.read_at(token.clone()).await {
            Ok(reader) => reader,
            Err(CatalogError::NotFound { .. }) => {
                return Ok(WorkspaceMetastoreBindingReadStatus::TokenUnavailable {
                    manifest_id: token.authority_manifest_id().to_string(),
                    logical_sequence: token.logical_sequence(),
                });
            }
            Err(error) => return Err(error),
        };
        let Some(bytes) = reader.get(&key).await? else {
            return Ok(WorkspaceMetastoreBindingReadStatus::Available(None));
        };
        decode_binding(&bytes)
            .map(|record| WorkspaceMetastoreBindingReadStatus::Available(Some(record)))
    }

    pub(crate) fn projection_lag_for(
        token: &StateToken,
        latest_projected_sequence: Option<u64>,
    ) -> WorkspaceMetastoreBindingProjectionLag {
        let committed_sequence = token.logical_sequence();
        WorkspaceMetastoreBindingProjectionLag {
            committed_sequence,
            latest_projected_sequence,
            pending_sequences: latest_projected_sequence
                .map(|projected| committed_sequence.saturating_sub(projected)),
        }
    }

    pub(crate) fn compiled_state_status_for(
        token: &StateToken,
        compiled: Option<&CompiledWorkspaceMetastoreBindingMetadataState>,
    ) -> WorkspaceMetastoreBindingCompiledStateStatus {
        let required_sequence = token.logical_sequence();
        let Some(compiled) = compiled else {
            return WorkspaceMetastoreBindingCompiledStateStatus::DenyClosedMissing {
                required_sequence,
            };
        };
        if compiled.source_token().scope() != token.scope() {
            return WorkspaceMetastoreBindingCompiledStateStatus::DenyClosedScopeMismatch {
                required_scope: token.scope().clone(),
                compiled_scope: compiled.source_token().scope().clone(),
            };
        }

        let compiled_sequence = compiled.source_token().logical_sequence();
        match compiled_sequence {
            compiled_sequence if compiled_sequence < required_sequence => {
                WorkspaceMetastoreBindingCompiledStateStatus::DenyClosedStale {
                    required_sequence,
                    compiled_sequence,
                }
            }
            compiled_sequence => WorkspaceMetastoreBindingCompiledStateStatus::Ready {
                required_sequence,
                compiled_sequence,
            },
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceMetastoreBindingMetadataInput {
    binding_id: String,
    workspace_id: String,
    metastore_id: String,
    owner: String,
    lifecycle_state: LifecycleState,
    updated_at_ms: i64,
    properties: BTreeMap<String, String>,
}

#[allow(dead_code)]
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
            properties: BTreeMap::new(),
        }
    }

    #[must_use]
    pub(crate) fn with_property(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    #[must_use]
    pub(crate) fn with_properties(mut self, properties: BTreeMap<String, String>) -> Self {
        self.properties.extend(properties);
        self
    }

    #[must_use]
    pub(crate) const fn lifecycle_state(&self) -> LifecycleState {
        self.lifecycle_state
    }

    #[must_use]
    pub(crate) const fn properties(&self) -> &BTreeMap<String, String> {
        &self.properties
    }
}

#[allow(dead_code)]
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

#[allow(dead_code)]
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

    #[must_use]
    pub(crate) const fn properties(&self) -> &BTreeMap<String, String> {
        &self.properties
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
            properties: value.properties,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceMetastoreBindingMetadataReceipt {
    token: StateToken,
    record: WorkspaceMetastoreBindingMetadataRecord,
}

#[allow(dead_code)]
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

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkspaceMetastoreBindingReadStatus {
    Available(Option<WorkspaceMetastoreBindingMetadataRecord>),
    TokenUnavailable {
        manifest_id: String,
        logical_sequence: u64,
    },
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceMetastoreBindingProjectionLag {
    committed_sequence: u64,
    latest_projected_sequence: Option<u64>,
    pending_sequences: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledWorkspaceMetastoreBindingMetadataState {
    source_token: StateToken,
}

#[allow(dead_code)]
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

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkspaceMetastoreBindingCompiledStateStatus {
    Ready {
        required_sequence: u64,
        compiled_sequence: u64,
    },
    DenyClosedMissing {
        required_sequence: u64,
    },
    DenyClosedStale {
        required_sequence: u64,
        compiled_sequence: u64,
    },
    DenyClosedScopeMismatch {
        required_scope: StateScope,
        compiled_scope: StateScope,
    },
}

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
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arco_core::{MemoryBackend, ScopedStorage};

    use super::*;
    use crate::error::CatalogError;
    use crate::metastore::events::LifecycleState;
    use crate::state_store::path_governance_metadata::PATH_GOVERNANCE_METADATA_DOMAIN;
    use crate::state_store::{ControlMvpPaths, StateScope};

    fn metadata_scope() -> StateScope {
        StateScope::new("tenant", "workspace", PATH_GOVERNANCE_METADATA_DOMAIN)
    }

    fn storage() -> ScopedStorage {
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scoped storage")
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
        .with_property("purpose", "tests")
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
        assert_eq!(
            Some("tests"),
            receipt
                .record()
                .properties()
                .get("purpose")
                .map(String::as_str)
        );
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

            let error =
                match WorkspaceMetastoreBindingMetadataWriter::new(storage(), unsupported_scope) {
                    Err(error) => error,
                    Ok(_) => panic!("unsupported scope {domain} must reject writer creation"),
                };

            assert!(
                matches!(error, CatalogError::Validation { .. }),
                "unexpected error for {domain}: {error:?}"
            );
        }
    }

    #[test]
    fn binding_input_properties_are_explicit_metadata() {
        let input = WorkspaceMetastoreBindingMetadataInput::active(
            "binding_01",
            "workspace",
            "metastore_01",
            "owner",
            300,
        )
        .with_properties(BTreeMap::from([(
            "purpose".to_string(),
            "tests".to_string(),
        )]));

        assert_eq!(
            Some("tests"),
            input.properties().get("purpose").map(String::as_str)
        );
        assert_eq!(LifecycleState::Active, input.lifecycle_state());
    }
}
