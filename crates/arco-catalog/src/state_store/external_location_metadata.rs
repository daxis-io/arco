use std::collections::BTreeMap;

use arco_core::ScopedStorage;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::{
    ArcoStateReader, ArcoStateTxn, ControlMvpStateStore, StateScope, StateToken, TxnOptions,
};
use crate::error::{CatalogError, Result};
use crate::metastore::events::LifecycleState;
use crate::state_store::path_governance_metadata::{
    PATH_GOVERNANCE_METADATA_DOMAIN, PathGovernanceDeclaration,
    path_governance_declaration_conflicts, stage_path_governance_declaration,
};
use crate::storage_governance::path_normalization::GovernedPath;

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct ExternalLocationMetadataWriter {
    store: ControlMvpStateStore,
    scope: StateScope,
}

#[allow(dead_code)]
impl ExternalLocationMetadataWriter {
    pub(crate) fn new(storage: ScopedStorage, scope: StateScope) -> Result<Self> {
        if scope.domain() != PATH_GOVERNANCE_METADATA_DOMAIN {
            return Err(validation_failed(format!(
                "external location metadata requires domain {PATH_GOVERNANCE_METADATA_DOMAIN}"
            )));
        }
        let store = ControlMvpStateStore::new(storage, scope.clone())?;
        Ok(Self { store, scope })
    }

    pub(crate) async fn declare_credential_reference(
        &self,
        input: CredentialReferenceMetadataInput,
    ) -> Result<CredentialReferenceMetadataReceipt> {
        let record = CredentialReferenceMetadataRecord::from(input);
        let key = credential_reference_key(record.credential_id());
        let mut txn = self
            .store
            .begin_control_txn(TxnOptions::new(Some(self.scope.clone())))
            .await?;
        if txn.get(&key).await?.is_some() {
            return Err(CatalogError::AlreadyExists {
                entity: "credential_reference_metadata".to_string(),
                name: record.credential_id().to_string(),
            });
        }
        txn.assert_absent(&key).await?;
        txn.put(&key, encode_credential_reference(&record)?).await?;
        let token = txn.commit().await?;
        Ok(CredentialReferenceMetadataReceipt { token, record })
    }

    pub(crate) async fn create_external_location(
        &self,
        input: ExternalLocationMetadataInput,
    ) -> Result<ExternalLocationMetadataReceipt> {
        let governed_path = GovernedPath::parse(&input.raw_uri)?;
        let record =
            ExternalLocationMetadataRecord::from_input(input, governed_path.canonical_uri());
        let credential_key = credential_reference_key(record.credential_id());
        let location_key = external_location_key(record.location_id());
        let path_declaration = PathGovernanceDeclaration::active_from_governed_path(
            record.path_declaration_id().to_string(),
            record.location_id().to_string(),
            "EXTERNAL_LOCATION",
            Some(self.scope.workspace_id().to_string()),
            &governed_path,
            record.owner().to_string(),
        );
        let mut txn = self
            .store
            .begin_control_txn(TxnOptions::new(Some(self.scope.clone())))
            .await?;

        let Some(credential) = txn.get(&credential_key).await? else {
            return Err(CatalogError::NotFound {
                entity: "credential_reference_metadata".to_string(),
                name: record.credential_id().to_string(),
            });
        };
        if let Some(generation) = credential.generation() {
            txn.assert_generation(&credential_key, generation).await?;
        }
        if txn.get(&location_key).await?.is_some() {
            return Err(CatalogError::AlreadyExists {
                entity: "external_location_metadata".to_string(),
                name: record.location_id().to_string(),
            });
        }
        txn.assert_absent(&location_key).await?;
        stage_path_governance_declaration(&mut txn, &self.scope, &path_declaration).await?;
        txn.put(&location_key, encode_external_location(&record)?)
            .await?;

        match txn.commit().await {
            Ok(token) => Ok(ExternalLocationMetadataReceipt {
                token,
                record,
                path_declaration,
            }),
            Err(CatalogError::CasFailed { .. }) => {
                if path_governance_declaration_conflicts(
                    &self.store,
                    &self.scope,
                    &path_declaration,
                )
                .await?
                {
                    Err(precondition_failed(
                        "external location metadata path conflict changed before commit",
                    ))
                } else {
                    Err(CatalogError::CasFailed {
                        message: "external location metadata pointer CAS lost".to_string(),
                    })
                }
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn read_credential_reference_at(
        &self,
        token: StateToken,
        credential_id: &str,
    ) -> Result<Option<CredentialReferenceMetadataRecord>> {
        let key = credential_reference_key(credential_id);
        let reader = self.store.read_at(token).await?;
        let Some(bytes) = reader.get(&key).await? else {
            return Ok(None);
        };
        decode_credential_reference(&bytes).map(Some)
    }

    pub(crate) async fn read_credential_reference_at_status(
        &self,
        token: StateToken,
        credential_id: &str,
    ) -> Result<CredentialReferenceMetadataReadStatus> {
        let key = credential_reference_key(credential_id);
        let reader = match self.store.read_at(token.clone()).await {
            Ok(reader) => reader,
            Err(CatalogError::NotFound { .. }) => {
                return Ok(CredentialReferenceMetadataReadStatus::TokenUnavailable {
                    manifest_id: token.authority_manifest_id().to_string(),
                    logical_sequence: token.logical_sequence(),
                });
            }
            Err(error) => return Err(error),
        };
        let Some(bytes) = reader.get(&key).await? else {
            return Ok(CredentialReferenceMetadataReadStatus::Available(None));
        };
        decode_credential_reference(&bytes)
            .map(|record| CredentialReferenceMetadataReadStatus::Available(Some(record)))
    }

    pub(crate) async fn read_external_location_at(
        &self,
        token: StateToken,
        location_id: &str,
    ) -> Result<Option<ExternalLocationMetadataRecord>> {
        let key = external_location_key(location_id);
        let reader = self.store.read_at(token).await?;
        let Some(bytes) = reader.get(&key).await? else {
            return Ok(None);
        };
        decode_external_location(&bytes).map(Some)
    }

    pub(crate) async fn read_external_location_at_status(
        &self,
        token: StateToken,
        location_id: &str,
    ) -> Result<ExternalLocationMetadataReadStatus> {
        let key = external_location_key(location_id);
        let reader = match self.store.read_at(token.clone()).await {
            Ok(reader) => reader,
            Err(CatalogError::NotFound { .. }) => {
                return Ok(ExternalLocationMetadataReadStatus::TokenUnavailable {
                    manifest_id: token.authority_manifest_id().to_string(),
                    logical_sequence: token.logical_sequence(),
                });
            }
            Err(error) => return Err(error),
        };
        let Some(bytes) = reader.get(&key).await? else {
            return Ok(ExternalLocationMetadataReadStatus::Available(None));
        };
        decode_external_location(&bytes)
            .map(|record| ExternalLocationMetadataReadStatus::Available(Some(record)))
    }

    pub(crate) fn projection_lag_for(
        token: &StateToken,
        latest_projected_sequence: Option<u64>,
    ) -> ExternalLocationProjectionLag {
        let committed_sequence = token.logical_sequence();
        ExternalLocationProjectionLag {
            committed_sequence,
            latest_projected_sequence,
            pending_sequences: latest_projected_sequence
                .map(|projected| committed_sequence.saturating_sub(projected)),
        }
    }

    pub(crate) fn compiled_state_status_for(
        token: &StateToken,
        compiled: Option<&CompiledExternalLocationMetadataState>,
    ) -> ExternalLocationCompiledStateStatus {
        let required_sequence = token.logical_sequence();
        let Some(compiled) = compiled else {
            return ExternalLocationCompiledStateStatus::DenyClosedMissing { required_sequence };
        };
        if compiled.source_token().scope() != token.scope() {
            return ExternalLocationCompiledStateStatus::DenyClosedScopeMismatch {
                required_scope: token.scope().clone(),
                compiled_scope: compiled.source_token().scope().clone(),
            };
        }

        let compiled_sequence = compiled.source_token().logical_sequence();
        match compiled_sequence {
            compiled_sequence if compiled_sequence < required_sequence => {
                ExternalLocationCompiledStateStatus::DenyClosedStale {
                    required_sequence,
                    compiled_sequence,
                }
            }
            compiled_sequence => ExternalLocationCompiledStateStatus::Ready {
                required_sequence,
                compiled_sequence,
            },
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CredentialReferenceMetadataInput {
    credential_id: String,
    name: String,
    cloud: String,
    owner: String,
    lifecycle_state: LifecycleState,
    updated_at_ms: i64,
}

#[allow(dead_code)]
impl CredentialReferenceMetadataInput {
    #[must_use]
    pub(crate) fn active(
        credential_id: impl Into<String>,
        name: impl Into<String>,
        cloud: impl Into<String>,
        owner: impl Into<String>,
        updated_at_ms: i64,
    ) -> Self {
        Self {
            credential_id: credential_id.into(),
            name: name.into(),
            cloud: cloud.into(),
            owner: owner.into(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CredentialReferenceMetadataRecord {
    credential_id: String,
    name: String,
    cloud: String,
    owner: String,
    lifecycle_state: LifecycleState,
    updated_at_ms: i64,
}

#[allow(dead_code)]
impl CredentialReferenceMetadataRecord {
    #[must_use]
    pub(crate) fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub(crate) fn cloud(&self) -> &str {
        &self.cloud
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

impl From<CredentialReferenceMetadataInput> for CredentialReferenceMetadataRecord {
    fn from(value: CredentialReferenceMetadataInput) -> Self {
        Self {
            credential_id: value.credential_id,
            name: value.name,
            cloud: value.cloud,
            owner: value.owner,
            lifecycle_state: value.lifecycle_state,
            updated_at_ms: value.updated_at_ms,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExternalLocationMetadataInput {
    location_id: String,
    name: String,
    raw_uri: String,
    credential_id: String,
    owner: String,
    lifecycle_state: LifecycleState,
    updated_at_ms: i64,
    properties: BTreeMap<String, String>,
}

#[allow(dead_code)]
impl ExternalLocationMetadataInput {
    #[must_use]
    pub(crate) fn active(
        location_id: impl Into<String>,
        name: impl Into<String>,
        raw_uri: impl Into<String>,
        credential_id: impl Into<String>,
        owner: impl Into<String>,
        updated_at_ms: i64,
    ) -> Self {
        Self {
            location_id: location_id.into(),
            name: name.into(),
            raw_uri: raw_uri.into(),
            credential_id: credential_id.into(),
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
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExternalLocationMetadataRecord {
    location_id: String,
    name: String,
    canonical_uri: String,
    credential_id: String,
    path_declaration_id: String,
    owner: String,
    lifecycle_state: LifecycleState,
    updated_at_ms: i64,
    properties: BTreeMap<String, String>,
}

#[allow(dead_code)]
impl ExternalLocationMetadataRecord {
    fn from_input(value: ExternalLocationMetadataInput, canonical_uri: String) -> Self {
        let path_declaration_id = format!("external-location/{}", value.location_id);
        Self {
            location_id: value.location_id,
            name: value.name,
            canonical_uri,
            credential_id: value.credential_id,
            path_declaration_id,
            owner: value.owner,
            lifecycle_state: value.lifecycle_state,
            updated_at_ms: value.updated_at_ms,
            properties: value.properties,
        }
    }

    #[must_use]
    pub(crate) fn location_id(&self) -> &str {
        &self.location_id
    }

    #[must_use]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub(crate) fn canonical_uri(&self) -> &str {
        &self.canonical_uri
    }

    #[must_use]
    pub(crate) fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub(crate) fn path_declaration_id(&self) -> &str {
        &self.path_declaration_id
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

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CredentialReferenceMetadataReceipt {
    token: StateToken,
    record: CredentialReferenceMetadataRecord,
}

#[allow(dead_code)]
impl CredentialReferenceMetadataReceipt {
    #[must_use]
    pub(crate) const fn token(&self) -> &StateToken {
        &self.token
    }

    #[must_use]
    pub(crate) const fn record(&self) -> &CredentialReferenceMetadataRecord {
        &self.record
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExternalLocationMetadataReceipt {
    token: StateToken,
    record: ExternalLocationMetadataRecord,
    path_declaration: PathGovernanceDeclaration,
}

#[allow(dead_code)]
impl ExternalLocationMetadataReceipt {
    #[must_use]
    pub(crate) const fn token(&self) -> &StateToken {
        &self.token
    }

    #[must_use]
    pub(crate) const fn record(&self) -> &ExternalLocationMetadataRecord {
        &self.record
    }

    #[must_use]
    pub(crate) const fn path_declaration(&self) -> &PathGovernanceDeclaration {
        &self.path_declaration
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CredentialReferenceMetadataReadStatus {
    Available(Option<CredentialReferenceMetadataRecord>),
    TokenUnavailable {
        manifest_id: String,
        logical_sequence: u64,
    },
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExternalLocationMetadataReadStatus {
    Available(Option<ExternalLocationMetadataRecord>),
    TokenUnavailable {
        manifest_id: String,
        logical_sequence: u64,
    },
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExternalLocationProjectionLag {
    committed_sequence: u64,
    latest_projected_sequence: Option<u64>,
    pending_sequences: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompiledExternalLocationMetadataState {
    source_token: StateToken,
}

#[allow(dead_code)]
impl CompiledExternalLocationMetadataState {
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
pub(crate) enum ExternalLocationCompiledStateStatus {
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

fn credential_reference_key(credential_id: &str) -> Vec<u8> {
    let mut key = b"external-location-metadata/credential-references/".to_vec();
    push_length_prefixed(&mut key, credential_id.as_bytes());
    key
}

fn external_location_key(location_id: &str) -> Vec<u8> {
    let mut key = b"external-location-metadata/external-locations/".to_vec();
    push_length_prefixed(&mut key, location_id.as_bytes());
    key
}

fn push_length_prefixed(key: &mut Vec<u8>, value: &[u8]) {
    key.extend_from_slice(value.len().to_string().as_bytes());
    key.push(b':');
    key.extend_from_slice(value);
}

fn encode_credential_reference(record: &CredentialReferenceMetadataRecord) -> Result<Bytes> {
    serde_json::to_vec(record)
        .map(Bytes::from)
        .map_err(|error| {
            serialization_failed(format!(
                "credential reference metadata record encode: {error}"
            ))
        })
}

fn decode_credential_reference(bytes: &Bytes) -> Result<CredentialReferenceMetadataRecord> {
    serde_json::from_slice(bytes).map_err(|error| {
        serialization_failed(format!(
            "credential reference metadata record decode: {error}"
        ))
    })
}

fn encode_external_location(record: &ExternalLocationMetadataRecord) -> Result<Bytes> {
    serde_json::to_vec(record)
        .map(Bytes::from)
        .map_err(|error| {
            serialization_failed(format!("external location metadata record encode: {error}"))
        })
}

fn decode_external_location(bytes: &Bytes) -> Result<ExternalLocationMetadataRecord> {
    serde_json::from_slice(bytes).map_err(|error| {
        serialization_failed(format!("external location metadata record decode: {error}"))
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
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;

    use arco_core::{MemoryBackend, ScopedStorage};

    use super::*;
    use crate::authz::privileges::Privilege;
    use crate::credential_vending::{
        CredentialDecision, CredentialOperation, CredentialVendingAuthorization,
        CredentialVendingEngine, CredentialVendingRequest,
    };
    use crate::error::{CatalogError, Result};
    use crate::state_store::path_governance_metadata::{
        PATH_GOVERNANCE_METADATA_DOMAIN, PathGovernanceDeclaration, PathGovernanceMetadataWriter,
    };
    use crate::state_store::{ControlMvpPaths, StateScope};
    use crate::storage_governance::StorageGovernanceState;

    fn metadata_scope() -> StateScope {
        StateScope::new("tenant", "workspace", PATH_GOVERNANCE_METADATA_DOMAIN)
    }

    fn storage() -> ScopedStorage {
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scoped storage")
    }

    fn writer(storage: ScopedStorage) -> ExternalLocationMetadataWriter {
        ExternalLocationMetadataWriter::new(storage, metadata_scope()).expect("metadata writer")
    }

    fn credential_input(id: &str) -> CredentialReferenceMetadataInput {
        CredentialReferenceMetadataInput::active(
            id,
            format!("credential-{id}"),
            "gcs",
            "owner",
            100,
        )
    }

    fn location_input(id: &str, uri: &str) -> ExternalLocationMetadataInput {
        ExternalLocationMetadataInput::active(
            id,
            format!("location-{id}"),
            uri,
            "cred_01",
            "owner",
            200,
        )
        .with_property("purpose", "tests")
    }

    fn phase6a_declaration(id: &str, uri: &str) -> PathGovernanceDeclaration {
        PathGovernanceDeclaration::active(
            id,
            format!("authority-{id}"),
            "EXTERNAL_LOCATION",
            Some("workspace"),
            uri,
            "owner",
        )
        .expect("phase6a declaration")
    }

    async fn declare_credential(
        writer: &ExternalLocationMetadataWriter,
    ) -> CredentialReferenceMetadataReceipt {
        writer
            .declare_credential_reference(credential_input("cred_01"))
            .await
            .expect("declare credential reference")
    }

    fn assert_precondition_failed<T>(result: Result<T>, context: &str) {
        match result {
            Err(CatalogError::PreconditionFailed { .. }) => {}
            Err(error) => panic!("expected precondition failure for {context}, got {error:?}"),
            Ok(_) => panic!("expected precondition failure for {context}"),
        }
    }

    #[tokio::test]
    async fn credential_reference_write_returns_usable_state_token() {
        let writer = writer(storage());

        let receipt = writer
            .declare_credential_reference(credential_input("cred_01"))
            .await
            .expect("declare credential reference");

        assert_eq!(&metadata_scope(), receipt.token().scope());
        assert_eq!(1, receipt.token().logical_sequence());
        assert!(!receipt.token().authority_manifest_id().is_empty());
        assert_eq!("cred_01", receipt.record().credential_id());
        assert_eq!(
            Some(receipt.record().clone()),
            writer
                .read_credential_reference_at(receipt.token().clone(), "cred_01")
                .await
                .expect("read credential at token")
        );
    }

    #[tokio::test]
    async fn duplicate_credential_reference_id_is_rejected() {
        let writer = writer(storage());
        writer
            .declare_credential_reference(credential_input("cred_01"))
            .await
            .expect("declare credential reference");

        match writer
            .declare_credential_reference(credential_input("cred_01"))
            .await
        {
            Err(CatalogError::AlreadyExists { entity, name }) => {
                assert_eq!("credential_reference_metadata", entity);
                assert_eq!("cred_01", name);
            }
            Err(error) => panic!("expected duplicate credential rejection, got {error:?}"),
            Ok(_) => panic!("duplicate credential reference must fail"),
        }
    }

    #[tokio::test]
    async fn external_location_create_returns_usable_state_token() {
        let writer = writer(storage());
        declare_credential(&writer).await;

        let receipt = writer
            .create_external_location(location_input("loc_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("create external location");

        assert_eq!(&metadata_scope(), receipt.token().scope());
        assert_eq!(2, receipt.token().logical_sequence());
        assert!(!receipt.token().authority_manifest_id().is_empty());
        assert_eq!("loc_orders", receipt.record().location_id());
        assert_eq!("cred_01", receipt.record().credential_id());
        assert_eq!(
            "external-location/loc_orders",
            receipt.record().path_declaration_id()
        );
        assert_eq!(
            "EXTERNAL_LOCATION",
            receipt.path_declaration().authority_object_type()
        );
        assert_eq!(
            "loc_orders",
            receipt.path_declaration().authority_object_id()
        );
        assert_eq!(
            "gs://bucket/warehouse/orders/",
            receipt.record().canonical_uri()
        );
    }

    #[tokio::test]
    async fn duplicate_external_location_id_is_rejected() {
        let writer = writer(storage());
        declare_credential(&writer).await;
        writer
            .create_external_location(location_input("loc_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("create external location");

        match writer
            .create_external_location(location_input(
                "loc_orders",
                "gs://bucket/warehouse/orders-duplicate",
            ))
            .await
        {
            Err(CatalogError::AlreadyExists { entity, name }) => {
                assert_eq!("external_location_metadata", entity);
                assert_eq!("loc_orders", name);
            }
            Err(error) => panic!("expected duplicate location rejection, got {error:?}"),
            Ok(_) => panic!("duplicate external location must fail"),
        }
    }

    #[tokio::test]
    async fn external_location_companion_path_declaration_is_readable_at_same_token() {
        let shared_storage = storage();
        let writer = writer(shared_storage.clone());
        let path_writer = PathGovernanceMetadataWriter::new(shared_storage, metadata_scope())
            .expect("path writer");
        declare_credential(&writer).await;

        let receipt = writer
            .create_external_location(location_input("loc_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("create external location");

        assert_eq!(
            Some(receipt.path_declaration().clone()),
            path_writer
                .read_declaration_at(
                    receipt.token().clone(),
                    receipt.record().path_declaration_id(),
                )
                .await
                .expect("read companion path declaration")
        );
    }

    #[tokio::test]
    async fn external_location_percent_escaped_path_is_canonicalized_once() {
        let writer = writer(storage());
        declare_credential(&writer).await;

        let receipt = writer
            .create_external_location(location_input(
                "loc_percent",
                "gs://bucket/warehouse/100%25-complete",
            ))
            .await
            .expect("create percent-bearing external location");

        assert_eq!(
            "gs://bucket/warehouse/100%-complete/",
            receipt.record().canonical_uri()
        );
        assert_eq!(
            receipt.record().canonical_uri(),
            receipt.path_declaration().canonical_uri()
        );
    }

    #[tokio::test]
    async fn read_external_location_at_state_token_returns_committed_record() {
        let writer = writer(storage());
        declare_credential(&writer).await;

        let orders = writer
            .create_external_location(location_input("loc_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("create orders");
        let customers = writer
            .create_external_location(location_input(
                "loc_customers",
                "gs://bucket/warehouse/customers",
            ))
            .await
            .expect("create customers");

        assert_eq!(
            Some(orders.record().clone()),
            writer
                .read_external_location_at(orders.token().clone(), "loc_orders")
                .await
                .expect("read orders at orders token")
        );
        assert_eq!(
            None,
            writer
                .read_external_location_at(orders.token().clone(), "loc_customers")
                .await
                .expect("orders token excludes later customers")
        );
        assert_eq!(
            Some(customers.record().clone()),
            writer
                .read_external_location_at(customers.token().clone(), "loc_customers")
                .await
                .expect("read customers at customers token")
        );
    }

    #[tokio::test]
    async fn credential_reference_serialization_has_exact_non_secret_fields() {
        let writer = writer(storage());

        let receipt = writer
            .declare_credential_reference(credential_input("cred_01"))
            .await
            .expect("declare credential reference");
        let encoded = serde_json::to_value(receipt.record()).expect("serialize credential record");
        let fields = encoded
            .as_object()
            .expect("credential record object")
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();

        assert_eq!(
            BTreeSet::from([
                "cloud",
                "credential_id",
                "lifecycle_state",
                "name",
                "owner",
                "updated_at_ms",
            ]),
            fields
        );
    }

    #[tokio::test]
    async fn credential_reference_is_readable_at_external_location_token() {
        let writer = writer(storage());
        let credential = declare_credential(&writer).await;

        let location = writer
            .create_external_location(location_input("loc_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("create external location");

        assert_eq!(
            Some(credential.record().clone()),
            writer
                .read_credential_reference_at(location.token().clone(), "cred_01")
                .await
                .expect("read credential reference at location token")
        );
    }

    #[tokio::test]
    async fn exact_normalized_phase6a_path_conflicts_with_external_location() {
        let shared_storage = storage();
        let path_writer =
            PathGovernanceMetadataWriter::new(shared_storage.clone(), metadata_scope())
                .expect("path writer");
        let writer = writer(shared_storage);
        declare_credential(&writer).await;
        path_writer
            .declare_path(phase6a_declaration(
                "decl_orders",
                "gs://Bucket//warehouse/orders",
            ))
            .await
            .expect("declare phase6a path");

        assert_precondition_failed(
            writer
                .create_external_location(location_input(
                    "loc_orders",
                    "gs://bucket/warehouse/orders/",
                ))
                .await,
            "normalized phase6a path conflicts with external location",
        );
    }

    #[tokio::test]
    async fn external_location_child_path_conflicts_with_phase6a_ancestor_declaration() {
        let shared_storage = storage();
        let path_writer =
            PathGovernanceMetadataWriter::new(shared_storage.clone(), metadata_scope())
                .expect("path writer");
        let writer = writer(shared_storage);
        declare_credential(&writer).await;
        path_writer
            .declare_path(phase6a_declaration(
                "decl_orders",
                "gs://bucket/warehouse/orders",
            ))
            .await
            .expect("declare phase6a ancestor");

        assert_precondition_failed(
            writer
                .create_external_location(location_input(
                    "loc_child",
                    "gs://bucket/warehouse/orders/2026",
                ))
                .await,
            "phase6a ancestor conflicts with phase6b child",
        );
    }

    #[tokio::test]
    async fn external_location_parent_path_conflicts_with_phase6a_descendant_declaration() {
        let shared_storage = storage();
        let path_writer =
            PathGovernanceMetadataWriter::new(shared_storage.clone(), metadata_scope())
                .expect("path writer");
        let writer = writer(shared_storage);
        declare_credential(&writer).await;
        path_writer
            .declare_path(phase6a_declaration(
                "decl_child",
                "gs://bucket/warehouse/orders/2026",
            ))
            .await
            .expect("declare phase6a child");

        assert_precondition_failed(
            writer
                .create_external_location(location_input(
                    "loc_parent",
                    "gs://bucket/warehouse/orders",
                ))
                .await,
            "phase6a descendant conflicts with phase6b parent",
        );
    }

    #[tokio::test]
    async fn non_overlapping_external_location_paths_are_accepted() {
        let writer = writer(storage());
        declare_credential(&writer).await;

        let orders = writer
            .create_external_location(location_input("loc_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("orders location");
        let archive = writer
            .create_external_location(location_input(
                "loc_orders_archive",
                "gs://bucket/warehouse/orders-archive",
            ))
            .await
            .expect("orders archive location");

        assert_eq!(2, orders.token().logical_sequence());
        assert_eq!(3, archive.token().logical_sequence());
    }

    #[tokio::test]
    async fn missing_and_stale_compiled_state_deny_closed() {
        let writer = writer(storage());
        declare_credential(&writer).await;
        let receipt = writer
            .create_external_location(location_input("loc_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("create external location");

        assert_eq!(
            ExternalLocationCompiledStateStatus::DenyClosedMissing {
                required_sequence: receipt.token().logical_sequence(),
            },
            ExternalLocationMetadataWriter::compiled_state_status_for(receipt.token(), None)
        );
        assert_eq!(
            ExternalLocationCompiledStateStatus::DenyClosedStale {
                required_sequence: receipt.token().logical_sequence(),
                compiled_sequence: 1,
            },
            ExternalLocationMetadataWriter::compiled_state_status_for(
                receipt.token(),
                Some(&CompiledExternalLocationMetadataState::new(
                    StateToken::for_test(metadata_scope(), 1, "manifest-compiled")
                ))
            )
        );
    }

    #[test]
    fn scope_mismatched_compiled_state_denies_closed() {
        let required_scope = metadata_scope();
        let required = StateToken::for_test(required_scope.clone(), 2, "manifest-required");
        let compiled_scope =
            StateScope::new("tenant", "other-workspace", PATH_GOVERNANCE_METADATA_DOMAIN);
        let compiled = CompiledExternalLocationMetadataState::new(StateToken::for_test(
            compiled_scope.clone(),
            2,
            "manifest-compiled",
        ));

        assert_eq!(
            ExternalLocationCompiledStateStatus::DenyClosedScopeMismatch {
                required_scope,
                compiled_scope,
            },
            ExternalLocationMetadataWriter::compiled_state_status_for(&required, Some(&compiled))
        );
    }

    #[tokio::test]
    async fn projection_lag_does_not_change_compiled_state_readiness() {
        let writer = writer(storage());
        declare_credential(&writer).await;
        let receipt = writer
            .create_external_location(location_input("loc_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("create external location");

        assert_eq!(
            ExternalLocationProjectionLag {
                committed_sequence: receipt.token().logical_sequence(),
                latest_projected_sequence: Some(0),
                pending_sequences: Some(receipt.token().logical_sequence()),
            },
            ExternalLocationMetadataWriter::projection_lag_for(receipt.token(), Some(0))
        );
        assert_eq!(
            ExternalLocationCompiledStateStatus::Ready {
                required_sequence: receipt.token().logical_sequence(),
                compiled_sequence: receipt.token().logical_sequence(),
            },
            ExternalLocationMetadataWriter::compiled_state_status_for(
                receipt.token(),
                Some(&CompiledExternalLocationMetadataState::new(
                    receipt.token().clone()
                ))
            )
        );
    }

    #[tokio::test]
    async fn credential_vending_ignores_phase6b_metadata() {
        let writer = writer(storage());
        declare_credential(&writer).await;
        writer
            .create_external_location(location_input("loc_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("create external location metadata");

        let decision = CredentialVendingEngine::default()
            .decide_path(
                &StorageGovernanceState::default(),
                &CredentialVendingRequest {
                    principal_id: "principal".to_string(),
                    groups_snapshot_version: "groups-1".to_string(),
                    workspace_id: "workspace".to_string(),
                    request_id: "request-1".to_string(),
                    operation: CredentialOperation::Read,
                    requested_path: "gs://bucket/warehouse/orders/part-1.parquet".to_string(),
                    requested_ttl: Duration::from_secs(300),
                    client_kind: "uc".to_string(),
                    catalog_snapshot_version: "watermark-1".to_string(),
                    authorization: Some(CredentialVendingAuthorization {
                        principal_id: "principal".to_string(),
                        object_id: "loc_orders".to_string(),
                        object_type: "EXTERNAL_LOCATION".to_string(),
                        privilege: Privilege::ReadFiles,
                        permission_ledger_watermark: "watermark-1".to_string(),
                        path_authority_object_id: "loc_orders".to_string(),
                        path_authority_object_type: "EXTERNAL_LOCATION".to_string(),
                    }),
                },
            )
            .expect("credential vending decision");

        assert_eq!(CredentialDecision::Deny, decision.decision);
        assert_eq!("path_not_governed", decision.reason_code);
    }

    #[test]
    fn unsupported_domains_reject_phase6b_metadata_writes() {
        for domain in [
            "catalog",
            "grants",
            "storage-governance",
            "credential-vending",
            "projection-outbox-acks",
        ] {
            let unsupported_scope = StateScope::new("tenant", "workspace", domain);

            let error = match ExternalLocationMetadataWriter::new(storage(), unsupported_scope) {
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
    async fn external_location_requires_existing_credential_reference() {
        let writer = writer(storage());

        match writer
            .create_external_location(location_input("loc_orders", "gs://bucket/warehouse/orders"))
            .await
        {
            Err(CatalogError::NotFound { entity, name }) => {
                assert_eq!("credential_reference_metadata", entity);
                assert_eq!("cred_01", name);
            }
            Err(error) => panic!("expected missing credential reference, got {error:?}"),
            Ok(_) => panic!("external location without credential reference must fail"),
        }
    }

    #[tokio::test]
    async fn credential_reference_status_marks_missing_retained_manifest_unavailable() {
        let shared_storage = storage();
        let writer = writer(shared_storage.clone());
        let receipt = declare_credential(&writer).await;
        let token = receipt.token().clone();
        let manifest_id = token.authority_manifest_id().to_string();
        let logical_sequence = token.logical_sequence();
        let paths = ControlMvpPaths::new(PATH_GOVERNANCE_METADATA_DOMAIN);
        shared_storage
            .delete(&paths.manifest_object(&manifest_id))
            .await
            .expect("expire retained manifest");

        assert_eq!(
            CredentialReferenceMetadataReadStatus::TokenUnavailable {
                manifest_id,
                logical_sequence,
            },
            writer
                .read_credential_reference_at_status(token, "cred_01")
                .await
                .expect("token status")
        );
    }

    #[tokio::test]
    async fn external_location_status_marks_missing_retained_manifest_unavailable() {
        let shared_storage = storage();
        let writer = writer(shared_storage.clone());
        declare_credential(&writer).await;
        let receipt = writer
            .create_external_location(location_input("loc_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("create external location");
        let token = receipt.token().clone();
        let manifest_id = token.authority_manifest_id().to_string();
        let logical_sequence = token.logical_sequence();
        let paths = ControlMvpPaths::new(PATH_GOVERNANCE_METADATA_DOMAIN);
        shared_storage
            .delete(&paths.manifest_object(&manifest_id))
            .await
            .expect("expire retained manifest");

        assert_eq!(
            ExternalLocationMetadataReadStatus::TokenUnavailable {
                manifest_id,
                logical_sequence,
            },
            writer
                .read_external_location_at_status(token, "loc_orders")
                .await
                .expect("token status")
        );
    }
}
