use arco_core::ScopedStorage;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::{
    ArcoStateReader, ArcoStateTxn, ControlMvpStateStore, KeyRange, PredicateInputSet, StateScope,
    StateToken, TxnOptions,
};
use crate::error::{CatalogError, Result};
use crate::metastore::events::LifecycleState;
use crate::storage_governance::path_normalization::GovernedPath;

#[allow(dead_code)]
pub(crate) const PATH_GOVERNANCE_METADATA_DOMAIN: &str = "path-governance-metadata";
const SCHEMA_VERSION: u32 = 1;

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct PathGovernanceMetadataWriter {
    store: ControlMvpStateStore,
    scope: StateScope,
}

#[allow(dead_code)]
impl PathGovernanceMetadataWriter {
    pub(crate) fn new(storage: ScopedStorage, scope: StateScope) -> Result<Self> {
        if scope.domain() != PATH_GOVERNANCE_METADATA_DOMAIN {
            return Err(validation_failed(format!(
                "path governance metadata requires domain {PATH_GOVERNANCE_METADATA_DOMAIN}"
            )));
        }
        let store = ControlMvpStateStore::new(storage, scope.clone())?;
        Ok(Self { store, scope })
    }

    pub(crate) async fn declare_path(
        &self,
        write: PathGovernanceMetadataWrite,
    ) -> Result<PathGovernanceMetadataReceipt> {
        let inputs = self.compile_inputs(write.clone()).await?;
        self.declare_path_with_inputs(write, inputs).await
    }

    pub(crate) async fn compile_inputs(
        &self,
        write: PathGovernanceMetadataWrite,
    ) -> Result<PathGovernanceMetadataInputs> {
        let record = PathGovernanceMetadataRecord::from_write(write)?;
        let keys = MetadataKeys::new(record.declaration_id(), record.canonical_uri())?;
        let mut txn = self
            .store
            .begin_control_txn(TxnOptions::new(Some(self.scope.clone())))
            .await?;
        let predicate_inputs = txn
            .read_set(
                &keys.predicate_point_keys(),
                &[keys.descendant_range.clone()],
            )
            .await?;

        Ok(PathGovernanceMetadataInputs {
            scope: self.scope.clone(),
            declaration_id: record.declaration_id().to_string(),
            canonical_uri: record.canonical_uri().to_string(),
            predicate_inputs,
        })
    }

    pub(crate) async fn declare_path_with_inputs(
        &self,
        write: PathGovernanceMetadataWrite,
        inputs: PathGovernanceMetadataInputs,
    ) -> Result<PathGovernanceMetadataReceipt> {
        if inputs.scope != self.scope {
            return Err(validation_failed(
                "path governance metadata compiled inputs scope does not match writer",
            ));
        }

        let record = PathGovernanceMetadataRecord::from_write(write)?;
        if inputs.declaration_id != record.declaration_id()
            || inputs.canonical_uri != record.canonical_uri()
        {
            return Err(validation_failed(
                "path governance metadata compiled inputs do not match write",
            ));
        }

        let keys = MetadataKeys::new(record.declaration_id(), record.canonical_uri())?;
        let mut txn = self
            .store
            .begin_control_txn(TxnOptions::new(Some(self.scope.clone())))
            .await?;
        txn.assert_inputs_unchanged(inputs.predicate_inputs).await?;

        if txn.get(&keys.record_key).await?.is_some() {
            return Err(CatalogError::AlreadyExists {
                entity: "path_governance_metadata".to_string(),
                name: record.declaration_id().to_string(),
            });
        }
        if txn.get(&keys.exact_path_key).await?.is_some() {
            return Err(precondition_failed(
                "exact path governance metadata conflict",
            ));
        }
        for ancestor_key in &keys.ancestor_path_keys {
            if txn.get(ancestor_key).await?.is_some() {
                return Err(precondition_failed(
                    "ancestor path governance metadata conflict",
                ));
            }
        }
        if !txn.scan_prefix(&keys.exact_path_key).await?.is_empty() {
            return Err(precondition_failed(
                "descendant path governance metadata conflict",
            ));
        }

        txn.assert_absent(&keys.record_key).await?;
        txn.assert_absent(&keys.exact_path_key).await?;
        txn.assert_range_empty(keys.descendant_range).await?;
        txn.put(&keys.record_key, encode_record(&record)?).await?;
        txn.put(
            &keys.exact_path_key,
            Bytes::from(record.declaration_id().to_string()),
        )
        .await?;
        let token = txn.commit().await?;

        Ok(PathGovernanceMetadataReceipt { token, record })
    }

    pub(crate) async fn read_declaration_at(
        &self,
        token: StateToken,
        declaration_id: &str,
    ) -> Result<Option<PathGovernanceMetadataRecord>> {
        let key = declaration_key(declaration_id);
        let reader = self.store.read_at(token).await?;
        let Some(bytes) = reader.get(&key).await? else {
            return Ok(None);
        };
        decode_record(&bytes).map(Some)
    }

    pub(crate) fn projection_lag_for(
        token: &StateToken,
        latest_projected_sequence: Option<u64>,
    ) -> PathGovernanceProjectionLag {
        let committed_sequence = token.logical_sequence();
        PathGovernanceProjectionLag {
            committed_sequence,
            latest_projected_sequence,
            pending_sequences: latest_projected_sequence
                .map(|projected| committed_sequence.saturating_sub(projected)),
        }
    }

    pub(crate) fn compiled_enforcement_readiness(
        required: &StateToken,
        compiled: Option<&CompiledPathGovernanceMetadataState>,
    ) -> PathGovernanceReadiness {
        let Some(compiled) = compiled else {
            return PathGovernanceReadiness::DenyClosed(
                PathGovernanceReadinessReason::MissingCompiledState,
            );
        };

        if &compiled.scope != required.scope() || compiled.token.scope() != required.scope() {
            return PathGovernanceReadiness::DenyClosed(
                PathGovernanceReadinessReason::ScopeMismatch,
            );
        }

        let required_sequence = required.logical_sequence();
        let compiled_sequence = compiled.token.logical_sequence();
        if compiled_sequence < required_sequence {
            return PathGovernanceReadiness::DenyClosed(
                PathGovernanceReadinessReason::StaleCompiledState {
                    required_sequence,
                    compiled_sequence,
                },
            );
        }

        PathGovernanceReadiness::Ready {
            required_sequence,
            compiled_sequence,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PathGovernanceMetadataWrite {
    declaration_id: String,
    name: String,
    raw_uri: String,
    owner: String,
    workspace_id: Option<String>,
}

#[allow(dead_code)]
impl PathGovernanceMetadataWrite {
    #[must_use]
    pub(crate) fn new(
        declaration_id: impl Into<String>,
        name: impl Into<String>,
        raw_uri: impl Into<String>,
        owner: impl Into<String>,
    ) -> Self {
        Self {
            declaration_id: declaration_id.into(),
            name: name.into(),
            raw_uri: raw_uri.into(),
            owner: owner.into(),
            workspace_id: None,
        }
    }

    #[must_use]
    pub(crate) fn with_workspace_id(mut self, workspace_id: impl Into<String>) -> Self {
        self.workspace_id = Some(workspace_id.into());
        self
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PathGovernanceMetadataRecord {
    schema_version: u32,
    declaration_id: String,
    name: String,
    canonical_uri: String,
    owner: String,
    workspace_id: Option<String>,
    lifecycle_state: LifecycleState,
}

#[allow(dead_code)]
impl PathGovernanceMetadataRecord {
    fn from_write(write: PathGovernanceMetadataWrite) -> Result<Self> {
        let governed_path = GovernedPath::parse(&write.raw_uri)?;
        Ok(Self {
            schema_version: SCHEMA_VERSION,
            declaration_id: write.declaration_id,
            name: write.name,
            canonical_uri: governed_path.canonical_uri(),
            owner: write.owner,
            workspace_id: write.workspace_id,
            lifecycle_state: LifecycleState::Active,
        })
    }

    #[must_use]
    pub(crate) const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    #[must_use]
    pub(crate) fn declaration_id(&self) -> &str {
        &self.declaration_id
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
    pub(crate) fn owner(&self) -> &str {
        &self.owner
    }

    #[must_use]
    pub(crate) fn workspace_id(&self) -> Option<&str> {
        self.workspace_id.as_deref()
    }

    #[must_use]
    pub(crate) const fn lifecycle_state(&self) -> LifecycleState {
        self.lifecycle_state
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct PathGovernanceMetadataInputs {
    scope: StateScope,
    declaration_id: String,
    canonical_uri: String,
    predicate_inputs: PredicateInputSet,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PathGovernanceMetadataReceipt {
    token: StateToken,
    record: PathGovernanceMetadataRecord,
}

#[allow(dead_code)]
impl PathGovernanceMetadataReceipt {
    #[must_use]
    pub(crate) const fn token(&self) -> &StateToken {
        &self.token
    }

    #[must_use]
    pub(crate) const fn record(&self) -> &PathGovernanceMetadataRecord {
        &self.record
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompiledPathGovernanceMetadataState {
    scope: StateScope,
    token: StateToken,
}

#[allow(dead_code)]
impl CompiledPathGovernanceMetadataState {
    #[must_use]
    pub(crate) const fn new(scope: StateScope, token: StateToken) -> Self {
        Self { scope, token }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PathGovernanceReadiness {
    Ready {
        required_sequence: u64,
        compiled_sequence: u64,
    },
    DenyClosed(PathGovernanceReadinessReason),
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PathGovernanceReadinessReason {
    MissingCompiledState,
    ScopeMismatch,
    StaleCompiledState {
        required_sequence: u64,
        compiled_sequence: u64,
    },
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PathGovernanceProjectionLag {
    committed_sequence: u64,
    latest_projected_sequence: Option<u64>,
    pending_sequences: Option<u64>,
}

struct MetadataKeys {
    record_key: Vec<u8>,
    exact_path_key: Vec<u8>,
    ancestor_path_keys: Vec<Vec<u8>>,
    descendant_range: KeyRange,
}

impl MetadataKeys {
    fn new(declaration_id: &str, canonical_uri: &str) -> Result<Self> {
        let governed_path = GovernedPath::parse(canonical_uri)?;
        let exact_path_key = path_index_key(canonical_uri);
        let ancestor_path_keys = governed_path
            .canonical_ancestor_uris()
            .into_iter()
            .map(|ancestor_uri| path_index_key(&ancestor_uri))
            .collect();
        let descendant_range = descendant_range(canonical_uri);

        Ok(Self {
            record_key: declaration_key(declaration_id),
            exact_path_key,
            ancestor_path_keys,
            descendant_range,
        })
    }

    fn predicate_point_keys(&self) -> Vec<Vec<u8>> {
        let mut keys = Vec::with_capacity(self.ancestor_path_keys.len() + 2);
        keys.push(self.record_key.clone());
        keys.push(self.exact_path_key.clone());
        keys.extend(self.ancestor_path_keys.iter().cloned());
        keys
    }
}

fn declaration_key(declaration_id: &str) -> Vec<u8> {
    let mut key = b"path-governance-metadata/declarations/".to_vec();
    push_length_prefixed(&mut key, declaration_id.as_bytes());
    key
}

fn path_index_key(canonical_uri: &str) -> Vec<u8> {
    let mut key = b"path-governance-metadata/path-index/".to_vec();
    key.extend_from_slice(canonical_uri.as_bytes());
    key
}

fn descendant_range(canonical_uri: &str) -> KeyRange {
    let start = path_index_key(canonical_uri);
    let mut end = start.clone();
    end.push(0xff);
    KeyRange::new(start, end)
}

fn push_length_prefixed(key: &mut Vec<u8>, value: &[u8]) {
    key.extend_from_slice(value.len().to_string().as_bytes());
    key.push(b':');
    key.extend_from_slice(value);
}

fn encode_record(record: &PathGovernanceMetadataRecord) -> Result<Bytes> {
    serde_json::to_vec(record)
        .map(Bytes::from)
        .map_err(|error| {
            serialization_failed(format!("path governance metadata record encode: {error}"))
        })
}

fn decode_record(bytes: &Bytes) -> Result<PathGovernanceMetadataRecord> {
    serde_json::from_slice(bytes).map_err(|error| {
        serialization_failed(format!("path governance metadata record decode: {error}"))
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
    use std::time::Duration;

    use arco_core::{MemoryBackend, ScopedStorage};

    use super::*;
    use crate::authz::privileges::Privilege;
    use crate::credential_vending::{
        CredentialDecision, CredentialOperation, CredentialVendingAuthorization,
        CredentialVendingEngine, CredentialVendingRequest,
    };
    use crate::error::{CatalogError, Result};
    use crate::metastore::events::LifecycleState;
    use crate::state_store::StateScope;
    use crate::storage_governance::StorageGovernanceState;

    fn metadata_scope() -> StateScope {
        StateScope::new("tenant", "workspace", PATH_GOVERNANCE_METADATA_DOMAIN)
    }

    fn other_scope(domain: &str) -> StateScope {
        StateScope::new("tenant", "workspace", domain)
    }

    fn storage() -> ScopedStorage {
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scoped storage")
    }

    fn writer(storage: ScopedStorage) -> PathGovernanceMetadataWriter {
        PathGovernanceMetadataWriter::new(storage, metadata_scope()).expect("metadata writer")
    }

    fn declaration(id: &str, raw_uri: &str) -> PathGovernanceMetadataWrite {
        PathGovernanceMetadataWrite::new(id, format!("path-{id}"), raw_uri, "owner")
            .with_workspace_id("workspace")
    }

    fn assert_precondition<T>(result: Result<T>, expected: &str) {
        match result {
            Err(CatalogError::PreconditionFailed { message }) => {
                assert!(
                    message.contains(expected),
                    "expected precondition containing {expected:?}, got {message:?}"
                );
            }
            Err(error) => panic!("expected PreconditionFailed for {expected}, got {error:?}"),
            Ok(_) => panic!("expected PreconditionFailed for {expected}"),
        }
    }

    fn assert_validation<T>(result: Result<T>, expected: &str) {
        match result {
            Err(CatalogError::Validation { message }) => {
                assert!(
                    message.contains(expected),
                    "expected validation containing {expected:?}, got {message:?}"
                );
            }
            Err(error) => panic!("expected Validation for {expected}, got {error:?}"),
            Ok(_) => panic!("expected Validation for {expected}"),
        }
    }

    #[tokio::test]
    async fn successful_declaration_returns_state_token() {
        let writer = writer(storage());

        let receipt = writer
            .declare_path(declaration("orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("declare path");

        assert_eq!(&metadata_scope(), receipt.token().scope());
        assert_eq!(1, receipt.token().logical_sequence());
        assert!(!receipt.token().authority_manifest_id().is_empty());
        assert_eq!("orders", receipt.record().declaration_id());
        assert_eq!("path-orders", receipt.record().name());
        assert_eq!(
            "gs://bucket/warehouse/orders/",
            receipt.record().canonical_uri()
        );
        assert_eq!("owner", receipt.record().owner());
        assert_eq!(Some("workspace"), receipt.record().workspace_id());
        assert_eq!(LifecycleState::Active, receipt.record().lifecycle_state());
    }

    #[tokio::test]
    async fn read_declaration_at_state_token_returns_committed_record() {
        let writer = writer(storage());

        let first = writer
            .declare_path(declaration("orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("first declaration");
        let second = writer
            .declare_path(declaration("customers", "gs://bucket/warehouse/customers"))
            .await
            .expect("second declaration");

        assert_eq!(
            Some(first.record().clone()),
            writer
                .read_declaration_at(first.token().clone(), "orders")
                .await
                .expect("read first token")
        );
        assert_eq!(
            None,
            writer
                .read_declaration_at(first.token().clone(), "customers")
                .await
                .expect("first token excludes later declaration")
        );
        assert_eq!(
            Some(second.record().clone()),
            writer
                .read_declaration_at(second.token().clone(), "customers")
                .await
                .expect("read second token")
        );
    }

    #[tokio::test]
    async fn ancestor_conflict_is_rejected() {
        let writer = writer(storage());
        writer
            .declare_path(declaration("warehouse", "gs://bucket/warehouse"))
            .await
            .expect("seed ancestor");

        assert_precondition(
            writer
                .declare_path(declaration("orders", "gs://bucket/warehouse/orders"))
                .await,
            "ancestor",
        );
    }

    #[tokio::test]
    async fn descendant_conflict_is_rejected() {
        let writer = writer(storage());
        writer
            .declare_path(declaration("orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("seed descendant");

        assert_precondition(
            writer
                .declare_path(declaration("warehouse", "gs://bucket/warehouse"))
                .await,
            "descendant",
        );
    }

    #[tokio::test]
    async fn non_overlapping_paths_are_accepted() {
        let writer = writer(storage());

        let first = writer
            .declare_path(declaration("orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("orders declaration");
        let sibling = writer
            .declare_path(declaration(
                "orders_archive",
                "gs://bucket/warehouse/orders-archive",
            ))
            .await
            .expect("sibling declaration");
        let other_bucket = writer
            .declare_path(declaration(
                "orders_other",
                "gs://other-bucket/warehouse/orders",
            ))
            .await
            .expect("other bucket declaration");

        assert_eq!(1, first.token().logical_sequence());
        assert_eq!(2, sibling.token().logical_sequence());
        assert_eq!(3, other_bucket.token().logical_sequence());
    }

    #[tokio::test]
    async fn range_empty_blocks_existing_descendant_conflict() {
        let writer = writer(storage());
        writer
            .declare_path(declaration("orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("seed descendant");

        assert_precondition(
            writer
                .declare_path(declaration("warehouse", "gs://bucket/warehouse"))
                .await,
            "descendant",
        );
    }

    #[tokio::test]
    async fn range_unchanged_catches_stale_compiled_assumptions() {
        let writer = writer(storage());
        let write = declaration("warehouse", "gs://bucket/warehouse");
        let inputs = writer
            .compile_inputs(write.clone())
            .await
            .expect("compile empty conflict inputs");

        writer
            .declare_path(declaration("orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("concurrent descendant declaration");

        assert_precondition(
            writer.declare_path_with_inputs(write, inputs).await,
            "stale",
        );
    }

    #[test]
    fn missing_compiled_state_denies_closed() {
        let required = required_token(7);

        assert_eq!(
            PathGovernanceReadiness::DenyClosed(
                PathGovernanceReadinessReason::MissingCompiledState
            ),
            PathGovernanceMetadataWriter::compiled_enforcement_readiness(&required, None)
        );
    }

    #[test]
    fn stale_compiled_state_denies_closed() {
        let required = required_token(7);
        let compiled =
            CompiledPathGovernanceMetadataState::new(metadata_scope(), required_token(6));

        assert_eq!(
            PathGovernanceReadiness::DenyClosed(
                PathGovernanceReadinessReason::StaleCompiledState {
                    required_sequence: 7,
                    compiled_sequence: 6,
                }
            ),
            PathGovernanceMetadataWriter::compiled_enforcement_readiness(
                &required,
                Some(&compiled)
            )
        );
    }

    #[test]
    fn scope_mismatched_compiled_state_denies_closed() {
        let required = required_token(7);
        let other_scope =
            StateScope::new("tenant", "other-workspace", PATH_GOVERNANCE_METADATA_DOMAIN);
        let compiled = CompiledPathGovernanceMetadataState::new(
            other_scope.clone(),
            StateToken::for_test(other_scope, 7, "manifest-7"),
        );

        assert_eq!(
            PathGovernanceReadiness::DenyClosed(PathGovernanceReadinessReason::ScopeMismatch),
            PathGovernanceMetadataWriter::compiled_enforcement_readiness(
                &required,
                Some(&compiled)
            )
        );
    }

    #[test]
    fn projection_lag_does_not_affect_enforcement_readiness() {
        let required = required_token(7);
        let compiled = CompiledPathGovernanceMetadataState::new(metadata_scope(), required.clone());

        assert_eq!(
            PathGovernanceProjectionLag {
                committed_sequence: 7,
                latest_projected_sequence: Some(2),
                pending_sequences: Some(5),
            },
            PathGovernanceMetadataWriter::projection_lag_for(&required, Some(2))
        );
        assert_eq!(
            PathGovernanceReadiness::Ready {
                required_sequence: 7,
                compiled_sequence: 7,
            },
            PathGovernanceMetadataWriter::compiled_enforcement_readiness(
                &required,
                Some(&compiled)
            )
        );
    }

    #[tokio::test]
    async fn credential_vending_does_not_read_path_governance_metadata() -> Result<()> {
        let writer = writer(storage());
        writer
            .declare_path(declaration("orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("declare metadata-only path");

        let engine = CredentialVendingEngine::default();
        let decision = engine.decide_path(
            &StorageGovernanceState::default(),
            &CredentialVendingRequest {
                principal_id: "user_alice".to_string(),
                groups_snapshot_version: "groups-rev-1".to_string(),
                workspace_id: "workspace".to_string(),
                request_id: "request-no-vending-authority".to_string(),
                operation: CredentialOperation::Read,
                requested_path: "gs://bucket/warehouse/orders/day=1/".to_string(),
                requested_ttl: Duration::from_secs(300),
                client_kind: "uc".to_string(),
                catalog_snapshot_version: "event_001".to_string(),
                authorization: Some(CredentialVendingAuthorization {
                    principal_id: "user_alice".to_string(),
                    object_id: "orders".to_string(),
                    object_type: "EXTERNAL_LOCATION".to_string(),
                    privilege: Privilege::ReadFiles,
                    permission_ledger_watermark: "event_001".to_string(),
                    path_authority_object_id: "orders".to_string(),
                    path_authority_object_type: "EXTERNAL_LOCATION".to_string(),
                }),
            },
        )?;

        assert_eq!(CredentialDecision::Deny, decision.decision);
        assert_eq!("path_not_governed", decision.reason_code);
        Ok(())
    }

    #[test]
    fn unsupported_domains_reject_phase6a_writes() {
        for domain in [
            "catalog",
            "grants",
            "credential-vending",
            "storage-credentials",
            "external-locations",
            "managed-roots",
            "projection-outbox-acks",
        ] {
            assert_validation(
                PathGovernanceMetadataWriter::new(storage(), other_scope(domain)),
                PATH_GOVERNANCE_METADATA_DOMAIN,
            );
        }
    }

    fn required_token(logical_sequence: u64) -> StateToken {
        StateToken::for_test(
            metadata_scope(),
            logical_sequence,
            format!("manifest-{logical_sequence}"),
        )
    }
}
