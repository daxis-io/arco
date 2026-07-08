use arco_core::ScopedStorage;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::{
    ArcoStateReader, ArcoStateTxn, ControlMvpStateStore, ControlMvpTxn, KeyRange, StateScope,
    StateToken, TxnOptions,
};
use crate::error::{CatalogError, Result};
use crate::metastore::events::LifecycleState;
use crate::storage_governance::path_normalization::GovernedPath;

#[allow(dead_code)]
pub(crate) const PATH_GOVERNANCE_METADATA_DOMAIN: &str = "path-governance-metadata";

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
        declaration: PathGovernanceDeclaration,
    ) -> Result<PathGovernanceMetadataReceipt> {
        self.begin_declare_path(declaration).await?.commit().await
    }

    pub(crate) async fn begin_declare_path(
        &self,
        declaration: PathGovernanceDeclaration,
    ) -> Result<PathGovernancePendingDeclaration> {
        let keys = MetadataKeys::new(declaration.declaration_id(), declaration.canonical_uri())?;
        let mut txn = self
            .store
            .begin_control_txn(TxnOptions::new(Some(self.scope.clone())))
            .await?;

        if txn.get(&keys.record_key).await?.is_some() {
            return Err(CatalogError::AlreadyExists {
                entity: "path_governance_metadata".to_string(),
                name: declaration.declaration_id().to_string(),
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
        if !txn.scan_prefix(&keys.descendant_prefix).await?.is_empty() {
            return Err(precondition_failed(
                "descendant path governance metadata conflict",
            ));
        }

        let descendant_witness = txn.range_witness(&keys.descendant_range);
        txn.assert_absent(&keys.record_key).await?;
        txn.assert_absent(&keys.exact_path_key).await?;
        for ancestor_key in &keys.ancestor_path_keys {
            txn.assert_absent(ancestor_key).await?;
        }
        txn.assert_range_empty(keys.descendant_range.clone())
            .await?;
        txn.assert_range_unchanged(keys.descendant_range.clone(), descendant_witness)
            .await?;
        let predicate_inputs = txn
            .read_set(
                &keys.predicate_point_keys(),
                &[keys.descendant_range.clone()],
            )
            .await?;
        txn.assert_inputs_unchanged(predicate_inputs).await?;

        txn.put(&keys.record_key, encode_declaration(&declaration)?)
            .await?;
        txn.put(
            &keys.exact_path_key,
            Bytes::from(declaration.declaration_id().to_string()),
        )
        .await?;

        Ok(PathGovernancePendingDeclaration {
            writer: self.clone(),
            txn,
            declaration,
        })
    }

    pub(crate) async fn read_declaration_at(
        &self,
        token: StateToken,
        declaration_id: &str,
    ) -> Result<Option<PathGovernanceDeclaration>> {
        let key = declaration_key(declaration_id);
        let reader = self.store.read_at(token).await?;
        let Some(bytes) = reader.get(&key).await? else {
            return Ok(None);
        };
        decode_declaration(&bytes).map(Some)
    }

    pub(crate) async fn read_declaration_at_status(
        &self,
        token: StateToken,
        declaration_id: &str,
    ) -> Result<PathGovernanceDeclarationReadStatus> {
        let key = declaration_key(declaration_id);
        let reader = match self.store.read_at(token.clone()).await {
            Ok(reader) => reader,
            Err(CatalogError::NotFound { .. }) => {
                return Ok(PathGovernanceDeclarationReadStatus::TokenUnavailable {
                    manifest_id: token.authority_manifest_id().to_string(),
                    logical_sequence: token.logical_sequence(),
                });
            }
            Err(error) => return Err(error),
        };
        let Some(bytes) = reader.get(&key).await? else {
            return Ok(PathGovernanceDeclarationReadStatus::Available(None));
        };
        decode_declaration(&bytes)
            .map(|declaration| PathGovernanceDeclarationReadStatus::Available(Some(declaration)))
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

    pub(crate) fn compiled_state_status_for(
        token: &StateToken,
        compiled_sequence: Option<u64>,
    ) -> PathGovernanceCompiledStateStatus {
        let required_sequence = token.logical_sequence();
        match compiled_sequence {
            None => PathGovernanceCompiledStateStatus::DenyClosedMissing { required_sequence },
            Some(compiled_sequence) if compiled_sequence < required_sequence => {
                PathGovernanceCompiledStateStatus::DenyClosedStale {
                    required_sequence,
                    compiled_sequence,
                }
            }
            Some(compiled_sequence) => PathGovernanceCompiledStateStatus::Ready {
                required_sequence,
                compiled_sequence,
            },
        }
    }

    async fn has_path_conflict(&self, declaration: &PathGovernanceDeclaration) -> Result<bool> {
        let keys = MetadataKeys::new(declaration.declaration_id(), declaration.canonical_uri())?;
        if self.store.get(&keys.exact_path_key).await?.is_some() {
            return Ok(true);
        }
        for ancestor_key in &keys.ancestor_path_keys {
            if self.store.get(ancestor_key).await?.is_some() {
                return Ok(true);
            }
        }
        Ok(!self
            .store
            .scan_prefix(&keys.descendant_prefix)
            .await?
            .is_empty())
    }
}

#[allow(dead_code)]
pub(crate) struct PathGovernancePendingDeclaration {
    writer: PathGovernanceMetadataWriter,
    txn: ControlMvpTxn,
    declaration: PathGovernanceDeclaration,
}

#[allow(dead_code)]
impl PathGovernancePendingDeclaration {
    pub(crate) async fn commit(self) -> Result<PathGovernanceMetadataReceipt> {
        let declaration = self.declaration;
        match self.txn.commit().await {
            Ok(token) => Ok(PathGovernanceMetadataReceipt { token, declaration }),
            Err(CatalogError::CasFailed { .. }) => {
                if self.writer.has_path_conflict(&declaration).await? {
                    Err(precondition_failed(
                        "path governance metadata conflict changed before commit",
                    ))
                } else {
                    Err(CatalogError::CasFailed {
                        message: "path governance metadata pointer CAS lost".to_string(),
                    })
                }
            }
            Err(error) => Err(error),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PathGovernanceDeclaration {
    declaration_id: String,
    authority_object_id: String,
    authority_object_type: String,
    workspace_id: Option<String>,
    canonical_uri: String,
    owner: String,
    lifecycle_state: LifecycleState,
}

#[allow(dead_code)]
impl PathGovernanceDeclaration {
    pub(crate) fn active<W>(
        declaration_id: impl Into<String>,
        authority_object_id: impl Into<String>,
        authority_object_type: impl Into<String>,
        workspace_id: Option<W>,
        raw_uri: impl AsRef<str>,
        owner: impl Into<String>,
    ) -> Result<Self>
    where
        W: Into<String>,
    {
        let governed_path = GovernedPath::parse(raw_uri.as_ref())?;
        Ok(Self {
            declaration_id: declaration_id.into(),
            authority_object_id: authority_object_id.into(),
            authority_object_type: authority_object_type.into(),
            workspace_id: workspace_id.map(Into::into),
            canonical_uri: governed_path.canonical_uri(),
            owner: owner.into(),
            lifecycle_state: LifecycleState::Active,
        })
    }

    #[must_use]
    pub(crate) fn declaration_id(&self) -> &str {
        &self.declaration_id
    }

    #[must_use]
    pub(crate) fn authority_object_id(&self) -> &str {
        &self.authority_object_id
    }

    #[must_use]
    pub(crate) fn authority_object_type(&self) -> &str {
        &self.authority_object_type
    }

    #[must_use]
    pub(crate) fn workspace_id(&self) -> Option<&str> {
        self.workspace_id.as_deref()
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
    pub(crate) const fn lifecycle_state(&self) -> LifecycleState {
        self.lifecycle_state
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PathGovernanceMetadataReceipt {
    token: StateToken,
    declaration: PathGovernanceDeclaration,
}

#[allow(dead_code)]
impl PathGovernanceMetadataReceipt {
    #[must_use]
    pub(crate) const fn token(&self) -> &StateToken {
        &self.token
    }

    #[must_use]
    pub(crate) const fn declaration(&self) -> &PathGovernanceDeclaration {
        &self.declaration
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PathGovernanceDeclarationReadStatus {
    Available(Option<PathGovernanceDeclaration>),
    TokenUnavailable {
        manifest_id: String,
        logical_sequence: u64,
    },
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PathGovernanceProjectionLag {
    committed_sequence: u64,
    latest_projected_sequence: Option<u64>,
    pending_sequences: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PathGovernanceCompiledStateStatus {
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
}

struct MetadataKeys {
    record_key: Vec<u8>,
    exact_path_key: Vec<u8>,
    ancestor_path_keys: Vec<Vec<u8>>,
    descendant_prefix: Vec<u8>,
    descendant_range: KeyRange,
}

impl MetadataKeys {
    fn new(declaration_id: &str, canonical_uri: &str) -> Result<Self> {
        GovernedPath::parse(canonical_uri)?;
        let exact_path_key = path_index_key(canonical_uri);
        let ancestor_path_keys = canonical_ancestor_uris(canonical_uri)?
            .into_iter()
            .map(|ancestor_uri| path_index_key(&ancestor_uri))
            .collect();
        let descendant_range = descendant_conflict_range(canonical_uri)?;

        Ok(Self {
            record_key: declaration_key(declaration_id),
            exact_path_key: exact_path_key.clone(),
            ancestor_path_keys,
            descendant_prefix: exact_path_key,
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
    let mut key = b"path-governance-metadata/active-path/".to_vec();
    key.extend_from_slice(canonical_uri.as_bytes());
    key
}

fn descendant_conflict_range(canonical_uri: &str) -> Result<KeyRange> {
    GovernedPath::parse(canonical_uri)?;
    let start = path_index_key(canonical_uri);
    let mut end = start.clone();
    end.push(0xff);
    Ok(KeyRange::new(start, end))
}

fn canonical_ancestor_uris(canonical_uri: &str) -> Result<Vec<String>> {
    let (scheme, rest) = canonical_uri
        .split_once("://")
        .ok_or_else(|| validation_failed("path must include a URI scheme"))?;
    let (authority, path) = if scheme == "file" {
        (None, rest)
    } else {
        let (authority, path) = rest
            .split_once('/')
            .ok_or_else(|| validation_failed("cloud URI authority must include a path root"))?;
        (Some(authority), path)
    };
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let mut ancestors = Vec::new();
    for depth in 0..segments.len() {
        let path = if depth == 0 {
            "/".to_string()
        } else {
            format!("/{}/", segments[..depth].join("/"))
        };
        ancestors.push(canonical_uri_for_parts(scheme, authority, &path));
    }
    Ok(ancestors)
}

fn canonical_uri_for_parts(scheme: &str, authority: Option<&str>, path: &str) -> String {
    authority.map_or_else(
        || format!("{scheme}://{path}"),
        |authority| format!("{scheme}://{authority}{path}"),
    )
}

fn push_length_prefixed(key: &mut Vec<u8>, value: &[u8]) {
    key.extend_from_slice(value.len().to_string().as_bytes());
    key.push(b':');
    key.extend_from_slice(value);
}

fn encode_declaration(declaration: &PathGovernanceDeclaration) -> Result<Bytes> {
    serde_json::to_vec(declaration)
        .map(Bytes::from)
        .map_err(|error| {
            serialization_failed(format!(
                "path governance metadata declaration encode: {error}"
            ))
        })
}

fn decode_declaration(bytes: &Bytes) -> Result<PathGovernanceDeclaration> {
    serde_json::from_slice(bytes).map_err(|error| {
        serialization_failed(format!(
            "path governance metadata declaration decode: {error}"
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
    use crate::error::{CatalogError, Result};
    use crate::state_store::{ArcoStateTxn, ControlMvpStateStore, StateScope, TxnOptions};

    fn metadata_scope() -> StateScope {
        StateScope::new("tenant", "workspace", PATH_GOVERNANCE_METADATA_DOMAIN)
    }

    fn storage() -> ScopedStorage {
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scoped storage")
    }

    fn writer(storage: ScopedStorage) -> PathGovernanceMetadataWriter {
        PathGovernanceMetadataWriter::new(storage, metadata_scope()).expect("metadata writer")
    }

    fn declaration(id: &str, uri: &str) -> PathGovernanceDeclaration {
        PathGovernanceDeclaration::active(
            id,
            format!("authority_{id}"),
            "EXTERNAL_LOCATION",
            Some("workspace"),
            uri,
            "owner",
        )
        .expect("declaration")
    }

    fn assert_precondition_failed<T>(result: Result<T>, context: &str) {
        match result {
            Err(CatalogError::PreconditionFailed { .. }) => {}
            Err(error) => panic!("expected precondition failure for {context}, got {error:?}"),
            Ok(_) => panic!("expected precondition failure for {context}"),
        }
    }

    fn assert_precondition_contains<T>(result: Result<T>, expected: &str) {
        match result {
            Err(CatalogError::PreconditionFailed { message }) => assert!(
                message.contains(expected),
                "expected precondition message containing {expected:?}, got {message:?}"
            ),
            Err(error) => {
                panic!("expected precondition failure containing {expected}, got {error:?}")
            }
            Ok(_) => panic!("expected precondition failure containing {expected}"),
        }
    }

    #[tokio::test]
    async fn successful_declaration_returns_state_token() {
        let writer = writer(storage());

        let receipt = writer
            .declare_path(declaration("decl_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("declare path");

        assert_eq!(&metadata_scope(), receipt.token().scope());
        assert_eq!(1, receipt.token().logical_sequence());
        assert!(!receipt.token().authority_manifest_id().is_empty());
        assert_eq!("decl_orders", receipt.declaration().declaration_id());
        assert_eq!(
            "gs://bucket/warehouse/orders/",
            receipt.declaration().canonical_uri()
        );
        assert_eq!("active", receipt.declaration().lifecycle_state().as_str());
    }

    #[tokio::test]
    async fn read_declaration_at_state_token_returns_committed_declaration() {
        let writer = writer(storage());

        let first = writer
            .declare_path(declaration("decl_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("first declaration");
        let second = writer
            .declare_path(declaration(
                "decl_customers",
                "gs://bucket/warehouse/customers",
            ))
            .await
            .expect("second declaration");

        assert_eq!(
            Some(declaration("decl_orders", "gs://bucket/warehouse/orders")),
            writer
                .read_declaration_at(first.token().clone(), "decl_orders")
                .await
                .expect("read first token")
        );
        assert_eq!(
            None,
            writer
                .read_declaration_at(first.token().clone(), "decl_customers")
                .await
                .expect("first token excludes later declaration")
        );
        assert_eq!(
            Some(declaration(
                "decl_customers",
                "gs://bucket/warehouse/customers"
            )),
            writer
                .read_declaration_at(second.token().clone(), "decl_customers")
                .await
                .expect("read second token")
        );
    }

    #[tokio::test]
    async fn token_status_marks_missing_retained_manifest_unavailable() {
        let storage = storage();
        let writer = writer(storage.clone());

        let receipt = writer
            .declare_path(declaration("decl_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("declare path");
        let token = receipt.token().clone();
        let manifest_id = token.authority_manifest_id().to_string();
        let logical_sequence = token.logical_sequence();
        let paths = crate::state_store::ControlMvpPaths::new(PATH_GOVERNANCE_METADATA_DOMAIN);
        storage
            .delete(&paths.manifest_object(&manifest_id))
            .await
            .expect("expire retained manifest");

        assert_eq!(
            PathGovernanceDeclarationReadStatus::TokenUnavailable {
                manifest_id,
                logical_sequence,
            },
            writer
                .read_declaration_at_status(token, "decl_orders")
                .await
                .expect("token status")
        );
    }

    #[tokio::test]
    async fn ancestor_conflict_is_rejected() {
        let writer = writer(storage());
        writer
            .declare_path(declaration("decl_parent", "gs://bucket/warehouse/orders"))
            .await
            .expect("declare parent");

        assert_precondition_failed(
            writer
                .declare_path(declaration(
                    "decl_child",
                    "gs://bucket/warehouse/orders/2026",
                ))
                .await,
            "ancestor conflict",
        );
    }

    #[tokio::test]
    async fn exact_canonical_conflict_is_rejected() {
        let writer = writer(storage());
        writer
            .declare_path(declaration("decl_orders", "gs://Bucket//warehouse/orders"))
            .await
            .expect("declare first canonical path");

        assert_precondition_contains(
            writer
                .declare_path(declaration(
                    "decl_orders_duplicate",
                    "gs://bucket/warehouse/orders/",
                ))
                .await,
            "exact path governance metadata conflict",
        );
    }

    #[tokio::test]
    async fn descendant_conflict_is_rejected() {
        let writer = writer(storage());
        writer
            .declare_path(declaration(
                "decl_child",
                "gs://bucket/warehouse/orders/2026",
            ))
            .await
            .expect("declare child");

        assert_precondition_failed(
            writer
                .declare_path(declaration("decl_parent", "gs://bucket/warehouse/orders"))
                .await,
            "descendant conflict",
        );
    }

    #[tokio::test]
    async fn non_overlapping_sibling_paths_are_accepted() {
        let writer = writer(storage());

        let orders = writer
            .declare_path(declaration("decl_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("orders declaration");
        let archive = writer
            .declare_path(declaration(
                "decl_orders_archive",
                "gs://bucket/warehouse/orders-archive",
            ))
            .await
            .expect("orders archive declaration");

        assert_eq!(1, orders.token().logical_sequence());
        assert_eq!(2, archive.token().logical_sequence());
    }

    #[tokio::test]
    async fn range_empty_blocks_tombstoned_descendant_index() {
        let storage = storage();
        let writer = writer(storage.clone());
        let store = ControlMvpStateStore::new(storage, metadata_scope()).expect("control store");
        let descendant_index_key = path_index_key("gs://bucket/warehouse/orders/");

        let mut seed_txn = store
            .begin_control_txn(TxnOptions::new(Some(metadata_scope())))
            .await
            .expect("begin seed transaction");
        seed_txn
            .put(&descendant_index_key, Bytes::from_static(b"decl_orders"))
            .await
            .expect("stage descendant index");
        seed_txn.commit().await.expect("commit descendant index");

        let mut delete_txn = store
            .begin_control_txn(TxnOptions::new(Some(metadata_scope())))
            .await
            .expect("begin delete transaction");
        delete_txn
            .delete(&descendant_index_key)
            .await
            .expect("tombstone descendant index");
        delete_txn.commit().await.expect("commit tombstone");

        assert_precondition_contains(
            writer
                .declare_path(declaration("decl_parent", "gs://bucket/warehouse"))
                .await,
            "cannot assert a non-empty control MVP range",
        );
    }

    #[tokio::test]
    async fn range_empty_blocks_concurrent_descendant_insert() {
        let writer = writer(storage());
        let pending_parent = writer
            .begin_declare_path(declaration("decl_parent", "gs://bucket/warehouse/orders"))
            .await
            .expect("begin parent declaration");
        writer
            .declare_path(declaration(
                "decl_child",
                "gs://bucket/warehouse/orders/2026",
            ))
            .await
            .expect("concurrent child declaration");

        assert_precondition_failed(
            pending_parent.commit().await,
            "concurrent descendant insert",
        );
    }

    #[tokio::test]
    async fn range_unchanged_catches_stale_assumptions() {
        let storage = storage();
        let writer = writer(storage.clone());
        let store = ControlMvpStateStore::new(storage, metadata_scope()).expect("control store");
        let txn = store
            .begin_control_txn(TxnOptions::new(Some(metadata_scope())))
            .await
            .expect("begin transaction");
        let range = descendant_conflict_range("gs://bucket/warehouse/orders/").expect("range");
        let stale_witness = txn.range_witness(&range);

        writer
            .declare_path(declaration(
                "decl_child",
                "gs://bucket/warehouse/orders/2026",
            ))
            .await
            .expect("child declaration");

        let mut stale_txn = store
            .begin_control_txn(TxnOptions::new(Some(metadata_scope())))
            .await
            .expect("begin stale transaction");
        assert_precondition_failed(
            stale_txn.assert_range_unchanged(range, stale_witness).await,
            "stale range witness",
        );
    }

    #[tokio::test]
    async fn missing_compiled_state_denies_closed() {
        let writer = writer(storage());
        let receipt = writer
            .declare_path(declaration("decl_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("declare path");

        assert_eq!(
            PathGovernanceCompiledStateStatus::DenyClosedMissing {
                required_sequence: receipt.token().logical_sequence(),
            },
            PathGovernanceMetadataWriter::compiled_state_status_for(receipt.token(), None)
        );
    }

    #[tokio::test]
    async fn stale_compiled_state_denies_closed() {
        let writer = writer(storage());
        let receipt = writer
            .declare_path(declaration("decl_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("declare path");

        assert_eq!(
            PathGovernanceCompiledStateStatus::DenyClosedStale {
                required_sequence: receipt.token().logical_sequence(),
                compiled_sequence: 0,
            },
            PathGovernanceMetadataWriter::compiled_state_status_for(receipt.token(), Some(0))
        );
    }

    #[tokio::test]
    async fn projection_lag_does_not_affect_compiled_state_gate() {
        let writer = writer(storage());
        let receipt = writer
            .declare_path(declaration("decl_orders", "gs://bucket/warehouse/orders"))
            .await
            .expect("declare path");

        assert_eq!(
            PathGovernanceProjectionLag {
                committed_sequence: receipt.token().logical_sequence(),
                latest_projected_sequence: Some(0),
                pending_sequences: Some(receipt.token().logical_sequence()),
            },
            PathGovernanceMetadataWriter::projection_lag_for(receipt.token(), Some(0))
        );
        assert_eq!(
            PathGovernanceCompiledStateStatus::Ready {
                required_sequence: receipt.token().logical_sequence(),
                compiled_sequence: receipt.token().logical_sequence(),
            },
            PathGovernanceMetadataWriter::compiled_state_status_for(
                receipt.token(),
                Some(receipt.token().logical_sequence())
            )
        );
    }

    #[test]
    fn unsupported_domains_reject_phase6a_metadata_writes() {
        for domain in [
            "catalog",
            "grants",
            "storage-governance",
            "projection-outbox-acks",
            "credential-vending",
        ] {
            let unsupported_scope = StateScope::new("tenant", "workspace", domain);

            let error = match PathGovernanceMetadataWriter::new(storage(), unsupported_scope) {
                Err(error) => error,
                Ok(_) => panic!("unsupported scope {domain} must reject writer creation"),
            };

            assert!(
                matches!(error, CatalogError::Validation { .. }),
                "unexpected error for {domain}: {error:?}"
            );
        }
    }
}
