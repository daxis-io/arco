//! State-store seam for future Tier-1 authority backends.
//!
//! The current adapter intentionally exposes only capability discovery. It does
//! not delegate production reads or writes, and it must not mint synthetic
//! state tokens for today's ledger plus synchronous compactor path.

use std::fmt;
use std::sync::Arc;

use arco_core::ScopedStorage;
use arco_core::storage::StorageBackend;
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};

use crate::error::{CatalogError, Result};

pub(crate) mod comparison_reads;
pub mod control_mvp;
#[allow(
    dead_code,
    reason = "Phase 6 metadata stays crate-internal until its public activation gate"
)]
pub(crate) mod external_location_metadata;
pub mod model;
pub(crate) mod path_governance_metadata;
pub mod projection_outbox_acks;
pub mod promotion_gate;
pub mod shadow_replay;
#[allow(
    dead_code,
    reason = "Phase 6 metadata stays crate-internal until its public activation gate"
)]
pub(crate) mod workspace_binding_metadata;

pub use control_mvp::{
    ControlMvpGcCandidate, ControlMvpGcOutcome, ControlMvpGcPlan, ControlMvpMaintenanceOutcome,
    ControlMvpMaintenanceWorker, ControlMvpOutboxTrimTarget, ControlMvpPaths,
    ControlMvpProjectionOutboxRecord, ControlMvpRestoreParticipant, ControlMvpRestorePlan,
    ControlMvpStateStore, ControlMvpTxn, control_mvp_outbox_event_id,
};
pub use model::{ModelCommitRecord, ModelStateStore, ModelWrite};

/// Opaque process-local identity for one configured state-store backend.
///
/// The value is deliberately non-serializable and exposes no provider details.
/// It is used only to prove that separately configured capabilities share the
/// same in-process backend authority.
#[derive(Clone)]
pub struct StateStoreBindingIdentity {
    backend: Arc<dyn StorageBackend>,
}

impl StateStoreBindingIdentity {
    /// Derives an opaque identity from a workspace-scoped backend handle.
    ///
    /// Clones of storage backed by the same [`Arc`] compare equal. Separately
    /// constructed backend handles compare unequal even when their scope strings
    /// or provider configuration happen to match.
    #[must_use]
    pub fn from_scoped_storage(storage: &ScopedStorage) -> Self {
        Self {
            backend: storage.backend().clone(),
        }
    }
}

impl fmt::Debug for StateStoreBindingIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StateStoreBindingIdentity(<opaque>)")
    }
}

impl PartialEq for StateStoreBindingIdentity {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.backend, &other.backend)
    }
}

impl Eq for StateStoreBindingIdentity {}

fn validate_required_metadata_field(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(CatalogError::Validation {
            message: format!("{field} must not be blank"),
        });
    }
    Ok(())
}

fn validate_metadata_timestamp(updated_at_ms: i64) -> Result<()> {
    if updated_at_ms < 0 {
        return Err(CatalogError::Validation {
            message: "updated_at_ms must not be negative".to_string(),
        });
    }
    Ok(())
}

/// Opaque retained authority token for a future state-store scope.
///
/// External crates cannot mint authority tokens directly.
///
/// ```compile_fail
/// use arco_catalog::{StateScope, StateToken};
///
/// let scope = StateScope::new("tenant", "workspace", "catalog");
/// let _token = StateToken::new(scope, 1, "manifest-1");
/// ```
///
/// ```compile_fail
/// use arco_catalog::StateToken;
/// use serde::Serialize;
///
/// fn assert_serializable<T: Serialize>() {}
/// assert_serializable::<StateToken>();
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateToken {
    scope: StateScope,
    logical_sequence: u64,
    authority_manifest_id: String,
}

impl StateToken {
    /// Creates a state token value for crate-local tests.
    #[cfg(test)]
    #[must_use]
    fn for_test(
        scope: StateScope,
        logical_sequence: u64,
        authority_manifest_id: impl Into<String>,
    ) -> Self {
        Self {
            scope,
            logical_sequence,
            authority_manifest_id: authority_manifest_id.into(),
        }
    }

    /// Returns the authority scope named by this token.
    #[must_use]
    pub const fn scope(&self) -> &StateScope {
        &self.scope
    }

    /// Returns the logical authority sequence named by this token.
    #[must_use]
    pub const fn logical_sequence(&self) -> u64 {
        self.logical_sequence
    }

    /// Returns the authority manifest identifier named by this token.
    #[must_use]
    pub fn authority_manifest_id(&self) -> &str {
        &self.authority_manifest_id
    }
}

/// Result of one logically committed authority transaction.
///
/// The state token and projection intents cross the commit boundary together.
/// Delivery remains a post-commit side effect: failure to enqueue an intent
/// cannot revoke or change the token returned here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOutcome {
    state_token: StateToken,
    projection_intents: Vec<ProjectionIntentV1>,
}

impl CommitOutcome {
    pub(crate) fn new(
        state_token: StateToken,
        projection_intents: Vec<ProjectionIntentV1>,
    ) -> Self {
        Self {
            state_token,
            projection_intents,
        }
    }

    /// Returns the opaque token naming the committed logical authority state.
    #[must_use]
    pub const fn state_token(&self) -> &StateToken {
        &self.state_token
    }

    /// Returns the projection intents committed by the transaction.
    #[must_use]
    pub fn projection_intents(&self) -> &[ProjectionIntentV1] {
        &self.projection_intents
    }

    /// Consumes the outcome and returns its opaque authority token.
    #[must_use]
    pub fn into_state_token(self) -> StateToken {
        self.state_token
    }

    /// Consumes the outcome and returns the token and committed intents.
    #[must_use]
    pub fn into_parts(self) -> (StateToken, Vec<ProjectionIntentV1>) {
        (self.state_token, self.projection_intents)
    }
}

impl std::ops::Deref for CommitOutcome {
    type Target = StateToken;

    fn deref(&self) -> &Self::Target {
        &self.state_token
    }
}

impl PartialEq<StateToken> for CommitOutcome {
    fn eq(&self, other: &StateToken) -> bool {
        self.state_token == *other
    }
}

impl PartialEq<CommitOutcome> for StateToken {
    fn eq(&self, other: &CommitOutcome) -> bool {
        *self == other.state_token
    }
}

/// Version-one committed projection-intent envelope.
///
/// The source authority is serialized as its constituent fields so the
/// otherwise opaque [`StateToken`] does not become a generally serializable
/// public capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectionIntentV1 {
    contract_version: u32,
    intent_id: String,
    projection_kind: String,
    source_scope: StateScope,
    source_logical_sequence: u64,
    source_authority_manifest_id: String,
    payload: Vec<u8>,
}

impl ProjectionIntentV1 {
    /// Wire-contract version written by this implementation.
    pub const CONTRACT_VERSION: u32 = 1;

    /// Creates and validates a committed projection intent.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed identifiers, an empty payload,
    /// or a token that does not name committed authority.
    pub fn new(
        intent_id: impl Into<String>,
        projection_kind: impl Into<String>,
        source: &StateToken,
        payload: impl AsRef<[u8]>,
    ) -> Result<Self> {
        let intent = Self {
            contract_version: Self::CONTRACT_VERSION,
            intent_id: intent_id.into(),
            projection_kind: projection_kind.into(),
            source_scope: source.scope.clone(),
            source_logical_sequence: source.logical_sequence,
            source_authority_manifest_id: source.authority_manifest_id.clone(),
            payload: payload.as_ref().to_vec(),
        };
        intent.validate()?;
        Ok(intent)
    }

    fn validate(&self) -> Result<()> {
        if self.contract_version != Self::CONTRACT_VERSION {
            return Err(CatalogError::Validation {
                message: "projection intent contract version is unsupported".to_string(),
            });
        }
        validate_scope_component(&self.intent_id, "projection intent_id")?;
        validate_scope_component(&self.projection_kind, "projection kind")?;
        self.source_scope.validate()?;
        if self.source_logical_sequence == 0 || self.source_authority_manifest_id.trim().is_empty()
        {
            return Err(CatalogError::Validation {
                message: "projection intent source token must name committed authority".to_string(),
            });
        }
        if self.payload.is_empty() {
            return Err(CatalogError::Validation {
                message: "projection intent payload must not be empty".to_string(),
            });
        }
        Ok(())
    }

    /// Returns the contract version.
    #[must_use]
    pub const fn contract_version(&self) -> u32 {
        self.contract_version
    }

    /// Returns the immutable intent identifier.
    #[must_use]
    pub fn intent_id(&self) -> &str {
        &self.intent_id
    }

    /// Returns the projection family this intent targets.
    #[must_use]
    pub fn projection_kind(&self) -> &str {
        &self.projection_kind
    }

    /// Returns the authority scope that produced this intent.
    #[must_use]
    pub const fn source_scope(&self) -> &StateScope {
        &self.source_scope
    }

    /// Returns the committed logical sequence that produced this intent.
    #[must_use]
    pub const fn source_logical_sequence(&self) -> u64 {
        self.source_logical_sequence
    }

    /// Returns the committed authority-manifest identifier as provenance.
    #[must_use]
    pub fn source_authority_manifest_id(&self) -> &str {
        &self.source_authority_manifest_id
    }

    /// Returns the versioned projection payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub(crate) fn source_token(&self) -> StateToken {
        StateToken {
            scope: self.source_scope.clone(),
            logical_sequence: self.source_logical_sequence,
            authority_manifest_id: self.source_authority_manifest_id.clone(),
        }
    }
}

impl<'de> Deserialize<'de> for ProjectionIntentV1 {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Wire {
            contract_version: u32,
            intent_id: String,
            projection_kind: String,
            source_scope: StateScope,
            source_logical_sequence: u64,
            source_authority_manifest_id: String,
            payload: Vec<u8>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let intent = Self {
            contract_version: wire.contract_version,
            intent_id: wire.intent_id,
            projection_kind: wire.projection_kind,
            source_scope: wire.source_scope,
            source_logical_sequence: wire.source_logical_sequence,
            source_authority_manifest_id: wire.source_authority_manifest_id,
            payload: wire.payload,
        };
        intent.validate().map_err(serde::de::Error::custom)?;
        Ok(intent)
    }
}

/// Threshold that requested asynchronous segment-layout maintenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LayoutMaintenanceReason {
    /// More than the supported number of level-zero segments are reachable.
    L0SegmentCount,
    /// Reachable level-zero segment bytes exceed the configured threshold.
    L0Bytes,
    /// The selected authority manifest exceeds the configured byte threshold.
    ManifestBytes,
}

/// Version-one asynchronous physical-layout maintenance envelope.
///
/// Its observed logical sequence is a precondition, not a new logical commit.
/// Applying maintenance may advance layout generation only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LayoutMaintenanceIntentV1 {
    contract_version: u32,
    intent_id: String,
    source_scope: StateScope,
    source_logical_sequence: u64,
    source_authority_manifest_id: String,
    layout_generation: u64,
    reason: LayoutMaintenanceReason,
}

impl LayoutMaintenanceIntentV1 {
    /// Wire-contract version written by this implementation.
    pub const CONTRACT_VERSION: u32 = 1;

    /// Creates a validated layout-maintenance intent.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed identity, zero generation, or
    /// a token that does not name committed authority.
    pub fn new(
        intent_id: impl Into<String>,
        source: &StateToken,
        layout_generation: u64,
        reason: LayoutMaintenanceReason,
    ) -> Result<Self> {
        let intent = Self {
            contract_version: Self::CONTRACT_VERSION,
            intent_id: intent_id.into(),
            source_scope: source.scope.clone(),
            source_logical_sequence: source.logical_sequence,
            source_authority_manifest_id: source.authority_manifest_id.clone(),
            layout_generation,
            reason,
        };
        intent.validate()?;
        Ok(intent)
    }

    fn validate(&self) -> Result<()> {
        if self.contract_version != Self::CONTRACT_VERSION {
            return Err(CatalogError::Validation {
                message: "layout-maintenance intent contract version is unsupported".to_string(),
            });
        }
        validate_scope_component(&self.intent_id, "layout-maintenance intent_id")?;
        self.source_scope.validate()?;
        if self.source_logical_sequence == 0 || self.source_authority_manifest_id.trim().is_empty()
        {
            return Err(CatalogError::Validation {
                message: "layout-maintenance source token must name committed authority"
                    .to_string(),
            });
        }
        if self.layout_generation == 0 {
            return Err(CatalogError::Validation {
                message: "layout-maintenance generation must be positive".to_string(),
            });
        }
        Ok(())
    }

    /// Returns the logical sequence the maintenance operation observed.
    #[must_use]
    pub const fn observed_logical_sequence(&self) -> u64 {
        self.source_logical_sequence
    }

    /// Returns the candidate physical layout generation.
    #[must_use]
    pub const fn layout_generation(&self) -> u64 {
        self.layout_generation
    }

    /// Returns the threshold that requested maintenance.
    #[must_use]
    pub const fn reason(&self) -> LayoutMaintenanceReason {
        self.reason
    }

    /// Returns the authority scope observed by the maintenance worker.
    #[must_use]
    pub const fn source_scope(&self) -> &StateScope {
        &self.source_scope
    }

    /// Returns the committed logical sequence observed by the worker.
    #[must_use]
    pub const fn source_logical_sequence(&self) -> u64 {
        self.source_logical_sequence
    }

    /// Returns the observed authority-manifest identifier as provenance.
    #[must_use]
    pub fn source_authority_manifest_id(&self) -> &str {
        &self.source_authority_manifest_id
    }
}

impl<'de> Deserialize<'de> for LayoutMaintenanceIntentV1 {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Wire {
            contract_version: u32,
            intent_id: String,
            source_scope: StateScope,
            source_logical_sequence: u64,
            source_authority_manifest_id: String,
            layout_generation: u64,
            reason: LayoutMaintenanceReason,
        }

        let wire = Wire::deserialize(deserializer)?;
        let intent = Self {
            contract_version: wire.contract_version,
            intent_id: wire.intent_id,
            source_scope: wire.source_scope,
            source_logical_sequence: wire.source_logical_sequence,
            source_authority_manifest_id: wire.source_authority_manifest_id,
            layout_generation: wire.layout_generation,
            reason: wire.reason,
        };
        intent.validate().map_err(serde::de::Error::custom)?;
        Ok(intent)
    }
}

mod metadata_readiness {
    use bytes::Bytes;

    use super::{ArcoStateReader, ControlMvpStateStore, StateScope, StateToken};
    use crate::error::{CatalogError, Result};

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum TokenPinnedReadStatus<T> {
        Available(Option<T>),
        TokenUnavailable {
            manifest_id: String,
            logical_sequence: u64,
        },
    }

    pub(super) async fn read_at_status<T>(
        store: &ControlMvpStateStore,
        token: StateToken,
        key: &[u8],
        decode: impl FnOnce(&Bytes) -> Result<T>,
    ) -> Result<TokenPinnedReadStatus<T>> {
        let manifest_id = token.authority_manifest_id().to_string();
        let logical_sequence = token.logical_sequence();
        let reader = match store.read_at(token).await {
            Ok(reader) => reader,
            Err(CatalogError::NotFound { .. }) => {
                return Ok(TokenPinnedReadStatus::TokenUnavailable {
                    manifest_id,
                    logical_sequence,
                });
            }
            Err(error) => return Err(error),
        };
        let Some(bytes) = reader.get(key).await? else {
            return Ok(TokenPinnedReadStatus::Available(None));
        };
        decode(&bytes).map(|value| TokenPinnedReadStatus::Available(Some(value)))
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ProjectionLag {
        pub(super) committed_sequence: u64,
        pub(super) latest_projected_sequence: Option<u64>,
        pub(super) pending_sequences: Option<u64>,
    }

    pub(super) fn projection_lag_for(
        token: &StateToken,
        latest_projected_sequence: Option<u64>,
    ) -> ProjectionLag {
        let committed_sequence = token.logical_sequence();
        ProjectionLag {
            committed_sequence,
            latest_projected_sequence,
            pending_sequences: latest_projected_sequence
                .map(|projected| committed_sequence.saturating_sub(projected)),
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum CompiledStateStatus {
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

    pub(super) fn compiled_state_status_for(
        required: &StateToken,
        compiled: Option<&StateToken>,
    ) -> CompiledStateStatus {
        let required_sequence = required.logical_sequence();
        let Some(compiled) = compiled else {
            return CompiledStateStatus::DenyClosedMissing { required_sequence };
        };
        if compiled.scope() != required.scope() {
            return CompiledStateStatus::DenyClosedScopeMismatch {
                required_scope: required.scope().clone(),
                compiled_scope: compiled.scope().clone(),
            };
        }

        let compiled_sequence = compiled.logical_sequence();
        if compiled_sequence < required_sequence {
            CompiledStateStatus::DenyClosedStale {
                required_sequence,
                compiled_sequence,
            }
        } else {
            CompiledStateStatus::Ready {
                required_sequence,
                compiled_sequence,
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn token(scope: &StateScope, sequence: u64) -> StateToken {
            StateToken::for_test(scope.clone(), sequence, format!("manifest-{sequence}"))
        }

        #[test]
        fn compiled_state_status_is_fail_closed_and_accepts_equal_or_newer_state() {
            let scope = StateScope::new("tenant", "workspace", "path-governance-metadata");
            let other_scope =
                StateScope::new("tenant", "other-workspace", "path-governance-metadata");
            let required = token(&scope, 7);

            assert_eq!(
                CompiledStateStatus::DenyClosedMissing {
                    required_sequence: 7,
                },
                compiled_state_status_for(&required, None)
            );
            assert_eq!(
                CompiledStateStatus::DenyClosedStale {
                    required_sequence: 7,
                    compiled_sequence: 6,
                },
                compiled_state_status_for(&required, Some(&token(&scope, 6)))
            );
            assert_eq!(
                CompiledStateStatus::DenyClosedScopeMismatch {
                    required_scope: scope.clone(),
                    compiled_scope: other_scope.clone(),
                },
                compiled_state_status_for(&required, Some(&token(&other_scope, 7)))
            );
            for compiled_sequence in [7, 9] {
                assert_eq!(
                    CompiledStateStatus::Ready {
                        required_sequence: 7,
                        compiled_sequence,
                    },
                    compiled_state_status_for(&required, Some(&token(&scope, compiled_sequence)))
                );
            }
        }

        #[test]
        fn projection_lag_is_diagnostic_and_saturates_when_projection_is_ahead() {
            let scope = StateScope::new("tenant", "workspace", "path-governance-metadata");
            let committed = token(&scope, 7);

            assert_eq!(
                ProjectionLag {
                    committed_sequence: 7,
                    latest_projected_sequence: None,
                    pending_sequences: None,
                },
                projection_lag_for(&committed, None)
            );
            for (projected, pending) in [(3, 4), (7, 0), (9, 0)] {
                assert_eq!(
                    ProjectionLag {
                        committed_sequence: 7,
                        latest_projected_sequence: Some(projected),
                        pending_sequences: Some(pending),
                    },
                    projection_lag_for(&committed, Some(projected))
                );
            }
        }
    }
}

#[cfg(test)]
mod test_support {
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use arco_core::error::Result as StorageResult;
    use arco_core::{MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
    use async_trait::async_trait;
    use bytes::Bytes;
    use tokio::sync::Notify;

    use super::ControlMvpPaths;

    pub(super) const POINTER_CAS_GATE_TIMEOUT: Duration = Duration::from_secs(5);

    /// Deterministically pauses the first current-pointer CAS after it is armed.
    pub(super) struct FirstPointerCasGateBackend {
        inner: MemoryBackend,
        pointer_suffix: String,
        armed: AtomicBool,
        intercepted: AtomicBool,
        blocked: Notify,
        release: Notify,
    }

    impl FirstPointerCasGateBackend {
        pub(super) fn new(domain: &str) -> Arc<Self> {
            Arc::new(Self {
                inner: MemoryBackend::new(),
                pointer_suffix: ControlMvpPaths::new(domain).current_pointer(),
                armed: AtomicBool::new(false),
                intercepted: AtomicBool::new(false),
                blocked: Notify::new(),
                release: Notify::new(),
            })
        }

        pub(super) fn arm(&self) {
            self.intercepted.store(false, Ordering::SeqCst);
            self.armed.store(true, Ordering::SeqCst);
        }

        pub(super) async fn wait_until_blocked(&self) {
            tokio::time::timeout(POINTER_CAS_GATE_TIMEOUT, async {
                loop {
                    let notified = self.blocked.notified();
                    if self.intercepted.load(Ordering::SeqCst) {
                        return;
                    }
                    notified.await;
                }
            })
            .await
            .expect("writer did not reach the first pointer CAS before timeout");
        }

        pub(super) fn release(&self) {
            self.release.notify_one();
        }
    }

    #[async_trait]
    impl StorageBackend for FirstPointerCasGateBackend {
        async fn get(&self, path: &str) -> StorageResult<Bytes> {
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> StorageResult<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> StorageResult<WriteResult> {
            if self.armed.load(Ordering::SeqCst)
                && path.ends_with(&self.pointer_suffix)
                && !self.intercepted.swap(true, Ordering::SeqCst)
            {
                self.blocked.notify_waiters();
                self.release.notified().await;
                self.armed.store(false, Ordering::SeqCst);
            }
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> StorageResult<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> StorageResult<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> StorageResult<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> StorageResult<String> {
            self.inner.signed_url(path, expiry).await
        }
    }
}

/// Opaque retained checkpoint token for longer-lived retained reads.
///
/// External crates cannot mint checkpoint tokens directly.
///
/// ```compile_fail
/// use arco_catalog::{CheckpointToken, StateScope};
///
/// let scope = StateScope::new("tenant", "workspace", "catalog");
/// let _token = CheckpointToken::new(scope, "checkpoint-1");
/// ```
///
/// ```compile_fail
/// use arco_catalog::CheckpointToken;
/// use serde::Serialize;
///
/// fn assert_serializable<T: Serialize>() {}
/// assert_serializable::<CheckpointToken>();
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointToken {
    scope: StateScope,
    checkpoint_id: String,
}

impl CheckpointToken {
    /// Returns the authority scope retained by this checkpoint.
    #[must_use]
    pub const fn scope(&self) -> &StateScope {
        &self.scope
    }

    /// Returns the checkpoint identifier.
    #[must_use]
    pub fn checkpoint_id(&self) -> &str {
        &self.checkpoint_id
    }
}

/// Options for opening a future state-store transaction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TxnOptions {
    scope: Option<StateScope>,
    request_id: Option<String>,
    operation_id: Option<String>,
}

impl TxnOptions {
    /// Creates transaction options for an optional authority scope.
    #[must_use]
    pub const fn new(scope: Option<StateScope>) -> Self {
        Self {
            scope,
            request_id: None,
            operation_id: None,
        }
    }

    /// Adds a request identifier to the transaction options.
    #[must_use]
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub(crate) fn with_operation_id(mut self, operation_id: impl Into<String>) -> Self {
        self.operation_id = Some(operation_id.into());
        self
    }

    /// Returns the requested authority scope, if one was provided.
    #[must_use]
    pub const fn scope(&self) -> Option<&StateScope> {
        self.scope.as_ref()
    }

    /// Returns the request identifier, if one was provided.
    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    pub(crate) fn operation_id(&self) -> Option<&str> {
        self.operation_id.as_deref()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(scope) = &self.scope {
            scope.validate()?;
        }
        if let Some(request_id) = &self.request_id {
            if request_id.len() > 256 {
                return Err(CatalogError::Validation {
                    message: "transaction request_id must not exceed 256 UTF-8 bytes".to_string(),
                });
            }
            validate_scope_component(request_id, "transaction request_id")?;
        }
        if let Some(operation_id) = &self.operation_id {
            if operation_id.len() > 128 {
                return Err(CatalogError::Validation {
                    message: "transaction operation_id must not exceed 128 UTF-8 bytes".to_string(),
                });
            }
            validate_scope_component(operation_id, "transaction operation_id")?;
        }
        Ok(())
    }
}

/// Options for creating a future retained authority checkpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckpointOptions {
    scope: Option<StateScope>,
    min_retention_seconds: Option<u64>,
    externally_retention_coordinated: bool,
}

impl CheckpointOptions {
    /// Creates checkpoint options for an optional authority scope.
    #[must_use]
    pub const fn new(scope: Option<StateScope>) -> Self {
        Self {
            scope,
            min_retention_seconds: None,
            externally_retention_coordinated: false,
        }
    }

    /// Adds a minimum retention request in seconds.
    #[must_use]
    pub const fn with_min_retention_seconds(mut self, seconds: u64) -> Self {
        self.min_retention_seconds = Some(seconds);
        self
    }

    /// Returns the requested authority scope, if one was provided.
    #[must_use]
    pub const fn scope(&self) -> Option<&StateScope> {
        self.scope.as_ref()
    }

    /// Returns the requested minimum retention in seconds, if one was provided.
    #[must_use]
    pub const fn min_retention_seconds(&self) -> Option<u64> {
        self.min_retention_seconds
    }

    /// Marks a checkpoint publication as covered by the caller's already
    /// claimed retention epoch. Only workspace retained-root publication may
    /// construct this mode; public checkpoint callers cannot bypass the
    /// store's own GC exclusion.
    pub(crate) const fn with_external_retention_coordination(mut self) -> Self {
        self.externally_retention_coordinated = true;
        self
    }

    pub(crate) const fn is_externally_retention_coordinated(&self) -> bool {
        self.externally_retention_coordinated
    }
}

/// Value plus generation evidence observed from a future state-store backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue {
    bytes: Bytes,
    generation: Option<u64>,
}

impl VersionedValue {
    /// Creates a versioned value.
    #[must_use]
    pub const fn new(bytes: Bytes, generation: Option<u64>) -> Self {
        Self { bytes, generation }
    }

    /// Returns the stored value bytes.
    #[must_use]
    pub const fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns generation evidence, if the backend exposed one.
    #[must_use]
    pub const fn generation(&self) -> Option<u64> {
        self.generation
    }
}

/// Half-open byte-key range used for range reads and preconditions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    start: Vec<u8>,
    end: Vec<u8>,
}

impl KeyRange {
    /// Creates a half-open key range `[start, end)`.
    #[must_use]
    pub fn new(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self {
            start: start.into(),
            end: end.into(),
        }
    }

    /// Returns the inclusive start key.
    #[must_use]
    pub fn start(&self) -> &[u8] {
        &self.start
    }

    /// Returns the exclusive end key.
    #[must_use]
    pub fn end(&self) -> &[u8] {
        &self.end
    }
}

/// Point and range inputs declared by a semantic predicate.
#[derive(Debug, Clone, Default)]
pub struct PredicateInputSet {
    point_keys: Vec<Vec<u8>>,
    ranges: Vec<KeyRange>,
    model_witness: Option<u64>,
}

impl PredicateInputSet {
    /// Creates a predicate input set from point keys and key ranges.
    #[must_use]
    pub fn new(point_keys: Vec<Vec<u8>>, ranges: Vec<KeyRange>) -> Self {
        Self {
            point_keys,
            ranges,
            model_witness: None,
        }
    }

    /// Returns point keys observed by the predicate.
    #[must_use]
    pub fn point_keys(&self) -> &[Vec<u8>] {
        &self.point_keys
    }

    /// Returns key ranges observed by the predicate.
    #[must_use]
    pub fn ranges(&self) -> &[KeyRange] {
        &self.ranges
    }

    /// Creates a predicate input set with a crate-local model witness.
    #[must_use]
    pub(crate) fn with_model_witness(
        point_keys: Vec<Vec<u8>>,
        ranges: Vec<KeyRange>,
        witness: u64,
    ) -> Self {
        Self {
            point_keys,
            ranges,
            model_witness: Some(witness),
        }
    }

    /// Returns the crate-local model witness, if one was recorded.
    #[must_use]
    pub(crate) const fn model_witness(&self) -> Option<u64> {
        self.model_witness
    }
}

impl PartialEq for PredicateInputSet {
    fn eq(&self, other: &Self) -> bool {
        self.point_keys == other.point_keys && self.ranges == other.ranges
    }
}

impl Eq for PredicateInputSet {}

/// Key/value pair returned from state-store scans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPair {
    key: Vec<u8>,
    value: VersionedValue,
}

/// Maximum decoded bytes returned by one state-store scan page.
pub const MAX_SCAN_PAGE_BYTES: usize = 4 * 1024 * 1024;

/// Maximum immutable segments a backend may fetch for one scan page.
pub const MAX_SCAN_PAGE_SEGMENTS: usize = 64;

/// Maximum rows accepted in one generic state-store scan request.
pub const MAX_SCAN_PAGE_ROWS: usize = 1_000_000;

/// Opaque cursor for the next page of one authority-pinned prefix scan.
///
/// The cursor binds the authority scope, prefix, manifest, logical sequence,
/// and exclusive last key. Callers can clone and return it but cannot alter
/// those fields independently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanContinuation {
    scope: StateScope,
    prefix: Vec<u8>,
    observed_token: StateToken,
    exclusive_last_key: Vec<u8>,
    query_binding: Option<Vec<u8>>,
}

const SCAN_CONTINUATION_VERSION: u32 = 2;
const SCAN_CONTINUATION_PREFIX: &str = "v2.";
const SCAN_CONTINUATION_AAD: &[u8] = b"arco/control-v1/scan-continuation/v2";
const SCAN_CONTINUATION_NONCE_BYTES: usize = 12;
const MAX_SCAN_CONTINUATION_ENCODED_BYTES: usize = 16 * 1024;

/// Authenticated-encryption key for protocol continuations.
///
/// The key is shared by every protocol facade through the exact-root binding
/// registry. Deployed servers construct it from one private replica-stable
/// secret. It is deliberately non-serializable and redacted from debug output;
/// explicit key rotation invalidates old cursors rather than exposing the
/// authority token they retain.
#[derive(Clone)]
pub(crate) struct ScanContinuationKey(Arc<[u8; 32]>);

impl fmt::Debug for ScanContinuationKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ScanContinuationKey(<redacted>)")
    }
}

impl ScanContinuationKey {
    pub(crate) fn generate() -> Result<Self> {
        let mut key = [0_u8; 32];
        SystemRandom::new()
            .fill(&mut key)
            .map_err(|_| CatalogError::Storage {
                message: "secure scan-continuation key generation failed".to_string(),
            })?;
        Ok(Self(Arc::new(key)))
    }

    pub(crate) fn from_bytes(key: &[u8]) -> Result<Self> {
        let key: [u8; 32] = key.try_into().map_err(|_| CatalogError::Validation {
            message: "scan-continuation key must contain exactly 32 bytes".to_string(),
        })?;
        Ok(Self(Arc::new(key)))
    }

    fn less_safe_key(&self) -> Result<LessSafeKey> {
        UnboundKey::new(&aead::AES_256_GCM, self.0.as_ref())
            .map(LessSafeKey::new)
            .map_err(|_| CatalogError::InvariantViolation {
                message: "scan-continuation key has an invalid length".to_string(),
            })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScanContinuationEnvelope {
    version: u32,
    tenant_id: String,
    workspace_id: String,
    domain: String,
    prefix_hex: String,
    manifest_id: String,
    logical_sequence: u64,
    exclusive_last_key_hex: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    query_binding_hex: Option<String>,
}

impl ScanContinuation {
    pub(crate) fn encode_opaque(&self, key: &ScanContinuationKey) -> Result<String> {
        let envelope = ScanContinuationEnvelope {
            version: SCAN_CONTINUATION_VERSION,
            tenant_id: self.scope.tenant_id().to_string(),
            workspace_id: self.scope.workspace_id().to_string(),
            domain: self.scope.domain().to_string(),
            prefix_hex: hex::encode(&self.prefix),
            manifest_id: self.observed_token.authority_manifest_id().to_string(),
            logical_sequence: self.observed_token.logical_sequence(),
            exclusive_last_key_hex: hex::encode(&self.exclusive_last_key),
            query_binding_hex: self.query_binding.as_ref().map(hex::encode),
        };
        let mut plaintext =
            serde_json::to_vec(&envelope).map_err(|error| CatalogError::Serialization {
                message: format!("failed to encode scan continuation: {error}"),
            })?;
        let mut nonce_bytes = [0_u8; SCAN_CONTINUATION_NONCE_BYTES];
        SystemRandom::new()
            .fill(&mut nonce_bytes)
            .map_err(|_| CatalogError::Storage {
                message: "secure scan-continuation nonce generation failed".to_string(),
            })?;
        key.less_safe_key()?
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(SCAN_CONTINUATION_AAD),
                &mut plaintext,
            )
            .map_err(|_| CatalogError::Serialization {
                message: "failed to seal scan continuation".to_string(),
            })?;
        let mut sealed = Vec::with_capacity(SCAN_CONTINUATION_NONCE_BYTES + plaintext.len());
        sealed.extend_from_slice(&nonce_bytes);
        sealed.extend_from_slice(&plaintext);
        Ok(format!(
            "{SCAN_CONTINUATION_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(sealed)
        ))
    }

    pub(crate) fn decode_opaque(value: &str, key: &ScanContinuationKey) -> Result<Self> {
        let encoded = value
            .strip_prefix(SCAN_CONTINUATION_PREFIX)
            .ok_or_else(|| CatalogError::Validation {
                message: "invalid opaque scan continuation".to_string(),
            })?;
        if encoded.len() > MAX_SCAN_CONTINUATION_ENCODED_BYTES {
            return Err(CatalogError::Validation {
                message: "invalid opaque scan continuation".to_string(),
            });
        }
        let mut sealed = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| CatalogError::Validation {
                message: "invalid opaque scan continuation".to_string(),
            })?;
        if sealed.len() <= SCAN_CONTINUATION_NONCE_BYTES + aead::AES_256_GCM.tag_len() {
            return Err(CatalogError::Validation {
                message: "invalid opaque scan continuation".to_string(),
            });
        }
        let nonce_bytes: [u8; SCAN_CONTINUATION_NONCE_BYTES] = sealed
            .get(..SCAN_CONTINUATION_NONCE_BYTES)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| CatalogError::Validation {
                message: "invalid opaque scan continuation".to_string(),
            })?;
        let plaintext = key
            .less_safe_key()?
            .open_in_place(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(SCAN_CONTINUATION_AAD),
                sealed
                    .get_mut(SCAN_CONTINUATION_NONCE_BYTES..)
                    .ok_or_else(|| CatalogError::Validation {
                        message: "invalid opaque scan continuation".to_string(),
                    })?,
            )
            .map_err(|_| CatalogError::Validation {
                message: "invalid opaque scan continuation".to_string(),
            })?;
        let envelope: ScanContinuationEnvelope =
            serde_json::from_slice(plaintext).map_err(|_| CatalogError::Validation {
                message: "invalid opaque scan continuation".to_string(),
            })?;
        if envelope.version != SCAN_CONTINUATION_VERSION {
            return Err(CatalogError::Validation {
                message: "unsupported opaque scan continuation version".to_string(),
            });
        }
        let prefix = hex::decode(envelope.prefix_hex).map_err(|_| CatalogError::Validation {
            message: "invalid opaque scan continuation prefix".to_string(),
        })?;
        let exclusive_last_key =
            hex::decode(envelope.exclusive_last_key_hex).map_err(|_| CatalogError::Validation {
                message: "invalid opaque scan continuation boundary".to_string(),
            })?;
        let query_binding = envelope
            .query_binding_hex
            .map(|binding| {
                hex::decode(binding).map_err(|_| CatalogError::Validation {
                    message: "invalid opaque scan continuation".to_string(),
                })
            })
            .transpose()?;
        let scope = StateScope::new(envelope.tenant_id, envelope.workspace_id, envelope.domain);
        scope.validate()?;
        Ok(Self {
            observed_token: StateToken {
                scope: scope.clone(),
                logical_sequence: envelope.logical_sequence,
                authority_manifest_id: envelope.manifest_id,
            },
            scope,
            prefix,
            exclusive_last_key,
            query_binding,
        })
    }

    pub(crate) const fn observed_token(&self) -> &StateToken {
        &self.observed_token
    }

    pub(crate) fn bind_query(mut self, query_binding: Option<&[u8]>) -> Self {
        self.query_binding = query_binding.map(<[u8]>::to_vec);
        self
    }

    pub(crate) fn validate_query_binding(&self, expected: Option<&[u8]>) -> Result<()> {
        if self.query_binding.as_deref() == expected {
            Ok(())
        } else {
            Err(CatalogError::Validation {
                message: "scan continuation query mismatch".to_string(),
            })
        }
    }
}

/// Bounded request for a prefix scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanRequest {
    prefix: Vec<u8>,
    start_after: Option<Vec<u8>>,
    max_rows: usize,
    max_bytes: usize,
    max_segments: usize,
    token: Option<ScanContinuation>,
}

impl ScanRequest {
    /// Creates a request with the production hard budgets.
    #[must_use]
    pub fn new(prefix: impl AsRef<[u8]>) -> Self {
        Self {
            prefix: prefix.as_ref().to_vec(),
            start_after: None,
            max_rows: MAX_SCAN_PAGE_ROWS,
            max_bytes: MAX_SCAN_PAGE_BYTES,
            max_segments: MAX_SCAN_PAGE_SEGMENTS,
            token: None,
        }
    }

    /// Sets the exclusive initial key for the first page.
    #[must_use]
    pub fn with_start_after(mut self, start_after: impl AsRef<[u8]>) -> Self {
        self.start_after = Some(start_after.as_ref().to_vec());
        self
    }

    /// Sets requested row, decoded-byte, and segment budgets.
    ///
    /// The request is validated by the backend so construction stays
    /// allocation-only and convenient for protocol adapters.
    #[must_use]
    pub const fn with_limits(
        mut self,
        max_rows: usize,
        max_bytes: usize,
        max_segments: usize,
    ) -> Self {
        self.max_rows = max_rows;
        self.max_bytes = max_bytes;
        self.max_segments = max_segments;
        self
    }

    /// Continues a prior authority-pinned page.
    #[must_use]
    pub fn with_token(mut self, token: ScanContinuation) -> Self {
        self.token = Some(token);
        self
    }

    /// Returns the requested prefix.
    #[must_use]
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    /// Returns the exclusive initial key, if supplied.
    #[must_use]
    pub fn start_after(&self) -> Option<&[u8]> {
        self.start_after.as_deref()
    }

    /// Returns the maximum rows requested for the page.
    #[must_use]
    pub const fn max_rows(&self) -> usize {
        self.max_rows
    }

    /// Returns the maximum decoded bytes requested for the page.
    #[must_use]
    pub const fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Returns the maximum segments requested for the page.
    #[must_use]
    pub const fn max_segments(&self) -> usize {
        self.max_segments
    }

    fn validate_for_scope(&self, scope: &StateScope) -> Result<()> {
        if self.max_rows == 0 || self.max_rows > MAX_SCAN_PAGE_ROWS {
            return Err(CatalogError::Validation {
                message: format!("scan max_rows must be between 1 and {MAX_SCAN_PAGE_ROWS}"),
            });
        }
        if self.max_bytes == 0 || self.max_bytes > MAX_SCAN_PAGE_BYTES {
            return Err(CatalogError::Validation {
                message: format!("scan max_bytes must be between 1 and {MAX_SCAN_PAGE_BYTES}"),
            });
        }
        if self.max_segments == 0 || self.max_segments > MAX_SCAN_PAGE_SEGMENTS {
            return Err(CatalogError::Validation {
                message: format!(
                    "scan max_segments must be between 1 and {MAX_SCAN_PAGE_SEGMENTS}"
                ),
            });
        }
        if self
            .start_after
            .as_ref()
            .is_some_and(|key| !key.starts_with(&self.prefix))
        {
            return Err(CatalogError::Validation {
                message: "scan start_after must be inside the requested prefix".to_string(),
            });
        }
        if let Some(token) = &self.token {
            if self.start_after.is_some() {
                return Err(CatalogError::Validation {
                    message: "scan continuation cannot be combined with start_after".to_string(),
                });
            }
            if &token.scope != scope {
                return Err(CatalogError::Validation {
                    message: "scan continuation scope mismatch".to_string(),
                });
            }
            if token.prefix != self.prefix {
                return Err(CatalogError::Validation {
                    message: "scan continuation prefix mismatch".to_string(),
                });
            }
            if !token.exclusive_last_key.starts_with(&self.prefix) {
                return Err(CatalogError::Validation {
                    message: "scan continuation last key is outside its prefix".to_string(),
                });
            }
        }
        Ok(())
    }

    fn continuation_token(&self) -> Option<&StateToken> {
        self.token.as_ref().map(|token| &token.observed_token)
    }

    fn effective_start_after(&self) -> Option<&[u8]> {
        self.token
            .as_ref()
            .map_or(self.start_after.as_deref(), |token| {
                Some(token.exclusive_last_key.as_slice())
            })
    }
}

/// One bounded page from an authority-pinned prefix scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPage {
    entries: Vec<KvPair>,
    continuation: Option<ScanContinuation>,
    observed_token: Option<StateToken>,
}

impl ScanPage {
    /// Returns entries in ascending binary-key order.
    #[must_use]
    pub fn entries(&self) -> &[KvPair] {
        &self.entries
    }

    /// Returns the opaque cursor for a subsequent page, if more entries exist.
    #[must_use]
    pub const fn continuation(&self) -> Option<&ScanContinuation> {
        self.continuation.as_ref()
    }

    /// Returns the authority cut observed by this page.
    ///
    /// An empty, never-committed backend has no authority token and returns
    /// `None`; any non-empty page is always bound to a token.
    #[must_use]
    pub const fn observed_token(&self) -> Option<&StateToken> {
        self.observed_token.as_ref()
    }
}

pub(crate) fn build_scan_page(
    scope: &StateScope,
    request: ScanRequest,
    observed_token: Option<StateToken>,
    entries: impl IntoIterator<Item = KvPair>,
) -> Result<ScanPage> {
    request.validate_for_scope(scope)?;
    if let Some(continued) = request.continuation_token()
        && observed_token.as_ref() != Some(continued)
    {
        return Err(CatalogError::Validation {
            message: "scan continuation authority mismatch".to_string(),
        });
    }

    let start_after = request.effective_start_after();
    let mut page_entries = Vec::new();
    let mut decoded_bytes = 0_usize;
    let mut has_more = false;
    for entry in entries.into_iter().filter(|entry| {
        entry.key().starts_with(request.prefix())
            && start_after.is_none_or(|start| entry.key() > start)
    }) {
        let entry_bytes = entry
            .key()
            .len()
            .checked_add(entry.value().bytes().len())
            .ok_or_else(|| CatalogError::Validation {
                message: "scan entry decoded byte size overflow".to_string(),
            })?;
        let would_exceed_rows = page_entries.len() == request.max_rows();
        let would_exceed_bytes = decoded_bytes
            .checked_add(entry_bytes)
            .is_none_or(|total| total > request.max_bytes());
        if would_exceed_rows || would_exceed_bytes {
            if page_entries.is_empty() {
                return Err(CatalogError::Validation {
                    message: format!(
                        "scan entry requires {entry_bytes} decoded bytes, above the requested page budget {}",
                        request.max_bytes()
                    ),
                });
            }
            has_more = true;
            break;
        }
        decoded_bytes += entry_bytes;
        page_entries.push(entry);
    }

    let continuation = if has_more {
        let observed_token =
            observed_token
                .as_ref()
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "non-empty continued scan has no observed authority token".to_string(),
                })?;
        let exclusive_last_key = page_entries
            .last()
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: "continued scan page is unexpectedly empty".to_string(),
            })?
            .key()
            .to_vec();
        Some(ScanContinuation {
            scope: scope.clone(),
            prefix: request.prefix,
            observed_token: observed_token.clone(),
            exclusive_last_key,
            query_binding: None,
        })
    } else {
        None
    };

    Ok(ScanPage {
        entries: page_entries,
        continuation,
        observed_token,
    })
}

pub(crate) fn build_scan_page_with_backend_boundary(
    scope: &StateScope,
    request: ScanRequest,
    observed_token: Option<StateToken>,
    entries: impl IntoIterator<Item = KvPair>,
    backend_resume_after: Option<Vec<u8>>,
) -> Result<ScanPage> {
    let prefix = request.prefix.clone();
    let prior_boundary = request.effective_start_after().map(<[u8]>::to_vec);
    let mut page = build_scan_page(scope, request, observed_token.clone(), entries)?;
    if page.continuation.is_none()
        && let Some(exclusive_last_key) = backend_resume_after
    {
        if !exclusive_last_key.starts_with(&prefix)
            || prior_boundary
                .as_deref()
                .is_some_and(|prior| exclusive_last_key.as_slice() <= prior)
        {
            return Err(CatalogError::InvariantViolation {
                message: "scan backend continuation boundary is not monotonic inside its prefix"
                    .to_string(),
            });
        }
        let observed_token = observed_token.ok_or_else(|| CatalogError::InvariantViolation {
            message: "continued backend scan has no observed authority token".to_string(),
        })?;
        page.continuation = Some(ScanContinuation {
            scope: scope.clone(),
            prefix,
            observed_token,
            exclusive_last_key,
            query_binding: None,
        });
    }
    Ok(page)
}

impl KvPair {
    /// Creates a key/value pair.
    #[must_use]
    pub fn new(key: impl Into<Vec<u8>>, value: VersionedValue) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }

    /// Returns the key bytes.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value and generation evidence.
    #[must_use]
    pub const fn value(&self) -> &VersionedValue {
        &self.value
    }
}

/// Authority scope addressed by state-store tokens and transactions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateScope {
    tenant_id: String,
    workspace_id: String,
    domain: String,
}

impl StateScope {
    /// Creates an authority scope.
    #[must_use]
    pub fn new(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        domain: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            workspace_id: workspace_id.into(),
            domain: domain.into(),
        }
    }

    /// Returns the tenant identifier.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the workspace identifier.
    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    /// Returns the state-store domain name.
    #[must_use]
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Validates that every scope component is one nonblank, printable, path-safe value.
    ///
    /// # Errors
    ///
    /// Returns a validation error for blank values, separators, dot segments, or controls.
    pub fn validate(&self) -> Result<()> {
        validate_scope_component(&self.tenant_id, "tenant_id")?;
        validate_scope_component(&self.workspace_id, "workspace_id")?;
        validate_scope_component(&self.domain, "domain")
    }
}

fn validate_scope_component(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty()
        || matches!(value, "." | "..")
        || value.contains(['/', '\\'])
        || value.chars().any(char::is_control)
    {
        return Err(CatalogError::Validation {
            message: format!(
                "{field} must be a nonblank path-safe component without separators, dot segments, or control characters"
            ),
        });
    }
    Ok(())
}

fn validate_authority_relative_path(path: &str, field: &str) -> Result<()> {
    if path.is_empty()
        || path.starts_with('/')
        || matches!(
            path.as_bytes(),
            [drive, b':', ..] if drive.is_ascii_alphabetic()
        )
        || path.contains('\\')
        || path.chars().any(char::is_control)
        || path
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(CatalogError::Validation {
            message: format!("{field} must be a canonical relative path"),
        });
    }
    Ok(())
}

fn validate_prefixed_sha256(value: &str, field: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(CatalogError::Validation {
            message: format!("{field} must use the sha256: prefix"),
        });
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CatalogError::Validation {
            message: format!("{field} must contain 64 lowercase hexadecimal characters"),
        });
    }
    Ok(())
}

/// Stable kind of authority named by a persisted reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedAuthorityKind {
    /// A retained state token backed directly by an authority manifest.
    StateToken,
    /// A retained checkpoint backed by both checkpoint and manifest objects.
    Checkpoint,
}

/// Serializable, validated storage reference for otherwise opaque authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedAuthorityReference {
    implementation: String,
    scope: StateScope,
    reference_kind: PersistedAuthorityKind,
    manifest_id: String,
    logical_sequence: u64,
    manifest_path: String,
    manifest_sha256: String,
    checkpoint_path: Option<String>,
    checkpoint_sha256: Option<String>,
    retention_deadline: DateTime<Utc>,
}

/// Deterministic identity of one domain participant inside a restore aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreAttemptIdentity {
    restore_id: String,
    attempt: u64,
    domain: String,
}

impl RestoreAttemptIdentity {
    /// Creates a validated participant attempt identity.
    ///
    /// # Errors
    ///
    /// Returns a validation error for a malformed restore ID, zero attempt, or domain.
    pub fn new(
        restore_id: impl Into<String>,
        attempt: u64,
        domain: impl Into<String>,
    ) -> Result<Self> {
        let identity = Self {
            restore_id: restore_id.into(),
            attempt,
            domain: domain.into(),
        };
        crate::workspace_restore::restore_request_path(&identity.restore_id)?;
        if identity.attempt == 0 {
            return Err(CatalogError::Validation {
                message: "restore participant attempt must be positive".to_string(),
            });
        }
        validate_scope_component(&identity.domain, "restore domain")?;
        Ok(identity)
    }

    /// Returns the canonical restore ID.
    #[must_use]
    pub fn restore_id(&self) -> &str {
        &self.restore_id
    }

    /// Returns the originating participant attempt number.
    #[must_use]
    pub const fn attempt(&self) -> u64 {
        self.attempt
    }

    /// Returns the exact domain name.
    #[must_use]
    pub fn domain(&self) -> &str {
        &self.domain
    }
}

/// Typed durable plan produced by an explicitly configured restore adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "plan_kind", rename_all = "snake_case")]
pub enum PersistedRestoreParticipantPlan {
    /// Deterministic object-store Control MVP transaction plan.
    ControlMvp(ControlMvpRestorePlan),
}

/// Stable, serializable proof of one visible restored authority manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoredAuthorityEvidence {
    implementation: String,
    scope: StateScope,
    transaction_id: String,
    manifest_id: String,
    manifest_path: String,
    manifest_sha256: String,
    logical_sequence: u64,
    participant_attempt: u64,
}

impl RestoredAuthorityEvidence {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        implementation: impl Into<String>,
        scope: StateScope,
        transaction_id: impl Into<String>,
        manifest_id: impl Into<String>,
        manifest_path: impl Into<String>,
        manifest_sha256: impl Into<String>,
        logical_sequence: u64,
        participant_attempt: u64,
    ) -> Result<Self> {
        let evidence = Self {
            implementation: implementation.into(),
            scope,
            transaction_id: transaction_id.into(),
            manifest_id: manifest_id.into(),
            manifest_path: manifest_path.into(),
            manifest_sha256: manifest_sha256.into(),
            logical_sequence,
            participant_attempt,
        };
        evidence.validate()?;
        Ok(evidence)
    }

    /// Revalidates every durable evidence field after deserialization.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed scope, identifiers, path, digest,
    /// or non-positive sequence fields.
    pub fn validate(&self) -> Result<()> {
        validate_scope_component(&self.implementation, "restore implementation")?;
        self.scope.validate()?;
        validate_scope_component(&self.transaction_id, "restore transaction_id")?;
        validate_scope_component(&self.manifest_id, "restore manifest_id")?;
        validate_authority_relative_path(&self.manifest_path, "restore manifest_path")?;
        validate_prefixed_sha256(&self.manifest_sha256, "restore manifest_sha256")?;
        if self.logical_sequence == 0 || self.participant_attempt == 0 {
            return Err(CatalogError::Validation {
                message: "restore evidence sequences must be positive".to_string(),
            });
        }
        Ok(())
    }

    /// Returns the state-store implementation.
    #[must_use]
    pub fn implementation(&self) -> &str {
        &self.implementation
    }

    /// Returns the exact state scope.
    #[must_use]
    pub const fn scope(&self) -> &StateScope {
        &self.scope
    }

    /// Returns the deterministic transaction ID.
    #[must_use]
    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }

    /// Returns the immutable manifest ID.
    #[must_use]
    pub fn manifest_id(&self) -> &str {
        &self.manifest_id
    }

    /// Returns the immutable manifest path.
    #[must_use]
    pub fn manifest_path(&self) -> &str {
        &self.manifest_path
    }

    /// Returns the exact manifest digest.
    #[must_use]
    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    /// Returns the restored logical sequence.
    #[must_use]
    pub const fn logical_sequence(&self) -> u64 {
        self.logical_sequence
    }

    /// Returns the originating participant attempt.
    #[must_use]
    pub const fn participant_attempt(&self) -> u64 {
        self.participant_attempt
    }
}

/// Exact-path inspection result for a deterministic participant restore plan.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum RestoreParticipantInspection {
    /// The exact planned base is still selected and the plan may be applied.
    Ready,
    /// The planned transaction occurs in current checksum-valid lineage.
    Visible {
        /// Opaque in-memory token for the restored manifest.
        token: StateToken,
        /// Stable evidence safe for durable journal records.
        evidence: RestoredAuthorityEvidence,
    },
    /// The planned base CAS is irreversibly lost and the plan is not in lineage.
    Superseded,
}

impl PersistedAuthorityReference {
    /// Creates and validates a stable persisted authority reference.
    ///
    /// # Errors
    ///
    /// Returns a validation error for incoherent kinds, scopes, paths, or digests.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        implementation: impl Into<String>,
        scope: StateScope,
        reference_kind: PersistedAuthorityKind,
        manifest_id: impl Into<String>,
        logical_sequence: u64,
        manifest_path: impl Into<String>,
        manifest_sha256: impl Into<String>,
        checkpoint_path: Option<String>,
        checkpoint_sha256: Option<String>,
        retention_deadline: DateTime<Utc>,
    ) -> Result<Self> {
        let reference = Self {
            implementation: implementation.into(),
            scope,
            reference_kind,
            manifest_id: manifest_id.into(),
            logical_sequence,
            manifest_path: manifest_path.into(),
            manifest_sha256: manifest_sha256.into(),
            checkpoint_path,
            checkpoint_sha256,
            retention_deadline,
        };
        reference.validate()?;
        Ok(reference)
    }

    /// Revalidates all persisted fields before the reference is trusted.
    ///
    /// # Errors
    ///
    /// Returns a validation error for malformed or kind-incoherent fields.
    pub fn validate(&self) -> Result<()> {
        validate_scope_component(&self.implementation, "implementation")?;
        self.scope.validate()?;
        validate_scope_component(&self.manifest_id, "manifest_id")?;
        validate_authority_relative_path(&self.manifest_path, "manifest_path")?;
        validate_prefixed_sha256(&self.manifest_sha256, "manifest_sha256")?;
        match self.reference_kind {
            PersistedAuthorityKind::StateToken => {
                if self.checkpoint_path.is_some() || self.checkpoint_sha256.is_some() {
                    return Err(CatalogError::Validation {
                        message: "state_token references must omit checkpoint fields".to_string(),
                    });
                }
            }
            PersistedAuthorityKind::Checkpoint => {
                let Some(path) = self.checkpoint_path.as_deref() else {
                    return Err(CatalogError::Validation {
                        message: "checkpoint references require checkpoint_path".to_string(),
                    });
                };
                let Some(digest) = self.checkpoint_sha256.as_deref() else {
                    return Err(CatalogError::Validation {
                        message: "checkpoint references require checkpoint_sha256".to_string(),
                    });
                };
                validate_authority_relative_path(path, "checkpoint_path")?;
                validate_prefixed_sha256(digest, "checkpoint_sha256")?;
            }
        }
        Ok(())
    }

    /// Returns the stable backend implementation identifier.
    #[must_use]
    pub fn implementation(&self) -> &str {
        &self.implementation
    }

    /// Returns the repeated authority scope.
    #[must_use]
    pub const fn scope(&self) -> &StateScope {
        &self.scope
    }

    /// Returns the persisted reference kind.
    #[must_use]
    pub const fn reference_kind(&self) -> PersistedAuthorityKind {
        self.reference_kind
    }

    /// Returns the authority manifest identifier.
    #[must_use]
    pub fn manifest_id(&self) -> &str {
        &self.manifest_id
    }

    /// Returns the logical authority sequence.
    #[must_use]
    pub const fn logical_sequence(&self) -> u64 {
        self.logical_sequence
    }

    /// Returns the workspace-relative authority manifest path.
    #[must_use]
    pub fn manifest_path(&self) -> &str {
        &self.manifest_path
    }

    /// Returns the checksum of the exact stored manifest bytes.
    #[must_use]
    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    /// Returns the workspace-relative checkpoint path, when applicable.
    #[must_use]
    pub fn checkpoint_path(&self) -> Option<&str> {
        self.checkpoint_path.as_deref()
    }

    /// Returns the checksum of the exact stored checkpoint bytes, when applicable.
    #[must_use]
    pub fn checkpoint_sha256(&self) -> Option<&str> {
        self.checkpoint_sha256.as_deref()
    }

    /// Returns the absolute retention deadline carried by the reference.
    #[must_use]
    pub const fn retention_deadline(&self) -> DateTime<Utc> {
        self.retention_deadline
    }
}

/// Backend capabilities exposed by a state-store implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateStoreCapabilities {
    /// Stable implementation identifier.
    implementation: &'static str,
    flags: StateStoreCapabilityFlags,
}

impl StateStoreCapabilities {
    /// Returns the explicit capabilities of the current-authority adapter.
    #[must_use]
    pub const fn arco_state_current() -> Self {
        Self {
            implementation: CurrentStateStore::IMPLEMENTATION,
            flags: StateStoreCapabilityFlags::empty(),
        }
    }

    pub(crate) const fn deterministic_model(implementation: &'static str) -> Self {
        Self {
            implementation,
            flags: StateStoreCapabilityFlags::RETAINED_STATE_TOKENS
                .union(StateStoreCapabilityFlags::READ_AT)
                .union(StateStoreCapabilityFlags::TRANSACTIONS)
                .union(StateStoreCapabilityFlags::RANGE_PRECONDITIONS)
                .union(StateStoreCapabilityFlags::PREDICATE_PRECONDITIONS),
        }
    }

    pub(crate) const fn control_mvp(implementation: &'static str) -> Self {
        Self {
            implementation,
            flags: StateStoreCapabilityFlags::RETAINED_STATE_TOKENS
                .union(StateStoreCapabilityFlags::CHECKPOINTS)
                .union(StateStoreCapabilityFlags::READ_AT)
                .union(StateStoreCapabilityFlags::TRANSACTIONS)
                .union(StateStoreCapabilityFlags::RANGE_PRECONDITIONS)
                .union(StateStoreCapabilityFlags::PREDICATE_PRECONDITIONS)
                .union(StateStoreCapabilityFlags::ROLL_FORWARD_RESTORE),
        }
    }

    /// Returns the stable implementation identifier.
    #[must_use]
    pub const fn implementation(&self) -> &'static str {
        self.implementation
    }

    /// Returns whether retained `StateToken` reads and issuance are supported.
    #[must_use]
    pub const fn retained_state_tokens(&self) -> bool {
        self.flags
            .contains(StateStoreCapabilityFlags::RETAINED_STATE_TOKENS)
    }

    /// Returns whether retained checkpoints are supported.
    #[must_use]
    pub const fn checkpoints(&self) -> bool {
        self.flags.contains(StateStoreCapabilityFlags::CHECKPOINTS)
    }

    /// Returns whether addressed historical reads through `read_at` are supported.
    #[must_use]
    pub const fn read_at(&self) -> bool {
        self.flags.contains(StateStoreCapabilityFlags::READ_AT)
    }

    /// Returns whether write transactions are supported.
    #[must_use]
    pub const fn transactions(&self) -> bool {
        self.flags.contains(StateStoreCapabilityFlags::TRANSACTIONS)
    }

    /// Returns whether range preconditions are supported.
    #[must_use]
    pub const fn range_preconditions(&self) -> bool {
        self.flags
            .contains(StateStoreCapabilityFlags::RANGE_PRECONDITIONS)
    }

    /// Returns whether semantic predicate input-set preconditions are supported.
    #[must_use]
    pub const fn predicate_preconditions(&self) -> bool {
        self.flags
            .contains(StateStoreCapabilityFlags::PREDICATE_PRECONDITIONS)
    }

    /// Returns whether an explicit deterministic roll-forward restore adapter is available.
    #[must_use]
    pub const fn roll_forward_restore(&self) -> bool {
        self.flags
            .contains(StateStoreCapabilityFlags::ROLL_FORWARD_RESTORE)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StateStoreCapabilityFlags(u8);

impl StateStoreCapabilityFlags {
    const RETAINED_STATE_TOKENS: Self = Self(1 << 0);
    const CHECKPOINTS: Self = Self(1 << 1);
    const READ_AT: Self = Self(1 << 2);
    const TRANSACTIONS: Self = Self(1 << 3);
    const RANGE_PRECONDITIONS: Self = Self(1 << 4);
    const PREDICATE_PRECONDITIONS: Self = Self(1 << 5);
    const ROLL_FORWARD_RESTORE: Self = Self(1 << 6);

    const fn empty() -> Self {
        Self(0)
    }

    const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    const fn contains(self, flag: Self) -> bool {
        self.0 & flag.0 == flag.0
    }
}

/// Read-only state-store operations.
#[async_trait]
pub trait ArcoStateReader: Send + Sync {
    /// Reads the current value for a key.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot perform the read.
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Scans current key/value pairs under explicit row, byte, and segment budgets.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot perform the scan.
    async fn scan(&self, request: ScanRequest) -> Result<ScanPage>;

    /// Opens a retained reader at a specific state token.
    ///
    /// # Errors
    ///
    /// Returns an error when retained token reads are unsupported or invalid.
    async fn read_at(&self, token: StateToken) -> Result<Box<dyn ArcoStateReader>>;

    /// Opens a retained reader at a checkpoint token.
    ///
    /// # Errors
    ///
    /// Returns an error when checkpoint reads are unsupported or invalid.
    async fn read_checkpoint(&self, token: CheckpointToken) -> Result<Box<dyn ArcoStateReader>>;
}

pub(crate) async fn scan_all_entries_bounded(
    reader: &dyn ArcoStateReader,
    prefix: &[u8],
    max_total_rows: usize,
    max_total_bytes: usize,
) -> Result<Vec<KvPair>> {
    if max_total_rows == 0 || max_total_bytes == 0 {
        return Err(CatalogError::Validation {
            message: "bounded scan aggregate limits must be positive".to_string(),
        });
    }
    let mut entries = Vec::new();
    let mut decoded_bytes = 0_usize;
    let mut continuation = None;
    loop {
        let mut request = ScanRequest::new(prefix).with_limits(
            MAX_SCAN_PAGE_ROWS,
            MAX_SCAN_PAGE_BYTES,
            MAX_SCAN_PAGE_SEGMENTS,
        );
        if let Some(token) = continuation.take() {
            request = request.with_token(token);
        }
        let page = reader.scan(request).await?;
        for entry in page.entries {
            let entry_bytes = entry
                .key()
                .len()
                .checked_add(entry.value().bytes().len())
                .ok_or_else(|| CatalogError::MaintenanceBackpressure {
                    message: "bounded scan aggregate byte count overflow".to_string(),
                })?;
            decoded_bytes = decoded_bytes.checked_add(entry_bytes).ok_or_else(|| {
                CatalogError::MaintenanceBackpressure {
                    message: "bounded scan aggregate byte count overflow".to_string(),
                }
            })?;
            if entries.len() == max_total_rows || decoded_bytes > max_total_bytes {
                return Err(CatalogError::MaintenanceBackpressure {
                    message: format!(
                        "bounded scan exceeds aggregate limit of {max_total_rows} rows or {max_total_bytes} decoded bytes"
                    ),
                });
            }
            entries.push(entry);
        }
        continuation = page.continuation;
        if continuation.is_none() {
            return Ok(entries);
        }
    }
}

/// Administrative state-store operations.
#[async_trait]
pub trait ArcoStateAdmin: Send + Sync {
    /// Returns this implementation's capability matrix.
    fn capabilities(&self) -> StateStoreCapabilities;

    /// Issues a token for current retained state.
    ///
    /// # Errors
    ///
    /// Returns an error when retained state tokens are unsupported.
    async fn current_state_token(&self) -> Result<StateToken>;

    /// Creates a retained checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when checkpoints are unsupported.
    async fn checkpoint(&self, opts: CheckpointOptions) -> Result<CheckpointToken>;
}

/// Adapter between opaque state tokens and validated durable storage references.
///
/// This surface is deliberately separate from [`ArcoStateAdmin`] so backends
/// without durable object references do not fabricate them.
#[async_trait]
pub trait PersistedAuthorityAdapter: Send + Sync {
    /// Converts an opaque state token into a validated stable storage reference.
    ///
    /// # Errors
    ///
    /// Returns an error when the token cannot be verified or retained durably.
    async fn persist_state_reference(
        &self,
        token: &StateToken,
        retention_deadline: DateTime<Utc>,
    ) -> Result<PersistedAuthorityReference>;

    /// Converts an opaque checkpoint token into a validated stable storage reference.
    ///
    /// # Errors
    ///
    /// Returns an error when the checkpoint cannot be verified or retained durably.
    async fn persist_checkpoint_reference(
        &self,
        token: &CheckpointToken,
        retention_deadline: DateTime<Utc>,
    ) -> Result<PersistedAuthorityReference>;

    /// Resolves a stable reference after revalidating every persisted field.
    ///
    /// # Errors
    ///
    /// Returns an error for expired, corrupt, incompatible, or out-of-scope references.
    async fn resolve_persisted_reference(
        &self,
        reference: &PersistedAuthorityReference,
    ) -> Result<Box<dyn ArcoStateReader>> {
        self.resolve_persisted_reference_at(reference, Utc::now())
            .await
    }

    /// Resolves a stable reference at an explicit decision time.
    ///
    /// # Errors
    ///
    /// Returns an error for expired, corrupt, incompatible, or out-of-scope references.
    async fn resolve_persisted_reference_at(
        &self,
        reference: &PersistedAuthorityReference,
        now: DateTime<Utc>,
    ) -> Result<Box<dyn ArcoStateReader>>;
}

/// Explicit adapter for a backend that can durably plan, inspect, and apply restore.
///
/// This seam is separate from [`ArcoStateStore`]: generic transactions do not
/// provide deterministic identities or crash-recovery evidence.
#[async_trait]
pub trait StateRestoreParticipant: Send + Sync {
    /// Returns the stable backend implementation identifier.
    fn implementation(&self) -> &'static str;

    /// Returns the exact domain authority scope.
    fn scope(&self) -> &StateScope;

    /// Returns an opaque identity for the backend authority this adapter mutates.
    ///
    /// Every usable restore adapter must explicitly identify its backing authority.
    fn restore_binding_identity(&self) -> StateStoreBindingIdentity;

    /// Builds a deterministic, read-only durable restore plan.
    ///
    /// # Errors
    ///
    /// Returns an error for incompatible, expired, corrupt, or unplannable authority.
    async fn plan_restore(
        &self,
        source: &PersistedAuthorityReference,
        identity: &RestoreAttemptIdentity,
        now: DateTime<Utc>,
    ) -> Result<PersistedRestoreParticipantPlan>;

    /// Inspects exact durable evidence without mutation or listing.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt or ambiguous evidence.
    async fn inspect_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
    ) -> Result<RestoreParticipantInspection>;

    /// Applies only the supplied deterministic plan through the backend's existing CAS.
    ///
    /// # Errors
    ///
    /// Returns an error for expired/corrupt evidence or failed immutable writes.
    async fn apply_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
        now: DateTime<Utc>,
    ) -> Result<RestoreParticipantInspection>;
}

/// Combined state-store read, admin, and transaction surface.
#[async_trait]
pub trait ArcoStateStore: ArcoStateReader + ArcoStateAdmin {
    /// Returns an opaque identity for the backend authority this store selects.
    ///
    /// Stores that do not support deterministic roll-forward restore may leave
    /// this unavailable.
    fn restore_binding_identity(&self) -> Option<StateStoreBindingIdentity> {
        None
    }

    /// Begins a write transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when transactions are unsupported.
    async fn begin_txn(&self, opts: TxnOptions) -> Result<Box<dyn ArcoStateTxn>>;
}

/// Mutable state-store transaction.
#[async_trait]
pub trait ArcoStateTxn: Send + Sync {
    /// Reads a value inside the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot perform the read.
    async fn get(&mut self, key: &[u8]) -> Result<Option<VersionedValue>>;

    /// Scans key/value pairs under explicit budgets inside the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot perform the scan.
    async fn scan(&mut self, request: ScanRequest) -> Result<ScanPage>;

    /// Stages a value write.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot stage the write.
    async fn put(&mut self, key: &[u8], value: Bytes) -> Result<()>;

    /// Stages a value delete.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot stage the delete.
    async fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Asserts that a key is absent at commit time.
    ///
    /// # Errors
    ///
    /// Returns an error when the assertion cannot be recorded or validated.
    async fn assert_absent(&mut self, key: &[u8]) -> Result<()>;

    /// Asserts that a key has the expected generation at commit time.
    ///
    /// # Errors
    ///
    /// Returns an error when the assertion cannot be recorded or validated.
    async fn assert_generation(&mut self, key: &[u8], generation: u64) -> Result<()>;

    /// Asserts that a key range is empty at commit time.
    ///
    /// # Errors
    ///
    /// Returns an error when range preconditions are unsupported or invalid.
    async fn assert_range_empty(&mut self, range: KeyRange) -> Result<()>;

    /// Asserts that a key range is unchanged at commit time.
    ///
    /// # Errors
    ///
    /// Returns an error when range preconditions are unsupported or invalid.
    async fn assert_range_unchanged(
        &mut self,
        range: KeyRange,
        observed_generation: u64,
    ) -> Result<()>;

    /// Records point and range inputs used by a semantic predicate.
    ///
    /// # Errors
    ///
    /// Returns an error when predicate input tracking is unsupported or invalid.
    async fn read_set(
        &mut self,
        keys: &[Vec<u8>],
        ranges: &[KeyRange],
    ) -> Result<PredicateInputSet>;

    /// Asserts that previously declared predicate inputs are unchanged.
    ///
    /// # Errors
    ///
    /// Returns an error when predicate preconditions are unsupported or invalid.
    async fn assert_inputs_unchanged(&mut self, inputs: PredicateInputSet) -> Result<()>;

    /// Commits the transaction and returns its authority token and projection intents.
    ///
    /// # Errors
    ///
    /// Returns an error when commit fails or transactions are unsupported.
    async fn commit(self: Box<Self>) -> Result<CommitOutcome>;

    /// Rolls back the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when rollback fails or transactions are unsupported.
    async fn rollback(self: Box<Self>) -> Result<()>;
}

pub(crate) async fn scan_txn_all_entries_bounded(
    txn: &mut dyn ArcoStateTxn,
    prefix: &[u8],
    max_total_rows: usize,
    max_total_bytes: usize,
) -> Result<Vec<KvPair>> {
    if max_total_rows == 0 || max_total_bytes == 0 {
        return Err(CatalogError::Validation {
            message: "bounded transaction scan aggregate limits must be positive".to_string(),
        });
    }
    let mut entries = Vec::new();
    let mut decoded_bytes = 0_usize;
    let mut continuation = None;
    loop {
        let mut request = ScanRequest::new(prefix).with_limits(
            MAX_SCAN_PAGE_ROWS,
            MAX_SCAN_PAGE_BYTES,
            MAX_SCAN_PAGE_SEGMENTS,
        );
        if let Some(token) = continuation.take() {
            request = request.with_token(token);
        }
        let page = txn.scan(request).await?;
        for entry in page.entries {
            let entry_bytes = entry
                .key()
                .len()
                .checked_add(entry.value().bytes().len())
                .ok_or_else(|| CatalogError::MaintenanceBackpressure {
                    message: "bounded transaction scan aggregate byte count overflow".to_string(),
                })?;
            decoded_bytes = decoded_bytes.checked_add(entry_bytes).ok_or_else(|| {
                CatalogError::MaintenanceBackpressure {
                    message: "bounded transaction scan aggregate byte count overflow".to_string(),
                }
            })?;
            if entries.len() == max_total_rows || decoded_bytes > max_total_bytes {
                return Err(CatalogError::MaintenanceBackpressure {
                    message: format!(
                        "bounded transaction scan exceeds aggregate limit of {max_total_rows} rows or {max_total_bytes} decoded bytes"
                    ),
                });
            }
            entries.push(entry);
        }
        continuation = page.continuation;
        if continuation.is_none() {
            return Ok(entries);
        }
    }
}

/// Capability-only adapter for today's ledger plus synchronous compactor path.
#[derive(Debug, Clone, Copy, Default)]
pub struct CurrentStateStore;

impl CurrentStateStore {
    /// Stable implementation identifier for the current-authority adapter.
    pub const IMPLEMENTATION: &'static str = "arco-state-current";

    /// Creates a current-authority state-store adapter.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ArcoStateReader for CurrentStateStore {
    async fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        Err(unsupported("point reads through arco-state-current"))
    }

    async fn scan(&self, _request: ScanRequest) -> Result<ScanPage> {
        Err(unsupported("range reads through arco-state-current"))
    }

    async fn read_at(&self, _token: StateToken) -> Result<Box<dyn ArcoStateReader>> {
        Err(unsupported("StateToken reads through arco-state-current"))
    }

    async fn read_checkpoint(&self, _token: CheckpointToken) -> Result<Box<dyn ArcoStateReader>> {
        Err(unsupported(
            "CheckpointToken reads through arco-state-current",
        ))
    }
}

#[async_trait]
impl ArcoStateAdmin for CurrentStateStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        StateStoreCapabilities::arco_state_current()
    }

    async fn current_state_token(&self) -> Result<StateToken> {
        Err(unsupported(
            "StateToken issuance through arco-state-current",
        ))
    }

    async fn checkpoint(&self, _opts: CheckpointOptions) -> Result<CheckpointToken> {
        Err(unsupported(
            "CheckpointToken issuance through arco-state-current",
        ))
    }
}

#[async_trait]
impl PersistedAuthorityAdapter for CurrentStateStore {
    async fn persist_state_reference(
        &self,
        _token: &StateToken,
        _retention_deadline: DateTime<Utc>,
    ) -> Result<PersistedAuthorityReference> {
        Err(unsupported(
            "persisted StateToken references through arco-state-current",
        ))
    }

    async fn persist_checkpoint_reference(
        &self,
        _token: &CheckpointToken,
        _retention_deadline: DateTime<Utc>,
    ) -> Result<PersistedAuthorityReference> {
        Err(unsupported(
            "persisted CheckpointToken references through arco-state-current",
        ))
    }

    async fn resolve_persisted_reference_at(
        &self,
        _reference: &PersistedAuthorityReference,
        _now: DateTime<Utc>,
    ) -> Result<Box<dyn ArcoStateReader>> {
        Err(unsupported(
            "persisted authority resolution through arco-state-current",
        ))
    }
}

#[async_trait]
impl ArcoStateStore for CurrentStateStore {
    async fn begin_txn(&self, _opts: TxnOptions) -> Result<Box<dyn ArcoStateTxn>> {
        Err(unsupported("transactions through arco-state-current"))
    }
}

fn unsupported(operation: &str) -> CatalogError {
    CatalogError::UnsupportedOperation {
        message: format!(
            "{operation} are not supported; the current adapter is a capability surface only"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_unsupported<T>(result: Result<T>, expected: &str) {
        match result {
            Err(CatalogError::UnsupportedOperation { .. }) => {}
            Err(error) => panic!("expected UnsupportedOperation for {expected}, got {error:?}"),
            Ok(_) => panic!("expected UnsupportedOperation for {expected}"),
        }
    }

    #[tokio::test]
    async fn current_state_store_rejects_read_at_with_internal_token() {
        let token = StateToken::for_test(
            StateScope::new("tenant", "workspace", "catalog"),
            1,
            "manifest-1",
        );

        assert_unsupported(CurrentStateStore::new().read_at(token).await, "read_at");
    }

    #[tokio::test]
    async fn current_state_store_rejects_read_checkpoint_with_internal_token() {
        let token = CheckpointToken {
            scope: StateScope::new("tenant", "workspace", "catalog"),
            checkpoint_id: "checkpoint-1".to_string(),
        };

        assert_unsupported(
            CurrentStateStore::new().read_checkpoint(token).await,
            "read_checkpoint",
        );
    }
}
