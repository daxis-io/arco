//! State-store seam for future Tier-1 authority backends.
//!
//! The current adapter intentionally exposes only capability discovery. It does
//! not delegate production reads or writes, and it must not mint synthetic
//! state tokens for today's ledger plus synchronous compactor path.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
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
pub(crate) mod projection_outbox_acks;
pub mod promotion_gate;
pub(crate) mod shadow_replay;
#[allow(
    dead_code,
    reason = "Phase 6 metadata stays crate-internal until its public activation gate"
)]
pub(crate) mod workspace_binding_metadata;

pub use control_mvp::{
    ControlMvpPaths, ControlMvpProjectionOutboxRecord, ControlMvpStateStore, ControlMvpTxn,
};
pub use model::{ModelCommitRecord, ModelStateStore, ModelWrite};

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
}

impl TxnOptions {
    /// Creates transaction options for an optional authority scope.
    #[must_use]
    pub const fn new(scope: Option<StateScope>) -> Self {
        Self {
            scope,
            request_id: None,
        }
    }

    /// Adds a request identifier to the transaction options.
    #[must_use]
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
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
}

/// Options for creating a future retained authority checkpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckpointOptions {
    scope: Option<StateScope>,
    min_retention_seconds: Option<u64>,
}

impl CheckpointOptions {
    /// Creates checkpoint options for an optional authority scope.
    #[must_use]
    pub const fn new(scope: Option<StateScope>) -> Self {
        Self {
            scope,
            min_retention_seconds: None,
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

    /// Validates that every scope component is a nonblank, printable value.
    ///
    /// # Errors
    ///
    /// Returns a validation error for blank or control-character-bearing fields.
    pub fn validate(&self) -> Result<()> {
        validate_scope_component(&self.tenant_id, "tenant_id")?;
        validate_scope_component(&self.workspace_id, "workspace_id")?;
        validate_scope_component(&self.domain, "domain")
    }
}

fn validate_scope_component(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(CatalogError::Validation {
            message: format!("{field} must be nonblank and contain no control characters"),
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
                .union(StateStoreCapabilityFlags::PREDICATE_PRECONDITIONS),
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

    /// Scans current key/value pairs by prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot perform the scan.
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvPair>>;

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

/// Combined state-store read, admin, and transaction surface.
#[async_trait]
pub trait ArcoStateStore: ArcoStateReader + ArcoStateAdmin {
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

    /// Scans key/value pairs by prefix inside the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot perform the scan.
    async fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Vec<KvPair>>;

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

    /// Commits the transaction and returns the resulting state token.
    ///
    /// # Errors
    ///
    /// Returns an error when commit fails or transactions are unsupported.
    async fn commit(self: Box<Self>) -> Result<StateToken>;

    /// Rolls back the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when rollback fails or transactions are unsupported.
    async fn rollback(self: Box<Self>) -> Result<()>;
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

    async fn scan_prefix(&self, _prefix: &[u8]) -> Result<Vec<KvPair>> {
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
