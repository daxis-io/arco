//! Test-only tenant identity-root lifecycle and principal probes.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use arco_core::lock::DistributedLock;
use arco_core::storage::StorageBackend;
use arco_core::{IdentityStorage, RootStorage};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::control_mvp::{
    ControlMvpGcOutcome, ControlMvpGcPlan, ControlMvpMaintenanceWorker, ControlMvpStateStore,
};
use super::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateTxn, CheckpointOptions, CheckpointToken,
    PersistedAuthorityAdapter, PersistedAuthorityReference, ScanPage, ScanRequest, StateScope,
    StateToken, TxnOptions,
};
use crate::error::{CatalogError, Result};
use crate::gc::reachability::sha256_digest;
use crate::metastore::events::PrincipalKind;
use crate::retention_coordination::{
    RecoveredRetentionEpoch, RetentionMutationEpoch, RetentionMutationKind,
    recover_stale_identity_epoch,
};
use crate::workspace_snapshot::{
    RETENTION_GC_LOCK_MAX_RETRIES, RETENTION_GC_LOCK_PATH, RETENTION_GC_LOCK_TTL,
};

const REFERENCE_PREFIX: &str = "retention/identity-references/";
const MAX_REFERENCE_BYTES: usize = 16 * 1024;
const REFERENCE_PROBE_END: u64 = MAX_REFERENCE_BYTES as u64 + 1;
pub(super) const REFERENCE_GC_CURSOR: &str = "identity-references:";

#[derive(Debug, Serialize, Deserialize)]
struct IdentityReferenceRecord {
    record_type: String,
    version: u32,
    reference: PersistedAuthorityReference,
}

impl IdentityReferenceRecord {
    fn bytes(reference: PersistedAuthorityReference) -> Result<Vec<u8>> {
        reference.validate()?;
        let bytes = serde_jcs::to_vec(&Self {
            record_type: "arco.identity_protected_reference".into(),
            version: 1,
            reference,
        })
        .map_err(|error| CatalogError::Serialization {
            message: format!("serialize identity protected reference: {error}"),
        })?;
        if bytes.len() > MAX_REFERENCE_BYTES {
            return Err(CatalogError::Validation {
                message: "identity protected reference exceeds byte bound".into(),
            });
        }
        Ok(bytes)
    }

    fn path(bytes: &[u8]) -> String {
        format!(
            "{REFERENCE_PREFIX}{}.json",
            sha256_digest(bytes).trim_start_matches("sha256:")
        )
    }

    fn decode(path: &str, bytes: &[u8], scope: &StateScope) -> Result<PersistedAuthorityReference> {
        if bytes.len() > MAX_REFERENCE_BYTES || Self::path(bytes) != path {
            return Err(CatalogError::Validation {
                message: "identity protected reference path or size is invalid".into(),
            });
        }
        let record: Self =
            serde_json::from_slice(bytes).map_err(|error| CatalogError::Serialization {
                message: format!("decode identity protected reference: {error}"),
            })?;
        if record.record_type != "arco.identity_protected_reference"
            || record.version != 1
            || record.reference.scope() != scope
        {
            return Err(CatalogError::Validation {
                message: "identity protected reference has a foreign scope or envelope".into(),
            });
        }
        record.reference.validate()?;
        if Self::bytes(record.reference.clone())? != bytes {
            return Err(CatalogError::Validation {
                message: "identity protected reference is not canonical".into(),
            });
        }
        Ok(record.reference)
    }
}

pub(super) struct IdentityRetainedReferences<'a> {
    storage: &'a RootStorage,
    scope: &'a StateScope,
    now: DateTime<Utc>,
    cursor: Option<String>,
    pending: VecDeque<String>,
    exhausted: bool,
}

impl<'a> IdentityRetainedReferences<'a> {
    pub(super) fn new(storage: &'a RootStorage, scope: &'a StateScope, now: DateTime<Utc>) -> Self {
        Self {
            storage,
            scope,
            now,
            cursor: None,
            pending: VecDeque::new(),
            exhausted: false,
        }
    }

    pub(super) async fn next(&mut self) -> Result<Option<PersistedAuthorityReference>> {
        loop {
            if let Some(path) = self.pending.pop_front() {
                let reference = read_reference(self.storage, self.scope, &path).await?;
                if reference.retention_deadline() > self.now {
                    return Ok(Some(reference));
                }
                continue;
            }
            if self.exhausted {
                return Ok(None);
            }
            let page = self
                .storage
                .list_page_meta(REFERENCE_PREFIX, self.cursor.as_deref(), 256)
                .await?;
            for object in page.objects {
                self.pending.push_back(object.path.to_string());
            }
            self.exhausted = page.next_start_after.is_none();
            self.cursor = page.next_start_after;
        }
    }
}

async fn read_reference(
    storage: &RootStorage,
    scope: &StateScope,
    path: &str,
) -> Result<PersistedAuthorityReference> {
    let bytes = storage.get_range(path, 0..REFERENCE_PROBE_END).await?;
    IdentityReferenceRecord::decode(path, &bytes, scope)
}

pub(super) async fn expired_reference_page(
    storage: &RootStorage,
    scope: &StateScope,
    now: DateTime<Utc>,
    cursor: &str,
) -> Result<(Vec<(String, String, u64)>, Option<String>)> {
    let after =
        cursor
            .strip_prefix(REFERENCE_GC_CURSOR)
            .ok_or_else(|| CatalogError::Validation {
                message: "invalid identity reference GC cursor".into(),
            })?;
    if !after.is_empty() && !after.starts_with(REFERENCE_PREFIX) {
        return Err(CatalogError::Validation {
            message: "identity reference GC cursor is outside root".into(),
        });
    }
    let page = storage
        .list_page_meta(REFERENCE_PREFIX, (!after.is_empty()).then_some(after), 256)
        .await?;
    let mut expired = Vec::new();
    for object in page.objects {
        let path = object.path.as_str();
        let reference = read_reference(storage, scope, path).await?;
        if reference.retention_deadline() <= now
            && object
                .last_modified
                .is_some_and(|modified| modified <= now - chrono::Duration::days(7))
        {
            expired.push((path.to_owned(), object.version, object.size));
        }
    }
    Ok((
        expired,
        page.next_start_after
            .map(|next| format!("{REFERENCE_GC_CURSOR}{next}")),
    ))
}

/// The only mutation admitted by the synthetic identity-root probe.
#[derive(Debug)]
pub struct SyntheticIdentityMutation {
    key: Vec<u8>,
    value: Bytes,
}

impl SyntheticIdentityMutation {
    /// Builds a synthetic put.
    #[must_use]
    pub fn put(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Self {
        Self {
            key: key.as_ref().to_vec(),
            value: Bytes::copy_from_slice(value.as_ref()),
        }
    }
}

/// Default-disabled synthetic commit/read probe over one tenant identity root.
///
/// Metastore mutations cannot enter this interface:
///
/// ```compile_fail
/// use arco_catalog::state_store::identity_probe::IdentityStore;
/// use arco_catalog::metastore::events::MetastoreMutation;
/// async fn wrong(store: &IdentityStore, mutation: MetastoreMutation) {
///     match mutation {
///         MetastoreMutation::GrantUpserted(..)
///         | MetastoreMutation::StorageCredentialUpserted(..)
///         | MetastoreMutation::ExternalLocationUpserted(..)
///         | MetastoreMutation::ManagedRootUpserted(..) => {
///             store.commit(mutation).await.unwrap();
///         }
///         _ => {}
///     }
/// }
/// ```
pub struct IdentityStore(ControlMvpStateStore);

// ponytail: one replay log fits the test-only 15-commit cap; use segmented events when identity layout maintenance is admitted.
const PRINCIPAL_EVENTS_KEY: &[u8] = b"principal/events-v1";

/// Optional request provenance; tenant authority is recorded on the event itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityOrigin {
    /// Workspace that originated the request, if any.
    pub workspace_id: Option<String>,
    /// Metastore that originated the request, if any.
    pub metastore_id: Option<String>,
}

/// The only principal mutations admitted by [`PrincipalIdentityStore`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IdentityMutation {
    /// Allocate a new, never reused principal ID.
    Create {
        /// Display name.
        name: String,
        /// Principal family.
        kind: PrincipalKind,
    },
    /// Disable a principal throughout its tenant authority.
    Disable {
        /// Stable principal ID.
        principal_id: String,
    },
    /// Replace direct group memberships at an expected revision.
    ReviseMembership {
        /// Stable principal ID.
        principal_id: String,
        /// Revision observed by the caller.
        expected_revision: u64,
        /// Complete set of direct group IDs.
        group_ids: BTreeSet<String>,
    },
}

/// One deterministic identity authority event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityEvent {
    /// Tenant that owns the principal; origin is separate provenance.
    pub tenant_id: String,
    /// Optional originating request context.
    pub origin: Option<IdentityOrigin>,
    /// Monotonic event sequence within this tenant identity root.
    pub sequence: u64,
    /// Stable event ID derived from the sequence.
    pub event_id: String,
    /// Store-assigned principal ID.
    pub principal_id: String,
    /// Typed principal operation.
    pub mutation: IdentityMutation,
}

/// Replayed principal state. Disabled records remain to reserve their IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityPrincipal {
    /// Stable principal ID.
    pub principal_id: String,
    /// Principal display name.
    pub name: String,
    /// Principal family.
    pub kind: PrincipalKind,
    /// Whether principal use is enabled.
    pub active: bool,
    /// Monotonic membership revision.
    pub membership_revision: u64,
    /// Direct group memberships.
    pub group_ids: BTreeSet<String>,
}

/// Deterministic replay of one tenant's identity events.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct IdentityState {
    /// All events in authority order.
    pub events: Vec<IdentityEvent>,
    /// Principals keyed by never-recycled ID.
    pub principals: BTreeMap<String, IdentityPrincipal>,
}

impl IdentityState {
    /// Apply a checked event. Duplicate IDs and invalid transitions fail closed.
    ///
    /// # Errors
    /// Returns a validation error for a foreign tenant, gap, duplicate ID, or invalid transition.
    pub fn apply_event(&mut self, event: IdentityEvent) -> Result<()> {
        let expected = u64::try_from(self.events.len())
            .ok()
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| identity_error("identity event sequence overflow"))?;
        if event.tenant_id.is_empty()
            || event.sequence != expected
            || event.event_id != format!("identity-{expected}")
            || self
                .events
                .first()
                .is_some_and(|first| first.tenant_id != event.tenant_id)
            || event.principal_id.is_empty()
        {
            return Err(identity_error(
                "identity event has foreign tenant or invalid envelope",
            ));
        }
        match &event.mutation {
            IdentityMutation::Create { name, kind } => {
                if name.is_empty() || self.principals.contains_key(&event.principal_id) {
                    return Err(identity_error(
                        "principal ID already assigned or name is empty",
                    ));
                }
                self.principals.insert(
                    event.principal_id.clone(),
                    IdentityPrincipal {
                        principal_id: event.principal_id.clone(),
                        name: name.clone(),
                        kind: *kind,
                        active: true,
                        membership_revision: 0,
                        group_ids: BTreeSet::new(),
                    },
                );
            }
            IdentityMutation::Disable { principal_id } => {
                if principal_id != &event.principal_id {
                    return Err(identity_error("principal ID differs from event"));
                }
                let principal = self
                    .principals
                    .get_mut(principal_id)
                    .ok_or_else(|| identity_error("principal does not exist"))?;
                if !principal.active {
                    return Err(identity_error("principal is already disabled"));
                }
                principal.active = false;
            }
            IdentityMutation::ReviseMembership {
                principal_id,
                expected_revision,
                group_ids,
            } => {
                if principal_id != &event.principal_id
                    || group_ids.contains(principal_id)
                    || group_ids.iter().any(|id| {
                        !self
                            .principals
                            .get(id)
                            .is_some_and(|group| group.active && group.kind == PrincipalKind::Group)
                    })
                {
                    return Err(identity_error(
                        "membership contains an invalid group or principal",
                    ));
                }
                let principal = self
                    .principals
                    .get_mut(principal_id)
                    .ok_or_else(|| identity_error("principal does not exist"))?;
                if !principal.active || principal.membership_revision != *expected_revision {
                    return Err(identity_error(
                        "principal is disabled or membership revision is stale",
                    ));
                }
                principal.membership_revision = principal
                    .membership_revision
                    .checked_add(1)
                    .ok_or_else(|| identity_error("membership revision overflow"))?;
                principal.group_ids.clone_from(group_ids);
            }
        }
        self.events.push(event);
        Ok(())
    }
}

fn identity_error(message: &str) -> CatalogError {
    CatalogError::Validation {
        message: message.into(),
    }
}

/// Test-only principal API over the existing identity-root authority kernel.
///
/// Metastore mutations cannot enter this type boundary:
///
/// ```compile_fail
/// use arco_catalog::state_store::identity_probe::PrincipalIdentityStore;
/// use arco_catalog::metastore::events::MetastoreMutation;
/// async fn wrong(store: &PrincipalIdentityStore, value: MetastoreMutation) {
///     if let MetastoreMutation::GrantUpserted(_) = &value { store.commit(value, None).await.unwrap(); }
/// }
/// ```
/// ```compile_fail
/// use arco_catalog::state_store::identity_probe::PrincipalIdentityStore;
/// use arco_catalog::metastore::events::MetastoreMutation;
/// async fn wrong(store: &PrincipalIdentityStore, value: MetastoreMutation) {
///     if let MetastoreMutation::StorageCredentialUpserted(_) = &value { store.commit(value, None).await.unwrap(); }
/// }
/// ```
/// ```compile_fail
/// use arco_catalog::state_store::identity_probe::PrincipalIdentityStore;
/// use arco_catalog::metastore::events::MetastoreMutation;
/// async fn wrong(store: &PrincipalIdentityStore, value: MetastoreMutation) {
///     if let MetastoreMutation::ExternalLocationUpserted(_) = &value { store.commit(value, None).await.unwrap(); }
/// }
/// ```
/// ```compile_fail
/// use arco_catalog::state_store::identity_probe::PrincipalIdentityStore;
/// use arco_catalog::metastore::events::MetastoreMutation;
/// async fn wrong(store: &PrincipalIdentityStore, value: MetastoreMutation) {
///     if let MetastoreMutation::ManagedRootUpserted(_) = &value { store.commit(value, None).await.unwrap(); }
/// }
/// ```
/// ```compile_fail
/// use arco_catalog::state_store::identity_probe::PrincipalIdentityStore;
/// use arco_catalog::TxnOptions;
/// async fn wrong(store: &PrincipalIdentityStore) {
///     store.begin_control_txn(TxnOptions::new(None)).await.unwrap();
/// }
/// ```
pub struct PrincipalIdentityStore {
    kernel: ControlMvpStateStore,
    tenant_id: String,
}

impl PrincipalIdentityStore {
    /// Open the test-only principal interface for one tenant identity root.
    ///
    /// # Errors
    /// Returns validation errors for an invalid identity storage capability.
    pub fn new(storage: IdentityStorage) -> Result<Self> {
        let tenant_id = storage.tenant_id().to_owned();
        Ok(Self {
            kernel: ControlMvpStateStore::new_identity_principals(storage)?,
            tenant_id,
        })
    }

    /// Return the authenticated current principal authority token.
    ///
    /// # Errors
    /// Returns an error when the identity head is unavailable or invalid.
    pub async fn current_token(&self) -> Result<StateToken> {
        self.kernel.current_state_token().await
    }

    /// Commit one typed mutation and return its durable event and authority token.
    ///
    /// # Errors
    /// Returns validation, storage, or CAS errors. A rejected mutation publishes nothing.
    pub async fn commit(
        &self,
        mutation: IdentityMutation,
        origin: Option<IdentityOrigin>,
    ) -> Result<(IdentityEvent, StateToken)> {
        let mut txn = self.kernel.begin_control_txn(TxnOptions::new(None)).await?;
        let events: Vec<IdentityEvent> = match txn.get(PRINCIPAL_EVENTS_KEY).await? {
            Some(value) => serde_json::from_slice(value.bytes()).map_err(|error| {
                CatalogError::Serialization {
                    message: format!("decode identity events: {error}"),
                }
            })?,
            None => Vec::new(),
        };
        let mut state = IdentityState::default();
        for event in events {
            if event.tenant_id != self.tenant_id {
                return Err(identity_error("foreign tenant identity event"));
            }
            state.apply_event(event)?;
        }
        let principal_id = match &mutation {
            IdentityMutation::Create { .. } => uuid::Uuid::new_v4().to_string(),
            IdentityMutation::Disable { principal_id }
            | IdentityMutation::ReviseMembership { principal_id, .. } => principal_id.clone(),
        };
        let sequence = u64::try_from(state.events.len())
            .ok()
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| identity_error("identity event sequence overflow"))?;
        let event = IdentityEvent {
            tenant_id: self.tenant_id.clone(),
            origin,
            sequence,
            event_id: format!("identity-{sequence}"),
            principal_id,
            mutation,
        };
        state.apply_event(event.clone())?;
        let bytes =
            serde_jcs::to_vec(&state.events).map_err(|error| CatalogError::Serialization {
                message: format!("serialize identity events: {error}"),
            })?;
        txn.put(PRINCIPAL_EVENTS_KEY, Bytes::from(bytes)).await?;
        let token = txn.commit().await?.into_state_token();
        Ok((event, token))
    }

    /// Replay current identity events from the authenticated authority state.
    ///
    /// # Errors
    /// Returns validation or storage errors for unreadable or invalid history.
    pub async fn replay(&self) -> Result<IdentityState> {
        self.replay_reader(&self.kernel).await
    }

    /// Replay identity events at an exact historical identity token.
    ///
    /// # Errors
    /// Returns validation or storage errors for foreign or unreadable tokens.
    pub async fn replay_at(&self, token: StateToken) -> Result<IdentityState> {
        let reader = self.kernel.read_at(token).await?;
        self.replay_reader(reader.as_ref()).await
    }

    async fn replay_reader(&self, reader: &dyn ArcoStateReader) -> Result<IdentityState> {
        let Some(bytes) = reader.get(PRINCIPAL_EVENTS_KEY).await? else {
            return Ok(IdentityState::default());
        };
        let events: Vec<IdentityEvent> =
            serde_json::from_slice(&bytes).map_err(|error| CatalogError::Serialization {
                message: format!("decode identity events: {error}"),
            })?;
        let mut state = IdentityState::default();
        for event in events {
            if event.tenant_id != self.tenant_id {
                return Err(identity_error("foreign tenant identity event"));
            }
            state.apply_event(event)?;
        }
        Ok(state)
    }
}

enum IdentityReferenceToken<'a> {
    State(&'a StateToken),
    Checkpoint(&'a CheckpointToken),
}

impl IdentityStore {
    /// Constructs the test-only probe over an identity-scoped capability.
    ///
    /// # Errors
    /// Returns a validation error for an invalid storage scope.
    pub fn new(storage: IdentityStorage) -> Result<Self> {
        Ok(Self(ControlMvpStateStore::new_identity_synthetic(storage)?))
    }

    /// Publishes one synthetic mutation through the format-9 authority kernel.
    /// The probe admits at most 15 commits before identity-root maintenance is qualified.
    ///
    /// # Errors
    /// Returns a validation, CAS, storage, ambiguous-outcome, or maintenance
    /// backpressure error.
    pub async fn commit(&self, mutation: SyntheticIdentityMutation) -> Result<StateToken> {
        let mut txn = self.0.begin_control_txn(TxnOptions::new(None)).await?;
        txn.put(&mutation.key, mutation.value).await?;
        Ok(txn.commit().await?.into_state_token())
    }

    /// Returns the current authenticated identity-root state token.
    ///
    /// # Errors
    /// Returns a storage or validation error if the published head is unavailable.
    pub async fn current_state_token(&self) -> Result<StateToken> {
        self.0.current_state_token().await
    }

    /// Publishes an identity-root checkpoint under its durable retention epoch.
    ///
    /// # Errors
    /// Returns validation, storage, or uncertain-publication errors.
    pub async fn checkpoint(&self, opts: CheckpointOptions) -> Result<CheckpointToken> {
        if opts
            .scope()
            .is_some_and(|scope| scope != self.0.identity_lifecycle().1)
        {
            return Err(CatalogError::Validation {
                message: "checkpoint scope does not match identity root".into(),
            });
        }
        self.0.checkpoint(opts).await
    }

    /// Publishes an exact, deadline-bound retained state reference.
    ///
    /// # Errors
    /// Returns validation, storage, or retention-coordination errors.
    pub async fn protect_state_token(
        &self,
        token: &StateToken,
        deadline: DateTime<Utc>,
    ) -> Result<PersistedAuthorityReference> {
        self.protect(IdentityReferenceToken::State(token), deadline)
            .await
    }

    /// Publishes an exact, deadline-bound retained checkpoint reference.
    ///
    /// # Errors
    /// Returns validation, storage, or retention-coordination errors.
    pub async fn protect_checkpoint_token(
        &self,
        token: &CheckpointToken,
        deadline: DateTime<Utc>,
    ) -> Result<PersistedAuthorityReference> {
        self.protect(IdentityReferenceToken::Checkpoint(token), deadline)
            .await
    }

    async fn protect(
        &self,
        token: IdentityReferenceToken<'_>,
        deadline: DateTime<Utc>,
    ) -> Result<PersistedAuthorityReference> {
        let (storage, scope) = self.0.identity_lifecycle();
        let token_scope = match &token {
            IdentityReferenceToken::State(token) => token.scope(),
            IdentityReferenceToken::Checkpoint(token) => token.scope(),
        };
        if token_scope != scope || deadline <= Utc::now() {
            return Err(CatalogError::Validation {
                message: "identity reference scope or deadline is invalid".into(),
            });
        }
        let mut guard = DistributedLock::new(Arc::new(storage.clone()), RETENTION_GC_LOCK_PATH)
            .acquire_with_operation(
                RETENTION_GC_LOCK_TTL,
                RETENTION_GC_LOCK_MAX_RETRIES,
                Some("identity-reference-publish".into()),
            )
            .await
            .map_err(CatalogError::from)?;
        let mut epoch = match RetentionMutationEpoch::claim(
            storage.clone(),
            &mut guard,
            RetentionMutationKind::IdentityReferencePublish,
            format!("identity-reference-{}", uuid::Uuid::new_v4()),
        )
        .await
        {
            Ok(epoch) => epoch,
            Err(error) => {
                let _ = guard.release().await;
                return Err(error);
            }
        };
        let publication = async {
            let reference = match token {
                IdentityReferenceToken::State(token) => {
                    self.0.persist_state_reference(token, deadline).await?
                }
                IdentityReferenceToken::Checkpoint(token) => {
                    self.0.persist_checkpoint_reference(token, deadline).await?
                }
            };
            let bytes = IdentityReferenceRecord::bytes(reference.clone())?;
            epoch
                .put_immutable_reconciled(
                    &IdentityReferenceRecord::path(&bytes),
                    Bytes::from(bytes),
                )
                .await?;
            Ok(reference)
        }
        .await;
        let settlement = epoch.settle().await;
        let release = guard.release().await.map_err(CatalogError::from);
        match (publication, settlement, release) {
            (Ok(reference), Ok(()), Ok(())) => Ok(reference),
            (Err(error), _, _) | (Ok(_), Err(error), _) | (Ok(_), Ok(()), Err(error)) => Err(error),
        }
    }

    /// Resolves only an exact, unexpired identity-root protected reference.
    ///
    /// # Errors
    /// Returns validation or storage errors for missing, altered, or expired evidence.
    pub async fn resolve_reference_at(
        &self,
        reference: &PersistedAuthorityReference,
        now: DateTime<Utc>,
    ) -> Result<Box<dyn ArcoStateReader>> {
        let (storage, scope) = self.0.identity_lifecycle();
        if reference.scope() != scope || reference.retention_deadline() <= now {
            return Err(CatalogError::Validation {
                message: "identity reference scope or deadline is invalid".into(),
            });
        }
        let bytes = IdentityReferenceRecord::bytes(reference.clone())?;
        let path = IdentityReferenceRecord::path(&bytes);
        if read_reference(storage, scope, &path).await? != *reference {
            return Err(CatalogError::Validation {
                message: "identity reference differs from durable protection".into(),
            });
        }
        self.0.resolve_persisted_reference_at(reference, now).await
    }

    /// Plans one bounded identity-root GC page without mutating storage.
    ///
    /// # Errors
    /// Returns storage or protection-validation errors.
    pub async fn plan_gc_at(&self, now: DateTime<Utc>) -> Result<ControlMvpGcPlan> {
        ControlMvpMaintenanceWorker::new_identity_synthetic(&self.0)
            .plan_gc_at(now, [])
            .await
    }

    /// Reclaims one bounded page after a root-local epoch and generation fence.
    ///
    /// # Errors
    /// Returns storage, CAS, or protection-validation errors.
    pub async fn collect_gc_at(&self, now: DateTime<Utc>) -> Result<ControlMvpGcOutcome> {
        self.collect_gc_page_at(now, None).await
    }

    /// Reclaims one identity-root page after the supplied bounded cursor.
    ///
    /// # Errors
    /// Returns storage, CAS, invalid-cursor, or protection-validation errors.
    pub async fn collect_gc_page_at(
        &self,
        now: DateTime<Utc>,
        continuation: Option<&str>,
    ) -> Result<ControlMvpGcOutcome> {
        ControlMvpMaintenanceWorker::new_identity_synthetic(&self.0)
            .collect_gc_page_at(now, [], continuation)
            .await
    }

    /// Operator recovery after all outstanding remote publication or deletion
    /// requests in the in-flight epoch have been proven terminal.
    ///
    /// # Errors
    /// Returns validation, lock, or CAS errors if recovery is unsafe.
    pub async fn recover_terminal_epoch(
        &self,
        reason: &str,
    ) -> Result<Option<RecoveredRetentionEpoch>> {
        recover_stale_identity_epoch(self.0.identity_lifecycle().0, reason).await
    }
}

#[async_trait]
impl ArcoStateReader for IdentityStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.0.get(key).await
    }

    async fn scan(&self, request: ScanRequest) -> Result<ScanPage> {
        self.0.scan(request).await
    }

    async fn read_at(&self, token: StateToken) -> Result<Box<dyn ArcoStateReader>> {
        self.0.read_at(token).await
    }

    async fn read_checkpoint(&self, token: CheckpointToken) -> Result<Box<dyn ArcoStateReader>> {
        self.0.read_checkpoint(token).await
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
    use std::time::Duration;

    use arco_core::MemoryBackend;
    use arco_core::storage::{
        ListPage, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
    };
    use tokio::sync::Barrier;

    use super::*;
    use crate::StateScope;

    struct HeadFaultBackend {
        inner: MemoryBackend,
        mode: AtomicU8,
        checkpoint_mode: AtomicU8,
        reference_mode: AtomicU8,
        delete_mode: AtomicU8,
        pause_publication: Option<(Arc<Barrier>, Arc<Barrier>)>,
        io: AtomicUsize,
    }

    #[async_trait]
    impl StorageBackend for HeadFaultBackend {
        async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
            self.io.fetch_add(1, Ordering::SeqCst);
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
            self.io.fetch_add(1, Ordering::SeqCst);
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::Result<WriteResult> {
            self.io.fetch_add(1, Ordering::SeqCst);
            let mode = if path.ends_with("/head/current.json") {
                self.mode.swap(0, Ordering::SeqCst)
            } else if path.contains("/checkpoints/") {
                self.checkpoint_mode.swap(0, Ordering::SeqCst)
            } else if path.contains("/retention/identity-references/") {
                self.reference_mode.swap(0, Ordering::SeqCst)
            } else {
                0
            };
            if mode == 2 {
                return Err(arco_core::Error::storage(
                    "immutable write did not complete",
                ));
            }
            let result = self.inner.put(path, data, precondition).await?;
            if mode == 3 {
                let (entered, release) = self.pause_publication.as_ref().expect("publication gate");
                entered.wait().await;
                release.wait().await;
            }
            if mode == 1 {
                return Err(arco_core::Error::storage(
                    "immutable write response was lost",
                ));
            }
            Ok(result)
        }

        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.io.fetch_add(1, Ordering::SeqCst);
            self.inner.delete(path).await?;
            if path.contains("/control/v1/") && self.delete_mode.swap(0, Ordering::SeqCst) == 1 {
                return Err(arco_core::Error::storage("delete response was lost"));
            }
            Ok(())
        }

        async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            self.io.fetch_add(1, Ordering::SeqCst);
            self.inner.list(prefix).await
        }

        async fn list_page(
            &self,
            prefix: &str,
            cursor: Option<&str>,
            limit: usize,
        ) -> arco_core::Result<ListPage> {
            self.io.fetch_add(1, Ordering::SeqCst);
            self.inner.list_page(prefix, cursor, limit).await
        }

        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            self.io.fetch_add(1, Ordering::SeqCst);
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
            self.io.fetch_add(1, Ordering::SeqCst);
            self.inner.signed_url(path, expiry).await
        }
    }

    #[tokio::test]
    async fn stale_cas_and_forged_tokens_fail_closed() {
        let store = IdentityStore::new(
            IdentityStorage::new(Arc::new(MemoryBackend::new()), "acme").unwrap(),
        )
        .unwrap();
        let mut first = store
            .0
            .begin_control_txn(TxnOptions::new(None))
            .await
            .unwrap();
        let mut stale = store
            .0
            .begin_control_txn(TxnOptions::new(None))
            .await
            .unwrap();
        first
            .put(b"p/a", Bytes::from_static(b"first"))
            .await
            .unwrap();
        stale
            .put(b"p/a", Bytes::from_static(b"stale"))
            .await
            .unwrap();
        let token = first.commit().await.unwrap().into_state_token();
        assert!(matches!(
            stale.commit().await,
            Err(CatalogError::CasFailed { .. })
        ));

        let mut bad_witness = token.clone();
        bad_witness.expected_manifest_sha256 = Some("0".repeat(64));
        assert!(store.read_at(bad_witness).await.is_err());
        let legacy_scope: StateScope = serde_json::from_value(serde_json::json!({
            "tenant_id": "acme",
            "workspace_id": "acme",
            "domain": "identity"
        }))
        .unwrap();
        let mut legacy_workspace = token.clone();
        legacy_workspace.scope = legacy_scope;
        assert!(store.read_at(legacy_workspace).await.is_err());
        let mut forged_root = token.clone();
        forged_root.scope = StateScope::metastore("acme", "acme", "identity");
        assert!(store.read_at(forged_root).await.is_err());
        assert_eq!(
            store
                .read_at(token)
                .await
                .unwrap()
                .get(b"p/a")
                .await
                .unwrap(),
            Some(Bytes::from_static(b"first"))
        );
    }

    #[test]
    fn cache_handles_reject_equal_ids_across_physical_roots_and_tenants() {
        let backend = Arc::new(MemoryBackend::new());
        let identity =
            IdentityStore::new(IdentityStorage::new(backend.clone(), "acme").unwrap()).unwrap();
        let workspace = ControlMvpStateStore::new(
            arco_core::ScopedStorage::new(backend.clone(), "acme", "acme").unwrap(),
            StateScope::new("acme", "acme", "identity"),
        )
        .unwrap();
        let other_tenant =
            IdentityStore::new(IdentityStorage::new(backend, "other").unwrap()).unwrap();

        let identity_cache = identity.0.read_cache().unwrap();
        assert!(
            identity
                .0
                .clone()
                .with_read_cache(identity_cache.clone())
                .is_ok()
        );
        assert!(
            workspace
                .clone()
                .with_read_cache(identity_cache.clone())
                .is_err()
        );
        assert!(other_tenant.0.with_read_cache(identity_cache).is_err());
        assert!(
            identity
                .0
                .with_read_cache(workspace.read_cache().unwrap())
                .is_err()
        );
    }

    #[tokio::test]
    async fn lost_head_response_reconciles_and_unresolved_write_stays_ambiguous() {
        let backend = Arc::new(HeadFaultBackend {
            inner: MemoryBackend::new(),
            mode: AtomicU8::new(1),
            checkpoint_mode: AtomicU8::new(0),
            reference_mode: AtomicU8::new(0),
            delete_mode: AtomicU8::new(0),
            pause_publication: None,
            io: AtomicUsize::new(0),
        });
        let store =
            IdentityStore::new(IdentityStorage::new(backend.clone(), "acme").unwrap()).unwrap();
        let token = store
            .commit(SyntheticIdentityMutation::put(b"p/a", b"one"))
            .await
            .unwrap();
        assert_eq!(store.current_state_token().await.unwrap(), token);

        backend.mode.store(2, Ordering::SeqCst);
        assert!(matches!(
            store
                .commit(SyntheticIdentityMutation::put(b"p/b", b"two"))
                .await,
            Err(CatalogError::AmbiguousAuthorityOutcome { .. })
        ));
        assert_eq!(store.current_state_token().await.unwrap(), token);
        assert_eq!(store.get(b"p/b").await.unwrap(), None);

        let checkpoint = store
            .checkpoint(CheckpointOptions::default())
            .await
            .unwrap();
        assert_eq!(
            store
                .read_checkpoint(checkpoint)
                .await
                .unwrap()
                .get(b"p/a")
                .await
                .unwrap(),
            Some(Bytes::from_static(b"one"))
        );
        assert!(backend.io.load(Ordering::SeqCst) > 0);
    }

    #[tokio::test]
    async fn probe_stops_before_workspace_maintenance_intent() {
        let backend = Arc::new(MemoryBackend::new());
        let store =
            IdentityStore::new(IdentityStorage::new(backend.clone(), "acme").unwrap()).unwrap();
        for ordinal in 0..15 {
            store
                .commit(SyntheticIdentityMutation::put(
                    format!("p/{ordinal}"),
                    b"value",
                ))
                .await
                .unwrap();
        }
        let before = backend.list("tenant=acme/identity/").await.unwrap().len();
        assert!(matches!(
            store
                .commit(SyntheticIdentityMutation::put(b"p/15", b"value"))
                .await,
            Err(CatalogError::MaintenanceBackpressure { .. })
        ));
        assert_eq!(
            backend.list("tenant=acme/identity/").await.unwrap().len(),
            before
        );
    }

    #[tokio::test]
    async fn identity_publication_reconciles_lost_response_and_requires_terminal_recovery() {
        let backend = Arc::new(HeadFaultBackend {
            inner: MemoryBackend::new(),
            mode: AtomicU8::new(0),
            checkpoint_mode: AtomicU8::new(0),
            reference_mode: AtomicU8::new(0),
            delete_mode: AtomicU8::new(0),
            pause_publication: None,
            io: AtomicUsize::new(0),
        });
        let storage = IdentityStorage::new(backend.clone(), "acme").unwrap();
        let store = IdentityStore::new(storage.clone()).unwrap();
        let token = store
            .commit(SyntheticIdentityMutation::put(b"p/a", b"old"))
            .await
            .unwrap();

        backend.checkpoint_mode.store(1, Ordering::SeqCst);
        let checkpoint = store
            .checkpoint(CheckpointOptions::default())
            .await
            .unwrap();
        backend.reference_mode.store(1, Ordering::SeqCst);
        let deadline = Utc::now() + chrono::Duration::days(90);
        let checkpoint_ref = store
            .protect_checkpoint_token(&checkpoint, deadline)
            .await
            .unwrap();
        assert_eq!(
            IdentityStore::new(storage.clone())
                .unwrap()
                .resolve_reference_at(&checkpoint_ref, Utc::now())
                .await
                .unwrap()
                .get(b"p/a")
                .await
                .unwrap(),
            Some(Bytes::from_static(b"old"))
        );

        backend.reference_mode.store(2, Ordering::SeqCst);
        assert!(store.protect_state_token(&token, deadline).await.is_err());
        assert!(store.protect_state_token(&token, deadline).await.is_err());
        assert!(
            store
                .checkpoint(CheckpointOptions::default())
                .await
                .is_err()
        );
        // The test backend returned before submitting the failed PUT, so the
        // outstanding request is independently known to be terminal.
        let recovered = store
            .recover_terminal_epoch("test backend confirmed no PUT was submitted")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            recovered.operation_kind,
            RetentionMutationKind::IdentityReferencePublish
        );
        store.protect_state_token(&token, deadline).await.unwrap();

        backend.checkpoint_mode.store(2, Ordering::SeqCst);
        assert!(
            store
                .checkpoint(CheckpointOptions::default())
                .await
                .is_err()
        );
        assert!(store.protect_state_token(&token, deadline).await.is_err());
        // The failed checkpoint PUT also returned before submitting any request.
        let recovered = store
            .recover_terminal_epoch("test backend confirmed no checkpoint PUT was submitted")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            recovered.operation_kind,
            RetentionMutationKind::CatalogCheckpointPublish
        );
        store
            .checkpoint(CheckpointOptions::default())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn identity_gc_fence_survives_lost_delete_response() {
        let backend = Arc::new(HeadFaultBackend {
            inner: MemoryBackend::new(),
            mode: AtomicU8::new(0),
            checkpoint_mode: AtomicU8::new(0),
            reference_mode: AtomicU8::new(0),
            delete_mode: AtomicU8::new(0),
            pause_publication: None,
            io: AtomicUsize::new(0),
        });
        let store =
            IdentityStore::new(IdentityStorage::new(backend.clone(), "acme").unwrap()).unwrap();
        store
            .commit(SyntheticIdentityMutation::put(b"p/a", b"live"))
            .await
            .unwrap();
        let orphan = "tenant=acme/identity/control/v1/domains/identity/transactions/orphan.json";
        backend
            .inner
            .put(
                orphan,
                Bytes::from_static(b"orphan"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        backend.delete_mode.store(1, Ordering::SeqCst);
        assert!(
            store
                .collect_gc_at(Utc::now() + chrono::Duration::days(8))
                .await
                .is_err()
        );
        assert!(backend.inner.head(orphan).await.unwrap().is_none());
        assert!(
            store
                .collect_gc_at(Utc::now() + chrono::Duration::days(8))
                .await
                .is_ok()
        );
        assert_eq!(
            store.get(b"p/a").await.unwrap(),
            Some(Bytes::from_static(b"live"))
        );
    }

    #[tokio::test]
    async fn identity_reference_publication_excludes_gc_until_pin_is_visible() {
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let backend = Arc::new(HeadFaultBackend {
            inner: MemoryBackend::new(),
            mode: AtomicU8::new(0),
            checkpoint_mode: AtomicU8::new(0),
            reference_mode: AtomicU8::new(0),
            delete_mode: AtomicU8::new(0),
            pause_publication: Some((entered.clone(), release.clone())),
            io: AtomicUsize::new(0),
        });
        let storage = IdentityStorage::new(backend.clone(), "acme").unwrap();
        let store = IdentityStore::new(storage.clone()).unwrap();
        let first = store
            .commit(SyntheticIdentityMutation::put(b"p/a", b"old"))
            .await
            .unwrap();
        store
            .commit(SyntheticIdentityMutation::put(b"p/a", b"new"))
            .await
            .unwrap();

        backend.reference_mode.store(3, Ordering::SeqCst);
        let publisher = IdentityStore::new(storage.clone()).unwrap();
        let publication = tokio::spawn(async move {
            publisher
                .protect_state_token(&first, Utc::now() + chrono::Duration::days(90))
                .await
        });
        entered.wait().await;
        let collector = IdentityStore::new(storage).unwrap();
        let now = Utc::now() + chrono::Duration::days(40);
        let gc = tokio::spawn(async move { collector.collect_gc_at(now).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!gc.is_finished(), "GC crossed the identity retention lock");
        release.wait().await;
        let reference = publication.await.unwrap().unwrap();
        gc.await.unwrap().unwrap();
        assert_eq!(
            store
                .resolve_reference_at(&reference, now)
                .await
                .unwrap()
                .get(b"p/a")
                .await
                .unwrap(),
            Some(Bytes::from_static(b"old"))
        );
    }

    #[tokio::test]
    async fn identity_checkpoint_publication_excludes_gc() {
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let backend = Arc::new(HeadFaultBackend {
            inner: MemoryBackend::new(),
            mode: AtomicU8::new(0),
            checkpoint_mode: AtomicU8::new(0),
            reference_mode: AtomicU8::new(0),
            delete_mode: AtomicU8::new(0),
            pause_publication: Some((entered.clone(), release.clone())),
            io: AtomicUsize::new(0),
        });
        let storage = IdentityStorage::new(backend.clone(), "acme").unwrap();
        let store = IdentityStore::new(storage.clone()).unwrap();
        store
            .commit(SyntheticIdentityMutation::put(b"p/a", b"live"))
            .await
            .unwrap();
        let orphan = "tenant=acme/identity/control/v1/domains/identity/transactions/orphan.json";
        backend
            .inner
            .put(
                orphan,
                Bytes::from_static(b"orphan"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();

        backend.checkpoint_mode.store(3, Ordering::SeqCst);
        let publisher = IdentityStore::new(storage.clone()).unwrap();
        let publication =
            tokio::spawn(async move { publisher.checkpoint(CheckpointOptions::default()).await });
        entered.wait().await;
        let collector = IdentityStore::new(storage).unwrap();
        let gc = tokio::spawn(async move {
            collector
                .collect_gc_at(Utc::now() + chrono::Duration::days(8))
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !gc.is_finished(),
            "GC crossed the identity checkpoint epoch"
        );
        release.wait().await;
        let checkpoint = publication.await.unwrap().unwrap();
        gc.await.unwrap().unwrap();
        assert_eq!(
            store
                .read_checkpoint(checkpoint)
                .await
                .unwrap()
                .get(b"p/a")
                .await
                .unwrap(),
            Some(Bytes::from_static(b"live"))
        );
        assert!(backend.inner.head(orphan).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn foreign_lifecycle_scopes_fail_before_identity_storage_io() {
        let backend = Arc::new(HeadFaultBackend {
            inner: MemoryBackend::new(),
            mode: AtomicU8::new(0),
            checkpoint_mode: AtomicU8::new(0),
            reference_mode: AtomicU8::new(0),
            delete_mode: AtomicU8::new(0),
            pause_publication: None,
            io: AtomicUsize::new(0),
        });
        let store =
            IdentityStore::new(IdentityStorage::new(backend.clone(), "acme").unwrap()).unwrap();
        let before_constructor = backend.io.load(Ordering::SeqCst);
        assert!(
            ControlMvpStateStore::new(
                IdentityStorage::new(backend.clone(), "acme").unwrap(),
                StateScope::tenant_identity("acme", "identity"),
            )
            .is_err()
        );
        assert_eq!(backend.io.load(Ordering::SeqCst), before_constructor);
        assert!(
            crate::retention_coordination::recover_stale_retention_epoch(
                store.0.identity_lifecycle().0,
                "production recovery attempt",
            )
            .await
            .is_err()
        );
        assert_eq!(backend.io.load(Ordering::SeqCst), before_constructor);
        let mut token = store
            .commit(SyntheticIdentityMutation::put(b"p/a", b"live"))
            .await
            .unwrap();
        token.scope = StateScope::new("acme", "acme", "identity");
        let before = backend.io.load(Ordering::SeqCst);
        assert!(
            store
                .protect_state_token(&token, Utc::now() + chrono::Duration::days(60))
                .await
                .is_err()
        );
        assert_eq!(backend.io.load(Ordering::SeqCst), before);
        assert!(
            store
                .checkpoint(CheckpointOptions::new(Some(token.scope)))
                .await
                .is_err()
        );
        assert_eq!(backend.io.load(Ordering::SeqCst), before);
        let valid = store.current_state_token().await.unwrap();
        let before = backend.io.load(Ordering::SeqCst);
        assert!(
            store
                .protect_state_token(&valid, Utc::now() - chrono::Duration::days(1))
                .await
                .is_err()
        );
        assert_eq!(backend.io.load(Ordering::SeqCst), before);
    }
}
