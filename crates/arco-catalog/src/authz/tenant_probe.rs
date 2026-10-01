//! Test-only cross-root authorization cut; no production route calls this module.

use arco_core::ScopedStorage;
use arco_core::{AuthorityRoot, ControlPlaneScope};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::authz::compiler::{
    CompiledPermissionSet, PermissionCompileInput, SecurableObject, compile_permissions_with_active,
};
use crate::authz::decision::{AuthzDecision, AuthzEvidence, AuthzRequest, DecisionOutcome};
use crate::error::{CatalogError, Result};
use crate::identity::memberships::{GroupMembership, IdentitySnapshot};
use crate::metastore::events::{
    GrantIdentityCut, GrantRecord, LifecycleState, MetastoreEvent, MetastoreMutation, PrincipalKind,
};
use crate::metastore::ledger::{MetastoreLedger, MetastoreLedgerWatermark};
use crate::metastore::replay::{MetastoreState, replay_events};
use crate::reader::CatalogReader;
use crate::state_store::identity_probe::{IdentityState, PrincipalIdentityStore};
use crate::writer::Table;
use crate::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateTxn, ControlMvpStateStore, StateScope, StateToken,
    TxnOptions,
};

/// State key used only by the test metastore fixture.
pub const METASTORE_AUTHZ_STATE_KEY: &[u8] = b"authz/tenant-probe-state-v1";
const NATIVE_LEDGER_WITNESS_KEY: &[u8] = b"authz/native-ledger-watermark-v1";
// ponytail: one pending grant per metastore; per-event intents need a multiwriter proof.
const PENDING_NATIVE_GRANT_KEY: &[u8] = b"authz/pending-native-grant-v1";

#[derive(Serialize, Deserialize)]
struct PendingNativeGrant {
    event: MetastoreEvent,
    previous_watermark: Option<MetastoreLedgerWatermark>,
    had_witness: bool,
}

fn aborted_grant_event(pending: &PendingNativeGrant) -> Result<MetastoreEvent> {
    let MetastoreMutation::GrantUpserted(grant) = &pending.event.mutation else {
        return Err(CatalogError::Validation {
            message: "prepared native event is not a grant".into(),
        });
    };
    let mut event = pending.event.clone();
    event.mutation = MetastoreMutation::GrantAdmissionAborted {
        grant_id: grant.grant_id.clone(),
    };
    Ok(event)
}

/// Permissions compiled from exact identity and metastore authority tokens.
pub struct TenantPermissionCut {
    identity_token: StateToken,
    metastore_token: StateToken,
    identity: IdentityState,
    compiled: CompiledPermissionSet,
}

impl TenantPermissionCut {
    /// Identity authority token used for this cut.
    #[must_use]
    pub const fn identity_token(&self) -> &StateToken {
        &self.identity_token
    }

    /// Metastore authority token used for this cut.
    #[must_use]
    pub const fn metastore_token(&self) -> &StateToken {
        &self.metastore_token
    }
}

/// Test-only bridge between tenant principal authority and one metastore.
pub struct TenantAuthorizationProbe<'a> {
    identity: &'a PrincipalIdentityStore,
    metastore: &'a ControlMvpStateStore,
}

impl<'a> TenantAuthorizationProbe<'a> {
    /// Bind one tenant identity store and one metastore store.
    #[must_use]
    pub const fn new(
        identity: &'a PrincipalIdentityStore,
        metastore: &'a ControlMvpStateStore,
    ) -> Self {
        Self {
            identity,
            metastore,
        }
    }

    /// Compile permissions from authenticated current tokens for both roots.
    ///
    /// # Errors
    /// Returns an error for missing, foreign, unreadable, or invalid authority state.
    pub async fn compile_current(&self) -> Result<TenantPermissionCut> {
        let identity_token = self.identity.current_token().await?;
        let metastore_token = self.metastore.current_state_token().await?;
        if identity_token.scope().domain() != "principals"
            || !matches!(identity_token.scope().root(), AuthorityRoot::TenantIdentity)
            || metastore_token.scope().domain() != "catalog"
            || !matches!(
                metastore_token.scope().root(),
                AuthorityRoot::Metastore { .. }
            )
            || identity_token.scope().tenant_id() != metastore_token.scope().tenant_id()
        {
            return Err(CatalogError::Validation {
                message: "tenant authorization cut has foreign authority roots".into(),
            });
        }
        let identity = self.identity.replay_at(identity_token.clone()).await?;
        let reader = self.metastore.read_at(metastore_token.clone()).await?;
        let bytes = reader
            .get(METASTORE_AUTHZ_STATE_KEY)
            .await?
            .ok_or_else(|| CatalogError::NotFound {
                entity: "test metastore authorization state".into(),
                name: metastore_token.authority_manifest_id().into(),
            })?;
        let (metastore, securables): (MetastoreState, Vec<SecurableObject>) =
            serde_json::from_slice(&bytes).map_err(|error| CatalogError::Serialization {
                message: format!("decode test metastore authorization state: {error}"),
            })?;
        let snapshot = identity_snapshot(&identity_token, &identity);
        let compiled = compile_permissions_with_active(
            PermissionCompileInput {
                metastore: &metastore,
                identity: &snapshot,
                securables: &securables,
                ledger_watermark: metastore.ledger_watermark.as_deref().unwrap_or(""),
            },
            |principal_id| {
                identity
                    .principals
                    .get(principal_id)
                    .is_some_and(|principal| {
                        principal.active && principal.kind != PrincipalKind::ExternalSubject
                    })
            },
        )?;
        Ok(TenantPermissionCut {
            identity_token,
            metastore_token,
            identity,
            compiled,
        })
    }

    /// Recheck current roots and principal lifecycle before using a compiled cut.
    /// Missing or unreadable authority denies the request.
    pub async fn evaluate(
        &self,
        cut: &TenantPermissionCut,
        request: &AuthzRequest,
    ) -> AuthzDecision {
        let current_identity_token = self.identity.current_token().await;
        let current_metastore_token = self.metastore.current_state_token().await;
        let Ok(identity_token) = current_identity_token else {
            return stale_decision(cut);
        };
        let Ok(metastore_token) = current_metastore_token else {
            return stale_decision(cut);
        };
        if identity_token != cut.identity_token || metastore_token != cut.metastore_token {
            return stale_decision(cut);
        }
        let Ok(current_identity) = self.identity.replay_at(identity_token).await else {
            return stale_decision(cut);
        };
        let principal_active_at_cut = cut.identity.principals.get(&request.principal_id);
        let principal_active_now = current_identity.principals.get(&request.principal_id);
        if !matches!((principal_active_at_cut, principal_active_now),
            (Some(before), Some(now)) if before.active
                && now.active
                && now.kind != PrincipalKind::ExternalSubject
                && before.membership_revision == now.membership_revision)
        {
            return stale_decision(cut);
        }
        AuthzDecision::evaluate(request, &cut.compiled)
    }
}

fn stale_decision(cut: &TenantPermissionCut) -> AuthzDecision {
    AuthzDecision {
        outcome: DecisionOutcome::Deny,
        reason_code: "stale_authority_cut".into(),
        reason_message_safe: "tenant authorization evidence is stale or unavailable".into(),
        evidence: AuthzEvidence::default(),
        group_snapshot_version: Some(cut.compiled.group_snapshot_version.clone()),
    }
}

/// Named cut compiled from the native metastore ledger and tenant identity root.
pub struct TenantCatalogCut {
    identity_token: StateToken,
    metastore_token: StateToken,
    metastore_scope: ControlPlaneScope,
    metastore_watermark: MetastoreLedgerWatermark,
    identity: IdentityState,
    compiled: CompiledPermissionSet,
}

impl TenantCatalogCut {
    /// Identity authority token used to compile this cut.
    #[must_use]
    pub const fn identity_token(&self) -> &StateToken {
        &self.identity_token
    }

    /// Authenticated metastore authority token binding the native ledger watermark.
    #[must_use]
    pub const fn metastore_token(&self) -> &StateToken {
        &self.metastore_token
    }

    /// Native metastore ledger watermark used to compile this cut.
    #[must_use]
    pub const fn metastore_watermark(&self) -> &MetastoreLedgerWatermark {
        &self.metastore_watermark
    }
}

/// Test-only admission and catalog-read path over the native metastore ledger.
pub struct TenantCatalogProbe<'a> {
    identity: &'a PrincipalIdentityStore,
    ledger: MetastoreLedger,
    kernel: ControlMvpStateStore,
    reader: CatalogReader,
}

impl<'a> TenantCatalogProbe<'a> {
    /// Bind one tenant identity root to one native metastore root.
    ///
    /// # Errors
    /// Rejects workspace and foreign authority roots.
    pub fn new(identity: &'a PrincipalIdentityStore, storage: ScopedStorage) -> Result<Self> {
        if !matches!(storage.scope().root(), AuthorityRoot::Metastore { .. }) {
            return Err(CatalogError::Validation {
                message: "tenant catalog probe requires a metastore root".into(),
            });
        }
        let scope = StateScope::metastore(
            storage.tenant_id(),
            storage
                .scope()
                .metastore_id()
                .ok_or_else(|| CatalogError::Validation {
                    message: "metastore root is missing a metastore ID".into(),
                })?,
            "catalog",
        );
        Ok(Self {
            identity,
            ledger: MetastoreLedger::new(storage.clone())?,
            kernel: ControlMvpStateStore::new(storage.clone(), scope)?,
            reader: CatalogReader::new(storage),
        })
    }

    /// Validate a grantee at a named identity cut, then append a native grant event.
    ///
    /// # Errors
    /// Rejects missing, disabled, foreign, or external principals and invalid objects.
    pub async fn append_grant(
        &self,
        event_id: &str,
        sequence: u64,
        grant: GrantRecord,
    ) -> Result<GrantIdentityCut> {
        let event = self.prepare_grant(event_id, sequence, grant).await?;
        let MetastoreMutation::GrantUpserted(grant) = &event.mutation else {
            unreachable!("prepared grant has a grant mutation")
        };
        let cut = grant
            .identity_cut
            .clone()
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: "prepared grant lacks identity evidence".into(),
            })?;
        self.ledger.append_event(&event).await?;
        self.reconcile_prepared_grant().await?;
        Ok(cut)
    }

    /// Persist exact admission intent before appending its native grant event.
    ///
    /// # Errors
    /// Rejects invalid grants and another grant that is awaiting reconciliation.
    pub async fn prepare_grant(
        &self,
        event_id: &str,
        sequence: u64,
        mut grant: GrantRecord,
    ) -> Result<MetastoreEvent> {
        if grant.lifecycle_state != LifecycleState::Active {
            return Err(CatalogError::Validation {
                message: "grant admission only accepts active grant creation".into(),
            });
        }
        let token = self.identity.current_token().await?;
        self.validate_identity_token(&token)?;
        let state = self.identity.replay_at(token.clone()).await?;
        let principal =
            state
                .principals
                .get(&grant.principal_id)
                .ok_or_else(|| CatalogError::Validation {
                    message: "grant grantee is absent from tenant identity".into(),
                })?;
        if !principal.active || principal.kind == PrincipalKind::ExternalSubject {
            return Err(CatalogError::Validation {
                message: "grant grantee is not active".into(),
            });
        }
        let metastore = self.ledger.replay().await?;
        if !metastore
            .catalog_objects
            .get(&grant.object_id)
            .is_some_and(|object| {
                object.lifecycle_state == LifecycleState::Active
                    && object.object_type.eq_ignore_ascii_case(&grant.object_type)
            })
        {
            return Err(CatalogError::Validation {
                message: "grant object is not active in this metastore".into(),
            });
        }
        let cut = GrantIdentityCut {
            tenant_id: token.scope().tenant_id().to_owned(),
            identity_manifest_id: token.authority_manifest_id().to_owned(),
            identity_sequence: token.logical_sequence(),
            membership_revision: principal.membership_revision,
        };
        grant.identity_cut = Some(cut);
        let event = MetastoreEvent::new_scoped(
            self.reader.scope(),
            event_id,
            sequence,
            MetastoreMutation::GrantUpserted(grant),
        );
        let mut txn = self.kernel.begin_control_txn(TxnOptions::new(None)).await?;
        txn.assert_absent(PENDING_NATIVE_GRANT_KEY).await?;
        let previous_watermark = self.ledger.latest_watermark().await?;
        let witnessed = txn.get(NATIVE_LEDGER_WITNESS_KEY).await?;
        let had_witness = witnessed.is_some();
        if let Some(witnessed) = witnessed {
            let prior: MetastoreLedgerWatermark = serde_json::from_slice(witnessed.bytes())
                .map_err(|error| CatalogError::Serialization {
                    message: format!("decode native ledger watermark: {error}"),
                })?;
            if previous_watermark.as_ref() != Some(&prior) {
                return Err(CatalogError::Validation {
                    message: "native ledger advanced beyond its previous witness".into(),
                });
            }
        } else {
            let prior_events = self.ledger.load_events().await?;
            if prior_events.iter().any(|prior| {
                matches!(
                    prior.mutation,
                    MetastoreMutation::GrantUpserted(_)
                        | MetastoreMutation::GrantAdmissionAborted { .. }
                ) || !self.event_has_native_scope(prior)
            }) {
                return Err(CatalogError::Validation {
                    message: "unwitnessed native grant or foreign event precedes admission".into(),
                });
            }
        }
        let pending = PendingNativeGrant {
            event: event.clone(),
            previous_watermark,
            had_witness,
        };
        let bytes = serde_jcs::to_vec(&pending).map_err(|error| CatalogError::Serialization {
            message: format!("serialize prepared native grant: {error}"),
        })?;
        txn.put(PENDING_NATIVE_GRANT_KEY, Bytes::from(bytes))
            .await?;
        txn.commit().await?;
        Ok(event)
    }

    /// Complete a prepared grant only when its exact ledger event is current.
    /// A missing or changed event leaves the pending record and denies compilation.
    ///
    /// # Errors
    /// Rejects missing, foreign, changed, or superseded native ledger evidence.
    pub async fn reconcile_prepared_grant(&self) -> Result<MetastoreLedgerWatermark> {
        self.reconcile_prepared_grant_event(false).await
    }

    /// Replace an absent prepared grant with a durable no-op in its exact native
    /// event slot, then witness that no-op. A late grant append cannot take the slot.
    ///
    /// # Errors
    /// Rejects a landed grant, advanced ledger, changed witness, or missing intent.
    pub async fn abort_prepared_grant(&self) -> Result<MetastoreLedgerWatermark> {
        let mut txn = self.kernel.begin_control_txn(TxnOptions::new(None)).await?;
        let bytes =
            txn.get(PENDING_NATIVE_GRANT_KEY)
                .await?
                .ok_or_else(|| CatalogError::Validation {
                    message: "metastore has no prepared native grant".into(),
                })?;
        let pending: PendingNativeGrant =
            serde_json::from_slice(bytes.bytes()).map_err(|error| CatalogError::Serialization {
                message: format!("decode prepared native grant: {error}"),
            })?;
        let abort = aborted_grant_event(&pending)?;
        let prior_sequence = pending
            .previous_watermark
            .as_ref()
            .map_or(0, |prior| prior.sequence);
        let events = self.ledger.load_events().await?;
        let tail = events
            .iter()
            .filter(|event| event.sequence > prior_sequence)
            .collect::<Vec<_>>();
        if !tail.is_empty() && tail != [&abort] {
            return Err(CatalogError::Validation {
                message: "native ledger advanced beyond prepared grant".into(),
            });
        }
        self.ledger.append_event(&abort).await?;
        self.reconcile_prepared_grant_event(true).await
    }

    async fn reconcile_prepared_grant_event(
        &self,
        aborted: bool,
    ) -> Result<MetastoreLedgerWatermark> {
        let mut txn = self.kernel.begin_control_txn(TxnOptions::new(None)).await?;
        let pending =
            txn.get(PENDING_NATIVE_GRANT_KEY)
                .await?
                .ok_or_else(|| CatalogError::Validation {
                    message: "metastore has no prepared native grant".into(),
                })?;
        let pending: PendingNativeGrant =
            serde_json::from_slice(pending.bytes()).map_err(|error| {
                CatalogError::Serialization {
                    message: format!("decode prepared native grant: {error}"),
                }
            })?;
        let current_witness = txn.get(NATIVE_LEDGER_WITNESS_KEY).await?;
        let current_witness = current_witness
            .map(|value| serde_json::from_slice::<MetastoreLedgerWatermark>(value.bytes()))
            .transpose()
            .map_err(|error| CatalogError::Serialization {
                message: format!("decode native ledger watermark: {error}"),
            })?;
        if (pending.had_witness && current_witness != pending.previous_watermark)
            || (!pending.had_witness && current_witness.is_some())
        {
            return Err(CatalogError::Validation {
                message: "prepared native grant has a changed prior witness".into(),
            });
        }
        if !self.event_has_native_scope(&pending.event)
            || !matches!(&pending.event.mutation, MetastoreMutation::GrantUpserted(grant)
            if grant.identity_cut.as_ref().is_some_and(|cut| cut.tenant_id == self.reader.scope().tenant_id()))
        {
            return Err(CatalogError::Validation {
                message: "prepared native grant has foreign authority evidence".into(),
            });
        }
        let expected_abort = aborted_grant_event(&pending)?;
        let event = if aborted {
            &expected_abort
        } else {
            &pending.event
        };
        let watermark =
            self.ledger
                .latest_watermark()
                .await?
                .ok_or_else(|| CatalogError::Validation {
                    message: "prepared native grant is absent from the ledger".into(),
                })?;
        if watermark.event_id != event.event_id || watermark.sequence != event.sequence {
            return Err(CatalogError::Validation {
                message: "prepared native grant is not the current ledger event".into(),
            });
        }
        let events = self.ledger.load_events().await?;
        let prior_sequence = pending
            .previous_watermark
            .as_ref()
            .map_or(0, |prior| prior.sequence);
        let tail = events
            .iter()
            .filter(|stored| stored.sequence > prior_sequence)
            .collect::<Vec<_>>();
        if tail != [event]
            || events
                .iter()
                .any(|stored| !self.event_has_native_scope(stored))
            || pending.previous_watermark.as_ref().is_some_and(|prior| {
                !events.iter().any(|stored| {
                    stored.sequence == prior.sequence && stored.event_id == prior.event_id
                })
            })
        {
            return Err(CatalogError::Validation {
                message: "prepared native grant does not match the scoped ledger".into(),
            });
        }
        let bytes =
            serde_json::to_vec(&watermark).map_err(|error| CatalogError::Serialization {
                message: format!("serialize native ledger watermark: {error}"),
            })?;
        txn.put(NATIVE_LEDGER_WITNESS_KEY, Bytes::from(bytes))
            .await?;
        txn.delete(PENDING_NATIVE_GRANT_KEY).await?;
        txn.commit().await?;
        Ok(watermark)
    }

    fn event_has_native_scope(&self, event: &MetastoreEvent) -> bool {
        event.scope.as_ref().is_some_and(|scope| {
            scope.tenant_id == self.reader.scope().tenant_id()
                && scope.metastore_id == self.reader.scope().metastore_id()
        })
    }

    /// Compile native ledger grants against current tenant principal state.
    ///
    /// # Errors
    /// Denies missing, foreign, incomplete, or unreadable authority evidence.
    pub async fn compile_current(&self) -> Result<TenantCatalogCut> {
        let identity_token = self.identity.current_token().await?;
        self.validate_identity_token(&identity_token)?;
        let identity = self.identity.replay_at(identity_token.clone()).await?;
        let (metastore_token, watermark) = self.witnessed_watermark().await?;
        let events = self.ledger.load_events().await?;
        let included = events
            .iter()
            .filter(|event| event.sequence <= watermark.sequence)
            .collect::<Vec<_>>();
        if included.iter().any(|event| {
            !event.scope.as_ref().is_some_and(|scope| {
                scope.tenant_id == self.reader.scope().tenant_id()
                    && scope.metastore_id == self.reader.scope().metastore_id()
            })
        }) {
            return Err(CatalogError::Validation {
                message: "native ledger contains a foreign authority event".into(),
            });
        }
        if !included.iter().any(|event| {
            event.sequence == watermark.sequence && event.event_id == watermark.event_id
        }) {
            return Err(CatalogError::Validation {
                message: "metastore watermark has no matching event".into(),
            });
        }
        let metastore = replay_events(included)?;
        if metastore.grants.values().any(|grant| {
            grant.lifecycle_state == LifecycleState::Active
                && !grant.identity_cut.as_ref().is_some_and(|cut| {
                    cut.tenant_id == self.reader.scope().tenant_id()
                        && !cut.identity_manifest_id.is_empty()
                        && cut.identity_sequence > 0
                })
        }) {
            return Err(CatalogError::Validation {
                message: "native grant lacks tenant identity admission evidence".into(),
            });
        }
        let securables = metastore
            .catalog_objects
            .values()
            .filter(|object| object.lifecycle_state == LifecycleState::Active)
            .map(|object| {
                // Owner changes have no tenant-identity admission in this probe.
                SecurableObject::new(&object.object_id, &object.object_type, None, "")
            })
            .collect::<Vec<_>>();
        let snapshot = identity_snapshot(&identity_token, &identity);
        let compiled = compile_permissions_with_active(
            PermissionCompileInput {
                metastore: &metastore,
                identity: &snapshot,
                securables: &securables,
                ledger_watermark: &watermark.event_id,
            },
            |principal_id| {
                identity
                    .principals
                    .get(principal_id)
                    .is_some_and(|principal| {
                        principal.active && principal.kind != PrincipalKind::ExternalSubject
                    })
            },
        )?;
        Ok(TenantCatalogCut {
            identity_token,
            metastore_token,
            metastore_scope: self.reader.scope().clone(),
            metastore_watermark: watermark,
            identity,
            compiled,
        })
    }

    async fn witnessed_watermark(&self) -> Result<(StateToken, MetastoreLedgerWatermark)> {
        let metastore_token = self.kernel.current_state_token().await?;
        if metastore_token.scope().domain() != "catalog"
            || metastore_token.scope().tenant_id() != self.reader.scope().tenant_id()
            || metastore_token.scope().metastore_id() != Some(self.reader.scope().metastore_id())
        {
            return Err(CatalogError::Validation {
                message: "foreign metastore authority token".into(),
            });
        }
        let metastore_reader = self.kernel.read_at(metastore_token.clone()).await?;
        let witness_bytes = metastore_reader
            .get(NATIVE_LEDGER_WITNESS_KEY)
            .await?
            .ok_or_else(|| CatalogError::Validation {
                message: "metastore authority token lacks native ledger witness".into(),
            })?;
        if metastore_reader
            .get(PENDING_NATIVE_GRANT_KEY)
            .await?
            .is_some()
        {
            return Err(CatalogError::Validation {
                message: "metastore has a grant awaiting reconciliation".into(),
            });
        }
        let witnessed: MetastoreLedgerWatermark =
            serde_json::from_slice(&witness_bytes).map_err(|error| {
                CatalogError::Serialization {
                    message: format!("decode native ledger watermark: {error}"),
                }
            })?;
        let watermark =
            self.ledger
                .latest_watermark()
                .await?
                .ok_or_else(|| CatalogError::Validation {
                    message: "metastore ledger has no current watermark".into(),
                })?;
        if witnessed != watermark {
            return Err(CatalogError::Validation {
                message: "native ledger advanced beyond metastore authority token".into(),
            });
        }
        Ok((metastore_token, watermark))
    }

    /// Revalidate both roots before using cached native-ledger permissions.
    pub async fn evaluate(&self, cut: &TenantCatalogCut, request: &AuthzRequest) -> AuthzDecision {
        let Ok(identity_token) = self.identity.current_token().await else {
            return stale_catalog_decision(cut);
        };
        let Ok(watermark) = self.ledger.latest_watermark().await else {
            return stale_catalog_decision(cut);
        };
        let Ok(metastore_token) = self.kernel.current_state_token().await else {
            return stale_catalog_decision(cut);
        };
        if identity_token != cut.identity_token
            || metastore_token != cut.metastore_token
            || self.reader.scope() != &cut.metastore_scope
            || watermark.as_ref() != Some(&cut.metastore_watermark)
        {
            return stale_catalog_decision(cut);
        }
        let Ok(current) = self.identity.replay_at(identity_token).await else {
            return stale_catalog_decision(cut);
        };
        if !matches!((cut.identity.principals.get(&request.principal_id), current.principals.get(&request.principal_id)),
            (Some(before), Some(now)) if before.active && now.active
                && now.kind != PrincipalKind::ExternalSubject
                && before.membership_revision == now.membership_revision)
        {
            return stale_catalog_decision(cut);
        }
        AuthzDecision::evaluate(request, &cut.compiled)
    }

    /// Read an actual catalog table only while the current identity cut allows it.
    ///
    /// # Errors
    /// Denies stale authority, absent permissions, and catalog read failures.
    pub async fn read_table(
        &self,
        cut: &TenantCatalogCut,
        request: &AuthzRequest,
    ) -> Result<Option<Table>> {
        if request.object_type != "TABLE"
            || self.evaluate(cut, request).await.outcome != DecisionOutcome::Allow
        {
            return Err(CatalogError::Validation {
                message: "tenant catalog read denied".into(),
            });
        }
        let table = self.reader.get_table_by_id(&request.object_id).await?;
        if self.evaluate(cut, request).await.outcome != DecisionOutcome::Allow {
            return Err(CatalogError::Validation {
                message: "tenant catalog read denied".into(),
            });
        }
        Ok(table)
    }

    /// Read a table at a historical catalog token under current identity authority.
    ///
    /// # Errors
    /// Denies stale authority, absent permissions, and invalid historical tokens.
    pub async fn read_table_at(
        &self,
        cut: &TenantCatalogCut,
        request: &AuthzRequest,
        read_token: &str,
    ) -> Result<Option<Table>> {
        if request.object_type != "TABLE"
            || self.evaluate(cut, request).await.outcome != DecisionOutcome::Allow
        {
            return Err(CatalogError::Validation {
                message: "tenant catalog read denied".into(),
            });
        }
        let table = self
            .reader
            .get_table_by_id_for_root_token(read_token, &request.object_id)
            .await?;
        if self.evaluate(cut, request).await.outcome != DecisionOutcome::Allow {
            return Err(CatalogError::Validation {
                message: "tenant catalog read denied".into(),
            });
        }
        Ok(table)
    }

    fn validate_identity_token(&self, token: &StateToken) -> Result<()> {
        if token.scope().domain() != "principals"
            || !matches!(token.scope().root(), AuthorityRoot::TenantIdentity)
            || token.scope().tenant_id() != self.reader.scope().tenant_id()
        {
            return Err(CatalogError::Validation {
                message: "foreign tenant identity root".into(),
            });
        }
        Ok(())
    }
}

fn stale_catalog_decision(cut: &TenantCatalogCut) -> AuthzDecision {
    AuthzDecision {
        outcome: DecisionOutcome::Deny,
        reason_code: "stale_authority_cut".into(),
        reason_message_safe: "tenant authorization evidence is stale or unavailable".into(),
        evidence: AuthzEvidence::default(),
        group_snapshot_version: Some(cut.compiled.group_snapshot_version.clone()),
    }
}

fn identity_snapshot(token: &StateToken, identity: &IdentityState) -> IdentitySnapshot {
    let memberships = identity
        .principals
        .values()
        .filter(|principal| principal.active)
        .flat_map(|principal| {
            principal.group_ids.iter().filter_map(|group_id| {
                identity
                    .principals
                    .get(group_id)
                    .filter(|group| group.active && group.kind == PrincipalKind::Group)
                    .map(|_| GroupMembership::new(&principal.principal_id, group_id))
            })
        })
        .collect();
    IdentitySnapshot::new(token.authority_manifest_id(), memberships)
}
