//! Test-only cross-root authorization cut; no production route calls this module.

use arco_core::AuthorityRoot;

use crate::authz::compiler::{
    CompiledPermissionSet, PermissionCompileInput, SecurableObject, compile_permissions_with_active,
};
use crate::authz::decision::{AuthzDecision, AuthzEvidence, AuthzRequest, DecisionOutcome};
use crate::error::{CatalogError, Result};
use crate::identity::memberships::{GroupMembership, IdentitySnapshot};
use crate::metastore::events::PrincipalKind;
use crate::metastore::replay::MetastoreState;
use crate::state_store::identity_probe::{IdentityState, PrincipalIdentityStore};
use crate::{ArcoStateAdmin, ArcoStateReader, ControlMvpStateStore, StateToken};

/// State key used only by the test metastore fixture.
pub const METASTORE_AUTHZ_STATE_KEY: &[u8] = b"authz/tenant-probe-state-v1";

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
        let snapshot = IdentitySnapshot::new(identity_token.authority_manifest_id(), memberships);
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
