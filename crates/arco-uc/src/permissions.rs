//! Authoritative compiled permission views for the Unity Catalog facade.
//!
//! Every UC authorization decision evaluates a [`CompiledPermissionSet`]. Until
//! this module existed, the only way to supply one was
//! [`crate::UnityCatalogState::with_compiled_permissions`], which the
//! production server wiring never called: `UnityCatalogState::new` left
//! `compiled_permissions` at `None`, so `require_authz` short-circuited to
//! `permissions_unavailable` and *every* authorized UC route — including the
//! storage-governance projection republish recovery route — was permanently
//! deny-closed in a deployed server, even for a METASTORE `Manage`
//! administrator.
//!
//! A [`CompiledPermissionSource`] closes that gap. It is resolved per request
//! scope (tenant + workspace), so one process serving several scopes never
//! evaluates one scope's grants against another's, and it stays fail-closed:
//! an absent source, an unreadable ledger, or a compilation failure yields no
//! permission view and the caller is denied.
//!
//! # Refresh lifecycle
//!
//! [`MetastorePermissionSource`] is authoritative-by-replay rather than
//! eventually consistent. On each request it reads the scope's latest
//! committed metastore ledger watermark and reuses its cached compiled view
//! only while that watermark is unchanged; any committed grant, revocation, or
//! principal-lifecycle event advances the watermark and forces a recompile on
//! the next request. The compiled view therefore carries the same watermark
//! the credential-vending path compares against
//! (`authz_*_denial_reason_for_watermark`), so a view that lags a committed
//! event is rejected as stale instead of being served.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::RwLock;

use arco_catalog::authz::compiler::{
    CompiledPermissionSet, PermissionCompileInput, SecurableObject, compile_permissions,
};
use arco_catalog::error::{CatalogError, Result};
use arco_catalog::identity::memberships::IdentitySnapshot;
use arco_catalog::metastore::events::LifecycleState;
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::metastore::replay::MetastoreState;
use arco_core::storage::StorageBackend;
use arco_core::{ControlPlaneScope, ScopedStorage};

/// Securable object type of the metastore root.
const METASTORE_OBJECT_TYPE: &str = "METASTORE";

/// Resolves the authoritative compiled permission view for one request scope.
///
/// Implementations must be scope-correct: the view returned for
/// `(tenant, workspace)` must be compiled only from that scope's authoritative
/// state.
#[async_trait::async_trait]
pub trait CompiledPermissionSource: Send + Sync + 'static {
    /// Returns the compiled permission view for a tenant/workspace scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the scope's authoritative state cannot be read or
    /// compiled. Callers treat any error as "no permission view" and deny.
    async fn compiled_permissions(
        &self,
        tenant: &str,
        workspace: &str,
    ) -> Result<Arc<CompiledPermissionSet>>;
}

/// Production [`CompiledPermissionSource`] backed by the metastore ledger.
///
/// Compiles ownership and grants from the replayed authoritative metastore
/// state of the request scope, keyed and invalidated by that scope's ledger
/// watermark. See the module docs for the refresh lifecycle.
pub struct MetastorePermissionSource {
    storage: Arc<dyn StorageBackend>,
    cached: RwLock<BTreeMap<(String, String), CachedPermissionView>>,
}

struct CachedPermissionView {
    ledger_watermark: Option<String>,
    permissions: Arc<CompiledPermissionSet>,
}

impl std::fmt::Debug for MetastorePermissionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetastorePermissionSource").finish()
    }
}

impl MetastorePermissionSource {
    /// Creates a permission source over a storage backend.
    #[must_use]
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            storage,
            cached: RwLock::new(BTreeMap::new()),
        }
    }

    fn cached_at_watermark(
        &self,
        key: &(String, String),
        watermark: Option<&str>,
    ) -> Option<Arc<CompiledPermissionSet>> {
        let cached = self.cached.read().ok()?;
        let hit = cached
            .get(key)
            .filter(|entry| entry.ledger_watermark.as_deref() == watermark)
            .map(|entry| Arc::clone(&entry.permissions));
        drop(cached);
        hit
    }
}

#[async_trait::async_trait]
impl CompiledPermissionSource for MetastorePermissionSource {
    async fn compiled_permissions(
        &self,
        tenant: &str,
        workspace: &str,
    ) -> Result<Arc<CompiledPermissionSet>> {
        let scope = ControlPlaneScope::workspace_alias(tenant, workspace).map_err(|err| {
            CatalogError::Validation {
                message: err.to_string(),
            }
        })?;
        let storage =
            ScopedStorage::new(Arc::clone(&self.storage), tenant, workspace).map_err(|err| {
                CatalogError::Validation {
                    message: err.to_string(),
                }
            })?;
        let ledger = MetastoreLedger::new(storage);
        let latest = ledger.latest_watermark().await?;
        let watermark = latest.as_ref().map(|watermark| watermark.event_id.clone());

        let key = (tenant.to_string(), workspace.to_string());
        if let Some(cached) = self.cached_at_watermark(&key, watermark.as_deref()) {
            return Ok(cached);
        }

        let state = ledger.replay().await?;
        // Compile against the watermark that was observed *before* the replay,
        // so a view can only ever be reported as older than the ledger, never
        // newer: staleness then fails the watermark comparison and denies.
        let compiled = Arc::new(compile_scope_permissions(
            &scope,
            &state,
            watermark.as_deref().unwrap_or_default(),
        )?);

        if let Ok(mut cached) = self.cached.write() {
            cached.insert(
                key,
                CachedPermissionView {
                    ledger_watermark: watermark,
                    permissions: Arc::clone(&compiled),
                },
            );
        }
        Ok(compiled)
    }
}

/// Compiles the permission view for one metastore scope.
///
/// Securables are the scope's metastore root plus every active catalog object,
/// external location, and managed root in the replayed state. Grants and
/// ownership therefore resolve on exactly the object identities the UC routes
/// authorize against (`METASTORE`, `CATALOG`/`SCHEMA`/`TABLE`,
/// `EXTERNAL_LOCATION`, `MANAGED_ROOT`).
///
/// The metastore event model does not record parent linkage between securable
/// objects, so the compiled hierarchy is flat: a grant authorizes the object it
/// names, and inheritance from an ancestor is not synthesized. This is the
/// fail-closed direction — a caller is never granted more than the ledger
/// states — but it means a metastore-wide grant does not by itself imply
/// per-catalog privileges.
///
/// # Errors
///
/// Returns an error when the securable hierarchy cannot be compiled.
fn compile_scope_permissions(
    scope: &ControlPlaneScope,
    state: &MetastoreState,
    ledger_watermark: &str,
) -> Result<CompiledPermissionSet> {
    let mut securables: Vec<SecurableObject> = Vec::new();
    let mut has_metastore_object = false;

    for object in state.catalog_objects.values() {
        if object.lifecycle_state != LifecycleState::Active {
            continue;
        }
        if object.object_id == scope.metastore_id() {
            has_metastore_object = true;
        }
        securables.push(SecurableObject::new(
            object.object_id.clone(),
            object.object_type.clone(),
            None,
            object.owner.clone(),
        ));
    }
    if !has_metastore_object {
        // The metastore root is a securable even when no catalog object
        // describes it: grants are made against `scope.metastore_id()`. It has
        // no owner of record, so it contributes no owner row.
        securables.push(SecurableObject::new(
            scope.metastore_id().to_string(),
            METASTORE_OBJECT_TYPE,
            None,
            String::new(),
        ));
    }
    for location in state.external_locations.values() {
        if location.lifecycle_state != LifecycleState::Active {
            continue;
        }
        securables.push(SecurableObject::new(
            location.location_id.clone(),
            "EXTERNAL_LOCATION",
            None,
            location.owner.clone(),
        ));
    }
    for root in state.managed_roots.values() {
        if root.lifecycle_state != LifecycleState::Active {
            continue;
        }
        securables.push(SecurableObject::new(
            root.root_id.clone(),
            "MANAGED_ROOT",
            None,
            root.owner.clone(),
        ));
    }

    // The metastore event model carries no group-membership records, so the
    // identity snapshot expands each principal to itself. Its version is
    // pinned to the ledger watermark so a compiled view is never reported as
    // fresher than the state it came from.
    let identity = IdentitySnapshot::new(ledger_watermark.to_string(), Vec::new());
    compile_permissions(PermissionCompileInput {
        metastore: state,
        identity: &identity,
        securables: &securables,
        ledger_watermark,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_catalog::authz::privileges::Privilege;
    use arco_catalog::metastore::events::{
        CatalogObjectRecord, GrantRecord, PrincipalKind, PrincipalRecord,
    };

    fn scope() -> ControlPlaneScope {
        ControlPlaneScope::workspace_alias("acme", "analytics").expect("scope")
    }

    fn principal(id: &str) -> PrincipalRecord {
        PrincipalRecord {
            principal_id: id.to_string(),
            name: id.to_string(),
            principal_kind: PrincipalKind::User,
            owner: "owner".to_string(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: 0,
            properties: BTreeMap::new(),
        }
    }

    #[test]
    fn metastore_grants_compile_onto_the_scope_metastore_securable() {
        let scope = scope();
        let mut state = MetastoreState::empty();
        state
            .principals
            .insert("user_admin".to_string(), principal("user_admin"));
        state.grants.insert(
            "grant_admin".to_string(),
            GrantRecord {
                grant_id: "grant_admin".to_string(),
                object_id: scope.metastore_id().to_string(),
                object_type: METASTORE_OBJECT_TYPE.to_string(),
                principal_id: "user_admin".to_string(),
                privilege: "MANAGE".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 0,
                properties: BTreeMap::new(),
            },
        );

        let compiled =
            compile_scope_permissions(&scope, &state, "event_007").expect("compile permissions");
        assert!(compiled.fresh);
        assert_eq!(compiled.ledger_watermark, "event_007");
        assert_eq!(
            compiled
                .rows_for_principal_object_privilege(
                    "user_admin",
                    scope.metastore_id(),
                    METASTORE_OBJECT_TYPE,
                    Privilege::Manage,
                )
                .count(),
            1
        );
        assert_eq!(
            compiled
                .rows_for_principal_object_privilege(
                    "user_reader",
                    scope.metastore_id(),
                    METASTORE_OBJECT_TYPE,
                    Privilege::Manage,
                )
                .count(),
            0,
            "an ungranted principal must compile to no metastore rows"
        );
    }

    #[test]
    fn deleted_grants_and_inactive_objects_do_not_compile() {
        let scope = scope();
        let mut state = MetastoreState::empty();
        state
            .principals
            .insert("user_admin".to_string(), principal("user_admin"));
        state.grants.insert(
            "grant_admin".to_string(),
            GrantRecord {
                grant_id: "grant_admin".to_string(),
                object_id: scope.metastore_id().to_string(),
                object_type: METASTORE_OBJECT_TYPE.to_string(),
                principal_id: "user_admin".to_string(),
                privilege: "MANAGE".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Deleted,
                updated_at_ms: 0,
                properties: BTreeMap::new(),
            },
        );
        state.catalog_objects.insert(
            "cat_dropped".to_string(),
            CatalogObjectRecord {
                object_id: "cat_dropped".to_string(),
                object_type: "CATALOG".to_string(),
                qualified_name: "dropped".to_string(),
                owner: "user_admin".to_string(),
                lifecycle_state: LifecycleState::Deleted,
                updated_at_ms: 0,
                properties: BTreeMap::new(),
            },
        );

        let compiled =
            compile_scope_permissions(&scope, &state, "event_009").expect("compile permissions");
        assert_eq!(
            compiled
                .rows_for_principal_object_privilege(
                    "user_admin",
                    scope.metastore_id(),
                    METASTORE_OBJECT_TYPE,
                    Privilege::Manage,
                )
                .count(),
            0,
            "a deleted grant must not compile into an enforcement row"
        );
        assert_eq!(
            compiled
                .rows_for_principal_object_privilege(
                    "user_admin",
                    "cat_dropped",
                    "CATALOG",
                    Privilege::Manage,
                )
                .count(),
            0,
            "a deleted securable must not compile an owner row"
        );
    }
}
