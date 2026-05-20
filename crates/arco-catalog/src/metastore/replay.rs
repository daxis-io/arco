//! Deterministic metastore replay.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use arco_core::ControlPlaneScope;

use crate::error::Result;

use super::events::{
    CatalogObjectRecord, ExternalLocationRecord, GrantRecord, LifecycleState, ManagedRootRecord,
    MetastoreEvent, MetastoreEventScope, MetastoreMutation, PrincipalRecord,
    StorageCredentialRecord, WorkspaceBindingRecord, deterministic_hash,
};

/// In-memory replayed metastore state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetastoreState {
    /// Event scopes keyed by stable event ID.
    pub event_scopes: BTreeMap<String, MetastoreEventScope>,
    /// Catalog authority objects keyed by stable object ID.
    pub catalog_objects: BTreeMap<String, CatalogObjectRecord>,
    /// Catalog authority object name index keyed by tenant, metastore, type, and name.
    pub catalog_object_names: BTreeMap<String, String>,
    /// Principals keyed by stable principal ID.
    pub principals: BTreeMap<String, PrincipalRecord>,
    /// Secondary principal name index.
    pub principal_names: BTreeMap<String, String>,
    /// Grants keyed by stable grant ID.
    pub grants: BTreeMap<String, GrantRecord>,
    /// Storage credential metadata keyed by stable credential ID.
    pub storage_credentials: BTreeMap<String, StorageCredentialRecord>,
    /// External locations keyed by stable location ID.
    pub external_locations: BTreeMap<String, ExternalLocationRecord>,
    /// Managed roots keyed by stable root ID.
    pub managed_roots: BTreeMap<String, ManagedRootRecord>,
    /// Workspace bindings keyed by stable binding ID.
    pub workspace_bindings: BTreeMap<String, WorkspaceBindingRecord>,
    /// Last applied event ID.
    pub ledger_watermark: Option<String>,
}

impl MetastoreState {
    /// Returns an empty metastore state.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Applies one metastore event.
    pub fn apply_event(&mut self, event: &MetastoreEvent) {
        if let Some(scope) = &event.scope {
            self.event_scopes
                .insert(event.event_id.clone(), scope.clone());
        }

        match &event.mutation {
            MetastoreMutation::CatalogObjectUpserted(record) => {
                if let Some(scope) = &event.scope {
                    self.catalog_object_names
                        .retain(|_, object_id| object_id != &record.object_id);
                    if record.lifecycle_state == LifecycleState::Active {
                        self.catalog_object_names.insert(
                            catalog_object_name_key(
                                &scope.tenant_id,
                                &scope.metastore_id,
                                &record.object_type,
                                &record.qualified_name,
                            ),
                            record.object_id.clone(),
                        );
                    }
                }
                self.catalog_objects
                    .insert(record.object_id.clone(), record.clone());
            }
            MetastoreMutation::PrincipalUpserted(record) => {
                self.principal_names
                    .insert(record.name.clone(), record.principal_id.clone());
                self.principals
                    .insert(record.principal_id.clone(), record.clone());
            }
            MetastoreMutation::GrantUpserted(record) => {
                self.grants.insert(record.grant_id.clone(), record.clone());
            }
            MetastoreMutation::StorageCredentialUpserted(record) => {
                self.storage_credentials
                    .insert(record.credential_id.clone(), record.clone());
            }
            MetastoreMutation::ExternalLocationUpserted(record) => {
                self.external_locations
                    .insert(record.location_id.clone(), record.clone());
            }
            MetastoreMutation::ManagedRootUpserted(record) => {
                self.managed_roots
                    .insert(record.root_id.clone(), record.clone());
            }
            MetastoreMutation::WorkspaceBindingUpserted(record) => {
                self.workspace_bindings
                    .insert(record.binding_id.clone(), record.clone());
            }
        }
        self.ledger_watermark = Some(event.event_id.clone());
    }

    /// Looks up a principal ID by secondary name.
    #[must_use]
    pub fn principal_id_by_name(&self, name: &str) -> Option<&str> {
        self.principal_names.get(name).map(String::as_str)
    }

    /// Looks up a catalog object ID by metastore authority scope and mutable name.
    #[must_use]
    pub fn catalog_object_id_for_scope(
        &self,
        scope: &ControlPlaneScope,
        object_type: &str,
        qualified_name: &str,
    ) -> Option<&str> {
        self.catalog_object_names
            .get(&catalog_object_name_key(
                scope.tenant_id(),
                scope.metastore_id(),
                object_type,
                qualified_name,
            ))
            .map(String::as_str)
    }

    /// Computes a deterministic digest of the replayed state.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical JSON serialization fails.
    pub fn try_deterministic_digest(&self) -> Result<String> {
        deterministic_hash(self)
    }

    /// Computes a deterministic digest of the replayed state.
    ///
    /// This convenience method panics only if serialization of the in-memory
    /// state fails, which would be a programming error.
    ///
    /// # Panics
    ///
    /// Panics if canonical JSON serialization of the in-memory metastore state
    /// fails.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn deterministic_digest(&self) -> String {
        self.try_deterministic_digest()
            .expect("metastore state serialization should be deterministic")
    }
}

/// Replays metastore events in sequence order.
///
/// # Errors
///
/// Returns an error if state validation fails. The current kernel has no
/// fallible validation beyond future extension points, so this returns `Ok`
/// after deterministic folding.
pub fn replay_events<'a>(
    events: impl IntoIterator<Item = &'a MetastoreEvent>,
) -> Result<MetastoreState> {
    let mut ordered: Vec<&MetastoreEvent> = events.into_iter().collect();
    ordered.sort_by_key(|event| event.sequence);

    let mut state = MetastoreState::empty();
    for event in ordered {
        state.apply_event(event);
    }

    Ok(state)
}

fn catalog_object_name_key(
    tenant_id: &str,
    metastore_id: &str,
    object_type: &str,
    qualified_name: &str,
) -> String {
    format!("{tenant_id}\u{1f}{metastore_id}\u{1f}{object_type}\u{1f}{qualified_name}")
}
