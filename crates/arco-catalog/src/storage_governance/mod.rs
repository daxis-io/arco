//! Storage-governance domain.

use std::collections::BTreeMap;

use crate::error::{CatalogError, Result};
use crate::metastore::events::LifecycleState;
use crate::metastore::replay::MetastoreState;

use self::bindings::WorkspaceBinding;
use self::credentials::{CredentialSecret, StorageCredentialMetadata};
use self::external_locations::ExternalLocation;
use self::managed_roots::ManagedRoot;
use self::path_normalization::GovernedPath;

pub mod bindings;
pub mod credentials;
pub mod external_locations;
pub mod managed_roots;
pub mod path_normalization;

/// Kind of path authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathAuthorityKind {
    /// External location authority.
    ExternalLocation,
    /// Managed root authority.
    ManagedRoot,
}

/// Path ownership decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathDecision {
    /// Stable authority object ID.
    pub object_id: String,
    /// Authority kind.
    pub authority_kind: PathAuthorityKind,
}

impl PathDecision {
    /// Creates an owned path decision.
    #[must_use]
    pub fn owned(object_id: impl Into<String>, authority_kind: PathAuthorityKind) -> Self {
        Self {
            object_id: object_id.into(),
            authority_kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PathAuthority {
    object_id: String,
    kind: PathAuthorityKind,
    path: GovernedPath,
    workspace_id: Option<String>,
    lifecycle_state: LifecycleState,
}

/// Authoritative storage-governance state.
#[derive(Debug, Clone, Default)]
pub struct StorageGovernanceState {
    credentials: BTreeMap<String, StorageCredentialMetadata>,
    credential_secrets: BTreeMap<String, CredentialSecret>,
    external_locations: BTreeMap<String, ExternalLocation>,
    managed_roots: BTreeMap<String, ManagedRoot>,
    workspace_bindings: BTreeMap<String, WorkspaceBinding>,
    #[cfg(test)]
    unsafe_authorities: BTreeMap<String, PathAuthority>,
}

impl StorageGovernanceState {
    /// Creates storage credential metadata and stores secret material separately.
    ///
    /// # Errors
    ///
    /// Returns an error when the credential ID already exists.
    pub fn create_storage_credential(
        &mut self,
        metadata: StorageCredentialMetadata,
        secret: CredentialSecret,
    ) -> Result<()> {
        if self.credentials.contains_key(&metadata.credential_id) {
            return Err(CatalogError::AlreadyExists {
                entity: "storage_credential".to_string(),
                name: metadata.credential_id,
            });
        }
        self.credential_secrets
            .insert(metadata.credential_id.clone(), secret);
        self.credentials
            .insert(metadata.credential_id.clone(), metadata);
        Ok(())
    }

    /// Returns redacted storage credential metadata.
    #[must_use]
    pub fn get_storage_credential(
        &self,
        credential_id: &str,
    ) -> Result<Option<StorageCredentialMetadata>> {
        Ok(self.credentials.get(credential_id).cloned())
    }

    /// Lists redacted storage credential metadata.
    #[must_use]
    pub fn list_storage_credentials(&self) -> Vec<StorageCredentialMetadata> {
        self.credentials.values().cloned().collect()
    }

    /// Returns an external location by stable ID.
    #[must_use]
    pub fn get_external_location(&self, location_id: &str) -> Option<ExternalLocation> {
        self.external_locations.get(location_id).cloned()
    }

    /// Lists external locations.
    #[must_use]
    pub fn list_external_locations(&self) -> Vec<ExternalLocation> {
        self.external_locations.values().cloned().collect()
    }

    /// Returns a managed root by stable ID.
    #[must_use]
    pub fn get_managed_root(&self, root_id: &str) -> Option<ManagedRoot> {
        self.managed_roots.get(root_id).cloned()
    }

    /// Lists managed roots.
    #[must_use]
    pub fn list_managed_roots(&self) -> Vec<ManagedRoot> {
        self.managed_roots.values().cloned().collect()
    }

    /// Returns a workspace binding by stable ID.
    #[must_use]
    pub fn get_workspace_binding(&self, binding_id: &str) -> Option<WorkspaceBinding> {
        self.workspace_bindings.get(binding_id).cloned()
    }

    /// Lists workspace bindings.
    #[must_use]
    pub fn list_workspace_bindings(&self) -> Vec<WorkspaceBinding> {
        self.workspace_bindings.values().cloned().collect()
    }

    /// Builds enforcement-grade storage-governance state from replayed metastore state.
    ///
    /// # Errors
    ///
    /// Returns an error when replayed records contain invalid paths, missing
    /// credential references, or unsafe path overlaps.
    pub fn from_metastore_state(metastore: &MetastoreState) -> Result<Self> {
        let mut state = Self::default();

        for record in metastore.storage_credentials.values() {
            state.create_storage_credential(
                StorageCredentialMetadata {
                    credential_id: record.credential_id.clone(),
                    name: record.name.clone(),
                    cloud: record.cloud.clone(),
                    owner: record.owner.clone(),
                    lifecycle_state: record.lifecycle_state,
                },
                CredentialSecret::new(
                    record.secret_material_ref.clone().unwrap_or_default(),
                    record.encrypted_payload.clone().unwrap_or_default(),
                ),
            )?;
        }

        for record in metastore.external_locations.values() {
            let mut location = ExternalLocation::new(
                record.location_id.clone(),
                record.name.clone(),
                &record.url,
                record.credential_id.clone(),
                record.owner.clone(),
            )?;
            location.lifecycle_state = record.lifecycle_state;
            if location.lifecycle_state == LifecycleState::Active {
                state.create_external_location(location)?;
            } else {
                state
                    .external_locations
                    .insert(location.location_id.clone(), location);
            }
        }

        for record in metastore.managed_roots.values() {
            let mut root = ManagedRoot::new(
                record.root_id.clone(),
                record.name.clone(),
                record.workspace_id.clone(),
                &record.url,
                record.owner.clone(),
            )?;
            root.lifecycle_state = record.lifecycle_state;
            if root.lifecycle_state == LifecycleState::Active {
                state.create_managed_root(root)?;
            } else {
                state.managed_roots.insert(root.root_id.clone(), root);
            }
        }

        for record in metastore.workspace_bindings.values() {
            let mut binding = WorkspaceBinding::new(
                record.binding_id.clone(),
                record.workspace_id.clone(),
                record.object_id.clone(),
                record.object_type.clone(),
                record.owner.clone(),
            );
            binding.lifecycle_state = record.lifecycle_state;
            state.bind_workspace(binding)?;
        }

        Ok(state)
    }

    /// Creates an external location.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown credentials, duplicate IDs, invalid paths,
    /// or unsafe path overlap.
    pub fn create_external_location(&mut self, location: ExternalLocation) -> Result<()> {
        if !self.credentials.contains_key(&location.credential_id) {
            return Err(CatalogError::NotFound {
                entity: "storage_credential".to_string(),
                name: location.credential_id,
            });
        }
        if self.external_locations.contains_key(&location.location_id) {
            return Err(CatalogError::AlreadyExists {
                entity: "external_location".to_string(),
                name: location.location_id,
            });
        }
        self.validate_no_overlap(
            &location.location_id,
            PathAuthorityKind::ExternalLocation,
            &location.path,
        )?;
        self.external_locations
            .insert(location.location_id.clone(), location);
        Ok(())
    }

    /// Creates a managed root.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate IDs, invalid paths, or unsafe path overlap.
    pub fn create_managed_root(&mut self, root: ManagedRoot) -> Result<()> {
        if self.managed_roots.contains_key(&root.root_id) {
            return Err(CatalogError::AlreadyExists {
                entity: "managed_root".to_string(),
                name: root.root_id,
            });
        }
        self.validate_no_overlap(&root.root_id, PathAuthorityKind::ManagedRoot, &root.path)?;
        self.managed_roots.insert(root.root_id.clone(), root);
        Ok(())
    }

    /// Creates a workspace binding.
    ///
    /// # Errors
    ///
    /// Returns an error when the binding ID already exists or when the target
    /// object ID/type does not match an existing storage-governance authority.
    pub fn bind_workspace(&mut self, binding: WorkspaceBinding) -> Result<()> {
        if self.workspace_bindings.contains_key(&binding.binding_id) {
            return Err(CatalogError::AlreadyExists {
                entity: "workspace_binding".to_string(),
                name: binding.binding_id,
            });
        }
        if !self.binding_target_exists(&binding) {
            return Err(CatalogError::Validation {
                message: format!(
                    "workspace binding '{}' references unknown or mismatched {} '{}'",
                    binding.binding_id, binding.object_type, binding.object_id
                ),
            });
        }
        self.workspace_bindings
            .insert(binding.binding_id.clone(), binding);
        Ok(())
    }

    /// Returns the path authority for a workspace and URI.
    ///
    /// # Errors
    ///
    /// Returns an explicit error for not-governed, unbound, or ambiguous paths.
    pub fn authority_for_path(&self, workspace_id: &str, uri: &str) -> Result<PathDecision> {
        let path = GovernedPath::parse(uri)?;
        let matches = self
            .active_authorities()
            .into_iter()
            .filter(|authority| authority.path.contains(&path))
            .filter(|authority| self.authority_bound_to_workspace(authority, workspace_id))
            .collect::<Vec<_>>();

        match matches.as_slice() {
            [] => Err(CatalogError::NotFound {
                entity: "path_authority".to_string(),
                name: path.canonical_uri(),
            }),
            [authority] => Ok(PathDecision::owned(
                authority.object_id.clone(),
                authority.kind,
            )),
            _ => Err(CatalogError::PreconditionFailed {
                message: "ambiguous path authority".to_string(),
            }),
        }
    }

    fn validate_no_overlap(
        &self,
        object_id: &str,
        kind: PathAuthorityKind,
        path: &GovernedPath,
    ) -> Result<()> {
        for authority in self.active_authorities() {
            if authority.object_id == object_id && authority.kind == kind {
                continue;
            }
            if authority.path.overlaps(path) {
                return Err(CatalogError::PreconditionFailed {
                    message: format!("path overlaps existing authority '{}'", authority.object_id),
                });
            }
        }
        Ok(())
    }

    fn active_authorities(&self) -> Vec<PathAuthority> {
        let mut authorities = Vec::new();
        authorities.extend(
            self.external_locations
                .values()
                .map(|location| PathAuthority {
                    object_id: location.location_id.clone(),
                    kind: PathAuthorityKind::ExternalLocation,
                    path: location.path.clone(),
                    workspace_id: None,
                    lifecycle_state: location.lifecycle_state,
                }),
        );
        authorities.extend(self.managed_roots.values().map(|root| PathAuthority {
            object_id: root.root_id.clone(),
            kind: PathAuthorityKind::ManagedRoot,
            path: root.path.clone(),
            workspace_id: Some(root.workspace_id.clone()),
            lifecycle_state: root.lifecycle_state,
        }));
        #[cfg(test)]
        authorities.extend(self.unsafe_authorities.values().cloned());
        authorities
            .into_iter()
            .filter(|authority| authority.lifecycle_state == LifecycleState::Active)
            .collect()
    }

    fn authority_bound_to_workspace(&self, authority: &PathAuthority, workspace_id: &str) -> bool {
        if authority.workspace_id.as_deref() == Some(workspace_id) {
            return true;
        }
        self.workspace_bindings.values().any(|binding| {
            binding.lifecycle_state == LifecycleState::Active
                && binding.workspace_id == workspace_id
                && binding.object_id == authority.object_id
                && binding
                    .object_type
                    .eq_ignore_ascii_case(authority_object_type(authority.kind))
        })
    }

    fn binding_target_exists(&self, binding: &WorkspaceBinding) -> bool {
        match binding.object_type.to_ascii_uppercase().as_str() {
            "EXTERNAL_LOCATION" => self.external_locations.contains_key(&binding.object_id),
            "MANAGED_ROOT" => self.managed_roots.contains_key(&binding.object_id),
            _ => false,
        }
    }
}

fn authority_object_type(kind: PathAuthorityKind) -> &'static str {
    match kind {
        PathAuthorityKind::ExternalLocation => "EXTERNAL_LOCATION",
        PathAuthorityKind::ManagedRoot => "MANAGED_ROOT",
    }
}

#[cfg(test)]
mod tests {
    use super::bindings::WorkspaceBinding;
    use super::credentials::{CredentialSecret, StorageCredentialMetadata};
    use super::external_locations::ExternalLocation;
    use super::*;

    #[test]
    fn path_authority_fails_closed_for_ambiguous_internal_state() -> Result<()> {
        let mut state = StorageGovernanceState::default();
        state.create_storage_credential(
            StorageCredentialMetadata::new("cred_01", "lakehouse-prod", "gcs", "owner"),
            CredentialSecret::new("secret://cred/01", "encrypted-token"),
        )?;
        state.create_external_location(ExternalLocation::new(
            "loc_orders",
            "orders",
            "gs://bucket/warehouse/orders",
            "cred_01",
            "owner",
        )?)?;
        state.bind_workspace(WorkspaceBinding::new(
            "binding_01",
            "workspace1",
            "loc_orders",
            "EXTERNAL_LOCATION",
            "owner",
        ))?;
        state.unsafe_authorities.insert(
            "unsafe_duplicate".to_string(),
            PathAuthority {
                object_id: "unsafe_duplicate".to_string(),
                kind: PathAuthorityKind::ExternalLocation,
                path: GovernedPath::parse("gs://bucket/warehouse/orders")?,
                workspace_id: Some("workspace1".to_string()),
                lifecycle_state: LifecycleState::Active,
            },
        );

        assert!(
            state
                .authority_for_path(
                    "workspace1",
                    "gs://bucket/warehouse/orders/day=1/file.parquet"
                )
                .is_err()
        );
        Ok(())
    }
}
