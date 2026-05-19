//! Metastore event and record types.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use arco_core::ControlPlaneScope;

use crate::error::{CatalogError, Result};

/// Lifecycle state shared by native metastore objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    /// Object is active and visible to authorized callers.
    Active,
    /// Object is tombstoned or deleted.
    Deleted,
    /// Object exists but cannot be used for enforcement or credential vending.
    Disabled,
}

impl LifecycleState {
    /// Returns the stable string form used in projections.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Deleted => "deleted",
            Self::Disabled => "disabled",
        }
    }
}

/// Principal family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    /// Human user.
    User,
    /// Service principal.
    ServicePrincipal,
    /// Workload identity.
    Workload,
    /// Group principal.
    Group,
    /// External or federated subject.
    ExternalSubject,
}

impl PrincipalKind {
    /// Returns the stable string form used in projections.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::ServicePrincipal => "service_principal",
            Self::Workload => "workload",
            Self::Group => "group",
            Self::ExternalSubject => "external_subject",
        }
    }
}

/// Principal record used by the replay kernel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalRecord {
    /// Stable principal ID.
    pub principal_id: String,
    /// Lookup name or display subject.
    pub name: String,
    /// Principal family.
    pub principal_kind: PrincipalKind,
    /// Owner or administering principal.
    pub owner: String,
    /// Lifecycle state.
    pub lifecycle_state: LifecycleState,
    /// Last update timestamp in milliseconds since epoch.
    pub updated_at_ms: i64,
    /// Compatibility metadata. Not an enforcement model.
    pub properties: BTreeMap<String, String>,
}

/// Grant record used by the replay kernel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantRecord {
    /// Stable grant ID.
    pub grant_id: String,
    /// Stable securable object ID.
    pub object_id: String,
    /// Securable object type.
    pub object_type: String,
    /// Stable principal ID.
    pub principal_id: String,
    /// Privilege name.
    pub privilege: String,
    /// Owner or grant administrator.
    pub owner: String,
    /// Lifecycle state.
    pub lifecycle_state: LifecycleState,
    /// Last update timestamp in milliseconds since epoch.
    pub updated_at_ms: i64,
    /// Compatibility metadata. Not an enforcement model.
    pub properties: BTreeMap<String, String>,
}

/// Storage credential metadata used by the replay kernel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCredentialRecord {
    /// Stable credential ID.
    pub credential_id: String,
    /// Credential display name.
    pub name: String,
    /// Cloud/provider identifier.
    pub cloud: String,
    /// Owner or administering principal.
    pub owner: String,
    /// Lifecycle state.
    pub lifecycle_state: LifecycleState,
    /// Last update timestamp in milliseconds since epoch.
    pub updated_at_ms: i64,
    /// Compatibility metadata. Not an enforcement model.
    pub properties: BTreeMap<String, String>,
    /// Internal reference to secret material. Never exposed in projections.
    pub secret_material_ref: Option<String>,
    /// Internal encrypted payload. Never exposed in projections.
    pub encrypted_payload: Option<String>,
}

/// External location metadata used by the replay kernel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalLocationRecord {
    /// Stable external location ID.
    pub location_id: String,
    /// External location display name.
    pub name: String,
    /// Canonical governed URL.
    pub url: String,
    /// Stable storage credential ID.
    pub credential_id: String,
    /// Owner or administering principal.
    pub owner: String,
    /// Lifecycle state.
    pub lifecycle_state: LifecycleState,
    /// Last update timestamp in milliseconds since epoch.
    pub updated_at_ms: i64,
    /// Compatibility metadata. Not an enforcement model.
    pub properties: BTreeMap<String, String>,
}

/// Managed root metadata used by the replay kernel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedRootRecord {
    /// Stable managed root ID.
    pub root_id: String,
    /// Managed root display name.
    pub name: String,
    /// Workspace bound to this managed root.
    pub workspace_id: String,
    /// Canonical governed URL.
    pub url: String,
    /// Owner or administering principal.
    pub owner: String,
    /// Lifecycle state.
    pub lifecycle_state: LifecycleState,
    /// Last update timestamp in milliseconds since epoch.
    pub updated_at_ms: i64,
    /// Compatibility metadata. Not an enforcement model.
    pub properties: BTreeMap<String, String>,
}

/// Workspace binding metadata used by the replay kernel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceBindingRecord {
    /// Stable binding ID.
    pub binding_id: String,
    /// Workspace ID.
    pub workspace_id: String,
    /// Stable bound object ID.
    pub object_id: String,
    /// Bound object type.
    pub object_type: String,
    /// Owner or administering principal.
    pub owner: String,
    /// Lifecycle state.
    pub lifecycle_state: LifecycleState,
    /// Last update timestamp in milliseconds since epoch.
    pub updated_at_ms: i64,
    /// Compatibility metadata. Not an enforcement model.
    pub properties: BTreeMap<String, String>,
}

/// Durable scope carried by a native metastore event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetastoreEventScope {
    /// Tenant/deployment boundary.
    pub tenant_id: String,
    /// Workspace execution context that produced or is reading the event.
    pub workspace_id: String,
    /// Governed catalog authority that owns the mutation.
    pub metastore_id: String,
}

impl MetastoreEventScope {
    /// Creates event scope evidence from a validated control-plane scope.
    #[must_use]
    pub fn from_control_plane_scope(scope: &ControlPlaneScope) -> Self {
        Self {
            tenant_id: scope.tenant_id().to_string(),
            workspace_id: scope.workspace_id().to_string(),
            metastore_id: scope.metastore_id().to_string(),
        }
    }
}

/// Native catalog object identity record used by metastore replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogObjectRecord {
    /// Stable object ID.
    pub object_id: String,
    /// Securable object type, for example `CATALOG`, `SCHEMA`, or `TABLE`.
    pub object_type: String,
    /// Fully qualified mutable name within the metastore.
    pub qualified_name: String,
    /// Owner or administering principal.
    pub owner: String,
    /// Lifecycle state.
    pub lifecycle_state: LifecycleState,
    /// Last update timestamp in milliseconds since epoch.
    pub updated_at_ms: i64,
    /// Compatibility metadata. Not an enforcement model.
    pub properties: BTreeMap<String, String>,
}

/// Native metastore mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MetastoreMutation {
    /// Upsert a catalog authority object by stable ID.
    CatalogObjectUpserted(CatalogObjectRecord),
    /// Upsert a principal by stable ID.
    PrincipalUpserted(PrincipalRecord),
    /// Upsert a grant by stable ID.
    GrantUpserted(GrantRecord),
    /// Upsert safe storage credential metadata by stable ID.
    StorageCredentialUpserted(StorageCredentialRecord),
    /// Upsert an external location by stable ID.
    ExternalLocationUpserted(ExternalLocationRecord),
    /// Upsert a managed root by stable ID.
    ManagedRootUpserted(ManagedRootRecord),
    /// Upsert a workspace binding by stable ID.
    WorkspaceBindingUpserted(WorkspaceBindingRecord),
}

/// Native metastore event envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetastoreEvent {
    /// Optional durable tenant/workspace/metastore scope.
    ///
    /// Legacy in-memory tests may omit this while compatibility call paths are
    /// being migrated. Scoped events include this field in deterministic hashes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<MetastoreEventScope>,
    /// Stable event ID.
    pub event_id: String,
    /// Monotonic replay sequence.
    pub sequence: u64,
    /// Mutation payload.
    pub mutation: MetastoreMutation,
}

impl MetastoreEvent {
    /// Creates a new metastore event.
    #[must_use]
    pub fn new(event_id: impl Into<String>, sequence: u64, mutation: MetastoreMutation) -> Self {
        Self {
            scope: None,
            event_id: event_id.into(),
            sequence,
            mutation,
        }
    }

    /// Creates a new scoped metastore event.
    #[must_use]
    pub fn new_scoped(
        scope: &ControlPlaneScope,
        event_id: impl Into<String>,
        sequence: u64,
        mutation: MetastoreMutation,
    ) -> Self {
        Self {
            scope: Some(MetastoreEventScope::from_control_plane_scope(scope)),
            event_id: event_id.into(),
            sequence,
            mutation,
        }
    }

    /// Computes a deterministic hash over canonical JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical JSON serialization fails.
    pub fn deterministic_hash(&self) -> Result<String> {
        deterministic_hash(self)
    }
}

/// Computes a stable `sha256:` hash for a serializable value.
///
/// # Errors
///
/// Returns an error if canonical JSON serialization fails.
pub fn deterministic_hash(value: &impl Serialize) -> Result<String> {
    let canonical = serde_jcs::to_string(value).map_err(|err| CatalogError::Serialization {
        message: format!("metastore canonical serialization failed: {err}"),
    })?;
    let hash = Sha256::digest(canonical.as_bytes());
    Ok(format!("sha256:{}", hex::encode(hash)))
}
