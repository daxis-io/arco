//! Storage credential metadata.

use serde::{Deserialize, Serialize};

use crate::metastore::events::LifecycleState;

/// Tenant-visible storage credential metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCredentialMetadata {
    /// Stable credential ID.
    pub credential_id: String,
    /// Credential name.
    pub name: String,
    /// Cloud provider.
    pub cloud: String,
    /// Owner principal ID.
    pub owner: String,
    /// Lifecycle state.
    pub lifecycle_state: LifecycleState,
}

impl StorageCredentialMetadata {
    /// Creates active storage credential metadata.
    #[must_use]
    pub fn new(
        credential_id: impl Into<String>,
        name: impl Into<String>,
        cloud: impl Into<String>,
        owner: impl Into<String>,
    ) -> Self {
        Self {
            credential_id: credential_id.into(),
            name: name.into(),
            cloud: cloud.into(),
            owner: owner.into(),
            lifecycle_state: LifecycleState::Active,
        }
    }
}

/// Internal storage credential secret material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialSecret {
    secret_material_ref: String,
    encrypted_payload: String,
}

impl CredentialSecret {
    /// Creates internal credential secret material.
    #[must_use]
    pub fn new(
        secret_material_ref: impl Into<String>,
        encrypted_payload: impl Into<String>,
    ) -> Self {
        Self {
            secret_material_ref: secret_material_ref.into(),
            encrypted_payload: encrypted_payload.into(),
        }
    }

    /// Returns the internal secret reference.
    #[must_use]
    pub fn secret_material_ref(&self) -> &str {
        &self.secret_material_ref
    }

    /// Returns the encrypted payload.
    #[must_use]
    pub fn encrypted_payload(&self) -> &str {
        &self.encrypted_payload
    }
}
