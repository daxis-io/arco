//! Tenant, workspace, and metastore control-plane scope identifiers.
//!
//! `tenant_id` identifies the deployment/customer boundary, `workspace_id`
//! identifies the execution context, and `metastore_id` identifies the governed
//! catalog authority. During migration, callers may use the workspace alias
//! constructor so `metastore_id == workspace_id`.

use crate::error::{Error, Result};

/// Tenant, workspace, and metastore scope for control-plane catalog state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneScope {
    tenant: String,
    workspace: String,
    metastore: String,
}

impl ControlPlaneScope {
    /// Creates a control-plane scope with explicit tenant, workspace, and metastore IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if any ID is empty or invalid for path construction.
    pub fn new(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        metastore_id: impl Into<String>,
    ) -> Result<Self> {
        let tenant_id = tenant_id.into();
        let workspace_id = workspace_id.into();
        let metastore_id = metastore_id.into();

        Self::validate_id(&tenant_id, "tenant_id")?;
        Self::validate_id(&workspace_id, "workspace_id")?;
        Self::validate_id(&metastore_id, "metastore_id")?;

        Ok(Self {
            tenant: tenant_id,
            workspace: workspace_id,
            metastore: metastore_id,
        })
    }

    /// Creates a migration-compatible scope where `metastore_id == workspace_id`.
    ///
    /// # Errors
    ///
    /// Returns an error if either ID is empty or invalid for path construction.
    pub fn workspace_alias(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
    ) -> Result<Self> {
        let tenant_id = tenant_id.into();
        let workspace_id = workspace_id.into();
        Self::new(tenant_id, workspace_id.clone(), workspace_id)
    }

    /// Returns the tenant ID.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        &self.tenant
    }

    /// Returns the workspace ID.
    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace
    }

    /// Returns the metastore ID.
    #[must_use]
    pub fn metastore_id(&self) -> &str {
        &self.metastore
    }

    /// Returns the workspace-scoped storage prefix with a trailing slash.
    #[must_use]
    pub fn workspace_prefix(&self) -> String {
        format!("{}/", self.workspace_storage_prefix())
    }

    /// Returns the metastore-scoped storage prefix with a trailing slash.
    #[must_use]
    pub fn metastore_prefix(&self) -> String {
        format!("{}/", self.metastore_storage_prefix())
    }

    pub(crate) fn workspace_storage_prefix(&self) -> String {
        format!("tenant={}/workspace={}", self.tenant, self.workspace)
    }

    pub(crate) fn metastore_storage_prefix(&self) -> String {
        format!("tenant={}/metastore={}", self.tenant, self.metastore)
    }

    fn validate_id(id: &str, field: &str) -> Result<()> {
        if id.is_empty() {
            return Err(Error::InvalidId {
                message: format!("{field} cannot be empty"),
            });
        }

        if id.contains('/') || id.contains('\\') {
            return Err(Error::InvalidId {
                message: format!("{field} cannot contain path separators"),
            });
        }

        if id.contains('\n') || id.contains('\r') || id.contains('\0') {
            return Err(Error::InvalidId {
                message: format!("{field} cannot contain control characters"),
            });
        }

        if !id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
        {
            return Err(Error::InvalidId {
                message: format!(
                    "{field} contains invalid characters (allowed: a-z, 0-9, '-', '_')"
                ),
            });
        }

        Ok(())
    }
}
