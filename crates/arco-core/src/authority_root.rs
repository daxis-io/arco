//! Typed authority root for scoped stores.

use crate::{
    ControlPlaneScope,
    error::{Error, Result},
};

/// The authority kind a scoped store is rooted at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityRoot {
    /// 'tenant={t}/identity' - tenant identity authority root.
    TenantIdentity,
    /// 'tenant={t}/metastore={m}/' - governed catalog authority root.
    Metastore {
        /// metastore ID
        metastore_id: String,
    },
    /// 'tenant={t}/workspace={w}' - execution / orchestration authority root.
    Workspace {
        /// workspace ID
        workspace_id: String,
    },
}

/// A tenant plus its authority root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityScope {
    tenant_id: String,
    root: AuthorityRoot,
}

impl AuthorityScope {
    /// Creates a tenant identity root scope.
    ///
    /// # Errors
    ///
    /// Returns an error if the tenant id is invalid.
    pub fn tenant_identity(tenant_id: impl Into<String>) -> Result<Self> {
        let tenant_id: String = tenant_id.into();

        Self::validate_id(&tenant_id, "tenant_id")?;

        Ok(Self {
            tenant_id,
            root: AuthorityRoot::TenantIdentity,
        })
    }

    /// Creates a metastore root scope.
    ///
    /// # Errors
    ///
    /// Returns an error if either id is invalid.
    pub fn metastore(
        tenant_id: impl Into<String>,
        metastore_id: impl Into<String>,
    ) -> Result<Self> {
        let tenant_id: String = tenant_id.into();
        let metastore_id: String = metastore_id.into();

        Self::validate_id(&tenant_id, "tenant_id")?;
        Self::validate_id(&metastore_id, "metastore_id")?;

        Ok(Self {
            tenant_id,
            root: AuthorityRoot::Metastore { metastore_id },
        })
    }

    /// Creates a workspace root scope.
    ///
    /// # Errors
    ///
    /// Returns an error if either id is invalid.
    pub fn workspace(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
    ) -> Result<Self> {
        let tenant_id: String = tenant_id.into();
        let workspace_id: String = workspace_id.into();

        Self::validate_id(&tenant_id, "tenant_id")?;
        Self::validate_id(&workspace_id, "workspace_id")?;

        Ok(Self {
            tenant_id,
            root: AuthorityRoot::Workspace { workspace_id },
        })
    }

    /// Creates a metastore root scope from a validated control-plane scope.
    #[must_use]
    pub fn from_metastore_scope(scope: &ControlPlaneScope) -> Self {
        Self {
            tenant_id: scope.tenant_id().to_string(),
            root: AuthorityRoot::Metastore {
                metastore_id: scope.metastore_id().to_string(),
            },
        }
    }

    /// Returns the tenant ID.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the typed root.
    #[must_use]
    pub fn root(&self) -> &AuthorityRoot {
        &self.root
    }

    /// Returns the workspace dimension for the legacy accessor shape.
    ///
    /// Workspace root: returns the workspace ID.
    /// Metastore root: returns the metastore ID for shape compatibility only.
    /// Identity root: returns the tenant ID for shape compatibility only.
    /// The metastore and tenant IDs must not be used for validation.
    #[allow(clippy::must_use_candidate)]
    pub fn workspace_dimension(&self) -> &str {
        match &self.root {
            AuthorityRoot::TenantIdentity => self.tenant_id(),
            AuthorityRoot::Metastore { metastore_id } => metastore_id,
            AuthorityRoot::Workspace { workspace_id } => workspace_id,
        }
    }

    /// Returns the scope-relative storage prefix (no trailing separator).
    #[must_use]
    pub fn prefix(&self) -> String {
        match &self.root {
            AuthorityRoot::TenantIdentity => format!("tenant={}/identity", self.tenant_id),
            AuthorityRoot::Metastore { metastore_id } => {
                format!("tenant={}/metastore={metastore_id}", self.tenant_id)
            }
            AuthorityRoot::Workspace { workspace_id } => {
                format!("tenant={}/workspace={workspace_id}", self.tenant_id)
            }
        }
    }

    /// Whether an event scope is accepted by this authority root.
    ///
    /// The event's `workspace_id` is asserted only for a workspace-rooted
    /// authority. A metastore-rooted authority is a shared store that any
    /// bound workspace may write, so the event workspace is provenance
    /// (recorded for audit), not a durability-boundary check (see
    /// the mutation path section in the metastore-scope-architecture.md).
    /// A tenant-identity authority is tenant-scoped, so neither workspace nor
    /// metastore is asserted. The tenant dimension is always enforced.
    ///
    #[must_use]
    pub fn accepts_scope(&self, tenant_id: &str, workspace_id: &str, metastore_id: &str) -> bool {
        if tenant_id != self.tenant_id {
            return false;
        }
        match &self.root {
            AuthorityRoot::TenantIdentity => true,
            AuthorityRoot::Metastore {
                metastore_id: root_metastore,
            } => root_metastore == metastore_id,
            AuthorityRoot::Workspace {
                workspace_id: root_workspace,
            } => root_workspace == workspace_id,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_matches_expected_authority_paths() {
        assert_eq!(
            AuthorityScope::tenant_identity("acme").unwrap().prefix(),
            "tenant=acme/identity"
        );
        assert_eq!(
            AuthorityScope::metastore("acme", "lakehouse")
                .unwrap()
                .prefix(),
            "tenant=acme/metastore=lakehouse"
        );
        assert_eq!(
            AuthorityScope::workspace("acme", "prod").unwrap().prefix(),
            "tenant=acme/workspace=prod"
        );
    }

    #[test]
    fn from_metastore_scope_matches_control_plane_metastore_prefix() {
        let scope = ControlPlaneScope::new("acme", "prod", "lakehouse").expect("scope");
        assert_eq!(
            AuthorityScope::from_metastore_scope(&scope).prefix(),
            scope.metastore_storage_prefix()
        );
    }

    #[test]
    fn workspace_dimension_returns_legacy_shim_per_root() {
        assert_eq!(
            AuthorityScope::workspace("acme", "prod")
                .unwrap()
                .workspace_dimension(),
            "prod"
        );
        assert_eq!(
            AuthorityScope::metastore("acme", "lakehouse")
                .unwrap()
                .workspace_dimension(),
            "lakehouse"
        );
        assert_eq!(
            AuthorityScope::tenant_identity("acme")
                .unwrap()
                .workspace_dimension(),
            "acme"
        );
    }

    #[test]
    fn accepts_scope_enforces_only_the_root_dimensions() {
        let ws = AuthorityScope::workspace("acme", "prod").unwrap();
        assert!(ws.accepts_scope("acme", "prod", "prod"));
        assert!(!ws.accepts_scope("acme", "staging", "prod"));
        assert!(!ws.accepts_scope("globex", "prod", "prod"));

        let ms = AuthorityScope::metastore("acme", "lakehouse").unwrap();
        assert!(ms.accepts_scope("acme", "notebooks", "lakehouse"));
        assert!(!ms.accepts_scope("acme", "notebooks", "other-metastore"));
        assert!(!ms.accepts_scope("globex", "notebooks", "lakehouse"));

        let id = AuthorityScope::tenant_identity("acme").unwrap();
        assert!(id.accepts_scope("acme", "any-workspace", "any-metastore"));
        assert!(!id.accepts_scope("globex", "any-workspace", "any-metastore"));
    }

    #[test]
    fn constructors_reject_invalid_ids() {
        assert!(AuthorityScope::tenant_identity("").is_err());
        assert!(AuthorityScope::tenant_identity("../acme").is_err());
        assert!(AuthorityScope::metastore("acme", "").is_err());
        assert!(AuthorityScope::workspace("acme", "prod/../evil").is_err());
    }
}
