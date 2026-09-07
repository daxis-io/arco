//! Typed authority identity and path construction.
//!
//! This is groundwork for additional authority families. It does not define a
//! persisted `StateScope` encoding or enable tenant identity in legacy stores.

use crate::{
    ControlPlaneScope,
    error::{Error, Result},
};

/// The authority kind a scoped store is rooted at.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
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

    /// Returns a workspace ID only for a workspace authority root.
    #[must_use]
    pub fn workspace_id(&self) -> Option<&str> {
        match &self.root {
            AuthorityRoot::Workspace { workspace_id } => Some(workspace_id),
            _ => None,
        }
    }

    /// Returns a metastore ID only for a metastore authority root.
    #[must_use]
    pub fn metastore_id(&self) -> Option<&str> {
        match &self.root {
            AuthorityRoot::Metastore { metastore_id } => Some(metastore_id),
            _ => None,
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

    /// Whether the supplied dimensions match this durable authority root.
    ///
    /// Tenant must match for every root; workspace matches only for a workspace root and
    /// metastore matches only for a metastore root. For identity, tenant alone
    /// identifies the root. Workspace provenance does not change a metastore key.
    ///
    /// This is not authorization or mutation-family validation. It checks no
    /// principal privileges, workspace bindings, or storage permissions. Domain
    /// services must authorize before acquiring a writer capability, and each
    /// store must restrict which authority families and mutations it supports.
    #[must_use]
    pub fn matches_durable_root(
        &self,
        tenant_id: &str,
        workspace_id: &str,
        metastore_id: &str,
    ) -> bool {
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
    fn root_specific_ids_never_synthesize_a_workspace() {
        let workspace = AuthorityScope::workspace("acme", "notebooks").unwrap();
        assert_eq!(workspace.workspace_id(), Some("notebooks"));
        assert_eq!(workspace.metastore_id(), None);
        let metastore = AuthorityScope::metastore("acme", "lakehouse").unwrap();
        assert_eq!(metastore.workspace_id(), None);
        assert_eq!(metastore.metastore_id(), Some("lakehouse"));
        let identity = AuthorityScope::tenant_identity("acme").unwrap();
        assert_eq!(identity.workspace_id(), None);
        assert_eq!(identity.metastore_id(), None);
    }

    #[test]
    fn equal_textual_ids_do_not_alias_authority_roots() {
        let identity = AuthorityScope::tenant_identity("acme").unwrap();
        let metastore = AuthorityScope::metastore("acme", "acme").unwrap();
        let workspace = AuthorityScope::workspace("acme", "acme").unwrap();
        for (left, right) in [
            (&identity, &workspace),
            (&metastore, &workspace),
            (&identity, &metastore),
        ] {
            assert_ne!(left, right);
            assert_ne!(left.prefix(), right.prefix());
        }
    }

    #[test]
    fn shared_metastore_preserves_distinct_request_provenance() {
        let notebooks = ControlPlaneScope::new("acme", "notebooks", "lakehouse").unwrap();
        let pipelines = ControlPlaneScope::new("acme", "pipelines", "lakehouse").unwrap();
        assert_ne!(notebooks.workspace_id(), pipelines.workspace_id());
        let first = AuthorityScope::from_metastore_scope(&notebooks);
        let second = AuthorityScope::from_metastore_scope(&pipelines);
        assert_eq!(first, second);
        assert_eq!(first.prefix(), second.prefix());
    }

    #[test]
    fn matches_durable_root_enforces_only_the_root_dimensions() {
        let ws = AuthorityScope::workspace("acme", "prod").unwrap();
        assert!(ws.matches_durable_root("acme", "prod", "prod"));
        assert!(!ws.matches_durable_root("acme", "staging", "prod"));
        assert!(!ws.matches_durable_root("globex", "prod", "prod"));

        let ms = AuthorityScope::metastore("acme", "lakehouse").unwrap();
        assert!(ms.matches_durable_root("acme", "notebooks", "lakehouse"));
        assert!(!ms.matches_durable_root("acme", "notebooks", "other-metastore"));
        assert!(!ms.matches_durable_root("globex", "notebooks", "lakehouse"));

        let id = AuthorityScope::tenant_identity("acme").unwrap();
        assert!(id.matches_durable_root("acme", "any-workspace", "any-metastore"));
        assert!(!id.matches_durable_root("globex", "any-workspace", "any-metastore"));
    }

    #[test]
    fn constructors_reject_invalid_ids() {
        assert!(AuthorityScope::tenant_identity("").is_err());
        assert!(AuthorityScope::tenant_identity("../acme").is_err());
        assert!(AuthorityScope::metastore("acme", "").is_err());
        assert!(AuthorityScope::workspace("acme", "prod/../evil").is_err());
    }
}
