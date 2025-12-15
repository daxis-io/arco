//! Multi-tenant isolation primitives.
//!
//! Tenant isolation is enforced at multiple levels in Arco:
//! - **Storage layout**: Each tenant's data is stored under a unique prefix
//! - **Service boundaries**: API requests are scoped to a single tenant
//! - **Query isolation**: All queries are automatically filtered by tenant
//!
//! # Example
//!
//! ```rust
//! use arco_core::tenant::TenantId;
//!
//! let tenant = TenantId::new("acme-corp").unwrap();
//! assert_eq!(tenant.storage_prefix(), "tenant=acme-corp/");
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::error::{Error, Result};

/// A unique identifier for a tenant.
///
/// Tenant IDs must be:
/// - Non-empty
/// - Lowercase alphanumeric with hyphens
/// - Between 3 and 63 characters (compatible with DNS/bucket naming)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantId(String);

impl TenantId {
    /// Creates a new tenant ID after validating the format.
    ///
    /// # Errors
    ///
    /// Returns an error if the tenant ID is invalid.
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        Self::validate(&id)?;
        Ok(Self(id))
    }

    /// Creates a tenant ID without validation.
    ///
    /// # Safety
    ///
    /// The caller must ensure the ID is valid. This is intended for
    /// use with IDs that have already been validated (e.g., from storage).
    #[must_use]
    pub fn new_unchecked(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the storage prefix for this tenant.
    ///
    /// Returns `tenant={tenant_id}/` - the key=value format matching [`ScopedStorage`](crate::ScopedStorage).
    /// This format provides operational ergonomics (grep-friendly, self-documenting paths).
    ///
    /// # Example
    ///
    /// ```rust
    /// use arco_core::TenantId;
    ///
    /// let tenant = TenantId::new("acme-corp").unwrap();
    /// assert_eq!(tenant.storage_prefix(), "tenant=acme-corp/");
    /// ```
    #[must_use]
    pub fn storage_prefix(&self) -> String {
        format!("tenant={}/", self.0)
    }

    /// Returns the tenant ID as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validates a tenant ID string.
    fn validate(id: &str) -> Result<()> {
        if id.is_empty() {
            return Err(Error::InvalidId {
                message: "tenant ID cannot be empty".to_string(),
            });
        }

        if id.len() < 3 {
            return Err(Error::InvalidId {
                message: format!("tenant ID '{id}' is too short (minimum 3 characters)"),
            });
        }

        if id.len() > 63 {
            return Err(Error::InvalidId {
                message: format!("tenant ID '{id}' is too long (maximum 63 characters)"),
            });
        }

        if !id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(Error::InvalidId {
                message: format!(
                    "tenant ID '{id}' contains invalid characters (only lowercase letters, digits, and hyphens allowed)"
                ),
            });
        }

        if id.starts_with('-') || id.ends_with('-') {
            return Err(Error::InvalidId {
                message: format!("tenant ID '{id}' cannot start or end with a hyphen"),
            });
        }

        Ok(())
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for TenantId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_tenant_ids() {
        assert!(TenantId::new("acme-corp").is_ok());
        assert!(TenantId::new("tenant123").is_ok());
        assert!(TenantId::new("my-cool-tenant").is_ok());
        assert!(TenantId::new("abc").is_ok());
    }

    #[test]
    fn invalid_tenant_ids() {
        assert!(TenantId::new("").is_err());
        assert!(TenantId::new("ab").is_err());
        assert!(TenantId::new("UPPERCASE").is_err());
        assert!(TenantId::new("-starts-with-hyphen").is_err());
        assert!(TenantId::new("ends-with-hyphen-").is_err());
        assert!(TenantId::new("has spaces").is_err());
        assert!(TenantId::new("has_underscore").is_err());
    }

    #[test]
    fn storage_prefix() {
        let tenant = TenantId::new("acme-corp").unwrap();
        // Aligns with ScopedStorage: tenant={tenant}/workspace={workspace}/... pattern
        assert_eq!(tenant.storage_prefix(), "tenant=acme-corp/");
    }
}
