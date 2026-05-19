//! External location metadata.

use crate::error::Result;
use crate::metastore::events::LifecycleState;

use super::path_normalization::GovernedPath;

/// Governed external location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalLocation {
    /// Stable location ID.
    pub location_id: String,
    /// Location name.
    pub name: String,
    /// Canonical path authority.
    pub path: GovernedPath,
    /// Storage credential ID.
    pub credential_id: String,
    /// Owner principal ID.
    pub owner: String,
    /// Lifecycle state.
    pub lifecycle_state: LifecycleState,
}

impl ExternalLocation {
    /// Creates an active external location.
    ///
    /// # Errors
    ///
    /// Returns an error when the URL cannot be canonicalized.
    pub fn new(
        location_id: impl Into<String>,
        name: impl Into<String>,
        url: &str,
        credential_id: impl Into<String>,
        owner: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            location_id: location_id.into(),
            name: name.into(),
            path: GovernedPath::parse(url)?,
            credential_id: credential_id.into(),
            owner: owner.into(),
            lifecycle_state: LifecycleState::Active,
        })
    }
}
