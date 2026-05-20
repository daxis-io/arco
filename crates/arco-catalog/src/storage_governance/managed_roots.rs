//! Managed storage root metadata.

use crate::error::Result;
use crate::metastore::events::LifecycleState;

use super::path_normalization::GovernedPath;

/// Governed managed storage root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedRoot {
    /// Stable root ID.
    pub root_id: String,
    /// Root name.
    pub name: String,
    /// Bound workspace ID.
    pub workspace_id: String,
    /// Canonical path authority.
    pub path: GovernedPath,
    /// Owner principal ID.
    pub owner: String,
    /// Lifecycle state.
    pub lifecycle_state: LifecycleState,
}

impl ManagedRoot {
    /// Creates an active managed root.
    ///
    /// # Errors
    ///
    /// Returns an error when the URL cannot be canonicalized.
    pub fn new(
        root_id: impl Into<String>,
        name: impl Into<String>,
        workspace_id: impl Into<String>,
        url: &str,
        owner: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            root_id: root_id.into(),
            name: name.into(),
            workspace_id: workspace_id.into(),
            path: GovernedPath::parse(url)?,
            owner: owner.into(),
            lifecycle_state: LifecycleState::Active,
        })
    }
}
