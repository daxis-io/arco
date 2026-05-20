//! Workspace bindings for governed storage objects.

use crate::metastore::events::LifecycleState;

/// Workspace binding for a governed object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceBinding {
    /// Stable binding ID.
    pub binding_id: String,
    /// Workspace ID.
    pub workspace_id: String,
    /// Bound object ID.
    pub object_id: String,
    /// Bound object type.
    pub object_type: String,
    /// Owner principal ID.
    pub owner: String,
    /// Lifecycle state.
    pub lifecycle_state: LifecycleState,
}

impl WorkspaceBinding {
    /// Creates an active workspace binding.
    #[must_use]
    pub fn new(
        binding_id: impl Into<String>,
        workspace_id: impl Into<String>,
        object_id: impl Into<String>,
        object_type: impl Into<String>,
        owner: impl Into<String>,
    ) -> Self {
        Self {
            binding_id: binding_id.into(),
            workspace_id: workspace_id.into(),
            object_id: object_id.into(),
            object_type: object_type.into(),
            owner: owner.into(),
            lifecycle_state: LifecycleState::Active,
        }
    }
}
