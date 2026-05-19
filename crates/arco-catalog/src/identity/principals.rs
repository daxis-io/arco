//! Principal helpers.

use crate::metastore::events::{LifecycleState, PrincipalRecord};

/// Returns true when a principal is active for authorization.
#[must_use]
pub fn is_active(principal: &PrincipalRecord) -> bool {
    principal.lifecycle_state == LifecycleState::Active
}
