//! Group identity helpers.

use crate::metastore::events::{PrincipalKind, PrincipalRecord};

/// Returns true when a principal record is a group.
#[must_use]
pub fn is_group(principal: &PrincipalRecord) -> bool {
    principal.principal_kind == PrincipalKind::Group
}
