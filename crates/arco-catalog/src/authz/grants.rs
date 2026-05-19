//! Grant helpers.

use crate::metastore::events::{GrantRecord, LifecycleState};

use super::privileges::Privilege;

/// Parsed active grant used by the permission compiler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveGrant {
    /// Stable grant ID.
    pub grant_id: String,
    /// Stable object ID.
    pub object_id: String,
    /// Object type.
    pub object_type: String,
    /// Stable principal ID named by the grant.
    pub principal_id: String,
    /// Granted privilege.
    pub privilege: Privilege,
    /// Whether the grantee can grant this privilege.
    pub grant_option: bool,
}

impl ActiveGrant {
    /// Parses an active grant record.
    #[must_use]
    pub fn from_record(record: &GrantRecord) -> Option<Self> {
        if record.lifecycle_state != LifecycleState::Active {
            return None;
        }
        let privilege = record.privilege.parse().ok()?;
        let grant_option = record
            .properties
            .get("grant_option")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        Some(Self {
            grant_id: record.grant_id.clone(),
            object_id: record.object_id.clone(),
            object_type: record.object_type.clone(),
            principal_id: record.principal_id.clone(),
            privilege,
            grant_option,
        })
    }
}
