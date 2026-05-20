//! Group membership expansion.

use std::collections::{BTreeMap, BTreeSet};

/// Direct group membership edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMembership {
    /// Member principal ID.
    pub member_principal_id: String,
    /// Group principal ID.
    pub group_principal_id: String,
}

impl GroupMembership {
    /// Creates a direct group membership.
    #[must_use]
    pub fn new(
        member_principal_id: impl Into<String>,
        group_principal_id: impl Into<String>,
    ) -> Self {
        Self {
            member_principal_id: member_principal_id.into(),
            group_principal_id: group_principal_id.into(),
        }
    }
}

/// Immutable identity snapshot used for permission compilation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentitySnapshot {
    snapshot_version: String,
    member_to_groups: BTreeMap<String, BTreeSet<String>>,
}

impl IdentitySnapshot {
    /// Creates an identity snapshot.
    #[must_use]
    pub fn new(snapshot_version: impl Into<String>, memberships: Vec<GroupMembership>) -> Self {
        let mut member_to_groups: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for membership in memberships {
            member_to_groups
                .entry(membership.member_principal_id)
                .or_default()
                .insert(membership.group_principal_id);
        }
        Self {
            snapshot_version: snapshot_version.into(),
            member_to_groups,
        }
    }

    /// Returns the snapshot version.
    #[must_use]
    pub fn snapshot_version(&self) -> &str {
        &self.snapshot_version
    }

    /// Returns the effective members of a principal, expanding group grants.
    #[must_use]
    pub fn effective_members(&self, principal_id: &str) -> Vec<String> {
        let mut effective = BTreeSet::from([principal_id.to_string()]);
        for (member_id, groups) in &self.member_to_groups {
            if groups.contains(principal_id)
                || groups
                    .iter()
                    .any(|group_id| group_reaches(group_id, principal_id, &self.member_to_groups))
            {
                effective.insert(member_id.clone());
            }
        }
        effective.into_iter().collect()
    }
}

fn group_reaches(
    start_group_id: &str,
    target_group_id: &str,
    member_to_groups: &BTreeMap<String, BTreeSet<String>>,
) -> bool {
    let mut visited = BTreeSet::new();
    let mut stack = vec![start_group_id.to_string()];
    while let Some(current) = stack.pop() {
        if current == target_group_id {
            return true;
        }
        if !visited.insert(current.clone()) {
            continue;
        }
        if let Some(parent_groups) = member_to_groups.get(&current) {
            stack.extend(parent_groups.iter().cloned());
        }
    }
    false
}
