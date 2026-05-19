//! Permission compilation from authoritative metastore state.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::{CatalogError, Result};
use crate::identity::memberships::IdentitySnapshot;
use crate::metastore::events::{LifecycleState, PrincipalKind};
use crate::metastore::replay::MetastoreState;

use super::grants::ActiveGrant;
use super::privileges::Privilege;

/// Securable object metadata needed for permission inheritance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurableObject {
    /// Stable object ID.
    pub object_id: String,
    /// Stable object type.
    pub object_type: String,
    /// Optional stable parent object ID.
    pub parent_object_id: Option<String>,
    /// Owner principal ID.
    pub owner_principal_id: String,
}

impl SecurableObject {
    /// Creates a securable object descriptor.
    #[must_use]
    pub fn new(
        object_id: impl Into<String>,
        object_type: impl Into<String>,
        parent_object_id: Option<&str>,
        owner_principal_id: impl Into<String>,
    ) -> Self {
        Self {
            object_id: object_id.into(),
            object_type: object_type.into(),
            parent_object_id: parent_object_id.map(str::to_string),
            owner_principal_id: owner_principal_id.into(),
        }
    }
}

/// Input for permission compilation.
#[derive(Clone, Copy)]
pub struct PermissionCompileInput<'a> {
    /// Replayed authoritative metastore state.
    pub metastore: &'a MetastoreState,
    /// Identity and group expansion snapshot.
    pub identity: &'a IdentitySnapshot,
    /// Securable hierarchy descriptors.
    pub securables: &'a [SecurableObject],
    /// Ledger watermark used for the compiled view.
    pub ledger_watermark: &'a str,
}

/// One compiled permission row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledPermissionRow {
    /// Effective principal ID.
    pub principal_id: String,
    /// Effective object ID.
    pub object_id: String,
    /// Effective object type.
    pub object_type: String,
    /// Effective privilege.
    pub privilege: Privilege,
    /// Source kind, such as `grant` or `owner`.
    pub source: String,
    /// Source grant ID, if row was derived from a grant.
    pub source_grant_id: Option<String>,
    /// Source principal ID.
    pub source_principal_id: String,
    /// Source object ID.
    pub source_object_id: String,
    /// Slash-delimited stable object inheritance path.
    pub inheritance_path: String,
    /// Whether grant option applies.
    pub grant_option: bool,
    /// Group snapshot version used for expansion.
    pub group_snapshot_version: String,
}

/// Compiled permission view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledPermissionSet {
    /// Ledger watermark for the compiled view.
    pub ledger_watermark: String,
    /// Group snapshot version.
    pub group_snapshot_version: String,
    /// Whether this view is fresh enough for enforcement.
    pub fresh: bool,
    /// Compiled permission rows.
    pub rows: Vec<CompiledPermissionRow>,
}

impl CompiledPermissionSet {
    /// Creates a compiled permission set.
    #[must_use]
    pub fn new(
        ledger_watermark: impl Into<String>,
        group_snapshot_version: impl Into<String>,
        fresh: bool,
        rows: Vec<CompiledPermissionRow>,
    ) -> Self {
        Self {
            ledger_watermark: ledger_watermark.into(),
            group_snapshot_version: group_snapshot_version.into(),
            fresh,
            rows,
        }
    }

    /// Iterates rows for principal, object, and privilege.
    pub fn rows_for_principal_object_privilege(
        &self,
        principal_id: &str,
        object_id: &str,
        privilege: Privilege,
    ) -> impl Iterator<Item = &CompiledPermissionRow> {
        self.rows.iter().filter(move |row| {
            row.principal_id == principal_id
                && row.object_id == object_id
                && row.privilege.implies(privilege)
        })
    }
}

/// Compiles grants and ownership into an enforcement view.
///
/// # Errors
///
/// Returns an error if the securable hierarchy references a missing parent.
pub fn compile_permissions(input: PermissionCompileInput<'_>) -> Result<CompiledPermissionSet> {
    let objects = input
        .securables
        .iter()
        .map(|object| (object.object_id.clone(), object.clone()))
        .collect::<BTreeMap<_, _>>();
    validate_hierarchy(&objects)?;

    let mut rows = Vec::new();
    for object in input.securables {
        if principal_is_active(input.metastore, &object.owner_principal_id) {
            rows.push(owner_row(object, input.identity.snapshot_version()));
        }
    }

    for record in input.metastore.grants.values() {
        let Some(grant) = ActiveGrant::from_record(record) else {
            continue;
        };
        if !principal_is_active(input.metastore, &grant.principal_id) {
            continue;
        }
        for object in input.securables {
            if !grant_applies_to_object(&grant, object, &objects) {
                continue;
            }
            let inheritance_path = inheritance_path(&grant.object_id, &object.object_id, &objects);
            for effective_principal in input.identity.effective_members(&grant.principal_id) {
                if !principal_is_active(input.metastore, &effective_principal) {
                    continue;
                }
                rows.push(CompiledPermissionRow {
                    principal_id: effective_principal,
                    object_id: object.object_id.clone(),
                    object_type: object.object_type.clone(),
                    privilege: grant.privilege,
                    source: "grant".to_string(),
                    source_grant_id: Some(grant.grant_id.clone()),
                    source_principal_id: grant.principal_id.clone(),
                    source_object_id: grant.object_id.clone(),
                    inheritance_path: inheritance_path.clone(),
                    grant_option: grant.grant_option,
                    group_snapshot_version: input.identity.snapshot_version().to_string(),
                });
            }
        }
    }

    rows.sort_by(|left, right| {
        (
            &left.principal_id,
            &left.object_id,
            left.privilege,
            &left.source,
        )
            .cmp(&(
                &right.principal_id,
                &right.object_id,
                right.privilege,
                &right.source,
            ))
    });

    Ok(CompiledPermissionSet::new(
        input.ledger_watermark,
        input.identity.snapshot_version(),
        true,
        rows,
    ))
}

fn owner_row(object: &SecurableObject, group_snapshot_version: &str) -> CompiledPermissionRow {
    CompiledPermissionRow {
        principal_id: object.owner_principal_id.clone(),
        object_id: object.object_id.clone(),
        object_type: object.object_type.clone(),
        privilege: Privilege::Manage,
        source: "owner".to_string(),
        source_grant_id: None,
        source_principal_id: object.owner_principal_id.clone(),
        source_object_id: object.object_id.clone(),
        inheritance_path: object.object_id.clone(),
        grant_option: true,
        group_snapshot_version: group_snapshot_version.to_string(),
    }
}

fn principal_is_active(state: &MetastoreState, principal_id: &str) -> bool {
    state.principals.get(principal_id).is_some_and(|principal| {
        principal.lifecycle_state == LifecycleState::Active
            && !matches!(principal.principal_kind, PrincipalKind::ExternalSubject)
    })
}

fn grant_applies_to_object(
    grant: &ActiveGrant,
    object: &SecurableObject,
    objects: &BTreeMap<String, SecurableObject>,
) -> bool {
    grant.object_id == object.object_id
        || ancestors(&object.object_id, objects).contains(&grant.object_id)
}

fn ancestors(object_id: &str, objects: &BTreeMap<String, SecurableObject>) -> BTreeSet<String> {
    let mut ancestor_ids = BTreeSet::new();
    let mut current = objects
        .get(object_id)
        .and_then(|object| object.parent_object_id.as_ref());
    while let Some(parent_id) = current {
        ancestor_ids.insert(parent_id.clone());
        current = objects
            .get(parent_id)
            .and_then(|object| object.parent_object_id.as_ref());
    }
    ancestor_ids
}

fn inheritance_path(
    source_object_id: &str,
    target_object_id: &str,
    objects: &BTreeMap<String, SecurableObject>,
) -> String {
    let mut path = vec![target_object_id.to_string()];
    let mut current = objects
        .get(target_object_id)
        .and_then(|object| object.parent_object_id.as_ref());
    while let Some(parent_id) = current {
        path.push(parent_id.clone());
        if parent_id == source_object_id {
            break;
        }
        current = objects
            .get(parent_id)
            .and_then(|object| object.parent_object_id.as_ref());
    }
    path.reverse();
    path.join("/")
}

fn validate_hierarchy(objects: &BTreeMap<String, SecurableObject>) -> Result<()> {
    for object in objects.values() {
        if let Some(parent_id) = &object.parent_object_id {
            if !objects.contains_key(parent_id) {
                return Err(CatalogError::Validation {
                    message: format!(
                        "securable '{}' references missing parent '{}'",
                        object.object_id, parent_id
                    ),
                });
            }
        }
    }
    Ok(())
}
