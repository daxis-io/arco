//! Task 3 coverage for compiled catalog permissions.

use std::collections::BTreeMap;

use arco_catalog::authz::compiler::{PermissionCompileInput, SecurableObject, compile_permissions};
use arco_catalog::authz::privileges::Privilege;
use arco_catalog::identity::memberships::{GroupMembership, IdentitySnapshot};
use arco_catalog::metastore::events::{
    GrantRecord, LifecycleState, MetastoreEvent, MetastoreMutation, PrincipalKind, PrincipalRecord,
};
use arco_catalog::metastore::replay::replay_events;

#[test]
fn permission_compiler_expands_transitive_groups_and_inherited_grants() {
    let state = replay_events(events().iter()).expect("metastore replay");
    let identity = IdentitySnapshot::new(
        "groups-rev-7",
        vec![
            GroupMembership::new("user_alice", "group_analysts"),
            GroupMembership::new("group_analysts", "group_data"),
        ],
    );
    let hierarchy = vec![
        SecurableObject::new("catalog_sales", "CATALOG", None, "owner_sales"),
        SecurableObject::new(
            "schema_retail",
            "SCHEMA",
            Some("catalog_sales"),
            "owner_sales",
        ),
        SecurableObject::new(
            "table_orders",
            "TABLE",
            Some("schema_retail"),
            "owner_sales",
        ),
    ];

    let compiled = compile_permissions(PermissionCompileInput {
        metastore: &state,
        identity: &identity,
        securables: &hierarchy,
        ledger_watermark: "event_004",
    })
    .expect("compile permissions");

    let alice_table_select = compiled
        .rows_for_principal_object_privilege("user_alice", "table_orders", "TABLE", Privilege::Select)
        .collect::<Vec<_>>();
    assert_eq!(alice_table_select.len(), 1);
    assert_eq!(alice_table_select[0].source_object_id, "catalog_sales");
    assert_eq!(alice_table_select[0].source_principal_id, "group_data");
    assert_eq!(
        alice_table_select[0].inheritance_path,
        "catalog_sales/schema_retail/table_orders"
    );
    assert_eq!(alice_table_select[0].group_snapshot_version, "groups-rev-7");
}

#[test]
fn permission_compiler_treats_owners_as_manage_grant_sources() {
    let state = replay_events(events().iter()).expect("metastore replay");
    let identity = IdentitySnapshot::new("groups-rev-8", Vec::new());
    let hierarchy = vec![SecurableObject::new(
        "table_orders",
        "TABLE",
        None,
        "owner_sales",
    )];

    let compiled = compile_permissions(PermissionCompileInput {
        metastore: &state,
        identity: &identity,
        securables: &hierarchy,
        ledger_watermark: "event_004",
    })
    .expect("compile permissions");

    let owner_manage = compiled
        .rows_for_principal_object_privilege("owner_sales", "table_orders", "TABLE", Privilege::Manage)
        .collect::<Vec<_>>();
    assert_eq!(owner_manage.len(), 1);
    assert_eq!(owner_manage[0].source, "owner");
    assert!(owner_manage[0].grant_option);
}

#[test]
fn permission_compiler_omits_deleted_grants_and_disabled_principals() {
    let state = replay_events(events().iter()).expect("metastore replay");
    let identity = IdentitySnapshot::new("groups-rev-9", Vec::new());
    let hierarchy = vec![SecurableObject::new(
        "table_orders",
        "TABLE",
        None,
        "owner_sales",
    )];

    let compiled = compile_permissions(PermissionCompileInput {
        metastore: &state,
        identity: &identity,
        securables: &hierarchy,
        ledger_watermark: "event_004",
    })
    .expect("compile permissions");

    assert_eq!(
        compiled
            .rows_for_principal_object_privilege(
                "user_disabled",
                "table_orders",
                "TABLE",
                Privilege::Select,
            )
            .count(),
        0
    );
    assert_eq!(
        compiled
            .rows_for_principal_object_privilege(
                "user_revoked",
                "table_orders",
                "TABLE",
                Privilege::Select,
            )
            .count(),
        0
    );
}

#[test]
fn permission_compiler_omits_inactive_effective_members_and_inactive_owners() {
    let state = replay_events(events().iter()).expect("metastore replay");
    let identity = IdentitySnapshot::new(
        "groups-rev-10",
        vec![GroupMembership::new("user_disabled", "group_data")],
    );
    let hierarchy = vec![SecurableObject::new(
        "table_orders",
        "TABLE",
        None,
        "user_disabled",
    )];

    let compiled = compile_permissions(PermissionCompileInput {
        metastore: &state,
        identity: &identity,
        securables: &hierarchy,
        ledger_watermark: "event_004",
    })
    .expect("compile permissions");

    assert_eq!(
        compiled
            .rows_for_principal_object_privilege(
                "user_disabled",
                "table_orders",
                "TABLE",
                Privilege::Select,
            )
            .count(),
        0
    );
    assert_eq!(
        compiled
            .rows_for_principal_object_privilege(
                "user_disabled",
                "table_orders",
                "TABLE",
                Privilege::Manage,
            )
            .count(),
        0
    );
}

fn events() -> Vec<MetastoreEvent> {
    vec![
        principal(
            "event_001",
            1,
            "user_alice",
            "alice@example.com",
            PrincipalKind::User,
            LifecycleState::Active,
        ),
        principal(
            "event_002",
            2,
            "group_analysts",
            "analysts",
            PrincipalKind::Group,
            LifecycleState::Active,
        ),
        principal(
            "event_003",
            3,
            "group_data",
            "data",
            PrincipalKind::Group,
            LifecycleState::Active,
        ),
        principal(
            "event_004",
            4,
            "owner_sales",
            "owner-sales",
            PrincipalKind::User,
            LifecycleState::Active,
        ),
        principal(
            "event_005",
            5,
            "user_disabled",
            "disabled@example.com",
            PrincipalKind::User,
            LifecycleState::Disabled,
        ),
        principal(
            "event_006",
            6,
            "user_revoked",
            "revoked@example.com",
            PrincipalKind::User,
            LifecycleState::Active,
        ),
        grant(
            "event_007",
            7,
            "grant_catalog_select",
            "catalog_sales",
            "CATALOG",
            "group_data",
            "SELECT",
            LifecycleState::Active,
        ),
        grant(
            "event_008",
            8,
            "grant_disabled",
            "table_orders",
            "TABLE",
            "user_disabled",
            "SELECT",
            LifecycleState::Active,
        ),
        grant(
            "event_009",
            9,
            "grant_deleted",
            "table_orders",
            "TABLE",
            "user_revoked",
            "SELECT",
            LifecycleState::Deleted,
        ),
    ]
}

fn principal(
    event_id: &str,
    sequence: u64,
    principal_id: &str,
    name: &str,
    principal_kind: PrincipalKind,
    lifecycle_state: LifecycleState,
) -> MetastoreEvent {
    MetastoreEvent::new(
        event_id,
        sequence,
        MetastoreMutation::PrincipalUpserted(PrincipalRecord {
            principal_id: principal_id.to_string(),
            name: name.to_string(),
            principal_kind,
            owner: "metastore-admin".to_string(),
            lifecycle_state,
            updated_at_ms: sequence as i64,
            properties: BTreeMap::new(),
        }),
    )
}

fn grant(
    event_id: &str,
    sequence: u64,
    grant_id: &str,
    object_id: &str,
    object_type: &str,
    principal_id: &str,
    privilege: &str,
    lifecycle_state: LifecycleState,
) -> MetastoreEvent {
    MetastoreEvent::new(
        event_id,
        sequence,
        MetastoreMutation::GrantUpserted(GrantRecord {
            grant_id: grant_id.to_string(),
            object_id: object_id.to_string(),
            object_type: object_type.to_string(),
            principal_id: principal_id.to_string(),
            privilege: privilege.to_string(),
            owner: "metastore-admin".to_string(),
            lifecycle_state,
            updated_at_ms: sequence as i64,
            properties: BTreeMap::new(),
        }),
    )
}
