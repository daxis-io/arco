//! Contract tests for typed Delta path derivation.

use arco_core::flow_paths::DeltaPaths;
use uuid::Uuid;

#[test]
fn builds_delta_control_plane_paths() {
    let table_id = Uuid::nil();
    let paths = DeltaPaths::new(table_id, "warehouse/sales/orders").unwrap();

    assert_eq!(
        paths.staging_payload("01HZZ00000000000000000000"),
        format!("delta/staging/{table_id}/01HZZ00000000000000000000.json")
    );
    assert_eq!(
        paths.coordinator_state(),
        format!("delta/coordinator/{table_id}.json")
    );
    assert_eq!(
        paths.idempotency_marker_from_hash("abcdef0123456789"),
        format!("delta/idempotency/{table_id}/ab/abcdef0123456789.json")
    );
}

#[test]
fn derives_delta_log_under_table_root() {
    let paths = DeltaPaths::new(Uuid::nil(), "warehouse/sales/orders").unwrap();
    assert_eq!(
        paths.delta_log_json(42).unwrap(),
        "warehouse/sales/orders/_delta_log/00000000000000000042.json"
    );
}

#[test]
fn resolves_location_uri_to_scope_relative_root() {
    let table_id = Uuid::nil();
    let paths = DeltaPaths::from_table_location(
        table_id,
        Some("gs://lake/tenant=acme/workspace=analytics/warehouse/sales/orders/"),
        "acme",
        "analytics",
    )
    .unwrap();
    assert_eq!(paths.table_root(), "warehouse/sales/orders");
}

#[test]
fn falls_back_to_legacy_root_when_location_is_missing() {
    let table_id = Uuid::nil();
    let paths = DeltaPaths::from_table_location(table_id, None, "acme", "analytics").unwrap();
    assert_eq!(paths.table_root(), format!("tables/{table_id}"));
}

#[test]
fn rejects_negative_delta_log_versions() {
    let paths = DeltaPaths::new(Uuid::nil(), "warehouse/sales/orders").unwrap();
    assert!(paths.delta_log_json(-1).is_err());
}

#[test]
fn rejects_invalid_table_location_paths() {
    let table_id = Uuid::nil();

    assert!(
        DeltaPaths::from_table_location(table_id, Some("../escape"), "acme", "analytics").is_err()
    );
    assert!(
        DeltaPaths::from_table_location(
            table_id,
            Some("warehouse/%2e%2e/escape"),
            "acme",
            "analytics"
        )
        .is_err()
    );
}
