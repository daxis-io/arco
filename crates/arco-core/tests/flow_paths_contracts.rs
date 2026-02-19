//! Contract tests for canonical flow/orchestration and Delta path builders.

use base64::Engine;
use chrono::NaiveDate;
use uuid::Uuid;

use arco_core::flow_paths::{ApiPaths, DeltaPaths, FlowPaths, IcebergPaths};

#[test]
fn orchestration_and_flow_paths_are_stable() {
    assert_eq!(
        FlowPaths::orchestration_event_path("2026-02-12", "01ABC"),
        "ledger/orchestration/2026-02-12/01ABC.json"
    );
    assert_eq!(
        FlowPaths::flow_event_path("executions", "2026-02-12", "01ABC"),
        "ledger/flow/executions/2026-02-12/01ABC.json"
    );
    assert_eq!(
        FlowPaths::orchestration_manifest_path(),
        "state/orchestration/manifest.json"
    );
    assert_eq!(
        FlowPaths::orchestration_l0_dir("delta-001"),
        "state/orchestration/l0/delta-001"
    );
}

#[test]
fn api_paths_are_stable() {
    assert_eq!(
        ApiPaths::manifest_path("manifest-01"),
        "manifests/manifest-01.json"
    );
    assert_eq!(
        ApiPaths::manifest_latest_index_path(),
        "manifests/_index.json"
    );

    let key = "req:tenant/workspace";
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.as_bytes());

    assert_eq!(
        ApiPaths::manifest_idempotency_path(key),
        format!("manifests/idempotency/{encoded}.json")
    );
    assert_eq!(
        ApiPaths::backfill_idempotency_path(key),
        format!("orchestration/backfills/idempotency/{encoded}.json")
    );
}

#[test]
fn iceberg_paths_are_stable() {
    let table_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("valid");
    let date = NaiveDate::from_ymd_opt(2026, 2, 12).expect("valid date");

    assert_eq!(
        IcebergPaths::pointer_path(&table_uuid),
        "_catalog/iceberg_pointers/550e8400-e29b-41d4-a716-446655440000.json"
    );
    assert_eq!(
        IcebergPaths::idempotency_table_prefix(&table_uuid),
        "_catalog/iceberg_idempotency/550e8400-e29b-41d4-a716-446655440000/"
    );
    assert_eq!(
        IcebergPaths::idempotency_marker_path(&table_uuid, "abcdef1234"),
        "_catalog/iceberg_idempotency/550e8400-e29b-41d4-a716-446655440000/ab/abcdef1234.json"
    );
    assert_eq!(
        IcebergPaths::transaction_record_path("tx-123"),
        "_catalog/iceberg_transactions/tx-123.json"
    );
    assert_eq!(
        IcebergPaths::pending_receipt_prefix(date),
        "events/2026-02-12/iceberg/pending/"
    );
    assert_eq!(
        IcebergPaths::pending_receipt_path(date, "commit-001"),
        "events/2026-02-12/iceberg/pending/commit-001.json"
    );
    assert_eq!(
        IcebergPaths::committed_receipt_prefix(date),
        "events/2026-02-12/iceberg/committed/"
    );
    assert_eq!(
        IcebergPaths::committed_receipt_path(date, "commit-001"),
        "events/2026-02-12/iceberg/committed/commit-001.json"
    );
}

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
