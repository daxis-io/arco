//! Contract tests for canonical flow/orchestration path builders.

use base64::Engine;
use chrono::NaiveDate;
use uuid::Uuid;

use arco_core::flow_paths::{ApiPaths, FlowPaths, IcebergPaths};

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
