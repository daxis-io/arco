//! Phase 7D safe transaction-handle catalog projection contracts.

// Test-target lint scope (#331): tests and their helpers signal failure by
// panicking. clippy.toml scopes the restriction lints out of #[test] fns;
// this header extends the same policy to this file's shared helpers.
#![allow(clippy::expect_used)]
// Advisory lint scope for test code (#331): the pedantic/nursery lints below
// conflict with test ergonomics here; production code keeps them active.
#![allow(clippy::too_many_lines)]

use chrono::{TimeZone as _, Utc};

use arco_catalog::parquet_util::{
    TransactionHandleCatalogRecord, transaction_handle_schema, write_transaction_handles,
};
use arco_core::control_plane_transactions::ControlPlaneHandleStatus;

const HANDLE_A: &str = "hdl_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const HANDLE_B: &str = "hdl_01ARZ3NDEKTSV4RRFFQ69G5FAW";

fn ts(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).single().expect("timestamp")
}

#[allow(clippy::too_many_arguments)]
fn row(
    handle_id: &str,
    lifecycle: ControlPlaneHandleStatus,
    prepared_at: Option<i64>,
    committing_at: Option<i64>,
    visible_at: Option<i64>,
    terminal_at: Option<i64>,
    mutation_count: usize,
    visible_mutation_count: usize,
) -> arco_catalog::Result<TransactionHandleCatalogRecord> {
    TransactionHandleCatalogRecord::new(
        handle_id,
        1,
        lifecycle,
        ts(100),
        ts(150),
        ts(200),
        prepared_at.map(ts),
        committing_at.map(ts),
        visible_at.map(ts),
        terminal_at.map(ts),
        mutation_count,
        visible_mutation_count,
    )
}

#[test]
fn transaction_handle_projection_has_the_exact_safe_schema_and_canonical_bytes() {
    let schema = transaction_handle_schema();
    let fields = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        fields,
        vec![
            "handle_id",
            "record_version",
            "lifecycle",
            "created_at",
            "updated_at",
            "expires_at",
            "prepared_at",
            "committing_at",
            "visible_at",
            "terminal_at",
            "mutation_count",
            "visible_mutation_count",
        ]
    );

    for forbidden in [
        "review_token",
        "review_token_verifier",
        "actor",
        "request_id",
        "idempotency_key",
        "mutation_kind",
        "mutation_payload",
        "staged_path",
        "digest",
        "transaction_id",
        "receipt",
        "manifest",
        "read_token",
        "failure_detail",
        "provider_uri",
        "storage_root",
        "credential",
    ] {
        assert!(!fields.contains(&forbidden), "must omit {forbidden}");
    }

    let first = row(
        HANDLE_A,
        ControlPlaneHandleStatus::Open,
        None,
        None,
        None,
        None,
        1,
        0,
    )
    .expect("first safe handle row");
    let second = row(
        HANDLE_B,
        ControlPlaneHandleStatus::Visible,
        Some(110),
        Some(120),
        Some(140),
        None,
        2,
        2,
    )
    .expect("second safe handle row");

    let forward =
        write_transaction_handles(&[first.clone(), second.clone()]).expect("write forward rows");
    let reverse = write_transaction_handles(&[second, first]).expect("write reverse rows");
    assert_eq!(forward, reverse, "row order must be canonical by handle ID");

    let duplicate = row(
        HANDLE_A,
        ControlPlaneHandleStatus::Open,
        None,
        None,
        None,
        None,
        1,
        0,
    )
    .expect("duplicate safe handle row");
    assert!(
        write_transaction_handles(&[duplicate.clone(), duplicate]).is_err(),
        "duplicate handle rows are ambiguous"
    );
}

#[test]
fn transaction_handle_projection_accepts_every_safe_lifecycle_shape() {
    let valid = [
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::Open,
            None,
            None,
            None,
            None,
            0,
            0,
        ),
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::Preparing,
            None,
            None,
            None,
            None,
            1,
            0,
        ),
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::Prepared,
            Some(110),
            None,
            None,
            None,
            1,
            0,
        ),
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::Committing,
            Some(110),
            Some(120),
            None,
            None,
            2,
            1,
        ),
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::Visible,
            Some(110),
            Some(120),
            Some(140),
            None,
            2,
            2,
        ),
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::RepairRequired,
            Some(110),
            Some(120),
            None,
            None,
            2,
            1,
        ),
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::Aborted,
            None,
            None,
            None,
            Some(140),
            1,
            0,
        ),
    ];
    assert!(valid.into_iter().all(|candidate| candidate.is_ok()));

    let expired = TransactionHandleCatalogRecord::new(
        HANDLE_A,
        1,
        ControlPlaneHandleStatus::Expired,
        ts(100),
        ts(210),
        ts(200),
        None,
        None,
        None,
        Some(ts(205)),
        1,
        0,
    );
    assert!(expired.is_ok());
}

#[test]
fn transaction_handle_projection_rejects_malformed_ids_and_versions() {
    for malformed in [
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "hdl_01arz3ndektsv4rrffq69g5fav",
        "hdl_01ARZ3NDEKTSV4RRFFQ69G5FA",
        "hdl_01ARZ3NDEKTSV4RRFFQ69G5FAI",
    ] {
        assert!(
            row(
                malformed,
                ControlPlaneHandleStatus::Open,
                None,
                None,
                None,
                None,
                0,
                0,
            )
            .is_err(),
            "must reject {malformed}"
        );
    }

    assert!(
        TransactionHandleCatalogRecord::new(
            HANDLE_A,
            2,
            ControlPlaneHandleStatus::Open,
            ts(100),
            ts(100),
            ts(200),
            None,
            None,
            None,
            None,
            0,
            0,
        )
        .is_err()
    );
}

#[test]
fn transaction_handle_projection_rejects_inconsistent_timestamp_evidence() {
    let build = |updated_at: i64,
                 expires_at: i64,
                 prepared_at: Option<i64>,
                 committing_at: Option<i64>,
                 visible_at: Option<i64>,
                 terminal_at: Option<i64>| {
        TransactionHandleCatalogRecord::new(
            HANDLE_A,
            1,
            ControlPlaneHandleStatus::Visible,
            ts(100),
            ts(updated_at),
            ts(expires_at),
            prepared_at.map(ts),
            committing_at.map(ts),
            visible_at.map(ts),
            terminal_at.map(ts),
            1,
            1,
        )
    };

    assert!(build(99, 200, Some(110), Some(120), Some(140), None).is_err());
    assert!(build(150, 100, Some(110), Some(120), Some(140), None).is_err());
    assert!(build(150, 200, Some(160), Some(170), Some(180), None).is_err());
    assert!(build(150, 200, Some(130), Some(120), Some(140), None).is_err());
    assert!(build(150, 200, Some(110), Some(140), Some(130), None).is_err());
    assert!(build(150, 200, Some(110), Some(120), Some(140), Some(145)).is_err());

    let expired_too_early = TransactionHandleCatalogRecord::new(
        HANDLE_A,
        1,
        ControlPlaneHandleStatus::Expired,
        ts(100),
        ts(210),
        ts(200),
        None,
        None,
        None,
        Some(ts(190)),
        0,
        0,
    );
    assert!(expired_too_early.is_err());

    let prepared_at_expiry = TransactionHandleCatalogRecord::new(
        HANDLE_A,
        1,
        ControlPlaneHandleStatus::Prepared,
        ts(100),
        ts(200),
        ts(200),
        Some(ts(200)),
        None,
        None,
        None,
        1,
        0,
    );
    assert!(prepared_at_expiry.is_err());

    let committing_at_expiry = TransactionHandleCatalogRecord::new(
        HANDLE_A,
        1,
        ControlPlaneHandleStatus::Committing,
        ts(100),
        ts(200),
        ts(200),
        Some(ts(150)),
        Some(ts(200)),
        None,
        None,
        1,
        0,
    );
    assert!(committing_at_expiry.is_err());

    let aborted_at_expiry = TransactionHandleCatalogRecord::new(
        HANDLE_A,
        1,
        ControlPlaneHandleStatus::Aborted,
        ts(100),
        ts(200),
        ts(200),
        Some(ts(150)),
        None,
        None,
        Some(ts(200)),
        1,
        0,
    );
    assert!(aborted_at_expiry.is_err());
}

#[test]
fn transaction_handle_projection_rejects_lifecycle_and_count_mismatches() {
    assert!(
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::Preparing,
            None,
            None,
            None,
            None,
            0,
            0,
        )
        .is_err()
    );
    assert!(
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::Prepared,
            None,
            None,
            None,
            None,
            1,
            0,
        )
        .is_err()
    );
    assert!(
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::Committing,
            Some(110),
            None,
            None,
            None,
            1,
            0,
        )
        .is_err()
    );
    assert!(
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::Visible,
            Some(110),
            Some(120),
            Some(140),
            None,
            2,
            1,
        )
        .is_err()
    );
    assert!(
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::Aborted,
            None,
            None,
            None,
            None,
            1,
            0,
        )
        .is_err()
    );
    assert!(
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::Expired,
            None,
            None,
            None,
            Some(140),
            1,
            1,
        )
        .is_err()
    );
    assert!(
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::RepairRequired,
            Some(110),
            Some(120),
            None,
            None,
            1,
            2,
        )
        .is_err()
    );
    assert!(
        row(
            HANDLE_A,
            ControlPlaneHandleStatus::Open,
            None,
            None,
            None,
            None,
            usize::MAX,
            0,
        )
        .is_err()
    );
}
