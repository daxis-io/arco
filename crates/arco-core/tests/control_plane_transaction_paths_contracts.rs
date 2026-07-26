//! Contract tests for canonical control-plane transaction paths and record shapes.

#![allow(clippy::expect_used)]

use chrono::{TimeZone, Utc};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;

use arco_core::control_plane_transactions::{
    CONTROL_PLANE_HANDLE_RECORD_TYPE, CONTROL_PLANE_HANDLE_RECORD_VERSION, CatalogTxReceipt,
    ControlPlaneHandleFailureCategory, ControlPlaneHandleMutationRef,
    ControlPlaneHandleParticipant, ControlPlaneHandleRecord, ControlPlaneHandleStatus,
    ControlPlaneIdempotencyRecord, ControlPlaneTxDomain, ControlPlaneTxKind, ControlPlaneTxPaths,
    ControlPlaneTxRecord, ControlPlaneTxStatus, DomainCommit, RootTxManifest, RootTxManifestDomain,
    RootTxReceipt, validate_handle_id,
};

const HANDLE_ID: &str = "hdl_00000000000000000000000000";

#[test]
fn handle_ids_and_paths_are_canonical_and_path_safe() {
    assert!(validate_handle_id(HANDLE_ID).is_ok());
    assert_eq!(
        ControlPlaneTxPaths::handle_record(HANDLE_ID).expect("valid handle record path"),
        "transactions/handles/hdl_00000000000000000000000000/handle.json"
    );
    assert_eq!(
        ControlPlaneTxPaths::handle_mutation(HANDLE_ID, 1).expect("valid mutation path"),
        "transactions/handles/hdl_00000000000000000000000000/mutations/00000000000000000001.json"
    );
    assert_eq!(
        ControlPlaneTxPaths::handle_mutation(HANDLE_ID, u64::MAX)
            .expect("maximum ordinal remains canonical"),
        "transactions/handles/hdl_00000000000000000000000000/mutations/18446744073709551615.json"
    );
    assert_eq!(
        ControlPlaneTxPaths::handle_identity_authority(HANDLE_ID, 1)
            .expect("valid identity authority path"),
        "transactions/handles/hdl_00000000000000000000000000/identities/00000000000000000001.json"
    );

    for malformed in [
        "00000000000000000000000000",
        "hdl_0000000000000000000000000",
        "hdl_000000000000000000000000000",
        "hdl_0000000000000000000000000o",
        "hdl_0000000000000000000000000a",
        "hdl_../../000000000000000000000",
    ] {
        assert!(
            validate_handle_id(malformed).is_err(),
            "accepted malformed handle id {malformed}"
        );
        assert!(ControlPlaneTxPaths::handle_record(malformed).is_err());
        assert!(ControlPlaneTxPaths::handle_identity_authority(malformed, 1).is_err());
    }
    assert!(ControlPlaneTxPaths::handle_mutation(HANDLE_ID, 0).is_err());
    assert!(ControlPlaneTxPaths::handle_identity_authority(HANDLE_ID, 0).is_err());
}

fn open_handle_wire() -> serde_json::Value {
    serde_json::json!({
        "record_type": "control_plane_transaction_handle",
        "version": 1,
        "handle_id": HANDLE_ID,
        "scope": {
            "tenant_id": "tenant-a",
            "workspace_id": "workspace-a"
        },
        "revision": 1,
        "status": "OPEN",
        "created_at": "2026-07-16T12:00:00Z",
        "updated_at": "2026-07-16T12:00:00Z",
        "expires_at": "2026-07-16T13:00:00Z",
        "mutation_refs": [],
        "review_token_verifier": concat!(
            "sha256:",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ),
        "participants": []
    })
}

#[test]
fn handle_v1_records_accept_additive_fields_and_reject_corrupt_wire() {
    let mut wire = open_handle_wire();
    wire["future_v1_field"] = serde_json::json!({"safe": true});
    let encoded = arco_core::canonical_json::to_canonical_bytes(&wire)
        .expect("encode canonical additive fixture");
    let record = ControlPlaneHandleRecord::from_json_slice(&encoded).expect("decode v1 record");
    assert_eq!(record.record_type, CONTROL_PLANE_HANDLE_RECORD_TYPE);
    assert_eq!(record.version, CONTROL_PLANE_HANDLE_RECORD_VERSION);
    assert_eq!(record.handle_id, HANDLE_ID);

    let round_trip = record.to_json_vec().expect("encode validated record");
    let decoded = ControlPlaneHandleRecord::from_json_slice(&round_trip).expect("round trip v1");
    assert_eq!(decoded, record);

    for (field, corrupt) in [
        ("record_type", serde_json::json!("workspace_snapshot")),
        ("version", serde_json::json!(2)),
        ("status", serde_json::json!("UNKNOWN")),
        ("created_at", serde_json::json!("not-a-timestamp")),
        ("handle_id", serde_json::json!("hdl_../../escape")),
        ("revision", serde_json::json!(0)),
        ("review_token_verifier", serde_json::json!("sha256:ABC")),
    ] {
        let mut corrupt_wire = open_handle_wire();
        corrupt_wire[field] = corrupt;
        assert!(
            ControlPlaneHandleRecord::from_json_slice(
                &arco_core::canonical_json::to_canonical_bytes(&corrupt_wire)
                    .expect("encode canonical corrupt fixture")
            )
            .is_err(),
            "accepted corrupt {field}"
        );
    }
}

#[test]
fn handle_v1_record_reads_require_canonical_original_bytes() {
    let wire = open_handle_wire();
    let canonical = arco_core::canonical_json::to_canonical_bytes(&wire)
        .expect("encode canonical handle fixture");
    ControlPlaneHandleRecord::from_json_slice(&canonical).expect("canonical handle is readable");

    let pretty = serde_json::to_vec_pretty(&wire).expect("encode noncanonical handle fixture");
    assert_ne!(pretty, canonical);
    assert!(
        ControlPlaneHandleRecord::from_json_slice(&pretty).is_err(),
        "accepted noncanonical original handle bytes"
    );
}

const DIGEST: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[test]
fn handle_mutation_references_require_canonical_order_paths_and_digests() {
    let first =
        ControlPlaneHandleMutationRef::new(HANDLE_ID, 1, ControlPlaneTxKind::CatalogDdl, DIGEST)
            .expect("first canonical mutation reference");
    let second = ControlPlaneHandleMutationRef::new(
        HANDLE_ID,
        2,
        ControlPlaneTxKind::OrchestrationBatch,
        DIGEST,
    )
    .expect("second canonical mutation reference");
    assert_eq!(
        first.path,
        "transactions/handles/hdl_00000000000000000000000000/mutations/00000000000000000001.json"
    );

    for malformed_digest in [
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "sha256:0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef",
        "sha256:0123456789abcdef",
    ] {
        assert!(
            ControlPlaneHandleMutationRef::new(
                HANDLE_ID,
                1,
                ControlPlaneTxKind::CatalogDdl,
                malformed_digest,
            )
            .is_err(),
            "accepted malformed digest {malformed_digest}"
        );
    }

    let mut record = ControlPlaneHandleRecord::from_json_slice(
        &serde_json::to_vec(&open_handle_wire()).expect("encode fixture"),
    )
    .expect("decode fixture");
    record.mutation_refs = vec![first.clone(), second.clone()];
    assert!(record.validate().is_ok());

    record.mutation_refs = vec![first.clone(), first.clone()];
    assert!(
        record.validate().is_err(),
        "accepted duplicate ordinal/path"
    );

    record.mutation_refs = vec![second, first.clone()];
    assert!(record.validate().is_err(), "accepted noncanonical ordering");

    let mut corrupt_path = first;
    corrupt_path.path = "transactions/handles/hdl_escape/mutations/../1.json".to_string();
    record.mutation_refs = vec![corrupt_path];
    assert!(record.validate().is_err(), "accepted corrupt staged path");
}

fn prepared_handle_record() -> ControlPlaneHandleRecord {
    let mut record = ControlPlaneHandleRecord::from_json_slice(
        &serde_json::to_vec(&open_handle_wire()).expect("encode fixture"),
    )
    .expect("decode fixture");
    record.revision = 3;
    record.status = ControlPlaneHandleStatus::Prepared;
    record.updated_at = Utc.with_ymd_and_hms(2026, 7, 16, 12, 5, 0).unwrap();
    record.prepared_at = Some(record.updated_at);
    record.mutation_refs = vec![
        ControlPlaneHandleMutationRef::new(HANDLE_ID, 1, ControlPlaneTxKind::CatalogDdl, DIGEST)
            .expect("canonical mutation reference"),
    ];
    record.participants = vec![ControlPlaneHandleParticipant {
        ordinal: 1,
        kind: ControlPlaneTxKind::CatalogDdl,
        domain: ControlPlaneTxDomain::Catalog,
        request_id: format!("handle:{HANDLE_ID}:mutation:{:020}", 1),
        idempotency_key: format!("handle:{HANDLE_ID}:mutation:{:020}", 1),
        request_hash: DIGEST.to_string(),
        tx_id: None,
        low_level_status: None,
        receipt_path: None,
    }];
    record.validate().expect("valid prepared handle");
    record
}

#[test]
fn handle_participants_bind_exact_identity_and_request_hash_to_their_mutation() {
    let prepared = prepared_handle_record();
    let participant = &prepared.participants[0];
    let expected_identity = format!("handle:{HANDLE_ID}:mutation:{:020}", 1);
    assert_eq!(participant.request_id, expected_identity);
    assert_eq!(participant.idempotency_key, expected_identity);
    assert_eq!(participant.request_hash, DIGEST);

    let mut wrong_request_id = prepared.clone();
    wrong_request_id.participants[0].request_id = format!("handle:{HANDLE_ID}:mutation:{:020}", 2);
    assert!(
        wrong_request_id.validate().is_err(),
        "accepted a participant request_id for a different mutation ordinal"
    );

    let mut wrong_idempotency_key = prepared.clone();
    wrong_idempotency_key.participants[0].idempotency_key =
        format!("handle:{HANDLE_ID}:mutation:{:020}", 2);
    assert!(
        wrong_idempotency_key.validate().is_err(),
        "accepted a participant idempotency_key for a different mutation ordinal"
    );

    let mut malformed_request_hash = prepared.clone();
    malformed_request_hash.participants[0].request_hash =
        "sha256:0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef".to_string();
    assert!(
        malformed_request_hash.validate().is_err(),
        "accepted a noncanonical participant request_hash"
    );

    let mut missing_request_hash =
        serde_json::to_value(&prepared).expect("encode prepared handle fixture");
    missing_request_hash["participants"][0]
        .as_object_mut()
        .expect("participant object")
        .remove("request_hash");
    assert!(
        ControlPlaneHandleRecord::from_json_slice(
            &serde_json::to_vec(&missing_request_hash).expect("encode missing-hash fixture")
        )
        .is_err(),
        "accepted a participant without a durable request_hash"
    );
}

#[test]
fn handle_participant_transaction_identity_and_status_are_all_or_nothing() {
    let mut tx_without_status = prepared_handle_record();
    tx_without_status.participants[0].tx_id = Some("00000000000000000000000000".to_string());
    assert!(
        tx_without_status.validate().is_err(),
        "accepted a participant tx_id without low-level status evidence"
    );

    let mut status_without_tx = prepared_handle_record();
    status_without_tx.participants[0].low_level_status = Some(ControlPlaneTxStatus::Prepared);
    assert!(
        status_without_tx.validate().is_err(),
        "accepted low-level status evidence without an exact-readable tx_id"
    );
}

#[test]
fn handle_lifecycle_timestamps_are_monotonic_and_respect_expiry_boundaries() {
    let prepared = prepared_handle_record();

    let mut prepared_at_expiry = prepared.clone();
    prepared_at_expiry.updated_at = prepared_at_expiry.expires_at;
    prepared_at_expiry.prepared_at = Some(prepared_at_expiry.expires_at);
    assert!(
        prepared_at_expiry.validate().is_err(),
        "accepted PREPARED at the exclusive expiry boundary"
    );

    let mut committing = prepared.clone();
    committing.revision += 1;
    committing.status = ControlPlaneHandleStatus::Committing;
    committing.updated_at = Utc.with_ymd_and_hms(2026, 7, 16, 12, 6, 0).unwrap();
    committing.committing_at = Some(committing.updated_at);
    committing.validate().expect("valid committing handle");

    let mut committing_before_prepared = committing.clone();
    committing_before_prepared.committing_at =
        Some(Utc.with_ymd_and_hms(2026, 7, 16, 12, 4, 59).unwrap());
    assert!(
        committing_before_prepared.validate().is_err(),
        "accepted committing_at before prepared_at"
    );

    let mut committing_at_expiry = committing.clone();
    committing_at_expiry.updated_at = committing_at_expiry.expires_at;
    committing_at_expiry.committing_at = Some(committing_at_expiry.expires_at);
    assert!(
        committing_at_expiry.validate().is_err(),
        "accepted COMMITTING at the exclusive expiry boundary"
    );

    let mut visible = committing.clone();
    visible.revision += 1;
    visible.status = ControlPlaneHandleStatus::Visible;
    visible.updated_at = Utc.with_ymd_and_hms(2026, 7, 16, 13, 1, 0).unwrap();
    visible.visible_at = Some(visible.updated_at);
    visible.participants[0].tx_id = Some("00000000000000000000000000".to_string());
    visible.participants[0].low_level_status = Some(ControlPlaneTxStatus::Visible);
    visible.participants[0].receipt_path = Some("commits/catalog/01JVISIBLE.json".to_string());
    visible
        .validate()
        .expect("a commit begun before expiry may finish after expiry");

    let mut visible_before_committing = visible;
    visible_before_committing.visible_at =
        Some(Utc.with_ymd_and_hms(2026, 7, 16, 12, 5, 59).unwrap());
    assert!(
        visible_before_committing.validate().is_err(),
        "accepted visible_at before committing_at"
    );

    let mut aborted_at_expiry = prepared;
    aborted_at_expiry.revision += 1;
    aborted_at_expiry.status = ControlPlaneHandleStatus::Aborted;
    aborted_at_expiry.updated_at = aborted_at_expiry.expires_at;
    aborted_at_expiry.terminal_at = Some(aborted_at_expiry.expires_at);
    assert!(
        aborted_at_expiry.validate().is_err(),
        "accepted ABORTED at a boundary that must become EXPIRED"
    );
}

#[test]
fn handle_json_bytes_are_canonical() {
    let record = prepared_handle_record();
    let encoded = record.to_json_vec().expect("encode validated handle");
    let canonical = arco_core::canonical_json::to_canonical_bytes(&record)
        .expect("canonicalize validated handle");
    assert_eq!(encoded, canonical);
}

#[test]
fn handle_lifecycle_exposes_only_the_legal_transition_graph() {
    use ControlPlaneHandleStatus::{
        Aborted, Committing, Expired, Open, Prepared, Preparing, RepairRequired, Visible,
    };

    let statuses = [
        Open,
        Preparing,
        Prepared,
        Committing,
        Visible,
        Aborted,
        Expired,
        RepairRequired,
    ];
    let legal = [
        (Open, Preparing),
        (Open, Aborted),
        (Open, Expired),
        (Open, RepairRequired),
        (Preparing, Prepared),
        (Preparing, Aborted),
        (Preparing, Expired),
        (Preparing, RepairRequired),
        (Prepared, Committing),
        (Prepared, Aborted),
        (Prepared, Expired),
        (Prepared, RepairRequired),
        (Committing, Visible),
        (Committing, RepairRequired),
        (RepairRequired, Committing),
        (RepairRequired, Visible),
    ];

    for from in statuses {
        for to in statuses {
            assert_eq!(
                from.can_transition_to(to),
                legal.contains(&(from, to)),
                "unexpected transition decision {from:?} -> {to:?}"
            );
        }
    }
}

#[test]
fn handle_record_lifecycle_evidence_fails_closed_and_counts_visibility() {
    let prepared = prepared_handle_record();

    let mut committing = prepared.clone();
    committing.revision += 1;
    committing.status = ControlPlaneHandleStatus::Committing;
    committing.updated_at = Utc.with_ymd_and_hms(2026, 7, 16, 12, 6, 0).unwrap();
    committing.committing_at = Some(committing.updated_at);
    committing.validate().expect("valid committing handle");
    assert_eq!(committing.visible_participant_count(), 0);

    let mut visible = committing.clone();
    visible.revision += 1;
    visible.status = ControlPlaneHandleStatus::Visible;
    visible.updated_at = Utc.with_ymd_and_hms(2026, 7, 16, 12, 7, 0).unwrap();
    visible.visible_at = Some(visible.updated_at);
    visible.participants[0].tx_id = Some("00000000000000000000000000".to_string());
    visible.participants[0].low_level_status = Some(ControlPlaneTxStatus::Visible);
    visible.participants[0].receipt_path = Some("commits/catalog/01JVISIBLE.json".to_string());
    visible.validate().expect("valid visible handle");
    assert_eq!(visible.visible_participant_count(), 1);

    let mut ambiguous_visible = visible.clone();
    ambiguous_visible.participants[0].low_level_status = Some(ControlPlaneTxStatus::Prepared);
    assert!(
        ambiguous_visible.validate().is_err(),
        "accepted VISIBLE without all low-level participants visible"
    );

    let mut repair = committing;
    repair.revision += 1;
    repair.status = ControlPlaneHandleStatus::RepairRequired;
    repair.updated_at = Utc.with_ymd_and_hms(2026, 7, 16, 12, 8, 0).unwrap();
    repair.failure_category = Some(ControlPlaneHandleFailureCategory::ParticipantUncertain);
    repair.participants[0].tx_id = Some("00000000000000000000000000".to_string());
    repair.participants[0].low_level_status = Some(ControlPlaneTxStatus::Prepared);
    repair.validate().expect("valid repair-required handle");

    let mut unsafe_abort = repair.clone();
    unsafe_abort.status = ControlPlaneHandleStatus::Aborted;
    unsafe_abort.terminal_at = Some(unsafe_abort.updated_at);
    assert!(
        unsafe_abort.validate().is_err(),
        "accepted abort after visibility became uncertain"
    );

    let mut expired = ControlPlaneHandleRecord::from_json_slice(
        &serde_json::to_vec(&open_handle_wire()).expect("encode fixture"),
    )
    .expect("decode fixture");
    expired.revision += 1;
    expired.status = ControlPlaneHandleStatus::Expired;
    expired.updated_at = expired.expires_at;
    expired.terminal_at = Some(expired.updated_at);
    expired.validate().expect("valid pre-visibility expiry");

    let mut premature_expiry = expired;
    premature_expiry.updated_at = Utc.with_ymd_and_hms(2026, 7, 16, 12, 30, 0).unwrap();
    premature_expiry.terminal_at = Some(premature_expiry.updated_at);
    assert!(
        premature_expiry.validate().is_err(),
        "accepted expiry before expires_at"
    );
}

#[test]
fn handle_terminal_and_recovery_evidence_rejects_ambiguous_low_level_state() {
    let mut repair_without_identity = prepared_handle_record();
    repair_without_identity.revision += 1;
    repair_without_identity.status = ControlPlaneHandleStatus::RepairRequired;
    repair_without_identity.updated_at = Utc.with_ymd_and_hms(2026, 7, 16, 12, 6, 0).unwrap();
    repair_without_identity.committing_at = Some(repair_without_identity.updated_at);
    repair_without_identity.failure_category =
        Some(ControlPlaneHandleFailureCategory::ParticipantUncertain);
    repair_without_identity.participants[0].low_level_status = Some(ControlPlaneTxStatus::Prepared);
    assert!(
        repair_without_identity.validate().is_err(),
        "accepted low-level status without an exact-readable tx_id"
    );

    let mut expired_after_claim = prepared_handle_record();
    expired_after_claim.revision += 1;
    expired_after_claim.status = ControlPlaneHandleStatus::Expired;
    expired_after_claim.updated_at = expired_after_claim.expires_at;
    expired_after_claim.terminal_at = Some(expired_after_claim.updated_at);
    expired_after_claim.participants[0].tx_id = Some("00000000000000000000000000".to_string());
    expired_after_claim.participants[0].low_level_status = Some(ControlPlaneTxStatus::Prepared);
    assert!(
        expired_after_claim.validate().is_err(),
        "accepted expiry after low-level visibility became uncertain"
    );

    let mut root_abort = prepared_handle_record();
    root_abort.mutation_refs[0].kind = ControlPlaneTxKind::RootCommit;
    root_abort.participants[0].kind = ControlPlaneTxKind::RootCommit;
    root_abort.participants[0].domain = ControlPlaneTxDomain::Root;
    root_abort.participants[0].tx_id = Some("00000000000000000000000000".to_string());
    root_abort.participants[0].low_level_status = Some(ControlPlaneTxStatus::Aborted);
    root_abort.revision += 1;
    root_abort.status = ControlPlaneHandleStatus::Aborted;
    root_abort.updated_at = Utc.with_ymd_and_hms(2026, 7, 16, 12, 6, 0).unwrap();
    root_abort.terminal_at = Some(root_abort.updated_at);
    root_abort.failure_category = Some(ControlPlaneHandleFailureCategory::ParticipantAborted);
    assert!(
        root_abort.validate().is_err(),
        "accepted a root ABORTED record as proof that its domain participants are invisible"
    );

    let mut catalog_abort = prepared_handle_record();
    catalog_abort.participants[0].tx_id = Some("00000000000000000000000000".to_string());
    catalog_abort.participants[0].low_level_status = Some(ControlPlaneTxStatus::Aborted);
    catalog_abort.revision += 1;
    catalog_abort.status = ControlPlaneHandleStatus::Aborted;
    catalog_abort.updated_at = Utc.with_ymd_and_hms(2026, 7, 16, 12, 6, 0).unwrap();
    catalog_abort.terminal_at = Some(catalog_abort.updated_at);
    catalog_abort.failure_category = Some(ControlPlaneHandleFailureCategory::ParticipantAborted);
    assert!(
        catalog_abort.validate().is_err(),
        "accepted an ABORTED handle after a catalog participant claim existed"
    );

    let mut unsafe_receipt = prepared_handle_record().participants.remove(0);
    unsafe_receipt.tx_id = Some("00000000000000000000000000".to_string());
    unsafe_receipt.low_level_status = Some(ControlPlaneTxStatus::Visible);
    unsafe_receipt.receipt_path = Some("C:receipt.json".to_string());
    assert!(
        unsafe_receipt.validate().is_err(),
        "accepted a drive-qualified receipt path"
    );
}

#[test]
fn handle_contract_keeps_low_level_transaction_status_at_exactly_three_states() {
    for (wire, expected) in [
        ("PREPARED", ControlPlaneTxStatus::Prepared),
        ("VISIBLE", ControlPlaneTxStatus::Visible),
        ("ABORTED", ControlPlaneTxStatus::Aborted),
    ] {
        let decoded: ControlPlaneTxStatus =
            serde_json::from_str(&format!("\"{wire}\"")).expect("known low-level status");
        assert_eq!(decoded, expected);
        assert_eq!(
            serde_json::to_string(&decoded).expect("serialize low-level status"),
            format!("\"{wire}\"")
        );
    }

    for handle_only in [
        "OPEN",
        "PREPARING",
        "COMMITTING",
        "REPAIR_REQUIRED",
        "EXPIRED",
    ] {
        assert!(
            serde_json::from_str::<ControlPlaneTxStatus>(&format!("\"{handle_only}\"")).is_err(),
            "handle-only state leaked into the low-level status contract"
        );
    }
}

#[test]
fn control_plane_transaction_paths_are_stable() {
    let key = "tenant/acme:catalog-ddl";
    let key_hash = format!("{:x}", Sha256::digest(key.as_bytes()));
    let prefix = &key_hash[..2];

    assert_eq!(
        ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Catalog, key),
        format!("transactions/idempotency/catalog/{prefix}/{key_hash}.json")
    );
    assert_eq!(
        ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Orchestration, key),
        format!("transactions/idempotency/orchestration/{prefix}/{key_hash}.json")
    );
    assert_eq!(
        ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, "01JTXCAT"),
        "transactions/catalog/01JTXCAT.json"
    );
    assert_eq!(
        ControlPlaneTxPaths::record(ControlPlaneTxDomain::Orchestration, "01JTXORCH"),
        "transactions/orchestration/01JTXORCH.json"
    );
    assert_eq!(
        ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, "01JTXROOT"),
        "transactions/root/01JTXROOT.json"
    );
    assert_eq!(ControlPlaneTxPaths::root_lock(), "locks/root.lock.json");
    assert_eq!(
        ControlPlaneTxPaths::root_super_manifest("01JTXROOT"),
        "transactions/root/01JTXROOT.manifest.json"
    );
    assert_eq!(
        ControlPlaneTxPaths::root_commit_receipt("01JROOTCOMMIT"),
        "commits/root/01JROOTCOMMIT.json"
    );
    assert_eq!(
        ControlPlaneTxPaths::orchestration_commit_receipt("01JORCHCOMMIT"),
        "commits/orchestration/01JORCHCOMMIT.json"
    );
}

#[test]
fn control_plane_transaction_record_serializes_camel_case_fields() {
    let record: ControlPlaneTxRecord<CatalogTxReceipt> =
        serde_json::from_value(serde_json::json!({
            "txId": "01JQTX",
            "kind": "catalog_ddl",
            "status": "VISIBLE",
            "repairPending": true,
            "requestId": "01JQREQ",
            "idempotencyKey": "client-key",
            "requestHash": "sha256:req",
            "lockPath": "locks/catalog.lock.json",
            "fencingToken": 42,
            "preparedAt": "2026-03-29T14:12:03Z",
            "visibleAt": "2026-03-29T14:12:03Z",
            "result": {
                "txId": "01JQTX",
                "eventId": "01JQEVENT",
                "commitId": "01JQCOMMIT",
                "manifestId": "00000000000000000117",
                "snapshotVersion": 17,
                "pointerVersion": "\"etag-123\"",
                "readToken": "catalog:00000000000000000117",
                "visibleAt": "2026-03-29T14:12:03Z"
            }
        }))
        .expect("deserialize");

    let json = serde_json::to_value(&record).expect("serialize");
    assert_eq!(json["txId"], "01JQTX");
    assert_eq!(json["kind"], "catalog_ddl");
    assert_eq!(json["status"], "VISIBLE");
    assert_eq!(json["repairPending"], true);
    assert_eq!(json["requestId"], "01JQREQ");
    assert_eq!(json["idempotencyKey"], "client-key");
    assert_eq!(json["lockPath"], "locks/catalog.lock.json");
    assert_eq!(json["fencingToken"], 42);
    assert_eq!(json["result"]["manifestId"], "00000000000000000117");
}

#[test]
fn transaction_record_tolerates_missing_audit_fields() {
    let record: ControlPlaneTxRecord<CatalogTxReceipt> =
        serde_json::from_value(serde_json::json!({
            "txId": "01JQTX",
            "kind": "catalog_ddl",
            "status": "VISIBLE",
            "repairPending": false,
            "requestHash": "sha256:req",
            "lockPath": "locks/catalog.lock.json",
            "fencingToken": 42,
            "preparedAt": "2026-03-29T14:12:03Z"
        }))
        .expect("deserialize");

    let json = serde_json::to_value(&record).expect("serialize");
    assert_eq!(json["txId"], "01JQTX");
    assert_eq!(json["requestId"], "");
    assert_eq!(json["idempotencyKey"], "");
}

#[test]
fn idempotency_record_serializes_audit_and_replay_fields() {
    let record: ControlPlaneIdempotencyRecord = serde_json::from_value(serde_json::json!({
        "txId": "01JQTX",
        "kind": "catalog_ddl",
        "requestId": "01JQREQ",
        "idempotencyKey": "client-key",
        "requestHash": "sha256:req",
        "createdAt": "2026-03-29T14:12:03Z"
    }))
    .expect("deserialize");

    let json = serde_json::to_value(&record).expect("serialize");
    assert_eq!(json["txId"], "01JQTX");
    assert_eq!(json["kind"], "catalog_ddl");
    assert_eq!(json["requestId"], "01JQREQ");
    assert_eq!(json["idempotencyKey"], "client-key");
    assert_eq!(json["requestHash"], "sha256:req");
}

#[test]
fn idempotency_record_tolerates_missing_audit_fields() {
    let record: ControlPlaneIdempotencyRecord = serde_json::from_value(serde_json::json!({
        "txId": "01JQTX",
        "kind": "catalog_ddl",
        "requestHash": "sha256:req",
        "createdAt": "2026-03-29T14:12:03Z"
    }))
    .expect("deserialize");

    let json = serde_json::to_value(&record).expect("serialize");
    assert_eq!(json["txId"], "01JQTX");
    assert_eq!(json["requestId"], "");
    assert_eq!(json["idempotencyKey"], "");
}

#[test]
fn root_transaction_manifest_serializes_pinned_domain_heads() {
    let created_at = Utc.with_ymd_and_hms(2026, 3, 29, 14, 15, 0).unwrap();
    let mut domains = BTreeMap::new();
    domains.insert(
        ControlPlaneTxDomain::Catalog,
        RootTxManifestDomain {
            manifest_id: "00000000000000000118".to_string(),
            manifest_path: "manifests/catalog/00000000000000000118.json".to_string(),
            commit_id: "01JQCAT".to_string(),
        },
    );
    domains.insert(
        ControlPlaneTxDomain::Orchestration,
        RootTxManifestDomain {
            manifest_id: "00000000000000000493".to_string(),
            manifest_path: "state/orchestration/manifests/00000000000000000493.json".to_string(),
            commit_id: "01JQORCH".to_string(),
        },
    );

    let manifest = RootTxManifest {
        tx_id: "01JQROOT".to_string(),
        fencing_token: 42,
        published_at: created_at,
        domains,
    };

    let json = serde_json::to_value(&manifest).expect("serialize");
    assert_eq!(json["txId"], "01JQROOT");
    assert_eq!(json["fencingToken"], 42);
    assert!(json.get("rootManifestId").is_none());
    assert!(json.get("parentHash").is_none());
    assert!(json.get("previousRootManifestPath").is_none());
    assert_eq!(json["domains"]["catalog"]["commitId"], "01JQCAT");
    assert_eq!(
        json["domains"]["orchestration"]["manifestPath"],
        "state/orchestration/manifests/00000000000000000493.json"
    );

    let commit = DomainCommit {
        domain: ControlPlaneTxDomain::Catalog,
        tx_id: "01JQCATTX".to_string(),
        commit_id: "01JQCAT".to_string(),
        manifest_id: "00000000000000000118".to_string(),
        manifest_path: "manifests/catalog/00000000000000000118.json".to_string(),
        read_token: "catalog:00000000000000000118".to_string(),
    };
    let commit_json = serde_json::to_value(&commit).expect("serialize");
    assert_eq!(commit_json["domain"], "catalog");
    assert_eq!(commit_json["readToken"], "catalog:00000000000000000118");

    let receipt = RootTxReceipt {
        tx_id: "01JQROOT".to_string(),
        root_commit_id: "01JQROOTCOMMIT".to_string(),
        super_manifest_path: "transactions/root/01JQROOT.manifest.json".to_string(),
        domain_commits: vec![commit],
        read_token: "root:01JQROOT".to_string(),
        visible_at: created_at,
    };
    let receipt_json = serde_json::to_value(&receipt).expect("serialize");
    assert_eq!(
        receipt_json["superManifestPath"],
        "transactions/root/01JQROOT.manifest.json"
    );
    assert_eq!(receipt_json["readToken"], "root:01JQROOT");
    assert!(receipt_json.get("pointerVersion").is_none());
}
