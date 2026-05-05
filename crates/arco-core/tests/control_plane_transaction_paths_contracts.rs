//! Contract tests for canonical control-plane transaction paths and record shapes.

#![allow(clippy::expect_used)]

use chrono::{TimeZone, Utc};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;

use arco_core::control_plane_transactions::{
    CatalogTxReceipt, ControlPlaneIdempotencyRecord, ControlPlaneTxDomain, ControlPlaneTxPaths,
    ControlPlaneTxRecord, DomainCommit, RootTxManifest, RootTxManifestDomain, RootTxReceipt,
};

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
