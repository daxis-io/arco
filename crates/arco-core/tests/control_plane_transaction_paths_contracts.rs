//! Contract tests for canonical control-plane transaction paths and record shapes.

#![allow(clippy::expect_used)]

use chrono::{TimeZone, Utc};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;

use arco_core::control_plane_transactions::{
    CatalogTxReceipt, ControlPlaneTxDomain, ControlPlaneTxKind, ControlPlaneTxPaths,
    ControlPlaneTxRecord, ControlPlaneTxStatus, DomainCommit, RootTxManifest, RootTxManifestDomain,
    RootTxReceipt,
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
    let visible_at = Utc.with_ymd_and_hms(2026, 3, 29, 14, 12, 3).unwrap();
    let record = ControlPlaneTxRecord {
        tx_id: "01JQTX".to_string(),
        kind: ControlPlaneTxKind::CatalogDdl,
        status: ControlPlaneTxStatus::Visible,
        repair_pending: true,
        request_id: "01JQREQ".to_string(),
        idempotency_key: "client-key".to_string(),
        request_hash: "sha256:req".to_string(),
        lock_path: "locks/catalog.lock.json".to_string(),
        fencing_token: 42,
        prepared_at: visible_at,
        visible_at: Some(visible_at),
        result: Some(CatalogTxReceipt {
            tx_id: "01JQTX".to_string(),
            event_id: "01JQEVENT".to_string(),
            commit_id: "01JQCOMMIT".to_string(),
            manifest_id: "00000000000000000117".to_string(),
            snapshot_version: 17,
            pointer_version: "\"etag-123\"".to_string(),
            read_token: "catalog:00000000000000000117".to_string(),
            visible_at,
        }),
    };

    let json = serde_json::to_value(&record).expect("serialize");
    assert_eq!(json["txId"], "01JQTX");
    assert_eq!(json["kind"], "catalog_ddl");
    assert_eq!(json["status"], "VISIBLE");
    assert_eq!(json["repairPending"], true);
    assert_eq!(json["lockPath"], "locks/catalog.lock.json");
    assert_eq!(json["fencingToken"], 42);
    assert_eq!(json["result"]["manifestId"], "00000000000000000117");
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
