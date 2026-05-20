//! Contract tests for tenant, workspace, and metastore control-plane scoping.

#![allow(clippy::expect_used)]

use std::sync::Arc;

use bytes::Bytes;

use arco_core::{
    ControlPlaneScope, MemoryBackend, ScopedStorage, StorageBackend, WritePrecondition,
};

#[test]
fn control_plane_scope_exposes_workspace_and_metastore_prefixes() {
    let scope = ControlPlaneScope::new("acme", "prod", "lakehouse-prod").expect("valid scope");

    assert_eq!(scope.tenant_id(), "acme");
    assert_eq!(scope.workspace_id(), "prod");
    assert_eq!(scope.metastore_id(), "lakehouse-prod");
    assert_eq!(scope.workspace_prefix(), "tenant=acme/workspace=prod/");
    assert_eq!(
        scope.metastore_prefix(),
        "tenant=acme/metastore=lakehouse-prod/"
    );
}

#[test]
fn control_plane_scope_workspace_alias_uses_workspace_as_metastore() {
    let scope = ControlPlaneScope::workspace_alias("acme", "prod").expect("valid alias scope");

    assert_eq!(scope.tenant_id(), "acme");
    assert_eq!(scope.workspace_id(), "prod");
    assert_eq!(scope.metastore_id(), "prod");
    assert_eq!(scope.workspace_prefix(), "tenant=acme/workspace=prod/");
    assert_eq!(scope.metastore_prefix(), "tenant=acme/metastore=prod/");
}

#[test]
fn control_plane_scope_rejects_empty_and_traversal_ids() {
    assert!(ControlPlaneScope::new("", "prod", "lakehouse-prod").is_err());
    assert!(ControlPlaneScope::new("acme", "", "lakehouse-prod").is_err());
    assert!(ControlPlaneScope::new("acme", "prod", "").is_err());
    assert!(ControlPlaneScope::new("acme", "../prod", "lakehouse-prod").is_err());
    assert!(ControlPlaneScope::new("acme", "prod", "../metastore").is_err());
    assert!(ControlPlaneScope::new("tenant/evil", "prod", "lakehouse-prod").is_err());
}

#[tokio::test]
async fn control_plane_scope_scoped_storage_can_be_constructed_for_metastore_scope() {
    let backend = Arc::new(MemoryBackend::new());
    let scope = ControlPlaneScope::new("acme", "prod", "lakehouse-prod").expect("valid scope");
    let storage = ScopedStorage::new_metastore_scoped(backend.clone(), &scope)
        .expect("metastore-scoped storage");

    storage
        .put_raw(
            "ledger/event-01.json",
            Bytes::from_static(b"{}"),
            WritePrecondition::None,
        )
        .await
        .expect("write should succeed");

    backend
        .get("tenant=acme/metastore=lakehouse-prod/ledger/event-01.json")
        .await
        .expect("object should be written under metastore prefix");

    assert!(
        backend
            .get("tenant=acme/workspace=prod/ledger/event-01.json")
            .await
            .is_err()
    );
}
