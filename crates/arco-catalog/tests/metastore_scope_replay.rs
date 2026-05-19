//! Task 3 metastore scope replay and API-threading contracts.

#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arco_catalog::metastore::events::{
    CatalogObjectRecord, LifecycleState, MetastoreEvent, MetastoreMutation,
};
use arco_catalog::metastore::replay::replay_events;
use arco_catalog::{CatalogReader, CatalogWriter, Tier1Compactor};
use arco_core::{ControlPlaneScope, MemoryBackend, ScopedStorage};

#[test]
fn metastore_scope_facades_default_to_workspace_alias_and_preserve_paths() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "acme", "workspace_a").expect("storage");

    let writer = CatalogWriter::new(storage.clone());
    let reader = CatalogReader::new(storage.clone());
    let compactor = Tier1Compactor::new(storage.clone());

    assert_eq!(writer.scope().tenant_id(), "acme");
    assert_eq!(writer.scope().workspace_id(), "workspace_a");
    assert_eq!(writer.scope().metastore_id(), "workspace_a");
    assert_eq!(reader.scope(), writer.scope());
    assert_eq!(compactor.scope(), writer.scope());
    assert_eq!(
        writer.storage().manifest_root(),
        "tenant=acme/workspace=workspace_a/manifests/root.manifest.json"
    );
}

#[test]
fn metastore_scope_facades_accept_explicit_scope_without_moving_workspace_paths() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "acme", "notebooks").expect("storage");
    let scope =
        ControlPlaneScope::new("acme", "notebooks", "lakehouse_prod").expect("explicit scope");

    let writer = CatalogWriter::new_with_scope(storage.clone(), scope.clone());
    let reader = CatalogReader::new_with_scope(storage.clone(), scope.clone());
    let compactor = Tier1Compactor::new_with_scope(storage, scope.clone());

    assert_eq!(writer.scope(), &scope);
    assert_eq!(reader.scope(), &scope);
    assert_eq!(compactor.scope(), &scope);
    assert_eq!(
        writer.storage().manifest_root(),
        "tenant=acme/workspace=notebooks/manifests/root.manifest.json"
    );
}

#[test]
fn metastore_scope_facades_reject_storage_scope_mismatches() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "acme", "notebooks").expect("storage");
    let mismatched_workspace =
        ControlPlaneScope::new("acme", "pipelines", "lakehouse_prod").expect("mismatched scope");
    let mismatched_tenant =
        ControlPlaneScope::new("other", "notebooks", "lakehouse_prod").expect("tenant scope");

    assert!(
        CatalogWriter::try_new_with_scope(storage.clone(), mismatched_workspace.clone()).is_err()
    );
    assert!(
        CatalogReader::try_new_with_scope(storage.clone(), mismatched_workspace.clone()).is_err()
    );
    assert!(Tier1Compactor::try_new_with_scope(storage.clone(), mismatched_workspace).is_err());
    assert!(CatalogWriter::try_new_with_scope(storage, mismatched_tenant).is_err());
}

#[test]
fn metastore_scope_replay_resolves_same_object_for_shared_metastore_across_workspaces() {
    let notebooks =
        ControlPlaneScope::new("acme", "notebooks", "lakehouse_prod").expect("notebooks scope");
    let pipelines =
        ControlPlaneScope::new("acme", "pipelines", "lakehouse_prod").expect("pipelines scope");
    let event = table_event(
        &notebooks,
        "event_001",
        1,
        "table_orders_shared",
        "sales.curated.orders",
    );

    let state = replay_events([&event]).expect("replay should succeed");

    assert_eq!(
        state.catalog_object_id_for_scope(&notebooks, "TABLE", "sales.curated.orders"),
        Some("table_orders_shared")
    );
    assert_eq!(
        state.catalog_object_id_for_scope(&pipelines, "TABLE", "sales.curated.orders"),
        Some("table_orders_shared")
    );
}

#[test]
fn metastore_scope_replay_keeps_same_names_isolated_between_metastores() {
    let prod = ControlPlaneScope::new("acme", "notebooks", "lakehouse_prod").expect("prod scope");
    let sandbox =
        ControlPlaneScope::new("acme", "notebooks", "lakehouse_sandbox").expect("sandbox scope");
    let prod_event = table_event(
        &prod,
        "event_001",
        1,
        "table_orders_prod",
        "sales.curated.orders",
    );
    let sandbox_event = table_event(
        &sandbox,
        "event_002",
        2,
        "table_orders_sandbox",
        "sales.curated.orders",
    );

    let state = replay_events([&prod_event, &sandbox_event]).expect("replay should succeed");

    assert_eq!(
        state.catalog_object_id_for_scope(&prod, "TABLE", "sales.curated.orders"),
        Some("table_orders_prod")
    );
    assert_eq!(
        state.catalog_object_id_for_scope(&sandbox, "TABLE", "sales.curated.orders"),
        Some("table_orders_sandbox")
    );
}

#[test]
fn metastore_scope_replay_digest_and_event_hash_include_metastore_scope() {
    let prod = ControlPlaneScope::new("acme", "notebooks", "lakehouse_prod").expect("prod scope");
    let sandbox =
        ControlPlaneScope::new("acme", "notebooks", "lakehouse_sandbox").expect("sandbox scope");
    let prod_event = table_event(
        &prod,
        "event_001",
        1,
        "table_orders",
        "sales.curated.orders",
    );
    let sandbox_event = table_event(
        &sandbox,
        "event_001",
        1,
        "table_orders",
        "sales.curated.orders",
    );

    assert_ne!(
        prod_event
            .deterministic_hash()
            .expect("prod event deterministic hash"),
        sandbox_event
            .deterministic_hash()
            .expect("sandbox event deterministic hash")
    );

    let prod_state = replay_events([&prod_event]).expect("prod replay");
    let sandbox_state = replay_events([&sandbox_event]).expect("sandbox replay");

    assert_ne!(
        prod_state.deterministic_digest(),
        sandbox_state.deterministic_digest()
    );
}

#[test]
fn metastore_scope_replay_removes_stale_aliases_on_rename() {
    let scope = ControlPlaneScope::new("acme", "notebooks", "lakehouse_prod").expect("scope");
    let old_name = table_event(
        &scope,
        "event_001",
        1,
        "table_orders",
        "sales.curated.orders_old",
    );
    let new_name = table_event(
        &scope,
        "event_002",
        2,
        "table_orders",
        "sales.curated.orders",
    );

    let state = replay_events([&old_name, &new_name]).expect("replay should succeed");

    assert_eq!(
        state.catalog_object_id_for_scope(&scope, "TABLE", "sales.curated.orders"),
        Some("table_orders")
    );
    assert_eq!(
        state.catalog_object_id_for_scope(&scope, "TABLE", "sales.curated.orders_old"),
        None
    );
}

#[test]
fn metastore_scope_replay_removes_deleted_objects_from_name_index() {
    let scope = ControlPlaneScope::new("acme", "notebooks", "lakehouse_prod").expect("scope");
    let active = table_event(
        &scope,
        "event_001",
        1,
        "table_orders",
        "sales.curated.orders",
    );
    let mut deleted = table_event(
        &scope,
        "event_002",
        2,
        "table_orders",
        "sales.curated.orders",
    );
    if let MetastoreMutation::CatalogObjectUpserted(record) = &mut deleted.mutation {
        record.lifecycle_state = LifecycleState::Deleted;
    }

    let state = replay_events([&active, &deleted]).expect("replay should succeed");

    assert_eq!(
        state.catalog_object_id_for_scope(&scope, "TABLE", "sales.curated.orders"),
        None
    );
}

fn table_event(
    scope: &ControlPlaneScope,
    event_id: &str,
    sequence: u64,
    object_id: &str,
    qualified_name: &str,
) -> MetastoreEvent {
    MetastoreEvent::new_scoped(
        scope,
        event_id,
        sequence,
        MetastoreMutation::CatalogObjectUpserted(CatalogObjectRecord {
            object_id: object_id.to_string(),
            object_type: "TABLE".to_string(),
            qualified_name: qualified_name.to_string(),
            owner: "metastore-admin".to_string(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: sequence as i64,
            properties: BTreeMap::new(),
        }),
    )
}
