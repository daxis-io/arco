//! Route-level tests for audit event wiring.
//!
//! These tests verify that audit events are correctly emitted from HTTP handlers
//! by mounting the router with a `TestAuditSink` and making actual requests.

use std::sync::Arc;

use arco_catalog::write_options::WriteOptions;
use arco_catalog::{CatalogWriter, RegisterTableRequest, Tier1Compactor};
use arco_core::ScopedStorage;
use arco_core::audit::{AuditAction, AuditEmitter, TestAuditSink};
use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition};
use arco_iceberg::pointer::IcebergTablePointer;
use arco_iceberg::router::iceberg_router;
use arco_iceberg::state::{IcebergConfig, IcebergState, Tier1CompactorFactory};
use arco_iceberg::types::{
    CommitTableRequest, PartitionSpec, Schema, SchemaField, SortOrder, TableMetadata, TableUuid,
    UpdateRequirement,
};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use tower::ServiceExt;

const TENANT: &str = "test-tenant";
const WORKSPACE: &str = "test-workspace";

async fn setup_state_with_table(sink: Arc<TestAuditSink>) -> (IcebergState, uuid::Uuid) {
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let scoped =
        ScopedStorage::new(Arc::clone(&storage), TENANT, WORKSPACE).expect("scoped storage");

    let compactor = Arc::new(Tier1Compactor::new(scoped.clone()));
    let writer = CatalogWriter::new(scoped.clone()).with_sync_compactor(compactor);
    writer.initialize().await.expect("init");

    writer
        .create_namespace("sales", None, WriteOptions::default())
        .await
        .expect("create namespace");

    let table_record = writer
        .register_table(
            RegisterTableRequest {
                namespace: "sales".to_string(),
                name: "orders".to_string(),
                description: None,
                location: Some(format!(
                    "tenant={TENANT}/workspace={WORKSPACE}/warehouse/orders"
                )),
                format: Some("iceberg".to_string()),
                columns: vec![],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register table");

    let table_uuid = uuid::Uuid::parse_str(&table_record.id).expect("table uuid");

    let metadata = TableMetadata {
        format_version: 2,
        table_uuid: TableUuid::new(table_uuid),
        location: format!("tenant={TENANT}/workspace={WORKSPACE}/warehouse/orders"),
        last_sequence_number: 0,
        last_updated_ms: 1_700_000_000_000,
        last_column_id: 1,
        current_schema_id: 0,
        schemas: vec![Schema {
            schema_id: 0,
            schema_type: "struct".to_string(),
            fields: vec![SchemaField {
                id: 1,
                name: "id".to_string(),
                field_type: serde_json::Value::String("long".to_string()),
                required: true,
            }],
        }],
        default_spec_id: 0,
        partition_specs: vec![PartitionSpec {
            spec_id: 0,
            fields: vec![],
        }],
        last_partition_id: 999,
        default_sort_order_id: 0,
        sort_orders: vec![SortOrder {
            order_id: 0,
            fields: vec![],
        }],
        properties: Default::default(),
        current_snapshot_id: None,
        refs: Default::default(),
        snapshots: vec![],
        snapshot_log: vec![],
        metadata_log: vec![],
    };

    let metadata_location = format!(
        "tenant={TENANT}/workspace={WORKSPACE}/warehouse/orders/metadata/00000.metadata.json"
    );
    let pointer = IcebergTablePointer::new(table_uuid, metadata_location);
    let pointer_path = IcebergTablePointer::storage_path(&table_uuid);
    let pointer_bytes = serde_json::to_vec(&pointer).expect("serialize pointer");
    scoped
        .put_raw(
            &pointer_path,
            Bytes::from(pointer_bytes),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("put pointer");

    let metadata_bytes = serde_json::to_vec(&metadata).expect("serialize metadata");
    scoped
        .put_raw(
            "warehouse/orders/metadata/00000.metadata.json",
            Bytes::from(metadata_bytes),
            WritePrecondition::None,
        )
        .await
        .expect("put metadata");

    let emitter = AuditEmitter::with_test_sink(sink);
    let state = IcebergState::new(storage).with_audit_emitter(emitter);

    (state, table_uuid)
}

async fn setup_state_with_table_write_enabled(
    sink: Arc<TestAuditSink>,
) -> (IcebergState, uuid::Uuid) {
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let scoped =
        ScopedStorage::new(Arc::clone(&storage), TENANT, WORKSPACE).expect("scoped storage");

    let compactor = Arc::new(Tier1Compactor::new(scoped.clone()));
    let writer = CatalogWriter::new(scoped.clone()).with_sync_compactor(compactor);
    writer.initialize().await.expect("init");

    writer
        .create_namespace("sales", None, WriteOptions::default())
        .await
        .expect("create namespace");

    let table_record = writer
        .register_table(
            RegisterTableRequest {
                namespace: "sales".to_string(),
                name: "orders".to_string(),
                description: None,
                location: Some(format!(
                    "tenant={TENANT}/workspace={WORKSPACE}/warehouse/orders"
                )),
                format: Some("iceberg".to_string()),
                columns: vec![],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register table");

    let table_uuid = uuid::Uuid::parse_str(&table_record.id).expect("table uuid");

    let metadata = TableMetadata {
        format_version: 2,
        table_uuid: TableUuid::new(table_uuid),
        location: format!("tenant={TENANT}/workspace={WORKSPACE}/warehouse/orders"),
        last_sequence_number: 0,
        last_updated_ms: 1_700_000_000_000,
        last_column_id: 1,
        current_schema_id: 0,
        schemas: vec![Schema {
            schema_id: 0,
            schema_type: "struct".to_string(),
            fields: vec![SchemaField {
                id: 1,
                name: "id".to_string(),
                field_type: serde_json::Value::String("long".to_string()),
                required: true,
            }],
        }],
        default_spec_id: 0,
        partition_specs: vec![PartitionSpec {
            spec_id: 0,
            fields: vec![],
        }],
        last_partition_id: 999,
        default_sort_order_id: 0,
        sort_orders: vec![SortOrder {
            order_id: 0,
            fields: vec![],
        }],
        properties: Default::default(),
        current_snapshot_id: None,
        refs: Default::default(),
        snapshots: vec![],
        snapshot_log: vec![],
        metadata_log: vec![],
    };

    let metadata_location = format!(
        "tenant={TENANT}/workspace={WORKSPACE}/warehouse/orders/metadata/00000.metadata.json"
    );
    let pointer = IcebergTablePointer::new(table_uuid, metadata_location);
    let pointer_path = IcebergTablePointer::storage_path(&table_uuid);
    let pointer_bytes = serde_json::to_vec(&pointer).expect("serialize pointer");
    scoped
        .put_raw(
            &pointer_path,
            Bytes::from(pointer_bytes),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("put pointer");

    let metadata_bytes = serde_json::to_vec(&metadata).expect("serialize metadata");
    scoped
        .put_raw(
            "warehouse/orders/metadata/00000.metadata.json",
            Bytes::from(metadata_bytes),
            WritePrecondition::None,
        )
        .await
        .expect("put metadata");

    let emitter = AuditEmitter::with_test_sink(sink);
    let config = IcebergConfig {
        allow_write: true,
        ..Default::default()
    };
    let state = IcebergState::with_config(storage, config)
        .with_compactor_factory(Arc::new(Tier1CompactorFactory))
        .with_audit_emitter(emitter);

    (state, table_uuid)
}

#[tokio::test]
async fn test_credential_vend_deny_emits_audit_when_disabled() {
    let sink = Arc::new(TestAuditSink::new());
    let (state, _) = setup_state_with_table(sink.clone()).await;
    let app = iceberg_router(state);

    let request = Request::builder()
        .method("GET")
        .uri("/v1/arco/namespaces/sales/tables/orders/credentials")
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE)
        .body(Body::empty())
        .expect("request");

    let response = app.oneshot(request).await.expect("response");
    let status = response.status();

    if status != StatusCode::BAD_REQUEST {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        panic!(
            "Expected BAD_REQUEST, got {status}: {}",
            String::from_utf8_lossy(&body)
        );
    }

    let events = sink.events();
    let cred_vend_events: Vec<_> = events
        .iter()
        .filter(|e| e.action == AuditAction::CredVendDeny)
        .collect();

    assert_eq!(
        cred_vend_events.len(),
        1,
        "Expected exactly one CredVendDeny audit event"
    );
    assert!(
        cred_vend_events[0].decision_reason.contains("disabled"),
        "Expected denial reason to mention disabled"
    );
}

#[tokio::test]
async fn test_load_table_does_not_emit_audit() {
    let sink = Arc::new(TestAuditSink::new());
    let (state, _) = setup_state_with_table(sink.clone()).await;
    let app = iceberg_router(state);

    let request = Request::builder()
        .method("GET")
        .uri("/v1/arco/namespaces/sales/tables/orders")
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE)
        .body(Body::empty())
        .expect("request");

    let response = app.oneshot(request).await.expect("response");
    let status = response.status();

    if status != StatusCode::OK {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        panic!(
            "Expected OK, got {status}: {}",
            String::from_utf8_lossy(&body)
        );
    }

    let events = sink.events();
    let audit_events: Vec<_> = events
        .iter()
        .filter(|e| {
            matches!(
                e.action,
                AuditAction::IcebergCommit
                    | AuditAction::IcebergCommitDeny
                    | AuditAction::CredVendAllow
                    | AuditAction::CredVendDeny
            )
        })
        .collect();

    assert!(
        audit_events.is_empty(),
        "Load table should not emit commit or cred-vend audit events"
    );
}

#[tokio::test]
async fn test_commit_with_wrong_uuid_emits_deny_audit() {
    let sink = Arc::new(TestAuditSink::new());
    let (state, table_uuid) = setup_state_with_table_write_enabled(sink.clone()).await;
    let app = iceberg_router(state);

    let wrong_uuid = uuid::Uuid::new_v4();
    assert_ne!(wrong_uuid, table_uuid, "UUIDs should differ for test");

    let commit_request = CommitTableRequest {
        identifier: None,
        requirements: vec![UpdateRequirement::AssertTableUuid { uuid: wrong_uuid }],
        updates: vec![],
    };
    let body = serde_json::to_string(&commit_request).expect("serialize commit request");

    let request = Request::builder()
        .method("POST")
        .uri("/v1/arco/namespaces/sales/tables/orders")
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE)
        .header("Content-Type", "application/json")
        .header("Idempotency-Key", uuid::Uuid::now_v7().to_string())
        .body(Body::from(body))
        .expect("request");

    let response = app.oneshot(request).await.expect("response");
    let status = response.status();

    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "Expected CONFLICT for UUID mismatch"
    );

    let events = sink.events();

    let deny_events: Vec<_> = events
        .iter()
        .filter(|e| e.action == AuditAction::IcebergCommitDeny)
        .collect();

    assert_eq!(
        deny_events.len(),
        1,
        "Expected exactly one IcebergCommitDeny audit event, got {}",
        deny_events.len()
    );
    assert!(
        deny_events[0].decision_reason.contains("conflict"),
        "Expected denial reason to mention conflict, got: {}",
        deny_events[0].decision_reason
    );
}
