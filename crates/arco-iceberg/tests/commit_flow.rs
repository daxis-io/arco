//! Integration tests for Iceberg write-path storage interactions.

use std::collections::HashMap;
use std::sync::Arc;

use arco_catalog::write_options::WriteOptions;
use arco_catalog::{CatalogWriter, Tier1Compactor};
use arco_core::ScopedStorage;
use arco_core::storage::{StorageBackend, WritePrecondition};
use arco_iceberg::commit::CommitService;
use arco_iceberg::idempotency::{
    IdempotencyMarker, IdempotencyStatus, IdempotencyStore, IdempotencyStoreImpl,
    canonical_request_hash,
};
use arco_iceberg::pointer::{IcebergTablePointer, PointerStore, PointerStoreImpl, UpdateSource};
use arco_iceberg::state::{IcebergConfig, IcebergState, Tier1CompactorFactory};
use arco_iceberg::types::commit::{CommitTableRequest, SnapshotRefType, TableUpdate};
use arco_iceberg::types::{
    CommitKey, PartitionSpec, Schema, SchemaField, Snapshot, SortOrder, TableMetadata, TableUuid,
};
use arco_test_utils::storage::TracingMemoryBackend;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use tower::ServiceExt;
use uuid::Uuid;

struct Fixture {
    backend: Arc<TracingMemoryBackend>,
    storage: Arc<ScopedStorage>,
    tenant: String,
    workspace: String,
    table: String,
    table_uuid: Uuid,
    table_location: String,
    metadata_location: String,
}

impl Fixture {
    async fn new() -> Self {
        let backend = Arc::new(TracingMemoryBackend::new());
        let tenant = "acme".to_string();
        let workspace = "analytics".to_string();
        let storage_backend: Arc<dyn StorageBackend> = backend.clone();
        let storage = ScopedStorage::new(storage_backend, tenant.clone(), workspace.clone())
            .expect("scoped storage");
        let storage = Arc::new(storage);
        let table = "orders".to_string();
        let table_uuid = Uuid::new_v4();
        let table_location = format!("tenant={tenant}/workspace={workspace}/warehouse/{table}");
        let metadata_location = format!("{table_location}/metadata/00000.metadata.json");

        let metadata = base_metadata(table_uuid, table_location.clone());
        let metadata_path = format!("warehouse/{table}/metadata/00000.metadata.json");
        let metadata_bytes = serde_json::to_vec(&metadata).expect("serialize metadata");
        storage
            .put(
                &metadata_path,
                Bytes::from(metadata_bytes),
                WritePrecondition::None,
            )
            .await
            .expect("put metadata");

        let pointer = IcebergTablePointer::new(table_uuid, metadata_location.clone());
        let pointer_path = IcebergTablePointer::storage_path(&table_uuid);
        let pointer_bytes = serde_json::to_vec(&pointer).expect("serialize pointer");
        storage
            .put(
                &pointer_path,
                Bytes::from(pointer_bytes),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("put pointer");

        Self {
            backend,
            storage,
            tenant,
            workspace,
            table,
            table_uuid,
            table_location,
            metadata_location,
        }
    }

    fn metadata_prefix(&self) -> String {
        format!(
            "tenant={}/workspace={}/warehouse/{}/metadata/",
            self.tenant, self.workspace, self.table
        )
    }
}

fn base_metadata(table_uuid: Uuid, location: String) -> TableMetadata {
    TableMetadata {
        format_version: 2,
        table_uuid: TableUuid::new(table_uuid),
        location,
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
                required: false,
                field_type: serde_json::Value::String("string".to_string()),
            }],
        }],
        current_snapshot_id: None,
        snapshots: vec![],
        snapshot_log: vec![],
        metadata_log: vec![],
        properties: HashMap::new(),
        default_spec_id: 0,
        partition_specs: vec![PartitionSpec {
            spec_id: 0,
            fields: vec![],
        }],
        last_partition_id: 0,
        refs: HashMap::new(),
        default_sort_order_id: 0,
        sort_orders: vec![SortOrder {
            order_id: 0,
            fields: vec![],
        }],
    }
}

fn commit_request(snapshot_id: i64) -> CommitTableRequest {
    let snapshot = Snapshot {
        snapshot_id,
        parent_snapshot_id: None,
        sequence_number: 1,
        timestamp_ms: 1_700_000_010_000,
        manifest_list: "s3://bucket/manifests/snap.avro".to_string(),
        summary: HashMap::new(),
        schema_id: Some(0),
    };

    CommitTableRequest::builder()
        .add_update(TableUpdate::AddSnapshot { snapshot })
        .add_update(TableUpdate::SetSnapshotRef {
            ref_name: "main".to_string(),
            ref_type: SnapshotRefType::Branch,
            snapshot_id,
            max_ref_age_ms: None,
            max_snapshot_age_ms: None,
            min_snapshots_to_keep: None,
        })
        .build()
}

#[tokio::test]
async fn test_commit_table_success_persists_state() {
    let fixture = Fixture::new().await;
    let service = CommitService::new(Arc::clone(&fixture.storage));

    let request = commit_request(101);
    let request_value = serde_json::to_value(&request).expect("serialize request");
    let request_hash = canonical_request_hash(&request_value).expect("request hash");
    let idempotency_key = Uuid::now_v7().to_string();

    let response = service
        .commit_table(
            fixture.table_uuid,
            "sales",
            &fixture.table,
            request,
            request_hash,
            idempotency_key.clone(),
            UpdateSource::IcebergRest {
                client_info: Some("test-suite".to_string()),
                principal: None,
            },
            &fixture.tenant,
            &fixture.workspace,
        )
        .await
        .expect("commit");

    let key_hash = IdempotencyMarker::hash_key(&idempotency_key);
    let expected_location = format!(
        "{}/metadata/1-{}.metadata.json",
        fixture.table_location, key_hash
    );
    assert_eq!(response.metadata_location, expected_location);

    let pointer_store = PointerStoreImpl::new(Arc::clone(&fixture.storage));
    let (pointer, _) = pointer_store
        .load(&fixture.table_uuid)
        .await
        .expect("load pointer")
        .expect("pointer exists");
    assert_eq!(pointer.current_metadata_location, expected_location);
    assert_eq!(
        pointer.previous_metadata_location.as_deref(),
        Some(fixture.metadata_location.as_str())
    );

    let idempotency_store = IdempotencyStoreImpl::new(Arc::clone(&fixture.storage));
    let (marker, _) = idempotency_store
        .load(&fixture.table_uuid, &key_hash)
        .await
        .expect("load marker")
        .expect("marker exists");
    assert_eq!(marker.status, IdempotencyStatus::Committed);
    assert_eq!(
        marker.response_metadata_location.as_deref(),
        Some(expected_location.as_str())
    );

    let expected_metadata_path = format!(
        "tenant={}/workspace={}/warehouse/{}/metadata/1-{}.metadata.json",
        fixture.tenant, fixture.workspace, fixture.table, key_hash
    );
    let stored_paths = fixture.backend.paths();
    assert!(stored_paths.contains(&expected_metadata_path));

    let commit_key = CommitKey::from_metadata_location(&expected_location);
    let pending_suffix = format!("iceberg/pending/{commit_key}.json");
    let committed_suffix = format!("iceberg/committed/{commit_key}.json");

    assert!(
        stored_paths
            .iter()
            .any(|path| path.ends_with(&pending_suffix))
    );
    assert!(
        stored_paths
            .iter()
            .any(|path| path.ends_with(&committed_suffix))
    );
}

#[tokio::test]
async fn test_commit_table_idempotent_replay_returns_cached_response() {
    let fixture = Fixture::new().await;
    let service = CommitService::new(Arc::clone(&fixture.storage));

    let request = commit_request(202);
    let request_value = serde_json::to_value(&request).expect("serialize request");
    let request_hash = canonical_request_hash(&request_value).expect("request hash");
    let idempotency_key = Uuid::now_v7().to_string();

    let response1 = service
        .commit_table(
            fixture.table_uuid,
            "sales",
            &fixture.table,
            request.clone(),
            request_hash.clone(),
            idempotency_key.clone(),
            UpdateSource::IcebergRest {
                client_info: Some("test-suite".to_string()),
                principal: None,
            },
            &fixture.tenant,
            &fixture.workspace,
        )
        .await
        .expect("commit");

    let metadata_count_before = fixture
        .backend
        .paths()
        .iter()
        .filter(|path| path.starts_with(&fixture.metadata_prefix()))
        .count();

    let response2 = service
        .commit_table(
            fixture.table_uuid,
            "sales",
            &fixture.table,
            request,
            request_hash,
            idempotency_key,
            UpdateSource::IcebergRest {
                client_info: Some("test-suite".to_string()),
                principal: None,
            },
            &fixture.tenant,
            &fixture.workspace,
        )
        .await
        .expect("replay");

    assert_eq!(response1.metadata_location, response2.metadata_location);

    let metadata_count_after = fixture
        .backend
        .paths()
        .iter()
        .filter(|path| path.starts_with(&fixture.metadata_prefix()))
        .count();
    assert_eq!(metadata_count_before, metadata_count_after);
}

#[tokio::test]
async fn test_transactions_commit_single_table_returns_204() {
    let backend = Arc::new(TracingMemoryBackend::new());
    let tenant = "acme".to_string();
    let workspace = "analytics".to_string();
    let table_name = "orders".to_string();

    let storage_backend: Arc<dyn StorageBackend> = backend.clone();
    let config = IcebergConfig {
        allow_write: true,
        ..Default::default()
    };
    let state = IcebergState::with_config(storage_backend.clone(), config)
        .with_compactor_factory(Arc::new(Tier1CompactorFactory));

    let catalog_storage =
        ScopedStorage::new(storage_backend.clone(), tenant.clone(), workspace.clone())
            .expect("scoped storage");
    let compactor = Arc::new(Tier1Compactor::new(catalog_storage.clone()));
    let writer = CatalogWriter::new(catalog_storage.clone()).with_sync_compactor(compactor);
    writer.initialize().await.expect("init");
    writer
        .create_namespace("sales", None, WriteOptions::default())
        .await
        .expect("create namespace");

    let table_location = format!("tenant={tenant}/workspace={workspace}/warehouse/{table_name}");

    let table = writer
        .register_table(
            arco_catalog::RegisterTableRequest {
                namespace: "sales".to_string(),
                name: table_name.clone(),
                description: None,
                location: Some(table_location.clone()),
                format: Some("iceberg".to_string()),
                columns: vec![],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register table");

    let table_uuid = Uuid::parse_str(&table.id).expect("parse table uuid");
    let metadata_location = format!("{table_location}/metadata/00000.metadata.json");

    let metadata = base_metadata(table_uuid, table_location.clone());
    let metadata_path = format!("warehouse/{table_name}/metadata/00000.metadata.json");
    let metadata_bytes = serde_json::to_vec(&metadata).expect("serialize metadata");
    catalog_storage
        .put(
            &metadata_path,
            Bytes::from(metadata_bytes),
            WritePrecondition::None,
        )
        .await
        .expect("put metadata");

    let pointer = IcebergTablePointer::new(table_uuid, metadata_location);
    let pointer_path = IcebergTablePointer::storage_path(&table_uuid);
    let pointer_bytes = serde_json::to_vec(&pointer).expect("serialize pointer");
    catalog_storage
        .put(
            &pointer_path,
            Bytes::from(pointer_bytes),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("put pointer");

    let app = Router::new()
        .nest("/v1/:prefix", arco_iceberg::routes::catalog::routes())
        .layer(axum::middleware::from_fn(
            arco_iceberg::context::context_middleware,
        ))
        .with_state(state);

    let snapshot = Snapshot {
        snapshot_id: 303,
        parent_snapshot_id: None,
        sequence_number: 1,
        timestamp_ms: 1_700_000_010_000,
        manifest_list: "s3://bucket/manifests/snap.avro".to_string(),
        summary: HashMap::new(),
        schema_id: Some(0),
    };

    let req_body = serde_json::json!({
        "table-changes": [{
            "identifier": {"namespace": ["sales"], "name": table_name},
            "requirements": [],
            "updates": [
                {"action": "add-snapshot", "snapshot": snapshot},
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "type": "branch",
                    "snapshot-id": 303
                }
            ]
        }]
    });

    let idempotency_key = Uuid::now_v7().to_string();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/arco/transactions/commit")
                .header("X-Tenant-Id", &tenant)
                .header("X-Workspace-Id", &workspace)
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", &idempotency_key)
                .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}
