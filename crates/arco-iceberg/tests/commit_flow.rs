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
use axum::http::{Request, Response, StatusCode};
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

struct MultiTableHttpFixture {
    app: Router,
    tenant: String,
    workspace: String,
    table_names: Vec<String>,
}

impl MultiTableHttpFixture {
    async fn new(config: IcebergConfig) -> Self {
        let backend = Arc::new(TracingMemoryBackend::new());
        let tenant = "acme".to_string();
        let workspace = "analytics".to_string();

        let storage_backend: Arc<dyn StorageBackend> = backend.clone();
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

        let table_names = vec!["orders".to_string(), "customers".to_string()];
        for name in &table_names {
            let table_location = format!("tenant={tenant}/workspace={workspace}/warehouse/{name}");

            let table = writer
                .register_table(
                    arco_catalog::RegisterTableRequest {
                        namespace: "sales".to_string(),
                        name: name.clone(),
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
            let metadata_path = format!("warehouse/{name}/metadata/00000.metadata.json");
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
        }

        let app = Router::new()
            .nest("/v1/:prefix", arco_iceberg::routes::catalog::routes())
            .layer(axum::middleware::from_fn(
                arco_iceberg::context::context_middleware,
            ))
            .with_state(state);

        Self {
            app,
            tenant,
            workspace,
            table_names,
        }
    }

    fn multi_table_request_body(&self) -> serde_json::Value {
        let table_changes: Vec<_> = self
            .table_names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let snapshot_id = 1000 + i as i64;
                serde_json::json!({
                    "identifier": {"namespace": ["sales"], "name": name},
                    "requirements": [],
                    "updates": [
                        {
                            "action": "add-snapshot",
                            "snapshot": {
                                "snapshot-id": snapshot_id,
                                "sequence-number": 1,
                                "timestamp-ms": 1_700_000_010_000_i64,
                                "manifest-list": format!("s3://bucket/manifests/{name}.avro"),
                                "summary": {}
                            }
                        },
                        {
                            "action": "set-snapshot-ref",
                            "ref-name": "main",
                            "type": "branch",
                            "snapshot-id": snapshot_id
                        }
                    ]
                })
            })
            .collect();

        serde_json::json!({ "table-changes": table_changes })
    }

    async fn send_commit(self) -> Response<Body> {
        let req_body = self.multi_table_request_body();
        let idempotency_key = Uuid::now_v7().to_string();

        self.app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/arco/transactions/commit")
                    .header("X-Tenant-Id", &self.tenant)
                    .header("X-Workspace-Id", &self.workspace)
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", &idempotency_key)
                    .body(Body::from(serde_json::to_string(&req_body).unwrap()))
                    .expect("request"),
            )
            .await
            .expect("response")
    }
}

#[tokio::test]
async fn test_transactions_commit_multi_table_returns_204() {
    let config = IcebergConfig {
        allow_write: true,
        allow_multi_table_transactions: true,
        ..Default::default()
    };
    let fixture = MultiTableHttpFixture::new(config).await;
    let response = fixture.send_commit().await;

    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "Multi-table transaction should return 204"
    );
}

#[tokio::test]
async fn test_transactions_commit_multi_table_returns_406_when_disabled() {
    let config = IcebergConfig {
        allow_write: true,
        allow_multi_table_transactions: false,
        ..Default::default()
    };
    let fixture = MultiTableHttpFixture::new(config).await;
    let response = fixture.send_commit().await;

    assert_eq!(
        response.status(),
        StatusCode::NOT_ACCEPTABLE,
        "Multi-table transaction should return 406 when flag is disabled"
    );
}

mod multi_table_transactions {
    use super::*;
    use arco_iceberg::coordinator::{
        CoordinatorConfig, MultiTableCommitError, MultiTableTransactionCoordinator,
        TableCommitInput,
    };
    use arco_iceberg::pointer::resolve_effective_metadata_location;
    use arco_iceberg::transactions::{TransactionStore, TransactionStoreImpl};
    use chrono::Duration;

    struct MultiTableFixture {
        storage: ScopedStorage,
        tenant: String,
        workspace: String,
        table1_uuid: Uuid,
        table2_uuid: Uuid,
    }

    impl MultiTableFixture {
        async fn new() -> Self {
            let backend: Arc<dyn StorageBackend> = Arc::new(TracingMemoryBackend::new());
            let tenant = "acme".to_string();
            let workspace = "analytics".to_string();
            let storage = ScopedStorage::new(backend, tenant.clone(), workspace.clone())
                .expect("scoped storage");

            let table1_uuid = Uuid::new_v4();
            let table1_location = format!("tenant={tenant}/workspace={workspace}/warehouse/orders");

            let table2_uuid = Uuid::new_v4();
            let table2_location =
                format!("tenant={tenant}/workspace={workspace}/warehouse/customers");

            for (uuid, location, name) in [
                (table1_uuid, &table1_location, "orders"),
                (table2_uuid, &table2_location, "customers"),
            ] {
                let metadata = base_metadata(uuid, location.clone());
                let metadata_path = format!("warehouse/{name}/metadata/00000.metadata.json");
                let metadata_bytes = serde_json::to_vec(&metadata).expect("serialize metadata");
                storage
                    .put(
                        &metadata_path,
                        Bytes::from(metadata_bytes),
                        WritePrecondition::None,
                    )
                    .await
                    .expect("put metadata");

                let metadata_location = format!("{location}/metadata/00000.metadata.json");
                let pointer = IcebergTablePointer::new(uuid, metadata_location);
                let pointer_path = IcebergTablePointer::storage_path(&uuid);
                let pointer_bytes = serde_json::to_vec(&pointer).expect("serialize pointer");
                storage
                    .put(
                        &pointer_path,
                        Bytes::from(pointer_bytes),
                        WritePrecondition::DoesNotExist,
                    )
                    .await
                    .expect("put pointer");
            }

            Self {
                storage,
                tenant,
                workspace,
                table1_uuid,
                table2_uuid,
            }
        }
    }

    fn table_commit_input(
        uuid: Uuid,
        namespace: &str,
        name: &str,
        snapshot_id: i64,
    ) -> TableCommitInput {
        TableCommitInput {
            table_uuid: uuid,
            namespace: namespace.to_string(),
            table_name: name.to_string(),
            request: commit_request(snapshot_id),
        }
    }

    #[tokio::test]
    async fn test_tx_commit_two_tables_success_is_atomic() {
        let fixture = MultiTableFixture::new().await;
        let storage = Arc::new(fixture.storage);
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));
        let coordinator =
            MultiTableTransactionCoordinator::new(Arc::clone(&storage), Arc::clone(&pointer_store));

        let tx_id = Uuid::now_v7().to_string();
        let inputs = vec![
            table_commit_input(fixture.table1_uuid, "sales", "orders", 1001),
            table_commit_input(fixture.table2_uuid, "sales", "customers", 2001),
        ];

        let result = coordinator
            .commit(
                tx_id.clone(),
                "hash123".to_string(),
                inputs,
                UpdateSource::IcebergRest {
                    client_info: None,
                    principal: None,
                },
                &fixture.tenant,
                &fixture.workspace,
            )
            .await
            .expect("commit should succeed");

        assert_eq!(result.tables.len(), 2);

        let (p1, _) = pointer_store
            .load(&fixture.table1_uuid)
            .await
            .expect("load")
            .expect("exists");
        let (p2, _) = pointer_store
            .load(&fixture.table2_uuid)
            .await
            .expect("load")
            .expect("exists");

        assert!(p1.current_metadata_location.contains(&tx_id));
        assert!(p2.current_metadata_location.contains(&tx_id));
        assert!(p1.pending.is_none());
        assert!(p2.pending.is_none());
    }

    #[tokio::test]
    async fn test_tx_commit_crash_before_commit_record_not_visible() {
        let fixture = MultiTableFixture::new().await;
        let storage = Arc::new(fixture.storage);
        let tx_store = TransactionStoreImpl::new(Arc::clone(&storage));

        let tx_id = Uuid::now_v7().to_string();
        let base_location = "tenant=acme/workspace=analytics/warehouse/orders";

        let mut pointer = IcebergTablePointer::new(
            fixture.table1_uuid,
            format!("{base_location}/metadata/00000.metadata.json"),
        );
        pointer.pending = Some(arco_iceberg::pointer::PendingPointerUpdate {
            tx_id: tx_id.clone(),
            metadata_location: format!("{base_location}/metadata/00001.metadata.json"),
            snapshot_id: Some(999),
            last_sequence_number: 1,
            prepared_at: chrono::Utc::now(),
        });

        let effective = resolve_effective_metadata_location(&pointer, &tx_store)
            .await
            .expect("resolve");

        assert_eq!(
            effective.metadata_location,
            format!("{base_location}/metadata/00000.metadata.json")
        );
        assert!(!effective.is_pending);
    }
    #[tokio::test]
    async fn test_tx_commit_crash_after_commit_record_visible() {
        let fixture = MultiTableFixture::new().await;
        let storage = Arc::new(fixture.storage);
        let tx_store = TransactionStoreImpl::new(Arc::clone(&storage));

        let tx_id = Uuid::now_v7().to_string();
        let base_location = "tenant=acme/workspace=analytics/warehouse/orders";
        let pending_location = format!("{base_location}/metadata/00001.metadata.json");

        let tx_record = arco_iceberg::transactions::TransactionRecord::new_preparing(
            tx_id.clone(),
            "hash".to_string(),
        )
        .commit();
        tx_store.create(&tx_record).await.expect("create tx");

        let mut pointer = IcebergTablePointer::new(
            fixture.table1_uuid,
            format!("{base_location}/metadata/00000.metadata.json"),
        );
        pointer.pending = Some(arco_iceberg::pointer::PendingPointerUpdate {
            tx_id: tx_id.clone(),
            metadata_location: pending_location.clone(),
            snapshot_id: Some(999),
            last_sequence_number: 1,
            prepared_at: chrono::Utc::now(),
        });

        let effective = resolve_effective_metadata_location(&pointer, &tx_store)
            .await
            .expect("resolve");

        assert_eq!(effective.metadata_location, pending_location);
        assert!(effective.is_pending);
    }

    #[tokio::test]
    async fn test_tx_idempotency_replay_returns_success() {
        let fixture = MultiTableFixture::new().await;
        let table1_uuid = fixture.table1_uuid;
        let tenant = fixture.tenant.clone();
        let workspace = fixture.workspace.clone();
        let storage = Arc::new(fixture.storage);
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));
        let coordinator =
            MultiTableTransactionCoordinator::new(Arc::clone(&storage), Arc::clone(&pointer_store));

        let tx_id = Uuid::now_v7().to_string();
        let request_hash = "same_hash".to_string();
        let inputs = vec![table_commit_input(table1_uuid, "sales", "orders", 3001)];

        let result1 = coordinator
            .commit(
                tx_id.clone(),
                request_hash.clone(),
                inputs.clone(),
                UpdateSource::IcebergRest {
                    client_info: None,
                    principal: None,
                },
                &tenant,
                &workspace,
            )
            .await
            .expect("first commit");

        let result2 = coordinator
            .commit(
                tx_id.clone(),
                request_hash,
                inputs,
                UpdateSource::IcebergRest {
                    client_info: None,
                    principal: None,
                },
                &tenant,
                &workspace,
            )
            .await
            .expect("replay commit");

        assert_eq!(
            result1.tables[0].metadata_location,
            result2.tables[0].metadata_location
        );
    }

    #[tokio::test]
    async fn test_tx_request_hash_mismatch_returns_conflict() {
        let fixture = MultiTableFixture::new().await;
        let table1_uuid = fixture.table1_uuid;
        let tenant = fixture.tenant.clone();
        let workspace = fixture.workspace.clone();
        let storage = Arc::new(fixture.storage);
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));
        let coordinator =
            MultiTableTransactionCoordinator::new(Arc::clone(&storage), Arc::clone(&pointer_store));

        let tx_id = Uuid::now_v7().to_string();
        let inputs = vec![table_commit_input(table1_uuid, "sales", "orders", 4001)];

        coordinator
            .commit(
                tx_id.clone(),
                "hash_1".to_string(),
                inputs.clone(),
                UpdateSource::IcebergRest {
                    client_info: None,
                    principal: None,
                },
                &tenant,
                &workspace,
            )
            .await
            .expect("first commit");

        let result = coordinator
            .commit(
                tx_id.clone(),
                "hash_2".to_string(),
                inputs,
                UpdateSource::IcebergRest {
                    client_info: None,
                    principal: None,
                },
                &tenant,
                &workspace,
            )
            .await;

        assert!(matches!(
            result,
            Err(MultiTableCommitError::Iceberg(
                arco_iceberg::error::IcebergError::Conflict { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn test_max_tables_limit_returns_400() {
        let fixture = MultiTableFixture::new().await;
        let table1_uuid = fixture.table1_uuid;
        let table2_uuid = fixture.table2_uuid;
        let tenant = fixture.tenant.clone();
        let workspace = fixture.workspace.clone();
        let storage = Arc::new(fixture.storage);
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));
        let config = CoordinatorConfig {
            max_tables: 1,
            transaction_timeout: Duration::minutes(15),
        };
        let coordinator = MultiTableTransactionCoordinator::with_config(
            Arc::clone(&storage),
            Arc::clone(&pointer_store),
            config,
        );

        let inputs = vec![
            table_commit_input(table1_uuid, "sales", "orders", 5001),
            table_commit_input(table2_uuid, "sales", "customers", 5002),
        ];

        let result = coordinator
            .commit(
                Uuid::now_v7().to_string(),
                "hash".to_string(),
                inputs,
                UpdateSource::IcebergRest {
                    client_info: None,
                    principal: None,
                },
                &tenant,
                &workspace,
            )
            .await;

        assert!(matches!(
            result,
            Err(MultiTableCommitError::Iceberg(
                arco_iceberg::error::IcebergError::BadRequest { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn test_tx_commit_overlap_conflict() {
        // Setup: two coordinators sharing the same storage
        let fixture = MultiTableFixture::new().await;
        let storage = Arc::new(fixture.storage);
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));
        let tx_store = TransactionStoreImpl::new(Arc::clone(&storage));

        // TX1: create transaction record in Preparing state and set pending on pointer
        let tx1_id = Uuid::now_v7().to_string();
        let tx1_record = arco_iceberg::transactions::TransactionRecord::new_preparing(
            tx1_id.clone(),
            "hash1".to_string(),
        );
        tx_store.create(&tx1_record).await.expect("create tx1");

        // Manually set pending on the pointer (simulating TX1's prepare phase)
        let (pointer, pointer_version) = pointer_store
            .load(&fixture.table1_uuid)
            .await
            .expect("load")
            .expect("pointer exists");

        let mut pointer_with_pending = pointer.clone();
        pointer_with_pending.pending = Some(arco_iceberg::pointer::PendingPointerUpdate {
            tx_id: tx1_id.clone(),
            metadata_location: format!(
                "tenant={}/workspace={}/warehouse/orders/metadata/00001-{}.metadata.json",
                fixture.tenant, fixture.workspace, tx1_id
            ),
            snapshot_id: Some(1001),
            last_sequence_number: 1,
            prepared_at: chrono::Utc::now(),
        });

        let _ = pointer_store
            .compare_and_swap(
                &fixture.table1_uuid,
                &pointer_version,
                &pointer_with_pending,
            )
            .await
            .expect("set pending");

        // TX2: attempt to commit the same table while TX1 is still preparing
        let coordinator =
            MultiTableTransactionCoordinator::new(Arc::clone(&storage), Arc::clone(&pointer_store));
        let tx2_id = Uuid::now_v7().to_string();
        let inputs = vec![table_commit_input(
            fixture.table1_uuid,
            "sales",
            "orders",
            2001,
        )];

        let result = coordinator
            .commit(
                tx2_id,
                "hash2".to_string(),
                inputs,
                UpdateSource::IcebergRest {
                    client_info: None,
                    principal: None,
                },
                &fixture.tenant,
                &fixture.workspace,
            )
            .await;

        // TX2 should get RetryAfter since TX1 is still preparing (not stale)
        assert!(
            matches!(
                result,
                Err(MultiTableCommitError::RetryAfter { seconds: 30 })
            ),
            "Expected RetryAfter(30) when another transaction is preparing, got: {result:?}"
        );

        // Verify TX1's pending state is still intact
        let (final_pointer, _) = pointer_store
            .load(&fixture.table1_uuid)
            .await
            .expect("load")
            .expect("pointer exists");
        assert!(final_pointer.pending.is_some());
        assert_eq!(final_pointer.pending.as_ref().unwrap().tx_id, tx1_id);
    }

    #[tokio::test]
    async fn test_stale_tx_record_aborted_on_retry() {
        let fixture = MultiTableFixture::new().await;
        let storage = Arc::new(fixture.storage);
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));
        let tx_store = TransactionStoreImpl::new(Arc::clone(&storage));

        let tx_id = Uuid::now_v7().to_string();
        let mut stale_record = arco_iceberg::transactions::TransactionRecord::new_preparing(
            tx_id.clone(),
            "hash".to_string(),
        );
        stale_record.started_at = chrono::Utc::now() - Duration::minutes(20);
        tx_store
            .create(&stale_record)
            .await
            .expect("create stale tx");

        let config = CoordinatorConfig {
            max_tables: 10,
            transaction_timeout: Duration::minutes(15),
        };
        let coordinator = MultiTableTransactionCoordinator::with_config(
            Arc::clone(&storage),
            Arc::clone(&pointer_store),
            config,
        );

        let inputs = vec![table_commit_input(
            fixture.table1_uuid,
            "sales",
            "orders",
            1001,
        )];
        let result = coordinator
            .commit(
                tx_id.clone(),
                "hash".to_string(),
                inputs,
                UpdateSource::IcebergRest {
                    client_info: None,
                    principal: None,
                },
                &fixture.tenant,
                &fixture.workspace,
            )
            .await;

        assert!(
            matches!(
                result,
                Err(MultiTableCommitError::RetryAfter { seconds: 1 })
            ),
            "Expected RetryAfter(1) after aborting stale tx, got: {result:?}"
        );

        let (aborted_record, _) = tx_store.load(&tx_id).await.expect("load").expect("exists");
        assert_eq!(
            aborted_record.status,
            arco_iceberg::transactions::TransactionStatus::Aborted
        );
    }

    #[tokio::test]
    async fn test_committed_pending_finalized_on_prepare() {
        let fixture = MultiTableFixture::new().await;
        let storage = Arc::new(fixture.storage);
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));
        let tx_store = TransactionStoreImpl::new(Arc::clone(&storage));

        let old_tx_id = Uuid::now_v7().to_string();
        let committed_record = arco_iceberg::transactions::TransactionRecord::new_preparing(
            old_tx_id.clone(),
            "hash".to_string(),
        )
        .commit();
        tx_store
            .create(&committed_record)
            .await
            .expect("create committed tx");

        let new_metadata_location = format!(
            "tenant={}/workspace={}/warehouse/orders/metadata/00001-{}.metadata.json",
            fixture.tenant, fixture.workspace, old_tx_id
        );
        let (pointer, pointer_version) = pointer_store
            .load(&fixture.table1_uuid)
            .await
            .expect("load")
            .expect("pointer exists");
        let pointer_with_pending =
            pointer
                .clone()
                .with_pending(arco_iceberg::pointer::PendingPointerUpdate {
                    tx_id: old_tx_id.clone(),
                    metadata_location: new_metadata_location.clone(),
                    snapshot_id: Some(999),
                    last_sequence_number: 1,
                    prepared_at: chrono::Utc::now(),
                });
        pointer_store
            .compare_and_swap(
                &fixture.table1_uuid,
                &pointer_version,
                &pointer_with_pending,
            )
            .await
            .expect("set pending");

        let coordinator =
            MultiTableTransactionCoordinator::new(Arc::clone(&storage), Arc::clone(&pointer_store));
        let new_tx_id = Uuid::now_v7().to_string();
        let inputs = vec![table_commit_input(
            fixture.table1_uuid,
            "sales",
            "orders",
            2001,
        )];

        let result = coordinator
            .commit(
                new_tx_id,
                "new_hash".to_string(),
                inputs,
                UpdateSource::IcebergRest {
                    client_info: None,
                    principal: None,
                },
                &fixture.tenant,
                &fixture.workspace,
            )
            .await;

        assert!(
            matches!(
                result,
                Err(MultiTableCommitError::RetryAfter { seconds: 1 })
            ),
            "Expected RetryAfter(1) after finalizing committed pending, got: {result:?}"
        );

        let (finalized_pointer, _) = pointer_store
            .load(&fixture.table1_uuid)
            .await
            .expect("load")
            .expect("pointer exists");
        assert!(finalized_pointer.pending.is_none());
        assert_eq!(
            finalized_pointer.current_metadata_location,
            new_metadata_location
        );
    }

    #[tokio::test]
    async fn test_finalize_only_affects_current_tx_id() {
        let fixture = MultiTableFixture::new().await;
        let storage = Arc::new(fixture.storage);
        let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));
        let tx_store = TransactionStoreImpl::new(Arc::clone(&storage));

        let tx1_id = Uuid::now_v7().to_string();
        let tx1_record = arco_iceberg::transactions::TransactionRecord::new_preparing(
            tx1_id.clone(),
            "hash1".to_string(),
        );
        tx_store.create(&tx1_record).await.expect("create tx1");

        let tx1_pending_location = format!(
            "tenant={}/workspace={}/warehouse/orders/metadata/00001-{}.metadata.json",
            fixture.tenant, fixture.workspace, tx1_id
        );
        let (pointer, pointer_version) = pointer_store
            .load(&fixture.table1_uuid)
            .await
            .expect("load")
            .expect("pointer exists");
        let original_location = pointer.current_metadata_location.clone();
        let pointer_with_pending =
            pointer
                .clone()
                .with_pending(arco_iceberg::pointer::PendingPointerUpdate {
                    tx_id: tx1_id.clone(),
                    metadata_location: tx1_pending_location.clone(),
                    snapshot_id: Some(1001),
                    last_sequence_number: 1,
                    prepared_at: chrono::Utc::now(),
                });
        pointer_store
            .compare_and_swap(
                &fixture.table1_uuid,
                &pointer_version,
                &pointer_with_pending,
            )
            .await
            .expect("set pending");

        let coordinator =
            MultiTableTransactionCoordinator::new(Arc::clone(&storage), Arc::clone(&pointer_store));
        let tx2_id = Uuid::now_v7().to_string();
        let inputs = vec![table_commit_input(
            fixture.table2_uuid,
            "sales",
            "customers",
            2001,
        )];

        let _result = coordinator
            .commit(
                tx2_id,
                "hash2".to_string(),
                inputs,
                UpdateSource::IcebergRest {
                    client_info: None,
                    principal: None,
                },
                &fixture.tenant,
                &fixture.workspace,
            )
            .await
            .expect("commit tx2");

        let (table1_pointer, _) = pointer_store
            .load(&fixture.table1_uuid)
            .await
            .expect("load")
            .expect("pointer exists");

        assert!(
            table1_pointer.pending.is_some(),
            "TX1's pending should NOT be finalized by TX2"
        );
        assert_eq!(table1_pointer.pending.as_ref().unwrap().tx_id, tx1_id);
        assert_eq!(table1_pointer.current_metadata_location, original_location);
    }
}
