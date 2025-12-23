//! Iceberg REST catalog integration tests using the iceberg-rust client.

use std::collections::HashMap;
use std::future::IntoFuture;
use std::sync::Arc;

use arco_catalog::write_options::WriteOptions;
use arco_catalog::{CatalogWriter, RegisterTableRequest, Tier1Compactor};
use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition};
use arco_core::ScopedStorage;
use arco_iceberg::pointer::IcebergTablePointer;
use arco_iceberg::router::iceberg_router;
use arco_iceberg::state::IcebergState;
use arco_iceberg::types::{
    PartitionSpec, Schema, SchemaField, SortOrder, TableMetadata, TableUuid,
};
use bytes::Bytes;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

async fn seed_iceberg_table(state: &IcebergState, namespace: &str, table: &str) {
    let storage = ScopedStorage::new(
        Arc::clone(&state.storage),
        "acme",
        "analytics",
    )
    .expect("scoped storage");
    let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);
    writer.initialize().await.expect("init");
    writer
        .create_namespace(namespace, None, WriteOptions::default())
        .await
        .expect("create namespace");

    let table_name = table.to_string();
    let table_record = writer
        .register_table(
            RegisterTableRequest {
                namespace: namespace.to_string(),
                name: table_name.clone(),
                description: None,
                location: Some(format!(
                    "tenant=acme/workspace=analytics/warehouse/{table_name}"
                )),
                format: Some("iceberg".to_string()),
                columns: vec![],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register table");

    let table_uuid = uuid::Uuid::parse_str(&table_record.id).expect("table uuid");
    let metadata_location = format!(
        "tenant=acme/workspace=analytics/warehouse/{table_name}/metadata/00000.metadata.json"
    );
    let pointer = IcebergTablePointer::new(table_uuid, metadata_location.clone());
    let pointer_path = IcebergTablePointer::storage_path(&table_uuid);
    let pointer_bytes = serde_json::to_vec(&pointer).expect("serialize pointer");
    storage
        .put_raw(
            &pointer_path,
            Bytes::from(pointer_bytes),
            WritePrecondition::None,
        )
        .await
        .expect("put pointer");

    let metadata = TableMetadata {
        format_version: 2,
        table_uuid: TableUuid::new(table_uuid),
        location: format!("tenant=acme/workspace=analytics/warehouse/{table_name}"),
        last_sequence_number: 0,
        last_updated_ms: 1_234_567_890_000,
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
    };
    let metadata_bytes = serde_json::to_vec(&metadata).expect("serialize metadata");
    storage
        .put_raw(
            &format!("warehouse/{table_name}/metadata/00000.metadata.json"),
            Bytes::from(metadata_bytes),
            WritePrecondition::None,
        )
        .await
        .expect("put metadata");
}

#[tokio::test]
async fn test_rest_catalog_roundtrip() {
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let state = IcebergState::new(Arc::clone(&storage));
    seed_iceberg_table(&state, "sales", "orders").await;

    let app = axum::Router::new().nest("/iceberg", iceberg_router(state));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        let _ = shutdown_rx.await;
    });
    let server_handle = tokio::spawn(server.into_future());

    let mut props = HashMap::new();
    props.insert("header.x-tenant-id".to_string(), "acme".to_string());
    props.insert(
        "header.x-workspace-id".to_string(),
        "analytics".to_string(),
    );

    let config = RestCatalogConfig::builder()
        .uri(format!("http://{addr}/iceberg"))
        .warehouse("memory://localhost".to_string())
        .props(props)
        .build();
    let catalog = RestCatalog::new(config);

    let namespace = NamespaceIdent::from_strs(["sales"]).expect("namespace");
    let table_ident = TableIdent::new(namespace.clone(), "orders".to_string());

    let namespaces = catalog
        .list_namespaces(None)
        .await
        .expect("list namespaces");
    assert!(namespaces.contains(&namespace));

    let tables = catalog
        .list_tables(&namespace)
        .await
        .expect("list tables");
    assert!(tables.contains(&table_ident));

    let table = catalog
        .load_table(&table_ident)
        .await
        .expect("load table");
    assert_eq!(table.identifier(), &table_ident);

    let _ = shutdown_tx.send(());
    server_handle
        .await
        .expect("server task")
        .expect("server shutdown");
}
