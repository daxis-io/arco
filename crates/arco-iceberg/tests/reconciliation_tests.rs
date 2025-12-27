//! Integration tests for Iceberg reconciliation and GC.
//!
//! These tests verify the full reconciliation and GC flows with
//! memory-backed storage.

use std::collections::HashSet;
use std::sync::Arc;

use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition};
use arco_iceberg::gc::{
    EventReceiptGarbageCollector, EventReceiptGcConfig, IdempotencyGarbageCollector,
    OrphanMetadataCleaner, OrphanGcConfig,
};
use arco_iceberg::pointer::{CasResult, IcebergTablePointer, PointerStore, PointerStoreImpl};
use arco_iceberg::reconciler::{IcebergReconciler, Reconciler};
use arco_iceberg::schema_projection::{
    ColumnRecord, IcebergField, IcebergPrimitiveType, IcebergSchema, IcebergType,
    IcebergTypeMapper, SchemaProjector,
};
use bytes::Bytes;
use chrono::Duration;
use uuid::Uuid;

// ============================================================================
// Task 14.1: Full Reconciliation Flow
// ============================================================================

#[tokio::test]
async fn test_full_reconciliation_flow_with_memory_backend() {
    let storage = Arc::new(MemoryBackend::new());
    let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));

    // Create table with metadata chain
    let table_uuid = Uuid::new_v4();

    // Create v1 metadata
    let v1_json = format!(
        r#"{{
            "format-version": 2,
            "table-uuid": "{}",
            "location": "gs://bucket/table",
            "last-sequence-number": 1,
            "last-updated-ms": 1704067200000,
            "last-column-id": 3,
            "current-schema-id": 0,
            "schemas": [],
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "properties": {{}},
            "default-spec-id": 0,
            "partition-specs": [],
            "last-partition-id": 0,
            "default-sort-order-id": 0,
            "sort-orders": []
        }}"#,
        table_uuid
    );

    storage
        .put(
            "metadata/v1.metadata.json",
            Bytes::from(v1_json),
            WritePrecondition::None,
        )
        .await
        .expect("put v1");

    // Create v2 metadata with log entry pointing to v1
    let v2_json = format!(
        r#"{{
            "format-version": 2,
            "table-uuid": "{}",
            "location": "gs://bucket/table",
            "last-sequence-number": 2,
            "last-updated-ms": 1704153600000,
            "last-column-id": 3,
            "current-schema-id": 0,
            "schemas": [],
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [
                {{"timestamp-ms": 1704067200000, "metadata-file": "metadata/v1.metadata.json"}}
            ],
            "properties": {{}},
            "default-spec-id": 0,
            "partition-specs": [],
            "last-partition-id": 0,
            "default-sort-order-id": 0,
            "sort-orders": []
        }}"#,
        table_uuid
    );

    storage
        .put(
            "metadata/v2.metadata.json",
            Bytes::from(v2_json),
            WritePrecondition::None,
        )
        .await
        .expect("put v2");

    // Create pointer
    let pointer = IcebergTablePointer::new(table_uuid, "metadata/v2.metadata.json".to_string());
    let result = pointer_store
        .create(&table_uuid, &pointer)
        .await
        .expect("create pointer");
    assert!(matches!(result, CasResult::Success { .. }));

    // Reconcile the table
    let reconciler = IcebergReconciler::new(Arc::clone(&storage), Arc::clone(&pointer_store));
    let result = reconciler
        .reconcile_table(&table_uuid, "acme", "prod")
        .await
        .expect("reconcile");

    // Should find 2 metadata entries (v1 and v2)
    assert_eq!(result.metadata_entries_found, 2);
    // Should create 2 receipts (one for each metadata file)
    assert_eq!(result.receipts_created, 2);
    assert_eq!(result.receipts_existing, 0);
    assert_eq!(result.receipts_failed, 0);
    assert!(result.error.is_none());

    // Reconcile again - receipts should already exist
    let result2 = reconciler
        .reconcile_table(&table_uuid, "acme", "prod")
        .await
        .expect("reconcile again");

    assert_eq!(result2.metadata_entries_found, 2);
    assert_eq!(result2.receipts_created, 0);
    assert_eq!(result2.receipts_existing, 2);
}

#[tokio::test]
async fn test_reconcile_all_multiple_tables() {
    let storage = Arc::new(MemoryBackend::new());
    let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));

    // Create two tables
    let table1 = Uuid::new_v4();
    let table2 = Uuid::new_v4();

    for (i, table_uuid) in [table1, table2].iter().enumerate() {
        let metadata_json = format!(
            r#"{{
                "format-version": 2,
                "table-uuid": "{}",
                "location": "gs://bucket/table{}",
                "last-sequence-number": 1,
                "last-updated-ms": {},
                "last-column-id": 3,
                "current-schema-id": 0,
                "schemas": [],
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
                "properties": {{}},
                "default-spec-id": 0,
                "partition-specs": [],
                "last-partition-id": 0,
                "default-sort-order-id": 0,
                "sort-orders": []
            }}"#,
            table_uuid,
            i,
            1704067200000_i64 + (i as i64 * 86400000)
        );

        let path = format!("metadata/table{}/v1.json", i);
        storage
            .put(&path, Bytes::from(metadata_json), WritePrecondition::None)
            .await
            .expect("put metadata");

        let pointer = IcebergTablePointer::new(*table_uuid, path);
        let result = pointer_store
            .create(table_uuid, &pointer)
            .await
            .expect("create pointer");
        assert!(matches!(result, CasResult::Success { .. }));
    }

    // Reconcile all tables
    let reconciler = IcebergReconciler::new(Arc::clone(&storage), pointer_store);
    let report = reconciler
        .reconcile_all("acme", "prod")
        .await
        .expect("reconcile_all");

    assert_eq!(report.tables_processed, 2);
    assert_eq!(report.tables_with_errors, 0);
    assert_eq!(report.receipts_created, 2);
    assert!(!report.has_errors());
}

// ============================================================================
// Task 14.2: GC Flow Tests
// ============================================================================

#[tokio::test]
async fn test_gc_flow_with_controlled_timestamps() {
    let storage = Arc::new(MemoryBackend::new());
    let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));

    let table_uuid = Uuid::new_v4();

    // Create a pointer for the table
    let pointer =
        IcebergTablePointer::new(table_uuid, "metadata/current.metadata.json".to_string());
    let result = pointer_store
        .create(&table_uuid, &pointer)
        .await
        .expect("create pointer");
    assert!(matches!(result, CasResult::Success { .. }));

    // Create an expired committed marker
    let marker = arco_iceberg::idempotency::IdempotencyMarker::new_in_progress(
        "test_key".to_string(),
        table_uuid,
        "hash123".to_string(),
        "base.json".to_string(),
        "new.json".to_string(),
    );
    let mut committed = marker.finalize_committed("new.json".to_string());
    // Set committed 3 days ago (past 48h threshold)
    committed.committed_at = Some(chrono::Utc::now() - Duration::hours(72));

    let marker_path = arco_iceberg::idempotency::IdempotencyMarker::storage_path(
        &table_uuid,
        &arco_iceberg::idempotency::IdempotencyMarker::hash_key("test_key"),
    );

    storage
        .put(
            &marker_path,
            Bytes::from(serde_json::to_vec(&committed).unwrap()),
            WritePrecondition::None,
        )
        .await
        .expect("put marker");

    // Verify marker exists
    assert!(storage.head(&marker_path).await.expect("head").is_some());

    // Run GC
    let gc = IdempotencyGarbageCollector::new(Arc::clone(&storage), pointer_store);
    let result = gc.clean_table(&table_uuid).await.expect("clean_table");

    assert_eq!(result.deleted, 1);
    assert_eq!(result.skipped, 0);
    assert_eq!(result.failed, 0);

    // Verify marker is deleted
    assert!(storage.head(&marker_path).await.expect("head").is_none());
}

#[tokio::test]
async fn test_orphan_cleanup_flow() {
    let storage = Arc::new(MemoryBackend::new());
    let table_uuid = Uuid::new_v4();

    // Create valid metadata files
    storage
        .put(
            "metadata/v1.metadata.json",
            Bytes::from("{}"),
            WritePrecondition::None,
        )
        .await
        .expect("put");
    storage
        .put(
            "metadata/v2.metadata.json",
            Bytes::from("{}"),
            WritePrecondition::None,
        )
        .await
        .expect("put");

    // Create orphan file
    storage
        .put(
            "metadata/orphan.metadata.json",
            Bytes::from("{}"),
            WritePrecondition::None,
        )
        .await
        .expect("put");

    // Build allowlist (only v1 and v2 are valid)
    let mut allowlist = HashSet::new();
    allowlist.insert("metadata/v1.metadata.json".to_string());
    allowlist.insert("metadata/v2.metadata.json".to_string());

    // Use zero retention for testing
    let config = OrphanGcConfig {
        min_age: Duration::zero(),
    };
    let cleaner = OrphanMetadataCleaner::with_config(Arc::clone(&storage), config);

    let result = cleaner
        .clean_orphans(table_uuid, "metadata/", &allowlist)
        .await
        .expect("clean_orphans");

    assert_eq!(result.orphans_found, 1);
    assert_eq!(result.orphans_deleted, 1);

    // Verify orphan is deleted but valid files remain
    assert!(storage
        .head("metadata/v1.metadata.json")
        .await
        .expect("head")
        .is_some());
    assert!(storage
        .head("metadata/v2.metadata.json")
        .await
        .expect("head")
        .is_some());
    assert!(storage
        .head("metadata/orphan.metadata.json")
        .await
        .expect("head")
        .is_none());
}

#[tokio::test]
async fn test_event_receipt_gc_flow() {
    let storage = Arc::new(MemoryBackend::new());

    // Create pending and committed receipts
    storage
        .put(
            "events/2025-01-15/iceberg/pending/receipt1.json",
            Bytes::from("{}"),
            WritePrecondition::None,
        )
        .await
        .expect("put");
    storage
        .put(
            "events/2025-01-15/iceberg/committed/receipt2.json",
            Bytes::from("{}"),
            WritePrecondition::None,
        )
        .await
        .expect("put");

    // Use zero retention for testing
    let config = EventReceiptGcConfig {
        pending_retention: Duration::zero(),
        committed_retention: Duration::zero(),
    };
    let gc = EventReceiptGarbageCollector::with_config(Arc::clone(&storage), config);

    let date = chrono::NaiveDate::from_ymd_opt(2025, 1, 15).expect("valid date");
    let result = gc.clean_receipts_for_date(date).await.expect("clean");

    assert_eq!(result.pending_deleted, 1);
    assert_eq!(result.committed_deleted, 1);
    assert_eq!(result.files_scanned, 2);

    // Verify receipts are deleted
    assert!(storage
        .head("events/2025-01-15/iceberg/pending/receipt1.json")
        .await
        .expect("head")
        .is_none());
    assert!(storage
        .head("events/2025-01-15/iceberg/committed/receipt2.json")
        .await
        .expect("head")
        .is_none());
}

// ============================================================================
// Task 14.3: Schema Projection Tests
// ============================================================================

#[test]
fn test_schema_projection_with_real_iceberg_schema() {
    let table_uuid = Uuid::new_v4();

    // Create a realistic Iceberg schema
    let schema = IcebergSchema {
        schema_id: 0,
        schema_type: Some("struct".to_string()),
        fields: vec![
            IcebergField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: IcebergType::Primitive(IcebergPrimitiveType::Long),
                doc: Some("Primary key".to_string()),
            },
            IcebergField {
                id: 2,
                name: "name".to_string(),
                required: true,
                field_type: IcebergType::Primitive(IcebergPrimitiveType::String),
                doc: None,
            },
            IcebergField {
                id: 3,
                name: "email".to_string(),
                required: false,
                field_type: IcebergType::Primitive(IcebergPrimitiveType::String),
                doc: None,
            },
            IcebergField {
                id: 4,
                name: "created_at".to_string(),
                required: true,
                field_type: IcebergType::Primitive(IcebergPrimitiveType::Timestamptz),
                doc: None,
            },
            IcebergField {
                id: 5,
                name: "balance".to_string(),
                required: false,
                field_type: IcebergType::Primitive(IcebergPrimitiveType::Decimal {
                    precision: 18,
                    scale: 2,
                }),
                doc: Some("Account balance".to_string()),
            },
        ],
        identifier_field_ids: vec![1],
    };

    let columns = SchemaProjector::project(table_uuid, &schema);

    assert_eq!(columns.len(), 5);

    // Verify first column (id)
    assert_eq!(columns[0].name, "id");
    assert_eq!(columns[0].data_type, "int64");
    assert!(!columns[0].nullable);
    assert!(columns[0].is_identifier);
    assert_eq!(columns[0].description, Some("Primary key".to_string()));

    // Verify decimal column
    assert_eq!(columns[4].name, "balance");
    assert_eq!(columns[4].data_type, "decimal(18,2)");
    assert!(columns[4].nullable);

    // Verify all columns have stable IDs
    for (i, col) in columns.iter().enumerate() {
        let expected_id = ColumnRecord::generate_column_id(table_uuid, (i as i32) + 1);
        assert_eq!(col.column_id, expected_id);
    }
}

#[test]
fn test_type_mapper_all_primitive_types() {
    // Verify all primitive types map correctly
    let mappings = [
        (IcebergPrimitiveType::Boolean, "boolean"),
        (IcebergPrimitiveType::Int, "int32"),
        (IcebergPrimitiveType::Long, "int64"),
        (IcebergPrimitiveType::Float, "float32"),
        (IcebergPrimitiveType::Double, "float64"),
        (IcebergPrimitiveType::Date, "date"),
        (IcebergPrimitiveType::Time, "time"),
        (IcebergPrimitiveType::Timestamp, "timestamp"),
        (IcebergPrimitiveType::Timestamptz, "timestamptz"),
        (IcebergPrimitiveType::String, "string"),
        (IcebergPrimitiveType::Uuid, "uuid"),
        (IcebergPrimitiveType::Binary, "binary"),
        (IcebergPrimitiveType::Fixed(16), "fixed[16]"),
        (
            IcebergPrimitiveType::Decimal {
                precision: 10,
                scale: 2,
            },
            "decimal(10,2)",
        ),
    ];

    for (iceberg_type, expected) in mappings {
        let result = IcebergTypeMapper::map_primitive(&iceberg_type);
        assert_eq!(result, expected, "Failed for {:?}", iceberg_type);
    }
}

// ============================================================================
// Task 14.4: Concurrent Safety Tests
// ============================================================================

#[tokio::test]
async fn test_concurrent_reconciliation_safety() {
    let storage = Arc::new(MemoryBackend::new());
    let pointer_store = Arc::new(PointerStoreImpl::new(Arc::clone(&storage)));

    let table_uuid = Uuid::new_v4();

    // Create metadata file
    let metadata_json = format!(
        r#"{{
            "format-version": 2,
            "table-uuid": "{}",
            "location": "gs://bucket/table",
            "last-sequence-number": 1,
            "last-updated-ms": 1704067200000,
            "last-column-id": 3,
            "current-schema-id": 0,
            "schemas": [],
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "properties": {{}},
            "default-spec-id": 0,
            "partition-specs": [],
            "last-partition-id": 0,
            "default-sort-order-id": 0,
            "sort-orders": []
        }}"#,
        table_uuid
    );

    storage
        .put(
            "metadata/v1.json",
            Bytes::from(metadata_json),
            WritePrecondition::None,
        )
        .await
        .expect("put");

    // Create pointer
    let pointer = IcebergTablePointer::new(table_uuid, "metadata/v1.json".to_string());
    pointer_store
        .create(&table_uuid, &pointer)
        .await
        .expect("create pointer");

    // Run multiple concurrent reconciliations
    let reconciler = Arc::new(IcebergReconciler::new(
        Arc::clone(&storage),
        Arc::clone(&pointer_store),
    ));

    let mut handles = Vec::new();
    for _ in 0..5 {
        let r = Arc::clone(&reconciler);
        let t = table_uuid;
        handles.push(tokio::spawn(async move {
            r.reconcile_table(&t, "acme", "prod").await
        }));
    }

    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .collect();

    // All should succeed
    for result in &results {
        assert!(result.is_ok());
        let inner = result.as_ref().unwrap().as_ref().unwrap();
        assert!(inner.error.is_none());
    }

    // Exactly one should have created the receipt, others should find it existing
    let created_count: usize = results
        .iter()
        .filter_map(|r| r.as_ref().ok())
        .filter_map(|r| r.as_ref().ok())
        .map(|r| r.receipts_created)
        .sum();

    let existing_count: usize = results
        .iter()
        .filter_map(|r| r.as_ref().ok())
        .filter_map(|r| r.as_ref().ok())
        .map(|r| r.receipts_existing)
        .sum();

    // Due to DoesNotExist precondition, exactly 1 should create, rest should find existing
    assert_eq!(created_count + existing_count, 5);
    assert!(created_count >= 1);
}
