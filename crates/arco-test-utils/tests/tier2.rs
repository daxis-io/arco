//! Tier 2 walking skeleton: append event -> compact -> read from Parquet.
//!
//! This is the minimum viable proof that Tier 2 consistency model works.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::Array;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::ops::Range;
use std::time::Duration;
use tokio::sync::Barrier;

use arco_catalog::{Compactor, EventWriter, MaterializationRecord, Tier1Writer};
use arco_core::Result as CoreResult;
use arco_core::scoped_storage::ScopedStorage;
use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use arco_test_utils::{StorageOp, TracingMemoryBackend};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MaterializationCompleted {
    materialization_id: String,
    asset_id: String,
    row_count: i64,
    byte_size: i64,
}

fn records_from_batches(batches: &[RecordBatch]) -> Vec<MaterializationRecord> {
    let mut records = Vec::new();

    for batch in batches {
        let schema = batch.schema();
        let materialization_idx = schema
            .index_of("materialization_id")
            .expect("materialization_id column");
        let asset_idx = schema.index_of("asset_id").expect("asset_id column");
        let row_count_idx = schema.index_of("row_count").expect("row_count column");
        let byte_size_idx = schema.index_of("byte_size").expect("byte_size column");

        let materialization_ids = batch
            .column(materialization_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("materialization_id type");
        let asset_ids = batch
            .column(asset_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("asset_id type");
        let row_counts = batch
            .column(row_count_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("row_count type");
        let byte_sizes = batch
            .column(byte_size_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("byte_size type");

        for row in 0..batch.num_rows() {
            records.push(MaterializationRecord {
                materialization_id: materialization_ids.value(row).to_string(),
                asset_id: asset_ids.value(row).to_string(),
                row_count: row_counts.value(row),
                byte_size: byte_sizes.value(row),
            });
        }
    }

    records
}

async fn read_snapshot_records(
    storage: &ScopedStorage,
    snapshot_path: &str,
) -> Vec<MaterializationRecord> {
    let parquet_data = storage.get_raw(snapshot_path).await.expect("read snapshot");
    let reader = ParquetRecordBatchReaderBuilder::try_new(parquet_data)
        .expect("valid parquet")
        .build()
        .expect("build reader");
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("read batches");
    records_from_batches(&batches)
}

fn parquet_bytes(records: &[MaterializationRecord]) -> Bytes {
    let materialization_ids: Vec<_> = records
        .iter()
        .map(|r| r.materialization_id.as_str())
        .collect();
    let asset_ids: Vec<_> = records.iter().map(|r| r.asset_id.as_str()).collect();
    let row_counts: Vec<_> = records.iter().map(|r| r.row_count).collect();
    let byte_sizes: Vec<_> = records.iter().map(|r| r.byte_size).collect();

    let schema = Schema::new(vec![
        Field::new("materialization_id", DataType::Utf8, false),
        Field::new("asset_id", DataType::Utf8, false),
        Field::new("row_count", DataType::Int64, false),
        Field::new("byte_size", DataType::Int64, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(materialization_ids)),
            Arc::new(StringArray::from(asset_ids)),
            Arc::new(Int64Array::from(row_counts)),
            Arc::new(Int64Array::from(byte_sizes)),
        ],
    )
    .expect("record batch");

    let mut buffer = Cursor::new(Vec::new());
    let props = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(&mut buffer, Arc::new(schema), Some(props)).expect("parquet writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    Bytes::from(buffer.into_inner())
}

#[derive(Debug)]
struct BarrierBackend {
    inner: TracingMemoryBackend,
    cas_barrier: Arc<Barrier>,
}

impl BarrierBackend {
    fn new(parties: usize) -> Self {
        Self {
            inner: TracingMemoryBackend::new(),
            cas_barrier: Arc::new(Barrier::new(parties)),
        }
    }

    fn is_execution_manifest_path(path: &str) -> bool {
        path.ends_with("/manifests/execution.manifest.json")
    }
}

#[async_trait::async_trait]
impl StorageBackend for BarrierBackend {
    async fn get(&self, path: &str) -> CoreResult<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> CoreResult<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> CoreResult<WriteResult> {
        if Self::is_execution_manifest_path(path)
            && matches!(precondition, WritePrecondition::MatchesVersion(_))
        {
            self.cas_barrier.wait().await;
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> CoreResult<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> CoreResult<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> CoreResult<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> CoreResult<String> {
        self.inner.signed_url(path, expiry).await
    }
}

#[tokio::test]
async fn tier2_append_compact_read_loop() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    // 1. Initialize catalog
    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    // 2. Append event to ledger
    let event_writer = EventWriter::new(storage.clone());
    let event = MaterializationCompleted {
        materialization_id: "mat_001".into(),
        asset_id: "asset_abc".into(),
        row_count: 1000,
        byte_size: 50000,
    };

    let event_id = event_writer
        .append("execution", &event)
        .await
        .expect("append");

    assert!(!event_id.is_empty());

    // 3. Verify event in ledger
    let ledger_files = storage.list("ledger/execution/").await.expect("list");
    assert!(!ledger_files.is_empty());

    // 4. Compact
    let compactor = Compactor::new(storage.clone());
    let result = compactor
        .compact_domain("execution")
        .await
        .expect("compact");

    assert!(result.events_processed > 0);
    assert!(result.parquet_files_written > 0);

    // 5. Verify watermark updated
    let manifest = writer.read_manifest().await.expect("read");
    assert!(manifest.execution.watermark_version > 0);

    // 6. Read snapshot via manifest pointer (Invariant 4 compliance)
    let snapshot_path = manifest
        .execution
        .snapshot_path
        .as_ref()
        .expect("snapshot_path");
    assert!(snapshot_path.ends_with(".parquet"));

    let parquet_data = storage.get_raw(snapshot_path).await.expect("read parquet");
    // bytes::Bytes implements ChunkReader
    let reader = ParquetRecordBatchReaderBuilder::try_new(parquet_data)
        .expect("valid parquet")
        .build()
        .expect("build reader");

    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("read batches");
    assert!(!batches.is_empty(), "Parquet should contain data");

    // Verify expected row count
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 1,
        "should have exactly 1 materialization record"
    );

    // Verify expected columns exist
    let schema = batches[0].schema();
    assert!(
        schema.field_with_name("materialization_id").is_ok(),
        "schema should have materialization_id column"
    );
    assert!(
        schema.field_with_name("asset_id").is_ok(),
        "schema should have asset_id column"
    );
}

#[tokio::test]
async fn tier2_out_of_order_processing() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());

    let id_low = "01ARZ3NDEKTSV4RRFFQ69G5FAA";
    let id_mid = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let id_high = "01ARZ3NDEKTSV4RRFFQ69G5FAZ";

    // Write newest first, then oldest, then middle (out-of-order arrival).
    event_writer
        .append_with_id(
            "execution",
            &MaterializationRecord {
                materialization_id: "mat_001".into(),
                asset_id: "asset_abc".into(),
                row_count: 300,
                byte_size: 3000,
            },
            id_high,
        )
        .await
        .expect("append high");
    event_writer
        .append_with_id(
            "execution",
            &MaterializationRecord {
                materialization_id: "mat_001".into(),
                asset_id: "asset_abc".into(),
                row_count: 100,
                byte_size: 1000,
            },
            id_low,
        )
        .await
        .expect("append low");
    event_writer
        .append_with_id(
            "execution",
            &MaterializationRecord {
                materialization_id: "mat_001".into(),
                asset_id: "asset_abc".into(),
                row_count: 200,
                byte_size: 2000,
            },
            id_mid,
        )
        .await
        .expect("append mid");

    let compactor = Compactor::new(storage.clone());
    let result = compactor
        .compact_domain("execution")
        .await
        .expect("compact");
    assert_eq!(result.events_processed, 3);

    let manifest = tier1.read_manifest().await.expect("read");
    let expected_watermark = format!("{id_high}.json");
    assert_eq!(
        manifest.execution.watermark_event_id.as_deref(),
        Some(expected_watermark.as_str()),
        "watermark should track max filename, not arrival order"
    );

    let snapshot_path = manifest.execution.snapshot_path.expect("snapshot_path");
    let records = read_snapshot_records(&storage, &snapshot_path).await;
    assert_eq!(records.len(), 1, "upsert semantics: one key");
    assert_eq!(
        records[0].row_count, 300,
        "latest ULID should win regardless of arrival order"
    );
}

#[tokio::test]
async fn tier2_idempotent_events() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());
    let event = MaterializationCompleted {
        materialization_id: "mat_001".into(),
        asset_id: "asset_abc".into(),
        row_count: 1000,
        byte_size: 50000,
    };

    let event_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    // Write same event 10 times (DoesNotExist prevents overwrites)
    // Per exit criteria: "Idempotency verified (10x)"
    for _ in 0..10 {
        event_writer
            .append_with_id("execution", &event, event_id)
            .await
            .expect("append");
    }

    // Should only have one file (DoesNotExist precondition)
    let files = storage.list("ledger/execution/").await.expect("list");
    assert_eq!(
        files.len(),
        1,
        "append-only: duplicate IDs don't create new files"
    );

    // Compact should process only one event
    let compactor = Compactor::new(storage.clone());
    let result = compactor
        .compact_domain("execution")
        .await
        .expect("compact");
    assert_eq!(result.events_processed, 1);

    // Read Parquet and verify only 1 row (no duplicates in output)
    let manifest = writer.read_manifest().await.expect("read");
    let snapshot_path = manifest.execution.snapshot_path.expect("snapshot_path");
    let parquet_data = storage.get_raw(&snapshot_path).await.expect("read");
    let reader = ParquetRecordBatchReaderBuilder::try_new(parquet_data)
        .expect("valid parquet")
        .build()
        .expect("build reader");
    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().expect("read");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 1,
        "dedupe: only 1 row even with duplicate events"
    );
}

#[tokio::test]
async fn tier2_incremental_compaction() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());
    let compactor = Compactor::new(storage.clone());

    // Batch 1: 3 events
    for i in 0..3 {
        let event = MaterializationRecord {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer
            .append("execution", &event)
            .await
            .expect("append");
    }

    let r1 = compactor
        .compact_domain("execution")
        .await
        .expect("compact1");
    assert_eq!(r1.events_processed, 3);
    let manifest1 = writer.read_manifest().await.expect("read");
    let snapshot_path1 = manifest1.execution.snapshot_path.expect("snapshot_path");
    let records1 = read_snapshot_records(&storage, &snapshot_path1).await;
    assert_eq!(records1.len(), 3, "snapshot should contain full state");

    // Batch 2: 2 more events
    for i in 3..5 {
        let event = MaterializationRecord {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer
            .append("execution", &event)
            .await
            .expect("append");
    }

    let r2 = compactor
        .compact_domain("execution")
        .await
        .expect("compact2");
    assert_eq!(r2.events_processed, 2, "only new events");

    // Verify watermark incremented
    let manifest2 = writer.read_manifest().await.expect("read");
    assert!(manifest2.execution.watermark_version >= 2);

    let snapshot_path2 = manifest2.execution.snapshot_path.expect("snapshot_path");
    let records2 = read_snapshot_records(&storage, &snapshot_path2).await;
    assert_eq!(
        records2.len(),
        5,
        "full snapshot must not shrink across compactions"
    );
}

#[tokio::test]
async fn tier2_crash_recovery() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());
    let id1 = "01ARZ3NDEKTSV4RRFFQ69G5FAA";
    let id2 = "01ARZ3NDEKTSV4RRFFQ69G5FAZ";

    event_writer
        .append_with_id(
            "execution",
            &MaterializationRecord {
                materialization_id: "mat_001".into(),
                asset_id: "asset_abc".into(),
                row_count: 100,
                byte_size: 5000,
            },
            id1,
        )
        .await
        .expect("append1");
    event_writer
        .append_with_id(
            "execution",
            &MaterializationRecord {
                materialization_id: "mat_002".into(),
                asset_id: "asset_xyz".into(),
                row_count: 200,
                byte_size: 9000,
            },
            id2,
        )
        .await
        .expect("append2");

    let manifest_before = tier1.read_manifest().await.expect("read");
    assert!(
        manifest_before.execution.snapshot_path.is_none(),
        "precondition: no published snapshot yet"
    );

    // Simulate a crash after snapshot write but before manifest CAS: snapshot exists, manifest not updated.
    let snapshot_version = manifest_before.execution.snapshot_version + 1;
    let snapshot_path = format!("state/execution/snapshot_v{snapshot_version}_{id2}.parquet");
    let bytes = parquet_bytes(&[
        MaterializationRecord {
            materialization_id: "mat_001".into(),
            asset_id: "asset_abc".into(),
            row_count: 100,
            byte_size: 5000,
        },
        MaterializationRecord {
            materialization_id: "mat_002".into(),
            asset_id: "asset_xyz".into(),
            row_count: 200,
            byte_size: 9000,
        },
    ]);
    let put_result = storage
        .put_raw(&snapshot_path, bytes, WritePrecondition::DoesNotExist)
        .await
        .expect("write snapshot");
    assert!(
        matches!(put_result, WriteResult::Success { .. }),
        "snapshot write should succeed"
    );

    let compactor = Compactor::new(storage.clone());
    let result = compactor
        .compact_domain("execution")
        .await
        .expect("recover compaction");
    assert_eq!(
        result.parquet_files_written, 0,
        "recovery should reuse existing snapshot path"
    );

    let manifest_after = tier1.read_manifest().await.expect("read");
    assert_eq!(
        manifest_after.execution.snapshot_path.as_deref(),
        Some(snapshot_path.as_str())
    );

    let records = read_snapshot_records(&storage, &snapshot_path).await;
    assert_eq!(records.len(), 2);
}

#[tokio::test]
async fn tier2_concurrent_compactor_race() {
    let backend = Arc::new(BarrierBackend::new(2));
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());
    event_writer
        .append_with_id(
            "execution",
            &MaterializationRecord {
                materialization_id: "mat_001".into(),
                asset_id: "asset_abc".into(),
                row_count: 100,
                byte_size: 5000,
            },
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        )
        .await
        .expect("append");

    let compactor1 = Compactor::new(storage.clone());
    let compactor2 = Compactor::new(storage.clone());

    let (r1, r2) = tokio::join!(
        compactor1.compact_domain("execution"),
        compactor2.compact_domain("execution")
    );

    let ok = usize::from(r1.is_ok()) + usize::from(r2.is_ok());
    let cas_failed = match (r1, r2) {
        (Ok(_), Err(arco_catalog::CatalogError::CasFailed { .. }))
        | (Err(arco_catalog::CatalogError::CasFailed { .. }), Ok(_)) => true,
        _ => false,
    };

    assert_eq!(ok, 1, "exactly one compactor should win the CAS race");
    assert!(cas_failed, "loser should return CatalogError::CasFailed");
}

#[tokio::test]
async fn tier2_workspace_isolation() {
    let backend = Arc::new(MemoryBackend::new());

    let storage_a = ScopedStorage::new(backend.clone(), "acme", "production").expect("storage a");
    let storage_b = ScopedStorage::new(backend.clone(), "acme", "staging").expect("storage b");

    let tier1_a = Tier1Writer::new(storage_a.clone());
    tier1_a.initialize().await.expect("init a");
    let tier1_b = Tier1Writer::new(storage_b.clone());
    tier1_b.initialize().await.expect("init b");

    let event_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let event = MaterializationRecord {
        materialization_id: "mat_001".into(),
        asset_id: "asset_abc".into(),
        row_count: 100,
        byte_size: 5000,
    };

    EventWriter::new(storage_a.clone())
        .append_with_id("execution", &event, event_id)
        .await
        .expect("append a");
    EventWriter::new(storage_b.clone())
        .append_with_id("execution", &event, event_id)
        .await
        .expect("append b");

    // Each workspace sees only its own ledger.
    assert_eq!(
        storage_a
            .list("ledger/execution/")
            .await
            .expect("list a")
            .len(),
        1
    );
    assert_eq!(
        storage_b
            .list("ledger/execution/")
            .await
            .expect("list b")
            .len(),
        1
    );

    Compactor::new(storage_a.clone())
        .compact_domain("execution")
        .await
        .expect("compact a");

    let manifest_a = tier1_a.read_manifest().await.expect("read a");
    let manifest_b = tier1_b.read_manifest().await.expect("read b");
    assert!(manifest_a.execution.snapshot_path.is_some());
    assert!(
        manifest_b.execution.snapshot_path.is_none(),
        "compacting workspace A must not publish a snapshot for workspace B"
    );

    Compactor::new(storage_b.clone())
        .compact_domain("execution")
        .await
        .expect("compact b");
    let manifest_b2 = tier1_b.read_manifest().await.expect("read b2");
    assert!(manifest_b2.execution.snapshot_path.is_some());
}

#[tokio::test]
async fn tier2_derived_current_pointers() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());
    event_writer
        .append_with_id(
            "execution",
            &MaterializationRecord {
                materialization_id: "mat_001".into(),
                asset_id: "asset_abc".into(),
                row_count: 100,
                byte_size: 5000,
            },
            "01ARZ3NDEKTSV4RRFFQ69G5FAA",
        )
        .await
        .expect("append1");

    let compactor = Compactor::new(storage.clone());
    compactor
        .compact_domain("execution")
        .await
        .expect("compact1");

    let manifest1 = tier1.read_manifest().await.expect("read1");
    let snapshot1 = manifest1.execution.snapshot_path.expect("snapshot1");
    let version1 = manifest1.execution.snapshot_version;

    event_writer
        .append_with_id(
            "execution",
            &MaterializationRecord {
                materialization_id: "mat_002".into(),
                asset_id: "asset_xyz".into(),
                row_count: 200,
                byte_size: 9000,
            },
            "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
        )
        .await
        .expect("append2");

    compactor
        .compact_domain("execution")
        .await
        .expect("compact2");

    let manifest2 = tier1.read_manifest().await.expect("read2");
    let snapshot2 = manifest2.execution.snapshot_path.expect("snapshot2");
    let version2 = manifest2.execution.snapshot_version;

    assert!(version2 > version1);
    assert_ne!(snapshot1, snapshot2);

    let state_files = storage.list("state/execution/").await.expect("list state");
    let parquet_files = state_files
        .iter()
        .filter(|p| p.as_str().ends_with(".parquet"))
        .count();
    assert!(
        parquet_files >= 2,
        "state should retain historical immutable snapshots"
    );

    let records = read_snapshot_records(&storage, &snapshot2).await;
    assert_eq!(
        records.len(),
        2,
        "snapshot_path points to current full state"
    );
}

#[tokio::test]
async fn tier2_snapshot_does_not_shrink_across_compactions() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());
    let compactor = Compactor::new(storage.clone());

    event_writer
        .append_with_id(
            "execution",
            &MaterializationRecord {
                materialization_id: "mat_001".into(),
                asset_id: "asset_abc".into(),
                row_count: 100,
                byte_size: 5000,
            },
            "01ARZ3NDEKTSV4RRFFQ69G5FAA",
        )
        .await
        .expect("append mat_001 v1");
    event_writer
        .append_with_id(
            "execution",
            &MaterializationRecord {
                materialization_id: "mat_002".into(),
                asset_id: "asset_xyz".into(),
                row_count: 200,
                byte_size: 9000,
            },
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        )
        .await
        .expect("append mat_002");

    compactor
        .compact_domain("execution")
        .await
        .expect("compact1");
    let manifest1 = tier1.read_manifest().await.expect("read1");
    let snapshot1 = manifest1.execution.snapshot_path.expect("snapshot1");
    let records1 = read_snapshot_records(&storage, &snapshot1).await;
    assert_eq!(records1.len(), 2);

    // Update mat_001 only (newer ULID).
    event_writer
        .append_with_id(
            "execution",
            &MaterializationRecord {
                materialization_id: "mat_001".into(),
                asset_id: "asset_abc".into(),
                row_count: 999,
                byte_size: 9999,
            },
            "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
        )
        .await
        .expect("append mat_001 v2");

    compactor
        .compact_domain("execution")
        .await
        .expect("compact2");
    let manifest2 = tier1.read_manifest().await.expect("read2");
    let snapshot2 = manifest2.execution.snapshot_path.expect("snapshot2");
    let records2 = read_snapshot_records(&storage, &snapshot2).await;

    assert_eq!(
        records2.len(),
        2,
        "full snapshot must retain unaffected records"
    );
    let mat_001 = records2
        .iter()
        .find(|r| r.materialization_id == "mat_001")
        .expect("mat_001 present");
    assert_eq!(mat_001.row_count, 999);
    assert!(
        records2.iter().any(|r| r.materialization_id == "mat_002"),
        "mat_002 should still be present after updating mat_001"
    );
}

#[tokio::test]
async fn tier2_compactor_sole_parquet_writer() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    // Before: no Parquet
    let before = storage.list("state/execution/").await.unwrap_or_default();
    assert!(before.is_empty());

    // Write events (EventWriter only writes JSON)
    let event_writer = EventWriter::new(storage.clone());
    for i in 0..3 {
        let event = MaterializationRecord {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer
            .append("execution", &event)
            .await
            .expect("append");
    }

    // Still no Parquet
    let after_write = storage.list("state/execution/").await.unwrap_or_default();
    assert!(after_write.is_empty(), "EventWriter must not write Parquet");

    // After compaction: Parquet exists
    let compactor = Compactor::new(storage.clone());
    compactor
        .compact_domain("execution")
        .await
        .expect("compact");

    let after_compact = storage.list("state/execution/").await.expect("list");
    assert!(
        !after_compact.is_empty(),
        "Compactor is the sole Parquet writer"
    );
}

/// Tests Invariant 1: Append-only ingest with DoesNotExist precondition.
#[tokio::test]
async fn invariant1_append_only_ingest() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());

    // Write first event
    let event1 = MaterializationRecord {
        materialization_id: "mat_001".into(),
        asset_id: "asset_abc".into(),
        row_count: 100,
        byte_size: 5000,
    };
    let id = "FIXED_EVENT_ID_12345678901";
    event_writer
        .append_with_id("execution", &event1, id)
        .await
        .expect("first write");

    // Attempt to overwrite with different content
    let event2 = MaterializationRecord {
        materialization_id: "mat_002".into(), // Different ID!
        asset_id: "asset_xyz".into(),
        row_count: 999,
        byte_size: 99999,
    };
    // This should NOT overwrite - DoesNotExist precondition prevents it
    event_writer
        .append_with_id("execution", &event2, id)
        .await
        .expect("duplicate handled gracefully");

    // Read back and verify original content preserved
    let files = storage.list("ledger/execution/").await.expect("list");
    assert_eq!(files.len(), 1);

    let data = storage.get_raw(files[0].as_str()).await.expect("read");
    let parsed: MaterializationRecord = serde_json::from_slice(&data).expect("parse");

    assert_eq!(
        parsed.materialization_id, "mat_001",
        "INVARIANT 1: Original event preserved, not overwritten"
    );
}

/// Tests Invariant 2: Compactor is sole writer of state/ Parquet files.
#[tokio::test]
async fn invariant2_compactor_sole_parquet_writer() {
    // This is essentially the same as tier2_compactor_sole_parquet_writer
    // but with explicit invariant labeling
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    // EventWriter writes only to ledger/, not state/
    let event_writer = EventWriter::new(storage.clone());
    event_writer
        .append(
            "execution",
            &MaterializationRecord {
                materialization_id: "mat_001".into(),
                asset_id: "asset_abc".into(),
                row_count: 100,
                byte_size: 5000,
            },
        )
        .await
        .expect("append");

    // INVARIANT 2: No Parquet until compactor runs
    let before_compact = storage.list("state/execution/").await.unwrap_or_default();
    assert!(
        before_compact.is_empty(),
        "INVARIANT 2: Only Compactor writes to state/"
    );

    // Compactor creates Parquet
    let compactor = Compactor::new(storage.clone());
    compactor
        .compact_domain("execution")
        .await
        .expect("compact");

    let after_compact = storage.list("state/execution/").await.expect("list");
    assert!(
        !after_compact.is_empty(),
        "INVARIANT 2: Compactor creates Parquet"
    );
}

/// Tests Invariant 3: Idempotent compaction (dedupe by primary key).
#[tokio::test]
async fn invariant3_idempotent_compaction() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());

    // Write multiple events with SAME materialization_id (simulating duplicates)
    for i in 0..3 {
        let event = MaterializationRecord {
            materialization_id: "mat_duplicate".into(), // Same key!
            asset_id: format!("asset_{i}"),             // Different values
            row_count: i64::from(i) * 100,
            byte_size: i64::from(i) * 5000,
        };
        event_writer
            .append("execution", &event)
            .await
            .expect("append");
    }

    let compactor = Compactor::new(storage.clone());
    compactor
        .compact_domain("execution")
        .await
        .expect("compact");

    // Read Parquet and verify only 1 row (dedupe by primary key)
    let manifest = tier1.read_manifest().await.expect("read");
    let snapshot_path = manifest.execution.snapshot_path.expect("snapshot_path");
    let parquet_data = storage.get_raw(&snapshot_path).await.expect("read");
    let reader = ParquetRecordBatchReaderBuilder::try_new(parquet_data)
        .expect("parquet")
        .build()
        .expect("reader");
    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().expect("read");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    assert_eq!(
        total_rows, 1,
        "INVARIANT 3: Compaction dedupes by primary key"
    );
}

/// Tests Invariant 4: Atomic publish (snapshot visible only after manifest CAS).
#[tokio::test]
async fn invariant4_atomic_publish() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    // Before compaction: no snapshot_path
    let manifest_before = tier1.read_manifest().await.expect("read");
    assert!(
        manifest_before.execution.snapshot_path.is_none(),
        "INVARIANT 4: No snapshot before compaction"
    );

    // Write event and compact
    let event_writer = EventWriter::new(storage.clone());
    event_writer
        .append(
            "execution",
            &MaterializationRecord {
                materialization_id: "mat_001".into(),
                asset_id: "asset_abc".into(),
                row_count: 100,
                byte_size: 5000,
            },
        )
        .await
        .expect("append");

    let compactor = Compactor::new(storage.clone());
    compactor
        .compact_domain("execution")
        .await
        .expect("compact");

    // After compaction: snapshot_path is set (atomic visibility gate)
    let manifest_after = tier1.read_manifest().await.expect("read");
    assert!(
        manifest_after.execution.snapshot_path.is_some(),
        "INVARIANT 4: snapshot_path set after CAS succeeds"
    );

    // Verify the path actually exists
    let snapshot_path = manifest_after.execution.snapshot_path.unwrap();
    let snapshot_data = storage
        .get_raw(&snapshot_path)
        .await
        .expect("read snapshot");
    assert!(
        !snapshot_data.is_empty(),
        "INVARIANT 4: Snapshot file exists at manifest path"
    );
}

/// Tests Invariant 5: Readers never need the ledger.
#[tokio::test]
async fn invariant5_readers_never_need_ledger() {
    let backend = Arc::new(TracingMemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());
    for i in 0..3 {
        let event = MaterializationRecord {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer
            .append("execution", &event)
            .await
            .expect("append");
    }

    let compactor = Compactor::new(storage.clone());
    compactor
        .compact_domain("execution")
        .await
        .expect("compact");

    // Clear operations to isolate read behavior.
    backend.clear_operations();

    let manifest = tier1.read_manifest().await.expect("read");
    let snapshot_path = manifest.execution.snapshot_path.expect("snapshot_path");
    let _snapshot = storage
        .get_raw(&snapshot_path)
        .await
        .expect("read snapshot");

    let ops = backend.operations();
    assert!(!ops.is_empty(), "read path should perform storage ops");

    for op in ops {
        let path = match op {
            StorageOp::Get { path }
            | StorageOp::GetRange { path, .. }
            | StorageOp::Head { path }
            | StorageOp::Put { path, .. }
            | StorageOp::Delete { path } => path,
            StorageOp::List { prefix } => prefix,
        };
        assert!(
            !path.contains("/ledger/"),
            "INVARIANT 5 violated: read path accessed ledger: {path}"
        );
    }
}

/// Tests Invariant 6 (MVP): compactor currently depends on listing.
#[tokio::test]
async fn invariant6_listing_dependency_documented() {
    let backend = Arc::new(TracingMemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());
    for i in 0..3 {
        let event = MaterializationRecord {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer
            .append("execution", &event)
            .await
            .expect("append");
    }

    backend.clear_operations();

    let compactor = Compactor::new(storage.clone());
    compactor
        .compact_domain("execution")
        .await
        .expect("compact");

    let ops = backend.operations();
    let uses_listing = ops
        .iter()
        .any(|op| matches!(op, StorageOp::List { prefix } if prefix.contains("/ledger/")));
    assert!(
        uses_listing,
        "MVP: compactor uses listing over ledger prefix (Invariant 6 relaxation documented)"
    );
}

// NOTE: CAS conflict handling is covered by `tier2_concurrent_compactor_race`,
// which uses a barrier-backed storage wrapper to deterministically force a CAS race.
