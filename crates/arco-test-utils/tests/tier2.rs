//! Tier 2 walking skeleton: append event -> compact -> read from Parquet.
//!
//! This is the minimum viable proof that Tier 2 consistency model works.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};

use arco_catalog::{Compactor, EventWriter, MaterializationRecord, Tier1Writer};
use arco_core::scoped_storage::ScopedStorage;
use arco_core::storage::MemoryBackend;
use arco_test_utils::{StorageOp, TracingMemoryBackend};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MaterializationCompleted {
    materialization_id: String,
    asset_id: String,
    row_count: i64,
    byte_size: i64,
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
    let manifest = writer.read_manifest().await.expect("read");
    assert!(manifest.execution.watermark_version >= 2);
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

/// Tests concurrent compactor race condition handling.
///
/// Per exit criteria: "CAS conflicts handled - Concurrent compactor race test passes"
#[tokio::test]
async fn concurrent_compactor_race() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    // Write events to compact
    let event_writer = EventWriter::new(storage.clone());
    for i in 0..5 {
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

    // Create two compactors racing on the same domain
    let compactor1 = Compactor::new(storage.clone());
    let compactor2 = Compactor::new(storage.clone());

    // Run both compactors concurrently
    let (result1, result2) = tokio::join!(
        compactor1.compact_domain("execution"),
        compactor2.compact_domain("execution")
    );

    // At least one must succeed
    let success_count = [&result1, &result2]
        .iter()
        .filter(|r| r.is_ok())
        .count();
    assert!(
        success_count >= 1,
        "At least one compactor must succeed in a race"
    );

    // If one failed, it should be CasFailed (not a different error)
    for result in [result1, result2] {
        if let Err(e) = result {
            let err_str = format!("{e:?}");
            assert!(
                err_str.contains("CasFailed") || err_str.contains("cas") || err_str.contains("conflict"),
                "Race loser should fail with CAS conflict, got: {err_str}"
            );
        }
    }

    // Verify final state is consistent - exactly 5 records
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
        total_rows, 5,
        "Race should not corrupt data - exactly 5 records expected"
    );
}
