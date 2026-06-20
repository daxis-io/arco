//! ADR-041 orchestration event-log contract tests.

#![allow(clippy::expect_used)]

use std::sync::Arc;

use arco_core::{FlowPaths, MemoryBackend, ScopedStorage};
use arco_flow::orchestration::event_log::{
    L0BundleEvent, L0InboxBundle, L0InboxError, L0InboxWriteOutcome, L0InboxWriter,
};
use chrono::{TimeZone, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn storage() -> ScopedStorage {
    ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant-a", "workspace-b")
        .expect("scoped storage")
}

fn occurred_at() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 6, 6, 12, 0, 0)
        .single()
        .expect("valid timestamp")
}

fn bundle_event(event_id: &str, producer_seq: u64) -> L0BundleEvent {
    L0BundleEvent {
        event_id: event_id.to_string(),
        tenant_id: "tenant-a".to_string(),
        workspace_id: "workspace-b".to_string(),
        run_id: "run-001".to_string(),
        task_key: "pipeline.orders".to_string(),
        attempt_id: "attempt-001".to_string(),
        event_type: "TaskStarted".to_string(),
        producer_id: "callback-api".to_string(),
        producer_seq,
        occurred_at: occurred_at(),
        payload: json!({
            "worker_id": "worker-1",
            "attempt": 1
        }),
        schema_version: 1,
    }
}

#[test]
fn l0_inbox_bundle_derives_metadata_and_checksum() -> TestResult {
    let created_at = occurred_at();
    let bundle = L0InboxBundle::new(
        "bundle-001",
        vec![bundle_event("event-001", 10), bundle_event("event-002", 11)],
        created_at,
    )?;

    assert_eq!(bundle.metadata.bundle_id, "bundle-001");
    assert_eq!(bundle.metadata.tenant_id, "tenant-a");
    assert_eq!(bundle.metadata.workspace_id, "workspace-b");
    assert_eq!(bundle.metadata.run_id, "run-001");
    assert_eq!(bundle.metadata.producer_id, "callback-api");
    assert_eq!(bundle.metadata.attempt_id, "attempt-001");
    assert_eq!(bundle.metadata.event_count, 2);
    assert_eq!(bundle.metadata.min_producer_seq, 10);
    assert_eq!(bundle.metadata.max_producer_seq, 11);
    assert_eq!(bundle.metadata.schema_version, 1);
    assert_eq!(bundle.metadata.format, "parquet");
    assert_eq!(bundle.metadata.compression, "uncompressed");
    assert_eq!(bundle.metadata.created_at, created_at);
    assert!(bundle.metadata.checksum.starts_with("sha256:"));

    Ok(())
}

#[test]
fn l0_inbox_bundle_rejects_mixed_scope() {
    let mut second = bundle_event("event-002", 11);
    second.workspace_id = "workspace-c".to_string();

    let err = L0InboxBundle::new(
        "bundle-001",
        vec![bundle_event("event-001", 10), second],
        occurred_at(),
    )
    .expect_err("mixed workspace should fail");

    assert!(matches!(err, L0InboxError::InvalidBundle { .. }));
    assert!(err.to_string().contains("workspace"));
}

#[test]
fn l0_inbox_bundle_rejects_mixed_task_key_for_same_attempt() {
    let mut second = bundle_event("event-002", 11);
    second.task_key = "pipeline.customers".to_string();

    let err = L0InboxBundle::new(
        "bundle-001",
        vec![bundle_event("event-001", 10), second],
        occurred_at(),
    )
    .expect_err("mixed task key should fail");

    assert!(matches!(err, L0InboxError::InvalidBundle { .. }));
    assert!(err.to_string().contains("task_key"));
}

#[test]
fn l0_inbox_bundle_rejects_duplicate_event_id() {
    let err = L0InboxBundle::new(
        "bundle-001",
        vec![bundle_event("event-001", 10), bundle_event("event-001", 11)],
        occurred_at(),
    )
    .expect_err("duplicate event id should fail");

    assert!(matches!(err, L0InboxError::InvalidBundle { .. }));
    assert!(err.to_string().contains("duplicate event_id"));
}

#[test]
fn l0_inbox_bundle_rejects_duplicate_producer_sequence() {
    let err = L0InboxBundle::new(
        "bundle-001",
        vec![bundle_event("event-001", 10), bundle_event("event-002", 10)],
        occurred_at(),
    )
    .expect_err("duplicate producer sequence should fail");

    assert!(matches!(err, L0InboxError::InvalidBundle { .. }));
    assert!(err.to_string().contains("producer_seq"));
}

#[test]
fn l0_inbox_bundle_rejects_path_unsafe_storage_scope_ids() {
    let mut event = bundle_event("event-001", 10);
    event.run_id = "run/001".to_string();

    let err = L0InboxBundle::new("bundle-001", vec![event], occurred_at())
        .expect_err("path-unsafe run id should fail");

    assert!(matches!(err, L0InboxError::InvalidBundle { .. }));
    assert!(err.to_string().contains("run_id"));

    let err = L0InboxBundle::new(
        "bundle/001",
        vec![bundle_event("event-001", 10)],
        occurred_at(),
    )
    .expect_err("path-unsafe bundle id should fail");

    assert!(matches!(err, L0InboxError::InvalidBundle { .. }));
    assert!(err.to_string().contains("bundle_id"));
}

#[tokio::test]
async fn l0_inbox_writer_uses_create_if_absent_and_does_not_write_legacy_ledger() -> TestResult {
    let storage = storage();
    let writer = L0InboxWriter::new(storage.clone());
    let bundle = L0InboxBundle::new(
        "bundle-001",
        vec![bundle_event("event-001", 10)],
        occurred_at(),
    )?;

    let path = FlowPaths::orchestration_l0_inbox_bundle_path(
        "run-001",
        "callback-api",
        "attempt-001",
        "bundle-001",
    );
    let outcome = writer.write_bundle(&bundle).await?;

    assert!(matches!(
        outcome,
        L0InboxWriteOutcome::Created {
            path: ref created_path,
            ..
        } if created_path == &path
    ));

    let stored_bytes = storage.get_raw(&path).await?;
    assert!(
        stored_bytes.ends_with(b"PAR1"),
        "L0 inbox bundles must be written as ADR-listed Parquet objects"
    );
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(stored_bytes.clone())?.build()?;
    let batch = reader
        .next()
        .transpose()?
        .expect("bundle parquet has a batch");
    assert_eq!(batch.num_rows(), 1);

    let legacy_ledger = storage.list_meta("ledger/orchestration").await?;
    assert!(
        legacy_ledger.is_empty(),
        "L0 inbox writer must not write legacy one-object orchestration ledger entries"
    );

    Ok(())
}

#[tokio::test]
async fn l0_inbox_writer_rejects_bundle_outside_storage_scope() -> TestResult {
    let storage = storage();
    let writer = L0InboxWriter::new(storage.clone());
    let mut event = bundle_event("event-001", 10);
    event.tenant_id = "tenant-b".to_string();
    let bundle = L0InboxBundle::new("bundle-001", vec![event], occurred_at())?;

    let err = writer
        .write_bundle(&bundle)
        .await
        .expect_err("writer must reject bundle scope mismatch");

    assert!(matches!(err, L0InboxError::InvalidBundle { .. }));
    assert!(err.to_string().contains("storage scope"));
    assert!(
        storage.list_meta("_inbox/orchestration").await?.is_empty(),
        "scope mismatch must fail closed without writing an object"
    );

    Ok(())
}

#[tokio::test]
async fn duplicate_l0_bundle_path_does_not_overwrite_existing_content() -> TestResult {
    let storage = storage();
    let writer = L0InboxWriter::new(storage.clone());
    let first = L0InboxBundle::new(
        "bundle-001",
        vec![bundle_event("event-001", 10)],
        occurred_at(),
    )?;
    let mut changed_event = bundle_event("event-999", 99);
    changed_event.payload = json!({ "worker_id": "different-worker", "attempt": 1 });
    let second = L0InboxBundle::new("bundle-001", vec![changed_event], occurred_at())?;

    let path = first.path();
    writer.write_bundle(&first).await?;
    let before = storage.get_raw(&path).await?;
    let outcome = writer.write_bundle(&second).await?;
    let after = storage.get_raw(&path).await?;

    assert!(matches!(
        outcome,
        L0InboxWriteOutcome::AlreadyExists {
            path: ref existing_path,
            ..
        } if existing_path == &path
    ));
    assert_eq!(before, after, "duplicate bundle writes must not overwrite");

    Ok(())
}
