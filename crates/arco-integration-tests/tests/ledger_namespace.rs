//! Ensures flow ledgers cannot collide with catalog Tier 2 compaction.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arco_catalog::{Compactor, EventWriter, MaterializationRecord, Tier1Writer};
use arco_core::scoped_storage::ScopedStorage;
use arco_core::storage::MemoryBackend;
use arco_core::{CatalogDomain, EventId, RunId};
use arco_flow::events::EventBuilder;
use arco_flow::outbox::LedgerWriter;

#[tokio::test]
async fn flow_ledger_namespace_does_not_collide_with_catalog_compaction() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "acme", "production").expect("valid storage");

    // Catalog initialization.
    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    // Write one catalog materialization record under `ledger/executions/...`.
    let catalog_writer = EventWriter::new(storage.clone());
    let event_id: EventId = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().expect("event id");
    catalog_writer
        .append_with_id(
            CatalogDomain::Executions,
            &MaterializationRecord {
                materialization_id: "mat_001".into(),
                asset_id: "asset_abc".into(),
                row_count: 100,
                byte_size: 5000,
            },
            &event_id,
        )
        .await
        .expect("append catalog event");

    // Write one orchestration event under `ledger/flow/executions/...`.
    let run_id = RunId::generate();
    let flow_event = EventBuilder::run_started("acme", "production", run_id, "plan-1");
    LedgerWriter::new(storage.clone(), "executions")
        .append(flow_event)
        .await
        .expect("append flow event");

    // Catalog compaction should only see the catalog ledger prefix, not flow events.
    let result = Compactor::new(storage.clone())
        .compact_domain(CatalogDomain::Executions)
        .await
        .expect("compact");
    assert_eq!(result.events_processed, 1);

    let manifest = tier1.read_manifest().await.expect("read");
    let snapshot_path = manifest.executions.snapshot_path.expect("snapshot_path");
    let snapshot_bytes = storage
        .get_raw(&snapshot_path)
        .await
        .expect("read snapshot");
    assert!(!snapshot_bytes.is_empty());

    let flow_files = storage
        .list("ledger/flow/executions/")
        .await
        .expect("list flow ledger");
    assert!(!flow_files.is_empty());
}
