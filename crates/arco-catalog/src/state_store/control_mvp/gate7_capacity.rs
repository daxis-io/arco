//! Capacity prerequisite using catalog-emitted receipt/audit bytes and the restore scanner.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]
use super::*;
use crate::{CatalogPatch, ControlCatalogAuthority, WriteOptions};
use arco_core::MemoryBackend;

struct Inventory {
    scope: StateScope,
    token: StateToken,
    rows: usize,
    value: Bytes,
}
#[async_trait]
impl ArcoStateReader for Inventory {
    async fn get(&self, _: &[u8]) -> Result<Option<Bytes>> {
        unreachable!()
    }
    async fn read_at(&self, _: StateToken) -> Result<Box<dyn ArcoStateReader>> {
        unreachable!()
    }
    async fn read_checkpoint(&self, _: CheckpointToken) -> Result<Box<dyn ArcoStateReader>> {
        unreachable!()
    }
    async fn scan(&self, request: ScanRequest) -> Result<ScanPage> {
        let start = request
            .effective_start_after()
            .map_or(0, |key| usize::from_be_bytes(key.try_into().unwrap()) + 1);
        build_scan_page(
            &self.scope,
            request,
            Some(self.token.clone()),
            (start..self.rows).map(|n| {
                KvPair::new(
                    n.to_be_bytes().to_vec(),
                    VersionedValue::new(self.value.clone(), Some(1)),
                )
            }),
        )
    }
}

#[tokio::test]
async fn pilot_retained_catalog_inventory_exceeds_unchanged_restore_capacity() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let authority = ControlCatalogAuthority::new(storage.clone(), scope.clone()).unwrap();
    authority
        .create_catalog("gate7", None, WriteOptions::with_idempotency("create"))
        .await
        .unwrap();
    authority
        .patch_catalog(
            "gate7",
            CatalogPatch {
                description: Some(Some("capacity sample".into())),
                ..CatalogPatch::default()
            },
            WriteOptions::with_idempotency("update"),
        )
        .await
        .unwrap();
    let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
    let rows = scan_all_entries_bounded(&store, b"", MAX_SEGMENT_ROWS, MAX_SEGMENT_BYTES)
        .await
        .unwrap();
    let receipts = rows
        .iter()
        .filter(|entry| {
            serde_json::from_slice::<serde_json::Value>(entry.value().bytes())
                .ok()
                .is_some_and(|v| v.get("requestDigest").is_some() && v.get("response").is_some())
        })
        .collect::<Vec<_>>();
    let audits = rows
        .iter()
        .filter(|entry| {
            serde_json::from_slice::<serde_json::Value>(entry.value().bytes())
                .ok()
                .is_some_and(|v| v.get("requestDigest").is_some() && v.get("actor").is_some())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        (receipts.len(), audits.len()),
        (2, 2),
        "one retained receipt/audit pair per accepted mutation"
    );
    let sample_bytes = receipts
        .iter()
        .chain(audits.iter())
        .map(|r| r.key().len() + r.value().bytes().len())
        .sum::<usize>();
    let mutations = 7 * (3600 * 25 + 23 * 3600);
    assert_eq!(mutations, 1_209_600);
    let retained_rows = mutations * 2;
    assert_eq!(retained_rows, 2_419_200);
    assert!(retained_rows > MAX_SEGMENT_ROWS);
    // Reuse actual serialized records. Keys here are deliberately shorter than
    // actual receipt/audit keys, so this understates aggregate decoded bytes.
    let shortest = receipts
        .iter()
        .chain(audits.iter())
        .min_by_key(|r| r.value().bytes().len())
        .unwrap()
        .value()
        .bytes()
        .clone();
    let inventory = Inventory {
        scope: scope.clone(),
        token: store.current_state_token().await.unwrap(),
        rows: retained_rows,
        value: shortest.clone(),
    };
    let error = scan_all_entries_bounded(&inventory, b"", MAX_SEGMENT_ROWS, MAX_SEGMENT_BYTES)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        CatalogError::MaintenanceBackpressure { .. }
    ));
    let row_probe = Inventory {
        scope,
        token: inventory.token.clone(),
        rows: retained_rows,
        value: Bytes::new(),
    };
    let row_error = scan_all_entries_bounded(&row_probe, b"", MAX_SEGMENT_ROWS, MAX_SEGMENT_BYTES)
        .await
        .unwrap_err();
    assert!(matches!(
        row_error,
        CatalogError::MaintenanceBackpressure { .. }
    ));
    let report = serde_json::json!({
        "status": "pilot-blocked", "successful_sample_mutations": 2,
        "actual_receipt_rows": receipts.len(), "actual_audit_rows": audits.len(),
        "sample_key_value_bytes": sample_bytes, "required_mutations": mutations,
        "required_retained_rows": retained_rows, "restore_max_rows": MAX_SEGMENT_ROWS,
        "restore_max_decoded_bytes": MAX_SEGMENT_BYTES,
        "shortest_actual_record_value_bytes": shortest.len(),
        "conservative_inventory_decoded_bytes": retained_rows * (shortest.len() + size_of::<usize>()),
        "actual_restore_scanner_byte_error": error.to_string(),
        "actual_restore_scanner_row_error": row_error.to_string(),
        "scope": "actual catalog sample plus generated reader over the production restore scanner; no full pilot root or provider measurement"
    });
    println!("{report}");
    if let Ok(path) = std::env::var("ARCO_GATE7_CAPACITY_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
}
