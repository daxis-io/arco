//! Test: A component with LedgerPutStore cannot write to state files.
//!
//! This test should FAIL TO COMPILE, demonstrating that the API layer
//! cannot write directly to state/ or snapshots/ paths.

use arco_core::storage_keys::{LedgerKey, StateKey};
use arco_core::storage_traits::{LedgerPutStore, ReadStore};
use bytes::Bytes;

/// An API handler that can read and write to ledger.
/// It should NOT be able to write to state files.
async fn api_handler<R: ReadStore, W: LedgerPutStore>(reader: &R, writer: &W) {
    // This should compile - we can read
    let _data = reader.get("state/catalog/snapshot.parquet").await;

    // This should compile - we can write to ledger
    let ledger_key = LedgerKey::event(arco_core::CatalogDomain::Catalog, "event");
    let _result = writer.put_ledger(&ledger_key, Bytes::from("{}")).await;

    // This should NOT compile - we don't have StatePutStore
    let state_key = StateKey::snapshot_file(
        arco_core::CatalogDomain::Catalog,
        1,
        "snapshot.parquet",
    );
    writer.put_state(&state_key, Bytes::from_static(&[])).await;
}

fn main() {}
