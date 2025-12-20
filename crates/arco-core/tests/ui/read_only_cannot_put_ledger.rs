//! Test: A component with only ReadStore cannot write to the ledger.
//!
//! This test should FAIL TO COMPILE, demonstrating that trait separation
//! enforces capability-based access at compile time.

use arco_core::storage_keys::LedgerKey;
use arco_core::storage_traits::ReadStore;
use bytes::Bytes;

/// An API handler that only has read access.
/// It should NOT be able to write to the ledger.
async fn read_only_handler<R: ReadStore>(reader: &R) {
    // This should compile - we can read
    let _data = reader.get("ledger/catalog/test.json").await;

    // This should NOT compile - we don't have LedgerPutStore
    let key = LedgerKey::event(arco_core::CatalogDomain::Catalog, "test");
    reader.put_ledger(&key, Bytes::from("{}")).await;
}

fn main() {}
