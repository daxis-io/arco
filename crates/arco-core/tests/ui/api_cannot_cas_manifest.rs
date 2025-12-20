//! Test: A component without CasStore cannot CAS-update manifests.
//!
//! This test should FAIL TO COMPILE, demonstrating that the API layer
//! cannot directly update manifests - only the compactor can.

use arco_core::storage_keys::{CommitKey, LedgerKey, ManifestKey};
use arco_core::storage_traits::{CommitPutStore, LedgerPutStore, ReadStore};
use bytes::Bytes;

/// An API handler with typical API capabilities.
/// It should NOT be able to CAS-update manifests.
async fn api_handler<S: ReadStore + LedgerPutStore + CommitPutStore>(storage: &S) {
    // This should compile - we can read
    let _data = storage.get("manifests/root.manifest.json").await;

    // This should compile - we can write to ledger
    let ledger_key = LedgerKey::event(arco_core::CatalogDomain::Catalog, "event");
    let _result = storage.put_ledger(&ledger_key, Bytes::from("{}")).await;

    // This should compile - we can write commits
    let commit_key = CommitKey::record(arco_core::CatalogDomain::Catalog, "commit");
    let _result = storage.put_commit(&commit_key, Bytes::from("{}")).await;

    // This should NOT compile - we don't have CasStore
    let manifest_key = ManifestKey::root();
    storage.cas(&manifest_key, Bytes::from("{}"), "v1").await;
}

fn main() {}
