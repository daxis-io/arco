//! Test: LedgerPutStore must use typed keys, not raw strings.
//!
//! This test should FAIL TO COMPILE, ensuring stringly-typed paths are rejected.

use arco_core::storage_traits::LedgerPutStore;
use bytes::Bytes;

async fn handler<W: LedgerPutStore>(writer: &W) {
    writer
        .put_ledger("ledger/catalog/event.json", Bytes::from("{}"))
        .await;
}

fn main() {}
