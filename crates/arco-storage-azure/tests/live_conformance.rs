//! Opt-in conformance test against a real Azure Blob Storage container.

#![allow(clippy::expect_used)]

#[path = "../../arco-storage-object-store/tests/support/live_conformance.rs"]
#[allow(dead_code)]
mod live_conformance;

use std::sync::Arc;

use arco_core::storage::StorageBackend;
use arco_storage_azure::AzureStorageBackend;

#[tokio::test]
#[ignore = "requires ARCO_TEST_AZURE_CONTAINER and cloud credentials"]
async fn azure_backend_satisfies_storage_conformance() {
    let container =
        std::env::var("ARCO_TEST_AZURE_CONTAINER").expect("ARCO_TEST_AZURE_CONTAINER must be set");
    let backend: Arc<dyn StorageBackend> =
        Arc::new(AzureStorageBackend::new(&container).expect("Azure backend"));
    live_conformance::assert_storage_conformance("azure", backend.clone()).await;
    let error = backend
        .list_page("conformance/azure", None, 10)
        .await
        .expect_err("Azure bounded ordered listing must fail closed");
    assert!(error.to_string().contains("unsupported"));
}
