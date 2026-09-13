//! Opt-in conformance test against a real Google Cloud Storage bucket.

#![allow(clippy::expect_used)]

#[path = "../../arco-storage-object-store/tests/support/live_conformance.rs"]
mod live_conformance;

use std::sync::Arc;

use arco_core::storage::StorageBackend;
use arco_storage_gcs::GcsStorageBackend;

#[tokio::test]
#[ignore = "requires ARCO_TEST_GCS_BUCKET and cloud credentials"]
async fn gcs_backend_satisfies_storage_conformance() {
    let bucket = std::env::var("ARCO_TEST_GCS_BUCKET").expect("ARCO_TEST_GCS_BUCKET must be set");
    let backend: Arc<dyn StorageBackend> =
        Arc::new(GcsStorageBackend::new(&bucket).expect("GCS backend"));
    live_conformance::assert_storage_conformance("gcs", backend.clone()).await;
    live_conformance::assert_bounded_list_conformance("gcs", &backend).await;
}
