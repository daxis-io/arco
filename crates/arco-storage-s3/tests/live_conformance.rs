//! Opt-in conformance test against a real Amazon S3 bucket.

#![allow(clippy::expect_used)]

#[path = "../../arco-storage-object-store/tests/support/live_conformance.rs"]
mod live_conformance;

use std::sync::Arc;

use arco_core::storage::StorageBackend;
use arco_storage_s3::S3StorageBackend;

#[tokio::test]
#[ignore = "requires ARCO_TEST_S3_BUCKET and cloud credentials"]
async fn s3_backend_satisfies_storage_conformance() {
    let bucket = std::env::var("ARCO_TEST_S3_BUCKET").expect("ARCO_TEST_S3_BUCKET must be set");
    let backend: Arc<dyn StorageBackend> =
        Arc::new(S3StorageBackend::new(&bucket).expect("S3 backend"));
    live_conformance::assert_storage_conformance("s3", backend.clone()).await;
    live_conformance::assert_bounded_list_conformance("s3", &backend).await;
}
