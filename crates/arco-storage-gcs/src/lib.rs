//! Google Cloud Storage adapter for Arco storage contracts.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use arco_core::storage::{ListPage, ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{Error, Result};
use arco_storage_object_store::{ObjectStoreBackend, no_automatic_request_retries};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::DynObjectStore;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::signer::Signer as ObjectStoreSigner;

/// Google Cloud Storage implementation of the Arco storage contract.
#[derive(Debug, Clone)]
pub struct GcsStorageBackend {
    inner: ObjectStoreBackend,
}

impl GcsStorageBackend {
    /// Creates a Google Cloud Storage adapter from a bucket name or
    /// `gs://`/`gcs://` bucket reference.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty bucket or invalid GCS configuration.
    pub fn new(bucket: &str) -> Result<Self> {
        let bucket = normalize_bucket(bucket)?;
        let gcs = Arc::new(
            GoogleCloudStorageBuilder::new()
                .with_bucket_name(&bucket)
                .with_retry(no_automatic_request_retries())
                .build()
                .map_err(|error| {
                    Error::storage_with_source(
                        format!("failed to configure GCS bucket '{bucket}'"),
                        error,
                    )
                })?,
        );
        let store: Arc<DynObjectStore> = gcs.clone();
        let signer: Arc<dyn ObjectStoreSigner> = gcs;
        Ok(Self {
            inner: ObjectStoreBackend::new_with_ordered_listing(store, Some(signer)),
        })
    }
}

fn normalize_bucket(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    let without_scheme = trimmed
        .strip_prefix("gs://")
        .or_else(|| trimmed.strip_prefix("gcs://"))
        .unwrap_or(trimmed);
    let bucket = without_scheme
        .split_once('/')
        .map_or(without_scheme, |(bucket, _)| bucket)
        .trim();
    if bucket.is_empty() {
        return Err(Error::InvalidInput(
            "GCS bucket name cannot be empty".to_string(),
        ));
    }
    Ok(bucket.to_string())
}

#[async_trait]
impl StorageBackend for GcsStorageBackend {
    async fn get(&self, path: &str) -> Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ListPage> {
        self.inner.list_page(prefix, start_after, limit).await
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

#[cfg(test)]
mod tests {
    use arco_core::storage::StorageBackend;

    use super::{GcsStorageBackend, normalize_bucket};

    fn assert_storage_backend<T: StorageBackend>() {}

    #[test]
    fn gcs_backend_implements_storage_contract() {
        assert_storage_backend::<GcsStorageBackend>();
    }

    #[test]
    fn normalizes_gcs_bucket_references() {
        assert_eq!(
            normalize_bucket("gs://authority/path").unwrap(),
            "authority"
        );
        assert_eq!(normalize_bucket("gcs://authority").unwrap(), "authority");
        assert_eq!(normalize_bucket("authority").unwrap(), "authority");
        assert!(normalize_bucket("  ").is_err());
    }
}
