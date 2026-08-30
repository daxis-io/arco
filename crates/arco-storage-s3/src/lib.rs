//! Amazon S3 adapter for Arco storage contracts.

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
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::signer::Signer as ObjectStoreSigner;

/// Amazon S3 implementation of the Arco storage contract.
#[derive(Debug, Clone)]
pub struct S3StorageBackend {
    inner: ObjectStoreBackend,
}

impl S3StorageBackend {
    /// Creates an Amazon S3 adapter from a bucket name or `s3://`/`s3a://`
    /// bucket reference.
    ///
    /// Credentials and endpoint configuration are read by the upstream S3
    /// builder from its standard environment.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty bucket or invalid S3 configuration.
    pub fn new(bucket: &str) -> Result<Self> {
        let bucket = normalize_bucket(bucket)?;
        let s3 = Arc::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(&bucket)
                .with_conditional_put(S3ConditionalPut::ETagMatch)
                .with_retry(no_automatic_request_retries())
                .build()
                .map_err(|error| {
                    Error::storage_with_source(
                        format!("failed to configure S3 bucket '{bucket}'"),
                        error,
                    )
                })?,
        );
        let store: Arc<DynObjectStore> = s3.clone();
        let signer: Arc<dyn ObjectStoreSigner> = s3;
        let inner = if supports_ordered_listing(&bucket) {
            ObjectStoreBackend::new_with_ordered_listing(store, Some(signer))
        } else {
            ObjectStoreBackend::new(store, Some(signer))
        };
        Ok(Self { inner })
    }

    /// Returns whether this S3 bucket supports bounded lexicographic listing.
    #[must_use]
    pub const fn ordered_listing_enabled(&self) -> bool {
        self.inner.ordered_listing_enabled()
    }
}

fn normalize_bucket(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    let without_scheme = trimmed
        .strip_prefix("s3://")
        .or_else(|| trimmed.strip_prefix("s3a://"))
        .unwrap_or(trimmed);
    let bucket = without_scheme
        .split_once('/')
        .map_or(without_scheme, |(bucket, _)| bucket)
        .trim();
    if bucket.is_empty() {
        return Err(Error::InvalidInput(
            "S3 bucket name cannot be empty".to_string(),
        ));
    }
    Ok(bucket.to_string())
}

fn supports_ordered_listing(bucket: &str) -> bool {
    !bucket.ends_with("--x-s3")
}

#[async_trait]
impl StorageBackend for S3StorageBackend {
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

    use super::{S3StorageBackend, normalize_bucket, supports_ordered_listing};

    fn assert_storage_backend<T: StorageBackend>() {}

    #[test]
    fn s3_backend_implements_storage_contract() {
        assert_storage_backend::<S3StorageBackend>();
    }

    #[test]
    fn normalizes_s3_bucket_references() {
        assert_eq!(
            normalize_bucket("s3://authority/path").unwrap(),
            "authority"
        );
        assert_eq!(normalize_bucket("s3a://authority").unwrap(), "authority");
        assert_eq!(normalize_bucket("authority").unwrap(), "authority");
        assert!(normalize_bucket("  ").is_err());
    }

    #[test]
    fn directory_buckets_disable_ordered_listing() {
        assert!(supports_ordered_listing("ordinary-bucket"));
        assert!(!supports_ordered_listing("events--usw2-az1--x-s3"));
    }
}
