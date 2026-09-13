//! Azure Blob Storage adapter for Arco storage contracts.

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
use object_store::azure::MicrosoftAzureBuilder;
use object_store::signer::Signer as ObjectStoreSigner;

/// Azure Blob Storage implementation of the Arco storage contract.
#[derive(Debug, Clone)]
pub struct AzureStorageBackend {
    inner: ObjectStoreBackend,
}

impl AzureStorageBackend {
    /// Creates an Azure Blob Storage adapter from a container name or
    /// `az://`/`azure://` container reference.
    ///
    /// Bounded ordered listing remains disabled because Azure account layout
    /// cannot be inferred safely at construction.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty container or invalid Azure configuration.
    pub fn new(container: &str) -> Result<Self> {
        let container = normalize_container(container)?;
        let azure = Arc::new(
            configured_azure_builder(&container)
                .build()
                .map_err(|error| {
                    Error::storage_with_source(
                        format!("failed to configure Azure container '{container}'"),
                        error,
                    )
                })?,
        );
        let conditional_azure = Arc::new(
            configured_azure_builder(&container)
                .with_retry(no_automatic_request_retries())
                .build()
                .map_err(|error| {
                    Error::storage_with_source(
                        format!("failed to configure conditional Azure container '{container}'"),
                        error,
                    )
                })?,
        );
        let store: Arc<DynObjectStore> = azure.clone();
        let conditional_write_store: Arc<DynObjectStore> = conditional_azure;
        let signer: Arc<dyn ObjectStoreSigner> = azure;
        Ok(Self {
            inner: ObjectStoreBackend::new_with_conditional_write_store(
                store,
                conditional_write_store,
                Some(signer),
            ),
        })
    }
}

fn configured_azure_builder(container: &str) -> MicrosoftAzureBuilder {
    MicrosoftAzureBuilder::from_env().with_container_name(container)
}

fn normalize_container(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    let without_scheme = trimmed
        .strip_prefix("az://")
        .or_else(|| trimmed.strip_prefix("azure://"))
        .unwrap_or(trimmed);
    let container = without_scheme
        .split_once('/')
        .map_or(without_scheme, |(container, _)| container)
        .trim();
    if container.is_empty() {
        return Err(Error::InvalidInput(
            "Azure container name cannot be empty".to_string(),
        ));
    }
    Ok(container.to_string())
}

#[async_trait]
impl StorageBackend for AzureStorageBackend {
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

    use super::{AzureStorageBackend, normalize_container};

    fn assert_storage_backend<T: StorageBackend>() {}

    #[test]
    fn azure_backend_implements_storage_contract() {
        assert_storage_backend::<AzureStorageBackend>();
    }

    #[test]
    fn normalizes_azure_container_references() {
        assert_eq!(
            normalize_container("az://authority/path").unwrap(),
            "authority"
        );
        assert_eq!(
            normalize_container("azure://authority").unwrap(),
            "authority"
        );
        assert_eq!(normalize_container("authority").unwrap(), "authority");
        assert!(normalize_container("  ").is_err());
    }
}
