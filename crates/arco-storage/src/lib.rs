//! Runtime selection and composition of Arco cloud storage adapters.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Arc;

use arco_core::storage::StorageBackend;
use arco_core::{Error, Result};

#[cfg(feature = "azure")]
use arco_storage_azure::AzureStorageBackend;
#[cfg(feature = "gcp")]
use arco_storage_gcs::GcsStorageBackend;
#[cfg(feature = "aws")]
use arco_storage_s3::S3StorageBackend;

/// Cloud provider selected for a storage bucket reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageProvider {
    /// Amazon S3.
    S3,
    /// Google Cloud Storage.
    Gcs,
    /// Azure Blob Storage.
    Azure,
}

/// Classifies a bucket reference without constructing a provider client.
///
/// # Errors
///
/// Returns an error for an empty reference or an unsupported explicit scheme.
pub fn provider_for_bucket(bucket: &str) -> Result<StorageProvider> {
    let bucket = bucket.trim();
    if bucket.is_empty() {
        return Err(Error::InvalidInput(
            "storage bucket reference cannot be empty".to_string(),
        ));
    }
    if bucket.starts_with("s3://") || bucket.starts_with("s3a://") {
        return Ok(StorageProvider::S3);
    }
    if bucket.starts_with("gs://") || bucket.starts_with("gcs://") {
        return Ok(StorageProvider::Gcs);
    }
    if bucket.starts_with("az://") || bucket.starts_with("azure://") {
        return Ok(StorageProvider::Azure);
    }
    if bucket.contains("://") {
        return Err(Error::InvalidInput(format!(
            "unsupported storage bucket scheme in '{bucket}'"
        )));
    }
    Ok(StorageProvider::Gcs)
}

/// Constructs the enabled cloud storage adapter selected by a bucket reference.
///
/// Bare bucket names retain the historical Google Cloud Storage default.
///
/// # Errors
///
/// Returns an error for invalid provider configuration or when the selected
/// provider was excluded from the crate feature graph.
pub fn from_bucket(bucket: &str) -> Result<Arc<dyn StorageBackend>> {
    match provider_for_bucket(bucket)? {
        StorageProvider::S3 => build_s3(bucket),
        StorageProvider::Gcs => build_gcs(bucket),
        StorageProvider::Azure => build_azure(bucket),
    }
}

#[cfg(feature = "aws")]
fn build_s3(bucket: &str) -> Result<Arc<dyn StorageBackend>> {
    Ok(Arc::new(S3StorageBackend::new(bucket)?))
}

#[cfg(not(feature = "aws"))]
fn build_s3(_bucket: &str) -> Result<Arc<dyn StorageBackend>> {
    Err(provider_disabled("S3", "aws"))
}

#[cfg(feature = "gcp")]
fn build_gcs(bucket: &str) -> Result<Arc<dyn StorageBackend>> {
    Ok(Arc::new(GcsStorageBackend::new(bucket)?))
}

#[cfg(not(feature = "gcp"))]
fn build_gcs(_bucket: &str) -> Result<Arc<dyn StorageBackend>> {
    Err(provider_disabled("GCS", "gcp"))
}

#[cfg(feature = "azure")]
fn build_azure(bucket: &str) -> Result<Arc<dyn StorageBackend>> {
    Ok(Arc::new(AzureStorageBackend::new(bucket)?))
}

#[cfg(not(feature = "azure"))]
fn build_azure(_bucket: &str) -> Result<Arc<dyn StorageBackend>> {
    Err(provider_disabled("Azure", "azure"))
}

#[cfg(any(not(feature = "aws"), not(feature = "gcp"), not(feature = "azure")))]
fn provider_disabled(provider: &str, feature: &str) -> Error {
    Error::InvalidInput(format!(
        "{provider} storage support is disabled; enable the '{feature}' feature"
    ))
}

#[cfg(test)]
mod tests {
    use super::{StorageProvider, provider_for_bucket};

    #[test]
    fn routes_supported_bucket_schemes() {
        assert_eq!(
            provider_for_bucket("s3://authority").unwrap(),
            StorageProvider::S3
        );
        assert_eq!(
            provider_for_bucket("s3a://authority").unwrap(),
            StorageProvider::S3
        );
        assert_eq!(
            provider_for_bucket("gs://authority").unwrap(),
            StorageProvider::Gcs
        );
        assert_eq!(
            provider_for_bucket("gcs://authority").unwrap(),
            StorageProvider::Gcs
        );
        assert_eq!(
            provider_for_bucket("az://authority").unwrap(),
            StorageProvider::Azure
        );
        assert_eq!(
            provider_for_bucket("azure://authority").unwrap(),
            StorageProvider::Azure
        );
    }

    #[test]
    fn preserves_bare_bucket_as_gcs_compatibility() {
        assert_eq!(
            provider_for_bucket("authority").unwrap(),
            StorageProvider::Gcs
        );
    }

    #[test]
    fn rejects_empty_and_unknown_schemes() {
        assert!(provider_for_bucket("  ").is_err());
        assert!(provider_for_bucket("https://authority").is_err());
    }
}
