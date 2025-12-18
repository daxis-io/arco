//! Storage backend abstraction for object storage (GCS, S3, local).
//!
//! This module defines the core storage contract that all backends must implement.
//! The contract matches the architecture docs:
//! - Conditional writes with preconditions
//! - Object metadata including `last_modified` and `etag`
//! - Signed URL generation for direct access
//!
//! ## Multi-Cloud Compatibility
//!
//! The storage version token is an opaque `String` to support different backends:
//! - GCS: Uses numeric generation (stored as string)
//! - S3: Uses `ETag` or version ID (already strings)
//! - Azure: Uses `ETag`
//!
//! This abstraction avoids leaking GCS-specific assumptions into the catalog layer.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use http::Method;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::signer::Signer as ObjectStoreSigner;
use object_store::{DynObjectStore, PutMode, PutOptions, UpdateVersion};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::error::{Error, Result};

/// Precondition for conditional writes (CAS operations).
///
/// The version token is opaque - backends interpret it according to their semantics:
/// - GCS: Numeric generation as string
/// - S3: `ETag` or version ID
#[derive(Debug, Clone)]
pub enum WritePrecondition {
    /// Write only if object does not exist.
    DoesNotExist,
    /// Write only if object's version matches the given token.
    MatchesVersion(String),
    /// Write unconditionally.
    None,
}

/// Result of a conditional write.
#[derive(Debug, Clone)]
pub enum WriteResult {
    /// Write succeeded, returns new version token.
    Success {
        /// The new version token after the write.
        version: String,
    },
    /// Precondition failed, returns current version token.
    PreconditionFailed {
        /// The current version that caused the precondition to fail.
        current_version: String,
    },
}

/// Metadata about a stored object.
///
/// Per architecture docs: must include `last_modified` and `etag`.
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    /// Object path (key).
    pub path: String,
    /// Object size in bytes.
    pub size: u64,
    /// Object version token for CAS operations.
    ///
    /// This is an opaque string that backends interpret:
    /// - GCS: Numeric generation as string
    /// - S3: `ETag` or version ID
    pub version: String,
    /// Last modification timestamp.
    pub last_modified: Option<DateTime<Utc>>,
    /// Entity tag for cache validation.
    pub etag: Option<String>,
}

/// Storage backend trait for object storage.
///
/// All storage backends (GCS, S3, memory) implement this trait.
/// The contract is designed for cloud object storage semantics.
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    /// Reads entire object.
    ///
    /// Returns `Error::NotFound` if object doesn't exist.
    async fn get(&self, path: &str) -> Result<Bytes>;

    /// Reads a byte range from an object.
    ///
    /// Returns `Error::InvalidInput` if start > object length.
    /// Returns `Error::InvalidInput` if end < start.
    /// Clamps end to object length if end > length.
    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes>;

    /// Writes with optional precondition.
    ///
    /// Returns `WriteResult::PreconditionFailed` if precondition not met.
    /// Never returns error for precondition failure - that's a normal result.
    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult>;

    /// Deletes an object.
    ///
    /// Succeeds even if object doesn't exist (idempotent).
    async fn delete(&self, path: &str) -> Result<()>;

    /// Lists objects with the given prefix.
    ///
    /// Returns empty vec if no objects match.
    ///
    /// **Ordering**: Results are returned in arbitrary order that may vary between
    /// backends and invocations. Callers requiring deterministic order should sort
    /// the results (e.g., by `path` or `last_modified`).
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>>;

    /// Gets object metadata without reading content.
    ///
    /// Returns `None` if object doesn't exist.
    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>>;

    /// Generates a signed URL for direct access.
    ///
    /// Per architecture docs: required for direct client access.
    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String>;
}

/// Storage backend adapter for [`object_store`] implementations.
///
/// This bridges the Arco `StorageBackend` contract (CAS writes, listing, signed URLs)
/// onto an `object_store::ObjectStore`.
///
/// ## Version Tokens
///
/// Object stores provide conditional update semantics using a combination of:
/// - `e_tag` (HTTP etag)
/// - `version` (backend-specific version ID)
///
/// Arco exposes these as an opaque `String` version token. This adapter encodes
/// the pair `{e_tag, version}` as a JSON string so callers can preserve both.
#[derive(Debug, Clone)]
pub struct ObjectStoreBackend {
    store: Arc<DynObjectStore>,
    signer: Option<Arc<dyn ObjectStoreSigner>>,
}

impl ObjectStoreBackend {
    /// Creates a new backend adapter.
    #[must_use]
    pub fn new(store: Arc<DynObjectStore>, signer: Option<Arc<dyn ObjectStoreSigner>>) -> Self {
        Self { store, signer }
    }

    /// Creates a Google Cloud Storage backend for the given bucket.
    ///
    /// `bucket` may be provided as a bare bucket name (`my-bucket`) or with a
    /// `gs://` prefix (`gs://my-bucket`).
    ///
    /// # Errors
    ///
    /// Returns an error if the GCS client cannot be configured.
    pub fn gcs(bucket: &str) -> Result<Self> {
        let bucket = normalize_bucket("gs://", bucket);
        if bucket.is_empty() {
            return Err(Error::InvalidInput(
                "ARCO_STORAGE_BUCKET cannot be empty".to_string(),
            ));
        }

        let gcs = object_store::gcp::GoogleCloudStorageBuilder::new()
            .with_bucket_name(&bucket)
            .build()
            .map_err(|e| {
                Error::storage_with_source(format!("failed to configure GCS bucket '{bucket}'"), e)
            })?;

        let gcs = Arc::new(gcs);
        let store: Arc<DynObjectStore> = gcs.clone();
        let signer: Arc<dyn ObjectStoreSigner> = gcs;
        Ok(Self::new(store, Some(signer)))
    }

    /// Creates an Amazon S3 backend for the given bucket.
    ///
    /// `bucket` may be provided as a bare bucket name (`my-bucket`) or with a
    /// `s3://` prefix (`s3://my-bucket`).
    ///
    /// # Errors
    ///
    /// Returns an error if the S3 client cannot be configured.
    pub fn s3(bucket: &str) -> Result<Self> {
        let bucket = normalize_bucket("s3://", bucket);
        if bucket.is_empty() {
            return Err(Error::InvalidInput(
                "ARCO_STORAGE_BUCKET cannot be empty".to_string(),
            ));
        }

        let s3 = AmazonS3Builder::from_env()
            .with_bucket_name(&bucket)
            .build()
            .map_err(|e| {
                Error::storage_with_source(format!("failed to configure S3 bucket '{bucket}'"), e)
            })?;

        let s3 = Arc::new(s3);
        let store: Arc<DynObjectStore> = s3.clone();
        let signer: Arc<dyn ObjectStoreSigner> = s3;
        Ok(Self::new(store, Some(signer)))
    }

    /// Creates a storage backend from a bucket string, inferring the provider.
    ///
    /// Defaults to GCS when no scheme prefix is provided.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot be configured.
    pub fn from_bucket(bucket: &str) -> Result<Self> {
        let trimmed = bucket.trim();
        if trimmed.starts_with("s3://") || trimmed.starts_with("s3a://") {
            return Self::s3(trimmed);
        }
        if trimmed.starts_with("gs://") || trimmed.starts_with("gcs://") {
            return Self::gcs(trimmed);
        }

        Self::gcs(trimmed)
    }
}

fn normalize_bucket(prefix: &str, raw: &str) -> String {
    let trimmed = raw.trim();
    let no_prefix = trimmed
        .strip_prefix(prefix)
        .or_else(|| match prefix {
            "gs://" => trimmed.strip_prefix("gcs://"),
            "s3://" => trimmed.strip_prefix("s3a://"),
            _ => None,
        })
        .unwrap_or(trimmed);

    no_prefix
        .split_once('/')
        .map_or(no_prefix, |(bucket, _)| bucket)
        .trim()
        .to_string()
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct VersionToken {
    e_tag: Option<String>,
    version: Option<String>,
}

impl VersionToken {
    fn from_parts(e_tag: Option<String>, version: Option<String>) -> Self {
        Self { e_tag, version }
    }

    fn to_update_version(&self) -> UpdateVersion {
        UpdateVersion {
            e_tag: self.e_tag.clone(),
            version: self.version.clone(),
        }
    }

    fn encode(&self) -> String {
        match serde_json::to_string(self) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "failed to serialize version token; falling back to raw version");
                self.version.clone().unwrap_or_default()
            }
        }
    }

    fn decode(token: &str) -> Self {
        let token = token.trim();
        if token.is_empty() {
            return Self::from_parts(None, None);
        }

        // Prefer the structured encoding used by this adapter.
        if let Ok(v) = serde_json::from_str::<Self>(token) {
            return v;
        }

        // Backwards/interop fallback: treat the token as a raw `version` string.
        Self::from_parts(None, Some(token.to_string()))
    }
}

fn map_object_store_error(err: object_store::Error) -> Error {
    match err {
        object_store::Error::NotFound { path, .. } => Error::NotFound(path),
        object_store::Error::InvalidPath { source } => {
            Error::InvalidInput(format!("invalid object store path: {source}"))
        }
        err @ object_store::Error::PermissionDenied { .. } => {
            Error::storage_with_source("permission denied".to_string(), err)
        }
        err @ object_store::Error::Unauthenticated { .. } => {
            Error::storage_with_source("unauthenticated".to_string(), err)
        }
        err => Error::storage_with_source("object store error".to_string(), err),
    }
}

fn object_store_meta_to_meta(meta: object_store::ObjectMeta) -> ObjectMeta {
    let version = VersionToken::from_parts(meta.e_tag.clone(), meta.version.clone()).encode();
    ObjectMeta {
        path: meta.location.to_string(),
        size: u64::try_from(meta.size).unwrap_or(u64::MAX),
        version,
        last_modified: Some(meta.last_modified),
        etag: meta.e_tag,
    }
}

#[async_trait]
impl StorageBackend for ObjectStoreBackend {
    async fn get(&self, path: &str) -> Result<Bytes> {
        let location = ObjectStorePath::from(path);
        let result = self
            .store
            .get(&location)
            .await
            .map_err(map_object_store_error)?;
        result.bytes().await.map_err(map_object_store_error)
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        let meta = self
            .head(path)
            .await?
            .ok_or_else(|| Error::NotFound(format!("object not found: {path}")))?;

        let size_usize = usize::try_from(meta.size).map_err(|_| {
            Error::InvalidInput(format!(
                "object size too large for range requests: {}",
                meta.size
            ))
        })?;

        let start = usize::try_from(range.start)
            .map_err(|_| Error::InvalidInput(format!("range start too large: {}", range.start)))?;

        if start > size_usize {
            return Err(Error::InvalidInput(format!(
                "range start {start} exceeds object length {size_usize}"
            )));
        }

        let end = usize::try_from(range.end)
            .unwrap_or(usize::MAX)
            .min(size_usize);

        if end < start {
            return Err(Error::InvalidInput(format!(
                "range end {end} is before start {start}"
            )));
        }

        let location = ObjectStorePath::from(path);
        self.store
            .get_range(&location, start..end)
            .await
            .map_err(map_object_store_error)
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        let location = ObjectStorePath::from(path);
        let opts = match precondition {
            WritePrecondition::DoesNotExist => PutOptions::from(PutMode::Create),
            WritePrecondition::MatchesVersion(token) => {
                let token = VersionToken::decode(&token);
                PutOptions::from(PutMode::Update(token.to_update_version()))
            }
            WritePrecondition::None => PutOptions::default(),
        };

        match self.store.put_opts(&location, data.into(), opts).await {
            Ok(result) => {
                let version = VersionToken::from_parts(result.e_tag, result.version).encode();
                Ok(WriteResult::Success { version })
            }
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => {
                let current = self.head(path).await?;
                let current_version = current.map_or_else(String::new, |m| m.version);
                Ok(WriteResult::PreconditionFailed { current_version })
            }
            Err(e) => Err(map_object_store_error(e)),
        }
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let location = ObjectStorePath::from(path);
        match self.store.delete(&location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(map_object_store_error(e)),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let prefix = ObjectStorePath::from(prefix);
        let metas = self
            .store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .map_err(map_object_store_error)?;

        Ok(metas.into_iter().map(object_store_meta_to_meta).collect())
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        let location = ObjectStorePath::from(path);
        match self.store.head(&location).await {
            Ok(meta) => Ok(Some(object_store_meta_to_meta(meta))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(map_object_store_error(e)),
        }
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        let Some(signer) = self.signer.as_ref() else {
            return Err(Error::storage(
                "signed URLs are not supported by this storage backend".to_string(),
            ));
        };

        let location = ObjectStorePath::from(path);
        let url = signer
            .signed_url(Method::GET, &location, expiry)
            .await
            .map_err(map_object_store_error)?;
        Ok(url.to_string())
    }
}

/// In-memory storage backend for testing.
///
/// Thread-safe via `RwLock`. Not suitable for production.
/// Uses numeric versions internally (stored as strings) to simulate GCS-like behavior.
#[derive(Debug, Default)]
pub struct MemoryBackend {
    objects: Arc<RwLock<HashMap<String, StoredObject>>>,
}

#[derive(Debug, Clone)]
struct StoredObject {
    data: Bytes,
    /// Numeric version stored as i64 internally, exposed as String via API.
    version: i64,
    last_modified: DateTime<Utc>,
}

impl MemoryBackend {
    /// Creates a new empty memory backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StorageBackend for MemoryBackend {
    async fn get(&self, path: &str) -> Result<Bytes> {
        let objects = self.objects.read().map_err(|_| Error::Internal {
            message: "lock poisoned".into(),
        })?;

        objects
            .get(path)
            .map(|o| o.data.clone())
            .ok_or_else(|| Error::NotFound(format!("object not found: {path}")))
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        let data = self.get(path).await?;
        let len = data.len();

        let start = usize::try_from(range.start).unwrap_or(usize::MAX);
        if start > len {
            return Err(Error::InvalidInput(format!(
                "range start {start} exceeds object length {len}"
            )));
        }

        let end = usize::try_from(range.end).unwrap_or(usize::MAX).min(len);
        if end < start {
            return Err(Error::InvalidInput(format!(
                "range end {end} is before start {start}"
            )));
        }
        Ok(data.slice(start..end))
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        let mut objects = self.objects.write().map_err(|_| Error::Internal {
            message: "lock poisoned".into(),
        })?;

        let current = objects.get(path);

        match precondition {
            WritePrecondition::DoesNotExist => {
                if let Some(obj) = current {
                    return Ok(WriteResult::PreconditionFailed {
                        current_version: obj.version.to_string(),
                    });
                }
            }
            WritePrecondition::MatchesVersion(expected) => {
                let expected_num: i64 = expected.parse().unwrap_or(-1);
                match current {
                    Some(obj) if obj.version != expected_num => {
                        return Ok(WriteResult::PreconditionFailed {
                            current_version: obj.version.to_string(),
                        });
                    }
                    None => {
                        return Ok(WriteResult::PreconditionFailed {
                            current_version: "0".to_string(),
                        });
                    }
                    _ => {}
                }
            }
            WritePrecondition::None => {}
        }

        let new_version = current.map_or(1, |o| o.version + 1);
        objects.insert(
            path.to_string(),
            StoredObject {
                data,
                version: new_version,
                last_modified: Utc::now(),
            },
        );
        drop(objects);

        Ok(WriteResult::Success {
            version: new_version.to_string(),
        })
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.objects
            .write()
            .map_err(|_| Error::Internal {
                message: "lock poisoned".into(),
            })?
            .remove(path);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let objects = self.objects.read().map_err(|_| Error::Internal {
            message: "lock poisoned".into(),
        })?;

        Ok(objects
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(path, obj)| ObjectMeta {
                path: path.clone(),
                size: obj.data.len() as u64,
                version: obj.version.to_string(),
                last_modified: Some(obj.last_modified),
                etag: Some(format!("\"{}\"", obj.version)),
            })
            .collect())
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        let objects = self.objects.read().map_err(|_| Error::Internal {
            message: "lock poisoned".into(),
        })?;

        Ok(objects.get(path).map(|obj| ObjectMeta {
            path: path.to_string(),
            size: obj.data.len() as u64,
            version: obj.version.to_string(),
            last_modified: Some(obj.last_modified),
            etag: Some(format!("\"{}\"", obj.version)),
        }))
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        // Mock implementation for testing
        Ok(format!(
            "memory://localhost/{path}?expires={}&signature=mock",
            expiry.as_secs()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_backend_roundtrip() {
        let backend = MemoryBackend::new();
        let data = Bytes::from("hello world");

        let result = backend
            .put("test/file.txt", data.clone(), WritePrecondition::None)
            .await
            .expect("put should succeed");

        assert!(matches!(result, WriteResult::Success { ref version } if version == "1"));

        let retrieved = backend
            .get("test/file.txt")
            .await
            .expect("get should succeed");
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_object_meta_has_required_fields() {
        let backend = MemoryBackend::new();
        backend
            .put("test.txt", Bytes::from("data"), WritePrecondition::None)
            .await
            .expect("put should succeed");

        let meta = backend
            .head("test.txt")
            .await
            .expect("head should succeed")
            .expect("object should exist");

        // Required by architecture contract
        assert_eq!(meta.path, "test.txt");
        assert_eq!(meta.size, 4);
        assert!(!meta.version.is_empty(), "must have version");
        assert!(meta.last_modified.is_some(), "must have last_modified");
        assert!(meta.etag.is_some(), "must have etag");
    }

    #[tokio::test]
    async fn test_get_range_valid() {
        let backend = MemoryBackend::new();
        backend
            .put(
                "test.txt",
                Bytes::from("hello world"),
                WritePrecondition::None,
            )
            .await
            .expect("put should succeed");

        let result = backend
            .get_range("test.txt", 0..5)
            .await
            .expect("should succeed");
        assert_eq!(result, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_get_range_clamps_end() {
        let backend = MemoryBackend::new();
        backend
            .put("test.txt", Bytes::from("hello"), WritePrecondition::None)
            .await
            .expect("put should succeed");

        // End beyond length should clamp, not panic
        let result = backend
            .get_range("test.txt", 0..100)
            .await
            .expect("should succeed");
        assert_eq!(result, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_get_range_invalid_start() {
        let backend = MemoryBackend::new();
        backend
            .put("test.txt", Bytes::from("hello"), WritePrecondition::None)
            .await
            .expect("put should succeed");

        // Start beyond length should error, not panic
        let result = backend.get_range("test.txt", 100..200).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_range_invalid_end_before_start() {
        let backend = MemoryBackend::new();
        backend
            .put(
                "test.txt",
                Bytes::from("hello world"),
                WritePrecondition::None,
            )
            .await
            .expect("put should succeed");

        // End before start should error, not panic
        let result = backend.get_range("test.txt", 8..2).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_signed_url_generation() {
        let backend = MemoryBackend::new();
        backend
            .put("test.txt", Bytes::from("data"), WritePrecondition::None)
            .await
            .expect("put should succeed");

        let url = backend
            .signed_url("test.txt", Duration::from_secs(3600))
            .await
            .expect("signed_url should succeed");

        // Memory backend returns mock URL
        assert!(url.contains("test.txt"));
        assert!(url.contains("expires="));
    }

    #[tokio::test]
    async fn test_precondition_does_not_exist() {
        let backend = MemoryBackend::new();

        // First write with DoesNotExist should succeed
        let result = backend
            .put(
                "new.txt",
                Bytes::from("data"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("should succeed");
        assert!(matches!(result, WriteResult::Success { .. }));

        // Second write with DoesNotExist should fail
        let result = backend
            .put(
                "new.txt",
                Bytes::from("data2"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("should succeed");
        assert!(matches!(result, WriteResult::PreconditionFailed { .. }));
    }

    #[tokio::test]
    async fn test_precondition_matches_version() {
        let backend = MemoryBackend::new();

        // Create object
        let result = backend
            .put("gen.txt", Bytes::from("v1"), WritePrecondition::None)
            .await
            .expect("should succeed");
        let first_version = match result {
            WriteResult::Success { version } => version,
            _ => panic!("expected success"),
        };

        // Update with correct version should succeed
        let result = backend
            .put(
                "gen.txt",
                Bytes::from("v2"),
                WritePrecondition::MatchesVersion(first_version.clone()),
            )
            .await
            .expect("should succeed");
        assert!(matches!(result, WriteResult::Success { .. }));

        // Update with stale version should fail
        let result = backend
            .put(
                "gen.txt",
                Bytes::from("v3"),
                WritePrecondition::MatchesVersion(first_version),
            )
            .await
            .expect("should succeed");
        assert!(matches!(result, WriteResult::PreconditionFailed { .. }));
    }

    #[tokio::test]
    async fn test_list_with_prefix() {
        let backend = MemoryBackend::new();

        backend
            .put("a/1.txt", Bytes::from("a1"), WritePrecondition::None)
            .await
            .unwrap();
        backend
            .put("a/2.txt", Bytes::from("a2"), WritePrecondition::None)
            .await
            .unwrap();
        backend
            .put("b/1.txt", Bytes::from("b1"), WritePrecondition::None)
            .await
            .unwrap();

        let list_a = backend.list("a/").await.expect("should succeed");
        assert_eq!(list_a.len(), 2);

        let list_b = backend.list("b/").await.expect("should succeed");
        assert_eq!(list_b.len(), 1);
    }

    #[tokio::test]
    async fn test_delete() {
        let backend = MemoryBackend::new();

        backend
            .put("del.txt", Bytes::from("data"), WritePrecondition::None)
            .await
            .unwrap();
        assert!(backend.head("del.txt").await.unwrap().is_some());

        backend.delete("del.txt").await.expect("should succeed");
        assert!(backend.head("del.txt").await.unwrap().is_none());
    }
}
