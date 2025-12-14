//! Storage backend abstraction for object storage (GCS, S3, local).
//!
//! This module defines the core storage contract that all backends must implement.
//! The contract matches the architecture docs:
//! - Conditional writes with preconditions
//! - Object metadata including `last_modified` and `etag`
//! - Signed URL generation for direct access

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::error::{Error, Result};

/// Precondition for conditional writes (CAS operations).
#[derive(Debug, Clone)]
pub enum WritePrecondition {
    /// Write only if object does not exist.
    DoesNotExist,
    /// Write only if object's generation matches.
    MatchesGeneration(i64),
    /// Write unconditionally.
    None,
}

/// Result of a conditional write.
#[derive(Debug, Clone)]
pub enum WriteResult {
    /// Write succeeded, returns new generation.
    Success {
        /// The new generation number after the write.
        generation: i64,
    },
    /// Precondition failed, returns current generation.
    PreconditionFailed {
        /// The current generation that caused the precondition to fail.
        current_generation: i64,
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
    /// Object generation (version number for CAS).
    pub generation: i64,
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

/// In-memory storage backend for testing.
///
/// Thread-safe via `RwLock`. Not suitable for production.
#[derive(Debug, Default)]
pub struct MemoryBackend {
    objects: Arc<RwLock<HashMap<String, StoredObject>>>,
}

#[derive(Debug, Clone)]
struct StoredObject {
    data: Bytes,
    generation: i64,
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
                        current_generation: obj.generation,
                    });
                }
            }
            WritePrecondition::MatchesGeneration(expected) => match current {
                Some(obj) if obj.generation != expected => {
                    return Ok(WriteResult::PreconditionFailed {
                        current_generation: obj.generation,
                    });
                }
                None => {
                    return Ok(WriteResult::PreconditionFailed {
                        current_generation: 0,
                    });
                }
                _ => {}
            },
            WritePrecondition::None => {}
        }

        let new_gen = current.map_or(1, |o| o.generation + 1);
        objects.insert(
            path.to_string(),
            StoredObject {
                data,
                generation: new_gen,
                last_modified: Utc::now(),
            },
        );
        drop(objects);

        Ok(WriteResult::Success {
            generation: new_gen,
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
                generation: obj.generation,
                last_modified: Some(obj.last_modified),
                etag: Some(format!("\"{}\"", obj.generation)),
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
            generation: obj.generation,
            last_modified: Some(obj.last_modified),
            etag: Some(format!("\"{}\"", obj.generation)),
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

        assert!(matches!(result, WriteResult::Success { generation: 1 }));

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
        assert!(meta.generation > 0);
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
    async fn test_precondition_matches_generation() {
        let backend = MemoryBackend::new();

        // Create object
        let result = backend
            .put("gen.txt", Bytes::from("v1"), WritePrecondition::None)
            .await
            .expect("should succeed");
        let first_gen = match result {
            WriteResult::Success { generation } => generation,
            _ => panic!("expected success"),
        };

        // Update with correct generation should succeed
        let result = backend
            .put(
                "gen.txt",
                Bytes::from("v2"),
                WritePrecondition::MatchesGeneration(first_gen),
            )
            .await
            .expect("should succeed");
        assert!(matches!(result, WriteResult::Success { .. }));

        // Update with stale generation should fail
        let result = backend
            .put(
                "gen.txt",
                Bytes::from("v3"),
                WritePrecondition::MatchesGeneration(first_gen),
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
