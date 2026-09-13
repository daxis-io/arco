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
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicI64, Ordering},
};
use std::time::Duration;

use crate::error::{Error, Result};

/// Precondition for conditional writes (CAS operations).
///
/// The version token is opaque - backends interpret it according to their semantics:
/// - GCS: Numeric generation as string
/// - S3: `ETag` or version ID
/// - Azure: `ETag`
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
    /// - Azure: `ETag`
    pub version: String,
    /// Last modification timestamp.
    pub last_modified: Option<DateTime<Utc>>,
    /// Entity tag for cache validation.
    pub etag: Option<String>,
}

/// One bounded, lexicographically ordered page of object metadata.
#[derive(Debug, Clone)]
pub struct ListPage {
    /// Objects in strictly increasing path order.
    pub objects: Vec<ObjectMeta>,
    /// Exclusive path cursor for the next page.
    ///
    /// A full page carries a cursor even when it was the final page. In that
    /// exact-multiple case, the following request returns an empty page with no
    /// cursor and establishes exhaustion without reading ahead.
    pub next_start_after: Option<String>,
}

impl ListPage {
    fn empty() -> Self {
        Self {
            objects: Vec::new(),
            next_start_after: None,
        }
    }
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

    /// Lists one bounded page under `prefix`, starting strictly after
    /// `start_after` in lexicographic path order.
    ///
    /// Implementations must return at most `limit` objects. A full page returns
    /// the final path as [`ListPage::next_start_after`]; a short page proves
    /// exhaustion and returns no cursor. A zero limit performs no listing and
    /// returns an empty exhausted page.
    ///
    /// The default deliberately fails closed. Falling back to [`Self::list`]
    /// would make a nominally bounded scan enumerate the entire prefix.
    async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ListPage> {
        validate_page_cursor(prefix, start_after)?;
        if limit == 0 {
            return Ok(ListPage::empty());
        }
        let _ = (prefix, start_after);
        Err(Error::storage(
            "bounded ordered listing is not supported by this storage backend",
        ))
    }

    /// Gets object metadata without reading content.
    ///
    /// Returns `None` if object doesn't exist.
    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>>;

    /// Generates a signed URL for direct access.
    ///
    /// Per architecture docs: required for direct client access.
    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String>;
}

fn validate_page_cursor(prefix: &str, start_after: Option<&str>) -> Result<()> {
    let Some(start_after) = start_after else {
        return Ok(());
    };
    let boundary = if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{prefix}/")
    };
    if !prefix.is_empty() && start_after != prefix && !start_after.starts_with(&boundary) {
        return Err(Error::InvalidInput(format!(
            "list cursor '{start_after}' is outside prefix '{prefix}'"
        )));
    }
    Ok(())
}

fn validate_ordered_page(
    prefix: &str,
    start_after: Option<&str>,
    limit: usize,
    objects: &[ObjectMeta],
) -> Result<()> {
    if objects.len() > limit {
        return Err(Error::storage(format!(
            "bounded list for '{prefix}' returned {} objects for limit {limit}",
            objects.len()
        )));
    }
    if let (Some(start_after), Some(first)) = (start_after, objects.first())
        && first.path.as_str() <= start_after
    {
        return Err(Error::storage(format!(
            "bounded list for '{prefix}' returned non-exclusive path '{}' after '{start_after}'",
            first.path
        )));
    }
    if let Some((left, right)) = objects.windows(2).find_map(|window| match window {
        [left, right] if left.path >= right.path => Some((left, right)),
        _ => None,
    }) {
        return Err(Error::storage(format!(
            "bounded list for '{prefix}' was not strictly ordered: '{}' then '{}'",
            left.path, right.path
        )));
    }
    Ok(())
}

fn next_start_after(objects: &[ObjectMeta], limit: usize) -> Option<String> {
    if objects.len() == limit {
        objects.last().map(|meta| meta.path.clone())
    } else {
        None
    }
}

/// In-memory storage backend for testing.
///
/// Thread-safe via `RwLock`. Not suitable for production.
/// Uses numeric versions internally (stored as strings) to simulate GCS-like behavior.
#[derive(Debug, Default)]
pub struct MemoryBackend {
    objects: Arc<RwLock<HashMap<String, StoredObject>>>,
    next_version: Arc<AtomicI64>,
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
                let expected_num = expected.parse::<i64>().ok();
                match current {
                    Some(obj) if Some(obj.version) != expected_num => {
                        return Ok(WriteResult::PreconditionFailed {
                            current_version: obj.version.to_string(),
                        });
                    }
                    None => {
                        return Ok(WriteResult::PreconditionFailed {
                            current_version: String::new(),
                        });
                    }
                    _ => {}
                }
            }
            WritePrecondition::None => {}
        }

        let new_version = self.next_version.fetch_add(1, Ordering::SeqCst) + 1;
        objects.insert(
            path.to_string(),
            StoredObject {
                data,
                version: new_version,
                last_modified: crate::wall_clock(),
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

    async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ListPage> {
        validate_page_cursor(prefix, start_after)?;
        if limit == 0 {
            return Ok(ListPage::empty());
        }

        let mut objects = self.list(prefix).await?;
        objects.sort_by(|left, right| left.path.cmp(&right.path));
        if let Some(start_after) = start_after {
            let first = objects.partition_point(|meta| meta.path.as_str() <= start_after);
            objects.drain(..first);
        }
        objects.truncate(limit);
        validate_ordered_page(prefix, start_after, limit, &objects)?;

        let next_start_after = next_start_after(&objects, limit);
        Ok(ListPage {
            objects,
            next_start_after,
        })
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
// Advisory lint scope for test code (#331): the allowed pedantic/nursery
// lints conflict with test ergonomics here; production code keeps them active.
#[allow(clippy::manual_let_else, clippy::match_wildcard_for_single_variants)]
mod tests {
    use super::*;

    async fn run_precondition_conformance<B: StorageBackend>(
        backend: &B,
        base_path: &str,
    ) -> Result<()> {
        let path = format!("{base_path}/conformance.txt");

        let first = backend
            .put(&path, Bytes::from("v1"), WritePrecondition::DoesNotExist)
            .await?;
        let first_version = match first {
            WriteResult::Success { version } => version,
            WriteResult::PreconditionFailed { .. } => {
                panic!("first DoesNotExist write must succeed")
            }
        };
        assert!(!first_version.is_empty(), "version token must be non-empty");

        let duplicate = backend
            .put(
                &path,
                Bytes::from("v1-duplicate"),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        match duplicate {
            WriteResult::PreconditionFailed { current_version } => {
                assert_eq!(current_version, first_version);
            }
            WriteResult::Success { .. } => panic!("second DoesNotExist write must fail"),
        }

        let second = backend
            .put(
                &path,
                Bytes::from("v2"),
                WritePrecondition::MatchesVersion(first_version.clone()),
            )
            .await?;
        let second_version = match second {
            WriteResult::Success { version } => version,
            WriteResult::PreconditionFailed { .. } => {
                panic!("MatchesVersion with current token must succeed")
            }
        };
        assert_ne!(
            first_version, second_version,
            "version token must advance on successful conditional write"
        );

        let stale = backend
            .put(
                &path,
                Bytes::from("v3"),
                WritePrecondition::MatchesVersion(first_version),
            )
            .await?;
        match stale {
            WriteResult::PreconditionFailed { current_version } => {
                assert_eq!(current_version, second_version);
            }
            WriteResult::Success { .. } => panic!("stale MatchesVersion must fail"),
        }

        let meta = backend
            .head(&path)
            .await?
            .expect("conformance object should exist");
        assert_eq!(meta.version, second_version);
        assert!(
            !meta.version.is_empty(),
            "head version token must be non-empty"
        );
        Ok(())
    }

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
    // The reversed range is the point of this test: get_range must reject
    // end-before-start as an error instead of panicking.
    #[allow(clippy::reversed_empty_ranges)]
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
    async fn test_list_page_is_exclusive_and_exact_multiples_end_empty() {
        let backend = MemoryBackend::new();
        for index in 0..6 {
            backend
                .put(
                    &format!("ledger/evt-{index:02}.json"),
                    Bytes::from_static(b"event"),
                    WritePrecondition::None,
                )
                .await
                .expect("seed page object");
        }

        let first = backend
            .list_page("ledger/", None, 3)
            .await
            .expect("first page");
        let first_paths: Vec<&str> = first
            .objects
            .iter()
            .map(|meta| meta.path.as_str())
            .collect();
        assert_eq!(
            first_paths,
            [
                "ledger/evt-00.json",
                "ledger/evt-01.json",
                "ledger/evt-02.json"
            ]
        );
        assert_eq!(
            first.next_start_after.as_deref(),
            Some("ledger/evt-02.json")
        );

        let second = backend
            .list_page("ledger/", first.next_start_after.as_deref(), 3)
            .await
            .expect("second page");
        let second_paths: Vec<&str> = second
            .objects
            .iter()
            .map(|meta| meta.path.as_str())
            .collect();
        assert_eq!(
            second_paths,
            [
                "ledger/evt-03.json",
                "ledger/evt-04.json",
                "ledger/evt-05.json"
            ]
        );
        assert_eq!(
            second.next_start_after.as_deref(),
            Some("ledger/evt-05.json")
        );

        let exhausted = backend
            .list_page("ledger/", second.next_start_after.as_deref(), 3)
            .await
            .expect("exhaustion page");
        assert!(exhausted.objects.is_empty());
        assert!(exhausted.next_start_after.is_none());
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

    #[tokio::test]
    async fn test_memory_backend_precondition_conformance_harness() {
        let backend = MemoryBackend::new();
        run_precondition_conformance(&backend, "memory")
            .await
            .expect("memory backend must satisfy precondition conformance");
    }
}
