//! Test storage implementations with operation tracing.
//!
//! Provides in-memory storage that records all operations for test assertions.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arco_core::error::{Error, Result};
use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use bytes::Bytes;
use chrono::{DateTime, Utc};

/// Record of a storage operation for test assertions.
#[derive(Debug, Clone)]
pub enum StorageOp {
    /// Get operation.
    Get {
        /// Path that was read.
        path: String,
    },
    /// GetRange operation (for Parquet range reads).
    GetRange {
        /// Path that was read.
        path: String,
        /// Start byte offset.
        start: u64,
        /// End byte offset.
        end: u64,
    },
    /// Head operation (metadata only).
    Head {
        /// Path that was checked.
        path: String,
    },
    /// Put operation.
    Put {
        /// Path that was written.
        path: String,
        /// Size of data written.
        size: usize,
        /// Precondition used.
        precondition: WritePrecondition,
    },
    /// Delete operation.
    Delete {
        /// Path that was deleted.
        path: String,
    },
    /// List operation.
    List {
        /// Prefix that was listed.
        prefix: String,
    },
}

/// In-memory storage backend with operation tracing.
///
/// Records all operations for later assertion in tests.
#[derive(Debug, Clone, Default)]
pub struct TracingMemoryBackend {
    data: Arc<Mutex<HashMap<String, StoredObject>>>,
    operations: Arc<Mutex<Vec<StorageOp>>>,
    fail_paths: Arc<Mutex<Vec<String>>>,
    latency: Option<Duration>,
}

#[derive(Debug, Clone)]
struct StoredObject {
    data: Bytes,
    /// Version stored as i64 internally, exposed as String via API (multi-cloud compat).
    version: i64,
    last_modified: DateTime<Utc>,
}

impl TracingMemoryBackend {
    /// Creates a new empty tracing storage.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates storage with simulated latency.
    #[must_use]
    pub fn with_latency(latency: Duration) -> Self {
        Self {
            latency: Some(latency),
            ..Self::default()
        }
    }

    /// Returns all recorded operations.
    #[must_use]
    pub fn operations(&self) -> Vec<StorageOp> {
        self.operations.lock().expect("lock").clone()
    }

    /// Clears recorded operations.
    pub fn clear_operations(&self) {
        self.operations.lock().expect("lock").clear();
    }

    /// Injects a failure for the given path prefix.
    pub fn inject_failure(&self, path: impl Into<String>) {
        self.fail_paths.lock().expect("lock").push(path.into());
    }

    /// Clears all injected failures.
    pub fn clear_failures(&self) {
        self.fail_paths.lock().expect("lock").clear();
    }

    /// Returns the current version for a path (for CAS testing).
    #[must_use]
    pub fn version(&self, path: &str) -> Option<String> {
        self.data
            .lock()
            .expect("lock")
            .get(path)
            .map(|o| o.version.to_string())
    }

    /// Returns all stored paths (for debugging).
    #[must_use]
    pub fn paths(&self) -> Vec<String> {
        self.data.lock().expect("lock").keys().cloned().collect()
    }

    fn record(&self, op: StorageOp) {
        self.operations.lock().expect("lock").push(op);
    }

    fn check_failure(&self, path: &str) -> Result<()> {
        let fail_paths = self.fail_paths.lock().expect("lock");
        if fail_paths.iter().any(|p| path.starts_with(p)) {
            return Err(Error::Internal {
                message: format!("Injected failure for path: {path}"),
            });
        }
        Ok(())
    }

    async fn maybe_delay(&self) {
        if let Some(latency) = self.latency {
            tokio::time::sleep(latency).await;
        }
    }
}

#[async_trait::async_trait]
impl StorageBackend for TracingMemoryBackend {
    async fn get(&self, path: &str) -> Result<Bytes> {
        self.maybe_delay().await;
        self.check_failure(path)?;
        self.record(StorageOp::Get {
            path: path.to_string(),
        });

        let data = self.data.lock().expect("lock");
        data.get(path)
            .map(|o| o.data.clone())
            .ok_or_else(|| Error::NotFound(format!("object not found: {path}")))
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.maybe_delay().await;
        self.check_failure(path)?;
        self.record(StorageOp::GetRange {
            path: path.to_string(),
            start: range.start,
            end: range.end,
        });

        // Validate range per StorageBackend contract
        if range.end < range.start {
            return Err(Error::InvalidInput(format!(
                "invalid range: end ({}) < start ({})",
                range.end, range.start
            )));
        }

        let data = self.data.lock().expect("lock");
        let obj = data
            .get(path)
            .ok_or_else(|| Error::NotFound(format!("object not found: {path}")))?;

        let len = obj.data.len() as u64;
        if range.start > len {
            return Err(Error::InvalidInput(format!(
                "invalid range: start ({}) > object length ({len})",
                range.start
            )));
        }

        let start = range.start as usize;
        let end = std::cmp::min(range.end as usize, obj.data.len());
        Ok(obj.data.slice(start..end))
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        self.maybe_delay().await;
        self.check_failure(path)?;
        self.record(StorageOp::Head {
            path: path.to_string(),
        });

        let data = self.data.lock().expect("lock");
        Ok(data.get(path).map(|o| ObjectMeta {
            path: path.to_string(),
            size: o.data.len() as u64,
            version: o.version.to_string(),
            last_modified: Some(o.last_modified),
            etag: Some(format!("\"{}\"", o.version)),
        }))
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        self.maybe_delay().await;
        self.check_failure(path)?;
        self.record(StorageOp::Put {
            path: path.to_string(),
            size: data.len(),
            precondition: precondition.clone(),
        });

        let mut store = self.data.lock().expect("lock");
        let existing = store.get(path);

        // Check precondition
        match &precondition {
            WritePrecondition::None => {}
            WritePrecondition::DoesNotExist => {
                if let Some(obj) = existing {
                    return Ok(WriteResult::PreconditionFailed {
                        current_version: obj.version.to_string(),
                    });
                }
            }
            WritePrecondition::MatchesVersion(expected) => {
                let expected_num: i64 = expected.parse().unwrap_or(-1);
                match existing {
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
        }

        let new_version = existing.map_or(1, |o| o.version + 1);

        store.insert(
            path.to_string(),
            StoredObject {
                data,
                version: new_version,
                last_modified: Utc::now(),
            },
        );

        Ok(WriteResult::Success {
            version: new_version.to_string(),
        })
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.maybe_delay().await;
        self.check_failure(path)?;
        self.record(StorageOp::Delete {
            path: path.to_string(),
        });

        self.data.lock().expect("lock").remove(path);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.maybe_delay().await;
        self.check_failure(prefix)?;
        self.record(StorageOp::List {
            prefix: prefix.to_string(),
        });

        let data = self.data.lock().expect("lock");
        Ok(data
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| ObjectMeta {
                path: k.clone(),
                size: v.data.len() as u64,
                version: v.version.to_string(),
                last_modified: Some(v.last_modified),
                etag: Some(format!("\"{}\"", v.version)),
            })
            .collect())
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        self.maybe_delay().await;
        self.check_failure(path)?;
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
    async fn tracing_storage_records_operations() {
        let storage = TracingMemoryBackend::new();

        storage
            .put("test.txt", Bytes::from("hello"), WritePrecondition::None)
            .await
            .expect("put");
        let _ = storage.get("test.txt").await;
        let _ = storage.list("").await;

        let ops = storage.operations();
        assert_eq!(ops.len(), 3);
        assert!(matches!(ops[0], StorageOp::Put { .. }));
        assert!(matches!(ops[1], StorageOp::Get { .. }));
        assert!(matches!(ops[2], StorageOp::List { .. }));
    }

    #[tokio::test]
    async fn tracing_storage_failure_injection() {
        let storage = TracingMemoryBackend::new();
        storage.inject_failure("fail/");

        let result = storage.get("fail/test.txt").await;
        assert!(result.is_err());

        // Write first to a non-failing path
        storage
            .put("ok/test.txt", Bytes::from("data"), WritePrecondition::None)
            .await
            .expect("put");
        let result = storage.get("ok/test.txt").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn tracing_storage_cas_works() {
        let storage = TracingMemoryBackend::new();

        // First write succeeds
        let result = storage
            .put("test.txt", Bytes::from("v1"), WritePrecondition::DoesNotExist)
            .await
            .expect("put");
        assert!(matches!(result, WriteResult::Success { ref version } if version == "1"));

        // Second write with DoesNotExist fails
        let result = storage
            .put("test.txt", Bytes::from("v2"), WritePrecondition::DoesNotExist)
            .await
            .expect("put");
        assert!(matches!(result, WriteResult::PreconditionFailed { .. }));

        // Write with correct version succeeds
        let result = storage
            .put(
                "test.txt",
                Bytes::from("v2"),
                WritePrecondition::MatchesVersion("1".to_string()),
            )
            .await
            .expect("put");
        assert!(matches!(result, WriteResult::Success { ref version } if version == "2"));
    }
}
