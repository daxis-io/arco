#![allow(dead_code)]

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use arco_core::error::Result;
use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Barrier;

#[derive(Debug, Clone)]
pub struct BarrierMatch {
    path_prefix: String,
    matches_version_only: bool,
}

impl BarrierMatch {
    pub fn matches_version(path_prefix: impl Into<String>) -> Self {
        Self {
            path_prefix: path_prefix.into(),
            matches_version_only: true,
        }
    }

    fn matches(&self, path: &str, precondition: &WritePrecondition) -> bool {
        path.starts_with(&self.path_prefix)
            && (!self.matches_version_only
                || matches!(precondition, WritePrecondition::MatchesVersion(_)))
    }
}

pub struct BarrierBackend {
    inner: Arc<dyn StorageBackend>,
    barrier: Arc<Barrier>,
    matcher: BarrierMatch,
}

impl std::fmt::Debug for BarrierBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BarrierBackend")
            .field("matcher", &self.matcher)
            .finish()
    }
}

impl BarrierBackend {
    pub fn new(inner: Arc<dyn StorageBackend>, parties: usize, matcher: BarrierMatch) -> Self {
        Self {
            inner,
            barrier: Arc::new(Barrier::new(parties)),
            matcher,
        }
    }
}

#[async_trait]
impl StorageBackend for BarrierBackend {
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
        if self.matcher.matches(path, &precondition) {
            self.barrier.wait().await;
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}
