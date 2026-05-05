#![allow(dead_code)]

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arco_core::error::{Error, Result};
use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum SpyOp {
    Get {
        path: String,
    },
    GetRange {
        path: String,
        start: u64,
        end: u64,
    },
    Put {
        path: String,
        precondition: WritePrecondition,
    },
    Delete {
        path: String,
    },
    List {
        prefix: String,
    },
    Head {
        path: String,
    },
    SignedUrl {
        path: String,
    },
}

pub struct SpyBackend {
    inner: Arc<dyn StorageBackend>,
    ops: std::sync::Mutex<Vec<SpyOp>>,
    fail_get_prefixes: std::sync::Mutex<Vec<String>>,
    fail_on_list: AtomicBool,
}

impl std::fmt::Debug for SpyBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpyBackend")
            .field("ops_len", &self.ops.lock().expect("spy ops lock").len())
            .field("fail_on_list", &self.fail_on_list.load(Ordering::SeqCst))
            .finish()
    }
}

impl SpyBackend {
    pub fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            ops: std::sync::Mutex::new(Vec::new()),
            fail_get_prefixes: std::sync::Mutex::new(Vec::new()),
            fail_on_list: AtomicBool::new(false),
        }
    }

    pub fn set_fail_on_list(&self, fail_on_list: bool) {
        self.fail_on_list.store(fail_on_list, Ordering::SeqCst);
    }

    pub fn ops(&self) -> Vec<SpyOp> {
        self.ops.lock().expect("spy ops lock").clone()
    }

    pub fn add_fail_get_prefix(&self, prefix: impl Into<String>) {
        self.fail_get_prefixes
            .lock()
            .expect("spy get prefixes lock")
            .push(prefix.into());
    }

    pub fn clear_ops(&self) {
        self.ops.lock().expect("spy ops lock").clear();
    }

    fn record(&self, op: SpyOp) {
        self.ops.lock().expect("spy ops lock").push(op);
    }

    fn should_fail_get(&self, path: &str) -> bool {
        self.fail_get_prefixes
            .lock()
            .expect("spy get prefixes lock")
            .iter()
            .any(|prefix| path.starts_with(prefix))
    }
}

#[async_trait]
impl StorageBackend for SpyBackend {
    async fn get(&self, path: &str) -> Result<Bytes> {
        self.record(SpyOp::Get {
            path: path.to_string(),
        });
        if self.should_fail_get(path) {
            return Err(Error::storage(format!(
                "unexpected get() call for path '{path}'"
            )));
        }
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.record(SpyOp::GetRange {
            path: path.to_string(),
            start: range.start,
            end: range.end,
        });
        if self.should_fail_get(path) {
            return Err(Error::storage(format!(
                "unexpected get_range() call for path '{path}'"
            )));
        }
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        self.record(SpyOp::Put {
            path: path.to_string(),
            precondition: precondition.clone(),
        });
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.record(SpyOp::Delete {
            path: path.to_string(),
        });
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.record(SpyOp::List {
            prefix: prefix.to_string(),
        });
        if self.fail_on_list.load(Ordering::SeqCst) {
            return Err(Error::storage(format!(
                "unexpected list() call for prefix '{prefix}'"
            )));
        }
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        self.record(SpyOp::Head {
            path: path.to_string(),
        });
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        self.record(SpyOp::SignedUrl {
            path: path.to_string(),
        });
        self.inner.signed_url(path, expiry).await
    }
}
