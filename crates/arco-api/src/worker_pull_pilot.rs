//! Starts one operator-configured Flow worker-pull workspace in the API.

use std::io::Read;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arco_core::storage::{ClassifiedBytes, ListPage, ObjectMeta, StorageBackend};
use arco_core::{Error, Result, ScopedStorage, WritePrecondition, WriteResult};
use arco_flow::orchestration::events::TaskDef;
use arco_flow::orchestration::worker_pull::{WorkerPull, WorkerPullConfig};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;

use crate::config::Config;
use crate::error::{ApiError, ApiResult};

const MAX_CONFIG_BYTES: u64 = 64 * 1024;
pub const MOUNT: &str = "/worker-pull";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PilotFile {
    tenant: String,
    workspace: String,
    storage_prefix: Option<String>,
    host: WorkerPullConfig,
}

pub struct WorkerPullPilot {
    tenant: String,
    workspace: String,
    pub(crate) host: WorkerPull,
}

impl WorkerPullPilot {
    pub(crate) fn load(
        path: &Path,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<(Arc<dyn StorageBackend>, Self)> {
        let invalid = || Error::InvalidInput("invalid worker-pull pilot configuration".into());
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .and_then(|file| file.take(MAX_CONFIG_BYTES + 1).read_to_end(&mut bytes))
            .map_err(|_| {
                Error::InvalidInput("cannot read worker-pull pilot configuration".into())
            })?;
        if bytes.len() as u64 > MAX_CONFIG_BYTES {
            return Err(invalid());
        }
        let PilotFile {
            tenant,
            workspace,
            storage_prefix,
            host,
        } = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        let callback: axum::http::Uri = host.callback_base_url.parse().map_err(|_| invalid())?;
        if !valid_segment(&tenant)
            || !valid_segment(&workspace)
            || callback.path() != MOUNT
            || callback
                .authority()
                .is_none_or(|authority| authority.as_str().contains('@'))
        {
            return Err(invalid());
        }
        let backend: Arc<dyn StorageBackend> = match storage_prefix {
            Some(prefix) => Arc::new(PrefixedStorage::new(backend, &prefix)?),
            None => backend,
        };
        let storage = ScopedStorage::new(Arc::clone(&backend), &tenant, &workspace)?;
        let host = WorkerPull::new(storage, host).map_err(|_| invalid())?;
        Ok((
            backend,
            Self {
                tenant,
                workspace,
                host,
            },
        ))
    }

    pub(crate) fn matches(&self, tenant: &str, workspace: &str) -> bool {
        self.tenant == tenant && self.workspace == workspace
    }
}

fn valid_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

pub fn validate_api_config(config: &Config) -> Result<()> {
    let bucket = config
        .storage
        .bucket
        .as_deref()
        .filter(|_| !config.debug)
        .ok_or_else(|| {
            Error::InvalidInput(
                "worker-pull pilot requires JWT authentication and durable storage".into(),
            )
        })?
        .trim();
    if bucket
        .strip_prefix("s3://")
        .or_else(|| bucket.strip_prefix("s3a://"))
        .is_some_and(|bucket| bucket.trim_end_matches('/').contains('/'))
    {
        return Err(Error::InvalidInput(
            "worker-pull storage bucket must omit its path; use storagePrefix in the pilot file"
                .into(),
        ));
    }
    Ok(())
}

pub fn require_legacy_flow_writes(config: &Config) -> ApiResult<()> {
    if config.worker_pull_pilot_path.is_some() {
        return Err(ApiError::conflict(
            "legacy orchestration writes are disabled for the worker-pull pilot",
        ));
    }
    Ok(())
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionLimits {
    max_attempts: Option<u32>,
    heartbeat_timeout_sec: Option<u32>,
}

impl ExecutionLimits {
    pub(crate) fn parse(execution: &Value, io: &Value) -> ApiResult<Self> {
        if !io.is_null() && !io.as_object().is_some_and(serde_json::Map::is_empty) {
            return Err(ApiError::bad_request(
                "worker-pull pilot assets must omit native I/O declarations",
            ));
        }
        if execution.is_null() {
            return Ok(Self::default());
        }
        let limits = Self::deserialize(execution).map_err(|_| {
            ApiError::bad_request("asset execution must declare valid worker-pull limits")
        })?;
        if limits.max_attempts == Some(0) || limits.heartbeat_timeout_sec == Some(0) {
            return Err(ApiError::bad_request(
                "worker-pull execution limits must be positive",
            ));
        }
        Ok(limits)
    }

    pub(crate) fn apply(self, task: &mut TaskDef) {
        if let Some(max_attempts) = self.max_attempts {
            task.max_attempts = max_attempts;
        }
        if let Some(timeout) = self.heartbeat_timeout_sec {
            task.heartbeat_timeout_sec = timeout;
        }
        task.requires_visible_output = false;
    }
}

// Translate object keys while preserving the provider's original conditional-write tokens.
struct PrefixedStorage {
    backend: Arc<dyn StorageBackend>,
    prefix: String,
}

impl PrefixedStorage {
    fn new(backend: Arc<dyn StorageBackend>, prefix: &str) -> Result<Self> {
        if prefix.len() > 512 || !prefix.split('/').all(valid_segment) {
            return Err(Error::InvalidInput(
                "invalid worker-pull storage prefix".into(),
            ));
        }
        Ok(Self {
            backend,
            prefix: format!("{prefix}/"),
        })
    }

    fn physical(&self, path: &str) -> Result<String> {
        ScopedStorage::validate_path(path)?;
        Ok(format!("{}{path}", self.prefix))
    }

    fn logical(&self, path: &str) -> Result<String> {
        path.strip_prefix(&self.prefix)
            .map(str::to_owned)
            .ok_or_else(|| Error::storage("storage returned a key outside the pilot prefix"))
    }

    fn logical_meta(&self, mut meta: ObjectMeta) -> Result<ObjectMeta> {
        meta.path = self.logical(&meta.path)?;
        Ok(meta)
    }
}

#[async_trait::async_trait]
impl StorageBackend for PrefixedStorage {
    async fn get(&self, path: &str) -> Result<Bytes> {
        self.backend.get(&self.physical(path)?).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.backend.get_range(&self.physical(path)?, range).await
    }

    async fn get_range_with_ownership(
        &self,
        path: &str,
        range: Range<u64>,
    ) -> Result<ClassifiedBytes> {
        self.backend
            .get_range_with_ownership(&self.physical(path)?, range)
            .await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        condition: WritePrecondition,
    ) -> Result<WriteResult> {
        self.backend
            .put(&self.physical(path)?, data, condition)
            .await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.backend.delete(&self.physical(path)?).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.backend
            .list(&self.physical(prefix)?)
            .await?
            .into_iter()
            .map(|meta| self.logical_meta(meta))
            .collect()
    }

    async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ListPage> {
        let cursor = start_after
            .map(|cursor| self.physical(cursor))
            .transpose()?;
        let page = self
            .backend
            .list_page(&self.physical(prefix)?, cursor.as_deref(), limit)
            .await?;
        Ok(ListPage {
            objects: page
                .objects
                .into_iter()
                .map(|meta| self.logical_meta(meta))
                .collect::<Result<_>>()?,
            next_start_after: page
                .next_start_after
                .as_deref()
                .map(|cursor| self.logical(cursor))
                .transpose()?,
        })
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        self.backend
            .head(&self.physical(path)?)
            .await?
            .map(|meta| self.logical_meta(meta))
            .transpose()
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        self.backend.signed_url(&self.physical(path)?, expiry).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use arco_core::storage::MemoryBackend;

    #[test]
    fn pilot_rejects_an_s3_bucket_path_instead_of_silently_using_the_bucket_root() {
        let mut config = Config::default();
        config.storage.bucket = Some("s3://pilot/ignored".into());
        assert!(validate_api_config(&config).is_err());
        config.storage.bucket = Some("s3://pilot".into());
        assert!(validate_api_config(&config).is_ok());
    }

    #[tokio::test]
    async fn prefix_storage_preserves_conditions_and_page_cursors() {
        let backing = Arc::new(MemoryBackend::new());
        let root = PrefixedStorage::new(backing.clone(), "conformance/s3/run").unwrap();
        let data = Bytes::from_static(b"test");
        for path in ["dir/a", "dir/b"] {
            root.put(path, data.clone(), WritePrecondition::DoesNotExist)
                .await
                .unwrap();
        }
        backing
            .put(
                "conformance/s3/run-other/dir/a",
                data.clone(),
                WritePrecondition::None,
            )
            .await
            .unwrap();
        let head = root.head("dir/a").await.unwrap().unwrap();
        let native = backing
            .head("conformance/s3/run/dir/a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.version, native.version);
        assert_eq!(head.etag, native.etag);
        assert_eq!(head.path, "dir/a");
        assert!(matches!(
            root.put(
                "dir/a",
                data,
                WritePrecondition::MatchesVersion(head.version)
            )
            .await
            .unwrap(),
            WriteResult::Success { .. }
        ));

        let first = root.list_page("dir/", None, 1).await.unwrap();
        assert_eq!(first.objects[0].path, "dir/a");
        assert_eq!(first.next_start_after.as_deref(), Some("dir/a"));
        let second = root
            .list_page("dir/", first.next_start_after.as_deref(), 1)
            .await
            .unwrap();
        assert_eq!(second.objects[0].path, "dir/b");
        assert!(
            root.list_page("dir/", Some("../other/dir/a"), 1)
                .await
                .is_err()
        );
        assert_eq!(root.list("").await.unwrap().len(), 2);
    }
}
