//! Loopback S3 fixture for physical isolation and conditional pointer faults.

use std::{
    fs::OpenOptions,
    io::Write,
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arco_core::{
    Error, Result,
    storage::{
        ClassifiedBytes, ListPage, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
    },
};
use arco_flow::{orchestration_compaction_lock_path, orchestration_manifest_pointer_path};
use async_trait::async_trait;
use axum::http::Uri;
use bytes::Bytes;
use serde_json::{Value, json};

enum FaultMode {
    StopBefore,
    StopAfter,
    LoseAfter,
    HoldBefore,
}

struct PointerFault {
    scope: String,
    mode: FaultMode,
    marker: PathBuf,
}

const HOSTED_BUCKET: &str = "s3://arco-030-q-452456948477-20261024";
const HOSTED_REGION: &str = "us-east-2";
const MAX_HOSTED_READS: usize = 256;
const MAX_HOSTED_WRITES: usize = 64;

#[derive(Default)]
struct Budget {
    reads: AtomicUsize,
    writes: AtomicUsize,
}

pub struct FixtureBackend {
    s3: Arc<dyn StorageBackend>,
    root: String,
    fault: Option<PointerFault>,
    budget: Option<Budget>,
}

pub struct FixtureConfig {
    root: String,
    fault: Option<PointerFault>,
    hosted: bool,
}

pub fn validate(bucket: &str, scope: String, action: &str) -> Result<FixtureConfig> {
    let hosted = match std::env::var("ARCO_PULL_TEST_MODE").as_deref() {
        Ok("hosted") => true,
        Err(std::env::VarError::NotPresent) => false,
        _ => return Err(Error::InvalidInput("invalid S3 fixture mode".into())),
    };
    let root = if hosted {
        hosted_root(bucket, action)?
    } else {
        loopback_root()?
    };
    let mode = match std::env::var("ARCO_PULL_TEST_FAULT").as_deref() {
        Ok("stop-before") if !hosted => Some(FaultMode::StopBefore),
        Ok("stop-after") if !hosted => Some(FaultMode::StopAfter),
        Ok("lose-after")
            if !hosted
                || root.ends_with("/reply-loss/")
                || (root.ends_with("/rivers-retry/") && action.ends_with("/completed")) =>
        {
            Some(FaultMode::LoseAfter)
        }
        Ok("hold-before") if !hosted => Some(FaultMode::HoldBefore),
        Err(std::env::VarError::NotPresent) => None,
        _ => return Err(Error::InvalidInput("invalid fixture pointer fault".into())),
    };
    let fault = mode
        .map(|mode| {
            let marker = std::env::var_os("ARCO_PULL_TEST_MARKER")
                .map(PathBuf::from)
                .filter(|path| path.is_absolute() && !path.exists())
                .ok_or_else(|| {
                    Error::InvalidInput("fixture marker must be a fresh absolute path".into())
                })?;
            Ok::<_, Error>(PointerFault {
                scope,
                mode,
                marker,
            })
        })
        .transpose()?;
    Ok(FixtureConfig {
        root,
        fault,
        hosted,
    })
}

impl FixtureConfig {
    pub fn describe(&self) -> Value {
        json!({
            "hosted": self.hosted,
            "physicalPrefix": self.root,
            "logicalReadLimit": self.hosted.then_some(MAX_HOSTED_READS),
            "logicalWriteLimit": self.hosted.then_some(MAX_HOSTED_WRITES),
        })
    }

    pub fn open(self, bucket: &str) -> Result<Arc<FixtureBackend>> {
        let s3 = arco_storage::from_bucket(bucket)?;
        Ok(Arc::new(FixtureBackend {
            s3,
            root: self.root,
            fault: self.fault,
            budget: self.hosted.then(Budget::default),
        }))
    }
}

fn loopback_root() -> Result<String> {
    let endpoint = std::env::var("AWS_ENDPOINT")
        .ok()
        .and_then(|endpoint| endpoint.parse::<Uri>().ok());
    if !endpoint.is_some_and(|uri| {
        uri.scheme_str() == Some("http")
            && uri.host() == Some("127.0.0.1")
            && uri.port_u16().is_some()
            && uri.path() == "/"
            && uri.query().is_none()
    }) || [
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
        "AWS_SESSION_TOKEN",
        "AWS_PROFILE",
    ]
    .iter()
    .any(|name| std::env::var_os(name).is_some())
    {
        return Err(Error::InvalidInput(
            "worker-pull S3 fixture requires an explicit loopback endpoint and fixture credentials"
                .into(),
        ));
    }
    match std::env::var("ARCO_PULL_TEST_PREFIX") {
        Ok(prefix) => fixture_root(&prefix),
        Err(std::env::VarError::NotPresent) => Ok(String::new()),
        Err(_) => Err(Error::InvalidInput("invalid fixture prefix".into())),
    }
}

fn hosted_root(bucket: &str, action: &str) -> Result<String> {
    const ALLOWED_AWS_ENV: &[&str] = &[
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_REGION",
        "AWS_EC2_METADATA_DISABLED",
        "AWS_CONFIG_FILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "AWS_MAX_ATTEMPTS",
    ];
    if bucket != HOSTED_BUCKET || std::env::var("AWS_REGION").as_deref() != Ok(HOSTED_REGION) {
        return Err(Error::InvalidInput(
            "hosted fixture requires the approved Ohio bucket".into(),
        ));
    }
    if std::env::vars_os().any(|(key, _)| {
        let key = key.to_string_lossy();
        (key.starts_with("AWS_") && !ALLOWED_AWS_ENV.contains(&key.as_ref()))
            || matches!(
                key.to_ascii_lowercase().as_str(),
                "http_proxy" | "https_proxy" | "all_proxy"
            )
    }) {
        return Err(Error::InvalidInput(
            "hosted fixture rejects AWS and proxy overrides".into(),
        ));
    }
    let printable = |name: &str, limit: usize| {
        std::env::var(name).is_ok_and(|value| {
            !value.is_empty()
                && value.len() <= limit
                && value.bytes().all(|byte| byte.is_ascii_graphic())
        })
    };
    let temporary = std::env::var("AWS_ACCESS_KEY_ID").is_ok_and(|id| {
        id.starts_with("ASIA")
            && id.len() == 20
            && id
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    });
    if !temporary
        || !printable("AWS_SECRET_ACCESS_KEY", 128)
        || !printable("AWS_SESSION_TOKEN", 8192)
        || std::env::var("AWS_EC2_METADATA_DISABLED").as_deref() != Ok("true")
        || std::env::var("AWS_CONFIG_FILE").as_deref() != Ok("/dev/null")
        || std::env::var("AWS_SHARED_CREDENTIALS_FILE").as_deref() != Ok("/dev/null")
    {
        return Err(Error::InvalidInput(
            "hosted fixture requires isolated temporary session credentials".into(),
        ));
    }
    let callback = action
        .strip_prefix("/api/v1/tasks/")
        .and_then(|rest| rest.split_once('/'))
        .is_some_and(|(task, operation)| {
            task.starts_with("ct1_")
                && task
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                && matches!(operation, "started" | "heartbeat" | "completed")
        });
    if !callback
        && !matches!(
            action,
            "validate-s3" | "accept" | "accept-rivers" | "inspect" | "/run" | "/api/v1/tasks/claim"
        )
    {
        return Err(Error::InvalidInput(
            "action is outside the hosted qualification".into(),
        ));
    }
    let prefix = std::env::var("ARCO_PULL_TEST_PREFIX")
        .map_err(|_| Error::InvalidInput("hosted fixture needs a physical prefix".into()))?;
    let valid = prefix
        .strip_prefix("conformance/s3/flow-worker-pull-")
        .and_then(|rest| rest.split_once('/'))
        .is_some_and(|(run, case)| {
            !run.is_empty() && matches!(case, "race" | "reply-loss" | "rivers-retry")
        });
    if !valid {
        return Err(Error::InvalidInput(
            "invalid hosted worker-pull physical prefix".into(),
        ));
    }
    if action == "accept-rivers" && !prefix.ends_with("/rivers-retry") {
        return Err(Error::InvalidInput(
            "action is outside the hosted qualification".into(),
        ));
    }
    fixture_root(&prefix)
}

fn fixture_root(raw: &str) -> Result<String> {
    let valid_part = |part: &str| {
        !part.is_empty()
            && part.len() <= 64
            && part
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    };
    let valid = raw
        .strip_prefix("conformance/s3/")
        .and_then(|rest| rest.split_once('/'))
        .is_some_and(|(run, case)| valid_part(run) && valid_part(case));
    if !valid {
        return Err(Error::InvalidInput(
            "fixture prefix must be conformance/s3/<run>/<case>".into(),
        ));
    }
    Ok(format!("{raw}/"))
}

impl PointerFault {
    fn record(
        &self,
        path: &str,
        pointer: &Value,
        expected: &str,
        stored: Option<&str>,
    ) -> Result<()> {
        let stage = if stored.is_some() { "after" } else { "before" };
        write_marker(
            &self.marker,
            &json!({
                "pid": std::process::id(),
                "stage": stage,
                "key": path,
                "pointer": pointer,
                "expectedVersion": expected,
                "storedVersion": stored,
            }),
        )
    }

    async fn wait_until_released(&self) {
        let release = self.marker.with_extension("resume");
        while !release.is_file() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn write_marker(path: &Path, receipt: &Value) -> Result<()> {
    let result = (|| -> std::io::Result<()> {
        let prepared = path.with_extension("preparing");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&prepared)?;
        serde_json::to_writer(&mut file, receipt)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::rename(prepared, path)
    })();
    result.map_err(|error| Error::storage(format!("write pointer fixture marker: {error}")))
}

impl FixtureBackend {
    pub const fn is_hosted(&self) -> bool {
        self.budget.is_some()
    }

    fn observe<T>(
        &self,
        operation: &str,
        path: &str,
        condition: &str,
        result: Result<T>,
    ) -> Result<T> {
        if let (true, Err(error)) = (self.is_hosted(), &result) {
            let logical = path.strip_prefix(&self.root).unwrap_or(path);
            let object = if logical == orchestration_compaction_lock_path() {
                "raw_lock"
            } else if logical.ends_with(orchestration_manifest_pointer_path()) {
                "selected_pointer"
            } else {
                "scoped_object"
            };
            super::hosted_diagnostics::storage_failure(operation, object, condition, error);
        }
        result
    }

    async fn provider_put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        let condition = match precondition {
            WritePrecondition::DoesNotExist => "does_not_exist",
            WritePrecondition::MatchesVersion(_) => "matches_version",
            WritePrecondition::None => "none",
        };
        let result = self.s3.put(path, data, precondition).await;
        self.observe("put", path, condition, result)
    }

    pub fn counts(&self) -> Option<Value> {
        self.budget.as_ref().map(|budget| {
            json!({
                "logicalReads": budget.reads.load(Ordering::Relaxed),
                "logicalWrites": budget.writes.load(Ordering::Relaxed),
            })
        })
    }

    fn charge(&self, write: bool) -> Result<()> {
        if let Some(budget) = self.budget.as_ref() {
            let (counter, limit) = if write {
                (&budget.writes, MAX_HOSTED_WRITES)
            } else {
                (&budget.reads, MAX_HOSTED_READS)
            };
            counter
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                    (count < limit).then_some(count + 1)
                })
                .map_err(|_| Error::storage("hosted fixture reached its scoped operation limit"))?;
        }
        Ok(())
    }

    fn local_only(&self) -> Result<()> {
        if self.budget.is_some() {
            return Err(Error::InvalidInput(
                "operation is outside the hosted qualification".into(),
            ));
        }
        Ok(())
    }

    fn physical(&self, path: &str) -> String {
        format!("{}{path}", self.root)
    }

    fn logical(&self, path: &str) -> Result<String> {
        path.strip_prefix(&self.root)
            .map(str::to_string)
            .ok_or_else(|| Error::storage("S3 fixture received an object outside its scope"))
    }

    fn logical_meta(&self, mut meta: ObjectMeta) -> Result<ObjectMeta> {
        meta.path = self.logical(&meta.path)?;
        Ok(meta)
    }
}

#[async_trait]
impl StorageBackend for FixtureBackend {
    async fn get(&self, path: &str) -> Result<Bytes> {
        self.charge(false)?;
        let result = self.s3.get(&self.physical(path)).await;
        self.observe("get", path, "none", result)
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.charge(false)?;
        let result = self.s3.get_range(&self.physical(path), range).await;
        self.observe("get_range", path, "none", result)
    }

    async fn get_range_with_ownership(
        &self,
        path: &str,
        range: Range<u64>,
    ) -> Result<ClassifiedBytes> {
        self.charge(false)?;
        let result = self
            .s3
            .get_range_with_ownership(&self.physical(path), range)
            .await;
        self.observe("get_range", path, "none", result)
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        self.charge(true)?;
        let physical = self.physical(path);
        let Some(fault) = self.fault.as_ref().filter(|fault| fault.scope == path) else {
            return self.provider_put(&physical, data, precondition).await;
        };
        let WritePrecondition::MatchesVersion(version) = &precondition else {
            return self.provider_put(&physical, data, precondition).await;
        };
        let pointer = serde_json::from_slice(&data)
            .map_err(|error| Error::storage(format!("decode fixture pointer: {error}")))?;
        let expected = version.clone();
        if matches!(fault.mode, FaultMode::StopBefore | FaultMode::HoldBefore) {
            fault.record(&physical, &pointer, &expected, None)?;
            if matches!(fault.mode, FaultMode::StopBefore) {
                return std::future::pending().await;
            }
            fault.wait_until_released().await;
            let result = self.provider_put(&physical, data, precondition).await?;
            let outcome = match &result {
                WriteResult::Success { version } => json!({"storedVersion": version}),
                WriteResult::PreconditionFailed { current_version } => {
                    json!({"currentVersion": current_version})
                }
            };
            write_marker(&fault.marker.with_extension("result.json"), &outcome)?;
            return Ok(result);
        }

        let result = self.provider_put(&physical, data, precondition).await?;
        if let WriteResult::Success { version } = &result {
            fault.record(&physical, &pointer, &expected, Some(version))?;
            if matches!(fault.mode, FaultMode::LoseAfter) {
                return Err(Error::storage(
                    "fixture lost the successful pointer response",
                ));
            }
            return std::future::pending().await;
        }
        Ok(result)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.local_only()?;
        self.s3.delete(&self.physical(path)).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.local_only()?;
        self.s3
            .list(&self.physical(prefix))
            .await?
            .into_iter()
            .map(|meta| self.logical_meta(meta))
            .collect()
    }

    async fn list_page(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<ListPage> {
        self.local_only()?;
        let after = after.map(|path| self.physical(path));
        let page = self
            .s3
            .list_page(&self.physical(prefix), after.as_deref(), limit)
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
                .map(|path| self.logical(path))
                .transpose()?,
        })
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        self.charge(false)?;
        let result = self.s3.head(&self.physical(path)).await;
        self.observe("head", path, "none", result)?
            .map(|meta| self.logical_meta(meta))
            .transpose()
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        self.local_only()?;
        self.s3.signed_url(&self.physical(path), expiry).await
    }
}

pub fn scoped_pointer(tenant: &str, workspace: &str) -> Result<String> {
    let scope = arco_core::AuthorityScope::workspace(tenant, workspace)?;
    Ok(format!(
        "{}/{}",
        scope.prefix(),
        orchestration_manifest_pointer_path()
    ))
}
