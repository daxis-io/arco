//! A single-process, single-host durable queue for the operator example.
//! ponytail: delivery is serial and done receipts are unbounded; use an
//! operator queue with bounded retention when throughput or scale requires it.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use arco_flow::dispatch::{EnqueueOptions, EnqueueResult, HttpTaskEnqueuer};
use arco_flow::error::{Error, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize)]
struct TaskRecord {
    task_id: String,
    target_url: String,
    body: Vec<u8>,
    audience: Option<String>,
    headers: HashMap<String, String>,
    not_before_ms: i64,
}

pub struct FileQueue {
    root: PathBuf,
}

impl FileQueue {
    pub fn open(root: &Path) -> Result<Self> {
        for path in [root.to_path_buf(), root.join("pending"), root.join("done")] {
            private_dir(&path)?;
        }
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    fn path(&self, status: &str, task_id: &str) -> PathBuf {
        let hash = hex::encode(Sha256::digest(task_id.as_bytes()));
        self.root.join(status).join(format!("{hash}.json"))
    }

    fn duplicate(&self, record: &TaskRecord) -> Result<Option<EnqueueResult>> {
        for status in ["done", "pending"] {
            let path = self.path(status, &record.task_id);
            match fs::read(&path) {
                Ok(bytes) => {
                    let existing: TaskRecord = serde_json::from_slice(&bytes)
                        .map_err(|e| Error::dispatch(format!("corrupt queue record: {e}")))?;
                    if !same_task(&existing, record) {
                        return Err(Error::dispatch("queue task ID conflicts with another task"));
                    }
                    sync_dir(&self.root.join(status))?;
                    return Ok(Some(EnqueueResult::Deduplicated {
                        existing_message_id: record.task_id.clone(),
                    }));
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(io_error("read queue record", error)),
            }
        }
        Ok(None)
    }

    pub async fn drain_once(&self, client: &reqwest::Client) -> Result<usize> {
        let mut paths = fs::read_dir(self.root.join("pending"))
            .map_err(|e| io_error("list pending tasks", e))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|e| io_error("list pending tasks", e))?;
        paths.sort();
        let mut delivered = 0;
        let mut delivery_error = None;
        for path in paths {
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let record: TaskRecord = serde_json::from_slice(
                &fs::read(&path).map_err(|e| io_error("read pending task", e))?,
            )
            .map_err(|e| Error::dispatch(format!("corrupt pending task: {e}")))?;
            let done = self.path("done", &record.task_id);
            if done.exists() {
                let completed: TaskRecord = serde_json::from_slice(
                    &fs::read(&done).map_err(|e| io_error("read completed task", e))?,
                )
                .map_err(|e| Error::dispatch(format!("corrupt completed task: {e}")))?;
                if !same_task(&completed, &record) {
                    return Err(Error::dispatch(
                        "completed task conflicts with pending task",
                    ));
                }
                fs::remove_file(&path).map_err(|e| io_error("remove completed pending task", e))?;
                sync_dir(&self.root.join("pending"))?;
                continue;
            }
            if record.not_before_ms > Utc::now().timestamp_millis() {
                continue;
            }
            let mut request = client
                .post(&record.target_url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(record.body);
            for (name, value) in record.headers {
                request = request.header(&name, &value);
            }
            match request.send().await {
                Ok(response) if response.status().is_success() => {}
                Ok(response) => {
                    delivery_error.get_or_insert_with(|| {
                        Error::dispatch(format!("worker delivery returned {}", response.status()))
                    });
                    continue;
                }
                Err(error) => {
                    delivery_error.get_or_insert_with(|| {
                        Error::dispatch(format!("worker delivery failed: {error}"))
                    });
                    continue;
                }
            }
            fs::hard_link(&path, &done).map_err(|e| io_error("finish delivered task", e))?;
            sync_dir(&self.root.join("done"))?;
            fs::remove_file(&path).map_err(|e| io_error("remove delivered pending task", e))?;
            sync_dir(&self.root.join("pending"))?;
            delivered += 1;
        }
        delivery_error.map_or(Ok(delivered), Err)
    }
}

#[async_trait::async_trait]
impl HttpTaskEnqueuer for FileQueue {
    async fn enqueue_http(
        &self,
        task_id: &str,
        target_url: &str,
        body: &[u8],
        options: EnqueueOptions,
        audience: Option<&str>,
        extra_headers: Option<HashMap<String, String>>,
    ) -> Result<EnqueueResult> {
        if task_id.is_empty() || options.routing_key.is_some() || options.priority.is_some() {
            return Err(Error::configuration(
                "filesystem queue requires a task ID and supports only the default route",
            ));
        }
        validate_target(target_url)?;
        let delay_ms = options.delay.map_or(0, |delay| {
            i64::try_from(delay.as_millis()).unwrap_or(i64::MAX)
        });
        let record = TaskRecord {
            task_id: task_id.to_string(),
            target_url: target_url.to_string(),
            body: body.to_vec(),
            audience: audience.map(str::to_string),
            headers: extra_headers.unwrap_or_default(),
            not_before_ms: Utc::now().timestamp_millis().saturating_add(delay_ms),
        };
        if let Some(result) = self.duplicate(&record)? {
            return Ok(result);
        }
        let pending_dir = self.root.join("pending");
        let temp = pending_dir.join(format!(".{}.tmp", ulid::Ulid::new()));
        let final_path = self.path("pending", task_id);
        let bytes = serde_json::to_vec(&record)
            .map_err(|e| Error::dispatch(format!("encode queue task: {e}")))?;
        let mut file = private_file(&temp)?;
        file.write_all(&bytes)
            .map_err(|e| io_error("write queue task", e))?;
        file.sync_all()
            .map_err(|e| io_error("sync queue task", e))?;
        fs::hard_link(&temp, &final_path).map_err(|e| io_error("publish queue task", e))?;
        sync_dir(&pending_dir)?;
        fs::remove_file(&temp).map_err(|e| io_error("remove queue temp", e))?;
        Ok(EnqueueResult::Enqueued {
            message_id: task_id.to_string(),
        })
    }
}

fn same_task(a: &TaskRecord, b: &TaskRecord) -> bool {
    a.task_id == b.task_id
        && a.target_url == b.target_url
        && a.audience == b.audience
        && a.headers == b.headers
        && same_body_except_token(&a.body, &b.body)
}

fn same_body_except_token(a: &[u8], b: &[u8]) -> bool {
    let (Ok(mut a), Ok(mut b)) = (
        serde_json::from_slice::<serde_json::Value>(a),
        serde_json::from_slice::<serde_json::Value>(b),
    ) else {
        return a == b;
    };
    for value in [&mut a, &mut b] {
        if let Some(object) = value.as_object_mut() {
            object.remove("taskToken");
            object.remove("tokenExpiresAt");
        }
    }
    a == b
}

fn validate_target(target: &str) -> Result<()> {
    let url = reqwest::Url::parse(target)
        .map_err(|e| Error::configuration(format!("invalid worker URL: {e}")))?;
    let local_http = url.scheme() == "http"
        && matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"));
    if url.scheme() != "https" && !local_http {
        return Err(Error::configuration(
            "worker URL must use HTTPS or loopback HTTP",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::configuration(
            "worker URL must not contain credentials",
        ));
    }
    Ok(())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "keeps map_err call sites small"
)]
fn io_error(action: &str, error: std::io::Error) -> Error {
    Error::dispatch(format!("{action}: {error}"))
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|e| io_error("sync queue directory", e))
}

#[cfg(unix)]
fn private_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(0o700);
    builder
        .create(path)
        .map_err(|e| io_error("create queue directory", e))?;
    let meta = fs::symlink_metadata(path).map_err(|e| io_error("inspect queue directory", e))?;
    if !meta.is_dir() || meta.permissions().mode() & 0o077 != 0 {
        return Err(Error::configuration(
            "queue directory must be private (0700)",
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn private_dir(_path: &Path) -> Result<()> {
    Err(Error::configuration(
        "filesystem queue example requires Unix",
    ))
}

#[cfg(unix)]
fn private_file(path: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| io_error("create queue temp", e))
}

#[cfg(not(unix))]
fn private_file(_path: &Path) -> Result<File> {
    Err(Error::configuration(
        "filesystem queue example requires Unix",
    ))
}
