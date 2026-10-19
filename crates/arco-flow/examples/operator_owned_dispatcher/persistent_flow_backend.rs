//! Test-only single-owner disk backend for a fresh Flow-state process view.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{Error, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Serialize, Deserialize)]
struct DiskState {
    version: u64,
    objects: BTreeMap<String, DiskObject>,
}

#[derive(Clone, Serialize, Deserialize)]
struct DiskObject {
    bytes: Vec<u8>,
    version: String,
    modified: DateTime<Utc>,
}

pub struct PersistentFlowBackend {
    file: PathBuf,
    state: Mutex<DiskState>,
}

impl PersistentFlowBackend {
    pub fn open(file: &Path) -> Result<Self> {
        let state = match fs::read(file) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|error| Error::storage(format!("decode persisted Flow state: {error}")))?,
            Err(error) if error.kind() == ErrorKind::NotFound => DiskState::default(),
            Err(error) => {
                return Err(Error::storage(format!(
                    "read persisted Flow state: {error}"
                )));
            }
        };
        Ok(Self {
            file: file.to_path_buf(),
            state: Mutex::new(state),
        })
    }

    fn persist(&self, state: &DiskState) -> Result<()> {
        let bytes = serde_json::to_vec(state)
            .map_err(|error| Error::storage(format!("encode persisted Flow state: {error}")))?;
        let temp = self
            .file
            .with_extension(format!("{}.tmp", ulid::Ulid::new()));
        let mut file = File::create(&temp)
            .map_err(|error| Error::storage(format!("create Flow state temp: {error}")))?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| Error::storage(format!("sync Flow state temp: {error}")))?;
        fs::rename(&temp, &self.file)
            .map_err(|error| Error::storage(format!("publish Flow state: {error}")))?;
        File::open(self.file.parent().expect("state file parent"))
            .and_then(|dir| dir.sync_all())
            .map_err(|error| Error::storage(format!("sync Flow state directory: {error}")))
    }

    fn meta(path: &str, object: &DiskObject) -> ObjectMeta {
        ObjectMeta {
            path: path.to_string(),
            size: object.bytes.len() as u64,
            version: object.version.clone(),
            last_modified: Some(object.modified),
            etag: None,
        }
    }
}

#[async_trait::async_trait]
impl StorageBackend for PersistentFlowBackend {
    async fn get(&self, path: &str) -> Result<Bytes> {
        self.state
            .lock()
            .expect("Flow state lock")
            .objects
            .get(path)
            .map(|object| Bytes::copy_from_slice(&object.bytes))
            .ok_or_else(|| Error::NotFound(path.to_string()))
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        let bytes = self.get(path).await?;
        let start = usize::try_from(range.start).unwrap_or(usize::MAX);
        let end = usize::try_from(range.end)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        if start > bytes.len() || end < start {
            return Err(Error::InvalidInput("invalid Flow state range".to_string()));
        }
        Ok(bytes.slice(start..end))
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        let mut guard = self.state.lock().expect("Flow state lock");
        let current = guard.objects.get(path);
        let current_version = current.map_or_else(String::new, |object| object.version.clone());
        let allowed = match precondition {
            WritePrecondition::None => true,
            WritePrecondition::DoesNotExist => current.is_none(),
            WritePrecondition::MatchesVersion(expected) => {
                current_version == expected && current.is_some()
            }
        };
        if !allowed {
            return Ok(WriteResult::PreconditionFailed { current_version });
        }
        let mut next = guard.clone();
        next.version += 1;
        let version = next.version.to_string();
        next.objects.insert(
            path.to_string(),
            DiskObject {
                bytes: data.to_vec(),
                version: version.clone(),
                modified: Utc::now(),
            },
        );
        self.persist(&next)?;
        *guard = next;
        Ok(WriteResult::Success { version })
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let mut guard = self.state.lock().expect("Flow state lock");
        if guard.objects.contains_key(path) {
            let mut next = guard.clone();
            next.objects.remove(path);
            self.persist(&next)?;
            *guard = next;
        }
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        Ok(self
            .state
            .lock()
            .expect("Flow state lock")
            .objects
            .iter()
            .filter(|(path, _)| path.starts_with(prefix))
            .map(|(path, object)| Self::meta(path, object))
            .collect())
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        Ok(self
            .state
            .lock()
            .expect("Flow state lock")
            .objects
            .get(path)
            .map(|object| Self::meta(path, object)))
    }

    async fn signed_url(&self, _path: &str, _expiry: Duration) -> Result<String> {
        Err(Error::storage("test backend has no signed URLs"))
    }
}
