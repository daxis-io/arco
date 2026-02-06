//! Delta commit coordination (Mode B / Arco-coordinated).
//!
//! The coordinator stores per-table state in object storage and uses CAS writes
//! to safely reserve and commit Delta log versions without a stateful server.

use std::time::Duration;

use arco_core::ScopedStorage;
use arco_core::storage::{WritePrecondition, WriteResult};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use ulid::Ulid;
use uuid::Uuid;

use crate::error::{DeltaError, Result};
use crate::types::{
    CommitDeltaRequest, CommitDeltaResponse, DeltaCoordinatorState, InflightCommit, StagedCommit,
};

/// Coordinator configuration.
#[derive(Debug, Clone)]
pub struct DeltaCommitCoordinatorConfig {
    /// Maximum time an inflight reservation is considered valid.
    pub inflight_ttl: Duration,
    /// Maximum number of CAS retries for coordinator state updates.
    pub max_cas_retries: usize,
}

impl Default for DeltaCommitCoordinatorConfig {
    fn default() -> Self {
        Self {
            inflight_ttl: Duration::from_secs(300),
            max_cas_retries: 16,
        }
    }
}

/// Delta commit coordinator for a single managed table.
#[derive(Clone)]
pub struct DeltaCommitCoordinator {
    storage: ScopedStorage,
    table_id: Uuid,
    config: DeltaCommitCoordinatorConfig,
}

impl std::fmt::Debug for DeltaCommitCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaCommitCoordinator")
            .field("table_id", &self.table_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl DeltaCommitCoordinator {
    /// Creates a coordinator for `table_id`.
    #[must_use]
    pub fn new(storage: ScopedStorage, table_id: Uuid) -> Self {
        Self {
            storage,
            table_id,
            config: DeltaCommitCoordinatorConfig::default(),
        }
    }

    /// Sets coordinator configuration.
    #[must_use]
    pub fn with_config(mut self, config: DeltaCommitCoordinatorConfig) -> Self {
        self.config = config;
        self
    }

    /// Stages a Delta commit payload (server-side upload).
    ///
    /// The returned `(path, version)` must be provided to [`Self::commit`].
    ///
    /// # Errors
    ///
    /// Returns an error if the staged payload cannot be written due to a storage
    /// failure or a precondition conflict.
    pub async fn stage_commit_payload(&self, payload: Bytes) -> Result<StagedCommit> {
        let ulid = Ulid::new().to_string();
        let staged_path = format!("delta/staging/{}/{}.json", self.table_id, ulid);

        let result = self
            .storage
            .put_raw(&staged_path, payload, WritePrecondition::DoesNotExist)
            .await?;

        let WriteResult::Success { version } = result else {
            return Err(DeltaError::conflict(
                "staging path already exists; retry".to_string(),
            ));
        };

        Ok(StagedCommit {
            staged_path,
            staged_version: version,
        })
    }

    /// Commits a staged Delta payload (Mode B).
    ///
    /// This function:
    /// 1) Applies idempotency replay if a committed marker exists.
    /// 2) Recovers and finalizes any expired inflight commit.
    /// 3) Reserves the next version via CAS.
    /// 4) Writes `_delta_log/{version}.json` with `DoesNotExist` precondition.
    /// 5) Finalizes coordinator state (`latest_version` + clear inflight) via CAS.
    /// 6) Writes an idempotency marker for safe retries.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request is invalid (bad idempotency key, staged path mismatch)
    /// - The commit conflicts with concurrent writers (stale read version, CAS races)
    /// - Storage operations fail while staging, reserving, or writing delta logs
    pub async fn commit(
        &self,
        req: CommitDeltaRequest,
        now: DateTime<Utc>,
    ) -> Result<CommitDeltaResponse> {
        validate_uuidv7(&req.idempotency_key)?;
        self.validate_staged_path(&req.staged_path)?;

        let request_hash = request_hash(&req)?;

        if let Some(record) = self.read_idempotency_record(&req.idempotency_key).await? {
            if record.request_hash != request_hash {
                return Err(DeltaError::conflict(
                    "Idempotency-Key already used with different request body".to_string(),
                ));
            }
            return Ok(CommitDeltaResponse {
                version: record.version,
                delta_log_path: record.delta_log_path,
            });
        }

        if let Some(recovered) = self.recover_inflight(now).await? {
            if recovered.commit_id == req.idempotency_key {
                if recovered.request_hash != request_hash {
                    return Err(DeltaError::conflict(
                        "Idempotency-Key already used with different request body".to_string(),
                    ));
                }
                return Ok(recovered.response);
            }
        }

        let reserved = self.reserve_or_resume_inflight(&req, now).await?;

        let payload = self
            .read_staged_payload(&req.staged_path, &req.staged_version)
            .await?;

        let delta_log_path = delta_log_path(self.table_id, reserved.version)?;

        let write = self
            .storage
            .put_raw(
                &delta_log_path,
                payload.clone(),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        match write {
            WriteResult::Success { .. } => {}
            WriteResult::PreconditionFailed { .. } => {
                // Another actor may have completed the same commit; verify payload matches.
                let existing = self.storage.get_raw(&delta_log_path).await?;
                if existing != payload {
                    return Err(DeltaError::conflict(
                        "delta log version already exists with different contents".to_string(),
                    ));
                }
            }
        }

        let response = CommitDeltaResponse {
            version: reserved.version,
            delta_log_path: delta_log_path.clone(),
        };

        self.finalize_inflight(&req.idempotency_key, reserved.version)
            .await?;

        self.write_idempotency_record(&req.idempotency_key, &request_hash, &response, now)
            .await?;

        Ok(response)
    }

    async fn read_staged_payload(&self, staged_path: &str, staged_version: &str) -> Result<Bytes> {
        let meta = self.storage.head_raw(staged_path).await?;
        let Some(meta) = meta else {
            return Err(DeltaError::not_found(format!(
                "staged payload not found: {staged_path}"
            )));
        };

        if meta.version != staged_version {
            return Err(DeltaError::conflict(format!(
                "staged payload version mismatch for {staged_path}"
            )));
        }

        Ok(self.storage.get_raw(staged_path).await?)
    }

    async fn load_state(&self) -> Result<(DeltaCoordinatorState, Option<String>)> {
        let path = coordinator_path(self.table_id);
        let meta = self.storage.head_raw(&path).await?;
        let Some(meta) = meta else {
            return Ok((DeltaCoordinatorState::default(), None));
        };

        let bytes = self.storage.get_raw(&path).await?;
        let state: DeltaCoordinatorState = serde_json::from_slice(&bytes).map_err(|e| {
            DeltaError::serialization(format!("failed to deserialize coordinator state: {e}"))
        })?;
        Ok((state, Some(meta.version)))
    }

    async fn store_state(
        &self,
        state: &DeltaCoordinatorState,
        expected_version: Option<&str>,
    ) -> Result<WriteResult> {
        let path = coordinator_path(self.table_id);
        let json = serde_json::to_vec(state).map_err(|e| {
            DeltaError::serialization(format!("failed to serialize coordinator state: {e}"))
        })?;
        let precondition = expected_version.map_or(WritePrecondition::DoesNotExist, |v| {
            WritePrecondition::MatchesVersion(v.to_string())
        });
        Ok(self
            .storage
            .put_raw(&path, Bytes::from(json), precondition)
            .await?)
    }

    async fn recover_inflight(&self, now: DateTime<Utc>) -> Result<Option<RecoveredInflight>> {
        let now_ms = now.timestamp_millis();

        for _ in 0..self.config.max_cas_retries {
            let (state, version) = self.load_state().await?;
            let Some(current_version) = version.as_deref() else {
                return Ok(None);
            };

            let Some(inflight) = state.inflight.clone() else {
                return Ok(None);
            };

            self.validate_staged_path(&inflight.staged_path)?;

            let delta_log_path = delta_log_path(self.table_id, inflight.version)?;

            let commit_exists = self.storage.head_raw(&delta_log_path).await?.is_some();

            if commit_exists || now_ms >= inflight.expires_at_ms {
                if !commit_exists {
                    // Attempt to materialize the inflight commit from its staged payload.
                    let payload = self
                        .read_staged_payload(&inflight.staged_path, &inflight.staged_version)
                        .await?;
                    let write = self
                        .storage
                        .put_raw(&delta_log_path, payload, WritePrecondition::DoesNotExist)
                        .await?;
                    match write {
                        WriteResult::PreconditionFailed { .. } | WriteResult::Success { .. } => {}
                    };
                }

                let mut updated = state.clone();
                updated.latest_version = inflight.version.max(state.latest_version);
                updated.inflight = None;

                let put = self.store_state(&updated, Some(current_version)).await?;
                match put {
                    WriteResult::Success { .. } => {
                        let read_version = inflight
                            .read_version
                            .unwrap_or_else(|| inflight.version.saturating_sub(1));
                        let recovered_request_hash = request_hash(&CommitDeltaRequest {
                            read_version,
                            staged_path: inflight.staged_path.clone(),
                            staged_version: inflight.staged_version.clone(),
                            idempotency_key: inflight.commit_id.clone(),
                        })?;

                        let response = CommitDeltaResponse {
                            version: inflight.version,
                            delta_log_path: delta_log_path.clone(),
                        };

                        self.write_idempotency_record(
                            &inflight.commit_id,
                            &recovered_request_hash,
                            &response,
                            now,
                        )
                        .await?;

                        return Ok(Some(RecoveredInflight {
                            commit_id: inflight.commit_id,
                            request_hash: recovered_request_hash,
                            response,
                        }));
                    }
                    WriteResult::PreconditionFailed { .. } => continue,
                }
            }

            return Ok(None);
        }

        Err(DeltaError::conflict(
            "failed to recover inflight delta commit after retries".to_string(),
        ))
    }

    async fn reserve_or_resume_inflight(
        &self,
        req: &CommitDeltaRequest,
        now: DateTime<Utc>,
    ) -> Result<ReservedCommit> {
        let now_ms = now.timestamp_millis();
        let expires_at_ms = now_ms.saturating_add(
            i64::try_from(self.config.inflight_ttl.as_millis()).unwrap_or(i64::MAX),
        );

        for _ in 0..self.config.max_cas_retries {
            let (state, version) = self.load_state().await?;

            if let Some(inflight) = &state.inflight {
                if now_ms < inflight.expires_at_ms {
                    if inflight.commit_id == req.idempotency_key {
                        if inflight.staged_path != req.staged_path
                            || inflight.staged_version != req.staged_version
                        {
                            return Err(DeltaError::conflict(
                                "Idempotency-Key already used with different request body"
                                    .to_string(),
                            ));
                        }

                        if let Some(inflight_read_version) = inflight.read_version {
                            if inflight_read_version != req.read_version {
                                return Err(DeltaError::conflict(
                                    "Idempotency-Key already used with different request body"
                                        .to_string(),
                                ));
                            }
                        }

                        if state.latest_version != req.read_version {
                            return Err(DeltaError::conflict(format!(
                                "stale read_version: expected {}, got {}",
                                state.latest_version, req.read_version
                            )));
                        }

                        return Ok(ReservedCommit {
                            version: inflight.version,
                        });
                    }

                    return Err(DeltaError::conflict(
                        "delta commit already reserved; retry later".to_string(),
                    ));
                }

                // Expired inflight: recover first, then retry reservation from latest state.
                let _ = self.recover_inflight(now).await?;
                continue;
            }

            if state.latest_version != req.read_version {
                return Err(DeltaError::conflict(format!(
                    "stale read_version: expected {}, got {}",
                    state.latest_version, req.read_version
                )));
            }

            let new_version = state
                .latest_version
                .checked_add(1)
                .ok_or_else(|| DeltaError::conflict("delta version overflow".to_string()))?;

            let inflight = InflightCommit {
                commit_id: req.idempotency_key.clone(),
                read_version: Some(req.read_version),
                version: new_version,
                staged_path: req.staged_path.clone(),
                staged_version: req.staged_version.clone(),
                started_at_ms: now_ms,
                expires_at_ms,
            };

            let mut updated = state.clone();
            updated.inflight = Some(inflight);

            let put = self.store_state(&updated, version.as_deref()).await?;
            match put {
                WriteResult::Success { .. } => {
                    return Ok(ReservedCommit {
                        version: new_version,
                    });
                }
                WriteResult::PreconditionFailed { .. } => continue,
            }
        }

        Err(DeltaError::conflict(
            "failed to reserve delta commit after retries".to_string(),
        ))
    }

    async fn finalize_inflight(&self, commit_id: &str, committed_version: i64) -> Result<()> {
        for _ in 0..self.config.max_cas_retries {
            let (state, version) = self.load_state().await?;
            let Some(version) = version else {
                return Err(DeltaError::conflict(
                    "coordinator state missing during finalize".to_string(),
                ));
            };

            if state.inflight.is_none() && state.latest_version >= committed_version {
                return Ok(());
            }

            if let Some(inflight) = &state.inflight {
                if inflight.commit_id != commit_id {
                    return Err(DeltaError::conflict(
                        "coordinator inflight changed during finalize; retry".to_string(),
                    ));
                }
            }

            let mut updated = state.clone();
            updated.latest_version = committed_version.max(state.latest_version);
            updated.inflight = None;

            let put = self.store_state(&updated, Some(&version)).await?;
            match put {
                WriteResult::Success { .. } => return Ok(()),
                WriteResult::PreconditionFailed { .. } => continue,
            }
        }

        Err(DeltaError::conflict(
            "failed to finalize delta commit after retries".to_string(),
        ))
    }

    async fn read_idempotency_record(
        &self,
        key: &str,
    ) -> Result<Option<DeltaCommitIdempotencyRecord>> {
        let path = idempotency_path(self.table_id, key);
        let meta = self.storage.head_raw(&path).await?;
        let Some(_) = meta else {
            return Ok(None);
        };

        let bytes = self.storage.get_raw(&path).await?;
        let record: DeltaCommitIdempotencyRecord = serde_json::from_slice(&bytes).map_err(|e| {
            DeltaError::serialization(format!("failed to deserialize idempotency record: {e}"))
        })?;
        Ok(Some(record))
    }

    async fn write_idempotency_record(
        &self,
        key: &str,
        request_hash: &str,
        response: &CommitDeltaResponse,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let record = DeltaCommitIdempotencyRecord {
            idempotency_key: key.to_string(),
            idempotency_key_hash: sha256_hex(key.as_bytes()),
            request_hash: request_hash.to_string(),
            version: response.version,
            delta_log_path: response.delta_log_path.clone(),
            committed_at_ms: now.timestamp_millis(),
        };

        let path = idempotency_path(self.table_id, key);
        let json = serde_json::to_vec(&record).map_err(|e| {
            DeltaError::serialization(format!("failed to serialize idempotency record: {e}"))
        })?;
        let put = self
            .storage
            .put_raw(&path, Bytes::from(json), WritePrecondition::DoesNotExist)
            .await?;
        match put {
            WriteResult::PreconditionFailed { .. } | WriteResult::Success { .. } => Ok(()),
        }
    }

    fn validate_staged_path(&self, staged_path: &str) -> Result<()> {
        let expected_prefix = format!("delta/staging/{}/", self.table_id);
        if !staged_path.starts_with(&expected_prefix) {
            return Err(DeltaError::bad_request(format!(
                "staged_path must start with {expected_prefix}",
            )));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ReservedCommit {
    version: i64,
}

#[derive(Debug)]
struct RecoveredInflight {
    commit_id: String,
    request_hash: String,
    response: CommitDeltaResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaCommitIdempotencyRecord {
    idempotency_key: String,
    idempotency_key_hash: String,
    request_hash: String,
    version: i64,
    delta_log_path: String,
    committed_at_ms: i64,
}

fn coordinator_path(table_id: Uuid) -> String {
    format!("delta/coordinator/{table_id}.json")
}

fn delta_log_path(table_id: Uuid, version: i64) -> Result<String> {
    if version < 0 {
        return Err(DeltaError::bad_request(
            "delta log version must be non-negative",
        ));
    }
    Ok(format!("tables/{table_id}/_delta_log/{version:020}.json"))
}

fn idempotency_path(table_id: Uuid, idempotency_key: &str) -> String {
    let hash = sha256_hex(idempotency_key.as_bytes());
    let prefix = hash.get(0..2).unwrap_or("00");
    format!("delta/idempotency/{table_id}/{prefix}/{hash}.json")
}

fn request_hash(req: &CommitDeltaRequest) -> Result<String> {
    let value = serde_json::json!({
        "read_version": req.read_version,
        "staged_path": req.staged_path,
        "staged_version": req.staged_version,
    });

    let canonical = serde_jcs::to_string(&value)
        .map_err(|e| DeltaError::serialization(format!("failed to canonicalize request: {e}")))?;
    Ok(sha256_hex(canonical.as_bytes()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn validate_uuidv7(key: &str) -> Result<()> {
    let uuid = Uuid::parse_str(key)
        .map_err(|_| DeltaError::bad_request("Idempotency-Key must be a valid UUIDv7"))?;

    if uuid.get_variant() != uuid::Variant::RFC4122 {
        return Err(DeltaError::bad_request(
            "Idempotency-Key must use RFC4122 variant",
        ));
    }
    if uuid.get_version_num() != 7 {
        return Err(DeltaError::bad_request(
            "Idempotency-Key must be UUIDv7 (RFC 9562)",
        ));
    }
    if uuid.to_string() != key {
        return Err(DeltaError::bad_request(
            "Idempotency-Key must be a canonical lowercase UUID string",
        ));
    }
    Ok(())
}
