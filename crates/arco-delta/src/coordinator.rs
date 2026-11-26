//! Delta commit coordination (Mode B / Arco-coordinated).
//!
//! The coordinator stores per-table state in object storage and uses CAS writes
//! to safely reserve and commit Delta log versions without a stateful server.

use std::time::Duration;

use arco_core::storage::{StorageBackend as _, WritePrecondition, WriteResult};
use arco_core::{DeltaPaths, ScopedStorage};
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

/// Last version supported by bounded reads that replay from version zero.
pub const MAX_PINNED_DELTA_VERSION: i64 = 15;
const MAX_PINNED_COORDINATOR_BYTES: u64 = 16 * 1024;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct StoredCoordinatorState {
    #[serde(flatten)]
    core: DeltaCoordinatorState,
    // The reservation CAS binds the staged bytes to the incarnation's canonical
    // root. Only entries covered by latest_version have finalized.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    canonical_log_sha256: Vec<String>,
}

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
    paths: DeltaPaths,
    config: DeltaCommitCoordinatorConfig,
}

impl std::fmt::Debug for DeltaCommitCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaCommitCoordinator")
            .field("table_id", &self.paths.table_id())
            .field("table_root", &self.paths.table_root())
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl DeltaCommitCoordinator {
    /// Creates a coordinator for `table_id`.
    #[must_use]
    pub fn new(storage: ScopedStorage, table_id: Uuid) -> Self {
        let paths = DeltaPaths::legacy(table_id);
        Self::with_paths(storage, paths)
    }

    /// Creates a coordinator from pre-resolved typed Delta paths.
    #[must_use]
    pub fn with_paths(storage: ScopedStorage, paths: DeltaPaths) -> Self {
        Self {
            storage,
            paths,
            config: DeltaCommitCoordinatorConfig::default(),
        }
    }

    /// Sets coordinator configuration.
    #[must_use]
    pub fn with_config(mut self, config: DeltaCommitCoordinatorConfig) -> Self {
        self.config = config;
        self
    }

    /// Returns the committed log digests from zero through exactly `version`.
    ///
    /// # Errors
    ///
    /// Fails for noncanonical roots, older state without digests, versions
    /// outside the bounded profile, and versions the coordinator has yet to finalize.
    pub async fn pinned_log_digests(&self, version: i64) -> Result<Vec<String>> {
        if !(0..=MAX_PINNED_DELTA_VERSION).contains(&version) || !self.is_canonical_root() {
            return Err(DeltaError::bad_request(
                "pinned_delta_version_or_root_unsupported",
            ));
        }
        let path = self.paths.coordinator_state();
        let Some(meta) = self.storage.head_raw(&path).await? else {
            return Err(DeltaError::not_found("delta_read_version_not_committed"));
        };
        if meta.size == 0 || meta.size > MAX_PINNED_COORDINATOR_BYTES {
            return Err(DeltaError::serialization(
                "pinned Delta coordinator state size invalid",
            ));
        }
        let bytes = self.storage.get_range(&path, 0..meta.size).await?;
        let state = decode_state(&bytes)?;
        if version > state.core.latest_version {
            return Err(DeltaError::not_found("delta_read_version_not_committed"));
        }
        let count = usize::try_from(version + 1)
            .map_err(|_| DeltaError::bad_request("pinned_delta_version_or_root_unsupported"))?;
        state
            .canonical_log_sha256
            .get(..count)
            .map(<[String]>::to_vec)
            .ok_or_else(|| DeltaError::conflict("delta_read_commit_identity_unavailable"))
    }

    fn is_canonical_root(&self) -> bool {
        self.paths == DeltaPaths::legacy(self.paths.table_id())
    }

    /// Returns the last finalized version, or `-1` before the first commit.
    ///
    /// # Errors
    /// Returns an error when coordinator state cannot be loaded.
    pub async fn finalized_version(&self) -> Result<i64> {
        Ok(self.load_state().await?.0.core.latest_version)
    }

    /// Appends an owner-prepared Delta payload whose identity is stable across retries.
    /// The caller must durably prepare any data files referenced by the payload first.
    ///
    /// # Errors
    /// Returns an error on changed payload, storage failure, or a concurrent commit.
    pub async fn commit_owner_append(
        &self,
        key: &str,
        payload: Bytes,
        now: DateTime<Utc>,
    ) -> Result<CommitDeltaResponse> {
        let staged = self.stage_idempotent_payload(key, payload.clone()).await?;
        let latest = self.finalized_version().await?;
        let read_version = if let Some(record) = self.read_idempotency_record(key).await? {
            let path = self.paths.delta_log_json(record.version)?;
            self.require_materialized_log(&path, &payload).await?;
            record.version.saturating_sub(1)
        } else {
            latest
        };
        self.commit(
            CommitDeltaRequest {
                read_version,
                staged_path: staged.staged_path,
                staged_version: staged.staged_version,
                idempotency_key: key.to_string(),
            },
            now,
        )
        .await
    }

    /// Verifies an owner append's log against its stored receipt for the exact version.
    ///
    /// # Errors
    /// Returns an error if the receipt is absent, has a different identity, or cannot be read.
    pub async fn verify_owner_commit(&self, key: &str, version: i64, payload: &[u8]) -> Result<()> {
        let record = self
            .read_idempotency_record(key)
            .await?
            .ok_or_else(|| DeltaError::conflict("owner append receipt is absent"))?;
        let expected = request_hash(
            key,
            version.saturating_sub(1),
            &self.paths.staging_payload(key),
            payload,
        )?;
        if record.version != version
            || record.delta_log_path != self.paths.delta_log_json(version)?
            || record.request_hash != expected
        {
            return Err(DeltaError::conflict("owner append log identity changed"));
        }
        Ok(())
    }

    async fn stage_idempotent_payload(&self, key: &str, payload: Bytes) -> Result<StagedCommit> {
        validate_uuidv7(key)?;
        let staged_path = self.paths.staging_payload(key);
        let staged_version = match self
            .storage
            .put_raw(
                &staged_path,
                payload.clone(),
                WritePrecondition::DoesNotExist,
            )
            .await?
        {
            WriteResult::Success { version } => version,
            WriteResult::PreconditionFailed { .. } => {
                let meta = self.storage.head_raw(&staged_path).await?.ok_or_else(|| {
                    DeltaError::conflict("staged payload disappeared after concurrent write")
                })?;
                if self
                    .read_staged_payload(&staged_path, &meta.version)
                    .await?
                    != payload
                {
                    return Err(DeltaError::conflict(
                        "Idempotency-Key already used with different request body",
                    ));
                }
                meta.version
            }
        };
        Ok(StagedCommit {
            staged_path,
            staged_version,
        })
    }

    /// Returns a committed response for `req` when an idempotency marker already exists.
    ///
    /// # Errors
    ///
    /// Returns an error when the idempotency key is invalid, staged path is invalid,
    /// or the idempotency key is reused with a different request payload.
    pub async fn replay_committed(
        &self,
        req: &CommitDeltaRequest,
    ) -> Result<Option<CommitDeltaResponse>> {
        validate_uuidv7(&req.idempotency_key)?;
        self.validate_staged_path(&req.staged_path)?;

        let Some(record) = self.read_idempotency_record(&req.idempotency_key).await? else {
            return Ok(None);
        };
        let legacy_hash = legacy_request_hash(req)?;
        if record.request_hash != legacy_hash {
            let payload = self
                .read_staged_payload(&req.staged_path, &req.staged_version)
                .await?;
            if record.request_hash
                != request_hash(
                    &req.idempotency_key,
                    req.read_version,
                    &req.staged_path,
                    &payload,
                )?
            {
                return Err(DeltaError::conflict(
                    "Idempotency-Key already used with different request body".to_string(),
                ));
            }
        }
        self.finalize_replayed_commit(&req.idempotency_key, record.version)
            .await?;
        Ok(Some(response_from_idempotency_record(record)))
    }

    async fn replay_committed_with_hashes(
        &self,
        idempotency_key: &str,
        request_hashes: &[String],
    ) -> Result<Option<CommitDeltaResponse>> {
        let Some(record) = self.read_idempotency_record(idempotency_key).await? else {
            return Ok(None);
        };

        if request_hashes.contains(&record.request_hash) {
            return Ok(Some(response_from_idempotency_record(record)));
        }

        Err(DeltaError::conflict(
            "Idempotency-Key already used with different request body".to_string(),
        ))
    }

    async fn read_validated_staged_payload(&self, req: &CommitDeltaRequest) -> Result<Bytes> {
        validate_uuidv7(&req.idempotency_key)?;
        self.validate_staged_path(&req.staged_path)?;
        let payload = self
            .read_staged_payload(&req.staged_path, &req.staged_version)
            .await?;
        Ok(payload)
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
        let staged_path = self.paths.staging_payload(&ulid);

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
    /// 1) Recovers and finalizes any expired or already-materialized inflight commit.
    /// 2) Applies idempotency replay if a committed marker exists.
    /// 3) Reserves the next version via CAS.
    /// 4) Writes `_delta_log/{version}.json` with `DoesNotExist` precondition.
    /// 5) Writes an idempotency marker for safe retries.
    /// 6) Finalizes coordinator state (`latest_version` + clear inflight) via CAS.
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
        let payload = self.read_validated_staged_payload(&req).await?;
        let request_hash = request_hash(
            &req.idempotency_key,
            req.read_version,
            &req.staged_path,
            &payload,
        )?;
        let legacy_hash = legacy_request_hash(&req)?;

        if let Some(response) = self
            .replay_committed_with_hashes(
                &req.idempotency_key,
                &[request_hash.clone(), legacy_hash],
            )
            .await?
        {
            self.finalize_replayed_commit(&req.idempotency_key, response.version)
                .await?;
            return Ok(response);
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
        let reserved = self.reserve_or_resume_inflight(&req, &payload, now).await?;

        let delta_log_path = self.paths.delta_log_json(reserved.version)?;

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
                self.require_materialized_log(&delta_log_path, &payload)
                    .await?;
            }
        }

        let response = CommitDeltaResponse {
            version: reserved.version,
            delta_log_path: delta_log_path.clone(),
        };

        self.write_idempotency_record(&req.idempotency_key, &request_hash, &response, now)
            .await?;

        self.finalize_inflight(&req.idempotency_key, reserved.version)
            .await?;

        Ok(response)
    }

    async fn require_materialized_log(&self, path: &str, payload: &[u8]) -> Result<()> {
        let expected_size = u64::try_from(payload.len())
            .map_err(|_| DeltaError::conflict("invalid Delta log size"))?;
        if self
            .storage
            .head_raw(path)
            .await?
            .is_none_or(|meta| meta.size != expected_size)
            || (!payload.is_empty()
                && self
                    .storage
                    .get_range(path, 0..expected_size)
                    .await?
                    .as_ref()
                    != payload)
        {
            return Err(DeltaError::conflict(
                "delta log version already exists with different contents",
            ));
        }
        Ok(())
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

    async fn load_state(&self) -> Result<(StoredCoordinatorState, Option<String>)> {
        let path = self.paths.coordinator_state();
        let meta = self.storage.head_raw(&path).await?;
        let Some(meta) = meta else {
            return Ok((StoredCoordinatorState::default(), None));
        };

        let bytes = self.storage.get_raw(&path).await?;
        Ok((decode_state(&bytes)?, Some(meta.version)))
    }

    async fn store_state(
        &self,
        state: &StoredCoordinatorState,
        expected_version: Option<&str>,
    ) -> Result<WriteResult> {
        let path = self.paths.coordinator_state();
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

            let Some(inflight) = state.core.inflight.clone() else {
                return Ok(None);
            };

            self.validate_staged_path(&inflight.staged_path)?;

            let delta_log_path = self.paths.delta_log_json(inflight.version)?;

            let commit_exists = self.storage.head_raw(&delta_log_path).await?.is_some();

            if commit_exists || now_ms >= inflight.expires_at_ms {
                let payload = self
                    .read_staged_payload(&inflight.staged_path, &inflight.staged_version)
                    .await?;
                self.verify_reserved_log(&state, inflight.version, &payload)?;
                let compare_log = if commit_exists {
                    true
                } else {
                    // Attempt to materialize the inflight commit from its staged payload.
                    let write = self
                        .storage
                        .put_raw(
                            &delta_log_path,
                            payload.clone(),
                            WritePrecondition::DoesNotExist,
                        )
                        .await?;
                    matches!(write, WriteResult::PreconditionFailed { .. })
                };
                if compare_log {
                    self.require_materialized_log(&delta_log_path, &payload)
                        .await?;
                }

                let read_version = inflight
                    .read_version
                    .unwrap_or_else(|| inflight.version.saturating_sub(1));
                let recovered_request_hash = request_hash(
                    &inflight.commit_id,
                    read_version,
                    &inflight.staged_path,
                    &payload,
                )?;

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

                let mut updated = state.clone();
                updated.core.latest_version = inflight.version.max(state.core.latest_version);
                updated.core.inflight = None;

                let put = self.store_state(&updated, Some(current_version)).await?;
                match put {
                    WriteResult::Success { .. } => {
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
        payload: &[u8],
        now: DateTime<Utc>,
    ) -> Result<ReservedCommit> {
        let now_ms = now.timestamp_millis();
        let expires_at_ms = now_ms.saturating_add(
            i64::try_from(self.config.inflight_ttl.as_millis()).unwrap_or(i64::MAX),
        );

        for _ in 0..self.config.max_cas_retries {
            let (state, version) = self.load_state().await?;

            if let Some(inflight) = &state.core.inflight {
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

                        if state.core.latest_version != req.read_version {
                            return Err(DeltaError::conflict(format!(
                                "stale read_version: expected {}, got {}",
                                state.core.latest_version, req.read_version
                            )));
                        }

                        self.verify_reserved_log(&state, inflight.version, payload)?;
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

            if state.core.latest_version != req.read_version {
                return Err(DeltaError::conflict(format!(
                    "stale read_version: expected {}, got {}",
                    state.core.latest_version, req.read_version
                )));
            }

            let new_version = state
                .core
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

            let mut updated = state;
            updated.core.inflight = Some(inflight);
            if new_version <= MAX_PINNED_DELTA_VERSION
                && usize::try_from(new_version).ok() == Some(updated.canonical_log_sha256.len())
                && self.is_canonical_root()
            {
                updated.canonical_log_sha256.push(sha256_hex(payload));
            }

            let put = self.store_state(&updated, version.as_deref()).await?;
            match put {
                WriteResult::Success { .. } => {
                    return Ok(ReservedCommit {
                        version: new_version,
                    });
                }
                WriteResult::PreconditionFailed { .. } => {}
            }
        }

        Err(DeltaError::conflict(
            "failed to reserve delta commit after retries".to_string(),
        ))
    }

    fn verify_reserved_log(
        &self,
        state: &StoredCoordinatorState,
        version: i64,
        payload: &[u8],
    ) -> Result<()> {
        let expected = usize::try_from(version)
            .ok()
            .and_then(|index| state.canonical_log_sha256.get(index));
        let Some(expected) = expected else {
            return Ok(());
        };
        if !self.is_canonical_root() || *expected != sha256_hex(payload) {
            return Err(DeltaError::conflict("reserved Delta log identity changed"));
        }
        Ok(())
    }

    async fn finalize_inflight(&self, commit_id: &str, committed_version: i64) -> Result<()> {
        for _ in 0..self.config.max_cas_retries {
            let (state, version) = self.load_state().await?;
            let Some(version) = version else {
                return Err(DeltaError::conflict(
                    "coordinator state missing during finalize".to_string(),
                ));
            };

            if state.core.inflight.is_none() && state.core.latest_version >= committed_version {
                return Ok(());
            }

            if let Some(inflight) = &state.core.inflight {
                if inflight.commit_id != commit_id {
                    return Err(DeltaError::conflict(
                        "coordinator inflight changed during finalize; retry".to_string(),
                    ));
                }
            }

            let mut updated = state.clone();
            updated.core.latest_version = committed_version.max(state.core.latest_version);
            updated.core.inflight = None;

            let put = self.store_state(&updated, Some(&version)).await?;
            match put {
                WriteResult::Success { .. } => return Ok(()),
                WriteResult::PreconditionFailed { .. } => {}
            }
        }

        Err(DeltaError::conflict(
            "failed to finalize delta commit after retries".to_string(),
        ))
    }

    async fn finalize_replayed_commit(
        &self,
        commit_id: &str,
        committed_version: i64,
    ) -> Result<()> {
        for _ in 0..self.config.max_cas_retries {
            let (state, version) = self.load_state().await?;
            let Some(version) = version else {
                return Ok(());
            };

            let Some(inflight) = &state.core.inflight else {
                if state.core.latest_version >= committed_version {
                    return Ok(());
                }

                let mut updated = state.clone();
                updated.core.latest_version = committed_version;

                let put = self.store_state(&updated, Some(&version)).await?;
                match put {
                    WriteResult::Success { .. } => return Ok(()),
                    WriteResult::PreconditionFailed { .. } => continue,
                }
            };

            if inflight.commit_id != commit_id {
                return Ok(());
            }

            let mut updated = state.clone();
            updated.core.latest_version = committed_version.max(inflight.version);
            updated.core.inflight = None;

            let put = self.store_state(&updated, Some(&version)).await?;
            match put {
                WriteResult::Success { .. } => return Ok(()),
                WriteResult::PreconditionFailed { .. } => {}
            }
        }

        Err(DeltaError::conflict(
            "failed to finalize replayed delta commit after retries".to_string(),
        ))
    }

    async fn read_idempotency_record(
        &self,
        key: &str,
    ) -> Result<Option<DeltaCommitIdempotencyRecord>> {
        let path = self.paths.idempotency_marker(key);
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

        let path = self.paths.idempotency_marker(key);
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
        let expected_prefix = format!("delta/staging/{}/", self.paths.table_id());
        if !staged_path.starts_with(&expected_prefix) {
            return Err(DeltaError::bad_request(format!(
                "staged_path must start with {expected_prefix}",
            )));
        }
        Ok(())
    }
}

fn decode_state(bytes: &[u8]) -> Result<StoredCoordinatorState> {
    serde_json::from_slice(bytes).map_err(|e| {
        DeltaError::serialization(format!("failed to deserialize coordinator state: {e}"))
    })
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

fn request_hash(
    key: &str,
    read_version: i64,
    staged_path: &str,
    staged_payload: &[u8],
) -> Result<String> {
    let staged_payload_sha256 = sha256_hex(staged_payload);
    let value = serde_json::json!({
        "idempotency_key": key,
        "read_version": read_version,
        "staged_path": staged_path,
        "staged_payload_sha256": staged_payload_sha256,
    });
    let value = serde_json::to_value(value).map_err(|e| {
        DeltaError::serialization(format!("failed to serialize commit request: {e}"))
    })?;
    canonical_hash_value(&value)
}

fn legacy_request_hash(req: &CommitDeltaRequest) -> Result<String> {
    let value = serde_json::to_value(req).map_err(|e| {
        DeltaError::serialization(format!("failed to serialize commit request: {e}"))
    })?;
    canonical_hash_value(&value)
}

fn canonical_hash_value(value: &serde_json::Value) -> Result<String> {
    let canonical = serde_jcs::to_string(&value).map_err(|e| {
        DeltaError::serialization(format!("failed to canonicalize commit request: {e}"))
    })?;
    Ok(sha256_hex(canonical.as_bytes()))
}

fn response_from_idempotency_record(record: DeltaCommitIdempotencyRecord) -> CommitDeltaResponse {
    CommitDeltaResponse {
        version: record.version,
        delta_log_path: record.delta_log_path,
    }
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

fn validate_uuidv7(key: &str) -> Result<()> {
    let uuid = Uuid::parse_str(key)
        .map_err(|_| DeltaError::bad_request("Idempotency-Key must be a valid UUID"))?;
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
