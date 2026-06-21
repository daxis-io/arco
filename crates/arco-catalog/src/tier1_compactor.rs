//! Tier-1 synchronous compaction for DDL events (ADR-018).

use std::collections::HashSet;

use bytes::Bytes;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use arco_core::lock::DistributedLock;
use arco_core::publish::Publisher;
use arco_core::storage::{StorageBackend, WriteResult};
use arco_core::storage_keys::{ManifestKey, StateKey};
use arco_core::sync_compact::SyncCompactRequest;
use arco_core::{
    CatalogDomain, CatalogEvent, CatalogEventPayload, CatalogPaths, ControlPlaneScope,
    ScopedStorage, VisibilityStatus,
};

use crate::error::{CatalogError, Result as CatalogResult};
use crate::manifest::{
    CatalogDomainManifest, DomainManifestPointer, LineageManifest, SearchManifest,
    compute_manifest_hash, next_manifest_id,
};
use crate::parquet_util::{CatalogCommitRecord, SearchPostingRecord};
use crate::sync_compact_permit_issuer;
use crate::tier1_events::{
    CatalogDdlEvent, CatalogDdlEventV2, CatalogDdlEventV3, CatalogDdlEventV4, LineageDdlEvent,
};
use crate::tier1_snapshot;
use crate::tier1_state;

/// Result of a Tier-1 sync compaction run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier1CompactionResult {
    /// New manifest version after compaction.
    pub manifest_version: String,
    /// Commit ULID for audit trail.
    pub commit_ulid: String,
    /// Visible immutable manifest identifier.
    pub manifest_id: String,
    /// Number of events processed.
    pub events_processed: usize,
    /// Snapshot version after compaction.
    pub snapshot_version: u64,
    /// Visibility of the compaction result for readers.
    pub visibility_status: VisibilityStatus,
    /// Whether legacy side effects must be repaired after pointer CAS success.
    pub repair_pending: bool,
}

/// Errors from Tier-1 synchronous compaction.
#[derive(Debug, Error)]
pub enum Tier1CompactionError {
    /// Fencing token doesn't match current lock holder.
    #[error("stale fencing token: expected {expected}, got {provided}")]
    StaleFencingToken {
        /// Expected fencing token value.
        expected: u64,
        /// Provided fencing token value.
        provided: u64,
    },
    /// Domain is not supported for sync compaction.
    #[error("unsupported domain for sync compaction: {domain}")]
    UnsupportedDomain {
        /// The unsupported domain name.
        domain: String,
    },
    /// Sync compaction is not yet implemented for the domain.
    #[error("sync compaction not implemented for {domain}: {message}")]
    NotImplemented {
        /// Domain requested for sync compaction.
        domain: String,
        /// Human-readable explanation.
        message: String,
    },
    /// Failed to read event files.
    #[error("failed to read event '{path}': {message}")]
    EventReadError {
        /// Path to the event file.
        path: String,
        /// Error message.
        message: String,
    },
    /// Failed to process events.
    #[error("event processing error: {message}")]
    ProcessingError {
        /// Error message.
        message: String,
    },
    /// Request validation failed.
    #[error("validation error: {message}")]
    Validation {
        /// Error message.
        message: String,
    },
    /// Failed to publish manifest (CAS conflict or storage error).
    #[error("manifest publish failed: {message}")]
    PublishFailed {
        /// Error message.
        message: String,
    },
}

impl From<Tier1CompactionError> for CatalogError {
    fn from(value: Tier1CompactionError) -> Self {
        match value {
            Tier1CompactionError::StaleFencingToken { expected, provided } => {
                Self::PreconditionFailed {
                    message: format!("stale fencing token: expected {expected}, got {provided}"),
                }
            }
            Tier1CompactionError::UnsupportedDomain { domain } => Self::Validation {
                message: format!("unsupported domain for sync compaction: {domain}"),
            },
            Tier1CompactionError::NotImplemented { domain, message } => Self::InvariantViolation {
                message: format!("sync compaction not implemented for {domain}: {message}"),
            },
            Tier1CompactionError::EventReadError { path, message } => Self::Storage {
                message: format!("failed to read event '{path}': {message}"),
            },
            Tier1CompactionError::ProcessingError { message } => {
                Self::InvariantViolation { message }
            }
            Tier1CompactionError::Validation { message } => Self::Validation { message },
            Tier1CompactionError::PublishFailed { message } => Self::CasFailed { message },
        }
    }
}

/// Compactor for Tier-1 DDL operations using explicit event paths.
pub struct Tier1Compactor {
    storage: ScopedStorage,
    scope: ControlPlaneScope,
    cas_max_retries: u32,
}

#[derive(Debug, Clone, Default)]
struct CatalogCommitMetadata {
    operation: Option<String>,
    object_type: Option<String>,
    object_id: Option<String>,
    object_name: Option<String>,
}

#[derive(Debug, Clone)]
struct ReservedCatalogCommit {
    previous_manifest_id: String,
    commit_ulid: String,
    published_at: chrono::DateTime<Utc>,
}

impl Tier1Compactor {
    /// Creates a new Tier-1 compactor.
    ///
    /// # Panics
    ///
    /// Panics if the already-validated scoped storage IDs cannot form a
    /// workspace alias scope.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new(storage: ScopedStorage) -> Self {
        let scope = ControlPlaneScope::workspace_alias(storage.tenant_id(), storage.workspace_id())
            .expect("ScopedStorage tenant/workspace IDs are already validated");
        Self::new_with_scope(storage, scope)
    }

    /// Creates a new Tier-1 compactor with an explicit control-plane scope.
    ///
    /// The supplied storage remains rooted at its current workspace prefix. This
    /// keeps Task 3 as an API-threading change only; moving durable catalog paths
    /// to metastore prefixes is handled by the later path migration tasks.
    ///
    /// # Panics
    ///
    /// Panics if the explicit scope does not match the scoped storage tenant and
    /// workspace.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new_with_scope(storage: ScopedStorage, scope: ControlPlaneScope) -> Self {
        Self::try_new_with_scope(storage, scope)
            .expect("explicit control-plane scope must match scoped storage")
    }

    /// Tries to create a Tier-1 compactor with an explicit control-plane scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage tenant/workspace does not match the
    /// execution tenant/workspace carried by the explicit scope.
    pub fn try_new_with_scope(
        storage: ScopedStorage,
        scope: ControlPlaneScope,
    ) -> CatalogResult<Self> {
        validate_storage_scope(&storage, &scope)?;
        Ok(Self {
            storage,
            scope,
            cas_max_retries: 5,
        })
    }

    /// Returns the explicit control-plane scope for this compactor.
    #[must_use]
    pub fn scope(&self) -> &ControlPlaneScope {
        &self.scope
    }

    /// Handles a synchronous compaction request.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails, any event cannot be read, or
    /// manifest publishing fails.
    pub async fn sync_compact_request(
        &self,
        request: SyncCompactRequest,
    ) -> Result<Tier1CompactionResult, Tier1CompactionError> {
        let domain = parse_domain(&request.domain)?;
        let canonical_lock_path = self.storage.lock(domain);

        if let Some(lock_path) = request.lock_path.as_deref() {
            if lock_path != canonical_lock_path {
                return Err(Tier1CompactionError::Validation {
                    message: format!(
                        "invalid lock_path: expected {canonical_lock_path}, got {lock_path}"
                    ),
                });
            }
        }

        tracing::info!(
            domain = %request.domain,
            event_count = request.event_paths.len(),
            fencing_token = request.fencing_token,
            tenant_id = %self.scope.tenant_id(),
            workspace_id = %self.scope.workspace_id(),
            metastore_id = %self.scope.metastore_id(),
            lock_path = request.lock_path.as_deref().unwrap_or(""),
            request_id = request.request_id.as_deref().unwrap_or(""),
            "handling sync compaction request"
        );

        self.sync_compact(&request.domain, request.event_paths, request.fencing_token)
            .await
    }

    /// Synchronously compacts pending event paths into the current domain manifest.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails, any event cannot be read, or
    /// manifest publishing fails.
    pub async fn sync_compact(
        &self,
        domain: &str,
        event_paths: Vec<String>,
        fencing_token: u64,
    ) -> Result<Tier1CompactionResult, Tier1CompactionError> {
        let domain = parse_domain(domain)?;

        // Reject unsupported domains early (before lock validation).
        match domain {
            CatalogDomain::Executions => {
                return Err(Tier1CompactionError::UnsupportedDomain {
                    domain: domain.as_str().to_string(),
                });
            }
            CatalogDomain::Catalog | CatalogDomain::Lineage | CatalogDomain::Search => {}
        }

        // Preserve the caller-supplied order for explicit event batches. This is
        // required for multi-event mutations like force deletes that must apply
        // table drops before namespace/catalog deletes.
        let event_paths = validate_event_paths(domain, event_paths)?;

        if event_paths.is_empty() && domain != CatalogDomain::Search {
            return Err(Tier1CompactionError::ProcessingError {
                message: "no event paths provided".to_string(),
            });
        }

        let lock_path = self.storage.lock(domain);
        let lock = DistributedLock::new(self.storage.backend().clone(), &lock_path);
        let lock_info =
            lock.read_lock_info()
                .await
                .map_err(|e| Tier1CompactionError::ProcessingError {
                    message: format!("failed to read lock info: {e}"),
                })?;

        let Some(lock_info) = lock_info else {
            return Err(Tier1CompactionError::StaleFencingToken {
                expected: 0,
                provided: fencing_token,
            });
        };

        if lock_info.is_expired() || lock_info.sequence_number != fencing_token {
            return Err(Tier1CompactionError::StaleFencingToken {
                expected: lock_info.sequence_number,
                provided: fencing_token,
            });
        }

        let issuer = sync_compact_permit_issuer(lock_info.sequence_number, &lock_path);

        match domain {
            CatalogDomain::Catalog => {
                self.sync_compact_catalog(event_paths, &lock, &issuer, fencing_token)
                    .await
            }
            CatalogDomain::Lineage => {
                self.sync_compact_lineage(event_paths, &lock, &issuer, fencing_token)
                    .await
            }
            CatalogDomain::Search => {
                self.sync_compact_search(event_paths, &lock, &issuer, fencing_token)
                    .await
            }
            // Early rejection cases handled above - these are unreachable.
            CatalogDomain::Executions => unreachable!(),
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn sync_compact_catalog(
        &self,
        event_paths: Vec<String>,
        lock: &DistributedLock<dyn StorageBackend>,
        issuer: &arco_core::publish::PermitIssuer,
        fencing_token: u64,
    ) -> Result<Tier1CompactionResult, Tier1CompactionError> {
        let publisher = Publisher::new(&self.storage);
        let mut reserved_commit: Option<ReservedCatalogCommit> = None;

        for attempt in 1..=self.cas_max_retries {
            let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
            let pointer_meta = self
                .storage
                .head_raw(&pointer_path)
                .await
                .map_err(map_processing_error)?
                .ok_or_else(|| Tier1CompactionError::ProcessingError {
                    message: "missing catalog manifest pointer".to_string(),
                })?;
            let pointer_bytes = self
                .storage
                .get_raw(&pointer_path)
                .await
                .map_err(map_processing_error)?;
            let pointer: DomainManifestPointer =
                serde_json::from_slice(&pointer_bytes).map_err(map_processing_error)?;
            if fencing_token < pointer.epoch {
                return Err(Tier1CompactionError::StaleFencingToken {
                    expected: pointer.epoch,
                    provided: fencing_token,
                });
            }
            let pointer_expected_version = Some(pointer_meta.version.clone());
            let pointer_parent_hash = Some(compute_manifest_hash(&pointer_bytes));
            let previous_manifest_path = pointer.manifest_path.clone();
            let prev_bytes = self
                .storage
                .get_raw(&pointer.manifest_path)
                .await
                .map_err(map_processing_error)?;

            let prev_raw_hash = compute_manifest_hash(&prev_bytes);
            let mut manifest: CatalogDomainManifest =
                serde_json::from_slice(&prev_bytes).map_err(map_processing_error)?;
            let prev_manifest = manifest.clone();
            let unapplied_event_paths = unapplied_event_paths_after_watermark(
                &event_paths,
                manifest.watermark_event_id.as_deref(),
            );
            if unapplied_event_paths.is_empty() {
                return Ok(Tier1CompactionResult {
                    manifest_version: pointer_meta.version,
                    commit_ulid: manifest
                        .last_commit_id
                        .clone()
                        .or_else(|| manifest.commit_ulid.clone())
                        .unwrap_or_default(),
                    manifest_id: manifest.manifest_id,
                    events_processed: 0,
                    snapshot_version: manifest.snapshot_version,
                    visibility_status: VisibilityStatus::Visible,
                    repair_pending: false,
                });
            }
            let events_processed = unapplied_event_paths.len();
            let last_event_id = max_event_id_from_paths(&unapplied_event_paths);

            let mut state = tier1_state::load_catalog_state(&self.storage, &manifest.snapshot_path)
                .await
                .map_err(map_processing_error)?;

            let mut commit_metadata = CatalogCommitMetadata::default();
            for path in &unapplied_event_paths {
                let event = read_catalog_event(&self.storage, path).await?;
                if unapplied_event_paths.len() == 1 {
                    commit_metadata = catalog_commit_metadata(&event);
                }
                apply_catalog_event(&mut state, event)?;
            }

            let next_version = manifest.snapshot_version + 1;
            let next_commit = reserved_commit
                .as_ref()
                .filter(|commit| commit.previous_manifest_id == prev_manifest.manifest_id)
                .cloned()
                .unwrap_or(ReservedCatalogCommit {
                    previous_manifest_id: prev_manifest.manifest_id.clone(),
                    commit_ulid: next_commit_ulid(prev_manifest.commit_ulid.as_deref())?,
                    published_at: Utc::now(),
                });
            reserved_commit = Some(next_commit.clone());
            let manifest_id = next_available_manifest_id(
                &self.storage,
                CatalogDomain::Catalog,
                &prev_manifest.manifest_id,
            )
            .await?;
            let snapshot_manifest_path =
                CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, &manifest_id);

            state.commits.push(build_catalog_commit_record(
                &next_commit.commit_ulid,
                next_version,
                next_commit.published_at.timestamp_millis(),
                fencing_token,
                last_event_id.clone(),
                commit_metadata,
            )?);

            let snapshot_dir =
                StateKey::snapshot_attempt_dir(CatalogDomain::Catalog, next_version, &manifest_id);
            let mut snapshot = tier1_snapshot::write_catalog_snapshot_in_dir(
                &self.storage,
                next_version,
                snapshot_dir.as_ref(),
                &state,
            )
            .await
            .map_err(map_processing_error)?;
            snapshot.published_at = next_commit.published_at;

            manifest.snapshot_version = snapshot.version;
            manifest.snapshot_path.clone_from(&snapshot.path);
            manifest.snapshot = Some(snapshot.clone());
            manifest.updated_at = next_commit.published_at;
            manifest.parent_hash = Some(prev_raw_hash.clone());
            manifest.fencing_token = Some(fencing_token);
            manifest.epoch = fencing_token;
            manifest.commit_ulid = Some(next_commit.commit_ulid.clone());
            manifest.previous_manifest_path = Some(previous_manifest_path.clone());
            manifest.writer_session_id = Some(issuer.resource().to_string());
            manifest.watermark_event_id.clone_from(&last_event_id);
            manifest.manifest_id = manifest_id;

            manifest
                .validate_succession(&prev_manifest, &prev_raw_hash)
                .map_err(|message| Tier1CompactionError::ProcessingError { message })?;

            manifest.last_commit_id = Some(next_commit.commit_ulid.clone());

            let manifest_bytes = serde_json::to_vec(&manifest).map_err(map_processing_error)?;
            match self
                .storage
                .put_raw(
                    &snapshot_manifest_path,
                    Bytes::from(manifest_bytes.clone()),
                    arco_core::WritePrecondition::DoesNotExist,
                )
                .await
                .map_err(map_processing_error)?
            {
                WriteResult::Success { .. } => {}
                WriteResult::PreconditionFailed { .. } => {
                    if attempt == self.cas_max_retries {
                        return Err(Tier1CompactionError::PublishFailed {
                            message: format!(
                                "catalog snapshot manifest already exists: {snapshot_manifest_path}"
                            ),
                        });
                    }
                    continue;
                }
            }

            let pointer = DomainManifestPointer {
                manifest_id: manifest.manifest_id.clone(),
                manifest_path: snapshot_manifest_path,
                epoch: fencing_token,
                parent_pointer_hash: pointer_parent_hash.clone(),
                updated_at: Utc::now(),
            };
            let pointer_bytes = serde_json::to_vec(&pointer).map_err(map_processing_error)?;
            let permit = pointer_expected_version.as_deref().map_or_else(
                || {
                    issuer.issue_create_permit_with_commit_ulid(
                        CatalogDomain::Catalog.as_str(),
                        next_commit.commit_ulid.clone(),
                    )
                },
                |expected| {
                    issuer.issue_permit_with_commit_ulid(
                        CatalogDomain::Catalog.as_str(),
                        expected.to_string(),
                        next_commit.commit_ulid.clone(),
                    )
                },
            );

            revalidate_lock(lock, fencing_token).await?;

            match publisher
                .publish(
                    permit,
                    &ManifestKey::domain_pointer(CatalogDomain::Catalog),
                    Bytes::from(pointer_bytes),
                )
                .await
                .map_err(map_publish_error)?
            {
                WriteResult::Success { version } => {
                    return Ok(Tier1CompactionResult {
                        manifest_version: version,
                        commit_ulid: next_commit.commit_ulid,
                        manifest_id: manifest.manifest_id,
                        events_processed,
                        snapshot_version: manifest.snapshot_version,
                        visibility_status: VisibilityStatus::Visible,
                        repair_pending: false,
                    });
                }
                WriteResult::PreconditionFailed { .. } => {
                    if attempt == self.cas_max_retries {
                        return Err(Tier1CompactionError::PublishFailed {
                            message: "manifest update lost CAS race after max retries".to_string(),
                        });
                    }
                }
            }
        }

        Err(Tier1CompactionError::PublishFailed {
            message: "manifest update lost CAS race after max retries".to_string(),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn sync_compact_lineage(
        &self,
        event_paths: Vec<String>,
        lock: &DistributedLock<dyn StorageBackend>,
        issuer: &arco_core::publish::PermitIssuer,
        fencing_token: u64,
    ) -> Result<Tier1CompactionResult, Tier1CompactionError> {
        let events_processed = event_paths.len();
        let last_event_id = max_event_id_from_paths(&event_paths);
        let publisher = Publisher::new(&self.storage);

        for attempt in 1..=self.cas_max_retries {
            let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Lineage);
            let pointer_meta = self
                .storage
                .head_raw(&pointer_path)
                .await
                .map_err(map_processing_error)?
                .ok_or_else(|| Tier1CompactionError::ProcessingError {
                    message: "missing lineage manifest pointer".to_string(),
                })?;
            let pointer_bytes = self
                .storage
                .get_raw(&pointer_path)
                .await
                .map_err(map_processing_error)?;
            let pointer: DomainManifestPointer =
                serde_json::from_slice(&pointer_bytes).map_err(map_processing_error)?;
            if fencing_token < pointer.epoch {
                return Err(Tier1CompactionError::StaleFencingToken {
                    expected: pointer.epoch,
                    provided: fencing_token,
                });
            }
            let pointer_expected_version = Some(pointer_meta.version.clone());
            let pointer_parent_hash = Some(compute_manifest_hash(&pointer_bytes));
            let previous_manifest_path = pointer.manifest_path.clone();
            let prev_bytes = self
                .storage
                .get_raw(&pointer.manifest_path)
                .await
                .map_err(map_processing_error)?;

            let prev_raw_hash = compute_manifest_hash(&prev_bytes);
            let mut manifest: LineageManifest =
                serde_json::from_slice(&prev_bytes).map_err(map_processing_error)?;
            let prev_manifest = manifest.clone();

            let mut state = tier1_state::load_lineage_state(&self.storage, &manifest.edges_path)
                .await
                .map_err(map_processing_error)?;

            for path in &event_paths {
                let event = read_lineage_event(&self.storage, path).await?;
                apply_lineage_event(&mut state, event)?;
            }

            let next_version = manifest.snapshot_version + 1;
            let commit_ulid = next_commit_ulid(prev_manifest.commit_ulid.as_deref())?;
            let manifest_id = next_available_manifest_id(
                &self.storage,
                CatalogDomain::Lineage,
                &prev_manifest.manifest_id,
            )
            .await?;
            let snapshot_dir =
                StateKey::snapshot_attempt_dir(CatalogDomain::Lineage, next_version, &manifest_id);
            let snapshot = tier1_snapshot::write_lineage_snapshot_in_dir(
                &self.storage,
                next_version,
                snapshot_dir.as_ref(),
                &state,
            )
            .await
            .map_err(map_processing_error)?;

            manifest.snapshot_version = snapshot.version;
            manifest.edges_path.clone_from(&snapshot.path);
            manifest.snapshot = Some(snapshot.clone());
            manifest.updated_at = Utc::now();
            manifest.parent_hash = Some(prev_raw_hash.clone());
            manifest.fencing_token = Some(fencing_token);
            manifest.epoch = fencing_token;
            manifest.commit_ulid = Some(commit_ulid.clone());
            manifest.previous_manifest_path = Some(previous_manifest_path.clone());
            manifest.writer_session_id = Some(issuer.resource().to_string());
            manifest.watermark_event_id.clone_from(&last_event_id);
            manifest.manifest_id = manifest_id;

            manifest
                .validate_succession(&prev_manifest, &prev_raw_hash)
                .map_err(|message| Tier1CompactionError::ProcessingError { message })?;

            manifest.last_commit_id = Some(commit_ulid.clone());

            let manifest_bytes = serde_json::to_vec(&manifest).map_err(map_processing_error)?;
            let snapshot_manifest_path = CatalogPaths::domain_manifest_snapshot(
                CatalogDomain::Lineage,
                &manifest.manifest_id,
            );
            match self
                .storage
                .put_raw(
                    &snapshot_manifest_path,
                    Bytes::from(manifest_bytes.clone()),
                    arco_core::WritePrecondition::DoesNotExist,
                )
                .await
                .map_err(map_processing_error)?
            {
                WriteResult::Success { .. } => {}
                WriteResult::PreconditionFailed { .. } => {
                    if attempt == self.cas_max_retries {
                        return Err(Tier1CompactionError::PublishFailed {
                            message: format!(
                                "lineage snapshot manifest already exists: {snapshot_manifest_path}"
                            ),
                        });
                    }
                    continue;
                }
            }

            let pointer = DomainManifestPointer {
                manifest_id: manifest.manifest_id.clone(),
                manifest_path: snapshot_manifest_path,
                epoch: fencing_token,
                parent_pointer_hash: pointer_parent_hash.clone(),
                updated_at: Utc::now(),
            };
            let pointer_bytes = serde_json::to_vec(&pointer).map_err(map_processing_error)?;
            let permit = pointer_expected_version.as_deref().map_or_else(
                || {
                    issuer.issue_create_permit_with_commit_ulid(
                        CatalogDomain::Lineage.as_str(),
                        commit_ulid.clone(),
                    )
                },
                |expected| {
                    issuer.issue_permit_with_commit_ulid(
                        CatalogDomain::Lineage.as_str(),
                        expected.to_string(),
                        commit_ulid.clone(),
                    )
                },
            );

            revalidate_lock(lock, fencing_token).await?;

            match publisher
                .publish(
                    permit,
                    &ManifestKey::domain_pointer(CatalogDomain::Lineage),
                    Bytes::from(pointer_bytes),
                )
                .await
                .map_err(map_publish_error)?
            {
                WriteResult::Success { version } => {
                    return Ok(Tier1CompactionResult {
                        manifest_version: version,
                        commit_ulid,
                        manifest_id: manifest.manifest_id,
                        events_processed,
                        snapshot_version: manifest.snapshot_version,
                        visibility_status: VisibilityStatus::Visible,
                        repair_pending: false,
                    });
                }
                WriteResult::PreconditionFailed { .. } => {
                    if attempt == self.cas_max_retries {
                        return Err(Tier1CompactionError::PublishFailed {
                            message: "lineage manifest update lost CAS race after max retries"
                                .to_string(),
                        });
                    }
                }
            }
        }

        Err(Tier1CompactionError::PublishFailed {
            message: "lineage manifest update lost CAS race after max retries".to_string(),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn sync_compact_search(
        &self,
        event_paths: Vec<String>,
        lock: &DistributedLock<dyn StorageBackend>,
        issuer: &arco_core::publish::PermitIssuer,
        fencing_token: u64,
    ) -> Result<Tier1CompactionResult, Tier1CompactionError> {
        let events_processed = event_paths.len();
        let last_event_id = max_event_id_from_paths(&event_paths);
        let publisher = Publisher::new(&self.storage);

        for attempt in 1..=self.cas_max_retries {
            let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Search);
            let pointer_meta = self
                .storage
                .head_raw(&pointer_path)
                .await
                .map_err(map_processing_error)?
                .ok_or_else(|| Tier1CompactionError::ProcessingError {
                    message: "missing search manifest pointer".to_string(),
                })?;
            let pointer_bytes = self
                .storage
                .get_raw(&pointer_path)
                .await
                .map_err(map_processing_error)?;
            let pointer: DomainManifestPointer =
                serde_json::from_slice(&pointer_bytes).map_err(map_processing_error)?;
            if fencing_token < pointer.epoch {
                return Err(Tier1CompactionError::StaleFencingToken {
                    expected: pointer.epoch,
                    provided: fencing_token,
                });
            }
            let pointer_expected_version = Some(pointer_meta.version);
            let pointer_parent_hash = Some(compute_manifest_hash(&pointer_bytes));
            let previous_manifest_path = pointer.manifest_path.clone();
            let prev_bytes = self
                .storage
                .get_raw(&pointer.manifest_path)
                .await
                .map_err(map_processing_error)?;

            let prev_raw_hash = compute_manifest_hash(&prev_bytes);
            let mut manifest: SearchManifest =
                serde_json::from_slice(&prev_bytes).map_err(map_processing_error)?;
            let prev_manifest = manifest.clone();

            let catalog_manifest_path =
                resolve_manifest_path(&self.storage, CatalogDomain::Catalog)
                    .await
                    .map_err(map_processing_error)?;
            let catalog_bytes = self
                .storage
                .get_raw(&catalog_manifest_path)
                .await
                .map_err(map_processing_error)?;
            let catalog_manifest: CatalogDomainManifest =
                serde_json::from_slice(&catalog_bytes).map_err(map_processing_error)?;

            let catalog_state =
                tier1_state::load_catalog_state(&self.storage, &catalog_manifest.snapshot_path)
                    .await
                    .map_err(map_processing_error)?;
            let search_state = build_search_state(&catalog_state);

            let next_version = manifest.snapshot_version + 1;
            let commit_ulid = next_commit_ulid(prev_manifest.commit_ulid.as_deref())?;
            let manifest_id = next_available_manifest_id(
                &self.storage,
                CatalogDomain::Search,
                &prev_manifest.manifest_id,
            )
            .await?;
            let snapshot_dir =
                StateKey::snapshot_attempt_dir(CatalogDomain::Search, next_version, &manifest_id);
            let snapshot = tier1_snapshot::write_search_snapshot_in_dir(
                &self.storage,
                next_version,
                snapshot_dir.as_ref(),
                &search_state,
            )
            .await
            .map_err(map_processing_error)?;

            manifest.snapshot_version = snapshot.version;
            manifest.base_path.clone_from(&snapshot.path);
            manifest.snapshot = Some(snapshot.clone());
            manifest.updated_at = Utc::now();
            manifest.parent_hash = Some(prev_raw_hash.clone());
            manifest.fencing_token = Some(fencing_token);
            manifest.epoch = fencing_token;
            manifest.commit_ulid = Some(commit_ulid.clone());
            manifest.previous_manifest_path = Some(previous_manifest_path.clone());
            manifest.writer_session_id = Some(issuer.resource().to_string());
            manifest.watermark_event_id.clone_from(&last_event_id);
            manifest.manifest_id = manifest_id;

            manifest
                .validate_succession(&prev_manifest, &prev_raw_hash)
                .map_err(|message| Tier1CompactionError::ProcessingError { message })?;

            manifest.last_commit_id = Some(commit_ulid.clone());

            let manifest_bytes = serde_json::to_vec(&manifest).map_err(map_processing_error)?;
            let snapshot_manifest_path = CatalogPaths::domain_manifest_snapshot(
                CatalogDomain::Search,
                &manifest.manifest_id,
            );
            match self
                .storage
                .put_raw(
                    &snapshot_manifest_path,
                    Bytes::from(manifest_bytes.clone()),
                    arco_core::WritePrecondition::DoesNotExist,
                )
                .await
                .map_err(map_processing_error)?
            {
                WriteResult::Success { .. } => {}
                WriteResult::PreconditionFailed { .. } => {
                    if attempt == self.cas_max_retries {
                        return Err(Tier1CompactionError::PublishFailed {
                            message: format!(
                                "search snapshot manifest already exists: {snapshot_manifest_path}"
                            ),
                        });
                    }
                    continue;
                }
            }

            let pointer = DomainManifestPointer {
                manifest_id: manifest.manifest_id.clone(),
                manifest_path: snapshot_manifest_path,
                epoch: fencing_token,
                parent_pointer_hash: pointer_parent_hash.clone(),
                updated_at: Utc::now(),
            };
            let pointer_bytes = serde_json::to_vec(&pointer).map_err(map_processing_error)?;
            let permit = pointer_expected_version.as_deref().map_or_else(
                || {
                    issuer.issue_create_permit_with_commit_ulid(
                        CatalogDomain::Search.as_str(),
                        commit_ulid.clone(),
                    )
                },
                |expected| {
                    issuer.issue_permit_with_commit_ulid(
                        CatalogDomain::Search.as_str(),
                        expected.to_string(),
                        commit_ulid.clone(),
                    )
                },
            );

            revalidate_lock(lock, fencing_token).await?;

            match publisher
                .publish(
                    permit,
                    &ManifestKey::domain_pointer(CatalogDomain::Search),
                    Bytes::from(pointer_bytes),
                )
                .await
                .map_err(map_publish_error)?
            {
                WriteResult::Success { version } => {
                    return Ok(Tier1CompactionResult {
                        manifest_version: version,
                        commit_ulid,
                        manifest_id: manifest.manifest_id,
                        events_processed,
                        snapshot_version: manifest.snapshot_version,
                        visibility_status: VisibilityStatus::Visible,
                        repair_pending: false,
                    });
                }
                WriteResult::PreconditionFailed { .. } => {
                    if attempt == self.cas_max_retries {
                        return Err(Tier1CompactionError::PublishFailed {
                            message: "search manifest update lost CAS race after max retries"
                                .to_string(),
                        });
                    }
                }
            }
        }

        Err(Tier1CompactionError::PublishFailed {
            message: "search manifest update lost CAS race after max retries".to_string(),
        })
    }
}

fn validate_storage_scope(storage: &ScopedStorage, scope: &ControlPlaneScope) -> CatalogResult<()> {
    if storage.tenant_id() != scope.tenant_id() {
        return Err(CatalogError::Validation {
            message: format!(
                "storage tenant '{}' does not match control-plane tenant '{}'",
                storage.tenant_id(),
                scope.tenant_id()
            ),
        });
    }
    if storage.workspace_id() != scope.workspace_id() {
        return Err(CatalogError::Validation {
            message: format!(
                "storage workspace '{}' does not match control-plane workspace '{}'",
                storage.workspace_id(),
                scope.workspace_id()
            ),
        });
    }
    Ok(())
}

fn parse_domain(domain: &str) -> Result<CatalogDomain, Tier1CompactionError> {
    let normalized = domain.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "catalog" => Ok(CatalogDomain::Catalog),
        "lineage" => Ok(CatalogDomain::Lineage),
        "search" => Ok(CatalogDomain::Search),
        "executions" => Ok(CatalogDomain::Executions),
        _ => Err(Tier1CompactionError::UnsupportedDomain {
            domain: domain.to_string(),
        }),
    }
}

fn event_id_from_path(path: &str) -> Option<&str> {
    path.rsplit('/')
        .next()
        .and_then(|name| name.strip_suffix(".json"))
}

fn max_event_id_from_paths(event_paths: &[String]) -> Option<String> {
    event_paths
        .iter()
        .filter_map(|path| event_id_from_path(path))
        .max()
        .map(str::to_string)
}

fn unapplied_event_paths_after_watermark(
    event_paths: &[String],
    watermark_event_id: Option<&str>,
) -> Vec<String> {
    let Some(watermark_event_id) = watermark_event_id else {
        return event_paths.to_vec();
    };
    event_paths
        .iter()
        .filter(|path| {
            event_id_from_path(path).is_none_or(|event_id| event_id > watermark_event_id)
        })
        .cloned()
        .collect()
}

fn validate_event_paths(
    domain: CatalogDomain,
    event_paths: Vec<String>,
) -> Result<Vec<String>, Tier1CompactionError> {
    let prefix = format!("ledger/{}/", domain.as_str());
    let mut unique = HashSet::new();
    let mut filtered = Vec::new();

    for path in event_paths {
        if !path.starts_with(&prefix) {
            return Err(Tier1CompactionError::ProcessingError {
                message: format!("event path '{path}' is outside {prefix}"),
            });
        }
        if !std::path::Path::new(&path)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
        {
            return Err(Tier1CompactionError::ProcessingError {
                message: format!("event path '{path}' must end with .json"),
            });
        }
        if unique.insert(path.clone()) {
            filtered.push(path);
        }
    }

    Ok(filtered)
}

#[derive(Debug)]
enum ParsedCatalogDdlEvent {
    V1(CatalogDdlEvent),
    V2(CatalogDdlEventV2),
    V3(CatalogDdlEventV3),
    V4(CatalogDdlEventV4),
}

fn catalog_commit_metadata(event: &ParsedCatalogDdlEvent) -> CatalogCommitMetadata {
    match event {
        ParsedCatalogDdlEvent::V1(event) => match event {
            CatalogDdlEvent::NamespaceCreated { namespace } => CatalogCommitMetadata {
                operation: Some("namespace_created".to_string()),
                object_type: Some("namespace".to_string()),
                object_id: Some(namespace.id.clone()),
                object_name: Some(namespace.name.clone()),
            },
            CatalogDdlEvent::NamespaceUpdated { namespace } => CatalogCommitMetadata {
                operation: Some("namespace_updated".to_string()),
                object_type: Some("namespace".to_string()),
                object_id: Some(namespace.id.clone()),
                object_name: Some(namespace.name.clone()),
            },
            CatalogDdlEvent::NamespaceDeleted {
                namespace_id,
                namespace_name,
            } => CatalogCommitMetadata {
                operation: Some("namespace_deleted".to_string()),
                object_type: Some("namespace".to_string()),
                object_id: Some(namespace_id.clone()),
                object_name: Some(namespace_name.clone()),
            },
            CatalogDdlEvent::TableRegistered { table, .. } => CatalogCommitMetadata {
                operation: Some("table_registered".to_string()),
                object_type: Some("table".to_string()),
                object_id: Some(table.id.clone()),
                object_name: Some(table.name.clone()),
            },
            CatalogDdlEvent::TableUpdated { table } => CatalogCommitMetadata {
                operation: Some("table_updated".to_string()),
                object_type: Some("table".to_string()),
                object_id: Some(table.id.clone()),
                object_name: Some(table.name.clone()),
            },
            CatalogDdlEvent::TableDropped {
                table_id,
                table_name,
                ..
            } => CatalogCommitMetadata {
                operation: Some("table_dropped".to_string()),
                object_type: Some("table".to_string()),
                object_id: Some(table_id.clone()),
                object_name: Some(table_name.clone()),
            },
            CatalogDdlEvent::TableRenamed {
                table_id, new_name, ..
            } => CatalogCommitMetadata {
                operation: Some("table_renamed".to_string()),
                object_type: Some("table".to_string()),
                object_id: Some(table_id.clone()),
                object_name: Some(new_name.clone()),
            },
        },
        ParsedCatalogDdlEvent::V2(event) => match event {
            CatalogDdlEventV2::CatalogCreated { catalog } => CatalogCommitMetadata {
                operation: Some("catalog_created".to_string()),
                object_type: Some("catalog".to_string()),
                object_id: Some(catalog.id.clone()),
                object_name: Some(catalog.name.clone()),
            },
        },
        ParsedCatalogDdlEvent::V3(event) => match event {
            CatalogDdlEventV3::CatalogUpdated { catalog } => CatalogCommitMetadata {
                operation: Some("catalog_updated".to_string()),
                object_type: Some("catalog".to_string()),
                object_id: Some(catalog.id.clone()),
                object_name: Some(catalog.name.clone()),
            },
            CatalogDdlEventV3::CatalogDeleted {
                catalog_id,
                catalog_name,
            } => CatalogCommitMetadata {
                operation: Some("catalog_deleted".to_string()),
                object_type: Some("catalog".to_string()),
                object_id: Some(catalog_id.clone()),
                object_name: Some(catalog_name.clone()),
            },
        },
        ParsedCatalogDdlEvent::V4(event) => match event {
            CatalogDdlEventV4::CatalogRenamed { catalog, .. } => CatalogCommitMetadata {
                operation: Some("catalog_renamed".to_string()),
                object_type: Some("catalog".to_string()),
                object_id: Some(catalog.id.clone()),
                object_name: Some(catalog.name.clone()),
            },
            CatalogDdlEventV4::NamespaceRenamed { namespace, .. } => CatalogCommitMetadata {
                operation: Some("namespace_renamed".to_string()),
                object_type: Some("namespace".to_string()),
                object_id: Some(namespace.id.clone()),
                object_name: Some(namespace.name.clone()),
            },
        },
    }
}

fn build_catalog_commit_record(
    commit_ulid: &str,
    snapshot_version: u64,
    published_at: i64,
    fencing_token: u64,
    watermark_event_id: Option<String>,
    metadata: CatalogCommitMetadata,
) -> Result<CatalogCommitRecord, Tier1CompactionError> {
    let snapshot_version =
        i64::try_from(snapshot_version).map_err(|_| Tier1CompactionError::ProcessingError {
            message: format!("snapshot_version {snapshot_version} does not fit in i64"),
        })?;
    let fencing_token =
        i64::try_from(fencing_token).map_err(|_| Tier1CompactionError::ProcessingError {
            message: format!("fencing_token {fencing_token} does not fit in i64"),
        })?;

    Ok(CatalogCommitRecord {
        commit_ulid: commit_ulid.to_string(),
        snapshot_version,
        published_at,
        fencing_token,
        watermark_event_id,
        operation: metadata.operation,
        object_type: metadata.object_type,
        object_id: metadata.object_id,
        object_name: metadata.object_name,
    })
}

async fn read_catalog_event(
    storage: &ScopedStorage,
    path: &str,
) -> Result<ParsedCatalogDdlEvent, Tier1CompactionError> {
    let data = storage
        .get_raw(path)
        .await
        .map_err(|e| Tier1CompactionError::EventReadError {
            path: path.to_string(),
            message: e.to_string(),
        })?;

    let envelope: CatalogEvent<serde_json::Value> =
        serde_json::from_slice(&data).map_err(map_processing_error)?;

    if envelope.event_type != CatalogDdlEvent::EVENT_TYPE {
        return Err(Tier1CompactionError::ProcessingError {
            message: format!(
                "unexpected catalog event type {} v{}",
                envelope.event_type, envelope.event_version
            ),
        });
    }

    envelope
        .validate()
        .map_err(|e| Tier1CompactionError::ProcessingError {
            message: format!("invalid catalog event envelope: {e}"),
        })?;

    match envelope.event_version {
        CatalogDdlEvent::EVENT_VERSION => {
            let payload: CatalogDdlEvent =
                serde_json::from_value(envelope.payload).map_err(map_processing_error)?;
            Ok(ParsedCatalogDdlEvent::V1(payload))
        }
        CatalogDdlEventV2::EVENT_VERSION => {
            let payload: CatalogDdlEventV2 =
                serde_json::from_value(envelope.payload).map_err(map_processing_error)?;
            Ok(ParsedCatalogDdlEvent::V2(payload))
        }
        CatalogDdlEventV3::EVENT_VERSION => {
            let payload: CatalogDdlEventV3 =
                serde_json::from_value(envelope.payload).map_err(map_processing_error)?;
            Ok(ParsedCatalogDdlEvent::V3(payload))
        }
        CatalogDdlEventV4::EVENT_VERSION => {
            let payload: CatalogDdlEventV4 =
                serde_json::from_value(envelope.payload).map_err(map_processing_error)?;
            Ok(ParsedCatalogDdlEvent::V4(payload))
        }
        other => Err(Tier1CompactionError::ProcessingError {
            message: format!(
                "unexpected catalog event type {} v{other}",
                envelope.event_type
            ),
        }),
    }
}

async fn read_lineage_event(
    storage: &ScopedStorage,
    path: &str,
) -> Result<LineageDdlEvent, Tier1CompactionError> {
    let data = storage
        .get_raw(path)
        .await
        .map_err(|e| Tier1CompactionError::EventReadError {
            path: path.to_string(),
            message: e.to_string(),
        })?;

    let envelope: CatalogEvent<LineageDdlEvent> =
        serde_json::from_slice(&data).map_err(map_processing_error)?;

    if envelope.event_type != LineageDdlEvent::EVENT_TYPE
        || envelope.event_version != LineageDdlEvent::EVENT_VERSION
    {
        return Err(Tier1CompactionError::ProcessingError {
            message: format!(
                "unexpected lineage event type {} v{}",
                envelope.event_type, envelope.event_version
            ),
        });
    }

    envelope
        .validate()
        .map_err(|e| Tier1CompactionError::ProcessingError {
            message: format!("invalid lineage event envelope: {e}"),
        })?;

    Ok(envelope.payload)
}

#[allow(clippy::too_many_lines, clippy::indexing_slicing)]
fn apply_catalog_event(
    state: &mut crate::state::CatalogState,
    event: ParsedCatalogDdlEvent,
) -> Result<(), Tier1CompactionError> {
    match event {
        ParsedCatalogDdlEvent::V1(event) => apply_catalog_event_v1(state, event),
        ParsedCatalogDdlEvent::V2(event) => apply_catalog_event_v2(state, event),
        ParsedCatalogDdlEvent::V3(event) => apply_catalog_event_v3(state, event),
        ParsedCatalogDdlEvent::V4(event) => apply_catalog_event_v4(state, event),
    }
}

#[allow(clippy::too_many_lines, clippy::indexing_slicing)]
fn apply_catalog_event_v1(
    state: &mut crate::state::CatalogState,
    event: CatalogDdlEvent,
) -> Result<(), Tier1CompactionError> {
    match event {
        CatalogDdlEvent::NamespaceCreated { namespace } => {
            if let Some(existing) = state.namespaces.iter().find(|ns| ns.id == namespace.id) {
                if existing == &namespace {
                    return Ok(());
                }
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("namespace id collision for {}", namespace.id),
                });
            }
            let default_catalog_id = state
                .catalogs
                .iter()
                .find(|c| c.name == "default")
                .map(|c| c.id.as_str());
            let new_catalog_id = namespace.catalog_id.as_deref().or(default_catalog_id);
            if state.namespaces.iter().any(|ns| {
                ns.name == namespace.name
                    && ns.catalog_id.as_deref().or(default_catalog_id) == new_catalog_id
            }) {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("namespace '{}' already exists", namespace.name),
                });
            }
            state.namespaces.push(namespace);
        }
        CatalogDdlEvent::NamespaceUpdated { namespace } => {
            let Some(existing) = state.namespaces.iter_mut().find(|ns| ns.id == namespace.id)
            else {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("namespace '{}' not found", namespace.id),
                });
            };

            if existing.name != namespace.name {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("namespace identity mismatch for {}", namespace.id),
                });
            }

            if existing == &namespace {
                return Ok(());
            }

            *existing = namespace;
        }
        CatalogDdlEvent::NamespaceDeleted {
            namespace_id,
            namespace_name,
        } => {
            let index = state.namespaces.iter().position(|ns| ns.id == namespace_id);

            let Some(index) = index else {
                return Ok(());
            };

            let Some(existing) = state.namespaces.get(index) else {
                return Ok(());
            };
            if existing.name != namespace_name {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("namespace name mismatch for {namespace_id}"),
                });
            }
            if state
                .tables
                .iter()
                .any(|table| table.namespace_id == namespace_id)
            {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("namespace '{namespace_name}' contains tables"),
                });
            }
            state.namespaces.remove(index);
        }
        CatalogDdlEvent::TableRegistered { table, columns } => {
            if !state
                .namespaces
                .iter()
                .any(|ns| ns.id == table.namespace_id)
            {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("namespace '{}' not found", table.namespace_id),
                });
            }

            if let Some(existing) = state.tables.iter().find(|t| t.id == table.id) {
                if existing == &table {
                    return Ok(());
                }
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("table id collision for {}", table.id),
                });
            }

            if state
                .tables
                .iter()
                .any(|t| t.namespace_id == table.namespace_id && t.name == table.name)
            {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!(
                        "table '{}' already exists in namespace {}",
                        table.name, table.namespace_id
                    ),
                });
            }

            state.tables.push(table.clone());

            for column in columns {
                if column.table_id != table.id {
                    return Err(Tier1CompactionError::ProcessingError {
                        message: format!("column '{}' belongs to different table", column.id),
                    });
                }
                if state.columns.iter().any(|c| c.id == column.id) {
                    continue;
                }
                state.columns.push(column);
            }
        }
        CatalogDdlEvent::TableUpdated { table } => {
            let Some(existing) = state.tables.iter_mut().find(|t| t.id == table.id) else {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("table '{}' not found", table.id),
                });
            };

            if existing.namespace_id != table.namespace_id || existing.name != table.name {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("table identity mismatch for {}", table.id),
                });
            }

            if existing == &table {
                return Ok(());
            }

            *existing = table;
        }
        CatalogDdlEvent::TableDropped {
            table_id,
            namespace_id,
            table_name,
        } => {
            let index = state.tables.iter().position(|t| t.id == table_id);
            let Some(index) = index else {
                return Ok(());
            };
            let Some(existing) = state.tables.get(index) else {
                return Ok(());
            };
            if existing.namespace_id != namespace_id || existing.name != table_name {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("table identity mismatch for {table_id}"),
                });
            }
            state.tables.remove(index);
            state.columns.retain(|c| c.table_id != table_id);
        }
        CatalogDdlEvent::TableRenamed {
            table_id,
            namespace_id,
            old_name,
            new_name,
            updated_at,
        } => {
            let table_idx = state
                .tables
                .iter()
                .position(|t| t.id == table_id)
                .ok_or_else(|| Tier1CompactionError::ProcessingError {
                    message: format!("table '{table_id}' not found for rename"),
                })?;

            let existing = &state.tables[table_idx];

            if existing.namespace_id != namespace_id {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!(
                        "namespace mismatch for table {table_id}: expected {}, got {namespace_id}",
                        existing.namespace_id
                    ),
                });
            }

            if existing.name != old_name {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!(
                        "table name mismatch for {table_id}: expected '{}', got '{old_name}'",
                        existing.name
                    ),
                });
            }

            if existing.name == new_name {
                return Ok(());
            }

            if state
                .tables
                .iter()
                .any(|t| t.namespace_id == namespace_id && t.name == new_name)
            {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!(
                        "table '{new_name}' already exists in namespace {namespace_id}"
                    ),
                });
            }

            state.tables[table_idx].name.clone_from(&new_name);
            state.tables[table_idx].updated_at = updated_at;
        }
    }

    Ok(())
}

fn apply_catalog_event_v2(
    state: &mut crate::state::CatalogState,
    event: CatalogDdlEventV2,
) -> Result<(), Tier1CompactionError> {
    match event {
        CatalogDdlEventV2::CatalogCreated { catalog } => {
            if let Some(existing) = state.catalogs.iter().find(|c| c.id == catalog.id) {
                if existing == &catalog {
                    return Ok(());
                }
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("catalog id collision for {}", catalog.id),
                });
            }
            if state.catalogs.iter().any(|c| c.name == catalog.name) {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("catalog '{}' already exists", catalog.name),
                });
            }
            state.catalogs.push(catalog);
        }
    }

    Ok(())
}

fn apply_catalog_event_v3(
    state: &mut crate::state::CatalogState,
    event: CatalogDdlEventV3,
) -> Result<(), Tier1CompactionError> {
    match event {
        CatalogDdlEventV3::CatalogUpdated { catalog } => {
            let Some(existing) = state
                .catalogs
                .iter_mut()
                .find(|candidate| candidate.id == catalog.id)
            else {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("catalog '{}' not found", catalog.id),
                });
            };

            if existing.name != catalog.name {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("catalog identity mismatch for {}", catalog.id),
                });
            }

            if existing == &catalog {
                return Ok(());
            }

            *existing = catalog;
        }
        CatalogDdlEventV3::CatalogDeleted {
            catalog_id,
            catalog_name,
        } => {
            let index = state
                .catalogs
                .iter()
                .position(|catalog| catalog.id == catalog_id);

            let Some(index) = index else {
                return Ok(());
            };

            let Some(existing) = state.catalogs.get(index) else {
                return Ok(());
            };
            if existing.name != catalog_name {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("catalog name mismatch for {catalog_id}"),
                });
            }

            let default_catalog_id = state
                .catalogs
                .iter()
                .find(|catalog| catalog.name == "default")
                .map(|catalog| catalog.id.as_str());
            if state.namespaces.iter().any(|namespace| {
                namespace.catalog_id.as_deref().or(default_catalog_id) == Some(catalog_id.as_str())
            }) {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("catalog '{catalog_name}' contains schemas"),
                });
            }

            state.catalogs.remove(index);
        }
    }

    Ok(())
}

fn apply_catalog_event_v4(
    state: &mut crate::state::CatalogState,
    event: CatalogDdlEventV4,
) -> Result<(), Tier1CompactionError> {
    match event {
        CatalogDdlEventV4::CatalogRenamed { catalog, old_name } => {
            let Some(index) = state
                .catalogs
                .iter()
                .position(|candidate| candidate.id == catalog.id)
            else {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("catalog '{}' not found", catalog.id),
                });
            };
            let Some(existing) = state.catalogs.get(index) else {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("catalog '{}' not found", catalog.id),
                });
            };

            if existing.name != old_name {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("catalog identity mismatch for {}", catalog.id),
                });
            }

            if old_name != catalog.name
                && state
                    .catalogs
                    .iter()
                    .any(|candidate| candidate.id != catalog.id && candidate.name == catalog.name)
            {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("catalog '{}' already exists", catalog.name),
                });
            }

            if existing == &catalog {
                return Ok(());
            }

            let Some(existing) = state.catalogs.get_mut(index) else {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("catalog '{}' not found", catalog.id),
                });
            };
            *existing = catalog;
        }
        CatalogDdlEventV4::NamespaceRenamed {
            namespace,
            old_name,
        } => {
            let default_catalog_id = state
                .catalogs
                .iter()
                .find(|catalog| catalog.name == "default")
                .map(|catalog| catalog.id.as_str());
            let Some(index) = state
                .namespaces
                .iter()
                .position(|candidate| candidate.id == namespace.id)
            else {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("namespace '{}' not found", namespace.id),
                });
            };
            let Some(existing) = state.namespaces.get(index) else {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("namespace '{}' not found", namespace.id),
                });
            };

            if existing.name != old_name {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("namespace identity mismatch for {}", namespace.id),
                });
            }

            let target_catalog_id = namespace.catalog_id.as_deref().or(default_catalog_id);
            if old_name != namespace.name
                && state.namespaces.iter().any(|candidate| {
                    candidate.id != namespace.id
                        && candidate.name == namespace.name
                        && candidate.catalog_id.as_deref().or(default_catalog_id)
                            == target_catalog_id
                })
            {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("namespace '{}' already exists", namespace.name),
                });
            }

            if existing == &namespace {
                return Ok(());
            }

            let Some(existing) = state.namespaces.get_mut(index) else {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("namespace '{}' not found", namespace.id),
                });
            };
            *existing = namespace;
        }
    }

    Ok(())
}

#[allow(clippy::unnecessary_wraps)]
fn apply_lineage_event(
    state: &mut crate::state::LineageState,
    event: LineageDdlEvent,
) -> Result<(), Tier1CompactionError> {
    match event {
        LineageDdlEvent::EdgesAdded { edges } => {
            let mut existing_ids: HashSet<String> =
                state.edges.iter().map(|e| e.id.clone()).collect();
            for edge in edges {
                if existing_ids.insert(edge.id.clone()) {
                    state.edges.push(edge);
                }
            }
        }
    }

    Ok(())
}

fn build_search_state(catalog: &crate::state::CatalogState) -> crate::state::SearchState {
    let mut postings = Vec::new();

    for ns in &catalog.namespaces {
        append_tokens(&mut postings, "namespace", &ns.id, "name", 1.0, &ns.name);
        if let Some(description) = &ns.description {
            append_tokens(
                &mut postings,
                "namespace",
                &ns.id,
                "description",
                0.5,
                description,
            );
        }
    }

    for table in &catalog.tables {
        append_tokens(&mut postings, "table", &table.id, "name", 1.0, &table.name);
        if let Some(description) = &table.description {
            append_tokens(
                &mut postings,
                "table",
                &table.id,
                "description",
                0.5,
                description,
            );
        }
    }

    for column in &catalog.columns {
        append_tokens(
            &mut postings,
            "column",
            &column.id,
            "name",
            1.0,
            &column.name,
        );
        if let Some(description) = &column.description {
            append_tokens(
                &mut postings,
                "column",
                &column.id,
                "description",
                0.5,
                description,
            );
        }
    }

    crate::state::SearchState { postings }
}

fn append_tokens(
    postings: &mut Vec<SearchPostingRecord>,
    doc_type: &str,
    doc_id: &str,
    field: &str,
    score: f32,
    text: &str,
) {
    for token in text.split(|c: char| !c.is_ascii_alphanumeric() && c != '_') {
        if token.len() < 2 {
            continue;
        }
        postings.push(SearchPostingRecord {
            token: token.to_string(),
            token_norm: token.to_ascii_lowercase(),
            doc_type: doc_type.to_string(),
            doc_id: doc_id.to_string(),
            field: field.to_string(),
            score,
        });
    }
}

async fn revalidate_lock(
    lock: &DistributedLock<dyn StorageBackend>,
    fencing_token: u64,
) -> Result<(), Tier1CompactionError> {
    let lock_info =
        lock.read_lock_info()
            .await
            .map_err(|e| Tier1CompactionError::ProcessingError {
                message: format!("failed to read lock info: {e}"),
            })?;

    let Some(lock_info) = lock_info else {
        return Err(Tier1CompactionError::StaleFencingToken {
            expected: 0,
            provided: fencing_token,
        });
    };

    if lock_info.is_expired() || lock_info.sequence_number != fencing_token {
        return Err(Tier1CompactionError::StaleFencingToken {
            expected: lock_info.sequence_number,
            provided: fencing_token,
        });
    }

    Ok(())
}

fn next_commit_ulid(previous: Option<&str>) -> Result<String, Tier1CompactionError> {
    let candidate = Ulid::new();

    let Some(previous) = previous else {
        return Ok(candidate.to_string());
    };

    let previous =
        Ulid::from_string(previous).map_err(|e| Tier1CompactionError::ProcessingError {
            message: format!("invalid previous commit_ulid '{previous}': {e}"),
        })?;

    if candidate > previous {
        return Ok(candidate.to_string());
    }

    let next = previous
        .increment()
        .ok_or_else(|| Tier1CompactionError::ProcessingError {
            message: "commit_ulid overflow while generating monotonic successor".to_string(),
        })?;
    Ok(next.to_string())
}

async fn next_available_manifest_id(
    storage: &ScopedStorage,
    domain: CatalogDomain,
    previous_manifest_id: &str,
) -> Result<String, Tier1CompactionError> {
    let mut candidate = next_manifest_id(previous_manifest_id)
        .map_err(|message| Tier1CompactionError::ProcessingError { message })?;
    loop {
        let candidate_path = CatalogPaths::domain_manifest_snapshot(domain, &candidate);
        if storage
            .head_raw(&candidate_path)
            .await
            .map_err(map_publish_error)?
            .is_none()
        {
            return Ok(candidate);
        }
        candidate = next_manifest_id(&candidate)
            .map_err(|message| Tier1CompactionError::ProcessingError { message })?;
    }
}

async fn resolve_manifest_path(
    storage: &ScopedStorage,
    domain: CatalogDomain,
) -> CatalogResult<String> {
    let pointer_path = CatalogPaths::domain_manifest_pointer(domain);
    let pointer_bytes = storage.get_raw(&pointer_path).await?;
    let pointer: DomainManifestPointer =
        serde_json::from_slice(&pointer_bytes).map_err(|e| CatalogError::Serialization {
            message: format!("parse JSON at {pointer_path}: {e}"),
        })?;
    Ok(pointer.manifest_path)
}

fn map_processing_error<E: std::fmt::Display>(err: E) -> Tier1CompactionError {
    Tier1CompactionError::ProcessingError {
        message: err.to_string(),
    }
}

fn map_publish_error<E: std::fmt::Display>(err: E) -> Tier1CompactionError {
    Tier1CompactionError::PublishFailed {
        message: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_util::{ColumnRecord, NamespaceRecord, TableRecord};
    use crate::tier1_snapshot;
    use crate::tier1_writer::Tier1Writer;
    use arco_core::storage::{MemoryBackend, WritePrecondition};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn sync_compact_search_writes_snapshot() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await.expect("initialize");

        let now = Utc::now().timestamp_millis();
        let state = crate::state::CatalogState {
            catalogs: Vec::new(),
            namespaces: vec![NamespaceRecord {
                id: "ns-1".to_string(),
                catalog_id: None,
                name: "sales".to_string(),
                description: Some("Sales".to_string()),
                properties_json: None,
                storage_root: None,
                created_at: now,
                updated_at: now,
            }],
            tables: vec![TableRecord {
                id: "tbl-1".to_string(),
                namespace_id: "ns-1".to_string(),
                name: "orders".to_string(),
                description: Some("Orders".to_string()),
                location: None,
                format: None,
                table_type: None,
                properties_json: None,
                created_at: now,
                updated_at: now,
            }],
            columns: vec![ColumnRecord {
                id: "col-1".to_string(),
                table_id: "tbl-1".to_string(),
                name: "order_id".to_string(),
                data_type: "string".to_string(),
                is_nullable: false,
                ordinal: 0,
                description: None,
            }],
            commits: Vec::new(),
        };

        let snapshot = tier1_snapshot::write_catalog_snapshot(&storage, 1, &state)
            .await
            .expect("snapshot");

        let catalog_manifest = CatalogDomainManifest {
            manifest_id: crate::manifest::format_manifest_id(snapshot.version),
            epoch: 0,
            previous_manifest_path: Some("manifests/catalog/00000000000000000000.json".to_string()),
            writer_session_id: Some("tier1-compactor-test".to_string()),
            snapshot_version: snapshot.version,
            snapshot_path: snapshot.path.clone(),
            snapshot: Some(snapshot),
            watermark_event_id: None,
            last_commit_id: None,
            fencing_token: None,
            commit_ulid: None,
            parent_hash: None,
            updated_at: Utc::now(),
        };
        let catalog_bytes = serde_json::to_vec(&catalog_manifest).expect("serialize catalog");
        let catalog_pointer_bytes = storage
            .get_raw(&CatalogPaths::domain_manifest_pointer(
                CatalogDomain::Catalog,
            ))
            .await
            .expect("catalog pointer");
        let catalog_pointer: DomainManifestPointer =
            serde_json::from_slice(&catalog_pointer_bytes).expect("parse catalog pointer");
        storage
            .put_raw(
                &catalog_pointer.manifest_path,
                Bytes::from(catalog_bytes),
                WritePrecondition::None,
            )
            .await
            .expect("write catalog manifest");

        let lock_path = storage.lock(CatalogDomain::Search);
        let lock = DistributedLock::new(storage.backend().clone(), &lock_path);
        let guard = lock
            .acquire(Duration::from_secs(30), 1)
            .await
            .expect("lock");
        let fencing_token = guard.fencing_token().sequence();

        let compactor = Tier1Compactor::new(storage.clone());
        let result = compactor
            .sync_compact("search", Vec::new(), fencing_token)
            .await
            .expect("search compaction");

        let search_pointer_bytes = storage
            .get_raw(&CatalogPaths::domain_manifest_pointer(
                CatalogDomain::Search,
            ))
            .await
            .expect("search pointer");
        let search_pointer: DomainManifestPointer =
            serde_json::from_slice(&search_pointer_bytes).expect("parse search pointer");
        let search_bytes = storage
            .get_raw(&search_pointer.manifest_path)
            .await
            .expect("search manifest");
        let search_manifest: SearchManifest =
            serde_json::from_slice(&search_bytes).expect("parse search manifest");
        assert_eq!(search_manifest.snapshot_version, result.snapshot_version);
        assert!(search_manifest.snapshot.is_some());

        let pointer_bytes = storage
            .get_raw(&CatalogPaths::domain_manifest_pointer(
                CatalogDomain::Search,
            ))
            .await
            .expect("search pointer");
        let pointer: DomainManifestPointer =
            serde_json::from_slice(&pointer_bytes).expect("parse search pointer");
        assert_eq!(pointer.manifest_id, search_manifest.manifest_id);
        assert!(pointer.manifest_path.contains("manifests/search/"));

        let postings_path =
            StateKey::snapshot_file_in_dir(&search_manifest.base_path, "token_postings.parquet");
        let postings_bytes = storage
            .get_raw(postings_path.as_ref())
            .await
            .expect("postings");
        let postings =
            crate::parquet_util::read_search_postings(&postings_bytes).expect("read postings");
        assert!(!postings.is_empty());

        guard.release().await.expect("release");
    }

    #[tokio::test]
    async fn sync_compact_catalog_preserves_unpublished_next_version_orphans() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await.expect("initialize");

        let guard = writer
            .acquire_lock(Duration::from_secs(30), 1)
            .await
            .expect("lock");
        let event = CatalogDdlEvent::NamespaceCreated {
            namespace: NamespaceRecord {
                id: "ns-1".to_string(),
                catalog_id: None,
                name: "sales".to_string(),
                description: Some("Sales".to_string()),
                storage_root: None,
                properties_json: None,
                created_at: Utc::now().timestamp_millis(),
                updated_at: Utc::now().timestamp_millis(),
            },
        };
        let event_id = writer
            .append_ledger_event(&guard, CatalogDomain::Catalog, &event, "test")
            .await
            .expect("append event");
        let orphan_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, 1, "catalogs.parquet");
        storage
            .put_raw(
                &orphan_path,
                Bytes::from_static(b"crash orphan from unpublished attempt"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write orphan");

        let compactor = Tier1Compactor::new(storage.clone());
        let result = compactor
            .sync_compact(
                "catalog",
                vec![CatalogPaths::ledger_event(
                    CatalogDomain::Catalog,
                    &event_id.to_string(),
                )],
                guard.fencing_token().sequence(),
            )
            .await
            .expect("sync compact");

        assert_eq!(result.snapshot_version, 1);
        let manifest = writer.read_manifest().await.expect("manifest");
        assert_eq!(manifest.catalog.snapshot_version, 1);
        assert_ne!(
            manifest.catalog.snapshot_path,
            CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1),
            "new compaction attempts must write to attempt-unique snapshot paths"
        );
        storage
            .get_raw(&orphan_path)
            .await
            .expect("stale next-version orphan must not be deleted by a later lock holder");
        guard.release().await.expect("release");
    }

    #[tokio::test]
    async fn sync_compact_catalog_watermark_uses_max_processed_event_id() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await.expect("initialize");

        let guard = writer
            .acquire_lock(Duration::from_secs(30), 1)
            .await
            .expect("lock");
        let first_event = CatalogDdlEvent::NamespaceCreated {
            namespace: NamespaceRecord {
                id: "ns-1".to_string(),
                catalog_id: None,
                name: "sales".to_string(),
                description: Some("Sales".to_string()),
                storage_root: None,
                properties_json: None,
                created_at: Utc::now().timestamp_millis(),
                updated_at: Utc::now().timestamp_millis(),
            },
        };
        let first_id = writer
            .append_ledger_event(&guard, CatalogDomain::Catalog, &first_event, "test")
            .await
            .expect("append first event");
        let second_event = CatalogDdlEvent::NamespaceCreated {
            namespace: NamespaceRecord {
                id: "ns-2".to_string(),
                catalog_id: None,
                name: "support".to_string(),
                description: Some("Support".to_string()),
                storage_root: None,
                properties_json: None,
                created_at: Utc::now().timestamp_millis(),
                updated_at: Utc::now().timestamp_millis(),
            },
        };
        let second_id = writer
            .append_ledger_event(&guard, CatalogDomain::Catalog, &second_event, "test")
            .await
            .expect("append second event");
        let mut event_ids = [first_id.to_string(), second_id.to_string()];
        event_ids.sort();
        let max_event_id = event_ids[1].clone();

        let compactor = Tier1Compactor::new(storage.clone());
        compactor
            .sync_compact(
                "catalog",
                vec![
                    CatalogPaths::ledger_event(CatalogDomain::Catalog, &event_ids[1]),
                    CatalogPaths::ledger_event(CatalogDomain::Catalog, &event_ids[0]),
                ],
                guard.fencing_token().sequence(),
            )
            .await
            .expect("sync compact");

        let manifest = writer.read_manifest().await.expect("manifest");
        assert_eq!(
            manifest.catalog.watermark_event_id.as_deref(),
            Some(max_event_id.as_str()),
            "watermark must be the max processed event ID, not the last input path"
        );
        guard.release().await.expect("release");
    }
}
