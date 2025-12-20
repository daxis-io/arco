//! Tier-1 synchronous compaction for DDL events (ADR-018).

use std::collections::HashSet;

use bytes::Bytes;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use thiserror::Error;
use ulid::Ulid;

use arco_core::lock::DistributedLock;
use arco_core::publish::Publisher;
use arco_core::storage::{StorageBackend, WriteResult};
use arco_core::storage_keys::{CommitKey, ManifestKey};
use arco_core::storage_traits::CommitPutStore;
use arco_core::{CatalogDomain, CatalogEvent, CatalogEventPayload, CatalogPaths, ScopedStorage};

use crate::error::{CatalogError, Result as CatalogResult};
use crate::manifest::{
    compute_manifest_hash, CatalogDomainManifest, CommitRecord, LineageManifest, RootManifest,
};
use crate::sync_compact_permit_issuer;
use crate::tier1_events::{CatalogDdlEvent, LineageDdlEvent};
use crate::tier1_snapshot;
use crate::tier1_state;

/// Result of a Tier-1 sync compaction run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tier1CompactionResult {
    /// New manifest version after compaction.
    pub manifest_version: String,
    /// Commit ULID for audit trail.
    pub commit_ulid: String,
    /// Number of events processed.
    pub events_processed: usize,
    /// Snapshot version after compaction.
    pub snapshot_version: u64,
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
                CatalogError::PreconditionFailed {
                    message: format!(
                        "stale fencing token: expected {expected}, got {provided}"
                    ),
                }
            }
            Tier1CompactionError::UnsupportedDomain { domain } => CatalogError::Validation {
                message: format!("unsupported domain for sync compaction: {domain}"),
            },
            Tier1CompactionError::NotImplemented { domain, message } => {
                CatalogError::InvariantViolation {
                    message: format!("sync compaction not implemented for {domain}: {message}"),
                }
            }
            Tier1CompactionError::EventReadError { path, message } => CatalogError::Storage {
                message: format!("failed to read event '{path}': {message}"),
            },
            Tier1CompactionError::ProcessingError { message } => {
                CatalogError::InvariantViolation { message }
            }
            Tier1CompactionError::PublishFailed { message } => CatalogError::CasFailed { message },
        }
    }
}

/// Compactor for Tier-1 DDL operations using explicit event paths.
pub struct Tier1Compactor {
    storage: ScopedStorage,
    cas_max_retries: u32,
}

impl Tier1Compactor {
    /// Creates a new Tier-1 compactor.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self {
            storage,
            cas_max_retries: 5,
        }
    }

    /// Handles a synchronous compaction request.
    pub async fn sync_compact(
        &self,
        domain: &str,
        event_paths: Vec<String>,
        fencing_token: u64,
    ) -> Result<Tier1CompactionResult, Tier1CompactionError> {
        let domain = parse_domain(domain)?;

        // Reject unsupported domains early (before lock validation).
        // Executions is async-only; search is not yet implemented.
        match domain {
            CatalogDomain::Executions => {
                return Err(Tier1CompactionError::UnsupportedDomain {
                    domain: domain.as_str().to_string(),
                });
            }
            CatalogDomain::Search => {
                return Err(Tier1CompactionError::NotImplemented {
                    domain: domain.as_str().to_string(),
                    message: "search compaction is not yet available".to_string(),
                });
            }
            CatalogDomain::Catalog | CatalogDomain::Lineage => {}
        }

        let mut event_paths = validate_event_paths(domain, event_paths)?;

        if event_paths.is_empty() {
            return Err(Tier1CompactionError::ProcessingError {
                message: "no event paths provided".to_string(),
            });
        }

        event_paths.sort();

        let lock_path = self.storage.lock(domain);
        let lock = DistributedLock::new(self.storage.backend().clone(), &lock_path);
        let lock_info = lock
            .read_lock_info()
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
            // Early rejection cases handled above - these are unreachable.
            CatalogDomain::Search | CatalogDomain::Executions => unreachable!(),
        }
    }

    async fn sync_compact_catalog(
        &self,
        event_paths: Vec<String>,
        lock: &DistributedLock<dyn StorageBackend>,
        issuer: &arco_core::publish::PermitIssuer,
        fencing_token: u64,
    ) -> Result<Tier1CompactionResult, Tier1CompactionError> {
        let events_processed = event_paths.len();
        let publisher = Publisher::new(&self.storage);

        for attempt in 1..=self.cas_max_retries {
            let mut root: RootManifest = read_json(&self.storage, CatalogPaths::ROOT_MANIFEST)
                .await
                .map_err(map_processing_error)?;
            root.normalize_paths();

            let meta = self
                .storage
                .head_raw(&root.catalog_manifest_path)
                .await
                .map_err(map_processing_error)?
                .ok_or_else(|| Tier1CompactionError::ProcessingError {
                    message: format!(
                        "missing catalog manifest at {}",
                        root.catalog_manifest_path
                    ),
                })?;

            let prev_bytes = self
                .storage
                .get_raw(&root.catalog_manifest_path)
                .await
                .map_err(map_processing_error)?;
            let prev_raw_hash = compute_manifest_hash(&prev_bytes);
            let mut manifest: CatalogDomainManifest =
                serde_json::from_slice(&prev_bytes).map_err(map_processing_error)?;
            let prev_manifest = manifest.clone();

            let mut state = tier1_state::load_catalog_state(&self.storage, &manifest.snapshot_path)
                .await
                .map_err(map_processing_error)?;

            for path in &event_paths {
                let event = read_catalog_event(&self.storage, path).await?;
                apply_catalog_event(&mut state, event)?;
            }

            let next_version = manifest.snapshot_version + 1;
            let snapshot = tier1_snapshot::write_catalog_snapshot(&self.storage, next_version, &state)
                .await
                .map_err(map_processing_error)?;

            let commit_ulid = next_commit_ulid(prev_manifest.commit_ulid.as_deref())?;

            manifest.snapshot_version = snapshot.version;
            manifest.snapshot_path.clone_from(&snapshot.path);
            manifest.snapshot = Some(snapshot.clone());
            manifest.updated_at = Utc::now();
            manifest.parent_hash = Some(prev_raw_hash.clone());
            manifest.fencing_token = Some(fencing_token);
            manifest.commit_ulid = Some(commit_ulid.clone());

            manifest
                .validate_succession(&prev_manifest, &prev_raw_hash)
                .map_err(|message| Tier1CompactionError::ProcessingError { message })?;

            let commit = build_commit_record(&self.storage, &prev_manifest, &manifest, &commit_ulid)
                .await?;
            manifest.last_commit_id = Some(commit.commit_id.clone());

            let bytes = serde_json::to_vec(&manifest).map_err(map_processing_error)?;
            let permit = issuer.issue_permit_with_commit_ulid(
                CatalogDomain::Catalog.as_str(),
                meta.version.clone(),
                commit_ulid.clone(),
            );

            revalidate_lock(lock, fencing_token).await?;

            match publisher
                .publish(permit, &ManifestKey::domain(CatalogDomain::Catalog), Bytes::from(bytes))
                .await
                .map_err(map_publish_error)?
            {
                WriteResult::Success { version } => {
                    persist_commit_record(&self.storage, CatalogDomain::Catalog, &commit).await?;
                    return Ok(Tier1CompactionResult {
                        manifest_version: version,
                        commit_ulid,
                        events_processed,
                        snapshot_version: manifest.snapshot_version,
                    });
                }
                WriteResult::PreconditionFailed { .. } => {
                    if attempt == self.cas_max_retries {
                        return Err(Tier1CompactionError::PublishFailed {
                            message: "manifest update lost CAS race after max retries".to_string(),
                        });
                    }
                    continue;
                }
            }
        }

        Err(Tier1CompactionError::PublishFailed {
            message: "manifest update lost CAS race after max retries".to_string(),
        })
    }

    async fn sync_compact_lineage(
        &self,
        event_paths: Vec<String>,
        lock: &DistributedLock<dyn StorageBackend>,
        issuer: &arco_core::publish::PermitIssuer,
        fencing_token: u64,
    ) -> Result<Tier1CompactionResult, Tier1CompactionError> {
        let events_processed = event_paths.len();
        let publisher = Publisher::new(&self.storage);

        for attempt in 1..=self.cas_max_retries {
            let mut root: RootManifest = read_json(&self.storage, CatalogPaths::ROOT_MANIFEST)
                .await
                .map_err(map_processing_error)?;
            root.normalize_paths();

            let meta = self
                .storage
                .head_raw(&root.lineage_manifest_path)
                .await
                .map_err(map_processing_error)?
                .ok_or_else(|| Tier1CompactionError::ProcessingError {
                    message: format!(
                        "missing lineage manifest at {}",
                        root.lineage_manifest_path
                    ),
                })?;

            let prev_bytes = self
                .storage
                .get_raw(&root.lineage_manifest_path)
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
            let snapshot =
                tier1_snapshot::write_lineage_snapshot(&self.storage, next_version, &state)
                    .await
                    .map_err(map_processing_error)?;

            let commit_ulid = next_commit_ulid(prev_manifest.commit_ulid.as_deref())?;

            manifest.snapshot_version = snapshot.version;
            manifest.edges_path.clone_from(&snapshot.path);
            manifest.snapshot = Some(snapshot.clone());
            manifest.updated_at = Utc::now();
            manifest.parent_hash = Some(prev_raw_hash.clone());
            manifest.fencing_token = Some(fencing_token);
            manifest.commit_ulid = Some(commit_ulid.clone());

            manifest
                .validate_succession(&prev_manifest, &prev_raw_hash)
                .map_err(|message| Tier1CompactionError::ProcessingError { message })?;

            let commit =
                build_lineage_commit_record(&self.storage, &prev_manifest, &manifest, &commit_ulid)
                    .await?;
            manifest.last_commit_id = Some(commit.commit_id.clone());

            let bytes = serde_json::to_vec(&manifest).map_err(map_processing_error)?;
            let permit = issuer.issue_permit_with_commit_ulid(
                CatalogDomain::Lineage.as_str(),
                meta.version.clone(),
                commit_ulid.clone(),
            );

            revalidate_lock(lock, fencing_token).await?;

            match publisher
                .publish(permit, &ManifestKey::domain(CatalogDomain::Lineage), Bytes::from(bytes))
                .await
                .map_err(map_publish_error)?
            {
                WriteResult::Success { version } => {
                    persist_commit_record(&self.storage, CatalogDomain::Lineage, &commit).await?;
                    return Ok(Tier1CompactionResult {
                        manifest_version: version,
                        commit_ulid,
                        events_processed,
                        snapshot_version: manifest.snapshot_version,
                    });
                }
                WriteResult::PreconditionFailed { .. } => {
                    if attempt == self.cas_max_retries {
                        return Err(Tier1CompactionError::PublishFailed {
                            message: "lineage manifest update lost CAS race after max retries"
                                .to_string(),
                        });
                    }
                    continue;
                }
            }
        }

        Err(Tier1CompactionError::PublishFailed {
            message: "lineage manifest update lost CAS race after max retries".to_string(),
        })
    }
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
                message: format!("event path '{path}' is outside {}", prefix),
            });
        }
        if !path.ends_with(".json") {
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

async fn read_catalog_event(
    storage: &ScopedStorage,
    path: &str,
) -> Result<CatalogDdlEvent, Tier1CompactionError> {
    let data = storage
        .get_raw(path)
        .await
        .map_err(|e| Tier1CompactionError::EventReadError {
            path: path.to_string(),
            message: e.to_string(),
        })?;

    let envelope: CatalogEvent<CatalogDdlEvent> =
        serde_json::from_slice(&data).map_err(map_processing_error)?;

    if envelope.event_type != CatalogDdlEvent::EVENT_TYPE
        || envelope.event_version != CatalogDdlEvent::EVENT_VERSION
    {
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

    Ok(envelope.payload)
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

fn apply_catalog_event(state: &mut crate::state::CatalogState, event: CatalogDdlEvent) -> Result<(), Tier1CompactionError> {
    match event {
        CatalogDdlEvent::NamespaceCreated { namespace } => {
            if let Some(existing) = state.namespaces.iter().find(|ns| ns.id == namespace.id) {
                if existing == &namespace {
                    return Ok(());
                }
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!(
                        "namespace id collision for {}",
                        namespace.id
                    ),
                });
            }
            if state.namespaces.iter().any(|ns| ns.name == namespace.name) {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!(
                        "namespace '{}' already exists",
                        namespace.name
                    ),
                });
            }
            state.namespaces.push(namespace);
        }
        CatalogDdlEvent::NamespaceDeleted {
            namespace_id,
            namespace_name,
        } => {
            let index = state
                .namespaces
                .iter()
                .position(|ns| ns.id == namespace_id);

            let Some(index) = index else {
                return Ok(());
            };

            let existing = &state.namespaces[index];
            if existing.name != namespace_name {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!(
                        "namespace name mismatch for {}",
                        namespace_id
                    ),
                });
            }
            if state
                .tables
                .iter()
                .any(|table| table.namespace_id == namespace_id)
            {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!(
                        "namespace '{}' contains tables",
                        namespace_name
                    ),
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
                    message: format!(
                        "namespace '{}' not found",
                        table.namespace_id
                    ),
                });
            }

            if let Some(existing) = state.tables.iter().find(|t| t.id == table.id) {
                if existing == &table {
                    return Ok(());
                }
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!(
                        "table id collision for {}",
                        table.id
                    ),
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
                        message: format!(
                            "column '{}' belongs to different table",
                            column.id
                        ),
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
            let existing = &state.tables[index];
            if existing.namespace_id != namespace_id || existing.name != table_name {
                return Err(Tier1CompactionError::ProcessingError {
                    message: format!("table identity mismatch for {}", table_id),
                });
            }
            state.tables.remove(index);
            state.columns.retain(|c| c.table_id != table_id);
        }
    }

    Ok(())
}

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

async fn revalidate_lock(
    lock: &DistributedLock<dyn StorageBackend>,
    fencing_token: u64,
) -> Result<(), Tier1CompactionError> {
    let lock_info = lock
        .read_lock_info()
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

async fn persist_commit_record(
    storage: &ScopedStorage,
    domain: CatalogDomain,
    commit: &CommitRecord,
) -> Result<(), Tier1CompactionError> {
    let key = CommitKey::record(domain, &commit.commit_id);
    let bytes = serde_json::to_vec(commit).map_err(map_processing_error)?;

    match storage
        .put_commit(&key, Bytes::from(bytes))
        .await
        .map_err(map_processing_error)?
    {
        WriteResult::Success { .. } => Ok(()),
        WriteResult::PreconditionFailed { .. } => Err(Tier1CompactionError::PublishFailed {
            message: format!("commit already exists: {}", commit.commit_id),
        }),
    }
}

async fn build_commit_record(
    storage: &ScopedStorage,
    prev: &CatalogDomainManifest,
    next: &CatalogDomainManifest,
    commit_id: &str,
) -> Result<CommitRecord, Tier1CompactionError> {
    let payload_hash = sha256_prefixed(&serde_json::to_vec(next).map_err(map_processing_error)?);
    let prev_commit_id = prev.last_commit_id.clone();

    let prev_commit_hash = match &prev_commit_id {
        Some(id) => {
            let path = CatalogPaths::commit(CatalogDomain::Catalog, id);
            match storage.get_raw(&path).await {
                Ok(bytes) => {
                    let record: CommitRecord =
                        serde_json::from_slice(&bytes).map_err(map_processing_error)?;
                    Some(record.compute_hash())
                }
                Err(arco_core::Error::NotFound(_)) => None,
                Err(e) => {
                    return Err(Tier1CompactionError::ProcessingError {
                        message: format!("failed to read commit '{path}': {e}"),
                    })
                }
            }
        }
        None => None,
    };

    Ok(CommitRecord {
        commit_id: commit_id.to_string(),
        prev_commit_id,
        prev_commit_hash,
        operation: "SyncCompact".into(),
        payload_hash,
        created_at: Utc::now(),
    })
}

async fn build_lineage_commit_record(
    storage: &ScopedStorage,
    prev: &LineageManifest,
    next: &LineageManifest,
    commit_id: &str,
) -> Result<CommitRecord, Tier1CompactionError> {
    let payload_hash = sha256_prefixed(&serde_json::to_vec(next).map_err(map_processing_error)?);
    let prev_commit_id = prev.last_commit_id.clone();

    let prev_commit_hash = match &prev_commit_id {
        Some(id) => {
            let path = CatalogPaths::commit(CatalogDomain::Lineage, id);
            match storage.get_raw(&path).await {
                Ok(bytes) => {
                    let record: CommitRecord =
                        serde_json::from_slice(&bytes).map_err(map_processing_error)?;
                    Some(record.compute_hash())
                }
                Err(arco_core::Error::NotFound(_)) => None,
                Err(e) => {
                    return Err(Tier1CompactionError::ProcessingError {
                        message: format!("failed to read commit '{path}': {e}"),
                    })
                }
            }
        }
        None => None,
    };

    Ok(CommitRecord {
        commit_id: commit_id.to_string(),
        prev_commit_id,
        prev_commit_hash,
        operation: "SyncCompact".into(),
        payload_hash,
        created_at: Utc::now(),
    })
}

fn next_commit_ulid(previous: Option<&str>) -> Result<String, Tier1CompactionError> {
    let candidate = Ulid::new();

    let Some(previous) = previous else {
        return Ok(candidate.to_string());
    };

    let previous = Ulid::from_string(previous).map_err(|e| Tier1CompactionError::ProcessingError {
        message: format!("invalid previous commit_ulid '{previous}': {e}"),
    })?;

    if candidate > previous {
        return Ok(candidate.to_string());
    }

    let next = previous.increment().ok_or_else(|| Tier1CompactionError::ProcessingError {
        message: "commit_ulid overflow while generating monotonic successor".to_string(),
    })?;
    Ok(next.to_string())
}

async fn read_json<T: serde::de::DeserializeOwned>(
    storage: &ScopedStorage,
    path: &str,
) -> CatalogResult<T> {
    let bytes = storage.get_raw(path).await?;
    serde_json::from_slice(&bytes).map_err(|e| CatalogError::Serialization {
        message: format!("parse JSON at {path}: {e}"),
    })
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let hash = sha2::Sha256::digest(bytes);
    format!("sha256:{}", hex::encode(hash))
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
