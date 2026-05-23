//! Pointer-publication planning for metastore projections.

use std::sync::Arc;
use std::sync::RwLock;
use std::time::Instant;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use arco_core::ScopedStorage;
use arco_core::storage::{ObjectMeta, WritePrecondition, WriteResult};

use crate::error::{CatalogError, Result};
use crate::metrics;
use crate::storage_governance::StorageGovernanceState;

use super::ledger::{MetastoreLedger, MetastoreLedgerWatermark};
use super::projections::{
    ProjectionSet, STORAGE_GOVERNANCE_PROJECTION, STORAGE_GOVERNANCE_SCHEMA_VERSION,
    read_metastore_object_rows,
};

const METASTORE_PROJECTION_POINTER: &str = "manifests/metastore_projection.pointer.json";
const METASTORE_PROJECTION_MANIFEST_PREFIX: &str = "manifests/metastore_projection/";

/// Outcome of the pointer compare-and-swap step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointerPublishResult {
    /// Pointer was published.
    Published,
    /// Pointer CAS failed and readers must remain on the previous set.
    CasFailed,
}

/// Projection set visible through a successfully published pointer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedProjectionSet {
    /// Immutable manifest identifier.
    pub manifest_id: String,
    /// Ledger watermark event ID.
    pub ledger_watermark: String,
    /// Projection files in this set.
    pub projections: ProjectionSet,
}

impl PublishedProjectionSet {
    /// Creates an empty visible projection set.
    #[must_use]
    pub fn empty(manifest_id: impl Into<String>, ledger_watermark: impl Into<String>) -> Self {
        Self {
            manifest_id: manifest_id.into(),
            ledger_watermark: ledger_watermark.into(),
            projections: ProjectionSet { files: Vec::new() },
        }
    }

    /// Creates a visible projection set from built projections.
    #[must_use]
    pub fn new(
        manifest_id: impl Into<String>,
        ledger_watermark: impl Into<String>,
        projections: ProjectionSet,
    ) -> Self {
        Self {
            manifest_id: manifest_id.into(),
            ledger_watermark: ledger_watermark.into(),
            projections,
        }
    }
}

/// Selects the reader-visible projection set after pointer publication.
///
/// This function models the all-or-nothing publication boundary: failed pointer
/// movement leaves readers on the previous complete set.
#[must_use]
pub fn complete_pointer_publication(
    previous: PublishedProjectionSet,
    candidate: PublishedProjectionSet,
    result: PointerPublishResult,
) -> PublishedProjectionSet {
    match result {
        PointerPublishResult::Published => candidate,
        PointerPublishResult::CasFailed => previous,
    }
}

/// Published metastore projection manifest pointer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetastoreProjectionPointer {
    /// Immutable manifest path.
    pub manifest_path: String,
}

/// Published metastore projection manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetastoreProjectionManifest {
    /// Immutable manifest identifier.
    pub manifest_id: String,
    /// Ledger watermark event ID visible in the projection files.
    pub ledger_watermark: String,
    /// Ledger watermark sequence visible in the projection files.
    pub ledger_watermark_sequence: u64,
    /// Files included in this projection set.
    pub files: Vec<MetastoreProjectionFileManifest>,
    /// Publication timestamp.
    pub published_at: DateTime<Utc>,
}

/// Published projection file metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetastoreProjectionFileManifest {
    /// Projection file name.
    pub file_name: String,
    /// Projection file path.
    pub path: String,
    /// Projection schema version.
    pub schema_version: i32,
    /// Row count.
    pub row_count: u64,
}

/// Published storage-governance projection state.
#[derive(Debug, Clone)]
pub struct PublishedStorageGovernance {
    /// Published storage-governance state.
    pub state: StorageGovernanceState,
    /// Published ledger watermark event ID.
    pub ledger_watermark: String,
}

/// Cache for published storage-governance projection state.
#[derive(Debug, Default)]
pub struct PublishedStorageGovernanceCache {
    current: RwLock<Option<PublishedStorageGovernanceCacheEntry>>,
    refresh: tokio::sync::Mutex<()>,
}

#[derive(Debug)]
struct PublishedStorageGovernanceCacheEntry {
    identity: PublishedStorageGovernanceCacheIdentity,
    value: Arc<PublishedStorageGovernance>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublishedStorageGovernanceCacheIdentity {
    tenant_id: String,
    workspace_id: String,
    manifest_id: String,
    ledger_watermark: String,
    ledger_watermark_sequence: u64,
    files: Vec<MetastoreProjectionFileManifest>,
    storage_governance_object: Option<PublishedStorageGovernanceObjectIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublishedStorageGovernanceObjectIdentity {
    path: String,
    size: u64,
    version: String,
    last_modified: Option<DateTime<Utc>>,
    etag: Option<String>,
}

impl PublishedStorageGovernanceCache {
    /// Loads the published storage-governance projection, reusing cached state
    /// only after revalidating pointer, manifest, and latest-ledger freshness.
    ///
    /// # Errors
    ///
    /// Returns `RequestFailed(503)` when the published projection is missing,
    /// stale, unsupported, or corrupt.
    pub async fn load(&self, storage: &ScopedStorage) -> Result<Arc<PublishedStorageGovernance>> {
        let manifest = load_projection_manifest(storage).await?;
        let latest = MetastoreLedger::new(storage.clone())
            .latest_watermark()
            .await?;
        validate_storage_governance_manifest_freshness(&manifest, latest.as_ref())?;
        let identity = storage_governance_cache_identity(storage, &manifest).await?;

        if let Some(current) = self
            .current
            .read()
            .map_err(|_| CatalogError::InvariantViolation {
                message: "storage governance cache lock poisoned".to_string(),
            })?
            .as_ref()
        {
            if current.identity == identity {
                metrics::inc_storage_governance_cache_hit();
                return Ok(Arc::clone(&current.value));
            }
        }

        let _guard = self.refresh.lock().await;
        let manifest = load_projection_manifest(storage).await?;
        let latest = MetastoreLedger::new(storage.clone())
            .latest_watermark()
            .await?;
        validate_storage_governance_manifest_freshness(&manifest, latest.as_ref())?;
        let identity = storage_governance_cache_identity(storage, &manifest).await?;
        if let Some(current) = self
            .current
            .read()
            .map_err(|_| CatalogError::InvariantViolation {
                message: "storage governance cache lock poisoned".to_string(),
            })?
            .as_ref()
        {
            if current.identity == identity {
                metrics::inc_storage_governance_cache_hit();
                return Ok(Arc::clone(&current.value));
            }
        }

        metrics::inc_storage_governance_cache_miss();
        let refresh_start = Instant::now();
        let loaded =
            Arc::new(load_published_storage_governance_from_manifest(storage, manifest).await?);
        metrics::record_storage_governance_refresh(refresh_start.elapsed().as_secs_f64());
        *self
            .current
            .write()
            .map_err(|_| CatalogError::InvariantViolation {
                message: "storage governance cache lock poisoned".to_string(),
            })? = Some(PublishedStorageGovernanceCacheEntry {
            identity,
            value: Arc::clone(&loaded),
        });
        Ok(loaded)
    }
}

/// Publishes a built metastore projection set behind a pointer.
///
/// # Errors
///
/// Returns an error if projection files, manifest, or pointer cannot be written.
pub async fn publish_metastore_projection_set(
    storage: &ScopedStorage,
    projection_set: &ProjectionSet,
    ledger_watermark_sequence: u64,
) -> Result<MetastoreProjectionManifest> {
    let manifest_id = format!("{ledger_watermark_sequence:020}");
    let snapshot_dir = format!("snapshots/metastore/v{ledger_watermark_sequence}/");
    let mut files = Vec::new();
    let mut projection_writes = Vec::new();
    let ledger_watermark = projection_set
        .files
        .first()
        .map_or_else(|| "empty".to_string(), |file| file.ledger_watermark.clone());

    for file in &projection_set.files {
        let bytes = file.write_parquet()?;
        let path = format!("{snapshot_dir}{}", file.file_name);
        projection_writes.push((path.clone(), bytes));
        files.push(MetastoreProjectionFileManifest {
            file_name: file.file_name.to_string(),
            path,
            schema_version: file.schema_version,
            row_count: file.rows.len() as u64,
        });
    }

    let manifest = MetastoreProjectionManifest {
        manifest_id: manifest_id.clone(),
        ledger_watermark,
        ledger_watermark_sequence,
        files,
        published_at: Utc::now(),
    };
    let manifest_path = format!("{METASTORE_PROJECTION_MANIFEST_PREFIX}{manifest_id}.json");
    let current = load_current_projection_pointer(storage).await?;
    let pointer_precondition = match current.as_ref() {
        Some(current) if current.manifest.ledger_watermark_sequence > ledger_watermark_sequence => {
            return Err(CatalogError::PreconditionFailed {
                message: format!(
                    "published metastore projection sequence {} is newer than candidate {}",
                    current.manifest.ledger_watermark_sequence, ledger_watermark_sequence
                ),
            });
        }
        Some(current)
            if current.manifest.ledger_watermark_sequence == ledger_watermark_sequence =>
        {
            if current.manifest_path == manifest_path
                && manifest_contents_match(&current.manifest, &manifest)
            {
                return Ok(manifest);
            }
            return Err(CatalogError::PreconditionFailed {
                message: format!(
                    "published metastore projection sequence {ledger_watermark_sequence} already has different content"
                ),
            });
        }
        Some(current) => WritePrecondition::MatchesVersion(current.pointer_version.clone()),
        None => WritePrecondition::DoesNotExist,
    };

    for (path, bytes) in projection_writes {
        put_if_absent(storage, &path, bytes).await?;
    }

    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).map_err(|err| CatalogError::Serialization {
            message: format!("failed to serialize metastore projection manifest: {err}"),
        })?;
    put_manifest_if_absent(
        storage,
        &manifest_path,
        &manifest,
        Bytes::from(manifest_bytes),
    )
    .await?;

    let pointer_bytes = serde_json::to_vec_pretty(&MetastoreProjectionPointer { manifest_path })
        .map_err(|err| CatalogError::Serialization {
            message: format!("failed to serialize metastore projection pointer: {err}"),
        })?;
    storage
        .put_raw(
            METASTORE_PROJECTION_POINTER,
            Bytes::from(pointer_bytes),
            pointer_precondition,
        )
        .await
        .map_err(CatalogError::from)
        .and_then(|result| match result {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { .. } => Err(CatalogError::PreconditionFailed {
                message: "metastore projection pointer changed during publication".to_string(),
            }),
        })?;

    Ok(manifest)
}

/// Loads the published storage-governance projection for enforcement.
///
/// # Errors
///
/// Returns `RequestFailed(503)` when the published projection is missing,
/// stale, unsupported, or corrupt.
pub async fn load_published_storage_governance(
    storage: &ScopedStorage,
) -> Result<PublishedStorageGovernance> {
    let manifest = load_projection_manifest(storage).await?;
    let latest = MetastoreLedger::new(storage.clone())
        .latest_watermark()
        .await?;
    validate_storage_governance_manifest_freshness(&manifest, latest.as_ref())?;

    load_published_storage_governance_from_manifest(storage, manifest).await
}

fn validate_storage_governance_manifest_freshness(
    manifest: &MetastoreProjectionManifest,
    latest: Option<&MetastoreLedgerWatermark>,
) -> Result<()> {
    match latest {
        Some(latest)
            if manifest.ledger_watermark_sequence == latest.sequence
                && manifest.ledger_watermark == latest.event_id => {}
        Some(_) => {
            return Err(projection_unavailable(
                "storage_governance_projection_stale",
            ));
        }
        None => {
            if manifest.ledger_watermark_sequence == 0
                && manifest.ledger_watermark == "empty"
                && manifest.files.is_empty()
            {
                return Ok(());
            }
            return Err(projection_unavailable(
                "storage_governance_projection_stale",
            ));
        }
    }
    Ok(())
}

async fn load_published_storage_governance_from_manifest(
    storage: &ScopedStorage,
    manifest: MetastoreProjectionManifest,
) -> Result<PublishedStorageGovernance> {
    let Some(file) = validate_storage_governance_projection_file(&manifest)? else {
        return Ok(PublishedStorageGovernance {
            state: StorageGovernanceState::default(),
            ledger_watermark: manifest.ledger_watermark,
        });
    };

    let bytes = storage
        .get_raw(&file.path)
        .await
        .map_err(|_| projection_unavailable("storage_governance_projection_unavailable"))?;
    let rows = read_metastore_object_rows(&bytes)
        .map_err(|_| projection_unavailable("storage_governance_projection_unavailable"))?;
    if rows.len() as u64 != file.row_count {
        return Err(projection_unavailable(
            "storage_governance_projection_unsupported",
        ));
    }
    if rows.iter().any(|row| {
        row.schema_version != STORAGE_GOVERNANCE_SCHEMA_VERSION
            || row.ledger_watermark != manifest.ledger_watermark
    }) {
        return Err(projection_unavailable(
            "storage_governance_projection_unsupported",
        ));
    }
    let state = StorageGovernanceState::from_projection_rows(&rows)
        .map_err(|_| projection_unavailable("storage_governance_projection_unavailable"))?;

    Ok(PublishedStorageGovernance {
        state,
        ledger_watermark: manifest.ledger_watermark,
    })
}

async fn load_projection_manifest(storage: &ScopedStorage) -> Result<MetastoreProjectionManifest> {
    let pointer_bytes = storage
        .get_raw(METASTORE_PROJECTION_POINTER)
        .await
        .map_err(|_| projection_unavailable("storage_governance_projection_unavailable"))?;
    let pointer = serde_json::from_slice::<MetastoreProjectionPointer>(&pointer_bytes)
        .map_err(|_| projection_unavailable("storage_governance_projection_unavailable"))?;
    let manifest_bytes = storage
        .get_raw(&pointer.manifest_path)
        .await
        .map_err(|_| projection_unavailable("storage_governance_projection_unavailable"))?;
    serde_json::from_slice::<MetastoreProjectionManifest>(&manifest_bytes)
        .map_err(|_| projection_unavailable("storage_governance_projection_unavailable"))
}

struct CurrentProjectionPointer {
    manifest_path: String,
    manifest: MetastoreProjectionManifest,
    pointer_version: String,
}

async fn load_current_projection_pointer(
    storage: &ScopedStorage,
) -> Result<Option<CurrentProjectionPointer>> {
    let Some(meta) = storage
        .head_raw(METASTORE_PROJECTION_POINTER)
        .await
        .map_err(CatalogError::from)?
    else {
        return Ok(None);
    };
    let pointer_bytes = storage
        .get_raw(METASTORE_PROJECTION_POINTER)
        .await
        .map_err(CatalogError::from)?;
    let pointer =
        serde_json::from_slice::<MetastoreProjectionPointer>(&pointer_bytes).map_err(|err| {
            CatalogError::Serialization {
                message: format!("failed to deserialize metastore projection pointer: {err}"),
            }
        })?;
    let manifest_bytes = storage
        .get_raw(&pointer.manifest_path)
        .await
        .map_err(CatalogError::from)?;
    let manifest =
        serde_json::from_slice::<MetastoreProjectionManifest>(&manifest_bytes).map_err(|err| {
            CatalogError::Serialization {
                message: format!("failed to deserialize metastore projection manifest: {err}"),
            }
        })?;

    Ok(Some(CurrentProjectionPointer {
        manifest_path: pointer.manifest_path,
        manifest,
        pointer_version: meta.version,
    }))
}

async fn put_if_absent(storage: &ScopedStorage, path: &str, bytes: Bytes) -> Result<()> {
    match storage
        .put_raw(path, bytes.clone(), WritePrecondition::DoesNotExist)
        .await
        .map_err(CatalogError::from)?
    {
        WriteResult::Success { .. } => Ok(()),
        WriteResult::PreconditionFailed { .. } => {
            let existing = storage.get_raw(path).await.map_err(CatalogError::from)?;
            if existing.as_ref() == bytes.as_ref() {
                Ok(())
            } else {
                Err(CatalogError::PreconditionFailed {
                    message: format!("published projection path already exists: {path}"),
                })
            }
        }
    }
}

async fn put_manifest_if_absent(
    storage: &ScopedStorage,
    path: &str,
    manifest: &MetastoreProjectionManifest,
    bytes: Bytes,
) -> Result<()> {
    match storage
        .put_raw(path, bytes, WritePrecondition::DoesNotExist)
        .await
        .map_err(CatalogError::from)?
    {
        WriteResult::Success { .. } => Ok(()),
        WriteResult::PreconditionFailed { .. } => {
            let existing_bytes = storage.get_raw(path).await.map_err(CatalogError::from)?;
            let existing = serde_json::from_slice::<MetastoreProjectionManifest>(&existing_bytes)
                .map_err(|err| CatalogError::Serialization {
                message: format!(
                    "failed to deserialize existing metastore projection manifest: {err}"
                ),
            })?;
            if manifest_contents_match(&existing, manifest) {
                Ok(())
            } else {
                Err(CatalogError::PreconditionFailed {
                    message: format!("published projection manifest already exists: {path}"),
                })
            }
        }
    }
}

fn manifest_contents_match(
    left: &MetastoreProjectionManifest,
    right: &MetastoreProjectionManifest,
) -> bool {
    left.manifest_id == right.manifest_id
        && left.ledger_watermark == right.ledger_watermark
        && left.ledger_watermark_sequence == right.ledger_watermark_sequence
        && left.files == right.files
}

async fn storage_governance_cache_identity(
    storage: &ScopedStorage,
    manifest: &MetastoreProjectionManifest,
) -> Result<PublishedStorageGovernanceCacheIdentity> {
    let storage_governance_object = match validate_storage_governance_projection_file(manifest)? {
        Some(file) => Some(storage_governance_object_identity(storage, file).await?),
        None => None,
    };

    Ok(PublishedStorageGovernanceCacheIdentity {
        tenant_id: storage.tenant_id().to_string(),
        workspace_id: storage.workspace_id().to_string(),
        manifest_id: manifest.manifest_id.clone(),
        ledger_watermark: manifest.ledger_watermark.clone(),
        ledger_watermark_sequence: manifest.ledger_watermark_sequence,
        files: manifest.files.clone(),
        storage_governance_object,
    })
}

async fn storage_governance_object_identity(
    storage: &ScopedStorage,
    file: &MetastoreProjectionFileManifest,
) -> Result<PublishedStorageGovernanceObjectIdentity> {
    let meta = storage
        .head_raw(&file.path)
        .await
        .map_err(|_| projection_unavailable("storage_governance_projection_unavailable"))?
        .ok_or_else(|| projection_unavailable("storage_governance_projection_unavailable"))?;
    Ok(object_identity_from_meta(meta))
}

fn object_identity_from_meta(meta: ObjectMeta) -> PublishedStorageGovernanceObjectIdentity {
    PublishedStorageGovernanceObjectIdentity {
        path: meta.path,
        size: meta.size,
        version: meta.version,
        last_modified: meta.last_modified,
        etag: meta.etag,
    }
}

fn validate_storage_governance_projection_file(
    manifest: &MetastoreProjectionManifest,
) -> Result<Option<&MetastoreProjectionFileManifest>> {
    if manifest.ledger_watermark_sequence == 0
        && manifest.ledger_watermark == "empty"
        && manifest.files.is_empty()
    {
        return Ok(None);
    }

    let file = manifest
        .files
        .iter()
        .find(|file| file.file_name == STORAGE_GOVERNANCE_PROJECTION)
        .ok_or_else(|| projection_unavailable("storage_governance_projection_missing"))?;
    if file.schema_version != STORAGE_GOVERNANCE_SCHEMA_VERSION {
        return Err(projection_unavailable(
            "storage_governance_projection_unsupported",
        ));
    }
    Ok(Some(file))
}

fn projection_unavailable(reason: &str) -> CatalogError {
    CatalogError::RequestFailed {
        http_status: 503,
        message: reason.to_string(),
    }
}
