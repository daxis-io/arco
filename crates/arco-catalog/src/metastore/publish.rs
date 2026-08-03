//! Pointer-publication planning for metastore projections.
//!
//! # Request amplification (program rule 6)
//!
//! [`publish_current_metastore_projection`] runs commit-synchronously on every
//! UC storage-governance POST. Each invocation performs one full
//! metastore-ledger LIST ([`MetastoreLedger::latest_watermark`] /
//! `load_events`) and, whenever the pointer is behind, replays the ledger and
//! rewrites the *entire* projection set — O(ledger size) work per commit, on
//! top of the request-time ledger LISTs the governance routes already perform
//! for replay-based validation. This is a measured, tracked deviation from
//! program rule 6 ("no listing for request-time correctness"); see the Rule 6
//! finding on the UC storage-governance routes in
//! `docs/reports/2026-07-30-design-program-progress-audit.md` (section 4,
//! rule 6). The follow-up is incremental projection publication (per-watermark
//! delta projections) so each commit republishes O(delta) instead of
//! O(ledger).

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
    ProjectionRegistry, ProjectionSet, STORAGE_GOVERNANCE_PROJECTION,
    STORAGE_GOVERNANCE_SCHEMA_VERSION, build_projection_set, read_metastore_object_rows,
};
use super::replay::replay_events;

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

/// A published storage-governance projection together with the instant its
/// freshness was observed.
///
/// Freshness validation is a point-in-time observation, not a lock: a
/// revocation can commit while a request that already validated the watermark
/// is still running. Consumers that mint time-bounded authority (credential
/// vending) must anchor expiry to [`Self::observed_at`], and may re-fence
/// against [`Self::observed_watermark`] before returning.
#[derive(Debug, Clone)]
pub struct ObservedStorageGovernance {
    /// Published projection state.
    pub published: Arc<PublishedStorageGovernance>,
    /// Instant at which the published watermark was validated against the
    /// latest committed ledger watermark.
    pub observed_at: DateTime<Utc>,
    /// Latest committed ledger watermark seen by that validation.
    pub observed_watermark: Option<MetastoreLedgerWatermark>,
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
        self.load_observed(storage)
            .await
            .map(|observed| observed.published)
    }

    /// Loads the published storage-governance projection together with the
    /// instant its freshness was observed and the watermark that observation
    /// saw.
    ///
    /// Callers that mint time-bounded authority must use this rather than
    /// [`Self::load`]: the returned observation is the anchor for expiry
    /// clamping and for re-fencing the watermark before a credential is
    /// returned. The observation instant is recorded immediately after
    /// watermark validation succeeds, including on a cache hit (freshness is
    /// revalidated on every call, so a hit is still a fresh observation).
    ///
    /// # Errors
    ///
    /// Returns `RequestFailed(503)` when the published projection is missing,
    /// stale, unsupported, or corrupt.
    pub async fn load_observed(
        &self,
        storage: &ScopedStorage,
    ) -> Result<ObservedStorageGovernance> {
        let manifest = load_projection_manifest(storage).await?;
        let latest = MetastoreLedger::new(storage.clone())
            .latest_watermark()
            .await?;
        validate_storage_governance_manifest_freshness(&manifest, latest.as_ref())?;
        let observed_at = Utc::now();
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
                return Ok(ObservedStorageGovernance {
                    published: Arc::clone(&current.value),
                    observed_at,
                    observed_watermark: latest,
                });
            }
        }

        let _guard = self.refresh.lock().await;
        let manifest = load_projection_manifest(storage).await?;
        let latest = MetastoreLedger::new(storage.clone())
            .latest_watermark()
            .await?;
        validate_storage_governance_manifest_freshness(&manifest, latest.as_ref())?;
        let observed_at = Utc::now();
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
                return Ok(ObservedStorageGovernance {
                    published: Arc::clone(&current.value),
                    observed_at,
                    observed_watermark: latest,
                });
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
        Ok(ObservedStorageGovernance {
            published: loaded,
            observed_at,
            observed_watermark: latest,
        })
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

/// Outcome of a commit-synchronous metastore projection publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetastoreProjectionPublication {
    /// The metastore ledger is empty; there is nothing to publish.
    EmptyLedger,
    /// The published pointer already covers the latest ledger watermark.
    AlreadyCurrent {
        /// Ledger watermark sequence covered by the published pointer.
        ledger_watermark_sequence: u64,
    },
    /// A projection set was published at the latest ledger watermark.
    Published(MetastoreProjectionManifest),
}

/// Publishes the metastore projection set at the current ledger watermark.
///
/// This is the production publisher for the storage-governance projection
/// (issue #362): metastore ledger committers append their event and then call
/// this to advance the published projection to the new watermark. Because
/// [`load_published_storage_governance`] enforces exact-watermark freshness,
/// publication must follow every ledger commit; a projection left behind the
/// ledger keeps credential vending deny-closed (HTTP 503) until republished.
///
/// The publication is idempotent and monotonic:
///
/// - a pointer already at (or beyond) the latest watermark is left untouched;
/// - losing the pointer CAS race to a publisher at the same or newer watermark
///   is reported as [`MetastoreProjectionPublication::AlreadyCurrent`];
/// - only events at or below the latest durable watermark are projected, so a
///   concurrent in-flight append never leaks into an older watermark's set.
///
/// # Errors
///
/// Returns an error when the ledger cannot be read, the latest watermark is
/// pending, or projection files/manifest/pointer cannot be written.
pub async fn publish_current_metastore_projection(
    storage: &ScopedStorage,
    registry: &ProjectionRegistry,
) -> Result<MetastoreProjectionPublication> {
    let ledger = MetastoreLedger::new(storage.clone());
    let Some(latest) = ledger.latest_watermark().await? else {
        return Ok(MetastoreProjectionPublication::EmptyLedger);
    };

    if let Some(current) = load_current_projection_pointer(storage).await? {
        if current.manifest.ledger_watermark_sequence >= latest.sequence {
            return Ok(MetastoreProjectionPublication::AlreadyCurrent {
                ledger_watermark_sequence: current.manifest.ledger_watermark_sequence,
            });
        }
    }

    let events = ledger.load_events().await?;
    if !events
        .iter()
        .any(|event| event.sequence == latest.sequence && event.event_id == latest.event_id)
    {
        // Event identifiers and sequences are internal ledger state: correlate
        // them in logs, return only a stable reason code to callers.
        tracing::error!(
            event_id = %latest.event_id,
            sequence = latest.sequence,
            "latest metastore watermark is not yet readable from the ledger"
        );
        return Err(CatalogError::InvariantViolation {
            message: "metastore_latest_watermark_not_readable".to_string(),
        });
    }
    let state = replay_events(
        events
            .iter()
            .filter(|event| event.sequence <= latest.sequence),
    )?;
    let projection_set = build_projection_set(&state, registry, &latest.event_id)?;

    match publish_metastore_projection_set(storage, &projection_set, latest.sequence).await {
        Ok(manifest) => Ok(MetastoreProjectionPublication::Published(manifest)),
        Err(CatalogError::PreconditionFailed { message }) => {
            // A concurrent publisher may have moved the pointer to the same or
            // a newer watermark; that publication covers this one.
            if let Some(current) = load_current_projection_pointer(storage).await? {
                if current.manifest.ledger_watermark_sequence >= latest.sequence {
                    return Ok(MetastoreProjectionPublication::AlreadyCurrent {
                        ledger_watermark_sequence: current.manifest.ledger_watermark_sequence,
                    });
                }
            }
            Err(CatalogError::PreconditionFailed { message })
        }
        Err(error) => Err(error),
    }
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

/// Loads the published storage-governance projection when one has been
/// configured for this tenant/workspace scope.
///
/// Returns `Ok(None)` when no projection pointer exists, meaning storage
/// governance has never been enabled for the scope and callers may preserve
/// ungoverned behavior. When a pointer exists the projection must be fresh:
/// stale or corrupt projections fail closed exactly like
/// [`load_published_storage_governance`].
///
/// # Errors
///
/// Returns `RequestFailed(503)` when a pointer exists but the projection is
/// stale, unsupported, or corrupt, and storage errors from the pointer probe.
pub async fn load_published_storage_governance_if_configured(
    storage: &ScopedStorage,
) -> Result<Option<PublishedStorageGovernance>> {
    if storage
        .head_raw(METASTORE_PROJECTION_POINTER)
        .await
        .map_err(CatalogError::from)?
        .is_none()
    {
        return Ok(None);
    }
    load_published_storage_governance(storage).await.map(Some)
}

/// Validates a client-supplied storage location against published storage
/// governance (#358).
///
/// Shared by the table-creation surfaces (Iceberg REST, UC `POST /tables`,
/// and the native API's `register_table_in_schema`) so every client-controlled
/// location channel enforces the same rules:
///
/// - **Governance not configured** (no projection pointer has ever been
///   published for the scope): returns `Ok(())` and callers preserve
///   ungoverned behavior unchanged.
/// - **Governance configured**: the location must resolve to exactly one
///   active path authority bound to `workspace_id`. Ungoverned, ambiguously
///   governed, and unparseable locations are rejected with
///   [`CatalogError::Validation`], which the route layers surface as a typed
///   400.
/// - **Configured but stale or corrupt projection**: fails closed with
///   `RequestFailed(503)`, matching the credential-vending posture.
///
/// # Errors
///
/// Returns [`CatalogError::Validation`] for denied locations and
/// `RequestFailed(503)` when the published projection is stale, unsupported,
/// or corrupt.
pub async fn validate_governed_location_if_configured(
    storage: &ScopedStorage,
    workspace_id: &str,
    location: &str,
) -> Result<()> {
    let Some(published) = load_published_storage_governance_if_configured(storage).await? else {
        return Ok(());
    };
    validate_governed_location_against(&published, workspace_id, location, None)
}

/// Table property keys whose value is a storage location.
///
/// Each of these redirects data or metadata writes away from the advertised
/// table location, so under storage governance they are validated exactly like
/// the advertised location. This is the single source of truth shared by every
/// table-creation, registration, and commit surface; adding a key here extends
/// enforcement everywhere at once.
pub const LOCATION_BEARING_TABLE_PROPERTIES: [&str; 3] = [
    "write.data.path",
    "write.metadata.path",
    "write.object-storage.path",
];

/// Validates the location-bearing entries of a table property map against
/// published storage governance (#358).
///
/// Behaves exactly like [`validate_governed_location_if_configured`] for each
/// [`LOCATION_BEARING_TABLE_PROPERTIES`] key present in `properties`, naming
/// the offending property in the rejection. Property maps with none of those
/// keys never load governance state and pass through unchanged.
///
/// # Errors
///
/// Returns [`CatalogError::Validation`] for denied property locations and
/// `RequestFailed(503)` when the published projection is stale, unsupported,
/// or corrupt.
pub async fn validate_governed_location_properties_if_configured<'a, I>(
    storage: &ScopedStorage,
    workspace_id: &str,
    properties: I,
) -> Result<()>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let targeted = properties
        .into_iter()
        .filter(|(key, _)| LOCATION_BEARING_TABLE_PROPERTIES.contains(key))
        .collect::<Vec<_>>();
    if targeted.is_empty() {
        return Ok(());
    }
    let Some(published) = load_published_storage_governance_if_configured(storage).await? else {
        return Ok(());
    };
    for (property, location) in targeted {
        validate_governed_location_against(&published, workspace_id, location, Some(property))?;
    }
    Ok(())
}

fn validate_governed_location_against(
    published: &PublishedStorageGovernance,
    workspace_id: &str,
    location: &str,
    property: Option<&str>,
) -> Result<()> {
    let described = property.map_or_else(
        || format!("storage location '{location}'"),
        |property| format!("table property '{property}' storage location '{location}'"),
    );
    match published.state.authority_for_path(workspace_id, location) {
        Ok(_) => Ok(()),
        Err(CatalogError::NotFound { .. }) => Err(CatalogError::Validation {
            message: format!(
                "{described} is not governed by any storage-governance path authority bound to \
                 this workspace"
            ),
        }),
        Err(CatalogError::PreconditionFailed { .. }) => Err(CatalogError::Validation {
            message: format!(
                "{described} is ambiguously governed by overlapping storage-governance path \
                 authorities"
            ),
        }),
        Err(CatalogError::Validation { message }) => Err(CatalogError::Validation {
            message: format!("invalid {described} under storage governance: {message}"),
        }),
        Err(error) => Err(error),
    }
}

/// Enforces the projection-staleness half of the revocation-freshness budget.
///
/// The allowed staleness is derived from
/// [`crate::credential_vending::MAX_PROJECTION_STALENESS`], so changing that
/// constant changes this validator's behavior directly. With a zero budget
/// (the current value) only a manifest at the *exact* latest ledger watermark
/// may serve credential decisions: a committed revocation is visible to every
/// subsequent decision and stale state denies closed (HTTP 503). A non-zero
/// budget admits a sequence-behind manifest only while its publication
/// timestamp is still within the budget. Any widening must update
/// `REVOCATION_FRESHNESS_BUDGET_SECS` and
/// `docs/guide/src/reference/credential-vending-security.md` together.
fn validate_storage_governance_manifest_freshness(
    manifest: &MetastoreProjectionManifest,
    latest: Option<&MetastoreLedgerWatermark>,
) -> Result<()> {
    match latest {
        Some(latest)
            if manifest.ledger_watermark_sequence == latest.sequence
                && manifest.ledger_watermark == latest.event_id => {}
        Some(_) => {
            if !manifest_within_staleness_budget(manifest) {
                return Err(projection_unavailable(
                    "storage_governance_projection_stale",
                ));
            }
        }
        None => {
            if manifest.ledger_watermark_sequence == 0
                && manifest.ledger_watermark == "empty"
                && manifest.files.is_empty()
            {
                return Ok(());
            }
            // A manifest claiming events on an empty ledger is corrupt, not
            // merely stale; no staleness budget can admit it.
            return Err(projection_unavailable(
                "storage_governance_projection_stale",
            ));
        }
    }
    Ok(())
}

/// Returns true when a manifest behind the latest ledger watermark is still
/// inside the projection-staleness budget.
///
/// With [`crate::credential_vending::MAX_PROJECTION_STALENESS`] at zero this
/// is always false, which makes exact-watermark equality the effective rule.
fn manifest_within_staleness_budget(manifest: &MetastoreProjectionManifest) -> bool {
    let budget = crate::credential_vending::MAX_PROJECTION_STALENESS;
    if budget.is_zero() {
        return false;
    }
    Utc::now()
        .signed_duration_since(manifest.published_at)
        .to_std()
        .is_ok_and(|age| age <= budget)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_at(sequence: u64, event_id: &str) -> MetastoreProjectionManifest {
        MetastoreProjectionManifest {
            manifest_id: format!("{sequence:020}"),
            ledger_watermark: event_id.to_string(),
            ledger_watermark_sequence: sequence,
            files: Vec::new(),
            published_at: Utc::now(),
        }
    }

    fn watermark_at(sequence: u64, event_id: &str) -> MetastoreLedgerWatermark {
        MetastoreLedgerWatermark {
            event_id: event_id.to_string(),
            sequence,
        }
    }

    /// The validator's allowed staleness derives from
    /// `credential_vending::MAX_PROJECTION_STALENESS` (zero today), so a
    /// freshly published manifest even one sequence behind the ledger is
    /// rejected.
    #[test]
    fn manifest_one_sequence_behind_is_rejected_under_zero_staleness_budget() {
        assert!(
            crate::credential_vending::MAX_PROJECTION_STALENESS.is_zero(),
            "this test pins the exact-match consequence of a zero budget"
        );

        let manifest = manifest_at(4, "event_004");
        let latest = watermark_at(5, "event_005");
        let err = validate_storage_governance_manifest_freshness(&manifest, Some(&latest))
            .expect_err("a manifest one sequence behind must be rejected");
        let CatalogError::RequestFailed {
            http_status,
            message,
        } = err
        else {
            panic!("expected RequestFailed(503) for stale manifest");
        };
        assert_eq!(http_status, 503);
        assert_eq!(message, "storage_governance_projection_stale");
    }

    #[test]
    fn manifest_at_exact_watermark_is_accepted() {
        let manifest = manifest_at(5, "event_005");
        let latest = watermark_at(5, "event_005");
        assert!(validate_storage_governance_manifest_freshness(&manifest, Some(&latest)).is_ok());
    }

    #[test]
    fn manifest_claiming_events_on_empty_ledger_is_rejected() {
        let manifest = manifest_at(3, "event_003");
        assert!(validate_storage_governance_manifest_freshness(&manifest, None).is_err());
    }
}
