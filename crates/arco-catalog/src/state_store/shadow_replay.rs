//! Phase 4A shadow replay and projection-equivalence scaffolding.
//!
//! This module imports the current published catalog snapshot into an isolated
//! control-MVP scope and compares the nine roadmap-mandated domains, each to
//! the extent current repo behavior makes an honest comparison possible.
//!
//! The base source/compare pair (`load_current_catalog_shadow_source` /
//! `compare_catalog_shadow`) stays listing-free and is what the Phase 4B
//! comparison-read request path uses. The extended pair used by the operator
//! importer additionally replays the metastore ledger and enumerates
//! idempotency markers — both `storage.list`-based, which is acceptable only
//! because shadow import is an operator/repair-lane tool, never a request-time
//! correctness path.
//!
//! # What the row-level comparisons prove
//!
//! The `TableCurrentPointers`, `GrantsOwnership`, and the row-level
//! `StorageGovernance`/`IdempotencyRecords` checks compare shadow rows
//! against expected rows derived by the *same* projection functions from the
//! *same* in-memory source. They therefore verify control-store round-trip
//! fidelity (import, replay, and read-back preserve the rows), NOT an
//! independent derivation of the domain. Likewise `ParquetProjectionEquality`
//! performs a second load of the published snapshot through the same loader,
//! verifying load determinism plus round-trip fidelity, not an independent
//! implementation. Independent evidence exists only where a second authority
//! is consulted: the published storage-governance projection versus ledger
//! replay, and ledger event bytes versus commit witnesses.

use std::collections::{BTreeMap, BTreeSet};

use arco_core::prelude::LedgerKey;
use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};
use bytes::Bytes;
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::{ArcoStateReader, ArcoStateStore, ControlMvpStateStore, StateScope, TxnOptions};
use crate::error::{CatalogError, Result};
use crate::idempotency::CATALOG_IDEMPOTENCY_PREFIX;
use crate::manifest::{CatalogDomainManifest, DomainManifestPointer, compute_manifest_hash};
use crate::metastore::ledger::MetastoreLedger;
use crate::metastore::publish::load_published_storage_governance;
use crate::metastore::replay::MetastoreState;
use crate::parquet_util::decode_catalog_commit_event_witnesses;
use crate::state::CatalogState;
use crate::storage_governance::StorageGovernanceState;
use crate::tier1_state;

const SHADOW_DOMAIN: &str = "catalog-shadow";
const KEY_PREFIX: &str = "shadow/catalog/";
const OBJECT_PREFIX: &str = "shadow/catalog/object/";
const INDEX_PREFIX: &str = "shadow/catalog/index/";
const TABLE_POINTER_PREFIX: &str = "shadow/catalog/pointer/table/";
const GRANT_PREFIX: &str = "shadow/catalog/grant/";
const GOVERNANCE_PREFIX: &str = "shadow/catalog/governance/";
const IDEMPOTENCY_PREFIX: &str = "shadow/catalog/idempotency/";
const REPLAY_WITNESS_PREFIX: &str = "shadow/catalog/replay-witness/";
const MANIFEST_WATERMARK_KEY: &str = "shadow/catalog/metadata/source-watermark";
#[cfg(test)]
const LEGACY_DEFAULT_CATALOG_PARENT: &str = "__legacy_default_catalog__";

#[derive(Debug, Clone, PartialEq, Eq)]
/// `ShadowReplayReport` shadow-comparison type.
pub struct ShadowReplayReport {
    source: CatalogShadowSourceIdentity,
    included_domains: Vec<ShadowIncludedDomain>,
    deferred_domains: Vec<ShadowDeferredEntry>,
    comparisons: Vec<ShadowComparison>,
}

impl ShadowReplayReport {
    #[must_use]
    /// Returns `source` for this shadow-comparison item.
    pub fn source(&self) -> &CatalogShadowSourceIdentity {
        &self.source
    }

    #[must_use]
    #[cfg(test)]
    /// Returns the domains this report compared.
    pub fn included_domains(&self) -> &[ShadowIncludedDomain] {
        &self.included_domains
    }

    #[must_use]
    /// Returns `deferred_domains` for this shadow-comparison item.
    pub fn deferred_domains(&self) -> &[ShadowDeferredEntry] {
        &self.deferred_domains
    }

    #[must_use]
    /// Returns `comparisons` for this shadow-comparison item.
    pub fn comparisons(&self) -> &[ShadowComparison] {
        &self.comparisons
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
/// `CatalogShadowSourceIdentity` shadow-comparison type.
pub struct CatalogShadowSourceIdentity {
    pointer_path: String,
    pointer_version: String,
    pointer_manifest_id: String,
    pointer_manifest_path: String,
    pointer_hash: String,
    manifest_id: String,
    snapshot_version: u64,
    snapshot_path: String,
    watermark_event_id: Option<String>,
    last_commit_id: Option<String>,
}

impl CatalogShadowSourceIdentity {
    #[must_use]
    /// Returns `manifest_id` for this shadow-comparison item.
    pub fn manifest_id(&self) -> &str {
        &self.manifest_id
    }

    #[must_use]
    /// Returns `snapshot_version` for this shadow-comparison item.
    pub const fn snapshot_version(&self) -> u64 {
        self.snapshot_version
    }
}

#[derive(Debug, Clone)]
/// `CatalogShadowSource` shadow-comparison type.
pub struct CatalogShadowSource {
    identity: CatalogShadowSourceIdentity,
    state: CatalogState,
}

impl CatalogShadowSource {
    #[must_use]
    /// Returns `identity` for this shadow-comparison item.
    pub const fn identity(&self) -> &CatalogShadowSourceIdentity {
        &self.identity
    }

    #[must_use]
    /// Returns `state` for this shadow-comparison item.
    pub const fn state(&self) -> &CatalogState {
        &self.state
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// `ShadowIncludedDomain` shadow-comparison classification.
pub enum ShadowIncludedDomain {
    /// Objects.
    Objects,
    /// Name indexes.
    NameIndexes,
    /// Manifest watermark.
    ManifestWatermark,
    /// Table current pointers.
    TableCurrentPointers,
    /// Grants ownership.
    GrantsOwnership,
    /// Storage governance.
    StorageGovernance,
    /// Idempotency records.
    IdempotencyRecords,
    /// Event replay hashes.
    EventReplayHashes,
    /// Parquet projection equality.
    ParquetProjectionEquality,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// `ShadowDeferredDomain` shadow-comparison classification.
pub enum ShadowDeferredDomain {
    /// Full projection watermarks.
    FullProjectionWatermarks,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// `ShadowComparisonDomain` shadow-comparison classification.
pub enum ShadowComparisonDomain {
    /// Objects.
    Objects,
    /// Name indexes.
    NameIndexes,
    /// Manifest watermark.
    ManifestWatermark,
    /// Table current pointers.
    TableCurrentPointers,
    /// Grants ownership.
    GrantsOwnership,
    /// Storage governance.
    StorageGovernance,
    /// Idempotency records.
    IdempotencyRecords,
    /// Event replay hashes.
    EventReplayHashes,
    /// Parquet projection equality.
    ParquetProjectionEquality,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// `ShadowDifferenceClass` shadow-comparison classification.
pub enum ShadowDifferenceClass {
    /// Current state gap.
    CurrentStateGap,
    /// Unsupported scope.
    UnsupportedScope,
    /// Stale projection.
    StaleProjection,
    /// Bug divergent result.
    BugDivergentResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `ShadowComparisonStatus` shadow-comparison classification.
pub enum ShadowComparisonStatus {
    /// Equivalent.
    Equivalent,
    /// Difference.
    Difference(ShadowDifferenceClass),
}

impl ShadowComparisonStatus {
    #[must_use]
    #[cfg(test)]
    /// Returns whether the status is equivalence.
    pub const fn is_equivalent(self) -> bool {
        matches!(self, Self::Equivalent)
    }

    #[must_use]
    #[cfg(test)]
    /// Returns the difference class when the status is a difference.
    pub const fn difference_class(self) -> Option<ShadowDifferenceClass> {
        match self {
            Self::Equivalent => None,
            Self::Difference(class) => Some(class),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `ShadowComparison` shadow-comparison type.
pub struct ShadowComparison {
    domain: ShadowComparisonDomain,
    status: ShadowComparisonStatus,
    detail: String,
}

impl ShadowComparison {
    #[must_use]
    /// Returns `domain` for this shadow-comparison item.
    pub const fn domain(&self) -> ShadowComparisonDomain {
        self.domain
    }

    #[must_use]
    /// Returns `status` for this shadow-comparison item.
    pub const fn status(&self) -> ShadowComparisonStatus {
        self.status
    }

    #[must_use]
    /// Returns `detail` for this shadow-comparison item.
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `ShadowDeferredEntry` shadow-comparison type.
pub struct ShadowDeferredEntry {
    domain: ShadowDeferredDomain,
    status: ShadowComparisonStatus,
    reason: String,
}

impl ShadowDeferredEntry {
    #[must_use]
    /// Returns `domain` for this shadow-comparison item.
    pub const fn domain(&self) -> ShadowDeferredDomain {
        self.domain
    }

    #[must_use]
    /// Returns `status` for this shadow-comparison item.
    pub const fn status(&self) -> ShadowComparisonStatus {
        self.status
    }

    #[must_use]
    /// Returns `reason` for this shadow-comparison item.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `ShadowObjectKind` shadow-comparison classification.
pub enum ShadowObjectKind {
    /// Catalog.
    Catalog,
    /// Schema.
    Schema,
    /// Table.
    Table,
    /// Column.
    Column,
}

/// Extended shadow source: the published catalog snapshot plus the
/// governance/idempotency surfaces the roadmap's nine comparison domains need.
#[derive(Debug, Clone)]
pub struct CatalogShadowExtendedSource {
    base: CatalogShadowSource,
    metastore: Option<MetastoreState>,
    idempotency: BTreeMap<String, Vec<u8>>,
}

impl CatalogShadowExtendedSource {
    /// Returns the base catalog snapshot source.
    #[must_use]
    pub const fn base(&self) -> &CatalogShadowSource {
        &self.base
    }
}

/// OPERATOR/REPAIR-LANE ONLY: imports current published state into the
/// isolated shadow scope and compares all nine roadmap domains.
///
/// This path replays the metastore ledger and enumerates idempotency markers
/// via `storage.list`, which is forbidden on request-time correctness paths —
/// it may only be invoked from operator surfaces (the compactor's internal
/// shadow-import endpoint).
///
/// # Errors
///
/// Returns storage, decode, or serialization errors from any source surface.
pub async fn import_current_catalog_shadow(storage: &ScopedStorage) -> Result<ShadowReplayReport> {
    let source = load_extended_catalog_shadow_source(storage).await?;
    let shadow = open_catalog_shadow_store(storage)?;
    import_extended_catalog_source_into_shadow(&shadow, &source).await?;
    compare_extended_catalog_shadow(storage, &shadow, &source).await
}

/// Loads the extended shadow source (operator/repair lane; lists the metastore
/// ledger prefix and the idempotency marker prefix).
///
/// # Errors
///
/// Returns storage or decode errors from any source surface.
pub async fn load_extended_catalog_shadow_source(
    storage: &ScopedStorage,
) -> Result<CatalogShadowExtendedSource> {
    let base = load_current_catalog_shadow_source(storage).await?;

    let metastore_state = MetastoreLedger::new(storage.clone()).replay().await?;
    let metastore = if metastore_state.ledger_watermark.is_none() {
        None
    } else {
        Some(metastore_state)
    };

    let mut idempotency = BTreeMap::new();
    let marker_prefix = format!("{CATALOG_IDEMPOTENCY_PREFIX}/");
    for path in storage.list(&marker_prefix).await? {
        let relative = path.as_str();
        let Some(suffix) = relative.strip_prefix(&marker_prefix) else {
            continue;
        };
        let bytes = storage.get_raw(relative).await?;
        idempotency.insert(suffix.to_string(), bytes.to_vec());
    }

    Ok(CatalogShadowExtendedSource {
        base,
        metastore,
        idempotency,
    })
}

/// Loads the published catalog snapshot source (listing-free; safe for the
/// Phase 4B comparison-read path).
///
/// # Errors
///
/// Returns storage or decode errors, and fails closed on pointer races.
pub async fn load_current_catalog_shadow_source(
    storage: &ScopedStorage,
) -> Result<CatalogShadowSource> {
    let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
    let pointer_meta =
        storage
            .head_raw(&pointer_path)
            .await?
            .ok_or_else(|| CatalogError::NotFound {
                entity: "catalog manifest pointer".to_string(),
                name: pointer_path.clone(),
            })?;
    let pointer_bytes = storage.get_raw(&pointer_path).await?;
    let pointer_meta_after =
        storage
            .head_raw(&pointer_path)
            .await?
            .ok_or_else(|| CatalogError::PreconditionFailed {
                message: format!(
                    "catalog manifest pointer {pointer_path} disappeared while loading catalog shadow source"
                ),
            })?;
    let pointer_version = stable_pointer_version(
        &pointer_path,
        &pointer_meta.version,
        &pointer_meta_after.version,
    )?;
    let pointer: DomainManifestPointer =
        serde_json::from_slice(&pointer_bytes).map_err(|err| CatalogError::Serialization {
            message: format!("parse catalog manifest pointer at {pointer_path}: {err}"),
        })?;

    let manifest_bytes = storage.get_raw(&pointer.manifest_path).await?;
    let manifest: CatalogDomainManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|err| CatalogError::Serialization {
            message: format!(
                "parse catalog domain manifest at {}: {err}",
                pointer.manifest_path
            ),
        })?;
    if pointer.manifest_id != manifest.manifest_id {
        return Err(CatalogError::InvariantViolation {
            message: format!(
                "catalog pointer manifest_id {} does not match manifest {}",
                pointer.manifest_id, manifest.manifest_id
            ),
        });
    }

    let state = tier1_state::load_catalog_state(storage, &manifest.snapshot_path).await?;
    Ok(CatalogShadowSource {
        identity: CatalogShadowSourceIdentity {
            pointer_path,
            pointer_version,
            pointer_manifest_id: pointer.manifest_id,
            pointer_manifest_path: pointer.manifest_path,
            pointer_hash: compute_manifest_hash(&pointer_bytes),
            manifest_id: manifest.manifest_id,
            snapshot_version: manifest.snapshot_version,
            snapshot_path: manifest.snapshot_path,
            watermark_event_id: manifest.watermark_event_id,
            last_commit_id: manifest.last_commit_id,
        },
        state,
    })
}

/// Opens the isolated `catalog-shadow` control-MVP store.
///
/// # Errors
///
/// Returns store-construction errors.
pub fn open_catalog_shadow_store(storage: &ScopedStorage) -> Result<ControlMvpStateStore> {
    ControlMvpStateStore::new(
        storage.clone(),
        StateScope::new(storage.tenant_id(), storage.workspace_id(), SHADOW_DOMAIN),
    )
}

/// Imports the base (Phase 4B-visible) row families into the shadow scope.
///
/// # Errors
///
/// Returns storage or serialization errors.
pub async fn import_catalog_source_into_shadow(
    store: &ControlMvpStateStore,
    source: &CatalogShadowSource,
) -> Result<()> {
    let expected = build_expected_shadow_rows(source)?;
    import_rows_into_shadow(store, source.identity().manifest_id(), expected.rows).await
}

/// Imports all nine domains' row families into the shadow scope.
///
/// # Errors
///
/// Returns storage or serialization errors.
pub async fn import_extended_catalog_source_into_shadow(
    store: &ControlMvpStateStore,
    source: &CatalogShadowExtendedSource,
) -> Result<()> {
    let expected = build_extended_expected_rows(source)?;
    import_rows_into_shadow(store, source.base.identity().manifest_id(), expected.rows).await
}

async fn import_rows_into_shadow(
    store: &ControlMvpStateStore,
    manifest_id: &str,
    rows: BTreeMap<Vec<u8>, Bytes>,
) -> Result<()> {
    let mut txn = store
        .begin_txn(
            TxnOptions::default().with_request_id(format!("phase4a-shadow-import-{manifest_id}")),
        )
        .await?;

    for existing in txn.scan_prefix(KEY_PREFIX.as_bytes()).await? {
        if !rows.contains_key(existing.key()) {
            txn.delete(existing.key()).await?;
        }
    }

    for (key, value) in rows {
        txn.put(&key, value).await?;
    }

    txn.commit().await?;
    Ok(())
}

/// Compares the base three domains (listing-free; used by Phase 4B).
///
/// # Errors
///
/// Returns storage or serialization errors.
pub async fn compare_catalog_shadow(
    store: &ControlMvpStateStore,
    source: &CatalogShadowSource,
) -> Result<ShadowReplayReport> {
    let expected = build_expected_shadow_rows(source)?;
    let actual = scan_shadow_rows(store).await?;
    let comparisons = base_comparisons(&expected, &actual);

    Ok(ShadowReplayReport {
        source: source.identity().clone(),
        included_domains: vec![
            ShadowIncludedDomain::Objects,
            ShadowIncludedDomain::NameIndexes,
            ShadowIncludedDomain::ManifestWatermark,
        ],
        deferred_domains: deferred_domains(),
        comparisons,
    })
}

/// OPERATOR/REPAIR-LANE ONLY nine-domain comparison.
///
/// Consults the metastore ledger, the published storage-governance
/// projection, the idempotency marker prefix, the event ledger, and a second
/// Parquet snapshot load through the same loader. See the module docs for
/// which comparisons prove control-store round-trip fidelity versus
/// independent evidence.
///
/// # Errors
///
/// Returns storage or serialization errors; classification differences are
/// reported in the returned comparisons, not as errors.
pub async fn compare_extended_catalog_shadow(
    storage: &ScopedStorage,
    store: &ControlMvpStateStore,
    source: &CatalogShadowExtendedSource,
) -> Result<ShadowReplayReport> {
    let expected = build_extended_expected_rows(source)?;
    let actual = scan_shadow_rows(store).await?;
    let mut comparisons = base_comparisons(&expected, &actual);

    comparisons.push(simple_row_comparison(
        ShadowComparisonDomain::TableCurrentPointers,
        &expected,
        &actual,
        is_table_pointer_key,
        "table current-pointer records round-tripped through the control store (both sides derive from the same projection over the same imported snapshot: this verifies store round-trip fidelity, not an independent derivation; the current path has no unified pointer object, so coverage is partial by design)",
    ));

    comparisons.push(if source.metastore.is_none() {
        ShadowComparison {
            domain: ShadowComparisonDomain::GrantsOwnership,
            status: ShadowComparisonStatus::Difference(ShadowDifferenceClass::CurrentStateGap),
            detail: "no metastore ledger events exist for this workspace; grant/ownership comparison has no authoritative source".to_string(),
        }
    } else {
        simple_row_comparison(
            ShadowComparisonDomain::GrantsOwnership,
            &expected,
            &actual,
            is_grant_key,
            "grant and ownership records round-tripped through the control store (both sides derive from the same metastore-ledger replay: store round-trip fidelity, not an independent derivation)",
        )
    });

    comparisons.push(storage_governance_comparison(storage, source, &expected, &actual).await);

    comparisons.push(idempotency_comparison(storage, source, &expected, &actual).await?);

    comparisons.push(event_replay_hash_comparison(storage, source, &expected, &actual).await?);

    comparisons.push(parquet_equality_comparison(storage, source, &actual).await);

    Ok(ShadowReplayReport {
        source: source.base.identity().clone(),
        included_domains: vec![
            ShadowIncludedDomain::Objects,
            ShadowIncludedDomain::NameIndexes,
            ShadowIncludedDomain::ManifestWatermark,
            ShadowIncludedDomain::TableCurrentPointers,
            ShadowIncludedDomain::GrantsOwnership,
            ShadowIncludedDomain::StorageGovernance,
            ShadowIncludedDomain::IdempotencyRecords,
            ShadowIncludedDomain::EventReplayHashes,
            ShadowIncludedDomain::ParquetProjectionEquality,
        ],
        deferred_domains: deferred_domains(),
        comparisons,
    })
}

async fn scan_shadow_rows(store: &ControlMvpStateStore) -> Result<BTreeMap<Vec<u8>, Bytes>> {
    Ok(store
        .scan_prefix(KEY_PREFIX.as_bytes())
        .await?
        .into_iter()
        .map(|entry| (entry.key().to_vec(), entry.value().bytes().clone()))
        .collect())
}

fn base_comparisons(
    expected: &ExpectedShadowRows,
    actual: &BTreeMap<Vec<u8>, Bytes>,
) -> Vec<ShadowComparison> {
    let unknown_keys = unknown_shadow_keys(actual);

    let object_status = if !unknown_keys.is_empty() {
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::BugDivergentResult)
    } else if rows_match_by(&expected.rows, actual, is_object_key) {
        ShadowComparisonStatus::Equivalent
    } else {
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::BugDivergentResult)
    };
    let name_index_status = if !expected.source_gaps.is_empty() {
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::CurrentStateGap)
    } else if rows_match_by(&expected.rows, actual, is_name_index_key) {
        ShadowComparisonStatus::Equivalent
    } else {
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::BugDivergentResult)
    };
    let watermark_status = if rows_match_by(&expected.rows, actual, is_manifest_watermark_key) {
        ShadowComparisonStatus::Equivalent
    } else {
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::StaleProjection)
    };

    vec![
        ShadowComparison {
            domain: ShadowComparisonDomain::Objects,
            status: object_status,
            detail: if unknown_keys.is_empty() {
                comparison_detail(object_status, "catalog object records")
            } else {
                format!(
                    "catalog object records diverged from current published state: unknown shadow row(s): {}",
                    unknown_keys.join(", ")
                )
            },
        },
        ShadowComparison {
            domain: ShadowComparisonDomain::NameIndexes,
            status: name_index_status,
            detail: if expected.source_gaps.is_empty() {
                comparison_detail(name_index_status, "catalog normalized name indexes")
            } else {
                expected.source_gaps.join("; ")
            },
        },
        ShadowComparison {
            domain: ShadowComparisonDomain::ManifestWatermark,
            status: watermark_status,
            detail: comparison_detail(watermark_status, "catalog manifest watermark metadata"),
        },
    ]
}

fn simple_row_comparison(
    domain: ShadowComparisonDomain,
    expected: &ExpectedShadowRows,
    actual: &BTreeMap<Vec<u8>, Bytes>,
    predicate: fn(&[u8]) -> bool,
    description: &str,
) -> ShadowComparison {
    let status = if rows_match_by(&expected.rows, actual, predicate) {
        ShadowComparisonStatus::Equivalent
    } else {
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::BugDivergentResult)
    };
    ShadowComparison {
        domain,
        status,
        detail: comparison_detail(status, description),
    }
}

async fn storage_governance_comparison(
    storage: &ScopedStorage,
    source: &CatalogShadowExtendedSource,
    expected: &ExpectedShadowRows,
    actual: &BTreeMap<Vec<u8>, Bytes>,
) -> ShadowComparison {
    // Import-fidelity failures are bugs regardless of projection status. This
    // row-level check compares rows derived by the same ledger replay on both
    // sides: it proves control-store round-trip fidelity only; the published
    // projection below is the independent evidence.
    if !rows_match_by(&expected.rows, actual, is_governance_key) {
        let status = ShadowComparisonStatus::Difference(ShadowDifferenceClass::BugDivergentResult);
        return ShadowComparison {
            domain: ShadowComparisonDomain::StorageGovernance,
            status,
            detail: comparison_detail(
                status,
                "storage-governance records round-tripped through the control store (same ledger replay derives both sides: store round-trip fidelity, not an independent derivation)",
            ),
        };
    }

    match load_published_storage_governance(storage).await {
        Ok(published) => match &source.metastore {
            Some(metastore) => match StorageGovernanceState::from_metastore_state(metastore) {
                Ok(replayed) if governance_states_agree(&replayed, &published.state) => {
                    ShadowComparison {
                        domain: ShadowComparisonDomain::StorageGovernance,
                        status: ShadowComparisonStatus::Equivalent,
                        detail: "storage-governance ledger replay, shadow rows, and the published projection agree".to_string(),
                    }
                }
                Ok(_) => ShadowComparison {
                    domain: ShadowComparisonDomain::StorageGovernance,
                    status: ShadowComparisonStatus::Difference(
                        ShadowDifferenceClass::BugDivergentResult,
                    ),
                    detail: "published storage-governance projection diverges from metastore ledger replay".to_string(),
                },
                Err(error) => ShadowComparison {
                    domain: ShadowComparisonDomain::StorageGovernance,
                    status: ShadowComparisonStatus::Difference(
                        ShadowDifferenceClass::CurrentStateGap,
                    ),
                    detail: format!(
                        "metastore ledger replay could not build governance state: {error}"
                    ),
                },
            },
            None => ShadowComparison {
                domain: ShadowComparisonDomain::StorageGovernance,
                status: ShadowComparisonStatus::Difference(
                    ShadowDifferenceClass::BugDivergentResult,
                ),
                detail: "a storage-governance projection is published but the metastore ledger has no events".to_string(),
            },
        },
        Err(error) => {
            let message = error.to_string();
            if message.contains("stale") {
                ShadowComparison {
                    domain: ShadowComparisonDomain::StorageGovernance,
                    status: ShadowComparisonStatus::Difference(
                        ShadowDifferenceClass::StaleProjection,
                    ),
                    detail: format!("published storage-governance projection is stale: {message}"),
                }
            } else {
                ShadowComparison {
                    domain: ShadowComparisonDomain::StorageGovernance,
                    status: ShadowComparisonStatus::Difference(
                        ShadowDifferenceClass::CurrentStateGap,
                    ),
                    detail: format!(
                        "no published storage-governance projection is available (#362: no production component publishes it, so deployed vending is deny-closed): {message}"
                    ),
                }
            }
        }
    }
}

fn governance_states_agree(a: &StorageGovernanceState, b: &StorageGovernanceState) -> bool {
    a.list_storage_credentials() == b.list_storage_credentials()
        && a.list_external_locations() == b.list_external_locations()
        && a.list_managed_roots() == b.list_managed_roots()
        && a.list_workspace_bindings() == b.list_workspace_bindings()
}

async fn idempotency_comparison(
    storage: &ScopedStorage,
    source: &CatalogShadowExtendedSource,
    expected: &ExpectedShadowRows,
    actual: &BTreeMap<Vec<u8>, Bytes>,
) -> Result<ShadowComparison> {
    // Row-level check: both sides carry the same imported marker bytes, so a
    // mismatch is a control-store round-trip fidelity bug, not an independent
    // re-derivation of the markers.
    if !rows_match_by(&expected.rows, actual, is_idempotency_key) {
        let status = ShadowComparisonStatus::Difference(ShadowDifferenceClass::BugDivergentResult);
        return Ok(ShadowComparison {
            domain: ShadowComparisonDomain::IdempotencyRecords,
            status,
            detail: comparison_detail(
                status,
                "catalog idempotency markers round-tripped through the control store (store round-trip fidelity, not an independent derivation)",
            ),
        });
    }

    // Re-enumerate now (operator lane): markers written since import are
    // shadow staleness, not divergence.
    let marker_prefix = format!("{CATALOG_IDEMPOTENCY_PREFIX}/");
    let mut current = BTreeMap::new();
    for path in storage.list(&marker_prefix).await? {
        let relative = path.as_str();
        let Some(suffix) = relative.strip_prefix(&marker_prefix) else {
            continue;
        };
        current.insert(
            suffix.to_string(),
            storage.get_raw(relative).await?.to_vec(),
        );
    }
    if current == source.idempotency {
        Ok(ShadowComparison {
            domain: ShadowComparisonDomain::IdempotencyRecords,
            status: ShadowComparisonStatus::Equivalent,
            detail: format!(
                "catalog idempotency markers are equivalent ({} marker(s))",
                current.len()
            ),
        })
    } else {
        Ok(ShadowComparison {
            domain: ShadowComparisonDomain::IdempotencyRecords,
            status: ShadowComparisonStatus::Difference(ShadowDifferenceClass::StaleProjection),
            detail: "idempotency markers changed after the shadow import; re-import to refresh"
                .to_string(),
        })
    }
}

async fn event_replay_hash_comparison(
    storage: &ScopedStorage,
    source: &CatalogShadowExtendedSource,
    expected: &ExpectedShadowRows,
    actual: &BTreeMap<Vec<u8>, Bytes>,
) -> Result<ShadowComparison> {
    if !rows_match_by(&expected.rows, actual, is_replay_witness_key) {
        let status = ShadowComparisonStatus::Difference(ShadowDifferenceClass::BugDivergentResult);
        return Ok(ShadowComparison {
            domain: ShadowComparisonDomain::EventReplayHashes,
            status,
            detail: comparison_detail(status, "catalog commit event-witness chains"),
        });
    }

    let mut witnessed_commits = 0usize;
    let mut verified_events = 0usize;
    let mut missing_events = Vec::new();
    let mut mismatched_events = Vec::new();
    for commit in &source.base.state().commits {
        let Some(witnesses_json) = &commit.event_witnesses_json else {
            continue;
        };
        witnessed_commits += 1;
        let witnesses = decode_catalog_commit_event_witnesses(witnesses_json)?;
        for witness in witnesses {
            let key = LedgerKey::event(CatalogDomain::Catalog, &witness.event_id);
            match storage.head_raw(key.as_ref()).await? {
                Some(_) => {
                    let bytes = storage.get_raw(key.as_ref()).await?;
                    let mut hasher = Sha256::new();
                    hasher.update(&bytes);
                    let digest = format!("sha256:{}", hex::encode(hasher.finalize()));
                    if digest == witness.event_sha256 {
                        verified_events += 1;
                    } else {
                        mismatched_events.push(witness.event_id.clone());
                    }
                }
                None => missing_events.push(witness.event_id.clone()),
            }
        }
    }

    Ok(if witnessed_commits == 0 {
        ShadowComparison {
            domain: ShadowComparisonDomain::EventReplayHashes,
            status: ShadowComparisonStatus::Difference(ShadowDifferenceClass::CurrentStateGap),
            detail: "no witnessed commits exist in the retained snapshot; event replay hashes have no source".to_string(),
        }
    } else if !mismatched_events.is_empty() {
        ShadowComparison {
            domain: ShadowComparisonDomain::EventReplayHashes,
            status: ShadowComparisonStatus::Difference(ShadowDifferenceClass::BugDivergentResult),
            detail: format!(
                "ledger event bytes do not match their commit witnesses: {}",
                mismatched_events.join(", ")
            ),
        }
    } else if !missing_events.is_empty() {
        ShadowComparison {
            domain: ShadowComparisonDomain::EventReplayHashes,
            status: ShadowComparisonStatus::Difference(ShadowDifferenceClass::CurrentStateGap),
            detail: format!(
                "witnessed ledger events are no longer retained: {}",
                missing_events.join(", ")
            ),
        }
    } else {
        ShadowComparison {
            domain: ShadowComparisonDomain::EventReplayHashes,
            status: ShadowComparisonStatus::Equivalent,
            detail: format!(
                "event replay hashes verified against ledger bytes ({verified_events} event(s) across {witnessed_commits} commit(s))"
            ),
        }
    })
}

async fn parquet_equality_comparison(
    storage: &ScopedStorage,
    source: &CatalogShadowExtendedSource,
    actual: &BTreeMap<Vec<u8>, Bytes>,
) -> ShadowComparison {
    let identity = source.base.identity().clone();
    match tier1_state::load_catalog_state(storage, &identity.snapshot_path).await {
        Ok(reloaded) => {
            let synthetic = CatalogShadowSource {
                identity,
                state: reloaded,
            };
            match build_expected_shadow_rows(&synthetic) {
                Ok(reloaded_expected) => {
                    let matches = rows_match_by(&reloaded_expected.rows, actual, is_object_key)
                        && rows_match_by(&reloaded_expected.rows, actual, is_name_index_key);
                    if matches {
                        ShadowComparison {
                            domain: ShadowComparisonDomain::ParquetProjectionEquality,
                            status: ShadowComparisonStatus::Equivalent,
                            detail: format!(
                                "a second Parquet snapshot load through the same loader at watermark {} matches the shadow rows (load determinism and store round-trip fidelity, not an independent implementation)",
                                synthetic.identity.snapshot_version
                            ),
                        }
                    } else {
                        ShadowComparison {
                            domain: ShadowComparisonDomain::ParquetProjectionEquality,
                            status: ShadowComparisonStatus::Difference(
                                ShadowDifferenceClass::BugDivergentResult,
                            ),
                            detail: "a second Parquet snapshot load through the same loader diverges from the shadow rows".to_string(),
                        }
                    }
                }
                Err(error) => ShadowComparison {
                    domain: ShadowComparisonDomain::ParquetProjectionEquality,
                    status: ShadowComparisonStatus::Difference(
                        ShadowDifferenceClass::CurrentStateGap,
                    ),
                    detail: format!("reloaded snapshot rows could not be built: {error}"),
                },
            }
        }
        Err(error) => ShadowComparison {
            domain: ShadowComparisonDomain::ParquetProjectionEquality,
            status: ShadowComparisonStatus::Difference(ShadowDifferenceClass::CurrentStateGap),
            detail: format!("the published Parquet snapshot could not be reloaded: {error}"),
        },
    }
}

fn stable_pointer_version(pointer_path: &str, before: &str, after: &str) -> Result<String> {
    if before == after {
        return Ok(before.to_string());
    }

    Err(CatalogError::PreconditionFailed {
        message: format!(
            "catalog manifest pointer {pointer_path} changed while loading catalog shadow source: version before read was {before}, version after read was {after}"
        ),
    })
}

fn object_key(kind: ShadowObjectKind, id: &str) -> Vec<u8> {
    format!("{OBJECT_PREFIX}{}/{id}", kind.path_segment()).into_bytes()
}

fn name_index_key(kind: ShadowObjectKind, parent_id: Option<&str>, name: &str) -> Vec<u8> {
    let normalized = normalize_name(name);
    let encoded_name = hex::encode(normalized.as_bytes());
    let parent = parent_id.unwrap_or("root");
    format!(
        "{INDEX_PREFIX}{}-name/{parent}/{encoded_name}",
        kind.path_segment()
    )
    .into_bytes()
}

fn manifest_watermark_key() -> Vec<u8> {
    MANIFEST_WATERMARK_KEY.as_bytes().to_vec()
}

fn encode_shadow_record<T: Serialize>(value: &T) -> Result<Bytes> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|err| CatalogError::Serialization {
            message: format!("serialize Phase 4A shadow row: {err}"),
        })
}

impl ShadowObjectKind {
    const fn path_segment(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Schema => "schema",
            Self::Table => "table",
            Self::Column => "column",
        }
    }
}

#[derive(Debug)]
struct ExpectedShadowRows {
    rows: BTreeMap<Vec<u8>, Bytes>,
    source_gaps: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ShadowNameIndexRecord<'a> {
    object_kind: &'static str,
    parent_id: Option<&'a str>,
    normalized_name: String,
    object_id: &'a str,
    source_manifest_id: &'a str,
}

fn build_expected_shadow_rows(source: &CatalogShadowSource) -> Result<ExpectedShadowRows> {
    let mut rows = BTreeMap::new();
    let mut source_gaps = Vec::new();
    let state = source.state();
    let manifest_id = source.identity().manifest_id();

    insert_catalog_shadow_rows(&mut rows, &mut source_gaps, state, manifest_id)?;
    insert_schema_shadow_rows(&mut rows, &mut source_gaps, state, manifest_id)?;
    insert_table_shadow_rows(&mut rows, &mut source_gaps, state, manifest_id)?;
    insert_column_shadow_rows(&mut rows, &mut source_gaps, state, manifest_id)?;
    rows.insert(
        manifest_watermark_key(),
        encode_shadow_record(source.identity())?,
    );

    Ok(ExpectedShadowRows { rows, source_gaps })
}

#[derive(Debug, Serialize)]
struct ShadowTablePointerRecord<'a> {
    table_id: &'a str,
    location: Option<&'a str>,
    format: Option<&'a str>,
    table_type: Option<&'a str>,
    updated_at: i64,
    source_manifest_id: &'a str,
}

fn build_extended_expected_rows(
    source: &CatalogShadowExtendedSource,
) -> Result<ExpectedShadowRows> {
    let mut expected = build_expected_shadow_rows(&source.base)?;
    let state = source.base.state();
    let manifest_id = source.base.identity().manifest_id();

    // Table current pointers: the catalog's record of each table's current
    // data pointer (location/format). The current path has no unified pointer
    // object, so this is the partial-but-honest coverage the roadmap's slice 3
    // allows ("only where current repo behavior is implemented or partial").
    for table in &state.tables {
        expected.rows.insert(
            format!("{TABLE_POINTER_PREFIX}{}", table.id).into_bytes(),
            encode_shadow_record(&ShadowTablePointerRecord {
                table_id: &table.id,
                location: table.location.as_deref(),
                format: table.format.as_deref(),
                table_type: table.table_type.as_deref(),
                updated_at: table.updated_at,
                source_manifest_id: manifest_id,
            })?,
        );
    }

    if let Some(metastore) = &source.metastore {
        for (grant_id, record) in &metastore.grants {
            expected.rows.insert(
                format!("{GRANT_PREFIX}{grant_id}").into_bytes(),
                encode_shadow_record(record)?,
            );
        }
        for (id, record) in &metastore.storage_credentials {
            expected.rows.insert(
                format!("{GOVERNANCE_PREFIX}credential/{id}").into_bytes(),
                encode_shadow_record(record)?,
            );
        }
        for (id, record) in &metastore.external_locations {
            expected.rows.insert(
                format!("{GOVERNANCE_PREFIX}external-location/{id}").into_bytes(),
                encode_shadow_record(record)?,
            );
        }
        for (id, record) in &metastore.managed_roots {
            expected.rows.insert(
                format!("{GOVERNANCE_PREFIX}managed-root/{id}").into_bytes(),
                encode_shadow_record(record)?,
            );
        }
        for (id, record) in &metastore.workspace_bindings {
            expected.rows.insert(
                format!("{GOVERNANCE_PREFIX}workspace-binding/{id}").into_bytes(),
                encode_shadow_record(record)?,
            );
        }
    }

    for (suffix, bytes) in &source.idempotency {
        expected.rows.insert(
            format!("{IDEMPOTENCY_PREFIX}{suffix}").into_bytes(),
            Bytes::from(bytes.clone()),
        );
    }

    for commit in &state.commits {
        if let Some(witnesses_json) = &commit.event_witnesses_json {
            expected.rows.insert(
                format!("{REPLAY_WITNESS_PREFIX}{}", commit.commit_ulid).into_bytes(),
                Bytes::from(witnesses_json.clone().into_bytes()),
            );
        }
    }

    Ok(expected)
}

fn insert_catalog_shadow_rows(
    rows: &mut BTreeMap<Vec<u8>, Bytes>,
    source_gaps: &mut Vec<String>,
    state: &CatalogState,
    manifest_id: &str,
) -> Result<()> {
    for catalog in &state.catalogs {
        insert_object(rows, ShadowObjectKind::Catalog, &catalog.id, catalog)?;
        insert_name_index(
            rows,
            source_gaps,
            ShadowObjectKind::Catalog,
            None,
            &catalog.name,
            &catalog.id,
            manifest_id,
        )?;
    }
    Ok(())
}

fn insert_schema_shadow_rows(
    rows: &mut BTreeMap<Vec<u8>, Bytes>,
    source_gaps: &mut Vec<String>,
    state: &CatalogState,
    manifest_id: &str,
) -> Result<()> {
    let catalog_ids = state
        .catalogs
        .iter()
        .map(|catalog| catalog.id.as_str())
        .collect::<BTreeSet<_>>();
    let default_catalog_id = default_catalog_id(state);
    for namespace in &state.namespaces {
        insert_object(rows, ShadowObjectKind::Schema, &namespace.id, namespace)?;
        let parent_id = match namespace.catalog_id.as_deref() {
            Some(catalog_id) if catalog_ids.contains(catalog_id) => catalog_id,
            Some(catalog_id) => {
                source_gaps.push(format!(
                    "schema {} references missing catalog {}",
                    namespace.id, catalog_id
                ));
                continue;
            }
            None => {
                if let Some(catalog_id) = default_catalog_id {
                    catalog_id
                } else {
                    source_gaps.push(format!(
                        "schema {} has legacy/default catalog_id but no default catalog exists",
                        namespace.id
                    ));
                    continue;
                }
            }
        };
        insert_name_index(
            rows,
            source_gaps,
            ShadowObjectKind::Schema,
            Some(parent_id),
            &namespace.name,
            &namespace.id,
            manifest_id,
        )?;
    }
    Ok(())
}

fn insert_table_shadow_rows(
    rows: &mut BTreeMap<Vec<u8>, Bytes>,
    source_gaps: &mut Vec<String>,
    state: &CatalogState,
    manifest_id: &str,
) -> Result<()> {
    let namespace_ids = state
        .namespaces
        .iter()
        .map(|namespace| namespace.id.as_str())
        .collect::<BTreeSet<_>>();
    for table in &state.tables {
        insert_object(rows, ShadowObjectKind::Table, &table.id, table)?;
        if !namespace_ids.contains(table.namespace_id.as_str()) {
            source_gaps.push(format!(
                "table {} references missing schema {}",
                table.id, table.namespace_id
            ));
            continue;
        }
        insert_name_index(
            rows,
            source_gaps,
            ShadowObjectKind::Table,
            Some(&table.namespace_id),
            &table.name,
            &table.id,
            manifest_id,
        )?;
    }
    Ok(())
}

fn insert_column_shadow_rows(
    rows: &mut BTreeMap<Vec<u8>, Bytes>,
    source_gaps: &mut Vec<String>,
    state: &CatalogState,
    manifest_id: &str,
) -> Result<()> {
    let table_ids = state
        .tables
        .iter()
        .map(|table| table.id.as_str())
        .collect::<BTreeSet<_>>();
    for column in &state.columns {
        insert_object(rows, ShadowObjectKind::Column, &column.id, column)?;
        if !table_ids.contains(column.table_id.as_str()) {
            source_gaps.push(format!(
                "column {} references missing table {}",
                column.id, column.table_id
            ));
            continue;
        }
        insert_name_index(
            rows,
            source_gaps,
            ShadowObjectKind::Column,
            Some(&column.table_id),
            &column.name,
            &column.id,
            manifest_id,
        )?;
    }
    Ok(())
}

fn default_catalog_id(state: &CatalogState) -> Option<&str> {
    state
        .catalogs
        .iter()
        .find(|catalog| catalog.name == "default")
        .map(|catalog| catalog.id.as_str())
}

fn insert_object<T: Serialize>(
    rows: &mut BTreeMap<Vec<u8>, Bytes>,
    kind: ShadowObjectKind,
    id: &str,
    record: &T,
) -> Result<()> {
    rows.insert(object_key(kind, id), encode_shadow_record(record)?);
    Ok(())
}

fn insert_name_index(
    rows: &mut BTreeMap<Vec<u8>, Bytes>,
    source_gaps: &mut Vec<String>,
    kind: ShadowObjectKind,
    parent_id: Option<&str>,
    name: &str,
    object_id: &str,
    source_manifest_id: &str,
) -> Result<()> {
    let key = name_index_key(kind, parent_id, name);
    let value = encode_shadow_record(&ShadowNameIndexRecord {
        object_kind: kind.path_segment(),
        parent_id,
        normalized_name: normalize_name(name),
        object_id,
        source_manifest_id,
    })?;
    if let Some(existing) = rows.get(&key) {
        if existing != &value {
            source_gaps.push(format!(
                "duplicate {} name index for parent {} and name {}",
                kind.path_segment(),
                parent_id.unwrap_or("root"),
                normalize_name(name)
            ));
        }
        return Ok(());
    }
    rows.insert(key, value);
    Ok(())
}

fn normalize_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

fn rows_match_by(
    expected: &BTreeMap<Vec<u8>, Bytes>,
    actual: &BTreeMap<Vec<u8>, Bytes>,
    predicate: fn(&[u8]) -> bool,
) -> bool {
    let expected_rows = expected
        .iter()
        .filter(|(key, _value)| predicate(key))
        .collect::<BTreeMap<_, _>>();
    let actual_rows = actual
        .iter()
        .filter(|(key, _value)| predicate(key))
        .collect::<BTreeMap<_, _>>();
    expected_rows == actual_rows
}

fn is_object_key(key: &[u8]) -> bool {
    key.starts_with(OBJECT_PREFIX.as_bytes())
}

fn is_name_index_key(key: &[u8]) -> bool {
    key.starts_with(INDEX_PREFIX.as_bytes())
}

fn is_manifest_watermark_key(key: &[u8]) -> bool {
    key == MANIFEST_WATERMARK_KEY.as_bytes()
}

fn is_table_pointer_key(key: &[u8]) -> bool {
    key.starts_with(TABLE_POINTER_PREFIX.as_bytes())
}

fn is_grant_key(key: &[u8]) -> bool {
    key.starts_with(GRANT_PREFIX.as_bytes())
}

fn is_governance_key(key: &[u8]) -> bool {
    key.starts_with(GOVERNANCE_PREFIX.as_bytes())
}

fn is_idempotency_key(key: &[u8]) -> bool {
    key.starts_with(IDEMPOTENCY_PREFIX.as_bytes())
}

fn is_replay_witness_key(key: &[u8]) -> bool {
    key.starts_with(REPLAY_WITNESS_PREFIX.as_bytes())
}

fn unknown_shadow_keys(actual: &BTreeMap<Vec<u8>, Bytes>) -> Vec<String> {
    actual
        .keys()
        .filter(|key| {
            key.starts_with(KEY_PREFIX.as_bytes())
                && !is_object_key(key)
                && !is_name_index_key(key)
                && !is_manifest_watermark_key(key)
                && !is_table_pointer_key(key)
                && !is_grant_key(key)
                && !is_governance_key(key)
                && !is_idempotency_key(key)
                && !is_replay_witness_key(key)
        })
        .map(|key| String::from_utf8_lossy(key).into_owned())
        .collect()
}

fn comparison_detail(status: ShadowComparisonStatus, domain: &str) -> String {
    match status {
        ShadowComparisonStatus::Equivalent => format!("{domain} are equivalent"),
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::CurrentStateGap) => {
            format!("{domain} cannot be compared because source state has missing inputs")
        }
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::UnsupportedScope) => {
            format!("{domain} is outside Phase 4A scope")
        }
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::StaleProjection) => {
            format!("{domain} is stale")
        }
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::BugDivergentResult) => {
            format!("{domain} diverged from current published state")
        }
    }
}

fn deferred_domains() -> Vec<ShadowDeferredEntry> {
    vec![ShadowDeferredEntry {
        domain: ShadowDeferredDomain::FullProjectionWatermarks,
        status: ShadowComparisonStatus::Difference(ShadowDifferenceClass::UnsupportedScope),
        reason: "full projection watermarks are deferred beyond the catalog manifest watermark metadata imported here".to_string(),
    }]
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use arco_core::storage::{
        MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
    };
    use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};
    use async_trait::async_trait;
    use chrono::Utc;
    use serde_json::json;

    use super::super::{ArcoStateReader, ArcoStateStore, TxnOptions};
    use super::*;
    use crate::manifest::{
        CatalogDomainManifest, DomainManifestPointer, compute_manifest_hash, format_manifest_id,
    };
    use crate::parquet_util::{CatalogRecord, ColumnRecord, NamespaceRecord, TableRecord};
    use crate::tier1_snapshot;

    fn storage() -> (Arc<MemoryBackend>, ScopedStorage) {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scoped storage");
        (backend, storage)
    }

    struct PointerRaceBackend {
        inner: Arc<MemoryBackend>,
        pointer_path: String,
        raced: AtomicBool,
    }

    impl PointerRaceBackend {
        fn new(inner: Arc<MemoryBackend>, pointer_path: String) -> Self {
            Self {
                inner,
                pointer_path,
                raced: AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl StorageBackend for PointerRaceBackend {
        async fn get(&self, path: &str) -> arco_core::error::Result<Bytes> {
            let bytes = self.inner.get(path).await?;
            if path.ends_with(&self.pointer_path) && !self.raced.swap(true, Ordering::SeqCst) {
                self.inner
                    .put(
                        path,
                        Bytes::from_static(
                            br#"{"manifest_id":"changed","manifest_path":"manifests/catalog/changed.json","epoch":5,"parent_pointer_hash":null,"updated_at":"2026-07-06T00:00:00Z"}"#,
                        ),
                        WritePrecondition::None,
                    )
                    .await?;
            }
            Ok(bytes)
        }

        async fn get_range(
            &self,
            path: &str,
            range: Range<u64>,
        ) -> arco_core::error::Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::error::Result<WriteResult> {
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::error::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::error::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> arco_core::error::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(
            &self,
            path: &str,
            expiry: Duration,
        ) -> arco_core::error::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    fn fixture_state() -> CatalogState {
        CatalogState {
            catalogs: vec![CatalogRecord {
                id: "cat-1".to_string(),
                name: "Main".to_string(),
                description: Some("Main catalog".to_string()),
                created_at: 10,
                updated_at: 11,
                properties_json: Some(r#"{"tier":"gold"}"#.to_string()),
                storage_root: Some("s3://warehouse/main".to_string()),
            }],
            namespaces: vec![NamespaceRecord {
                id: "schema-1".to_string(),
                catalog_id: Some("cat-1".to_string()),
                name: "Sales".to_string(),
                description: Some("Sales schema".to_string()),
                created_at: 12,
                updated_at: 13,
                properties_json: None,
                storage_root: None,
            }],
            tables: vec![TableRecord {
                id: "table-1".to_string(),
                namespace_id: "schema-1".to_string(),
                name: "Orders".to_string(),
                description: Some("Orders table".to_string()),
                location: Some("s3://warehouse/main/sales/orders".to_string()),
                format: Some("delta".to_string()),
                created_at: 14,
                updated_at: 15,
                table_type: Some("MANAGED".to_string()),
                properties_json: Some(r#"{"owner":"analytics"}"#.to_string()),
            }],
            columns: vec![ColumnRecord {
                id: "column-1".to_string(),
                table_id: "table-1".to_string(),
                name: "Order_ID".to_string(),
                data_type: "string".to_string(),
                is_nullable: false,
                ordinal: 0,
                description: Some("Order identifier".to_string()),
            }],
            commits: Vec::new(),
        }
    }

    async fn publish_catalog_fixture(
        storage: &ScopedStorage,
        state: CatalogState,
    ) -> CatalogShadowSourceIdentity {
        let snapshot = tier1_snapshot::write_catalog_snapshot(storage, 7, &state)
            .await
            .expect("write catalog snapshot");
        let manifest_id = format_manifest_id(snapshot.version);
        let manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, &manifest_id);
        let manifest = CatalogDomainManifest {
            manifest_id: manifest_id.clone(),
            epoch: 4,
            previous_manifest_path: Some("manifests/catalog/00000000000000000006.json".to_string()),
            writer_session_id: Some("phase4a-test".to_string()),
            snapshot_version: snapshot.version,
            snapshot_path: snapshot.path.clone(),
            snapshot: Some(snapshot),
            watermark_event_id: Some("catalog-event-0007".to_string()),
            last_commit_id: Some("commit-0007".to_string()),
            fencing_token: Some(4),
            commit_ulid: Some("01JPHASE4ASHADOW0007".to_string()),
            parent_hash: None,
            updated_at: Utc::now(),
        };
        let manifest_bytes = Bytes::from(serde_json::to_vec(&manifest).expect("manifest json"));
        storage
            .put_raw(
                &manifest_path,
                manifest_bytes,
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write catalog manifest");

        let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
        let pointer = DomainManifestPointer {
            manifest_id: manifest_id.clone(),
            manifest_path: manifest_path.clone(),
            epoch: 4,
            parent_pointer_hash: None,
            updated_at: Utc::now(),
        };
        let pointer_bytes = Bytes::from(serde_json::to_vec(&pointer).expect("pointer json"));
        storage
            .put_raw(
                &pointer_path,
                pointer_bytes.clone(),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write catalog pointer");
        let pointer_meta = storage
            .head_raw(&pointer_path)
            .await
            .expect("head catalog pointer")
            .expect("catalog pointer metadata");

        CatalogShadowSourceIdentity {
            pointer_path,
            pointer_version: pointer_meta.version,
            pointer_manifest_id: manifest_id.clone(),
            pointer_manifest_path: manifest_path,
            pointer_hash: compute_manifest_hash(&pointer_bytes),
            manifest_id,
            snapshot_version: 7,
            snapshot_path: "snapshots/catalog/v7/".to_string(),
            watermark_event_id: Some("catalog-event-0007".to_string()),
            last_commit_id: Some("commit-0007".to_string()),
        }
    }

    fn comparison_status(
        report: &ShadowReplayReport,
        domain: ShadowComparisonDomain,
    ) -> ShadowComparisonStatus {
        comparison(report, domain).status()
    }

    fn comparison(
        report: &ShadowReplayReport,
        domain: ShadowComparisonDomain,
    ) -> &ShadowComparison {
        report
            .comparisons()
            .iter()
            .find(|comparison| comparison.domain() == domain)
            .expect("comparison domain")
    }

    #[tokio::test]
    async fn imports_current_catalog_objects_and_name_indexes_into_shadow_scope() {
        let (_backend, storage) = storage();
        let state = fixture_state();
        let expected_source = publish_catalog_fixture(&storage, state.clone()).await;

        let report = import_current_catalog_shadow(&storage)
            .await
            .expect("import shadow state");

        assert_eq!(&expected_source, report.source());
        assert_eq!(
            &[
                ShadowIncludedDomain::Objects,
                ShadowIncludedDomain::NameIndexes,
                ShadowIncludedDomain::ManifestWatermark,
                ShadowIncludedDomain::TableCurrentPointers,
                ShadowIncludedDomain::GrantsOwnership,
                ShadowIncludedDomain::StorageGovernance,
                ShadowIncludedDomain::IdempotencyRecords,
                ShadowIncludedDomain::EventReplayHashes,
                ShadowIncludedDomain::ParquetProjectionEquality,
            ],
            report.included_domains()
        );
        assert!(comparison_status(&report, ShadowComparisonDomain::Objects).is_equivalent());
        assert!(comparison_status(&report, ShadowComparisonDomain::NameIndexes).is_equivalent());
        assert!(
            comparison_status(&report, ShadowComparisonDomain::ManifestWatermark).is_equivalent()
        );
        assert!(
            comparison_status(&report, ShadowComparisonDomain::TableCurrentPointers)
                .is_equivalent()
        );
        // The fixture has no metastore ledger, no published governance
        // projection, and no witnessed commits: those domains classify as
        // current-state gaps rather than fake equivalence.
        assert_eq!(
            Some(ShadowDifferenceClass::CurrentStateGap),
            comparison_status(&report, ShadowComparisonDomain::GrantsOwnership).difference_class()
        );
        assert_eq!(
            Some(ShadowDifferenceClass::CurrentStateGap),
            comparison_status(&report, ShadowComparisonDomain::StorageGovernance)
                .difference_class()
        );
        assert!(
            comparison(&report, ShadowComparisonDomain::StorageGovernance)
                .detail()
                .contains("#362"),
            "storage-governance gap must cite the missing production publisher"
        );
        assert!(
            comparison_status(&report, ShadowComparisonDomain::IdempotencyRecords).is_equivalent()
        );
        assert_eq!(
            Some(ShadowDifferenceClass::CurrentStateGap),
            comparison_status(&report, ShadowComparisonDomain::EventReplayHashes)
                .difference_class()
        );
        assert!(
            comparison_status(&report, ShadowComparisonDomain::ParquetProjectionEquality)
                .is_equivalent()
        );

        let shadow = open_catalog_shadow_store(&storage).expect("shadow store");
        assert_eq!(
            Some(encode_shadow_record(&state.catalogs[0]).expect("catalog bytes")),
            shadow
                .get(&object_key(ShadowObjectKind::Catalog, "cat-1"))
                .await
                .expect("read shadow catalog")
        );
        assert_eq!(
            Some(encode_shadow_record(&state.namespaces[0]).expect("schema bytes")),
            shadow
                .get(&object_key(ShadowObjectKind::Schema, "schema-1"))
                .await
                .expect("read shadow schema")
        );
        assert_eq!(
            Some(encode_shadow_record(&state.tables[0]).expect("table bytes")),
            shadow
                .get(&object_key(ShadowObjectKind::Table, "table-1"))
                .await
                .expect("read shadow table")
        );
        assert_eq!(
            Some(encode_shadow_record(&state.columns[0]).expect("column bytes")),
            shadow
                .get(&object_key(ShadowObjectKind::Column, "column-1"))
                .await
                .expect("read shadow column")
        );
        assert!(
            shadow
                .get(&name_index_key(ShadowObjectKind::Catalog, None, "MAIN"))
                .await
                .expect("read catalog name index")
                .is_some()
        );
        assert!(
            shadow
                .get(&name_index_key(
                    ShadowObjectKind::Schema,
                    Some("cat-1"),
                    "sales"
                ))
                .await
                .expect("read schema name index")
                .is_some()
        );
        assert!(
            shadow
                .get(&name_index_key(
                    ShadowObjectKind::Table,
                    Some("schema-1"),
                    "orders"
                ))
                .await
                .expect("read table name index")
                .is_some()
        );
        assert!(
            shadow
                .get(&name_index_key(
                    ShadowObjectKind::Column,
                    Some("table-1"),
                    "order_id"
                ))
                .await
                .expect("read column name index")
                .is_some()
        );
    }

    #[tokio::test]
    async fn current_catalog_pointer_bytes_and_version_are_unchanged_after_shadow_import() {
        let (_backend, storage) = storage();
        publish_catalog_fixture(&storage, fixture_state()).await;
        let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
        let before_bytes = storage
            .get_raw(&pointer_path)
            .await
            .expect("current pointer bytes before import");
        let before_version = storage
            .head_raw(&pointer_path)
            .await
            .expect("current pointer metadata before import")
            .expect("current pointer before import")
            .version;

        import_current_catalog_shadow(&storage)
            .await
            .expect("import shadow state");

        assert_eq!(
            before_bytes,
            storage
                .get_raw(&pointer_path)
                .await
                .expect("current pointer bytes after import")
        );
        assert_eq!(
            before_version,
            storage
                .head_raw(&pointer_path)
                .await
                .expect("current pointer metadata after import")
                .expect("current pointer after import")
                .version
        );
    }

    #[tokio::test]
    async fn load_source_fails_if_pointer_version_changes_while_reading() {
        let (backend, storage) = storage();
        publish_catalog_fixture(&storage, fixture_state()).await;
        let raced_storage = ScopedStorage::new(
            Arc::new(PointerRaceBackend::new(
                backend,
                CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog),
            )),
            "tenant",
            "workspace",
        )
        .expect("raced scoped storage");

        let err = load_current_catalog_shadow_source(&raced_storage)
            .await
            .expect_err("pointer version race must fail closed");

        assert!(
            matches!(err, CatalogError::PreconditionFailed { ref message } if message.contains("changed while loading catalog shadow source")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn legacy_default_schema_indexes_under_default_catalog_when_available() {
        let (_backend, storage) = storage();
        let mut state = fixture_state();
        state.catalogs[0].id = "cat-default".to_string();
        state.catalogs[0].name = "default".to_string();
        state.namespaces[0].catalog_id = None;
        publish_catalog_fixture(&storage, state).await;

        let report = import_current_catalog_shadow(&storage)
            .await
            .expect("import shadow state");

        assert!(comparison_status(&report, ShadowComparisonDomain::NameIndexes).is_equivalent());
        let shadow = open_catalog_shadow_store(&storage).expect("shadow store");
        assert!(
            shadow
                .get(&name_index_key(
                    ShadowObjectKind::Schema,
                    Some("cat-default"),
                    "sales"
                ))
                .await
                .expect("read default-catalog schema index")
                .is_some()
        );
        assert!(
            shadow
                .get(&name_index_key(
                    ShadowObjectKind::Schema,
                    Some(LEGACY_DEFAULT_CATALOG_PARENT),
                    "sales"
                ))
                .await
                .expect("read synthetic legacy schema index")
                .is_none()
        );
    }

    #[tokio::test]
    async fn legacy_default_schema_without_default_catalog_is_current_state_gap() {
        let (_backend, storage) = storage();
        let mut state = fixture_state();
        state.namespaces[0].catalog_id = None;
        publish_catalog_fixture(&storage, state).await;

        let report = import_current_catalog_shadow(&storage)
            .await
            .expect("import shadow state");

        let name_indexes = comparison(&report, ShadowComparisonDomain::NameIndexes);
        assert_eq!(
            Some(ShadowDifferenceClass::CurrentStateGap),
            name_indexes.status().difference_class()
        );
        assert!(
            name_indexes
                .detail()
                .contains("legacy/default catalog_id but no default catalog exists"),
            "unexpected detail: {}",
            name_indexes.detail()
        );
    }

    #[tokio::test]
    async fn missing_source_parent_inputs_are_classified_as_current_state_gap() {
        let (_backend, storage) = storage();
        let mut state = fixture_state();
        state.tables[0].namespace_id = "missing-schema".to_string();
        publish_catalog_fixture(&storage, state).await;

        let report = import_current_catalog_shadow(&storage)
            .await
            .expect("import shadow state");

        assert_eq!(
            Some(ShadowDifferenceClass::CurrentStateGap),
            comparison_status(&report, ShadowComparisonDomain::NameIndexes).difference_class()
        );
    }

    #[tokio::test]
    async fn tampered_shadow_rows_are_classified_as_bug_divergent_result() {
        let (_backend, storage) = storage();
        publish_catalog_fixture(&storage, fixture_state()).await;
        let source = load_current_catalog_shadow_source(&storage)
            .await
            .expect("load current source");
        let shadow = open_catalog_shadow_store(&storage).expect("shadow store");
        import_catalog_source_into_shadow(&shadow, &source)
            .await
            .expect("import shadow state");

        let mut txn = shadow
            .begin_txn(TxnOptions::default().with_request_id("tamper"))
            .await
            .expect("begin tamper transaction");
        txn.put(
            &object_key(ShadowObjectKind::Table, "table-1"),
            Bytes::from_static(br#"{"tampered":true}"#),
        )
        .await
        .expect("tamper table row");
        txn.commit().await.expect("commit tamper");

        let report = compare_catalog_shadow(&shadow, &source)
            .await
            .expect("compare tampered shadow");

        assert_eq!(
            Some(ShadowDifferenceClass::BugDivergentResult),
            comparison_status(&report, ShadowComparisonDomain::Objects).difference_class()
        );
    }

    #[tokio::test]
    async fn unknown_extra_shadow_keys_are_classified_as_bug_divergent_result() {
        let (_backend, storage) = storage();
        publish_catalog_fixture(&storage, fixture_state()).await;
        let source = load_current_catalog_shadow_source(&storage)
            .await
            .expect("load current source");
        let shadow = open_catalog_shadow_store(&storage).expect("shadow store");
        import_catalog_source_into_shadow(&shadow, &source)
            .await
            .expect("import shadow state");

        let mut txn = shadow
            .begin_txn(TxnOptions::default().with_request_id("unknown-shadow-key"))
            .await
            .expect("begin unknown-key transaction");
        txn.put(
            b"shadow/catalog/unknown/extra",
            Bytes::from_static(br#"{"unknown":true}"#),
        )
        .await
        .expect("write unknown shadow key");
        txn.commit().await.expect("commit unknown-key transaction");

        let report = compare_catalog_shadow(&shadow, &source)
            .await
            .expect("compare unknown-key shadow");
        let catalog_objects = comparison(&report, ShadowComparisonDomain::Objects);

        assert_eq!(
            Some(ShadowDifferenceClass::BugDivergentResult),
            catalog_objects.status().difference_class()
        );
        assert!(
            catalog_objects.detail().contains("unknown shadow row"),
            "unexpected detail: {}",
            catalog_objects.detail()
        );
    }

    #[tokio::test]
    async fn stale_shadow_watermark_metadata_is_classified_as_stale_projection() {
        let (_backend, storage) = storage();
        publish_catalog_fixture(&storage, fixture_state()).await;
        let source = load_current_catalog_shadow_source(&storage)
            .await
            .expect("load current source");
        let shadow = open_catalog_shadow_store(&storage).expect("shadow store");
        import_catalog_source_into_shadow(&shadow, &source)
            .await
            .expect("import shadow state");

        let mut txn = shadow
            .begin_txn(TxnOptions::default().with_request_id("stale-watermark"))
            .await
            .expect("begin stale watermark transaction");
        txn.put(
            &manifest_watermark_key(),
            Bytes::from(serde_json::to_vec(&json!({"snapshot_version": 6})).expect("json")),
        )
        .await
        .expect("write stale watermark");
        txn.commit().await.expect("commit stale watermark");

        let report = compare_catalog_shadow(&shadow, &source)
            .await
            .expect("compare stale watermark");

        assert_eq!(
            Some(ShadowDifferenceClass::StaleProjection),
            comparison_status(&report, ShadowComparisonDomain::ManifestWatermark)
                .difference_class()
        );
    }

    #[tokio::test]
    async fn deferred_domains_are_explicit_unsupported_scope_entries() {
        let (_backend, storage) = storage();
        publish_catalog_fixture(&storage, fixture_state()).await;

        let report = import_current_catalog_shadow(&storage)
            .await
            .expect("import shadow state");

        let domains = report
            .deferred_domains()
            .iter()
            .map(ShadowDeferredEntry::domain)
            .collect::<Vec<_>>();
        assert_eq!(
            vec![ShadowDeferredDomain::FullProjectionWatermarks],
            domains
        );
        for deferred in report.deferred_domains() {
            assert_eq!(
                Some(ShadowDifferenceClass::UnsupportedScope),
                deferred.status().difference_class()
            );
            assert!(!deferred.reason().is_empty());
        }
    }

    fn witnessed_event(event_id: &str, payload: &'static [u8]) -> (String, String) {
        let mut hasher = Sha256::new();
        hasher.update(payload);
        (
            event_id.to_string(),
            format!("sha256:{}", hex::encode(hasher.finalize())),
        )
    }

    async fn publish_witnessed_fixture(storage: &ScopedStorage) -> (String, &'static [u8]) {
        let event_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let payload: &'static [u8] = br#"{"event":"create_namespace"}"#;
        let ledger_key = LedgerKey::event(CatalogDomain::Catalog, event_id);
        storage
            .put_raw(
                ledger_key.as_ref(),
                Bytes::from_static(payload),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write ledger event");

        let (id, digest) = witnessed_event(event_id, payload);
        let witnesses_json =
            serde_json::to_string(&serde_json::json!([{ "eventId": id, "eventSha256": digest }]))
                .expect("witnesses json");
        let mut state = fixture_state();
        state.commits = vec![crate::parquet_util::CatalogCommitRecord {
            commit_ulid: "01JPHASE4AWITNESS000000001".to_string(),
            manifest_id: Some("manifest-witness".to_string()),
            event_witnesses_json: Some(witnesses_json),
            snapshot_version: 7,
            published_at: 1_800_000_000_000,
            fencing_token: 4,
            watermark_event_id: Some(event_id.to_string()),
            operation: Some("create_namespace".to_string()),
            object_type: Some("namespace".to_string()),
            object_id: Some("schema-1".to_string()),
            object_name: Some("Sales".to_string()),
        }];
        publish_catalog_fixture(&storage, state).await;
        (event_id.to_string(), payload)
    }

    #[tokio::test]
    async fn event_replay_hashes_verify_against_ledger_bytes_and_detect_tamper() {
        let (_backend, storage) = storage();
        let (event_id, _payload) = publish_witnessed_fixture(&storage).await;

        let report = import_current_catalog_shadow(&storage)
            .await
            .expect("import shadow state");
        let replay = comparison(&report, ShadowComparisonDomain::EventReplayHashes);
        assert!(
            replay.status().is_equivalent(),
            "expected verified witnesses, got: {}",
            replay.detail()
        );

        // Tamper the retained ledger event bytes: the witness chain must
        // classify the divergence as a bug.
        let ledger_key = LedgerKey::event(CatalogDomain::Catalog, &event_id);
        storage
            .put_raw(
                ledger_key.as_ref(),
                Bytes::from_static(br#"{"event":"tampered"}"#),
                WritePrecondition::None,
            )
            .await
            .expect("tamper ledger event");
        let source = load_extended_catalog_shadow_source(&storage)
            .await
            .expect("reload source");
        let shadow = open_catalog_shadow_store(&storage).expect("shadow store");
        let report = compare_extended_catalog_shadow(&storage, &shadow, &source)
            .await
            .expect("compare tampered ledger");
        assert_eq!(
            Some(ShadowDifferenceClass::BugDivergentResult),
            comparison_status(&report, ShadowComparisonDomain::EventReplayHashes)
                .difference_class()
        );

        // A missing (expired) ledger event is an archive gap, not a bug.
        storage
            .delete(ledger_key.as_ref())
            .await
            .expect("expire ledger event");
        let report = compare_extended_catalog_shadow(&storage, &shadow, &source)
            .await
            .expect("compare missing ledger event");
        let replay = comparison(&report, ShadowComparisonDomain::EventReplayHashes);
        assert_eq!(
            Some(ShadowDifferenceClass::CurrentStateGap),
            replay.status().difference_class()
        );
        assert!(replay.detail().contains("no longer retained"));
    }

    #[tokio::test]
    async fn grants_from_metastore_ledger_compare_equivalent_and_tampered_pointer_is_bug() {
        use crate::metastore::events::{
            GrantRecord, LifecycleState, MetastoreEvent, MetastoreMutation,
        };
        use arco_core::ControlPlaneScope;

        let (_backend, storage) = storage();
        publish_catalog_fixture(&storage, fixture_state()).await;
        let scope =
            ControlPlaneScope::workspace_alias("tenant", "workspace").expect("control plane scope");
        let ledger = MetastoreLedger::new(storage.clone());
        ledger
            .append_event(&MetastoreEvent::new_scoped(
                &scope,
                "event_001",
                1,
                MetastoreMutation::GrantUpserted(GrantRecord {
                    grant_id: "grant_01".to_string(),
                    object_id: "table-1".to_string(),
                    object_type: "TABLE".to_string(),
                    principal_id: "principal_01".to_string(),
                    privilege: "SELECT".to_string(),
                    owner: "metastore-admin".to_string(),
                    lifecycle_state: LifecycleState::Active,
                    updated_at_ms: 1_800_000_000_001,
                    properties: BTreeMap::new(),
                }),
            ))
            .await
            .expect("append grant event");

        let report = import_current_catalog_shadow(&storage)
            .await
            .expect("import shadow state");
        assert!(
            comparison_status(&report, ShadowComparisonDomain::GrantsOwnership).is_equivalent(),
            "expected grant equivalence, got: {}",
            comparison(&report, ShadowComparisonDomain::GrantsOwnership).detail()
        );

        // Tamper a shadow table-pointer row: classified as a bug.
        let shadow = open_catalog_shadow_store(&storage).expect("shadow store");
        let mut txn = shadow
            .begin_txn(TxnOptions::default().with_request_id("tamper-pointer"))
            .await
            .expect("begin tamper txn");
        txn.put(
            format!("{TABLE_POINTER_PREFIX}table-1").as_bytes(),
            Bytes::from_static(br#"{"tampered":true}"#),
        )
        .await
        .expect("tamper pointer row");
        txn.commit().await.expect("commit tamper");

        let source = load_extended_catalog_shadow_source(&storage)
            .await
            .expect("reload source");
        let report = compare_extended_catalog_shadow(&storage, &shadow, &source)
            .await
            .expect("compare tampered pointer");
        assert_eq!(
            Some(ShadowDifferenceClass::BugDivergentResult),
            comparison_status(&report, ShadowComparisonDomain::TableCurrentPointers)
                .difference_class()
        );
    }

    #[tokio::test]
    async fn storage_governance_with_published_projection_compares_equivalent() {
        use crate::metastore::events::{
            LifecycleState, MetastoreEvent, MetastoreMutation, StorageCredentialRecord,
        };
        use crate::metastore::projections::{ProjectionRegistry, build_projection_set};
        use crate::metastore::publish::publish_metastore_projection_set;
        use crate::metastore::replay::replay_events;
        use arco_core::ControlPlaneScope;

        let (_backend, storage) = storage();
        publish_catalog_fixture(&storage, fixture_state()).await;
        let scope =
            ControlPlaneScope::workspace_alias("tenant", "workspace").expect("control plane scope");
        let ledger = MetastoreLedger::new(storage.clone());
        let event = MetastoreEvent::new_scoped(
            &scope,
            "event_001",
            1,
            MetastoreMutation::StorageCredentialUpserted(StorageCredentialRecord {
                credential_id: "cred_01".to_string(),
                name: "lakehouse-prod".to_string(),
                cloud: "gcp".to_string(),
                owner: "group:data-platform".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_002,
                properties: BTreeMap::new(),
                secret_material_ref: None,
                encrypted_payload: None,
            }),
        );
        ledger.append_event(&event).await.expect("append event");
        let metastore_state = replay_events([event].iter()).expect("replay events");
        let set = build_projection_set(
            &metastore_state,
            &ProjectionRegistry::default(),
            "event_001",
        )
        .expect("projection set");
        publish_metastore_projection_set(&storage, &set, 1)
            .await
            .expect("publish projection");

        let report = import_current_catalog_shadow(&storage)
            .await
            .expect("import shadow state");
        let governance = comparison(&report, ShadowComparisonDomain::StorageGovernance);
        assert!(
            governance.status().is_equivalent(),
            "expected governance equivalence, got: {}",
            governance.detail()
        );
    }

    #[tokio::test]
    async fn idempotency_markers_written_after_import_are_stale_projection() {
        let (_backend, storage) = storage();
        publish_catalog_fixture(&storage, fixture_state()).await;
        let source = load_extended_catalog_shadow_source(&storage)
            .await
            .expect("load source");
        let shadow = open_catalog_shadow_store(&storage).expect("shadow store");
        import_extended_catalog_source_into_shadow(&shadow, &source)
            .await
            .expect("import shadow state");

        storage
            .put_raw(
                &format!("{CATALOG_IDEMPOTENCY_PREFIX}/create_namespace/ab/abcd.json"),
                Bytes::from_static(br#"{"status":"committed"}"#),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write marker after import");

        let report = compare_extended_catalog_shadow(&storage, &shadow, &source)
            .await
            .expect("compare after marker write");
        assert_eq!(
            Some(ShadowDifferenceClass::StaleProjection),
            comparison_status(&report, ShadowComparisonDomain::IdempotencyRecords)
                .difference_class()
        );
    }

    /// Larger opt-in fixture (roadmap 4A slice 6). Loudly skipped unless
    /// `ARCO_TEST_LARGE_SHADOW_FIXTURE=1`.
    #[tokio::test]
    async fn opt_in_larger_fixture_runs_all_nine_domains_end_to_end() {
        if std::env::var("ARCO_TEST_LARGE_SHADOW_FIXTURE").is_err() {
            eprintln!(
                "SKIPPED: opt_in_larger_fixture_runs_all_nine_domains_end_to_end — set ARCO_TEST_LARGE_SHADOW_FIXTURE=1 to run the larger Phase 4A fixture"
            );
            return;
        }

        let (_backend, storage) = storage();
        let mut state = fixture_state();
        state.namespaces.clear();
        state.tables.clear();
        state.columns.clear();
        for namespace_index in 0..30 {
            let namespace_id = format!("schema-{namespace_index}");
            state.namespaces.push(NamespaceRecord {
                id: namespace_id.clone(),
                catalog_id: Some("cat-1".to_string()),
                name: format!("Schema {namespace_index}"),
                description: None,
                created_at: 12,
                updated_at: 13,
                properties_json: None,
                storage_root: None,
            });
            for table_index in 0..10 {
                let table_id = format!("table-{namespace_index}-{table_index}");
                state.tables.push(TableRecord {
                    id: table_id.clone(),
                    namespace_id: namespace_id.clone(),
                    name: format!("Table {table_index}"),
                    description: None,
                    location: Some(format!("s3://warehouse/{namespace_id}/{table_id}")),
                    format: Some("delta".to_string()),
                    created_at: 14,
                    updated_at: 15,
                    table_type: Some("MANAGED".to_string()),
                    properties_json: None,
                });
                state.columns.push(ColumnRecord {
                    id: format!("column-{namespace_index}-{table_index}"),
                    table_id,
                    name: "id".to_string(),
                    data_type: "string".to_string(),
                    is_nullable: false,
                    ordinal: 0,
                    description: None,
                });
            }
        }
        publish_catalog_fixture(&storage, state).await;

        let report = import_current_catalog_shadow(&storage)
            .await
            .expect("import larger fixture");
        assert_eq!(9, report.included_domains().len());
        assert!(comparison_status(&report, ShadowComparisonDomain::Objects).is_equivalent());
        assert!(comparison_status(&report, ShadowComparisonDomain::NameIndexes).is_equivalent());
        assert!(
            comparison_status(&report, ShadowComparisonDomain::TableCurrentPointers)
                .is_equivalent()
        );
        assert!(
            comparison_status(&report, ShadowComparisonDomain::ParquetProjectionEquality)
                .is_equivalent()
        );
    }
}
