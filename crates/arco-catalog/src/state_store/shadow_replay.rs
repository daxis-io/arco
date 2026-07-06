//! Phase 4A shadow replay and projection-equivalence scaffolding.
//!
//! This module imports the current published catalog snapshot into an isolated
//! control-MVP scope and compares only the domains Phase 4A can honestly prove.

// Phase 4A keeps shadow replay crate-private until later operator wiring.
// Crate-local tests exercise the importer, report accessors, and diagnostics.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};
use bytes::Bytes;
use serde::Serialize;

use super::{ArcoStateReader, ArcoStateStore, ControlMvpStateStore, StateScope, TxnOptions};
use crate::error::{CatalogError, Result};
use crate::manifest::{CatalogDomainManifest, DomainManifestPointer, compute_manifest_hash};
use crate::state::CatalogState;
use crate::tier1_state;

const SHADOW_DOMAIN: &str = "catalog-shadow";
const KEY_PREFIX: &str = "shadow/catalog/";
const OBJECT_PREFIX: &str = "shadow/catalog/object/";
const INDEX_PREFIX: &str = "shadow/catalog/index/";
const MANIFEST_WATERMARK_KEY: &str = "shadow/catalog/metadata/source-watermark";
#[cfg(test)]
const LEGACY_DEFAULT_CATALOG_PARENT: &str = "__legacy_default_catalog__";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShadowReplayReport {
    source: CatalogShadowSourceIdentity,
    included_domains: Vec<ShadowIncludedDomain>,
    deferred_domains: Vec<ShadowDeferredEntry>,
    comparisons: Vec<ShadowComparison>,
}

impl ShadowReplayReport {
    #[must_use]
    pub(crate) fn source(&self) -> &CatalogShadowSourceIdentity {
        &self.source
    }

    #[must_use]
    pub(crate) fn included_domains(&self) -> &[ShadowIncludedDomain] {
        &self.included_domains
    }

    #[must_use]
    pub(crate) fn deferred_domains(&self) -> &[ShadowDeferredEntry] {
        &self.deferred_domains
    }

    #[must_use]
    pub(crate) fn comparisons(&self) -> &[ShadowComparison] {
        &self.comparisons
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CatalogShadowSourceIdentity {
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
    pub(crate) fn pointer_path(&self) -> &str {
        &self.pointer_path
    }

    #[must_use]
    pub(crate) fn pointer_version(&self) -> &str {
        &self.pointer_version
    }

    #[must_use]
    pub(crate) fn pointer_manifest_id(&self) -> &str {
        &self.pointer_manifest_id
    }

    #[must_use]
    pub(crate) fn pointer_manifest_path(&self) -> &str {
        &self.pointer_manifest_path
    }

    #[must_use]
    pub(crate) fn pointer_hash(&self) -> &str {
        &self.pointer_hash
    }

    #[must_use]
    pub(crate) fn manifest_id(&self) -> &str {
        &self.manifest_id
    }

    #[must_use]
    pub(crate) const fn snapshot_version(&self) -> u64 {
        self.snapshot_version
    }

    #[must_use]
    pub(crate) fn snapshot_path(&self) -> &str {
        &self.snapshot_path
    }

    #[must_use]
    pub(crate) fn watermark_event_id(&self) -> Option<&str> {
        self.watermark_event_id.as_deref()
    }

    #[must_use]
    pub(crate) fn last_commit_id(&self) -> Option<&str> {
        self.last_commit_id.as_deref()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogShadowSource {
    identity: CatalogShadowSourceIdentity,
    state: CatalogState,
}

impl CatalogShadowSource {
    #[must_use]
    pub(crate) const fn identity(&self) -> &CatalogShadowSourceIdentity {
        &self.identity
    }

    #[must_use]
    pub(crate) const fn state(&self) -> &CatalogState {
        &self.state
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ShadowIncludedDomain {
    CatalogObjects,
    CatalogNameIndexes,
    CatalogManifestWatermark,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ShadowDeferredDomain {
    TableCurrentPointers,
    GrantsOwnership,
    StorageGovernanceEquivalence,
    IdempotencyRecords,
    FullProjectionWatermarks,
    EventReplayHashes,
    ParquetProjectionEquality,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ShadowComparisonDomain {
    CatalogObjects,
    CatalogNameIndexes,
    CatalogManifestWatermark,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ShadowDifferenceClass {
    CurrentStateGap,
    UnsupportedScope,
    StaleProjection,
    BugDivergentResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShadowComparisonStatus {
    Equivalent,
    Difference(ShadowDifferenceClass),
}

impl ShadowComparisonStatus {
    #[must_use]
    pub(crate) const fn is_equivalent(self) -> bool {
        matches!(self, Self::Equivalent)
    }

    #[must_use]
    pub(crate) const fn difference_class(self) -> Option<ShadowDifferenceClass> {
        match self {
            Self::Equivalent => None,
            Self::Difference(class) => Some(class),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShadowComparison {
    domain: ShadowComparisonDomain,
    status: ShadowComparisonStatus,
    detail: String,
}

impl ShadowComparison {
    #[must_use]
    pub(crate) const fn domain(&self) -> ShadowComparisonDomain {
        self.domain
    }

    #[must_use]
    pub(crate) const fn status(&self) -> ShadowComparisonStatus {
        self.status
    }

    #[must_use]
    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShadowDeferredEntry {
    domain: ShadowDeferredDomain,
    status: ShadowComparisonStatus,
    reason: String,
}

impl ShadowDeferredEntry {
    #[must_use]
    pub(crate) const fn domain(&self) -> ShadowDeferredDomain {
        self.domain
    }

    #[must_use]
    pub(crate) const fn status(&self) -> ShadowComparisonStatus {
        self.status
    }

    #[must_use]
    pub(crate) fn reason(&self) -> &str {
        &self.reason
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShadowObjectKind {
    Catalog,
    Schema,
    Table,
    Column,
}

pub(crate) async fn import_current_catalog_shadow(
    storage: &ScopedStorage,
) -> Result<ShadowReplayReport> {
    let source = load_current_catalog_shadow_source(storage).await?;
    let shadow = open_catalog_shadow_store(storage)?;
    import_catalog_source_into_shadow(&shadow, &source).await?;
    compare_catalog_shadow(&shadow, &source).await
}

pub(crate) async fn load_current_catalog_shadow_source(
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

pub(crate) fn open_catalog_shadow_store(storage: &ScopedStorage) -> Result<ControlMvpStateStore> {
    ControlMvpStateStore::new(
        storage.clone(),
        StateScope::new(storage.tenant_id(), storage.workspace_id(), SHADOW_DOMAIN),
    )
}

pub(crate) async fn import_catalog_source_into_shadow(
    store: &ControlMvpStateStore,
    source: &CatalogShadowSource,
) -> Result<()> {
    let expected = build_expected_shadow_rows(source)?;
    let mut txn = store
        .begin_txn(TxnOptions::default().with_request_id(format!(
            "phase4a-shadow-import-{}",
            source.identity().manifest_id()
        )))
        .await?;

    for existing in txn.scan_prefix(KEY_PREFIX.as_bytes()).await? {
        if !expected.rows.contains_key(existing.key()) {
            txn.delete(existing.key()).await?;
        }
    }

    for (key, value) in expected.rows {
        txn.put(&key, value).await?;
    }

    txn.commit().await?;
    Ok(())
}

pub(crate) async fn compare_catalog_shadow(
    store: &ControlMvpStateStore,
    source: &CatalogShadowSource,
) -> Result<ShadowReplayReport> {
    let expected = build_expected_shadow_rows(source)?;
    let actual = store
        .scan_prefix(KEY_PREFIX.as_bytes())
        .await?
        .into_iter()
        .map(|entry| (entry.key().to_vec(), entry.value().bytes().clone()))
        .collect::<BTreeMap<_, _>>();

    let unknown_keys = unknown_shadow_keys(&actual);

    let object_status = if !unknown_keys.is_empty() {
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::BugDivergentResult)
    } else if rows_match_by(&expected.rows, &actual, is_object_key) {
        ShadowComparisonStatus::Equivalent
    } else {
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::BugDivergentResult)
    };
    let name_index_status = if !expected.source_gaps.is_empty() {
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::CurrentStateGap)
    } else if rows_match_by(&expected.rows, &actual, is_name_index_key) {
        ShadowComparisonStatus::Equivalent
    } else {
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::BugDivergentResult)
    };
    let watermark_status = if rows_match_by(&expected.rows, &actual, is_manifest_watermark_key) {
        ShadowComparisonStatus::Equivalent
    } else {
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::StaleProjection)
    };

    Ok(ShadowReplayReport {
        source: source.identity().clone(),
        included_domains: vec![
            ShadowIncludedDomain::CatalogObjects,
            ShadowIncludedDomain::CatalogNameIndexes,
            ShadowIncludedDomain::CatalogManifestWatermark,
        ],
        deferred_domains: deferred_domains(),
        comparisons: vec![
            ShadowComparison {
                domain: ShadowComparisonDomain::CatalogObjects,
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
                domain: ShadowComparisonDomain::CatalogNameIndexes,
                status: name_index_status,
                detail: if expected.source_gaps.is_empty() {
                    comparison_detail(name_index_status, "catalog normalized name indexes")
                } else {
                    expected.source_gaps.join("; ")
                },
            },
            ShadowComparison {
                domain: ShadowComparisonDomain::CatalogManifestWatermark,
                status: watermark_status,
                detail: comparison_detail(watermark_status, "catalog manifest watermark metadata"),
            },
        ],
    })
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

    for catalog in &state.catalogs {
        insert_object(&mut rows, ShadowObjectKind::Catalog, &catalog.id, catalog)?;
        insert_name_index(
            &mut rows,
            &mut source_gaps,
            ShadowObjectKind::Catalog,
            None,
            &catalog.name,
            &catalog.id,
            manifest_id,
        )?;
    }

    let catalog_ids = state
        .catalogs
        .iter()
        .map(|catalog| catalog.id.as_str())
        .collect::<BTreeSet<_>>();
    let default_catalog_id = default_catalog_id(state);
    for namespace in &state.namespaces {
        insert_object(
            &mut rows,
            ShadowObjectKind::Schema,
            &namespace.id,
            namespace,
        )?;
        let parent_id = match namespace.catalog_id.as_deref() {
            Some(catalog_id) if catalog_ids.contains(catalog_id) => catalog_id,
            Some(catalog_id) => {
                source_gaps.push(format!(
                    "schema {} references missing catalog {}",
                    namespace.id, catalog_id
                ));
                continue;
            }
            None => match default_catalog_id {
                Some(catalog_id) => catalog_id,
                None => {
                    source_gaps.push(format!(
                        "schema {} has legacy/default catalog_id but no default catalog exists",
                        namespace.id
                    ));
                    continue;
                }
            },
        };
        insert_name_index(
            &mut rows,
            &mut source_gaps,
            ShadowObjectKind::Schema,
            Some(parent_id),
            &namespace.name,
            &namespace.id,
            manifest_id,
        )?;
    }

    let namespace_ids = state
        .namespaces
        .iter()
        .map(|namespace| namespace.id.as_str())
        .collect::<BTreeSet<_>>();
    for table in &state.tables {
        insert_object(&mut rows, ShadowObjectKind::Table, &table.id, table)?;
        if !namespace_ids.contains(table.namespace_id.as_str()) {
            source_gaps.push(format!(
                "table {} references missing schema {}",
                table.id, table.namespace_id
            ));
            continue;
        }
        insert_name_index(
            &mut rows,
            &mut source_gaps,
            ShadowObjectKind::Table,
            Some(&table.namespace_id),
            &table.name,
            &table.id,
            manifest_id,
        )?;
    }

    let table_ids = state
        .tables
        .iter()
        .map(|table| table.id.as_str())
        .collect::<BTreeSet<_>>();
    for column in &state.columns {
        insert_object(&mut rows, ShadowObjectKind::Column, &column.id, column)?;
        if !table_ids.contains(column.table_id.as_str()) {
            source_gaps.push(format!(
                "column {} references missing table {}",
                column.id, column.table_id
            ));
            continue;
        }
        insert_name_index(
            &mut rows,
            &mut source_gaps,
            ShadowObjectKind::Column,
            Some(&column.table_id),
            &column.name,
            &column.id,
            manifest_id,
        )?;
    }

    rows.insert(
        manifest_watermark_key(),
        encode_shadow_record(source.identity())?,
    );

    Ok(ExpectedShadowRows { rows, source_gaps })
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

fn unknown_shadow_keys(actual: &BTreeMap<Vec<u8>, Bytes>) -> Vec<String> {
    actual
        .keys()
        .filter(|key| {
            key.starts_with(KEY_PREFIX.as_bytes())
                && !is_object_key(key)
                && !is_name_index_key(key)
                && !is_manifest_watermark_key(key)
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
    [
        (
            ShadowDeferredDomain::TableCurrentPointers,
            "table current-pointer equivalence is deferred until table-pointer state is routed into the shadow domain",
        ),
        (
            ShadowDeferredDomain::GrantsOwnership,
            "grant and ownership equivalence is deferred because current governance state is partial",
        ),
        (
            ShadowDeferredDomain::StorageGovernanceEquivalence,
            "storage-governance equivalence is deferred until the authoritative storage-governance scope is included",
        ),
        (
            ShadowDeferredDomain::IdempotencyRecords,
            "idempotency records are deferred until the protected mutation state migrates with the control store",
        ),
        (
            ShadowDeferredDomain::FullProjectionWatermarks,
            "full projection watermarks are deferred beyond the catalog manifest watermark metadata imported here",
        ),
        (
            ShadowDeferredDomain::EventReplayHashes,
            "event replay hashes are deferred until an event-archive replay substrate is directly reusable",
        ),
        (
            ShadowDeferredDomain::ParquetProjectionEquality,
            "Parquet projection equality is deferred until projection routing exposes comparable watermarks",
        ),
    ]
    .into_iter()
    .map(|(domain, reason)| ShadowDeferredEntry {
        domain,
        status: ShadowComparisonStatus::Difference(ShadowDifferenceClass::UnsupportedScope),
        reason: reason.to_string(),
    })
    .collect()
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
                ShadowIncludedDomain::CatalogObjects,
                ShadowIncludedDomain::CatalogNameIndexes,
                ShadowIncludedDomain::CatalogManifestWatermark,
            ],
            report.included_domains()
        );
        assert!(comparison_status(&report, ShadowComparisonDomain::CatalogObjects).is_equivalent());
        assert!(
            comparison_status(&report, ShadowComparisonDomain::CatalogNameIndexes).is_equivalent()
        );
        assert!(
            comparison_status(&report, ShadowComparisonDomain::CatalogManifestWatermark)
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

        assert!(
            comparison_status(&report, ShadowComparisonDomain::CatalogNameIndexes).is_equivalent()
        );
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

        let name_indexes = comparison(&report, ShadowComparisonDomain::CatalogNameIndexes);
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
            comparison_status(&report, ShadowComparisonDomain::CatalogNameIndexes)
                .difference_class()
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
            comparison_status(&report, ShadowComparisonDomain::CatalogObjects).difference_class()
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
        let catalog_objects = comparison(&report, ShadowComparisonDomain::CatalogObjects);

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
            comparison_status(&report, ShadowComparisonDomain::CatalogManifestWatermark)
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
            vec![
                ShadowDeferredDomain::TableCurrentPointers,
                ShadowDeferredDomain::GrantsOwnership,
                ShadowDeferredDomain::StorageGovernanceEquivalence,
                ShadowDeferredDomain::IdempotencyRecords,
                ShadowDeferredDomain::FullProjectionWatermarks,
                ShadowDeferredDomain::EventReplayHashes,
                ShadowDeferredDomain::ParquetProjectionEquality,
            ],
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
}
