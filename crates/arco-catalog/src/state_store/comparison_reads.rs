//! Phase 4B internal read-only comparison reads.

use std::future::Future;

use arco_core::ScopedStorage;

use super::shadow_replay::{
    self, ShadowComparisonDomain, ShadowComparisonStatus, ShadowDeferredDomain,
    ShadowDifferenceClass, ShadowReplayReport,
};
use crate::error::{CatalogError, Result};
use crate::reader::CatalogSnapshotDescriptor;

#[derive(Debug, Clone)]
pub(crate) struct CatalogInventoryComparisonRead {
    current: CatalogSnapshotDescriptor,
    diagnostic: CatalogInventoryComparisonDiagnostic,
}

impl CatalogInventoryComparisonRead {
    #[must_use]
    pub(crate) const fn current(&self) -> &CatalogSnapshotDescriptor {
        &self.current
    }

    #[must_use]
    pub(crate) const fn diagnostic(&self) -> &CatalogInventoryComparisonDiagnostic {
        &self.diagnostic
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogInventoryComparisonDiagnostic {
    status: CatalogInventoryComparisonStatus,
    details: Vec<CatalogInventoryComparisonDetail>,
}

impl CatalogInventoryComparisonDiagnostic {
    #[must_use]
    pub(crate) const fn status(&self) -> CatalogInventoryComparisonStatus {
        self.status
    }

    #[must_use]
    pub(crate) fn details(&self) -> &[CatalogInventoryComparisonDetail] {
        &self.details
    }

    fn from_shadow_report(
        report: &ShadowReplayReport,
        current: &CatalogSnapshotDescriptor,
    ) -> Self {
        let mut details = report
            .comparisons()
            .iter()
            .map(|comparison| {
                CatalogInventoryComparisonDetail::new(
                    comparison_domain_name(comparison.domain()),
                    status_from_shadow(comparison.status()),
                    comparison.detail(),
                )
            })
            .collect::<Vec<_>>();
        if report.source().manifest_id() != current.manifest_id
            || report.source().snapshot_version() != current.snapshot_version.as_u64()
        {
            details.push(CatalogInventoryComparisonDetail::new(
                "current_descriptor",
                CatalogInventoryComparisonStatus::StaleProjection,
                format!(
                    "shadow source {}@{} does not match current descriptor {}@{}",
                    report.source().manifest_id(),
                    report.source().snapshot_version(),
                    current.manifest_id,
                    current.snapshot_version.as_u64()
                ),
            ));
        }
        let status = details
            .iter()
            .map(CatalogInventoryComparisonDetail::status)
            .max_by_key(|status| status.rank())
            .unwrap_or(CatalogInventoryComparisonStatus::Equivalent);

        details.extend(report.deferred_domains().iter().map(|deferred| {
            CatalogInventoryComparisonDetail::new(
                deferred_domain_name(deferred.domain()),
                status_from_shadow(deferred.status()),
                deferred.reason(),
            )
        }));

        Self { status, details }
    }

    fn from_shadow_error(error: CatalogError) -> Self {
        Self {
            status: CatalogInventoryComparisonStatus::BugDivergentResult,
            details: vec![CatalogInventoryComparisonDetail::new(
                "shadow_backend",
                CatalogInventoryComparisonStatus::BugDivergentResult,
                format!("shadow comparison failed after current read succeeded: {error}"),
            )],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CatalogInventoryComparisonStatus {
    Equivalent,
    CurrentStateGap,
    UnsupportedScope,
    StaleProjection,
    BugDivergentResult,
}

impl CatalogInventoryComparisonStatus {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Equivalent => "equivalent",
            Self::CurrentStateGap => "current_state_gap",
            Self::UnsupportedScope => "unsupported_scope",
            Self::StaleProjection => "stale_projection",
            Self::BugDivergentResult => "bug_divergent_result",
        }
    }

    const fn rank(self) -> u8 {
        match self {
            Self::Equivalent => 0,
            Self::UnsupportedScope => 1,
            Self::CurrentStateGap => 2,
            Self::StaleProjection => 3,
            Self::BugDivergentResult => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogInventoryComparisonDetail {
    domain: &'static str,
    status: CatalogInventoryComparisonStatus,
    detail: String,
}

impl CatalogInventoryComparisonDetail {
    fn new(
        domain: &'static str,
        status: CatalogInventoryComparisonStatus,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            domain,
            status,
            detail: detail.into(),
        }
    }

    #[must_use]
    pub(crate) const fn domain(&self) -> &'static str {
        self.domain
    }

    #[must_use]
    pub(crate) const fn status(&self) -> CatalogInventoryComparisonStatus {
        self.status
    }

    #[must_use]
    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

pub(crate) async fn read_catalog_inventory_with_shadow_comparison<F>(
    storage: &ScopedStorage,
    current_read: F,
) -> Result<CatalogInventoryComparisonRead>
where
    F: Future<Output = Result<CatalogSnapshotDescriptor>>,
{
    let current = current_read.await?;
    let diagnostic = catalog_inventory_shadow_diagnostic(storage, &current)
        .await
        .unwrap_or_else(CatalogInventoryComparisonDiagnostic::from_shadow_error);

    Ok(CatalogInventoryComparisonRead {
        current,
        diagnostic,
    })
}

async fn catalog_inventory_shadow_diagnostic(
    storage: &ScopedStorage,
    current: &CatalogSnapshotDescriptor,
) -> Result<CatalogInventoryComparisonDiagnostic> {
    let source = shadow_replay::load_current_catalog_shadow_source(storage).await?;
    let shadow = shadow_replay::open_catalog_shadow_store(storage)?;
    let report = shadow_replay::compare_catalog_shadow(&shadow, &source).await?;
    Ok(CatalogInventoryComparisonDiagnostic::from_shadow_report(
        &report, current,
    ))
}

fn status_from_shadow(status: ShadowComparisonStatus) -> CatalogInventoryComparisonStatus {
    match status {
        ShadowComparisonStatus::Equivalent => CatalogInventoryComparisonStatus::Equivalent,
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::CurrentStateGap) => {
            CatalogInventoryComparisonStatus::CurrentStateGap
        }
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::UnsupportedScope) => {
            CatalogInventoryComparisonStatus::UnsupportedScope
        }
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::StaleProjection) => {
            CatalogInventoryComparisonStatus::StaleProjection
        }
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::BugDivergentResult) => {
            CatalogInventoryComparisonStatus::BugDivergentResult
        }
    }
}

const fn comparison_domain_name(domain: ShadowComparisonDomain) -> &'static str {
    match domain {
        ShadowComparisonDomain::CatalogObjects => "catalog_objects",
        ShadowComparisonDomain::CatalogNameIndexes => "catalog_name_indexes",
        ShadowComparisonDomain::CatalogManifestWatermark => "catalog_manifest_watermark",
    }
}

const fn deferred_domain_name(domain: ShadowDeferredDomain) -> &'static str {
    match domain {
        ShadowDeferredDomain::TableCurrentPointers => "table_current_pointers",
        ShadowDeferredDomain::GrantsOwnership => "grants_ownership",
        ShadowDeferredDomain::StorageGovernanceEquivalence => "storage_governance_equivalence",
        ShadowDeferredDomain::IdempotencyRecords => "idempotency_records",
        ShadowDeferredDomain::FullProjectionWatermarks => "full_projection_watermarks",
        ShadowDeferredDomain::EventReplayHashes => "event_replay_hashes",
        ShadowDeferredDomain::ParquetProjectionEquality => "parquet_projection_equality",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arco_core::storage::{MemoryBackend, WritePrecondition};
    use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};
    use bytes::Bytes;
    use chrono::Utc;

    use super::*;
    use crate::manifest::{CatalogDomainManifest, DomainManifestPointer, format_manifest_id};
    use crate::parquet_util::{CatalogRecord, ColumnRecord, NamespaceRecord, TableRecord};
    use crate::reader::CatalogSnapshotDescriptor;
    use crate::state::CatalogState;
    use crate::state_store::shadow_replay::{
        import_catalog_source_into_shadow, load_current_catalog_shadow_source,
        open_catalog_shadow_store,
    };
    use crate::state_store::{ArcoStateStore, TxnOptions};
    use crate::tier1_snapshot;
    use crate::write_options::SnapshotVersion;

    fn storage() -> ScopedStorage {
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scoped storage")
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
    ) -> CatalogSnapshotDescriptor {
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
            writer_session_id: Some("phase4b-test".to_string()),
            snapshot_version: snapshot.version,
            snapshot_path: snapshot.path.clone(),
            snapshot: Some(snapshot.clone()),
            watermark_event_id: Some("catalog-event-0007".to_string()),
            last_commit_id: Some("commit-0007".to_string()),
            fencing_token: Some(4),
            commit_ulid: Some("01JPHASE4BCOMPARE0007".to_string()),
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
            manifest_path,
            epoch: 4,
            parent_pointer_hash: None,
            updated_at: Utc::now(),
        };
        storage
            .put_raw(
                &pointer_path,
                Bytes::from(serde_json::to_vec(&pointer).expect("pointer json")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write catalog pointer");

        CatalogSnapshotDescriptor {
            manifest_id,
            snapshot_version: SnapshotVersion::new(7),
            published_at: snapshot.published_at,
            snapshot: Some(snapshot),
        }
    }

    async fn import_shadow(storage: &ScopedStorage) {
        let source = load_current_catalog_shadow_source(storage)
            .await
            .expect("load current source");
        let shadow = open_catalog_shadow_store(storage).expect("shadow store");
        import_catalog_source_into_shadow(&shadow, &source)
            .await
            .expect("import shadow state");
    }

    async fn compare(
        storage: &ScopedStorage,
        descriptor: CatalogSnapshotDescriptor,
    ) -> CatalogInventoryComparisonRead {
        read_catalog_inventory_with_shadow_comparison(storage, async { Ok(descriptor) })
            .await
            .expect("current inventory read succeeds")
    }

    fn assert_current_unchanged(
        actual: &CatalogSnapshotDescriptor,
        expected: &CatalogSnapshotDescriptor,
    ) {
        assert_eq!(expected.manifest_id, actual.manifest_id);
        assert_eq!(
            expected.snapshot_version.as_u64(),
            actual.snapshot_version.as_u64()
        );
        assert_eq!(
            expected
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.total_rows),
            actual.snapshot.as_ref().map(|snapshot| snapshot.total_rows)
        );
    }

    #[tokio::test]
    async fn equivalent_catalog_inventory_comparison_returns_current_descriptor() {
        let storage = storage();
        let descriptor = publish_catalog_fixture(&storage, fixture_state()).await;
        import_shadow(&storage).await;

        let read = compare(&storage, descriptor.clone()).await;

        assert_current_unchanged(read.current(), &descriptor);
        assert_eq!(
            CatalogInventoryComparisonStatus::Equivalent,
            read.diagnostic().status()
        );
    }

    #[tokio::test]
    async fn stale_shadow_watermark_is_diagnostic_only_after_current_read_succeeds() {
        let storage = storage();
        let descriptor = publish_catalog_fixture(&storage, fixture_state()).await;
        import_shadow(&storage).await;
        let shadow = open_catalog_shadow_store(&storage).expect("shadow store");
        let mut txn = shadow
            .begin_txn(TxnOptions::default().with_request_id("stale-watermark"))
            .await
            .expect("begin stale watermark transaction");
        txn.put(
            b"shadow/catalog/metadata/source-watermark",
            Bytes::from_static(br#"{"snapshot_version":6}"#),
        )
        .await
        .expect("write stale watermark");
        txn.commit().await.expect("commit stale watermark");

        let read = compare(&storage, descriptor.clone()).await;

        assert_current_unchanged(read.current(), &descriptor);
        assert_eq!(
            CatalogInventoryComparisonStatus::StaleProjection,
            read.diagnostic().status()
        );
    }

    #[tokio::test]
    async fn unsupported_deferred_domains_are_structured_diagnostics() {
        let storage = storage();
        let descriptor = publish_catalog_fixture(&storage, fixture_state()).await;
        import_shadow(&storage).await;

        let read = compare(&storage, descriptor).await;

        assert!(
            read.diagnostic()
                .details()
                .iter()
                .any(|detail| detail.status()
                    == CatalogInventoryComparisonStatus::UnsupportedScope
                    && detail.domain() == "table_current_pointers")
        );
    }

    #[tokio::test]
    async fn divergent_shadow_object_state_is_diagnostic_only_after_current_read_succeeds() {
        let storage = storage();
        let descriptor = publish_catalog_fixture(&storage, fixture_state()).await;
        import_shadow(&storage).await;
        let shadow = open_catalog_shadow_store(&storage).expect("shadow store");
        let mut txn = shadow
            .begin_txn(TxnOptions::default().with_request_id("tamper-shadow-object"))
            .await
            .expect("begin tamper transaction");
        txn.put(
            b"shadow/catalog/object/table/table-1",
            Bytes::from_static(br#"{"tampered":true}"#),
        )
        .await
        .expect("tamper table row");
        txn.commit().await.expect("commit tamper");

        let read = compare(&storage, descriptor.clone()).await;

        assert_current_unchanged(read.current(), &descriptor);
        assert_eq!(
            CatalogInventoryComparisonStatus::BugDivergentResult,
            read.diagnostic().status()
        );
    }

    #[tokio::test]
    async fn current_state_gap_is_diagnostic_only_after_current_read_succeeds() {
        let storage = storage();
        let mut state = fixture_state();
        state.tables[0].namespace_id = "missing-schema".to_string();
        let descriptor = publish_catalog_fixture(&storage, state).await;
        import_shadow(&storage).await;

        let read = compare(&storage, descriptor.clone()).await;

        assert_current_unchanged(read.current(), &descriptor);
        assert_eq!(
            CatalogInventoryComparisonStatus::CurrentStateGap,
            read.diagnostic().status()
        );
    }

    #[tokio::test]
    async fn shadow_backend_errors_are_diagnostic_only_after_current_read_succeeds() {
        let storage = storage();
        let descriptor = CatalogSnapshotDescriptor {
            manifest_id: "manifest-current".to_string(),
            snapshot_version: SnapshotVersion::new(42),
            published_at: Utc::now(),
            snapshot: None,
        };

        let read = compare(&storage, descriptor.clone()).await;

        assert_current_unchanged(read.current(), &descriptor);
        assert_eq!(
            CatalogInventoryComparisonStatus::BugDivergentResult,
            read.diagnostic().status()
        );
        assert_eq!("shadow_backend", read.diagnostic().details()[0].domain());
    }

    #[tokio::test]
    async fn source_descriptor_mismatch_is_stale_projection_diagnostic() {
        let storage = storage();
        let mut descriptor = publish_catalog_fixture(&storage, fixture_state()).await;
        import_shadow(&storage).await;
        descriptor.manifest_id = "different-current-manifest".to_string();
        descriptor.snapshot_version = SnapshotVersion::new(8);

        let read = compare(&storage, descriptor.clone()).await;

        assert_current_unchanged(read.current(), &descriptor);
        assert_eq!(
            CatalogInventoryComparisonStatus::StaleProjection,
            read.diagnostic().status()
        );
        assert!(
            read.diagnostic()
                .details()
                .iter()
                .any(|detail| detail.domain() == "current_descriptor"
                    && detail.status() == CatalogInventoryComparisonStatus::StaleProjection)
        );
    }

    #[test]
    fn statuses_have_exact_internal_identifiers() {
        assert_eq!(
            "equivalent",
            CatalogInventoryComparisonStatus::Equivalent.as_str()
        );
        assert_eq!(
            "current_state_gap",
            CatalogInventoryComparisonStatus::CurrentStateGap.as_str()
        );
        assert_eq!(
            "unsupported_scope",
            CatalogInventoryComparisonStatus::UnsupportedScope.as_str()
        );
        assert_eq!(
            "stale_projection",
            CatalogInventoryComparisonStatus::StaleProjection.as_str()
        );
        assert_eq!(
            "bug_divergent_result",
            CatalogInventoryComparisonStatus::BugDivergentResult.as_str()
        );
    }
}
