//! Gate 7 qualification entry point. Provider admission is a separate reviewed packet.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::too_many_lines,
    clippy::print_stdout
)]
use arco_catalog::{
    ArcoStateAdmin as _, ArcoStateReader as _, ArcoStateTxn as _, CatalogProjectionMaterializer,
    CheckpointOptions, ControlCatalogAuthority, ControlMvpReadCacheConfig,
    ControlMvpRestoreParticipant, ControlMvpStateStore, PersistedAuthorityAdapter as _,
    RestoreAttemptIdentity, ScanRequest, StateRestoreParticipant as _, StateScope, TxnOptions,
    WriteOptions,
};
use arco_core::{MemoryBackend, ScopedStorage, StorageBackend, WritePrecondition, WriteResult};
use arco_storage_s3::S3StorageBackend;
use bytes::Bytes;
use futures::FutureExt as _;
use serde_json::json;
use std::{sync::Arc, time::Instant};

#[path = "../../../arco-storage-object-store/tests/support/live_conformance.rs"]
mod conformance;

#[path = "qualification/provider.rs"]
mod provider;
static JOURNAL: std::sync::OnceLock<Arc<provider::Journal>> = std::sync::OnceLock::new();
macro_rules! emit {
    ($format:literal, $value:expr $(,)?) => {{
        let row = $value;
        if let Some(journal) = JOURNAL.get() {
            journal.record(&row, false)?;
        }
        println!($format, row);
    }};
}

const INVENTORY: &[&str] = &[
    "scoped-conditional-storage-conformance",
    "bounded-list-1001-objects",
    "catalog-projection-ack-watermark",
    "commit-older-cut-restore-retained-reader",
    "changed-deleted-warm-objects-three-caches",
    "maintenance-reclamation-restart",
    "cold-point-scan-five-runs-200-observations-three-caches",
];

const LOOPBACK_INVENTORY: &[&str] = &[
    "http-403-404-408-409-412-503-conditional-outcomes",
    "lost-response-delayed-application-exact-reconciliation-read-retry",
];

#[allow(clippy::cognitive_complexity)]
async fn loopback_transport_faults(storage: ScopedStorage) -> Result<()> {
    let token_path = "conformance/observed-version-token";
    let WriteResult::Success { version } = storage
        .put(
            token_path,
            Bytes::from_static(b"candidate"),
            WritePrecondition::DoesNotExist,
        )
        .await?
    else {
        panic!("fresh token fixture");
    };
    storage.delete(token_path).await?;
    for (case, precondition, conflict) in [
        ("403", WritePrecondition::DoesNotExist, false),
        ("404", WritePrecondition::MatchesVersion(version), true),
        ("408", WritePrecondition::DoesNotExist, false),
        ("409", WritePrecondition::DoesNotExist, true),
        ("412", WritePrecondition::DoesNotExist, true),
        ("503", WritePrecondition::DoesNotExist, false),
    ] {
        let path = format!("conformance/fault/{case}");
        let outcome = storage
            .put(&path, Bytes::from_static(b"candidate"), precondition)
            .await;
        if conflict {
            assert!(matches!(outcome?, WriteResult::PreconditionFailed { .. }));
        } else {
            assert!(
                outcome.is_err(),
                "{case} must remain an error, not a committed write"
            );
        }
        emit!(
            "{}",
            json!({"kind":"injected-http-outcome", "status":case, "conflict":conflict, "provider":false})
        );
    }
    for fault in ["lost-response", "delayed-application"] {
        let path = format!("conformance/fault/{fault}");
        let result = storage
            .put(
                &path,
                Bytes::from_static(b"candidate"),
                WritePrecondition::DoesNotExist,
            )
            .await;
        assert!(
            result.is_err(),
            "conditional publication must remain uncertain"
        );
        let observed = storage.head(&path).await?.expect("reconciled application");
        assert_eq!(storage.get(&path).await?, Bytes::from_static(b"candidate"));
        assert!(matches!(
            storage
                .put(
                    &path,
                    Bytes::from_static(b"candidate"),
                    WritePrecondition::DoesNotExist
                )
                .await?,
            WriteResult::PreconditionFailed { .. }
        ));
        emit!(
            "{}",
            json!({"kind":"injected-publication-reconciled", "fault":fault, "version":observed.version, "provider":false})
        );
        storage.delete(&path).await?;
    }
    let path = "conformance/fault/read-retry";
    storage
        .put(
            path,
            Bytes::from_static(b"retry-visible"),
            WritePrecondition::DoesNotExist,
        )
        .await?;
    assert_eq!(
        storage.get(path).await?,
        Bytes::from_static(b"retry-visible")
    );
    storage.delete(path).await?;
    emit!(
        "{}",
        json!({"kind":"loopback-transport-completed", "scenarios":LOOPBACK_INVENTORY})
    );
    Ok(())
}

// The scenario explicitly drives delivery and acknowledgement below.
struct ExplicitProjectionWorker;
impl arco_catalog::CatalogProjectionNotifier for ExplicitProjectionWorker {
    fn notify(&self, _: &arco_catalog::ProjectionIntentV1) -> arco_catalog::Result<()> {
        Ok(())
    }
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[allow(
    clippy::cognitive_complexity,
    reason = "ordered qualification assertions retain the complete scenario trace"
)]
async fn scenarios(
    backend: Arc<dyn StorageBackend>,
    tenant: &str,
    workspace: &str,
    repetition: Option<usize>,
) -> Result<()> {
    let storage = ScopedStorage::new(backend, tenant, workspace)?;
    let scoped: Arc<dyn StorageBackend> = Arc::new(storage.clone());
    // The shared helper's raw conformance prefix is rooted by ScopedStorage.
    conformance::assert_storage_conformance("gate7", scoped.clone()).await;
    conformance::assert_bounded_list_conformance("gate7", &scoped).await;
    for index in 0..1001 {
        assert!(matches!(
            scoped
                .put(
                    &format!("conformance/list/{index:04}"),
                    Bytes::from_static(b"x"),
                    WritePrecondition::DoesNotExist
                )
                .await?,
            WriteResult::Success { .. }
        ));
    }
    let mut cursor = None;
    let mut seen = Vec::new();
    loop {
        let page = scoped
            .list_page("conformance/list/", cursor.as_deref(), 137)
            .await?;
        assert!(page.objects.len() <= 137);
        seen.extend(page.objects.into_iter().map(|object| object.path));
        cursor = page.next_start_after;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        seen,
        (0..1001)
            .map(|index| format!("conformance/list/{index:04}"))
            .collect::<Vec<_>>()
    );
    for path in seen {
        scoped.delete(&path).await?;
    }
    let scope = StateScope::new(tenant, workspace, "catalog");
    let authority = ControlCatalogAuthority::new(storage.clone(), scope.clone())?
        .with_projection_notifier(Arc::new(ExplicitProjectionWorker));
    let catalog = authority
        .create_catalog(
            "gate7",
            Some("synthetic qualification"),
            WriteOptions::with_idempotency("gate7-catalog"),
        )
        .await?;
    let materializer = CatalogProjectionMaterializer::new(storage.clone())?;
    let drained = materializer.drain_once().await?;
    assert_eq!(drained.drained_record_ids.len(), 1);
    let status = materializer.status().await?.expect("projection status");
    assert!(status.failure_state().is_none());
    assert!(
        storage
            .head_raw(
                status
                    .artifact_manifest_path()
                    .expect("materialized manifest")
            )
            .await?
            .is_some()
    );
    assert_eq!(status.applied_authority_sequence(), Some(1));
    let directory = status
        .artifact_manifest_path()
        .unwrap()
        .strip_suffix("manifest.json")
        .unwrap();
    let projected = arco_catalog::parquet_util::read_catalogs(
        &storage
            .get_raw(&format!("{directory}catalogs.parquet"))
            .await?,
    )?;
    assert_eq!(projected.len(), 1);
    let projected = projected.first().unwrap();
    assert_eq!(
        (&projected.id, &projected.name, &projected.description),
        (&catalog.id, &catalog.name, &catalog.description)
    );
    assert_eq!(
        (projected.created_at, projected.updated_at),
        (catalog.created_at, catalog.updated_at)
    );
    let store = ControlMvpStateStore::new(storage.clone(), scope.clone())?;
    let mut tx = store.begin_control_txn(TxnOptions::default()).await?;
    tx.put(b"gate7-key", Bytes::from_static(b"retained"))
        .await?;
    tx.commit().await?;
    let checkpoint = store.checkpoint(CheckpointOptions::default()).await?;
    let retained = store.read_checkpoint(checkpoint.clone()).await?;
    let reference = store
        .persist_checkpoint_reference(&checkpoint, chrono::Utc::now() + chrono::Duration::days(30))
        .await?;
    authority
        .patch_catalog(
            "gate7",
            arco_catalog::CatalogPatch {
                description: Some(Some("later catalog metadata".into())),
                ..arco_catalog::CatalogPatch::default()
            },
            WriteOptions::with_idempotency("gate7-later-catalog"),
        )
        .await?;
    assert_eq!(
        authority
            .get_catalog("gate7")
            .await?
            .unwrap()
            .description
            .as_deref(),
        Some("later catalog metadata")
    );
    let mut tx = store.begin_control_txn(TxnOptions::default()).await?;
    tx.put(b"gate7-key", Bytes::from_static(b"new")).await?;
    tx.commit().await?;
    assert_eq!(
        retained.get(b"gate7-key").await?,
        Some(Bytes::from_static(b"retained"))
    );
    let participant = ControlMvpRestoreParticipant::new(store.clone());
    let identity = RestoreAttemptIdentity::new(format!("rst_{}", ulid::Ulid::new()), 1, "catalog")?;
    let now = chrono::Utc::now();
    let plan = participant.plan_restore(&reference, &identity, now).await?;
    participant.apply_restore(&plan, now).await?;
    assert_eq!(
        authority.get_catalog("gate7").await?.unwrap().description,
        catalog.description
    );

    assert_eq!(
        store.get(b"gate7-key").await?,
        Some(Bytes::from_static(b"retained"))
    );
    for repetition in repetition.map_or(0..=4, |r| r..=r) {
        for (mode, config) in [
            (
                "disabled",
                ControlMvpReadCacheConfig {
                    metadata_bytes: 0,
                    decoded_bytes: 0,
                },
            ),
            ("default", ControlMvpReadCacheConfig::default()),
            (
                "pressure",
                ControlMvpReadCacheConfig {
                    metadata_bytes: 1024 * 1024,
                    decoded_bytes: 4 * 1024 * 1024,
                },
            ),
        ] {
            for operation in ["point", "scan"] {
                for observation in 0..200 {
                    let fresh = ControlMvpStateStore::new(storage.clone(), scope.clone())?
                        .with_read_cache_config(config)?;
                    let initial = fresh.read_cache().map(|cache| cache.statistics());
                    assert!(
                        initial
                            .as_ref()
                            .is_none_or(|stats| stats.demands == 0 && stats.loads == 0)
                    );
                    let started = Instant::now();
                    if operation == "point" {
                        assert_eq!(
                            fresh.get(b"gate7-key").await?,
                            Some(Bytes::from_static(b"retained"))
                        );
                    } else {
                        let page = fresh
                            .scan(ScanRequest::new(b"gate7-key").with_limits(128, 64 * 1024, 32))
                            .await?;
                        assert_eq!(page.entries().len(), 1);
                        assert_eq!(
                            page.entries().first().unwrap().value().bytes(),
                            &Bytes::from_static(b"retained")
                        );
                        assert!(page.continuation().is_none());
                    }
                    emit!(
                        "{}",
                        json!({"kind":"cold-observation", "repetition":repetition,
                        "mode":mode, "operation":operation, "observation":observation,
                        "fresh_cache":true, "elapsed_ns":started.elapsed().as_nanos(),
                        "handle_id":ulid::Ulid::new().to_string(), "initial_cache_ledger":initial,
                        "cache_ledger":fresh.read_cache().map(|cache| cache.statistics())})
                    );
                }
            }
        }
    }
    for (mode, config) in [
        (
            "disabled",
            ControlMvpReadCacheConfig {
                metadata_bytes: 0,
                decoded_bytes: 0,
            },
        ),
        ("default", ControlMvpReadCacheConfig::default()),
        (
            "pressure",
            ControlMvpReadCacheConfig {
                metadata_bytes: 1024 * 1024,
                decoded_bytes: 4 * 1024 * 1024,
            },
        ),
    ] {
        let domain = format!("warm-auth-{mode}");
        let warm_scope = StateScope::new(tenant, workspace, &domain);
        let warm = ControlMvpStateStore::new(storage.clone(), warm_scope.clone())?
            .with_read_cache_config(config)?;
        let mut tx = warm.begin_control_txn(TxnOptions::default()).await?;
        tx.put(b"key", Bytes::from_static(b"authenticated")).await?;
        tx.commit().await?;
        for _ in 0..2 {
            assert_eq!(
                warm.get(b"key").await?,
                Some(Bytes::from_static(b"authenticated"))
            );
        }
        let segments = storage
            .list_meta(&format!("control/v1/domains/{domain}/segments/"))
            .await?;
        assert_eq!(segments.len(), 1);
        let path = segments.first().unwrap().path.to_string();
        let original = storage.get_raw(&path).await?;
        storage
            .put_raw(
                &path,
                Bytes::from_static(b"injected changed immutable bytes"),
                WritePrecondition::None,
            )
            .await?;
        assert!(
            warm.get(b"key").await.is_err(),
            "changed warm object must authenticate"
        );
        // Exact restore deliberately replaces the injected object within this disposable fixture.
        storage
            .put_raw(&path, original.clone(), WritePrecondition::None)
            .await?;
        assert_eq!(
            warm.get(b"key").await?,
            Some(Bytes::from_static(b"authenticated"))
        );
        storage.delete(&path).await?;
        assert!(
            warm.get(b"key").await.is_err(),
            "deleted warm object must not be substituted"
        );
        assert!(matches!(
            storage
                .put_raw(&path, original, WritePrecondition::DoesNotExist)
                .await?,
            WriteResult::Success { .. }
        ));
        assert_eq!(
            warm.get(b"key").await?,
            Some(Bytes::from_static(b"authenticated"))
        );
        emit!(
            "{}",
            json!({"kind":"injected-client-fault", "mode":mode, "changed_and_deleted_warm_object":path, "reconciled":true})
        );
    }
    let maintenance_scope = StateScope::new(tenant, workspace, "maintenance-probe");
    let maintenance_store = ControlMvpStateStore::new(storage.clone(), maintenance_scope.clone())?;
    for generation in 0..16 {
        let mut tx = maintenance_store
            .begin_control_txn(TxnOptions::default())
            .await?;
        tx.put(b"key", Bytes::from(format!("generation-{generation}")))
            .await?;
        tx.commit().await?;
    }
    let retained = maintenance_store
        .read_at(maintenance_store.current_state_token().await?)
        .await?;
    let binding = arco_catalog::DurableAuthorityBinding::new([73; 32]);
    let worker = arco_catalog::DurableMaintenanceWorker::new(
        storage.clone(),
        maintenance_scope.clone(),
        binding,
    )?;
    let now = chrono::Utc::now();
    let plan = worker
        .prepare_at(now)
        .await?
        .expect("16 L0s require actual maintenance");
    worker.start_at(&plan, now).await?;
    let restarted = arco_catalog::DurableMaintenanceWorker::new(
        storage.clone(),
        maintenance_scope.clone(),
        binding,
    )?;
    let mut progress = restarted.resume_at(plan.job_id(), now).await?;
    for _ in 0..512 {
        if progress.status == arco_catalog::MaintenanceStatus::ReadyToPublish {
            break;
        }
        progress = restarted.advance_at(plan.job_id(), now).await?;
    }
    assert_eq!(
        progress.status,
        arco_catalog::MaintenanceStatus::ReadyToPublish
    );
    assert!(restarted.publish_at(plan.job_id(), now).await?.is_some());
    assert_eq!(
        retained.get(b"key").await?,
        Some(Bytes::from_static(b"generation-15"))
    );
    let orphan = maintenance_store
        .paths()
        .tx_object("gate7-disposable-orphan");
    storage
        .put_raw(
            &orphan,
            Bytes::from_static(b"orphan"),
            WritePrecondition::DoesNotExist,
        )
        .await?;
    let collector =
        arco_catalog::ControlMvpMaintenanceWorker::new(storage.clone(), maintenance_scope)?;
    let mut cursor = None;
    loop {
        let page = collector
            .collect_gc_page_at(now + chrono::Duration::days(9), vec![], cursor.as_deref())
            .await?;
        cursor = page.continuation().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert!(storage.head_raw(&orphan).await?.is_none());
    assert_eq!(
        retained.get(b"key").await?,
        Some(Bytes::from_static(b"generation-15"))
    );
    emit!(
        "{}",
        json!({"kind":"simulated-retention-clock", "advance_days":9, "provider_elapsed_proof":false})
    );
    for object in storage.list_meta("").await? {
        emit!(
            "{}",
            json!({"kind":"retained-object", "path":object.path.to_string(), "size":object.size})
        );
    }
    emit!(
        "{}",
        json!({"kind":"completed", "scenarios":INVENTORY,
        "tenant":tenant, "workspace":workspace, "qualification":if repetition.is_some() { "provider-scenario-evidence-only" } else { "local-scenario-evidence-only" }})
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [command, manifest, expected] if ["provider", "provider-validate", "provider-rehearsal", "provider-rehearsal-listing-probe", "provider-rehearsal-put-307", "provider-rehearsal-put-308"].contains(&command.as_str()) => {
            let manifest_path = std::path::Path::new(manifest);
            let config = Arc::new(provider::Manifest::load(manifest_path, expected)?);
            let real = !command.starts_with("provider-rehearsal");
            config.validate(real)?;
            if command == "provider-validate" {
                emit!("{}", json!({"kind":"configuration-validated", "traffic":false}));
                return Ok(());
            }
            if real { config.supervised(expected)?; }
            let _port = if real { None } else {
                for (key, _) in std::env::vars_os() {
                    let key = key.to_string_lossy();
                    if (key.starts_with("AWS_") && !["AWS_ENDPOINT", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_ALLOW_HTTP", "AWS_REGION", "AWS_EC2_METADATA_DISABLED"].contains(&key.as_ref()))
                        || ["http_proxy", "https_proxy", "all_proxy"].contains(&key.to_ascii_lowercase().as_str()) { return Err("unexpected rehearsal configuration".into()); }
                }
                for (key, value) in [("AWS_ACCESS_KEY_ID", "gate7-test"), ("AWS_SECRET_ACCESS_KEY", "gate7-test"), ("AWS_ALLOW_HTTP", "true"), ("AWS_REGION", "us-east-2"), ("AWS_EC2_METADATA_DISABLED", "true")] {
                    if std::env::var(key)?.as_str() != value { return Err("rehearsal requires dummy credentials and disabled metadata".into()); }
                }
                let endpoint = std::env::var("AWS_ENDPOINT")?;
                let port = endpoint.strip_prefix("http://127.0.0.1:").or_else(|| endpoint.strip_prefix("https://127.0.0.1:")).ok_or("numeric loopback endpoint required")?.parse::<u16>()?;
                if port == 0 { return Err("nonzero port required".into()); } Some(port)
            };
            let identity = if real { config.identity().await? } else { json!({"rehearsal":true,"provider":false}) };
            let journal = provider::Journal::create(config.clone(), manifest_path, expected)?;
            JOURNAL.set(journal.clone()).map_err(|_| "duplicate run journal")?;
            journal.record(&json!({"kind":"verified-identity", "identity":identity, "provider":real}), true)?;
            let backend: Arc<dyn StorageBackend> = Arc::new(provider::Observed {
                inner: Arc::new(S3StorageBackend::for_qualification(&config.bucket, &config.listing_proxy)?), journal:journal.clone() });
            if let Some(status) = command.strip_prefix("provider-rehearsal-put-") {
                let result = backend.put(&(config.prefix(0) + "conformance/fault/" + status), Bytes::from_static(b"redirect-candidate"), WritePrecondition::DoesNotExist).await;
                journal.state("failed-recovery-required")?;
                result?;
                return Err("redirected conditional PUT unexpectedly completed".into());
            }
            if command == "provider-rehearsal-listing-probe" {
                let result = backend.list_page(&(config.prefix(0) + "conformance/proxy-probe/"), None, 1).await;
                journal.state(if result.is_ok() { "listing-probe-completed" } else { "failed-recovery-required" })?;
                if result?.objects.len() != 1 { return Err("listing probe row count mismatch".into()); }
                emit!("{}", json!({"kind":"listing-probe-completed", "provider":false}));
                return Ok(());
            }
            let workload = async {
                for repetition in 0..5 {
                    config.verify_sources(real)?;
                    if provider::hash(manifest_path)? != *expected { return Err("manifest drift".into()); }
                    let workspace = format!("{}-r{repetition}", config.workspace);
                    provider::client_fault(ScopedStorage::new(backend.clone(), &config.tenant, &workspace)?).await?;
                    Box::pin(scenarios(backend.clone(), &config.tenant, &workspace, Some(repetition))).await?;
                    journal.state("running")?;
                }
                config.verify_sources(real)?;
                if provider::hash(manifest_path)? != *expected { return Err("manifest drift".into()); }
                Ok::<(), Box<dyn std::error::Error>>(())
            };
            let result = tokio::time::timeout(std::time::Duration::from_secs(config.ceilings.elapsed_seconds),
                std::panic::AssertUnwindSafe(workload).catch_unwind()).await;
            let success = matches!(&result, Ok(Ok(Ok(()))));
            journal.state(if success { "completed-evidence-pending-validation" } else { "failed-recovery-required" })?;
            if !success {
                let detail = format!("{result:?}");
                journal.record(&json!({"kind":"provider-execution-failed", "detail":detail}), true)?;
                journal.state("failed-recovery-required")?;
                return Err(format!("provider execution did not complete: {detail}; preserve journal and reconcile before cleanup or a new run").into());
            }
            emit!("{}", json!({"kind":"provider-execution-completed", "provider":real, "repetitions":5, "pilot_qualified":false, "qualification":"evidence-pending-validation"}));
            journal.state("completed-evidence-pending-validation")?;
        },
        [command] if command == "inventory" => emit!("{}", json!({"common":INVENTORY,"loopback_transport":LOOPBACK_INVENTORY,"provider":provider::inventory(),"pilot":[]})),
        [command] if command == "local" => {
            let tenant = format!("gate7-{}", ulid::Ulid::new().to_string().to_ascii_lowercase());
            Box::pin(scenarios(Arc::new(MemoryBackend::new()), &tenant, "qualification", None)).await?;
        }
        [command] if command == "loopback" => {
            let allowed = ["AWS_ENDPOINT", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_ALLOW_HTTP", "AWS_REGION", "AWS_EC2_METADATA_DISABLED"];
            for (key, _) in std::env::vars_os() {
                let key = key.to_string_lossy();
                if (key.starts_with("AWS_") && !allowed.contains(&key.as_ref()))
                    || ["http_proxy", "https_proxy", "all_proxy"].contains(&key.to_ascii_lowercase().as_str()) {
                    return Err(format!("unexpected loopback configuration: {key}").into());
                }
            }
            let endpoint = std::env::var("AWS_ENDPOINT")?;
            // This command is credential-free and can only address a numeric loopback host.
            let port = endpoint.strip_prefix("http://127.0.0.1:").ok_or("loopback endpoint required")?.parse::<u16>()?;
            if port == 0 { return Err("nonzero loopback port required".into()); }
            for (name, expected) in [("AWS_ACCESS_KEY_ID", "gate7-test"), ("AWS_SECRET_ACCESS_KEY", "gate7-test"), ("AWS_ALLOW_HTTP", "true"), ("AWS_REGION", "us-east-1")] {
                if std::env::var(name)?.as_str() != expected { return Err(format!("loopback configuration mismatch: {name}").into()); }
            }
            let tenant = format!("gate7-{}", ulid::Ulid::new().to_string().to_ascii_lowercase());
            let backend: Arc<dyn StorageBackend> = Arc::new(S3StorageBackend::new("gate7-loopback")?);
            loopback_transport_faults(ScopedStorage::new(backend.clone(), &tenant, "qualification")?).await?;
            Box::pin(scenarios(backend, &tenant, "qualification", None)).await?;
        }
        _ => return Err("usage: inventory | local | loopback | provider[-validate|-rehearsal] MANIFEST SHA256; pilot remains capacity-blocked".into()),
    }
    Ok(())
}
