#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
use super::super::{
    ArcoStateTxn, ControlMvpProjectionOutboxRecord, TxnOptions, state_object_from_segment_rows,
};
use super::*;
use arco_core::MemoryBackend;

async fn prepared_fixture() -> (DurableMaintenanceWorker, PreparedMaintenance, DateTime<Utc>) {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let worker = DurableMaintenanceWorker::new(
        storage,
        StateScope::new("tenant", "workspace", "catalog"),
        DurableAuthorityBinding::new([9; 32]),
    )
    .unwrap()
    .with_test_segment_sizing(1, 8 * 1024)
    .unwrap();
    for ordinal in 0..16 {
        let mut tx = worker
            .worker
            .store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        if ordinal == 0 {
            tx.put(b"", Bytes::from_static(b"empty key")).await.unwrap();
            tx.put(b"binary\0", Bytes::from_static(b"value\0"))
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
    }
    let now = Utc::now();
    let plan = worker.prepare_at(now).await.unwrap().unwrap();
    (worker, plan, now)
}

async fn collect_all(worker: &DurableMaintenanceWorker, now: DateTime<Utc>) {
    let mut cursor = None;
    loop {
        let page = worker
            .worker
            .collect_gc_page_at(now, Vec::new(), cursor.as_deref())
            .await
            .unwrap();
        cursor = page.continuation().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
}

#[tokio::test]
async fn publication_authenticates_completed_bytes_and_every_render_cut_field() {
    let (worker, plan, now) = prepared_fixture().await;
    worker.start_at(&plan, now).await.unwrap();
    loop {
        if worker.advance_at(&plan.id, now).await.unwrap().status
            == MaintenanceStatus::ReadyToPublish
        {
            break;
        }
    }
    let mut tx = worker
        .worker
        .store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.put(b"binary\0", Bytes::from_static(b"later masks base value"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    for field in 0..7 {
        let mut job = LoadedJob::load(&worker.worker.store, &plan.id, worker.binding)
            .await
            .unwrap();
        match field {
            0 => job.descriptor.source_id = "forged-render-cut".into(),
            1 => job.descriptor.source_digest = "a".repeat(64),
            2 => job.descriptor.source_checksum = "b".repeat(64),
            3 => job.descriptor.source_history = "c".repeat(64),
            4 => job.descriptor.source_physical = "d".repeat(64),
            5 => job.descriptor.source_sequence += 1,
            6 => job.descriptor.layout_generation += 1,
            _ => unreachable!(),
        }
        assert!(
            worker.prepare_publication(&plan.id, &job, 0).await.is_err(),
            "forged render field {field}"
        );
    }
    let job = LoadedJob::load(&worker.worker.store, &plan.id, worker.binding)
        .await
        .unwrap();
    let path = worker
        .worker
        .store
        .paths
        .state_object(&job.pages[0].output.state_id);
    let original = worker.worker.lifecycle.get_raw(&path).await.unwrap();
    let mut corrupt = original.to_vec();
    corrupt[0] ^= 1;
    worker
        .worker
        .lifecycle
        .put_raw(
            &path,
            Bytes::from(corrupt),
            arco_core::WritePrecondition::None,
        )
        .await
        .unwrap();
    // Metadata-only resume can establish length/existence, never byte integrity.
    worker.resume_at(&plan.id, now).await.unwrap();
    let before = worker.worker.store.load_pointer().await.unwrap();
    assert!(worker.publish_at(&plan.id, now).await.is_err());
    assert_eq!(
        worker
            .worker
            .store
            .load_pointer()
            .await
            .unwrap()
            .manifest_id,
        before.manifest_id
    );
    assert_eq!(
        LoadedJob::load(&worker.worker.store, &plan.id, worker.binding)
            .await
            .unwrap()
            .last()
            .unwrap()
            .1
            .status,
        MaintenanceStatus::ReadyToPublish
    );
}

#[tokio::test]
async fn inline_anchor_promotion_consumes_original_l1_ownership() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let store = ControlMvpStateStore::new(storage.clone(), scope.clone())
        .unwrap()
        .with_checkpoint_interval(std::num::NonZeroU64::new(1).unwrap());
    store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let worker =
        DurableMaintenanceWorker::new(storage, scope, DurableAuthorityBinding::new([12; 32]))
            .unwrap()
            .with_fixture_store(store.clone());
    let now = Utc::now();
    let plan = worker.test_prepare_forced_at(now).await.unwrap().unwrap();
    worker.start_at(&plan, now).await.unwrap();
    worker.advance_at(&plan.id, now).await.unwrap();
    store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    assert!(matches!(
        worker.publish_at(&plan.id, now).await,
        Err(CatalogError::PreconditionFailed { .. })
    ));
    assert_eq!(
        LoadedJob::load(&worker.worker.store, &plan.id, worker.binding)
            .await
            .unwrap()
            .last()
            .unwrap()
            .1
            .status,
        MaintenanceStatus::ReadyToPublish
    );
}

#[tokio::test]
async fn pin_gc_requires_both_selected_objects_to_age_and_recovers_selector_first_deletion() {
    use crate::workspace_snapshot::{retention_pin_latest_path, retention_pin_revision_path};
    let (worker, mut plan, now) = prepared_fixture().await;
    // The root deadline has elapsed before the object-age threshold. A
    // delayed root repair can produce exactly this timestamp ordering.
    plan.descriptor.created_at -= ChronoDuration::days(2);
    plan.descriptor.expires_at -= ChronoDuration::days(2);
    plan.descriptor.retained_until -= ChronoDuration::days(2);
    let bytes = activation_bytes(&plan.descriptor, &plan.id).unwrap();
    let revision = retention_pin_revision_path(&plan.descriptor.pin_id(), 1).unwrap();
    let latest = retention_pin_latest_path(&plan.descriptor.pin_id()).unwrap();
    worker
        .worker
        .lifecycle
        .put_raw(
            &revision,
            bytes.0,
            arco_core::WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    let older = worker
        .worker
        .lifecycle
        .head_raw(&revision)
        .await
        .unwrap()
        .unwrap()
        .last_modified
        .unwrap();
    worker
        .worker
        .lifecycle
        .put_raw(&latest, bytes.1, arco_core::WritePrecondition::DoesNotExist)
        .await
        .unwrap();
    let newer = worker
        .worker
        .lifecycle
        .head_raw(&latest)
        .await
        .unwrap()
        .unwrap()
        .last_modified
        .unwrap();
    assert!(newer > older);
    let plan = worker
        .worker
        .plan_gc_page_at(
            older + ChronoDuration::days(7),
            Vec::new(),
            Some(PIN_GC_CURSOR),
        )
        .await
        .unwrap();
    assert!(plan.candidates().is_empty());
    let plan = worker
        .worker
        .plan_gc_page_at(
            newer + ChronoDuration::days(7),
            Vec::new(),
            Some(PIN_GC_CURSOR),
        )
        .await
        .unwrap();
    assert_eq!(plan.candidates().len(), 2);
    assert_eq!(plan.candidates()[0].path, latest);
    assert_eq!(plan.candidates()[1].path, revision);
    worker.worker.lifecycle.delete(&latest).await.unwrap();
    collect_all(&worker, now + ChronoDuration::days(8)).await;
    assert!(
        worker
            .worker
            .lifecycle
            .head_raw(&revision)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn gc_collects_expired_orphan_pin_revisions_and_inactive_descriptors() {
    let (worker, plan, now) = prepared_fixture().await;
    let descriptor = descriptor_path(&worker.worker.store, plan.id.as_str());
    immutable_reconciled(&worker.worker.store, &descriptor, plan.bytes.clone())
        .await
        .unwrap();
    let pin = crate::workspace_snapshot::retention_pin_revision_path(&plan.descriptor.pin_id(), 1)
        .unwrap();
    worker
        .worker
        .lifecycle
        .put_raw(
            &pin,
            activation_bytes(&plan.descriptor, &plan.id).unwrap().0,
            arco_core::WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    collect_all(&worker, now + ChronoDuration::days(8)).await;
    assert!(
        worker
            .worker
            .lifecycle
            .head_raw(&pin)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        worker
            .worker
            .store
            .storage
            .head(&descriptor)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn gc_protects_planned_outputs_before_a_receipt_then_collects_expired_job_artifacts() {
    let (worker, plan, now) = prepared_fixture().await;
    worker.start_at(&plan, now).await.unwrap();
    let job = LoadedJob::load(&worker.worker.store, &plan.id, worker.binding)
        .await
        .unwrap();
    let (_, _, _, source) = worker.compatible(&job.descriptor).await.unwrap();
    let rendered = job.pages[0]
        .construct(&worker.worker.store, &source)
        .await
        .unwrap();
    let output = worker
        .worker
        .store
        .paths
        .state_object(&rendered.reference.state_id);
    immutable_reconciled(&worker.worker.store, &output, rendered.bytes)
        .await
        .unwrap();
    collect_all(&worker, now + ChronoDuration::days(7)).await;
    assert!(
        worker
            .worker
            .store
            .storage
            .head(&output)
            .await
            .unwrap()
            .is_some()
    );
    collect_all(&worker, now + ChronoDuration::days(8)).await;
    assert!(
        worker
            .worker
            .store
            .storage
            .head(&output)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        worker
            .worker
            .store
            .storage
            .head(&selector_path(&worker.worker.store, plan.id.as_str()))
            .await
            .unwrap()
            .is_none()
    );
    let pin =
        crate::workspace_snapshot::retention_pin_latest_path(&plan.descriptor.pin_id()).unwrap();
    assert!(
        worker
            .worker
            .lifecycle
            .head_raw(&pin)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn incomplete_plan_never_activates_a_retention_root() {
    for page_prefix in 0..2 {
        let (worker, plan, now) = prepared_fixture().await;
        let store = &worker.worker.store;
        assert_eq!(plan.pages.len(), 2);
        immutable_reconciled(
            store,
            &descriptor_path(store, plan.id.as_str()),
            plan.bytes.clone(),
        )
        .await
        .unwrap();
        for ordinal in 0..page_prefix {
            immutable_reconciled(
                store,
                &page_path(
                    store,
                    plan.id.as_str(),
                    ordinal,
                    &plan.descriptor.pages[ordinal],
                ),
                plan.pages[ordinal].clone(),
            )
            .await
            .unwrap();
        }
        assert!(
            worker
                .recover_activation_at(plan.job_id(), now)
                .await
                .is_err()
        );
        assert!(
            store
                .storage
                .head(&selector_path(store, plan.id.as_str()))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            worker
                .worker
                .lifecycle
                .head_raw(
                    &crate::workspace_snapshot::retention_pin_latest_path(
                        &plan.descriptor.pin_id()
                    )
                    .unwrap()
                )
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            worker
                .worker
                .lifecycle
                .head_raw(crate::retention_coordination::RETENTION_MUTATION_EPOCH_PATH)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn expired_activation_repairs_every_submitted_root_prefix_without_execution() {
    use crate::workspace_snapshot::{retention_pin_latest_path, retention_pin_revision_path};
    for schedule in 0..10 {
        let prefix = schedule % 5;
        let elapsed = if schedule < 5 {
            ChronoDuration::hours(24)
        } else {
            ChronoDuration::days(9)
        };
        let (worker, plan, now) = prepared_fixture().await;
        let store = &worker.worker.store;
        persist_inactive(&worker, &plan).await;
        let expired = now + elapsed;
        assert!(
            worker
                .recover_activation_at(plan.job_id(), expired)
                .await
                .is_err(),
            "expiry cannot initiate an unclaimed root"
        );
        let mut guard = DistributedLock::new(
            Arc::new(worker.worker.lifecycle.clone()),
            RETENTION_GC_LOCK_PATH,
        )
        .acquire_with_operation(
            RETENTION_GC_LOCK_TTL,
            RETENTION_GC_LOCK_MAX_RETRIES,
            Some("test activation crash".into()),
        )
        .await
        .unwrap();
        let mut epoch = RetentionMutationEpoch::claim_maintenance_root(
            worker.worker.lifecycle.clone(),
            &mut guard,
            plan.id.as_str(),
            None,
        )
        .await
        .unwrap();
        let bytes = activation_bytes(&plan.descriptor, &plan.id).unwrap();
        let writes = [
            (
                retention_pin_revision_path(&plan.descriptor.pin_id(), 1).unwrap(),
                bytes.0,
            ),
            (
                retention_pin_latest_path(&plan.descriptor.pin_id()).unwrap(),
                bytes.1,
            ),
            (
                revision_path(store, plan.id.as_str(), &sha256_hex(&bytes.2)),
                bytes.2,
            ),
            (selector_path(store, plan.id.as_str()), bytes.3),
        ];
        for (path, bytes) in writes.iter().take(prefix) {
            epoch
                .put_immutable_reconciled(path, bytes.clone())
                .await
                .unwrap();
        }
        // A dead worker can lose its lease while its durable epoch remains in flight.
        guard.release().await.unwrap();
        let recovered = worker
            .recover_activation_at(plan.job_id(), expired)
            .await
            .unwrap();
        assert_eq!(recovered.completed, 0);
        worker
            .recover_activation_at(plan.job_id(), expired + ChronoDuration::seconds(1))
            .await
            .unwrap();
        for (path, bytes) in &writes {
            assert_eq!(
                &worker.worker.lifecycle.get_raw(path).await.unwrap(),
                bytes,
                "repair must retain original bytes and deadlines"
            );
        }
        assert!(worker.resume_at(plan.job_id(), expired).await.is_err());
        assert!(worker.advance_at(plan.job_id(), expired).await.is_err());
        assert!(worker.publish_at(plan.job_id(), expired).await.is_err());
        assert!(
            epoch.settle().await.is_err(),
            "old epoch cannot settle the replacement"
        );
        let mut roots = RetainedAuthorityRoots::new(&worker.worker.lifecycle, expired);
        assert_eq!(
            roots.next().await.unwrap().is_some(),
            schedule < 5,
            "repair never extends the fixed retention deadline"
        );
    }
}

#[tokio::test]
async fn publication_submission_reservations_are_bounded_and_exclusive() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let worker = DurableMaintenanceWorker::new(
        storage,
        StateScope::new("tenant", "workspace", "catalog"),
        DurableAuthorityBinding::new([4; 32]),
    )
    .unwrap();
    for _ in 0..16 {
        worker
            .worker
            .store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
    let now = Utc::now();
    let id = worker.test_start_at(now).await.unwrap().unwrap();
    worker.advance_at(&id, now).await.unwrap();
    let mut job = LoadedJob::load(&worker.worker.store, &id, worker.binding)
        .await
        .unwrap();
    let candidate = worker.prepare_publication(&id, &job, 0).await.unwrap();
    let bytes =
        encode_json_limited(&candidate.attempt, MAX_PLAN_PAGE_BYTES, "test attempt").unwrap();
    let digest = sha256_hex(&bytes);
    // Submission serialization is a pure admission step, before any attempt PUT.
    assert!(matches!(
        prepare_submission(&MaintenanceJobId("x".repeat(8193)), &job, &digest),
        Err(CatalogError::MaintenanceBackpressure { .. })
    ));
    immutable_reconciled(
        &worker.worker.store,
        &attempt_path(&worker.worker.store, id.as_str(), &digest),
        bytes,
    )
    .await
    .unwrap();
    for submission in 1..=16 {
        let next = worker
            .begin_submission(&id, &job, prepare_submission(&id, &job, &digest).unwrap())
            .await
            .unwrap();
        assert_eq!(next.last().unwrap().1.submissions, submission);
        assert!(
            worker
                .begin_submission(&id, &job, prepare_submission(&id, &job, &digest).unwrap())
                .await
                .is_err(),
            "stale selector must not grant a second submission"
        );
        job = next;
    }
    assert!(prepare_submission(&id, &job, &digest).is_err());
    assert_eq!(
        worker
            .worker
            .store
            .load_pointer()
            .await
            .unwrap()
            .manifest_id,
        candidate.attempt.source_id
    );
}

#[tokio::test]
async fn durable_publication_preserves_descendants_after_restart() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let worker = DurableMaintenanceWorker::new(
        storage.clone(),
        scope.clone(),
        DurableAuthorityBinding::new([3; 32]),
    )
    .unwrap();
    for _ in 0..16 {
        worker
            .worker
            .store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
    let now = Utc::now();
    let id = worker.test_start_at(now).await.unwrap().unwrap();
    let ready = worker.advance_at(&id, now).await.unwrap();
    assert_eq!(ready.status, MaintenanceStatus::ReadyToPublish);
    let mut tx = worker
        .worker
        .store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.put(b"later", Bytes::from_static(b"preserved"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let restarted =
        DurableMaintenanceWorker::new(storage, scope, DurableAuthorityBinding::new([3; 32]))
            .unwrap();
    assert_eq!(restarted.resume_at(&id, now).await.unwrap().completed, 1);
    assert_eq!(restarted.advance_at(&id, now).await.unwrap().completed, 1);
    let outcome = restarted.publish_at(&id, now).await.unwrap().unwrap();
    assert_eq!(outcome.source_token().logical_sequence(), 17);
    let pointer = restarted.worker.store.load_pointer().await.unwrap();
    let manifest = restarted
        .worker
        .store
        .load_manifest_for_pointer(&pointer)
        .await
        .unwrap();
    assert_eq!(manifest.tx_refs.len(), 1);
    assert_eq!(manifest.equivalence.as_ref().unwrap().encoding_version, 2);
    let state = restarted
        .worker
        .store
        .replay_for_successor(&manifest)
        .await
        .unwrap();
    assert_eq!(
        state.kv.get(b"later".as_slice()).unwrap().bytes,
        b"preserved".as_slice()
    );
    assert_eq!(
        restarted.publish_at(&id, now).await.unwrap().unwrap(),
        outcome
    );
}

#[tokio::test]
async fn activation_is_durable_scope_bound_and_grants_only_gc_protection() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let worker = DurableMaintenanceWorker::new(
        storage.clone(),
        scope.clone(),
        DurableAuthorityBinding::new([7; 32]),
    )
    .unwrap();
    for _ in 0..16 {
        worker
            .worker
            .store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
    let now = Utc::now();
    let id = worker.test_start_at(now).await.unwrap().unwrap();
    let restarted = DurableMaintenanceWorker::new(
        storage.clone(),
        scope.clone(),
        DurableAuthorityBinding::new([7; 32]),
    )
    .unwrap();
    let progress = restarted.resume_at(&id, now).await.unwrap();
    assert_eq!(progress.status, MaintenanceStatus::Active);
    assert_eq!(progress.completed, 0);
    assert_eq!(progress.total, 1);
    let loaded = LoadedJob::load(&restarted.worker.store, &id, restarted.binding)
        .await
        .unwrap();
    for page in &loaded.pages {
        assert!(
            restarted
                .worker
                .store
                .storage
                .head(
                    &restarted
                        .worker
                        .store
                        .paths
                        .state_object(&page.output.state_id)
                )
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(
        !restarted
            .worker
            .store
            .externally_protected(now, |_| true)
            .await
            .unwrap()
    );
    let wrong =
        DurableMaintenanceWorker::new(storage, scope, DurableAuthorityBinding::new([8; 32]))
            .unwrap();
    assert!(wrong.resume_at(&id, now).await.is_err());
    assert!(
        restarted
            .resume_at(&id, now + ChronoDuration::hours(24))
            .await
            .is_err()
    );
    let pin = expected_pin(&loaded.descriptor, id.as_str()).unwrap();
    assert!(pin.renew(2, now + ChronoDuration::days(9), now).is_err());
    assert!(pin.release(2, now).is_err());
}

#[tokio::test]
async fn preflight_constructs_logical_outbox_slices_and_canonical_empty_state() {
    for outbox in [false, true] {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store =
            ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", "catalog"))
                .unwrap()
                .with_test_segment_sizing(2, 8 * 1024)
                .unwrap();
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        if outbox {
            for ordinal in 0..12 {
                tx.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
                    format!("event-{ordinal}"),
                    Bytes::from(vec![1; 16 * 1024]),
                ))
                .await
                .unwrap();
            }
        }
        tx.commit().await.unwrap();
        let source = store
            .load_manifest_for_pointer(&store.load_pointer().await.unwrap())
            .await
            .unwrap();
        let plan = PreparedPlan::build(&store, &source, &"a".repeat(64))
            .await
            .unwrap();
        assert_eq!(plan.pages.len(), if outbox { 6 } else { 1 });
        let mut actual = ReplayState {
            logical_sequence: source.logical_sequence,
            history_root: source.history_root.clone(),
            ..ReplayState::default()
        };
        for bytes in plan.pages {
            let page: PlanPage = decode_json(&bytes, "test plan").unwrap();
            cost::take();
            let rendered = page.construct(&store, &source).await.unwrap();
            let work = cost::take();
            assert_eq!(
                work.values()
                    .map(|row| row.get(19).copied().unwrap_or(0))
                    .sum::<u64>(),
                page.rows as u64,
                "construction must account for every rendered row"
            );
            let rows = decode_segment_rows(
                &rendered.bytes,
                &rendered.index_bytes,
                &state_segment_reference(&rendered.reference),
                &store.scope,
            )
            .unwrap();
            actual
                .append_snapshot(
                    state_object_from_segment_rows(&rendered.reference, rows, &store.scope)
                        .unwrap(),
                )
                .unwrap();
        }
        assert_eq!(actual, store.replay_for_successor(&source).await.unwrap());
    }
}
#[tokio::test]
async fn selected_progress_rejects_corruption_reordering_and_cross_job_references() {
    let (worker, plan, now) = prepared_fixture().await;
    worker.start_at(&plan, now).await.unwrap();
    worker.advance_at(&plan.id, now).await.unwrap();
    let store = &worker.worker.store;
    worker
        .worker
        .lifecycle
        .put_raw(
            &store.paths.state_object("orphan-progress-test"),
            Bytes::from_static(b"orphan"),
            arco_core::WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    let path = selector_path(store, plan.id.as_str());
    let original = worker.worker.lifecycle.get_raw(&path).await.unwrap();
    let job = LoadedJob::load(store, &plan.id, worker.binding)
        .await
        .unwrap();
    for mutation in 0..11 {
        let mut revision = job.last().unwrap().1.clone();
        match mutation {
            0 => revision.version = 2,
            1 => revision.job = "a".repeat(64),
            2 => revision.revision = 0,
            3 => revision.completed += 1,
            4 => revision.predecessor = None,
            5 => revision.receipt.as_mut().unwrap().ordinal += 1,
            6 => revision.receipt.as_mut().unwrap().logical_digest = "b".repeat(64),
            7 => revision.receipt.as_mut().unwrap().output.segment_size_bytes += 1,
            8 => revision.attempt = Some("c".repeat(64)),
            9 => revision.submissions = 1,
            10 => revision.submission_nonce = Some(Ulid::new().to_string()),
            _ => unreachable!(),
        }
        let (bytes, selector) = revision_bytes(&revision).unwrap();
        immutable_reconciled(
            store,
            &revision_path(store, plan.id.as_str(), &sha256_hex(&bytes)),
            bytes,
        )
        .await
        .unwrap();
        worker
            .worker
            .lifecycle
            .put_raw(&path, selector, arco_core::WritePrecondition::None)
            .await
            .unwrap();
        assert!(
            worker.resume_at(&plan.id, now).await.is_err(),
            "invalid progress mutation {mutation}"
        );
        assert!(
            worker
                .worker
                .plan_gc_at(
                    now + ChronoDuration::days(7) + ChronoDuration::hours(1),
                    Vec::new()
                )
                .await
                .is_err(),
            "GC ignored invalid selected progress {mutation}"
        );
        worker
            .worker
            .lifecycle
            .put_raw(&path, original.clone(), arco_core::WritePrecondition::None)
            .await
            .unwrap();
    }
    for bytes in [Bytes::from_static(b"{"), Bytes::from(vec![b' '; 8193])] {
        worker
            .worker
            .lifecycle
            .put_raw(&path, bytes, arco_core::WritePrecondition::None)
            .await
            .unwrap();
        assert!(worker.resume_at(&plan.id, now).await.is_err());
    }
    worker
        .worker
        .lifecycle
        .put_raw(&path, original, arco_core::WritePrecondition::None)
        .await
        .unwrap();
    assert_eq!(worker.resume_at(&plan.id, now).await.unwrap().completed, 1);
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Keep corruption schedules and restoration in one fixture.
async fn maintenance_evidence_rejects_missing_truncated_oversized_and_copied_records() {
    use crate::workspace_snapshot::{retention_pin_latest_path, retention_pin_revision_path};
    let (worker, plan, now) = prepared_fixture().await;
    worker.start_at(&plan, now).await.unwrap();
    while worker.advance_at(&plan.id, now).await.unwrap().status == MaintenanceStatus::Active {}
    let store = &worker.worker.store;
    let job = LoadedJob::load(store, &plan.id, worker.binding)
        .await
        .unwrap();
    let candidate = worker.prepare_publication(&plan.id, &job, 0).await.unwrap();
    let attempt_bytes =
        encode_json_limited(&candidate.attempt, MAX_PLAN_PAGE_BYTES, "test attempt").unwrap();
    let digest = sha256_hex(&attempt_bytes);
    let attempt = attempt_path(store, plan.id.as_str(), &digest);
    immutable_reconciled(store, &attempt, attempt_bytes)
        .await
        .unwrap();
    worker
        .begin_submission(
            &plan.id,
            &job,
            prepare_submission(&plan.id, &job, &digest).unwrap(),
        )
        .await
        .unwrap();
    let (foreign, foreign_plan, _) = prepared_fixture().await;
    let copied_descriptor = foreign_plan.bytes.clone();
    let copied_page = foreign_plan.pages[0].clone();
    let mut copied_attempt = candidate.attempt.clone();
    copied_attempt.job = foreign_plan.id.as_str().into();
    let copied_attempt =
        encode_json_limited(&copied_attempt, MAX_PLAN_PAGE_BYTES, "foreign attempt").unwrap();
    let foreign_pin = activation_bytes(&foreign_plan.descriptor, &foreign_plan.id).unwrap();
    drop(foreign);
    let mut artifacts = vec![
        (
            descriptor_path(store, plan.id.as_str()),
            Some(copied_descriptor),
        ),
        (
            page_path(store, plan.id.as_str(), 0, &plan.descriptor.pages[0]),
            Some(copied_page),
        ),
        (attempt, Some(copied_attempt)),
        (
            retention_pin_latest_path(&plan.descriptor.pin_id()).unwrap(),
            Some(foreign_pin.1),
        ),
        (
            retention_pin_revision_path(&plan.descriptor.pin_id(), 1).unwrap(),
            Some(foreign_pin.0),
        ),
    ];
    artifacts.push((
        store.paths.state_object(&job.pages[0].output.state_id),
        None,
    ));
    artifacts.push((
        store.paths.segment_index(&job.pages[0].output.state_id),
        None,
    ));
    let head_path = store.paths.current_pointer();
    let expected_head = worker.worker.lifecycle.get_raw(&head_path).await.unwrap();
    for (path, copied) in artifacts {
        let original = worker.worker.lifecycle.get_raw(&path).await.unwrap();
        let mut mutations = vec![
            None,
            Some(Bytes::from_static(b"{")),
            Some(Bytes::from(vec![b' '; 65537])),
        ];
        if let Some(copied) = copied {
            mutations.push(Some(copied));
        }
        for mutation in mutations {
            if let Some(bytes) = mutation {
                worker
                    .worker
                    .lifecycle
                    .put_raw(&path, bytes, arco_core::WritePrecondition::None)
                    .await
                    .unwrap();
            } else {
                worker.worker.lifecycle.delete(&path).await.unwrap();
            }
            assert!(
                worker.resume_at(&plan.id, now).await.is_err(),
                "resume accepted {path}"
            );
            assert!(
                worker.publish_at(&plan.id, now).await.is_err(),
                "publication accepted {path}"
            );
            assert_eq!(
                worker.worker.lifecycle.get_raw(&head_path).await.unwrap(),
                expected_head
            );
            worker
                .worker
                .lifecycle
                .put_raw(&path, original.clone(), arco_core::WritePrecondition::None)
                .await
                .unwrap();
        }
    }
    worker.publish_at(&plan.id, now).await.unwrap().unwrap();
    let candidate_path = store.paths.manifest_object(&candidate.attempt.candidate_id);
    for path in [candidate_path, head_path] {
        let original = worker.worker.lifecycle.get_raw(&path).await.unwrap();
        for mutation in [
            None,
            Some(Bytes::from_static(b"{")),
            Some(Bytes::from(vec![b' '; 1_048_577])),
        ] {
            if let Some(bytes) = mutation {
                worker
                    .worker
                    .lifecycle
                    .put_raw(&path, bytes, arco_core::WritePrecondition::None)
                    .await
                    .unwrap();
            } else {
                worker.worker.lifecycle.delete(&path).await.unwrap();
            }
            assert!(
                worker.publish_at(&plan.id, now).await.is_err(),
                "reconciliation accepted unavailable {path}"
            );
            worker
                .worker
                .lifecycle
                .put_raw(&path, original.clone(), arco_core::WritePrecondition::None)
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn completed_outputs_survive_writer_epoch_change_and_aba_descendants() {
    let (worker, plan, now) = prepared_fixture().await;
    worker.start_at(&plan, now).await.unwrap();
    while worker.advance_at(&plan.id, now).await.unwrap().status == MaintenanceStatus::Active {}
    let job = LoadedJob::load(&worker.worker.store, &plan.id, worker.binding)
        .await
        .unwrap();
    let mut output_versions = Vec::new();
    for page in &job.pages {
        for path in [
            worker
                .worker
                .store
                .paths
                .state_object(&page.output.state_id),
            worker
                .worker
                .store
                .paths
                .segment_index(&page.output.state_id),
        ] {
            output_versions.push((
                path.clone(),
                worker
                    .worker
                    .lifecycle
                    .head_raw(&path)
                    .await
                    .unwrap()
                    .unwrap()
                    .version,
            ));
        }
    }
    let writer = worker
        .worker
        .store
        .clone()
        .claim_writer_authority()
        .await
        .unwrap();
    for value in [
        Some(b"changed".as_slice()),
        None,
        Some(b"value\0".as_slice()),
        Some(b"value\0".as_slice()),
    ] {
        let mut tx = writer
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        if let Some(value) = value {
            tx.put(b"binary\0", Bytes::copy_from_slice(value))
                .await
                .unwrap();
        } else {
            tx.delete(b"binary\0").await.unwrap();
        }
        tx.commit().await.unwrap();
    }
    let pointer = writer.load_pointer().await.unwrap();
    let source = writer.load_manifest_for_pointer(&pointer).await.unwrap();
    let expected = writer.replay_for_successor(&source).await.unwrap();
    assert_eq!(
        worker.resume_at(&plan.id, now).await.unwrap().completed,
        job.pages.len()
    );
    worker.publish_at(&plan.id, now).await.unwrap().unwrap();
    let selected = writer.load_pointer().await.unwrap();
    assert_eq!(selected.writer_epoch, writer.writer_epoch());
    let manifest = writer.load_manifest_for_pointer(&selected).await.unwrap();
    assert_eq!(
        writer.replay_for_successor(&manifest).await.unwrap(),
        expected
    );
    for (path, version) in output_versions {
        assert_eq!(
            worker
                .worker
                .lifecycle
                .head_raw(&path)
                .await
                .unwrap()
                .unwrap()
                .version,
            version,
            "completed bytes were PUT again"
        );
    }
}

#[tokio::test]
async fn retention_hash_ledger_counts_each_actual_digest_once() {
    let (_worker, plan, _) = prepared_fixture().await;
    let bytes = activation_bytes(&plan.descriptor, &plan.id).unwrap();
    let selector = crate::workspace_snapshot::decode_retention_pin_latest(&bytes.1).unwrap();
    cost::take();
    crate::gc::reachability::SelectedRetentionPin::from_revision_bytes(
        selector,
        &[bytes.0.to_vec()],
    )
    .unwrap();
    let work = cost::take();
    assert_eq!(
        work.values()
            .map(|row| row.get(17).copied().unwrap_or(0))
            .sum::<u64>(),
        2
    );
    assert_eq!(
        work.values()
            .map(|row| row.get(18).copied().unwrap_or(0))
            .sum::<u64>(),
        2 * bytes.0.len() as u64
    );
    crate::workspace_snapshot::decode_retention_pin_revision(&bytes.0).unwrap();
    let work = cost::take();
    assert_eq!(
        work.values()
            .map(|row| row.get(17).copied().unwrap_or(0))
            .sum::<u64>(),
        1
    );
    assert_eq!(
        work.values()
            .map(|row| row.get(18).copied().unwrap_or(0))
            .sum::<u64>(),
        bytes.0.len() as u64
    );
}

async fn persist_inactive(worker: &DurableMaintenanceWorker, plan: &PreparedMaintenance) {
    let store = &worker.worker.store;
    immutable_reconciled(
        store,
        &descriptor_path(store, plan.id.as_str()),
        plan.bytes.clone(),
    )
    .await
    .unwrap();
    for (ordinal, bytes) in plan.pages.iter().enumerate() {
        immutable_reconciled(
            store,
            &page_path(
                store,
                plan.id.as_str(),
                ordinal,
                &plan.descriptor.pages[ordinal],
            ),
            bytes.clone(),
        )
        .await
        .unwrap();
    }
}

async fn assert_no_activation(worker: &DurableMaintenanceWorker, plan: &PreparedMaintenance) {
    let store = &worker.worker.store;
    assert!(
        store
            .storage
            .head(&selector_path(store, plan.id.as_str()))
            .await
            .unwrap()
            .is_none()
    );
    for path in [
        crate::workspace_snapshot::retention_pin_revision_path(&plan.descriptor.pin_id(), 1)
            .unwrap(),
        crate::workspace_snapshot::retention_pin_latest_path(&plan.descriptor.pin_id()).unwrap(),
        crate::retention_coordination::RETENTION_MUTATION_EPOCH_PATH.to_owned(),
    ] {
        assert!(
            worker
                .worker
                .lifecycle
                .head_raw(&path)
                .await
                .unwrap()
                .is_none(),
            "unexpected activation artifact: {path}"
        );
    }
}

#[tokio::test]
async fn stale_time_inactive_recovery_rejects_without_activation() {
    let (worker, _, now) = prepared_fixture().await;
    let stale = now - ChronoDuration::hours(25);
    let plan = worker.prepare_at(stale).await.unwrap().unwrap();
    persist_inactive(&worker, &plan).await;
    assert!(matches!(
        worker.recover_activation_at(&plan.id, stale).await,
        Err(CatalogError::PreconditionFailed { .. })
    ));
    assert_no_activation(&worker, &plan).await;
}

#[tokio::test]
async fn unsubmitted_recovery_waiting_on_retention_lock_rechecks_expiry() {
    let (worker, _, _) = prepared_fixture().await;
    let stale = Utc::now() - ChronoDuration::hours(24) + ChronoDuration::milliseconds(650);
    let plan = worker.prepare_at(stale).await.unwrap().unwrap();
    persist_inactive(&worker, &plan).await;
    let guard = DistributedLock::new(
        Arc::new(worker.worker.lifecycle.clone()),
        RETENTION_GC_LOCK_PATH,
    )
    .acquire(RETENTION_GC_LOCK_TTL, 1)
    .await
    .unwrap();
    let recovery = worker.recover_activation_at(&plan.id, stale);
    tokio::pin!(recovery);
    let wait = (plan.descriptor.expires_at - Utc::now()).to_std().unwrap()
        + std::time::Duration::from_millis(20);
    tokio::select! {
        result = &mut recovery => panic!("recovery did not wait for retention lock: {result:?}"),
        () = tokio::time::sleep(wait) => {}
    }
    guard.release().await.unwrap();
    assert!(matches!(
        recovery.await,
        Err(CatalogError::PreconditionFailed { .. })
    ));
    assert_no_activation(&worker, &plan).await;
}

#[tokio::test]
async fn stale_time_resume_and_no_work_advance_abandon_reject_expiry() {
    for abandoned in [false, true] {
        let (worker, _, _) = prepared_fixture().await;
        let stale = Utc::now() - ChronoDuration::hours(24) + ChronoDuration::seconds(2);
        let plan = worker.prepare_at(stale).await.unwrap().unwrap();
        worker.start_at(&plan, stale).await.unwrap();
        if abandoned {
            worker.abandon_at(&plan.id, stale).await.unwrap();
        } else {
            while worker.advance_at(&plan.id, stale).await.unwrap().status
                != MaintenanceStatus::ReadyToPublish
            {}
        }
        let wait = (plan.descriptor.expires_at - Utc::now()).to_std().unwrap()
            + std::time::Duration::from_millis(20);
        tokio::time::sleep(wait).await;
        assert!(matches!(
            worker.resume_at(&plan.id, stale).await,
            Err(CatalogError::PreconditionFailed { .. })
        ));
        let result = if abandoned {
            worker.abandon_at(&plan.id, stale).await
        } else {
            worker.advance_at(&plan.id, stale).await
        };
        assert!(matches!(
            result,
            Err(CatalogError::PreconditionFailed { .. })
        ));
    }
}

#[tokio::test]
async fn maintenance_phase_accounting_separates_selection_reads_render_reuse_equivalence() {
    let (worker, plan, now) = prepared_fixture().await;
    let store = &worker.worker.store;
    let job_source = store
        .load_manifest_with_expected_checksum(
            &plan.descriptor.source_id,
            Some(&plan.descriptor.source_digest),
        )
        .await
        .unwrap();
    let pages = plan
        .pages
        .iter()
        .map(|b| decode_json::<PlanPage>(b, "test page").unwrap())
        .collect::<Vec<_>>();
    cost::take();
    pages[0].construct(store, &job_source).await.unwrap();
    let single = cost::take();
    assert!(single.contains_key("maintenance-selection"));
    assert!(single.contains_key("maintenance-source-metadata"));
    assert!(single.contains_key("maintenance-selected-data-reads"));
    assert_eq!(
        single["maintenance-output-rendering-validation"][19],
        pages[0].rows as u64
    );
    for _ in 0..2 {
        pages[0].construct(store, &job_source).await.unwrap();
    }
    let double = cost::take();
    assert_eq!(
        double["maintenance-output-rendering-validation"][19],
        2 * single["maintenance-output-rendering-validation"][19]
    );
    worker.start_at(&plan, now).await.unwrap();
    while worker.advance_at(&plan.id, now).await.unwrap().status
        != MaintenanceStatus::ReadyToPublish
    {}
    cost::take();
    worker.publish_at(&plan.id, now).await.unwrap().unwrap();
    let publication = cost::take();
    assert!(publication.contains_key("maintenance-completed-output-reuse"));
    assert!(publication.contains_key("maintenance-final-equivalence"));
    assert!(!publication.contains_key("maintenance-output-rendering-validation"));
}

#[tokio::test]
async fn expired_activation_claim_is_not_prior_submission_evidence() {
    let (worker, _, now) = prepared_fixture().await;
    let stale = now - ChronoDuration::hours(25);
    let plan = worker.prepare_at(stale).await.unwrap().unwrap();
    persist_inactive(&worker, &plan).await;
    let mut guard = DistributedLock::new(
        Arc::new(worker.worker.lifecycle.clone()),
        RETENTION_GC_LOCK_PATH,
    )
    .acquire(RETENTION_GC_LOCK_TTL, 1)
    .await
    .unwrap();
    let _epoch = RetentionMutationEpoch::claim_maintenance_root(
        worker.worker.lifecycle.clone(),
        &mut guard,
        plan.id.as_str(),
        None,
    )
    .await
    .unwrap();
    guard.release().await.unwrap();
    assert!(matches!(
        worker.recover_activation_at(&plan.id, stale).await,
        Err(CatalogError::PreconditionFailed { .. })
    ));
    assert!(
        worker
            .worker
            .store
            .storage
            .head(&selector_path(&worker.worker.store, plan.id.as_str()))
            .await
            .unwrap()
            .is_none()
    );
}
