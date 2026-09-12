//! Additional Gate 7 seeds; the original model and seed inventory remain unchanged.
use super::*;
use arco_catalog::{
    ControlMvpOutboxTrimTarget, ControlMvpProjectionOutboxRecord, ControlMvpReadCacheConfig,
    ControlMvpRestoreParticipant, DurableAuthorityBinding, DurableMaintenanceWorker,
    RestoreAttemptIdentity, StateRestoreParticipant as _,
};

#[derive(serde::Serialize)]
struct Notice<'a> {
    restore_id: &'a str,
    participant_attempt: u64,
    domain: &'a str,
    source_logical_sequence: u64,
    result_logical_sequence: u64,
}

pub fn configured(f: &Fixture, mode: usize, block: usize) -> ControlMvpStateStore {
    let config = match mode {
        0 => ControlMvpReadCacheConfig {
            metadata_bytes: 0,
            decoded_bytes: 0,
        },
        1 => ControlMvpReadCacheConfig::default(),
        2 => ControlMvpReadCacheConfig {
            metadata_bytes: 1024 * 1024,
            decoded_bytes: 4 * 1024 * 1024,
        },
        _ => unreachable!(),
    };
    ControlMvpStateStore::new(
        f.storage.clone(),
        StateScope::new("tenant", "workspace", "catalog"),
    )
    .unwrap()
    .with_read_cache_config(config)
    .unwrap()
    .with_test_segment_sizing(500_000, block)
    .unwrap()
}

async fn check(f: &Fixture, logical: &LogicalOracle, contents: &Contents) {
    let token = f.store.current_state_token().await.unwrap();
    assert_eq!(token.logical_sequence(), logical.sequence);
    logical
        .assert_manifest(&f.storage, token.authority_manifest_id())
        .await;
    compare(f.store.as_ref(), contents).await;
    assert_eq!(
        f.store
            .current_projection_outbox()
            .await
            .unwrap()
            .iter()
            .map(|r| (
                r.record_id().to_owned(),
                r.payload().clone(),
                r.origin_sequence().unwrap()
            ))
            .collect::<Vec<_>>(),
        logical.outbox
    );
}

async fn mutation(
    f: &Fixture,
    logical: &mut LogicalOracle,
    contents: &mut Contents,
    value: Bytes,
    additions: Vec<(String, Bytes)>,
    trims: Vec<(String, u64)>,
) {
    let mut txn = f
        .store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    txn.put(b"key", value.clone()).await.unwrap();
    for (id, payload) in &additions {
        txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(id, payload.clone()))
            .await
            .unwrap();
    }
    txn.trim_projection_outbox(
        trims
            .iter()
            .map(|(id, origin)| ControlMvpOutboxTrimTarget::new(id, *origin)),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    contents.insert(b"key".to_vec(), value.clone());
    logical.commit(vec![(b"key".to_vec(), Some(value))], additions, trims);
    check(f, logical, contents).await;
}

async fn seed_layout(
    f: &Fixture,
    logical: &mut LogicalOracle,
    contents: &mut Contents,
    block: usize,
    trace: &Mutex<Vec<String>>,
) {
    // Seed enough encoded data to force distinct Arrow batch layouts at both targets.
    let mut tx = f
        .store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    let mut seed_writes = Vec::new();
    for row in 0..64 {
        let key = format!("layout-{row:03}").into_bytes();
        let value = Bytes::from(vec![u8::try_from(row).unwrap(); 2048]);
        tx.put(&key, value.clone()).await.unwrap();
        contents.insert(key.clone(), value.clone());
        seed_writes.push((key, Some(value)));
    }
    tx.commit().await.unwrap();
    logical.commit(seed_writes, vec![], vec![]);
    let layout_worker = DurableMaintenanceWorker::new(
        f.storage.clone(),
        StateScope::new("tenant", "workspace", "catalog"),
        DurableAuthorityBinding::new([71; 32]),
    )
    .unwrap()
    .with_test_segment_sizing(500_000, block)
    .unwrap();
    let plan = layout_worker
        .test_prepare_forced_at(f.start)
        .await
        .unwrap()
        .unwrap();
    let mut progress = layout_worker.start_at(&plan, f.start).await.unwrap();
    for _ in 0..512 {
        if progress.status == arco_catalog::MaintenanceStatus::ReadyToPublish {
            break;
        }
        progress = layout_worker
            .advance_at(plan.job_id(), f.start)
            .await
            .unwrap();
    }
    assert_eq!(
        progress.status,
        arco_catalog::MaintenanceStatus::ReadyToPublish
    );
    layout_worker
        .publish_at(plan.job_id(), f.start)
        .await
        .unwrap()
        .unwrap();
    let token = f.store.current_state_token().await.unwrap();
    logical
        .assert_manifest(&f.storage, token.authority_manifest_id())
        .await;
    let base = "control/v1/domains/catalog";
    let manifest: serde_json::Value = serde_json::from_slice(
        &f.storage
            .get_raw(&format!(
                "{base}/manifests/{}.json",
                token.authority_manifest_id()
            ))
            .await
            .unwrap(),
    )
    .unwrap();
    let mut batches = Vec::new();
    for reference in manifest
        .get("payload")
        .unwrap()
        .get("base_states")
        .unwrap()
        .as_array()
        .unwrap()
    {
        let id = reference.get("state_id").unwrap().as_str().unwrap();
        let directory: serde_json::Value = serde_json::from_slice(
            &f.storage
                .get_raw(&format!("{base}/indexes/{id}.idx"))
                .await
                .unwrap(),
        )
        .unwrap();
        batches.push(directory.get("blocks").unwrap().as_array().unwrap().len());
        trace.lock().unwrap().push(format!(
            "owned_layout manifest={} segment={id}",
            token.authority_manifest_id()
        ));
    }
    assert!(
        !batches.is_empty(),
        "physical layout must actually be materialized"
    );
    if block == 32 * 1024 {
        assert!(
            batches.iter().any(|count| *count > 1),
            "32 KiB layout must have multiple batches: {batches:?}"
        );
    } else {
        assert!(
            batches.iter().all(|count| *count == 1),
            "256 KiB layout must fit one batch: {batches:?}"
        );
    }
    trace.lock().unwrap().push(format!(
        "physical_layout target={block} batches={batches:?}"
    ));
}

#[allow(
    clippy::cognitive_complexity,
    reason = "keep generated transition assertions beside their oracle updates"
)]
async fn run(seed: u64, mode: usize, block: usize, trace: &Mutex<Vec<String>>) {
    let mut f = Fixture::new().await;
    f.store = Arc::new(configured(&f, mode, block));
    if let Ok(directory) = std::env::var("ARCO_GATE7_EVIDENCE") {
        f.backend.trace.lock().unwrap().attach(
            &std::path::Path::new(&directory).join(format!("live-model-{seed}-{mode}-{block}.log")),
            &format!("seed={seed} mode={mode} block={block} setup"),
        );
    }

    let mut logical = LogicalOracle::new();
    let mut contents = Contents::from([(b"key".to_vec(), Bytes::from_static(b"value"))]);
    logical.commit(
        vec![(b"key".to_vec(), Some(Bytes::from_static(b"value")))],
        vec![],
        vec![],
    );
    Box::pin(seed_layout(&f, &mut logical, &mut contents, block, trace)).await;
    let checkpoint = f
        .store
        .checkpoint(CheckpointOptions::default())
        .await
        .unwrap();
    let source_sequence = logical.sequence;
    let reference = f
        .store
        .persist_checkpoint_reference(&checkpoint, f.start + chrono::Duration::days(30))
        .await
        .unwrap();
    let retained = f.store.read_checkpoint(checkpoint).await.unwrap();
    assert_eq!(
        retained.get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"value"))
    );
    let worker = DurableMaintenanceWorker::new(
        f.storage.clone(),
        StateScope::new("tenant", "workspace", "catalog"),
        DurableAuthorityBinding::new([71; 32]),
    )
    .unwrap()
    .with_test_segment_sizing(500_000, block)
    .unwrap();
    let mut random = seed;
    let mut counts = [0_u32; 10];
    for step in 0..128 {
        let op = if step < 10 {
            step
        } else {
            (next_random(&mut random) % 10) as usize
        };
        trace.lock().unwrap().push(format!("seed={seed} mode={mode} block={block} step={step} family={op} before_sequence={} history={}", logical.sequence, logical.root));
        f.backend.trace.lock().unwrap().push(format!(
            "seed={seed} mode={mode} block={block} step={step} family={op} before_sequence={} history={}", logical.sequence, logical.root
        ));
        durable_maintenance::consolidate_pending(&worker)
            .await
            .unwrap();
        let value = Bytes::from(format!("{seed}-{step}"));
        match op {
            0 => {
                let additions = (0..3)
                    .map(|n| {
                        (
                            format!("event-{step}-{n}"),
                            Bytes::from(format!("payload-{seed}-{step}-{n}")),
                        )
                    })
                    .collect();
                mutation(&f, &mut logical, &mut contents, value, additions, vec![]).await;
            }
            1 => {
                // Establish prerequisites, then count an actual trim and a later incarnation.
                let id = format!("incarnation-{step}");
                mutation(
                    &f,
                    &mut logical,
                    &mut contents,
                    value.clone(),
                    vec![(id.clone(), Bytes::from_static(b"first"))],
                    vec![],
                )
                .await;
                let origin = logical.sequence;
                mutation(
                    &f,
                    &mut logical,
                    &mut contents,
                    value.clone(),
                    vec![],
                    vec![(id.clone(), origin)],
                )
                .await;
                mutation(
                    &f,
                    &mut logical,
                    &mut contents,
                    value,
                    vec![(id, Bytes::from_static(b"second"))],
                    vec![],
                )
                .await;
            }
            2 => {
                // Restore an older retained cut while preserving the destination's outbox/history.
                mutation(&f, &mut logical, &mut contents, value, vec![], vec![]).await;
                assert!(logical.sequence > source_sequence);
                let id = format!(
                    "rst_{}",
                    ulid::Ulid::from(u128::from(seed) * 1000 + step as u128)
                );
                let identity = RestoreAttemptIdentity::new(&id, 1, "catalog").unwrap();
                let participant = ControlMvpRestoreParticipant::new(f.store.as_ref().clone());
                let plan = participant
                    .plan_restore(&reference, &identity, f.start)
                    .await
                    .unwrap();
                participant.apply_restore(&plan, f.start).await.unwrap();
                let notice = Bytes::from(
                    serde_json::to_vec(&Notice {
                        restore_id: &id,
                        participant_attempt: 1,
                        domain: "catalog",
                        source_logical_sequence: source_sequence,
                        result_logical_sequence: logical.sequence + 1,
                    })
                    .unwrap(),
                );
                let request = format!("restore:{id}:1:catalog");
                logical.commit_with_request(
                    vec![(b"key".to_vec(), Some(Bytes::from_static(b"value")))],
                    vec![(request.clone(), notice)],
                    vec![],
                    Some(&request),
                );
                contents.insert(b"key".to_vec(), Bytes::from_static(b"value"));
            }
            3 => {
                assert_eq!(
                    retained.get(b"key").await.unwrap(),
                    Some(Bytes::from_static(b"value"))
                );
                let fresh = f
                    .store
                    .read_at(f.store.current_state_token().await.unwrap())
                    .await
                    .unwrap();
                compare(fresh.as_ref(), &contents).await;
            }
            4 => {
                let mut loser = f
                    .store
                    .begin_control_txn(TxnOptions::default())
                    .await
                    .unwrap();
                loser.get(b"key").await.unwrap();
                loser
                    .put(b"key", Bytes::from_static(b"must conflict"))
                    .await
                    .unwrap();
                mutation(&f, &mut logical, &mut contents, value, vec![], vec![]).await;
                assert!(
                    loser.commit().await.is_err(),
                    "stale lazy writer must not replace winner"
                );
            }
            5 => {
                f.store = Arc::new(configured(&f, mode, block));
                compare(f.store.as_ref(), &contents).await;
                compare(f.store.as_ref(), &contents).await;
            }
            6 => {
                let plan = worker
                    .test_prepare_forced_at(f.start)
                    .await
                    .unwrap()
                    .unwrap();
                worker.start_at(&plan, f.start).await.unwrap();
                let restarted = DurableMaintenanceWorker::new(
                    f.storage.clone(),
                    StateScope::new("tenant", "workspace", "catalog"),
                    DurableAuthorityBinding::new([71; 32]),
                )
                .unwrap();
                restarted.resume_at(plan.job_id(), f.start).await.unwrap();
                restarted.advance_at(plan.job_id(), f.start).await.unwrap();
                restarted.abandon_at(plan.job_id(), f.start).await.unwrap();
            }
            7 => {
                let plan = worker
                    .test_prepare_forced_at(f.start)
                    .await
                    .unwrap()
                    .unwrap();
                worker.start_at(&plan, f.start).await.unwrap();
                let expired = f.start + chrono::Duration::hours(25);
                assert!(worker.resume_at(plan.job_id(), expired).await.is_err());
                assert!(worker.advance_at(plan.job_id(), expired).await.is_err());
            }
            8 => {
                let path = f.store.paths().tx_object(&format!("gate7-orphan-{step}"));
                f.storage
                    .put_raw(
                        &path,
                        Bytes::from_static(b"orphan"),
                        WritePrecondition::DoesNotExist,
                    )
                    .await
                    .unwrap();
                let mut cursor = None;
                loop {
                    let page = f
                        .worker
                        .collect_gc_page_at(
                            f.start + chrono::Duration::days(9),
                            vec![],
                            cursor.as_deref(),
                        )
                        .await
                        .unwrap();
                    cursor = page.continuation().map(str::to_owned);
                    if cursor.is_none() {
                        break;
                    }
                }
                assert!(f.storage.head_raw(&path).await.unwrap().is_none());
            }
            9 => {
                let mut a = f
                    .store
                    .begin_control_txn(TxnOptions::default())
                    .await
                    .unwrap();
                let mut b = f
                    .store
                    .begin_control_txn(TxnOptions::default())
                    .await
                    .unwrap();
                a.put(b"key", value.clone()).await.unwrap();
                b.put(b"key", Bytes::from_static(b"competitor"))
                    .await
                    .unwrap();
                let (a, b) = tokio::join!(a.commit(), b.commit());
                assert_ne!(a.is_ok(), b.is_ok(), "exactly one HEAD winner");
                let winner = if a.is_ok() {
                    value
                } else {
                    Bytes::from_static(b"competitor")
                };
                logical.commit(
                    vec![(b"key".to_vec(), Some(winner.clone()))],
                    vec![],
                    vec![],
                );
                contents.insert(b"key".to_vec(), winner);
                trace
                    .lock()
                    .unwrap()
                    .push(format!("publication a={a:?} b={b:?}"));
            }
            _ => unreachable!(),
        }
        *counts.get_mut(op).unwrap() += 1;
        check(&f, &logical, &contents).await;
        assert_eq!(
            retained.get(b"key").await.unwrap(),
            Some(Bytes::from_static(b"value"))
        );
        trace
            .lock()
            .unwrap()
            .extend(std::mem::take(&mut f.backend.trace.lock().unwrap().events));
    }
    assert!(counts.iter().all(|n| *n > 0), "actual coverage {counts:?}");
    trace
        .lock()
        .unwrap()
        .push(format!("accepted_family_counts={counts:?}"));
}

#[tokio::test]
async fn older_cut_outbox_model_32_seeds_128_operations_three_caches_two_layouts() {
    for seed in 33..=64 {
        for mode in 0..3 {
            for block in [32 * 1024, 256 * 1024] {
                let trace = Mutex::new(Vec::new());
                let result = std::panic::AssertUnwindSafe(Box::pin(run(seed, mode, block, &trace)))
                    .catch_unwind()
                    .await;
                let trace = trace.into_inner().unwrap().join("\n");
                if let Ok(directory) = std::env::var("ARCO_GATE7_EVIDENCE") {
                    std::fs::write(
                        std::path::Path::new(&directory)
                            .join(format!("model-{seed}-{mode}-{block}.log")),
                        &trace,
                    )
                    .unwrap();
                }
                assert!(
                    result.is_ok(),
                    "Gate 7 seed={seed} mode={mode} block={block}\n{trace}"
                );
            }
        }
    }
}
