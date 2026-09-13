//! Eight real remote-operation boundaries crossed with four faults and three reader caches.
use super::*;
use arco_catalog::state_store::projection_outbox_acks::{
    PROJECTION_OUTBOX_ACK_DOMAIN, ProjectionOutboxAckWriter, ProjectionOutboxDeliveryId,
};
use arco_catalog::{
    ControlMvpRestoreParticipant, DurableAuthorityBinding, DurableMaintenanceWorker,
    MaintenanceStatus, RestoreAttemptIdentity, StateRestoreParticipant as _,
};

#[derive(serde::Serialize)]
struct Notice {
    restore_id: &'static str,
    participant_attempt: u64,
    domain: &'static str,
    source_logical_sequence: u64,
    result_logical_sequence: u64,
}

async fn deliver<T: std::fmt::Debug>(
    schedule: &Arc<Schedule>,
    operation: impl Future<Output = Result<T>>,
) {
    let mut call = Box::pin(operation);
    if matches!(schedule.fault, Fault::PauseBefore | Fault::PauseAfter) {
        tokio::select! { result = &mut call => panic!("boundary not reached: {result:?}"), () = schedule.issued() => {} }
        drop(call);
    } else {
        let result = guard(call).await;
        if matches!(schedule.fault, Fault::DelayedError) {
            assert!(
                result.is_err(),
                "pending remote request must not be acknowledged"
            );
        }
    }
    schedule.finish().await;
}

async fn run(boundary: usize, mode: usize, fault: Fault) {
    let mut f = Fixture::new().await;
    f.store = Arc::new(gate7_model::configured(&f, mode, 32 * 1024));
    if let Ok(directory) = std::env::var("ARCO_GATE7_EVIDENCE") {
        f.backend.trace.lock().unwrap().attach(
            &std::path::Path::new(&directory)
                .join(format!("live-schedule-{boundary}-{mode}-{fault:?}.log")),
            &format!("boundary={boundary} mode={mode} fault={fault:?}"),
        );
    }

    let result = Box::pin(
        std::panic::AssertUnwindSafe(async {
            let original = f.store.current_state_token().await.unwrap();
            let retained = f.store.read_at(original.clone()).await.unwrap();
            assert_eq!(
                retained.get(b"key").await.unwrap(),
                Some(Bytes::from_static(b"value"))
            );
            // The mode axis belongs to this retained catalog reader; worker ownership is unchanged.
            assert_eq!(
                retained.get(b"key").await.unwrap(),
                Some(Bytes::from_static(b"value"))
            );
            let warm_ledger = f.store.read_cache().map(|cache| cache.statistics());
            if mode == 0 {
                assert!(warm_ledger.is_none());
            } else {
                let ledger = warm_ledger.as_ref().unwrap();
                let factor = if mode == 1 { 32 } else { 1 };
                assert_eq!(ledger.metadata.capacity_bytes, factor * 1024 * 1024);
                assert_eq!(ledger.decoded.capacity_bytes, factor * 4 * 1024 * 1024);
                assert!(
                    ledger.loads > 0 && ledger.hits > 0,
                    "retained reader must actually be warm"
                );
            }
            f.backend.trace.lock().unwrap().push(format!(
                "RETAINED_READER_BEFORE mode={mode} ledger={}",
                serde_json::to_string(&warm_ledger).unwrap()
            ));
            let mut oracle = LogicalOracle::new();
            oracle.commit(
                vec![(b"key".to_vec(), Some(Bytes::from_static(b"value")))],
                vec![],
                vec![],
            );
            match boundary {
                0..=2 => {
                    let checkpoint = f
                        .store
                        .checkpoint(CheckpointOptions::default())
                        .await
                        .unwrap();
                    let reference = f
                        .store
                        .persist_checkpoint_reference(
                            &checkpoint,
                            f.start + chrono::Duration::days(30),
                        )
                        .await
                        .unwrap();
                    f.commit(b"later").await;
                    oracle.commit(
                        vec![(b"key".to_vec(), Some(Bytes::from_static(b"later")))],
                        vec![],
                        vec![],
                    );
                    let participant = ControlMvpRestoreParticipant::new(f.store.as_ref().clone());
                    let id =
                        RestoreAttemptIdentity::new("rst_00000000000000000000000007", 1, "catalog")
                            .unwrap();
                    let plan = participant
                        .plan_restore(&reference, &id, f.start)
                        .await
                        .unwrap();
                    let needle = match boundary {
                        0 => "/transactions/",
                        1 => "/manifests/",
                        _ => "/head/current.json",
                    };
                    let schedule = f.backend.arm(needle.into(), 0, fault);
                    deliver(&schedule, participant.apply_restore(&plan, f.start)).await;
                    f.backend.expire_lease(&f.storage).await;
                    participant.apply_restore(&plan, f.start).await.unwrap();
                    let request = "restore:rst_00000000000000000000000007:1:catalog";
                    let notice = Bytes::from(
                        serde_json::to_vec(&Notice {
                            restore_id: "rst_00000000000000000000000007",
                            participant_attempt: 1,
                            domain: "catalog",
                            source_logical_sequence: 1,
                            result_logical_sequence: 3,
                        })
                        .unwrap(),
                    );
                    oracle.commit_with_request(
                        vec![(b"key".to_vec(), Some(Bytes::from_static(b"value")))],
                        vec![(request.into(), notice)],
                        vec![],
                        Some(request),
                    );
                }
                3..=4 => {
                    let scope = StateScope::new("tenant", "workspace", "catalog");
                    let binding = DurableAuthorityBinding::new([72; 32]);
                    let worker =
                        DurableMaintenanceWorker::new(f.storage.clone(), scope.clone(), binding)
                            .unwrap();
                    let plan = worker
                        .test_prepare_forced_at(f.start)
                        .await
                        .unwrap()
                        .unwrap();
                    worker.start_at(&plan, f.start).await.unwrap();
                    if boundary == 4 {
                        assert_eq!(
                            worker
                                .advance_at(plan.job_id(), f.start)
                                .await
                                .unwrap()
                                .status,
                            MaintenanceStatus::ReadyToPublish
                        );
                    }
                    let needle = if boundary == 3 {
                        "/selected.json"
                    } else {
                        "/head/current.json"
                    };
                    let schedule = f.backend.arm(needle.into(), 0, fault);
                    if boundary == 3 {
                        deliver(&schedule, worker.advance_at(plan.job_id(), f.start)).await;
                    } else {
                        deliver(&schedule, worker.publish_at(plan.job_id(), f.start)).await;
                    }
                    f.backend.expire_lease(&f.storage).await;
                    let restarted =
                        DurableMaintenanceWorker::new(f.storage.clone(), scope, binding).unwrap();
                    let progress = restarted.resume_at(plan.job_id(), f.start).await.unwrap();
                    if progress.status == MaintenanceStatus::Active {
                        restarted.advance_at(plan.job_id(), f.start).await.unwrap();
                    }
                    assert!(
                        restarted
                            .publish_at(plan.job_id(), f.start)
                            .await
                            .unwrap()
                            .is_some()
                    );
                }
                5 => {
                    let path = f.store.paths().tx_object("gate7-scheduled-orphan");
                    f.storage
                        .put_raw(
                            &path,
                            Bytes::from_static(b"orphan"),
                            WritePrecondition::DoesNotExist,
                        )
                        .await
                        .unwrap();
                    let schedule = f.backend.arm(path.clone(), 0, fault);
                    deliver(&schedule, async {
                        f.worker
                            .collect_gc_page_at(f.start + chrono::Duration::days(9), vec![], None)
                            .await
                    })
                    .await;
                    f.backend.expire_lease(&f.storage).await;
                    recover_stale_retention_epoch(&f.storage, "Gate 7 completed remote deletion")
                        .await
                        .unwrap();
                    assert!(f.storage.head_raw(&path).await.unwrap().is_none());
                }
                6..=7 => {
                    let mut tx = f
                        .store
                        .begin_control_txn(TxnOptions::default())
                        .await
                        .unwrap();
                    tx.stage_projection_outbox(
                        arco_catalog::ControlMvpProjectionOutboxRecord::new(
                            "event",
                            Bytes::from_static(b"payload"),
                        ),
                    )
                    .await
                    .unwrap();
                    tx.commit().await.unwrap();
                    oracle.commit(
                        vec![],
                        vec![("event".into(), Bytes::from_static(b"payload"))],
                        vec![],
                    );
                    let delivery = ProjectionOutboxDeliveryId::new("gate7", 1, "event", 2);
                    let acks = ProjectionOutboxAckWriter::new(
                        f.storage.clone(),
                        StateScope::new("tenant", "workspace", PROJECTION_OUTBOX_ACK_DOMAIN),
                    )
                    .unwrap();
                    acks.acknowledge(&delivery).await.unwrap();
                    if boundary == 6 {
                        let needle = format!("/{PROJECTION_OUTBOX_ACK_DOMAIN}/head/current.json");
                        let schedule = f.backend.arm(needle, 0, fault);
                        deliver(&schedule, acks.retire_acknowledgements(&[delivery.clone()])).await;
                        f.backend.expire_lease(&f.storage).await;
                    }
                    // Retire and reconcile the acknowledgement before any source trim.
                    acks.retire_acknowledgements(&[delivery]).await.unwrap();
                    assert!(
                        acks.acknowledged_event_ids("gate7", 1)
                            .await
                            .unwrap()
                            .is_empty()
                    );
                    let mut tx = f
                        .store
                        .begin_control_txn(TxnOptions::default())
                        .await
                        .unwrap();
                    tx.trim_projection_outbox([arco_catalog::ControlMvpOutboxTrimTarget::new(
                        "event", 2,
                    )])
                    .await
                    .unwrap();
                    if boundary == 7 {
                        let schedule = f.backend.arm("/catalog/head/current.json".into(), 0, fault);
                        deliver(&schedule, tx.commit()).await;
                        f.backend.expire_lease(&f.storage).await;
                    } else {
                        tx.commit().await.unwrap();
                    }
                    if !f
                        .store
                        .current_projection_outbox()
                        .await
                        .unwrap()
                        .is_empty()
                    {
                        let mut tx = f
                            .store
                            .begin_control_txn(TxnOptions::default())
                            .await
                            .unwrap();
                        tx.trim_projection_outbox([arco_catalog::ControlMvpOutboxTrimTarget::new(
                            "event", 2,
                        )])
                        .await
                        .unwrap();
                        tx.commit().await.unwrap();
                    }
                    oracle.commit(vec![], vec![], vec![("event".into(), 2)]);
                }
                _ => unreachable!(),
            }
            oracle
                .assert_manifest(
                    &f.storage,
                    f.store
                        .current_state_token()
                        .await
                        .unwrap()
                        .authority_manifest_id(),
                )
                .await;
            assert_eq!(
                retained.get(b"key").await.unwrap(),
                Some(Bytes::from_static(b"value"))
            );
            let final_ledger = f.store.read_cache().map(|cache| cache.statistics());
            if let Some(ledger) = &final_ledger {
                assert!(ledger.metadata.high_water_bytes <= ledger.metadata.capacity_bytes);
                assert!(ledger.decoded.high_water_bytes <= ledger.decoded.capacity_bytes);
                assert_eq!((ledger.active_loads, ledger.participants), (0, 0));
            }
            f.backend.trace.lock().unwrap().push(format!(
                "RETAINED_READER_AFTER mode={mode} ledger={}",
                serde_json::to_string(&final_ledger).unwrap()
            ));
            assert_eq!(
                f.store.get(b"key").await.unwrap(),
                Some(Bytes::from_static(b"value"))
            );
        })
        .catch_unwind(),
    )
    .await;
    if let Ok(directory) = std::env::var("ARCO_GATE7_EVIDENCE") {
        std::fs::write(
            std::path::Path::new(&directory)
                .join(format!("schedule-{boundary}-{mode}-{fault:?}.log")),
            f.backend.trace.lock().unwrap().join("\n"),
        )
        .unwrap();
    }
    assert!(
        result.is_ok(),
        "boundary={boundary} mode={mode} fault={fault:?} trace={:?}",
        f.backend.trace.lock().unwrap()
    );
}

#[tokio::test]
async fn eight_boundaries_four_faults_three_cache_modes() {
    for boundary in 0..8 {
        for mode in 0..3 {
            for fault in [
                Fault::PauseBefore,
                Fault::PauseAfter,
                Fault::LostResponse,
                Fault::DelayedError,
            ] {
                let result = std::panic::AssertUnwindSafe(Box::pin(run(boundary, mode, fault)))
                    .catch_unwind()
                    .await;
                assert!(
                    result.is_ok(),
                    "boundary={boundary} mode={mode} fault={fault:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn lost_delete_response_is_observable_after_remote_application() {
    let f = Fixture::new().await;
    f.storage
        .put_raw(
            "gate7-delete-probe",
            Bytes::from_static(b"x"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    let schedule = f
        .backend
        .arm("gate7-delete-probe".into(), 0, Fault::LostResponse);
    let result = f.storage.delete("gate7-delete-probe").await;
    schedule.finish().await;
    assert!(
        f.storage
            .head_raw("gate7-delete-probe")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        result.is_err(),
        "the fixture must actually lose the DELETE response"
    );
}

#[tokio::test]
#[ignore = "subprocess fixture intentionally waits for its parent to terminate it"]
async fn crash_trace_child() {
    use std::io::Write as _;
    let Some(path) = std::env::var_os("ARCO_GATE7_CRASH_TRACE") else {
        return;
    };
    let f = Fixture::new().await;
    f.backend.trace.lock().unwrap().attach(
        std::path::Path::new(&path),
        "seed=33 mode=0 block=32768 step=0 family=9 boundary=probe fault=PauseBefore",
    );
    let schedule = f
        .backend
        .arm("gate7-crash-trace".into(), 0, Fault::PauseBefore);
    let backend = f.backend.clone();
    tokio::spawn(async move {
        backend
            .put(
                "gate7-crash-trace",
                Bytes::from_static(b"pending"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
    });
    schedule.issued().await;
    writeln!(std::io::stdout().lock(), "GATE7_BOUNDARY_ARMED").unwrap();
    std::future::pending::<()>().await;
}

#[test]
fn interrupted_schedule_preserves_live_identity_and_boundary() {
    use std::io::BufRead as _;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("crash.log");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "gate7_schedules::crash_trace_child",
            "--ignored",
            "--nocapture",
        ])
        .env("ARCO_GATE7_CRASH_TRACE", &path)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(output).lines() {
            if line.unwrap().contains("GATE7_BOUNDARY_ARMED") {
                sender.send(()).unwrap();
                break;
            }
        }
    });
    let reached = receiver.recv_timeout(Duration::from_secs(20));
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    reader.join().unwrap();
    reached.unwrap();
    let evidence = std::fs::read_to_string(path).unwrap();
    assert!(
        evidence.contains(
            "seed=33 mode=0 block=32768 step=0 family=9 boundary=probe fault=PauseBefore"
        )
    );
    assert!(evidence.contains("ARM gate7-crash-trace"));
    assert!(evidence.contains("ISSUE PUT gate7-crash-trace"));
    assert!(!evidence.contains("APPLY PUT gate7-crash-trace"));
}
