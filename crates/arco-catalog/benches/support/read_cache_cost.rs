//! Gate 6 common workload, also compiled against exact Gate 5 production source.
use super::{
    Arc, ArcoStateAdmin, ArcoStateReader, ArcoStateTxn, Bytes, Contents, ControlMvpStateStore,
    CountingBackend, Instant, Recorder, TxnOptions, measure_allocations, read_contents, store,
};

// Frozen before the fresh-audit rerun; each observation owns a new empty cache.
const COLD_SAMPLES: usize = 200;

async fn fresh_reader(
    store: ControlMvpStateStore,
    configure: fn(ControlMvpStateStore) -> ControlMvpStateStore,
    statistics: fn(&ControlMvpStateStore) -> serde_json::Value,
) -> (ControlMvpStateStore, Box<dyn ArcoStateReader>) {
    let store = configure(store);
    let initial = statistics(&store);
    if !initial.is_null() {
        assert_eq!(
            initial.get("loads").and_then(serde_json::Value::as_u64),
            Some(0),
            "cold reader must own an empty cache"
        );
        assert_eq!(
            initial.get("demands").and_then(serde_json::Value::as_u64),
            Some(0),
            "cold reader cannot reuse earlier reads"
        );
    }
    let reader = store
        .read_at(store.current_state_token().await.unwrap())
        .await
        .unwrap();
    (store, reader)
}

// Preserve every cold duration so the comparator can independently recompute ranks.
fn cold_operations(recorder: Recorder) -> Vec<serde_json::Value> {
    recorder
        .finish()
        .into_iter()
        .map(|operation| {
            let mut value = serde_json::to_value(&operation).unwrap();
            if operation.phase.starts_with("cold") {
                value.as_object_mut().unwrap().insert(
                    "latency_samples_micros".into(),
                    serde_json::json!(operation.durations),
                );
            }
            value
        })
        .collect()
}

/// Configuration and statistics are supplied by the thin base/candidate driver.
/// The measured workload itself is identical on both sources.
#[allow(clippy::too_many_lines)]
pub async fn run(
    configure: fn(ControlMvpStateStore) -> ControlMvpStateStore,
    statistics: fn(&ControlMvpStateStore) -> serde_json::Value,
) -> serde_json::Value {
    #[cfg(feature = "test-utils")]
    let _fixed_inputs = arco_core::test_inputs::FixedInputs::scoped();
    let backend = Arc::new(CountingBackend::new(1));
    let writer = store(backend.clone());
    let mut tx = writer
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    let expected: Contents = (0..64)
        .map(|i| {
            (
                format!("catalog/{i:04}").into_bytes(),
                Bytes::from(vec![42; 1024]),
            )
        })
        .collect();
    for (key, value) in &expected {
        tx.put(key, value.clone()).await.unwrap();
    }
    tx.commit().await.unwrap();
    let mut recorder = Recorder::default();
    let mut cold_fresh_starts = 0;
    let mut warmed = None;
    for _ in 0..COLD_SAMPLES {
        drop(warmed.take());
        let (point_store, point) =
            fresh_reader(store(backend.clone()), configure, statistics).await;
        let value = recorder
            .measure(
                "cold_point",
                &backend,
                1024,
                false,
                point.get(b"catalog/0032"),
            )
            .await
            .unwrap();
        assert_eq!(value.as_ref(), expected.get(b"catalog/0032".as_slice()));
        drop(point);
        drop(point_store);
        let (scan_store, scan) = fresh_reader(store(backend.clone()), configure, statistics).await;
        let contents = recorder
            .measure(
                "cold_scan",
                &backend,
                64 * 1024,
                false,
                read_contents(scan.as_ref()),
            )
            .await;
        assert_eq!(contents, expected);
        cold_fresh_starts += 1;
        warmed = Some((scan_store, scan));
    }
    let (reader_store, reader) = warmed.expect("cold samples leave a warmed scan");
    let after_warmup = statistics(&reader_store);
    for _ in 0..40 {
        let value = recorder
            .measure(
                "warm_point",
                &backend,
                1024,
                false,
                reader.get(b"catalog/0032"),
            )
            .await
            .unwrap();
        assert_eq!(value.as_ref(), expected.get(b"catalog/0032".as_slice()));
        let contents = recorder
            .measure(
                "warm_scan",
                &backend,
                64 * 1024,
                false,
                read_contents(reader.as_ref()),
            )
            .await;
        assert_eq!(contents, expected);
    }
    let mut bursts = Vec::new();
    for callers in [1, 8, 32] {
        let fresh = configure(store(backend.clone()));
        let reader = fresh
            .read_at(fresh.current_state_token().await.unwrap())
            .await
            .unwrap();
        backend.take();
        let started = Instant::now();
        let (values, allocations) = measure_allocations(futures::future::join_all(
            (0..callers).map(|_| reader.get(b"catalog/0032")),
        ))
        .await;
        let elapsed = started.elapsed().as_nanos();
        let cost = backend.take();
        for value in values {
            assert_eq!(
                value.unwrap().as_ref(),
                expected.get(b"catalog/0032".as_slice())
            );
        }
        bursts.push(
            serde_json::json!({"caller_demands":callers,"elapsed_nanos":elapsed,
            "backend":cost,"allocations":allocations,"cache":statistics(&fresh)}),
        );
    }
    serde_json::json!({"samples_per_warm_operation":40,"result_parity":true,
        "operations":cold_operations(recorder),
        "cold_fresh_starts":{"cold_point":cold_fresh_starts,"cold_scan":cold_fresh_starts},
        "cache_after_warmup":after_warmup,
        "cache_final":statistics(&reader_store),"bursts":bursts})
}

pub async fn pressure(
    configure: fn(ControlMvpStateStore) -> ControlMvpStateStore,
    statistics: fn(&ControlMvpStateStore) -> serde_json::Value,
) -> serde_json::Value {
    #[cfg(feature = "test-utils")]
    let _fixed_inputs = arco_core::test_inputs::FixedInputs::scoped();
    let backend = Arc::new(CountingBackend::new(1));
    let writer = store(backend.clone());
    let mut tx = writer
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    for key in 0..4096_u32 {
        tx.put(&key.to_be_bytes(), Bytes::from(vec![42; 1024]))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    let reader_store = configure(store(backend.clone()));
    let reader = reader_store
        .read_at(reader_store.current_state_token().await.unwrap())
        .await
        .unwrap();
    let mut recorder = Recorder::default();
    for _ in 0..3 {
        for key in (0..4096_u32).step_by(32) {
            let value = recorder
                .measure(
                    "pressure_point",
                    &backend,
                    1024,
                    false,
                    reader.get(&key.to_be_bytes()),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(value, Bytes::from(vec![42; 1024]));
        }
    }
    serde_json::json!({"keys":4096,"payload_bytes":4096 * 1024,"passes":3,
        "operations":recorder.finish(),"cache":statistics(&reader_store),"result_parity":true})
}

/// The original 17 layout fixtures retain their Gate 5 preventing assertions.
#[cfg(feature = "test-utils")]
pub async fn layout_matrix(
    configure: fn(ControlMvpStateStore) -> ControlMvpStateStore,
    statistics: fn(&ControlMvpStateStore) -> serde_json::Value,
) -> serde_json::Value {
    use super::maintenance_cost::all_contents;
    let cases = [32, 64, 128, 256]
        .map(|target| (4096, 1, target * 1024, 0))
        .into_iter()
        .chain([1, 4, 16, 64].map(|blocks| (blocks * 55, 1, 64 * 1024, 0)))
        .chain([1, 4, 16, 64].map(|owners| (4096, owners, 64 * 1024, 0)))
        .chain([0, 1, 8, 16, 31].map(|suffix| (256, 1, 64 * 1024, suffix)));
    let mut reports = Vec::new();
    for (rows, owners, target, suffix) in cases {
        let (original, store, backend) = Box::pin(super::scaling_fixture_with_store(
            rows, owners, target, suffix, false,
        ))
        .await;
        let mut expected: Contents = (0..rows)
            .map(|ordinal| (super::scaling_key(ordinal), Bytes::from(vec![42; 1024])))
            .collect();
        if suffix > 0 {
            expected.insert(super::scaling_key(rows / 2), Bytes::from(vec![43; 1024]));
        }
        let key = super::scaling_key(rows / 2);
        let mut repetitions = Vec::new();
        for repetition in 1..=5 {
            let mut recorder = Recorder::default();
            let mut cold_fresh_starts = 0;
            let mut warmed = None;
            for _ in 0..COLD_SAMPLES {
                drop(warmed.take());
                let (point_store, point) = fresh_reader(store.clone(), configure, statistics).await;
                assert_eq!(
                    recorder
                        .measure("cold_point", &backend, 1024, false, point.get(&key))
                        .await
                        .unwrap()
                        .as_ref(),
                    expected.get(&key)
                );
                drop(point);
                drop(point_store);
                let (scan_store, scan) = fresh_reader(store.clone(), configure, statistics).await;
                assert_eq!(
                    recorder
                        .measure(
                            "cold_scan",
                            &backend,
                            rows * 1024,
                            false,
                            all_contents(scan.as_ref())
                        )
                        .await,
                    expected
                );
                cold_fresh_starts += 1;
                warmed = Some((scan_store, scan));
            }
            let (scan_store, scan) = warmed.expect("cold samples leave a warmed scan");
            for _ in 0..8 {
                assert_eq!(
                    recorder
                        .measure("warm_point", &backend, 1024, false, scan.get(&key))
                        .await
                        .unwrap()
                        .as_ref(),
                    expected.get(&key)
                );
                assert_eq!(
                    recorder
                        .measure(
                            "warm_scan",
                            &backend,
                            rows * 1024,
                            false,
                            all_contents(scan.as_ref())
                        )
                        .await,
                    expected
                );
            }
            repetitions.push(
                serde_json::json!({"repetition":repetition, "operations":cold_operations(recorder),
                "cold_fresh_starts":{"cold_point":cold_fresh_starts,"cold_scan":cold_fresh_starts},
                "cache":statistics(&scan_store), "result_parity":true}),
            );
        }
        reports.push(serde_json::json!({"original":original,"repetitions":repetitions}));
        // Preserve each completed case even if a later fixture fails.
        if let Ok(path) = std::env::var("ARCO_READ_CACHE_REPORT") {
            std::fs::write(path, serde_json::to_vec_pretty(&reports).unwrap()).unwrap();
        }
    }
    serde_json::json!({"fixtures":reports,"repetitions":5,"warm_samples":8,"result_parity":true})
}
