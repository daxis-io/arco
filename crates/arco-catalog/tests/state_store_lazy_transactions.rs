//! Lazy and pre-Gate-4 eager transactions checked against an independent oracle.
#![cfg(feature = "test-utils")]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::too_many_lines,
    clippy::type_complexity
)]
#[path = "support/integrity_oracle.rs"]
mod oracle;
use arco_catalog::{
    ArcoStateTxn, CatalogError, ControlMvpStateStore, KeyRange, ScanRequest, StateScope, TxnOptions,
};
use arco_core::{MemoryBackend, ScopedStorage};
use bytes::Bytes;
use oracle::{LogicalOracle, LogicalTransaction};
use std::{collections::BTreeMap, num::NonZeroU64, sync::Arc};

#[tokio::test]
async fn pinned_overlays_and_assertions_match_eager_and_independent_oracle() {
    for seed in 0..16_usize {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap()
        .with_checkpoint_interval(NonZeroU64::new(4).unwrap());
        let mut state = LogicalOracle::new();
        let mut head = 0;
        let keys = [
            vec![],
            vec![0],
            vec![0, 255],
            b"a".to_vec(),
            b"ab".to_vec(),
            b"z".to_vec(),
            vec![255],
        ];
        for generation in 0..8_usize {
            let mut tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            let key = keys[(generation + seed) % keys.len()].clone();
            let value = if generation % 3 == 0 {
                None
            } else {
                Some(Bytes::from(vec![
                    u8::try_from(generation).unwrap();
                    generation % 3
                ]))
            };
            match &value {
                Some(v) => tx.put(&key, v.clone()).await.unwrap(),
                None => tx.delete(&key).await.unwrap(),
            }
            tx.commit().await.unwrap();
            state.commit(vec![(key, value)], vec![], vec![]);
            head += 1;
        }
        let mut model = LogicalTransaction::pin(&state, head);
        let mut lazy = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        let mut eager = store
            .begin_eager_reference(TxnOptions::default())
            .await
            .unwrap();
        for operation in 0..64_usize {
            let key = &keys[(operation * 5 + seed) % keys.len()];
            match operation % 7 {
                0 | 1 => {
                    let value = Bytes::from(vec![u8::try_from(operation).unwrap(); operation % 4]);
                    model.writes.insert(key.clone(), Some(value.clone()));
                    lazy.put(key, value.clone()).await.unwrap();
                    eager.put(key, value).await.unwrap();
                }
                2 => {
                    model.writes.insert(key.clone(), None);
                    lazy.delete(key).await.unwrap();
                    eager.delete(key).await.unwrap();
                }
                _ => {
                    let expected = model.get(key);
                    for tx in [&mut lazy, &mut eager] {
                        assert_eq!(
                            tx.get(key)
                                .await
                                .unwrap()
                                .map(|v| (v.bytes().clone(), v.generation())),
                            expected
                        );
                        assert_eq!(
                            tx.assert_absent(key).await.is_ok(),
                            model.assert_absent(key)
                        );
                        for generation in [1, 4, 8, 9] {
                            assert_eq!(
                                tx.assert_generation(key, generation).await.is_ok(),
                                model.assert_generation(key, generation)
                            );
                        }
                    }
                }
            }
            for (start, end) in [
                (b"".as_slice(), b"\xff".as_slice()),
                (b"a", b"b"),
                (b"z", b"a"),
                (b"", b""),
            ] {
                let range = KeyRange::new(start.to_vec(), end.to_vec());
                let witness = model.range_witness(start, end);
                for tx in [&mut lazy, &mut eager] {
                    assert_eq!(
                        tx.assert_range_empty(range.clone()).await.is_ok(),
                        model.range_empty(start, end)
                    );
                    tx.assert_range_unchanged(range.clone(), witness)
                        .await
                        .unwrap();
                    let inputs = tx
                        .read_set(&keys, &[range.clone(), range.clone()])
                        .await
                        .unwrap();
                    tx.assert_inputs_unchanged(inputs).await.unwrap();
                }
            }
            let expected = model.scan(b"", None);
            for tx in [&mut lazy, &mut eager] {
                let page = tx.scan(ScanRequest::new(b"")).await.unwrap();
                assert_eq!(
                    page.entries()
                        .iter()
                        .map(|v| (
                            v.key().to_vec(),
                            v.value().bytes().clone(),
                            v.value().generation()
                        ))
                        .collect::<Vec<_>>(),
                    expected
                );
            }
        }
        let committed = lazy.commit().await.unwrap();
        assert!(model.clone().commit(&mut state, &mut head));
        state
            .assert_manifest(&storage, committed.authority_manifest_id())
            .await;
        assert!(matches!(
            eager.commit().await,
            Err(CatalogError::CasFailed { .. })
        ));
        assert!(!model.commit(&mut state, &mut head));
    }
}

#[tokio::test]
async fn scan_keysets_merge_l1_l0_and_mutations_above_boundary() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let store =
        ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", "catalog"))
            .unwrap()
            .with_checkpoint_interval(NonZeroU64::new(1).unwrap())
            .with_test_segment_sizing(40, 8192)
            .unwrap();
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    let mut expected = BTreeMap::new();
    for n in 0..200_u32 {
        let key = n.to_be_bytes().to_vec();
        let v = Bytes::from(vec![42; 700]);
        tx.put(&key, v.clone()).await.unwrap();
        expected.insert(key, v);
    }
    tx.commit().await.unwrap();
    // This successor promotes the L1 anchor while retaining an L0 suffix.
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.delete(&50_u32.to_be_bytes()).await.unwrap();
    tx.commit().await.unwrap();
    expected.remove(50_u32.to_be_bytes().as_slice());
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    let mut cursor = None;
    let mut found = Vec::new();
    loop {
        let mut req = ScanRequest::new(b"").with_limits(13, 16 * 1024, 64);
        if let Some(c) = cursor.take() {
            req = req.with_token(c);
        }
        let page = tx.scan(req).await.unwrap();
        found.extend(
            page.entries()
                .iter()
                .map(|v| (v.key().to_vec(), v.value().bytes().clone())),
        );
        cursor = page.continuation().cloned();
        if found.len() == 13 {
            tx.put(&0_u32.to_be_bytes(), Bytes::from_static(b"past"))
                .await
                .unwrap();
            tx.delete(&99_u32.to_be_bytes()).await.unwrap();
            expected.remove(99_u32.to_be_bytes().as_slice());
            tx.put(&101_u32.to_be_bytes(), Bytes::new()).await.unwrap();
            expected.insert(101_u32.to_be_bytes().to_vec(), Bytes::new());
            tx.put(&201_u32.to_be_bytes(), Bytes::new()).await.unwrap();
            expected.insert(201_u32.to_be_bytes().to_vec(), Bytes::new());
        }
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(found, expected.into_iter().collect::<Vec<_>>());
    tx.commit().await.unwrap();
}
