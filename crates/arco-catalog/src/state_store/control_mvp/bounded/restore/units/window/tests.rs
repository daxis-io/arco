#![allow(
    clippy::too_many_lines,
    reason = "keep each bounded end-to-end fault or oracle matrix in one test"
)]
#![allow(
    clippy::large_futures,
    reason = "exercise the fixed stack product slots without adding an unaccounted production box"
)]
use super::super::{OwnedSelectedPlan, behavioral_tests, prefixed_sha256, read_selected_progress};
use super::*;
use crate::state_store::control_mvp::{self as control, ControlMvpStateStore};
use crate::workspace_io_budget::WorkspaceIoBudget;
use physical::restore_io::UnitPayloadAdmission;
use std::collections::BTreeMap;

fn row(key: &[u8], value: Option<&[u8]>, sequence: u64) -> ControlMvpSegmentRow {
    ControlMvpSegmentRow {
        record_kind: control::SEGMENT_RECORD_KV,
        key: key.to_vec(),
        value: value.map(<[u8]>::to_vec),
        generation: sequence - 1,
        tombstone: value.is_none(),
        logical_sequence: sequence,
        logical_ordinal: 0,
        origin_sequence: None,
    }
}

// Fixture construction uses production segment/index/descriptor codecs, outside
// measured per-unit work. Leading descriptor whitespace checks raw-size binding.
async fn root(
    store: &ControlMvpStateStore,
    label: &str,
    chunks: &[Vec<ControlMvpSegmentRow>],
) -> String {
    let directory =
        directory::Directory::new(store.retention.clone(), &store.scope).expect("directory");
    let mut builder = directory.builder();
    for (part, rows) in chunks.iter().enumerate() {
        let mut rows = rows.clone();
        for (i, r) in rows.iter_mut().enumerate() {
            r.logical_ordinal = i as u64;
        }
        let id = control::sha256_hex(format!("{label}:{part}").as_bytes());
        let (segment, index, reference) = control::encode_segment(
            &id,
            control::ControlMvpSegmentLevel::L1,
            rows[0].logical_sequence,
            &store.scope,
            &rows,
            control::SegmentLimits {
                block_target: MAX_BLOCK_BYTES,
                ..control::PRODUCTION_SEGMENT_LIMITS
            },
        )
        .expect("fixture segment");
        let decoded: control::ControlMvpSegmentIndex =
            control::decode_json(&index, "index").expect("index");
        let sp = store.paths.state_object(&id);
        let ip = store.paths.segment_index(&id);
        for (path, raw) in [(&sp, segment), (&ip, index)] {
            store
                .retention
                .put_raw(
                    path,
                    raw,
                    arco_core::storage::WritePrecondition::DoesNotExist,
                )
                .await
                .expect("fixture object");
        }
        for block in decoded.blocks {
            let descriptor = physical::Descriptor {
                encoding_version: 1,
                scope: control::ControlMvpScopeDoc::from(&store.scope),
                role: physical::Role::Kv,
                segment: reference.clone(),
                segment_version: store.storage.head(&sp).await.unwrap().unwrap().version,
                index_version: store.storage.head(&ip).await.unwrap().unwrap().version,
                block,
            };
            let mut raw = vec![b' '; 7];
            raw.extend(control::encode_json(&descriptor, "descriptor").expect("descriptor"));
            let digest = control::sha256_hex(&raw);
            let path = format!(
                "{}/physical/descriptors/{digest}.json",
                store.paths.base_prefix()
            );
            store
                .retention
                .put_raw(
                    &path,
                    raw.into(),
                    arco_core::storage::WritePrecondition::DoesNotExist,
                )
                .await
                .expect("descriptor");
            builder
                .push(directory::Leaf {
                    first: hex::decode(descriptor.block.min_key_hex.as_ref().unwrap()).unwrap(),
                    last: hex::decode(descriptor.block.max_key_hex.as_ref().unwrap()).unwrap(),
                    rows: descriptor.block.row_count,
                    bytes: u32::try_from(descriptor.block.length).unwrap(),
                    digest: hex::decode(digest).unwrap().try_into().unwrap(),
                })
                .await
                .expect("leaf");
        }
    }
    URL_SAFE_NO_PAD.encode(builder.finish().await.expect("fixture root").encode())
}

async fn fixture(
    source: &[Vec<ControlMvpSegmentRow>],
    current: &[Vec<ControlMvpSegmentRow>],
    absent: bool,
) -> (
    ControlMvpStateStore,
    super::super::ControlMvpRestorePlanV7,
    String,
) {
    let (store, plan) = super::super::super::tests::inspection_fixture().await;
    fixture_on_store(store, plan, source, current, absent).await
}

async fn fixture_on_store(
    store: ControlMvpStateStore,
    mut plan: super::super::ControlMvpRestorePlanV7,
    source: &[Vec<ControlMvpSegmentRow>],
    current: &[Vec<ControlMvpSegmentRow>],
    absent: bool,
) -> (
    ControlMvpStateStore,
    super::super::ControlMvpRestorePlanV7,
    String,
) {
    plan.fields.source_kv_root_b64 = root(&store, "source", source).await;
    plan.fields.base_kv_root_b64 = if absent {
        plan.fields.source_kv_root_b64.clone()
    } else {
        root(&store, "current", current).await
    };
    plan.fields.source_logical_sequence = 6;
    plan.fields.base_logical_sequence = if absent { 6 } else { 9 };
    plan.fields.result_logical_sequence = plan.fields.base_logical_sequence + 1;
    plan.fields.mode = if absent { Mode::Absent } else { Mode::Present };
    let digest = prefixed_sha256(b"standard window synthetic fixture");
    let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
    behavioral_tests::seed_selected_read_case(&store, &expected, 14).await;
    (store, plan, digest)
}

fn oracle(
    source: &[Vec<ControlMvpSegmentRow>],
    current: &[Vec<ControlMvpSegmentRow>],
    absent: bool,
) -> BTreeMap<Vec<u8>, (Option<Vec<u8>>, u64)> {
    let s: BTreeMap<_, _> = source
        .iter()
        .flatten()
        .map(|r| (r.key.clone(), r))
        .collect();
    let c: BTreeMap<_, _> = current
        .iter()
        .flatten()
        .filter(|_| !absent)
        .map(|r| (r.key.clone(), r))
        .collect();
    let mut out = BTreeMap::new();
    let sequence = if absent { 7 } else { 10 };
    for key in s.keys().chain(c.keys()) {
        let value = s.get(key).and_then(|r| r.value.clone());
        let generation = if value.is_some() {
            c.get(key)
                .filter(|r| r.value == value)
                .map_or(sequence, |r| r.generation)
        } else if let Some(r) = c.get(key) {
            if r.tombstone { r.generation } else { sequence }
        } else if absent {
            s[key].generation
        } else {
            continue;
        };
        out.insert(key.clone(), (value, generation));
    }
    out
}

#[tokio::test]
async fn standard_window_large_key_metadata_is_admitted() {
    let a = vec![b'a'; 20_000];
    let b = vec![b'b'; 20_000];
    let (store, plan, digest) = fixture(
        &[vec![row(&a, Some(b"a"), 6)]],
        &[vec![row(&b, Some(b"b"), 9)]],
        false,
    )
    .await;
    let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
    let mut workspace = WorkspaceIoBudget::new();
    let mut payload = UnitPayloadAdmission::new();
    let mut route = RestorePhysicalRoute::OrdinaryUnit {
        workspace: &mut workspace,
        payload: &mut payload,
    };
    let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
        Ok(OwnedSelectedPlan {
            plan,
            plan_sha256: digest,
        })
    })
    .unwrap();
    let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
    let selected = read_selected_progress(&mut io, &mut route, &expected)
        .await
        .unwrap();
    let unit = prepare(&mut io, &mut route, &owned, &expected, &selected)
        .await
        .expect("large valid keys");
    assert_eq!(io.writing_evidence().0, 3);
    drop(unit);
    assert_eq!(io.allocation_underestimates(), 0);
}

#[tokio::test]
async fn standard_window_multiunit_matches_independent_oracle_and_final_receipt_prefix() {
    for absent in [false, true] {
        for case in 0..13 {
            let (source, current) = match case {
                0 => (vec![], vec![]),
                1 => (
                    vec![vec![row(b"a", Some(b"same"), 6), row(b"z", None, 6)]],
                    vec![
                        vec![row(b"b", Some(b"removed"), 9), row(b"c", None, 9)],
                        vec![row(b"z", Some(b"removed"), 9)],
                    ],
                ),
                2 => (
                    vec![vec![row(b"a", Some(b"same"), 6)], vec![row(b"m", None, 6)]],
                    vec![vec![row(b"n", Some(b"later"), 9), row(b"z", None, 9)]],
                ),
                3 => (
                    vec![vec![
                        row(b"a", Some(b"same"), 6),
                        row(b"b", None, 6),
                        row(b"c", Some(b"revived"), 6),
                    ]],
                    vec![vec![
                        row(b"a", Some(b"same"), 9),
                        row(b"b", None, 9),
                        row(b"c", None, 9),
                    ]],
                ),
                4 => (
                    vec![vec![
                        row(b"\0", Some(b""), 6),
                        row(&[0, 255], Some(&[255, 0]), 6),
                        row(&[255, 255], None, 6),
                    ]],
                    vec![],
                ),
                5 => (
                    vec![],
                    vec![vec![row(b"a", None, 9), row(b"z", Some(b""), 9)]],
                ),
                6 => (
                    vec![vec![
                        row(b"a", Some(&vec![1; 100_000]), 6),
                        row(b"z", Some(&vec![2; 100_000]), 6),
                    ]],
                    vec![vec![
                        row(&vec![b'b'; 20_000], Some(b"old"), 9),
                        row(b"z", None, 9),
                    ]],
                ),
                7 => (
                    vec![
                        vec![row(&vec![1; 20_000], Some(b""), 6)],
                        vec![row(&vec![3; 20_000], None, 6)],
                    ],
                    vec![
                        vec![row(&vec![2; 20_000], Some(b""), 9)],
                        vec![row(&vec![4; 20_000], None, 9)],
                    ],
                ),
                8 => (
                    vec![
                        vec![row(b"", Some(b"first"), 6)],
                        vec![row(b"a", Some(b"next"), 6)],
                    ],
                    vec![vec![row(b"a", Some(b"old"), 9)]],
                ),
                9 => (
                    vec![vec![row(b"", None, 6)]],
                    vec![vec![row(b"", Some(b"old"), 9)]],
                ),
                10 => (vec![], vec![vec![row(b"", None, 9)]]),
                12 => (
                    vec![vec![row(&vec![b'a'; 48 * 1024], Some(b"value"), 6)]],
                    vec![],
                ),
                _ => (
                    vec![vec![row(b"", Some(b"same"), 6)]],
                    vec![vec![row(b"", Some(b"same"), 9)]],
                ),
            };
            let expected_rows = oracle(&source, &current, absent);
            let (store, plan, digest) = fixture(&source, &current, absent).await;
            let mut actual = BTreeMap::new();
            let mut finished = false;
            for ordinal in 0..12 {
                let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut route = RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                };
                let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
                    Ok(OwnedSelectedPlan {
                        plan: plan.clone(),
                        plan_sha256: digest.clone(),
                    })
                })
                .unwrap();
                let expected =
                    super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
                let selected = read_selected_progress(&mut io, &mut route, &expected)
                    .await
                    .unwrap();
                let unit = prepare(&mut io, &mut route, &owned, &expected, &selected)
                    .await
                    .unwrap_or_else(|e| {
                        panic!("case {case} absent {absent} ordinal {ordinal}: {e:?}")
                    });
                let target = unit.recovery_target();
                assert_eq!(
                    publication::publish(&mut io, &mut route, unit)
                        .await
                        .unwrap(),
                    publication::Publication::Written
                );
                assert_eq!(io.allocation_underestimates(), 0);
                assert!(io.peak_owned_evidence() <= 64 * 1024 * 1024);
                assert_eq!(
                    io.native_work_evidence().bounded.streaming_builder_inputs,
                    0
                );
                assert!(!io.native_work_evidence().overflow);
                println!(
                    "window case={case} absent={absent} unit={ordinal} reads={:?} writes={:?} peak={} hashes={:?}",
                    io.reading_evidence(),
                    io.writing_evidence(),
                    io.peak_owned_evidence(),
                    io.hashing_evidence()
                );
                drop((selected, expected));
                drop(owned);
                drop(io);
                // Fresh invocation: verify persisted raw witnesses and output rows.
                let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut route = RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                };
                let expected =
                    decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
                        ExpectedPlan::from_selected(&plan, &digest)
                    })
                    .unwrap();
                assert_eq!(
                    publication::reconcile(&mut io, &mut route, &expected, target)
                        .await
                        .unwrap(),
                    publication::Reconciliation::Exact
                );
                let raw = store
                    .storage
                    .get(&expected.value().receipt_path(ordinal))
                    .await
                    .unwrap();
                let receipt: ControlMvpRestoreReceiptV1 =
                    control::decode_json(&raw, "receipt").unwrap();
                if case == 6 && !absent && ordinal == 0 {
                    assert_eq!(receipt.outputs.len(), 2, "local split exercised");
                }
                assert!(receipt.counts.decoded_blocks <= 4);
                assert!(receipt.counts.input_encoded_bytes <= 1024 * 1024);
                if absent {
                    assert!(receipt.current_inputs.is_empty());
                }
                for input in receipt.source_inputs.iter().chain(&receipt.current_inputs) {
                    let raw = store.storage.get(&input.descriptor.path).await.unwrap();
                    assert_eq!(input.descriptor.byte_size, raw.len() as u64);
                    assert_eq!(input.descriptor.sha256, prefixed_sha256(&raw));
                    let index = store.storage.get(&input.index.path).await.unwrap();
                    assert_eq!(input.index.byte_size, index.len() as u64);
                    assert_eq!(input.index.sha256, prefixed_sha256(&index));
                }
                let mut output_rows = Vec::new();
                let mut parts = Vec::new();
                for output in &receipt.outputs {
                    let leaf = directory::Leaf {
                        first: binary(&output.directory_leaf.first_key_b64url).unwrap(),
                        last: binary(&output.directory_leaf.last_key_b64url).unwrap(),
                        rows: output.directory_leaf.rows,
                        bytes: output.directory_leaf.bytes,
                        digest: hex::decode(
                            super::super::raw_digest(&output.directory_leaf.digest).unwrap(),
                        )
                        .unwrap()
                        .try_into()
                        .unwrap(),
                    };
                    let (_, rows) = physical::restore_io::read_restore_leaf(
                        &mut io,
                        &mut route,
                        physical::Role::Kv,
                        &leaf,
                    )
                    .await
                    .unwrap();
                    let start = output_rows.len();
                    output_rows.extend(rows.value().iter().cloned());
                    parts.push((start, output_rows.len()));
                    for row in rows.value() {
                        assert!(
                            actual
                                .insert(row.key.clone(), (row.value.clone(), row.generation))
                                .is_none(),
                            "duplicate output key"
                        );
                    }
                }
                assert_eq!(io.allocation_underestimates(), 0);
                let mut projected = receipt.clone();
                projected.outputs = projected_outputs(&store, &output_rows, &parts).unwrap();
                projected.counts.output_encoded_bytes = u64::MAX;
                assert!(
                    super::super::jcs(&projected).unwrap().len() >= raw.len(),
                    "prospective full receipt bounds actual persisted JCS"
                );
                if matches!(receipt.after.global, GlobalCut::End) {
                    finished = true;
                    break;
                }
            }
            assert!(finished, "bounded fixture must finish");
            assert_eq!(actual, expected_rows, "case {case} absent {absent}");
            // Fresh ordinary-authenticated terminal selection; one forward receipt per chunk.
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
                Ok(OwnedSelectedPlan {
                    plan: plan.clone(),
                    plan_sha256: digest.clone(),
                })
            })
            .expect("selected plan");
            let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned)
                .expect("expected");
            let selected = read_selected_progress(&mut io, &mut route, &expected)
                .await
                .expect("terminal selection");
            assert!(selected.progress.value().terminal);
            let mut prefix =
                super::super::prefix::Prefix::new(&mut io, &mut route, &expected, &selected)
                    .expect("terminal prefix");
            let mut totals = physical::restore_io::FinalStreamTotals::new();
            let mut count = 0;
            loop {
                let mut chunk =
                    physical::restore_io::FinalMicrochunk::begin(&mut totals, 0, &mut io)
                        .expect("receipt chunk");
                let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
                let Some(receipt) = prefix
                    .next(&mut io, &mut route)
                    .await
                    .expect("forward receipt")
                else {
                    break;
                };
                assert_eq!(receipt.value().ordinal, count);
                drop(chunk);
                super::super::coverage::verify_standard(
                    &mut io,
                    &mut totals,
                    &owned,
                    &expected,
                    &receipt,
                )
                .await
                .expect("independent final physical interval");
                count += 1;
            }
            assert_eq!(count, selected.progress.value().receipt_count);
            assert_eq!(io.allocation_underestimates(), 0);
            println!(
                "receipt prefix fixture case={case} absent={absent} receipts={count} reads={:?} peak={}",
                io.reading_evidence(),
                io.peak_owned_evidence()
            );
            drop(prefix);
            drop(selected);
            drop(expected);
            drop(owned);
            assert_eq!(io.live_ownership_evidence(), (0, 0));
        }
    }
}

#[tokio::test]
async fn standard_window_rejects_manifest_sequence_on_gap_evidence() {
    let (store, plan, digest) = fixture(
        &[vec![row(b"a", Some(b"a"), 6)]],
        &[vec![row(b"z", Some(b"z"), 99)]],
        false,
    )
    .await;
    let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
    let mut workspace = WorkspaceIoBudget::new();
    let mut payload = UnitPayloadAdmission::new();
    let mut route = RestorePhysicalRoute::OrdinaryUnit {
        workspace: &mut workspace,
        payload: &mut payload,
    };
    let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
        Ok(OwnedSelectedPlan {
            plan,
            plan_sha256: digest,
        })
    })
    .unwrap();
    let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
    let selected = read_selected_progress(&mut io, &mut route, &expected)
        .await
        .unwrap();
    assert!(
        prepare(&mut io, &mut route, &owned, &expected, &selected)
            .await
            .is_err(),
        "gap evidence must obey its pinned manifest sequence even when no row is merged"
    );
    assert_eq!(io.writing_evidence().0, 0);
    assert_eq!(io.allocation_underestimates(), 0);
}

#[tokio::test]
async fn standard_window_saved_cursor_proofs_reject_gaps_and_forgery() {
    use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
    let (store, _, _) = fixture(
        &[vec![
            row(b"a", Some(b"a"), 6),
            row(b"m", None, 6),
            row(b"z", Some(b"z"), 6),
        ]],
        &[],
        false,
    )
    .await;
    let encoded = root(
        &store,
        "cursor",
        &[vec![
            row(b"a", Some(b"a"), 6),
            row(b"m", None, 6),
            row(b"z", Some(b"z"), 6),
        ]],
    )
    .await;
    let directory = directory::Directory::new(store.retention.clone(), &store.scope).unwrap();
    let root = directory.decode_root(&binary(&encoded).unwrap()).unwrap();
    let position = {
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut w = WorkspaceIoBudget::new();
        let mut p = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut w,
            payload: &mut p,
        };
        let pos = directory::restore::first_after(&mut io, &mut route, &root, None)
            .await
            .unwrap();
        let input = load(&mut io, &mut route, pos).await.unwrap().unwrap();
        position_witness(&encoded, &input).unwrap()
    };
    for final_route in [false, true] {
        for case in 0..14 {
            let mut cursor = SideCursor::After {
                key_b64url: URL_SAFE_NO_PAD.encode(b"m"),
                position: position.clone(),
            };
            let mut global = Some(b"m".as_slice());
            match case {
                0 => cursor = SideCursor::Start,
                1 => cursor = SideCursor::End,
                2 => {
                    if let SideCursor::After { position, .. } = &mut cursor {
                        position.path[0].child_index += 1;
                    }
                }
                3 => {
                    if let SideCursor::After { key_b64url, .. } = &mut cursor {
                        *key_b64url = URL_SAFE_NO_PAD.encode(b"b");
                    }
                }
                4 => global = Some(b"a"),
                5 => {
                    if let SideCursor::After { key_b64url, .. } = &mut cursor {
                        *key_b64url = URL_SAFE_NO_PAD.encode(b"a");
                    }
                }
                7 => global = Some(b"n"),
                8 => {
                    cursor = SideCursor::Start;
                    global = None;
                }
                9 => {
                    cursor = SideCursor::End;
                    global = Some(b"z");
                }
                10 => {
                    global = Some(b"z");
                    if let SideCursor::After { key_b64url, .. } = &mut cursor {
                        *key_b64url = URL_SAFE_NO_PAD.encode(b"z");
                    }
                }
                11 => {
                    if let SideCursor::After { position, .. } = &mut cursor {
                        position.root_b64 = "AA".into();
                    }
                }
                12 => {
                    if let SideCursor::After { position, .. } = &mut cursor {
                        position.role = physical::Role::ActiveId;
                    }
                }
                13 => {
                    if let SideCursor::After { position, .. } = &mut cursor {
                        position.leaf.digest = prefixed_sha256(b"wrong leaf");
                    }
                }
                _ => {}
            }
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut w = WorkspaceIoBudget::new();
            let mut p = UnitPayloadAdmission::new();
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).unwrap();
            let mut route = if final_route {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut w,
                    payload: &mut p,
                }
            };
            let result = read_side(&mut io, &mut route, &root, &encoded, &cursor, global).await;
            assert_eq!(
                result.is_ok(),
                (6..=10).contains(&case),
                "cursor case {case}, final {final_route}"
            );
            assert_eq!(io.writing_evidence().0, 0);
            assert_eq!(io.allocation_underestimates(), 0);
        }
    }
}

#[tokio::test]
async fn standard_window_rejects_bad_inputs_owners_and_admission_before_writes() {
    for case in 0..7 {
        let value = vec![1; if case == 1 { 230_000 } else { 1 }];
        let (store, mut plan, digest) =
            fixture(&[vec![row(b"a", Some(&value), 6)]], &[], false).await;
        if case == 0 {
            plan.fields.result_logical_sequence = 0;
        }
        if case == 2 || case == 3 {
            let directory =
                directory::Directory::new(store.retention.clone(), &store.scope).unwrap();
            let root = directory
                .decode_root(&binary(&plan.fields.source_kv_root_b64).unwrap())
                .unwrap();
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut w = WorkspaceIoBudget::new();
            let mut p = UnitPayloadAdmission::new();
            let mut r = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut w,
                payload: &mut p,
            };
            let pos = directory::restore::first_after(&mut io, &mut r, &root, None)
                .await
                .unwrap();
            let pos = pos.value().as_ref().unwrap();
            let path = if case == 2 {
                format!(
                    "{}/physical/descriptors/{}.json",
                    store.paths.base_prefix(),
                    hex::encode(pos.leaf.digest)
                )
            } else {
                format!(
                    "{}/directory/v1/pages/{}.bin",
                    store.paths.base_prefix(),
                    hex::encode(pos.path[0].0)
                )
            };
            // Descriptor corruption and missing directory page are separate cases.
            let path = if case == 3 {
                directory_page_path(&store, &pos.path[0].0)
            } else {
                path
            };
            let meta = store.retention.head_raw(&path).await.unwrap().unwrap();
            store
                .retention
                .put_raw(
                    &path,
                    bytes::Bytes::from_static(b"corrupt"),
                    arco_core::storage::WritePrecondition::MatchesVersion(meta.version),
                )
                .await
                .unwrap();
        }
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut w = WorkspaceIoBudget::new();
        let mut p = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut w,
            payload: &mut p,
        };
        let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan: plan.clone(),
                plan_sha256: digest.clone(),
            })
        })
        .unwrap();
        let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .unwrap();
        let other = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan: plan.clone(),
                plan_sha256: prefixed_sha256(b"wrong"),
            })
        })
        .unwrap();
        let held = if case == 5 {
            let remain = 64 * 1024 * 1024 - io.live_ownership_evidence().0 - 4096;
            Some(
                decode_with_reservation(&mut io, &mut route, Some(remain), || {
                    Ok(vec![0u8; remain])
                })
                .unwrap(),
            )
        } else {
            None
        };
        if case == 6 {
            let mut foreign = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut fw = WorkspaceIoBudget::new();
            let mut fp = UnitPayloadAdmission::new();
            let mut fr = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut fw,
                payload: &mut fp,
            };
            assert!(
                prepare(&mut foreign, &mut fr, &owned, &expected, &selected)
                    .await
                    .is_err()
            );
            assert_eq!(foreign.writing_evidence().0, 0);
            assert_eq!(foreign.reading_evidence().0, 0);
        } else {
            assert!(
                prepare(
                    &mut io,
                    &mut route,
                    if case == 4 { &other } else { &owned },
                    &expected,
                    &selected
                )
                .await
                .is_err(),
                "case {case}"
            );
            let reads = io.reading_evidence();
            assert!(
                prepare(&mut io, &mut route, &owned, &expected, &selected)
                    .await
                    .is_err()
            );
            assert_eq!(io.reading_evidence(), reads);
        }
        assert_eq!(io.writing_evidence().0, 0, "case {case}");
        assert_eq!(io.allocation_underestimates(), 0);
        drop(held);
    }
}

fn directory_page_path(store: &ControlMvpStateStore, digest: &[u8; 32]) -> String {
    format!(
        "control/directory/v1/domains/{}/pages/{}",
        store.scope.domain(),
        hex::encode(digest)
    )
}

#[tokio::test]
async fn standard_window_cancellation_stops_every_await_and_releases_owners() {
    use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
    use std::sync::atomic::Ordering;
    for resumed in [false, true] {
        let mut boundaries = 0;
        {
            let final_route = false; // Control transport is ordinary-only; rejection has separate coverage.
            for at in 0..100 {
                if at > 0 && at > boundaries {
                    break;
                }
                let (store, remaining) = physical::restore_io::window_pending_store();
                let store = store.with_durable_authority_binding(
                    crate::state_store::DurableAuthorityBinding::new([39; 32]),
                );
                let (_, mut plan) = super::super::super::tests::inspection_fixture().await;
                plan.fields.source_kv_root_b64 = root(
                    &store,
                    "cancel-source",
                    &if resumed {
                        vec![
                            vec![row(b"a", Some(b"same"), 6)],
                            vec![
                                row(b"b", Some(&vec![1; 100_000]), 6),
                                row(b"z", Some(&vec![2; 100_000]), 6),
                            ],
                        ]
                    } else {
                        vec![vec![row(b"a", Some(b"value"), 6), row(b"z", None, 6)]]
                    },
                )
                .await;
                plan.fields.base_kv_root_b64 = root(
                    &store,
                    "cancel-current",
                    &if resumed {
                        vec![
                            vec![row(b"a", Some(b"same"), 9)],
                            vec![
                                row(&vec![b'c'; 20_000], Some(b"old"), 9),
                                row(b"z", None, 9),
                            ],
                        ]
                    } else {
                        vec![vec![row(b"b", Some(b"old"), 9), row(b"z", None, 9)]]
                    },
                )
                .await;
                plan.fields.source_logical_sequence = 6;
                plan.fields.base_logical_sequence = 9;
                plan.fields.result_logical_sequence = 10;
                let digest = prefixed_sha256(b"cancellation window");
                let e = ExpectedPlan::from_selected(&plan, &digest).unwrap();
                behavioral_tests::seed_selected_read_case(&store, &e, 14).await;
                if resumed {
                    advance_once(&store, &plan, &digest).await;
                }
                let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut route = RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                };
                let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
                    Ok(OwnedSelectedPlan {
                        plan,
                        plan_sha256: digest,
                    })
                })
                .unwrap();
                let expected =
                    super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
                let selected = read_selected_progress(&mut io, &mut route, &expected)
                    .await
                    .unwrap();
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).unwrap();
                let mut route = if final_route {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let baseline = io.live_ownership_evidence();
                remaining.store(if at == 0 { usize::MAX } else { at }, Ordering::SeqCst);
                if at == 0 {
                    let unit = prepare(&mut io, &mut route, &owned, &expected, &selected)
                        .await
                        .expect("complete probe");
                    drop(unit);
                    let observed = usize::MAX - remaining.load(Ordering::SeqCst);
                    assert!(observed > 3 && observed < 100);
                    if boundaries == 0 {
                        boundaries = observed;
                    } else {
                        assert_eq!(boundaries, observed);
                    }
                } else {
                    let mut pending =
                        Box::pin(prepare(&mut io, &mut route, &owned, &expected, &selected));
                    assert!(size_of_val(pending.as_ref().get_ref()) <= 64 * 1024);
                    assert!(
                        matches!(futures::poll!(pending.as_mut()), std::task::Poll::Pending),
                        "boundary {at}"
                    );
                    drop(pending);
                    assert_eq!(remaining.load(Ordering::SeqCst), 0);
                    assert_eq!(
                        io.live_ownership_evidence(),
                        baseline,
                        "canceled owners at {at}"
                    );
                    let reads = io.reading_evidence();
                    let writes = io.writing_evidence();
                    assert!(
                        prepare(&mut io, &mut route, &owned, &expected, &selected)
                            .await
                            .is_err()
                    );
                    assert_eq!(io.reading_evidence(), reads);
                    assert_eq!(io.writing_evidence(), writes);
                }
                assert_eq!(io.allocation_underestimates(), 0);
                assert!(io.peak_owned_evidence() <= 64 * 1024 * 1024);
                assert_eq!(
                    io.native_work_evidence().bounded.streaming_builder_inputs,
                    0
                );
                assert!(!io.native_work_evidence().overflow);
            }
        }
        println!(
            "window cancellation resumed={resumed}: {boundaries} await boundaries on the ordinary route"
        );
    }
}

#[tokio::test]
async fn standard_window_preflights_later_output_index_before_any_put() {
    for too_wide in [true, false] {
        let mut key = vec![0x20];
        key.extend(vec![0xff; if too_wide { 140 * 1024 } else { 50 * 1024 }]);
        let value = vec![1; if too_wide { 100 * 1024 } else { 160 * 1024 }];
        let source = vec![vec![
            row(&[0x10], Some(&value), 6),
            row(&key, Some(b""), 6),
            row(&[0x30], Some(b"c"), 6),
            row(&[0x40], None, 6),
        ]];
        let (store, plan, digest) =
            fixture(&source, &[vec![row(&[0x30], Some(b"c"), 9)]], false).await;
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan,
                plan_sha256: digest,
            })
        })
        .unwrap();
        let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .unwrap();
        let result = prepare(&mut io, &mut route, &owned, &expected, &selected).await;
        assert_eq!(result.is_ok(), !too_wide);
        drop(result);
        assert_eq!(
            io.writing_evidence().0,
            if too_wide { 0 } else { 6 },
            "every chosen output index must fit before any output PUT"
        );
        assert_eq!(io.allocation_underestimates(), 0);
    }
}

#[test]
fn standard_window_independent_verifier_rejects_changed_coverage() {
    let source = vec![row(b"a", Some(b"new"), 6), row(b"c", Some(b""), 6)];
    let current = vec![row(b"b", Some(b"old"), 9)];
    let mut source = source;
    source[1].logical_ordinal = 1;
    let output = merge_standard_kv_rows(&source, &current, Mode::Present, 6, 9, 0, (6, 9)).unwrap();
    verify_merge(&source, &current, &output, Mode::Present, 10).unwrap();
    for case in 0..7 {
        let mut forged = output.clone();
        match case {
            0 => {
                forged.remove(1);
            }
            1 => forged.push(forged[0].clone()),
            2 => forged[0].generation -= 1,
            3 => forged[1].tombstone = false,
            4 => forged[1].logical_ordinal += 1,
            5 => forged.swap(0, 1),
            _ => forged[0].value = Some(b"forged".to_vec()),
        }
        assert!(verify_merge(&source, &current, &forged, Mode::Present, 10).is_err());
    }
}

async fn advance_once(
    store: &ControlMvpStateStore,
    plan: &super::super::ControlMvpRestorePlanV7,
    digest: &str,
) {
    let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 64 * 1024 * 1024);
    let mut workspace = WorkspaceIoBudget::new();
    let mut payload = UnitPayloadAdmission::new();
    let mut route = RestorePhysicalRoute::OrdinaryUnit {
        workspace: &mut workspace,
        payload: &mut payload,
    };
    let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
        Ok(OwnedSelectedPlan {
            plan: plan.clone(),
            plan_sha256: digest.into(),
        })
    })
    .unwrap();
    let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
    let selected = read_selected_progress(&mut io, &mut route, &expected)
        .await
        .unwrap();
    let unit = prepare(&mut io, &mut route, &owned, &expected, &selected)
        .await
        .unwrap();
    assert_eq!(
        publication::publish(&mut io, &mut route, unit)
            .await
            .unwrap(),
        publication::Publication::Written
    );
}

#[tokio::test]
async fn standard_window_rejects_newer_exhausted_boundary_descriptor() {
    {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        let final_route = false; // Control transport is ordinary-only; rejection has separate coverage.
        let (store, plan, digest) = fixture(
            &[
                vec![row(b"a", Some(b"a"), 99)],
                vec![row(b"z", Some(b"z"), 6)],
            ],
            &[],
            false,
        )
        .await;
        // Install a locally consistent but semantically forged prior receipt.
        // Its authentication does not establish the unseen prefix's coverage.
        {
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut w = WorkspaceIoBudget::new();
            let mut p = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut w,
                payload: &mut p,
            };
            let expected = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
                ExpectedPlan::from_selected(&plan, &digest)
            })
            .unwrap();
            let selected = read_selected_progress(&mut io, &mut route, &expected)
                .await
                .unwrap();
            let directory =
                directory::Directory::new(store.retention.clone(), &store.scope).unwrap();
            let root = directory
                .decode_root(&binary(&plan.fields.source_kv_root_b64).unwrap())
                .unwrap();
            let side = read_side(
                &mut io,
                &mut route,
                &root,
                &plan.fields.source_kv_root_b64,
                &SideCursor::Start,
                None,
            )
            .await
            .unwrap();
            let receipt = decode_with_reservation(&mut io, &mut route, Some(MODEL_BYTES), || {
                Ok(behavioral_tests::receipt(
                    expected.value(),
                    0,
                    super::super::genesis_receipt_raw_sha256(expected.value())?,
                    super::super::genesis_chain_sha256(expected.value())?,
                    super::super::zero_cursor(),
                    MergeCursor {
                        global: GlobalCut::After {
                            key_b64url: URL_SAFE_NO_PAD.encode(b"a"),
                        },
                        source: side_after(
                            &side,
                            side.rows(),
                            &SideCursor::Start,
                            &plan.fields.source_kv_root_b64,
                        )?,
                        current: SideCursor::End,
                    },
                    super::super::CumulativeSemanticCounts::default(),
                ))
            })
            .unwrap();
            let progress = decode_with_reservation(&mut io, &mut route, Some(MODEL_BYTES), || {
                Ok(behavioral_tests::progress_after(
                    expected.value(),
                    receipt.value(),
                    &super::super::jcs(receipt.value())?,
                    false,
                ))
            })
            .unwrap();
            let unit = publication::assemble(
                &mut io,
                &mut route,
                &expected,
                &selected,
                &receipt,
                &progress,
                &[],
            )
            .unwrap();
            publication::publish(&mut io, &mut route, unit)
                .await
                .unwrap();
        }
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut w = WorkspaceIoBudget::new();
        let mut p = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut w,
            payload: &mut p,
        };
        let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan,
                plan_sha256: digest,
            })
        })
        .unwrap();
        let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .unwrap();
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).unwrap();
        let mut route = if final_route {
            RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
        } else {
            RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut w,
                payload: &mut p,
            }
        };
        let result = prepare(&mut io, &mut route, &owned, &expected, &selected).await;
        assert!(
            matches!(result,Err(CatalogError::InvariantViolation {ref message}) if message=="window evidence block exceeds pinned manifest sequence")
        );
        drop(result);
        assert_eq!(io.writing_evidence().0, 0);
        assert_eq!(io.allocation_underestimates(), 0);
    }
}

#[tokio::test]
async fn standard_window_cannot_select_inputs_in_final_stream() {
    use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
    let (store, plan, digest) = fixture(&[vec![row(b"a", Some(b"value"), 6)]], &[], false).await;
    let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
    let mut workspace = WorkspaceIoBudget::new();
    let mut payload = UnitPayloadAdmission::new();
    let mut ordinary = RestorePhysicalRoute::OrdinaryUnit {
        workspace: &mut workspace,
        payload: &mut payload,
    };
    let owned = decode_with_reservation(&mut io, &mut ordinary, Some(1024 * 1024), || {
        Ok(OwnedSelectedPlan {
            plan,
            plan_sha256: digest,
        })
    })
    .expect("plan");
    let expected =
        super::super::admitted_expected_plan(&mut io, &mut ordinary, &owned).expect("expected");
    let selected = read_selected_progress(&mut io, &mut ordinary, &expected)
        .await
        .expect("selected");
    let reads = io.reading_evidence();
    let mut totals = FinalStreamTotals::new();
    let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
    let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
    assert!(
        prepare(&mut io, &mut route, &owned, &expected, &selected)
            .await
            .is_err()
    );
    assert_eq!(io.reading_evidence(), reads, "no new merge selection");
    assert_eq!(io.writing_evidence(), (0, 0));
}

#[tokio::test]
async fn final_receipt_prefix_rejects_middle_tampering_and_ignores_orphans() {
    for mutation in 0..14 {
        let chunks: Vec<_> = (b'a'..=b'e')
            .map(|key| vec![row(&[key], Some(&[key]), 6)])
            .collect();
        let (store, plan, digest) = fixture(&chunks, &[], false).await;
        let fixture_expected = ExpectedPlan::from_selected(&plan, &digest).expect("fixture plan");
        let mut finished = false;
        for _ in 0..8 {
            advance_once(&store, &plan, &digest).await;
            let selector_raw = store
                .storage
                .get(&fixture_expected.selector_path())
                .await
                .expect("fixture selector");
            let selector: super::super::RestoreProgressSelectorV1 =
                control::decode_json(&selector_raw, "fixture selector").expect("selector");
            let progress_raw = store
                .storage
                .get(&selector.current_progress_path)
                .await
                .expect("fixture progress");
            let progress: RestoreProgressV1 =
                control::decode_json(&progress_raw, "fixture progress").expect("progress");
            if progress.terminal {
                finished = true;
                break;
            }
        }
        assert!(finished);
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan: plan.clone(),
                plan_sha256: digest.clone(),
            })
        })
        .expect("plan");
        let expected =
            super::super::admitted_expected_plan(&mut io, &mut route, &owned).expect("expected");
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .expect("terminal bundle");
        assert!(selected.progress.value().receipt_count >= 3);
        let mut prefix =
            super::super::prefix::Prefix::new(&mut io, &mut route, &expected, &selected)
                .expect("prefix");
        let ordinal = match mutation {
            10 | 11 => 0,
            12 => selected.progress.value().receipt_count - 1,
            _ => 1,
        };
        let path = expected.value().receipt_path(ordinal);
        if mutation == 0 {
            store
                .retention
                .delete(&path)
                .await
                .expect("missing middle fixture");
        } else if mutation == 1 {
            store
                .retention
                .put_raw(
                    &path,
                    bytes::Bytes::from_static(b"not-json"),
                    arco_core::WritePrecondition::None,
                )
                .await
                .expect("malformed fixture");
        } else if mutation == 9 {
            let orphan_path = expected
                .value()
                .receipt_path(selected.progress.value().receipt_count);
            store
                .retention
                .put_raw(
                    &orphan_path,
                    bytes::Bytes::from_static(b"unattached losing writer"),
                    arco_core::WritePrecondition::DoesNotExist,
                )
                .await
                .expect("orphan fixture");
        } else {
            let raw = store.storage.get(&path).await.expect("middle receipt");
            let mut receipt: ControlMvpRestoreReceiptV1 =
                control::decode_json(&raw, "receipt").expect("fixture model");
            match mutation {
                2 => receipt.ordinal += 1,
                3 => receipt.predecessor_receipt_sha256 = prefixed_sha256(b"wrong raw predecessor"),
                4 => receipt.predecessor_chain_sha256 = prefixed_sha256(b"wrong chain predecessor"),
                5 => receipt.before = super::super::zero_cursor(),
                6 => {
                    receipt.singleton_before = SingletonState::Complete {
                        key_b64url: "YQ".into(),
                    }
                }
                7 => receipt.prefix_cumulative_counts.mutations += 1,
                10 => receipt.predecessor_receipt_sha256 = prefixed_sha256(b"wrong genesis raw"),
                11 => receipt.predecessor_chain_sha256 = prefixed_sha256(b"wrong genesis chain"),
                12 => receipt.counts.mutations += 1,
                13 => {
                    receipt.after = MergeCursor {
                        global: GlobalCut::End,
                        source: SideCursor::End,
                        current: SideCursor::End,
                    }
                }
                _ => receipt.source_inputs[0].root_b64 = plan.fields.base_kv_root_b64.clone(),
            }
            // Repair local hashes so the continuity tests cannot pass merely by detecting an old body hash.
            receipt.receipt_body_sha256 =
                super::super::receipt_body_sha256(&receipt).expect("repaired body");
            receipt.chain_sha256 =
                super::super::receipt_chain_sha256(&receipt).expect("repaired chain");
            if matches!(mutation, 3 | 4 | 7 | 10..=13) {
                super::super::validate_receipt_shape(expected.value(), &receipt)
                    .expect("locally valid corrupted edge");
            }
            store
                .retention
                .put_raw(
                    &path,
                    super::super::jcs(&receipt)
                        .expect("corrupt canonical receipt")
                        .into(),
                    arco_core::WritePrecondition::None,
                )
                .await
                .expect("tampered fixture");
        }
        let mut totals = physical::restore_io::FinalStreamTotals::new();
        let mut count = 0;
        let mut failed = false;
        loop {
            let mut chunk = physical::restore_io::FinalMicrochunk::begin(&mut totals, 0, &mut io)
                .expect("chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            match prefix.next(&mut io, &mut route).await {
                Ok(Some(receipt)) => {
                    assert_eq!(receipt.value().ordinal, count);
                    count += 1;
                }
                Ok(None) => break,
                Err(_) => {
                    failed = true;
                    let reads = io.reading_evidence();
                    assert!(prefix.next(&mut io, &mut route).await.is_err());
                    assert_eq!(
                        io.reading_evidence(),
                        reads,
                        "failed prefix cannot retry I/O"
                    );
                    break;
                }
            }
        }
        assert_eq!(failed, mutation != 9, "mutation {mutation}");
        if mutation == 9 {
            assert_eq!(count, selected.progress.value().receipt_count);
        }
        assert_eq!(io.allocation_underestimates(), 0);
    }
}

#[tokio::test]
async fn final_receipt_prefix_cancellation_stops_first_and_resumed_reads() {
    use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
    use std::sync::atomic::Ordering;
    for resumed in [false, true] {
        for boundary in 1..=3 {
            let (store, remaining) = physical::restore_io::window_pending_store();
            let store = store.with_durable_authority_binding(
                crate::state_store::DurableAuthorityBinding::new([39; 32]),
            );
            let (_, plan) = super::super::super::tests::inspection_fixture().await;
            let chunks: Vec<_> = (b'a'..=b'e')
                .map(|key| vec![row(&[key], Some(&[key]), 6)])
                .collect();
            let (store, plan, digest) = fixture_on_store(store, plan, &chunks, &[], false).await;
            let e = ExpectedPlan::from_selected(&plan, &digest).unwrap();
            let mut terminal = false;
            for _ in 0..8 {
                advance_once(&store, &plan, &digest).await;
                let raw = store.storage.get(&e.selector_path()).await.unwrap();
                let selector: super::super::RestoreProgressSelectorV1 =
                    control::decode_json(&raw, "fixture selector").unwrap();
                let raw = store
                    .storage
                    .get(&selector.current_progress_path)
                    .await
                    .unwrap();
                let progress: RestoreProgressV1 =
                    control::decode_json(&raw, "fixture progress").unwrap();
                if progress.terminal {
                    assert!(progress.receipt_count >= 3);
                    terminal = true;
                    break;
                }
            }
            assert!(terminal);
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
                Ok(OwnedSelectedPlan {
                    plan,
                    plan_sha256: digest,
                })
            })
            .unwrap();
            let expected =
                super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
            let selected = read_selected_progress(&mut io, &mut route, &expected)
                .await
                .unwrap();
            let mut prefix =
                super::super::prefix::Prefix::new(&mut io, &mut route, &expected, &selected)
                    .unwrap();
            let mut totals = FinalStreamTotals::new();
            if resumed {
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).unwrap();
                drop(
                    prefix
                        .next(
                            &mut io,
                            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
                        )
                        .await
                        .unwrap()
                        .unwrap(),
                );
            }
            remaining.store(boundary, Ordering::SeqCst);
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).unwrap();
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let mut pending = Box::pin(prefix.next(&mut io, &mut route));
            assert!(matches!(
                futures::poll!(pending.as_mut()),
                std::task::Poll::Pending
            ));
            drop(pending);
            let reads = io.reading_evidence();
            let error = prefix
                .next(&mut io, &mut route)
                .await
                .err()
                .expect("cancelled prefix stops");
            assert_eq!(io.reading_evidence(), reads);
            assert_eq!(remaining.load(Ordering::SeqCst), 0);
            assert_eq!(io.allocation_underestimates(), 0);
            drop(prefix);
            drop(selected);
            drop(expected);
            drop(owned);
            assert_eq!(
                io.live_ownership_evidence(),
                (
                    crate::workspace_io_budget::catalog_error_string_capacity(&error)
                        .expect("known diagnostic"),
                    0
                ),
                "only the retained retry diagnostic remains after carried owners drop",
            );
            println!(
                "prefix cancellation resumed={resumed} boundary={boundary} retained_diagnostic={} error={error}",
                io.live_ownership_evidence().0
            );
        }
    }
}

#[tokio::test]
async fn final_receipt_prefix_rejects_nonterminal_owner_and_phase_offers() {
    use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
    for offer in 0..5 {
        let (store, plan, digest) = fixture(&[], &[], false).await;
        if offer != 0 {
            advance_once(&store, &plan, &digest).await;
        }
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan,
                plan_sha256: digest,
            })
        })
        .unwrap();
        let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .unwrap();
        let mut foreign = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let reads = io.reading_evidence();
        if offer == 0 {
            assert!(
                super::super::prefix::Prefix::new(&mut io, &mut route, &expected, &selected)
                    .is_err()
            );
        } else if offer == 1 {
            assert!(
                super::super::prefix::Prefix::new(&mut foreign, &mut route, &expected, &selected)
                    .is_err()
            );
        } else if offer == 2 {
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).unwrap();
            assert!(
                super::super::prefix::Prefix::new(
                    &mut io,
                    &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
                    &expected,
                    &selected
                )
                .is_err()
            );
        } else {
            let mut prefix =
                super::super::prefix::Prefix::new(&mut io, &mut route, &expected, &selected)
                    .unwrap();
            if offer == 3 {
                assert!(prefix.next(&mut io, &mut route).await.is_err());
            } else {
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut foreign).unwrap();
                assert!(
                    prefix
                        .next(
                            &mut foreign,
                            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                        )
                        .await
                        .is_err()
                );
            }
        }
        assert_eq!(io.reading_evidence(), reads);
        assert_eq!(foreign.reading_evidence(), (0, 0, 0));
        assert_eq!(io.allocation_underestimates(), 0);
        assert_eq!(foreign.allocation_underestimates(), 0);
    }
}

#[tokio::test]
async fn final_coverage_rejects_forged_witnesses_and_corrupt_objects() {
    for mutation in 0..13 {
        let (store, plan, digest) =
            fixture(&[vec![row(b"a", Some(b"value"), 6)]], &[], false).await;
        advance_once(&store, &plan, &digest).await;
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan,
                plan_sha256: digest,
            })
        })
        .unwrap();
        let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
        let (raw, metadata) = physical::restore_io::read_restore_control_record(
            &mut io,
            &mut route,
            expected.value().candidate_id,
            physical::restore_io::RestoreControlRecord::Receipt(0),
        )
        .await
        .unwrap()
        .unwrap();
        let receipt = codec::decode_receipt(&mut io, &mut route, &raw).unwrap();
        let forged = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            let mut value = receipt.value().clone();
            match mutation {
                0 | 1 => {
                    let descriptor = if mutation == 1 {
                        &mut value.outputs[0].descriptor
                    } else {
                        &mut value.source_inputs[0].descriptor
                    };
                    descriptor.sha256 = prefixed_sha256(b"forged descriptor identity");
                    descriptor.path = format!(
                        "{}/physical/descriptors/{}.json",
                        store.paths.base_prefix(),
                        super::super::raw_digest(&descriptor.sha256)?
                    );
                }
                2 => {
                    value.source_inputs.clear();
                    value.counts.source_leaves = 0;
                    value.counts.decoded_blocks = 0;
                    value.counts.decoded_rows = 0;
                    value.counts.input_encoded_bytes = 0;
                }
                3 => value.counts.mutations += 1,
                4 => {
                    let SideCursor::After { position, .. } = &mut value.after.source else {
                        panic!("after position")
                    };
                    position.path[0].child_index ^= 1;
                }
                5 => {
                    value.outputs.clear();
                    value.counts.output_blocks = 0;
                    value.counts.output_encoded_bytes = 0;
                }
                6 => value.source_inputs[0].block.sha256 = prefixed_sha256(b"wrong block"),
                _ => {}
            }
            Ok(value)
        })
        .unwrap();
        let forged = finish_receipt(&mut io, &mut route, forged).unwrap();
        drop(
            codec::validate_receipt(&mut io, &mut route, &expected, &forged)
                .expect("locally valid forged identity"),
        );
        if mutation >= 7 {
            let (descriptor_path, index_path) = if mutation >= 10 {
                (
                    &receipt.value().outputs[0].descriptor.path,
                    &receipt.value().outputs[0].index.path,
                )
            } else {
                (
                    &receipt.value().source_inputs[0].descriptor.path,
                    &receipt.value().source_inputs[0].index.path,
                )
            };
            let path = match (mutation - 7) % 3 {
                0 => descriptor_path.clone(),
                1 => index_path.clone(),
                _ => {
                    let raw = store.storage.get(descriptor_path).await.unwrap();
                    let d: physical::Descriptor =
                        control::decode_json(&raw, "fixture descriptor").unwrap();
                    store.paths.state_object(&d.segment.segment_id)
                }
            };
            let mut bytes = store.storage.get(&path).await.unwrap().to_vec();
            bytes[0] ^= 1;
            store
                .retention
                .put_raw(&path, bytes.into(), arco_core::WritePrecondition::None)
                .await
                .unwrap();
        }
        drop((receipt, raw, metadata));
        let mut totals = physical::restore_io::FinalStreamTotals::new();
        assert!(
            super::super::coverage::verify_standard(
                &mut io,
                &mut totals,
                &owned,
                &expected,
                &forged
            )
            .await
            .is_err(),
            "forgery or corruption {mutation}"
        );
    }
}

#[tokio::test]
async fn final_coverage_uses_one_chunk_per_physical_primitive() {
    let (store, plan, digest) = fixture(&[vec![row(b"a", Some(b"value"), 6)]], &[], false).await;
    advance_once(&store, &plan, &digest).await;
    let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
    let mut workspace = WorkspaceIoBudget::new();
    let mut payload = UnitPayloadAdmission::new();
    let mut route = RestorePhysicalRoute::OrdinaryUnit {
        workspace: &mut workspace,
        payload: &mut payload,
    };
    let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
        Ok(OwnedSelectedPlan {
            plan,
            plan_sha256: digest,
        })
    })
    .unwrap();
    let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
    let (raw, metadata) = physical::restore_io::read_restore_control_record(
        &mut io,
        &mut route,
        expected.value().candidate_id,
        physical::restore_io::RestoreControlRecord::Receipt(0),
    )
    .await
    .unwrap()
    .unwrap();
    let receipt = codec::decode_receipt(&mut io, &mut route, &raw).unwrap();
    drop((raw, metadata));
    let mut totals = physical::restore_io::FinalStreamTotals::new();
    super::super::coverage::verify_standard(&mut io, &mut totals, &owned, &expected, &receipt)
        .await
        .unwrap();
    // Two paths plus input and output descriptor/index and payload primitives.
    assert_eq!(
        totals.microchunks(),
        6,
        "CPU-only resets cannot renew a physical primitive allowance"
    );
}

#[tokio::test]
async fn final_coverage_exhausted_stream_stops_the_invocation_owner() {
    let (store, plan, digest) = fixture(&[vec![row(b"a", Some(b"value"), 6)]], &[], false).await;
    advance_once(&store, &plan, &digest).await;
    let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
    let mut workspace = WorkspaceIoBudget::new();
    let mut payload = UnitPayloadAdmission::new();
    let mut route = RestorePhysicalRoute::OrdinaryUnit {
        workspace: &mut workspace,
        payload: &mut payload,
    };
    let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
        Ok(OwnedSelectedPlan {
            plan,
            plan_sha256: digest,
        })
    })
    .unwrap();
    let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
    let (raw, metadata) = physical::restore_io::read_restore_control_record(
        &mut io,
        &mut route,
        expected.value().candidate_id,
        physical::restore_io::RestoreControlRecord::Receipt(0),
    )
    .await
    .unwrap()
    .unwrap();
    let receipt = codec::decode_receipt(&mut io, &mut route, &raw).unwrap();
    drop((raw, metadata));
    let mut totals = physical::restore_io::FinalStreamTotals::new();
    assert!(
        physical::restore_io::FinalMicrochunk::begin(&mut totals, usize::MAX, &mut io).is_err()
    );
    assert!(
        super::super::coverage::verify_standard(&mut io, &mut totals, &owned, &expected, &receipt)
            .await
            .is_err()
    );
    let mut fresh = physical::restore_io::FinalStreamTotals::new();
    assert!(
        physical::restore_io::FinalMicrochunk::begin(&mut fresh, 0, &mut io).is_err(),
        "a failed final invocation cannot obtain fresh admission with the same owner"
    );
}

#[tokio::test]
async fn final_coverage_rejects_coherent_false_segment_checksum() {
    let (store, plan, digest) = fixture(&[vec![row(b"a", Some(b"value"), 6)]], &[], false).await;
    advance_once(&store, &plan, &digest).await;
    let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
    let mut workspace = WorkspaceIoBudget::new();
    let mut payload = UnitPayloadAdmission::new();
    let mut route = RestorePhysicalRoute::OrdinaryUnit {
        workspace: &mut workspace,
        payload: &mut payload,
    };
    let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
        Ok(OwnedSelectedPlan {
            plan,
            plan_sha256: digest,
        })
    })
    .unwrap();
    let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
    let (raw, metadata) = physical::restore_io::read_restore_control_record(
        &mut io,
        &mut route,
        expected.value().candidate_id,
        physical::restore_io::RestoreControlRecord::Receipt(0),
    )
    .await
    .unwrap()
    .unwrap();
    let receipt = codec::decode_receipt(&mut io, &mut route, &raw).unwrap();
    drop((raw, metadata));
    let witness = &receipt.value().outputs[0];
    let raw = store.storage.get(&witness.descriptor.path).await.unwrap();
    let mut descriptor: physical::Descriptor =
        control::decode_json(&raw, "fixture descriptor").unwrap();
    let raw = store.storage.get(&witness.index.path).await.unwrap();
    let mut index: control::ControlMvpSegmentIndex =
        control::decode_json(&raw, "fixture index").unwrap();
    let false_checksum = "ab".repeat(32);
    assert_ne!(descriptor.block.checksum_sha256, false_checksum);
    index.segment_checksum_sha256 = false_checksum.clone();
    descriptor.segment.checksum_sha256 = false_checksum;
    let index_raw = serde_json::to_vec(&index).unwrap();
    let index_size = index_raw.len() as u64;
    let index_sha = prefixed_sha256(&index_raw);
    descriptor.segment.index_size_bytes = index_size;
    descriptor.segment.index_checksum_sha256 = super::super::raw_digest(&index_sha).unwrap().into();
    let arco_core::WriteResult::Success { version } = store
        .retention
        .put_raw(
            &witness.index.path,
            index_raw.into(),
            arco_core::WritePrecondition::None,
        )
        .await
        .unwrap()
    else {
        panic!("index fixture")
    };
    descriptor.index_version = version;
    let descriptor_raw = serde_json::to_vec(&descriptor).unwrap();
    let descriptor_size = descriptor_raw.len() as u64;
    let descriptor_sha = prefixed_sha256(&descriptor_raw);
    let descriptor_path = format!(
        "{}/physical/descriptors/{}.json",
        store.paths.base_prefix(),
        super::super::raw_digest(&descriptor_sha).unwrap()
    );
    store
        .retention
        .put_raw(
            &descriptor_path,
            descriptor_raw.into(),
            arco_core::WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    let forged = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
        let mut value = receipt.value().clone();
        let output = &mut value.outputs[0];
        output.directory_leaf.digest = descriptor_sha.clone();
        output.descriptor.sha256 = descriptor_sha.clone();
        output.descriptor.byte_size = descriptor_size;
        output.descriptor.path = descriptor_path.clone();
        output.index.sha256 = index_sha.clone();
        output.index.byte_size = index_size;
        Ok(value)
    })
    .unwrap();
    let forged = finish_receipt(&mut io, &mut route, forged).unwrap();
    drop(
        codec::validate_receipt(&mut io, &mut route, &expected, &forged)
            .expect("locally valid coherent forgery"),
    );
    let mut totals = physical::restore_io::FinalStreamTotals::new();
    assert!(
        super::super::coverage::verify_standard(&mut io, &mut totals, &owned, &expected, &forged)
            .await
            .is_err(),
        "a complete output block must authenticate the segment checksum as the same bytes"
    );
}

#[tokio::test]
async fn final_coverage_cancellation_stops_every_physical_await_and_releases_carry() {
    use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
    use std::sync::atomic::Ordering;
    for resumed in [false, true] {
        let mut boundaries = 0;
        for at in 0..80 {
            if at > 0 && at > boundaries {
                break;
            }
            let (store, remaining) = physical::restore_io::window_pending_store();
            let store = store.with_durable_authority_binding(
                crate::state_store::DurableAuthorityBinding::new([39; 32]),
            );
            let (_, plan) = super::super::super::tests::inspection_fixture().await;
            let (store, plan, digest) = fixture_on_store(
                store,
                plan,
                &[vec![
                    row(b"a", Some(&vec![1; 100_000]), 6),
                    row(b"z", Some(&vec![2; 100_000]), 6),
                ]],
                &[vec![
                    row(&vec![b'b'; 20_000], Some(b"old"), 9),
                    row(b"z", None, 9),
                ]],
                false,
            )
            .await;
            advance_once(&store, &plan, &digest).await;
            if resumed {
                advance_once(&store, &plan, &digest).await;
            }
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
                Ok(OwnedSelectedPlan {
                    plan,
                    plan_sha256: digest,
                })
            })
            .unwrap();
            let expected =
                super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
            let (raw, metadata) = physical::restore_io::read_restore_control_record(
                &mut io,
                &mut route,
                expected.value().candidate_id,
                physical::restore_io::RestoreControlRecord::Receipt(u64::from(resumed)),
            )
            .await
            .unwrap()
            .unwrap();
            let receipt = codec::decode_receipt(&mut io, &mut route, &raw).unwrap();
            drop((raw, metadata));
            let baseline = io.live_ownership_evidence();
            let mut totals = FinalStreamTotals::new();
            remaining.store(if at == 0 { usize::MAX } else { at }, Ordering::SeqCst);
            let diagnostic = if at == 0 {
                super::super::coverage::verify_standard(
                    &mut io,
                    &mut totals,
                    &owned,
                    &expected,
                    &receipt,
                )
                .await
                .unwrap();
                boundaries = usize::MAX - remaining.load(Ordering::SeqCst);
                assert!(boundaries > 6 && boundaries < 80);
                assert_eq!(io.live_ownership_evidence(), baseline);
                0
            } else {
                let mut pending = Box::pin(super::super::coverage::verify_standard(
                    &mut io,
                    &mut totals,
                    &owned,
                    &expected,
                    &receipt,
                ));
                assert!(
                    matches!(futures::poll!(pending.as_mut()), std::task::Poll::Pending),
                    "resumed={resumed} boundary={at}"
                );
                drop(pending);
                assert_eq!(
                    io.live_ownership_evidence(),
                    baseline,
                    "cancelled physical owners release"
                );
                let reads = io.reading_evidence();
                let mut fresh = FinalStreamTotals::new();
                let error = FinalMicrochunk::begin(&mut fresh, 0, &mut io)
                    .err()
                    .expect("cancelled owner remains stopped");
                assert_eq!(io.reading_evidence(), reads);
                assert_eq!(remaining.load(Ordering::SeqCst), 0);
                crate::workspace_io_budget::catalog_error_string_capacity(&error).unwrap()
            };
            assert_eq!(io.allocation_underestimates(), 0);
            assert!(io.peak_owned_evidence() <= 64 * 1024 * 1024);
            drop(receipt);
            drop(expected);
            drop(owned);
            assert_eq!(io.live_ownership_evidence(), (diagnostic, 0));
        }
        println!("final physical cancellation resumed={resumed} boundaries={boundaries}");
    }
}

#[tokio::test]
async fn standard_final_assembly_matches_actual_restore_roots_and_history() {
    use crate::state_store::{
        ArcoStateTxn, PersistedAuthorityAdapter, RestorePlanningContext, TxnOptions,
    };
    use physical::restore_io::FinalStreamTotals;
    for (changed, large) in [(false, false), (true, false), (true, true)] {
        let (store, mut plan) = super::super::super::tests::inspection_fixture().await;
        if large {
            let mut txn = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            txn.set_logical_operation("assembly-source", "test", &"ae".repeat(32))
                .unwrap();
            for key in [b"a".as_slice(), b"z".as_slice()] {
                txn.put(key, bytes::Bytes::from(vec![7; 180_000]))
                    .await
                    .unwrap();
            }
            let token = txn.commit_v2().await.unwrap().token().clone();
            let source = store
                .persist_state_reference(
                    &token,
                    plan.fields.source_retention_deadline.datetime().unwrap(),
                )
                .await
                .unwrap();
            let mut budget = WorkspaceIoBudget::new();
            let mut context = RestorePlanningContext::new(
                plan.fields.workspace_request_sha256.clone(),
                plan.fields.requested_at.datetime().unwrap(),
                plan.fields.execution_deadline.datetime().unwrap(),
                plan.fields.requested_at.datetime().unwrap(),
                crate::workspace_io_budget::WorkspaceCaptureIo::new(&store.retention, &mut budget),
            );
            plan = super::super::super::plan(&store, &source, &plan.fields.identity, &mut context)
                .await
                .unwrap();
        }
        if changed {
            // Explicit fixture construction of the protected retained root;
            // this is not an executed workspace capture operation.
            store
                .install_test_retained_source(&plan.fields.source)
                .await
                .unwrap();
            let mut txn = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            txn.set_logical_operation("assembly-target", "test", &"ad".repeat(32))
                .unwrap();
            txn.put(b"key", bytes::Bytes::from_static(b"changed"))
                .await
                .unwrap();
            txn.put(b"extra", bytes::Bytes::from_static(b"remove"))
                .await
                .unwrap();
            txn.commit_v2().await.unwrap();
            let mut budget = WorkspaceIoBudget::new();
            let mut context = RestorePlanningContext::new(
                plan.fields.workspace_request_sha256.clone(),
                plan.fields.requested_at.datetime().unwrap(),
                plan.fields.execution_deadline.datetime().unwrap(),
                // Keep the fixture's fixed-nanosecond logical clock. Wall time
                // can still precede that instant within the same second.
                plan.fields.requested_at.datetime().unwrap(),
                crate::workspace_io_budget::WorkspaceCaptureIo::new(&store.retention, &mut budget),
            );
            plan = super::super::super::plan(
                &store,
                &plan.fields.source,
                &plan.fields.identity,
                &mut context,
            )
            .await
            .unwrap();
        }
        let digest = prefixed_sha256(
            &super::super::jcs(
                &crate::state_store::PersistedRestoreParticipantPlan::ControlMvpV7(Box::new(
                    plan.clone(),
                )),
            )
            .unwrap(),
        );
        let expected = ExpectedPlan::from_selected(&plan, &digest).unwrap();
        behavioral_tests::seed_selected_read_case(&store, &expected, 14).await;
        let mut terminal = false;
        for ordinal in 0..8 {
            advance_once(&store, &plan, &digest).await;
            let raw = store
                .storage
                .get(&expected.progress_path(ordinal + 1))
                .await
                .unwrap();
            let progress: RestoreProgressV1 = control::decode_json(&raw, "progress").unwrap();
            if progress.terminal {
                terminal = true;
                break;
            }
        }
        assert!(terminal);
        let request = plan.logical_request().unwrap();
        let mut oracle = request
            .history(
                super::super::raw_digest(&plan.fields.logical_commit_id).unwrap(),
                if changed { 2 } else { 0 },
            )
            .unwrap();
        if changed {
            oracle
                .push(b"extra", plan.fields.result_logical_sequence, None)
                .unwrap();
            oracle
                .push(b"key", plan.fields.result_logical_sequence, Some(b"value"))
                .unwrap();
        }
        let expected_digest = oracle.finish().unwrap().0;
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan: plan.clone(),
                plan_sha256: digest.clone(),
            })
        })
        .unwrap();
        let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .unwrap();
        if large {
            assert!(selected.progress.value().receipt_count >= 3);
            assert!(selected.progress.value().cumulative_counts.output_blocks >= 2);
        }
        let mut totals = FinalStreamTotals::new();
        let assembled = super::super::coverage::assemble_standard(
            &mut io,
            &mut route,
            &mut totals,
            &owned,
            &expected,
            &selected,
        )
        .await
        .unwrap();
        assert_eq!(assembled.digest(), expected_digest);
        assert_eq!(io.allocation_underestimates(), 0);
        assert!(io.peak_owned_evidence() <= 64 * 1024 * 1024);
        let mut last = None;
        let mut actual = BTreeMap::new();
        loop {
            let position = directory::restore::first_after(
                &mut io,
                &mut route,
                assembled.root(),
                last.as_deref(),
            )
            .await
            .unwrap();
            let Some(position) = position.value() else {
                break;
            };
            let (_, rows) = physical::restore_io::read_restore_leaf(
                &mut io,
                &mut route,
                physical::Role::Kv,
                &position.leaf,
            )
            .await
            .unwrap();
            for row in rows.value() {
                actual.insert(row.key.clone(), (row.value.clone(), row.generation));
            }
            last = Some(position.leaf.last.clone());
        }
        let mut expected_rows = BTreeMap::new();
        expected_rows.insert(
            b"key".to_vec(),
            (
                Some(b"value".to_vec()),
                if changed {
                    plan.fields.result_logical_sequence
                } else {
                    plan.fields.base_logical_sequence
                },
            ),
        );
        if changed {
            expected_rows.insert(
                b"extra".to_vec(),
                (None, plan.fields.result_logical_sequence),
            );
        }
        if large {
            for key in [b"a".as_slice(), b"z".as_slice()] {
                expected_rows.insert(
                    key.to_vec(),
                    (Some(vec![7; 180_000]), plan.fields.source_logical_sequence),
                );
            }
        }
        assert_eq!(actual, expected_rows);
    }
}

#[tokio::test]
async fn standard_final_assembly_cancellation_releases_all_internal_owners() {
    use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
    use std::sync::atomic::Ordering;
    let mut boundaries = 0;
    for at in 0..100 {
        if at > 0 && at > boundaries {
            break;
        }
        let (store, remaining) = physical::restore_io::window_pending_store();
        let store = store.with_durable_authority_binding(
            crate::state_store::DurableAuthorityBinding::new([39; 32]),
        );
        let (store, plan) = super::super::super::tests::inspection_fixture_on_store(store).await;
        let digest = prefixed_sha256(
            &super::super::jcs(
                &crate::state_store::PersistedRestoreParticipantPlan::ControlMvpV7(Box::new(
                    plan.clone(),
                )),
            )
            .unwrap(),
        );
        let expected = ExpectedPlan::from_selected(&plan, &digest).unwrap();
        behavioral_tests::seed_selected_read_case(&store, &expected, 14).await;
        advance_once(&store, &plan, &digest).await;
        advance_once(&store, &plan, &digest).await;
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan: plan.clone(),
                plan_sha256: digest.clone(),
            })
        })
        .unwrap();
        let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .unwrap();
        let baseline = io.live_ownership_evidence();
        let mut totals = FinalStreamTotals::new();
        remaining.store(if at == 0 { usize::MAX } else { at }, Ordering::SeqCst);
        let diagnostic = if at == 0 {
            let assembled = super::super::coverage::assemble_standard(
                &mut io,
                &mut route,
                &mut totals,
                &owned,
                &expected,
                &selected,
            )
            .await
            .unwrap();
            assert!(io.live_ownership_evidence().0 > baseline.0 + MAX_BLOCK_BYTES);
            boundaries = usize::MAX - remaining.load(Ordering::SeqCst);
            assert!(boundaries > 20 && boundaries < 100);
            drop(assembled);
            assert_eq!(io.live_ownership_evidence(), baseline);
            0
        } else {
            let mut pending = Box::pin(super::super::coverage::assemble_standard(
                &mut io,
                &mut route,
                &mut totals,
                &owned,
                &expected,
                &selected,
            ));
            assert!(
                matches!(futures::poll!(pending.as_mut()), std::task::Poll::Pending),
                "boundary={at}"
            );
            drop(pending);
            assert_eq!(io.live_ownership_evidence(), baseline, "boundary={at}");
            let reads = io.reading_evidence();
            let writes = io.writing_evidence();
            let mut fresh = FinalStreamTotals::new();
            let error = FinalMicrochunk::begin(&mut fresh, 0, &mut io)
                .err()
                .expect("cancelled assembly owner stops");
            assert_eq!(remaining.load(Ordering::SeqCst), 0);
            assert_eq!(io.reading_evidence(), reads);
            assert_eq!(io.writing_evidence(), writes);
            crate::workspace_io_budget::catalog_error_string_capacity(&error).unwrap()
        };
        assert_eq!(io.allocation_underestimates(), 0);
        assert!(io.peak_owned_evidence() <= 64 * 1024 * 1024);
        drop(selected);
        drop(expected);
        drop(owned);
        assert_eq!(io.live_ownership_evidence(), (diagnostic, 0));
    }
    println!("standard final assembly cancellation boundaries={boundaries}");
}

#[tokio::test]
async fn standard_final_assembly_rejects_coherent_terminal_and_output_forgery() {
    use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
    for mutation in 0..3 {
        let (store, plan) = super::super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(
            &super::super::jcs(
                &crate::state_store::PersistedRestoreParticipantPlan::ControlMvpV7(Box::new(
                    plan.clone(),
                )),
            )
            .unwrap(),
        );
        let e = ExpectedPlan::from_selected(&plan, &digest).unwrap();
        behavioral_tests::seed_selected_read_case(&store, &e, 14).await;
        advance_once(&store, &plan, &digest).await;
        advance_once(&store, &plan, &digest).await;
        let head = store
            .storage
            .get(&store.paths.current_pointer())
            .await
            .unwrap();
        let mut first: ControlMvpRestoreReceiptV1 = control::decode_json(
            &store.storage.get(&e.receipt_path(0)).await.unwrap(),
            "first",
        )
        .unwrap();
        let mut last: ControlMvpRestoreReceiptV1 = control::decode_json(
            &store.storage.get(&e.receipt_path(1)).await.unwrap(),
            "last",
        )
        .unwrap();
        if mutation == 0 {
            last.prefix_cumulative_counts.mutations += 1;
        }
        if mutation == 1 {
            last.predecessor_chain_sha256 = prefixed_sha256(b"forged predecessor");
        }
        if mutation == 2 {
            first.outputs.clear();
            first.counts.output_blocks = 0;
            first.counts.output_encoded_bytes = 0;
            first.receipt_body_sha256 = super::super::receipt_body_sha256(&first).unwrap();
            first.chain_sha256 = super::super::receipt_chain_sha256(&first).unwrap();
            super::super::validate_receipt_shape(&e, &first).unwrap();
            let raw = super::super::jcs(&first).unwrap();
            last.predecessor_receipt_sha256 = prefixed_sha256(&raw);
            last.predecessor_chain_sha256 = first.chain_sha256.clone();
            last.prefix_cumulative_counts =
                checked_sum(&first.prefix_cumulative_counts, &first.counts).unwrap();
            store
                .retention
                .put_raw(
                    &e.receipt_path(0),
                    raw.into(),
                    arco_core::WritePrecondition::None,
                )
                .await
                .unwrap();
        }
        last.receipt_body_sha256 = super::super::receipt_body_sha256(&last).unwrap();
        last.chain_sha256 = super::super::receipt_chain_sha256(&last).unwrap();
        super::super::validate_receipt_shape(&e, &last).unwrap();
        let raw = super::super::jcs(&last).unwrap();
        let mut progress: RestoreProgressV1 = control::decode_json(
            &store.storage.get(&e.progress_path(2)).await.unwrap(),
            "progress",
        )
        .unwrap();
        progress.last_receipt.as_mut().unwrap().raw_sha256 = prefixed_sha256(&raw);
        progress.chain_sha256 = last.chain_sha256.clone();
        progress.cumulative_counts =
            checked_sum(&last.prefix_cumulative_counts, &last.counts).unwrap();
        store
            .retention
            .put_raw(
                &e.receipt_path(1),
                raw.into(),
                arco_core::WritePrecondition::None,
            )
            .await
            .unwrap();
        let raw = super::super::jcs(&progress).unwrap();
        let mut selector: super::super::RestoreProgressSelectorV1 = control::decode_json(
            &store.storage.get(&e.selector_path()).await.unwrap(),
            "selector",
        )
        .unwrap();
        selector.current_progress_sha256 = prefixed_sha256(&raw);
        store
            .retention
            .put_raw(
                &e.progress_path(2),
                raw.into(),
                arco_core::WritePrecondition::None,
            )
            .await
            .unwrap();
        store
            .retention
            .put_raw(
                &e.selector_path(),
                super::super::jcs(&selector).unwrap().into(),
                arco_core::WritePrecondition::None,
            )
            .await
            .unwrap();
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan: plan.clone(),
                plan_sha256: digest.clone(),
            })
        })
        .unwrap();
        let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .expect("coherent locally valid terminal selection");
        let baseline = io.live_ownership_evidence();
        let mut totals = FinalStreamTotals::new();
        let error = super::super::coverage::assemble_standard(
            &mut io,
            &mut route,
            &mut totals,
            &owned,
            &expected,
            &selected,
        )
        .await
        .err()
        .expect("independent final proof rejects forgery");
        let diagnostic = crate::workspace_io_budget::catalog_error_string_capacity(&error).unwrap();
        assert_eq!(
            io.live_ownership_evidence(),
            (baseline.0 + diagnostic, baseline.1)
        );
        assert_eq!(io.allocation_underestimates(), 0);
        assert_eq!(
            store
                .storage
                .get(&store.paths.current_pointer())
                .await
                .unwrap(),
            head
        );
        let mut fresh = FinalStreamTotals::new();
        assert!(FinalMicrochunk::begin(&mut fresh, 0, &mut io).is_err());
    }
}

#[tokio::test]
async fn singleton_comparison_restarts_with_compact_witnesses() {
    use super::super::{SingletonPhase, SingletonState};
    let source = vec![41; 40 * 1024 * 1024];
    let current = vec![42; 40 * 1024 * 1024];
    let (store, plan, digest) = fixture(
        &[vec![row(b"large", Some(&source), 6)]],
        &[vec![row(b"large", Some(&current), 9)]],
        false,
    )
    .await;
    for (ordinal, phase) in [
        SingletonPhase::CompareSource,
        SingletonPhase::CompareCurrent,
        SingletonPhase::Emit,
    ]
    .into_iter()
    .enumerate()
    {
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan: plan.clone(),
                plan_sha256: digest.clone(),
            })
        })
        .unwrap();
        let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .unwrap();
        let unit =
            super::super::singleton::prepare(&mut io, &mut route, &owned, &expected, &selected)
                .await
                .expect("one bounded singleton comparison");
        assert!(
            io.live_ownership_evidence().0 < 1024 * 1024,
            "decoded payload must be dropped before staging"
        );
        assert_eq!(
            publication::publish(&mut io, &mut route, unit)
                .await
                .unwrap(),
            publication::Publication::Written
        );
        drop(selected);
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .unwrap();
        let p = selected.progress.value();
        assert_eq!(p.next_ordinal, ordinal as u64 + 1);
        assert!(matches!(p.cursor.global, GlobalCut::Start));
        assert!(matches!(p.cursor.source, SideCursor::Start));
        assert!(matches!(p.cursor.current, SideCursor::Start));
        let SingletonState::Pending {
            source: s,
            current: c,
            phase: actual,
            ..
        } = &p.singleton_state
        else {
            panic!("pending singleton");
        };
        assert_eq!(*actual, phase);
        assert_eq!(s.as_ref().unwrap().value_sha256, prefixed_sha256(&source));
        assert_eq!(s.as_ref().unwrap().value_length, source.len() as u64);
        assert!(io.hashing_evidence().1 >= source.len() as u64);
        assert!(
            io.peak_owned_evidence()
                <= 64 * 1024 * 1024
                    + 24 * usize::try_from(s.as_ref().unwrap().block.length).unwrap()
        );
        println!(
            "singleton ordinal={ordinal} reads={:?} writes={:?} hashes={:?} peak={}",
            io.reading_evidence(),
            io.writing_evidence(),
            io.hashing_evidence(),
            io.peak_owned_evidence()
        );
        if ordinal > 0 {
            assert_eq!(c.as_ref().unwrap().value_sha256, prefixed_sha256(&current));
        } else {
            assert!(c.is_none());
        }
        assert_eq!(p.cumulative_counts.decoded_blocks, ordinal as u64 + 1);
        assert_eq!(p.cumulative_counts.decoded_rows, ordinal as u64 + 1);
        assert_eq!(p.cumulative_counts.output_blocks, 0);
        assert_eq!(io.allocation_underestimates(), 0);
        drop(selected);
        drop(expected);
        drop(owned);
        assert_eq!(io.live_ownership_evidence(), (0, 0));
    }
}

async fn singleton_test_step(
    store: &ControlMvpStateStore,
    plan: &super::super::ControlMvpRestorePlanV7,
    digest: &str,
) -> RestoreProgressV1 {
    let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 64 * 1024 * 1024);
    let mut workspace = WorkspaceIoBudget::new();
    let mut payload = UnitPayloadAdmission::new();
    let mut route = RestorePhysicalRoute::OrdinaryUnit {
        workspace: &mut workspace,
        payload: &mut payload,
    };
    let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
        Ok(OwnedSelectedPlan {
            plan: plan.clone(),
            plan_sha256: digest.into(),
        })
    })
    .unwrap();
    let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
    let selected = read_selected_progress(&mut io, &mut route, &expected)
        .await
        .unwrap();
    let unit = super::super::singleton::prepare(&mut io, &mut route, &owned, &expected, &selected)
        .await
        .unwrap();
    let target = unit.recovery_target();
    assert_eq!(
        publication::publish(&mut io, &mut route, unit)
            .await
            .unwrap(),
        publication::Publication::Written
    );
    assert_eq!(
        publication::reconcile(&mut io, &mut route, &expected, target)
            .await
            .unwrap(),
        publication::Reconciliation::Exact
    );
    let next = read_selected_progress(&mut io, &mut route, &expected)
        .await
        .unwrap();
    assert_eq!(io.allocation_underestimates(), 0);
    next.progress.value().clone()
}

#[tokio::test]
async fn singleton_comparison_absence_empty_tombstone_and_opaque_values() {
    use super::super::{SingletonPhase, SingletonState};
    for case in 0..6 {
        let source = match case {
            0 => vec![],
            1 => vec![vec![row(b"key", Some(b""), 6)]],
            2 => vec![vec![row(b"key", None, 6)]],
            3 => vec![vec![row(b"key", Some(&[0, 255, 128, 0]), 6)]],
            4 => vec![vec![row(b"later", Some(b"later"), 6)]],
            _ => vec![vec![row(b"key", Some(b"source"), 6)]],
        };
        let current = match case {
            0 | 1 | 4 => vec![vec![row(b"key", None, 9)]],
            2 => vec![vec![row(b"key", Some(b""), 9)]],
            _ => vec![],
        };
        let (store, plan, digest) = fixture(&source, &current, case == 5).await;
        for phase in [
            SingletonPhase::CompareSource,
            SingletonPhase::CompareCurrent,
            SingletonPhase::Emit,
        ] {
            let p = singleton_test_step(&store, &plan, &digest).await;
            let SingletonState::Pending {
                source: s,
                current: c,
                phase: actual,
                ..
            } = p.singleton_state
            else {
                panic!("pending");
            };
            assert_eq!(actual, phase);
            for (actual, rows) in [(s.as_ref(), &source), (c.as_ref(), &current)] {
                if let Some(w) = actual {
                    let r = &rows[0][0];
                    assert_eq!(w.generation, r.generation);
                    assert_eq!(w.tombstone, r.tombstone);
                    assert_eq!(
                        w.value_length,
                        r.value.as_ref().map_or(0, |v| v.len() as u64)
                    );
                    assert_eq!(
                        w.value_sha256,
                        prefixed_sha256(r.value.as_deref().unwrap_or_default())
                    );
                }
            }
            if case == 0 || case == 4 {
                assert!(s.is_none());
                assert!(c.is_some());
            }
            if case >= 3 && case != 4 {
                assert!(s.is_some());
                assert!(c.is_none());
            }
            if case == 1 && phase != SingletonPhase::CompareSource {
                let (s, c) = (s.unwrap(), c.unwrap());
                assert_eq!(s.value_sha256, c.value_sha256);
                assert!(!s.tombstone && c.tombstone);
            }
        }
    }
}

#[tokio::test]
async fn singleton_comparison_rejects_forged_observations_and_unsupported_boundaries() {
    use super::super::{SingletonPhase, SingletonState};
    for case in 0..9 {
        let source = if case == 1 {
            vec![vec![
                row(b"a", Some(b"a"), 6),
                row(b"key", Some(b"source"), 6),
            ]]
        } else {
            vec![vec![row(
                b"key",
                Some(b"source"),
                if case == 0 { 99 } else { 6 },
            )]]
        };
        let (store, plan, digest) =
            fixture(&source, &[vec![row(b"key", Some(b"current"), 9)]], false).await;
        if case >= 4 {
            singleton_test_step(&store, &plan, &digest).await;
            if case >= 5 {
                singleton_test_step(&store, &plan, &digest).await;
            }
            if case == 8 {
                singleton_test_step(&store, &plan, &digest).await;
            }
        }
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan: plan.clone(),
                plan_sha256: digest.clone(),
            })
        })
        .unwrap();
        let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
        let mut selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .unwrap();
        selected.progress = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            let mut p = selected.progress.value().clone();
            match case {
                2 => p.cursor.source = SideCursor::End,
                3 => {
                    p.cursor.global = GlobalCut::After {
                        key_b64url: URL_SAFE_NO_PAD.encode(b"z"),
                    }
                }
                4 => {
                    if let SingletonState::Pending { source, .. } = &mut p.singleton_state {
                        *source = None;
                    }
                }
                5 => {
                    if let SingletonState::Pending {
                        source: Some(w), ..
                    } = &mut p.singleton_state
                    {
                        w.value_sha256 = prefixed_sha256(b"forged");
                    }
                }
                6 => {
                    if let SingletonState::Pending {
                        source: Some(w), ..
                    } = &mut p.singleton_state
                    {
                        w.generation += 1;
                    }
                }
                7 => {
                    if let SingletonState::Pending { key_b64url, .. } = &mut p.singleton_state {
                        *key_b64url = URL_SAFE_NO_PAD.encode(b"skip");
                    }
                }
                _ => {}
            }
            Ok(p)
        })
        .unwrap();
        if case == 8 {
            assert!(matches!(
                selected.progress.value().singleton_state,
                SingletonState::Pending {
                    phase: SingletonPhase::Emit,
                    ..
                }
            ));
        }
        let error = match super::super::singleton::prepare(
            &mut io, &mut route, &owned, &expected, &selected,
        )
        .await
        {
            Ok(_) => panic!("accepted singleton case {case}"),
            Err(e) => e,
        };
        println!("singleton refusal {case}: {error}");
        assert_eq!(io.writing_evidence(), (0, 0));
        assert_eq!(io.allocation_underestimates(), 0);
        let before = io.reading_evidence();
        assert!(
            super::super::singleton::prepare(&mut io, &mut route, &owned, &expected, &selected)
                .await
                .is_err()
        );
        assert_eq!(io.reading_evidence(), before);
    }
}

#[tokio::test]
async fn singleton_comparison_resumes_after_an_authenticated_standard_boundary() {
    let (store, plan, digest) = fixture(
        &[
            vec![row(b"a", Some(b"same"), 6)],
            vec![row(b"z", Some(&vec![1; 1024 * 1024]), 6)],
        ],
        &[
            vec![row(b"a", Some(b"same"), 9)],
            vec![row(b"z", Some(&vec![2; 1024 * 1024]), 9)],
        ],
        false,
    )
    .await;
    advance_once(&store, &plan, &digest).await;
    for ordinal in 2..=4 {
        let p = singleton_test_step(&store, &plan, &digest).await;
        assert_eq!(p.next_ordinal, ordinal);
        assert_eq!(
            p.cursor.global,
            GlobalCut::After {
                key_b64url: URL_SAFE_NO_PAD.encode(b"a")
            }
        );
        assert!(matches!(p.cursor.source, SideCursor::After { .. }));
        assert!(matches!(p.cursor.current, SideCursor::After { .. }));
    }
}

#[tokio::test]
async fn singleton_comparison_cancellation_releases_every_await() {
    use std::sync::atomic::Ordering;
    for phase in 0..3 {
        let mut boundaries = 0;
        for at in 0..100 {
            if at > 0 && at > boundaries {
                break;
            }
            let (store, remaining) = physical::restore_io::window_pending_store();
            let store = store.with_durable_authority_binding(
                crate::state_store::DurableAuthorityBinding::new([39; 32]),
            );
            let (_, plan) = super::super::super::tests::inspection_fixture().await;
            let (store, plan, digest) = fixture_on_store(
                store,
                plan,
                &[vec![row(b"key", Some(&vec![1; 1024 * 1024]), 6)]],
                &[vec![row(b"key", Some(&vec![2; 1024 * 1024]), 9)]],
                false,
            )
            .await;
            for _ in 0..phase {
                singleton_test_step(&store, &plan, &digest).await;
            }
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
                Ok(OwnedSelectedPlan {
                    plan,
                    plan_sha256: digest,
                })
            })
            .unwrap();
            let expected =
                super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
            let selected = read_selected_progress(&mut io, &mut route, &expected)
                .await
                .unwrap();
            let baseline = io.live_ownership_evidence();
            remaining.store(if at == 0 { usize::MAX } else { at }, Ordering::SeqCst);
            if at == 0 {
                let unit = super::super::singleton::prepare(
                    &mut io, &mut route, &owned, &expected, &selected,
                )
                .await
                .unwrap();
                drop(unit);
                boundaries = usize::MAX - remaining.load(Ordering::SeqCst);
                assert!(boundaries > 9 && boundaries < 100);
            } else {
                let mut pending = Box::pin(super::super::singleton::prepare(
                    &mut io, &mut route, &owned, &expected, &selected,
                ));
                assert!(size_of_val(pending.as_ref().get_ref()) <= 64 * 1024);
                assert!(
                    matches!(futures::poll!(pending.as_mut()), std::task::Poll::Pending),
                    "phase {phase} await {at}"
                );
                drop(pending);
                assert_eq!(remaining.load(Ordering::SeqCst), 0);
                assert_eq!(
                    io.live_ownership_evidence(),
                    baseline,
                    "phase {phase} await {at}"
                );
                let reads = io.reading_evidence();
                assert!(
                    super::super::singleton::prepare(
                        &mut io, &mut route, &owned, &expected, &selected
                    )
                    .await
                    .is_err()
                );
                assert_eq!(io.reading_evidence(), reads);
            }
            assert_eq!(io.writing_evidence(), (0, 0));
            assert_eq!(io.allocation_underestimates(), 0);
        }
        println!("singleton phase {phase}: {boundaries} canceled physical awaits");
    }
}

#[tokio::test]
async fn singleton_comparison_publication_reconciles_competitors_and_immutable_collision() {
    for case in 0..3 {
        let (store, plan, digest) = fixture(
            &[vec![row(b"key", Some(&vec![1; 1024 * 1024]), 6)]],
            &[vec![row(b"key", Some(&vec![2; 1024 * 1024]), 9)]],
            false,
        )
        .await;
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan: plan.clone(),
                plan_sha256: digest.clone(),
            })
        })
        .unwrap();
        let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .unwrap();
        let unit =
            super::super::singleton::prepare(&mut io, &mut route, &owned, &expected, &selected)
                .await
                .unwrap();
        let target = unit.recovery_target();
        if case < 2 {
            for _ in 0..=case {
                singleton_test_step(&store, &plan, &digest).await;
            }
        } else {
            store
                .retention
                .put_raw(
                    &expected.value().receipt_path(0),
                    bytes::Bytes::from_static(b"immutable collision"),
                    arco_core::WritePrecondition::DoesNotExist,
                )
                .await
                .unwrap();
        }
        let result = publication::publish(&mut io, &mut route, unit).await;
        match case {
            0 => assert_eq!(result.unwrap(), publication::Publication::ExactSelected),
            1 => assert_eq!(
                result.unwrap(),
                publication::Publication::Conflict(publication::Reconciliation::Different)
            ),
            _ => assert!(result.is_err()),
        }
        assert_eq!(io.allocation_underestimates(), 0);
        drop(selected);
        drop(expected);
        drop(owned);
        drop(io);
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let expected = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            ExpectedPlan::from_selected(&plan, &digest)
        })
        .unwrap();
        assert_eq!(
            publication::reconcile(&mut io, &mut route, &expected, target)
                .await
                .unwrap(),
            match case {
                0 => publication::Reconciliation::Exact,
                1 => publication::Reconciliation::Different,
                _ => publication::Reconciliation::Prior,
            }
        );
        let next = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .unwrap();
        assert_eq!(
            next.progress.value().next_ordinal,
            if case == 2 { 0 } else { case + 1 }
        );
    }
}

#[tokio::test]
async fn singleton_comparison_rejects_foreign_owner_final_route_and_current_tampering() {
    use super::super::SingletonState;
    use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
    for case in 0..3 {
        let (store, plan, digest) =
            fixture(&[], &[vec![row(b"key", Some(b"current"), 9)]], false).await;
        singleton_test_step(&store, &plan, &digest).await;
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut other = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan: plan.clone(),
                plan_sha256: digest.clone(),
            })
        })
        .unwrap();
        let expected = super::super::admitted_expected_plan(&mut io, &mut route, &owned).unwrap();
        let mut selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .unwrap();
        if case == 0 || case == 2 {
            selected.progress = decode_with_reservation(
                if case == 0 { &mut other } else { &mut io },
                &mut route,
                Some(1024 * 1024),
                || {
                    let mut p = selected.progress.value().clone();
                    if case == 2 {
                        if let SingletonState::Pending {
                            current: Some(w), ..
                        } = &mut p.singleton_state
                        {
                            w.value_sha256 = prefixed_sha256(b"forged");
                        }
                    }
                    Ok(p)
                },
            )
            .unwrap();
        }
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).unwrap();
        let mut final_route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        let before = io.reading_evidence();
        let result = super::super::singleton::prepare(
            &mut io,
            if case == 1 {
                &mut final_route
            } else {
                &mut route
            },
            &owned,
            &expected,
            &selected,
        )
        .await;
        assert!(result.is_err(), "case {case}");
        if case < 2 {
            assert_eq!(io.reading_evidence(), before);
        }
        assert_eq!(io.writing_evidence(), (0, 0));
        assert_eq!(io.allocation_underestimates(), 0);
    }
}
