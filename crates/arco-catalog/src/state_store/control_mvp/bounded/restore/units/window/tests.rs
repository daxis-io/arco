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
    let (store, mut plan) = super::super::super::tests::inspection_fixture().await;
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
async fn standard_window_multiunit_matches_independent_oracle() {
    for absent in [false, true] {
        for case in 0..12 {
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
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &io).unwrap();
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
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &io).unwrap();
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
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &io).unwrap();
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
    let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &io).expect("chunk");
    let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
    assert!(
        prepare(&mut io, &mut route, &owned, &expected, &selected)
            .await
            .is_err()
    );
    assert_eq!(io.reading_evidence(), reads, "no new merge selection");
    assert_eq!(io.writing_evidence(), (0, 0));
}
