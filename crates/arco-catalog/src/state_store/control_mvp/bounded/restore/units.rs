//! Plan7 unit records and bounded row merging. Records alone establish no coverage or publication authority.
use super::super::super::{
    CatalogError, ControlMvpPaths, ControlMvpSegmentLevel, ControlMvpSegmentRow, MAX_BLOCK_BYTES,
    Result, SEGMENT_RECORD_KV, invariant_violation, physical, prefixed_sha256, read_cache,
};
use super::{ControlMvpRestorePlanV7, ImmutableObjectWitness, binary, jcs, raw_digest, tagged};
use crate::state_store::RestoreAttemptIdentity;
use physical::restore_io::{
    RestoreControlRecord, RestorePhysicalIo, RestorePhysicalRoute, WorkingValue,
    decode_with_reservation,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

mod admission;
mod bootstrap;
mod codec;
mod coverage;
mod decode;
mod driver;
mod history;
mod prefix;
mod publication;
mod singleton;
mod window;
pub(super) use driver::advance;

// Read-only local selection authentication; Ready cannot settle an uncertain
// workspace epoch and does not attest the unseen receipt prefix.
pub(super) async fn inspect_progress(
    store: &super::super::super::ControlMvpStateStore,
    plan: &ControlMvpRestorePlanV7,
    digest: &str,
    workspace: &mut crate::workspace_io_budget::WorkspaceIoBudget,
) -> Result<()> {
    let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 64 * 1024 * 1024);
    let mut payload = physical::restore_io::UnitPayloadAdmission::new();
    let mut route = RestorePhysicalRoute::OrdinaryUnit {
        workspace,
        payload: &mut payload,
    };
    let owned = decode_with_reservation(
        &mut io,
        &mut route,
        selected_plan_copy_reservation(plan),
        || {
            raw_digest(digest)?;
            Ok(OwnedSelectedPlan {
                plan: plan.clone(),
                plan_sha256: digest.to_owned(),
            })
        },
    )?;
    let expected = admitted_expected_plan(&mut io, &mut route, &owned)?;
    drop(read_selected_progress(&mut io, &mut route, &expected).await?);
    Ok(())
}

/// Owned selection provenance only; native use still requires store/root validation.
pub(super) struct OwnedSelectedPlan {
    plan: ControlMvpRestorePlanV7,
    plan_sha256: String,
}

pub(super) fn own_selected_plan(
    io: &mut RestorePhysicalIo<'_>,
    selection: &mut crate::workspace_restore::RestoreUnitSelection<'_>,
    payload: &mut physical::restore_io::UnitPayloadAdmission,
) -> Result<WorkingValue<OwnedSelectedPlan>> {
    let (selected, plan_sha256, workspace) = selection.parts();
    let mut route = RestorePhysicalRoute::OrdinaryUnit { workspace, payload };
    let result = (|| {
        let reservation = decode_with_reservation(io, &mut route, Some(64 * 1024), || {
            let crate::state_store::PersistedRestoreParticipantPlan::ControlMvpV7(plan) = selected
            else {
                return Err(invariant_violation("selected restore plan is not V7"));
            };
            raw_digest(plan_sha256)?;
            if usize::BITS != 64 || size_of::<OwnedSelectedPlan>() > 64 * 1024 {
                return Err(CatalogError::MaintenanceBackpressure {
                    message: "selected plan copy layout is unqualified".into(),
                });
            }
            selected_plan_copy_reservation(plan).ok_or_else(|| {
                CatalogError::MaintenanceBackpressure {
                    message: "selected plan copy reservation overflow".into(),
                }
            })
        })?;
        let bytes = *reservation.value();
        drop(reservation);
        decode_with_reservation(io, &mut route, Some(bytes), || {
            let crate::state_store::PersistedRestoreParticipantPlan::ControlMvpV7(plan) = selected
            else {
                return Err(invariant_violation("selected restore plan is not V7"));
            };
            Ok(OwnedSelectedPlan {
                plan: plan.as_ref().clone(),
                plan_sha256: plan_sha256.to_owned(),
            })
        })
    })();
    if result.is_err() {
        io.stop(&mut route);
    }
    result
}

// Every owned leaf in the closed Plan7 graph is a String. The selected enum
// digest has 71 bytes; no enum Box is copied. Keep this census source-qualified.
fn selected_plan_copy_reservation(plan: &ControlMvpRestorePlanV7) -> Option<usize> {
    let f = &plan.fields;
    let source = &f.source;
    let strings: [&str; 32] = [
        &f.record_type,
        &f.implementation,
        f.scope.tenant_id(),
        f.scope.workspace_id()?,
        f.scope.domain(),
        f.identity.restore_id(),
        f.identity.domain(),
        source.implementation(),
        source.scope().tenant_id(),
        source.scope().workspace_id()?,
        source.scope().domain(),
        source.manifest_id(),
        source.manifest_path(),
        source.manifest_sha256(),
        &f.source_reference_sha256,
        &f.source_manifest.path,
        &f.source_manifest.sha256,
        &f.source_kv_root_b64,
        &f.source_active_id_root_b64,
        &f.source_delivery_order_root_b64,
        &f.source_history_sha256,
        &f.workspace_request_sha256,
        &f.base_history_sha256,
        &f.base_kv_root_b64,
        &f.base_active_id_root_b64,
        &f.base_delivery_order_root_b64,
        &f.restore_request_digest,
        &f.logical_commit_id,
        &f.restore_notice_intent_id,
        &f.restore_notice_payload_b64,
        &f.candidate_seed_sha256,
        &f.candidate_id,
    ];
    let target: [&str; 6] = match &f.target {
        super::Target::Present {
            current_pointer_path,
            current_pointer_raw_b64,
            current_pointer_sha256,
            current_pointer_version,
            manifest,
            ..
        } => [
            current_pointer_path,
            current_pointer_raw_b64,
            current_pointer_sha256,
            current_pointer_version,
            &manifest.path,
            &manifest.sha256,
        ],
        super::Target::Absent {
            current_pointer_path,
            absence_marker,
            ..
        } => [current_pointer_path, absence_marker, "", "", "", ""],
    };
    strings
        .into_iter()
        .chain(target)
        .chain(source.checkpoint_path())
        .chain(source.checkpoint_sha256())
        .try_fold(64_usize * 1024 + 71, |total, value| {
            total.checked_add(value.len())
        })
}

const UNIT_RECORD_BYTES: usize = 4 * 1024 * 1024;
const RECEIPT_PROBE_BYTES: usize = UNIT_RECORD_BYTES + 1;
const DIRECTORY_PATH_DEPTH: usize = 8;
const STANDARD_COMBINED_LEAF_LIMIT: u64 = 16;
const STANDARD_DECODE_LIMIT: u64 = 64;
const STANDARD_INPUT_BYTE_LIMIT: u64 = 64 * 1024 * 1024;
const STANDARD_OUTPUT_BLOCK_LIMIT: u64 = 32;
const STANDARD_PACKED_OUTPUT_BYTE_LIMIT: u64 = 32 * 1024 * 1024;
const SINGLETON_INPUT_BYTE_LIMIT: u64 = 64 * 1024 * 1024;
const SINGLETON_SEGMENT_BYTE_LIMIT: u64 = 64 * 1024 * 1024;
const SINGLETON_SCRATCH_BASE_BYTES: u64 = 64 * 1024 * 1024;
const SINGLETON_SCRATCH_PAYLOAD_MULTIPLIER: u64 = 24;

#[allow(
    clippy::too_many_arguments,
    reason = "bind both Plan7 input sequences and the unit ordinal explicitly"
)]
fn merge_standard_restore_rows(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    source: &WorkingValue<Vec<ControlMvpSegmentRow>>,
    current: &WorkingValue<Vec<ControlMvpSegmentRow>>,
    mode: super::Mode,
    source_sequence: u64,
    base_sequence: u64,
    start_ordinal: u64,
    block_sequences: (u64, u64),
) -> Result<WorkingValue<Vec<ControlMvpSegmentRow>>> {
    let same_owner = source.is_owned_by(io) && current.is_owned_by(io);
    let reservation =
        standard_kv_merge_reservation(source.value(), current.value()).unwrap_or(64 * 1024);
    let result = decode_with_reservation(io, route, Some(reservation), || {
        if !same_owner {
            return Err(invariant_violation("restore merge input ownership differs"));
        }
        merge_standard_kv_rows(
            source.value(),
            current.value(),
            mode,
            source_sequence,
            base_sequence,
            start_ordinal,
            block_sequences,
        )
    });
    if result.is_err() {
        io.stop(route);
    }
    result
}

fn standard_kv_merge_reservation(
    source: &[ControlMvpSegmentRow],
    current: &[ControlMvpSegmentRow],
) -> Option<usize> {
    if usize::BITS != 64
        || size_of::<ControlMvpSegmentRow>() > 96
        || source.len() > MAX_BLOCK_BYTES / 41
        || current.len() > MAX_BLOCK_BYTES / 41
    {
        return None;
    }
    let bytes = source.iter().chain(current).try_fold(0_usize, |sum, row| {
        sum.checked_add(row.key.len())
            .and_then(|sum| sum.checked_add(row.value.as_ref().map_or(0, Vec::len)))
            .filter(|sum| *sum <= 2 * MAX_BLOCK_BYTES)
    })?;
    source
        .len()
        .checked_add(current.len())
        .and_then(|rows| rows.checked_mul(size_of::<ControlMvpSegmentRow>()))
        .and_then(|rows| rows.checked_add(bytes))
        .and_then(|bytes| bytes.checked_add(64 * 1024))
}

// Called inside the shared allocation reservation; the caller proves the exact
// interval and keeps both authenticated input guards alive through this copy.
fn merge_standard_kv_rows(
    source: &[ControlMvpSegmentRow],
    current: &[ControlMvpSegmentRow],
    mode: super::Mode,
    source_sequence: u64,
    base_sequence: u64,
    start_ordinal: u64,
    block_sequences: (u64, u64),
) -> Result<Vec<ControlMvpSegmentRow>> {
    standard_kv_merge_reservation(source, current).ok_or_else(|| {
        CatalogError::MaintenanceBackpressure {
            message: "standard restore KV merge exceeds allocation admission".into(),
        }
    })?;
    let result_sequence = base_sequence
        .checked_add(1)
        .ok_or_else(|| invariant_violation("restore merge result sequence overflow"))?;
    if mode == super::Mode::Absent && (!current.is_empty() || base_sequence != source_sequence) {
        return Err(invariant_violation(
            "invalid restore merge mode or result sequence",
        ));
    }
    for (rows, sequence, bound) in [
        (source, block_sequences.0, source_sequence),
        (current, block_sequences.1, base_sequence),
    ] {
        if rows.is_empty() {
            continue;
        }
        if sequence == 0 || sequence > bound {
            return Err(invariant_violation(
                "restore merge block sequence exceeds its manifest",
            ));
        }
        read_cache::validate_rows_for_sequence(ControlMvpSegmentLevel::L1, sequence, rows)?;
        if rows.iter().any(|row| row.record_kind != SEGMENT_RECORD_KV)
            || rows.windows(2).any(|pair| matches!(pair, [left, right]
                if left.key >= right.key || left.logical_ordinal.checked_add(1) != Some(right.logical_ordinal)))
        {
            return Err(invariant_violation("restore merge KV ordering or ordinals are invalid"));
        }
    }
    let mut output = Vec::with_capacity(source.len() + current.len());
    let mut source = source.iter().peekable();
    let mut current = current.iter().peekable();
    loop {
        let (source_row, current_row) = match (source.peek(), current.peek()) {
            (None, None) => break,
            (Some(_), None) => (source.next(), None),
            (None, Some(_)) => (None, current.next()),
            (Some(left), Some(right)) => match left.key.cmp(&right.key) {
                std::cmp::Ordering::Less => (source.next(), None),
                std::cmp::Ordering::Greater => (None, current.next()),
                std::cmp::Ordering::Equal => (source.next(), current.next()),
            },
        };
        let (selected, generation, visible) =
            if let Some(row) = source_row.filter(|row| !row.tombstone) {
                let generation = current_row
                    .filter(|current| current.value == row.value)
                    .map_or(result_sequence, |current| current.generation);
                (row, generation, true)
            } else if let Some(row) = current_row {
                (
                    row,
                    if row.tombstone {
                        row.generation
                    } else {
                        result_sequence
                    },
                    false,
                )
            } else if let Some(row) = source_row.filter(|_| mode == super::Mode::Absent) {
                (row, row.generation, false)
            } else {
                continue;
            };
        let logical_ordinal = u64::try_from(output.len())
            .ok()
            .and_then(|offset| start_ordinal.checked_add(offset))
            .ok_or_else(|| invariant_violation("restore merge output ordinal overflow"))?;
        output.push(ControlMvpSegmentRow {
            record_kind: SEGMENT_RECORD_KV,
            key: selected.key.clone(),
            value: if visible {
                selected.value.clone()
            } else {
                None
            },
            generation,
            tombstone: !visible,
            logical_sequence: result_sequence,
            logical_ordinal,
            origin_sequence: None,
            expires_at_ms: None,
        });
    }
    Ok(output)
}

#[cfg(test)]
mod kv_merge_tests {
    use super::super::super::super::{ControlMvpSegmentRow, SEGMENT_RECORD_KV};
    use super::*;
    use crate::workspace_io_budget::WorkspaceIoBudget;
    use physical::restore_io::{UnitPayloadAdmission, decode_owned};
    use std::collections::BTreeMap;

    fn store() -> super::super::super::super::ControlMvpStateStore {
        use super::super::super::super::{ControlMvpStateStore, StateScope};
        ControlMvpStateStore::new_synthetic_bounded(
            arco_core::ScopedStorage::new(
                std::sync::Arc::new(arco_core::MemoryBackend::new()),
                "tenant",
                "workspace",
            )
            .expect("scope"),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store")
    }

    #[test]
    fn standard_kv_merge_final_carry_is_not_available_for_allocation() {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        let store = store();
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut ordinary = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let source =
            decode_with_reservation(&mut io, &mut ordinary, Some(1024), || Ok(input(6, 2)))
                .expect("source");
        let current =
            decode_with_reservation(&mut io, &mut ordinary, Some(1024), || Ok(Vec::new()))
                .expect("current");
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 64 * 1024 * 1024 - 4096, &mut io)
            .expect("initial carry");
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        assert!(
            merge_standard_restore_rows(
                &mut io,
                &mut route,
                &source,
                &current,
                super::super::Mode::Present,
                3,
                5,
                0,
                (3, 5)
            )
            .is_err()
        );
        assert!(
            decode_owned(&mut io, &mut route, || -> Result<()> {
                panic!("stopped final merge")
            })
            .is_err()
        );
    }

    #[tokio::test]
    async fn standard_kv_merge_owned_rows_reach_exact_output_pipeline() {
        use physical::restore_io::{
            FinalMicrochunk, FinalStreamTotals, write_standard_restore_output,
        };
        let store = store();
        {
            let final_route = false; // Control transport is ordinary-only; rejection has separate coverage.
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = if final_route {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let source =
                decode_with_reservation(&mut io, &mut route, Some(1024), || Ok(input(6, 2)))
                    .expect("source");
            let current =
                decode_with_reservation(&mut io, &mut route, Some(1024), || Ok(input(21, 4)))
                    .expect("current");
            let rows = merge_standard_restore_rows(
                &mut io,
                &mut route,
                &source,
                &current,
                super::super::Mode::Present,
                3,
                5,
                17,
                (3, 5),
            )
            .expect("merge");
            assert_eq!(
                rows.value(),
                &oracle(source.value(), current.value(), super::super::Mode::Present)
            );
            drop(source);
            drop(current);
            let output =
                write_standard_restore_output(&mut io, &mut route, &"a".repeat(64), 6, &rows)
                    .await
                    .expect("write exact merge");
            assert_eq!(output.descriptor.value().block.row_count, 3);
            drop(output);
            drop(rows);
        }
    }

    #[test]
    fn standard_kv_merge_allocation_bound_and_exact_caps() {
        const BLOCK: usize = 256 * 1024;
        let dense = |offset: u64| {
            (0..BLOCK / 41)
                .map(|i| {
                    let mut row = row(0, 1, 2, u64::try_from(i).expect("ordinal"));
                    row.key = (offset + u64::try_from(i).expect("key"))
                        .to_be_bytes()
                        .to_vec();
                    row
                })
                .collect::<Vec<_>>()
        };
        let mut long = row(0, 2, 2, 0);
        long.key.clear();
        long.value = Some(vec![255; 2 * BLOCK]);
        for (source, current) in [
            (dense(0), dense(10_000)),
            (vec![long.clone()], Vec::new()),
            (input(63, 2), Vec::new()),
        ] {
            let reservation = standard_kv_merge_reservation(&source, &current).expect("bound");
            let mut output = None;
            let measured = allocation_counter::measure(|| {
                output = Some(
                    merge_standard_kv_rows(
                        &source,
                        &current,
                        super::super::Mode::Present,
                        3,
                        3,
                        0,
                        (3, 3),
                    )
                    .expect("merge"),
                );
            });
            assert!(usize::try_from(measured.bytes_total).expect("allocated") <= reservation);
            println!(
                "standard KV merge source={} current={} output={} reserved={} allocated={} peak={}",
                source.len(),
                current.len(),
                output.as_ref().expect("output").len(),
                reservation,
                measured.bytes_total,
                measured.bytes_max
            );
        }
        long.value.as_mut().expect("value").push(0);
        assert!(standard_kv_merge_reservation(&[long], &[]).is_none());
        let mut too_many = dense(0);
        too_many.push(row(0, 1, 2, 0));
        assert!(standard_kv_merge_reservation(&too_many, &[]).is_none());
        let one = [row(0, 1, 2, 0)];
        assert_eq!(
            merge_standard_kv_rows(
                &one,
                &[],
                super::super::Mode::Present,
                3,
                0,
                u64::MAX,
                (3, 0)
            )
            .expect("last ordinal")
            .first()
            .expect("output")
            .logical_ordinal,
            u64::MAX
        );
    }

    #[test]
    fn standard_kv_merge_requires_reservation_and_same_input_owner() {
        use super::super::super::super::{ControlMvpStateStore, StateScope};
        let store = ControlMvpStateStore::new_synthetic_bounded(
            arco_core::ScopedStorage::new(
                std::sync::Arc::new(arco_core::MemoryBackend::new()),
                "tenant",
                "workspace",
            )
            .expect("scope"),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store");
        let mut accepted = Vec::new();
        for foreign in [false, true] {
            let mut io =
                RestorePhysicalIo::new(&store, if foreign { 64 * 1024 * 1024 } else { 4096 }, 0);
            let mut other = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let source = decode_with_reservation(
                if foreign { &mut other } else { &mut io },
                &mut route,
                Some(1024),
                || Ok(input(6, 2)),
            )
            .expect("source ownership");
            let current =
                decode_with_reservation(&mut io, &mut route, Some(1024), || Ok(Vec::new()))
                    .expect("current ownership");
            if merge_standard_restore_rows(
                &mut io,
                &mut route,
                &source,
                &current,
                super::super::Mode::Present,
                3,
                5,
                0,
                (3, 5),
            )
            .is_ok()
            {
                accepted.push(foreign);
            } else {
                assert!(
                    decode_owned(&mut io, &mut route, || -> Result<()> {
                        panic!("stopped merge cannot allocate")
                    })
                    .is_err()
                );
            }
        }
        assert!(
            accepted.is_empty(),
            "merge bypassed reservation or owner check: {accepted:?}"
        );
    }

    fn row(key: u8, state: u8, generation: u64, ordinal: u64) -> ControlMvpSegmentRow {
        ControlMvpSegmentRow {
            record_kind: SEGMENT_RECORD_KV,
            key: vec![0, key, 255],
            value: match state {
                1 => Some(vec![]),
                2 => Some(vec![255, 0, key]),
                _ => None,
            },
            generation,
            tombstone: state == 3,
            logical_sequence: generation + 1,
            logical_ordinal: ordinal,
            origin_sequence: None,
        }
    }

    fn input(mut code: u32, generation: u64) -> Vec<ControlMvpSegmentRow> {
        let mut rows = Vec::new();
        for key in 0..3 {
            let state = u8::try_from(code % 4).expect("small state");
            code /= 4;
            if state != 0 {
                rows.push(row(
                    key,
                    state,
                    generation,
                    u64::try_from(rows.len()).expect("ordinal"),
                ));
            }
        }
        rows
    }

    // Independent map oracle: iterate the union of keys, rather than reproducing
    // the production two-cursor merge. Every missing/value/tombstone combination
    // appears at the beginning, middle and end of an interval.
    fn oracle(
        source: &[ControlMvpSegmentRow],
        current: &[ControlMvpSegmentRow],
        mode: super::super::Mode,
    ) -> Vec<ControlMvpSegmentRow> {
        let result_sequence = if mode == super::super::Mode::Present {
            6
        } else {
            4
        };
        let source: BTreeMap<_, _> = source.iter().map(|r| (r.key.clone(), r)).collect();
        let current: BTreeMap<_, _> = current.iter().map(|r| (r.key.clone(), r)).collect();
        let mut keys: Vec<_> = source.keys().chain(current.keys()).cloned().collect();
        keys.sort();
        keys.dedup();
        let mut output = Vec::new();
        for key in keys {
            let source = source.get(&key);
            let current = current.get(&key);
            let value = source.and_then(|r| r.value.clone());
            let generation = if value.is_some() {
                current
                    .filter(|r| r.value == value)
                    .map_or(result_sequence, |r| r.generation)
            } else if let Some(current) = current {
                if current.tombstone {
                    current.generation
                } else {
                    result_sequence
                }
            } else if mode == super::super::Mode::Absent {
                source.expect("union key").generation
            } else {
                continue;
            };
            output.push(ControlMvpSegmentRow {
                record_kind: SEGMENT_RECORD_KV,
                key,
                tombstone: value.is_none(),
                value,
                generation,
                logical_sequence: result_sequence,
                logical_ordinal: 17 + u64::try_from(output.len()).expect("ordinal"),
                origin_sequence: None,
            });
        }
        output
    }

    #[test]
    fn standard_kv_merge_matches_independent_present_and_absent_oracle() {
        for source_code in 0..64 {
            let source = input(source_code, 2);
            for current_code in 0..64 {
                let current = input(current_code, 4);
                let actual = merge_standard_kv_rows(
                    &source,
                    &current,
                    super::super::Mode::Present,
                    3,
                    5,
                    17,
                    (3, 5),
                )
                .expect("present merge");
                assert_eq!(
                    actual,
                    oracle(&source, &current, super::super::Mode::Present)
                );
            }
            let actual =
                merge_standard_kv_rows(&source, &[], super::super::Mode::Absent, 3, 3, 17, (3, 3))
                    .expect("absent merge");
            assert_eq!(actual, oracle(&source, &[], super::super::Mode::Absent));
        }
    }

    #[test]
    fn standard_kv_merge_accepts_present_source_newer_than_result() {
        let source = input(6, 9);
        let current = input(1, 4);
        let output = merge_standard_kv_rows(
            &source,
            &current,
            super::super::Mode::Present,
            10,
            5,
            0,
            (10, 5),
        )
        .expect("present source may be newer than result");
        assert_eq!(output.len(), 2);
        assert!(
            output
                .iter()
                .all(|row| row.logical_sequence == 6 && row.generation == 6)
        );
    }

    #[test]
    fn standard_kv_merge_accepts_older_authenticated_blocks() {
        let source = input(6, 2);
        let current = input(21, 2);
        let output = merge_standard_kv_rows(
            &source,
            &current,
            super::super::Mode::Present,
            10,
            5,
            17,
            (3, 3),
        )
        .expect("path-copy keeps older blocks");
        assert_eq!(
            output,
            oracle(&source, &current, super::super::Mode::Present)
        );
        for sequences in [(0, 3), (4, 3), (3, 0), (3, 4)] {
            assert!(
                merge_standard_kv_rows(
                    &source,
                    &current,
                    super::super::Mode::Present,
                    10,
                    5,
                    17,
                    sequences
                )
                .is_err()
            );
        }
        for (source_bound, current_bound) in [(2, 5), (10, 2)] {
            assert!(
                merge_standard_kv_rows(
                    &source,
                    &current,
                    super::super::Mode::Present,
                    source_bound,
                    current_bound,
                    17,
                    (3, 3)
                )
                .is_err()
            );
        }
    }

    #[cfg(feature = "test-utils")]
    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "keep real commits and authenticated reads on both routes in one regression"
    )]
    async fn standard_kv_merge_reads_block_reused_by_real_commits() {
        use super::super::super::super::{SyntheticKvEntry, directory};
        use crate::state_store::{ArcoStateTxn, TxnOptions};
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals, read_restore_leaf};
        let store = store();
        store
            .install_synthetic_genesis(
                "merge-old-block",
                3,
                512,
                (0..512).map(|i| SyntheticKvEntry {
                    key: format!("key-{i:08}").into_bytes(),
                    generation: 2,
                    value: Some(vec![255, 0]),
                }),
                0,
                std::iter::empty(),
            )
            .await
            .expect("fixture genesis");
        let source_base = store.pin_bounded_base().await.expect("source root");
        for sequence in [4, 5] {
            let mut txn = store
                .begin_control_txn(TxnOptions::default())
                .await
                .expect("txn");
            txn.set_logical_operation(
                &format!("merge-commit-{sequence}"),
                "test",
                &"a1".repeat(32),
            )
            .expect("operation");
            txn.put(
                b"key-00000511",
                bytes::Bytes::from(format!("value-{sequence}")),
            )
            .await
            .expect("write distant block");
            assert_eq!(
                txn.commit_v2()
                    .await
                    .expect("commit")
                    .token()
                    .logical_sequence(),
                sequence
            );
        }
        let current_base = store.pin_bounded_base().await.expect("current root");
        for final_route in [false, true] {
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = if final_route {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let source_position =
                directory::restore::first_after(&mut io, &mut route, &source_base.kv_root, None)
                    .await
                    .expect("source membership");
            let current_position =
                directory::restore::first_after(&mut io, &mut route, &current_base.kv_root, None)
                    .await
                    .expect("current membership");
            assert_eq!(
                source_position
                    .value()
                    .as_ref()
                    .expect("source leaf")
                    .leaf
                    .digest,
                current_position
                    .value()
                    .as_ref()
                    .expect("current leaf")
                    .leaf
                    .digest
            );
            let (source_descriptor, source) = read_restore_leaf(
                &mut io,
                &mut route,
                physical::Role::Kv,
                &source_position.value().as_ref().expect("source leaf").leaf,
            )
            .await
            .expect("source rows");
            let (current_descriptor, current) = read_restore_leaf(
                &mut io,
                &mut route,
                physical::Role::Kv,
                &current_position
                    .value()
                    .as_ref()
                    .expect("current leaf")
                    .leaf,
            )
            .await
            .expect("current rows");
            let sequences = (
                source_descriptor.value().segment.logical_sequence,
                current_descriptor.value().segment.logical_sequence,
            );
            assert_eq!(sequences, (3, 3));
            let output = merge_standard_restore_rows(
                &mut io,
                &mut route,
                &source,
                &current,
                super::super::Mode::Present,
                source_base.logical_sequence(),
                current_base.logical_sequence(),
                0,
                sequences,
            )
            .expect("reused block merge");
            assert_eq!(output.value().len(), 256);
            assert!(
                output
                    .value()
                    .iter()
                    .all(|row| row.generation == 2 && row.logical_sequence == 6)
            );
        }
    }

    #[test]
    fn standard_kv_merge_preflight_errors_retain_owned_allocation() {
        let store = store();
        for foreign in [false, true] {
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut other = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let source = decode_with_reservation(
                if foreign { &mut other } else { &mut io },
                &mut route,
                Some(1024 * 1024),
                || {
                    let mut rows = input(6, 2);
                    if !foreign {
                        rows.first_mut().expect("row").value =
                            Some(vec![0; 2 * MAX_BLOCK_BYTES + 1]);
                    }
                    Ok(rows)
                },
            )
            .expect("source");
            let current =
                decode_with_reservation(&mut io, &mut route, Some(1024), || Ok(Vec::new()))
                    .expect("current");
            let before = io.allocation_evidence();
            let error = merge_standard_restore_rows(
                &mut io,
                &mut route,
                &source,
                &current,
                super::super::Mode::Present,
                3,
                5,
                0,
                (3, 5),
            )
            .err()
            .expect("invalid input");
            let capacity = match error {
                CatalogError::InvariantViolation { ref message }
                | CatalogError::MaintenanceBackpressure { ref message } => message.capacity(),
                _ => panic!("unexpected error"),
            };
            let after = io.allocation_evidence();
            assert!(
                after.0 - before.0 >= u64::try_from(capacity).expect("capacity"),
                "foreign={foreign}: preflight error was not measured"
            );
            assert!(
                after.1 >= before.1 + capacity,
                "failed allocation must remain owned"
            );
        }
    }

    #[test]
    fn standard_kv_merge_preserves_mixed_current_generations() {
        let mut source = input(22, 2);
        let mut current = input(53, 4);
        for (index, row) in current.iter_mut().enumerate() {
            row.generation = 1 + u64::try_from(index).expect("generation");
        }
        source.first_mut().expect("first").key.clear();
        current.first_mut().expect("first").key.clear();
        let output = merge_standard_kv_rows(
            &source,
            &current,
            super::super::Mode::Present,
            3,
            5,
            17,
            (3, 5),
        )
        .expect("mixed generations");
        assert_eq!(
            output,
            oracle(&source, &current, super::super::Mode::Present)
        );
        assert_eq!(
            output.iter().map(|row| row.generation).collect::<Vec<_>>(),
            vec![6, 2, 6]
        );
    }

    #[test]
    fn standard_kv_merge_rejects_invalid_rows_sequences_and_ordinals() {
        let valid = input(6, 2);
        for invalid in 0..11 {
            let mut rows = valid.clone();
            let first = rows.first_mut().expect("row");
            match invalid {
                0 => first.record_kind = 255,
                1 => first.generation = 0,
                2 => first.logical_sequence = 10,
                3 => first.tombstone = true,
                4 => first.origin_sequence = Some(1),
                5 => rows.reverse(),
                6 => rows.last_mut().expect("row").key = rows.first().expect("row").key.clone(),
                7 => rows.last_mut().expect("row").logical_ordinal = 8,
                8 => first.generation = 9,
                9 => first.logical_ordinal = u64::MAX,
                _ => rows.last_mut().expect("row").logical_sequence = 4,
            }
            assert!(
                merge_standard_kv_rows(&rows, &[], super::super::Mode::Present, 3, 5, 0, (3, 5))
                    .is_err(),
                "case {invalid}"
            );
            assert!(
                merge_standard_kv_rows(&[], &rows, super::super::Mode::Present, 0, 3, 0, (0, 3))
                    .is_err(),
                "current case {invalid}"
            );
        }
        assert!(
            merge_standard_kv_rows(
                &valid,
                &[],
                super::super::Mode::Present,
                3,
                u64::MAX,
                0,
                (3, u64::MAX)
            )
            .is_err()
        );
        assert!(
            merge_standard_kv_rows(
                &valid,
                &[],
                super::super::Mode::Present,
                3,
                5,
                u64::MAX,
                (3, 5)
            )
            .is_err()
        );
        assert!(
            merge_standard_kv_rows(&[], &valid, super::super::Mode::Absent, 3, 3, 0, (3, 3))
                .is_err()
        );
        assert!(
            merge_standard_kv_rows(&[], &[], super::super::Mode::Absent, 3, 5, 0, (3, 5)).is_err()
        );
        assert!(
            merge_standard_kv_rows(&valid, &[], super::super::Mode::Present, 4, 5, 0, (4, 5))
                .is_err()
        );
        assert!(
            merge_standard_kv_rows(&[], &valid, super::super::Mode::Present, 0, 5, 0, (0, 5))
                .is_err()
        );
        assert!(
            merge_standard_kv_rows(
                &[],
                &[],
                super::super::Mode::Present,
                u64::MAX,
                5,
                0,
                (u64::MAX, 5)
            )
            .expect("empty future source")
            .is_empty()
        );
    }
}

/// Selected Plan7 fields used by unit records. Construct only after the parent
/// Plan7 validator has decoded its directory roots for this store/scope.
struct ExpectedPlan<'a> {
    plan_sha256: &'a str,
    candidate_id: &'a str,
    candidate_seed_sha256: &'a str,
    identity: &'a RestoreAttemptIdentity,
    owner_generation: u64,
    source_kv_root_b64: &'a str,
    base_kv_root_b64: &'a str,
    prefix: String,
}

// The inseparable selected copy already passed authenticated Plan7 shape
// validation. This adds the configured physical store and six-root checks.
fn admitted_expected_plan<'a>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    selected: &'a WorkingValue<OwnedSelectedPlan>,
) -> Result<WorkingValue<ExpectedPlan<'a>>> {
    let same_owner = selected.is_owned_by(io);
    let store = io.store();
    let result = (|| {
        let reservation = decode_with_reservation(io, route, Some(64 * 1024), || {
            if !same_owner {
                return Err(invariant_violation(
                    "selected plan physical ownership differs",
                ));
            }
            if usize::BITS != 64 || size_of::<WorkingValue<ExpectedPlan<'_>>>() > 64 * 1024 {
                return Err(invariant_violation("expected plan layout is unqualified"));
            }
            raw_digest(&selected.value().plan_sha256)?;
            let fields = &selected.value().plan.fields;
            for root in [
                &fields.source_kv_root_b64,
                &fields.source_active_id_root_b64,
                &fields.source_delivery_order_root_b64,
                &fields.base_kv_root_b64,
                &fields.base_active_id_root_b64,
                &fields.base_delivery_order_root_b64,
            ] {
                if root.len() != 380 {
                    return Err(invariant_violation("selected directory root width differs"));
                }
            }
            expected_plan_reservation(store)
        })?;
        let bytes = *reservation.value();
        drop(reservation);
        decode_with_reservation(io, route, Some(bytes), || {
            let selected = selected.value();
            selected.plan.validate_roots(store)?;
            ExpectedPlan::from_selected(&selected.plan, &selected.plan_sha256)
        })
    })();
    if result.is_err() {
        io.stop(route);
    }
    result
}

fn expected_plan_reservation(store: &super::super::super::ControlMvpStateStore) -> Result<usize> {
    physical::restore_io::directory_scope_reservation(store)?
        .checked_add(64 * 1024 + 3990 + 512)
        .and_then(|bytes| bytes.checked_add(store.scope.domain().len().checked_mul(9)?))
        .ok_or_else(|| CatalogError::MaintenanceBackpressure {
            message: "expected plan reservation overflow".into(),
        })
}

impl<'a> ExpectedPlan<'a> {
    fn from_selected(plan: &'a ControlMvpRestorePlanV7, plan_sha256: &'a str) -> Result<Self> {
        raw_digest(plan_sha256)?;
        let fields = &plan.fields;
        let paths = ControlMvpPaths::new(fields.scope.domain());
        validate_candidate_id(&fields.candidate_id)?;
        Ok(Self {
            plan_sha256,
            candidate_id: &fields.candidate_id,
            candidate_seed_sha256: &fields.candidate_seed_sha256,
            identity: &fields.identity,
            owner_generation: fields.owner_generation,
            source_kv_root_b64: &fields.source_kv_root_b64,
            base_kv_root_b64: &fields.base_kv_root_b64,
            prefix: format!("{}/restore/v7/{}", paths.base_prefix(), fields.candidate_id),
        })
    }

    fn candidate_id(&self) -> &str {
        self.candidate_id
    }

    fn selector_path(&self) -> String {
        RestoreControlRecord::Selector.path(&self.prefix)
    }
    fn progress_path(&self, next_ordinal: u64) -> String {
        RestoreControlRecord::Progress(next_ordinal).path(&self.prefix)
    }
    fn receipt_path(&self, ordinal: u64) -> String {
        RestoreControlRecord::Receipt(ordinal).path(&self.prefix)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryPositionLeafWitness {
    first_b64url: String,
    last_b64url: String,
    rows: u64,
    bytes: u32,
    digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryPathEntry {
    page_sha256: String,
    child_index: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryPosition {
    role: physical::Role,
    root_b64: String,
    #[serde(deserialize_with = "decode::list::<_, _, 8>")]
    path: Vec<DirectoryPathEntry>,
    leaf: DirectoryPositionLeafWitness,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[serde(try_from = "decode::GlobalCutWire")]
enum GlobalCut {
    Start,
    After { key_b64url: String },
    End,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[serde(try_from = "decode::SideCursorWire")]
enum SideCursor {
    Start,
    After {
        key_b64url: String,
        position: DirectoryPosition,
    },
    End,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MergeCursor {
    global: GlobalCut,
    source: SideCursor,
    current: SideCursor,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlockWitness {
    offset: u64,
    length: u64,
    sha256: String,
    rows: u64,
    min_key_b64url: String,
    max_key_b64url: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InputWitness {
    role: physical::Role,
    root_b64: String,
    directory_leaf: DirectoryPositionLeafWitness,
    descriptor: ImmutableObjectWitness,
    index: ImmutableObjectWitness,
    block: BlockWitness,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputDirectoryLeafWitness {
    first_key_b64url: String,
    last_key_b64url: String,
    rows: u64,
    bytes: u32,
    digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputWitness {
    role: physical::Role,
    output_id: String,
    part: u32,
    directory_leaf: OutputDirectoryLeafWitness,
    descriptor: ImmutableObjectWitness,
    index: ImmutableObjectWitness,
    block: BlockWitness,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SingletonValueWitness {
    generation: u64,
    tombstone: bool,
    value_length: u64,
    value_sha256: String,
    descriptor: ImmutableObjectWitness,
    index: ImmutableObjectWitness,
    block: BlockWitness,
    row_ordinal: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SingletonPhase {
    CompareSource,
    CompareCurrent,
    Emit,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[serde(try_from = "decode::SingletonStateWire")]
enum SingletonState {
    None,
    Pending {
        key_b64url: String,
        source: Option<Box<SingletonValueWitness>>,
        current: Option<Box<SingletonValueWitness>>,
        phase: SingletonPhase,
    },
    Complete {
        key_b64url: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[serde(try_from = "decode::UnitReservationWire")]
enum UnitReservationV1 {
    Standard {
        combined_leaf_limit: u64,
        decode_limit: u64,
        input_byte_limit: u64,
        output_block_limit: u64,
        packed_output_byte_limit: u64,
    },
    Singleton {
        phase: SingletonPhase,
        authenticated_payload_bytes: u64,
        input_byte_limit: u64,
        segment_byte_limit: u64,
        scratch_base_bytes: u64,
        scratch_payload_multiplier: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SemanticCounts {
    source_leaves: u64,
    current_leaves: u64,
    input_encoded_bytes: u64,
    decoded_blocks: u64,
    decoded_rows: u64,
    output_blocks: u64,
    output_encoded_bytes: u64,
    mutations: u64,
    reservation: UnitReservationV1,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CumulativeSemanticCounts {
    source_leaves: u64,
    current_leaves: u64,
    input_encoded_bytes: u64,
    decoded_blocks: u64,
    decoded_rows: u64,
    output_blocks: u64,
    output_encoded_bytes: u64,
    mutations: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptRef {
    path: String,
    raw_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlMvpRestoreReceiptV1 {
    record_type: String,
    version: u32,
    plan_sha256: String,
    identity: RestoreAttemptIdentity,
    owner_generation: u64,
    ordinal: u64,
    predecessor_receipt_sha256: String,
    predecessor_chain_sha256: String,
    before: MergeCursor,
    after: MergeCursor,
    singleton_before: SingletonState,
    singleton_after: SingletonState,
    #[serde(deserialize_with = "decode::list::<_, _, 16>")]
    source_inputs: Vec<InputWitness>,
    #[serde(deserialize_with = "decode::list::<_, _, 16>")]
    current_inputs: Vec<InputWitness>,
    #[serde(deserialize_with = "decode::list::<_, _, 32>")]
    outputs: Vec<OutputWitness>,
    prefix_cumulative_counts: CumulativeSemanticCounts,
    counts: SemanticCounts,
    receipt_body_sha256: String,
    chain_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreProgressV1 {
    record_type: String,
    version: u32,
    plan_sha256: String,
    identity: RestoreAttemptIdentity,
    owner_generation: u64,
    next_ordinal: u64,
    receipt_count: u64,
    last_receipt: Option<ReceiptRef>,
    chain_sha256: String,
    cursor: MergeCursor,
    singleton_state: SingletonState,
    terminal: bool,
    cumulative_counts: CumulativeSemanticCounts,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreProgressSelectorV1 {
    record_type: String,
    version: u32,
    plan_sha256: String,
    identity: RestoreAttemptIdentity,
    owner_generation: u64,
    current_progress_path: String,
    current_progress_sha256: String,
}

#[derive(Serialize)]
struct OutputIdBody<'a> {
    candidate_seed_sha256: &'a str,
    ordinal: u64,
    part: u32,
    role: physical::Role,
}

#[derive(Serialize)]
struct ReceiptBody<'a> {
    record_type: &'a str,
    version: u32,
    plan_sha256: &'a str,
    identity: &'a RestoreAttemptIdentity,
    owner_generation: u64,
    ordinal: u64,
    predecessor_receipt_sha256: &'a str,
    predecessor_chain_sha256: &'a str,
    before: &'a MergeCursor,
    after: &'a MergeCursor,
    singleton_before: &'a SingletonState,
    singleton_after: &'a SingletonState,
    source_inputs: &'a [InputWitness],
    current_inputs: &'a [InputWitness],
    outputs: &'a [OutputWitness],
    prefix_cumulative_counts: &'a CumulativeSemanticCounts,
    counts: &'a SemanticCounts,
}

#[derive(Serialize)]
struct ReceiptChainBody<'a> {
    plan_sha256: &'a str,
    owner_generation: u64,
    ordinal: u64,
    predecessor_chain_sha256: &'a str,
    receipt_body_sha256: &'a str,
}

#[derive(Serialize)]
struct GenesisReceiptNoneBody<'a> {
    plan_sha256: &'a str,
    identity: &'a RestoreAttemptIdentity,
    owner_generation: u64,
}

#[derive(Serialize)]
struct GenesisChainBody<'a> {
    plan_sha256: &'a str,
    identity: &'a RestoreAttemptIdentity,
    owner_generation: u64,
    genesis_receipt_raw_sha256: &'a str,
}

/// Exact JCS record decoding. A typed parse never authorizes an alternate raw
/// spelling: the canonical encoding must equal the received immutable bytes.
fn decode_jcs_exact<T>(raw: &[u8], limit: usize, context: &str) -> Result<T>
where
    T: DeserializeOwned + Serialize,
{
    if raw.len() > limit {
        return Err(invariant_violation(format!("{context} exceeds record cap")));
    }
    let value: T = serde_json::from_slice(raw)
        .map_err(|_| invariant_violation(format!("invalid {context} JSON")))?;
    if jcs(&value)? != raw {
        return Err(invariant_violation(format!("{context} is not exact JCS")));
    }
    Ok(value)
}

fn encode_jcs_record<T: Serialize>(value: &T, context: &str) -> Result<Vec<u8>> {
    let raw = jcs(value)?;
    if raw.len() > UNIT_RECORD_BYTES {
        return Err(invariant_violation(format!("{context} exceeds record cap")));
    }
    Ok(raw)
}

fn require_path(actual: &str, expected: &str, context: &str) -> Result<()> {
    if actual != expected {
        return Err(invariant_violation(format!("{context} path differs")));
    }
    Ok(())
}

fn receipt_raw_sha256(raw_jcs: &[u8]) -> String {
    prefixed_sha256(raw_jcs)
}

fn tagged_raw_hex<T: Serialize>(tag: &[u8], body: &T) -> Result<String> {
    Ok(raw_digest(&tagged(tag, body)?)?.to_owned())
}

fn output_id(
    expected: &ExpectedPlan<'_>,
    ordinal: u64,
    part: u32,
    role: physical::Role,
) -> Result<String> {
    raw_digest(expected.candidate_seed_sha256)?;
    tagged_raw_hex(
        b"arco/control-v2/restore-output-v1",
        &OutputIdBody {
            candidate_seed_sha256: expected.candidate_seed_sha256,
            ordinal,
            part,
            role,
        },
    )
}

fn genesis_receipt_raw_sha256(expected: &ExpectedPlan<'_>) -> Result<String> {
    tagged(
        b"arco/control-v2/restore-receipt-none-v1",
        &GenesisReceiptNoneBody {
            plan_sha256: expected.plan_sha256,
            identity: expected.identity,
            owner_generation: expected.owner_generation,
        },
    )
}

fn genesis_chain_sha256(expected: &ExpectedPlan<'_>) -> Result<String> {
    let genesis = genesis_receipt_raw_sha256(expected)?;
    tagged(
        b"arco/control-v2/restore-receipt-genesis-chain-v1",
        &GenesisChainBody {
            plan_sha256: expected.plan_sha256,
            identity: expected.identity,
            owner_generation: expected.owner_generation,
            genesis_receipt_raw_sha256: &genesis,
        },
    )
}

fn receipt_body_sha256(receipt: &ControlMvpRestoreReceiptV1) -> Result<String> {
    tagged(
        b"arco/control-v2/restore-receipt-body-v1",
        &ReceiptBody {
            record_type: &receipt.record_type,
            version: receipt.version,
            plan_sha256: &receipt.plan_sha256,
            identity: &receipt.identity,
            owner_generation: receipt.owner_generation,
            ordinal: receipt.ordinal,
            predecessor_receipt_sha256: &receipt.predecessor_receipt_sha256,
            predecessor_chain_sha256: &receipt.predecessor_chain_sha256,
            before: &receipt.before,
            after: &receipt.after,
            singleton_before: &receipt.singleton_before,
            singleton_after: &receipt.singleton_after,
            source_inputs: &receipt.source_inputs,
            current_inputs: &receipt.current_inputs,
            outputs: &receipt.outputs,
            prefix_cumulative_counts: &receipt.prefix_cumulative_counts,
            counts: &receipt.counts,
        },
    )
}

fn receipt_chain_sha256(receipt: &ControlMvpRestoreReceiptV1) -> Result<String> {
    tagged(
        b"arco/control-v2/restore-receipt-chain-v1",
        &ReceiptChainBody {
            plan_sha256: &receipt.plan_sha256,
            owner_generation: receipt.owner_generation,
            ordinal: receipt.ordinal,
            predecessor_chain_sha256: &receipt.predecessor_chain_sha256,
            receipt_body_sha256: &receipt.receipt_body_sha256,
        },
    )
}

fn validate_candidate_id(value: &str) -> Result<()> {
    if !super::super::super::valid_raw_digest(value) {
        return Err(invariant_violation(
            "Plan7 candidate id is not lower-case raw SHA-256",
        ));
    }
    Ok(())
}

fn canonical_binary(value: &str, field: &str, nonempty: bool) -> Result<Vec<u8>> {
    let bytes = binary(value)?;
    if nonempty && bytes.is_empty() {
        return Err(invariant_violation(format!("{field} is empty")));
    }
    Ok(bytes)
}

fn validate_directory_leaf(leaf: &DirectoryPositionLeafWitness) -> Result<(Vec<u8>, Vec<u8>)> {
    let first = canonical_binary(&leaf.first_b64url, "directory leaf first key", false)?;
    let last = canonical_binary(&leaf.last_b64url, "directory leaf last key", false)?;
    if first > last || leaf.rows == 0 || leaf.bytes == 0 {
        return Err(invariant_violation("invalid directory leaf witness"));
    }
    raw_digest(&leaf.digest)?;
    Ok((first, last))
}

fn validate_block(block: &BlockWitness) -> Result<(Vec<u8>, Vec<u8>)> {
    let min = canonical_binary(&block.min_key_b64url, "block minimum key", false)?;
    let max = canonical_binary(&block.max_key_b64url, "block maximum key", false)?;
    if min > max
        || block.length == 0
        || block.rows == 0
        || block.offset.checked_add(block.length).is_none()
    {
        return Err(invariant_violation("invalid block witness bounds"));
    }
    raw_digest(&block.sha256)?;
    Ok((min, max))
}

fn validate_immutable_object(witness: &ImmutableObjectWitness, field: &str) -> Result<()> {
    if witness.path.is_empty() || witness.byte_size == 0 || witness.path.contains(['\\', '\0']) {
        return Err(invariant_violation(format!(
            "invalid {field} witness path or size"
        )));
    }
    raw_digest(&witness.sha256)?;
    Ok(())
}

fn validate_position(expected_root_b64: &str, position: &DirectoryPosition) -> Result<()> {
    if position.role != physical::Role::Kv
        || position.root_b64 != expected_root_b64
        || position.path.len() > DIRECTORY_PATH_DEPTH
    {
        return Err(invariant_violation(
            "directory position has a wrong role, root, or depth",
        ));
    }
    canonical_binary(&position.root_b64, "directory position root", true)?;
    for entry in &position.path {
        raw_digest(&entry.page_sha256)?;
    }
    validate_directory_leaf(&position.leaf)?;
    Ok(())
}

fn validate_side_cursor(expected_root_b64: &str, cursor: &SideCursor) -> Result<()> {
    match cursor {
        SideCursor::Start | SideCursor::End => Ok(()),
        SideCursor::After {
            key_b64url,
            position,
        } => {
            let key = canonical_binary(key_b64url, "side cursor key", false)?;
            validate_position(expected_root_b64, position)?;
            let (first, last) = validate_directory_leaf(&position.leaf)?;
            if key < first || key > last {
                return Err(invariant_violation("side cursor key is outside named leaf"));
            }
            Ok(())
        }
    }
}

fn validate_cursor_shape(expected: &ExpectedPlan<'_>, cursor: &MergeCursor) -> Result<()> {
    if let GlobalCut::After { key_b64url } = &cursor.global {
        canonical_binary(key_b64url, "global cut key", false)?;
    }
    validate_side_cursor(expected.source_kv_root_b64, &cursor.source)?;
    validate_side_cursor(expected.base_kv_root_b64, &cursor.current)
}

fn global_key(cut: &GlobalCut) -> Result<Option<Vec<u8>>> {
    match cut {
        GlobalCut::Start | GlobalCut::End => Ok(None),
        GlobalCut::After { key_b64url } => {
            canonical_binary(key_b64url, "global cut key", false).map(Some)
        }
    }
}

fn validate_side_advance(before: &SideCursor, after: &SideCursor) -> Result<()> {
    if before == after {
        return Ok(());
    }
    match (before, after) {
        (SideCursor::End, _) => Err(invariant_violation("side cursor moved after end")),
        (SideCursor::Start, _) | (SideCursor::After { .. }, SideCursor::End) => Ok(()),
        (
            SideCursor::After {
                key_b64url: left, ..
            },
            SideCursor::After {
                key_b64url: right, ..
            },
        ) => {
            if canonical_binary(left, "side cursor key", false)?
                >= canonical_binary(right, "side cursor key", false)?
            {
                Err(invariant_violation(
                    "side cursor regressed or changed a retained position",
                ))
            } else {
                Ok(())
            }
        }
        (SideCursor::After { .. }, SideCursor::Start) => {
            Err(invariant_violation("side cursor regressed to start"))
        }
    }
}

fn pending_key(state: &SingletonState) -> Result<Option<Vec<u8>>> {
    match state {
        SingletonState::None => Ok(None),
        SingletonState::Pending { key_b64url, .. } | SingletonState::Complete { key_b64url } => {
            canonical_binary(key_b64url, "singleton key", false).map(Some)
        }
    }
}

fn validate_singleton_value(value: &SingletonValueWitness) -> Result<()> {
    raw_digest(&value.value_sha256)?;
    validate_immutable_object(&value.descriptor, "singleton descriptor")?;
    validate_immutable_object(&value.index, "singleton index")?;
    validate_block(&value.block).map(|_| ())
}

fn validate_singleton_shape(state: &SingletonState) -> Result<()> {
    match state {
        SingletonState::None | SingletonState::Complete { .. } => {
            pending_key(state)?;
            Ok(())
        }
        SingletonState::Pending {
            source, current, ..
        } => {
            pending_key(state)?;
            if source.is_none() && current.is_none() {
                return Err(invariant_violation("pending singleton has no side witness"));
            }
            if let Some(value) = source {
                validate_singleton_value(value)?;
            }
            if let Some(value) = current {
                validate_singleton_value(value)?;
            }
            Ok(())
        }
    }
}

fn validate_singleton_transition(before: &SingletonState, after: &SingletonState) -> Result<()> {
    validate_singleton_shape(before)?;
    validate_singleton_shape(after)?;
    match (before, after) {
        (
            SingletonState::None,
            SingletonState::Pending {
                phase: SingletonPhase::CompareSource,
                ..
            },
        )
        | (SingletonState::None | SingletonState::Complete { .. }, SingletonState::None) => Ok(()),
        (
            SingletonState::Pending {
                key_b64url: before_key,
                source: before_source,
                current: before_current,
                phase: before_phase @ SingletonPhase::CompareSource,
            },
            SingletonState::Pending {
                key_b64url: after_key,
                source: after_source,
                current: after_current,
                phase: SingletonPhase::CompareCurrent,
            },
        )
        | (
            SingletonState::Pending {
                key_b64url: before_key,
                source: before_source,
                current: before_current,
                phase: before_phase @ SingletonPhase::CompareCurrent,
            },
            SingletonState::Pending {
                key_b64url: after_key,
                source: after_source,
                current: after_current,
                phase: SingletonPhase::Emit,
            },
        ) => {
            if before_key == after_key
                && before_source == after_source
                // The first comparison may add the not-yet-observed current row.
                // Every already observed witness remains immutable.
                && (before_current == after_current
                    || (*before_phase == SingletonPhase::CompareSource && before_current.is_none()))
            {
                Ok(())
            } else {
                Err(invariant_violation(
                    "singleton phase transition changed witness",
                ))
            }
        }
        (
            SingletonState::Pending {
                key_b64url,
                phase: SingletonPhase::Emit,
                ..
            },
            SingletonState::Complete {
                key_b64url: complete,
            },
        ) if key_b64url == complete => Ok(()),
        _ => Err(invariant_violation("illegal singleton state transition")),
    }
}

fn validate_cursor_transition(
    expected: &ExpectedPlan<'_>,
    before: &MergeCursor,
    after: &MergeCursor,
    singleton_before: &SingletonState,
    singleton_after: &SingletonState,
) -> Result<()> {
    validate_cursor_shape(expected, before)?;
    validate_cursor_shape(expected, after)?;
    validate_side_advance(&before.source, &after.source)?;
    validate_side_advance(&before.current, &after.current)?;
    validate_singleton_transition(singleton_before, singleton_after)?;
    match (&before.global, &after.global) {
        (GlobalCut::After { .. }, GlobalCut::End) | (GlobalCut::Start, _) => {}
        (GlobalCut::End, _) => return Err(invariant_violation("global cut moved after end")),
        (GlobalCut::After { key_b64url: left }, GlobalCut::After { key_b64url: right }) => {
            if canonical_binary(left, "global cut key", false)?
                > canonical_binary(right, "global cut key", false)?
            {
                return Err(invariant_violation("global cut regressed"));
            }
        }
        (GlobalCut::After { .. }, GlobalCut::Start) => {
            return Err(invariant_violation("global cut regressed to start"));
        }
    }
    if before == after && singleton_before == singleton_after {
        return Err(invariant_violation(
            "receipt does not advance cursor or singleton state",
        ));
    }
    // Check both endpoints: a newly installed pending key must not start behind
    // the cut, and a completed key may be cleared only after its cut advanced.
    for (cursor, singleton) in [(before, singleton_before), (after, singleton_after)] {
        let key = pending_key(singleton)?;
        match (singleton, key) {
            (SingletonState::Pending { .. }, Some(key)) => {
                if matches!(cursor.global, GlobalCut::End)
                    || global_key(&cursor.global)?.is_some_and(|cut| cut >= key)
                {
                    return Err(invariant_violation("global cut passed a pending singleton"));
                }
            }
            (SingletonState::Complete { .. }, Some(key)) => {
                if matches!(cursor.global, GlobalCut::Start)
                    || global_key(&cursor.global)?.is_some_and(|cut| cut < key)
                {
                    return Err(invariant_violation(
                        "completed singleton has not advanced its cut",
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_input(
    expected_root: &str,
    input: &InputWitness,
) -> Result<(Vec<u8>, Vec<u8>, u64, u64)> {
    if input.role != physical::Role::Kv || input.root_b64 != expected_root {
        return Err(invariant_violation("input witness role or root differs"));
    }
    let (first, last) = validate_directory_leaf(&input.directory_leaf)?;
    let (min, max) = validate_block(&input.block)?;
    if min < first || max > last {
        return Err(invariant_violation(
            "input block escapes directory leaf fence",
        ));
    }
    validate_immutable_object(&input.descriptor, "input descriptor")?;
    validate_immutable_object(&input.index, "input index")?;
    Ok((first, last, input.block.length, input.block.rows))
}

fn validate_output(output: &OutputWitness, expected_id: &str) -> Result<(Vec<u8>, Vec<u8>, u64)> {
    if output.role != physical::Role::Kv || output.output_id != expected_id {
        return Err(invariant_violation(
            "output witness role or deterministic id differs",
        ));
    }
    let first = canonical_binary(
        &output.directory_leaf.first_key_b64url,
        "output leaf first key",
        false,
    )?;
    let last = canonical_binary(
        &output.directory_leaf.last_key_b64url,
        "output leaf last key",
        false,
    )?;
    if first > last || output.directory_leaf.rows == 0 || output.directory_leaf.bytes == 0 {
        return Err(invariant_violation("invalid output leaf witness"));
    }
    raw_digest(&output.directory_leaf.digest)?;
    let (min, max) = validate_block(&output.block)?;
    if min < first || max > last {
        return Err(invariant_violation(
            "output block escapes output leaf fence",
        ));
    }
    validate_immutable_object(&output.descriptor, "output descriptor")?;
    validate_immutable_object(&output.index, "output index")?;
    Ok((first, last, output.block.length))
}

fn validate_persisted_output(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    expected: &WorkingValue<ExpectedPlan<'_>>,
    ordinal: u64,
    witness: &WorkingValue<OutputWitness>,
    output: &physical::restore_io::StandardRestoreOutput,
) -> Result<WorkingValue<()>> {
    let same_owner = expected.is_owned_by(io)
        && witness.is_owned_by(io)
        && output.descriptor.is_owned_by(io)
        && output.bytes.is_owned_by(io);
    let store = io.store();
    let result = (|| {
        let reservation = decode_with_reservation(io, route, Some(64 * 1024), || {
            if !same_owner || usize::BITS != 64 {
                return Err(invariant_violation("output binding ownership differs"));
            }
            let bytes = output_binding_string_bytes(witness.value(), output.descriptor.value())
                .filter(|bytes| *bytes <= UNIT_RECORD_BYTES)
                .ok_or_else(|| invariant_violation("output binding exceeds string admission"))?;
            physical::restore_io::directory_scope_reservation(store)?
                .checked_add(64 * 1024)
                .and_then(|fixed| bytes.checked_mul(4)?.checked_add(fixed))
                .ok_or_else(|| invariant_violation("output binding reservation overflow"))
        })?;
        let bytes = *reservation.value();
        drop(reservation);
        let id = codec::hashes::output_id(
            io,
            route,
            expected,
            ordinal,
            witness.value().part,
            physical::Role::Kv,
        )?;
        let raw_hash = codec::hashes::raw_encoded(io, route, &output.bytes)?;
        decode_with_reservation(io, route, Some(bytes), || {
            validate_persisted_output_fields(
                store,
                witness.value(),
                output,
                id.value(),
                raw_hash.value(),
            )
        })
    })();
    if result.is_err() {
        io.stop(route);
    }
    result
}

fn validate_persisted_output_fields(
    store: &super::super::super::ControlMvpStateStore,
    value: &OutputWitness,
    output: &physical::restore_io::StandardRestoreOutput,
    id: &str,
    raw_hash: &str,
) -> Result<()> {
    let descriptor = output.descriptor.value();
    let block = &descriptor.block;
    let (first, last, _) = validate_output(value, id)?;
    let actual_bounds = super::super::super::block_key_bounds(block)?;
    let descriptor_hash = format!("sha256:{}", hex::encode(output.digest));
    if descriptor.encoding_version != 1
        || !descriptor.scope.matches_scope(&store.scope)
        || descriptor.role != physical::Role::Kv
        || descriptor.segment.level != ControlMvpSegmentLevel::L1
        || descriptor.segment.segment_id != id
        || descriptor.segment_version.is_empty()
        || descriptor.index_version.is_empty()
        || block.record_kind != Some(SEGMENT_RECORD_KV)
        || block.offset != 0
        || block.length != descriptor.segment.segment_size_bytes
        || raw_hash != descriptor_hash
        || value.descriptor.sha256 != descriptor_hash
        || value.descriptor.byte_size != output.bytes.value().len() as u64
        || value.descriptor.path
            != format!(
                "{}/physical/descriptors/{}.json",
                store.paths.base_prefix(),
                hex::encode(output.digest)
            )
        || value.index.path != store.paths.segment_index(id)
        || value.index.byte_size != descriptor.segment.index_size_bytes
        || value.index.sha256.strip_prefix("sha256:")
            != Some(descriptor.segment.index_checksum_sha256.as_str())
        || value.directory_leaf.digest != descriptor_hash
        || value.directory_leaf.rows != block.row_count
        || u64::from(value.directory_leaf.bytes) != block.length
        || actual_bounds.as_ref() != Some(&(first, last))
        || value.block.offset != block.offset
        || value.block.length != block.length
        || value.block.rows != block.row_count
        || value.block.sha256.strip_prefix("sha256:") != Some(block.checksum_sha256.as_str())
        || value.block.min_key_b64url != value.directory_leaf.first_key_b64url
        || value.block.max_key_b64url != value.directory_leaf.last_key_b64url
    {
        return Err(invariant_violation(
            "receipt output differs from persisted artifacts",
        ));
    }
    Ok(())
}

fn output_binding_string_bytes(w: &OutputWitness, d: &physical::Descriptor) -> Option<usize> {
    [
        &w.output_id,
        &w.directory_leaf.first_key_b64url,
        &w.directory_leaf.last_key_b64url,
        &w.directory_leaf.digest,
        &w.descriptor.path,
        &w.descriptor.sha256,
        &w.index.path,
        &w.index.sha256,
        &w.block.sha256,
        &w.block.min_key_b64url,
        &w.block.max_key_b64url,
    ]
    .into_iter()
    .map(String::len)
    .chain(d.block.min_key_hex.as_ref().map(String::len))
    .chain(d.block.max_key_hex.as_ref().map(String::len))
    .try_fold(0_usize, usize::checked_add)
}

fn strictly_ordered_inputs(expected_root: &str, inputs: &[InputWitness]) -> Result<(u64, u64)> {
    let mut previous_last = None;
    let mut bytes = 0_u64;
    let mut rows = 0_u64;
    for input in inputs {
        let (first, last, length, input_rows) = validate_input(expected_root, input)?;
        if previous_last
            .as_ref()
            .is_some_and(|previous: &Vec<u8>| previous >= &first)
        {
            return Err(invariant_violation("input leaves are not strictly ordered"));
        }
        previous_last = Some(last);
        bytes = bytes
            .checked_add(length)
            .ok_or_else(|| invariant_violation("input byte count overflow"))?;
        rows = rows
            .checked_add(input_rows)
            .ok_or_else(|| invariant_violation("input row count overflow"))?;
    }
    Ok((bytes, rows))
}

fn legacy_output_ids(
    expected: &ExpectedPlan<'_>,
    ordinal: u64,
    outputs: &[OutputWitness],
) -> Result<[Option<String>; 32]> {
    if outputs.len() > 32 {
        return Err(invariant_violation("output identity count exceeds bound"));
    }
    let mut ids = std::array::from_fn(|_| None);
    for (slot, output) in ids.iter_mut().zip(outputs) {
        *slot = Some(output_id(expected, ordinal, output.part, output.role)?);
    }
    Ok(ids)
}

fn strictly_ordered_outputs(
    expected: &ExpectedPlan<'_>,
    ordinal: u64,
    outputs: &[OutputWitness],
) -> Result<u64> {
    let ids = legacy_output_ids(expected, ordinal, outputs)?;
    let borrowed: [&str; 32] =
        std::array::from_fn(|index| ids.get(index).and_then(Option::as_deref).unwrap_or(""));
    let borrowed = borrowed
        .get(..outputs.len())
        .ok_or_else(|| invariant_violation("output identity count exceeds bound"))?;
    strictly_ordered_outputs_with_ids(outputs, borrowed)
}

fn strictly_ordered_outputs_with_ids(outputs: &[OutputWitness], ids: &[&str]) -> Result<u64> {
    if outputs.len() != ids.len() {
        return Err(invariant_violation("output identity cardinality differs"));
    }
    let mut previous_last = None;
    let mut previous_part = None;
    let mut bytes = 0_u64;
    for (output, id) in outputs.iter().zip(ids) {
        if previous_part.is_some_and(|part| part >= output.part) {
            return Err(invariant_violation("output parts are not strictly ordered"));
        }
        previous_part = Some(output.part);
        let (first, last, length) = validate_output(output, id)?;
        if previous_last
            .as_ref()
            .is_some_and(|previous: &Vec<u8>| previous >= &first)
        {
            return Err(invariant_violation(
                "output leaves are not strictly ordered",
            ));
        }
        previous_last = Some(last);
        bytes = bytes
            .checked_add(length)
            .ok_or_else(|| invariant_violation("output byte count overflow"))?;
    }
    Ok(bytes)
}

fn checked_sum(
    prefix: &CumulativeSemanticCounts,
    counts: &SemanticCounts,
) -> Result<CumulativeSemanticCounts> {
    macro_rules! add {
        ($field:ident) => {
            prefix
                .$field
                .checked_add(counts.$field)
                .ok_or_else(|| invariant_violation("cumulative semantic count overflow"))?
        };
    }
    Ok(CumulativeSemanticCounts {
        source_leaves: add!(source_leaves),
        current_leaves: add!(current_leaves),
        input_encoded_bytes: add!(input_encoded_bytes),
        decoded_blocks: add!(decoded_blocks),
        decoded_rows: add!(decoded_rows),
        output_blocks: add!(output_blocks),
        output_encoded_bytes: add!(output_encoded_bytes),
        mutations: add!(mutations),
    })
}

fn validate_reservation(
    counts: &SemanticCounts,
    before: &SingletonState,
    after: &SingletonState,
) -> Result<()> {
    let output_byte_limit = match &counts.reservation {
        UnitReservationV1::Standard {
            combined_leaf_limit,
            decode_limit,
            input_byte_limit,
            output_block_limit,
            packed_output_byte_limit,
        } => {
            if (
                *combined_leaf_limit,
                *decode_limit,
                *input_byte_limit,
                *output_block_limit,
                *packed_output_byte_limit,
            ) != (
                STANDARD_COMBINED_LEAF_LIMIT,
                STANDARD_DECODE_LIMIT,
                STANDARD_INPUT_BYTE_LIMIT,
                STANDARD_OUTPUT_BLOCK_LIMIT,
                STANDARD_PACKED_OUTPUT_BYTE_LIMIT,
            ) {
                return Err(invariant_violation(
                    "standard reservation differs from frozen bounds",
                ));
            }
            if matches!(before, SingletonState::Pending { .. })
                || matches!(after, SingletonState::Pending { .. })
            {
                return Err(invariant_violation(
                    "standard reservation cannot advance pending singleton",
                ));
            }
            STANDARD_PACKED_OUTPUT_BYTE_LIMIT
        }
        UnitReservationV1::Singleton {
            phase,
            authenticated_payload_bytes,
            input_byte_limit,
            segment_byte_limit,
            scratch_base_bytes,
            scratch_payload_multiplier,
        } => {
            if (
                *input_byte_limit,
                *segment_byte_limit,
                *scratch_base_bytes,
                *scratch_payload_multiplier,
            ) != (
                SINGLETON_INPUT_BYTE_LIMIT,
                SINGLETON_SEGMENT_BYTE_LIMIT,
                SINGLETON_SCRATCH_BASE_BYTES,
                SINGLETON_SCRATCH_PAYLOAD_MULTIPLIER,
            ) {
                return Err(invariant_violation(
                    "singleton reservation differs from frozen bounds",
                ));
            }
            if *authenticated_payload_bytes == 0
                || *authenticated_payload_bytes > *segment_byte_limit
            {
                return Err(invariant_violation(
                    "singleton payload reservation exceeds its encoded range ceiling",
                ));
            }
            scratch_payload_multiplier
                .checked_mul(*authenticated_payload_bytes)
                .and_then(|payload| scratch_base_bytes.checked_add(payload))
                .ok_or_else(|| invariant_violation("singleton scratch reservation overflows"))?;
            let state_phase = match before {
                SingletonState::Pending { phase, .. } => Some(*phase),
                _ => match after {
                    SingletonState::Pending { phase, .. } => Some(*phase),
                    _ => None,
                },
            };
            if state_phase != Some(*phase) {
                return Err(invariant_violation(
                    "singleton reservation phase differs from state",
                ));
            }
            SINGLETON_SEGMENT_BYTE_LIMIT
        }
    };
    let combined_leaves = counts
        .source_leaves
        .checked_add(counts.current_leaves)
        .ok_or_else(|| invariant_violation("combined leaf count overflow"))?;
    if combined_leaves > STANDARD_COMBINED_LEAF_LIMIT
        || counts.decoded_blocks > STANDARD_DECODE_LIMIT
        || counts.input_encoded_bytes > STANDARD_INPUT_BYTE_LIMIT
        || counts.output_blocks > STANDARD_OUTPUT_BLOCK_LIMIT
        || counts.output_encoded_bytes > output_byte_limit
    {
        return Err(invariant_violation(
            "semantic counts exceed frozen unit bounds",
        ));
    }
    Ok(())
}

fn validate_counts(
    expected: &ExpectedPlan<'_>,
    receipt: &ControlMvpRestoreReceiptV1,
    output_ids: &[&str],
) -> Result<()> {
    let (source_bytes, source_rows) =
        strictly_ordered_inputs(expected.source_kv_root_b64, &receipt.source_inputs)?;
    let (current_bytes, current_rows) =
        strictly_ordered_inputs(expected.base_kv_root_b64, &receipt.current_inputs)?;
    let output_bytes = strictly_ordered_outputs_with_ids(&receipt.outputs, output_ids)?;
    let source_leaves = u64::try_from(receipt.source_inputs.len())
        .map_err(|_| invariant_violation("source input count overflows"))?;
    let current_leaves = u64::try_from(receipt.current_inputs.len())
        .map_err(|_| invariant_violation("current input count overflows"))?;
    let output_blocks = u64::try_from(receipt.outputs.len())
        .map_err(|_| invariant_violation("output count overflows"))?;
    let input_bytes = source_bytes
        .checked_add(current_bytes)
        .ok_or_else(|| invariant_violation("input byte count overflow"))?;
    let decoded_rows = source_rows
        .checked_add(current_rows)
        .ok_or_else(|| invariant_violation("decoded row count overflow"))?;
    let decoded_blocks = source_leaves
        .checked_add(current_leaves)
        .ok_or_else(|| invariant_violation("decoded block count overflow"))?;
    let listed_counts_match = receipt.counts.source_leaves == source_leaves
        && receipt.counts.current_leaves == current_leaves
        && receipt.counts.input_encoded_bytes == input_bytes
        && receipt.counts.decoded_blocks == decoded_blocks
        && receipt.counts.decoded_rows == decoded_rows
        && receipt.counts.output_blocks == output_blocks
        && receipt.counts.output_encoded_bytes == output_bytes;
    match &receipt.counts.reservation {
        // Ordinary units persist every decoded block/output leaf in their lists,
        // so their deterministic dimensions must be exact here.
        UnitReservationV1::Standard { .. } if !listed_counts_match => {
            return Err(invariant_violation(
                "standard receipt semantic counts differ from witnesses",
            ));
        }
        // A singleton value witness may carry the one selected row outside the
        // ordinary leaf lists. Reject a smaller declared count, but leave the
        // exact singleton row/payload recomputation to the authenticated runtime.
        UnitReservationV1::Singleton { .. }
            if receipt.counts.source_leaves < source_leaves
                || receipt.counts.current_leaves < current_leaves
                || receipt.counts.input_encoded_bytes < input_bytes
                || receipt.counts.decoded_blocks < decoded_blocks
                || receipt.counts.decoded_rows < decoded_rows
                || receipt.counts.output_blocks < output_blocks
                || receipt.counts.output_encoded_bytes < output_bytes =>
        {
            return Err(invariant_violation(
                "singleton receipt semantic counts omit listed witness work",
            ));
        }
        _ => {}
    }
    validate_reservation(
        &receipt.counts,
        &receipt.singleton_before,
        &receipt.singleton_after,
    )
}

fn validate_receipt_shape(
    expected: &ExpectedPlan<'_>,
    receipt: &ControlMvpRestoreReceiptV1,
) -> Result<()> {
    let body = receipt_body_sha256(receipt)?;
    let chain = receipt_chain_sha256(receipt)?;
    let ids = legacy_output_ids(expected, receipt.ordinal, &receipt.outputs)?;
    let borrowed: [&str; 32] =
        std::array::from_fn(|index| ids.get(index).and_then(Option::as_deref).unwrap_or(""));
    let borrowed = borrowed
        .get(..receipt.outputs.len())
        .ok_or_else(|| invariant_violation("output identity count exceeds bound"))?;
    validate_receipt_shape_with_hashes(expected, receipt, &body, &chain, borrowed)
}

// Hash-free structural semantics shared by the legacy oracle and admitted path.
fn validate_receipt_shape_with_hashes(
    expected: &ExpectedPlan<'_>,
    receipt: &ControlMvpRestoreReceiptV1,
    body: &str,
    chain: &str,
    output_ids: &[&str],
) -> Result<()> {
    if receipt.record_type != "control_mvp_restore_receipt"
        || receipt.version != 1
        || receipt.plan_sha256 != expected.plan_sha256
        || &receipt.identity != expected.identity
        || receipt.owner_generation != expected.owner_generation
    {
        return Err(invariant_violation(
            "receipt envelope differs from selected Plan7",
        ));
    }
    raw_digest(&receipt.plan_sha256)?;
    raw_digest(&receipt.predecessor_receipt_sha256)?;
    raw_digest(&receipt.predecessor_chain_sha256)?;
    raw_digest(&receipt.receipt_body_sha256)?;
    raw_digest(&receipt.chain_sha256)?;
    if receipt.receipt_body_sha256 != body || receipt.chain_sha256 != chain {
        return Err(invariant_violation("receipt tagged hash differs"));
    }
    validate_cursor_transition(
        expected,
        &receipt.before,
        &receipt.after,
        &receipt.singleton_before,
        &receipt.singleton_after,
    )?;
    validate_counts(expected, receipt, output_ids)
}

/// Bounded forward state, created only by exact receipt decoding. It retains
/// no prior receipt body, output list, or raw allocation.
struct ReceiptTail {
    ordinal: u64,
    raw_sha256: String,
    chain_sha256: String,
    cursor: MergeCursor,
    singleton: SingletonState,
    cumulative: CumulativeSemanticCounts,
}

/// Decode the exact raw object before checking a direct structural chain edge.
/// Physical row/coverage verification remains separate from this codec.
fn validate_receipt_chain(
    expected: &ExpectedPlan<'_>,
    receipt_raw: &[u8],
    predecessor: Option<&ReceiptTail>,
) -> Result<(ControlMvpRestoreReceiptV1, ReceiptTail)> {
    let receipt: ControlMvpRestoreReceiptV1 =
        decode_jcs_exact(receipt_raw, UNIT_RECORD_BYTES, "forward receipt")?;
    validate_receipt_shape(expected, &receipt)?;
    match (receipt.ordinal, predecessor) {
        (0, None) => {
            if receipt.predecessor_receipt_sha256 != genesis_receipt_raw_sha256(expected)?
                || receipt.predecessor_chain_sha256 != genesis_chain_sha256(expected)?
                || receipt.before != zero_cursor()
                || receipt.singleton_before != SingletonState::None
                || receipt.prefix_cumulative_counts != CumulativeSemanticCounts::default()
            {
                return Err(invariant_violation(
                    "ordinal-zero receipt differs from genesis",
                ));
            }
        }
        (0, Some(_)) => return Err(invariant_violation("ordinal-zero receipt has predecessor")),
        (_, Some(prior)) => {
            if prior.ordinal.checked_add(1) != Some(receipt.ordinal)
                || receipt.predecessor_receipt_sha256 != prior.raw_sha256
                || receipt.predecessor_chain_sha256 != prior.chain_sha256
                || receipt.before != prior.cursor
                || receipt.singleton_before != prior.singleton
                || receipt.prefix_cumulative_counts != prior.cumulative
            {
                return Err(invariant_violation(
                    "receipt does not continue its direct predecessor",
                ));
            }
        }
        (_, None) => {
            return Err(invariant_violation(
                "non-genesis receipt lacks direct predecessor",
            ));
        }
    }
    receipt
        .ordinal
        .checked_add(1)
        .ok_or_else(|| invariant_violation("receipt ordinal overflow"))?;
    let tail = ReceiptTail {
        ordinal: receipt.ordinal,
        raw_sha256: receipt_raw_sha256(receipt_raw),
        chain_sha256: receipt.chain_sha256.clone(),
        cursor: receipt.after.clone(),
        singleton: receipt.singleton_after.clone(),
        cumulative: checked_sum(&receipt.prefix_cumulative_counts, &receipt.counts)?,
    };
    Ok((receipt, tail))
}

fn zero_cursor() -> MergeCursor {
    MergeCursor {
        global: GlobalCut::Start,
        source: SideCursor::Start,
        current: SideCursor::Start,
    }
}
fn is_end_cursor(cursor: &MergeCursor) -> bool {
    matches!(cursor.global, GlobalCut::End)
        && matches!(cursor.source, SideCursor::End)
        && matches!(cursor.current, SideCursor::End)
}

fn validate_progress_shape(
    expected: &ExpectedPlan<'_>,
    progress: &RestoreProgressV1,
) -> Result<()> {
    let genesis = if progress.next_ordinal == 0 {
        Some(genesis_chain_sha256(expected)?)
    } else {
        None
    };
    validate_progress_shape_with_genesis(expected, progress, genesis.as_deref())
}

fn validate_progress_shape_with_genesis(
    expected: &ExpectedPlan<'_>,
    progress: &RestoreProgressV1,
    genesis_chain: Option<&str>,
) -> Result<()> {
    if genesis_chain.is_some() != (progress.next_ordinal == 0) {
        return Err(invariant_violation(
            "progress genesis evidence cardinality differs",
        ));
    }
    if progress.record_type != "control_mvp_restore_progress"
        || progress.version != 1
        || progress.plan_sha256 != expected.plan_sha256
        || &progress.identity != expected.identity
        || progress.owner_generation != expected.owner_generation
        || progress.next_ordinal != progress.receipt_count
    {
        return Err(invariant_violation(
            "progress envelope differs from selected Plan7",
        ));
    }
    raw_digest(&progress.plan_sha256)?;
    raw_digest(&progress.chain_sha256)?;
    validate_cursor_shape(expected, &progress.cursor)?;
    validate_singleton_shape(&progress.singleton_state)?;
    if progress.next_ordinal == 0 {
        if progress.last_receipt.is_some()
            || genesis_chain != Some(progress.chain_sha256.as_str())
            || progress.cursor != zero_cursor()
            || progress.singleton_state != SingletonState::None
            || progress.terminal
            || progress.cumulative_counts != CumulativeSemanticCounts::default()
        {
            return Err(invariant_violation(
                "genesis progress differs from frozen state",
            ));
        }
    } else {
        let last = progress
            .last_receipt
            .as_ref()
            .ok_or_else(|| invariant_violation("non-genesis progress lacks receipt reference"))?;
        if last.path != expected.receipt_path(progress.next_ordinal - 1) {
            return Err(invariant_violation("progress last receipt path differs"));
        }
        raw_digest(&last.raw_sha256)?;
    }
    if progress.terminal {
        if !is_end_cursor(&progress.cursor)
            || matches!(progress.singleton_state, SingletonState::Pending { .. })
        {
            return Err(invariant_violation("terminal progress is incomplete"));
        }
    } else if matches!(progress.cursor.global, GlobalCut::End) {
        return Err(invariant_violation("nonterminal progress has global end"));
    }
    Ok(())
}

fn validate_selector_shape(
    expected: &ExpectedPlan<'_>,
    selector: &RestoreProgressSelectorV1,
) -> Result<()> {
    if selector.record_type != "control_mvp_restore_progress_selector"
        || selector.version != 1
        || selector.plan_sha256 != expected.plan_sha256
        || &selector.identity != expected.identity
        || selector.owner_generation != expected.owner_generation
    {
        return Err(invariant_violation(
            "progress selector differs from selected Plan7",
        ));
    }
    raw_digest(&selector.plan_sha256)?;
    raw_digest(&selector.current_progress_sha256)?;
    Ok(())
}

/// Validates one selected local edge. It reads no storage and therefore cannot
/// prove any unseen chain prefix, directory membership, or output existence.
#[allow(
    clippy::too_many_arguments,
    reason = "the selected direct predecessor is the whole local recovery boundary"
)]
fn validate_local_advance(
    expected: &ExpectedPlan<'_>,
    selector_path: &str,
    selector_raw_jcs: &[u8],
    selected_progress_path: &str,
    selected_progress_raw_jcs: &[u8],
    selected_last_receipt_path: Option<&str>,
    selected_last_receipt_raw_jcs: Option<&[u8]>,
    proposed_receipt_path: &str,
    proposed_receipt_raw_jcs: &[u8],
    proposed_progress_path: &str,
    proposed_progress_raw_jcs: &[u8],
) -> Result<()> {
    let selected = validate_selector_current(
        expected,
        selector_path,
        selector_raw_jcs,
        selected_progress_path,
        selected_progress_raw_jcs,
        selected_last_receipt_path,
        selected_last_receipt_raw_jcs,
    )?;
    if selected.terminal {
        return Err(invariant_violation(
            "terminal progress cannot accept another receipt",
        ));
    }
    let proposed_receipt: ControlMvpRestoreReceiptV1 = decode_jcs_exact(
        proposed_receipt_raw_jcs,
        UNIT_RECORD_BYTES,
        "restore receipt",
    )?;
    let proposed_progress: RestoreProgressV1 = decode_jcs_exact(
        proposed_progress_raw_jcs,
        UNIT_RECORD_BYTES,
        "proposed restore progress",
    )?;
    validate_receipt_shape(expected, &proposed_receipt)?;
    validate_progress_shape(expected, &proposed_progress)?;
    require_path(
        proposed_receipt_path,
        &expected.receipt_path(proposed_receipt.ordinal),
        "proposed receipt",
    )?;
    require_path(
        proposed_progress_path,
        &expected.progress_path(proposed_progress.next_ordinal),
        "proposed progress",
    )?;
    if proposed_receipt.ordinal != selected.next_ordinal
        || proposed_receipt.prefix_cumulative_counts != selected.cumulative_counts
        || proposed_receipt.before != selected.cursor
        || proposed_receipt.singleton_before != selected.singleton_state
    {
        return Err(invariant_violation(
            "proposed receipt does not continue selected progress",
        ));
    }
    let predecessor_raw_sha256 = match &selected.last_receipt {
        Some(reference) => reference.raw_sha256.clone(),
        None => genesis_receipt_raw_sha256(expected)?,
    };
    if proposed_receipt.predecessor_receipt_sha256 != predecessor_raw_sha256
        || proposed_receipt.predecessor_chain_sha256 != selected.chain_sha256
    {
        return Err(invariant_violation("proposed receipt predecessor differs"));
    }
    if proposed_progress.next_ordinal
        != selected
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| invariant_violation("progress ordinal overflow"))?
        || proposed_progress.receipt_count != proposed_progress.next_ordinal
        || proposed_progress.last_receipt.as_ref()
            != Some(&ReceiptRef {
                path: expected.receipt_path(proposed_receipt.ordinal),
                raw_sha256: receipt_raw_sha256(proposed_receipt_raw_jcs),
            })
        || proposed_progress.chain_sha256 != proposed_receipt.chain_sha256
        || proposed_progress.cursor != proposed_receipt.after
        || proposed_progress.singleton_state != proposed_receipt.singleton_after
        || proposed_progress.cumulative_counts
            != checked_sum(&selected.cumulative_counts, &proposed_receipt.counts)?
    {
        return Err(invariant_violation(
            "proposed progress does not exactly follow proposed receipt",
        ));
    }
    Ok(())
}

/// Selector/current-progress consistency used after an observed selector CAS
/// loss. The caller separately compares the observed object version.
fn validate_selector_current(
    expected: &ExpectedPlan<'_>,
    selector_path: &str,
    selector_raw_jcs: &[u8],
    progress_path: &str,
    progress_raw_jcs: &[u8],
    direct_last_receipt_path: Option<&str>,
    direct_last_receipt_raw_jcs: Option<&[u8]>,
) -> Result<RestoreProgressV1> {
    let selector: RestoreProgressSelectorV1 = decode_jcs_exact(
        selector_raw_jcs,
        UNIT_RECORD_BYTES,
        "restore progress selector",
    )?;
    let progress: RestoreProgressV1 =
        decode_jcs_exact(progress_raw_jcs, UNIT_RECORD_BYTES, "restore progress")?;
    validate_selector_shape(expected, &selector)?;
    validate_progress_shape(expected, &progress)?;
    require_path(selector_path, &expected.selector_path(), "selector")?;
    require_path(
        progress_path,
        &expected.progress_path(progress.next_ordinal),
        "selected progress",
    )?;
    if selector.current_progress_path != expected.progress_path(progress.next_ordinal)
        || selector.current_progress_sha256 != receipt_raw_sha256(progress_raw_jcs)
    {
        return Err(invariant_violation(
            "selector does not name current progress",
        ));
    }
    if progress.next_ordinal == 0 {
        if direct_last_receipt_raw_jcs.is_some() || direct_last_receipt_path.is_some() {
            return Err(invariant_violation(
                "genesis progress supplied last receipt",
            ));
        }
        return Ok(progress);
    }
    let last_path = direct_last_receipt_path
        .ok_or_else(|| invariant_violation("non-genesis progress missing last receipt path"))?;
    require_path(
        last_path,
        &expected.receipt_path(progress.next_ordinal - 1),
        "direct last receipt",
    )?;
    let raw = direct_last_receipt_raw_jcs
        .ok_or_else(|| invariant_violation("non-genesis progress missing last receipt"))?;
    let last: ControlMvpRestoreReceiptV1 =
        decode_jcs_exact(raw, UNIT_RECORD_BYTES, "direct last receipt")?;
    validate_receipt_shape(expected, &last)?;
    let reference = progress
        .last_receipt
        .as_ref()
        .ok_or_else(|| invariant_violation("non-genesis progress lacks receipt reference"))?;
    if last.ordinal.checked_add(1) != Some(progress.next_ordinal)
        || reference.path != expected.receipt_path(progress.next_ordinal - 1)
        || reference.raw_sha256 != receipt_raw_sha256(raw)
        || progress.chain_sha256 != last.chain_sha256
        || progress.cursor != last.after
        || progress.singleton_state != last.singleton_after
        || progress.cumulative_counts != checked_sum(&last.prefix_cumulative_counts, &last.counts)?
    {
        return Err(invariant_violation(
            "progress differs from direct last receipt",
        ));
    }
    Ok(progress)
}

struct SelectedProgress {
    selector_raw: physical::restore_io::AccountedBytes,
    selector_meta: physical::restore_io::Accounted<arco_core::ObjectMeta>,
    progress: WorkingValue<RestoreProgressV1>,
}

async fn read_selected_progress(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    expected: &WorkingValue<ExpectedPlan<'_>>,
) -> Result<SelectedProgress> {
    let same_owner = expected.is_owned_by(io);
    let result = async {
        drop(decode_with_reservation(io, route, Some(64 * 1024), || {
            if !same_owner || usize::BITS != 64 || size_of::<SelectedProgress>() > 64 * 1024 {
                return Err(invariant_violation(
                    "selected progress ownership or layout differs",
                ));
            }
            Ok(())
        })?);
        let (selector_raw, selector_meta) = read_required_selected_record(
            io,
            route,
            expected.value().candidate_id,
            RestoreControlRecord::Selector,
        )
        .await?;
        let selector = codec::decode_selector(io, route, &selector_raw)?;
        drop(codec::validate_selector(io, route, expected, &selector)?);
        let ordinal = decode_with_reservation(io, route, Some(64 * 1024), || {
            selected_progress_ordinal(expected.value(), &selector.value().current_progress_path)
        })?;
        let (raw, metadata) = read_required_selected_record(
            io,
            route,
            expected.value().candidate_id,
            RestoreControlRecord::Progress(*ordinal.value()),
        )
        .await?;
        let progress = codec::decode_progress(io, route, &raw)?;
        drop(codec::validate_progress(io, route, expected, &progress)?);
        let raw_hash = codec::hashes::raw_read(io, route, &raw)?;
        drop(decode_with_reservation(io, route, Some(64 * 1024), || {
            if progress.value().next_ordinal != *ordinal.value()
                || selector.value().current_progress_sha256 != *raw_hash.value()
            {
                return Err(invariant_violation(
                    "selector does not bind exact selected progress",
                ));
            }
            Ok(())
        })?);
        drop((raw_hash, raw, metadata, selector, ordinal));
        if progress.value().next_ordinal != 0 {
            let (raw, metadata) = read_required_selected_record(
                io,
                route,
                expected.value().candidate_id,
                RestoreControlRecord::Receipt(progress.value().next_ordinal - 1),
            )
            .await?;
            let last = codec::decode_receipt(io, route, &raw)?;
            drop(codec::validate_receipt(io, route, expected, &last)?);
            let raw_hash = codec::hashes::raw_read(io, route, &raw)?;
            drop(decode_with_reservation(io, route, Some(64 * 1024), || {
                validate_selected_receipt_edge(progress.value(), last.value(), raw_hash.value())
            })?);
            drop((raw_hash, last, raw, metadata));
        }
        Ok(SelectedProgress {
            selector_raw,
            selector_meta,
            progress,
        })
    }
    .await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

async fn read_required_selected_record(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    candidate: &str,
    record: RestoreControlRecord,
) -> Result<(
    physical::restore_io::AccountedBytes,
    physical::restore_io::Accounted<arco_core::ObjectMeta>,
)> {
    let found =
        physical::restore_io::read_restore_control_record(io, route, candidate, record).await?;
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        if found.is_none() {
            return Err(invariant_violation("selected restore record is missing"));
        }
        Ok(())
    })?);
    found.map_or_else(
        || unreachable!("the admitted presence check returned an error"),
        Ok,
    )
}

fn selected_progress_ordinal(expected: &ExpectedPlan<'_>, path: &str) -> Result<u64> {
    let digits = path
        .strip_prefix(expected.prefix.as_str())
        .and_then(|suffix| suffix.strip_prefix("/progress/"))
        .and_then(|suffix| suffix.strip_suffix(".json"))
        .filter(|digits| digits.len() == 20 && digits.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| invariant_violation("selector progress path is not canonical"))?;
    digits
        .parse()
        .map_err(|_| invariant_violation("selector progress ordinal overflows"))
}

// Validates a local proposal only; this result grants no publication authority.
fn validate_proposed_transition(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    expected: &WorkingValue<ExpectedPlan<'_>>,
    selected: &SelectedProgress,
    receipt: &WorkingValue<ControlMvpRestoreReceiptV1>,
    progress: &WorkingValue<RestoreProgressV1>,
) -> Result<WorkingValue<()>> {
    let same_owner = expected.is_owned_by(io)
        && selected.selector_raw.is_owned_by(io)
        && selected.selector_meta.is_owned_by(io)
        && selected.progress.is_owned_by(io)
        && receipt.is_owned_by(io)
        && progress.is_owned_by(io);
    let result = (|| {
        drop(decode_with_reservation(io, route, Some(64 * 1024), || {
            let prior = selected.progress.value();
            if !same_owner || usize::BITS != 64 {
                return Err(invariant_violation("proposed transition ownership differs"));
            }
            if prior.plan_sha256 != expected.value().plan_sha256
                || &prior.identity != expected.value().identity
                || prior.owner_generation != expected.value().owner_generation
            {
                return Err(invariant_violation(
                    "selected progress belongs to another plan",
                ));
            }
            if prior.terminal || prior.next_ordinal.checked_add(1).is_none() {
                return Err(invariant_violation("selected progress cannot advance"));
            }
            Ok(())
        })?);
        drop(codec::validate_receipt(io, route, expected, receipt)?);
        drop(codec::validate_progress(io, route, expected, progress)?);
        let genesis = if selected.progress.value().next_ordinal == 0 {
            Some(codec::hashes::genesis_none(io, route, &selected.progress)?)
        } else {
            None
        };
        let raw = codec::encode_receipt(io, route, receipt)?;
        let raw_hash = codec::hashes::raw_encoded(io, route, &raw)?;
        drop(raw);
        decode_with_reservation(io, route, Some(64 * 1024), || {
            let prior = selected.progress.value();
            let proposed = receipt.value();
            let predecessor = prior
                .last_receipt
                .as_ref()
                .map(|reference| reference.raw_sha256.as_str())
                .or_else(|| genesis.as_ref().map(|hash| hash.value().as_str()));
            if proposed.ordinal != prior.next_ordinal
                || proposed.before != prior.cursor
                || proposed.singleton_before != prior.singleton_state
                || proposed.prefix_cumulative_counts != prior.cumulative_counts
                || Some(proposed.predecessor_receipt_sha256.as_str()) != predecessor
                || proposed.predecessor_chain_sha256 != prior.chain_sha256
            {
                return Err(invariant_violation(
                    "proposed receipt does not continue selected progress",
                ));
            }
            validate_selected_receipt_edge(progress.value(), proposed, raw_hash.value())
        })
    })();
    if result.is_err() {
        io.stop(route);
    }
    result
}

fn validate_selected_receipt_edge(
    progress: &RestoreProgressV1,
    last: &ControlMvpRestoreReceiptV1,
    raw_hash: &str,
) -> Result<()> {
    let reference = progress
        .last_receipt
        .as_ref()
        .ok_or_else(|| invariant_violation("selected progress lacks direct receipt"))?;
    if last.ordinal.checked_add(1) != Some(progress.next_ordinal)
        || reference.raw_sha256 != raw_hash
        || progress.chain_sha256 != last.chain_sha256
        || progress.cursor != last.after
        || progress.singleton_state != last.singleton_after
        || progress.cumulative_counts != checked_sum(&last.prefix_cumulative_counts, &last.counts)?
    {
        return Err(invariant_violation(
            "selected progress differs from direct receipt",
        ));
    }
    Ok(())
}

/// Receipt reads are intentionally a fixed 4 MiB-plus-one probe. This helper
/// only supplies the constant; the bounded runtime charges the real request.
const fn receipt_probe_bytes() -> usize {
    RECEIPT_PROBE_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_unit_decode_rejects_extra_path_before_its_typed_value() {
        let entry = serde_json::json!({"page_sha256": "00", "child_index": 0});
        let mut value = serde_json::json!({
            "role": "kv", "root_b64": "AA", "path": vec![entry; 8],
            "leaf": {"first_b64url":"YQ", "last_b64url":"YQ", "rows":1, "bytes":1, "digest":"00"}
        });
        let raw = serde_json::to_vec(&value).expect("fixture");
        assert!(serde_json::from_slice::<DirectoryPosition>(&raw).is_ok());
        for count in [0, 1, 7] {
            let mut shorter = value.clone();
            shorter["path"]
                .as_array_mut()
                .expect("path")
                .truncate(count);
            assert!(
                serde_json::from_slice::<DirectoryPosition>(
                    &serde_json::to_vec(&shorter).expect("shorter fixture")
                )
                .is_ok()
            );
        }
        let valid_entry = value["path"][0].clone();
        value["path"]
            .as_array_mut()
            .expect("path")
            .push(valid_entry);
        assert!(
            serde_json::from_slice::<DirectoryPosition>(
                &serde_json::to_vec(&value).expect("nine entries")
            )
            .is_err()
        );
        value["path"].as_array_mut().expect("path").pop();
        value["path"]
            .as_array_mut()
            .expect("path")
            .push(serde_json::Value::Null);
        let raw = serde_json::to_vec(&value).expect("extra fixture");
        let error = serde_json::from_slice::<DirectoryPosition>(&raw).expect_err("extra entry");
        assert!(
            error
                .to_string()
                .contains("restore record list exceeds its declared bound"),
            "{error}"
        );
    }

    #[test]
    fn bounded_unit_decode_unknown_pre_kind_value_is_not_buffered() {
        fn rejected<T: DeserializeOwned>(raw: &[u8]) -> bool {
            serde_json::from_slice::<T>(raw).is_err()
        }
        let raw = format!(
            "{{\"unknown\":[{}0],\"kind\":\"start\"}}",
            "0,".repeat(10_000)
        );
        for parse in [
            rejected::<GlobalCut>,
            rejected::<SideCursor>,
            rejected::<SingletonState>,
            rejected::<UnitReservationV1>,
        ] {
            let mut rejected = false;
            let observed = allocation_counter::measure(|| rejected = parse(raw.as_bytes()));
            println!(
                "unit unknown pre-kind rejected={rejected} allocated={}",
                observed.bytes_total
            );
            assert!(
                observed.bytes_total < 64 * 1024,
                "unknown value allocated {} bytes",
                observed.bytes_total
            );
            assert!(rejected);
        }
    }

    // Integration fixtures construct a selected expected plan from a real Plan7.
    // These unit tests retain behavioral red cases without creating authority.
    #[test]
    fn exact_jcs_rejects_whitespace_unknown_fields_and_duplicate_keys() {
        let whitespace = br#" {"record_type":"control_mvp_restore_progress_selector"}"#;
        assert!(
            decode_jcs_exact::<serde_json::Value>(whitespace, UNIT_RECORD_BYTES, "test").is_err()
        );
        let duplicate = br#"{"a":1,"a":2}"#;
        assert!(
            decode_jcs_exact::<serde_json::Value>(duplicate, UNIT_RECORD_BYTES, "test").is_err()
        );
    }

    #[test]
    fn canonical_binary_and_digest_are_not_normalized() {
        assert!(canonical_binary("YQ", "test", true).is_ok());
        assert!(canonical_binary("YQ==", "test", true).is_err());
        assert!(raw_digest("sha256:ABC").is_err());
        assert!(raw_digest("abc").is_err());
    }

    fn expected() -> ExpectedPlan<'static> {
        let identity = Box::leak(Box::new(
            RestoreAttemptIdentity::new("rst_01ARZ3NDEKTSV4RRFFQ69G5FAV", 1, "catalog")
                .expect("canonical fixture identity"),
        ));
        ExpectedPlan {
            plan_sha256: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            candidate_id: "1111111111111111111111111111111111111111111111111111111111111111",
            candidate_seed_sha256: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            identity,
            owner_generation: 1,
            source_kv_root_b64: "AA",
            base_kv_root_b64: "AQ",
            prefix: "control/v1/domains/catalog/restore/v7/1111111111111111111111111111111111111111111111111111111111111111".into(),
        }
    }

    #[test]
    fn candidate_scoped_paths_are_exact_and_ordinals_are_twenty_digits() {
        let expected = expected();
        assert_eq!(
            expected.candidate_id(),
            "1111111111111111111111111111111111111111111111111111111111111111"
        );
        assert_eq!(
            expected.selector_path(),
            "control/v1/domains/catalog/restore/v7/1111111111111111111111111111111111111111111111111111111111111111/selector.json"
        );
        assert_eq!(
            expected.progress_path(7),
            "control/v1/domains/catalog/restore/v7/1111111111111111111111111111111111111111111111111111111111111111/progress/00000000000000000007.json"
        );
        assert_eq!(
            expected.receipt_path(u64::MAX),
            "control/v1/domains/catalog/restore/v7/1111111111111111111111111111111111111111111111111111111111111111/receipts/18446744073709551615.json"
        );
    }

    #[test]
    fn output_identity_uses_nul_tag_and_all_frozen_tuple_fields() {
        let expected = expected();
        let actual = output_id(&expected, 3, 2, physical::Role::Kv).expect("output id");
        let changed_part = output_id(&expected, 3, 3, physical::Role::Kv).expect("output id");
        let literal_backslash_zero = tagged_raw_hex(
            b"arco/control-v2/restore-output-v1\\0",
            &OutputIdBody {
                candidate_seed_sha256: expected.candidate_seed_sha256,
                ordinal: 3,
                part: 2,
                role: physical::Role::Kv,
            },
        )
        .expect("alternate tag digest");
        assert_ne!(actual, changed_part);
        assert_ne!(actual, literal_backslash_zero);
        assert_eq!(actual.len(), 64);
        assert!(validate_candidate_id(&actual).is_ok());
    }

    #[test]
    fn singleton_rejects_skipped_phase_and_global_advance_before_complete() {
        let pending = SingletonState::Pending {
            key_b64url: "aw".into(),
            source: None,
            current: Some(Box::new(SingletonValueWitness {
                generation: 0,
                tombstone: true,
                value_length: 0,
                value_sha256:
                    "sha256:0000000000000000000000000000000000000000000000000000000000000000".into(),
                descriptor: ImmutableObjectWitness {
                    path: "x".into(),
                    byte_size: 1,
                    sha256:
                        "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                            .into(),
                },
                index: ImmutableObjectWitness {
                    path: "y".into(),
                    byte_size: 1,
                    sha256:
                        "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                            .into(),
                },
                block: BlockWitness {
                    offset: 0,
                    length: 1,
                    sha256:
                        "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                            .into(),
                    rows: 1,
                    min_key_b64url: "aw".into(),
                    max_key_b64url: "aw".into(),
                },
                row_ordinal: 0,
            })),
            phase: SingletonPhase::CompareSource,
        };
        let emit = match pending.clone() {
            SingletonState::Pending {
                key_b64url,
                source,
                current,
                ..
            } => SingletonState::Pending {
                key_b64url,
                source,
                current,
                phase: SingletonPhase::Emit,
            },
            _ => unreachable!("fixture is pending"),
        };
        assert!(validate_singleton_transition(&pending, &emit).is_err());
        let before = zero_cursor();
        let after = MergeCursor {
            global: GlobalCut::After {
                key_b64url: "aw".into(),
            },
            source: SideCursor::Start,
            current: SideCursor::Start,
        };
        assert!(
            validate_cursor_transition(&expected(), &before, &after, &pending, &pending,).is_err()
        );
    }
}

#[cfg(test)]
mod behavioral_tests {
    #![allow(clippy::expect_used, clippy::indexing_slicing)]

    use super::*;

    #[tokio::test]
    #[allow(
        clippy::large_futures,
        reason = "exercise fixed stack product slots without an unaccounted heap box"
    )]
    async fn standard_window_reads_merges_and_assembles_actual_unit() {
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"standard window");
        let e = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        seed_selected_read_case(&store, &e, 14).await;
        for ordinal in 0..2 {
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
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
            .expect("owned plan");
            let expected =
                admitted_expected_plan(&mut io, &mut route, &owned).expect("admitted expected");
            let selected = read_selected_progress(&mut io, &mut route, &expected)
                .await
                .expect("selected");
            let unit = window::prepare(&mut io, &mut route, &owned, &expected, &selected)
                .await
                .expect("bounded window must assemble from authenticated input");
            assert_eq!(
                io.writing_evidence().0,
                if ordinal == 0 { 3 } else { 0 },
                "assembly writes only physical products"
            );
            assert_eq!(
                publication::publish(&mut io, &mut route, unit)
                    .await
                    .expect("publish"),
                publication::Publication::Written
            );
            let next = read_selected_progress(&mut io, &mut route, &expected)
                .await
                .expect("published progress");
            assert_eq!(next.progress.value().next_ordinal, ordinal + 1);
            assert_eq!(next.progress.value().terminal, ordinal == 1);
            assert_eq!(io.allocation_underestimates(), 0);
        }
    }

    #[tokio::test]
    async fn durable_unit_publishes_all_records_before_selecting_progress() {
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"durable unit");
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let expected = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
            ExpectedPlan::from_selected(&plan, &digest)
        })
        .expect("expected");
        seed_selected_read_case(&store, expected.value(), 14).await;
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .expect("selected");
        let receipt = decode_with_reservation(&mut io, &mut route, Some(2 * 1024 * 1024), || {
            Ok(receipt(
                expected.value(),
                0,
                genesis_receipt_raw_sha256(expected.value())?,
                genesis_chain_sha256(expected.value())?,
                zero_cursor(),
                MergeCursor {
                    global: GlobalCut::End,
                    source: SideCursor::End,
                    current: SideCursor::End,
                },
                CumulativeSemanticCounts::default(),
            ))
        })
        .expect("receipt");
        let progress = decode_with_reservation(&mut io, &mut route, Some(2 * 1024 * 1024), || {
            Ok(progress_after(
                expected.value(),
                receipt.value(),
                &encode(receipt.value(), "receipt"),
                true,
            ))
        })
        .expect("progress");
        let unit = publication::assemble(
            &mut io,
            &mut route,
            &expected,
            &selected,
            &receipt,
            &progress,
            &[],
        )
        .expect("assembled");
        assert_eq!(
            publication::publish(&mut io, &mut route, unit)
                .await
                .expect("published"),
            publication::Publication::Written
        );
        let written = store
            .storage
            .head(&expected.value().receipt_path(0))
            .await
            .expect("receipt HEAD");
        assert!(
            written.is_some(),
            "receipt must be durable before selected progress can advance"
        );
        let current = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .expect("current");
        assert_eq!(current.progress.value().next_ordinal, 1);
        assert!(current.progress.value().terminal);
    }

    fn durable_unit_models(
        io: &mut RestorePhysicalIo<'_>,
        route: &mut RestorePhysicalRoute<'_, '_>,
        expected: &WorkingValue<ExpectedPlan<'_>>,
    ) -> (
        WorkingValue<ControlMvpRestoreReceiptV1>,
        WorkingValue<RestoreProgressV1>,
    ) {
        let receipt = decode_with_reservation(io, route, Some(2 * 1024 * 1024), || {
            Ok(receipt(
                expected.value(),
                0,
                genesis_receipt_raw_sha256(expected.value())?,
                genesis_chain_sha256(expected.value())?,
                zero_cursor(),
                MergeCursor {
                    global: GlobalCut::End,
                    source: SideCursor::End,
                    current: SideCursor::End,
                },
                CumulativeSemanticCounts::default(),
            ))
        })
        .expect("receipt");
        let progress = decode_with_reservation(io, route, Some(2 * 1024 * 1024), || {
            Ok(progress_after(
                expected.value(),
                receipt.value(),
                &encode(receipt.value(), "receipt"),
                true,
            ))
        })
        .expect("progress");
        (receipt, progress)
    }

    #[tokio::test]
    async fn durable_unit_publication_faults_reconcile_without_resending() {
        {
            let final_route = false; // Control transport is ordinary-only; rejection has separate coverage.
            for at in 3..=5 {
                for after in [false, true] {
                    for pending in [false, true] {
                        durable_unit_fault_case(final_route, at, after, pending).await;
                    }
                }
            }
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "keep cancellation and retained target lifetimes visible through fresh recovery"
    )]
    async fn durable_unit_fault_case(final_route: bool, at: usize, after: bool, pending: bool) {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        let (_, plan) = super::super::tests::inspection_fixture().await;
        let store = physical::restore_io::unit_publication_test_store(at, after, pending);
        let digest = prefixed_sha256(b"durable fault matrix");
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
        let mut route = if final_route {
            RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
        } else {
            RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            }
        };
        let expected = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
            ExpectedPlan::from_selected(&plan, &digest)
        })
        .expect("expected");
        seed_selected_read_case(&store, expected.value(), 14).await;
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .expect("selected");
        let (receipt, progress) = durable_unit_models(&mut io, &mut route, &expected);
        let live = io.live_ownership_evidence();
        let unit = publication::assemble(
            &mut io,
            &mut route,
            &expected,
            &selected,
            &receipt,
            &progress,
            &[],
        )
        .expect("assembly");
        let target = unit.recovery_target();
        let reads = io.reading_evidence();
        if pending {
            let mut future = Box::pin(publication::publish(&mut io, &mut route, unit));
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
            drop(future);
            assert_eq!(io.live_ownership_evidence(), live);
        } else {
            assert!(
                publication::publish(&mut io, &mut route, unit)
                    .await
                    .is_err()
            );
        }
        assert_eq!(io.writing_evidence().0, (at - 2) as u64);
        assert_eq!(
            io.reading_evidence(),
            reads,
            "uncertain outcome must not auto-read"
        );
        assert_eq!(io.allocation_underestimates(), 0);
        let writes = io.writing_evidence();
        assert!(
            publication::reconcile(&mut io, &mut route, &expected, target)
                .await
                .is_err()
        );
        assert_eq!(io.reading_evidence(), reads);
        assert_eq!(io.writing_evidence(), writes);
        // The fixed target survives dropping all old invocation heap owners.
        drop((expected, selected, receipt, progress, io));
        let mut recovery = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let expected = decode_with_reservation(&mut recovery, &mut route, Some(64 * 1024), || {
            ExpectedPlan::from_selected(&plan, &digest)
        })
        .expect("recovery expected");
        let observed = publication::reconcile(&mut recovery, &mut route, &expected, target)
            .await
            .expect("fresh read-only recovery");
        assert_eq!(
            observed,
            if at == 5 && after {
                publication::Reconciliation::Exact
            } else {
                publication::Reconciliation::Prior
            }
        );
        assert_eq!(recovery.writing_evidence(), (0, 0));
        assert_eq!(recovery.allocation_underestimates(), 0);
    }

    #[tokio::test]
    async fn durable_unit_identical_writers_use_original_version_cas() {
        let (_, plan) = super::super::tests::inspection_fixture().await;
        let store = physical::restore_io::unit_publication_barrier_store();
        let digest = prefixed_sha256(b"barrier contenders");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        seed_selected_read_case(&store, &expected, 14).await;
        let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(
                durable_unit_contender(&store, &plan, &digest),
                durable_unit_contender(&store, &plan, &digest)
            )
        })
        .await
        .expect("both writers reached CAS barrier");
        assert!(matches!(
            (&first, &second),
            (
                publication::Publication::Written,
                publication::Publication::ExactSelected
            ) | (
                publication::Publication::ExactSelected,
                publication::Publication::Written
            )
        ));
    }

    async fn durable_unit_contender(
        store: &super::super::super::super::ControlMvpStateStore,
        plan: &ControlMvpRestorePlanV7,
        digest: &str,
    ) -> publication::Publication {
        let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let expected = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
            ExpectedPlan::from_selected(plan, digest)
        })
        .expect("expected");
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .expect("selected");
        let (receipt, progress) = durable_unit_models(&mut io, &mut route, &expected);
        let unit = publication::assemble(
            &mut io,
            &mut route,
            &expected,
            &selected,
            &receipt,
            &progress,
            &[],
        )
        .expect("assembly");
        let outcome = publication::publish(&mut io, &mut route, unit)
            .await
            .expect("publication");
        assert_eq!(io.writing_evidence().0, 3);
        assert_eq!(io.allocation_underestimates(), 0);
        outcome
    }

    #[tokio::test]
    async fn durable_unit_exact_bytes_stale_versions_and_read_only_recovery() {
        {
            let final_route = false; // Control transport is ordinary-only; rejection has separate coverage.
            for case in 0..7 {
                durable_unit_observation_case(final_route, case).await;
            }
        }
    }

    async fn durable_unit_install_later(
        store: &super::super::super::super::ControlMvpStateStore,
        expected: &ExpectedPlan<'_>,
    ) {
        let (selector_raw, progress, receipt, ordinal) = selected_read_fixture(expected, 18);
        for (record, raw) in [
            (RestoreControlRecord::Progress(ordinal), progress),
            (RestoreControlRecord::Receipt(ordinal - 1), receipt),
        ] {
            store
                .storage
                .put(
                    &record.path(&expected.prefix),
                    bytes::Bytes::from(raw),
                    arco_core::AuthorityWritePrecondition::DoesNotExist,
                )
                .await
                .expect("later immutable");
        }
        let path = expected.selector_path();
        let meta = store
            .storage
            .head(&path)
            .await
            .expect("head")
            .expect("selector");
        store
            .storage
            .put(
                &path,
                bytes::Bytes::from(selector_raw),
                arco_core::AuthorityWritePrecondition::MatchesVersion(meta.version),
            )
            .await
            .expect("later selector");
    }

    #[allow(
        clippy::too_many_lines,
        clippy::cognitive_complexity,
        reason = "end-to-end ordered publication and fresh recovery scenario"
    )]
    async fn durable_unit_observation_case(final_route: bool, case: u8) {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        use publication::{Publication, Reconciliation};
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"observation matrix");
        let other_digest = prefixed_sha256(b"other plan");
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
        let mut route = if final_route {
            RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
        } else {
            RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            }
        };
        let expected = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
            ExpectedPlan::from_selected(&plan, &digest)
        })
        .expect("expected");
        seed_selected_read_case(&store, expected.value(), 14).await;
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .expect("selected");
        let (receipt, progress) = durable_unit_models(&mut io, &mut route, &expected);
        let unit = publication::assemble(
            &mut io,
            &mut route,
            &expected,
            &selected,
            &receipt,
            &progress,
            &[],
        )
        .expect("assembly");
        let target = unit.recovery_target();
        if case == 1 {
            store
                .storage
                .put(
                    &expected.value().selector_path(),
                    bytes::Bytes::copy_from_slice(selected.selector_raw.as_slice()),
                    arco_core::AuthorityWritePrecondition::MatchesVersion(
                        selected.selector_meta.value().version.clone(),
                    ),
                )
                .await
                .expect("same bytes new version");
        }
        if case == 2 {
            durable_unit_install_later(&store, expected.value()).await;
        }
        if case == 4 {
            store
                .storage
                .put(
                    &expected.value().receipt_path(0),
                    bytes::Bytes::from_static(b"wrong"),
                    arco_core::AuthorityWritePrecondition::DoesNotExist,
                )
                .await
                .expect("collision");
        }
        let outcome = publication::publish(&mut io, &mut route, unit).await;
        match case {
            1 => assert_eq!(
                outcome.expect("stale CAS"),
                Publication::Conflict(Reconciliation::Prior)
            ),
            2 => assert_eq!(
                outcome.expect("different CAS winner"),
                Publication::Conflict(Reconciliation::Different)
            ),
            4 => assert!(outcome.is_err()),
            _ => assert_eq!(outcome.expect("written"), Publication::Written),
        }
        if matches!(case, 1 | 2) {
            let reads = io.reading_evidence();
            assert!(
                read_selected_progress(&mut io, &mut route, &expected)
                    .await
                    .is_err(),
                "non-exact CAS conflict must stop the publisher route"
            );
            assert_eq!(io.reading_evidence(), reads);
        }
        assert_eq!(io.writing_evidence().0, if case == 4 { 1 } else { 3 });
        assert_eq!(io.allocation_underestimates(), 0);
        if case == 0 {
            for (path, raw) in [
                (
                    expected.value().receipt_path(0),
                    encode(receipt.value(), "receipt"),
                ),
                (
                    expected.value().progress_path(1),
                    encode(progress.value(), "progress"),
                ),
                (
                    expected.value().selector_path(),
                    encode(
                        &selector(
                            expected.value(),
                            progress.value(),
                            &encode(progress.value(), "progress"),
                        ),
                        "selector",
                    ),
                ),
            ] {
                assert_eq!(store.storage.get(&path).await.expect("raw").as_ref(), raw);
            }
        }
        if case == 3 {
            durable_unit_install_later(&store, expected.value()).await;
        }
        if case == 5 {
            let path = expected.value().progress_path(1);
            let meta = store
                .storage
                .head(&path)
                .await
                .expect("head")
                .expect("progress");
            store
                .storage
                .put(
                    &path,
                    bytes::Bytes::from_static(b"{}"),
                    arco_core::AuthorityWritePrecondition::MatchesVersion(meta.version),
                )
                .await
                .expect("corruption");
        }
        drop((expected, selected, receipt, progress, io));
        let mut recovery = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let expected = decode_with_reservation(&mut recovery, &mut route, Some(64 * 1024), || {
            ExpectedPlan::from_selected(&plan, if case == 6 { &other_digest } else { &digest })
        })
        .expect("expected");
        let outcome = publication::reconcile(&mut recovery, &mut route, &expected, target).await;
        match case {
            1 | 4 => assert_eq!(outcome.expect("prior"), Reconciliation::Prior),
            2 | 3 => assert_eq!(outcome.expect("different"), Reconciliation::Different),
            5 | 6 => assert!(outcome.is_err()),
            _ => assert_eq!(outcome.expect("exact"), Reconciliation::Exact),
        }
        if case == 6 {
            assert_eq!(recovery.reading_evidence(), (0, 0, 0));
        }
        assert_eq!(recovery.writing_evidence(), (0, 0));
        assert_eq!(recovery.allocation_underestimates(), 0);
    }

    async fn durable_unit_output_fixture(
        io: &mut RestorePhysicalIo<'_>,
        route: &mut RestorePhysicalRoute<'_, '_>,
        expected: &WorkingValue<ExpectedPlan<'_>>,
    ) -> physical::restore_io::StandardRestoreOutput {
        let id =
            codec::hashes::output_id(io, route, expected, 0, 0, physical::Role::Kv).expect("id");
        let rows = decode_with_reservation(io, route, Some(64 * 1024), || {
            Ok(vec![ControlMvpSegmentRow {
                record_kind: SEGMENT_RECORD_KV,
                key: b"a".to_vec(),
                value: Some(b"value".to_vec()),
                generation: 1,
                tombstone: false,
                logical_sequence: 1,
                logical_ordinal: 0,
                origin_sequence: None,
            }])
        })
        .expect("rows");
        physical::restore_io::write_standard_restore_output(io, route, id.value(), 1, &rows)
            .await
            .expect("output")
    }

    #[tokio::test]
    async fn durable_unit_assembly_rejects_unbound_records_before_control_writes() {
        {
            let final_route = false; // Control transport is ordinary-only; rejection has separate coverage.
            for case in 0..7 {
                durable_unit_assembly_case(final_route, case).await;
            }
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "retain all guarded fixture products across assembly admission"
    )]
    async fn durable_unit_assembly_case(final_route: bool, case: u8) {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"assembly matrix");
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut foreign = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
        let mut route = if final_route {
            RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
        } else {
            RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            }
        };
        let expected = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
            ExpectedPlan::from_selected(&plan, &digest)
        })
        .expect("expected");
        seed_selected_read_case(&store, expected.value(), 14).await;
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .expect("selected");
        let output = if case >= 3 {
            Some(durable_unit_output_fixture(&mut io, &mut route, &expected).await)
        } else {
            None
        };
        let mut foreign_workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut foreign_payload = physical::restore_io::UnitPayloadAdmission::new();
        let mut foreign_route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut foreign_workspace,
            payload: &mut foreign_payload,
        };
        let receipt = decode_with_reservation(
            if case == 6 { &mut foreign } else { &mut io },
            if case == 6 {
                &mut foreign_route
            } else {
                &mut route
            },
            Some(2 * 1024 * 1024),
            || {
                let mut value = native_receipt_fixture(expected.value());
                if case != 2 {
                    value.outputs.clear();
                    value.counts.output_blocks = 0;
                    value.counts.output_encoded_bytes = 0;
                }
                if matches!(case, 0 | 4 | 5 | 6) {
                    if let Some(output) = output.as_ref() {
                        value.outputs.push(output_binding_witness(
                            &store,
                            output,
                            &output.descriptor.value().segment.segment_id,
                            u8::from(case == 5),
                        ));
                        value.counts.output_blocks = 1;
                        value.counts.output_encoded_bytes = output.descriptor.value().block.length;
                    }
                }
                if case == 1 {
                    value.predecessor_chain_sha256 = prefixed_sha256(b"wrong chain");
                }
                rehash_receipt(&mut value);
                Ok(value)
            },
        )
        .expect("receipt");
        let progress = decode_with_reservation(&mut io, &mut route, Some(2 * 1024 * 1024), || {
            Ok(progress_after(
                expected.value(),
                receipt.value(),
                &encode(receipt.value(), "receipt"),
                false,
            ))
        })
        .expect("progress");
        let writes = io.writing_evidence();
        let reads = io.reading_evidence();
        let unit = publication::assemble(
            &mut io,
            &mut route,
            &expected,
            &selected,
            &receipt,
            &progress,
            output.as_slice(),
        );
        assert_eq!(
            unit.is_ok(),
            matches!(case, 0 | 4),
            "case={case} final={final_route}: {:?}",
            unit.as_ref().err()
        );
        assert_eq!(io.writing_evidence(), writes);
        assert_eq!(io.reading_evidence(), reads);
        if let Ok(unit) = unit {
            assert_eq!(
                publication::publish(&mut io, &mut route, unit)
                    .await
                    .expect("publish"),
                publication::Publication::Written
            );
        }
        assert_eq!(io.allocation_underestimates(), 0);
    }

    #[tokio::test]
    async fn durable_unit_non_genesis_continuity_and_later_selection_are_distinct() {
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"two durable units");
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let expected = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
            ExpectedPlan::from_selected(&plan, &digest)
        })
        .expect("expected");
        seed_selected_read_case(&store, expected.value(), 14).await;
        let mut first = None;
        for ordinal in 0..2 {
            let selected = read_selected_progress(&mut io, &mut route, &expected)
                .await
                .expect("selected");
            assert_eq!(selected.progress.value().next_ordinal, ordinal);
            let receipt =
                decode_with_reservation(&mut io, &mut route, Some(2 * 1024 * 1024), || {
                    Ok(proposed_receipt_fixture(
                        expected.value(),
                        selected.progress.value(),
                        0,
                    ))
                })
                .expect("receipt");
            let progress =
                decode_with_reservation(&mut io, &mut route, Some(2 * 1024 * 1024), || {
                    Ok(progress_after(
                        expected.value(),
                        receipt.value(),
                        &encode(receipt.value(), "receipt"),
                        is_end_cursor(&receipt.value().after),
                    ))
                })
                .expect("progress");
            let unit = publication::assemble(
                &mut io,
                &mut route,
                &expected,
                &selected,
                &receipt,
                &progress,
                &[],
            )
            .expect("assembly");
            let target = unit.recovery_target();
            assert_eq!(
                publication::publish(&mut io, &mut route, unit)
                    .await
                    .expect("publication"),
                publication::Publication::Written
            );
            assert_eq!(
                publication::reconcile(&mut io, &mut route, &expected, target)
                    .await
                    .expect("exact"),
                publication::Reconciliation::Exact
            );
            if ordinal == 0 {
                first = Some(target);
            }
        }
        assert_eq!(
            publication::reconcile(&mut io, &mut route, &expected, first.expect("first target"))
                .await
                .expect("later selection"),
            publication::Reconciliation::Different
        );
        assert_eq!(io.writing_evidence().0, 6);
        assert_eq!(io.allocation_underestimates(), 0);
    }

    #[tokio::test]
    async fn durable_unit_preflight_preserves_no_effect_on_foreign_or_exhausted_owners() {
        for final_route in [false, true] {
            for case in 0..3 {
                durable_unit_admission_case(final_route, case).await;
            }
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "keep original and foreign owner lifetimes explicit in preflight matrix"
    )]
    async fn durable_unit_admission_case(final_route: bool, case: u8) {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"durable admission");
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut foreign = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let expected = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
            ExpectedPlan::from_selected(&plan, &digest)
        })
        .expect("expected");
        seed_selected_read_case(&store, expected.value(), 14).await;
        let selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .expect("selected");
        let (receipt, progress) = durable_unit_models(&mut io, &mut route, &expected);
        let unit = publication::assemble(
            &mut io,
            &mut route,
            &expected,
            &selected,
            &receipt,
            &progress,
            &[],
        )
        .expect("assembly");
        let target = unit.recovery_target();
        let owner = if case == 0 { &mut foreign } else { &mut io };
        let held = if case != 0 && !final_route {
            let remaining = 64 * 1024 * 1024 - owner.live_ownership_evidence().0;
            Some(
                decode_with_reservation(owner, &mut route, Some(remaining), || {
                    Ok(vec![0_u8; remaining - 32 * 1024])
                })
                .expect("carry"),
            )
        } else {
            None
        };
        let mut totals = FinalStreamTotals::new();
        let carry = if case != 0 && final_route {
            64 * 1024 * 1024 - owner.live_ownership_evidence().0 - 64 * 1024 + 1
        } else {
            0
        };
        let mut chunk = FinalMicrochunk::begin(&mut totals, carry, owner).expect("chunk");
        let mut route = if final_route {
            RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
        } else {
            route
        };
        let reads = owner.reading_evidence();
        let hashes = owner.hashing_evidence();
        if case == 2 {
            assert!(
                publication::assemble(
                    owner,
                    &mut route,
                    &expected,
                    &selected,
                    &receipt,
                    &progress,
                    &[]
                )
                .is_err()
            );
            drop(unit);
        } else {
            assert!(publication::publish(owner, &mut route, unit).await.is_err());
        }
        assert_eq!(owner.writing_evidence(), (0, 0));
        assert_eq!(owner.reading_evidence(), reads);
        assert_eq!(owner.hashing_evidence(), hashes);
        assert_eq!(owner.allocation_underestimates(), 0);
        drop(held);
        assert!(
            publication::reconcile(owner, &mut route, &expected, target)
                .await
                .is_err()
        );
        assert_eq!(owner.reading_evidence(), reads);
    }

    #[tokio::test]
    async fn output_binding_rejects_substituted_objects() {
        let mut accepted = Vec::new();
        for case in 0..3 {
            accepted.push(output_binding_case(false, case).await);
        }
        assert_eq!(
            accepted,
            vec![true, false, false],
            "shape validation cannot bind a witness to persisted output"
        );
    }

    #[tokio::test]
    async fn output_binding_preserves_sparse_part_ids() {
        let mut accepted = Vec::new();
        for final_route in [false, true] {
            for case in [28, 29] {
                accepted.push(output_binding_case(final_route, case).await);
            }
        }
        assert_eq!(
            accepted,
            vec![true; 4],
            "part is a u32 identity, not the output list length"
        );
    }

    fn output_binding_witness(
        store: &super::super::super::super::ControlMvpStateStore,
        output: &physical::restore_io::StandardRestoreOutput,
        id: &str,
        case: u8,
    ) -> OutputWitness {
        let d = output.descriptor.value();
        let mut value = OutputWitness {
            role: physical::Role::Kv,
            output_id: id.into(),
            part: 0,
            directory_leaf: OutputDirectoryLeafWitness {
                first_key_b64url: "YQ".into(),
                last_key_b64url: "YQ".into(),
                rows: 1,
                bytes: u32::try_from(d.block.length).expect("length"),
                digest: prefixed_sha256(output.bytes.value()),
            },
            descriptor: ImmutableObjectWitness {
                path: format!(
                    "{}/physical/descriptors/{}.json",
                    store.paths.base_prefix(),
                    hex::encode(output.digest)
                ),
                byte_size: output.bytes.value().len() as u64,
                sha256: prefixed_sha256(output.bytes.value()),
            },
            index: ImmutableObjectWitness {
                path: store.paths.segment_index(id),
                byte_size: d.segment.index_size_bytes,
                sha256: format!("sha256:{}", d.segment.index_checksum_sha256),
            },
            block: BlockWitness {
                offset: 0,
                length: d.block.length,
                sha256: format!("sha256:{}", d.block.checksum_sha256),
                rows: 1,
                min_key_b64url: "YQ".into(),
                max_key_b64url: "YQ".into(),
            },
        };
        match case {
            1 => value.index.path.push_str("wrong"),
            2 => value.descriptor.path.push_str("wrong"),
            3 => value.index.byte_size += 1,
            4 => value.index.sha256 = prefixed_sha256(b"wrong"),
            5 => value.descriptor.byte_size += 1,
            6 => value.descriptor.sha256 = prefixed_sha256(b"wrong"),
            7 => value.directory_leaf.digest = prefixed_sha256(b"wrong"),
            8 => value.directory_leaf.rows += 1,
            9 => value.directory_leaf.bytes += 1,
            10 => value.directory_leaf.first_key_b64url = "YA".into(),
            11 => value.directory_leaf.last_key_b64url = "Yg".into(),
            12 => value.block.offset += 1,
            13 => value.block.length += 1,
            14 => value.block.sha256 = prefixed_sha256(b"wrong"),
            15 => value.block.rows += 1,
            16 => value.block.min_key_b64url = "YA".into(),
            17 => value.block.max_key_b64url = "Yg".into(),
            18 => value.role = physical::Role::ActiveId,
            19 => value.output_id = "f".repeat(64),
            20 => value.part = 1,
            27 => value.index.path = "x".repeat(UNIT_RECORD_BYTES),
            28 => value.part = 32,
            29 => value.part = u32::MAX,
            _ => {}
        }
        value
    }

    #[tokio::test]
    async fn output_binding_matrix_has_exact_physical_fields() {
        for final_route in [false, true] {
            for case in 0..30 {
                assert_eq!(
                    output_binding_case(final_route, case).await,
                    matches!(case, 0 | 28 | 29),
                    "case={case} final={final_route}"
                );
            }
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "keep output, model and route ownership lifetimes visible through the matrix"
    )]
    async fn output_binding_case(final_route: bool, case: u8) -> bool {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"output binding");
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut foreign = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let expected = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
            ExpectedPlan::from_selected(&plan, &digest)
        })
        .expect("expected");
        let id = codec::hashes::output_id(
            &mut io,
            &mut route,
            &expected,
            0,
            match case {
                28 => 32,
                29 => u32::MAX,
                _ => 0,
            },
            physical::Role::Kv,
        )
        .expect("id");
        let rows = decode_with_reservation(
            if case == 23 { &mut foreign } else { &mut io },
            &mut route,
            Some(64 * 1024),
            || {
                Ok(vec![ControlMvpSegmentRow {
                    record_kind: SEGMENT_RECORD_KV,
                    key: b"a".to_vec(),
                    value: Some(b"value".to_vec()),
                    generation: 1,
                    tombstone: false,
                    logical_sequence: 1,
                    logical_ordinal: 0,
                    origin_sequence: None,
                }])
            },
        )
        .expect("rows");
        let mut output = physical::restore_io::write_standard_restore_output(
            if case == 23 { &mut foreign } else { &mut io },
            &mut route,
            id.value(),
            1,
            &rows,
        )
        .await
        .expect("persisted output");
        let witness = decode_with_reservation(
            if case == 21 { &mut foreign } else { &mut io },
            &mut route,
            Some(8 * 1024 * 1024),
            || Ok(output_binding_witness(&store, &output, id.value(), case)),
        )
        .expect("witness");
        let other = decode_with_reservation(&mut foreign, &mut route, Some(64 * 1024), || {
            ExpectedPlan::from_selected(&plan, &digest)
        })
        .expect("foreign expected");
        if case == 25 {
            output.digest = [0; 32];
        }
        if case == 26 {
            output.bytes = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                Ok(bytes::Bytes::from_static(b"wrong"))
            })
            .expect("wrong bytes");
        }
        let held = if case == 24 && !final_route {
            let remaining = 64 * 1024 * 1024 - io.live_ownership_evidence().0;
            Some(
                decode_with_reservation(&mut io, &mut route, Some(remaining), || {
                    Ok(vec![0_u8; remaining - 32 * 1024])
                })
                .expect("carry"),
            )
        } else {
            None
        };
        let mut totals = FinalStreamTotals::new();
        let carry = if case == 24 && final_route {
            64 * 1024 * 1024 - io.live_ownership_evidence().0 - 64 * 1024 + 1
        } else {
            0
        };
        let mut chunk = FinalMicrochunk::begin(&mut totals, carry, &mut io).expect("chunk");
        let mut route = if final_route {
            RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
        } else {
            route
        };
        let reads = io.reading_evidence();
        let hashes = io.hashing_evidence();
        let live = io.allocation_evidence().1;
        let checked = validate_persisted_output(
            &mut io,
            &mut route,
            if case == 22 { &other } else { &expected },
            0,
            &witness,
            &output,
        );
        let accepted = checked.is_ok();
        drop(checked);
        assert_eq!(io.reading_evidence(), reads);
        assert_eq!(io.allocation_underestimates(), 0);
        if accepted {
            assert_eq!(io.allocation_evidence().1, live);
        } else {
            assert!(io.allocation_evidence().1 <= live + 64 * 1024);
        }
        if matches!(case, 21..=24 | 27) {
            assert_eq!(io.hashing_evidence(), hashes);
        }
        drop(held);
        if !accepted {
            let hashes = io.hashing_evidence();
            assert!(
                validate_persisted_output(&mut io, &mut route, &expected, 0, &witness, &output)
                    .is_err()
            );
            assert_eq!(io.hashing_evidence(), hashes);
            assert_eq!(io.reading_evidence(), reads);
        }
        accepted
    }

    #[tokio::test]
    async fn proposed_transition_rejects_disconnected_records() {
        let mut accepted = Vec::new();
        for case in 0..3 {
            let (store, plan) = super::super::tests::inspection_fixture().await;
            let digest = prefixed_sha256(b"proposed transition");
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let expected =
                decode_with_reservation(&mut io, &mut route, Some(2 * 1024 * 1024), || {
                    ExpectedPlan::from_selected(&plan, &digest)
                })
                .expect("expected");
            seed_selected_read_case(&store, expected.value(), 14).await;
            let selected = read_selected_progress(&mut io, &mut route, &expected)
                .await
                .expect("selected genesis");
            let receipt =
                decode_with_reservation(&mut io, &mut route, Some(2 * 1024 * 1024), || {
                    let mut value = native_receipt_fixture(expected.value());
                    if case == 1 {
                        value.predecessor_receipt_sha256 = prefixed_sha256(b"wrong predecessor");
                    }
                    rehash_receipt(&mut value);
                    Ok(value)
                })
                .expect("receipt");
            let progress =
                decode_with_reservation(&mut io, &mut route, Some(2 * 1024 * 1024), || {
                    let mut value = progress_after(
                        expected.value(),
                        receipt.value(),
                        &encode(receipt.value(), "receipt"),
                        false,
                    );
                    if case == 2 {
                        value.chain_sha256 = prefixed_sha256(b"wrong selected edge");
                    }
                    Ok(value)
                })
                .expect("progress");
            let checked = validate_proposed_transition(
                &mut io, &mut route, &expected, &selected, &receipt, &progress,
            );
            accepted.push(checked.is_ok());
            drop(selected);
        }
        assert_eq!(
            accepted,
            vec![true, false, false],
            "standalone valid shapes must not authorize disconnected transitions"
        );
    }

    #[tokio::test]
    async fn proposed_transition_binds_every_edge_on_both_routes() {
        for final_route in [false, true] {
            for case in 0..21 {
                proposed_transition_case(final_route, case).await;
            }
        }
    }

    fn proposed_receipt_fixture(
        expected: &ExpectedPlan<'_>,
        selected: &RestoreProgressV1,
        case: u8,
    ) -> ControlMvpRestoreReceiptV1 {
        let mut value = receipt(
            expected,
            selected.next_ordinal,
            selected.last_receipt.as_ref().map_or_else(
                || genesis_receipt_raw_sha256(expected).expect("genesis"),
                |r| r.raw_sha256.clone(),
            ),
            selected.chain_sha256.clone(),
            selected.cursor.clone(),
            if selected.next_ordinal == 0 {
                audit_after("eg")
            } else {
                MergeCursor {
                    global: GlobalCut::End,
                    source: SideCursor::End,
                    current: SideCursor::End,
                }
            },
            selected.cumulative_counts.clone(),
        );
        match case {
            1 => value.predecessor_receipt_sha256 = prefixed_sha256(b"wrong predecessor"),
            2 => value.predecessor_chain_sha256 = prefixed_sha256(b"wrong chain"),
            3 => value.prefix_cumulative_counts.mutations += 1,
            4 => value.before = audit_after("YQ"),
            5 => value.ordinal += 1,
            19 => {
                value.singleton_before = SingletonState::Complete {
                    key_b64url: "YQ".into(),
                }
            }
            _ => {}
        }
        rehash_receipt(&mut value);
        value
    }

    #[allow(
        clippy::too_many_lines,
        clippy::cognitive_complexity,
        reason = "one matrix keeps the selected and proposed guard lifetimes visible through admission and retry"
    )]
    async fn proposed_transition_case(final_route: bool, case: u8) {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"proposal matrix");
        let other_digest = prefixed_sha256(b"other expected plan");
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut foreign = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let expected = decode_with_reservation(&mut io, &mut route, Some(2 * 1024 * 1024), || {
            ExpectedPlan::from_selected(&plan, &digest)
        })
        .expect("expected");
        seed_selected_read_case(
            &store,
            expected.value(),
            if case == 14 {
                0
            } else if case == 16 {
                18
            } else {
                14
            },
        )
        .await;
        let mut selected = read_selected_progress(&mut io, &mut route, &expected)
            .await
            .expect("selected");
        if case == 15 {
            selected.progress =
                decode_with_reservation(&mut io, &mut route, Some(2 * 1024 * 1024), || {
                    let mut value = progress_after(
                        expected.value(),
                        &native_receipt_fixture(expected.value()),
                        b"terminal fixture",
                        true,
                    );
                    value.cursor = MergeCursor {
                        global: GlobalCut::End,
                        source: SideCursor::End,
                        current: SideCursor::End,
                    };
                    validate_progress_shape(expected.value(), &value)?;
                    Ok(value)
                })
                .expect("terminal fixture");
        }
        let receipt = decode_with_reservation(
            if case == 13 { &mut foreign } else { &mut io },
            &mut route,
            Some(2 * 1024 * 1024),
            || {
                Ok(if case == 16 {
                    native_receipt_fixture(expected.value())
                } else {
                    proposed_receipt_fixture(expected.value(), selected.progress.value(), case)
                })
            },
        )
        .expect("proposal");
        let progress = decode_with_reservation(
            if case == 17 { &mut foreign } else { &mut io },
            &mut route,
            Some(2 * 1024 * 1024),
            || {
                let mut value = progress_after(
                    expected.value(),
                    receipt.value(),
                    &encode(receipt.value(), "proposal"),
                    is_end_cursor(&receipt.value().after),
                );
                match case {
                    6 => {
                        value.last_receipt.as_mut().expect("last").raw_sha256 =
                            prefixed_sha256(b"wrong raw");
                    }
                    7 => value.chain_sha256 = prefixed_sha256(b"wrong chain"),
                    8 => value.cumulative_counts.mutations += 1,
                    9 => value.cursor = audit_after("eno"),
                    10 => {
                        value.last_receipt.as_mut().expect("last").path =
                            expected.value().receipt_path(9);
                    }
                    11 => value.receipt_count += 1,
                    20 => {
                        value.singleton_state = SingletonState::Complete {
                            key_b64url: "YQ".into(),
                        }
                    }
                    _ => {}
                }
                Ok(value)
            },
        )
        .expect("proposed progress");
        let alternate = decode_with_reservation(&mut io, &mut route, Some(2 * 1024 * 1024), || {
            ExpectedPlan::from_selected(&plan, &other_digest)
        })
        .expect("alternate");
        let held = if case == 18 && !final_route {
            let remaining = 64 * 1024 * 1024 - io.live_ownership_evidence().0;
            Some(
                decode_with_reservation(&mut io, &mut route, Some(remaining), || {
                    Ok(vec![0_u8; remaining - 32 * 1024])
                })
                .expect("retained admission fixture"),
            )
        } else {
            None
        };
        let reads = io.reading_evidence();
        let hashes = io.hashing_evidence();
        let encodes = io.encoding_evidence();
        let live = io.allocation_evidence().1;
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(
            &mut totals,
            if case == 18 && final_route {
                64 * 1024 * 1024 - io.live_ownership_evidence().0 - 64 * 1024 + 1
            } else {
                0
            },
            &mut io,
        )
        .expect("carry");
        let mut route = if final_route {
            RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
        } else {
            route
        };
        let checked = validate_proposed_transition(
            &mut io,
            &mut route,
            if case == 12 { &alternate } else { &expected },
            &selected,
            &receipt,
            &progress,
        );
        assert_eq!(
            checked.is_ok(),
            matches!(case, 0 | 14),
            "case={case} final={final_route}: {:?}",
            checked.as_ref().err()
        );
        drop(checked);
        assert_eq!(io.reading_evidence(), reads);
        if matches!(case, 0 | 14) {
            assert_eq!(io.allocation_evidence().1, live);
        } else {
            assert!(io.allocation_evidence().1 <= live + 64 * 1024);
        }
        assert_eq!(io.allocation_underestimates(), 0);
        if matches!(case, 12 | 13 | 15 | 16 | 17 | 18) {
            assert_eq!(io.hashing_evidence(), hashes);
            assert_eq!(io.encoding_evidence(), encodes);
        }
        drop(held);
        if !matches!(case, 0 | 14) {
            let hashes = io.hashing_evidence();
            let encodes = io.encoding_evidence();
            assert!(
                validate_proposed_transition(
                    &mut io, &mut route, &expected, &selected, &receipt, &progress
                )
                .is_err()
            );
            assert_eq!(io.hashing_evidence(), hashes);
            assert_eq!(io.encoding_evidence(), encodes);
        }
    }

    fn selected_read_fixture(
        expected: &ExpectedPlan<'_>,
        case: u8,
    ) -> (Vec<u8>, Vec<u8>, Vec<u8>, u64) {
        let mut receipt = native_receipt_fixture(expected);
        if case == 18 {
            receipt.ordinal = u64::MAX - 1;
            for output in &mut receipt.outputs {
                output.output_id =
                    output_id(expected, receipt.ordinal, output.part, output.role).expect("max ID");
            }
            rehash_receipt(&mut receipt);
        }
        let receipt_raw = encode(&receipt, "receipt");
        let mut progress = if case == 14 {
            genesis_progress(expected)
        } else {
            progress_after(expected, &receipt, &receipt_raw, false)
        };
        let ordinal = progress.next_ordinal;
        if case == 2 {
            progress.chain_sha256 = prefixed_sha256(b"wrong chain");
        }
        if case == 10 {
            progress.next_ordinal = 2;
            progress.receipt_count = 2;
            progress.last_receipt.as_mut().expect("reference").path = expected.receipt_path(1);
        }
        if case == 13 {
            progress.last_receipt = None;
        }
        let mut progress_raw = encode(&progress, "progress");
        if case == 11 {
            progress_raw.push(b' ');
        }
        let mut selected = selector(expected, &progress, &progress_raw);
        match case {
            1 => selected.current_progress_sha256 = prefixed_sha256(b"wrong raw"),
            3 => {
                selected.current_progress_path = "/other/progress/00000000000000000001.json".into();
            }
            4 => selected.record_type = "other".into(),
            8 => {
                selected.current_progress_path =
                    format!("{}/progress/99999999999999999999.json", expected.prefix);
            }
            9 => selected.current_progress_path = format!("{}/progress/1.json", expected.prefix),
            10 => selected.current_progress_path = expected.progress_path(1),
            16 => selected.current_progress_path.push(' '),
            17 => {
                selected.current_progress_path =
                    format!("{}/progress/%300000000000000000001.json", expected.prefix);
            }
            _ => {}
        }
        let receipt_raw = if case == 12 {
            let mut changed = receipt.clone();
            changed.prefix_cumulative_counts.mutations += 1;
            rehash_receipt(&mut changed);
            encode(&changed, "changed receipt")
        } else {
            receipt_raw
        };
        (
            encode(&selected, "selector"),
            progress_raw,
            receipt_raw,
            ordinal,
        )
    }

    #[tokio::test]
    async fn selected_progress_read_binds_records_and_bounds_lookups() {
        {
            let final_route = false; // Control transport is ordinary-only; rejection has separate coverage.
            for case in 0..19 {
                selected_read_case(final_route, case).await;
            }
        }
    }

    pub(super) async fn seed_selected_read_case(
        store: &super::super::super::super::ControlMvpStateStore,
        expected: &ExpectedPlan<'_>,
        case: u8,
    ) -> (Vec<u8>, u64) {
        let (selector_raw, progress_raw, receipt_raw, ordinal) =
            selected_read_fixture(expected, case);
        for (index, record, raw) in [
            (5, RestoreControlRecord::Selector, selector_raw.clone()),
            (6, RestoreControlRecord::Progress(ordinal), progress_raw),
            (
                7,
                RestoreControlRecord::Receipt(ordinal.saturating_sub(1)),
                receipt_raw,
            ),
        ] {
            if case == index || (ordinal == 0 && index == 7) {
                continue;
            }
            store
                .storage
                .put(
                    &record.path(&expected.prefix),
                    bytes::Bytes::from(raw),
                    arco_core::AuthorityWritePrecondition::DoesNotExist,
                )
                .await
                .expect("fixture write");
        }
        (selector_raw, ordinal)
    }

    const SELECTED_READ_IO: [(u64, u64); 19] = [
        (3, 6),
        (2, 4),
        (3, 6),
        (1, 2),
        (1, 2),
        (0, 1),
        (1, 3),
        (2, 5),
        (1, 2),
        (1, 2),
        (2, 4),
        (2, 4),
        (3, 6),
        (2, 4),
        (2, 4),
        (0, 0),
        (1, 2),
        (1, 2),
        (3, 6),
    ];

    async fn selected_read_case(final_route: bool, case: u8) {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"selected progress matrix");
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut foreign = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
        let expected = {
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            decode_with_reservation(
                if case == 15 { &mut foreign } else { &mut io },
                &mut route,
                Some(64 * 1024),
                || ExpectedPlan::from_selected(&plan, &digest),
            )
            .expect("expected model")
        };
        let (selector_raw, ordinal) = seed_selected_read_case(&store, expected.value(), case).await;
        let retained = io.allocation_evidence().1;
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("carry");
        let mut route = if final_route {
            RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
        } else {
            RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            }
        };
        let selected = read_selected_progress(&mut io, &mut route, &expected).await;
        assert_eq!(
            selected.is_ok(),
            matches!(case, 0 | 14 | 18),
            "case={case} final={final_route}: {:?}",
            selected.as_ref().err()
        );
        let expected_reads = SELECTED_READ_IO[usize::from(case)];
        let (ranges, heads, bytes) = io.reading_evidence();
        assert_eq!((ranges, heads), expected_reads, "case={case}");
        assert!(bytes <= ranges * (UNIT_RECORD_BYTES as u64));
        assert_eq!(io.allocation_underestimates(), 0);
        if let Ok(selected) = selected {
            assert_eq!(selected.progress.value().next_ordinal, ordinal);
            assert_eq!(selected.selector_raw.as_slice(), selector_raw);
            let old_version = selected.selector_meta.value().version.clone();
            let replaced = store
                .storage
                .put(
                    &expected.value().selector_path(),
                    bytes::Bytes::from_static(b"replacement after pinned read"),
                    arco_core::AuthorityWritePrecondition::MatchesVersion(old_version.clone()),
                )
                .await
                .expect("replace selector");
            assert!(matches!(replaced, arco_core::WriteResult::Success { .. }));
            assert_eq!(selected.selector_meta.value().version, old_version);
            assert_eq!(selected.selector_raw.as_slice(), selector_raw);
            let mut pending = Box::pin(async move {
                std::future::pending::<()>().await;
                drop(selected);
            });
            assert!(futures::poll!(&mut pending).is_pending());
            drop(pending);
            assert_eq!(io.allocation_evidence().1, retained);
        } else {
            let work = (io.reading_evidence(), io.hashing_evidence());
            assert!(
                read_selected_progress(&mut io, &mut route, &expected)
                    .await
                    .is_err()
            );
            assert_eq!((io.reading_evidence(), io.hashing_evidence()), work);
        }
        println!(
            "selected read final={final_route} case={case} ranges={ranges} heads={heads} bytes={bytes} hashes={:?}",
            io.hashing_evidence()
        );
    }

    #[tokio::test]
    async fn selected_progress_read_cancellation_releases_all_prior_records() {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        let (_fixture_store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"cancellation model");
        {
            let final_route = false; // Control transport is ordinary-only; rejection has separate coverage.
            for boundary in 1..=9 {
                let store = physical::restore_io::selected_read_pending_store(boundary);
                let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
                let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
                let mut payload = physical::restore_io::UnitPayloadAdmission::new();
                let expected = {
                    let mut route = RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    };
                    decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                        ExpectedPlan::from_selected(&plan, &digest)
                    })
                    .expect("expected model")
                };
                let (selector, progress, receipt, ordinal) =
                    selected_read_fixture(expected.value(), 0);
                for (record, raw) in [
                    (RestoreControlRecord::Selector, selector),
                    (RestoreControlRecord::Progress(ordinal), progress),
                    (RestoreControlRecord::Receipt(ordinal - 1), receipt),
                ] {
                    store
                        .storage
                        .put(
                            &record.path(&expected.value().prefix),
                            bytes::Bytes::from(raw),
                            arco_core::AuthorityWritePrecondition::DoesNotExist,
                        )
                        .await
                        .expect("fixture write");
                }
                let retained = io.live_ownership_evidence();
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("carry");
                let mut route = if final_route {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let mut pending = Box::pin(read_selected_progress(&mut io, &mut route, &expected));
                assert!(
                    futures::poll!(&mut pending).is_pending(),
                    "boundary={boundary}"
                );
                drop(pending);
                assert_eq!(io.live_ownership_evidence(), retained);
                assert_eq!(io.allocation_underestimates(), 0);
                let work = (io.reading_evidence(), io.hashing_evidence());
                assert!(
                    read_selected_progress(&mut io, &mut route, &expected)
                        .await
                        .is_err()
                );
                assert_eq!((io.reading_evidence(), io.hashing_evidence()), work);
            }
        }
    }

    #[tokio::test]
    async fn selected_progress_read_requires_complete_preflight_before_io() {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"preflight model");
        for final_route in [false, true] {
            let mut io = RestorePhysicalIo::new(
                &store,
                if final_route {
                    64 * 1024 * 1024
                } else {
                    64 * 1024
                },
                0,
            );
            let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let expected = {
                let mut route = RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                };
                decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                    ExpectedPlan::from_selected(&plan, &digest)
                })
                .expect("expected model")
            };
            let mut totals = FinalStreamTotals::new();
            let carry = if final_route {
                64 * 1024 * 1024 - io.live_ownership_evidence().0 - 64 * 1024 + 1
            } else {
                0
            };
            let mut chunk = FinalMicrochunk::begin(&mut totals, carry, &mut io).expect("carry");
            let mut route = if final_route {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            assert!(
                read_selected_progress(&mut io, &mut route, &expected)
                    .await
                    .is_err()
            );
            assert_eq!(io.reading_evidence(), (0, 0, 0));
            assert_eq!(io.hashing_evidence(), (0, 0));
            assert_eq!(io.allocation_underestimates(), 0);
            assert!(
                read_selected_progress(&mut io, &mut route, &expected)
                    .await
                    .is_err()
            );
            assert_eq!(io.reading_evidence(), (0, 0, 0));
        }
    }

    #[tokio::test]
    async fn selected_progress_read_rejects_disconnected_records() {
        let mut rejected = Vec::new();
        for corrupt_selector in [false, true] {
            let (store, plan) = super::super::tests::inspection_fixture().await;
            let digest = prefixed_sha256(b"selected progress model");
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let expected = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                ExpectedPlan::from_selected(&plan, &digest)
            })
            .expect("expected model");
            let receipt = native_receipt_fixture(expected.value());
            let receipt_raw = encode(&receipt, "receipt");
            let mut progress = progress_after(expected.value(), &receipt, &receipt_raw, false);
            if !corrupt_selector {
                progress.chain_sha256 = prefixed_sha256(b"disconnected chain");
            }
            let progress_raw = encode(&progress, "progress");
            let mut selected = selector(expected.value(), &progress, &progress_raw);
            if corrupt_selector {
                selected.current_progress_sha256 = prefixed_sha256(b"wrong raw");
            }
            for (record, raw) in [
                (
                    RestoreControlRecord::Selector,
                    encode(&selected, "selector"),
                ),
                (RestoreControlRecord::Progress(1), progress_raw),
                (RestoreControlRecord::Receipt(0), receipt_raw),
            ] {
                store
                    .storage
                    .put(
                        &record.path(&expected.value().prefix),
                        bytes::Bytes::from(raw),
                        arco_core::AuthorityWritePrecondition::DoesNotExist,
                    )
                    .await
                    .expect("fixture write");
            }
            rejected.push(
                read_selected_progress(&mut io, &mut route, &expected)
                    .await
                    .is_err(),
            );
        }
        assert_eq!(
            rejected,
            vec![true, true],
            "direct receipt and selector raw bindings are mandatory"
        );
    }

    #[tokio::test]
    async fn admitted_progress_validation_requires_full_reservation() {
        progress_validation_rejection(false, 0).await;
    }
    #[tokio::test]
    async fn admitted_progress_validation_requires_model_owner() {
        progress_validation_rejection(false, 1).await;
    }
    #[tokio::test]
    async fn admitted_progress_validation_requires_expected_owner() {
        progress_validation_rejection(false, 2).await;
    }
    #[tokio::test]
    async fn admitted_selector_validation_requires_full_reservation() {
        progress_validation_rejection(true, 0).await;
    }
    #[tokio::test]
    async fn admitted_selector_validation_requires_model_owner() {
        progress_validation_rejection(true, 1).await;
    }
    #[tokio::test]
    async fn admitted_selector_validation_requires_expected_owner() {
        progress_validation_rejection(true, 2).await;
    }

    async fn progress_validation_rejection(is_selector: bool, case: u8) {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        crate::workspace_restore::with_unit_selection(|selection, store| {
            let mut io = RestorePhysicalIo::new(
                store,
                if !is_selector && case == 0 {
                    2 * 1024 * 1024
                } else {
                    64 * 1024 * 1024
                },
                0,
            );
            let mut origin = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 0);
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let selected = own_selected_plan(
                if case == 2 { &mut origin } else { &mut io },
                selection,
                &mut payload,
            )
            .expect("selected");
            let (_, _, workspace) = selection.parts();
            let expected = {
                let mut route = RestorePhysicalRoute::OrdinaryUnit {
                    workspace,
                    payload: &mut payload,
                };
                admitted_expected_plan(
                    if case == 2 { &mut origin } else { &mut io },
                    &mut route,
                    &selected,
                )
                .expect("expected")
            };
            let fixture = genesis_progress(expected.value());
            if is_selector {
                let fixture = selector(expected.value(), &fixture, b"fixture");
                let model = {
                    let mut route = RestorePhysicalRoute::OrdinaryUnit {
                        workspace,
                        payload: &mut payload,
                    };
                    decode_with_reservation(
                        if case == 1 { &mut origin } else { &mut io },
                        &mut route,
                        Some(64 * 1024),
                        || Ok(fixture.clone()),
                    )
                    .expect("selector")
                };
                let mut totals = FinalStreamTotals::new();
                let carry = if case == 0 {
                    64 * 1024 * 1024 - io.allocation_evidence().1 - 32 * 1024
                } else {
                    0
                };
                let mut chunk = FinalMicrochunk::begin(&mut totals, carry, &mut io).expect("chunk");
                let mut route = if case == 0 {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace,
                        payload: &mut payload,
                    }
                };
                assert!(
                    codec::validate_selector(&mut io, &mut route, &expected, &model).is_err(),
                    "selector requires both owners and its complete fixed reservation"
                );
            } else {
                let mut route = RestorePhysicalRoute::OrdinaryUnit {
                    workspace,
                    payload: &mut payload,
                };
                let model = decode_with_reservation(
                    if case == 1 { &mut origin } else { &mut io },
                    &mut route,
                    Some(64 * 1024),
                    || Ok(fixture.clone()),
                )
                .expect("progress");
                assert!(
                    codec::validate_progress(&mut io, &mut route, &expected, &model).is_err(),
                    "progress requires both owners and admitted genesis hashing"
                );
            }
            assert_eq!(io.hashing_evidence(), (0, 0));
        })
        .await;
    }

    fn progress_validation_case(
        expected: &ExpectedPlan<'_>,
        genesis: bool,
        case: u8,
    ) -> RestoreProgressV1 {
        let mut value = if genesis {
            genesis_progress(expected)
        } else {
            progress_after(
                expected,
                &native_receipt_fixture(expected),
                b"receipt fixture",
                false,
            )
        };
        match case {
            0 => {}
            1 => value.record_type = "other".into(),
            2 => value.version = 2,
            3 => value.plan_sha256 = prefixed_sha256(b"other plan"),
            4 => value.owner_generation += 1,
            5 => value.receipt_count += 1,
            6 => value.chain_sha256 = "bad".into(),
            7 => {
                value.cursor.global = GlobalCut::After {
                    key_b64url: "!".into(),
                }
            }
            8 => {
                value.singleton_state = SingletonState::Pending {
                    key_b64url: "YQ".into(),
                    source: None,
                    current: None,
                    phase: SingletonPhase::Emit,
                }
            }
            9 => value.cursor.global = GlobalCut::End,
            10 => value.terminal = true,
            11 => {
                value.last_receipt = if genesis {
                    Some(ReceiptRef {
                        path: expected.receipt_path(0),
                        raw_sha256: prefixed_sha256(b"raw"),
                    })
                } else {
                    None
                };
            }
            12 | 13 => {
                value.last_receipt = Some(ReceiptRef {
                    path: if case == 12 {
                        "wrong path".into()
                    } else {
                        expected.receipt_path(0)
                    },
                    raw_sha256: if case == 13 {
                        "bad".into()
                    } else {
                        prefixed_sha256(b"raw")
                    },
                });
            }
            14 => value.cumulative_counts.mutations += 1,
            15 => {
                value.terminal = true;
                value.cursor = MergeCursor {
                    global: GlobalCut::End,
                    source: SideCursor::End,
                    current: SideCursor::End,
                };
            }
            16 => {
                value.next_ordinal = u64::MAX;
                value.receipt_count = u64::MAX;
                value.last_receipt = Some(ReceiptRef {
                    path: expected.receipt_path(u64::MAX - 1),
                    raw_sha256: prefixed_sha256(b"raw"),
                });
            }
            _ => {
                value.identity = RestoreAttemptIdentity::new(
                    value.identity.restore_id(),
                    value.identity.attempt() + 1,
                    value.identity.domain(),
                )
                .expect("different identity");
            }
        }
        value
    }

    #[tokio::test]
    async fn admitted_progress_validation_matches_legacy() {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        crate::workspace_restore::with_unit_selection(|selection, store| {
            for final_route in [false, true] {
                for genesis in [false, true] {
                    for case in 0..18 {
                        let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 0);
                        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
                        let selected =
                            own_selected_plan(&mut io, selection, &mut payload).expect("selection");
                        let (_, _, workspace) = selection.parts();
                        let expected = {
                            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                                workspace,
                                payload: &mut payload,
                            };
                            admitted_expected_plan(&mut io, &mut route, &selected)
                                .expect("expected")
                        };
                        let value = progress_validation_case(expected.value(), genesis, case);
                        let accepted =
                            matches!(case, 0 | 16) || (!genesis && matches!(case, 14 | 15));
                        assert_eq!(
                            validate_progress_shape(expected.value(), &value).is_ok(),
                            accepted,
                            "legacy genesis={genesis} case={case}"
                        );
                        let model = {
                            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                                workspace,
                                payload: &mut payload,
                            };
                            decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                                Ok(value.clone())
                            })
                            .expect("progress")
                        };
                        let retained = io.allocation_evidence().1;
                        let native = io.native_work_evidence();
                        let mut totals = FinalStreamTotals::new();
                        let mut chunk =
                            FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("carry");
                        let mut route = if final_route {
                            RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                        } else {
                            RestorePhysicalRoute::OrdinaryUnit {
                                workspace,
                                payload: &mut payload,
                            }
                        };
                        let checked =
                            codec::validate_progress(&mut io, &mut route, &expected, &model);
                        assert_eq!(
                            checked.is_ok(),
                            accepted,
                            "genesis={genesis} case={case} final={final_route}"
                        );
                        assert_eq!(
                            io.hashing_evidence().0,
                            if value.next_ordinal == 0 { 2 } else { 0 }
                        );
                        assert_eq!(io.native_work_evidence(), native);
                        assert_eq!(io.allocation_underestimates(), 0);
                        drop(checked);
                        if accepted {
                            assert_eq!(io.allocation_evidence().1, retained);
                        } else {
                            let hashes = io.hashing_evidence();
                            assert!(
                                codec::validate_progress(&mut io, &mut route, &expected, &model)
                                    .is_err()
                            );
                            assert_eq!(io.hashing_evidence(), hashes);
                        }
                    }
                }
            }
        })
        .await;
    }

    #[tokio::test]
    async fn admitted_selector_validation_matches_legacy() {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        crate::workspace_restore::with_unit_selection(|selection, store| {
            for final_route in [false, true] {
                for case in 0..9 {
                    let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 0);
                    let mut payload = physical::restore_io::UnitPayloadAdmission::new();
                    let selected =
                        own_selected_plan(&mut io, selection, &mut payload).expect("selection");
                    let (_, _, workspace) = selection.parts();
                    let expected = {
                        let mut route = RestorePhysicalRoute::OrdinaryUnit {
                            workspace,
                            payload: &mut payload,
                        };
                        admitted_expected_plan(&mut io, &mut route, &selected).expect("expected")
                    };
                    let mut value = selector(
                        expected.value(),
                        &genesis_progress(expected.value()),
                        b"fixture",
                    );
                    match case {
                        0 => {}
                        1 => value.record_type = "other".into(),
                        2 => value.version = 2,
                        3 => value.plan_sha256 = prefixed_sha256(b"other plan"),
                        4 => value.owner_generation += 1,
                        5 => value.current_progress_sha256 = "bad".into(),
                        6 => value.plan_sha256 = "bad".into(),
                        7 => {
                            value.identity = RestoreAttemptIdentity::new(
                                value.identity.restore_id(),
                                value.identity.attempt() + 1,
                                value.identity.domain(),
                            )
                            .expect("different identity");
                        }
                        _ => value.current_progress_path = "unverified path".into(),
                    }
                    let accepted = matches!(case, 0 | 8);
                    assert_eq!(
                        validate_selector_shape(expected.value(), &value).is_ok(),
                        accepted
                    );
                    let model = {
                        let mut route = RestorePhysicalRoute::OrdinaryUnit {
                            workspace,
                            payload: &mut payload,
                        };
                        decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                            Ok(value.clone())
                        })
                        .expect("selector")
                    };
                    let retained = io.allocation_evidence().1;
                    let mut totals = FinalStreamTotals::new();
                    let mut chunk =
                        FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("carry");
                    let mut route = if final_route {
                        RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                    } else {
                        RestorePhysicalRoute::OrdinaryUnit {
                            workspace,
                            payload: &mut payload,
                        }
                    };
                    let checked = codec::validate_selector(&mut io, &mut route, &expected, &model);
                    assert_eq!(checked.is_ok(), accepted);
                    assert_eq!(io.hashing_evidence(), (0, 0));
                    assert_eq!(io.allocation_underestimates(), 0);
                    drop(checked);
                    if accepted {
                        assert_eq!(io.allocation_evidence().1, retained);
                    } else {
                        assert!(
                            codec::validate_selector(&mut io, &mut route, &expected, &model)
                                .is_err()
                        );
                    }
                }
            }
        })
        .await;
    }

    #[tokio::test]
    async fn admitted_progress_validation_dense_exact_bound() {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        crate::workspace_restore::with_unit_selection(|selection, store| {
            for final_route in [false, true] {
                let mut exact_limit = 64 * 1024 * 1024;
                for limit_case in 0..3 {
                    let limit = if final_route || limit_case == 0 { 64 * 1024 * 1024 } else { exact_limit - usize::from(limit_case == 2) };
                    let mut io = RestorePhysicalIo::new(store, limit, 0);
                    let mut payload = physical::restore_io::UnitPayloadAdmission::new();
                    let selected = own_selected_plan(&mut io, selection, &mut payload).expect("selection");
                    let (_, _, workspace) = selection.parts();
                    let expected = {
                        let mut route = RestorePhysicalRoute::OrdinaryUnit { workspace, payload: &mut payload };
                        admitted_expected_plan(&mut io, &mut route, &selected).expect("expected")
                    };
                    let value = progress_after(expected.value(), &dense_native_receipt(expected.value(), 8), b"receipt fixture", false);
                    let compact = serde_json::to_vec(&value).expect("independent count").len();
                    let bound = 4 * compact + 4 * (expected.value().prefix.len() + 128) + 64 * 1024;
                    let measured = allocation_counter::measure(|| validate_progress_shape_with_genesis(expected.value(), &value, None).expect("dense structure"));
                    assert!(measured.bytes_total <= u64::try_from(bound).expect("64 bit"));
                    let model = {
                        let mut route = RestorePhysicalRoute::OrdinaryUnit { workspace, payload: &mut payload };
                        decode_with_reservation(&mut io, &mut route, Some(compact + 64 * 1024), || Ok(value.clone())).expect("progress")
                    };
                    let retained = io.allocation_evidence().1;
                    if limit_case == 0 { exact_limit = retained + bound; }
                    let carry = if final_route && limit_case != 0 { 64 * 1024 * 1024 - exact_limit + usize::from(limit_case == 2) } else { 4096 };
                    let mut totals = FinalStreamTotals::new();
                    let mut chunk = FinalMicrochunk::begin(&mut totals, carry, &mut io).expect("carry");
                    let mut route = if final_route { RestorePhysicalRoute::FinalMicrochunk(&mut chunk) } else { RestorePhysicalRoute::OrdinaryUnit { workspace, payload: &mut payload } };
                    let checked = codec::validate_progress(&mut io, &mut route, &expected, &model);
                    assert_eq!(checked.is_ok(), limit_case != 2, "final={final_route} limit={limit_case}: {:?}", checked.as_ref().err());
                    assert_eq!(io.hashing_evidence(), (0, 0));
                    assert_eq!(io.allocation_underestimates(), 0);
                    if checked.is_ok() { assert_eq!(io.allocation_evidence().1 - retained, usize::try_from(measured.bytes_total).expect("64 bit")); }
                    println!("progress structural final={final_route} limit={limit_case} compact={compact} allocated={} bound={bound}", measured.bytes_total);
                    drop(checked);
                    if limit_case != 2 { assert_eq!(io.allocation_evidence().1, retained); }
                }
            }
        }).await;
    }

    #[tokio::test]
    async fn admitted_progress_validation_preflight_caps() {
        crate::workspace_restore::with_unit_selection(|selection, store| {
            for too_large in [false, true] {
                let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 0);
                let mut payload = physical::restore_io::UnitPayloadAdmission::new();
                let selected =
                    own_selected_plan(&mut io, selection, &mut payload).expect("selection");
                let (_, _, workspace) = selection.parts();
                let mut route = RestorePhysicalRoute::OrdinaryUnit {
                    workspace,
                    payload: &mut payload,
                };
                let expected =
                    admitted_expected_plan(&mut io, &mut route, &selected).expect("expected");
                let mut value = progress_after(
                    expected.value(),
                    &dense_native_receipt(expected.value(), 8),
                    b"receipt fixture",
                    false,
                );
                if too_large {
                    value.cursor.global = GlobalCut::After {
                        key_b64url: "A".repeat(UNIT_RECORD_BYTES),
                    };
                } else {
                    let SideCursor::After { position, .. } = &mut value.cursor.source else {
                        panic!("after")
                    };
                    position.path.push(position.path[0].clone());
                }
                let model =
                    decode_with_reservation(&mut io, &mut route, Some(8 * 1024 * 1024), || {
                        Ok(value.clone())
                    })
                    .expect("bounded test model");
                assert!(codec::validate_progress(&mut io, &mut route, &expected, &model).is_err());
                assert_eq!(io.hashing_evidence(), (0, 0));
                assert_eq!(io.allocation_underestimates(), 0);
                assert!(codec::validate_progress(&mut io, &mut route, &expected, &model).is_err());
            }
        })
        .await;
    }

    #[tokio::test]
    async fn admitted_progress_validation_genesis_witness_cardinality() {
        let (_store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"genesis model");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        let genesis = genesis_progress(&expected);
        assert!(
            validate_progress_shape_with_genesis(&expected, &genesis, Some(&genesis.chain_sha256))
                .is_ok()
        );
        assert!(validate_progress_shape_with_genesis(&expected, &genesis, None).is_err());
        assert!(validate_progress_shape_with_genesis(&expected, &genesis, Some("bad")).is_err());
        let non_genesis = progress_validation_case(&expected, false, 0);
        assert!(validate_progress_shape_with_genesis(&expected, &non_genesis, None).is_ok());
        assert!(
            validate_progress_shape_with_genesis(
                &expected,
                &non_genesis,
                Some(&genesis.chain_sha256)
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn admitted_receipt_validation_requires_reservation() {
        receipt_validation_rejection(0).await;
    }

    #[tokio::test]
    async fn admitted_receipt_validation_requires_receipt_owner() {
        receipt_validation_rejection(1).await;
    }

    #[tokio::test]
    async fn admitted_receipt_validation_requires_expected_owner() {
        receipt_validation_rejection(2).await;
    }

    async fn receipt_validation_rejection(case: u8) {
        crate::workspace_restore::with_unit_selection(|selection, store| {
            let mut io = RestorePhysicalIo::new(
                store,
                if case == 0 {
                    2 * 1024 * 1024
                } else {
                    64 * 1024 * 1024
                },
                0,
            );
            let mut origin = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 0);
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let selected = own_selected_plan(
                if case == 2 { &mut origin } else { &mut io },
                selection,
                &mut payload,
            )
            .expect("selection");
            let (_, _, workspace) = selection.parts();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace,
                payload: &mut payload,
            };
            let expected = admitted_expected_plan(
                if case == 2 { &mut origin } else { &mut io },
                &mut route,
                &selected,
            )
            .expect("expected");
            let fixture = receipt(
                expected.value(),
                0,
                genesis_receipt_raw_sha256(expected.value()).expect("genesis"),
                genesis_chain_sha256(expected.value()).expect("chain"),
                zero_cursor(),
                audit_after("YQ"),
                CumulativeSemanticCounts::default(),
            );
            validate_receipt_shape(expected.value(), &fixture).expect("valid fixture");
            let model = decode_with_reservation(
                if case == 1 { &mut origin } else { &mut io },
                &mut route,
                Some(64 * 1024),
                || Ok(fixture.clone()),
            )
            .expect("guarded receipt");
            assert!(
                codec::validate_receipt(&mut io, &mut route, &expected, &model).is_err(),
                "receipt validation must require complete admission and both owners"
            );
            assert_eq!(io.hashing_evidence(), (0, 0));
        })
        .await;
    }

    fn native_receipt_fixture(expected: &ExpectedPlan<'_>) -> ControlMvpRestoreReceiptV1 {
        let mut value = receipt(
            expected,
            0,
            genesis_receipt_raw_sha256(expected).expect("genesis"),
            genesis_chain_sha256(expected).expect("chain"),
            zero_cursor(),
            audit_after("eg"),
            CumulativeSemanticCounts::default(),
        );
        value.outputs = vec![
            audit_output(expected, 0, 0, "YQ"),
            audit_output(expected, 0, 1, "Yg"),
        ];
        value.counts.output_blocks = 2;
        value.counts.output_encoded_bytes = 2;
        rehash_receipt(&mut value);
        value
    }

    fn receipt_validation_case(
        expected: &ExpectedPlan<'_>,
        case: u8,
    ) -> ControlMvpRestoreReceiptV1 {
        let mut value = native_receipt_fixture(expected);
        match case {
            0 | 5 | 6 => {}
            1 => value.record_type = "other".into(),
            2 => value.version = 2,
            3 => value.plan_sha256 = prefixed_sha256(b"other plan"),
            4 => value.owner_generation += 1,
            7 => value.before = value.after.clone(),
            8 => value.predecessor_receipt_sha256 = "bad".into(),
            9 => value.outputs[0].output_id = "0".repeat(64),
            10 => value.outputs.swap(0, 1),
            11 => {
                value.outputs[1].part = 0;
                value.outputs[1].output_id = value.outputs[0].output_id.clone();
            }
            12 => {
                value.outputs[0].role = physical::Role::ActiveId;
                value.outputs[0].output_id =
                    output_id(expected, 0, 0, physical::Role::ActiveId).expect("role id");
            }
            13 => value.outputs[0].directory_leaf.first_key_b64url = "!".into(),
            14 => {
                value.outputs[0].block.min_key_b64url = "eg".into();
                value.outputs[0].block.max_key_b64url = "eg".into();
            }
            15 => value.outputs[0].descriptor.path = "bad\\path".into(),
            16 => value.counts.output_blocks = 0,
            17 => value.outputs[0].block.offset = u64::MAX,
            18 => {
                value.after.global = GlobalCut::After {
                    key_b64url: "!".into(),
                }
            }
            19 => value.outputs.resize(33, value.outputs[0].clone()),
            20 => value.outputs[0].directory_leaf.rows = 0,
            21 => value.outputs[0].index.sha256.clear(),
            _ => {
                let UnitReservationV1::Standard {
                    combined_leaf_limit,
                    ..
                } = &mut value.counts.reservation
                else {
                    panic!("standard")
                };
                *combined_leaf_limit = 1;
            }
        }
        rehash_receipt(&mut value);
        if case == 5 {
            value.receipt_body_sha256 = prefixed_sha256(b"wrong body");
        }
        if case == 6 {
            value.chain_sha256 = prefixed_sha256(b"wrong chain");
        }
        value
    }

    #[tokio::test]
    async fn admitted_receipt_validation_matches_legacy_and_retains_failed_work() {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        crate::workspace_restore::with_unit_selection(|selection, store| {
            for final_route in [false, true] {
                for case in 0..23 {
                    let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 0);
                    let mut payload = physical::restore_io::UnitPayloadAdmission::new();
                    let selected =
                        own_selected_plan(&mut io, selection, &mut payload).expect("selection");
                    let (_, _, workspace) = selection.parts();
                    let expected = {
                        let mut route = RestorePhysicalRoute::OrdinaryUnit {
                            workspace,
                            payload: &mut payload,
                        };
                        admitted_expected_plan(&mut io, &mut route, &selected).expect("expected")
                    };
                    let value = receipt_validation_case(expected.value(), case);
                    let legacy_ok = validate_receipt_shape(expected.value(), &value).is_ok();
                    assert_eq!(legacy_ok, case == 0, "fixture case {case}");
                    let model = {
                        let mut route = RestorePhysicalRoute::OrdinaryUnit {
                            workspace,
                            payload: &mut payload,
                        };
                        decode_with_reservation(&mut io, &mut route, Some(256 * 1024), || {
                            Ok(value.clone())
                        })
                        .expect("receipt")
                    };
                    let retained = io.allocation_evidence().1;
                    let native = io.native_work_evidence();
                    let mut totals = FinalStreamTotals::new();
                    let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                    let mut route = if final_route {
                        RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                    } else {
                        RestorePhysicalRoute::OrdinaryUnit {
                            workspace,
                            payload: &mut payload,
                        }
                    };
                    let checked = codec::validate_receipt(&mut io, &mut route, &expected, &model);
                    assert_eq!(
                        checked.is_ok(),
                        legacy_ok,
                        "case={case} final={final_route}"
                    );
                    assert_eq!(io.hashing_evidence().0, if case == 19 { 0 } else { 4 });
                    assert_eq!(io.native_work_evidence(), native);
                    assert_eq!(io.allocation_underestimates(), 0);
                    drop(checked);
                    if legacy_ok {
                        assert_eq!(io.allocation_evidence().1, retained);
                    } else {
                        let before = io.hashing_evidence();
                        assert!(
                            codec::validate_receipt(&mut io, &mut route, &expected, &model)
                                .is_err()
                        );
                        assert_eq!(io.hashing_evidence(), before);
                    }
                }
            }
        })
        .await;
    }

    fn dense_native_receipt(
        expected: &ExpectedPlan<'_>,
        side_inputs: usize,
    ) -> ControlMvpRestoreReceiptV1 {
        use base64::Engine;
        let key =
            |index: u8| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(vec![index; 4096]);
        let leaf = DirectoryPositionLeafWitness {
            first_b64url: key(0),
            last_b64url: key(99),
            rows: 100,
            bytes: 100,
            digest: prefixed_sha256(b"leaf"),
        };
        let cursor = |index| MergeCursor {
            global: GlobalCut::After {
                key_b64url: key(index),
            },
            source: SideCursor::After {
                key_b64url: key(index),
                position: DirectoryPosition {
                    role: physical::Role::Kv,
                    root_b64: expected.source_kv_root_b64.into(),
                    path: vec![
                        DirectoryPathEntry {
                            page_sha256: prefixed_sha256(b"page"),
                            child_index: 0
                        };
                        8
                    ],
                    leaf: leaf.clone(),
                },
            },
            current: SideCursor::After {
                key_b64url: key(index),
                position: DirectoryPosition {
                    role: physical::Role::Kv,
                    root_b64: expected.base_kv_root_b64.into(),
                    path: vec![
                        DirectoryPathEntry {
                            page_sha256: prefixed_sha256(b"page"),
                            child_index: 0
                        };
                        8
                    ],
                    leaf: leaf.clone(),
                },
            },
        };
        let mut value = receipt(
            expected,
            0,
            genesis_receipt_raw_sha256(expected).expect("genesis"),
            genesis_chain_sha256(expected).expect("chain"),
            cursor(0),
            cursor(99),
            CumulativeSemanticCounts::default(),
        );
        value.outputs = (0..32)
            .map(|part| {
                audit_output(
                    expected,
                    0,
                    part,
                    &key(u8::try_from(part + 1).expect("bounded part")),
                )
            })
            .collect();
        let inputs = |root: &str| {
            value
                .outputs
                .iter()
                .take(side_inputs)
                .map(|output| InputWitness {
                    role: physical::Role::Kv,
                    root_b64: root.into(),
                    directory_leaf: DirectoryPositionLeafWitness {
                        first_b64url: output.directory_leaf.first_key_b64url.clone(),
                        last_b64url: output.directory_leaf.last_key_b64url.clone(),
                        rows: 1,
                        bytes: 1,
                        digest: output.directory_leaf.digest.clone(),
                    },
                    descriptor: output.descriptor.clone(),
                    index: output.index.clone(),
                    block: output.block.clone(),
                })
                .collect()
        };
        value.source_inputs = inputs(expected.source_kv_root_b64);
        value.current_inputs = inputs(expected.base_kv_root_b64);
        let side_count = u64::try_from(side_inputs).expect("bounded inputs");
        value.counts.source_leaves = side_count;
        value.counts.current_leaves = side_count;
        value.counts.input_encoded_bytes = 2 * side_count;
        value.counts.decoded_blocks = 2 * side_count;
        value.counts.decoded_rows = 2 * side_count;
        value.counts.output_blocks = 32;
        value.counts.output_encoded_bytes = 32;
        rehash_receipt(&mut value);
        value
    }

    #[tokio::test]
    async fn admitted_receipt_validation_dense_bound_and_guard_release() {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        crate::workspace_restore::with_unit_selection(|selection, store| {
            for final_route in [false, true] {
                for side_inputs in [8, 16] {
                    let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 0);
                    let mut payload = physical::restore_io::UnitPayloadAdmission::new();
                    let selected = own_selected_plan(&mut io, selection, &mut payload).expect("selection");
                    let (_, _, workspace) = selection.parts();
                    let expected = {
                        let mut route = RestorePhysicalRoute::OrdinaryUnit { workspace, payload: &mut payload };
                        admitted_expected_plan(&mut io, &mut route, &selected).expect("expected")
                    };
                    let value = dense_native_receipt(expected.value(), side_inputs);
                    let compact_bytes = serde_json::to_vec(&value).expect("compact independent count").len();
                    let structural_bound = 6 * compact_bytes + 64 * 1024;
                    let ids: Vec<_> = value.outputs.iter().map(|output| output.output_id.as_str()).collect();
                    let measured = allocation_counter::measure(|| {
                        assert_eq!(validate_receipt_shape_with_hashes(expected.value(), &value, &value.receipt_body_sha256, &value.chain_sha256, &ids).is_ok(), side_inputs == 8);
                    });
                    assert!(measured.bytes_total <= u64::try_from(structural_bound).expect("64 bit"));
                    let model = {
                        let mut route = RestorePhysicalRoute::OrdinaryUnit { workspace, payload: &mut payload };
                        decode_with_reservation(&mut io, &mut route, Some(4 * 1024 * 1024), || Ok(value.clone())).expect("receipt model")
                    };
                    let retained = io.allocation_evidence().1;
                    let mut totals = FinalStreamTotals::new();
                    let mut chunk = FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("chunk carry");
                    let mut route = if final_route { RestorePhysicalRoute::FinalMicrochunk(&mut chunk) } else { RestorePhysicalRoute::OrdinaryUnit { workspace, payload: &mut payload } };
                    let checked = codec::validate_receipt(&mut io, &mut route, &expected, &model);
                    assert_eq!(checked.is_ok(), side_inputs == 8, "side_inputs={side_inputs} final={final_route}: {:?}", checked.as_ref().err());
                    assert_eq!(io.hashing_evidence().0, 34);
                    assert_eq!(io.allocation_underestimates(), 0);
                    if checked.is_ok() { assert_eq!(io.allocation_evidence().1 - retained, usize::try_from(measured.bytes_total).expect("64 bit")); }
                    println!("receipt structural side_inputs={side_inputs} final={final_route} compact={compact_bytes} allocated={} bound={structural_bound} hashes={:?}", measured.bytes_total, io.hashing_evidence());
                    drop(checked);
                    if side_inputs == 8 { assert_eq!(io.allocation_evidence().1, retained); }
                    else {
                        let hashes = io.hashing_evidence();
                        assert!(codec::validate_receipt(&mut io, &mut route, &expected, &model).is_err());
                        assert_eq!(io.hashing_evidence(), hashes);
                    }
                }
            }
        }).await;
    }

    #[tokio::test]
    async fn legacy_receipt_validation_adds_no_output_id_container_allocations() {
        use base64::Engine;
        let (_store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"legacy allocation model");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        let outputs: Vec<_> = (0..32)
            .map(|part| {
                let key = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode([u8::try_from(part).expect("bounded part")]);
                audit_output(&expected, 0, part, &key)
            })
            .collect();
        let mut individual_bytes = 0;
        for output in &outputs {
            let measured = allocation_counter::measure(|| {
                let id = output_id(&expected, 0, output.part, output.role).expect("id");
                drop(validate_output(output, &id).expect("output"));
            });
            individual_bytes += measured.bytes_total;
        }
        let combined = allocation_counter::measure(|| {
            assert_eq!(
                strictly_ordered_outputs(&expected, 0, &outputs).expect("ordered outputs"),
                32
            );
        });
        assert_eq!(
            combined.bytes_total, individual_bytes,
            "bounded ID adapters must not allocate container backing"
        );
    }

    #[tokio::test]
    async fn admitted_receipt_validation_exact_output_id_coverage() {
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"coverage model");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        let value = native_receipt_fixture(&expected);
        let body = receipt_body_sha256(&value).expect("body");
        let chain = receipt_chain_sha256(&value).expect("chain");
        let ids = [
            value.outputs[0].output_id.as_str(),
            value.outputs[1].output_id.as_str(),
        ];
        assert!(validate_receipt_shape_with_hashes(&expected, &value, &body, &chain, &ids).is_ok());
        for altered in [
            vec![ids[0]],
            vec![ids[0], ids[1], ids[1]],
            vec![ids[1], ids[0]],
            vec![ids[0], ids[0]],
        ] {
            assert!(
                validate_receipt_shape_with_hashes(&expected, &value, &body, &chain, &altered)
                    .is_err()
            );
        }
        drop(store);
    }

    #[tokio::test]
    async fn admitted_output_id_rejects_missing_reservation() {
        output_id_rejection(false).await;
    }

    #[tokio::test]
    async fn admitted_output_id_rejects_foreign_owner() {
        output_id_rejection(true).await;
    }

    async fn output_id_rejection(foreign: bool) {
        crate::workspace_restore::with_unit_selection(|selection, store| {
            let mut origin = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 0);
            let mut io = RestorePhysicalIo::new(store, 2 * 1024 * 1024, 0);
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let owner = if foreign { &mut origin } else { &mut io };
            let selected =
                own_selected_plan(owner, selection, &mut payload).expect("selected copy");
            let (_, _, workspace) = selection.parts();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace,
                payload: &mut payload,
            };
            let expected =
                admitted_expected_plan(owner, &mut route, &selected).expect("admitted plan");
            assert!(
                codec::hashes::output_id(&mut io, &mut route, &expected, 0, 0, physical::Role::Kv)
                    .is_err(),
                "hash must require complete reservation and same owner"
            );
            assert_eq!(io.hashing_evidence(), (0, 0));
        })
        .await;
    }

    #[tokio::test]
    async fn admitted_output_id_vectors_roles_extrema_and_ownership() {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        use sha2::{Digest, Sha256};
        crate::workspace_restore::with_unit_selection(|selection, store| {
            let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 0);
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let selected = own_selected_plan(&mut io, selection, &mut payload).expect("selection");
            let (_, _, workspace) = selection.parts();
            let expected = {
                let mut route = RestorePhysicalRoute::OrdinaryUnit { workspace, payload: &mut payload };
                admitted_expected_plan(&mut io, &mut route, &selected).expect("expected")
            };
            for final_route in [false, true] {
                for (role, label) in [(physical::Role::Kv, "kv"), (physical::Role::ActiveId, "active_id"), (physical::Role::DeliveryOrder, "delivery_order")] {
                    for (ordinal, part) in [(0, 0), (u64::MAX, u32::MAX)] {
                        let mut totals = FinalStreamTotals::new();
                        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                        let mut route = if final_route { RestorePhysicalRoute::FinalMicrochunk(&mut chunk) } else { RestorePhysicalRoute::OrdinaryUnit { workspace, payload: &mut payload } };
                        let before = io.hashing_evidence();
                        let retained = io.allocation_evidence().1;
                        let native = io.native_work_evidence();
                        let actual = codec::hashes::output_id(&mut io, &mut route, &expected, ordinal, part, role).expect("output id");
                        // Independent scalar-only canonical wire, outside the measured pass.
                        let raw = format!("{{\"candidate_seed_sha256\":\"{}\",\"ordinal\":{ordinal},\"part\":{part},\"role\":\"{label}\"}}", expected.value().candidate_seed_sha256);
                        let mut hash = Sha256::new();
                        hash.update(b"arco/control-v2/restore-output-v1\0");
                        hash.update(raw.as_bytes());
                        assert_eq!(actual.value(), &format!("{:x}", hash.finalize()));
                        assert_eq!(*actual.value(), output_id(expected.value(), ordinal, part, role).expect("legacy identity"));
                        assert_eq!(actual.value().len(), 64);
                        assert_eq!(actual.value().capacity(), 71);
                        assert!(actual.is_owned_by(&io));
                        assert_eq!(io.hashing_evidence(), (before.0+1, before.1+u64::try_from(b"arco/control-v2/restore-output-v1\0".len()+raw.len()).expect("input bytes")));
                        assert_eq!(io.native_work_evidence(), native, "direct hash cannot count twice");
                        assert_eq!(io.allocation_underestimates(), 0);
                        assert!(io.allocation_evidence().1 > retained);
                        drop(actual);
                        assert_eq!(io.allocation_evidence().1, retained);
                    }
                }
            }
        }).await;
    }

    #[tokio::test]
    async fn admitted_output_id_frozen_seed_vectors() {
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"private hash model");
        for (seed, vector) in [
            (
                "sha256:1212121212121212121212121212121212121212121212121212121212121212",
                "ea0532abfba7361f4c8efc942c6327d06389d6f84020f492e6aa688037dd0219",
            ),
            (
                "sha256:1313131313131313131313131313131313131313131313131313131313131313",
                "f556497a0f92cf7dd3a535fc694d0f117ae9d8af5cbdc1722edbf9b6203609e8",
            ),
        ] {
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            // Fixed scalar hash model only, never selected authority.
            let expected = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                let mut expected = ExpectedPlan::from_selected(&plan, &digest)?;
                expected.candidate_seed_sha256 = seed;
                Ok(expected)
            })
            .expect("guarded model");
            let actual =
                codec::hashes::output_id(&mut io, &mut route, &expected, 3, 2, physical::Role::Kv)
                    .expect("output id");
            assert_eq!(actual.value(), vector);
            assert_eq!(
                *actual.value(),
                output_id(expected.value(), 3, 2, physical::Role::Kv).expect("legacy vector")
            );
            assert_eq!(io.allocation_underestimates(), 0);
        }
    }

    #[tokio::test]
    async fn admitted_output_id_malformed_seed_stops_before_hashing() {
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"private hash model");
        for seed in [
            "sha256:ABC".to_owned(),
            "sha256:".to_owned() + &"A".repeat(64),
            "12".repeat(32),
            "sha256:".to_owned() + &"g".repeat(64),
        ] {
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            // Negative private model only; it never establishes selected authority.
            let expected = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                let mut expected = ExpectedPlan::from_selected(&plan, &digest)?;
                expected.candidate_seed_sha256 = &seed;
                Ok(expected)
            })
            .expect("guarded model");
            assert!(
                codec::hashes::output_id(&mut io, &mut route, &expected, 0, 0, physical::Role::Kv)
                    .is_err()
            );
            assert_eq!(io.hashing_evidence(), (0, 0));
            assert_eq!(io.allocation_underestimates(), 0);
            assert!(
                codec::hashes::output_id(&mut io, &mut route, &expected, 0, 0, physical::Role::Kv)
                    .is_err()
            );
            assert_eq!(io.hashing_evidence(), (0, 0));
        }
    }

    #[tokio::test]
    async fn admitted_expected_plan_authentic_selection_borrows_and_releases() {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        crate::workspace_restore::with_unit_selection(|selection, store| {
            let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 0);
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let owned = own_selected_plan(&mut io, selection, &mut payload).expect("selected copy");
            let before = io.allocation_evidence();
            let (_, _, workspace) = selection.parts();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace,
                payload: &mut payload,
            };
            let expected =
                admitted_expected_plan(&mut io, &mut route, &owned).expect("validated projection");
            assert!(expected.is_owned_by(&io));
            assert_eq!(
                expected.value().plan_sha256.as_ptr(),
                owned.value().plan_sha256.as_ptr()
            );
            assert_eq!(
                expected.value().candidate_id.as_ptr(),
                owned.value().plan.fields.candidate_id.as_ptr()
            );
            assert_eq!(
                expected.value().prefix,
                format!(
                    "{}/restore/v7/{}",
                    store.paths.base_prefix(),
                    owned.value().plan.fields.candidate_id
                )
            );
            let after = io.allocation_evidence();
            assert!(after.0 > before.0);
            assert_eq!(
                after.1 - before.1,
                usize::try_from(after.0 - before.0).expect("allocation count")
            );
            assert!(after.1 - before.1 <= expected_plan_reservation(store).expect("bound"));
            let native = io.native_work_evidence();
            assert_eq!(native.bounded.directory_references, 6);
            assert_eq!(native.bounded.decoded_rows, 0);
            assert_eq!(native.bounded.transition_proof_rows, 0);
            assert_eq!(native.bounded.selected_blocks, 0);
            assert_eq!(native.bounded.rewritten_blocks, 0);
            assert_eq!(native.bounded.streaming_builder_inputs, 0);
            assert_eq!(&native.slots[..2], &[1, 70]);
            assert!(native.slots[2..].iter().all(|value| *value == 0));
            assert!(!native.overflow);
            assert_eq!(io.allocation_underestimates(), 0);
            let mut exact = FinalStreamTotals::new();
            assert!(
                FinalMicrochunk::begin(&mut exact, 64 * 1024 * 1024 - after.1, &mut io).is_ok()
            );
            let mut excessive = FinalStreamTotals::new();
            let error =
                FinalMicrochunk::begin(&mut excessive, 64 * 1024 * 1024 - after.1 + 1, &mut io)
                    .err()
                    .expect("excess carry");
            let diagnostic = crate::workspace_io_budget::catalog_error_string_capacity(&error)
                .expect("diagnostic");
            println!(
                "expected-plan authentic allocation={} bound={} native={:?}",
                after.0 - before.0,
                expected_plan_reservation(store).expect("bound"),
                io.native_work_evidence()
            );
            drop(expected);
            assert_eq!(io.allocation_evidence().1, before.1 + diagnostic);
            drop(owned);
            assert_eq!(io.allocation_evidence().1, diagnostic);
        })
        .await;
    }

    #[tokio::test]
    async fn admitted_expected_plan_exact_admission_and_final_carry() {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        crate::workspace_restore::with_unit_selection(|selection, store| {
            for final_route in [false, true] {
                for below in [false, true] {
                    let bound = expected_plan_reservation(store).expect("bound");
                    let copied = {
                        let (selected, _, _) = selection.parts();
                        let crate::state_store::PersistedRestoreParticipantPlan::ControlMvpV7(plan) = selected else { panic!("Plan7") };
                        selected_plan_copy_reservation(plan).expect("copy bound") - 64 * 1024
                    };
                    let limit = if final_route { 64 * 1024 * 1024 } else { copied + bound - usize::from(below) };
                    let mut io = RestorePhysicalIo::new(store, limit, 0);
                    let mut payload = physical::restore_io::UnitPayloadAdmission::new();
                    let owned = own_selected_plan(&mut io, selection, &mut payload).expect("selected copy");
                    let (_, _, workspace) = selection.parts();
                    let mut totals = FinalStreamTotals::new();
                    let mut chunk = FinalMicrochunk::begin(&mut totals, if final_route { limit - copied - bound + usize::from(below) } else { 0 }, &mut io).expect("carry");
                    let mut route = if final_route { RestorePhysicalRoute::FinalMicrochunk(&mut chunk) } else { RestorePhysicalRoute::OrdinaryUnit { workspace, payload: &mut payload } };
                    let result = admitted_expected_plan(&mut io, &mut route, &owned);
                    assert_eq!(result.is_ok(), !below, "final={final_route} below={below}");
                    let mut diagnostic_bytes = match &result {
                        Err(CatalogError::MaintenanceBackpressure { message }) => message.capacity(),
                        Ok(_) => 0,
                        Err(error) => panic!("unexpected {error:?}"),
                    };
                    drop(result);
                    assert_eq!(io.native_work_evidence().bounded.directory_references, if below {0} else {6});
                    assert_eq!(io.allocation_underestimates(), 0);
                    if below {
                        let error = decode_with_reservation(&mut io, &mut route, Some(1), || -> Result<()> { panic!("route must remain stopped") }).err().expect("stopped");
                        let CatalogError::MaintenanceBackpressure { message } = error else { panic!("unexpected stop") };
                        diagnostic_bytes += message.capacity();
                    }
                    drop(owned);
                    assert_eq!(io.allocation_evidence().1, diagnostic_bytes);
                }
            }
        }).await;
    }

    #[tokio::test]
    async fn admitted_expected_plan_foreign_ledger_stops_before_roots() {
        crate::workspace_restore::with_unit_selection(|selection, store| {
            let mut origin = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 0);
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let owned =
                own_selected_plan(&mut origin, selection, &mut payload).expect("selected copy");
            let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 0);
            let (_, _, workspace) = selection.parts();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace,
                payload: &mut payload,
            };
            assert!(admitted_expected_plan(&mut io, &mut route, &owned).is_err());
            assert_eq!(io.native_work_evidence().bounded.directory_references, 0);
            assert_eq!(io.allocation_underestimates(), 0);
            assert!(
                decode_with_reservation(&mut io, &mut route, Some(1), || -> Result<()> {
                    panic!("stopped")
                })
                .is_err()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn admitted_expected_plan_long_domain_and_invalid_path_models() {
        use crate::state_store::StateScope;
        let (original_store, original) = super::super::tests::inspection_fixture().await;
        for case in ["long", "path", "scope"] {
            // The long-domain plan comes from the real planner. Invalid
            // path/scope cases are private negative models, never authority.
            let domain = if case == "path" {
                "%".repeat(32 * 1024)
            } else {
                "x".repeat(32 * 1024)
            };
            let (mut store, mut plan) = if case == "long" {
                super::super::tests::inspection_fixture_for_domain(&domain).await
            } else {
                (original_store.clone(), original.clone())
            };
            store.scope = StateScope::new("tenant", "workspace", &domain);
            if case != "scope" {
                plan.fields.scope = store.scope.clone();
            }
            let digest = "sha256:".to_owned() + &"12".repeat(32);
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let owned = decode_with_reservation(
                &mut io,
                &mut route,
                selected_plan_copy_reservation(&plan),
                || {
                    Ok(OwnedSelectedPlan {
                        plan: plan.clone(),
                        plan_sha256: digest.clone(),
                    })
                },
            )
            .expect("admitted model");
            let before = io.allocation_evidence();
            let result = admitted_expected_plan(&mut io, &mut route, &owned);
            assert_eq!(result.is_ok(), case == "long", "case {case}");
            let allocated = io.allocation_evidence().0 - before.0;
            assert!(
                allocated
                    <= u64::try_from(expected_plan_reservation(&store).expect("bound"))
                        .expect("64 bit")
            );
            assert_eq!(io.allocation_underestimates(), 0);
            assert_eq!(
                io.native_work_evidence().bounded.directory_references,
                if case == "long" { 6 } else { 0 }
            );
            if let Ok(expected) = &result {
                assert!(expected.value().prefix.contains(&domain));
            }
            println!(
                "expected-plan model case={case} domain={} allocated={allocated} bound={}",
                domain.len(),
                expected_plan_reservation(&store).expect("bound")
            );
            drop(result);
            drop(owned);
            if case == "long" {
                assert_eq!(io.allocation_evidence().1, 0);
            }
        }
    }

    #[tokio::test]
    async fn admitted_expected_plan_absent_shape_model() {
        use crate::state_store::control_mvp::logical_v2;
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        let (store, mut plan) = super::super::tests::inspection_fixture().await;
        // Shape-valid absent target model only: the fixture's target still
        // exists, so this does not establish an absent-target authority fence.
        plan.fields.mode = super::super::Mode::Absent;
        plan.fields.target = super::super::Target::Absent {
            current_pointer_path: store.paths.current_pointer(),
            absence_marker: "does_not_exist".into(),
            observed_writer_epoch: 0,
            observed_reclamation_generation: 0,
            source_parent_writer_epoch: 0,
            source_parent_reclamation_generation: 0,
        };
        let request = plan.logical_request().expect("request");
        let commit = logical_v2::commit_id(
            &plan.fields.scope,
            request.prior_history(),
            request.result_sequence(),
            &request.operation().expect("operation"),
        )
        .expect("commit");
        let notice = request.notice(&commit).expect("notice");
        plan.fields.result_logical_sequence = request.result_sequence();
        plan.fields.restore_request_digest =
            format!("sha256:{}", request.request_digest().expect("digest"));
        plan.fields.logical_commit_id = format!("sha256:{commit}");
        plan.fields.restore_notice_intent_id = notice.intent_id().into();
        plan.fields.restore_notice_payload_b64 = URL_SAFE_NO_PAD.encode(notice.payload());
        plan.fields.candidate_seed_sha256 = plan.candidate_seed().expect("seed");
        plan.fields.candidate_id = raw_digest(&plan.fields.candidate_seed_sha256)
            .expect("candidate")
            .into();
        plan.validate_shape().expect("shape-valid absence model");
        let selected =
            crate::state_store::PersistedRestoreParticipantPlan::ControlMvpV7(Box::new(plan));
        let digest = prefixed_sha256(&jcs(&selected).expect("enum-wire bytes"));
        let crate::state_store::PersistedRestoreParticipantPlan::ControlMvpV7(plan) = selected
        else {
            panic!("Plan7")
        };
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned = decode_with_reservation(
            &mut io,
            &mut route,
            selected_plan_copy_reservation(&plan),
            || {
                Ok(OwnedSelectedPlan {
                    plan: plan.as_ref().clone(),
                    plan_sha256: digest.clone(),
                })
            },
        )
        .expect("admitted model");
        let before = io.allocation_evidence();
        let expected =
            admitted_expected_plan(&mut io, &mut route, &owned).expect("absent projection");
        assert_eq!(expected.value().plan_sha256, digest);
        assert_eq!(io.native_work_evidence().bounded.directory_references, 6);
        assert_eq!(io.allocation_underestimates(), 0);
        println!(
            "expected-plan absent allocation={} bound={} layout={}",
            io.allocation_evidence().0 - before.0,
            expected_plan_reservation(&store).expect("bound"),
            size_of::<WorkingValue<ExpectedPlan<'_>>>()
        );
        drop(expected);
        assert_eq!(io.allocation_evidence().1, before.1);
        drop(owned);
        assert_eq!(io.allocation_evidence().1, 0);
    }

    #[tokio::test]
    async fn admitted_expected_plan_requires_configured_store_binding() {
        crate::workspace_restore::with_unit_selection(|selection, store| {
            let wrong = store.clone().with_durable_authority_binding(
                crate::state_store::DurableAuthorityBinding::new([99; 32]),
            );
            let mut io = RestorePhysicalIo::new(&wrong, 64 * 1024 * 1024, 0);
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let owned = own_selected_plan(&mut io, selection, &mut payload)
                .expect("authentic selected copy");
            let (_, _, workspace) = selection.parts();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace,
                payload: &mut payload,
            };
            assert!(
                admitted_expected_plan(&mut io, &mut route, &owned).is_err(),
                "selected provenance does not prove this physical store's durable binding"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn admitted_expected_plan_reserves_complete_constructor_before_root_work() {
        crate::workspace_restore::with_unit_selection(|selection, store| {
            let copied = {
                let (selected, _, _) = selection.parts();
                let crate::state_store::PersistedRestoreParticipantPlan::ControlMvpV7(plan) =
                    selected
                else {
                    panic!("Plan7")
                };
                selected_plan_copy_reservation(plan).expect("copy bound") - 64 * 1024
            };
            let mut io = RestorePhysicalIo::new(store, copied + 80 * 1024, 0);
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let owned =
                own_selected_plan(&mut io, selection, &mut payload).expect("selected copy fits");
            let (_, _, workspace) = selection.parts();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace,
                payload: &mut payload,
            };
            assert!(
                admitted_expected_plan(&mut io, &mut route, &owned).is_err(),
                "small observed prefix must not replace complete constructor reservation"
            );
            assert_eq!(io.native_work_evidence().bounded.directory_references, 0);
        })
        .await;
    }

    #[tokio::test]
    async fn admitted_expected_plan_validates_every_directory_root() {
        let (store, original) = super::super::tests::inspection_fixture().await;
        for (malformation, replacement) in [
            ("short", "A".repeat(379)),
            ("long", "A".repeat(381)),
            ("base64", "!".repeat(380)),
            ("directory", "A".repeat(380)),
        ] {
            for root in 0..6 {
                // Deliberately malformed private model, never selected as authority.
                // Its graph is copied under the same previously qualified admission.
                let mut plan = original.clone();
                let fields = &mut plan.fields;
                let roots = [
                    &mut fields.source_kv_root_b64,
                    &mut fields.source_active_id_root_b64,
                    &mut fields.source_delivery_order_root_b64,
                    &mut fields.base_kv_root_b64,
                    &mut fields.base_active_id_root_b64,
                    &mut fields.base_delivery_order_root_b64,
                ];
                *roots.into_iter().nth(root).expect("root") = replacement.clone();
                let digest = "sha256:".to_owned() + &"12".repeat(32);
                let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
                let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
                let mut payload = physical::restore_io::UnitPayloadAdmission::new();
                let mut route = RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                };
                let owned = decode_with_reservation(
                    &mut io,
                    &mut route,
                    selected_plan_copy_reservation(&plan),
                    || {
                        Ok(OwnedSelectedPlan {
                            plan: plan.clone(),
                            plan_sha256: digest.clone(),
                        })
                    },
                )
                .expect("guarded model");
                assert!(
                    admitted_expected_plan(&mut io, &mut route, &owned).is_err(),
                    "unvalidated {malformation} root {root}"
                );
                assert_eq!(io.allocation_underestimates(), 0);
                if malformation == "short" || malformation == "long" {
                    assert_eq!(io.native_work_evidence().bounded.directory_references, 0);
                }
                assert!(
                    decode_with_reservation(&mut io, &mut route, Some(1), || -> Result<()> {
                        panic!("stopped")
                    })
                    .is_err()
                );
            }
        }
    }

    #[tokio::test]
    async fn selected_plan_copy_reserves_before_cloning() {
        crate::workspace_restore::with_unit_selection(|selection, store| {
            for limit in [32 * 1024, 64 * 1024] {
                let mut io = RestorePhysicalIo::new(store, limit, limit);
                let mut payload = physical::restore_io::UnitPayloadAdmission::new();
                assert!(
                    matches!(
                        own_selected_plan(&mut io, selection, &mut payload),
                        Err(CatalogError::MaintenanceBackpressure { .. })
                    ),
                    "copy must reserve complete graph plus fixed scratch before cloning"
                );
                let first = io.allocation_evidence();
                assert!(first.0 > 0, "rejection diagnostic is counted");
                assert_eq!(first.0, first.1 as u64);
                assert_eq!(
                    io.decoding_operations(),
                    u64::from(limit >= 64 * 1024),
                    "only fixed preflight may run"
                );
                assert!(own_selected_plan(&mut io, selection, &mut payload).is_err());
                let stopped = io.allocation_evidence();
                assert!(stopped.0 > first.0, "stopped diagnostic is counted");
                assert_eq!(stopped.0, stopped.1 as u64);
                assert_eq!(
                    io.decoding_operations(),
                    u64::from(limit >= 64 * 1024),
                    "sticky stop"
                );
            }
        })
        .await;
    }

    #[tokio::test]
    async fn selected_plan_copy_owns_exact_pair_after_loan_and_fixture_drop() {
        let (owned, expected, digest) =
            crate::workspace_restore::with_unit_selection(|selection, store| {
                let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 64 * 1024 * 1024);
                let mut payload = physical::restore_io::UnitPayloadAdmission::new();
                let owned = own_selected_plan(&mut io, selection, &mut payload)
                    .expect("owned selected plan");
                let (original, digest, _) = selection.parts();
                let crate::state_store::PersistedRestoreParticipantPlan::ControlMvpV7(original) =
                    original
                else {
                    panic!("Plan7");
                };
                assert!(owned.is_owned_by(&io));
                assert_eq!(&owned.value().plan, original.as_ref());
                assert_eq!(owned.value().plan_sha256, digest);
                assert_ne!(
                    owned.value().plan.fields.record_type.as_ptr(),
                    original.fields.record_type.as_ptr()
                );
                assert_ne!(owned.value().plan_sha256.as_ptr(), digest.as_ptr());
                let expected_bytes =
                    selected_plan_copy_reservation(original).expect("bound") - 64 * 1024;
                assert_eq!(
                    io.allocation_evidence(),
                    (expected_bytes as u64, expected_bytes)
                );
                (owned, original.as_ref().clone(), digest.to_owned())
            })
            .await;
        tokio::task::yield_now().await;
        assert_eq!(owned.value().plan, expected);
        assert_eq!(owned.value().plan_sha256, digest);
    }

    #[tokio::test]
    async fn selected_plan_copy_final_carry_is_counted_once_and_released() {
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals};
        crate::workspace_restore::with_unit_selection(|selection, store| {
            let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut payload = physical::restore_io::UnitPayloadAdmission::new();
            let owned =
                own_selected_plan(&mut io, selection, &mut payload).expect("copy before stream");
            let carried = io.allocation_evidence().1;
            assert!(carried > 0);
            let mut exact = FinalStreamTotals::new();
            assert!(
                FinalMicrochunk::begin(&mut exact, 64 * 1024 * 1024 - carried, &mut io).is_ok()
            );
            let mut excessive = FinalStreamTotals::new();
            let error =
                FinalMicrochunk::begin(&mut excessive, 64 * 1024 * 1024 - carried + 1, &mut io)
                    .err()
                    .expect("excessive carry rejected");
            drop(owned);
            assert_eq!(
                io.allocation_evidence().1,
                crate::workspace_io_budget::catalog_error_string_capacity(&error).unwrap()
            );
            let mut released = FinalStreamTotals::new();
            assert!(
                FinalMicrochunk::begin(&mut released, 0, &mut io).is_err(),
                "releasing carried state cannot revive a failed invocation"
            );
            let mut fresh = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut fresh_totals = FinalStreamTotals::new();
            assert!(
                FinalMicrochunk::begin(&mut fresh_totals, 64 * 1024 * 1024, &mut fresh).is_ok()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn selected_plan_copy_clone_graph_allocation_qualification() {
        crate::workspace_restore::with_unit_selection(|selection, _store| {
            let (selected, digest, _) = selection.parts();
            let crate::state_store::PersistedRestoreParticipantPlan::ControlMvpV7(original) = selected else { panic!("Plan7"); };
            for absent in [false, true] {
                for optional in [false, true] {
                    for width in [0, 1, 4096, 1024 * 1024] {
                        // Only the owned graph is qualified here. The deliberately
                        // altered typed fixture is never used as selected authority.
                        let mut plan = original.as_ref().clone();
                        let repeated = "🔥".repeat(width);
                        plan.fields.record_type = String::with_capacity(repeated.len() + 4096);
                        plan.fields.record_type.push_str(&repeated);
                        assert!(plan.fields.record_type.capacity() > plan.fields.record_type.len());
                        if absent {
                            plan.fields.mode = super::super::Mode::Absent;
                            plan.fields.target = super::super::Target::Absent {
                                current_pointer_path: "control/pointer".into(),
                                absence_marker: "ABSENT".into(),
                                observed_writer_epoch: 0,
                                observed_reclamation_generation: 0,
                                source_parent_writer_epoch: 0,
                                source_parent_reclamation_generation: 0,
                            };
                        }
                        if optional {
                            plan.fields.source.checkpoint_path = Some("defensive optional checkpoint".into());
                            plan.fields.source.checkpoint_sha256 = Some(digest.to_owned());
                        }
                        let reservation = selected_plan_copy_reservation(&plan).expect("checked graph bound");
                        let mut copied = None;
                        let allocations = allocation_counter::measure(|| {
                            copied = Some(OwnedSelectedPlan { plan: plan.clone(), plan_sha256: digest.to_owned() });
                        });
                        let copied = copied.expect("direct graph copy");
                        assert_eq!(copied.plan, plan);
                        assert_eq!(copied.plan_sha256, digest);
                        assert_eq!(allocations.bytes_total, u64::try_from(reservation - 64 * 1024).expect("64 bit"));
                        let nonempty_strings = if absent { 35 } else { 39 } + if optional { 2 } else { 0 } - u64::from(width == 0);
                        assert_eq!(allocations.count_total, nonempty_strings);
                        assert!(size_of::<OwnedSelectedPlan>() <= 64 * 1024);
                        println!("selected plan clone: absent={absent} optional={optional} width={width} layout={} allocations={} bytes={} reservation={reservation}",
                            size_of::<OwnedSelectedPlan>(), allocations.count_total, allocations.bytes_total);
                    }
                }
            }
        }).await;
    }

    async fn codec_raw(
        io: &mut RestorePhysicalIo<'_>,
        _codec_route: &mut RestorePhysicalRoute<'_, '_>,
        raw: &[u8],
    ) -> physical::restore_io::AccountedBytes {
        let candidate = prefixed_sha256(raw)
            .trim_start_matches("sha256:")
            .to_owned();
        let prefix = format!("{}/restore/v7/{candidate}", io.store().paths.base_prefix());
        io.store()
            .storage
            .put(
                &RestoreControlRecord::Selector.path(&prefix),
                bytes::Bytes::copy_from_slice(raw),
                arco_core::AuthorityWritePrecondition::DoesNotExist,
            )
            .await
            .expect("fixture record");
        // Fixture transport is control work even when the codec runs in a final chunk.
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = physical::restore_io::UnitPayloadAdmission::new();
        let mut control_route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        physical::restore_io::read_restore_control_record(
            io,
            &mut control_route,
            &candidate,
            RestoreControlRecord::Selector,
        )
        .await
        .expect("classified raw read")
        .expect("present")
        .0
    }

    #[tokio::test]
    async fn admitted_unit_codec_reserves_before_parsing() {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::UnitPayloadAdmission;
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let plan_digest = prefixed_sha256(b"codec fixture selected plan");
        let expected = ExpectedPlan::from_selected(&plan, &plan_digest).expect("expected");
        let progress = genesis_progress(&expected);
        let value = selector(&expected, &progress, &encode(&progress, "progress"));
        let raw = encode(&value, "selector");
        let mut io = RestorePhysicalIo::new(&store, 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let raw = codec_raw(&mut io, &mut route, &raw).await;
        let before = io.decoding_operations();
        assert!(
            codec::decode_selector(&mut io, &mut route, &raw).is_err(),
            "record parsing must require its complete reservation"
        );
        assert_eq!(
            io.decoding_operations(),
            before,
            "parser must not run on failed admission"
        );
    }

    #[tokio::test]
    async fn admitted_unit_codec_rejects_a_foreign_raw_owner() {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::UnitPayloadAdmission;
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let plan_digest = prefixed_sha256(b"codec fixture selected plan");
        let expected = ExpectedPlan::from_selected(&plan, &plan_digest).expect("expected");
        let progress = genesis_progress(&expected);
        let value = selector(&expected, &progress, &encode(&progress, "progress"));
        let raw = encode(&value, "selector");
        let mut owner = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let raw = codec_raw(&mut owner, &mut route, &raw).await;
        assert!(
            codec::decode_selector(&mut io, &mut route, &raw).is_err(),
            "raw ownership must remain on the decoding ledger"
        );
    }

    pub(super) fn codec_max_receipt(expected: &ExpectedPlan<'_>) -> ControlMvpRestoreReceiptV1 {
        let output = audit_output(expected, 0, 0, "YQ");
        let leaf = DirectoryPositionLeafWitness {
            first_b64url: "YQ".into(),
            last_b64url: "YQ".into(),
            rows: 1,
            bytes: 1,
            digest: output.directory_leaf.digest.clone(),
        };
        let input = InputWitness {
            role: physical::Role::Kv,
            root_b64: expected.source_kv_root_b64.into(),
            directory_leaf: leaf.clone(),
            descriptor: output.descriptor.clone(),
            index: output.index.clone(),
            block: output.block.clone(),
        };
        let side = SideCursor::After {
            key_b64url: "YQ".into(),
            position: DirectoryPosition {
                role: physical::Role::Kv,
                root_b64: expected.source_kv_root_b64.into(),
                path: vec![
                    DirectoryPathEntry {
                        page_sha256: prefixed_sha256(b"page"),
                        child_index: 0
                    };
                    8
                ],
                leaf,
            },
        };
        let cursor = MergeCursor {
            global: GlobalCut::After {
                key_b64url: "YQ".into(),
            },
            source: side.clone(),
            current: side,
        };
        let witness = Box::new(SingletonValueWitness {
            generation: 1,
            tombstone: false,
            value_length: 1,
            value_sha256: prefixed_sha256(b"v"),
            descriptor: output.descriptor.clone(),
            index: output.index.clone(),
            block: output.block.clone(),
            row_ordinal: 0,
        });
        let singleton = SingletonState::Pending {
            key_b64url: "YQ".into(),
            source: Some(witness.clone()),
            current: Some(witness),
            phase: SingletonPhase::Emit,
        };
        let mut receipt = receipt(
            expected,
            0,
            genesis_receipt_raw_sha256(expected).expect("genesis"),
            genesis_chain_sha256(expected).expect("chain"),
            cursor.clone(),
            cursor,
            CumulativeSemanticCounts::default(),
        );
        receipt.singleton_before = singleton.clone();
        receipt.singleton_after = singleton;
        receipt.source_inputs = vec![input.clone(); 16];
        receipt.current_inputs = vec![input; 16];
        receipt.outputs = vec![output; 32];
        receipt.counts.reservation = UnitReservationV1::Singleton {
            phase: SingletonPhase::Emit,
            authenticated_payload_bytes: 1,
            input_byte_limit: SINGLETON_INPUT_BYTE_LIMIT,
            segment_byte_limit: SINGLETON_SEGMENT_BYTE_LIMIT,
            scratch_base_bytes: SINGLETON_SCRATCH_BASE_BYTES,
            scratch_payload_multiplier: SINGLETON_SCRATCH_PAYLOAD_MULTIPLIER,
        };
        receipt
    }

    #[tokio::test]
    async fn admitted_unit_hash_rejects_missing_reservation() {
        assert_hash_admission_rejection(0).await;
    }

    #[tokio::test]
    async fn admitted_unit_hash_rejects_foreign_owner() {
        assert_hash_admission_rejection(1).await;
    }

    #[tokio::test]
    async fn admitted_unit_hash_rejects_unbounded_shape() {
        assert_hash_admission_rejection(2).await;
    }

    async fn assert_hash_admission_rejection(case: u8) {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::UnitPayloadAdmission;
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"hash fixture");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        let mut receipt = codec_max_receipt(&expected);
        if case == 2 {
            receipt.outputs.push(receipt.outputs[0].clone());
        }
        let limit = if case == 0 {
            2 * 1024 * 1024
        } else {
            64 * 1024 * 1024
        };
        let mut io = RestorePhysicalIo::new(&store, limit, 0);
        let mut origin = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let model = decode_with_reservation(
            if case == 1 { &mut origin } else { &mut io },
            &mut route,
            Some(256 * 1024),
            || Ok(receipt.clone()),
        )
        .expect("owned model");
        let result = codec::hashes::receipt_body(&mut io, &mut route, &model);
        if case == 0 {
            assert!(matches!(
                result,
                Err(CatalogError::MaintenanceBackpressure { .. })
            ));
        } else {
            assert!(matches!(
                result,
                Err(CatalogError::InvariantViolation { .. })
            ));
        }
        assert_eq!(io.hashing_evidence(), (0, 0));
        assert!(codec::hashes::receipt_body(&mut io, &mut route, &model).is_err());
        assert_eq!(
            io.hashing_evidence(),
            (0, 0),
            "failed route cannot hash again"
        );
    }

    #[tokio::test]
    async fn admitted_unit_hash_all_projection_owners_and_shapes() {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::UnitPayloadAdmission;
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"hash fixture");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        for foreign in [false, true] {
            for family in 0..if foreign { 5 } else { 4 } {
                let mut receipt = codec_max_receipt(&expected);
                let mut progress = genesis_progress(&expected);
                if !foreign {
                    receipt.outputs.push(receipt.outputs[0].clone());
                    progress.cursor = receipt.before.clone();
                    let SideCursor::After { position, .. } = &mut progress.cursor.source else {
                        panic!("fixture cursor")
                    };
                    position.path.push(position.path[0].clone());
                }
                let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
                let mut origin = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut route = RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                };
                let owner = if foreign && family != 4 {
                    &mut origin
                } else {
                    &mut io
                };
                let receipt = decode_with_reservation(owner, &mut route, Some(256 * 1024), || {
                    Ok(receipt.clone())
                })
                .expect("receipt");
                let progress = decode_with_reservation(owner, &mut route, Some(64 * 1024), || {
                    Ok(progress.clone())
                })
                .expect("progress");
                let none = decode_with_reservation(
                    if foreign { &mut origin } else { &mut io },
                    &mut route,
                    Some(1024),
                    || Ok(digest.clone()),
                )
                .expect("none");
                let result = match family {
                    0 => codec::hashes::receipt_body(&mut io, &mut route, &receipt),
                    1 => codec::hashes::receipt_chain(&mut io, &mut route, &receipt),
                    2 => codec::hashes::genesis_none(&mut io, &mut route, &progress),
                    _ => codec::hashes::genesis_chain(&mut io, &mut route, &progress, &none),
                };
                assert!(
                    matches!(result, Err(CatalogError::InvariantViolation { .. })),
                    "family {family} foreign {foreign}"
                );
                assert_eq!(io.hashing_evidence(), (0, 0));
                assert_eq!(
                    io.encoding_evidence().0,
                    1,
                    "only the rejection reservation ran"
                );
            }
        }
    }

    #[tokio::test]
    async fn admitted_unit_hash_tagged_caps_stop_before_hash() {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::UnitPayloadAdmission;
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"hash fixture");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        for escaped in [false, true] {
            let mut receipt = codec_max_receipt(&expected);
            receipt.record_type = if escaped {
                "\\".repeat(1024 * 1024)
            } else {
                "x".repeat(UNIT_RECORD_BYTES)
            };
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let receipt =
                decode_with_reservation(&mut io, &mut route, Some(8 * 1024 * 1024), || {
                    Ok(receipt.clone())
                })
                .expect("model");
            assert!(codec::hashes::receipt_body(&mut io, &mut route, &receipt).is_err());
            assert_eq!(io.hashing_evidence(), (0, 0));
            assert_eq!(
                io.encoding_evidence().0,
                1,
                "cap or allocation admission precedes JCS"
            );
        }
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one composition verifies every hash domain on both routes"
    )]
    async fn admitted_unit_hash_parity_work_and_publication_on_both_routes() {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::{
            FinalMicrochunk, FinalStreamTotals, UnitPayloadAdmission, read_restore_control_record,
            write_restore_control_record,
        };
        use sha2::{Digest, Sha256};
        let (store, plan) =
            super::super::tests::inspection_fixture_for_domain("catalog\"hash🔥").await;
        let plan_digest = prefixed_sha256(b"hash fixture");
        let expected = ExpectedPlan::from_selected(&plan, &plan_digest).expect("expected");
        let progress = genesis_progress(&expected);
        let mut receipt = codec_max_receipt(&expected);
        receipt.ordinal = u64::MAX;
        for final_stream in [false, true] {
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut transport_workspace = WorkspaceIoBudget::new();
            let mut transport_payload = UnitPayloadAdmission::new();
            let mut transport = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut transport_workspace,
                payload: &mut transport_payload,
            };
            let mut payload = UnitPayloadAdmission::new();
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let owned_receipt =
                decode_with_reservation(&mut io, &mut route, Some(256 * 1024), || {
                    Ok(receipt.clone())
                })
                .expect("receipt");
            let owned_progress =
                decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                    Ok(progress.clone())
                })
                .expect("progress");
            let mut body_tree = serde_json::to_value(&receipt).expect("independent projection");
            body_tree
                .as_object_mut()
                .expect("object")
                .remove("receipt_body_sha256");
            body_tree
                .as_object_mut()
                .expect("object")
                .remove("chain_sha256");
            let canonical = serde_jcs::to_vec(&body_tree).expect("JCS");
            let tag = b"arco/control-v2/restore-receipt-body-v1";
            let independent = format!(
                "sha256:{}",
                hex::encode(Sha256::digest([tag.as_slice(), &[0], &canonical].concat()))
            );
            let body =
                codec::hashes::receipt_body(&mut io, &mut route, &owned_receipt).expect("body");
            assert_eq!(body.value(), &independent);
            assert_eq!(
                body.value(),
                &receipt_body_sha256(&receipt).expect("legacy body")
            );
            assert_eq!(
                io.hashing_evidence(),
                (1, (tag.len() + 1 + canonical.len()) as u64)
            );
            let chain =
                codec::hashes::receipt_chain(&mut io, &mut route, &owned_receipt).expect("chain");
            assert_eq!(
                chain.value(),
                &receipt_chain_sha256(&receipt).expect("legacy chain")
            );
            let none =
                codec::hashes::genesis_none(&mut io, &mut route, &owned_progress).expect("none");
            assert_eq!(
                none.value(),
                &genesis_receipt_raw_sha256(&expected).expect("legacy none")
            );
            let genesis = codec::hashes::genesis_chain(&mut io, &mut route, &owned_progress, &none)
                .expect("genesis");
            assert_eq!(
                genesis.value(),
                &genesis_chain_sha256(&expected).expect("legacy genesis")
            );
            let independent_projections = [
                (
                    "arco/control-v2/restore-receipt-chain-v1",
                    serde_json::json!({
                        "plan_sha256": receipt.plan_sha256, "owner_generation": receipt.owner_generation,
                        "ordinal": receipt.ordinal, "predecessor_chain_sha256": receipt.predecessor_chain_sha256,
                        "receipt_body_sha256": receipt.receipt_body_sha256,
                    }),
                    chain.value(),
                ),
                (
                    "arco/control-v2/restore-receipt-none-v1",
                    serde_json::json!({
                        "plan_sha256": progress.plan_sha256, "identity": progress.identity,
                        "owner_generation": progress.owner_generation,
                    }),
                    none.value(),
                ),
                (
                    "arco/control-v2/restore-receipt-genesis-chain-v1",
                    serde_json::json!({
                        "plan_sha256": progress.plan_sha256, "identity": progress.identity,
                        "owner_generation": progress.owner_generation, "genesis_receipt_raw_sha256": none.value(),
                    }),
                    genesis.value(),
                ),
            ];
            let mut exact_hashed_bytes = tag.len() + 1 + canonical.len();
            for (tag, tree, actual) in independent_projections {
                let raw = serde_jcs::to_vec(&tree).expect("independent JCS");
                let bytes = [tag.as_bytes(), &[0], &raw].concat();
                exact_hashed_bytes += bytes.len();
                assert_eq!(
                    actual,
                    &format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
                );
            }
            assert_eq!(io.hashing_evidence(), (4, exact_hashed_bytes as u64));
            let mut changed = receipt.clone();
            changed.counts.mutations += 1;
            let changed =
                decode_with_reservation(&mut io, &mut route, Some(256 * 1024), || Ok(changed))
                    .expect("changed");
            let altered =
                codec::hashes::receipt_body(&mut io, &mut route, &changed).expect("altered");
            assert_ne!(altered.value(), body.value());
            assert_ne!(chain.value(), body.value());
            assert_ne!(genesis.value(), none.value());

            let encoded =
                codec::encode_receipt(&mut io, &mut route, &owned_receipt).expect("encoded");
            let before = io.hashing_evidence();
            let raw_hash =
                codec::hashes::raw_encoded(&mut io, &mut route, &encoded).expect("raw hash");
            assert_eq!(raw_hash.value(), &receipt_raw_sha256(encoded.value()));
            assert_ne!(raw_hash.value(), body.value());
            assert_eq!(
                io.hashing_evidence(),
                (before.0 + 1, before.1 + encoded.value().len() as u64)
            );
            let candidate = if final_stream {
                "a".repeat(64)
            } else {
                "b".repeat(64)
            };
            let published = write_restore_control_record(
                &mut io,
                &mut transport,
                &candidate,
                RestoreControlRecord::Receipt(0),
                &encoded,
                None,
            )
            .await
            .expect("publish");
            let (raw, metadata) = read_restore_control_record(
                &mut io,
                &mut transport,
                &candidate,
                RestoreControlRecord::Receipt(0),
            )
            .await
            .expect("read")
            .expect("present");
            let before = io.hashing_evidence();
            let read_hash = codec::hashes::raw_read(&mut io, &mut route, &raw).expect("read hash");
            assert_eq!(read_hash.value(), raw_hash.value());
            assert_eq!(
                io.hashing_evidence(),
                (before.0 + 1, before.1 + raw.as_slice().len() as u64)
            );
            assert_eq!(io.hashing_evidence().0, 7);
            let retained = io.allocation_evidence().1;
            tokio::task::yield_now().await;
            assert_eq!(io.allocation_evidence().1, retained);
            let mut pending = Box::pin(async move {
                std::future::pending::<()>().await;
                drop(read_hash);
            });
            assert!(futures::poll!(&mut pending).is_pending());
            drop(pending);
            assert!(io.allocation_evidence().1 < retained);
            println!(
                "unit hashes final={final_stream} operations={} bytes={}",
                io.hashing_evidence().0,
                io.hashing_evidence().1
            );
            let mut foreign = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut foreign_route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            assert!(codec::hashes::raw_read(&mut foreign, &mut foreign_route, &raw).is_err());
            assert_eq!(foreign.hashing_evidence(), (0, 0));
            drop((
                body, chain, none, genesis, altered, encoded, raw_hash, published, raw, metadata,
            ));
        }
    }

    #[tokio::test]
    async fn admitted_unit_encoding_reserves_before_canonicalization() {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::UnitPayloadAdmission;
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"encoding fixture");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        let progress = genesis_progress(&expected);
        let value = selector(&expected, &progress, &encode(&progress, "progress"));
        let mut io = RestorePhysicalIo::new(&store, 1024 * 1024, 0);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned =
            decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || Ok(value.clone()))
                .expect("model");
        assert!(
            codec::encode_selector(&mut io, &mut route, &owned).is_err(),
            "JCS must not run without its full reservation"
        );
        assert_eq!(io.encoding_evidence().0, 1, "only the counting pass ran");
    }

    #[tokio::test]
    async fn admitted_unit_encoding_rejects_foreign_models() {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::UnitPayloadAdmission;
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"encoding fixture");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        let progress = genesis_progress(&expected);
        let value = selector(&expected, &progress, &encode(&progress, "progress"));
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
        let mut origin = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned = decode_with_reservation(&mut origin, &mut route, Some(64 * 1024), || {
            Ok(value.clone())
        })
        .expect("model");
        assert!(
            codec::encode_selector(&mut io, &mut route, &owned).is_err(),
            "model ownership must be on the encoding ledger"
        );
    }

    #[tokio::test]
    async fn admitted_unit_encoding_rejects_oversized_receipt_shape() {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::UnitPayloadAdmission;
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"encoding fixture");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        for case in 0..9 {
            let mut value = codec_max_receipt(&expected);
            match case {
                0 => value.outputs.push(value.outputs[0].clone()),
                1 => value.source_inputs.push(value.source_inputs[0].clone()),
                2 => value.current_inputs.push(value.current_inputs[0].clone()),
                _ => {
                    let side = match case {
                        3 | 7 => &mut value.before.source,
                        4 | 8 => &mut value.before.current,
                        5 => &mut value.after.source,
                        _ => &mut value.after.current,
                    };
                    let SideCursor::After { position, .. } = side else {
                        panic!("max fixture")
                    };
                    position.path.push(position.path[0].clone());
                }
            }
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let result = if case < 7 {
                let owned = decode_with_reservation(&mut io, &mut route, Some(256 * 1024), || {
                    Ok(value.clone())
                })
                .expect("model");
                codec::encode_receipt(&mut io, &mut route, &owned)
            } else {
                let mut progress = genesis_progress(&expected);
                progress.cursor = value.before.clone();
                let owned = decode_with_reservation(&mut io, &mut route, Some(256 * 1024), || {
                    Ok(progress.clone())
                })
                .expect("model");
                codec::encode_progress(&mut io, &mut route, &owned)
            };
            assert!(
                matches!(result, Err(CatalogError::MaintenanceBackpressure { .. })),
                "shape {case}"
            );
            assert_eq!(io.encoding_evidence().0, 1, "shape fails in preflight");
        }
    }

    #[tokio::test]
    async fn admitted_unit_encoding_rejects_wire_cap_and_final_or_output_admission() {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals, UnitPayloadAdmission};
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"encoding fixture");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        for case in 0..3 {
            let mut progress = genesis_progress(&expected);
            match case {
                0 => progress.record_type = "x".repeat(UNIT_RECORD_BYTES),
                2 => progress.record_type = "\\".repeat(1024 * 1024),
                _ => {}
            }
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut ordinary = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let owned =
                decode_with_reservation(&mut io, &mut ordinary, Some(8 * 1024 * 1024), || {
                    Ok(progress.clone())
                })
                .expect("model");
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(
                &mut totals,
                if case == 1 { 63 * 1024 * 1024 } else { 0 },
                &mut io,
            )
            .expect("carry");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            assert!(matches!(
                codec::encode_progress(&mut io, &mut route, &owned),
                Err(CatalogError::MaintenanceBackpressure { .. })
            ));
            assert_eq!(io.encoding_evidence().0, 1, "case {case} never runs JCS");
            assert!(codec::encode_progress(&mut io, &mut route, &owned).is_err());
            assert_eq!(io.encoding_evidence().0, 1);
        }
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "compose all three record families with CAS, replay, reads and cancellation on both routes"
    )]
    async fn admitted_unit_encoding_composes_with_publication_and_admitted_reads() {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::{
            FinalMicrochunk, FinalStreamTotals, UnitPayloadAdmission, read_restore_control_record,
            write_restore_control_record,
        };
        let (store, plan) =
            super::super::tests::inspection_fixture_for_domain("catalog\"encoded🔥").await;
        let digest = prefixed_sha256(b"encoding fixture");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        let progress = genesis_progress(&expected);
        let selector = selector(&expected, &progress, &encode(&progress, "progress"));
        let receipt = codec_max_receipt(&expected);
        for final_stream in [false, true] {
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut transport_workspace = WorkspaceIoBudget::new();
            let mut transport_payload = UnitPayloadAdmission::new();
            let mut transport = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut transport_workspace,
                payload: &mut transport_payload,
            };
            let mut payload = UnitPayloadAdmission::new();
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            macro_rules! check {
                ($encode:ident, $decode:ident, $record:expr, $value:expr) => {{
                    let model =
                        decode_with_reservation(&mut io, &mut route, Some(256 * 1024), || {
                            Ok($value.clone())
                        })
                        .expect("owned model");
                    let before = io.encoding_evidence();
                    let encoded =
                        codec::$encode(&mut io, &mut route, &model).expect("admitted encoding");
                    let after = io.encoding_evidence();
                    assert_eq!(after.0 - before.0, 2, "count and JCS each record work");
                    assert_eq!(encoded.value().as_ref(), encode(&$value, "oracle"));
                    let clone = allocation_counter::measure(|| drop(encoded.value().clone()));
                    assert_eq!(clone.bytes_total, 0, "sharing was prepaid");
                    let owned = io.allocation_evidence().1;
                    tokio::task::yield_now().await;
                    assert_eq!(io.allocation_evidence().1, owned);
                    let candidate = prefixed_sha256(
                        format!("outgoing-{final_stream}-{}", stringify!($encode)).as_bytes(),
                    )
                    .trim_start_matches("sha256:")
                    .to_owned();
                    let first = write_restore_control_record(
                        &mut io,
                        &mut transport,
                        &candidate,
                        $record,
                        &encoded,
                        None,
                    )
                    .await
                    .expect("publish record");
                    let arco_core::WriteResult::Success { version } = first.value() else {
                        panic!("fresh record")
                    };
                    let selector = matches!($record, RestoreControlRecord::Selector);
                    let second = write_restore_control_record(
                        &mut io,
                        &mut transport,
                        &candidate,
                        $record,
                        &encoded,
                        selector.then_some(version.as_str()),
                    )
                    .await
                    .expect("CAS or exact immutable replay");
                    assert_eq!(
                        matches!(second.value(), arco_core::WriteResult::Success { .. }),
                        selector
                    );
                    let (raw, metadata) =
                        read_restore_control_record(&mut io, &mut transport, &candidate, $record)
                            .await
                            .expect("read")
                            .expect("present");
                    let decoded = codec::$decode(&mut io, &mut route, &raw)
                        .expect("admitted published bytes");
                    assert_eq!(decoded.value(), &$value);
                    drop((decoded, raw, metadata, first, second));
                    let before_cancel = io.allocation_evidence().1;
                    let mut pending = Box::pin(async move {
                        std::future::pending::<()>().await;
                        drop(encoded);
                    });
                    assert!(futures::poll!(&mut pending).is_pending());
                    drop(pending);
                    assert!(io.allocation_evidence().1 < before_cancel);
                    println!(
                        "unit encoding {} final={final_stream} allocated={}",
                        stringify!($encode),
                        after.1 - before.1
                    );
                }};
            }
            check!(
                encode_selector,
                decode_selector,
                RestoreControlRecord::Selector,
                selector
            );
            check!(
                encode_progress,
                decode_progress,
                RestoreControlRecord::Progress(0),
                progress
            );
            check!(
                encode_receipt,
                decode_receipt,
                RestoreControlRecord::Receipt(0),
                receipt
            );
        }
    }

    #[tokio::test]
    async fn admitted_unit_codec_roundtrips_all_families_with_owned_results_on_both_routes() {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals, UnitPayloadAdmission};
        let (store, plan) =
            super::super::tests::inspection_fixture_for_domain("catalog\"quoted🔥").await;
        let digest = prefixed_sha256(b"codec fixture");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        let progress = genesis_progress(&expected);
        assert_eq!(progress.identity.domain(), "catalog\"quoted🔥");
        let progress_raw = encode(&progress, "progress");
        let selector = selector(&expected, &progress, &progress_raw);
        let receipt = codec_max_receipt(&expected);
        for final_stream in [false, true] {
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            macro_rules! check {
                ($decode:ident, $value:expr) => {{
                    let encoded = encode(&$value, "roundtrip fixture");
                    let raw = codec_raw(&mut io, &mut route, &encoded).await;
                    let before = io.allocation_evidence();
                    let decoded =
                        codec::$decode(&mut io, &mut route, &raw).expect("admitted codec");
                    assert_eq!(decoded.value(), &$value);
                    let during = io.allocation_evidence();
                    assert!(during.1 > before.1);
                    tokio::task::yield_now().await;
                    assert_eq!(io.allocation_evidence(), during);
                    let mut pending = Box::pin(async move {
                        std::future::pending::<()>().await;
                        drop(decoded);
                    });
                    assert!(futures::poll!(&mut pending).is_pending());
                    drop(pending);
                    assert_eq!(io.allocation_evidence().1, before.1);
                    println!(
                        "unit codec {} final={final_stream} P={} allocated={}",
                        stringify!($decode),
                        raw.as_slice().len(),
                        during.0 - before.0
                    );
                }};
            }
            check!(decode_selector, selector);
            check!(decode_progress, progress);
            check!(decode_receipt, receipt);
        }
    }

    #[tokio::test]
    async fn admitted_unit_codec_rejects_noncanonical_and_malformed_records_with_terminal_ownership()
     {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::UnitPayloadAdmission;
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"codec fixture");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        let progress = genesis_progress(&expected);
        let canonical = String::from_utf8(encode(&progress, "progress")).expect("UTF8");
        let cases = [
            canonical.replacen("\"last_receipt\":null,", "", 1),
            format!(" {canonical}"),
            canonical.replacen("\"version\":1", "\"version\":1,\"version\":1", 1),
            format!("{{\"{}\":0}}", "\\u007f".repeat(24_000)),
            format!("{{\"version\":0.{}1e-309}}", "0".repeat(160_000)),
            format!("{{\"version\":\"{}\"}}", "\u{7f}".repeat(160_000)),
        ];
        for (case, raw) in cases.iter().enumerate() {
            assert_ne!(raw, &canonical);
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let raw = codec_raw(&mut io, &mut route, raw.as_bytes()).await;
            let before = io.allocation_evidence();
            assert!(
                matches!(
                    codec::decode_progress(&mut io, &mut route, &raw),
                    Err(CatalogError::InvariantViolation { .. })
                ),
                "a measured underestimate cannot count as JSON rejection"
            );
            let after = io.allocation_evidence();
            assert!(after.1 > before.1, "failed allocations remain owned");
            let mut executed = false;
            assert!(
                decode_with_reservation(&mut io, &mut route, Some(64), || {
                    executed = true;
                    Ok(())
                })
                .is_err()
            );
            assert!(!executed);
            println!(
                "unit codec rejected case={case} P={} allocated={}",
                raw.as_slice().len(),
                after.0 - before.0
            );
        }
    }

    #[tokio::test]
    async fn admitted_unit_codec_final_carry_and_legal_large_escapes_reject_before_parse() {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::{FinalMicrochunk, FinalStreamTotals, UnitPayloadAdmission};
        let (store, plan) = super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"codec fixture");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        for large in [false, true] {
            let mut progress = genesis_progress(&expected);
            if large {
                progress.record_type = "\\".repeat(1024 * 1024);
            }
            let raw = encode(&progress, "legal fixture");
            assert!(raw.len() <= UNIT_RECORD_BYTES);
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut ordinary = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let raw = codec_raw(&mut io, &mut ordinary, &raw).await;
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(
                &mut totals,
                if large { 0 } else { 63 * 1024 * 1024 },
                &mut io,
            )
            .expect("carry");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let before = io.decoding_operations();
            assert!(matches!(
                codec::decode_progress(&mut io, &mut route, &raw),
                Err(CatalogError::MaintenanceBackpressure { .. })
            ));
            assert_eq!(io.decoding_operations(), before);
        }
    }

    #[tokio::test]
    async fn bounded_unit_decode_all_receipt_list_limits_preserve_canonical_shapes() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");
        let base = receipt(
            &expected,
            0,
            genesis_receipt_raw_sha256(&expected).expect("genesis receipt"),
            genesis_chain_sha256(&expected).expect("chain"),
            zero_cursor(),
            zero_cursor(),
            CumulativeSemanticCounts::default(),
        );
        let output = audit_output(&expected, 0, 0, "YQ");
        let input = InputWitness {
            role: physical::Role::Kv,
            root_b64: expected.source_kv_root_b64.into(),
            directory_leaf: DirectoryPositionLeafWitness {
                first_b64url: "YQ".into(),
                last_b64url: "YQ".into(),
                rows: 1,
                bytes: 1,
                digest: output.directory_leaf.digest.clone(),
            },
            descriptor: output.descriptor.clone(),
            index: output.index.clone(),
            block: output.block.clone(),
        };
        for (field, limit, item) in [
            (
                "source_inputs",
                16,
                serde_json::to_value(&input).expect("input"),
            ),
            (
                "current_inputs",
                16,
                serde_json::to_value(&input).expect("input"),
            ),
            (
                "outputs",
                32,
                serde_json::to_value(&output).expect("output"),
            ),
        ] {
            for count in [0, 1, limit - 1, limit] {
                let mut value = serde_json::to_value(&base).expect("receipt");
                value[field] = serde_json::Value::Array(vec![item.clone(); count]);
                let raw = serde_jcs::to_vec(&value).expect("JCS fixture");
                let parsed = decode_jcs_exact::<ControlMvpRestoreReceiptV1>(
                    &raw,
                    UNIT_RECORD_BYTES,
                    "receipt",
                )
                .expect("bounded wire receipt");
                assert_eq!(
                    encode_jcs_record(&parsed, "receipt").expect("JCS typed"),
                    raw
                );
            }
            let mut value = serde_json::to_value(&base).expect("receipt");
            value[field] = serde_json::Value::Array(vec![item; limit + 1]);
            let raw = serde_jcs::to_vec(&value).expect("extra fixture");
            assert!(serde_json::from_slice::<ControlMvpRestoreReceiptV1>(&raw).is_err());
            value[field][limit] = serde_json::Value::Null;
            let raw = serde_jcs::to_vec(&value).expect("invalid extra fixture");
            let error = serde_json::from_slice::<ControlMvpRestoreReceiptV1>(&raw)
                .expect_err("extra entry");
            assert!(
                error
                    .to_string()
                    .contains("restore record list exceeds its declared bound"),
                "{error}"
            );
        }
    }

    fn standard_counts() -> SemanticCounts {
        SemanticCounts {
            source_leaves: 0,
            current_leaves: 0,
            input_encoded_bytes: 0,
            decoded_blocks: 0,
            decoded_rows: 0,
            output_blocks: 0,
            output_encoded_bytes: 0,
            mutations: 0,
            reservation: UnitReservationV1::Standard {
                combined_leaf_limit: STANDARD_COMBINED_LEAF_LIMIT,
                decode_limit: STANDARD_DECODE_LIMIT,
                input_byte_limit: STANDARD_INPUT_BYTE_LIMIT,
                output_block_limit: STANDARD_OUTPUT_BLOCK_LIMIT,
                packed_output_byte_limit: STANDARD_PACKED_OUTPUT_BYTE_LIMIT,
            },
        }
    }

    fn genesis_progress(expected: &ExpectedPlan<'_>) -> RestoreProgressV1 {
        RestoreProgressV1 {
            record_type: "control_mvp_restore_progress".into(),
            version: 1,
            plan_sha256: expected.plan_sha256.into(),
            identity: expected.identity.clone(),
            owner_generation: expected.owner_generation,
            next_ordinal: 0,
            receipt_count: 0,
            last_receipt: None,
            chain_sha256: genesis_chain_sha256(expected).expect("genesis chain"),
            cursor: zero_cursor(),
            singleton_state: SingletonState::None,
            terminal: false,
            cumulative_counts: CumulativeSemanticCounts::default(),
        }
    }

    fn selector(
        expected: &ExpectedPlan<'_>,
        progress: &RestoreProgressV1,
        progress_raw: &[u8],
    ) -> RestoreProgressSelectorV1 {
        RestoreProgressSelectorV1 {
            record_type: "control_mvp_restore_progress_selector".into(),
            version: 1,
            plan_sha256: expected.plan_sha256.into(),
            identity: expected.identity.clone(),
            owner_generation: expected.owner_generation,
            current_progress_path: expected.progress_path(progress.next_ordinal),
            current_progress_sha256: receipt_raw_sha256(progress_raw),
        }
    }

    pub(super) fn receipt(
        expected: &ExpectedPlan<'_>,
        ordinal: u64,
        predecessor_receipt_sha256: String,
        predecessor_chain_sha256: String,
        before: MergeCursor,
        after: MergeCursor,
        prefix_cumulative_counts: CumulativeSemanticCounts,
    ) -> ControlMvpRestoreReceiptV1 {
        let mut receipt = ControlMvpRestoreReceiptV1 {
            record_type: "control_mvp_restore_receipt".into(),
            version: 1,
            plan_sha256: expected.plan_sha256.into(),
            identity: expected.identity.clone(),
            owner_generation: expected.owner_generation,
            ordinal,
            predecessor_receipt_sha256,
            predecessor_chain_sha256,
            before,
            after,
            singleton_before: SingletonState::None,
            singleton_after: SingletonState::None,
            source_inputs: Vec::new(),
            current_inputs: Vec::new(),
            outputs: Vec::new(),
            prefix_cumulative_counts,
            counts: standard_counts(),
            receipt_body_sha256: String::new(),
            chain_sha256: String::new(),
        };
        receipt.receipt_body_sha256 = receipt_body_sha256(&receipt).expect("receipt body");
        receipt.chain_sha256 = receipt_chain_sha256(&receipt).expect("receipt chain");
        receipt
    }

    pub(super) fn progress_after(
        expected: &ExpectedPlan<'_>,
        receipt: &ControlMvpRestoreReceiptV1,
        receipt_raw: &[u8],
        terminal: bool,
    ) -> RestoreProgressV1 {
        RestoreProgressV1 {
            record_type: "control_mvp_restore_progress".into(),
            version: 1,
            plan_sha256: expected.plan_sha256.into(),
            identity: expected.identity.clone(),
            owner_generation: expected.owner_generation,
            next_ordinal: receipt.ordinal.checked_add(1).expect("fixture ordinal"),
            receipt_count: receipt.ordinal.checked_add(1).expect("fixture ordinal"),
            last_receipt: Some(ReceiptRef {
                path: expected.receipt_path(receipt.ordinal),
                raw_sha256: receipt_raw_sha256(receipt_raw),
            }),
            chain_sha256: receipt.chain_sha256.clone(),
            cursor: receipt.after.clone(),
            singleton_state: receipt.singleton_after.clone(),
            terminal,
            cumulative_counts: checked_sum(&receipt.prefix_cumulative_counts, &receipt.counts)
                .expect("fixture counts"),
        }
    }

    async fn selected_plan() -> (ControlMvpRestorePlanV7, String) {
        let (_store, plan) = super::super::tests::inspection_fixture().await;
        let raw = jcs(
            &super::super::super::super::PersistedRestoreParticipantPlan::ControlMvpV7(Box::new(
                plan.clone(),
            )),
        )
        .expect("Plan7 wire JCS");
        (plan, prefixed_sha256(&raw))
    }

    fn encode<T: Serialize>(value: &T, context: &str) -> Vec<u8> {
        encode_jcs_record(value, context).expect("fixture record JCS")
    }

    fn validate_genesis(
        expected: &ExpectedPlan<'_>,
        selector_raw: &[u8],
        progress_raw: &[u8],
    ) -> Result<()> {
        validate_selector_current(
            expected,
            &expected.selector_path(),
            selector_raw,
            &expected.progress_path(0),
            progress_raw,
            None,
            None,
        )
        .map(|_| ())
    }

    #[tokio::test]
    async fn exact_jcs_and_selected_plan_binding_reject_substitution() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");
        let progress = genesis_progress(&expected);
        let progress_raw = encode(&progress, "genesis progress");
        let selected = selector(&expected, &progress, &progress_raw);
        let selector_raw = encode(&selected, "genesis selector");

        validate_genesis(&expected, &selector_raw, &progress_raw).expect("exact selected genesis");

        let canonical = String::from_utf8(selector_raw.clone()).expect("selector UTF-8");
        let whitespace = format!(" {canonical}");
        assert!(
            decode_jcs_exact::<RestoreProgressSelectorV1>(
                whitespace.as_bytes(),
                UNIT_RECORD_BYTES,
                "selector",
            )
            .is_err(),
            "a typed selector must not accept a noncanonical raw spelling"
        );
        let unknown = format!("{},\"unknown\":true}}", &canonical[..canonical.len() - 1]);
        assert!(
            decode_jcs_exact::<RestoreProgressSelectorV1>(
                unknown.as_bytes(),
                UNIT_RECORD_BYTES,
                "selector",
            )
            .is_err(),
            "unknown selector fields must fail before selection"
        );
        let duplicate = format!(
            "{},\"owner_generation\":{}}}",
            &canonical[..canonical.len() - 1],
            selected.owner_generation
        );
        assert!(
            decode_jcs_exact::<RestoreProgressSelectorV1>(
                duplicate.as_bytes(),
                UNIT_RECORD_BYTES,
                "selector",
            )
            .is_err(),
            "a duplicate key must not be an alternate accepted selector spelling"
        );

        let foreign_candidate = "a".repeat(64);
        let foreign_candidate_path = expected
            .selector_path()
            .replace(expected.candidate_id(), &foreign_candidate);
        assert!(
            validate_selector_current(
                &expected,
                &foreign_candidate_path,
                &selector_raw,
                &expected.progress_path(0),
                &progress_raw,
                None,
                None,
            )
            .is_err(),
            "a selector from another candidate path must not be adopted"
        );

        let mut wrong_owner = selected.clone();
        wrong_owner.owner_generation = wrong_owner.owner_generation.checked_add(1).expect("owner");
        let wrong_owner_raw = encode(&wrong_owner, "wrong owner selector");
        assert!(
            validate_genesis(&expected, &wrong_owner_raw, &progress_raw).is_err(),
            "a selector with a substituted owner must not be adopted"
        );

        let mut wrong_plan = selected.clone();
        wrong_plan.plan_sha256 = format!("sha256:{}", "0".repeat(64));
        let wrong_plan_raw = encode(&wrong_plan, "wrong plan selector");
        assert!(
            validate_genesis(&expected, &wrong_plan_raw, &progress_raw).is_err(),
            "a selector with a substituted Plan7 digest must not be adopted"
        );

        let mut wrong_progress_digest = selected;
        wrong_progress_digest.current_progress_sha256 = format!("sha256:{}", "f".repeat(64));
        let wrong_progress_digest_raw = encode(&wrong_progress_digest, "wrong progress digest");
        assert!(
            validate_genesis(&expected, &wrong_progress_digest_raw, &progress_raw).is_err(),
            "a selector with a substituted progress digest must not be adopted"
        );
    }

    #[tokio::test]
    async fn local_edge_rejects_ordinal_and_direct_predecessor_discontinuity() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");
        let genesis = genesis_progress(&expected);
        let genesis_raw = encode(&genesis, "genesis progress");
        let genesis_selector = selector(&expected, &genesis, &genesis_raw);
        let genesis_selector_raw = encode(&genesis_selector, "genesis selector");

        let zero = receipt(
            &expected,
            0,
            genesis_receipt_raw_sha256(&expected).expect("genesis receipt"),
            genesis_chain_sha256(&expected).expect("genesis chain"),
            zero_cursor(),
            audit_after("YQ"),
            CumulativeSemanticCounts::default(),
        );
        let zero_raw = encode(&zero, "ordinal zero receipt");
        let one = progress_after(&expected, &zero, &zero_raw, false);
        let one_raw = encode(&one, "progress one");

        let mut skipped = zero.clone();
        skipped.ordinal = 1;
        skipped.receipt_body_sha256 = receipt_body_sha256(&skipped).expect("body");
        skipped.chain_sha256 = receipt_chain_sha256(&skipped).expect("chain");
        let skipped_raw = encode(&skipped, "skipped receipt");
        let skipped_progress = progress_after(&expected, &skipped, &skipped_raw, false);
        let skipped_progress_raw = encode(&skipped_progress, "skipped progress");
        assert!(
            validate_local_advance(
                &expected,
                &expected.selector_path(),
                &genesis_selector_raw,
                &expected.progress_path(0),
                &genesis_raw,
                None,
                None,
                &expected.receipt_path(1),
                &skipped_raw,
                &expected.progress_path(2),
                &skipped_progress_raw,
            )
            .is_err(),
            "an ordinal-zero selected progress must not accept ordinal one"
        );

        let one_selector = selector(&expected, &one, &one_raw);
        let one_selector_raw = encode(&one_selector, "progress one selector");
        let wrong_predecessor = receipt(
            &expected,
            1,
            format!("sha256:{}", "0".repeat(64)),
            zero.chain_sha256,
            one.cursor.clone(),
            audit_after("Yg"),
            one.cumulative_counts,
        );
        let wrong_predecessor_raw = encode(&wrong_predecessor, "wrong predecessor receipt");
        let two = progress_after(&expected, &wrong_predecessor, &wrong_predecessor_raw, false);
        let two_raw = encode(&two, "progress two");
        assert_eq!(
            validate_local_advance(
                &expected,
                &expected.selector_path(),
                &one_selector_raw,
                &expected.progress_path(1),
                &one_raw,
                Some(&expected.receipt_path(0)),
                Some(&zero_raw),
                &expected.receipt_path(1),
                &wrong_predecessor_raw,
                &expected.progress_path(2),
                &two_raw,
            )
            .expect_err("changed direct predecessor")
            .to_string(),
            "invariant violation: proposed receipt predecessor differs"
        );
    }

    #[tokio::test]
    async fn local_edge_rejects_repeated_global_cut() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");

        let genesis = genesis_progress(&expected);
        let genesis_raw = encode(&genesis, "genesis progress");
        let selector_raw = encode(&selector(&expected, &genesis, &genesis_raw), "selector");
        let first = receipt(
            &expected,
            0,
            genesis_receipt_raw_sha256(&expected).expect("genesis receipt"),
            genesis_chain_sha256(&expected).expect("genesis chain"),
            zero_cursor(),
            MergeCursor {
                global: GlobalCut::After {
                    key_b64url: "aw".into(),
                },
                source: SideCursor::Start,
                current: SideCursor::Start,
            },
            CumulativeSemanticCounts::default(),
        );
        let first_raw = encode(&first, "first receipt");
        let first_progress = progress_after(&expected, &first, &first_raw, false);
        let first_progress_raw = encode(&first_progress, "first progress");
        validate_local_advance(
            &expected,
            &expected.selector_path(),
            &selector_raw,
            &expected.progress_path(0),
            &genesis_raw,
            None,
            None,
            &expected.receipt_path(0),
            &first_raw,
            &expected.progress_path(1),
            &first_progress_raw,
        )
        .expect("first local edge");

        let repeated = receipt(
            &expected,
            1,
            receipt_raw_sha256(&first_raw),
            first.chain_sha256,
            first_progress.cursor.clone(),
            first_progress.cursor.clone(),
            first_progress.cumulative_counts.clone(),
        );
        let repeated_raw = encode(&repeated, "repeated cut receipt");
        let repeated_progress = progress_after(&expected, &repeated, &repeated_raw, false);
        let repeated_progress_raw = encode(&repeated_progress, "repeated cut progress");
        let first_selector_raw = encode(
            &selector(&expected, &first_progress, &first_progress_raw),
            "first progress selector",
        );
        assert!(
            validate_local_advance(
                &expected,
                &expected.selector_path(),
                &first_selector_raw,
                &expected.progress_path(1),
                &first_progress_raw,
                Some(&expected.receipt_path(0)),
                Some(&first_raw),
                &expected.receipt_path(1),
                &repeated_raw,
                &expected.progress_path(2),
                &repeated_progress_raw,
            )
            .is_err(),
            "the next receipt must strictly advance a nonterminal global cut"
        );
    }

    #[tokio::test]
    async fn local_edge_rejects_ordinal_overflow() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");
        let near_last = receipt(
            &expected,
            u64::MAX - 1,
            genesis_receipt_raw_sha256(&expected).expect("genesis receipt"),
            genesis_chain_sha256(&expected).expect("genesis chain"),
            zero_cursor(),
            audit_after("YQ"),
            CumulativeSemanticCounts::default(),
        );
        let near_last_raw = encode(&near_last, "near-last receipt");
        let selected_max = progress_after(&expected, &near_last, &near_last_raw, false);
        let selected_max_raw = encode(&selected_max, "max progress");
        let max_selector_raw = encode(
            &selector(&expected, &selected_max, &selected_max_raw),
            "max selector",
        );
        let attempted_overflow = receipt(
            &expected,
            u64::MAX,
            receipt_raw_sha256(&near_last_raw),
            near_last.chain_sha256,
            selected_max.cursor.clone(),
            audit_after("Yg"),
            selected_max.cumulative_counts.clone(),
        );
        let attempted_overflow_raw = encode(&attempted_overflow, "overflow receipt");
        // Its progress is syntactically valid but cannot be the successor of
        // `progress/u64::MAX`: local validation must fail the checked addition
        // before accepting it as a selected edge.
        let proposed_max = selected_max;
        let proposed_max_raw = encode(&proposed_max, "overflow proposed progress");
        assert_eq!(
            validate_local_advance(
                &expected,
                &expected.selector_path(),
                &max_selector_raw,
                &expected.progress_path(u64::MAX),
                &selected_max_raw,
                Some(&expected.receipt_path(u64::MAX - 1)),
                Some(&near_last_raw),
                &expected.receipt_path(u64::MAX),
                &attempted_overflow_raw,
                &expected.progress_path(u64::MAX),
                &proposed_max_raw,
            )
            .expect_err("ordinal overflow")
            .to_string(),
            "invariant violation: progress ordinal overflow"
        );
    }

    #[tokio::test]
    async fn local_edge_rejects_cumulative_counter_overflow() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");
        let genesis = genesis_progress(&expected);
        let genesis_raw = encode(&genesis, "genesis progress");
        let genesis_selector_raw = encode(&selector(&expected, &genesis, &genesis_raw), "selector");

        let mut max_counts = standard_counts();
        max_counts.mutations = u64::MAX;
        let mut first = receipt(
            &expected,
            0,
            genesis_receipt_raw_sha256(&expected).expect("genesis receipt"),
            genesis_chain_sha256(&expected).expect("genesis chain"),
            zero_cursor(),
            audit_after("YQ"),
            CumulativeSemanticCounts::default(),
        );
        first.counts = max_counts;
        first.receipt_body_sha256 = receipt_body_sha256(&first).expect("body");
        first.chain_sha256 = receipt_chain_sha256(&first).expect("chain");
        let first_raw = encode(&first, "max-count first receipt");
        let selected = progress_after(&expected, &first, &first_raw, false);
        let selected_raw = encode(&selected, "max-count selected progress");
        validate_local_advance(
            &expected,
            &expected.selector_path(),
            &genesis_selector_raw,
            &expected.progress_path(0),
            &genesis_raw,
            None,
            None,
            &expected.receipt_path(0),
            &first_raw,
            &expected.progress_path(1),
            &selected_raw,
        )
        .expect("u64::MAX is still a valid selected total");

        let mut one_count = standard_counts();
        one_count.mutations = 1;
        let mut overflowing = receipt(
            &expected,
            1,
            receipt_raw_sha256(&first_raw),
            first.chain_sha256,
            selected.cursor.clone(),
            audit_after("Yg"),
            selected.cumulative_counts.clone(),
        );
        overflowing.counts = one_count;
        overflowing.receipt_body_sha256 = receipt_body_sha256(&overflowing).expect("body");
        overflowing.chain_sha256 = receipt_chain_sha256(&overflowing).expect("chain");
        let overflowing_raw = encode(&overflowing, "overflowing receipt");

        // `RestoreProgressV1` cannot derive this total.  Supply a syntactically
        // shaped proposed progress so `validate_local_advance` reaches its
        // checked cumulative addition instead of accepting a wrapped value.
        let proposed = RestoreProgressV1 {
            next_ordinal: 2,
            receipt_count: 2,
            last_receipt: Some(ReceiptRef {
                path: expected.receipt_path(1),
                raw_sha256: receipt_raw_sha256(&overflowing_raw),
            }),
            chain_sha256: overflowing.chain_sha256.clone(),
            cursor: overflowing.after.clone(),
            singleton_state: overflowing.singleton_after,
            terminal: false,
            cumulative_counts: selected.cumulative_counts.clone(),
            ..selected.clone()
        };
        let proposed_raw = encode(&proposed, "overflow proposed progress");
        let selected_selector_raw = encode(
            &selector(&expected, &selected, &selected_raw),
            "max-count selected selector",
        );
        assert_eq!(
            validate_local_advance(
                &expected,
                &expected.selector_path(),
                &selected_selector_raw,
                &expected.progress_path(1),
                &selected_raw,
                Some(&expected.receipt_path(0)),
                Some(&first_raw),
                &expected.receipt_path(1),
                &overflowing_raw,
                &expected.progress_path(2),
                &proposed_raw,
            )
            .expect_err("cumulative overflow")
            .to_string(),
            "invariant violation: cumulative semantic count overflow"
        );
    }
    fn audit_singleton_value(key_b64url: &str) -> SingletonValueWitness {
        let digest = format!("sha256:{}", "0".repeat(64));
        SingletonValueWitness {
            generation: 1,
            tombstone: false,
            value_length: 1,
            value_sha256: digest.clone(),
            descriptor: ImmutableObjectWitness {
                path: "descriptor".into(),
                byte_size: 1,
                sha256: digest.clone(),
            },
            index: ImmutableObjectWitness {
                path: "index".into(),
                byte_size: 1,
                sha256: digest.clone(),
            },
            block: BlockWitness {
                offset: 0,
                length: 1,
                sha256: digest,
                rows: 1,
                min_key_b64url: key_b64url.into(),
                max_key_b64url: key_b64url.into(),
            },
            row_ordinal: 0,
        }
    }

    fn audit_pending(key_b64url: &str, phase: SingletonPhase) -> SingletonState {
        SingletonState::Pending {
            key_b64url: key_b64url.into(),
            source: Some(Box::new(audit_singleton_value(key_b64url))),
            current: None,
            phase,
        }
    }

    #[test]
    fn singleton_comparison_accumulates_only_the_unobserved_current_witness() {
        let before = audit_pending("YQ", SingletonPhase::CompareSource);
        let mut after = audit_pending("YQ", SingletonPhase::CompareCurrent);
        if let SingletonState::Pending { current, .. } = &mut after {
            *current = Some(Box::new(audit_singleton_value("YQ")));
        }
        assert!(
            validate_singleton_transition(&before, &after).is_ok(),
            "separate bounded reads must be able to retain the second value witness"
        );
        for mutation in 0..6 {
            let mut changed = after.clone();
            if let SingletonState::Pending {
                key_b64url,
                source,
                current,
                phase,
            } = &mut changed
            {
                match mutation {
                    0 => *key_b64url = "Yg".into(),
                    1 => source.as_mut().unwrap().generation += 1,
                    2 => *source = None,
                    3 => *phase = SingletonPhase::Emit,
                    4 => *phase = SingletonPhase::CompareSource,
                    5 => {
                        *source = None;
                        *current = None;
                    }
                    _ => unreachable!(),
                }
            }
            assert!(
                validate_singleton_transition(&before, &changed).is_err(),
                "mutation={mutation}"
            );
        }
        let mut observed = after.clone();
        if let SingletonState::Pending { phase, .. } = &mut observed {
            *phase = SingletonPhase::CompareSource;
        }
        for remove in [false, true] {
            let mut changed = after.clone();
            if let SingletonState::Pending { current, .. } = &mut changed {
                if remove {
                    *current = None;
                } else {
                    current.as_mut().unwrap().generation += 1;
                }
            }
            assert!(validate_singleton_transition(&observed, &changed).is_err());
        }
        let mut emit = after.clone();
        if let SingletonState::Pending { phase, .. } = &mut emit {
            *phase = SingletonPhase::Emit;
        }
        assert!(validate_singleton_transition(&after, &emit).is_ok());
        let absent = audit_pending("YQ", SingletonPhase::CompareCurrent);
        assert!(
            validate_singleton_transition(&absent, &emit).is_err(),
            "current cannot appear after comparison"
        );
    }

    fn audit_after(key_b64url: &str) -> MergeCursor {
        MergeCursor {
            global: GlobalCut::After {
                key_b64url: key_b64url.into(),
            },
            source: SideCursor::Start,
            current: SideCursor::Start,
        }
    }

    #[tokio::test]
    async fn receipt_chain_rejects_raw_type_disconnect_and_non_genesis_endpoint() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");
        let zero = receipt(
            &expected,
            0,
            genesis_receipt_raw_sha256(&expected).expect("genesis receipt"),
            genesis_chain_sha256(&expected).expect("genesis chain"),
            zero_cursor(),
            audit_after("YQ"),
            CumulativeSemanticCounts::default(),
        );
        let zero_raw = encode(&zero, "receipt zero");
        let disconnected = receipt(
            &expected,
            1,
            receipt_raw_sha256(&zero_raw),
            zero.chain_sha256,
            zero_cursor(), // must equal zero.after, but does not
            MergeCursor {
                global: GlobalCut::End,
                source: SideCursor::End,
                current: SideCursor::End,
            },
            CumulativeSemanticCounts::default(),
        );
        let disconnected_raw = encode(&disconnected, "disconnected receipt");
        let (_, zero_tail) =
            validate_receipt_chain(&expected, &zero_raw, None).expect("genesis edge");
        assert!(
            validate_receipt_chain(&expected, &disconnected_raw, Some(&zero_tail),).is_err(),
            "a direct digest edge must also preserve cursor/singleton/count endpoints"
        );
        assert!(
            validate_receipt_chain(&expected, b"{}", None).is_err(),
            "typed receipt fields must be decoded from the supplied exact raw bytes"
        );

        let wrong_genesis_endpoint = receipt(
            &expected,
            0,
            genesis_receipt_raw_sha256(&expected).expect("genesis receipt"),
            genesis_chain_sha256(&expected).expect("genesis chain"),
            audit_after("YQ"), // ordinal zero must start at the genesis cursor
            MergeCursor {
                global: GlobalCut::End,
                source: SideCursor::End,
                current: SideCursor::End,
            },
            CumulativeSemanticCounts::default(),
        );
        let wrong_genesis_raw = encode(&wrong_genesis_endpoint, "wrong genesis endpoint");
        assert!(
            validate_receipt_chain(&expected, &wrong_genesis_raw, None).is_err(),
            "ordinal zero must bind the zero cursor, singleton none, and zero prefix"
        );
    }

    #[tokio::test]
    async fn selected_progress_rejects_last_receipt_with_wrong_internal_ordinal() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");
        let zero = receipt(
            &expected,
            0,
            genesis_receipt_raw_sha256(&expected).expect("genesis receipt"),
            genesis_chain_sha256(&expected).expect("genesis chain"),
            zero_cursor(),
            audit_after("YQ"),
            CumulativeSemanticCounts::default(),
        );
        let zero_raw = encode(&zero, "receipt zero");
        let selected = RestoreProgressV1 {
            record_type: "control_mvp_restore_progress".into(),
            version: 1,
            plan_sha256: expected.plan_sha256.into(),
            identity: expected.identity.clone(),
            owner_generation: expected.owner_generation,
            next_ordinal: 2,
            receipt_count: 2,
            // The path says ordinal one, but the exact raw object says ordinal zero.
            last_receipt: Some(ReceiptRef {
                path: expected.receipt_path(1),
                raw_sha256: receipt_raw_sha256(&zero_raw),
            }),
            chain_sha256: zero.chain_sha256.clone(),
            cursor: zero.after.clone(),
            singleton_state: zero.singleton_after.clone(),
            terminal: false,
            cumulative_counts: checked_sum(&zero.prefix_cumulative_counts, &zero.counts)
                .expect("counts"),
        };
        let selected_raw = encode(&selected, "selected progress");
        let selector_raw = encode(
            &selector(&expected, &selected, &selected_raw),
            "selected selector",
        );
        assert!(
            validate_selector_current(
                &expected,
                &expected.selector_path(),
                &selector_raw,
                &expected.progress_path(2),
                &selected_raw,
                Some(&expected.receipt_path(1)),
                Some(&zero_raw),
            )
            .is_err(),
            "progress/N must bind receipts/(N-1) to an internal ordinal N-1"
        );

        let proposed = receipt(
            &expected,
            2,
            receipt_raw_sha256(&zero_raw),
            zero.chain_sha256,
            selected.cursor.clone(),
            MergeCursor {
                global: GlobalCut::End,
                source: SideCursor::End,
                current: SideCursor::End,
            },
            selected.cumulative_counts,
        );
        let proposed_raw = encode(&proposed, "proposed receipt");
        let next = progress_after(&expected, &proposed, &proposed_raw, true);
        let next_raw = encode(&next, "proposed progress");
        assert!(
            validate_local_advance(
                &expected,
                &expected.selector_path(),
                &selector_raw,
                &expected.progress_path(2),
                &selected_raw,
                Some(&expected.receipt_path(1)),
                Some(&zero_raw),
                &expected.receipt_path(2),
                &proposed_raw,
                &expected.progress_path(3),
                &next_raw,
            )
            .is_err(),
            "a forged selected ordinal must not anchor another locally valid edge"
        );
    }

    #[tokio::test]
    async fn cursor_validator_allows_retained_positions_and_singleton_phase_steps_only() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");
        let digest = format!("sha256:{}", "1".repeat(64));
        let position = DirectoryPosition {
            role: physical::Role::Kv,
            root_b64: expected.source_kv_root_b64.into(),
            path: Vec::new(),
            leaf: DirectoryPositionLeafWitness {
                first_b64url: "YQ".into(),
                last_b64url: "eg".into(),
                rows: 1,
                bytes: 1,
                digest,
            },
        };
        let retained_source = SideCursor::After {
            key_b64url: "YQ".into(),
            position,
        };
        let before_gap = MergeCursor {
            global: GlobalCut::Start,
            source: retained_source.clone(),
            current: SideCursor::Start,
        };
        let after_gap = MergeCursor {
            global: GlobalCut::After {
                key_b64url: "Yg".into(),
            },
            source: retained_source,
            current: SideCursor::Start,
        };
        assert!(
            validate_cursor_transition(
                &expected,
                &before_gap,
                &after_gap,
                &SingletonState::None,
                &SingletonState::None,
            )
            .is_ok(),
            "one side may retain its exact After position while another cut advances"
        );

        let compare_source = audit_pending("YQ", SingletonPhase::CompareSource);
        let compare_current = audit_pending("YQ", SingletonPhase::CompareCurrent);
        let held = audit_after("YA");
        assert!(
            validate_cursor_transition(&expected, &held, &held, &compare_source, &compare_current,)
                .is_ok(),
            "a singleton phase transition must retain the exact merge cursor"
        );
        assert!(
            validate_cursor_transition(
                &expected,
                &zero_cursor(),
                &audit_after("YQ"),
                &SingletonState::None,
                &compare_source,
            )
            .is_err(),
            "creating a pending singleton cannot move the global cut past its key"
        );
        assert!(
            validate_cursor_transition(
                &expected,
                &audit_after("YQ"),
                &audit_after("YQ"),
                &SingletonState::Complete {
                    key_b64url: "YQ".into(),
                },
                &SingletonState::None,
            )
            .is_ok(),
            "complete-to-none cleanup must retain the already advanced cut"
        );
    }

    #[test]
    fn singleton_reservation_checks_payload_range_without_standard_packing_cap() {
        let before = audit_pending("YQ", SingletonPhase::CompareSource);
        let after = audit_pending("YQ", SingletonPhase::CompareCurrent);
        let singleton_counts = |payload, output_bytes| SemanticCounts {
            source_leaves: 0,
            current_leaves: 0,
            input_encoded_bytes: 0,
            decoded_blocks: 0,
            decoded_rows: 0,
            output_blocks: u64::from(output_bytes != 0),
            output_encoded_bytes: output_bytes,
            mutations: 0,
            reservation: UnitReservationV1::Singleton {
                phase: SingletonPhase::CompareSource,
                authenticated_payload_bytes: payload,
                input_byte_limit: SINGLETON_INPUT_BYTE_LIMIT,
                segment_byte_limit: SINGLETON_SEGMENT_BYTE_LIMIT,
                scratch_base_bytes: SINGLETON_SCRATCH_BASE_BYTES,
                scratch_payload_multiplier: SINGLETON_SCRATCH_PAYLOAD_MULTIPLIER,
            },
        };
        assert!(validate_reservation(&singleton_counts(0, 0), &before, &after).is_err());
        assert!(
            validate_reservation(
                &singleton_counts(SINGLETON_SEGMENT_BYTE_LIMIT + 1, 0),
                &before,
                &after,
            )
            .is_err()
        );

        let emit = audit_pending("YQ", SingletonPhase::Emit);
        let complete = SingletonState::Complete {
            key_b64url: "YQ".into(),
        };
        let valid_large = SemanticCounts {
            reservation: UnitReservationV1::Singleton {
                phase: SingletonPhase::Emit,
                authenticated_payload_bytes: 40 * 1024 * 1024,
                input_byte_limit: SINGLETON_INPUT_BYTE_LIMIT,
                segment_byte_limit: SINGLETON_SEGMENT_BYTE_LIMIT,
                scratch_base_bytes: SINGLETON_SCRATCH_BASE_BYTES,
                scratch_payload_multiplier: SINGLETON_SCRATCH_PAYLOAD_MULTIPLIER,
            },
            output_blocks: 1,
            output_encoded_bytes: 40 * 1024 * 1024,
            ..standard_counts()
        };
        assert!(
            validate_reservation(&valid_large, &emit, &complete).is_ok(),
            "the singleton 64 MiB segment ceiling is distinct from standard 32 MiB packing"
        );
    }

    fn audit_output(
        expected: &ExpectedPlan<'_>,
        ordinal: u64,
        part: u32,
        key_b64url: &str,
    ) -> OutputWitness {
        let digest = format!("sha256:{}", "2".repeat(64));
        OutputWitness {
            role: physical::Role::Kv,
            output_id: output_id(expected, ordinal, part, physical::Role::Kv).expect("output id"),
            part,
            directory_leaf: OutputDirectoryLeafWitness {
                first_key_b64url: key_b64url.into(),
                last_key_b64url: key_b64url.into(),
                rows: 1,
                bytes: 1,
                digest: digest.clone(),
            },
            descriptor: ImmutableObjectWitness {
                path: format!("descriptor-{key_b64url}"),
                byte_size: 1,
                sha256: digest.clone(),
            },
            index: ImmutableObjectWitness {
                path: format!("index-{key_b64url}"),
                byte_size: 1,
                sha256: digest.clone(),
            },
            block: BlockWitness {
                offset: 0,
                length: 1,
                sha256: digest,
                rows: 1,
                min_key_b64url: key_b64url.into(),
                max_key_b64url: key_b64url.into(),
            },
        }
    }

    #[tokio::test]
    async fn output_parts_are_unique_and_ordered_with_their_leaf_witnesses() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");
        let outputs = [
            audit_output(&expected, 7, 0, "YQ"),
            // Disjoint key ranges currently hide the duplicate deterministic part/id.
            audit_output(&expected, 7, 0, "Yg"),
        ];
        assert!(
            strictly_ordered_outputs(&expected, 7, &outputs).is_err(),
            "one ordinal cannot bind two distinct leaves to the same output part/id"
        );
    }
    #[tokio::test]
    async fn terminal_selected_progress_cannot_accept_another_receipt() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");
        let end = MergeCursor {
            global: GlobalCut::End,
            source: SideCursor::End,
            current: SideCursor::End,
        };
        let mut last = receipt(
            &expected,
            3,
            format!("sha256:{}", "1".repeat(64)),
            format!("sha256:{}", "2".repeat(64)),
            zero_cursor(),
            end.clone(),
            CumulativeSemanticCounts::default(),
        );
        last.singleton_before = audit_pending("YQ", SingletonPhase::Emit);
        last.singleton_after = SingletonState::Complete {
            key_b64url: "YQ".into(),
        };
        last.counts.reservation = UnitReservationV1::Singleton {
            phase: SingletonPhase::Emit,
            authenticated_payload_bytes: 1,
            input_byte_limit: SINGLETON_INPUT_BYTE_LIMIT,
            segment_byte_limit: SINGLETON_SEGMENT_BYTE_LIMIT,
            scratch_base_bytes: SINGLETON_SCRATCH_BASE_BYTES,
            scratch_payload_multiplier: SINGLETON_SCRATCH_PAYLOAD_MULTIPLIER,
        };
        last.receipt_body_sha256 = receipt_body_sha256(&last).expect("body");
        last.chain_sha256 = receipt_chain_sha256(&last).expect("chain");
        let last_raw = encode(&last, "selected last");
        let selected = progress_after(&expected, &last, &last_raw, true);
        let selected_raw = encode(&selected, "terminal progress");
        let selected_selector = encode(&selector(&expected, &selected, &selected_raw), "selector");
        validate_selector_current(
            &expected,
            &expected.selector_path(),
            &selected_selector,
            &expected.progress_path(4),
            &selected_raw,
            Some(&expected.receipt_path(3)),
            Some(&last_raw),
        )
        .expect("well-formed selected terminal progress");
        let mut cleanup = receipt(
            &expected,
            4,
            receipt_raw_sha256(&last_raw),
            last.chain_sha256,
            end.clone(),
            end,
            selected.cumulative_counts,
        );
        cleanup.singleton_before = last.singleton_after;
        cleanup.receipt_body_sha256 = receipt_body_sha256(&cleanup).expect("body");
        cleanup.chain_sha256 = receipt_chain_sha256(&cleanup).expect("chain");
        let cleanup_raw = encode(&cleanup, "extra cleanup receipt");
        let proposed = progress_after(&expected, &cleanup, &cleanup_raw, true);
        let proposed_raw = encode(&proposed, "extra terminal progress");
        assert!(
            validate_local_advance(
                &expected,
                &expected.selector_path(),
                &selected_selector,
                &expected.progress_path(4),
                &selected_raw,
                Some(&expected.receipt_path(3)),
                Some(&last_raw),
                &expected.receipt_path(4),
                &cleanup_raw,
                &expected.progress_path(5),
                &proposed_raw
            )
            .is_err(),
            "the terminal tuple is immutable input to final verification, not another unit base"
        );
    }
    fn rehash_receipt(receipt: &mut ControlMvpRestoreReceiptV1) {
        receipt.receipt_body_sha256 = receipt_body_sha256(receipt).expect("receipt body");
        receipt.chain_sha256 = receipt_chain_sha256(receipt).expect("receipt chain");
    }

    fn with_mutations(
        mut receipt: ControlMvpRestoreReceiptV1,
        mutations: u64,
    ) -> ControlMvpRestoreReceiptV1 {
        receipt.counts.mutations = mutations;
        rehash_receipt(&mut receipt);
        receipt
    }

    #[tokio::test]
    async fn receipt_chain_accepts_two_raw_forward_edges_and_returns_the_exact_tail() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");
        let first = with_mutations(
            receipt(
                &expected,
                0,
                genesis_receipt_raw_sha256(&expected).expect("genesis receipt"),
                genesis_chain_sha256(&expected).expect("genesis chain"),
                zero_cursor(),
                audit_after("YQ"),
                CumulativeSemanticCounts::default(),
            ),
            1,
        );
        let first_raw = encode(&first, "first receipt");
        let (_decoded_first, first_tail) =
            validate_receipt_chain(&expected, &first_raw, None).expect("first raw edge");
        assert_eq!(first_tail.ordinal, 0);
        assert_eq!(first_tail.cumulative.mutations, 1);
        assert_eq!(first_tail.raw_sha256, receipt_raw_sha256(&first_raw));

        let second = with_mutations(
            receipt(
                &expected,
                1,
                first_tail.raw_sha256.clone(),
                first_tail.chain_sha256.clone(),
                first_tail.cursor.clone(),
                audit_after("Yg"),
                first_tail.cumulative.clone(),
            ),
            2,
        );
        let second_raw = encode(&second, "second receipt");
        let (decoded_second, second_tail) =
            validate_receipt_chain(&expected, &second_raw, Some(&first_tail))
                .expect("second raw edge");
        assert_eq!(decoded_second.ordinal, 1);
        assert_eq!(second_tail.ordinal, 1);
        assert_eq!(second_tail.cumulative.mutations, 3);
        assert_eq!(second_tail.chain_sha256, second.chain_sha256);
    }

    #[tokio::test]
    async fn receipt_chain_rejects_hash_consistent_direct_edge_substitutions() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");
        let first = with_mutations(
            receipt(
                &expected,
                0,
                genesis_receipt_raw_sha256(&expected).expect("genesis receipt"),
                genesis_chain_sha256(&expected).expect("genesis chain"),
                zero_cursor(),
                audit_after("YQ"),
                CumulativeSemanticCounts::default(),
            ),
            1,
        );
        let first_raw = encode(&first, "first receipt");
        let (_, first_tail) =
            validate_receipt_chain(&expected, &first_raw, None).expect("first raw edge");
        let second = with_mutations(
            receipt(
                &expected,
                1,
                first_tail.raw_sha256.clone(),
                first_tail.chain_sha256.clone(),
                first_tail.cursor.clone(),
                audit_after("Yg"),
                first_tail.cumulative.clone(),
            ),
            2,
        );

        let mut changed_before = second.clone();
        changed_before.before = zero_cursor();
        rehash_receipt(&mut changed_before);
        assert!(
            validate_receipt_chain(
                &expected,
                &encode(&changed_before, "changed before"),
                Some(&first_tail),
            )
            .is_err(),
            "a hash-consistent successor may not change its inherited before cursor"
        );

        let mut changed_singleton = second.clone();
        changed_singleton.singleton_before = SingletonState::Complete {
            key_b64url: "YQ".into(),
        };
        rehash_receipt(&mut changed_singleton);
        assert!(
            validate_receipt_chain(
                &expected,
                &encode(&changed_singleton, "changed singleton"),
                Some(&first_tail),
            )
            .is_err(),
            "a hash-consistent successor may not substitute singleton_before"
        );

        let mut reset_prefix = second;
        reset_prefix.prefix_cumulative_counts = CumulativeSemanticCounts::default();
        rehash_receipt(&mut reset_prefix);
        assert!(
            validate_receipt_chain(
                &expected,
                &encode(&reset_prefix, "reset prefix"),
                Some(&first_tail),
            )
            .is_err(),
            "a successor must retain the predecessor cumulative totals"
        );

        let mut nonzero_genesis_prefix = first;
        nonzero_genesis_prefix.prefix_cumulative_counts.mutations = 1;
        rehash_receipt(&mut nonzero_genesis_prefix);
        assert!(
            validate_receipt_chain(
                &expected,
                &encode(&nonzero_genesis_prefix, "nonzero genesis prefix"),
                None,
            )
            .is_err(),
            "ordinal zero must retain the all-zero cumulative prefix"
        );
        assert!(
            validate_receipt_chain(&expected, b"{}", None).is_err(),
            "the validator must decode and authenticate the supplied raw JSON"
        );
    }

    #[tokio::test]
    async fn cursor_and_outputs_reject_relabelled_retained_position_and_reversed_parts() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");
        let position = |digest_hex: &str| DirectoryPosition {
            role: physical::Role::Kv,
            root_b64: expected.source_kv_root_b64.into(),
            path: Vec::new(),
            leaf: DirectoryPositionLeafWitness {
                first_b64url: "YQ".into(),
                last_b64url: "YQ".into(),
                rows: 1,
                bytes: 1,
                digest: format!("sha256:{digest_hex}"),
            },
        };
        let before = MergeCursor {
            global: GlobalCut::After {
                key_b64url: "YQ".into(),
            },
            source: SideCursor::After {
                key_b64url: "YQ".into(),
                position: position(&"1".repeat(64)),
            },
            current: SideCursor::Start,
        };
        let after = MergeCursor {
            global: GlobalCut::After {
                key_b64url: "Yg".into(),
            },
            source: SideCursor::After {
                key_b64url: "YQ".into(),
                position: position(&"2".repeat(64)),
            },
            current: SideCursor::Start,
        };
        assert!(
            validate_cursor_transition(
                &expected,
                &before,
                &after,
                &SingletonState::None,
                &SingletonState::None,
            )
            .is_err(),
            "a retained After key cannot change its directory witness while another cut advances"
        );

        let reversed = [
            audit_output(&expected, 3, 1, "YQ"),
            audit_output(&expected, 3, 0, "Yg"),
        ];
        assert!(
            strictly_ordered_outputs(&expected, 3, &reversed).is_err(),
            "output parts must remain in increasing part order as well as key order"
        );
    }
    #[tokio::test]
    async fn forward_receipt_chain_cannot_continue_after_terminal_singleton() {
        let (plan, plan_sha256) = selected_plan().await;
        let expected = ExpectedPlan::from_selected(&plan, &plan_sha256).expect("selected");
        let mut tail: Option<ReceiptTail> = None;
        let mut before = SingletonState::None;
        for (ordinal, after) in [
            audit_pending("YQ", SingletonPhase::CompareSource),
            audit_pending("YQ", SingletonPhase::CompareCurrent),
            audit_pending("YQ", SingletonPhase::Emit),
            SingletonState::Complete {
                key_b64url: "YQ".into(),
            },
        ]
        .into_iter()
        .enumerate()
        {
            let phase = match (&before, &after) {
                (SingletonState::Pending { phase, .. }, _)
                | (_, SingletonState::Pending { phase, .. }) => *phase,
                _ => panic!("singleton phase fixture"),
            };
            let after_cursor = if matches!(after, SingletonState::Complete { .. }) {
                MergeCursor {
                    global: GlobalCut::End,
                    source: SideCursor::End,
                    current: SideCursor::End,
                }
            } else {
                zero_cursor()
            };
            let mut next = receipt(
                &expected,
                ordinal as u64,
                tail.as_ref().map_or_else(
                    || genesis_receipt_raw_sha256(&expected).expect("genesis raw"),
                    |tail| tail.raw_sha256.clone(),
                ),
                tail.as_ref().map_or_else(
                    || genesis_chain_sha256(&expected).expect("genesis chain"),
                    |tail| tail.chain_sha256.clone(),
                ),
                zero_cursor(),
                after_cursor,
                tail.as_ref()
                    .map_or_else(CumulativeSemanticCounts::default, |tail| {
                        tail.cumulative.clone()
                    }),
            );
            next.singleton_before = before;
            next.singleton_after = after.clone();
            next.counts.reservation = UnitReservationV1::Singleton {
                phase,
                authenticated_payload_bytes: 1,
                input_byte_limit: SINGLETON_INPUT_BYTE_LIMIT,
                segment_byte_limit: SINGLETON_SEGMENT_BYTE_LIMIT,
                scratch_base_bytes: SINGLETON_SCRATCH_BASE_BYTES,
                scratch_payload_multiplier: SINGLETON_SCRATCH_PAYLOAD_MULTIPLIER,
            };
            rehash_receipt(&mut next);
            let (_, next_tail) = validate_receipt_chain(
                &expected,
                &encode(&next, "singleton receipt"),
                tail.as_ref(),
            )
            .expect("valid structural singleton chain reaches terminal cut");
            tail = Some(next_tail);
            before = after;
        }
        let tail = tail.expect("four validated receipts");
        assert!(is_end_cursor(&tail.cursor));
        let mut cleanup = receipt(
            &expected,
            4,
            tail.raw_sha256.clone(),
            tail.chain_sha256.clone(),
            tail.cursor.clone(),
            tail.cursor.clone(),
            tail.cumulative.clone(),
        );
        cleanup.singleton_before = tail.singleton.clone();
        rehash_receipt(&mut cleanup);
        assert!(
            validate_receipt_chain(&expected, &encode(&cleanup, "late cleanup"), Some(&tail))
                .is_err(),
            "forward validation must reject a receipt after the terminal cut"
        );
    }
}
