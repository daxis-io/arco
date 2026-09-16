//! Owned logical history only; no publication authority.
use super::super::super::super::logical_v2;
use super::{
    OwnedSelectedPlan, RestorePhysicalIo, RestorePhysicalRoute, Result, WorkingValue,
    invariant_violation,
};

pub(super) fn new(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    plan: &WorkingValue<OwnedSelectedPlan>,
    mutations: u64,
) -> Result<WorkingValue<logical_v2::RestoreHistory>> {
    let store = io.store();
    let owned = plan.is_owned_by(io);
    let final_stream = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
    let reservation = super::physical::restore_io::directory_scope_reservation(store)?
        .checked_mul(16)
        .and_then(|bytes| bytes.checked_add(super::MAX_BLOCK_BYTES + 2 * 1024 * 1024))
        .ok_or_else(|| invariant_violation("restore history reservation overflow"))?;
    super::decode_with_reservation(io, route, Some(reservation), || {
        if !owned || !final_stream {
            return Err(invariant_violation(
                "restore history owner or phase differs",
            ));
        }
        plan.value().plan.validate_roots(store)?;
        plan.value().plan.validate_shape()?;
        let request = plan.value().plan.logical_request()?;
        request.history(
            super::raw_digest(&plan.value().plan.fields.logical_commit_id)?,
            mutations,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::super::{
        ControlMvpSegmentRow, SEGMENT_RECORD_KV, decode_with_reservation, raw_digest,
    };
    use super::*;
    use crate::state_store::control_mvp::physical::restore_io::{
        FinalMicrochunk, FinalStreamTotals, UnitPayloadAdmission,
    };
    use crate::workspace_io_budget::WorkspaceIoBudget;

    #[tokio::test]
    async fn native_history_retains_key_and_notice_and_charges_each_push() {
        for domain in ["catalog".to_owned(), "d".repeat(32 * 1024)] {
            let (store, plan) =
                super::super::super::tests::inspection_fixture_for_domain(&domain).await;
            let sequence = plan.fields.result_logical_sequence;
            let request = plan.logical_request().unwrap();
            let mut oracle = request
                .history(raw_digest(&plan.fields.logical_commit_id).unwrap(), 1)
                .unwrap();
            oracle.push(b"a", sequence, Some(b"value")).unwrap();
            let expected = oracle.finish().unwrap().0;
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let owned = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
                Ok(OwnedSelectedPlan {
                    plan: plan.clone(),
                    plan_sha256: "sha256:".to_owned() + &"a".repeat(64),
                })
            })
            .unwrap();
            let rows = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                Ok(vec![ControlMvpSegmentRow {
                    record_kind: SEGMENT_RECORD_KV,
                    key: b"a".to_vec(),
                    value: Some(b"value".to_vec()),
                    generation: sequence,
                    tombstone: false,
                    logical_sequence: sequence,
                    logical_ordinal: 0,
                    origin_sequence: None,
                }])
            })
            .unwrap();
            let before = io.live_ownership_evidence().0;
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).unwrap();
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let mut history = new(&mut io, &mut route, &owned, 1).unwrap();
            assert!(io.live_ownership_evidence().0 >= before + super::super::MAX_BLOCK_BYTES);
            let work = io.native_work_evidence();
            history
                .push_restore_history_rows(&mut io, &mut route, &rows)
                .unwrap();
            let after = io.native_work_evidence();
            assert_eq!(after.slots[1] - work.slots[1], 31);
            assert_eq!(after.slots[0], work.slots[0]);
            let digest = history.finish_restore_history(&mut io, &mut route).unwrap();
            assert_eq!(digest.value(), &expected);
            assert_eq!(io.allocation_underestimates(), 0);
            drop(digest);
            drop(history);
            assert_eq!(io.live_ownership_evidence().0, before);
            drop(rows);
            drop(owned);
            assert_eq!(io.live_ownership_evidence(), (0, 0));
            println!(
                "native history domain_bytes={} initial_owned={before}",
                domain.len()
            );
        }
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one matrix checks the closed history owner and phase entry points"
    )]
    async fn native_history_rejects_foreign_owners_phases_and_invalid_continuations() {
        for case in 0..12 {
            let (store, plan) = super::super::super::tests::inspection_fixture().await;
            let sequence = plan.fields.result_logical_sequence;
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut foreign = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut ordinary = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let owned = decode_with_reservation(&mut io, &mut ordinary, Some(1024 * 1024), || {
                Ok(OwnedSelectedPlan {
                    plan: plan.clone(),
                    plan_sha256: "sha256:".to_owned() + &"a".repeat(64),
                })
            })
            .unwrap();
            let make_rows = || {
                Ok(vec![ControlMvpSegmentRow {
                    record_kind: SEGMENT_RECORD_KV,
                    key: b"a".to_vec(),
                    value: Some(b"value".to_vec()),
                    generation: sequence,
                    tombstone: false,
                    logical_sequence: if case == 6 { sequence + 1 } else { sequence },
                    logical_ordinal: 0,
                    origin_sequence: None,
                }])
            };
            let rows = decode_with_reservation(&mut io, &mut ordinary, Some(64 * 1024), make_rows)
                .unwrap();
            let other_rows =
                decode_with_reservation(&mut foreign, &mut ordinary, Some(64 * 1024), make_rows)
                    .unwrap();
            if case == 0 {
                assert!(new(&mut io, &mut ordinary, &owned, 1).is_err());
                assert_eq!(io.allocation_underestimates(), 0);
                continue;
            }
            let mut totals = FinalStreamTotals::new();
            let mut other_totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).unwrap();
            let mut other_chunk =
                FinalMicrochunk::begin(&mut other_totals, 0, &mut foreign).unwrap();
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let mut other_route = RestorePhysicalRoute::FinalMicrochunk(&mut other_chunk);
            if case == 1 {
                assert!(new(&mut foreign, &mut other_route, &owned, 1).is_err());
                assert_eq!(foreign.allocation_underestimates(), 0);
                continue;
            }
            let mut history =
                new(&mut io, &mut route, &owned, if case == 8 { 2 } else { 1 }).unwrap();
            let result = match case {
                2 => history.push_restore_history_rows(&mut io, &mut route, &other_rows),
                3 => history.push_restore_history_rows(&mut io, &mut ordinary, &rows),
                4 => history
                    .finish_restore_history(&mut foreign, &mut other_route)
                    .map(drop),
                5 => history
                    .finish_restore_history(&mut io, &mut ordinary)
                    .map(drop),
                6 => history.push_restore_history_rows(&mut io, &mut route, &rows),
                7 => {
                    history
                        .push_restore_history_rows(&mut io, &mut route, &rows)
                        .unwrap();
                    history.push_restore_history_rows(&mut io, &mut route, &rows)
                }
                8 => {
                    history
                        .push_restore_history_rows(&mut io, &mut route, &rows)
                        .unwrap();
                    history
                        .finish_restore_history(&mut io, &mut route)
                        .map(drop)
                }
                9 => {
                    history
                        .push_restore_history_rows(&mut io, &mut route, &rows)
                        .unwrap();
                    drop(history.finish_restore_history(&mut io, &mut route).unwrap());
                    history
                        .finish_restore_history(&mut io, &mut route)
                        .map(drop)
                }
                10 => {
                    history.push_restore_history_rows(&mut foreign, &mut other_route, &other_rows)
                }
                11 => {
                    history
                        .push_restore_history_rows(&mut io, &mut route, &rows)
                        .unwrap();
                    drop(history.finish_restore_history(&mut io, &mut route).unwrap());
                    let empty =
                        decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                            Ok(Vec::new())
                        })
                        .unwrap();
                    history.push_restore_history_rows(&mut io, &mut route, &empty)
                }
                _ => unreachable!(),
            };
            assert!(result.is_err(), "case {case}");
            if !matches!(case, 4 | 10) {
                assert!(
                    history.finish_restore_history(&mut io, &mut route).is_err(),
                    "stopped case {case}"
                );
            }
            assert_eq!(io.allocation_underestimates(), 0, "case {case}");
            assert_eq!(foreign.allocation_underestimates(), 0, "case {case}");
        }
    }
    #[tokio::test]
    async fn native_history_exact_reservations_reject_before_hashing() {
        for operation in 0..3 {
            for below in [false, true] {
                let (store, plan) = super::super::super::tests::inspection_fixture().await;
                let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut ordinary = RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                };
                let owned =
                    decode_with_reservation(&mut io, &mut ordinary, Some(1024 * 1024), || {
                        Ok(OwnedSelectedPlan {
                            plan: plan.clone(),
                            plan_sha256: "sha256:".to_owned() + &"a".repeat(64),
                        })
                    })
                    .unwrap();
                let rows = decode_with_reservation(&mut io, &mut ordinary, Some(64 * 1024), || {
                    Ok(Vec::new())
                })
                .unwrap();
                let mut totals = FinalStreamTotals::new();
                let mut history = if operation > 0 {
                    let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).unwrap();
                    Some(
                        new(
                            &mut io,
                            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
                            &owned,
                            0,
                        )
                        .unwrap(),
                    )
                } else {
                    None
                };
                let bound = if operation == 0 {
                    super::super::physical::restore_io::directory_scope_reservation(&store).unwrap()
                        * 16
                        + super::super::MAX_BLOCK_BYTES
                        + 2 * 1024 * 1024
                } else {
                    64 * 1024
                };
                let carried = io.live_ownership_evidence().0;
                let mut chunk = FinalMicrochunk::begin(
                    &mut totals,
                    64 * 1024 * 1024 - carried - bound + usize::from(below),
                    &mut io,
                )
                .unwrap();
                let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
                let before = io.native_work_evidence();
                let result = match operation {
                    0 => new(&mut io, &mut route, &owned, 0).map(drop),
                    1 => history
                        .as_mut()
                        .unwrap()
                        .push_restore_history_rows(&mut io, &mut route, &rows),
                    2 => history
                        .as_mut()
                        .unwrap()
                        .finish_restore_history(&mut io, &mut route)
                        .map(drop),
                    _ => unreachable!(),
                };
                assert_eq!(
                    result.is_err(),
                    below,
                    "operation={operation} below={below}"
                );
                if below {
                    assert_eq!(
                        io.native_work_evidence(),
                        before,
                        "refusal precedes hash/codec"
                    );
                }
                assert_eq!(io.allocation_underestimates(), 0);
            }
        }
    }

    #[tokio::test]
    async fn native_history_partitioned_chunks_match_exact_logical_work() {
        let (store, plan) = super::super::super::tests::inspection_fixture().await;
        let sequence = plan.fields.result_logical_sequence;
        let request = plan.logical_request().unwrap();
        let mut oracle = request
            .history(raw_digest(&plan.fields.logical_commit_id).unwrap(), 2)
            .unwrap();
        let capture = super::super::super::super::super::cost::NativeCapture::begin();
        oracle.push(b"a", sequence, Some(b"value")).unwrap();
        oracle.push(b"c", sequence, None).unwrap();
        let expected = oracle.finish().unwrap().0;
        let expected_work = capture.finish();
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut ordinary = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let owned = decode_with_reservation(&mut io, &mut ordinary, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan: plan.clone(),
                plan_sha256: "sha256:".to_owned() + &"a".repeat(64),
            })
        })
        .unwrap();
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).unwrap();
        let mut history = new(
            &mut io,
            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
            &owned,
            2,
        )
        .unwrap();
        let before = io.native_work_evidence();
        for (key, value, generation) in [
            (b'a', Some(b"value".as_slice()), sequence),
            (b'b', Some(b"kept".as_slice()), sequence - 1),
            (b'c', None, sequence),
        ] {
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).unwrap();
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let rows = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                Ok(vec![ControlMvpSegmentRow {
                    record_kind: SEGMENT_RECORD_KV,
                    key: vec![key],
                    value: value.map(<[u8]>::to_vec),
                    generation,
                    tombstone: value.is_none(),
                    logical_sequence: sequence,
                    logical_ordinal: 0,
                    origin_sequence: None,
                }])
            })
            .unwrap();
            history
                .push_restore_history_rows(&mut io, &mut route, &rows)
                .unwrap();
        }
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).unwrap();
        let digest = history
            .finish_restore_history(
                &mut io,
                &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
            )
            .unwrap();
        let actual = io.native_work_evidence();
        assert_eq!(digest.value(), &expected);
        assert_eq!(actual.slots[0] - before.slots[0], expected_work.slots[0]);
        assert_eq!(actual.slots[1] - before.slots[1], expected_work.slots[1]);
        assert_eq!(io.allocation_underestimates(), 0);
        drop(digest);
        drop(history);
        drop(owned);
        assert_eq!(io.live_ownership_evidence(), (0, 0));
    }
}
