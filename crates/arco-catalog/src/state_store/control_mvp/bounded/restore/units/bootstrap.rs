//! Restartable initial progress selection; no catalog publication authority.
use super::{
    CumulativeSemanticCounts, ExpectedPlan, RestoreControlRecord, RestorePhysicalIo,
    RestorePhysicalRoute, RestoreProgressSelectorV1, RestoreProgressV1, Result, SelectedProgress,
    SingletonState, WorkingValue, codec, decode_with_reservation, invariant_violation, physical,
    read_selected_progress, zero_cursor,
};

#[allow(
    clippy::large_enum_variant,
    reason = "fixed admitted progress stays on stack without an uncharged box"
)]
pub(super) enum StagedBootstrap<'a, 'p> {
    Existing(SelectedProgress),
    Pending(PendingBootstrap<'a, 'p>),
}

pub(super) struct PendingBootstrap<'a, 'p> {
    expected: &'a WorkingValue<ExpectedPlan<'p>>,
    selector: WorkingValue<bytes::Bytes>,
}

#[cfg(test)]
pub(super) async fn select_or_initialize(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    expected: &WorkingValue<ExpectedPlan<'_>>,
) -> Result<SelectedProgress> {
    let staged = stage(io, route, expected).await?;
    select(io, route, staged).await
}

pub(super) async fn stage<'a, 'p>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    expected: &'a WorkingValue<ExpectedPlan<'p>>,
) -> Result<StagedBootstrap<'a, 'p>> {
    let same_owner = expected.is_owned_by(io);
    let result = async {
        let reservation = decode_with_reservation(io, route, Some(64 * 1024), || {
            if !same_owner {
                return Err(invariant_violation("bootstrap plan owner differs"));
            }
            let e = expected.value();
            [
                e.prefix.len(),
                e.plan_sha256.len(),
                e.identity.restore_id().len(),
                e.identity.domain().len(),
            ]
            .into_iter()
            .try_fold(64_usize * 1024, |sum, n| sum.checked_add(n.checked_mul(4)?))
            .ok_or_else(|| invariant_violation("bootstrap reservation overflow"))
        })?;
        let existing = physical::restore_io::read_restore_control_record(
            io,
            route,
            expected.value().candidate_id,
            RestoreControlRecord::Selector,
        )
        .await?;
        if existing.is_some() {
            drop(existing);
            return read_selected_progress(io, route, expected)
                .await
                .map(StagedBootstrap::Existing);
        }
        let model_bytes = *reservation.value();
        drop(reservation);
        let initial = decode_with_reservation(io, route, Some(model_bytes), || {
            let e = expected.value();
            Ok(RestoreProgressV1 {
                record_type: "control_mvp_restore_progress".into(),
                version: 1,
                plan_sha256: e.plan_sha256.into(),
                identity: e.identity.clone(),
                owner_generation: e.owner_generation,
                next_ordinal: 0,
                receipt_count: 0,
                last_receipt: None,
                chain_sha256: String::new(),
                cursor: zero_cursor(),
                singleton_state: SingletonState::None,
                terminal: false,
                cumulative_counts: CumulativeSemanticCounts::default(),
            })
        })?;
        let none = codec::hashes::genesis_none(io, route, &initial)?;
        let chain = codec::hashes::genesis_chain(io, route, &initial, &none)?;
        let progress = decode_with_reservation(io, route, Some(model_bytes), || {
            let mut value = initial.value().clone();
            value.chain_sha256.clone_from(chain.value());
            Ok(value)
        })?;
        drop((initial, none, chain));
        drop(codec::validate_progress(io, route, expected, &progress)?);
        let raw = codec::encode_progress(io, route, &progress)?;
        let digest = codec::hashes::raw_encoded(io, route, &raw)?;
        let selector = decode_with_reservation(io, route, Some(model_bytes), || {
            let e = expected.value();
            Ok(RestoreProgressSelectorV1 {
                record_type: "control_mvp_restore_progress_selector".into(),
                version: 1,
                plan_sha256: e.plan_sha256.into(),
                identity: e.identity.clone(),
                owner_generation: e.owner_generation,
                current_progress_path: e.progress_path(0),
                current_progress_sha256: digest.value().clone(),
            })
        })?;
        drop(codec::validate_selector(io, route, expected, &selector)?);
        let selector_raw = codec::encode_selector(io, route, &selector)?;
        // Immutable deduplication verifies exact bytes. The selector is always
        // conditional: uncertainty never authorizes overwriting selected work.
        drop(
            physical::restore_io::write_restore_control_record(
                io,
                route,
                expected.value().candidate_id,
                RestoreControlRecord::Progress(0),
                &raw,
                None,
            )
            .await?,
        );
        Ok(StagedBootstrap::Pending(PendingBootstrap {
            expected,
            selector: selector_raw,
        }))
    }
    .await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

pub(super) async fn select(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    staged: StagedBootstrap<'_, '_>,
) -> Result<SelectedProgress> {
    let StagedBootstrap::Pending(pending) = staged else {
        let StagedBootstrap::Existing(selected) = staged else {
            unreachable!()
        };
        return Ok(selected);
    };
    let result = async {
        drop(
            physical::restore_io::write_restore_control_record(
                io,
                route,
                pending.expected.value().candidate_id,
                RestoreControlRecord::Selector,
                &pending.selector,
                None,
            )
            .await?,
        );
        read_selected_progress(io, route, pending.expected).await
    }
    .await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::super::{jcs, prefixed_sha256};
    use super::*;
    use crate::workspace_io_budget::WorkspaceIoBudget;
    use physical::restore_io::UnitPayloadAdmission;

    #[tokio::test]
    async fn restore_bootstrap_initializes_once_and_resumes_exact_selection() {
        let (store, plan) = super::super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(&jcs(&plan).expect("fixture plan"));
        for restart in 0..2 {
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
            .expect("expected");
            let selected = select_or_initialize(&mut io, &mut route, &expected)
                .await
                .expect("initial or resumed selection");
            assert_eq!(selected.progress.value().next_ordinal, 0);
            assert_eq!(selected.progress.value().receipt_count, 0);
            assert_eq!(selected.progress.value().cursor, zero_cursor());
            assert!(!selected.progress.value().terminal);
            assert_eq!(io.writing_evidence().0, if restart == 0 { 2 } else { 0 });
            assert_eq!(io.allocation_underestimates(), 0);
            drop((selected, expected));
            assert_eq!(io.live_ownership_evidence(), (0, 0));
        }
    }
    #[tokio::test]
    async fn restore_bootstrap_recovers_before_after_and_cancelled_writes() {
        let (_, plan) = super::super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(&jcs(&plan).expect("fixture plan"));
        for at in [1, 2] {
            for after in [false, true] {
                for pending in [false, true] {
                    let store =
                        physical::restore_io::unit_publication_test_store(at, after, pending);
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
                        .expect("expected");
                    let baseline = io.live_ownership_evidence();
                    if pending {
                        let mut future =
                            Box::pin(select_or_initialize(&mut io, &mut route, &expected));
                        assert!(futures::poll!(&mut future).is_pending());
                        drop(future);
                    } else {
                        assert!(
                            select_or_initialize(&mut io, &mut route, &expected)
                                .await
                                .is_err()
                        );
                    }
                    // Arrived storage errors retain their charged owner until io drops.
                    if pending {
                        assert_eq!(io.live_ownership_evidence(), baseline);
                    }
                    let writes = io.writing_evidence();
                    assert!(
                        select_or_initialize(&mut io, &mut route, &expected)
                            .await
                            .is_err()
                    );
                    assert_eq!(io.writing_evidence(), writes);
                    assert_eq!(io.allocation_underestimates(), 0);
                    drop((expected, route, io));
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
                        .expect("expected");
                    let selected = select_or_initialize(&mut io, &mut route, &expected)
                        .await
                        .expect("fresh restart");
                    assert_eq!(selected.progress.value().next_ordinal, 0);
                    assert_eq!(
                        io.writing_evidence().0,
                        if at == 2 && after { 0 } else { 2 }
                    );
                    assert_eq!(io.allocation_underestimates(), 0);
                }
            }
        }
    }

    #[tokio::test]
    async fn restore_bootstrap_never_replaces_corrupt_selected_or_immutable_progress() {
        let (_, plan) = super::super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(&jcs(&plan).expect("fixture plan"));
        for record in [
            RestoreControlRecord::Selector,
            RestoreControlRecord::Progress(0),
        ] {
            let store = physical::restore_io::unit_publication_test_store(usize::MAX, false, false);
            let e = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
            store
                .retention
                .put_raw(
                    &record.path(&e.prefix),
                    bytes::Bytes::from_static(b"{}"),
                    arco_core::WritePrecondition::DoesNotExist,
                )
                .await
                .expect("corrupt fixture");
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let expected =
                decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || Ok(e))
                    .expect("expected");
            assert!(
                select_or_initialize(&mut io, &mut route, &expected)
                    .await
                    .is_err()
            );
            assert_eq!(
                store
                    .retention
                    .get_raw(&record.path(&expected.value().prefix))
                    .await
                    .expect("retained"),
                bytes::Bytes::from_static(b"{}")
            );
            assert_eq!(io.allocation_underestimates(), 0);
        }
    }

    #[tokio::test]
    async fn restore_bootstrap_cancellation_covers_all_read_and_write_awaits() {
        use std::sync::atomic::Ordering;
        let (_, plan) = super::super::super::tests::inspection_fixture().await;
        let digest = prefixed_sha256(&jcs(&plan).expect("fixture plan"));
        for existing in [false, true] {
            let mut boundaries = 0;
            for at in 0..60 {
                if at > 0 && at > boundaries {
                    break;
                }
                let (store, remaining) = physical::restore_io::window_pending_store();
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
                    .expect("expected");
                if existing {
                    drop(
                        select_or_initialize(&mut io, &mut route, &expected)
                            .await
                            .expect("setup"),
                    );
                }
                let baseline = io.live_ownership_evidence();
                remaining.store(if at == 0 { usize::MAX } else { at }, Ordering::SeqCst);
                if at == 0 {
                    drop(
                        select_or_initialize(&mut io, &mut route, &expected)
                            .await
                            .expect("probe"),
                    );
                    boundaries = usize::MAX - remaining.load(Ordering::SeqCst);
                    assert!((3..60).contains(&boundaries));
                } else {
                    let mut future = Box::pin(select_or_initialize(&mut io, &mut route, &expected));
                    assert!(
                        futures::poll!(&mut future).is_pending(),
                        "existing={existing} at={at}"
                    );
                    drop(future);
                    assert_eq!(io.live_ownership_evidence(), baseline);
                    let work = (io.reading_evidence(), io.writing_evidence());
                    assert!(
                        select_or_initialize(&mut io, &mut route, &expected)
                            .await
                            .is_err()
                    );
                    assert_eq!((io.reading_evidence(), io.writing_evidence()), work);
                }
                assert_eq!(io.allocation_underestimates(), 0);
            }
            println!("bootstrap existing={existing} cancellation boundaries={boundaries}");
        }
    }
}
