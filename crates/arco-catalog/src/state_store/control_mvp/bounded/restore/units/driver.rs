//! One selected standard unit under workspace coordination. Finalization is separate.
use super::{
    CatalogError, RestorePhysicalIo, RestorePhysicalRoute, Result, admission,
    admitted_expected_plan, bootstrap, decode_with_reservation, own_selected_plan, physical,
    publication, window,
};
use crate::state_store::{
    PersistedRestoreParticipantPlan, RestoreAdvanceContext, RestoreParticipantAdvance,
    RestoreParticipantInspection,
};
use physical::restore_io::UnitPayloadAdmission;

#[allow(
    clippy::large_futures,
    reason = "bounded unit products remain on stack instead of an uncharged heap box"
)]
pub(in super::super) async fn advance(
    store: &super::super::super::super::ControlMvpStateStore,
    persisted: &PersistedRestoreParticipantPlan,
    context: &mut RestoreAdvanceContext<'_>,
) -> Result<RestoreParticipantAdvance> {
    let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 64 * 1024 * 1024);
    let mut payload = UnitPayloadAdmission::new();
    let owned;
    let expected;
    let staged_bootstrap = {
        let mut selection = context.unit_selection(persisted).await?;
        let now = selection.observed_now();
        owned = own_selected_plan(&mut io, &mut selection, &mut payload)?;
        let (_, _, workspace) = selection.parts();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace,
            payload: &mut payload,
        };
        expected = admitted_expected_plan(&mut io, &mut route, &owned)?;
        if admission::observe(&mut io, &mut route, &owned, now).await?
            == admission::Observation::Superseded
        {
            return Ok(RestoreParticipantAdvance::Terminal(
                RestoreParticipantInspection::Superseded,
            ));
        }
        let staged = bootstrap::stage(&mut io, &mut route, &expected).await?;
        if admission::observe(&mut io, &mut route, &owned, now).await?
            == admission::Observation::Superseded
        {
            return Ok(RestoreParticipantAdvance::Terminal(
                RestoreParticipantInspection::Superseded,
            ));
        }
        staged
    };
    let selected;
    let staged = {
        let mut selection = context.unit_selection(persisted).await?;
        let now = selection.observed_now();
        let (_, _, workspace) = selection.parts();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace,
            payload: &mut payload,
        };
        selected = bootstrap::select(&mut io, &mut route, staged_bootstrap).await?;
        if selected.progress.value().terminal {
            return reject(&mut io, &mut route, || CatalogError::UnsupportedOperation {
                message: "terminal restore requires independent streamed final verification".into(),
            });
        }
        let unit = window::prepare(&mut io, &mut route, &owned, &expected, &selected).await?;
        let staged = publication::stage(&mut io, &mut route, unit).await?;
        if admission::observe(&mut io, &mut route, &owned, now).await?
            == admission::Observation::Superseded
        {
            return Ok(RestoreParticipantAdvance::Terminal(
                RestoreParticipantInspection::Superseded,
            ));
        }
        staged
    };
    // Journal-last refencing follows every awaited physical observation and
    // immutable staging operation. The original selector CAS is next.
    let mut selection = context.unit_selection(persisted).await?;
    let (_, _, workspace) = selection.parts();
    let mut route = RestorePhysicalRoute::OrdinaryUnit {
        workspace,
        payload: &mut payload,
    };
    match publication::select(&mut io, &mut route, staged).await? {
        publication::Publication::Written | publication::Publication::ExactSelected => {
            Ok(RestoreParticipantAdvance::InProgress {
                completed_units: selected.progress.value().next_ordinal + 1,
            })
        }
        publication::Publication::Conflict(_) => {
            reject(&mut io, &mut route, || CatalogError::CasFailed {
                message: "restore unit selection changed during publication".into(),
            })
        }
    }
}

fn reject(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    error: impl FnOnce() -> CatalogError,
) -> Result<RestoreParticipantAdvance> {
    decode_with_reservation::<()>(io, route, Some(64 * 1024), || Err(error()))
        .map(|_| unreachable!("native rejection always returns an admitted error"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_io_budget::WorkspaceIoBudget;

    #[tokio::test]
    async fn native_driver_errors_retain_measured_ownership() {
        let (store, _) = super::super::super::tests::inspection_fixture().await;
        for terminal in [false, true] {
            for limit in [64 * 1024 * 1024, 0] {
                let mut io = RestorePhysicalIo::new(&store, limit, 64 * 1024 * 1024);
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut route = RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                };
                let _setup = io.allocation_evidence();
                let invoked = std::cell::Cell::new(false);
                let mut result = None;
                let measured = allocation_counter::measure(|| {
                    result = Some(reject(&mut io, &mut route, || {
                        invoked.set(true);
                        if terminal {
                            CatalogError::UnsupportedOperation { message: "terminal restore requires independent streamed final verification".into() }
                        } else {
                            CatalogError::CasFailed {
                                message: "restore unit selection changed during publication".into(),
                            }
                        }
                    }));
                });
                let error = result.expect("called").expect_err("native failure");
                assert_eq!(
                    io.allocation_evidence().0,
                    measured.bytes_total,
                    "every native error allocation"
                );
                let capacity = match error {
                    CatalogError::UnsupportedOperation { ref message }
                    | CatalogError::CasFailed { ref message }
                    | CatalogError::MaintenanceBackpressure { ref message } => message.capacity(),
                    _ => panic!("unexpected error {error:?}"),
                };
                assert!(io.allocation_evidence().1 >= capacity);
                assert_eq!(invoked.get(), limit != 0);
                assert_eq!(io.reading_evidence(), (0, 0, 0));
                assert_eq!(io.writing_evidence(), (0, 0));
            }
        }
    }
}
