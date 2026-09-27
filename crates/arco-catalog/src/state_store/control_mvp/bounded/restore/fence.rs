//! Authenticated external floors. This module never writes the witness or HEAD.
use super::super::super::physical::restore_io::{
    RestoreGateRecord, RestorePhysicalIo, RestorePhysicalRoute, UnitPayloadAdmission,
    decode_with_reservation, hash_with_reservation, head_restore_gate_record,
    read_restore_gate_record,
};
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Floors {
    pub writer: u64,
    pub reclamation: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalFenceRecord {
    record_type: String,
    version: u32,
    scope: StateScope,
    durable_authority_binding: DurableAuthorityBinding,
    writer_epoch: u64,
    reclamation_generation: u64,
}

pub(super) async fn observe_workspace(
    store: &ControlMvpStateStore,
    workspace: &mut crate::workspace_io_budget::WorkspaceIoBudget,
) -> Result<Option<Floors>> {
    let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 128 * 1024 * 1024);
    let mut payload = UnitPayloadAdmission::new();
    observe(
        &mut io,
        &mut RestorePhysicalRoute::OrdinaryUnit {
            workspace,
            payload: &mut payload,
        },
    )
    .await
}

pub(super) async fn observe(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
) -> Result<Option<Floors>> {
    let result = observe_inner(io, route).await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

async fn observe_inner(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
) -> Result<Option<Floors>> {
    let store = io.store();
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        let Some(pin) = &store.absent_restore_fence else {
            return Err(CatalogError::UnsupportedOperation {
                message: "absent restore requires an external fence witness".into(),
            });
        };
        if store.authority_format != 8
            || pin.scope != store.scope
            || pin.durable_authority_binding != store.durable_authority8_binding()?
            || pin.path.len() > 4096
            || !pin
                .path
                .starts_with(&format!("{}/", store.paths.base_prefix()))
            || pin.byte_size == 0
            || pin.byte_size > 4096
            || pin.object_version.is_empty()
            || pin.object_version.len() > 4096
            || pin.object_version.chars().any(char::is_control)
        {
            return Err(invariant_violation("absent restore fence pin is invalid"));
        }
        arco_core::ScopedStorage::validate_path(&pin.path)?;
        raw_digest(&pin.sha256)?;
        Ok(())
    })?);
    let Some(pin) = &store.absent_restore_fence else {
        unreachable!("pin presence was admitted without an intervening await")
    };
    if head_restore_gate_record(io, route, RestoreGateRecord::Head)
        .await?
        .is_some()
    {
        return Ok(None);
    }
    let metadata =
        head_restore_gate_record(io, route, RestoreGateRecord::AbsentFence(&pin.path)).await?;
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        if metadata.is_none() {
            return Err(invariant_violation("absent restore fence proof is missing"));
        }
        Ok(())
    })?);
    if metadata
        .as_ref()
        .is_some_and(|meta| meta.value().version != pin.object_version)
    {
        return Ok(None);
    }
    drop(metadata);
    let found =
        read_restore_gate_record(io, route, RestoreGateRecord::AbsentFence(&pin.path)).await?;
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        if found.is_none() {
            return Err(invariant_violation("absent restore fence proof is missing"));
        }
        Ok(())
    })?);
    let Some((raw, metadata)) = found else {
        unreachable!("presence admitted")
    };
    if metadata.value().version != pin.object_version {
        return Ok(None);
    }
    let digest = hash_with_reservation(io, route, 64 * 1024, raw.as_slice().len(), || {
        Ok(prefixed_sha256(raw.as_slice()))
    })?;
    let floors = decode_with_reservation(io, route, Some(128 * 1024), || {
        if raw.as_slice().len() as u64 != pin.byte_size || *digest.value() != pin.sha256 {
            return Err(invariant_violation(
                "absent restore fence bytes differ from trusted pin",
            ));
        }
        let record: ExternalFenceRecord = decode_json(raw.as_slice(), "absent restore fence")?;
        if record.record_type != "control_mvp_restore_fence"
            || record.version != 1
            || record.scope != pin.scope
            || record.durable_authority_binding != pin.durable_authority_binding
            || record.writer_epoch == u64::MAX
            || jcs(&record)? != raw.as_slice()
        {
            return Err(invariant_violation(
                "absent restore fence record is invalid",
            ));
        }
        super::super::super::validate_publication_epoch(store.writer_epoch, record.writer_epoch)?;
        Ok(Floors {
            writer: record.writer_epoch,
            reclamation: record.reclamation_generation,
        })
    })?;
    if head_restore_gate_record(io, route, RestoreGateRecord::Head)
        .await?
        .is_some()
    {
        return Ok(None);
    }
    Ok(Some(*floors.value()))
}

pub(super) fn verify_source(target: &Target, writer: u64, reclamation: u64) -> Result<()> {
    let Target::Absent {
        observed_writer_epoch,
        observed_reclamation_generation,
        source_parent_writer_epoch,
        source_parent_reclamation_generation,
        ..
    } = target
    else {
        return Err(invariant_violation(
            "absent fence validation requires absent target",
        ));
    };
    if *source_parent_writer_epoch != writer
        || *source_parent_reclamation_generation != reclamation
        || *observed_writer_epoch < writer
        || *observed_reclamation_generation < reclamation
    {
        return Err(invariant_violation(
            "absent restore source-parent fences differ",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn absent_fence_cancellation_stops_every_await_and_releases_owners() {
        use std::{
            future::Future,
            sync::atomic::Ordering,
            task::{Context, Poll, Waker},
        };
        let mut boundaries = 0;
        for at in 0..7 {
            let (store, remaining) =
                super::super::super::super::physical::restore_io::window_pending_store();
            let store =
                store.with_durable_authority_binding(DurableAuthorityBinding::new([39; 32]));
            let (store, source) = super::super::tests::inspection_fixture_on_store(store).await;
            let (store, _) = super::super::tests::absent_fixture_from_source(store, source).await;
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 128 * 1024 * 1024);
            let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            remaining.store(at, Ordering::SeqCst);
            {
                let future = observe(&mut io, &mut route);
                let mut future = std::pin::pin!(future);
                let result = future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
                if at == 0 {
                    assert!(matches!(result, Poll::Ready(Ok(Some(_)))));
                } else {
                    assert!(result.is_pending(), "boundary {at}");
                    boundaries += 1;
                }
            }
            assert_eq!(io.live_ownership_evidence(), (0, 0));
            assert_eq!(io.allocation_underestimates(), 0);
            if at != 0 {
                let reads = io.reading_evidence();
                assert!(observe(&mut io, &mut route).await.is_err());
                assert_eq!(io.reading_evidence(), reads);
            }
            assert_eq!(io.writing_evidence(), (0, 0));
        }
        assert_eq!(boundaries, 6);
    }

    #[tokio::test]
    async fn absent_fence_control_exhaustion_stops_before_io() {
        let (store, _) = super::super::tests::absent_inspection_fixture().await;
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 128 * 1024 * 1024);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        workspace.charge_operations(4096).expect("exhaust control");
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        assert!(observe(&mut io, &mut route).await.is_err());
        assert!(observe(&mut io, &mut route).await.is_err());
        assert_eq!(io.reading_evidence(), (0, 0, 0));
        assert_eq!(io.writing_evidence(), (0, 0));
    }

    #[test]
    fn source_parent_floors_are_authenticated_not_inferred() {
        let target = Target::Absent {
            current_pointer_path: "head".into(),
            absence_marker: "does_not_exist".into(),
            observed_writer_epoch: 7,
            observed_reclamation_generation: 9,
            source_parent_writer_epoch: 4,
            source_parent_reclamation_generation: 5,
        };
        assert!(verify_source(&target, 4, 5).is_ok());
        assert!(verify_source(&target, 3, 5).is_err());
        assert!(verify_source(&target, 4, 6).is_err());
        assert!(verify_source(&target, 8, 10).is_err());
    }
}
