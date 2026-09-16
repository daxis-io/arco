//! Frozen physical target/source observations; not publication or retry authority.
use super::{
    CatalogError, OwnedSelectedPlan, RestorePhysicalIo, RestorePhysicalRoute, Result, WorkingValue,
    binary, decode_with_reservation, invariant_violation, physical, raw_digest,
    selected_plan_copy_reservation,
};
use chrono::{DateTime, Utc};

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Observation {
    Unchanged,
    Superseded,
}

use super::super::super::{Manifest8, decode_roots};
use super::super::Target;
use crate::state_store::control_mvp as control;
use physical::restore_io::{RestoreGateRecord, hash_with_reservation, read_restore_gate_record};

pub(super) async fn observe(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    selected: &WorkingValue<OwnedSelectedPlan>,
    now: DateTime<Utc>,
) -> Result<Observation> {
    let result = observe_inner(io, route, selected, now).await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

#[allow(
    clippy::too_many_lines,
    reason = "keep frozen witnesses and their admitted owners visible through observations"
)]
async fn observe_inner(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    selected: &WorkingValue<OwnedSelectedPlan>,
    now: DateTime<Utc>,
) -> Result<Observation> {
    let store = io.store();
    let owned = selected.is_owned_by(io);
    let reservation = decode_with_reservation(io, route, Some(64 * 1024), || {
        if !owned || store.authority_format != 8 {
            return Err(invariant_violation(
                "restore admission owner or format differs",
            ));
        }
        selected_plan_copy_reservation(&selected.value().plan)
            .and_then(|n| n.checked_mul(16)?.checked_add(2 * 1024 * 1024))
            .ok_or_else(|| invariant_violation("restore admission reservation overflow"))
    })?;
    let plan = &selected.value().plan;
    let f = &plan.fields;
    drop(decode_with_reservation(
        io,
        route,
        Some(*reservation.value()),
        || {
            plan.validate_shape()?;
            plan.validate_roots(store)?;
            if now < f.requested_at.datetime()?
                || now >= f.execution_deadline.datetime()?
                || now >= f.source_deadline.datetime()?
            {
                return Err(invariant_violation(
                    "restore admission is outside its frozen window",
                ));
            }
            Ok(())
        },
    )?);
    drop(reservation);
    let prepared =
        read_restore_gate_record(io, route, RestoreGateRecord::Prepared(&f.candidate_id)).await?;
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        if prepared.is_some() {
            return Err(CatalogError::AmbiguousAuthorityOutcome {
                message: "prepared restore requires read-only candidate reconciliation".into(),
            });
        }
        if !matches!(f.target, Target::Present { .. }) {
            return Err(CatalogError::UnsupportedOperation {
                message: "absent restore requires an authenticated target fence floor".into(),
            });
        }
        Ok(())
    })?);
    let Target::Present {
        current_pointer_raw_b64,
        current_pointer_version,
        writer_epoch,
        reclamation_generation,
        manifest: target_manifest,
        ..
    } = &f.target
    else {
        unreachable!("admitted target presence check returned an error");
    };
    let found = read_restore_gate_record(io, route, RestoreGateRecord::Head).await?;
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        if found.is_none() {
            return Err(CatalogError::AmbiguousAuthorityOutcome {
                message: "present restore target HEAD disappeared".into(),
            });
        }
        Ok(())
    })?);
    let Some((head, metadata)) = found else {
        unreachable!("admitted HEAD presence check returned an error");
    };
    let same = decode_with_reservation(io, route, Some(2 * 1024 * 1024), || {
        Ok(metadata.value().version == *current_pointer_version
            && head.as_slice() == binary(current_pointer_raw_b64)?)
    })?;
    if !*same.value() {
        return Ok(Observation::Superseded);
    }
    let pointer = decode_with_reservation(io, route, Some(2 * 1024 * 1024), || {
        let pointer: control::ControlMvpPointer =
            control::decode_json(head.as_slice(), "restore admission HEAD")?;
        pointer.validate_versioned(&store.scope, 8)?;
        Ok(pointer)
    })?;
    let p = pointer.value();
    let (current, size) =
        read_manifest(io, route, &p.manifest_id, &p.manifest_checksum_sha256).await?;
    drop(decode_with_reservation(
        io,
        route,
        Some(2 * 1024 * 1024),
        || {
            let m = current.value();
            if m.logical_sequence != p.logical_sequence
                || p.writer_epoch < m.writer_epoch
                || p.reclamation_generation < m.reclamation_generation
            {
                return Err(invariant_violation(
                    "restore admission HEAD manifest binding differs",
                ));
            }
            Ok(())
        },
    )?);
    drop(decode_with_reservation(
        io,
        route,
        Some(2 * 1024 * 1024),
        || {
            control::validate_publication_epoch(store.writer_epoch, *writer_epoch)?;
            if size != target_manifest.byte_size
                || p.writer_epoch != *writer_epoch
                || p.reclamation_generation != *reclamation_generation
            {
                return Err(invariant_violation(
                    "restore admission target size or fences differ",
                ));
            }
            verify_roots(
                store,
                current.value(),
                f.base_logical_sequence,
                &f.base_history_sha256,
                [
                    &f.base_kv_root_b64,
                    &f.base_active_id_root_b64,
                    &f.base_delivery_order_root_b64,
                ],
            )
        },
    )?);
    drop((same, current, pointer, head, metadata));
    let (source, size) = read_manifest(
        io,
        route,
        f.source.manifest_id(),
        raw_digest(&f.source_manifest.sha256)?,
    )
    .await?;
    drop(decode_with_reservation(
        io,
        route,
        Some(2 * 1024 * 1024),
        || {
            if size != f.source_manifest.byte_size {
                return Err(invariant_violation("restore admission source size differs"));
            }
            verify_roots(
                store,
                source.value(),
                f.source_logical_sequence,
                &f.source_history_sha256,
                [
                    &f.source_kv_root_b64,
                    &f.source_active_id_root_b64,
                    &f.source_delivery_order_root_b64,
                ],
            )
        },
    )?);
    Ok(Observation::Unchanged)
}

async fn read_manifest(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    id: &str,
    digest: &str,
) -> Result<(WorkingValue<Manifest8>, u64)> {
    let store = io.store();
    let found = read_restore_gate_record(io, route, RestoreGateRecord::Manifest(id)).await?;
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        if found.is_none() {
            return Err(invariant_violation("restore admission manifest is missing"));
        }
        Ok(())
    })?);
    let Some((raw, _metadata)) = found else {
        unreachable!("admitted manifest presence check returned an error");
    };
    let hash = hash_with_reservation(io, route, 64 * 1024, raw.as_slice().len(), || {
        Ok(control::sha256_hex(raw.as_slice()))
    })?;
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        if hash.value() != digest {
            return Err(invariant_violation(
                "restore admission manifest digest differs",
            ));
        }
        Ok(())
    })?);
    let reservation = raw
        .as_slice()
        .len()
        .checked_mul(16)
        .and_then(|n| n.checked_add(2 * 1024 * 1024));
    let value = decode_with_reservation(io, route, reservation, || {
        let manifest: Manifest8 =
            control::decode_json(raw.as_slice(), "restore admission manifest")?;
        manifest.validate(&store.scope, id)?;
        Ok(manifest)
    })?;
    Ok((value, raw.as_slice().len() as u64))
}

fn verify_roots(
    store: &control::ControlMvpStateStore,
    manifest: &Manifest8,
    sequence: u64,
    history: &str,
    roots: [&str; 3],
) -> Result<()> {
    let directory = control::directory::Directory::new(store.retention.clone(), &store.scope)?;
    let decoded = decode_roots(&directory, manifest)?;
    if manifest.logical_sequence != sequence
        || manifest.logical_history != raw_digest(history)?
        || decoded.0.encode() != binary(roots[0])?
        || decoded.1.encode() != binary(roots[1])?
        || decoded.2.encode() != binary(roots[2])?
    {
        return Err(invariant_violation(
            "restore admission manifest roots or history differ",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{jcs, prefixed_sha256};
    use super::*;
    use crate::workspace_io_budget::WorkspaceIoBudget;
    use physical::restore_io::UnitPayloadAdmission;

    #[tokio::test]
    async fn restore_admission_stable_present_plan_is_unchanged() {
        let (store, plan) = super::super::super::tests::inspection_fixture().await;
        let now = plan.fields.requested_at.datetime().expect("time");
        let digest = prefixed_sha256(&jcs(&plan).expect("fixture plan"));
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let selected = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan,
                plan_sha256: digest,
            })
        })
        .expect("selected");
        assert_eq!(
            observe(&mut io, &mut route, &selected, now)
                .await
                .expect("admission"),
            Observation::Unchanged
        );
        assert_eq!(io.writing_evidence().0, 0);
        assert_eq!(io.allocation_underestimates(), 0);
        drop(selected);
        assert_eq!(io.live_ownership_evidence(), (0, 0));
    }
    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one fixture matrix retains exact corruption and no-write assertions"
    )]
    async fn restore_admission_rejects_expiry_prepared_corruption_and_changed_targets() {
        use crate::state_store::{ArcoStateTxn, TxnOptions};
        for case in 0..10 {
            let (mut store, mut plan) = super::super::super::tests::inspection_fixture().await;
            let mut now = plan.fields.requested_at.datetime().expect("time");
            match case {
                0 => now = plan.fields.execution_deadline.datetime().expect("deadline"),
                1 => store = store.with_writer_epoch(1).expect("unclaimed"),
                2 => {
                    store
                        .clone()
                        .claim_writer_authority()
                        .await
                        .expect("new fence");
                }
                3 | 8 => {
                    let mut txn = store
                        .begin_control_txn(TxnOptions::default())
                        .await
                        .expect("txn");
                    txn.set_logical_operation("admission-winner", "test", &"bc".repeat(32))
                        .expect("operation");
                    txn.put(b"other", bytes::Bytes::from_static(b"new"))
                        .await
                        .expect("put");
                    txn.commit_v2().await.expect("new target");
                    if case == 8 {
                        let pointer = store.load_pointer().await.expect("successor pointer");
                        store
                            .retention
                            .delete(&store.paths.manifest_object(&pointer.manifest_id))
                            .await
                            .expect("remove successor manifest");
                    }
                }
                4 => {
                    store
                        .retention
                        .put_raw(
                            &plan.fields.source_manifest.path,
                            bytes::Bytes::from_static(b"{}"),
                            arco_core::WritePrecondition::None,
                        )
                        .await
                        .expect("corrupt manifest");
                }
                5 => {
                    let path = format!(
                        "{}/restore/v7/{}/prepared.json",
                        store.paths.base_prefix(),
                        plan.fields.candidate_id
                    );
                    store
                        .retention
                        .put_raw(
                            &path,
                            bytes::Bytes::from_static(b"{}"),
                            arco_core::WritePrecondition::DoesNotExist,
                        )
                        .await
                        .expect("prepared boundary");
                }
                6 => {
                    use base64::Engine as _;
                    let d =
                        control::directory::Directory::new(store.retention.clone(), &store.scope)
                            .expect("directory");
                    plan.fields.base_kv_root_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
                        .encode(d.empty_root().await.expect("empty root").encode());
                    plan.fields.candidate_seed_sha256 = plan.candidate_seed().expect("seed");
                    plan.fields.candidate_id = raw_digest(&plan.fields.candidate_seed_sha256)
                        .expect("raw")
                        .into();
                    plan.validate_shape().expect("self-consistent forged plan");
                }
                9 => store
                    .retention
                    .delete(&plan.fields.source_manifest.path)
                    .await
                    .expect("missing manifest"),
                _ => store
                    .retention
                    .delete(&store.paths.current_pointer())
                    .await
                    .expect("missing HEAD"),
            }
            let digest = prefixed_sha256(&jcs(&plan).expect("fixture plan"));
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let selected = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
                Ok(OwnedSelectedPlan {
                    plan,
                    plan_sha256: digest,
                })
            })
            .expect("selected");
            let result = observe(&mut io, &mut route, &selected, now).await;
            if case == 2 || case == 3 || case == 8 {
                assert_eq!(result.expect("changed target"), Observation::Superseded);
                if case == 8 {
                    let (ranges, heads, _) = io.reading_evidence();
                    assert_eq!(
                        (ranges, heads),
                        (1, 3),
                        "only prepared probe and pinned HEAD"
                    );
                }
            } else {
                assert!(result.is_err(), "case {case}");
                if case == 5 || case == 7 {
                    assert!(matches!(
                        result,
                        Err(CatalogError::AmbiguousAuthorityOutcome { .. })
                    ));
                }
                if case == 5 || case == 7 || case == 9 {
                    let error = result.as_ref().expect_err("native failure");
                    let capacity = match error {
                        CatalogError::AmbiguousAuthorityOutcome { message }
                        | CatalogError::InvariantViolation { message } => message.capacity(),
                        _ => panic!("unexpected error {error:?}"),
                    };
                    drop(selected);
                    assert!(
                        io.allocation_evidence().1 >= capacity,
                        "case {case}: returned error retains its admitted owner"
                    );
                    assert_eq!(io.allocation_underestimates(), 0);
                    continue;
                }
                let reads = io.reading_evidence();
                assert!(observe(&mut io, &mut route, &selected, now).await.is_err());
                assert_eq!(io.reading_evidence(), reads);
            }
            if case == 0 {
                assert_eq!(io.reading_evidence(), (0, 0, 0));
            }
            assert_eq!(io.writing_evidence().0, 0);
            assert_eq!(io.allocation_underestimates(), 0);
        }
    }
}
