//! One selected standard unit under workspace coordination. Finalization is separate.
use super::super::certificate;
use super::super::directory;
use super::{
    CatalogError, ControlMvpSegmentRow, ExpectedPlan, OwnedSelectedPlan, RestorePhysicalIo,
    RestorePhysicalRoute, Result, SelectedProgress, admission, admitted_expected_plan, binary,
    bootstrap, codec, coverage, decode_with_reservation, invariant_violation, own_selected_plan,
    physical, publication, window,
};
use crate::state_store::{
    PersistedRestoreParticipantPlan, RestoreAdvanceContext, RestoreParticipantAdvance,
    RestoreParticipantInspection,
};
use base64::Engine as _;
use physical::restore_io::{
    FinalMicrochunk, FinalStreamTotals, UnitPayloadAdmission, WorkingValue,
};

#[allow(
    clippy::large_futures,
    reason = "bounded unit products remain on stack instead of an uncharged heap box"
)]
pub(in super::super) async fn advance(
    store: &super::super::super::super::ControlMvpStateStore,
    persisted: &PersistedRestoreParticipantPlan,
    context: &mut RestoreAdvanceContext<'_>,
) -> Result<RestoreParticipantAdvance> {
    let mut io = RestorePhysicalIo::new(store, 64 * 1024 * 1024, 128 * 1024 * 1024);
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
            None
        } else {
            let unit = window::prepare(&mut io, &mut route, &owned, &expected, &selected).await?;
            let staged = publication::stage(&mut io, &mut route, unit).await?;
            if admission::observe(&mut io, &mut route, &owned, now).await?
                == admission::Observation::Superseded
            {
                return Ok(RestoreParticipantAdvance::Terminal(
                    RestoreParticipantInspection::Superseded,
                ));
            }
            Some(staged)
        }
    };
    let Some(staged) = staged else {
        return verify_terminal(
            &mut io,
            &mut payload,
            &owned,
            &expected,
            &selected,
            context,
            persisted,
        )
        .await;
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

#[allow(
    clippy::large_futures,
    reason = "one bounded final receipt remains on stack through verification"
)]
async fn verify_terminal(
    io: &mut RestorePhysicalIo<'_>,
    payload: &mut UnitPayloadAdmission,
    owned: &WorkingValue<OwnedSelectedPlan>,
    expected: &WorkingValue<ExpectedPlan<'_>>,
    selected: &SelectedProgress,
    context: &mut RestoreAdvanceContext<'_>,
    persisted: &PersistedRestoreParticipantPlan,
) -> Result<RestoreParticipantAdvance> {
    let mut totals = FinalStreamTotals::with_deadline(context.final_deadline()?);
    #[cfg(feature = "test-utils")]
    {
        totals = totals.with_clock(context.final_clock());
    }
    let verification = async {
        let mut stream = {
            let mut selection = context.unit_selection(persisted).await?;
            let (_, _, workspace) = selection.parts();
            let mut route = RestorePhysicalRoute::OrdinaryUnit { workspace, payload };
            coverage::StandardStream::new(io, &mut route, &mut totals, owned, expected, selected)?
        };
        for ordinal in 0..selected.progress.value().receipt_count {
            if ordinal != 0 && ordinal % 64 == 0 {
                context.refence().await?;
            }
            stream.next(io).await?;
        }
        context.refence().await?;
        let assembly = stream.finish(io).await?;
        context.refence().await?;
        Ok::<_, CatalogError>(assembly)
    }
    .await;
    if verification.is_err() {
        io.stop_final(&mut totals);
    }
    let assembly = verification?;
    let private_candidate = async {
        let progress = selected.progress.value();
        let notice = assemble_notice(
            io,
            &mut totals,
            owned,
            assembly.root(),
            progress.next_ordinal,
        )
        .await?;
        context.refence().await?;
        let certificate = {
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, io)?;
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let certificate = new_restore_certificate(
                io,
                &mut route,
                owned,
                certificate::Terminal {
                    next_ordinal: progress.next_ordinal,
                    receipt_count: progress.receipt_count,
                    chain_sha256: progress.chain_sha256.clone(),
                },
                assembly.root(),
                assembly.digest(),
                progress.cumulative_counts.mutations,
                &notice,
            )?;
            let _ = chunk.finish();
            certificate
        };
        Ok::<_, CatalogError>((notice, certificate))
    }
    .await;
    if private_candidate.is_err() {
        io.stop_final(&mut totals);
    }
    let (notice, certificate) = private_candidate?;
    drop(certificate);
    drop(notice);
    drop(assembly);
    context.refence().await?;
    let mut selection = context.unit_selection(persisted).await?;
    let (_, _, workspace) = selection.parts();
    let mut route = RestorePhysicalRoute::OrdinaryUnit { workspace, payload };
    reject(io, &mut route, || CatalogError::UnsupportedOperation {
        message: "terminal restore candidate publication is not implemented".into(),
    })
}

struct NoticePaths {
    roots: WorkingValue<(directory::Root, directory::Root, Vec<u8>, Vec<u8>)>,
    active: WorkingValue<Option<directory::restore::Position>>,
    delivery: WorkingValue<Option<directory::restore::Position>>,
}

async fn select_notice_paths(
    io: &mut RestorePhysicalIo<'_>,
    totals: &mut FinalStreamTotals,
    owned: &WorkingValue<OwnedSelectedPlan>,
) -> Result<NoticePaths> {
    let store = io.store();
    let mut chunk = FinalMicrochunk::begin(totals, 0, io)?;
    let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
    let owned_by_io = owned.is_owned_by(io);
    let selected = decode_with_reservation(io, &mut route, Some(2 * 1024 * 1024), || {
        if !owned_by_io {
            return Err(CatalogError::MaintenanceBackpressure {
                message: "notice selection plan owner differs".into(),
            });
        }
        let fields = &owned.value().plan.fields;
        let directory = directory::Directory::new(store.retention.clone(), &store.scope)?;
        let active = directory.decode_root(&binary(&fields.base_active_id_root_b64)?)?;
        let delivery = directory.decode_root(&binary(&fields.base_delivery_order_root_b64)?)?;
        let active_key = fields.restore_notice_intent_id.as_bytes().to_vec();
        let delivery_key = super::super::super::delivery_key(
            fields.result_logical_sequence,
            0,
            &fields.restore_notice_intent_id,
        )?;
        Ok((active, delivery, active_key, delivery_key))
    })?;
    let _ = chunk.finish();
    let active = {
        let mut chunk = FinalMicrochunk::begin(totals, 0, io)?;
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        let position = directory::restore::floor_at_or_before(
            io,
            &mut route,
            &selected.value().0,
            &selected.value().2,
        )
        .await?;
        let _ = chunk.finish();
        position
    };
    let delivery = {
        let mut chunk = FinalMicrochunk::begin(totals, 0, io)?;
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        let position = directory::restore::floor_at_or_before(
            io,
            &mut route,
            &selected.value().1,
            &selected.value().3,
        )
        .await?;
        let _ = chunk.finish();
        position
    };
    Ok(NoticePaths {
        roots: selected,
        active,
        delivery,
    })
}

fn new_restore_notice_rows(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    owned: &WorkingValue<OwnedSelectedPlan>,
    source: &WorkingValue<super::super::super::ArtifactRef>,
) -> Result<WorkingValue<(ControlMvpSegmentRow, ControlMvpSegmentRow)>> {
    let same_owner = owned.is_owned_by(io) && source.is_owned_by(io);
    decode_with_reservation(io, route, Some(4 * 1024 * 1024), || {
        if !same_owner {
            return Err(invariant_violation(
                "restore notice plan or source owner differs",
            ));
        }
        let fields = &owned.value().plan.fields;
        let commit = super::raw_digest(&fields.logical_commit_id)?;
        let notice = owned.value().plan.logical_request()?.notice(commit)?;
        if notice.intent_id() != fields.restore_notice_intent_id
            || notice.payload() != binary(&fields.restore_notice_payload_b64)?
        {
            return Err(invariant_violation(
                "restore notice differs from the selected plan",
            ));
        }
        let (active, delivery) = super::super::super::outbox_mutations(
            &[notice],
            &[],
            fields.result_logical_sequence,
            Some(source.value()),
        )?;
        let active = active
            .into_values()
            .next()
            .flatten()
            .ok_or_else(|| invariant_violation("restore active notice row is absent"))?;
        let delivery = delivery
            .into_values()
            .next()
            .flatten()
            .ok_or_else(|| invariant_violation("restore delivery notice row is absent"))?;
        Ok((active, delivery))
    })
}

struct NoticeAssembly {
    source: WorkingValue<super::super::super::ArtifactRef>,
    active: WorkingValue<physical::restore_io::NoticeRoleEdit>,
    delivery: WorkingValue<physical::restore_io::NoticeRoleEdit>,
    proofs: WorkingValue<[super::super::super::RoleTransition8; 2]>,
}

#[allow(
    clippy::too_many_arguments,
    reason = "the final verifier supplies the terminal, root and history witnesses"
)]
fn new_restore_certificate(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    owned: &WorkingValue<OwnedSelectedPlan>,
    terminal: certificate::Terminal,
    kv_root: &directory::Root,
    history: &str,
    mutation_count: u64,
    notice: &NoticeAssembly,
) -> Result<WorkingValue<certificate::RestoreCertificateV1>> {
    let same_owner = owned.is_owned_by(io)
        && notice.source.is_owned_by(io)
        && notice.active.is_owned_by(io)
        && notice.delivery.is_owned_by(io)
        && notice.proofs.is_owned_by(io);
    let final_route = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
    let store = io.store();
    decode_with_reservation(io, route, Some(8 * 1024 * 1024), || {
        if !same_owner || !final_route {
            return Err(invariant_violation(
                "restore certificate owner or phase differs",
            ));
        }
        let directory = directory::Directory::new(store.retention.clone(), &store.scope)?;
        directory.decode_root(&kv_root.encode())?;
        certificate::RestoreCertificateV1::from_final(
            &owned.value().plan,
            &owned.value().plan_sha256,
            terminal,
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(kv_root.encode()),
            history,
            mutation_count,
            notice.source.value(),
            notice.proofs.value(),
        )
    })
}

fn notice_transition_proofs(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    owned: &WorkingValue<OwnedSelectedPlan>,
    source: &WorkingValue<super::super::super::ArtifactRef>,
    active: &WorkingValue<physical::restore_io::NoticeRoleEdit>,
    delivery: &WorkingValue<physical::restore_io::NoticeRoleEdit>,
) -> Result<WorkingValue<[super::super::super::RoleTransition8; 2]>> {
    let same_owner = owned.is_owned_by(io)
        && source.is_owned_by(io)
        && active.is_owned_by(io)
        && delivery.is_owned_by(io);
    let final_route = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
    let store = io.store();
    decode_with_reservation(io, route, Some(2 * 1024 * 1024), || {
        if !same_owner || !final_route {
            return Err(invariant_violation(
                "restore notice proof owner or phase differs",
            ));
        }
        let fields = &owned.value().plan.fields;
        let directory = directory::Directory::new(store.retention.clone(), &store.scope)?;
        let old_active = directory.decode_root(&binary(&fields.base_active_id_root_b64)?)?;
        let old_delivery = directory.decode_root(&binary(&fields.base_delivery_order_root_b64)?)?;
        let proof = |role, old: &directory::Root, edit: &physical::restore_io::NoticeRoleEdit| {
            super::super::super::role_transition(
                role,
                old,
                &edit.root,
                vec![super::super::super::EditProof8 {
                    old: edit.old.as_ref().map(super::super::super::leaf_proof),
                    new: edit
                        .new
                        .iter()
                        .map(super::super::super::leaf_proof)
                        .collect(),
                }],
            )
        };
        Ok([
            proof(physical::Role::ActiveId, &old_active, active.value()),
            proof(
                physical::Role::DeliveryOrder,
                &old_delivery,
                delivery.value(),
            ),
        ])
    })
}

#[allow(
    clippy::too_many_lines,
    reason = "one final owner derives four frozen IDs and retains both role edits"
)]
async fn assemble_notice(
    io: &mut RestorePhysicalIo<'_>,
    totals: &mut FinalStreamTotals,
    owned: &WorkingValue<OwnedSelectedPlan>,
    kv_root: &directory::Root,
    ordinal: u64,
) -> Result<NoticeAssembly> {
    let result = async {
        let output_ids = {
            let mut chunk = FinalMicrochunk::begin(totals, 0, io)?;
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let expected = admitted_expected_plan(io, &mut route, owned)?;
            let active = [
                codec::hashes::output_id(
                    io,
                    &mut route,
                    &expected,
                    ordinal,
                    0,
                    physical::Role::ActiveId,
                )?,
                codec::hashes::output_id(
                    io,
                    &mut route,
                    &expected,
                    ordinal,
                    1,
                    physical::Role::ActiveId,
                )?,
            ];
            let delivery = [
                codec::hashes::output_id(
                    io,
                    &mut route,
                    &expected,
                    ordinal,
                    0,
                    physical::Role::DeliveryOrder,
                )?,
                codec::hashes::output_id(
                    io,
                    &mut route,
                    &expected,
                    ordinal,
                    1,
                    physical::Role::DeliveryOrder,
                )?,
            ];
            drop(expected);
            let _ = chunk.finish();
            [active, delivery]
        };
        let paths = select_notice_paths(io, totals, owned).await?;
        let source = {
            let mut chunk = FinalMicrochunk::begin(totals, 0, io)?;
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let fields = &owned.value().plan.fields;
            let source = super::super::super::write_restore_projection_source(
                io,
                &mut route,
                fields.result_logical_sequence,
                super::raw_digest(&fields.logical_commit_id)?,
                kv_root,
            )
            .await?;
            let _ = chunk.finish();
            source
        };
        let (active_row, delivery_row) = {
            let mut chunk = FinalMicrochunk::begin(totals, 0, io)?;
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let rows = new_restore_notice_rows(io, &mut route, owned, &source)?;
            let active = decode_with_reservation(io, &mut route, Some(2 * 1024 * 1024), || {
                Ok(rows.value().0.clone())
            })?;
            let delivery = decode_with_reservation(io, &mut route, Some(2 * 1024 * 1024), || {
                Ok(rows.value().1.clone())
            })?;
            drop(rows);
            let _ = chunk.finish();
            (active, delivery)
        };
        let [
            [active_first, active_second],
            [delivery_first, delivery_second],
        ] = output_ids;
        let active = physical::restore_io::splice_restore_notice_role(
            io,
            totals,
            physical::Role::ActiveId,
            &paths.roots.value().0,
            &paths.active,
            &active_row,
            [active_first.value(), active_second.value()],
        )
        .await?;
        let delivery = physical::restore_io::splice_restore_notice_role(
            io,
            totals,
            physical::Role::DeliveryOrder,
            &paths.roots.value().1,
            &paths.delivery,
            &delivery_row,
            [delivery_first.value(), delivery_second.value()],
        )
        .await?;
        let proofs = {
            let mut chunk = FinalMicrochunk::begin(totals, 0, io)?;
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let proofs =
                notice_transition_proofs(io, &mut route, owned, &source, &active, &delivery)?;
            let _ = chunk.finish();
            proofs
        };
        Ok(NoticeAssembly {
            source,
            active,
            delivery,
            proofs,
        })
    }
    .await;
    if result.is_err() {
        io.stop_final(totals);
    }
    result
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
    async fn staged_restore_manifest_cannot_mint_bounded_token() {
        let (store, plan) = super::super::super::tests::inspection_fixture().await;
        let candidate = &plan.fields.candidate_id;
        let source = store
            .get_json(
                &plan.fields.source_manifest.path,
                super::super::super::MAX_CONTROL_JSON_BYTES,
            )
            .await
            .expect("authenticated fixture manifest");
        let mut staged: serde_json::Value =
            serde_json::from_slice(&source).expect("source manifest JSON");
        staged["manifest_id"] = serde_json::Value::String(candidate.clone());
        staged["kind"] = serde_json::Value::String("restore".into());
        staged["logical_sequence"] =
            serde_json::Value::Number(plan.fields.result_logical_sequence.into());
        let bytes = serde_jcs::to_vec(&staged).expect("staged restore manifest bytes");
        let digest = super::super::super::super::sha256_hex(&bytes);
        let write = store
            .storage
            .put(
                &store.paths.manifest_object(candidate),
                bytes::Bytes::from(bytes),
                arco_core::AuthorityWritePrecondition::DoesNotExist,
            )
            .await
            .expect("private staged manifest");
        assert!(matches!(write, arco_core::WriteResult::Success { .. }));
        let token = store
            .token(candidate.clone(), plan.fields.result_logical_sequence)
            .with_manifest_witness(digest);
        assert!(matches!(
            store.read_bounded_token(&token).await,
            Err(CatalogError::Serialization { message }) if message.contains("unknown variant `restore`")
        ));
    }

    #[tokio::test]
    async fn restore_notice_rows_match_ordinary_paired_rows() {
        let (store, plan) = super::super::super::tests::inspection_fixture().await;
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("notice chunk");
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        let owned = decode_with_reservation(&mut io, &mut route, Some(4 * 1024 * 1024), || {
            Ok(OwnedSelectedPlan {
                plan,
                plan_sha256: "ac".repeat(32),
            })
        })
        .expect("owned plan");
        let source = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
            Ok(super::super::super::super::ArtifactRef {
                path: "unused".into(),
                sha256: "bb".repeat(32),
            })
        })
        .expect("owned source");
        let rows = new_restore_notice_rows(&mut io, &mut route, &owned, &source)
            .expect("metered paired rows");
        let fields = &owned.value().plan.fields;
        let commit = super::super::raw_digest(&fields.logical_commit_id).expect("commit");
        let notice = owned
            .value()
            .plan
            .logical_request()
            .expect("request")
            .notice(commit)
            .expect("notice");
        let (active, delivery) = super::super::super::super::outbox_mutations(
            &[notice],
            &[],
            fields.result_logical_sequence,
            Some(source.value()),
        )
        .expect("ordinary rows");
        assert_eq!(
            rows.value().0,
            active
                .into_values()
                .next()
                .expect("active")
                .expect("addition")
        );
        assert_eq!(
            rows.value().1,
            delivery
                .into_values()
                .next()
                .expect("delivery")
                .expect("addition")
        );
        drop(rows);
        drop(source);
        drop(owned);
        let _ = chunk.finish();
        assert_eq!(io.live_ownership_evidence(), (0, 0));
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one assembled notice is checked against both independent directory proofs"
    )]
    async fn restore_notice_assembly_links_source_and_both_roles() {
        use super::super::super::certificate;

        let (store, plan) = super::super::super::tests::inspection_fixture().await;
        let directory =
            directory::Directory::new(store.retention.clone(), &store.scope).expect("directory");
        let kv_root = directory
            .decode_root(&binary(&plan.fields.base_kv_root_b64).expect("base root bytes"))
            .expect("base KV root");
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut totals = FinalStreamTotals::new();
        let owned = {
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("plan chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let owned = decode_with_reservation(&mut io, &mut route, Some(4 * 1024 * 1024), || {
                Ok(OwnedSelectedPlan {
                    plan,
                    plan_sha256: format!("sha256:{}", "ac".repeat(32)),
                })
            })
            .expect("owned plan");
            let _ = chunk.finish();
            owned
        };
        let assembly = assemble_notice(&mut io, &mut totals, &owned, &kv_root, 3)
            .await
            .expect("paired notice assembly");
        for (role, root_b64, edit, proof) in [
            (
                physical::Role::ActiveId,
                &owned.value().plan.fields.base_active_id_root_b64,
                &assembly.active,
                &assembly.proofs.value()[0],
            ),
            (
                physical::Role::DeliveryOrder,
                &owned.value().plan.fields.base_delivery_order_root_b64,
                &assembly.delivery,
                &assembly.proofs.value()[1],
            ),
        ] {
            let old_root = directory
                .decode_root(&binary(root_b64).expect("inherited root bytes"))
                .expect("inherited root");
            assert_eq!(proof.role, role);
            assert_eq!(proof.old_root_hex, hex::encode(old_root.encode()));
            assert_eq!(proof.new_root_hex, hex::encode(edit.value().root.encode()));
            assert_eq!(proof.edits.len(), 1);
            assert_eq!(proof.edits[0].new.len(), edit.value().new.len());
            let proof_edit = directory::update::Edit {
                old: proof.edits[0]
                    .old
                    .as_ref()
                    .map(super::super::super::super::leaf_from_proof)
                    .transpose()
                    .expect("old leaf proof"),
                new: proof.edits[0]
                    .new
                    .iter()
                    .map(super::super::super::super::leaf_from_proof)
                    .collect::<Result<Vec<_>>>()
                    .expect("new leaf proofs"),
            };
            directory
                .verify_update(
                    &old_root,
                    &edit.value().root,
                    &[proof_edit],
                    &mut directory::ReadBudget::default(),
                )
                .await
                .expect("independent directory proof oracle");
        }
        let certificate = {
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("cert chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let certificate = new_restore_certificate(
                &mut io,
                &mut route,
                &owned,
                certificate::Terminal {
                    next_ordinal: 3,
                    receipt_count: 3,
                    chain_sha256: format!("sha256:{}", "aa".repeat(32)),
                },
                &kv_root,
                &"bb".repeat(32),
                0,
                &assembly,
            )
            .expect("private restore certificate");
            let _ = chunk.finish();
            certificate
        };
        assert_eq!(certificate.value().active, assembly.proofs.value()[0]);
        assert_eq!(certificate.value().delivery, assembly.proofs.value()[1]);
        let bytes = serde_jcs::to_vec(certificate.value()).expect("certificate bytes");
        let certificate_path = format!(
            "{}/restore/v7/{}/certificate.json",
            store.paths.base_prefix(),
            owned.value().plan.fields.candidate_id
        );
        let certificate_ref = super::super::super::super::ArtifactRef {
            path: certificate_path.clone(),
            sha256: super::super::super::super::sha256_hex(&bytes),
        };
        let write = store
            .storage
            .put(
                &certificate_path,
                bytes::Bytes::copy_from_slice(&bytes),
                arco_core::AuthorityWritePrecondition::DoesNotExist,
            )
            .await
            .expect("private certificate write");
        let mut version = match write {
            arco_core::WriteResult::Success { version } => version,
            arco_core::WriteResult::PreconditionFailed { .. } => {
                panic!("private certificate path already exists")
            }
        };
        let read = certificate::RestoreCertificateV1::read_bounded(
            &store,
            &mut super::super::super::super::retained::ReadBudget::new(),
            &owned.value().plan,
            &owned.value().plan_sha256,
            &certificate_ref,
        )
        .await
        .expect("budgeted private certificate read");
        assert_eq!(read, *certificate.value());
        let wrong_path = super::super::super::super::ArtifactRef {
            path: format!("{certificate_path}.other"),
            ..certificate_ref.clone()
        };
        assert!(
            certificate::RestoreCertificateV1::read_bounded(
                &store,
                &mut super::super::super::super::retained::ReadBudget::new(),
                &owned.value().plan,
                &owned.value().plan_sha256,
                &wrong_path,
            )
            .await
            .is_err()
        );
        for role in ["active", "delivery"] {
            let mut forged: serde_json::Value =
                serde_json::from_slice(&bytes).expect("certificate JSON");
            forged[role]["edits"][0]["new"][0]["digest_hex"] =
                serde_json::Value::String("ff".repeat(32));
            let forged = serde_jcs::to_vec(&forged).expect("forged proof bytes");
            let forged_write = store
                .storage
                .put(
                    &certificate_path,
                    bytes::Bytes::copy_from_slice(&forged),
                    arco_core::AuthorityWritePrecondition::MatchesVersion(version),
                )
                .await
                .expect("simulate forged certificate proof");
            version = match forged_write {
                arco_core::WriteResult::Success { version } => version,
                arco_core::WriteResult::PreconditionFailed { .. } => {
                    panic!("forged certificate update conflicted")
                }
            };
            let forged_ref = super::super::super::super::ArtifactRef {
                sha256: super::super::super::super::sha256_hex(&forged),
                ..certificate_ref.clone()
            };
            assert!(
                certificate::RestoreCertificateV1::read_bounded(
                    &store,
                    &mut super::super::super::super::retained::ReadBudget::new(),
                    &owned.value().plan,
                    &owned.value().plan_sha256,
                    &forged_ref,
                )
                .await
                .is_err(),
                "a canonical, checksum-matched certificate must reject forged {role} proof"
            );
        }
        let mut forged_source: serde_json::Value =
            serde_json::from_slice(&bytes).expect("certificate JSON");
        let missing_digest = "ee".repeat(32);
        forged_source["projection_source"]["sha256"] =
            serde_json::Value::String(missing_digest.clone());
        forged_source["projection_source"]["path"] = serde_json::Value::String(format!(
            "{}/projection-sources/{missing_digest}.json",
            store.paths.base_prefix()
        ));
        let forged_source = serde_jcs::to_vec(&forged_source).expect("forged source bytes");
        let source_write = store
            .storage
            .put(
                &certificate_path,
                bytes::Bytes::copy_from_slice(&forged_source),
                arco_core::AuthorityWritePrecondition::MatchesVersion(version),
            )
            .await
            .expect("simulate forged projection source");
        version = match source_write {
            arco_core::WriteResult::Success { version } => version,
            arco_core::WriteResult::PreconditionFailed { .. } => {
                panic!("forged source update conflicted")
            }
        };
        let forged_source_ref = super::super::super::super::ArtifactRef {
            sha256: super::super::super::super::sha256_hex(&forged_source),
            ..certificate_ref.clone()
        };
        assert!(
            certificate::RestoreCertificateV1::read_bounded(
                &store,
                &mut super::super::super::super::retained::ReadBudget::new(),
                &owned.value().plan,
                &owned.value().plan_sha256,
                &forged_source_ref,
            )
            .await
            .is_err(),
            "a certificate must authenticate its projection-source object"
        );
        let mut noncanonical = bytes.clone();
        noncanonical.push(b' ');
        let rewritten = store
            .storage
            .put(
                &certificate_path,
                bytes::Bytes::copy_from_slice(&noncanonical),
                arco_core::AuthorityWritePrecondition::MatchesVersion(version),
            )
            .await
            .expect("simulate changed immutable bytes");
        assert!(matches!(rewritten, arco_core::WriteResult::Success { .. }));
        assert!(
            certificate::RestoreCertificateV1::read_bounded(
                &store,
                &mut super::super::super::super::retained::ReadBudget::new(),
                &owned.value().plan,
                &owned.value().plan_sha256,
                &certificate_ref,
            )
            .await
            .is_err(),
            "the original checksum must reject changed bytes"
        );
        let noncanonical_ref = super::super::super::super::ArtifactRef {
            sha256: super::super::super::super::sha256_hex(&noncanonical),
            ..certificate_ref
        };
        assert!(
            certificate::RestoreCertificateV1::read_bounded(
                &store,
                &mut super::super::super::super::retained::ReadBudget::new(),
                &owned.value().plan,
                &owned.value().plan_sha256,
                &noncanonical_ref,
            )
            .await
            .is_err(),
            "a matching checksum must not admit noncanonical bytes"
        );
        let decoded: certificate::RestoreCertificateV1 =
            serde_json::from_slice(&bytes).expect("certificate decode");
        assert_eq!(decoded, *certificate.value());
        decoded
            .validate_plan(&owned.value().plan, &owned.value().plan_sha256)
            .expect("selected plan binding");
        for path in ["target_witness_digest", "active.old_root_hex"] {
            let mut altered: serde_json::Value =
                serde_json::from_slice(&bytes).expect("certificate JSON");
            let field = if path == "target_witness_digest" {
                &mut altered["target_witness_digest"]
            } else {
                &mut altered["active"]["old_root_hex"]
            };
            *field = serde_json::Value::String("ff".repeat(32));
            let altered: certificate::RestoreCertificateV1 =
                serde_json::from_value(altered).expect("altered certificate decode");
            assert!(
                altered
                    .validate_plan(&owned.value().plan, &owned.value().plan_sha256)
                    .is_err(),
                "{path} must remain bound to Plan7"
            );
        }
        let mut extra: serde_json::Value =
            serde_json::from_slice(&bytes).expect("certificate JSON");
        extra["unexpected"] = serde_json::Value::Bool(true);
        assert!(serde_json::from_value::<certificate::RestoreCertificateV1>(extra).is_err());
        let expected = ExpectedPlan::from_selected(&owned.value().plan, &owned.value().plan_sha256)
            .expect("frozen candidate seed");
        for role in [physical::Role::ActiveId, physical::Role::DeliveryOrder] {
            let id = super::super::output_id(&expected, 3, 0, role).expect("frozen output ID");
            assert!(
                store
                    .storage
                    .head(&store.paths.state_object(&id))
                    .await
                    .expect("output HEAD")
                    .is_some(),
                "notice output must use the frozen candidate seed"
            );
        }
        let source = super::super::super::super::load_projection_source(
            &store,
            &assembly.source.value().sha256,
        )
        .await
        .expect("readable projection source");
        assert_eq!(source.kv_root_hex, hex::encode(kv_root.encode()));
        assert_eq!(
            source.logical_sequence,
            owned.value().plan.fields.result_logical_sequence
        );
        for (role, edit) in [
            (physical::Role::ActiveId, &assembly.active),
            (physical::Role::DeliveryOrder, &assembly.delivery),
        ] {
            assert_eq!(edit.value().new.len(), 1);
            let rows = store
                .resolve_physical_block(role, edit.value().new.first().expect("notice leaf"))
                .await
                .expect("readable notice output");
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].origin_sequence,
                Some(owned.value().plan.fields.result_logical_sequence)
            );
            match role {
                physical::Role::ActiveId => {
                    let active: super::super::super::super::ActiveRecord8 =
                        serde_json::from_slice(rows[0].value.as_deref().expect("active payload"))
                            .expect("active record");
                    assert_eq!(
                        active.source_descriptor_sha256,
                        assembly.source.value().sha256
                    );
                    assert_eq!(
                        active.record_id,
                        owned.value().plan.fields.restore_notice_intent_id
                    );
                }
                physical::Role::DeliveryOrder => assert_eq!(
                    rows[0].value.as_deref(),
                    Some(assembly.source.value().sha256.as_bytes())
                ),
                physical::Role::Kv => unreachable!("notice role"),
            }
        }
        drop(certificate);
        drop(assembly);
        drop(owned);
        assert_eq!(io.live_ownership_evidence(), (0, 0));
    }

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
