//! One selected standard unit under workspace coordination. Finalization is separate.
use super::super::certificate;
use super::super::directory;
use super::super::prepared;
use super::super::seal;
use super::{
    CatalogError, ControlMvpSegmentRow, ExpectedPlan, OwnedSelectedPlan, RestorePhysicalIo,
    RestorePhysicalRoute, Result, SelectedProgress, admission, admitted_expected_plan, binary,
    bootstrap, codec, coverage, decode_with_reservation, invariant_violation, own_selected_plan,
    physical, publication, window,
};
use crate::state_store::control_mvp::retained;
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
        return Box::pin(verify_terminal(
            &mut io,
            &mut payload,
            &owned,
            &expected,
            &selected,
            context,
            persisted,
        ))
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
    clippy::print_stderr,
    reason = "one bounded final receipt remains on stack and test-only final evidence is retained"
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
        let mut last_refence = std::time::Instant::now();
        for ordinal in 0..selected.progress.value().receipt_count {
            if ordinal != 0
                && (ordinal % 512 == 0
                    || last_refence.elapsed() >= std::time::Duration::from_secs(10))
            {
                context.refence_final().await?;
                last_refence = std::time::Instant::now();
            }
            stream.next(io).await?;
        }
        context.refence_final().await?;
        let assembly = stream.finish(io).await?;
        context.refence_final().await?;
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
    let staged = stage_terminal_artifacts(
        io,
        owned,
        assembly.root(),
        assembly.digest(),
        &notice,
        certificate.value(),
        context,
        persisted,
    )
    .await;
    if staged.is_err() {
        io.stop_final(&mut totals);
    }
    let staged = staged?;
    #[cfg(feature = "test-utils")]
    if std::env::var_os("ARCO_STEP3_FINAL_STREAM_EVIDENCE").is_some() {
        let (operations, io_reservation, returned_owned, peak_live, peak_chunk) = totals.evidence();
        eprintln!(
            "step3-final-stream receipts={} operations={operations} io_reservation_bytes={io_reservation} returned_owned_bytes={returned_owned} peak_request_owned_live_bytes={peak_live} peak_chunk_owned_upper_bound_bytes={peak_chunk}",
            selected.progress.value().receipt_count
        );
    }
    drop(certificate);
    drop(notice);
    drop(assembly);
    publish_staged(io, payload, owned, staged, context, persisted).await
}

struct StagedCandidate {
    candidate_id: String,
    prepared_bytes: Vec<u8>,
    prepared_sha256: String,
    pointer_bytes: bytes::Bytes,
}

#[allow(
    clippy::too_many_lines,
    reason = "one protected arm, prepared write, HEAD send, and direct visibility proof"
)]
async fn publish_staged(
    io: &mut RestorePhysicalIo<'_>,
    payload: &mut UnitPayloadAdmission,
    owned: &WorkingValue<OwnedSelectedPlan>,
    staged: StagedCandidate,
    context: &mut RestoreAdvanceContext<'_>,
    persisted: &PersistedRestoreParticipantPlan,
) -> Result<RestoreParticipantAdvance> {
    use crate::retention_coordination::{
        ArmedRestorePublicationIntent, RetainedPointerIntentPrecondition,
    };
    use crate::state_store::RestoredAuthorityEvidence;
    use arco_core::{AuthorityWritePrecondition, WriteResult};

    let store = io.store();
    let plan = &owned.value().plan;
    let fields = &plan.fields;
    let prefix = format!(
        "{}/restore/v7/{}",
        store.paths.base_prefix(),
        staged.candidate_id
    );
    let prepared_path = format!("{prefix}/prepared.json");
    let precondition = plan.publication_precondition();
    {
        let mut selection = context.unit_selection(persisted).await?;
        let now = selection.observed_now();
        let (_, _, workspace) = selection.parts();
        let mut route = RestorePhysicalRoute::OrdinaryUnit { workspace, payload };
        if admission::observe(io, &mut route, owned, now).await?
            == admission::Observation::Superseded
        {
            return Ok(RestoreParticipantAdvance::Terminal(
                RestoreParticipantInspection::Superseded,
            ));
        }
    }
    let selected_digest = owned
        .value()
        .plan_sha256
        .strip_prefix("sha256:")
        .ok_or_else(|| invariant_violation("selected Plan7 digest lacks SHA-256 prefix"))?;
    context
        .arm_restore_publication(ArmedRestorePublicationIntent {
            intent_type: "arco.restore.publication-intent".into(),
            version: 1,
            domain: store.scope.domain().into(),
            plan_sha256: selected_digest.into(),
            owner_generation: fields.owner_generation,
            candidate_id: staged.candidate_id.clone(),
            prepared_path: prepared_path.clone(),
            prepared_sha256: staged.prepared_sha256.clone(),
            precondition: precondition.clone(),
        })
        .await?;
    {
        let mut selection = context.unit_selection(persisted).await?;
        let (_, _, workspace) = selection.parts();
        let written = write_terminal_immutable(
            store,
            workspace,
            &prepared_path,
            staged.prepared_bytes.clone(),
            super::super::MAX_CONTROL_JSON_BYTES,
        )
        .await?;
        if written.sha256 != staged.prepared_sha256 {
            return Err(invariant_violation(
                "restore prepared bytes changed after arming",
            ));
        }
    }
    {
        let mut selection = context.unit_selection(persisted).await?;
        let now = selection.observed_now();
        let (_, _, workspace) = selection.parts();
        let mut route = RestorePhysicalRoute::OrdinaryUnit { workspace, payload };
        if admission::observe_after_prepared(io, &mut route, owned, now, &staged.prepared_bytes)
            .await?
            == admission::Observation::Superseded
        {
            return Ok(RestoreParticipantAdvance::Terminal(
                RestoreParticipantInspection::Superseded,
            ));
        }
    }
    context.refence().await?;
    let mut selection = context.unit_selection(persisted).await?;
    let (_, _, workspace) = selection.parts();
    let condition = match precondition {
        RetainedPointerIntentPrecondition::DoesNotExist {} => {
            AuthorityWritePrecondition::DoesNotExist
        }
        RetainedPointerIntentPrecondition::MatchesVersion { version } => {
            AuthorityWritePrecondition::MatchesVersion(version)
        }
    };
    workspace.reserve_bytes(staged.pointer_bytes.len())?;
    workspace.charge_operations(1)?;
    let outcome = store
        .storage
        .put(
            &store.paths.current_pointer(),
            staged.pointer_bytes.clone(),
            condition,
        )
        .await;
    let result = match outcome {
        Ok(result) => result,
        Err(error) => {
            let error = CatalogError::from(error);
            let _ = workspace.charge_error(&error);
            return Err(CatalogError::AmbiguousAuthorityOutcome {
                message: format!(
                    "restore HEAD outcome for candidate {} is uncertain: {error}",
                    staged.candidate_id
                ),
            });
        }
    };
    workspace
        .charge_write(&result)
        .map_err(|_| CatalogError::AmbiguousAuthorityOutcome {
            message: "restore HEAD response exceeded bounded admission".into(),
        })?;
    if matches!(result, WriteResult::PreconditionFailed { .. }) {
        return Ok(RestoreParticipantAdvance::Terminal(
            RestoreParticipantInspection::Superseded,
        ));
    }
    let mut budget = retained::ReadBudget::with_workspace(workspace);
    let Some((pointer, version, head_bytes)) =
        retained::load_current_pointer(store, &mut budget).await?
    else {
        return Err(CatalogError::AmbiguousAuthorityOutcome {
            message: "successful restore HEAD send has no readable HEAD".into(),
        });
    };
    if head_bytes != staged.pointer_bytes {
        return Err(CatalogError::AmbiguousAuthorityOutcome {
            message: "restore HEAD advanced before direct visibility proof".into(),
        });
    }
    let manifest_path = store.paths.manifest_object(&staged.candidate_id);
    let manifest_bytes = retained::read_bounded_json(
        store,
        &mut budget,
        &manifest_path,
        super::super::MAX_CONTROL_JSON_BYTES,
        "visible restore manifest",
    )
    .await?;
    let token = store
        .token(staged.candidate_id.clone(), fields.result_logical_sequence)
        .with_manifest_witness(pointer.manifest_checksum_sha256.clone());
    store
        .read_bounded_current_from_manifest(
            &token,
            &manifest_bytes,
            &pointer,
            version,
            head_bytes,
            &mut budget,
        )
        .await?;
    let evidence = RestoredAuthorityEvidence::new(
        super::super::super::super::IMPLEMENTATION,
        store.scope.clone(),
        &staged.candidate_id,
        &staged.candidate_id,
        &manifest_path,
        format!("sha256:{}", pointer.manifest_checksum_sha256),
        fields.result_logical_sequence,
        fields.identity.attempt(),
    )?;
    Ok(RestoreParticipantAdvance::Terminal(
        RestoreParticipantInspection::Visible { token, evidence },
    ))
}

async fn write_terminal_immutable(
    store: &super::super::super::super::ControlMvpStateStore,
    workspace: &mut crate::workspace_io_budget::WorkspaceIoBudget,
    path: &str,
    bytes: Vec<u8>,
    cap: usize,
) -> Result<super::super::super::ArtifactRef> {
    use arco_core::{AuthorityWritePrecondition, WriteResult};
    if bytes.is_empty() || bytes.len() > cap {
        return Err(invariant_violation(
            "restore final artifact exceeds its bound",
        ));
    }
    let bytes = bytes::Bytes::from(bytes);
    let digest = super::super::super::super::sha256_hex(&bytes);
    workspace.reserve_bytes(bytes.len())?;
    workspace.charge_operations(1)?;
    let outcome = store
        .storage
        .put(
            path,
            bytes.clone(),
            AuthorityWritePrecondition::DoesNotExist,
        )
        .await;
    let result = match outcome {
        Ok(result) => result,
        Err(error) => {
            let error = CatalogError::from(error);
            let _ = workspace.charge_error(&error);
            return Err(error);
        }
    };
    workspace.charge_write(&result)?;
    if let WriteResult::PreconditionFailed { .. } = result {
        let existing = workspace
            .read_immutable(&store.retention, path, Some(bytes.len()), &digest, cap)
            .await?;
        if existing != bytes {
            return Err(invariant_violation(
                "restore final artifact already exists with different bytes",
            ));
        }
    }
    Ok(super::super::super::ArtifactRef {
        path: path.to_owned(),
        sha256: digest,
    })
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the verified terminal binds its candidate roots and immutable artifacts in one held invocation"
)]
async fn stage_terminal_artifacts(
    io: &RestorePhysicalIo<'_>,
    owned: &WorkingValue<OwnedSelectedPlan>,
    kv_root: &directory::Root,
    history: &str,
    notice: &NoticeAssembly,
    certificate: &certificate::RestoreCertificateV1,
    context: &mut RestoreAdvanceContext<'_>,
    persisted: &PersistedRestoreParticipantPlan,
) -> Result<StagedCandidate> {
    use super::super::super::{LEAF_ENCODING, Manifest8, ManifestKind8, RootBinding};
    use super::super::{Target, retained};
    let store = io.store();
    let plan = &owned.value().plan;
    let fields = &plan.fields;
    let candidate = &fields.candidate_id;
    let prefix = format!("{}/restore/v7/{candidate}", store.paths.base_prefix());
    let plan_bytes = serde_jcs::to_vec(plan)
        .map_err(|_| invariant_violation("restore final plan encoding failed"))?;
    let certificate_bytes = serde_jcs::to_vec(certificate)
        .map_err(|_| invariant_violation("restore final certificate encoding failed"))?;
    let mut selection = context.unit_selection(persisted).await?;
    let (_, selected_digest, workspace) = selection.parts();
    if selected_digest != owned.value().plan_sha256 {
        return Err(invariant_violation("restore final selected plan changed"));
    }
    let plan_ref = write_terminal_immutable(
        store,
        workspace,
        &format!("{prefix}/plan.json"),
        plan_bytes,
        4 * 1024 * 1024,
    )
    .await?;
    let certificate_ref = write_terminal_immutable(
        store,
        workspace,
        &format!("{prefix}/certificate.json"),
        certificate_bytes,
        super::super::MAX_CONTROL_JSON_BYTES,
    )
    .await?;
    let (parent_id, parent_digest, writer_epoch, reclamation_generation) = match &fields.target {
        Target::Present {
            current_pointer_raw_b64,
            writer_epoch,
            reclamation_generation,
            ..
        } => {
            let pointer: super::super::ControlMvpPointer = super::super::decode_json(
                &binary(current_pointer_raw_b64)?,
                "restore final original HEAD",
            )?;
            (
                pointer.manifest_id,
                pointer.manifest_checksum_sha256,
                *writer_epoch,
                *reclamation_generation,
            )
        }
        Target::Absent {
            observed_writer_epoch,
            observed_reclamation_generation,
            ..
        } => (
            fields.source.manifest_id().to_owned(),
            super::raw_digest(&fields.source_manifest.sha256)?.to_owned(),
            *observed_writer_epoch,
            *observed_reclamation_generation,
        ),
    };
    let root = |role, root: &directory::Root| RootBinding {
        role,
        leaf_encoding: LEAF_ENCODING.into(),
        directory_root_hex: hex::encode(root.encode()),
    };
    let manifest = Manifest8 {
        format_version: 8,
        implementation: super::super::super::super::IMPLEMENTATION.into(),
        scope: store.scope.clone(),
        manifest_id: candidate.clone(),
        logical_sequence: fields.result_logical_sequence,
        logical_history: history.to_owned(),
        kind: ManifestKind8::Restore,
        parent_manifest_id: Some(parent_id),
        parent_manifest_sha256: Some(parent_digest),
        writer_epoch,
        reclamation_generation,
        kv_root: root(physical::Role::Kv, kv_root),
        active_id_root: root(physical::Role::ActiveId, &notice.active.value().root),
        delivery_order_root: root(physical::Role::DeliveryOrder, &notice.delivery.value().root),
        transaction: None,
        transition: None,
        genesis_witness: None,
        projection_source: Some(notice.source.value().clone()),
        restore_plan: Some(plan_ref),
        restore_certificate: Some(certificate_ref.clone()),
        directory_object_version: Some(2),
    };
    manifest.validate(&store.scope, candidate)?;
    certificate.validate_manifest(
        &manifest,
        (
            kv_root,
            &notice.active.value().root,
            &notice.delivery.value().root,
        ),
    )?;
    let manifest_bytes = super::super::super::super::encode_json_limited(
        &manifest,
        super::super::MAX_CONTROL_JSON_BYTES,
        "restore final manifest",
    )?;
    let manifest_ref = write_terminal_immutable(
        store,
        workspace,
        &store.paths.manifest_object(candidate),
        manifest_bytes.to_vec(),
        super::super::MAX_CONTROL_JSON_BYTES,
    )
    .await?;
    let seal = seal::FinalSealV1::from_verified(
        plan,
        selected_digest,
        &manifest,
        &manifest_ref,
        &certificate_ref,
        certificate,
    )?;
    let seal_bytes = serde_jcs::to_vec(&seal)
        .map_err(|_| invariant_violation("restore final seal encoding failed"))?;
    let seal_ref = write_terminal_immutable(
        store,
        workspace,
        &format!("{prefix}/seal.json"),
        seal_bytes,
        seal::MAX_BYTES,
    )
    .await?;
    let prepared = prepared::PreparedRestoreV1::from_verified(
        plan,
        selected_digest,
        &manifest,
        &manifest_ref,
        &certificate_ref,
        &seal_ref,
    )?;
    let prepared_bytes = prepared.canonical_bytes()?;
    let prepared_sha256 = super::super::super::super::sha256_hex(&prepared_bytes);
    let pointer_bytes = prepared.candidate_head_bytes()?;
    let directory = directory::Directory::new(store.retention.clone(), &store.scope)?;
    let roots = super::super::super::decode_roots(&directory, &manifest)?;
    super::super::super::validate_manifest_artifacts_with_budget(
        store,
        &manifest,
        (&roots.0, &roots.1, &roots.2),
        Some(&mut retained::ReadBudget::with_workspace(workspace)),
    )
    .await?;
    Ok(StagedCandidate {
        candidate_id: candidate.clone(),
        prepared_bytes,
        prepared_sha256,
        pointer_bytes,
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
            Some(&owned.value().plan.fields.candidate_id),
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
            Some(&owned.value().plan.fields.candidate_id),
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
            Err(CatalogError::InvariantViolation { .. })
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
        reason = "the reader fixture assembles one authenticated candidate and then removes its certificate"
    )]
    async fn restore_manifest_reader_authenticates_plan_certificate_and_roots() {
        let (store, plan) = super::super::super::tests::inspection_fixture().await;
        let candidate = plan.fields.candidate_id.clone();
        let prefix = format!("{}/restore/v7/{candidate}", store.paths.base_prefix());
        let plan_bytes = serde_jcs::to_vec(&plan).expect("plan bytes");
        let plan_digest = super::super::super::super::sha256_hex(&plan_bytes);
        let selected_digest = super::super::prefixed_sha256(
            &serde_jcs::to_vec(&PersistedRestoreParticipantPlan::ControlMvpV7(Box::new(
                plan.clone(),
            )))
            .expect("selected plan bytes"),
        );
        assert_ne!(selected_digest, format!("sha256:{plan_digest}"));
        let plan_ref = super::super::super::super::ArtifactRef {
            path: format!("{prefix}/plan.json"),
            sha256: plan_digest.clone(),
        };
        store
            .storage
            .put(
                &plan_ref.path,
                bytes::Bytes::from(plan_bytes),
                arco_core::AuthorityWritePrecondition::DoesNotExist,
            )
            .await
            .expect("private plan");
        let directory =
            directory::Directory::new(store.retention.clone(), &store.scope).expect("directory");
        let kv_root = directory
            .decode_root(&binary(&plan.fields.base_kv_root_b64).expect("KV root"))
            .expect("root");
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
        let mut totals = FinalStreamTotals::new();
        let owned = {
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("plan chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let owned = decode_with_reservation(&mut io, &mut route, Some(4 * 1024 * 1024), || {
                Ok(OwnedSelectedPlan {
                    plan,
                    plan_sha256: selected_digest,
                })
            })
            .expect("owned plan");
            let _ = chunk.finish();
            owned
        };
        let notice = assemble_notice(&mut io, &mut totals, &owned, &kv_root, 3)
            .await
            .expect("notice");
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
                &notice,
            )
            .expect("certificate");
            let _ = chunk.finish();
            certificate
        };
        let certificate_bytes = serde_jcs::to_vec(certificate.value()).expect("certificate bytes");
        let certificate_ref = super::super::super::super::ArtifactRef {
            path: format!("{prefix}/certificate.json"),
            sha256: super::super::super::super::sha256_hex(&certificate_bytes),
        };
        store
            .storage
            .put(
                &certificate_ref.path,
                bytes::Bytes::from(certificate_bytes),
                arco_core::AuthorityWritePrecondition::DoesNotExist,
            )
            .await
            .expect("private certificate");
        let base = store.pin_bounded_base().await.expect("target base");
        let mut manifest = base.manifest.expect("target manifest");
        manifest.parent_manifest_id = Some(manifest.manifest_id.clone());
        manifest.parent_manifest_sha256 = base.manifest_digest;
        manifest.manifest_id = candidate;
        manifest.kind = super::super::super::super::ManifestKind8::Restore;
        manifest.logical_sequence = owned.value().plan.fields.result_logical_sequence;
        manifest.logical_history = "bb".repeat(32);
        manifest.kv_root.directory_root_hex = hex::encode(kv_root.encode());
        manifest.active_id_root.directory_root_hex =
            hex::encode(notice.active.value().root.encode());
        manifest.delivery_order_root.directory_root_hex =
            hex::encode(notice.delivery.value().root.encode());
        manifest.transaction = None;
        manifest.transition = None;
        manifest.projection_source = Some(notice.source.value().clone());
        manifest.restore_plan = Some(plan_ref);
        manifest.restore_certificate = Some(certificate_ref.clone());
        manifest
            .validate(&store.scope, &manifest.manifest_id)
            .expect("restore shape");
        assert!(
            super::super::super::super::validate_manifest_artifacts(
                &store,
                &manifest,
                (
                    &kv_root,
                    &notice.active.value().root,
                    &notice.delivery.value().root,
                ),
            )
            .await
            .is_err(),
            "restore manifest must require its final seal"
        );
        let manifest_bytes = super::super::super::super::encode_json_limited(
            &manifest,
            super::super::super::MAX_CONTROL_JSON_BYTES,
            "restore test manifest",
        )
        .expect("manifest bytes");
        let manifest_digest = super::super::super::super::sha256_hex(&manifest_bytes);
        let manifest_ref = super::super::super::super::ArtifactRef {
            path: store.paths.manifest_object(&manifest.manifest_id),
            sha256: manifest_digest.clone(),
        };
        let seal = seal::FinalSealV1::from_verified(
            &owned.value().plan,
            &owned.value().plan_sha256,
            &manifest,
            &manifest_ref,
            &certificate_ref,
            certificate.value(),
        )
        .expect("final seal");
        let seal_path = format!("{prefix}/seal.json");
        let seal_bytes = bytes::Bytes::from(serde_jcs::to_vec(&seal).expect("seal bytes"));
        let seal_ref = super::super::super::super::ArtifactRef {
            path: seal_path.clone(),
            sha256: super::super::super::super::sha256_hex(&seal_bytes),
        };
        let prepared = prepared::PreparedRestoreV1::from_verified(
            &owned.value().plan,
            &owned.value().plan_sha256,
            &manifest,
            &manifest_ref,
            &certificate_ref,
            &seal_ref,
        )
        .expect("prepared bytes from sealed candidate");
        let prepared_bytes = prepared.canonical_bytes().expect("bounded prepared bytes");
        assert_eq!(
            serde_json::from_slice::<prepared::PreparedRestoreV1>(&prepared_bytes)
                .expect("prepared replay"),
            prepared
        );
        store
            .storage
            .put(
                &seal_path,
                seal_bytes.clone(),
                arco_core::AuthorityWritePrecondition::DoesNotExist,
            )
            .await
            .expect("private seal");
        super::super::super::super::validate_manifest_artifacts(
            &store,
            &manifest,
            (
                &kv_root,
                &notice.active.value().root,
                &notice.delivery.value().root,
            ),
        )
        .await
        .expect("authenticated restore manifest");
        store
            .storage
            .put(
                &store.paths.manifest_object(&manifest.manifest_id),
                manifest_bytes,
                arco_core::AuthorityWritePrecondition::DoesNotExist,
            )
            .await
            .expect("private manifest");
        let token = store
            .token(manifest.manifest_id.clone(), manifest.logical_sequence)
            .with_manifest_witness(manifest_digest.clone());
        store
            .read_bounded_token(&token)
            .await
            .expect("sealed token");
        store
            .retention
            .delete(&seal_path)
            .await
            .expect("remove seal");
        assert!(store.read_bounded_token(&token).await.is_err());
        assert!(
            super::super::super::super::recovery_manifest(
                &store,
                &manifest.manifest_id,
                &manifest_digest,
            )
            .await
            .is_err(),
            "ancestry recovery must reject a restore manifest without its seal"
        );
        store
            .storage
            .put(
                &seal_path,
                seal_bytes,
                arco_core::AuthorityWritePrecondition::DoesNotExist,
            )
            .await
            .expect("restore seal");
        store
            .retention
            .delete(&certificate_ref.path)
            .await
            .expect("remove certificate");
        assert!(
            super::super::super::super::recovery_manifest(
                &store,
                &manifest.manifest_id,
                &manifest_digest,
            )
            .await
            .is_err(),
            "ancestry recovery must reject a restore manifest without its certificate"
        );
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
            directory::Directory::new(store.retention.clone(), &store.scope)
                .expect("directory")
                .for_publication(&owned.value().plan.fields.candidate_id)
                .expect("candidate directory")
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
            &mut retained::ReadBudget::new(),
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
                &mut retained::ReadBudget::new(),
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
                    &mut retained::ReadBudget::new(),
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
                &mut retained::ReadBudget::new(),
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
                &mut retained::ReadBudget::new(),
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
                &mut retained::ReadBudget::new(),
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
