//! Durable singleton phases and private Emit; no historical-prefix or publication authority.
use super::super::{Mode, directory};
use super::{
    CatalogError, ControlMvpRestoreReceiptV1, DirectoryPathEntry, DirectoryPosition,
    DirectoryPositionLeafWitness, ExpectedPlan, GlobalCut, ImmutableObjectWitness, InputWitness,
    MAX_BLOCK_BYTES, OwnedSelectedPlan, ReceiptRef, RestorePhysicalIo, RestorePhysicalRoute,
    RestoreProgressV1, Result, SINGLETON_INPUT_BYTE_LIMIT, SINGLETON_SCRATCH_BASE_BYTES,
    SINGLETON_SCRATCH_PAYLOAD_MULTIPLIER, SINGLETON_SEGMENT_BYTE_LIMIT, SelectedProgress,
    SemanticCounts, SideCursor, SingletonPhase, SingletonState, SingletonValueWitness,
    UnitReservationV1, WorkingValue, binary, checked_sum, codec, decode_with_reservation,
    invariant_violation, pending_key, physical, prefixed_sha256, publication, raw_digest,
    validate_cursor_shape, validate_side_cursor, validate_singleton_shape, window,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use physical::restore_io::{
    hash_with_reservation, read_restore_descriptor_sized, read_singleton_phase_leaf,
};

pub(in crate::state_store::control_mvp) struct SingletonEmitAdmission<'a>(
    &'a WorkingValue<RestoreProgressV1>,
);
impl SingletonEmitAdmission<'_> {
    pub(in crate::state_store::control_mvp) fn is_owned_by(
        &self,
        io: &RestorePhysicalIo<'_>,
    ) -> bool {
        self.0.is_owned_by(io)
    }
    pub(in crate::state_store::control_mvp) fn permits(
        &self,
        owned: bool,
        descriptor: &physical::Descriptor,
        ordinal: usize,
        key: &[u8],
    ) -> bool {
        owned
            && matches!(&self.0.value().singleton_state,
            SingletonState::Pending { key_b64url, phase: SingletonPhase::Emit, source, current }
            if *key_b64url == URL_SAFE_NO_PAD.encode(key)
                && source.as_deref().into_iter().chain(current.as_deref()).any(|w|
                    w.row_ordinal == ordinal as u64
                    && w.index.sha256 == format!("sha256:{}", descriptor.segment.index_checksum_sha256)
                    && w.block.offset == descriptor.block.offset
                    && w.block.length == descriptor.block.length
                    && w.block.sha256 == format!("sha256:{}", descriptor.block.checksum_sha256)))
    }
}
#[cfg(test)]
pub(super) fn test_emit_admission(
    progress: &WorkingValue<RestoreProgressV1>,
) -> SingletonEmitAdmission<'_> {
    SingletonEmitAdmission(progress)
}

const MODEL_BYTES: usize = 16 * 1024 * 1024;

fn capacity() -> CatalogError {
    CatalogError::MaintenanceBackpressure {
        message: "singleton comparison requires bounded singleton boundaries".into(),
    }
}

pub(super) async fn prepare<'a, 'progress>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    plan: &WorkingValue<OwnedSelectedPlan>,
    expected: &'a WorkingValue<ExpectedPlan<'progress>>,
    selected: &'a SelectedProgress,
) -> Result<publication::PreparedUnit<'a, 'progress>> {
    let result = prepare_inner(io, route, plan, expected, selected).await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

#[allow(
    clippy::too_many_lines,
    reason = "keep guarded observation lifetimes visible through durable assembly"
)]
async fn prepare_inner<'a, 'progress>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    plan: &WorkingValue<OwnedSelectedPlan>,
    expected: &'a WorkingValue<ExpectedPlan<'progress>>,
    selected: &'a SelectedProgress,
) -> Result<publication::PreparedUnit<'a, 'progress>> {
    let owned = plan.is_owned_by(io)
        && expected.is_owned_by(io)
        && selected.progress.is_owned_by(io)
        && selected.selector_raw.is_owned_by(io)
        && selected.selector_meta.is_owned_by(io);
    let ordinary = matches!(route, RestorePhysicalRoute::OrdinaryUnit { .. });
    let store = io.store();
    let reservation = physical::restore_io::directory_scope_reservation(store)?
        .checked_add(MODEL_BYTES)
        .ok_or_else(capacity)?;
    let roots = decode_with_reservation(io, route, Some(reservation), || {
        let fields = &plan.value().plan.fields;
        let expected_plan = expected.value();
        let progress = selected.progress.value();
        if !owned
            || !ordinary
            || store.authority_format != 8
            || plan.value().plan_sha256 != expected_plan.plan_sha256
            || fields.candidate_id != expected_plan.candidate_id
            || fields.identity != *expected_plan.identity
            || fields.owner_generation != expected_plan.owner_generation
            || fields.candidate_seed_sha256 != expected_plan.candidate_seed_sha256
            || fields.source_kv_root_b64 != expected_plan.source_kv_root_b64
            || fields.base_kv_root_b64 != expected_plan.base_kv_root_b64
            || progress.plan_sha256 != expected_plan.plan_sha256
            || progress.identity != *expected_plan.identity
            || progress.owner_generation != expected_plan.owner_generation
            || progress.terminal
            || progress.next_ordinal == u64::MAX
            || fields.base_logical_sequence.checked_add(1) != Some(fields.result_logical_sequence)
            || (fields.mode == Mode::Absent
                && fields.base_logical_sequence != fields.source_logical_sequence)
        {
            return Err(invariant_violation(
                "singleton plan, owner, or selected state differs",
            ));
        }
        if !matches!(
            progress.singleton_state,
            SingletonState::None
                | SingletonState::Complete { .. }
                | SingletonState::Pending {
                    phase: SingletonPhase::CompareSource
                        | SingletonPhase::CompareCurrent
                        | SingletonPhase::Emit,
                    ..
                }
        ) {
            return Err(capacity());
        }
        validate_cursor_shape(expected_plan, &progress.cursor)?;
        validate_singleton_shape(&progress.singleton_state)?;
        plan.value().plan.validate_roots(store)?;
        let after = match &progress.cursor.global {
            GlobalCut::Start => None,
            GlobalCut::After { key_b64url } if key_b64url.len() <= 4 * MAX_BLOCK_BYTES / 3 + 4 => {
                Some(binary(key_b64url)?)
            }
            _ => return Err(capacity()),
        };
        let d = directory::Directory::new(store.retention.clone(), &store.scope)?;
        Ok((
            d.decode_root(&binary(expected_plan.source_kv_root_b64)?)?,
            d.decode_root(&binary(expected_plan.base_kv_root_b64)?)?,
            after,
        ))
    })?;
    let fields = &plan.value().plan.fields;
    let expected_plan = expected.value();
    let progress = selected.progress.value();
    let source = candidate(
        io,
        route,
        &roots.value().0,
        expected_plan.source_kv_root_b64,
        &progress.cursor.source,
        roots.value().2.as_deref(),
        fields.source_logical_sequence,
    )
    .await?;
    let current = if fields.mode == Mode::Absent {
        decode_with_reservation(io, route, Some(64 * 1024), || {
            if !matches!(progress.cursor.current, SideCursor::Start | SideCursor::End) {
                return Err(invariant_violation(
                    "absent singleton current cursor differs",
                ));
            }
            Ok(None)
        })?
    } else {
        candidate(
            io,
            route,
            &roots.value().1,
            expected_plan.base_kv_root_b64,
            &progress.cursor.current,
            roots.value().2.as_deref(),
            fields.base_logical_sequence,
        )
        .await?
    };
    let key = decode_with_reservation(io, route, Some(MODEL_BYTES), || {
        if let Some(key) = pending_key(&progress.singleton_state)?
            .filter(|_| matches!(progress.singleton_state, SingletonState::Pending { .. }))
        {
            return Ok(key);
        }
        let key = source
            .value()
            .as_ref()
            .into_iter()
            .chain(current.value().as_ref())
            .map(|progress| progress.leaf.first.as_slice())
            .min()
            .ok_or_else(capacity)?;
        if matches!(progress.singleton_state, SingletonState::Pending { .. })
            && pending_key(&progress.singleton_state)?.is_some_and(|k| k != key)
        {
            return Err(invariant_violation(
                "pending singleton skips the next rooted key",
            ));
        }
        Ok(key.to_vec())
    })?;
    let at_key = |position: &Option<directory::restore::Position>, source_side: bool| {
        position.as_ref().is_some_and(|v| {
            if v.leaf.bytes as usize > MAX_BLOCK_BYTES || v.leaf.rows == 1 {
                v.leaf.first == *key.value()
            } else {
                match &progress.singleton_state {
                    SingletonState::Pending {
                        source, current, ..
                    } => {
                        if source_side {
                            source.is_some()
                        } else {
                            current.is_some()
                        }
                    }
                    _ => false,
                }
            }
        })
    };
    let has_source = at_key(source.value(), true);
    let has_current = at_key(current.value(), false);
    if let SingletonState::Pending {
        phase: SingletonPhase::Emit,
        source: s,
        current: c,
        ..
    } = &progress.singleton_state
    {
        return emit(
            io,
            route,
            plan,
            expected,
            selected,
            &source,
            &current,
            key.value(),
            s.as_deref(),
            c.as_deref(),
        )
        .await;
    }
    let read_source = match &progress.singleton_state {
        SingletonState::Pending {
            phase: SingletonPhase::CompareSource,
            ..
        } => match &progress.singleton_state {
            SingletonState::Pending {
                source, current, ..
            } => {
                if source.is_none() && has_source {
                    true
                } else if current.is_none() && has_current {
                    false
                } else {
                    has_source
                }
            }
            _ => !has_current,
        },
        _ => has_source,
    };
    let (position, root, sequence) = if read_source {
        (
            &source,
            expected_plan.source_kv_root_b64,
            fields.source_logical_sequence,
        )
    } else {
        (
            &current,
            expected_plan.base_kv_root_b64,
            fields.base_logical_sequence,
        )
    };
    let observation = observe(io, route, position, root, sequence, key.value()).await?;
    let genesis = if progress.last_receipt.is_none() {
        Some(codec::hashes::genesis_none(io, route, &selected.progress)?)
    } else {
        None
    };
    let receipt = decode_with_reservation(io, route, Some(reservation), || {
        let (input, witness) = observation.value();
        let (mut source_witness, mut current_witness, phase, charged_phase) =
            match &progress.singleton_state {
                SingletonState::None | SingletonState::Complete { .. } => (
                    None,
                    None,
                    SingletonPhase::CompareSource,
                    SingletonPhase::CompareSource,
                ),
                SingletonState::Pending {
                    source,
                    current,
                    phase: SingletonPhase::CompareSource,
                    ..
                } => (
                    source.clone(),
                    current.clone(),
                    SingletonPhase::CompareCurrent,
                    SingletonPhase::CompareSource,
                ),
                SingletonState::Pending {
                    source,
                    current,
                    phase: SingletonPhase::CompareCurrent,
                    ..
                } => (
                    source.clone(),
                    current.clone(),
                    SingletonPhase::Emit,
                    SingletonPhase::CompareCurrent,
                ),
                SingletonState::Pending { .. } => return Err(capacity()),
            };
        // These checks bind retained observations to their original immutable
        // locators. Full value truth across all phases still requires final replay.
        for (prior, pos) in [(&source_witness, &source), (&current_witness, &current)] {
            if let Some(prior) = prior {
                let leaf = &pos
                    .value()
                    .as_ref()
                    .ok_or_else(|| invariant_violation("singleton witness side disappeared"))?
                    .leaf;
                if key.value().as_slice() < leaf.first.as_slice()
                    || key.value().as_slice() > leaf.last.as_slice()
                    || prior.descriptor.sha256 != format!("sha256:{}", hex::encode(leaf.digest))
                    || prior.block.length != u64::from(leaf.bytes)
                    || prior.block.rows != leaf.rows
                    || prior.row_ordinal >= leaf.rows
                {
                    return Err(invariant_violation(
                        "singleton witness locator differs from root",
                    ));
                }
            }
        }
        let slot = if read_source {
            &mut source_witness
        } else {
            &mut current_witness
        };
        if slot.as_ref().is_some_and(|old| **old != *witness) {
            return Err(invariant_violation(
                "singleton reread differs from recorded observation",
            ));
        }
        *slot = Some(Box::new(witness.clone()));
        if source_witness.is_some() != has_source
            || (phase != SingletonPhase::CompareSource && current_witness.is_some() != has_current)
        {
            return Err(invariant_violation(
                "singleton phase observation or rooted absence differs",
            ));
        }
        let bytes = witness.block.length;
        Ok(ControlMvpRestoreReceiptV1 {
            record_type: "control_mvp_restore_receipt".into(),
            version: 1,
            plan_sha256: expected_plan.plan_sha256.into(),
            identity: expected_plan.identity.clone(),
            owner_generation: expected_plan.owner_generation,
            ordinal: progress.next_ordinal,
            predecessor_receipt_sha256: progress
                .last_receipt
                .as_ref()
                .map(|r| r.raw_sha256.as_str())
                .or_else(|| genesis.as_ref().map(|g| g.value().as_str()))
                .ok_or_else(|| invariant_violation("missing singleton predecessor"))?
                .into(),
            predecessor_chain_sha256: progress.chain_sha256.clone(),
            before: progress.cursor.clone(),
            after: progress.cursor.clone(),
            singleton_before: progress.singleton_state.clone(),
            singleton_after: SingletonState::Pending {
                key_b64url: URL_SAFE_NO_PAD.encode(key.value()),
                source: source_witness,
                current: current_witness,
                phase,
            },
            source_inputs: if read_source {
                vec![input.clone()]
            } else {
                vec![]
            },
            current_inputs: if read_source {
                vec![]
            } else {
                vec![input.clone()]
            },
            outputs: vec![],
            prefix_cumulative_counts: progress.cumulative_counts.clone(),
            counts: SemanticCounts {
                source_leaves: u64::from(read_source),
                current_leaves: u64::from(!read_source),
                input_encoded_bytes: bytes,
                decoded_blocks: 1,
                decoded_rows: witness.block.rows,
                output_blocks: 0,
                output_encoded_bytes: 0,
                mutations: 0,
                reservation: UnitReservationV1::Singleton {
                    phase: charged_phase,
                    authenticated_payload_bytes: bytes,
                    input_byte_limit: SINGLETON_INPUT_BYTE_LIMIT,
                    segment_byte_limit: SINGLETON_SEGMENT_BYTE_LIMIT,
                    scratch_base_bytes: SINGLETON_SCRATCH_BASE_BYTES,
                    scratch_payload_multiplier: SINGLETON_SCRATCH_PAYLOAD_MULTIPLIER,
                },
            },
            receipt_body_sha256: format!("sha256:{}", "0".repeat(64)),
            chain_sha256: format!("sha256:{}", "0".repeat(64)),
        })
    })?;
    drop((source, current, roots, key, observation, genesis));
    finish(io, route, expected, selected, receipt, &[])
}

pub(super) fn position_witness(root: &str, p: &directory::restore::Position) -> DirectoryPosition {
    DirectoryPosition {
        role: physical::Role::Kv,
        root_b64: root.into(),
        path: p
            .path
            .iter()
            .map(|(digest, index)| DirectoryPathEntry {
                page_sha256: format!("sha256:{}", hex::encode(digest)),
                child_index: *index,
            })
            .collect(),
        leaf: DirectoryPositionLeafWitness {
            first_b64url: URL_SAFE_NO_PAD.encode(&p.leaf.first),
            last_b64url: URL_SAFE_NO_PAD.encode(&p.leaf.last),
            rows: p.leaf.rows,
            bytes: p.leaf.bytes,
            digest: format!("sha256:{}", hex::encode(p.leaf.digest)),
        },
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "root, cursor and sequence are the selected role's authentication inputs"
)]
async fn candidate(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    root: &directory::Root,
    root_b64: &str,
    cursor: &SideCursor,
    global: Option<&[u8]>,
    sequence: u64,
) -> Result<WorkingValue<Option<directory::restore::Position>>> {
    let key = decode_with_reservation(io, route, Some(MODEL_BYTES), || {
        validate_side_cursor(root_b64, cursor)?;
        match cursor {
            SideCursor::After { key_b64url, .. }
                if key_b64url.len() <= 4 * MAX_BLOCK_BYTES / 3 + 4 =>
            {
                let key = binary(key_b64url)?;
                if global.is_none_or(|g| key.as_slice() > g) {
                    return Err(invariant_violation("singleton cursor exceeds cut"));
                }
                Ok(Some(key))
            }
            SideCursor::After { .. } => Err(capacity()),
            _ => Ok(None),
        }
    })?;
    if let SideCursor::After { position, .. } = cursor {
        let boundary = directory::restore::first_at_or_after(
            io,
            route,
            root,
            key.value().as_deref().ok_or_else(capacity)?,
        )
        .await?;
        authenticate_boundary(io, route, &boundary, sequence).await?;
        drop(decode_with_reservation(
            io,
            route,
            Some(MODEL_BYTES),
            || {
                let b = boundary
                    .value()
                    .as_ref()
                    .ok_or_else(|| invariant_violation("singleton cursor leaf missing"))?;
                if position_witness(root_b64, b) != *position
                    || key.value().as_deref().is_none_or(|key| {
                        key < b.leaf.first.as_slice() || key > b.leaf.last.as_slice()
                    })
                {
                    return Err(invariant_violation("singleton cursor path or key differs"));
                }
                Ok(())
            },
        )?);
    }
    let position = if let Some(key) = key.value().as_deref() {
        let floor = directory::restore::first_at_or_after(io, route, root, key).await?;
        if floor
            .value()
            .as_ref()
            .is_some_and(|p| p.leaf.last.as_slice() > key)
        {
            floor
        } else {
            directory::restore::first_after(io, route, root, Some(key)).await?
        }
    } else {
        directory::restore::first_after(
            io,
            route,
            root,
            if matches!(cursor, SideCursor::End) {
                global
            } else {
                None
            },
        )
        .await?
    };
    authenticate_boundary(io, route, &position, sequence).await?;
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        if let Some(p) = position.value() {
            if matches!(cursor, SideCursor::End)
                || global.is_some_and(|g| p.leaf.first.as_slice() <= g)
                    && !matches!(cursor, SideCursor::After { position, .. } if position.leaf.digest == format!("sha256:{}", hex::encode(p.leaf.digest)) && global.is_some_and(|g| p.leaf.last.as_slice() > g))
            {
                return Err(invariant_violation("singleton cursor omits rooted input"));
            }
        }
        Ok(())
    })?);
    Ok(position)
}

async fn authenticate_boundary(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    position: &WorkingValue<Option<directory::restore::Position>>,
    sequence: u64,
) -> Result<()> {
    if let Some(p) = position.value() {
        drop(decode_with_reservation(io, route, Some(64 * 1024), || {
            if (p.leaf.bytes as usize > MAX_BLOCK_BYTES
                && (p.leaf.rows != 1 || p.leaf.first != p.leaf.last))
                || p.leaf.first.len() > MAX_BLOCK_BYTES
            {
                return Err(capacity());
            }
            Ok(())
        })?);
        let (descriptor, _) =
            read_restore_descriptor_sized(io, route, physical::Role::Kv, &p.leaf).await?;
        drop(decode_with_reservation(io, route, Some(64 * 1024), || {
            if descriptor.value().segment.logical_sequence > sequence {
                return Err(invariant_violation(
                    "singleton descriptor is newer than pinned root",
                ));
            }
            Ok(())
        })?);
    }
    Ok(())
}

async fn observe(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    position: &WorkingValue<Option<directory::restore::Position>>,
    root: &str,
    sequence: u64,
    key: &[u8],
) -> Result<WorkingValue<(InputWitness, SingletonValueWitness)>> {
    let (observation, _, _) = observe_rows(io, route, position, root, sequence, key).await?;
    Ok(observation)
}

async fn observe_rows(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    position: &WorkingValue<Option<directory::restore::Position>>,
    root: &str,
    sequence: u64,
    key: &[u8],
) -> Result<(
    WorkingValue<(InputWitness, SingletonValueWitness)>,
    WorkingValue<Vec<super::ControlMvpSegmentRow>>,
    WorkingValue<physical::Descriptor>,
)> {
    let p = position.value().as_ref().ok_or_else(capacity)?;
    let leaf = decode_with_reservation(io, route, Some(MODEL_BYTES), || Ok(p.leaf.clone()))?;
    let (descriptor, rows, descriptor_bytes) = read_singleton_phase_leaf(io, route, &leaf).await?;
    let checked_row = decode_with_reservation(io, route, Some(64 * 1024), || {
        let ordinal = rows
            .value()
            .binary_search_by(|row| row.key.as_slice().cmp(key))
            .map_err(|_| invariant_violation("singleton selected key is absent"))?;
        let row = rows.value().get(ordinal).ok_or_else(capacity)?;
        if row.key.as_slice() != key || descriptor.value().segment.logical_sequence > sequence {
            return Err(invariant_violation(
                "singleton observation key or pinned sequence differs",
            ));
        }
        Ok(row)
    })?;
    let row = checked_row.value();
    let value = row.value.as_deref().unwrap_or_default();
    let hash = hash_with_reservation(io, route, 64 * 1024, value.len(), || {
        Ok(prefixed_sha256(value))
    })?;
    let store = io.store();
    let reservation = physical::restore_io::directory_scope_reservation(store)?
        .checked_add(MODEL_BYTES)
        .ok_or_else(capacity)?;
    let observation = decode_with_reservation(io, route, Some(reservation), || {
        let d = descriptor.value();
        let leaf = position_witness(root, p).leaf;
        let input = InputWitness {
            role: physical::Role::Kv,
            root_b64: root.into(),
            descriptor: ImmutableObjectWitness {
                path: format!(
                    "{}/physical/descriptors/{}.json",
                    store.paths.base_prefix(),
                    raw_digest(&leaf.digest)?
                ),
                byte_size: descriptor_bytes,
                sha256: leaf.digest.clone(),
            },
            directory_leaf: leaf,
            index: ImmutableObjectWitness {
                path: store.paths.segment_index(&d.segment.segment_id),
                byte_size: d.segment.index_size_bytes,
                sha256: format!("sha256:{}", d.segment.index_checksum_sha256),
            },
            block: window::block_witness(d)?,
        };
        let witness = SingletonValueWitness {
            generation: row.generation,
            tombstone: row.tombstone,
            value_length: value.len() as u64,
            value_sha256: hash.value().clone(),
            descriptor: input.descriptor.clone(),
            index: input.index.clone(),
            block: input.block.clone(),
            row_ordinal: rows
                .value()
                .binary_search_by(|row| row.key.as_slice().cmp(key))
                .map_err(|_| capacity())? as u64,
        };
        Ok((input, witness))
    })?;
    drop(checked_row);
    Ok((observation, rows, descriptor))
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "keep scoped Emit ownership and immutable products visible"
)]
async fn emit<'a, 'progress>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    plan: &WorkingValue<OwnedSelectedPlan>,
    expected: &'a WorkingValue<ExpectedPlan<'progress>>,
    selected: &'a SelectedProgress,
    source: &WorkingValue<Option<directory::restore::Position>>,
    current: &WorkingValue<Option<directory::restore::Position>>,
    key: &[u8],
    s: Option<&SingletonValueWitness>,
    c: Option<&SingletonValueWitness>,
) -> Result<publication::PreparedUnit<'a, 'progress>> {
    let fields = &plan.value().plan.fields;
    let expected_plan = expected.value();
    let progress = selected.progress.value();
    let read_source = s.is_some_and(|s| !s.tombstone) || c.is_none();
    let (position, root, sequence, prior) = if read_source {
        (
            source,
            expected_plan.source_kv_root_b64,
            fields.source_logical_sequence,
            s,
        )
    } else {
        (
            current,
            expected_plan.base_kv_root_b64,
            fields.base_logical_sequence,
            c,
        )
    };
    let (observation, mut input_rows, descriptor) =
        observe_rows(io, route, position, root, sequence, key).await?;
    drop(decode_with_reservation(
        io,
        route,
        Some(MODEL_BYTES),
        || {
            for (witness, pos) in [(s, source), (c, current)] {
                let at_key = pos.value().as_ref().is_some_and(|progress| {
                    if progress.leaf.rows == 1 {
                        progress.leaf.first == key
                    } else {
                        witness.is_some()
                    }
                });
                if witness.is_some() != at_key {
                    return Err(invariant_violation("Emit rooted membership differs"));
                }
                if let Some(w) = witness {
                    let leaf = &pos.value().as_ref().ok_or_else(capacity)?.leaf;
                    if w.descriptor.sha256 != format!("sha256:{}", hex::encode(leaf.digest))
                        || w.row_ordinal >= leaf.rows
                        || w.block.rows != leaf.rows
                        || w.block.length != u64::from(leaf.bytes)
                    {
                        return Err(invariant_violation("Emit locator differs"));
                    }
                }
            }
            if prior != Some(&observation.value().1) {
                return Err(invariant_violation("Emit reread observation differs"));
            }
            Ok(())
        },
    )?);
    let output_spec = decode_with_reservation(io, route, Some(64 * 1024), || {
        Ok(match (s.filter(|s| !s.tombstone), c, fields.mode) {
            (Some(source), current, _) => Some((
                false,
                current
                    .filter(|current| {
                        !current.tombstone
                            && current.value_length == source.value_length
                            && current.value_sha256 == source.value_sha256
                    })
                    .map_or(fields.result_logical_sequence, |current| current.generation),
            )),
            (None, Some(current), _) => Some((
                true,
                if current.tombstone {
                    current.generation
                } else {
                    fields.result_logical_sequence
                },
            )),
            (None, None, Mode::Absent) => s.map(|source| (true, source.generation)),
            (None, None, Mode::Present) => None,
        })
    })?;
    let mut products = Vec::new();
    let product = if let Some(spec) = *output_spec.value() {
        let id = codec::hashes::output_id(
            io,
            route,
            expected,
            progress.next_ordinal,
            0,
            physical::Role::Kv,
        )?;
        Some(
            physical::restore_io::write_singleton_restore_output(
                io,
                route,
                id.value(),
                fields.result_logical_sequence,
                &descriptor,
                &SingletonEmitAdmission(&selected.progress),
                usize::try_from(observation.value().1.row_ordinal).map_err(|_| capacity())?,
                &mut input_rows,
                spec,
            )
            .await?,
        )
    } else {
        None
    };
    let products = decode_with_reservation(io, route, Some(64 * 1024), || {
        products.extend(product);
        Ok(products)
    })?;
    let store = io.store();
    let receipt = decode_with_reservation(io, route, Some(MODEL_BYTES), || {
        let (input, w) = observation.value();
        let after_side = |pos: &WorkingValue<Option<directory::restore::Position>>,
                          prior: &SideCursor,
                          root: &str,
                          member: bool|
         -> Result<SideCursor> {
            Ok(match pos.value().as_ref() {
                Some(pos) if member => SideCursor::After {
                    key_b64url: URL_SAFE_NO_PAD.encode(key),
                    position: position_witness(root, pos),
                },
                None => SideCursor::End,
                Some(_) => prior.clone(),
            })
        };
        let outputs = products
            .value()
            .iter()
            .map(|o| window::output_witness(store, o, 0))
            .collect::<Result<Vec<_>>>()?;
        Ok(ControlMvpRestoreReceiptV1 {
            record_type: "control_mvp_restore_receipt".into(),
            version: 1,
            plan_sha256: expected_plan.plan_sha256.into(),
            identity: expected_plan.identity.clone(),
            owner_generation: expected_plan.owner_generation,
            ordinal: progress.next_ordinal,
            predecessor_receipt_sha256: progress
                .last_receipt
                .as_ref()
                .ok_or_else(capacity)?
                .raw_sha256
                .clone(),
            predecessor_chain_sha256: progress.chain_sha256.clone(),
            before: progress.cursor.clone(),
            after: super::MergeCursor {
                global: GlobalCut::After {
                    key_b64url: URL_SAFE_NO_PAD.encode(key),
                },
                source: after_side(
                    source,
                    &progress.cursor.source,
                    expected_plan.source_kv_root_b64,
                    s.is_some(),
                )?,
                current: after_side(
                    current,
                    &progress.cursor.current,
                    expected_plan.base_kv_root_b64,
                    c.is_some(),
                )?,
            },
            singleton_before: progress.singleton_state.clone(),
            singleton_after: SingletonState::Complete {
                key_b64url: URL_SAFE_NO_PAD.encode(key),
            },
            source_inputs: if read_source {
                vec![input.clone()]
            } else {
                vec![]
            },
            current_inputs: if read_source {
                vec![]
            } else {
                vec![input.clone()]
            },
            prefix_cumulative_counts: progress.cumulative_counts.clone(),
            counts: SemanticCounts {
                source_leaves: u64::from(read_source),
                current_leaves: u64::from(!read_source),
                input_encoded_bytes: w.block.length,
                decoded_blocks: 1,
                decoded_rows: w.block.rows,
                output_blocks: outputs.len() as u64,
                output_encoded_bytes: outputs.iter().map(|o| o.block.length).sum(),
                mutations: u64::from(
                    output_spec
                        .value()
                        .is_some_and(|(_, g)| g == fields.result_logical_sequence),
                ),
                reservation: UnitReservationV1::Singleton {
                    phase: SingletonPhase::Emit,
                    authenticated_payload_bytes: w.block.length,
                    input_byte_limit: SINGLETON_INPUT_BYTE_LIMIT,
                    segment_byte_limit: SINGLETON_SEGMENT_BYTE_LIMIT,
                    scratch_base_bytes: SINGLETON_SCRATCH_BASE_BYTES,
                    scratch_payload_multiplier: SINGLETON_SCRATCH_PAYLOAD_MULTIPLIER,
                },
            },
            outputs,
            receipt_body_sha256: format!("sha256:{}", "0".repeat(64)),
            chain_sha256: format!("sha256:{}", "0".repeat(64)),
        })
    })?;
    drop((input_rows, descriptor, observation));
    finish(io, route, expected, selected, receipt, products.value())
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "record bounded standard observations before oversized admission"
)]
pub(super) fn standard_first<'a, 'p>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    expected: &'a WorkingValue<ExpectedPlan<'p>>,
    selected: &'a SelectedProgress,
    source: &window::Side,
    current: &window::Side,
    key: &[u8],
) -> Result<publication::PreparedUnit<'a, 'p>> {
    fn hash_side<'a>(side: &'a window::Side, key: &[u8]) -> Option<&'a [u8]> {
        let input = side.candidate.as_ref()?;
        let ordinal = input
            .rows
            .value()
            .binary_search_by(|r| r.key.as_slice().cmp(key))
            .ok()?;
        Some(
            input
                .rows
                .value()
                .get(ordinal)?
                .value
                .as_deref()
                .unwrap_or_default(),
        )
    }
    let store = io.store();
    let genesis = if selected.progress.value().last_receipt.is_none() {
        Some(codec::hashes::genesis_none(io, route, &selected.progress)?)
    } else {
        None
    };
    let source_hash = hash_side(source, key)
        .map(|value| {
            hash_with_reservation(io, route, 64 * 1024, value.len(), || {
                Ok(prefixed_sha256(value))
            })
        })
        .transpose()?;
    let current_hash = hash_side(current, key)
        .map(|value| {
            hash_with_reservation(io, route, 64 * 1024, value.len(), || {
                Ok(prefixed_sha256(value))
            })
        })
        .transpose()?;
    let receipt = decode_with_reservation(io, route, Some(MODEL_BYTES), || {
        let e = expected.value();
        let p = selected.progress.value();
        let source_inputs = window::input_witnesses(store, e.source_kv_root_b64, source)?;
        let current_inputs = window::input_witnesses(store, e.base_kv_root_b64, current)?;
        let observe = |side: &window::Side,
                       inputs: &[InputWitness],
                       hash: &Option<WorkingValue<String>>|
         -> Result<Option<Box<SingletonValueWitness>>> {
            let Some(input) = &side.candidate else {
                return Ok(None);
            };
            let Ok(ordinal) = input
                .rows
                .value()
                .binary_search_by(|r| r.key.as_slice().cmp(key))
            else {
                return Ok(None);
            };
            let row = input.rows.value().get(ordinal).ok_or_else(capacity)?;
            let witness = inputs.last().ok_or_else(capacity)?;
            let value = row.value.as_deref().unwrap_or_default();
            Ok(Some(Box::new(SingletonValueWitness {
                generation: row.generation,
                tombstone: row.tombstone,
                value_length: value.len() as u64,
                value_sha256: hash.as_ref().ok_or_else(capacity)?.value().clone(),
                descriptor: witness.descriptor.clone(),
                index: witness.index.clone(),
                block: witness.block.clone(),
                row_ordinal: ordinal as u64,
            })))
        };
        let payload = source_inputs
            .iter()
            .chain(&current_inputs)
            .map(|i| i.block.length)
            .max()
            .ok_or_else(capacity)?;
        let counts = SemanticCounts {
            source_leaves: source_inputs.len() as u64,
            current_leaves: current_inputs.len() as u64,
            input_encoded_bytes: source_inputs
                .iter()
                .chain(&current_inputs)
                .map(|i| i.block.length)
                .sum(),
            decoded_blocks: (source_inputs.len() + current_inputs.len()) as u64,
            decoded_rows: source_inputs
                .iter()
                .chain(&current_inputs)
                .map(|i| i.block.rows)
                .sum(),
            output_blocks: 0,
            output_encoded_bytes: 0,
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
        Ok(ControlMvpRestoreReceiptV1 {
            record_type: "control_mvp_restore_receipt".into(),
            version: 1,
            plan_sha256: e.plan_sha256.into(),
            identity: e.identity.clone(),
            owner_generation: e.owner_generation,
            ordinal: p.next_ordinal,
            predecessor_receipt_sha256: p
                .last_receipt
                .as_ref()
                .map(|r| r.raw_sha256.as_str())
                .or_else(|| genesis.as_ref().map(|g| g.value().as_str()))
                .ok_or_else(capacity)?
                .into(),
            predecessor_chain_sha256: p.chain_sha256.clone(),
            before: p.cursor.clone(),
            after: p.cursor.clone(),
            singleton_before: p.singleton_state.clone(),
            singleton_after: SingletonState::Pending {
                key_b64url: URL_SAFE_NO_PAD.encode(key),
                source: observe(source, &source_inputs, &source_hash)?,
                current: observe(current, &current_inputs, &current_hash)?,
                phase: SingletonPhase::CompareSource,
            },
            source_inputs,
            current_inputs,
            outputs: vec![],
            prefix_cumulative_counts: p.cumulative_counts.clone(),
            counts,
            receipt_body_sha256: format!("sha256:{}", "0".repeat(64)),
            chain_sha256: format!("sha256:{}", "0".repeat(64)),
        })
    })?;
    finish(io, route, expected, selected, receipt, &[])
}

fn finish<'a, 'p>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    expected: &'a WorkingValue<ExpectedPlan<'p>>,
    selected: &'a SelectedProgress,
    receipt: WorkingValue<ControlMvpRestoreReceiptV1>,
    products: &[physical::restore_io::StandardRestoreOutput],
) -> Result<publication::PreparedUnit<'a, 'p>> {
    let receipt = window::finish_receipt(io, route, receipt)?;
    let encoded = codec::encode_receipt(io, route, &receipt)?;
    let raw = codec::hashes::raw_encoded(io, route, &encoded)?;
    let progress = decode_with_reservation(io, route, Some(MODEL_BYTES), || {
        let r = receipt.value();
        let e = expected.value();
        Ok(RestoreProgressV1 {
            record_type: "control_mvp_restore_progress".into(),
            version: 1,
            plan_sha256: e.plan_sha256.into(),
            identity: e.identity.clone(),
            owner_generation: e.owner_generation,
            next_ordinal: r.ordinal + 1,
            receipt_count: r.ordinal + 1,
            last_receipt: Some(ReceiptRef {
                path: e.receipt_path(r.ordinal),
                raw_sha256: raw.value().clone(),
            }),
            chain_sha256: r.chain_sha256.clone(),
            cursor: r.after.clone(),
            singleton_state: r.singleton_after.clone(),
            terminal: false,
            cumulative_counts: checked_sum(&r.prefix_cumulative_counts, &r.counts)?,
        })
    })?;
    publication::assemble(io, route, expected, selected, &receipt, &progress, products)
}
