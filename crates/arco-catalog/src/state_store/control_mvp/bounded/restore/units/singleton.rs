//! Durable comparison observations only; no output or historical-prefix authority.
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
    hash_with_reservation, read_restore_descriptor_sized, read_singleton_leaf,
};

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
                | SingletonState::Pending {
                    phase: SingletonPhase::CompareSource | SingletonPhase::CompareCurrent,
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
        let key = source
            .value()
            .as_ref()
            .into_iter()
            .chain(current.value().as_ref())
            .map(|progress| progress.leaf.first.as_slice())
            .min()
            .ok_or_else(capacity)?;
        if pending_key(&progress.singleton_state)?.is_some_and(|k| k != key) {
            return Err(invariant_violation(
                "pending singleton skips the next rooted key",
            ));
        }
        Ok(key.to_vec())
    })?;
    let at_key = |position: &Option<directory::restore::Position>| {
        position
            .as_ref()
            .is_some_and(|v| v.leaf.first == *key.value())
    };
    let has_source = at_key(source.value());
    let has_current = at_key(current.value());
    let read_source = match &progress.singleton_state {
        SingletonState::Pending {
            phase: SingletonPhase::CompareSource,
            ..
        } => !has_current,
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
    let observation = observe(io, route, position, root, sequence).await?;
    let genesis = if progress.last_receipt.is_none() {
        Some(codec::hashes::genesis_none(io, route, &selected.progress)?)
    } else {
        None
    };
    let receipt = decode_with_reservation(io, route, Some(reservation), || {
        let (input, witness) = observation.value();
        let (mut source_witness, mut current_witness, phase, charged_phase) =
            match &progress.singleton_state {
                SingletonState::None => (
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
                _ => return Err(capacity()),
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
                if leaf.first != *key.value()
                    || prior.descriptor.sha256 != format!("sha256:{}", hex::encode(leaf.digest))
                    || prior.block.length != u64::from(leaf.bytes)
                    || prior.block.rows != 1
                    || prior.row_ordinal != 0
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
                decoded_rows: 1,
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
    let receipt = window::finish_receipt(io, route, receipt)?;
    let encoded = codec::encode_receipt(io, route, &receipt)?;
    let raw = codec::hashes::raw_encoded(io, route, &encoded)?;
    let progress = decode_with_reservation(io, route, Some(reservation), || {
        let r = receipt.value();
        Ok(RestoreProgressV1 {
            record_type: "control_mvp_restore_progress".into(),
            version: 1,
            plan_sha256: expected_plan.plan_sha256.into(),
            identity: expected_plan.identity.clone(),
            owner_generation: expected_plan.owner_generation,
            next_ordinal: r.ordinal + 1,
            receipt_count: r.ordinal + 1,
            last_receipt: Some(ReceiptRef {
                path: expected_plan.receipt_path(r.ordinal),
                raw_sha256: raw.value().clone(),
            }),
            chain_sha256: r.chain_sha256.clone(),
            cursor: r.after.clone(),
            singleton_state: r.singleton_after.clone(),
            terminal: false,
            cumulative_counts: checked_sum(&r.prefix_cumulative_counts, &r.counts)?,
        })
    })?;
    drop((encoded, raw));
    publication::assemble(io, route, expected, selected, &receipt, &progress, &[])
}

fn position_witness(root: &str, p: &directory::restore::Position) -> DirectoryPosition {
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
                    || Some(b.leaf.first.as_slice()) != key.value().as_deref()
                {
                    return Err(invariant_violation("singleton cursor path or key differs"));
                }
                Ok(())
            },
        )?);
    }
    let position = directory::restore::first_after(
        io,
        route,
        root,
        if matches!(cursor, SideCursor::End) {
            global
        } else {
            key.value().as_deref()
        },
    )
    .await?;
    authenticate_boundary(io, route, &position, sequence).await?;
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        if let Some(p) = position.value() {
            if matches!(cursor, SideCursor::End)
                || global.is_some_and(|g| p.leaf.first.as_slice() <= g)
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
            if p.leaf.rows != 1
                || p.leaf.first != p.leaf.last
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
) -> Result<WorkingValue<(InputWitness, SingletonValueWitness)>> {
    let p = position.value().as_ref().ok_or_else(capacity)?;
    let leaf = decode_with_reservation(io, route, Some(MODEL_BYTES), || Ok(p.leaf.clone()))?;
    let (descriptor, rows, descriptor_bytes) = read_singleton_leaf(io, route, &leaf).await?;
    let checked_row = decode_with_reservation(io, route, Some(64 * 1024), || {
        let [row] = rows.value().as_slice() else {
            return Err(invariant_violation(
                "singleton observation row count differs",
            ));
        };
        if row.key != p.leaf.first || descriptor.value().segment.logical_sequence > sequence {
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
    decode_with_reservation(io, route, Some(reservation), || {
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
            row_ordinal: 0,
        };
        Ok((input, witness))
    })
}
