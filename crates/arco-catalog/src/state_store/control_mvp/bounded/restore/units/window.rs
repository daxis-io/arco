//! One proved standard interval; no native driver or historical-prefix authority.
use super::super::{Mode, directory};
use super::{
    BlockWitness, CatalogError, ControlMvpRestoreReceiptV1, ControlMvpSegmentRow,
    DirectoryPathEntry, DirectoryPosition, DirectoryPositionLeafWitness, ExpectedPlan, GlobalCut,
    ImmutableObjectWitness, InputWitness, MAX_BLOCK_BYTES, MergeCursor, OutputDirectoryLeafWitness,
    OutputWitness, OwnedSelectedPlan, ReceiptRef, RestorePhysicalIo, RestorePhysicalRoute,
    RestoreProgressV1, Result, SelectedProgress, SemanticCounts, SideCursor, SingletonState,
    UnitReservationV1, WorkingValue, binary, checked_sum, codec, decode_with_reservation,
    invariant_violation, merge_standard_kv_rows, physical, publication,
    standard_kv_merge_reservation,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use physical::restore_io::{
    StandardRestoreOutput, preflight_standard_restore_output, read_restore_leaf_sized,
    standard_output_reservation, write_standard_restore_output,
};

// Four input descriptors, before/after cursors and <=32 outputs. Payload keys
// total <=1 MiB; repeated base64 endpoints and bounded old cursor fit this bound.
const MODEL_BYTES: usize = 16 * 1024 * 1024;

struct Input {
    position: WorkingValue<Option<directory::restore::Position>>,
    descriptor: WorkingValue<physical::Descriptor>,
    rows: WorkingValue<Vec<ControlMvpSegmentRow>>,
    descriptor_bytes: u64,
}
struct Side {
    boundary: Option<Input>,
    candidate: Option<Input>,
}
impl Side {
    fn inputs(&self) -> impl Iterator<Item = &Input> {
        self.boundary.iter().chain(self.candidate.iter())
    }
    fn rows(&self) -> &[ControlMvpSegmentRow] {
        self.candidate.as_ref().map_or(&[], |v| v.rows.value())
    }
    fn sequence(&self) -> u64 {
        self.candidate
            .as_ref()
            .map_or(0, |v| v.descriptor.value().segment.logical_sequence)
    }
}
fn capacity() -> CatalogError {
    CatalogError::MaintenanceBackpressure {
        message: "standard restore window exceeds admission".into(),
    }
}

#[allow(
    clippy::large_futures,
    reason = "fixed 32 product slots stay on stack without an unaccounted heap box"
)]
pub(super) async fn prepare<'a, 'p>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    plan: &WorkingValue<OwnedSelectedPlan>,
    expected: &'a WorkingValue<ExpectedPlan<'p>>,
    selected: &'a SelectedProgress,
) -> Result<publication::PreparedUnit<'a, 'p>> {
    let result = prepare_inner(io, route, plan, expected, selected).await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

#[allow(
    clippy::too_many_lines,
    reason = "keep admitted input and output lifetimes visible through unit assembly"
)]
async fn prepare_inner<'a, 'p>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    plan: &WorkingValue<OwnedSelectedPlan>,
    expected: &'a WorkingValue<ExpectedPlan<'p>>,
    selected: &'a SelectedProgress,
) -> Result<publication::PreparedUnit<'a, 'p>> {
    let owned = plan.is_owned_by(io)
        && expected.is_owned_by(io)
        && selected.progress.is_owned_by(io)
        && selected.selector_raw.is_owned_by(io)
        && selected.selector_meta.is_owned_by(io);
    let ordinary = matches!(route, RestorePhysicalRoute::OrdinaryUnit { .. });
    let store = io.store();
    let root_reservation = physical::restore_io::directory_scope_reservation(store)?
        .checked_add(MODEL_BYTES)
        .ok_or_else(capacity)?;
    let roots = decode_with_reservation(io, route, Some(root_reservation), || {
        let f = &plan.value().plan.fields;
        let e = expected.value();
        let p = selected.progress.value();
        if !ordinary {
            return Err(invariant_violation(
                "standard restore windows require the control ledger",
            ));
        }
        if !owned
            || plan.value().plan_sha256 != e.plan_sha256
            || f.candidate_id != e.candidate_id
            || f.identity != *e.identity
            || f.owner_generation != e.owner_generation
            || f.candidate_seed_sha256 != e.candidate_seed_sha256
            || f.source_kv_root_b64 != e.source_kv_root_b64
            || f.base_kv_root_b64 != e.base_kv_root_b64
            || p.plan_sha256 != e.plan_sha256
            || p.identity != *e.identity
            || p.owner_generation != e.owner_generation
            || p.terminal
            || p.next_ordinal == u64::MAX
            || p.singleton_state != SingletonState::None
            || f.base_logical_sequence.checked_add(1) != Some(f.result_logical_sequence)
            || (f.mode == Mode::Absent && f.base_logical_sequence != f.source_logical_sequence)
        {
            return Err(invariant_violation(
                "standard window plan, owner, or selected state differs",
            ));
        }
        plan.value().plan.validate_roots(store)?;
        let after = match &p.cursor.global {
            GlobalCut::Start => None,
            GlobalCut::After { key_b64url } if key_b64url.len() <= 4 * MAX_BLOCK_BYTES / 3 + 4 => {
                Some(binary(key_b64url)?)
            }
            _ => return Err(capacity()),
        };
        let d = directory::Directory::new(store.retention.clone(), &store.scope)?;
        Ok((
            d.decode_root(&binary(e.source_kv_root_b64)?)?,
            d.decode_root(&binary(e.base_kv_root_b64)?)?,
            after,
        ))
    })?;
    let f = &plan.value().plan.fields;
    let before = &selected.progress.value().cursor;
    let source = read_side(
        io,
        route,
        &roots.value().0,
        expected.value().source_kv_root_b64,
        &before.source,
        roots.value().2.as_deref(),
    )
    .await?;
    let current = if f.mode == Mode::Absent {
        drop(decode_with_reservation(io, route, Some(64 * 1024), || {
            if !matches!(before.current, SideCursor::Start | SideCursor::End) {
                return Err(invariant_violation(
                    "absent restore current cursor is not empty",
                ));
            }
            Ok(())
        })?);
        Side {
            boundary: None,
            candidate: None,
        }
    } else {
        read_side(
            io,
            route,
            &roots.value().1,
            expected.value().base_kv_root_b64,
            &before.current,
            roots.value().2.as_deref(),
        )
        .await?
    };
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        for (side, bound) in [
            (&source, f.source_logical_sequence),
            (&current, f.base_logical_sequence),
        ] {
            if side.inputs().any(|input| {
                input.descriptor.value().segment.logical_sequence == 0
                    || input.descriptor.value().segment.logical_sequence > bound
            }) {
                return Err(invariant_violation(
                    "window evidence block exceeds pinned manifest sequence",
                ));
            }
        }
        Ok(())
    })?);
    let cut = source
        .rows()
        .last()
        .map(|r| r.key.as_slice())
        .into_iter()
        .chain(current.rows().last().map(|r| r.key.as_slice()))
        .min();
    let source_rows = fragment(source.rows(), roots.value().2.as_deref(), cut)?;
    let current_rows = fragment(current.rows(), roots.value().2.as_deref(), cut)?;
    let reservation = standard_kv_merge_reservation(source_rows, current_rows).unwrap_or(64 * 1024);
    let rows = decode_with_reservation(io, route, Some(reservation), || {
        let rows = merge_standard_kv_rows(
            source_rows,
            current_rows,
            f.mode,
            f.source_logical_sequence,
            f.base_logical_sequence,
            0,
            (source.sequence(), current.sequence()),
        )?;
        verify_merge(
            source_rows,
            current_rows,
            &rows,
            f.mode,
            f.result_logical_sequence,
        )?;
        Ok(rows)
    })?;
    let partitions =
        decode_with_reservation(io, route, Some(64 * 1024), || partition(rows.value()))?;
    let genesis = if selected.progress.value().last_receipt.is_none() {
        Some(codec::hashes::genesis_none(io, route, &selected.progress)?)
    } else {
        None
    };
    let model_reservation = decode_with_reservation(io, route, Some(64 * 1024), || {
        // Path constructors allocate their base prefix and final path; cloning
        // the completed model retains another copy. Scope size is not fixed.
        expected
            .value()
            .prefix
            .len()
            .checked_mul(16)
            .and_then(|bytes| {
                bytes.checked_mul(
                    source.inputs().count()
                        + current.inputs().count()
                        + partitions.value().len()
                        + 1,
                )
            })
            .and_then(|bytes| bytes.checked_add(MODEL_BYTES))
            .ok_or_else(capacity)
    })?;
    let receipt = decode_with_reservation(io, route, Some(*model_reservation.value()), || {
        let e = expected.value();
        let p = selected.progress.value();
        let source_inputs = input_witnesses(store, e.source_kv_root_b64, &source)?;
        let current_inputs = input_witnesses(store, e.base_kv_root_b64, &current)?;
        let outputs = projected_outputs(store, rows.value(), partitions.value())?;
        let mut counts = counts(
            &source_inputs,
            &current_inputs,
            &[],
            rows.value(),
            f.result_logical_sequence,
        );
        counts.output_blocks = outputs.len() as u64;
        counts.output_encoded_bytes = u64::MAX;
        for input in &source_inputs {
            super::validate_input(e.source_kv_root_b64, input)?;
        }
        for input in &current_inputs {
            super::validate_input(e.base_kv_root_b64, input)?;
        }
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
                .or_else(|| genesis.as_ref().map(|v| v.value().as_str()))
                .ok_or_else(|| invariant_violation("missing predecessor"))?
                .into(),
            predecessor_chain_sha256: p.chain_sha256.clone(),
            before: before.clone(),
            after: MergeCursor {
                global: cut.map_or(GlobalCut::End, |key| GlobalCut::After {
                    key_b64url: URL_SAFE_NO_PAD.encode(key),
                }),
                source: side_after(&source, source_rows, &before.source, e.source_kv_root_b64)?,
                current: side_after(&current, current_rows, &before.current, e.base_kv_root_b64)?,
            },
            singleton_before: SingletonState::None,
            singleton_after: SingletonState::None,
            source_inputs,
            current_inputs,
            outputs,
            prefix_cumulative_counts: p.cumulative_counts.clone(),
            counts,
            receipt_body_sha256: format!("sha256:{}", "0".repeat(64)),
            chain_sha256: format!("sha256:{}", "0".repeat(64)),
        })
    })?;
    drop(decode_with_reservation(
        io,
        route,
        Some(MODEL_BYTES),
        || {
            super::validate_cursor_transition(
                expected.value(),
                &receipt.value().before,
                &receipt.value().after,
                &SingletonState::None,
                &SingletonState::None,
            )
        },
    )?);
    drop(codec::encode_receipt(io, route, &receipt)?);
    for (part, &(start, end)) in partitions.value().iter().enumerate() {
        let fragment = rows.value().get(start..end).ok_or_else(capacity)?;
        let reservation = standard_kv_merge_reservation(fragment, &[]).ok_or_else(capacity)?;
        let copy = decode_with_reservation(io, route, Some(reservation), || Ok(fragment.to_vec()))?;
        let id = codec::hashes::output_id(
            io,
            route,
            expected,
            selected.progress.value().next_ordinal,
            u32::try_from(part).map_err(|_| capacity())?,
            physical::Role::Kv,
        )?;
        preflight_standard_restore_output(io, route, id.value(), f.result_logical_sequence, &copy)?;
    }
    let mut products: [Option<StandardRestoreOutput>; 32] = std::array::from_fn(|_| None);
    for (part, &(start, end)) in partitions.value().iter().enumerate() {
        let fragment = rows.value().get(start..end).ok_or_else(capacity)?;
        let reservation = standard_kv_merge_reservation(fragment, &[]).ok_or_else(capacity)?;
        let copy = decode_with_reservation(io, route, Some(reservation), || Ok(fragment.to_vec()))?;
        let id = codec::hashes::output_id(
            io,
            route,
            expected,
            selected.progress.value().next_ordinal,
            u32::try_from(part).map_err(|_| capacity())?,
            physical::Role::Kv,
        )?;
        *products.get_mut(part).ok_or_else(capacity)? = Some(
            write_standard_restore_output(io, route, id.value(), f.result_logical_sequence, &copy)
                .await?,
        );
    }
    let products = decode_with_reservation(io, route, Some(64 * 1024), || {
        Ok(products.into_iter().flatten().collect::<Vec<_>>())
    })?;
    let actual = decode_with_reservation(io, route, Some(*model_reservation.value()), || {
        let mut actual = receipt.value().clone();
        actual.outputs = products
            .value()
            .iter()
            .enumerate()
            .map(|(part, output)| {
                output_witness(store, output, u32::try_from(part).map_err(|_| capacity())?)
            })
            .collect::<Result<Vec<_>>>()?;
        actual.counts.output_encoded_bytes = actual.outputs.iter().map(|o| o.block.length).sum();
        Ok(actual)
    })?;
    drop(receipt);
    let receipt = actual;
    drop((source, current, rows, roots));
    let receipt = finish_receipt(io, route, receipt)?;
    let encoded = codec::encode_receipt(io, route, &receipt)?;
    let raw = codec::hashes::raw_encoded(io, route, &encoded)?;
    let progress = decode_with_reservation(io, route, Some(MODEL_BYTES), || {
        let e = expected.value();
        let r = receipt.value();
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
            singleton_state: SingletonState::None,
            terminal: matches!(r.after.global, GlobalCut::End),
            cumulative_counts: checked_sum(&r.prefix_cumulative_counts, &r.counts)?,
        })
    })?;
    drop((encoded, raw, genesis));
    publication::assemble(
        io,
        route,
        expected,
        selected,
        &receipt,
        &progress,
        products.value(),
    )
}

async fn load(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    position: WorkingValue<Option<directory::restore::Position>>,
) -> Result<Option<Input>> {
    let Some(p) = position.value() else {
        return Ok(None);
    };
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        if p.leaf.bytes as usize > MAX_BLOCK_BYTES {
            return Err(capacity());
        }
        Ok(())
    })?);
    let (descriptor, rows, descriptor_bytes) =
        read_restore_leaf_sized(io, route, physical::Role::Kv, &p.leaf).await?;
    Ok(Some(Input {
        position,
        descriptor,
        rows,
        descriptor_bytes,
    }))
}

async fn read_side(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    root: &directory::Root,
    root_b64: &str,
    cursor: &SideCursor,
    global: Option<&[u8]>,
) -> Result<Side> {
    let key = decode_with_reservation(io, route, Some(MODEL_BYTES), || {
        super::validate_side_cursor(root_b64, cursor)?;
        match cursor {
            SideCursor::After { key_b64url, .. } => {
                if key_b64url.len() > 4 * MAX_BLOCK_BYTES / 3 + 4 {
                    return Err(capacity());
                }
                let key = binary(key_b64url)?;
                if global.is_none_or(|g| key.as_slice() > g) {
                    return Err(invariant_violation("side cursor is beyond global cut"));
                }
                Ok(Some(key))
            }
            _ => Ok(None),
        }
    })?;
    let position = if let Some(key) = key.value() {
        directory::restore::first_at_or_after(io, route, root, key).await?
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
    let mut boundary = None;
    let mut candidate = load(io, route, position).await?;
    if let SideCursor::After { position, .. } = cursor {
        drop(decode_with_reservation(
            io,
            route,
            Some(MODEL_BYTES),
            || {
                let input = candidate
                    .as_ref()
                    .ok_or_else(|| invariant_violation("saved cursor leaf missing"))?;
                if position_witness(root_b64, input)? != *position
                    || input
                        .rows
                        .value()
                        .binary_search_by(|row| {
                            row.key
                                .as_slice()
                                .cmp(key.value().as_deref().unwrap_or_default())
                        })
                        .is_err()
                {
                    return Err(invariant_violation(
                        "saved cursor path or key is not authenticated",
                    ));
                }
                Ok(())
            },
        )?);
        if candidate
            .as_ref()
            .and_then(|v| v.rows.value().last())
            .map(|r| r.key.as_slice())
            == key.value().as_deref()
        {
            boundary = candidate.take();
            let next =
                directory::restore::first_after(io, route, root, key.value().as_deref()).await?;
            candidate = load(io, route, next).await?;
        }
    }
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        if matches!(cursor, SideCursor::End) && candidate.is_some() {
            return Err(invariant_violation("end cursor omits remaining input"));
        }
        if let Some(input) = &candidate {
            if input.rows.value().iter().any(|r| {
                key.value().as_deref().is_none_or(|k| r.key.as_slice() > k)
                    && global.is_some_and(|g| r.key.as_slice() <= g)
            }) {
                return Err(invariant_violation(
                    "side cursor omits input before global cut",
                ));
            }
        }
        Ok(())
    })?);
    Ok(Side {
        boundary,
        candidate,
    })
}

fn fragment<'a>(
    rows: &'a [ControlMvpSegmentRow],
    before: Option<&[u8]>,
    cut: Option<&[u8]>,
) -> Result<&'a [ControlMvpSegmentRow]> {
    let start = rows.partition_point(|r| before.is_some_and(|key| r.key.as_slice() <= key));
    let end = rows.partition_point(|r| cut.is_some_and(|key| r.key.as_slice() <= key));
    rows.get(start..end)
        .ok_or_else(|| invariant_violation("window fragment cut regressed"))
}

#[allow(
    clippy::option_if_let_else,
    reason = "keep the independent pointwise semantic cases explicit"
)]
fn verify_merge(
    source: &[ControlMvpSegmentRow],
    current: &[ControlMvpSegmentRow],
    output: &[ControlMvpSegmentRow],
    mode: Mode,
    sequence: u64,
) -> Result<()> {
    let lookup = |rows: &[ControlMvpSegmentRow], key: &[u8]| {
        rows.binary_search_by(|r| r.key.as_slice().cmp(key)).ok()
    };
    let mut count = 0;
    for key in source.iter().map(|r| &r.key).chain(
        current
            .iter()
            .filter(|r| lookup(source, &r.key).is_none())
            .map(|r| &r.key),
    ) {
        let s = lookup(source, key).and_then(|i| source.get(i));
        let c = lookup(current, key).and_then(|i| current.get(i));
        let expected = if let Some(s) = s.filter(|s| !s.tombstone) {
            Some((
                s.value.as_ref(),
                c.filter(|c| c.value == s.value)
                    .map_or(sequence, |c| c.generation),
            ))
        } else if let Some(c) = c {
            Some((None, if c.tombstone { c.generation } else { sequence }))
        } else if mode == Mode::Absent {
            s.map(|s| (None, s.generation))
        } else {
            None
        };
        let actual = lookup(output, key).and_then(|i| output.get(i));
        if let Some((value, generation)) = expected {
            count += 1;
            if actual.is_none_or(|r| {
                r.value.as_ref() != value
                    || r.generation != generation
                    || r.tombstone != value.is_none()
            }) {
                return Err(invariant_violation("independent merge value check failed"));
            }
        } else if actual.is_some() {
            return Err(invariant_violation(
                "independent merge emitted source-only tombstone",
            ));
        }
    }
    if output.len() != count
        || output.iter().enumerate().any(|(i, r)| {
            r.logical_sequence != sequence
                || r.logical_ordinal != i as u64
                || r.origin_sequence.is_some()
                || r.record_kind != super::SEGMENT_RECORD_KV
        })
        || output
            .windows(2)
            .any(|p| matches!(p, [left, right] if left.key >= right.key))
    {
        return Err(invariant_violation(
            "independent merge coverage check failed",
        ));
    }
    Ok(())
}

fn partition(rows: &[ControlMvpSegmentRow]) -> Result<Vec<(usize, usize)>> {
    let mut result = Vec::with_capacity(32);
    let mut start = 0;
    while start < rows.len() {
        standard_output_reservation(rows.get(start..=start).ok_or_else(capacity)?)?;
        let mut low = start + 1;
        let mut high = rows.len();
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            if standard_output_reservation(rows.get(start..mid).ok_or_else(capacity)?).is_ok() {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        if result.len() == 32 {
            return Err(capacity());
        }
        result.push((start, low));
        start = low;
    }
    Ok(result)
}

fn leaf_witness(input: &Input) -> Result<DirectoryPositionLeafWitness> {
    let p = input
        .position
        .value()
        .as_ref()
        .ok_or_else(|| invariant_violation("input position missing"))?;
    Ok(DirectoryPositionLeafWitness {
        first_b64url: URL_SAFE_NO_PAD.encode(&p.leaf.first),
        last_b64url: URL_SAFE_NO_PAD.encode(&p.leaf.last),
        rows: p.leaf.rows,
        bytes: p.leaf.bytes,
        digest: format!("sha256:{}", hex::encode(p.leaf.digest)),
    })
}
fn position_witness(root: &str, input: &Input) -> Result<DirectoryPosition> {
    let p = input
        .position
        .value()
        .as_ref()
        .ok_or_else(|| invariant_violation("input position missing"))?;
    Ok(DirectoryPosition {
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
        leaf: leaf_witness(input)?,
    })
}
fn side_after(
    side: &Side,
    rows: &[ControlMvpSegmentRow],
    prior: &SideCursor,
    root: &str,
) -> Result<SideCursor> {
    let Some(input) = &side.candidate else {
        return Ok(SideCursor::End);
    };
    Ok(if let Some(row) = rows.last() {
        SideCursor::After {
            key_b64url: URL_SAFE_NO_PAD.encode(&row.key),
            position: position_witness(root, input)?,
        }
    } else {
        prior.clone()
    })
}
pub(super) fn block_witness(d: &physical::Descriptor) -> Result<BlockWitness> {
    let (first, last) = crate::state_store::control_mvp::block_key_bounds(&d.block)?
        .ok_or_else(|| invariant_violation("KV block endpoints missing"))?;
    Ok(BlockWitness {
        offset: d.block.offset,
        length: d.block.length,
        sha256: format!("sha256:{}", d.block.checksum_sha256),
        rows: d.block.row_count,
        min_key_b64url: URL_SAFE_NO_PAD.encode(first),
        max_key_b64url: URL_SAFE_NO_PAD.encode(last),
    })
}
fn input_witnesses(
    store: &super::super::super::super::ControlMvpStateStore,
    root: &str,
    side: &Side,
) -> Result<Vec<InputWitness>> {
    side.inputs()
        .map(|i| {
            let d = i.descriptor.value();
            let leaf = leaf_witness(i)?;
            Ok(InputWitness {
                role: physical::Role::Kv,
                root_b64: root.into(),
                descriptor: ImmutableObjectWitness {
                    path: format!(
                        "{}/physical/descriptors/{}.json",
                        store.paths.base_prefix(),
                        super::raw_digest(&leaf.digest)?
                    ),
                    byte_size: i.descriptor_bytes,
                    sha256: leaf.digest.clone(),
                },
                directory_leaf: leaf,
                index: ImmutableObjectWitness {
                    path: store.paths.segment_index(&d.segment.segment_id),
                    byte_size: d.segment.index_size_bytes,
                    sha256: format!("sha256:{}", d.segment.index_checksum_sha256),
                },
                block: block_witness(d)?,
            })
        })
        .collect()
}
fn output_witness(
    store: &super::super::super::super::ControlMvpStateStore,
    output: &StandardRestoreOutput,
    part: u32,
) -> Result<OutputWitness> {
    let d = output.descriptor.value();
    let block = block_witness(d)?;
    let digest = format!("sha256:{}", hex::encode(output.digest));
    Ok(OutputWitness {
        role: physical::Role::Kv,
        output_id: d.segment.segment_id.clone(),
        part,
        directory_leaf: OutputDirectoryLeafWitness {
            first_key_b64url: block.min_key_b64url.clone(),
            last_key_b64url: block.max_key_b64url.clone(),
            rows: block.rows,
            bytes: u32::try_from(block.length).map_err(|_| capacity())?,
            digest: digest.clone(),
        },
        descriptor: ImmutableObjectWitness {
            path: format!(
                "{}/physical/descriptors/{}.json",
                store.paths.base_prefix(),
                hex::encode(output.digest)
            ),
            byte_size: output.bytes.value().len() as u64,
            sha256: digest,
        },
        index: ImmutableObjectWitness {
            path: store.paths.segment_index(&d.segment.segment_id),
            byte_size: d.segment.index_size_bytes,
            sha256: format!("sha256:{}", d.segment.index_checksum_sha256),
        },
        block,
    })
}
// Endpoint/path strings have exactly the eventual widths; maximal decimal fields
// dominate every actual writer product. This model is encoded but never stored.
fn projected_outputs(
    store: &super::super::super::super::ControlMvpStateStore,
    rows: &[ControlMvpSegmentRow],
    parts: &[(usize, usize)],
) -> Result<Vec<OutputWitness>> {
    parts
        .iter()
        .enumerate()
        .map(|(part, &(start, end))| {
            let id = "0".repeat(64);
            let digest = format!("sha256:{id}");
            let first = URL_SAFE_NO_PAD.encode(&rows.get(start).ok_or_else(capacity)?.key);
            let last = URL_SAFE_NO_PAD.encode(
                &rows
                    .get(end.checked_sub(1).ok_or_else(capacity)?)
                    .ok_or_else(capacity)?
                    .key,
            );
            Ok(OutputWitness {
                role: physical::Role::Kv,
                output_id: id.clone(),
                part: u32::try_from(part).map_err(|_| capacity())?,
                directory_leaf: OutputDirectoryLeafWitness {
                    first_key_b64url: first.clone(),
                    last_key_b64url: last.clone(),
                    rows: u64::MAX,
                    bytes: u32::MAX,
                    digest: digest.clone(),
                },
                descriptor: ImmutableObjectWitness {
                    path: format!(
                        "{}/physical/descriptors/{id}.json",
                        store.paths.base_prefix()
                    ),
                    byte_size: u64::MAX,
                    sha256: digest.clone(),
                },
                index: ImmutableObjectWitness {
                    path: store.paths.segment_index(&id),
                    byte_size: u64::MAX,
                    sha256: digest.clone(),
                },
                block: BlockWitness {
                    offset: u64::MAX,
                    length: u64::MAX,
                    sha256: digest,
                    rows: u64::MAX,
                    min_key_b64url: first,
                    max_key_b64url: last,
                },
            })
        })
        .collect()
}

fn counts(
    source: &[InputWitness],
    current: &[InputWitness],
    outputs: &[OutputWitness],
    rows: &[ControlMvpSegmentRow],
    sequence: u64,
) -> SemanticCounts {
    SemanticCounts {
        source_leaves: source.len() as u64,
        current_leaves: current.len() as u64,
        input_encoded_bytes: source.iter().chain(current).map(|i| i.block.length).sum(),
        decoded_blocks: (source.len() + current.len()) as u64,
        decoded_rows: source.iter().chain(current).map(|i| i.block.rows).sum(),
        output_blocks: outputs.len() as u64,
        output_encoded_bytes: outputs.iter().map(|o| o.block.length).sum(),
        mutations: rows.iter().filter(|r| r.generation == sequence).count() as u64,
        reservation: UnitReservationV1::Standard {
            combined_leaf_limit: super::STANDARD_COMBINED_LEAF_LIMIT,
            decode_limit: super::STANDARD_DECODE_LIMIT,
            input_byte_limit: super::STANDARD_INPUT_BYTE_LIMIT,
            output_block_limit: super::STANDARD_OUTPUT_BLOCK_LIMIT,
            packed_output_byte_limit: super::STANDARD_PACKED_OUTPUT_BYTE_LIMIT,
        },
    }
}
pub(super) fn finish_receipt(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    receipt: WorkingValue<ControlMvpRestoreReceiptV1>,
) -> Result<WorkingValue<ControlMvpRestoreReceiptV1>> {
    let body = codec::hashes::receipt_body(io, route, &receipt)?;
    let next = decode_with_reservation(io, route, Some(MODEL_BYTES), || {
        let mut r = receipt.value().clone();
        r.receipt_body_sha256.clone_from(body.value());
        Ok(r)
    })?;
    drop((receipt, body));
    let chain = codec::hashes::receipt_chain(io, route, &next)?;
    decode_with_reservation(io, route, Some(MODEL_BYTES), || {
        let mut r = next.value().clone();
        r.chain_sha256.clone_from(chain.value());
        Ok(r)
    })
}

#[cfg(test)]
mod tests;
