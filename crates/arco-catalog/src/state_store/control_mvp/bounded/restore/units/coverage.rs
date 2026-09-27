//! Independent standard physical intervals; no whole-history or publication authority.
use super::super::{Mode, directory};
use super::{
    BlockWitness, ControlMvpRestoreReceiptV1, ControlMvpSegmentRow, DirectoryPosition,
    DirectoryPositionLeafWitness, ExpectedPlan, GlobalCut, ImmutableObjectWitness, InputWitness,
    MAX_BLOCK_BYTES, OwnedSelectedPlan, RestorePhysicalIo, RestorePhysicalRoute, Result,
    SEGMENT_RECORD_KV, SideCursor, SingletonState, UnitReservationV1, WorkingValue, binary, codec,
    decode_with_reservation, invariant_violation, physical, raw_digest, validate_side_cursor,
};
use physical::restore_io::{FinalDescriptor, FinalMicrochunk, FinalStreamTotals};

const BINDING_BYTES: usize = 8 * 1024 * 1024;

struct Input {
    position: WorkingValue<Option<directory::restore::Position>>,
    descriptor: FinalDescriptor,
    rows: WorkingValue<Vec<ControlMvpSegmentRow>>,
}
struct Side {
    boundary: Option<Input>,
    candidate: Option<Input>,
}
impl Side {
    fn inputs(&self) -> impl Iterator<Item = &Input> {
        self.boundary
            .iter()
            .chain(self.candidate.iter())
            .filter(|i| !i.rows.value().is_empty())
    }
    fn rows(&self) -> &[ControlMvpSegmentRow] {
        self.candidate
            .as_ref()
            .map_or(&[], |input| input.rows.value())
    }
}
struct Output {
    leaf: WorkingValue<directory::Leaf>,
    _descriptor: FinalDescriptor,
    rows: WorkingValue<Vec<ControlMvpSegmentRow>>,
}

pub(super) struct VerifiedInterval {
    outputs: [Option<Output>; 32],
}

impl VerifiedInterval {
    async fn append(
        self,
        io: &mut RestorePhysicalIo<'_>,
        totals: &mut FinalStreamTotals,
        history: &mut WorkingValue<super::super::super::super::logical_v2::RestoreHistory>,
        builder: &mut WorkingValue<directory::restore::NativeBuilder>,
    ) -> Result<()> {
        for output in self.outputs.into_iter().flatten() {
            let mut chunk = FinalMicrochunk::begin(totals, 0, io)?;
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            history.push_restore_history_rows(io, &mut route, &output.rows)?;
            builder
                .push_directory_leaf(io, &mut route, &output.leaf)
                .await?;
        }
        Ok(())
    }
}

fn reservation(io: &RestorePhysicalIo<'_>) -> Result<usize> {
    physical::restore_io::directory_scope_reservation(io.store())?
        .checked_mul(16)
        .and_then(|bytes| bytes.checked_add(BINDING_BYTES))
        .ok_or_else(|| invariant_violation("final binding reservation overflow"))
}

#[allow(
    clippy::large_futures,
    reason = "fixed bounded input/output slots retain explicit owners"
)]
pub(super) async fn verify_standard(
    io: &mut RestorePhysicalIo<'_>,
    totals: &mut FinalStreamTotals,
    plan: &WorkingValue<OwnedSelectedPlan>,
    expected: &WorkingValue<ExpectedPlan<'_>>,
    receipt: &WorkingValue<ControlMvpRestoreReceiptV1>,
) -> Result<VerifiedInterval> {
    let result = verify_inner(io, totals, plan, expected, receipt).await;
    if result.is_err() {
        io.stop_final(totals);
    }
    result
}

#[allow(
    clippy::too_many_lines,
    reason = "keep independently verified owners and fixed physical chunks visible"
)]
async fn verify_inner(
    io: &mut RestorePhysicalIo<'_>,
    totals: &mut FinalStreamTotals,
    plan: &WorkingValue<OwnedSelectedPlan>,
    expected: &WorkingValue<ExpectedPlan<'_>>,
    receipt: &WorkingValue<ControlMvpRestoreReceiptV1>,
) -> Result<VerifiedInterval> {
    let owned = plan.is_owned_by(io) && expected.is_owned_by(io) && receipt.is_owned_by(io);
    let store = io.store();
    // Preflight belongs to the first source directory path, not a separate chunk.
    let mut chunk = FinalMicrochunk::begin(totals, 0, io)?;
    let roots = {
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        let roots = decode_with_reservation(io, &mut route, Some(reservation(io)?), || {
            let f = &plan.value().plan.fields;
            if !owned
                || plan.value().plan_sha256 != expected.value().plan_sha256
                || f.candidate_id != expected.value().candidate_id
                || !matches!(
                    receipt.value().counts.reservation,
                    UnitReservationV1::Standard { .. }
                )
                || matches!(
                    receipt.value().singleton_before,
                    SingletonState::Pending { .. }
                )
                || receipt.value().singleton_after != SingletonState::None
                || matches!(receipt.value().before.global, GlobalCut::End)
            {
                return Err(invariant_violation(
                    "final standard plan, owner or phase differs",
                ));
            }
            plan.value().plan.validate_roots(store)?;
            if f.source_kv_root_b64 != expected.value().source_kv_root_b64
                || f.base_kv_root_b64 != expected.value().base_kv_root_b64
                || f.identity != *expected.value().identity
                || f.owner_generation != expected.value().owner_generation
                || f.candidate_seed_sha256 != expected.value().candidate_seed_sha256
            {
                return Err(invariant_violation("final selected plan binding differs"));
            }
            let directory = directory::Directory::new(store.retention.clone(), &store.scope)?;
            let before = match &receipt.value().before.global {
                GlobalCut::Start => None,
                GlobalCut::After { key_b64url } => Some(binary(key_b64url)?),
                GlobalCut::End => unreachable!("checked above"),
            };
            Ok((
                directory.decode_root(&binary(&f.source_kv_root_b64)?)?,
                directory.decode_root(&binary(&f.base_kv_root_b64)?)?,
                before,
            ))
        })?;
        drop(codec::validate_receipt(io, &mut route, expected, receipt)?);
        roots
    };
    let value = receipt.value();
    let f = &plan.value().plan.fields;
    let (source, mut chunk) = read_side(
        io,
        chunk,
        &roots.value().0,
        &value.before.source,
        roots.value().2.as_deref(),
        &f.source_kv_root_b64,
    )
    .await?;
    let current = if f.mode == Mode::Absent {
        Side {
            boundary: None,
            candidate: None,
        }
    } else {
        chunk = FinalMicrochunk::begin(chunk.finish(), 0, io)?;
        let (current, last) = read_side(
            io,
            chunk,
            &roots.value().1,
            &value.before.current,
            roots.value().2.as_deref(),
            &f.base_kv_root_b64,
        )
        .await?;
        chunk = last;
        current
    };
    let singleton_key = source
        .candidate
        .as_ref()
        .into_iter()
        .chain(current.candidate.as_ref())
        .filter(|i| i.descriptor.value().block.length > MAX_BLOCK_BYTES as u64)
        .filter_map(|i| i.position.value().as_ref().map(|p| p.leaf.first.as_slice()))
        .min();
    let cut = last_prefix(source.rows(), roots.value().2.as_deref(), singleton_key)
        .into_iter()
        .chain(last_prefix(
            current.rows(),
            roots.value().2.as_deref(),
            singleton_key,
        ))
        .min();
    if singleton_key.is_some() && cut.is_none() {
        return Err(invariant_violation(
            "standard final receipt must enter singleton comparison",
        ));
    }
    // Last input payload (or empty directory path) includes interval closure.
    {
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        drop(decode_with_reservation(
            io,
            &mut route,
            Some(reservation(io)?),
            || {
                for (side, witnesses, root, sequence) in [
                    (
                        &source,
                        &value.source_inputs,
                        &f.source_kv_root_b64,
                        f.source_logical_sequence,
                    ),
                    (
                        &current,
                        &value.current_inputs,
                        &f.base_kv_root_b64,
                        f.base_logical_sequence,
                    ),
                ] {
                    if side.boundary.iter().chain(side.candidate.iter()).any(|i| {
                        i.descriptor.value().segment.logical_sequence == 0
                            || i.descriptor.value().segment.logical_sequence > sequence
                    }) {
                        return Err(invariant_violation(
                            "final input sequence exceeds pinned root",
                        ));
                    }
                    if side.inputs().count() != witnesses.len() {
                        return Err(invariant_violation(
                            "final input coverage cardinality differs",
                        ));
                    }
                    for (input, witness) in side.inputs().zip(witnesses) {
                        bind_input(store, root, input, witness, sequence)?;
                    }
                }
                let global = match (&value.after.global, cut) {
                    (GlobalCut::End, None) => true,
                    (GlobalCut::After { key_b64url }, Some(key)) => binary(key_b64url)? == key,
                    _ => false,
                };
                if !global
                    || (f.mode == Mode::Absent
                        && !matches!(value.before.current, SideCursor::Start | SideCursor::End))
                {
                    return Err(invariant_violation("final interval cut differs"));
                }
                check_after(
                    &source,
                    &value.before.source,
                    &value.after.source,
                    &f.source_kv_root_b64,
                    roots.value().2.as_deref(),
                    cut,
                )?;
                check_after(
                    &current,
                    &value.before.current,
                    &value.after.current,
                    &f.base_kv_root_b64,
                    roots.value().2.as_deref(),
                    cut,
                )?;
                Ok(())
            },
        )?);
    }
    let mut outputs: [Option<Output>; 32] = std::array::from_fn(|_| None);
    for (slot, witness) in outputs.iter_mut().zip(&value.outputs) {
        chunk = FinalMicrochunk::begin(chunk.finish(), 0, io)?;
        let (leaf, descriptor) = {
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let leaf = decode_with_reservation(io, &mut route, Some(reservation(io)?), || {
                if witness.directory_leaf.bytes as usize > MAX_BLOCK_BYTES {
                    return Err(invariant_violation(
                        "standard final output exceeds block limit",
                    ));
                }
                Ok(directory::Leaf {
                    first: binary(&witness.directory_leaf.first_key_b64url)?,
                    last: binary(&witness.directory_leaf.last_key_b64url)?,
                    rows: witness.directory_leaf.rows,
                    bytes: witness.directory_leaf.bytes,
                    digest: digest_array(&witness.directory_leaf.digest)?,
                })
            })?;
            let descriptor =
                physical::restore_io::read_final_descriptor(io, &mut route, &leaf).await?;
            drop(decode_with_reservation(
                io,
                &mut route,
                Some(reservation(io)?),
                || {
                    bind_objects(
                        store,
                        &descriptor,
                        &witness.descriptor,
                        &witness.index,
                        &witness.block,
                    )?;
                    let d = descriptor.value();
                    if digest_array(&witness.descriptor.sha256)? != leaf.value().digest
                        || d.segment.segment_id != witness.output_id
                        || d.segment.logical_sequence != f.result_logical_sequence
                        || d.block.offset != 0
                        || d.block.length != d.segment.segment_size_bytes
                        || d.block.checksum_sha256 != d.segment.checksum_sha256
                    {
                        return Err(invariant_violation(
                            "final output identity or complete block differs",
                        ));
                    }
                    Ok(())
                },
            )?);
            (leaf, descriptor)
        };
        chunk = FinalMicrochunk::begin(chunk.finish(), 0, io)?;
        let rows = physical::restore_io::read_final_payload(
            io,
            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
            &descriptor,
        )
        .await?;
        *slot = Some(Output {
            leaf,
            _descriptor: descriptor,
            rows,
        });
    }
    // No outputs: retain the last input/path chunk. Otherwise this is the last output payload.
    drop(decode_with_reservation(
        io,
        &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
        Some(64 * 1024),
        || {
            compare_merge(
                &source,
                &current,
                &outputs,
                roots.value().2.as_deref(),
                cut,
                f.mode,
                f.result_logical_sequence,
                value.counts.mutations,
            )
        },
    )?);
    Ok(VerifiedInterval { outputs })
}

#[allow(
    clippy::too_many_lines,
    reason = "keep directory, descriptor, payload and boundary ownership in their fixed physical chunks"
)]
async fn read_side<'a>(
    io: &mut RestorePhysicalIo<'_>,
    mut chunk: FinalMicrochunk<'a>,
    root: &directory::Root,
    before: &SideCursor,
    global: Option<&[u8]>,
    root_b64: &str,
) -> Result<(Side, FinalMicrochunk<'a>)> {
    let (key, position) = {
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        let key = decode_with_reservation(io, &mut route, Some(reservation(io)?), || {
            validate_side_cursor(root_b64, before)?;
            match before {
                SideCursor::After { key_b64url, .. } => {
                    let key = binary(key_b64url)?;
                    if global.is_none_or(|cut| key.as_slice() > cut) {
                        return Err(invariant_violation("final cursor exceeds preceding cut"));
                    }
                    Ok(Some(key))
                }
                _ => Ok(None),
            }
        })?;
        let position = if let Some(key) = key.value() {
            directory::restore::first_at_or_after(io, &mut route, root, key).await?
        } else {
            directory::restore::first_after(
                io,
                &mut route,
                root,
                if matches!(before, SideCursor::End) {
                    global
                } else {
                    None
                },
            )
            .await?
        };
        (key, position)
    };
    let metadata_only = matches!(before, SideCursor::After { position, key_b64url } if *key_b64url == position.leaf.last_b64url);
    let (mut candidate, mut chunk) = load(io, chunk, position, metadata_only).await?;
    let mut boundary = None;
    if let SideCursor::After { position, .. } = before {
        let exhausted = {
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            decode_with_reservation(io, &mut route, Some(reservation(io)?), || {
                let input = candidate
                    .as_ref()
                    .ok_or_else(|| invariant_violation("final cursor leaf missing"))?;
                check_position(
                    root_b64,
                    position,
                    input
                        .position
                        .value()
                        .as_ref()
                        .ok_or_else(|| invariant_violation("final input position missing"))?,
                )?;
                let key = key
                    .value()
                    .as_deref()
                    .ok_or_else(|| invariant_violation("final saved key missing"))?;
                if if input.rows.value().is_empty() {
                    input
                        .position
                        .value()
                        .as_ref()
                        .is_none_or(|p| p.leaf.last.as_slice() != key)
                } else {
                    input
                        .rows
                        .value()
                        .binary_search_by(|row| row.key.as_slice().cmp(key))
                        .is_err()
                } {
                    return Err(invariant_violation("final cursor row missing"));
                }
                Ok(input
                    .position
                    .value()
                    .as_ref()
                    .is_some_and(|p| p.leaf.last.as_slice() == key))
            })?
        };
        if *exhausted.value() {
            boundary = candidate.take();
            chunk = FinalMicrochunk::begin(chunk.finish(), 0, io)?;
            let position = directory::restore::first_after(
                io,
                &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
                root,
                key.value().as_deref(),
            )
            .await?;
            (candidate, chunk) = load(io, chunk, position, false).await?;
        }
    }
    drop(decode_with_reservation(
        io,
        &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
        Some(64 * 1024),
        || {
            if matches!(before, SideCursor::End) && candidate.is_some() {
                return Err(invariant_violation("final end cursor omits input"));
            }
            if candidate.as_ref().is_some_and(|input| {
                input.rows.value().iter().any(|row| {
                    key.value()
                        .as_deref()
                        .is_none_or(|key| row.key.as_slice() > key)
                        && global.is_some_and(|cut| row.key.as_slice() <= cut)
                })
            }) {
                return Err(invariant_violation("final cursor skips input before cut"));
            }
            Ok(())
        },
    )?);
    Ok((
        Side {
            boundary,
            candidate,
        },
        chunk,
    ))
}

async fn load<'a>(
    io: &mut RestorePhysicalIo<'_>,
    chunk: FinalMicrochunk<'a>,
    position: WorkingValue<Option<directory::restore::Position>>,
    metadata_only: bool,
) -> Result<(Option<Input>, FinalMicrochunk<'a>)> {
    let Some(value) = position.value() else {
        return Ok((None, chunk));
    };
    let mut chunk = FinalMicrochunk::begin(chunk.finish(), 0, io)?;
    let descriptor = {
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        let leaf = decode_with_reservation(io, &mut route, Some(reservation(io)?), || {
            Ok(value.leaf.clone())
        })?;
        physical::restore_io::read_final_descriptor(io, &mut route, &leaf).await?
    };
    let rows = if metadata_only || value.leaf.bytes as usize > MAX_BLOCK_BYTES {
        decode_with_reservation(
            io,
            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
            Some(64 * 1024),
            || {
                if value.leaf.bytes as usize > MAX_BLOCK_BYTES
                    && (value.leaf.rows != 1 || value.leaf.first != value.leaf.last)
                {
                    return Err(invariant_violation("oversized final lookahead differs"));
                }
                Ok(vec![])
            },
        )?
    } else {
        chunk = FinalMicrochunk::begin(chunk.finish(), 0, io)?;
        physical::restore_io::read_final_payload(
            io,
            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
            &descriptor,
        )
        .await?
    };
    Ok((
        Some(Input {
            position,
            descriptor,
            rows,
        }),
        chunk,
    ))
}

fn digest_array(value: &str) -> Result<[u8; 32]> {
    let mut digest = [0; 32];
    hex::decode_to_slice(raw_digest(value)?, &mut digest)
        .map_err(|_| invariant_violation("final witness digest differs"))?;
    Ok(digest)
}

fn check_leaf(w: &DirectoryPositionLeafWitness, leaf: &directory::Leaf) -> Result<()> {
    if binary(&w.first_b64url)? != leaf.first
        || binary(&w.last_b64url)? != leaf.last
        || w.rows != leaf.rows
        || w.bytes != leaf.bytes
        || digest_array(&w.digest)? != leaf.digest
    {
        return Err(invariant_violation("final leaf witness differs"));
    }
    Ok(())
}
fn check_position(
    root: &str,
    witness: &DirectoryPosition,
    position: &directory::restore::Position,
) -> Result<()> {
    if witness.role != physical::Role::Kv
        || witness.root_b64 != root
        || witness.path.len() != position.path.len()
    {
        return Err(invariant_violation("final position root or depth differs"));
    }
    for (w, (digest, slot)) in witness.path.iter().zip(&position.path) {
        if digest_array(&w.page_sha256)? != *digest || w.child_index != *slot {
            return Err(invariant_violation("final saved path differs"));
        }
    }
    check_leaf(&witness.leaf, &position.leaf)
}
fn bind_input(
    store: &super::super::super::super::ControlMvpStateStore,
    root: &str,
    input: &Input,
    witness: &InputWitness,
    sequence: u64,
) -> Result<()> {
    if witness.role != physical::Role::Kv
        || witness.root_b64 != root
        || input.descriptor.value().segment.logical_sequence == 0
        || input.descriptor.value().segment.logical_sequence > sequence
    {
        return Err(invariant_violation("final input root or sequence differs"));
    }
    check_leaf(
        &witness.directory_leaf,
        &input
            .position
            .value()
            .as_ref()
            .ok_or_else(|| invariant_violation("final input position missing"))?
            .leaf,
    )?;
    if digest_array(&witness.descriptor.sha256)?
        != input
            .position
            .value()
            .as_ref()
            .ok_or_else(|| invariant_violation("final input position missing"))?
            .leaf
            .digest
    {
        return Err(invariant_violation(
            "final input descriptor digest differs from authenticated leaf",
        ));
    }

    bind_objects(
        store,
        &input.descriptor,
        &witness.descriptor,
        &witness.index,
        &witness.block,
    )
}
fn bind_objects(
    store: &super::super::super::super::ControlMvpStateStore,
    authenticated: &FinalDescriptor,
    descriptor: &ImmutableObjectWitness,
    index: &ImmutableObjectWitness,
    block: &BlockWitness,
) -> Result<()> {
    let d = authenticated.value();
    let expected_path = format!(
        "{}/physical/descriptors/{}.json",
        store.paths.base_prefix(),
        raw_digest(&descriptor.sha256)?
    );
    let (first, last) = super::super::super::super::block_key_bounds(&d.block)?
        .ok_or_else(|| invariant_violation("final block bounds missing"))?;
    if descriptor.path != expected_path
        || descriptor.byte_size != authenticated.raw_bytes()
        || index.path != store.paths.segment_index(&d.segment.segment_id)
        || index.byte_size != d.segment.index_size_bytes
        || raw_digest(&index.sha256)? != d.segment.index_checksum_sha256
        || block.offset != d.block.offset
        || block.length != d.block.length
        || raw_digest(&block.sha256)? != d.block.checksum_sha256
        || block.rows != d.block.row_count
        || binary(&block.min_key_b64url)? != first
        || binary(&block.max_key_b64url)? != last
    {
        return Err(invariant_violation("final physical object witness differs"));
    }
    Ok(())
}
fn selected_rows<'a>(
    side: &'a Side,
    before: Option<&'a [u8]>,
    cut: Option<&'a [u8]>,
) -> impl Iterator<Item = &'a ControlMvpSegmentRow> {
    side.rows().iter().filter(move |row| {
        before.is_none_or(|key| row.key.as_slice() > key)
            && cut.is_none_or(|key| row.key.as_slice() <= key)
    })
}
fn check_after(
    side: &Side,
    prior: &SideCursor,
    after: &SideCursor,
    root: &str,
    before: Option<&[u8]>,
    cut: Option<&[u8]>,
) -> Result<()> {
    let valid = if let Some(input) = &side.candidate {
        if let Some(row) = selected_rows(side, before, cut).last() {
            if let SideCursor::After {
                key_b64url,
                position,
            } = after
            {
                check_position(
                    root,
                    position,
                    input
                        .position
                        .value()
                        .as_ref()
                        .ok_or_else(|| invariant_violation("final input position missing"))?,
                )?;
                binary(key_b64url)? == row.key
            } else {
                false
            }
        } else {
            prior == after
        }
    } else {
        matches!(after, SideCursor::End)
    };
    if !valid {
        return Err(invariant_violation("final after cursor differs"));
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "independent comparison borrows the exact bounded interval"
)]
fn compare_merge(
    source: &Side,
    current: &Side,
    outputs: &[Option<Output>; 32],
    before: Option<&[u8]>,
    cut: Option<&[u8]>,
    mode: Mode,
    sequence: u64,
    declared_mutations: u64,
) -> Result<()> {
    let mut source = selected_rows(source, before, cut).peekable();
    let mut current = selected_rows(current, before, cut).peekable();
    let mut output = outputs
        .iter()
        .flatten()
        .flat_map(|block| block.rows.value());
    let mut mutations = 0_u64;
    while source.peek().is_some() || current.peek().is_some() {
        let take_source = match (source.peek(), current.peek()) {
            (Some(s), Some(c)) => s.key <= c.key,
            (Some(_), None) => true,
            _ => false,
        };
        let (s, c) = if take_source {
            let s = source
                .next()
                .ok_or_else(|| invariant_violation("final source row missing"))?;
            let c = if current.peek().is_some_and(|c| c.key == s.key) {
                current.next()
            } else {
                None
            };
            (Some(s), c)
        } else {
            (None, current.next())
        };
        let key = s
            .or(c)
            .ok_or_else(|| invariant_violation("final merge row missing"))?
            .key
            .as_slice();
        let expected = match (s.filter(|row| !row.tombstone), c) {
            (Some(s), c) => Some((
                s.value.as_deref(),
                c.filter(|c| c.value == s.value)
                    .map_or(sequence, |c| c.generation),
            )),
            (None, Some(c)) => Some((None, if c.tombstone { c.generation } else { sequence })),
            (None, None) if mode == Mode::Absent => s.map(|s| (None, s.generation)),
            (None, None) => None,
        };
        if let Some((value, generation)) = expected {
            let actual = output
                .next()
                .ok_or_else(|| invariant_violation("final output omits merged row"))?;
            if actual.key != key
                || actual.value.as_deref() != value
                || actual.generation != generation
                || actual.tombstone != value.is_none()
            {
                return Err(invariant_violation("final merged row differs"));
            }
            if generation == sequence {
                mutations = mutations
                    .checked_add(1)
                    .ok_or_else(|| invariant_violation("final mutation overflow"))?;
            }
        }
    }
    if output.next().is_some() || mutations != declared_mutations {
        return Err(invariant_violation(
            "final output coverage or mutation count differs",
        ));
    }
    for (ordinal, row) in outputs
        .iter()
        .flatten()
        .flat_map(|block| block.rows.value())
        .enumerate()
    {
        if row.logical_sequence != sequence
            || row.logical_ordinal != ordinal as u64
            || row.origin_sequence.is_some()
            || row.record_kind != SEGMENT_RECORD_KV
        {
            return Err(invariant_violation("final output row semantics differ"));
        }
    }
    Ok(())
}

pub(super) struct StandardAssembly {
    root: WorkingValue<directory::Root>,
    digest: WorkingValue<String>,
    _history: WorkingValue<super::super::super::super::logical_v2::RestoreHistory>,
}
impl StandardAssembly {
    pub(super) fn root(&self) -> &directory::Root {
        self.root.value()
    }
    pub(super) fn digest(&self) -> &str {
        self.digest.value()
    }
}

pub(super) struct StandardStream<'a, 'p, 't> {
    prefix: super::prefix::Prefix<'a, 'p>,
    plan: &'a WorkingValue<OwnedSelectedPlan>,
    expected: &'a WorkingValue<ExpectedPlan<'p>>,
    history: WorkingValue<super::super::super::super::logical_v2::RestoreHistory>,
    builder: WorkingValue<directory::restore::NativeBuilder>,
    chunk: Option<FinalMicrochunk<'t>>,
}

impl<'a, 'p, 't> StandardStream<'a, 'p, 't> {
    pub(super) fn new(
        io: &mut RestorePhysicalIo<'_>,
        route: &mut RestorePhysicalRoute<'_, '_>,
        totals: &'t mut FinalStreamTotals,
        plan: &'a WorkingValue<OwnedSelectedPlan>,
        expected: &'a WorkingValue<ExpectedPlan<'p>>,
        selected: &'a super::SelectedProgress,
    ) -> Result<Self> {
        let prefix = super::prefix::Prefix::new(io, route, expected, selected)?;
        // Initialization shares the first actual receipt's allowance.
        let mut chunk = FinalMicrochunk::begin(totals, 0, io)?;
        let mut final_route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        let history = super::history::new(
            io,
            &mut final_route,
            plan,
            selected.progress.value().cumulative_counts.mutations,
        )?;
        let builder = physical::restore_io::new_directory_builder(io, &mut final_route)?;
        Ok(Self {
            prefix,
            plan,
            expected,
            history,
            builder,
            chunk: Some(chunk),
        })
    }

    #[allow(
        clippy::large_futures,
        reason = "one bounded verified interval remains on stack through its builder writes"
    )]
    pub(super) async fn next(&mut self, io: &mut RestorePhysicalIo<'_>) -> Result<()> {
        let mut chunk = self
            .chunk
            .take()
            .ok_or_else(|| invariant_violation("final stream has no active microchunk"))?;
        let receipt = {
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let Some(receipt) = self.prefix.next(io, &mut route).await? else {
                return decode_with_reservation::<()>(io, &mut route, Some(64 * 1024), || {
                    Err(invariant_violation("final receipt prefix ended early"))
                })
                .map(|_| unreachable!("admitted rejection always returns an error"));
            };
            receipt
        };
        let totals = chunk.finish();
        if matches!(
            receipt.value().counts.reservation,
            UnitReservationV1::Singleton { .. }
        ) {
            super::singleton_coverage::verify_append(
                io,
                totals,
                self.plan,
                self.expected,
                &receipt,
                &mut self.history,
                &mut self.builder,
            )
            .await?;
        } else {
            let interval = verify_standard(io, totals, self.plan, self.expected, &receipt).await?;
            interval
                .append(io, totals, &mut self.history, &mut self.builder)
                .await?;
        }
        self.chunk = Some(FinalMicrochunk::begin(totals, 0, io)?);
        Ok(())
    }

    pub(super) async fn finish(
        mut self,
        io: &mut RestorePhysicalIo<'_>,
    ) -> Result<StandardAssembly> {
        let mut chunk = self
            .chunk
            .take()
            .ok_or_else(|| invariant_violation("final stream has no finish microchunk"))?;
        drop(self.prefix);
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        let root = self.builder.finish_directory(io, &mut route).await?;
        let digest = self.history.finish_restore_history(io, &mut route)?;
        Ok(StandardAssembly {
            root,
            digest,
            _history: self.history,
        })
    }
}

#[allow(
    clippy::large_futures,
    reason = "bounded verified interval owners remain on stack through builder writes"
)]
pub(super) async fn assemble_standard(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    totals: &mut FinalStreamTotals,
    plan: &WorkingValue<OwnedSelectedPlan>,
    expected: &WorkingValue<ExpectedPlan<'_>>,
    selected: &super::SelectedProgress,
) -> Result<StandardAssembly> {
    let result = async {
        let mut stream = StandardStream::new(io, route, totals, plan, expected, selected)?;
        for _ in 0..selected.progress.value().receipt_count {
            stream.next(io).await?;
        }
        stream.finish(io).await
    }
    .await;
    if result.is_err() {
        io.stop_final(totals);
    }
    result
}

fn last_prefix<'a>(
    rows: &'a [ControlMvpSegmentRow],
    before: Option<&[u8]>,
    singleton: Option<&[u8]>,
) -> Option<&'a [u8]> {
    rows.iter()
        .rev()
        .find(|row| {
            before.is_none_or(|key| row.key.as_slice() > key)
                && singleton.is_none_or(|key| row.key.as_slice() < key)
        })
        .map(|row| row.key.as_slice())
}
