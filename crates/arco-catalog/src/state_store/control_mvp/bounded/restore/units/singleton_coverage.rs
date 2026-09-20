//! Independent singleton semantic replay; values never enter carried observations.
use super::super::{Mode, directory};
use super::{
    ControlMvpRestoreReceiptV1, DirectoryPosition, ExpectedPlan, GlobalCut, ImmutableObjectWitness,
    InputWitness, MAX_BLOCK_BYTES, MergeCursor, OutputWitness, OwnedSelectedPlan,
    RestorePhysicalIo, RestorePhysicalRoute, Result, SideCursor, SingletonPhase, SingletonState,
    SingletonValueWitness, UnitReservationV1, WorkingValue, binary, codec, decode_with_reservation,
    invariant_violation, physical, prefixed_sha256,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use physical::restore_io::{
    FinalDescriptor, FinalMicrochunk, FinalStreamTotals, hash_with_reservation,
};

const MODEL_BYTES: usize = 8 * 1024 * 1024;

struct Observation {
    next_key: Option<Vec<u8>>,
    position: Option<DirectoryPosition>,
    input: Option<InputWitness>,
    value: Option<SingletonValueWitness>,
}

fn input_witness(
    store: &super::super::super::super::ControlMvpStateStore,
    root: &str,
    position: &directory::restore::Position,
    descriptor: &FinalDescriptor,
) -> Result<InputWitness> {
    let d = descriptor.value();
    let digest = format!("sha256:{}", hex::encode(position.leaf.digest));
    Ok(InputWitness {
        role: physical::Role::Kv,
        root_b64: root.into(),
        directory_leaf: super::singleton::position_witness(root, position).leaf,
        descriptor: ImmutableObjectWitness {
            path: format!(
                "{}/physical/descriptors/{}.json",
                store.paths.base_prefix(),
                hex::encode(position.leaf.digest)
            ),
            byte_size: descriptor.raw_bytes(),
            sha256: digest,
        },
        index: ImmutableObjectWitness {
            path: store.paths.segment_index(&d.segment.segment_id),
            byte_size: d.segment.index_size_bytes,
            sha256: format!("sha256:{}", d.segment.index_checksum_sha256),
        },
        block: super::window::block_witness(d)?,
    })
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "independent rooted observation releases each payload before returning bounded witnesses"
)]
async fn observe<'a>(
    io: &mut RestorePhysicalIo<'_>,
    mut chunk: FinalMicrochunk<'a>,
    root: &directory::Root,
    root_b64: &str,
    cursor: &SideCursor,
    global: Option<&[u8]>,
    key: &[u8],
    sequence: u64,
) -> Result<(WorkingValue<Observation>, FinalMicrochunk<'a>)> {
    let store = io.store();
    let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
    let saved_key = decode_with_reservation(io, &mut route, Some(MODEL_BYTES), || {
        super::validate_side_cursor(root_b64, cursor)?;
        match cursor {
            SideCursor::After { key_b64url, .. } => {
                let saved = binary(key_b64url)?;
                if global.is_none_or(|g| saved.as_slice() > g) {
                    return Err(invariant_violation(
                        "singleton final side exceeds global cut",
                    ));
                }
                Ok(Some(saved))
            }
            _ => Ok(None),
        }
    })?;
    let mut position = if let Some(saved) = saved_key.value().as_deref() {
        directory::restore::first_at_or_after(io, &mut route, root, saved).await?
    } else {
        directory::restore::first_after(
            io,
            &mut route,
            root,
            if matches!(cursor, SideCursor::End) {
                global
            } else {
                None
            },
        )
        .await?
    };
    drop(decode_with_reservation(
        io,
        &mut route,
        Some(MODEL_BYTES),
        || {
            if let SideCursor::After {
                position: witness, ..
            } = cursor
            {
                let p = position
                    .value()
                    .as_ref()
                    .ok_or_else(|| invariant_violation("singleton final saved leaf missing"))?;
                if super::singleton::position_witness(root_b64, p) != *witness {
                    return Err(invariant_violation("singleton final saved locator differs"));
                }
            }
            if matches!(cursor, SideCursor::End) && position.value().is_some() {
                return Err(invariant_violation("singleton final End omits input"));
            }
            Ok(())
        },
    )?);
    if let (Some(saved), Some(p)) = (saved_key.value().as_deref(), position.value()) {
        if p.leaf.last.as_slice() == saved {
            let (descriptor, mut floor_chunk) = read_descriptor(io, chunk.finish(), p).await?;
            drop(decode_with_reservation(
                io,
                &mut RestorePhysicalRoute::FinalMicrochunk(&mut floor_chunk),
                Some(MODEL_BYTES),
                || {
                    if descriptor.value().segment.logical_sequence == 0
                        || descriptor.value().segment.logical_sequence > sequence
                        || (p.leaf.bytes as usize > MAX_BLOCK_BYTES
                            && (p.leaf.first != p.leaf.last || p.leaf.rows != 1))
                    {
                        return Err(invariant_violation(
                            "singleton final consumed floor differs",
                        ));
                    }
                    Ok(())
                },
            )?);
            drop(descriptor);
            chunk = FinalMicrochunk::begin(floor_chunk.finish(), 0, io)?;
            position = directory::restore::first_after(
                io,
                &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
                root,
                Some(saved),
            )
            .await?;
        }
    }
    let Some(p) = position.value() else {
        let observation = decode_with_reservation(
            io,
            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
            Some(MODEL_BYTES),
            || {
                Ok(Observation {
                    next_key: None,
                    position: None,
                    input: None,
                    value: None,
                })
            },
        )?;
        return Ok((observation, chunk));
    };
    let (descriptor, mut metadata_chunk) = read_descriptor(io, chunk.finish(), p).await?;
    if p.leaf.bytes as usize > MAX_BLOCK_BYTES && p.leaf.first.as_slice() > key {
        let observation = decode_with_reservation(
            io,
            &mut RestorePhysicalRoute::FinalMicrochunk(&mut metadata_chunk),
            Some(MODEL_BYTES),
            || {
                if descriptor.value().segment.logical_sequence > sequence
                    || p.leaf.rows != 1
                    || p.leaf.first != p.leaf.last
                {
                    return Err(invariant_violation(
                        "singleton final excluded oversized leaf differs",
                    ));
                }
                Ok(Observation {
                    next_key: Some(p.leaf.first.clone()),
                    position: Some(super::singleton::position_witness(root_b64, p)),
                    input: Some(input_witness(store, root_b64, p, &descriptor)?),
                    value: None,
                })
            },
        )?;
        return Ok((observation, metadata_chunk));
    }
    let mut chunk = payload_chunk(metadata_chunk.finish(), io, &descriptor)?;
    let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
    let rows = physical::restore_io::read_final_payload(io, &mut route, &descriptor).await?;
    let selected = rows
        .value()
        .binary_search_by(|r| r.key.as_slice().cmp(key))
        .ok();
    let value_hash = if let Some(ordinal) = selected {
        let bytes = rows
            .value()
            .get(ordinal)
            .ok_or_else(|| invariant_violation("singleton final ordinal missing"))?
            .value
            .as_deref()
            .unwrap_or_default();
        Some(hash_with_reservation(
            io,
            &mut route,
            64 * 1024,
            bytes.len(),
            || Ok(prefixed_sha256(bytes)),
        )?)
    } else {
        None
    };
    let result = decode_with_reservation(io, &mut route, Some(MODEL_BYTES), || {
        let d = descriptor.value();
        if d.segment.logical_sequence == 0 || d.segment.logical_sequence > sequence {
            return Err(invariant_violation(
                "singleton final input sequence differs",
            ));
        }
        if let Some(saved) = saved_key
            .value()
            .as_deref()
            .filter(|saved| p.leaf.first.as_slice() <= *saved && p.leaf.last.as_slice() > *saved)
        {
            if rows
                .value()
                .binary_search_by(|r| r.key.as_slice().cmp(saved))
                .is_err()
            {
                return Err(invariant_violation(
                    "singleton final partial floor key missing",
                ));
            }
        }
        let next = rows.value().iter().find(|r| {
            saved_key
                .value()
                .as_deref()
                .is_none_or(|saved| r.key.as_slice() > saved)
        });
        if next.is_some_and(|r| global.is_some_and(|g| r.key.as_slice() <= g)) {
            return Err(invariant_violation("singleton final cursor omitted a row"));
        }
        let input = input_witness(store, root_b64, p, &descriptor)?;
        let value = selected
            .map(|ordinal| -> Result<SingletonValueWitness> {
                let row = rows.value().get(ordinal).ok_or_else(|| {
                    invariant_violation("singleton final selected ordinal missing")
                })?;
                Ok(SingletonValueWitness {
                    generation: row.generation,
                    tombstone: row.tombstone,
                    value_length: row.value.as_ref().map_or(0, |v| v.len() as u64),
                    value_sha256: value_hash
                        .as_ref()
                        .ok_or_else(|| invariant_violation("singleton final value hash missing"))?
                        .value()
                        .clone(),
                    descriptor: input.descriptor.clone(),
                    index: input.index.clone(),
                    block: input.block.clone(),
                    row_ordinal: ordinal as u64,
                })
            })
            .transpose()?;
        Ok(Observation {
            next_key: next.map(|r| r.key.clone()),
            position: Some(super::singleton::position_witness(root_b64, p)),
            input: Some(input),
            value,
        })
    })?;
    drop(rows);
    Ok((result, chunk))
}

async fn read_descriptor<'a>(
    io: &mut RestorePhysicalIo<'_>,
    totals: &'a mut FinalStreamTotals,
    p: &directory::restore::Position,
) -> Result<(FinalDescriptor, FinalMicrochunk<'a>)> {
    let mut chunk = FinalMicrochunk::begin(totals, 0, io)?;
    let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
    let leaf = decode_with_reservation(io, &mut route, Some(MODEL_BYTES), || Ok(p.leaf.clone()))?;
    let descriptor = physical::restore_io::read_final_descriptor(io, &mut route, &leaf).await?;
    Ok((descriptor, chunk))
}
fn payload_chunk<'a>(
    totals: &'a mut FinalStreamTotals,
    io: &mut RestorePhysicalIo<'_>,
    descriptor: &FinalDescriptor,
) -> Result<FinalMicrochunk<'a>> {
    if descriptor.value().block.length > MAX_BLOCK_BYTES as u64 {
        FinalMicrochunk::begin_singleton(totals, 0, io, descriptor)
    } else {
        FinalMicrochunk::begin(totals, 0, io)
    }
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "independent phase, semantic and count verification before history or builder effects"
)]
pub(super) async fn verify_append(
    io: &mut RestorePhysicalIo<'_>,
    totals: &mut FinalStreamTotals,
    plan: &WorkingValue<OwnedSelectedPlan>,
    expected: &WorkingValue<ExpectedPlan<'_>>,
    receipt: &WorkingValue<ControlMvpRestoreReceiptV1>,
    history: &mut WorkingValue<super::super::super::super::logical_v2::RestoreHistory>,
    builder: &mut WorkingValue<directory::restore::NativeBuilder>,
) -> Result<()> {
    let same_owner = plan.is_owned_by(io) && expected.is_owned_by(io) && receipt.is_owned_by(io);
    let f = &plan.value().plan.fields;
    let r = receipt.value();
    let store = io.store();
    let mut chunk = FinalMicrochunk::begin(totals, 0, io)?;
    let roots = {
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        drop(codec::validate_receipt(io, &mut route, expected, receipt)?);
        decode_with_reservation(io, &mut route, Some(MODEL_BYTES), || {
            let e = expected.value();
            if !same_owner
                || plan.value().plan_sha256 != e.plan_sha256
                || f.candidate_id != e.candidate_id
                || f.source_kv_root_b64 != e.source_kv_root_b64
                || f.base_kv_root_b64 != e.base_kv_root_b64
                || f.identity != *e.identity
                || f.owner_generation != e.owner_generation
                || f.candidate_seed_sha256 != e.candidate_seed_sha256
            {
                return Err(invariant_violation(
                    "singleton final selected plan binding differs",
                ));
            }
            plan.value().plan.validate_roots(store)?;
            let d = directory::Directory::new(store.retention.clone(), &store.scope)?;
            let key = match (&r.singleton_before, &r.singleton_after) {
                (SingletonState::Pending { key_b64url, .. }, _)
                | (_, SingletonState::Pending { key_b64url, .. }) => binary(key_b64url)?,
                _ => return Err(invariant_violation("singleton final pending key missing")),
            };
            let global = match &r.before.global {
                GlobalCut::Start => None,
                GlobalCut::After { key_b64url } => Some(binary(key_b64url)?),
                GlobalCut::End => {
                    return Err(invariant_violation("singleton final starts after End"));
                }
            };
            Ok((
                d.decode_root(&binary(&f.source_kv_root_b64)?)?,
                d.decode_root(&binary(&f.base_kv_root_b64)?)?,
                key,
                global,
            ))
        })?
    };
    let (source_root, current_root, key, global) = roots.value();
    let (source, mut chunk) = observe(
        io,
        chunk,
        source_root,
        &f.source_kv_root_b64,
        &r.before.source,
        global.as_deref(),
        key,
        f.source_logical_sequence,
    )
    .await?;
    let (current, mut chunk) = if f.mode == Mode::Absent {
        let current = decode_with_reservation(
            io,
            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
            Some(MODEL_BYTES),
            || {
                if !matches!(r.before.current, SideCursor::Start | SideCursor::End) {
                    return Err(invariant_violation(
                        "singleton final absent current differs",
                    ));
                }
                Ok(Observation {
                    next_key: None,
                    position: None,
                    input: None,
                    value: None,
                })
            },
        )?;
        (current, chunk)
    } else {
        let chunk = FinalMicrochunk::begin(chunk.finish(), 0, io)?;
        observe(
            io,
            chunk,
            current_root,
            &f.base_kv_root_b64,
            &r.before.current,
            global.as_deref(),
            key,
            f.base_logical_sequence,
        )
        .await?
    };
    let s = source.value();
    let c = current.value();
    let output_spec = decode_with_reservation(
        io,
        &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
        Some(MODEL_BYTES),
        || {
            let next = s
                .next_key
                .as_deref()
                .into_iter()
                .chain(c.next_key.as_deref())
                .min();
            if next != Some(key.as_slice()) {
                return Err(invariant_violation(
                    "singleton final pending key skips rooted prefix",
                ));
            }
            let first = matches!(
                r.singleton_before,
                SingletonState::None | SingletonState::Complete { .. }
            );
            if first
                && ![(s, &s.value), (c, &c.value)]
                    .into_iter()
                    .any(|(observation, value)| {
                        value.is_some()
                            && observation
                                .input
                                .as_ref()
                                .is_some_and(|input| input.block.length > MAX_BLOCK_BYTES as u64)
                    })
            {
                return Err(invariant_violation(
                    "singleton final pending key is not oversized",
                ));
            }
            let standard_first = first
                && s.input
                    .as_ref()
                    .into_iter()
                    .chain(c.input.as_ref())
                    .any(|i| i.block.length > MAX_BLOCK_BYTES as u64)
                && r.source_inputs
                    .iter()
                    .chain(&r.current_inputs)
                    .any(|i| i.block.length <= MAX_BLOCK_BYTES as u64);
            let UnitReservationV1::Singleton { phase, .. } = r.counts.reservation else {
                return Err(invariant_violation("singleton final reservation differs"));
            };
            if !first {
                let (old_s, old_c) = state_values(&r.singleton_before)?;
                for (old, actual) in [(old_s, s.value.as_ref()), (old_c, c.value.as_ref())] {
                    if old.is_some() && old != actual {
                        return Err(invariant_violation(
                            "singleton final changed prior observation",
                        ));
                    }
                }
            }
            let read_source = if first {
                s.value.is_some()
            } else {
                match &r.singleton_before {
                    SingletonState::Pending {
                        phase: SingletonPhase::CompareSource,
                        source,
                        current,
                        ..
                    } => {
                        if source.is_none() && s.value.is_some() {
                            true
                        } else if current.is_none() && c.value.is_some() {
                            false
                        } else {
                            s.value.is_some()
                        }
                    }
                    SingletonState::Pending {
                        phase: SingletonPhase::Emit,
                        ..
                    } => s.value.as_ref().is_some_and(|s| !s.tombstone) || c.value.is_none(),
                    _ => s.value.is_some(),
                }
            };
            let expected_inputs = |obs: &Observation, selected: bool| -> Vec<InputWitness> {
                if standard_first {
                    obs.input
                        .iter()
                        .filter(|i| i.block.length <= MAX_BLOCK_BYTES as u64)
                        .cloned()
                        .collect()
                } else if selected {
                    obs.input.iter().cloned().collect()
                } else {
                    vec![]
                }
            };
            if r.source_inputs != expected_inputs(s, read_source)
                || r.current_inputs != expected_inputs(c, !read_source)
            {
                return Err(invariant_violation(
                    "singleton final selected input locators differ",
                ));
            }
            let all = r.source_inputs.iter().chain(&r.current_inputs);
            let input_bytes = all.clone().map(|i| i.block.length).sum::<u64>();
            let input_rows = all.clone().map(|i| i.block.rows).sum::<u64>();
            let max_payload = all
                .map(|i| i.block.length)
                .max()
                .ok_or_else(|| invariant_violation("singleton final input missing"))?;
            if r.counts.source_leaves != r.source_inputs.len() as u64
                || r.counts.current_leaves != r.current_inputs.len() as u64
                || r.counts.input_encoded_bytes != input_bytes
                || r.counts.decoded_blocks
                    != (r.source_inputs.len() + r.current_inputs.len()) as u64
                || r.counts.decoded_rows != input_rows
                || !matches!(r.counts.reservation, UnitReservationV1::Singleton { authenticated_payload_bytes, .. } if authenticated_payload_bytes == max_payload)
            {
                return Err(invariant_violation("singleton final work counts differ"));
            }
            if phase != SingletonPhase::Emit {
                let (new_s, new_c) = state_values(&r.singleton_after)?;
                let expected_s = if first {
                    if standard_first {
                        s.value.as_ref().filter(|_| {
                            s.input
                                .as_ref()
                                .is_some_and(|i| i.block.length <= MAX_BLOCK_BYTES as u64)
                        })
                    } else if read_source {
                        s.value.as_ref()
                    } else {
                        None
                    }
                } else {
                    s.value.as_ref()
                };
                let expected_c = if first {
                    if standard_first {
                        c.value.as_ref().filter(|_| {
                            c.input
                                .as_ref()
                                .is_some_and(|i| i.block.length <= MAX_BLOCK_BYTES as u64)
                        })
                    } else if !read_source {
                        c.value.as_ref()
                    } else {
                        None
                    }
                } else {
                    c.value.as_ref()
                };
                if new_s != expected_s
                    || new_c != expected_c
                    || r.before != r.after
                    || !r.outputs.is_empty()
                    || r.counts.mutations != 0
                    || r.counts.output_blocks != 0
                    || r.counts.output_encoded_bytes != 0
                {
                    return Err(invariant_violation(
                        "singleton final comparison semantics differ",
                    ));
                }
                return Ok(None);
            }
            let (old_s, old_c) = state_values(&r.singleton_before)?;
            if old_s != s.value.as_ref() || old_c != c.value.as_ref() {
                return Err(invariant_violation(
                    "singleton final Emit observations incomplete",
                ));
            }
            let after_side = |obs: &Observation, prior: &SideCursor| -> Result<SideCursor> {
                Ok(if obs.value.is_some() {
                    SideCursor::After {
                        key_b64url: URL_SAFE_NO_PAD.encode(key),
                        position: obs
                            .position
                            .as_ref()
                            .ok_or_else(|| {
                                invariant_violation("singleton final value position missing")
                            })?
                            .clone(),
                    }
                } else if obs.next_key.is_none() {
                    SideCursor::End
                } else {
                    prior.clone()
                })
            };
            if r.after
                != (MergeCursor {
                    global: GlobalCut::After {
                        key_b64url: URL_SAFE_NO_PAD.encode(key),
                    },
                    source: after_side(s, &r.before.source)?,
                    current: after_side(c, &r.before.current)?,
                })
            {
                return Err(invariant_violation("singleton final Emit cursor differs"));
            }
            let result = match (s.value.as_ref().filter(|s| !s.tombstone), c.value.as_ref()) {
                (Some(s), c) => Some((
                    false,
                    c.filter(|c| {
                        !c.tombstone
                            && c.value_length == s.value_length
                            && c.value_sha256 == s.value_sha256
                    })
                    .map_or(f.result_logical_sequence, |c| c.generation),
                    s.value_length,
                    s.value_sha256.clone(),
                )),
                (None, Some(c)) => Some((
                    true,
                    if c.tombstone {
                        c.generation
                    } else {
                        f.result_logical_sequence
                    },
                    0,
                    prefixed_sha256(b""),
                )),
                (None, None) if f.mode == Mode::Absent => s
                    .value
                    .as_ref()
                    .map(|s| (true, s.generation, 0, prefixed_sha256(b""))),
                _ => None,
            };
            if r.outputs.len() != usize::from(result.is_some())
                || r.counts.output_blocks != r.outputs.len() as u64
                || r.counts.output_encoded_bytes
                    != r.outputs.iter().map(|o| o.block.length).sum::<u64>()
                || r.counts.mutations
                    != u64::from(
                        result
                            .as_ref()
                            .is_some_and(|(_, g, _, _)| *g == f.result_logical_sequence),
                    )
            {
                return Err(invariant_violation("singleton final output counts differ"));
            }
            Ok(result)
        },
    )?;
    if let Some(spec) = output_spec.value().as_ref() {
        let output = r
            .outputs
            .first()
            .ok_or_else(|| invariant_violation("singleton final output missing"))?;
        let leaf = verify_output(
            io,
            chunk.finish(),
            output,
            key,
            f.result_logical_sequence,
            spec,
            history,
        )
        .await?;
        // All value owners were dropped by verify_output before page insertion.
        let mut chunk = FinalMicrochunk::begin(totals, 0, io)?;
        builder
            .push_directory_leaf(
                io,
                &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
                &leaf,
            )
            .await?;
    }
    Ok(())
}

async fn verify_output(
    io: &mut RestorePhysicalIo<'_>,
    totals: &mut FinalStreamTotals,
    output: &OutputWitness,
    key: &[u8],
    sequence: u64,
    spec: &(bool, u64, u64, String),
    history: &mut WorkingValue<super::super::super::super::logical_v2::RestoreHistory>,
) -> Result<WorkingValue<directory::Leaf>> {
    let store = io.store();
    let mut chunk = FinalMicrochunk::begin(totals, 0, io)?;
    let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
    let leaf = decode_with_reservation(io, &mut route, Some(MODEL_BYTES), || {
        let mut digest = [0; 32];
        hex::decode_to_slice(
            super::raw_digest(&output.directory_leaf.digest)?,
            &mut digest,
        )
        .map_err(|_| invariant_violation("singleton final output digest differs"))?;
        if binary(&output.directory_leaf.first_key_b64url)? != key
            || binary(&output.directory_leaf.last_key_b64url)? != key
            || output.directory_leaf.rows != 1
        {
            return Err(invariant_violation("singleton final output leaf differs"));
        }
        Ok(directory::Leaf {
            first: key.to_vec(),
            last: key.to_vec(),
            rows: 1,
            bytes: output.directory_leaf.bytes,
            digest,
        })
    })?;
    let descriptor = physical::restore_io::read_final_descriptor(io, &mut route, &leaf).await?;
    drop(decode_with_reservation(
        io,
        &mut route,
        Some(MODEL_BYTES),
        || {
            let p = directory::restore::Position {
                leaf: leaf.value().clone(),
                path: vec![],
            };
            let input = input_witness(store, "", &p, &descriptor)?;
            if input.descriptor != output.descriptor
                || input.index != output.index
                || input.block != output.block
                || descriptor.value().segment.segment_id != output.output_id
                || descriptor.value().segment.logical_sequence != sequence
                || descriptor.value().block.offset != 0
                || descriptor.value().block.checksum_sha256
                    != descriptor.value().segment.checksum_sha256
                || descriptor.value().block.length != descriptor.value().segment.segment_size_bytes
            {
                return Err(invariant_violation(
                    "singleton final output physical identity differs",
                ));
            }
            Ok(())
        },
    )?);
    let mut chunk = payload_chunk(chunk.finish(), io, &descriptor)?;
    let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
    let rows = physical::restore_io::read_final_payload(io, &mut route, &descriptor).await?;
    let row = rows
        .value()
        .first()
        .ok_or_else(|| invariant_violation("singleton final output row missing"))?;
    let bytes = row.value.as_deref().unwrap_or_default();
    let hash = hash_with_reservation(io, &mut route, 64 * 1024, bytes.len(), || {
        Ok(prefixed_sha256(bytes))
    })?;
    drop(decode_with_reservation(
        io,
        &mut route,
        Some(64 * 1024),
        || {
            if rows.value().len() != 1
                || row.key != key
                || row.tombstone != spec.0
                || row.generation != spec.1
                || bytes.len() as u64 != spec.2
                || *hash.value() != spec.3
                || row.logical_sequence != sequence
                || row.logical_ordinal != 0
                || row.origin_sequence.is_some()
                || row.record_kind != super::SEGMENT_RECORD_KV
            {
                return Err(invariant_violation(
                    "singleton final output semantics differ",
                ));
            }
            Ok(())
        },
    )?);
    history.push_restore_history_rows(io, &mut route, &rows)?;
    drop(rows);
    Ok(leaf)
}

fn state_values(
    state: &SingletonState,
) -> Result<(
    Option<&SingletonValueWitness>,
    Option<&SingletonValueWitness>,
)> {
    match state {
        SingletonState::Pending {
            source, current, ..
        } => Ok((source.as_deref(), current.as_deref())),
        _ => Err(invariant_violation("singleton final observations missing")),
    }
}
