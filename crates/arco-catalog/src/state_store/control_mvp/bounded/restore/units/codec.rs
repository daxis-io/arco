//! Private admitted unit-record codec. Record decoding establishes no authority.
use super::{
    CatalogError, ControlMvpRestoreReceiptV1, DirectoryPathEntry, InputWitness, MergeCursor,
    OutputWitness, RestorePhysicalIo, RestorePhysicalRoute, RestoreProgressSelectorV1,
    RestoreProgressV1, Result, SideCursor, SingletonValueWitness, UNIT_RECORD_BYTES, WorkingValue,
    decode_with_reservation, invariant_violation, physical,
};
use physical::restore_io::{AccountedBytes, encode_with_reservation};
use serde::{Serialize, de::DeserializeOwned};

pub(super) mod hashes;

pub(super) fn validate_progress(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    expected: &WorkingValue<super::ExpectedPlan<'_>>,
    progress: &WorkingValue<RestoreProgressV1>,
) -> Result<WorkingValue<()>> {
    let same_owner = expected.is_owned_by(io) && progress.is_owned_by(io);
    let result = (|| {
        let preflight = encode_with_reservation(io, route, 64 * 1024, || {
            if !same_owner {
                return Err(invariant_violation("progress validation ownership differs"));
            }
            if usize::BITS != 64
                || typed_backing_bound().is_none()
                || !bounded_cursor(&progress.value().cursor)
            {
                return Err(invariant_violation(
                    "progress validation shape exceeds admission",
                ));
            }
            let count = compact_record_count(progress.value())?;
            expected
                .value()
                .prefix
                .len()
                .checked_add(128)
                .and_then(|bytes| bytes.checked_mul(4))
                .and_then(|path| count.encoded.checked_mul(4)?.checked_add(path))
                .and_then(|bytes| bytes.checked_add(64 * 1024))
                .ok_or_else(|| invariant_violation("progress validation reservation overflow"))
        })?;
        let reservation = *preflight.value();
        drop(preflight);
        let genesis = if progress.value().next_ordinal == 0 {
            let none = hashes::genesis_none(io, route, progress)?;
            let chain = hashes::genesis_chain(io, route, progress, &none)?;
            Some((none, chain))
        } else {
            None
        };
        decode_with_reservation(io, route, Some(reservation), || {
            super::validate_progress_shape_with_genesis(
                expected.value(),
                progress.value(),
                genesis.as_ref().map(|(_, chain)| chain.value().as_str()),
            )
        })
    })();
    if result.is_err() {
        io.stop(route);
    }
    result
}

pub(super) fn validate_selector(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    expected: &WorkingValue<super::ExpectedPlan<'_>>,
    selector: &WorkingValue<RestoreProgressSelectorV1>,
) -> Result<WorkingValue<()>> {
    let same_owner = expected.is_owned_by(io) && selector.is_owned_by(io);
    let result = decode_with_reservation(io, route, Some(64 * 1024), || {
        if !same_owner {
            return Err(invariant_violation("selector validation ownership differs"));
        }
        if usize::BITS != 64 || typed_backing_bound().is_none() {
            return Err(invariant_violation(
                "selector validation shape exceeds admission",
            ));
        }
        super::validate_selector_shape(expected.value(), selector.value())
    });
    if result.is_err() {
        io.stop(route);
    }
    result
}

pub(super) fn validate_receipt(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    expected: &WorkingValue<super::ExpectedPlan<'_>>,
    receipt: &WorkingValue<ControlMvpRestoreReceiptV1>,
) -> Result<WorkingValue<()>> {
    let same_owner = expected.is_owned_by(io) && receipt.is_owned_by(io);
    let result = (|| {
        let preflight = encode_with_reservation(io, route, 64 * 1024, || {
            let value = receipt.value();
            if !same_owner {
                return Err(invariant_violation("receipt validation ownership differs"));
            }
            if usize::BITS != 64
                || typed_backing_bound().is_none()
                || size_of::<[Option<WorkingValue<String>>; 32]>() + size_of::<[&str; 32]>()
                    > 64 * 1024
                || value.source_inputs.len() > 16
                || value.current_inputs.len() > 16
                || value.outputs.len() > 32
                || !bounded_cursor(&value.before)
                || !bounded_cursor(&value.after)
            {
                return Err(invariant_violation(
                    "receipt validation shape exceeds admission",
                ));
            }
            compact_record_count(value)?
                .encoded
                .checked_mul(6)
                .and_then(|bytes| bytes.checked_add(64 * 1024))
                .ok_or_else(|| invariant_violation("receipt validation reservation overflow"))
        })?;
        let reservation = *preflight.value();
        drop(preflight);
        let body = hashes::receipt_body(io, route, receipt)?;
        let chain = hashes::receipt_chain(io, route, receipt)?;
        let mut ids: [Option<WorkingValue<String>>; 32] = std::array::from_fn(|_| None);
        for (slot, output) in ids.iter_mut().zip(&receipt.value().outputs) {
            *slot = Some(hashes::output_id(
                io,
                route,
                expected,
                receipt.value().ordinal,
                output.part,
                output.role,
            )?);
        }
        let borrowed: [&str; 32] = std::array::from_fn(|index| {
            ids.get(index)
                .and_then(Option::as_ref)
                .map_or("", |id| id.value().as_str())
        });
        decode_with_reservation(io, route, Some(reservation), || {
            let output_ids = borrowed
                .get(..receipt.value().outputs.len())
                .ok_or_else(|| {
                    invariant_violation("receipt output identity count exceeds admission")
                })?;
            super::validate_receipt_shape_with_hashes(
                expected.value(),
                receipt.value(),
                body.value(),
                chain.value(),
                output_ids,
            )
        })
    })();
    if result.is_err() {
        io.stop(route);
    }
    result
}

pub(super) fn encode_selector(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    value: &WorkingValue<RestoreProgressSelectorV1>,
) -> Result<WorkingValue<bytes::Bytes>> {
    encode_record(io, route, value, true)
}

pub(super) fn encode_progress(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    value: &WorkingValue<RestoreProgressV1>,
) -> Result<WorkingValue<bytes::Bytes>> {
    encode_record(io, route, value, bounded_cursor(&value.value().cursor))
}

pub(super) fn encode_receipt(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    value: &WorkingValue<ControlMvpRestoreReceiptV1>,
) -> Result<WorkingValue<bytes::Bytes>> {
    let receipt = value.value();
    let bounded = receipt.source_inputs.len() <= 16
        && receipt.current_inputs.len() <= 16
        && receipt.outputs.len() <= 32
        && bounded_cursor(&receipt.before)
        && bounded_cursor(&receipt.after);
    encode_record(io, route, value, bounded)
}

fn bounded_cursor(cursor: &MergeCursor) -> bool {
    [&cursor.source, &cursor.current]
        .into_iter()
        .all(|side| match side {
            SideCursor::After { position, .. } => position.path.len() <= 8,
            SideCursor::Start | SideCursor::End => true,
        })
}

#[allow(
    clippy::redundant_clone,
    reason = "the first Bytes clone must promote its Shared header inside admission"
)]
fn encode_record<T: Serialize>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    value: &WorkingValue<T>,
    bounded: bool,
) -> Result<WorkingValue<bytes::Bytes>> {
    let same_owner = value.is_owned_by(io);
    let result = (|| {
        let preflight = encode_with_reservation(io, route, 64 * 1024, || {
            if !same_owner {
                return Err(invariant_violation(
                    "restore record model ownership differs",
                ));
            }
            if !bounded || usize::BITS != 64 || typed_backing_bound().is_none() {
                return Err(CatalogError::MaintenanceBackpressure {
                    message: "restore record encoding shape exceeds admission".into(),
                });
            }
            compact_record_count(value.value())
        })?;
        let count = *preflight.value();
        drop(preflight);
        let reservation = count.output_reservation();
        encode_with_reservation(io, route, reservation.unwrap_or(64 * 1024), || {
            if reservation.is_none() {
                return Err(CatalogError::MaintenanceBackpressure {
                    message: "restore record encoding bound overflow".into(),
                });
            }
            let encoded = jcs_fixed(value.value(), count.encoded)?;
            if encoded.len() != count.encoded
                || count_backslashes(&encoded) != count.raw_backslashes
            {
                return Err(invariant_violation(
                    "restore record canonical count differs",
                ));
            }
            // Account the first Shared header here; transport clones then
            // retain the same guarded backing without allocating a header.
            let bytes = bytes::Bytes::from(encoded);
            Ok(bytes.clone())
        })
    })();
    if result.is_err() {
        io.stop(route);
    }
    result
}

#[derive(Clone, Copy, Default)]
struct CompactRecordCount {
    encoded: usize,
    raw_backslashes: usize,
}

impl CompactRecordCount {
    fn output_reservation(self) -> Option<usize> {
        if self.encoded == 0
            || self.encoded > UNIT_RECORD_BYTES
            || self.raw_backslashes > self.encoded
        {
            return None;
        }
        self.encoded
            .checked_mul(21)?
            .checked_add(self.raw_backslashes.checked_mul(16)?)?
            .checked_add(2 * 1024 * 1024)?
            .checked_add(24)
    }
}

impl std::io::Write for CompactRecordCount {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .encoded
            .checked_add(bytes.len())
            .filter(|next| *next <= UNIT_RECORD_BYTES)
            .ok_or(std::io::ErrorKind::WriteZero)?;
        self.raw_backslashes = self
            .raw_backslashes
            .checked_add(count_backslashes(bytes))
            .ok_or(std::io::ErrorKind::WriteZero)?;
        self.encoded = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[allow(
    clippy::naive_bytecount,
    reason = "bounded allocation-free scan with the qualified dependency set"
)]
fn count_backslashes(bytes: &[u8]) -> usize {
    bytes.iter().filter(|byte| **byte == b'\\').count()
}

fn compact_record_count<T: Serialize>(value: &T) -> Result<CompactRecordCount> {
    let mut count = CompactRecordCount::default();
    serde_json::to_writer(&mut count, value).map_err(|_| {
        CatalogError::MaintenanceBackpressure {
            message: "restore record compact count exceeds admission".into(),
        }
    })?;
    Ok(count)
}

pub(super) fn decode_selector(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    raw: &AccountedBytes,
) -> Result<WorkingValue<RestoreProgressSelectorV1>> {
    decode_jcs_exact(io, route, raw)
}

pub(super) fn decode_progress(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    raw: &AccountedBytes,
) -> Result<WorkingValue<RestoreProgressV1>> {
    decode_jcs_exact(io, route, raw)
}

pub(super) fn decode_receipt(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    raw: &AccountedBytes,
) -> Result<WorkingValue<ControlMvpRestoreReceiptV1>> {
    decode_jcs_exact(io, route, raw)
}

// Only the three concrete wrappers above can select a decoded type. The bound
// includes parsing before canonicalization; raw response ownership is separate.
fn decode_jcs_exact<T: DeserializeOwned + Serialize>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    raw: &AccountedBytes,
) -> Result<WorkingValue<T>> {
    let same_owner = raw.is_owned_by(io);
    let reservation = same_owner
        .then(|| unit_record_decode_reservation(raw.as_slice()))
        .flatten();
    let result = decode_with_reservation(io, route, Some(reservation.unwrap_or(64 * 1024)), || {
        if !same_owner {
            return Err(invariant_violation("restore record raw ownership differs"));
        }
        if reservation.is_none() {
            return Err(CatalogError::MaintenanceBackpressure {
                message: "restore record exceeds codec allocation admission".into(),
            });
        }
        let value: T = serde_json::from_slice(raw.as_slice())
            .map_err(|_| invariant_violation("invalid restore unit record JSON"))?;
        let canonical = jcs_fixed(&value, raw.as_slice().len() + 32)?;
        if canonical != raw.as_slice() {
            return Err(invariant_violation("restore unit record is not exact JCS"));
        }
        Ok(value)
    });
    if result.is_err() {
        io.stop(route);
    }
    result
}

#[allow(
    clippy::naive_bytecount,
    reason = "bounded allocation-free scan with the qualified dependency set"
)]
fn unit_record_decode_reservation(raw: &[u8]) -> Option<usize> {
    if usize::BITS != 64 || raw.is_empty() || raw.len() > UNIT_RECORD_BYTES {
        return None;
    }
    typed_backing_bound()?;
    let p = raw.len();
    let escapes = raw.iter().filter(|byte| **byte == b'\\').count();
    let failure = p.checked_mul(60)?.checked_add(512 * 1024)?;
    let success = p
        .checked_mul(29)?
        .checked_add(escapes.checked_mul(16)?)?
        .checked_add(2 * 1024 * 1024)?;
    Some(failure.max(success))
}

fn typed_backing_bound() -> Option<usize> {
    size_of::<InputWitness>()
        .checked_mul(32)?
        .checked_add(size_of::<OutputWitness>().checked_mul(32)?)?
        .checked_add(size_of::<DirectoryPathEntry>().checked_mul(32)?)?
        .checked_add(size_of::<SingletonValueWitness>().checked_mul(4)?)?
        .checked_add(4096)
        .filter(|bytes| *bytes <= 64 * 1024)
}

// Cursor's slice cannot grow. Both the intermediate serializer error and this
// static canonical serialization error are included in the fixed reservation.
fn jcs_fixed<T: Serialize>(value: &T, capacity: usize) -> Result<Vec<u8>> {
    let mut bytes = vec![0; capacity];
    let mut writer = std::io::Cursor::new(bytes.as_mut_slice());
    serde_jcs::to_writer(&mut writer, value)
        .map_err(|_| invariant_violation("restore unit canonical writer exhausted"))?;
    let length = usize::try_from(writer.position())
        .map_err(|_| invariant_violation("restore unit canonical length overflow"))?;
    bytes.truncate(length);
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::super::{CumulativeSemanticCounts, ExpectedPlan, behavioral_tests, prefixed_sha256};
    use super::*;

    #[test]
    fn admitted_unit_encoding_counter_escape_cap_and_first_clone_qualification() {
        let controls: String = (0_u8..32).map(char::from).collect();
        for value in [
            String::new(),
            "quote\" reverse\\ solidus/🔥\u{2028}".into(),
            controls,
        ] {
            let canonical = serde_jcs::to_vec(&value).expect("independent JCS");
            let mut counted = None;
            let observed =
                allocation_counter::measure(|| counted = Some(compact_record_count(&value)));
            let count = counted.expect("executed").expect("count");
            assert_eq!(count.encoded, canonical.len());
            assert_eq!(count.raw_backslashes, count_backslashes(&canonical));
            assert_eq!(observed.bytes_total, 0, "compact count borrows all data");
        }
        for value in [0, 1, u64::MAX, 1_u64 << 53] {
            let count = compact_record_count(&value).expect("integer");
            let canonical = jcs_fixed(&value, count.encoded).expect("integer JCS");
            assert_eq!(canonical, value.to_string().as_bytes());
            assert_eq!(count.raw_backslashes, 0);
        }
        let oversized = "x".repeat(UNIT_RECORD_BYTES);
        let mut rejected = false;
        let failed =
            allocation_counter::measure(|| rejected = compact_record_count(&oversized).is_err());
        assert!(rejected && failed.bytes_total < 64 * 1024);
        let vec = vec![1_u8; 128];
        assert_eq!(vec.len(), vec.capacity());
        let mut bytes = None;
        let conversion = allocation_counter::measure(|| bytes = Some(bytes::Bytes::from(vec)));
        assert_eq!(conversion.bytes_total, 0);
        let bytes = bytes.expect("owned");
        let mut shared = None;
        let first = allocation_counter::measure(|| shared = Some(bytes.clone()));
        let second = allocation_counter::measure(|| drop(bytes.clone()));
        assert_eq!(first.bytes_total, 24);
        assert_eq!(second.bytes_total, 0);
        drop(shared);
        println!(
            "unit encoding counter failure={} first_clone={} second_clone={} conversion={}",
            failed.bytes_total, first.bytes_total, second.bytes_total, conversion.bytes_total
        );
    }

    #[tokio::test]
    async fn admitted_unit_encoding_parity_drift_stops_before_shared_bytes_or_retry() {
        use crate::workspace_io_budget::WorkspaceIoBudget;
        use physical::restore_io::UnitPayloadAdmission;
        struct Drifting(std::cell::Cell<usize>, u8);
        impl Serialize for Drifting {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                let prior = self.0.get();
                self.0.set(prior + 1);
                serializer.serialize_str(match (prior, self.1) {
                    (0, 1) => "\"",
                    (_, 1) => "\\",
                    (0, _) => "a",
                    (_, 2) => "larger than the fixed output",
                    _ => "",
                })
            }
        }
        let (store, _) =
            crate::state_store::control_mvp::bounded::restore::tests::inspection_fixture().await;
        for foreign in [false, true] {
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut origin = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let value = decode_with_reservation(
                if foreign { &mut origin } else { &mut io },
                &mut route,
                Some(1024),
                || Ok(Drifting(std::cell::Cell::new(0), 0)),
            )
            .expect("model");
            assert!(encode_record(&mut io, &mut route, &value, foreign).is_err());
            assert_eq!(
                value.value().0.get(),
                0,
                "owner or shape rejection must precede serialization"
            );
        }
        for drift in 0..3 {
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let value = decode_with_reservation(&mut io, &mut route, Some(1024), || {
                Ok(Drifting(std::cell::Cell::new(0), drift))
            })
            .expect("model");
            assert!(matches!(
                encode_record(&mut io, &mut route, &value, true),
                Err(CatalogError::InvariantViolation { .. })
            ));
            assert_eq!(value.value().0.get(), 2);
            assert_eq!(io.encoding_evidence().0, 2);
            assert!(encode_record(&mut io, &mut route, &value, true).is_err());
            assert_eq!(value.value().0.get(), 2);
        }
    }

    #[test]
    fn admitted_unit_codec_jcs_fixed_layout_qualification() {
        use std::{
            alloc::Layout,
            collections::BTreeMap,
            io::Cursor,
            mem::{MaybeUninit, align_of},
        };
        assert_eq!(usize::BITS, 64);
        let entry_bound = size_of::<BTreeMap<Vec<u8>, Vec<u8>>>()
            + 2 * size_of::<Vec<u8>>()
            + size_of::<bool>()
            + 4 * (align_of::<usize>() - 1);
        assert!(entry_bound <= 128, "all field orders including padding");
        // Pinned repr(C) Rust 1.88 B-tree layout, with a conservative pointer
        // substitute for Option<NonNull<InternalNode>> and node child pointers.
        let leaf = Layout::new::<usize>()
            .extend(Layout::new::<MaybeUninit<u16>>())
            .expect("parent index")
            .0
            .extend(Layout::new::<u16>())
            .expect("length")
            .0
            .extend(Layout::array::<MaybeUninit<Vec<u8>>>(11).expect("keys"))
            .expect("key layout")
            .0
            .extend(Layout::array::<MaybeUninit<Vec<u8>>>(11).expect("values"))
            .expect("value layout")
            .0
            .pad_to_align();
        let internal = leaf
            .extend(Layout::array::<usize>(12).expect("children"))
            .expect("internal")
            .0
            .pad_to_align();
        assert!(leaf.size() <= 704 && internal.size() <= 704);
        let mut empty: [u8; 0] = [];
        let mut cursor = Cursor::new(empty.as_mut_slice());
        let boxed = allocation_counter::measure(|| {
            drop(std::hint::black_box(Box::new(&mut cursor)));
        });
        assert_eq!(boxed.bytes_total, 8);
        let first_use = std::thread::spawn(|| allocation_counter::measure(|| {}).bytes_total)
            .join()
            .expect("fresh TLS");
        assert_eq!(first_use, 0);
        for reverse in [false, true] {
            let mut entries = (0_u8..23)
                .map(|key| (vec![key], Vec::<u8>::new()))
                .collect::<Vec<_>>();
            if reverse {
                entries.reverse();
            }
            let mut map = BTreeMap::new();
            let observed = allocation_counter::measure(|| {
                for (key, value) in entries {
                    map.insert(key, value);
                }
            });
            assert_eq!(map.len(), 23);
            assert!(observed.bytes_total <= 4 * 704);
            println!(
                "unit codec BTree23 reverse={reverse} bytes={}",
                observed.bytes_total
            );
        }
        println!(
            "unit codec JCS layout entry_upper={entry_bound} leaf={} internal={} box={} first_measure={first_use}",
            leaf.size(),
            internal.size(),
            boxed.bytes_total
        );
    }

    #[tokio::test]
    async fn admitted_unit_codec_max_graph_and_terminal_writer_failure() {
        fn count(value: &serde_json::Value, depth: usize, counts: &mut [usize; 5]) {
            match value {
                serde_json::Value::Object(fields) => {
                    counts[0] += fields.len();
                    counts[1] += 1;
                    counts[4] = counts[4].max(depth + 1);
                    for child in fields.values() {
                        count(child, depth + 1, counts);
                    }
                }
                serde_json::Value::Array(values) => {
                    counts[2] += values.len();
                    counts[3] += 1;
                    for child in values {
                        count(child, depth, counts);
                    }
                }
                _ => {}
            }
        }
        let (_, plan) =
            crate::state_store::control_mvp::bounded::restore::tests::inspection_fixture().await;
        let digest = prefixed_sha256(b"graph fixture");
        let expected = ExpectedPlan::from_selected(&plan, &digest).expect("expected");
        let receipt = behavioral_tests::codec_max_receipt(&expected);
        let tree = serde_json::to_value(&receipt).expect("independent shape");
        let mut counts = [0; 5];
        count(&tree, 0, &mut counts);
        assert_eq!(counts, [1762, 391, 96, 7, 5]);
        let canonical = serde_jcs::to_vec(&receipt).expect("fixture JCS");
        let mut compact = None;
        let counted =
            allocation_counter::measure(|| compact = Some(compact_record_count(&receipt)));
        let count = compact.expect("count executed").expect("max count");
        assert_eq!(count.encoded, canonical.len());
        assert_eq!(count.raw_backslashes, count_backslashes(&canonical));
        assert_eq!(counted.bytes_total, 0);
        println!(
            "unit encoding maximum graph count allocated={}",
            counted.bytes_total
        );
        let mut result = None;
        let observed = allocation_counter::measure(|| {
            result = Some(jcs_fixed(&receipt, canonical.len() - 1));
        });
        assert!(result.expect("executed").is_err());
        assert!(
            observed.bytes_total
                < unit_record_decode_reservation(&canonical).expect("bound") as u64
        );
        // Omitting nullable last_receipt adds exactly 20 bytes before equality
        // rejects it; the fixed writer must reach equality rather than grow.
        let mut progress_tree = serde_json::json!({
            "record_type":"control_mvp_restore_progress", "version":1,
            "plan_sha256":digest, "identity":plan.fields.identity,
            "owner_generation":1, "next_ordinal":0, "receipt_count":0,
            "last_receipt":null, "chain_sha256":"hash", "cursor":{
                "global":{"kind":"start"}, "source":{"kind":"start"}, "current":{"kind":"start"}},
            "singleton_state":{"kind":"none"}, "terminal":false,
            "cumulative_counts":CumulativeSemanticCounts::default()
        });
        let complete = serde_jcs::to_vec(&progress_tree).expect("complete");
        progress_tree
            .as_object_mut()
            .expect("object")
            .remove("last_receipt");
        let omitted = serde_jcs::to_vec(&progress_tree).expect("omitted");
        let parsed: RestoreProgressV1 = serde_json::from_slice(&omitted).expect("optional default");
        assert_eq!(complete.len(), omitted.len() + 20);
        assert_eq!(
            jcs_fixed(&parsed, omitted.len() + 32).expect("fixed allowance"),
            complete
        );
        println!(
            "unit codec max graph={counts:?} terminal_WriteZero_allocated={}",
            observed.bytes_total
        );
    }

    #[test]
    fn admitted_unit_codec_fixed_writer_and_layout() {
        assert_eq!(usize::BITS, 64);
        let backing = typed_backing_bound().expect("qualified typed backing");
        assert!(backing <= 64 * 1024);
        assert!(unit_record_decode_reservation(&[]).is_none());
        assert!(unit_record_decode_reservation(&vec![0; UNIT_RECORD_BYTES]).is_some());
        assert!(unit_record_decode_reservation(&vec![0; UNIT_RECORD_BYTES + 1]).is_none());
        assert_eq!(
            unit_record_decode_reservation(b"\\\\"),
            Some(2 * 29 + 2 * 16 + 2 * 1024 * 1024)
        );
        let value = "quoted\\\"value\u{7f}";
        let exact = serde_jcs::to_vec(value).expect("independent writer");
        assert_eq!(jcs_fixed(&value, exact.len()).expect("fits"), exact);
        assert!(jcs_fixed(&value, exact.len() - 1).is_err());
        println!("unit codec typed backing including 4096 slack={backing}");
    }
}
