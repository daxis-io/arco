//! Admitted unit hashes. A computed digest alone establishes no authority.
use super::super::{GenesisChainBody, GenesisReceiptNoneBody, ReceiptBody, ReceiptChainBody};
use super::physical::restore_io::hash_with_reservation;
use super::{
    AccountedBytes, ControlMvpRestoreReceiptV1, RestorePhysicalIo, RestorePhysicalRoute,
    RestoreProgressV1, Result, UNIT_RECORD_BYTES, WorkingValue, bounded_cursor,
    compact_record_count, count_backslashes, encode_with_reservation, invariant_violation,
    jcs_fixed, typed_backing_bound,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

pub(in super::super) fn output_id(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    expected: &WorkingValue<super::super::ExpectedPlan<'_>>,
    ordinal: u64,
    part: u32,
    role: super::physical::Role,
) -> Result<WorkingValue<String>> {
    let seed = expected.value().candidate_seed_sha256;
    let bounded = seed
        .strip_prefix("sha256:")
        .is_some_and(crate::state_store::control_mvp::valid_raw_digest);
    tagged::<true, _>(
        io,
        route,
        expected.is_owned_by(io),
        bounded,
        b"arco/control-v2/restore-output-v1",
        &super::super::OutputIdBody {
            candidate_seed_sha256: seed,
            ordinal,
            part,
            role,
        },
    )
}

pub(in super::super) fn receipt_body(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    value: &WorkingValue<ControlMvpRestoreReceiptV1>,
) -> Result<WorkingValue<String>> {
    let receipt = value.value();
    tagged::<false, _>(
        io,
        route,
        value.is_owned_by(io),
        bounded_receipt(receipt),
        b"arco/control-v2/restore-receipt-body-v1",
        &ReceiptBody {
            record_type: &receipt.record_type,
            version: receipt.version,
            plan_sha256: &receipt.plan_sha256,
            identity: &receipt.identity,
            owner_generation: receipt.owner_generation,
            ordinal: receipt.ordinal,
            predecessor_receipt_sha256: &receipt.predecessor_receipt_sha256,
            predecessor_chain_sha256: &receipt.predecessor_chain_sha256,
            before: &receipt.before,
            after: &receipt.after,
            singleton_before: &receipt.singleton_before,
            singleton_after: &receipt.singleton_after,
            source_inputs: &receipt.source_inputs,
            current_inputs: &receipt.current_inputs,
            outputs: &receipt.outputs,
            prefix_cumulative_counts: &receipt.prefix_cumulative_counts,
            counts: &receipt.counts,
        },
    )
}

pub(in super::super) fn receipt_chain(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    value: &WorkingValue<ControlMvpRestoreReceiptV1>,
) -> Result<WorkingValue<String>> {
    let receipt = value.value();
    tagged::<false, _>(
        io,
        route,
        value.is_owned_by(io),
        bounded_receipt(receipt),
        b"arco/control-v2/restore-receipt-chain-v1",
        &ReceiptChainBody {
            plan_sha256: &receipt.plan_sha256,
            owner_generation: receipt.owner_generation,
            ordinal: receipt.ordinal,
            predecessor_chain_sha256: &receipt.predecessor_chain_sha256,
            receipt_body_sha256: &receipt.receipt_body_sha256,
        },
    )
}

pub(in super::super) fn genesis_none(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    value: &WorkingValue<RestoreProgressV1>,
) -> Result<WorkingValue<String>> {
    let progress = value.value();
    tagged::<false, _>(
        io,
        route,
        value.is_owned_by(io),
        bounded_cursor(&progress.cursor),
        b"arco/control-v2/restore-receipt-none-v1",
        &GenesisReceiptNoneBody {
            plan_sha256: &progress.plan_sha256,
            identity: &progress.identity,
            owner_generation: progress.owner_generation,
        },
    )
}

pub(in super::super) fn genesis_chain(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    value: &WorkingValue<RestoreProgressV1>,
    genesis: &WorkingValue<String>,
) -> Result<WorkingValue<String>> {
    let progress = value.value();
    tagged::<false, _>(
        io,
        route,
        value.is_owned_by(io) && genesis.is_owned_by(io),
        bounded_cursor(&progress.cursor),
        b"arco/control-v2/restore-receipt-genesis-chain-v1",
        &GenesisChainBody {
            plan_sha256: &progress.plan_sha256,
            identity: &progress.identity,
            owner_generation: progress.owner_generation,
            genesis_receipt_raw_sha256: genesis.value(),
        },
    )
}

fn bounded_receipt(receipt: &ControlMvpRestoreReceiptV1) -> bool {
    receipt.source_inputs.len() <= 16
        && receipt.current_inputs.len() <= 16
        && receipt.outputs.len() <= 32
        && bounded_cursor(&receipt.before)
        && bounded_cursor(&receipt.after)
}

// Only the five concrete projections above select T. All fields remain borrowed
// from guarded models; these computations confer no selected-plan authority.
fn tagged<const RAW_HEX: bool, T: Serialize>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    same_owner: bool,
    bounded: bool,
    tag: &[u8],
    body: &T,
) -> Result<WorkingValue<String>> {
    let result = (|| {
        let preflight = encode_with_reservation(io, route, 64 * 1024, || {
            if !same_owner {
                return Err(invariant_violation("restore hash model ownership differs"));
            }
            if !bounded || usize::BITS != 64 || typed_backing_bound().is_none() {
                return Err(invariant_violation(
                    "restore hash model shape exceeds admission",
                ));
            }
            compact_record_count(body)
        })?;
        let count = *preflight.value();
        drop(preflight);
        let reservation = count
            .output_reservation()
            .and_then(|bytes| bytes.checked_sub(24))
            .and_then(|bytes| bytes.checked_add(71));
        let input_bytes = tag
            .len()
            .checked_add(1)
            .and_then(|n| n.checked_add(count.encoded));
        hash_with_reservation(
            io,
            route,
            reservation.unwrap_or(64 * 1024),
            input_bytes.unwrap_or(usize::MAX),
            || {
                if reservation.is_none() || input_bytes.is_none() {
                    return Err(invariant_violation("restore hash bound overflow"));
                }
                let encoded = jcs_fixed(body, count.encoded)?;
                if encoded.len() != count.encoded
                    || count_backslashes(&encoded) != count.raw_backslashes
                {
                    return Err(invariant_violation("restore hash canonical count differs"));
                }
                let mut result = digest(Some(tag), &encoded);
                if RAW_HEX {
                    drop(result.drain(..7));
                }
                Ok(result)
            },
        )
    })();
    if result.is_err() {
        io.stop(route);
    }
    result
}

pub(in super::super) fn raw_read(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    raw: &AccountedBytes,
) -> Result<WorkingValue<String>> {
    raw_digest(io, route, raw.is_owned_by(io), raw.as_slice())
}

pub(in super::super) fn raw_encoded(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    raw: &WorkingValue<bytes::Bytes>,
) -> Result<WorkingValue<String>> {
    raw_digest(io, route, raw.is_owned_by(io), raw.value())
}

fn raw_digest(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    same_owner: bool,
    raw: &[u8],
) -> Result<WorkingValue<String>> {
    hash_with_reservation(io, route, 64 * 1024, raw.len(), || {
        if !same_owner || usize::BITS != 64 || raw.is_empty() || raw.len() > UNIT_RECORD_BYTES {
            return Err(invariant_violation("restore raw hash exceeds admission"));
        }
        Ok(digest(None, raw))
    })
}

// Sha256 owns fixed stack state. The result has one allocation of exactly 71
// bytes; push cannot grow it and no temporary hexadecimal String is allocated.
#[allow(
    clippy::indexing_slicing,
    reason = "both u8 nibbles are in 0..16, exactly the fixed alphabet length"
)]
fn digest(tag: Option<&[u8]>, bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut hash = Sha256::new();
    if let Some(tag) = tag {
        hash.update(tag);
        hash.update([0]);
    }
    hash.update(bytes);
    let mut result = String::with_capacity(71);
    result.push_str("sha256:");
    for byte in hash.finalize() {
        result.push(char::from(HEX[usize::from(byte >> 4)]));
        result.push(char::from(HEX[usize::from(byte & 15)]));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::super::physical::restore_io::{
        FinalMicrochunk, FinalStreamTotals, UnitPayloadAdmission,
    };
    use super::*;
    use crate::workspace_io_budget::WorkspaceIoBudget;

    #[test]
    fn admitted_unit_hash_fixed_state_and_single_allocation() {
        assert!(size_of::<Sha256>() <= 256);
        for bytes in [
            b"abc".as_slice(),
            &[0, 255, 0, 128],
            &vec![255; UNIT_RECORD_BYTES],
        ] {
            let mut actual = None;
            let observed = allocation_counter::measure(|| actual = Some(digest(None, bytes)));
            let actual = actual.expect("hash");
            assert_eq!(actual.capacity(), 71);
            assert_eq!(observed.bytes_total, 71);
            assert_eq!(actual, super::super::super::prefixed_sha256(bytes));
        }
        assert_eq!(
            digest(None, b"abc"),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_ne!(digest(Some(b"a"), b"bc"), digest(Some(b"ab"), b"c"));
        assert_ne!(digest(Some(b""), b"abc"), digest(None, b"abc"));
        println!(
            "unit hashes SHA256 state={} digest_allocation=71",
            size_of::<Sha256>()
        );
    }

    #[tokio::test]
    async fn admitted_unit_hash_preflight_and_parity_reject_before_hash() {
        struct Drifting(std::cell::Cell<usize>, u8);
        impl Serialize for Drifting {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                let call = self.0.get();
                self.0.set(call + 1);
                serializer.serialize_str(match (call, self.1) {
                    (0, 1) => "\"",
                    (_, 1) => "\\",
                    (0, _) => "a",
                    (_, 2) => "larger than the fixed output",
                    _ => "",
                })
            }
        }
        let (store, _) = super::super::super::super::tests::inspection_fixture().await;
        for case in 0..5 {
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let value = Drifting(std::cell::Cell::new(0), case);
            assert!(
                tagged::<false, _>(&mut io, &mut route, case != 3, case != 4, b"test", &value)
                    .is_err()
            );
            assert_eq!(value.0.get(), if case < 3 { 2 } else { 0 });
            assert_eq!(io.hashing_evidence(), (0, 0));
            assert!(tagged::<false, _>(&mut io, &mut route, true, true, b"test", &value).is_err());
            assert_eq!(value.0.get(), if case < 3 { 2 } else { 0 });
        }
    }

    #[tokio::test]
    async fn admitted_unit_hash_raw_limits_and_final_carry() {
        let (store, _) = super::super::super::super::tests::inspection_fixture().await;
        for case in 0..6 {
            let raw = bytes::Bytes::from(vec![
                255;
                match case {
                    0 => 0,
                    1 => UNIT_RECORD_BYTES + 1,
                    5 => UNIT_RECORD_BYTES,
                    _ => 1,
                }
            ]);
            let mut io = RestorePhysicalIo::new(
                &store,
                if case == 3 {
                    32 * 1024
                } else {
                    64 * 1024 * 1024
                },
                0,
            );
            let mut origin = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let owned = super::super::decode_with_reservation(
                if case == 2 { &mut origin } else { &mut io },
                &mut route,
                Some(raw.len() + 1024),
                || Ok(bytes::Bytes::from(raw.to_vec())),
            )
            .expect("raw owner");
            let mut totals = FinalStreamTotals::new();
            let carry = if case == 4 {
                64 * 1024 * 1024 - 32 * 1024
            } else {
                0
            };
            let mut chunk = FinalMicrochunk::begin(&mut totals, carry, &io).expect("final chunk");
            if case == 4 {
                route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            }
            let result = raw_encoded(&mut io, &mut route, &owned);
            if case == 5 {
                assert_eq!(result.expect("cap fits").value(), &digest(None, &raw));
                assert_eq!(io.hashing_evidence(), (1, UNIT_RECORD_BYTES as u64));
                assert_eq!(
                    io.native_work_evidence(),
                    super::super::super::super::super::super::cost::NativeWork::default()
                );
            } else {
                assert!(result.is_err(), "case {case}");
                assert_eq!(io.hashing_evidence(), (0, 0));
                assert!(raw_encoded(&mut io, &mut route, &owned).is_err());
            }
        }
    }
}
