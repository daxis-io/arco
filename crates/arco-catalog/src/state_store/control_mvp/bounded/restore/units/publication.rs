//! Durable unit control records only; no native driver or catalog publication authority.
use super::{
    ControlMvpRestoreReceiptV1, ExpectedPlan, RestoreControlRecord, RestorePhysicalIo,
    RestorePhysicalRoute, RestoreProgressSelectorV1, RestoreProgressV1, Result, SelectedProgress,
    UNIT_RECORD_BYTES, WorkingValue, codec, decode_with_reservation, invariant_violation,
    output_binding_string_bytes, physical, raw_digest, read_selected_progress,
    validate_persisted_output, validate_proposed_transition,
};
use physical::restore_io::{StandardRestoreOutput, write_restore_control_record};

#[derive(Clone, Copy)]
pub(super) struct RecoveryTarget {
    plan: [u8; 32],
    candidate: [u8; 32],
    prior: [u8; 32],
    proposed: [u8; 32],
    next_ordinal: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Reconciliation {
    Exact,
    Prior,
    Different,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Publication {
    Written,
    ExactSelected,
    Conflict(Reconciliation),
}

pub(super) struct PreparedUnit<'a, 'p> {
    expected: &'a WorkingValue<ExpectedPlan<'p>>,
    selected: &'a SelectedProgress,
    receipt: WorkingValue<bytes::Bytes>,
    progress: WorkingValue<bytes::Bytes>,
    selector: WorkingValue<bytes::Bytes>,
    target: RecoveryTarget,
}

impl PreparedUnit<'_, '_> {
    pub(super) fn recovery_target(&self) -> RecoveryTarget {
        self.target
    }
}

fn digest_bytes(prefixed: &str) -> Result<[u8; 32]> {
    let mut bytes = [0; 32];
    hex::decode_to_slice(raw_digest(prefixed)?, &mut bytes)
        .map_err(|_| invariant_violation("invalid unit recovery digest"))?;
    Ok(bytes)
}

#[allow(
    clippy::too_many_lines,
    reason = "retain guarded models and all encoded bodies through assembly"
)]
pub(super) fn assemble<'a, 'p>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    expected: &'a WorkingValue<ExpectedPlan<'p>>,
    selected: &'a SelectedProgress,
    receipt: &WorkingValue<ControlMvpRestoreReceiptV1>,
    progress: &WorkingValue<RestoreProgressV1>,
    outputs: &[StandardRestoreOutput],
) -> Result<PreparedUnit<'a, 'p>> {
    let result = (|| {
        drop(decode_with_reservation(io, route, Some(64 * 1024), || {
            if outputs.len() > 32 || outputs.len() != receipt.value().outputs.len() {
                return Err(invariant_violation(
                    "unit output product cardinality differs",
                ));
            }
            Ok(())
        })?);
        drop(validate_proposed_transition(
            io, route, expected, selected, receipt, progress,
        )?);
        for (witness, output) in receipt.value().outputs.iter().zip(outputs) {
            let reservation = output_binding_string_bytes(witness, output.descriptor.value())
                .filter(|bytes| *bytes <= UNIT_RECORD_BYTES)
                .and_then(|bytes| bytes.checked_mul(4)?.checked_add(64 * 1024));
            let copy =
                decode_with_reservation(io, route, Some(reservation.unwrap_or(64 * 1024)), || {
                    if reservation.is_none() {
                        return Err(invariant_violation("unit output copy exceeds admission"));
                    }
                    Ok(witness.clone())
                })?;
            drop(validate_persisted_output(
                io,
                route,
                expected,
                receipt.value().ordinal,
                &copy,
                output,
            )?);
        }
        let receipt = codec::encode_receipt(io, route, receipt)?;
        let progress_bytes = codec::encode_progress(io, route, progress)?;
        let progress_hash = codec::hashes::raw_encoded(io, route, &progress_bytes)?;
        let prior_selector = codec::decode_selector(io, route, &selected.selector_raw)?;
        let reservation = decode_with_reservation(io, route, Some(64 * 1024), || {
            [
                expected.value().prefix.len(),
                expected.value().plan_sha256.len(),
                expected.value().identity.restore_id().len(),
                expected.value().identity.domain().len(),
                progress_hash.value().len(),
            ]
            .into_iter()
            .try_fold(64_usize * 1024, |sum, length| {
                sum.checked_add(length.checked_mul(4)?)
            })
            .ok_or_else(|| invariant_violation("unit selector reservation overflow"))
        })?;
        let selector = decode_with_reservation(io, route, Some(*reservation.value()), || {
            Ok(RestoreProgressSelectorV1 {
                record_type: "control_mvp_restore_progress_selector".into(),
                version: 1,
                plan_sha256: expected.value().plan_sha256.into(),
                identity: expected.value().identity.clone(),
                owner_generation: expected.value().owner_generation,
                current_progress_path: expected
                    .value()
                    .progress_path(progress.value().next_ordinal),
                current_progress_sha256: progress_hash.value().clone(),
            })
        })?;
        let selector = codec::encode_selector(io, route, &selector)?;
        let target = decode_with_reservation(io, route, Some(64 * 1024), || {
            let mut candidate = [0; 32];
            hex::decode_to_slice(expected.value().candidate_id, &mut candidate)
                .map_err(|_| invariant_violation("invalid unit candidate"))?;
            Ok(RecoveryTarget {
                plan: digest_bytes(expected.value().plan_sha256)?,
                candidate,
                prior: digest_bytes(&prior_selector.value().current_progress_sha256)?,
                proposed: digest_bytes(progress_hash.value())?,
                next_ordinal: progress.value().next_ordinal,
            })
        })?;
        Ok(PreparedUnit {
            expected,
            selected,
            receipt,
            progress: progress_bytes,
            selector,
            target: *target.value(),
        })
    })();
    if result.is_err() {
        io.stop(route);
    }
    result
}

pub(super) struct StagedUnit<'a, 'p>(PreparedUnit<'a, 'p>);

#[cfg(test)]
pub(super) async fn publish(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    unit: PreparedUnit<'_, '_>,
) -> Result<Publication> {
    let staged = stage(io, route, unit).await?;
    let result = select(io, route, staged).await;
    if matches!(result, Ok(Publication::Conflict(_))) {
        io.stop(route);
    }
    result
}

pub(super) async fn stage<'a, 'p>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    unit: PreparedUnit<'a, 'p>,
) -> Result<StagedUnit<'a, 'p>> {
    let same_owner = unit.expected.is_owned_by(io)
        && unit.selected.selector_meta.is_owned_by(io)
        && unit.selected.selector_raw.is_owned_by(io)
        && unit.selected.progress.is_owned_by(io)
        && unit.receipt.is_owned_by(io)
        && unit.progress.is_owned_by(io)
        && unit.selector.is_owned_by(io);
    let result = async {
        drop(decode_with_reservation(io, route, Some(64 * 1024), || {
            if !same_owner || unit.target.next_ordinal == 0 {
                return Err(invariant_violation(
                    "prepared unit owner or ordinal differs",
                ));
            }
            Ok(())
        })?);
        let candidate = unit.expected.value().candidate_id;
        drop(
            write_restore_control_record(
                io,
                route,
                candidate,
                RestoreControlRecord::Receipt(unit.target.next_ordinal - 1),
                &unit.receipt,
                None,
            )
            .await?,
        );
        drop(
            write_restore_control_record(
                io,
                route,
                candidate,
                RestoreControlRecord::Progress(unit.target.next_ordinal),
                &unit.progress,
                None,
            )
            .await?,
        );
        Ok(StagedUnit(unit))
    }
    .await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

pub(super) async fn select(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    staged: StagedUnit<'_, '_>,
) -> Result<Publication> {
    let unit = staged.0;
    let result = async {
        let candidate = unit.expected.value().candidate_id;
        let result = write_restore_control_record(
            io,
            route,
            candidate,
            RestoreControlRecord::Selector,
            &unit.selector,
            Some(&unit.selected.selector_meta.value().version),
        )
        .await?;
        if matches!(result.value(), arco_core::WriteResult::Success { .. }) {
            return Ok(Publication::Written);
        }
        drop(result);
        let expected = unit.expected;
        let target = unit.target;
        drop(unit);
        let observed = reconcile(io, route, expected, target).await?;
        if observed == Reconciliation::Exact {
            Ok(Publication::ExactSelected)
        } else {
            // The native caller admits its typed conflict error before stopping.
            Ok(Publication::Conflict(observed))
        }
    }
    .await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

/// Observations only: prior selection never authorizes retry or final publication.
pub(super) async fn reconcile(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    expected: &WorkingValue<ExpectedPlan<'_>>,
    target: RecoveryTarget,
) -> Result<Reconciliation> {
    let same_owner = expected.is_owned_by(io);
    let result = async {
        drop(decode_with_reservation(io, route, Some(64 * 1024), || {
            let mut candidate = [0; 32];
            if !same_owner || target.next_ordinal == 0 {
                return Err(invariant_violation(
                    "unit recovery owner or ordinal differs",
                ));
            }
            hex::decode_to_slice(expected.value().candidate_id, &mut candidate)
                .map_err(|_| invariant_violation("invalid recovery candidate"))?;
            if candidate != target.candidate
                || digest_bytes(expected.value().plan_sha256)? != target.plan
            {
                return Err(invariant_violation("unit recovery belongs to another plan"));
            }
            Ok(())
        })?);
        let current = read_selected_progress(io, route, expected).await?;
        let selector = codec::decode_selector(io, route, &current.selector_raw)?;
        let outcome = decode_with_reservation(io, route, Some(64 * 1024), || {
            let digest = digest_bytes(&selector.value().current_progress_sha256)?;
            let ordinal = current.progress.value().next_ordinal;
            Ok(
                if digest == target.proposed && ordinal == target.next_ordinal {
                    Reconciliation::Exact
                } else if digest == target.prior && ordinal == target.next_ordinal - 1 {
                    Reconciliation::Prior
                } else {
                    Reconciliation::Different
                },
            )
        })?;
        Ok(*outcome.value())
    }
    .await;
    if result.is_err() {
        io.stop(route);
    }
    result
}
