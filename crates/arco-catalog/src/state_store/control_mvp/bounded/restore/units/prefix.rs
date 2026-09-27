//! Ascending receipt-prefix structure only; no physical coverage or publication authority.
use super::{
    ControlMvpRestoreReceiptV1, CumulativeSemanticCounts, ExpectedPlan, GlobalCut, ReceiptTail,
    RestorePhysicalIo, RestorePhysicalRoute, Result, SelectedProgress, SingletonState,
    WorkingValue, checked_sum, codec, decode_with_reservation, invariant_violation, physical,
    zero_cursor,
};

pub(super) struct Prefix<'a, 'p> {
    expected: &'a WorkingValue<ExpectedPlan<'p>>,
    selected: &'a SelectedProgress,
    genesis_raw: WorkingValue<String>,
    genesis_chain: WorkingValue<String>,
    tail: Option<WorkingValue<ReceiptTail>>,
    next_ordinal: u64,
    stopped: bool,
}

impl<'a, 'p> Prefix<'a, 'p> {
    pub(super) fn new(
        io: &mut RestorePhysicalIo<'_>,
        route: &mut RestorePhysicalRoute<'_, '_>,
        expected: &'a WorkingValue<ExpectedPlan<'p>>,
        selected: &'a SelectedProgress,
    ) -> Result<Self> {
        let ordinary = matches!(route, RestorePhysicalRoute::OrdinaryUnit { .. });
        let same_owner = expected.is_owned_by(io)
            && selected.progress.is_owned_by(io)
            && selected.selector_raw.is_owned_by(io)
            && selected.selector_meta.is_owned_by(io);
        let checked = decode_with_reservation(io, route, Some(64 * 1024), || {
            if !ordinary
                || !same_owner
                || selected.selector_meta.value().version.is_empty()
                || !selected.progress.value().terminal
                || selected.progress.value().next_ordinal == 0
            {
                return Err(invariant_violation(
                    "receipt prefix requires authenticated terminal control selection",
                ));
            }
            Ok(())
        })?;
        drop(checked);
        drop(codec::validate_progress(
            io,
            route,
            expected,
            &selected.progress,
        )?);
        let selector = codec::decode_selector(io, route, &selected.selector_raw)?;
        drop(codec::validate_selector(io, route, expected, &selector)?);
        let reservation = expected
            .value()
            .prefix
            .len()
            .checked_add(128)
            .and_then(|bytes| bytes.checked_mul(4))
            .and_then(|bytes| bytes.checked_add(64 * 1024));
        let checked =
            decode_with_reservation(io, route, Some(reservation.unwrap_or(64 * 1024)), || {
                if reservation.is_none() {
                    return Err(invariant_violation(
                        "receipt prefix selector reservation overflow",
                    ));
                }
                if selector.value().current_progress_path
                    != expected
                        .value()
                        .progress_path(selected.progress.value().next_ordinal)
                {
                    return Err(invariant_violation(
                        "receipt prefix terminal selector path differs",
                    ));
                }
                Ok(())
            })?;
        drop((checked, selector));
        let genesis_raw = codec::hashes::genesis_none(io, route, &selected.progress)?;
        let genesis_chain =
            codec::hashes::genesis_chain(io, route, &selected.progress, &genesis_raw)?;
        Ok(Self {
            expected,
            selected,
            genesis_raw,
            genesis_chain,
            tail: None,
            next_ordinal: 0,
            stopped: false,
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "keep receipt and carried-tail ownership visible through one admitted traversal step"
    )]
    pub(super) async fn next(
        &mut self,
        io: &mut RestorePhysicalIo<'_>,
        route: &mut RestorePhysicalRoute<'_, '_>,
    ) -> Result<Option<WorkingValue<ControlMvpRestoreReceiptV1>>> {
        let final_stream = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
        let same_owner = self.expected.is_owned_by(io)
            && self.selected.progress.is_owned_by(io)
            && self.genesis_raw.is_owned_by(io)
            && self.genesis_chain.is_owned_by(io)
            && self.tail.as_ref().is_none_or(|tail| tail.is_owned_by(io));
        let done = decode_with_reservation(io, route, Some(64 * 1024), || {
            if self.stopped
                || !final_stream
                || !same_owner
                || self.next_ordinal > self.selected.progress.value().receipt_count
            {
                return Err(invariant_violation(
                    "receipt prefix ordinal, owner or phase differs",
                ));
            }
            if self.next_ordinal == self.selected.progress.value().receipt_count {
                return Ok(true);
            }
            if self
                .tail
                .as_ref()
                .is_some_and(|tail| matches!(tail.value().cursor.global, GlobalCut::End))
            {
                return Err(invariant_violation(
                    "receipt prefix continues after global end",
                ));
            }
            self.stopped = true;
            Ok(false)
        })?;
        if *done.value() {
            return Ok(None);
        }
        drop(done);
        let found = physical::restore_io::read_final_restore_receipt(
            io,
            route,
            self.expected.value().candidate_id,
            self.next_ordinal,
        )
        .await?;
        let present = decode_with_reservation(io, route, Some(64 * 1024), || {
            if found.is_none() {
                return Err(invariant_violation("selected forward receipt is missing"));
            }
            Ok(())
        })?;
        drop(present);
        let (raw, metadata) = found.unwrap_or_else(|| unreachable!("presence admitted"));
        let raw_hash = codec::hashes::raw_read(io, route, &raw)?;
        let receipt = codec::decode_receipt(io, route, &raw)?;
        drop(codec::validate_receipt(io, route, self.expected, &receipt)?);
        let reservation = codec::receipt_tail_copy_reservation(
            raw.as_slice().len(),
            self.expected.value().prefix.len(),
        );
        let tail =
            decode_with_reservation(io, route, Some(reservation.unwrap_or(64 * 1024)), || {
                if reservation.is_none() {
                    return Err(invariant_violation(
                        "receipt prefix tail exceeds allocation admission",
                    ));
                }
                let value = receipt.value();
                if value.ordinal != self.next_ordinal {
                    return Err(invariant_violation("forward receipt ordinal differs"));
                }
                if let Some(prior) = &self.tail {
                    let prior = prior.value();
                    if prior.ordinal.checked_add(1) != Some(value.ordinal)
                        || value.predecessor_receipt_sha256 != prior.raw_sha256
                        || value.predecessor_chain_sha256 != prior.chain_sha256
                        || value.before != prior.cursor
                        || value.singleton_before != prior.singleton
                        || value.prefix_cumulative_counts != prior.cumulative
                    {
                        return Err(invariant_violation(
                            "forward receipt does not continue predecessor",
                        ));
                    }
                } else if value.ordinal != 0
                    || value.predecessor_receipt_sha256 != *self.genesis_raw.value()
                    || value.predecessor_chain_sha256 != *self.genesis_chain.value()
                    || value.before != zero_cursor()
                    || value.singleton_before != SingletonState::None
                    || value.prefix_cumulative_counts != CumulativeSemanticCounts::default()
                {
                    return Err(invariant_violation("forward receipt differs from genesis"));
                }
                let cumulative = checked_sum(&value.prefix_cumulative_counts, &value.counts)?;
                let next = value
                    .ordinal
                    .checked_add(1)
                    .ok_or_else(|| invariant_violation("receipt ordinal overflow"))?;
                let terminal = self.selected.progress.value();
                if next == terminal.receipt_count {
                    let last = terminal
                        .last_receipt
                        .as_ref()
                        .ok_or_else(|| invariant_violation("terminal last receipt missing"))?;
                    if last.path != self.expected.value().receipt_path(value.ordinal)
                        || last.raw_sha256 != *raw_hash.value()
                        || terminal.chain_sha256 != value.chain_sha256
                        || terminal.cursor != value.after
                        || terminal.singleton_state != value.singleton_after
                        || terminal.cumulative_counts != cumulative
                    {
                        return Err(invariant_violation(
                            "receipt prefix differs from selected terminal",
                        ));
                    }
                } else if matches!(value.after.global, GlobalCut::End) {
                    return Err(invariant_violation(
                        "receipt prefix ends before terminal count",
                    ));
                }
                Ok(ReceiptTail {
                    ordinal: value.ordinal,
                    raw_sha256: raw_hash.value().clone(),
                    chain_sha256: value.chain_sha256.clone(),
                    cursor: value.after.clone(),
                    singleton: value.singleton_after.clone(),
                    cumulative,
                })
            })?;
        drop((raw, metadata, raw_hash));
        self.next_ordinal += 1; // Checked above before replacing any carried state.
        self.tail = Some(tail);
        self.stopped = false;
        Ok(Some(receipt))
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        MergeCursor, OwnedSelectedPlan, RestoreControlRecord, RestoreProgressSelectorV1,
        SideCursor, admitted_expected_plan, behavioral_tests, genesis_chain_sha256,
        genesis_receipt_raw_sha256, jcs, prefixed_sha256, read_selected_progress,
        selected_plan_copy_reservation,
    };
    use super::*;
    use crate::workspace_io_budget::WorkspaceIoBudget;
    use physical::restore_io::{FinalMicrochunk, FinalStreamTotals, UnitPayloadAdmission};

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "keep the real terminal selection and allocation regression in one test"
    )]
    async fn final_receipt_prefix_long_domain_path_admission() {
        for domain_bytes in [32 * 1024, 63 * 1024] {
            let domain = "x".repeat(domain_bytes);
            let (store, plan) =
                super::super::super::tests::inspection_fixture_for_domain(&domain).await;
            let digest = prefixed_sha256(
                &jcs(
                    &crate::state_store::PersistedRestoreParticipantPlan::ControlMvpV7(Box::new(
                        plan.clone(),
                    )),
                )
                .expect("real plan encoding"),
            );
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let owned = decode_with_reservation(
                &mut io,
                &mut route,
                selected_plan_copy_reservation(&plan),
                || {
                    Ok(OwnedSelectedPlan {
                        plan: plan.clone(),
                        plan_sha256: digest.clone(),
                    })
                },
            )
            .expect("owned plan");
            let expected =
                admitted_expected_plan(&mut io, &mut route, &owned).expect("expected plan");
            // Structural prefix fixture only: the final physical verifier must separately prove row coverage.
            let receipt = behavioral_tests::receipt(
                expected.value(),
                0,
                genesis_receipt_raw_sha256(expected.value()).expect("genesis raw"),
                genesis_chain_sha256(expected.value()).expect("genesis chain"),
                zero_cursor(),
                MergeCursor {
                    global: GlobalCut::End,
                    source: SideCursor::End,
                    current: SideCursor::End,
                },
                CumulativeSemanticCounts::default(),
            );
            let receipt_raw = jcs(&receipt).expect("receipt JCS");
            let progress =
                behavioral_tests::progress_after(expected.value(), &receipt, &receipt_raw, true);
            let progress_raw = jcs(&progress).expect("progress JCS");
            let selector = RestoreProgressSelectorV1 {
                record_type: "control_mvp_restore_progress_selector".into(),
                version: 1,
                plan_sha256: digest.clone(),
                identity: expected.value().identity.clone(),
                owner_generation: expected.value().owner_generation,
                current_progress_path: expected.value().progress_path(1),
                current_progress_sha256: prefixed_sha256(&progress_raw),
            };
            for (record, raw) in [
                (RestoreControlRecord::Receipt(0), receipt_raw),
                (RestoreControlRecord::Progress(1), progress_raw),
                (
                    RestoreControlRecord::Selector,
                    jcs(&selector).expect("selector JCS"),
                ),
            ] {
                store
                    .retention
                    .put_raw(
                        &record.path(&expected.value().prefix),
                        raw.into(),
                        arco_core::WritePrecondition::DoesNotExist,
                    )
                    .await
                    .expect("immutable fixture records");
            }
            let selected = read_selected_progress(&mut io, &mut route, &expected)
                .await
                .expect("real opaque selected terminal");
            let mut prefix = Prefix::new(&mut io, &mut route, &expected, &selected)
                .expect("long-domain prefix constructor");
            let mut totals = FinalStreamTotals::new();
            {
                let mut chunk =
                    FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("receipt chunk");
                let mut final_route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
                let receipt = prefix
                    .next(&mut io, &mut final_route)
                    .await
                    .expect("long-domain terminal tail")
                    .expect("terminal receipt");
                assert_eq!(receipt.value().ordinal, 0);
            }
            {
                let mut chunk =
                    FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("closure chunk");
                assert!(
                    prefix
                        .next(
                            &mut io,
                            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                        )
                        .await
                        .expect("prefix closed")
                        .is_none()
                );
            }
            assert_eq!(io.allocation_underestimates(), 0);
            println!(
                "prefix path domain={domain_bytes} peak={}",
                io.peak_owned_evidence()
            );
            drop(prefix);
            drop(selected);
            drop(expected);
            drop(owned);
            assert_eq!(io.live_ownership_evidence(), (0, 0));
        }
    }
}
