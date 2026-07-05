//! Deterministic promotion-gate tests for the state-store prototype.

use arco_catalog::state_store::promotion_gate::{
    FallbackRecommendation, MeasurementSource, PromotionCriterion, PromotionDecision,
    PromotionGateInput, PromotionMeasurement, PromotionMeasurementKind,
};

fn fixture_measurements() -> Vec<PromotionMeasurement> {
    PromotionMeasurementKind::ALL
        .into_iter()
        .map(|kind| PromotionMeasurement::new(kind, MeasurementSource::DeterministicFixture))
        .collect()
}

fn complete_input() -> PromotionGateInput {
    PromotionGateInput::new(PromotionCriterion::ALL, fixture_measurements())
}

fn input_without(criterion: PromotionCriterion) -> PromotionGateInput {
    PromotionGateInput::new(
        PromotionCriterion::ALL
            .into_iter()
            .filter(|candidate| *candidate != criterion),
        fixture_measurements(),
    )
}

fn assert_missing(report_input: PromotionGateInput, criterion: PromotionCriterion) {
    let report = report_input.evaluate();

    assert_eq!(PromotionDecision::RejectAdvancement, report.decision());
    assert!(
        report
            .criteria()
            .iter()
            .any(|status| status.criterion() == criterion && !status.satisfied()),
        "report should mark {criterion:?} as missing"
    );
}

#[test]
fn rejects_promotion_when_phase_3b_correctness_evidence_is_missing() {
    assert_missing(
        input_without(PromotionCriterion::CorrectnessAndFailureStateTests),
        PromotionCriterion::CorrectnessAndFailureStateTests,
    );
}

#[test]
fn rejects_promotion_when_provider_cas_retry_evidence_is_missing() {
    assert_missing(
        input_without(PromotionCriterion::ProviderCasAndRetryBehavior),
        PromotionCriterion::ProviderCasAndRetryBehavior,
    );
}

#[test]
fn rejects_promotion_when_state_token_read_after_write_evidence_is_missing() {
    assert_missing(
        input_without(PromotionCriterion::StateTokenReadAfterWrite),
        PromotionCriterion::StateTokenReadAfterWrite,
    );
}

#[test]
fn rejects_promotion_when_replay_equivalence_evidence_is_missing() {
    assert_missing(
        input_without(PromotionCriterion::ModelReplayEquivalence),
        PromotionCriterion::ModelReplayEquivalence,
    );
    assert_missing(
        input_without(PromotionCriterion::ObjectStoreMvpReplayEquivalence),
        PromotionCriterion::ObjectStoreMvpReplayEquivalence,
    );
}

#[test]
fn records_unavailable_projection_governance_and_measurement_evidence_without_benchmarks() {
    let report = PromotionGateInput::new(
        [
            PromotionCriterion::CorrectnessAndFailureStateTests,
            PromotionCriterion::ProviderCasAndRetryBehavior,
            PromotionCriterion::StateTokenReadAfterWrite,
            PromotionCriterion::ModelReplayEquivalence,
            PromotionCriterion::ObjectStoreMvpReplayEquivalence,
        ],
        [],
    )
    .evaluate();

    assert_eq!(PromotionDecision::RejectAdvancement, report.decision());

    for criterion in [
        PromotionCriterion::ProjectionEqualityWatermark,
        PromotionCriterion::EnforcementAndVendingFreshness,
        PromotionCriterion::OperationalComplexityAcceptable,
    ] {
        assert!(
            report
                .criteria()
                .iter()
                .any(|status| status.criterion() == criterion && !status.satisfied()),
            "report should mark {criterion:?} as unavailable"
        );
    }

    assert_eq!(
        PromotionMeasurementKind::ALL.len(),
        report.measurements().len()
    );
    for measurement in report.measurements() {
        assert_eq!(MeasurementSource::Unavailable, measurement.source());
        assert_eq!(None, measurement.value());
    }
}

#[test]
fn rejects_promotion_when_required_measurement_is_unavailable() {
    let report = PromotionGateInput::new(
        PromotionCriterion::ALL,
        PromotionMeasurementKind::ALL.into_iter().map(|kind| {
            if kind == PromotionMeasurementKind::ProjectionWatermarkLag {
                PromotionMeasurement::unavailable(kind)
            } else {
                PromotionMeasurement::new(kind, MeasurementSource::DeterministicFixture)
            }
        }),
    )
    .evaluate();

    assert_eq!(PromotionDecision::RejectAdvancement, report.decision());
    assert!(
        report.measurements().iter().any(|measurement| {
            measurement.kind() == PromotionMeasurementKind::ProjectionWatermarkLag
                && measurement.source() == MeasurementSource::Unavailable
        }),
        "report should preserve the unavailable measurement"
    );
}

#[test]
fn emits_required_fallback_recommendation() {
    let report = PromotionGateInput::new([], []).evaluate();
    let fallback = report.fallback_recommendation();

    assert_eq!(
        &FallbackRecommendation::current_synchronous_compactor_authority(),
        fallback
    );
    assert!(fallback.keep_current_synchronous_compactor_authority());
    assert!(fallback.continue_derived_indexes_and_projection_acceleration_only());
    assert_eq!(
        [
            "catalog DDL",
            "grants",
            "credential vending",
            "broad governance"
        ],
        fallback.do_not_cut_over()
    );
}

#[test]
fn complete_evidence_is_only_candidate_complete_not_a_cutover() {
    let report = complete_input().evaluate();

    assert_eq!(
        PromotionDecision::CandidateEvidenceComplete,
        report.decision()
    );
    assert_eq!(
        &FallbackRecommendation::current_synchronous_compactor_authority(),
        report.fallback_recommendation()
    );
}
