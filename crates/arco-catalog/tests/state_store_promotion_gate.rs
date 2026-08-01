//! Deterministic promotion-gate tests for the state-store prototype.

// Advisory lint scope for test code (#331): the pedantic/nursery lints below
// conflict with test ergonomics here; production code keeps them active.
#![allow(clippy::needless_pass_by_value)]

use arco_catalog::state_store::promotion_gate::{
    FallbackRecommendation, MeasurementSource, MeasurementValue, PromotionCriterion,
    PromotionDecision, PromotionGateInput, PromotionMeasurement, PromotionMeasurementKind,
};

fn fixture_measurements() -> Vec<PromotionMeasurement> {
    PromotionMeasurementKind::ALL
        .into_iter()
        .map(|kind| match kind.budget() {
            Some(budget) => PromotionMeasurement::with_value(
                kind,
                MeasurementSource::DeterministicFixture,
                budget,
            ),
            None => PromotionMeasurement::new(kind, MeasurementSource::DeterministicFixture),
        })
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

#[test]
fn budgeted_measurement_at_the_budget_boundary_passes() {
    let report = complete_input().evaluate();
    assert_eq!(
        PromotionDecision::CandidateEvidenceComplete,
        report.decision()
    );
    for status in report.measurements() {
        assert!(
            status.satisfied(),
            "boundary-value measurement {:?} should satisfy its budget",
            status.kind()
        );
    }
}

#[test]
fn budgeted_measurement_over_budget_rejects_advancement() {
    let over_budget = [
        (
            PromotionMeasurementKind::WarmWriteP99NarrowMetadataMutation,
            MeasurementValue::DurationMicros(250_001),
        ),
        (
            PromotionMeasurementKind::WarmPointReadP99,
            MeasurementValue::DurationMicros(50_001),
        ),
        (
            PromotionMeasurementKind::ColdWriterStartupToWriteReady,
            MeasurementValue::DurationMicros(2_000_001),
        ),
        (
            PromotionMeasurementKind::ManifestReachableReplayBytes,
            MeasurementValue::Bytes(64 * 1024 * 1024 + 1),
        ),
    ];
    for (kind, value) in over_budget {
        let measurements = fixture_measurements().into_iter().map(|measurement| {
            if measurement.kind() == kind {
                PromotionMeasurement::with_value(kind, MeasurementSource::OptInBenchmark, value)
            } else {
                measurement
            }
        });
        let report = PromotionGateInput::new(PromotionCriterion::ALL, measurements).evaluate();
        assert_eq!(
            PromotionDecision::RejectAdvancement,
            report.decision(),
            "over-budget {kind:?} must reject advancement"
        );
        assert!(
            report
                .measurements()
                .iter()
                .any(|status| status.kind() == kind && !status.satisfied()),
            "report must mark over-budget {kind:?} unsatisfied"
        );
    }
}

#[test]
fn budgeted_measurement_without_a_value_or_with_wrong_unit_fails() {
    let missing_value = fixture_measurements().into_iter().map(|measurement| {
        if measurement.kind() == PromotionMeasurementKind::WarmWriteP99NarrowMetadataMutation {
            PromotionMeasurement::new(measurement.kind(), MeasurementSource::OptInBenchmark)
        } else {
            measurement
        }
    });
    let report = PromotionGateInput::new(PromotionCriterion::ALL, missing_value).evaluate();
    assert_eq!(PromotionDecision::RejectAdvancement, report.decision());

    let wrong_unit = fixture_measurements().into_iter().map(|measurement| {
        if measurement.kind() == PromotionMeasurementKind::ManifestReachableReplayBytes {
            PromotionMeasurement::with_value(
                measurement.kind(),
                MeasurementSource::OptInBenchmark,
                MeasurementValue::DurationMicros(1),
            )
        } else {
            measurement
        }
    });
    let report = PromotionGateInput::new(PromotionCriterion::ALL, wrong_unit).evaluate();
    assert_eq!(
        PromotionDecision::RejectAdvancement,
        report.decision(),
        "a unit mismatch must never satisfy a budget"
    );
}

#[test]
fn unavailable_source_discards_typed_values() {
    let measurement = PromotionMeasurement::with_value(
        PromotionMeasurementKind::WarmPointReadP99,
        MeasurementSource::Unavailable,
        MeasurementValue::DurationMicros(1),
    );
    assert_eq!(None, measurement.value());
}

#[test]
fn gate_input_and_report_round_trip_through_serde() {
    let input = complete_input();
    let input_json = serde_json::to_string(&input).expect("serialize gate input");
    let input_back: PromotionGateInput =
        serde_json::from_str(&input_json).expect("deserialize gate input");
    assert_eq!(input, input_back);

    let report = input.evaluate();
    let report_json = serde_json::to_string_pretty(&report).expect("serialize gate report");
    let report_back = serde_json::from_str::<
        arco_catalog::state_store::promotion_gate::PromotionGateReport,
    >(&report_json)
    .expect("deserialize gate report");
    assert_eq!(report, report_back);
    assert!(report_json.contains("\"decision\""));
    assert!(report_json.contains("candidate_evidence_complete"));
}
