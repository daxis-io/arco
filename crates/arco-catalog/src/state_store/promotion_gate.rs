//! Pure advisory gate for Phase 3C state-store prototype promotion.
//!
//! This module reports whether prototype evidence is complete enough to discuss
//! a later advancement. It does not route traffic, mutate state, write object
//! artifacts, or call catalog/governance production paths.

use std::collections::{BTreeMap, BTreeSet};

/// Required Phase 3C promote-only criteria.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PromotionCriterion {
    /// Phase 3B correctness and failure-state tests passed.
    CorrectnessAndFailureStateTests,
    /// Provider compare-and-swap and retry behavior is proven.
    ProviderCasAndRetryBehavior,
    /// Read-after-write through `StateToken` is proven.
    StateTokenReadAfterWrite,
    /// Deterministic model replay equivalence holds.
    ModelReplayEquivalence,
    /// Object-store MVP manifest-reachable replay equivalence holds.
    ObjectStoreMvpReplayEquivalence,
    /// Projection equality can be measured through a watermark.
    ProjectionEqualityWatermark,
    /// Enforcement and vending fail closed from authority or fresh-enough compiled state.
    EnforcementAndVendingFreshness,
    /// Operational complexity remains acceptable.
    OperationalComplexityAcceptable,
}

impl PromotionCriterion {
    /// All required Phase 3C promotion criteria in deterministic report order.
    pub const ALL: [Self; 8] = [
        Self::CorrectnessAndFailureStateTests,
        Self::ProviderCasAndRetryBehavior,
        Self::StateTokenReadAfterWrite,
        Self::ModelReplayEquivalence,
        Self::ObjectStoreMvpReplayEquivalence,
        Self::ProjectionEqualityWatermark,
        Self::EnforcementAndVendingFreshness,
        Self::OperationalComplexityAcceptable,
    ];
}

/// Required Phase 3C performance and operations measurements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PromotionMeasurementKind {
    /// Warm write p99 for a narrow metadata mutation.
    WarmWriteP99NarrowMetadataMutation,
    /// Warm point-read p99.
    WarmPointReadP99,
    /// Bounded prefix-scan p99.
    BoundedPrefixScanP99,
    /// Cold writer startup to write-ready.
    ColdWriterStartupToWriteReady,
    /// Manifest-reachable replay bytes.
    ManifestReachableReplayBytes,
    /// Projection watermark lag.
    ProjectionWatermarkLag,
    /// Compaction backlog before replay budget breach.
    CompactionBacklogBeforeReplayBudgetBreach,
    /// `StateToken` read-after-write retention.
    StateTokenReadAfterWriteRetention,
}

impl PromotionMeasurementKind {
    /// All required Phase 3C measurements in deterministic report order.
    pub const ALL: [Self; 8] = [
        Self::WarmWriteP99NarrowMetadataMutation,
        Self::WarmPointReadP99,
        Self::BoundedPrefixScanP99,
        Self::ColdWriterStartupToWriteReady,
        Self::ManifestReachableReplayBytes,
        Self::ProjectionWatermarkLag,
        Self::CompactionBacklogBeforeReplayBudgetBreach,
        Self::StateTokenReadAfterWriteRetention,
    ];
}

/// Source class for a promotion measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MeasurementSource {
    /// Measurement came from a deterministic fixture.
    DeterministicFixture,
    /// Measurement came from an explicitly opted-in benchmark.
    OptInBenchmark,
    /// Measurement is unavailable and must not be treated as evidence.
    Unavailable,
}

/// A deterministic performance or operations measurement record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionMeasurement {
    kind: PromotionMeasurementKind,
    source: MeasurementSource,
    value: Option<String>,
}

impl PromotionMeasurement {
    /// Creates a measurement record without a display value.
    #[must_use]
    pub const fn new(kind: PromotionMeasurementKind, source: MeasurementSource) -> Self {
        Self {
            kind,
            source,
            value: None,
        }
    }

    /// Creates a measurement record with a display value.
    ///
    /// Unavailable measurements intentionally discard values so unavailable
    /// evidence cannot look like a benchmark result.
    #[must_use]
    pub fn with_value(
        kind: PromotionMeasurementKind,
        source: MeasurementSource,
        value: impl Into<String>,
    ) -> Self {
        let value = if source == MeasurementSource::Unavailable {
            None
        } else {
            Some(value.into())
        };
        Self {
            kind,
            source,
            value,
        }
    }

    /// Creates an unavailable measurement record.
    #[must_use]
    pub const fn unavailable(kind: PromotionMeasurementKind) -> Self {
        Self::new(kind, MeasurementSource::Unavailable)
    }

    /// Returns the measurement kind.
    #[must_use]
    pub const fn kind(&self) -> PromotionMeasurementKind {
        self.kind
    }

    /// Returns the measurement source class.
    #[must_use]
    pub const fn source(&self) -> MeasurementSource {
        self.source
    }

    /// Returns the optional display value.
    #[must_use]
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    fn is_available(&self) -> bool {
        self.source != MeasurementSource::Unavailable
    }
}

/// A criterion status in a deterministic gate report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromotionCriterionStatus {
    criterion: PromotionCriterion,
    satisfied: bool,
}

impl PromotionCriterionStatus {
    const fn new(criterion: PromotionCriterion, satisfied: bool) -> Self {
        Self {
            criterion,
            satisfied,
        }
    }

    /// Returns the criterion represented by this status.
    #[must_use]
    pub const fn criterion(&self) -> PromotionCriterion {
        self.criterion
    }

    /// Returns whether the required evidence was supplied.
    #[must_use]
    pub const fn satisfied(&self) -> bool {
        self.satisfied
    }
}

/// Advisory Phase 3C decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotionDecision {
    /// Required evidence or measurements are missing, failed, or unavailable.
    RejectAdvancement,
    /// Evidence is complete enough for review, but no production cutover occurs.
    CandidateEvidenceComplete,
}

/// Required fallback recommendation when the prototype is not promoted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FallbackRecommendation {
    keep_current_synchronous_compactor_authority: bool,
    continue_derived_indexes_and_projection_acceleration_only: bool,
    do_not_cut_over: [&'static str; 4],
}

impl FallbackRecommendation {
    /// Returns the Phase 3C fallback recommendation.
    #[must_use]
    pub const fn current_synchronous_compactor_authority() -> Self {
        Self {
            keep_current_synchronous_compactor_authority: true,
            continue_derived_indexes_and_projection_acceleration_only: true,
            do_not_cut_over: [
                "catalog DDL",
                "grants",
                "credential vending",
                "broad governance",
            ],
        }
    }

    /// Returns whether current synchronous-compactor authority must remain.
    #[must_use]
    pub const fn keep_current_synchronous_compactor_authority(&self) -> bool {
        self.keep_current_synchronous_compactor_authority
    }

    /// Returns whether only derived indexes and projection acceleration should continue.
    #[must_use]
    pub const fn continue_derived_indexes_and_projection_acceleration_only(&self) -> bool {
        self.continue_derived_indexes_and_projection_acceleration_only
    }

    /// Returns the domains that must not be cut over by this slice.
    #[must_use]
    pub const fn do_not_cut_over(&self) -> [&'static str; 4] {
        self.do_not_cut_over
    }
}

/// Pure input for evaluating the Phase 3C promotion gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionGateInput {
    satisfied_criteria: BTreeSet<PromotionCriterion>,
    measurements: BTreeMap<PromotionMeasurementKind, PromotionMeasurement>,
}

impl PromotionGateInput {
    /// Creates a promotion-gate input from satisfied criteria and measurements.
    #[must_use]
    pub fn new(
        satisfied_criteria: impl IntoIterator<Item = PromotionCriterion>,
        measurements: impl IntoIterator<Item = PromotionMeasurement>,
    ) -> Self {
        let measurements = measurements
            .into_iter()
            .map(|measurement| (measurement.kind(), measurement))
            .collect();
        Self {
            satisfied_criteria: satisfied_criteria.into_iter().collect(),
            measurements,
        }
    }

    /// Evaluates this input into a deterministic advisory report.
    #[must_use]
    pub fn evaluate(&self) -> PromotionGateReport {
        let criteria = PromotionCriterion::ALL
            .into_iter()
            .map(|criterion| {
                PromotionCriterionStatus::new(
                    criterion,
                    self.satisfied_criteria.contains(&criterion),
                )
            })
            .collect::<Vec<_>>();

        let measurements = PromotionMeasurementKind::ALL
            .into_iter()
            .map(|kind| {
                self.measurements
                    .get(&kind)
                    .cloned()
                    .unwrap_or_else(|| PromotionMeasurement::unavailable(kind))
            })
            .collect::<Vec<_>>();

        let decision = if criteria.iter().all(PromotionCriterionStatus::satisfied)
            && measurements.iter().all(PromotionMeasurement::is_available)
        {
            PromotionDecision::CandidateEvidenceComplete
        } else {
            PromotionDecision::RejectAdvancement
        };

        PromotionGateReport {
            decision,
            criteria,
            measurements,
            fallback_recommendation:
                FallbackRecommendation::current_synchronous_compactor_authority(),
        }
    }
}

/// Deterministic advisory report for the Phase 3C promotion gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionGateReport {
    decision: PromotionDecision,
    criteria: Vec<PromotionCriterionStatus>,
    measurements: Vec<PromotionMeasurement>,
    fallback_recommendation: FallbackRecommendation,
}

impl PromotionGateReport {
    /// Returns the advisory decision.
    #[must_use]
    pub const fn decision(&self) -> PromotionDecision {
        self.decision
    }

    /// Returns criterion statuses in deterministic Phase 3C order.
    #[must_use]
    pub fn criteria(&self) -> &[PromotionCriterionStatus] {
        &self.criteria
    }

    /// Returns measurement records in deterministic Phase 3C order.
    #[must_use]
    pub fn measurements(&self) -> &[PromotionMeasurement] {
        &self.measurements
    }

    /// Returns the fallback recommendation attached to the report.
    #[must_use]
    pub const fn fallback_recommendation(&self) -> &FallbackRecommendation {
        &self.fallback_recommendation
    }
}
