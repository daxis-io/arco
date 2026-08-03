//! Pure advisory gate for Phase 3C state-store prototype promotion.
//!
//! This module reports whether prototype evidence is complete enough to discuss
//! a later advancement. It does not route traffic, mutate state, write object
//! artifacts, or call catalog/governance production paths.
//!
//! Measurements are typed quantities checked against the roadmap's numeric
//! budgets (warm write p99 ≤ 250ms, warm point read p99 ≤ 50ms, cold writer
//! startup ≤ 2s, manifest-reachable replay ≤ 64MiB). Every input and report is
//! serde-serializable so an evidence packet can be durably recorded.

use serde::{Deserialize, Serialize};

/// Required Phase 3C promote-only criteria.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

    /// Returns the roadmap budget this measurement must satisfy, when one is
    /// numerically defined.
    #[must_use]
    pub const fn budget(self) -> Option<MeasurementValue> {
        match self {
            Self::WarmWriteP99NarrowMetadataMutation => {
                Some(MeasurementValue::DurationMicros(250_000))
            }
            Self::WarmPointReadP99 => Some(MeasurementValue::DurationMicros(50_000)),
            Self::ColdWriterStartupToWriteReady => {
                Some(MeasurementValue::DurationMicros(2_000_000))
            }
            Self::ManifestReachableReplayBytes => Some(MeasurementValue::Bytes(64 * 1024 * 1024)),
            Self::BoundedPrefixScanP99
            | Self::ProjectionWatermarkLag
            | Self::CompactionBacklogBeforeReplayBudgetBreach
            | Self::StateTokenReadAfterWriteRetention => None,
        }
    }
}

/// Source class for a promotion measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeasurementSource {
    /// Measurement came from a deterministic fixture.
    DeterministicFixture,
    /// Measurement came from an explicitly opted-in benchmark.
    OptInBenchmark,
    /// Measurement is unavailable and must not be treated as evidence.
    Unavailable,
}

/// A typed measured quantity with an explicit unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "unit", content = "amount", rename_all = "snake_case")]
pub enum MeasurementValue {
    /// A duration expressed in whole microseconds.
    DurationMicros(u64),
    /// A byte count.
    Bytes(u64),
    /// A dimensionless count (e.g. backlog depth or retained tokens).
    Count(u64),
}

impl MeasurementValue {
    /// Compares this value against a budget of the same unit.
    ///
    /// Returns `None` when the units differ, which callers must treat as a
    /// failed comparison rather than a pass.
    #[must_use]
    pub const fn within(self, budget: Self) -> Option<bool> {
        match (self, budget) {
            (Self::DurationMicros(value), Self::DurationMicros(limit))
            | (Self::Bytes(value), Self::Bytes(limit))
            | (Self::Count(value), Self::Count(limit)) => Some(value <= limit),
            _ => None,
        }
    }
}

/// A deterministic performance or operations measurement record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionMeasurement {
    kind: PromotionMeasurementKind,
    source: MeasurementSource,
    value: Option<MeasurementValue>,
}

impl PromotionMeasurement {
    /// Creates a measurement record without a measured value.
    #[must_use]
    pub const fn new(kind: PromotionMeasurementKind, source: MeasurementSource) -> Self {
        Self {
            kind,
            source,
            value: None,
        }
    }

    /// Creates a measurement record with a typed value.
    ///
    /// Unavailable measurements intentionally discard values so unavailable
    /// evidence cannot look like a benchmark result.
    #[must_use]
    pub fn with_value(
        kind: PromotionMeasurementKind,
        source: MeasurementSource,
        value: MeasurementValue,
    ) -> Self {
        let value = if source == MeasurementSource::Unavailable {
            None
        } else {
            Some(value)
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

    /// Returns the typed measured value, when present.
    #[must_use]
    pub const fn value(&self) -> Option<MeasurementValue> {
        self.value
    }

    fn evaluate(&self) -> PromotionMeasurementStatus {
        let budget = self.kind.budget();
        let satisfied = self.source != MeasurementSource::Unavailable
            && budget.is_none_or(|limit| {
                self.value
                    .and_then(|value| value.within(limit))
                    .unwrap_or(false)
            });
        PromotionMeasurementStatus {
            measurement: self.clone(),
            budget,
            satisfied,
        }
    }
}

/// A criterion status in a deterministic gate report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

/// A measurement evaluated against its roadmap budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionMeasurementStatus {
    measurement: PromotionMeasurement,
    budget: Option<MeasurementValue>,
    satisfied: bool,
}

impl PromotionMeasurementStatus {
    /// Returns the underlying measurement record.
    #[must_use]
    pub const fn measurement(&self) -> &PromotionMeasurement {
        &self.measurement
    }

    /// Returns the measurement kind.
    #[must_use]
    pub const fn kind(&self) -> PromotionMeasurementKind {
        self.measurement.kind()
    }

    /// Returns the measurement source class.
    #[must_use]
    pub const fn source(&self) -> MeasurementSource {
        self.measurement.source()
    }

    /// Returns the typed measured value, when present.
    #[must_use]
    pub const fn value(&self) -> Option<MeasurementValue> {
        self.measurement.value()
    }

    /// Returns the roadmap budget this measurement was checked against.
    #[must_use]
    pub const fn budget(&self) -> Option<MeasurementValue> {
        self.budget
    }

    /// Returns whether the measurement is available and within budget.
    #[must_use]
    pub const fn satisfied(&self) -> bool {
        self.satisfied
    }
}

/// Advisory Phase 3C decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromotionDecision {
    /// Required evidence or measurements are missing, failed, or unavailable.
    RejectAdvancement,
    /// Evidence is complete enough for review, but no production cutover occurs.
    CandidateEvidenceComplete,
}

/// Required fallback recommendation when the prototype is not promoted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FallbackRecommendation {
    keep_current_synchronous_compactor_authority: bool,
    continue_derived_indexes_and_projection_acceleration_only: bool,
    do_not_cut_over: [String; 4],
}

impl FallbackRecommendation {
    /// Returns the Phase 3C fallback recommendation.
    #[must_use]
    pub fn current_synchronous_compactor_authority() -> Self {
        Self {
            keep_current_synchronous_compactor_authority: true,
            continue_derived_indexes_and_projection_acceleration_only: true,
            do_not_cut_over: [
                "catalog DDL".to_string(),
                "grants".to_string(),
                "credential vending".to_string(),
                "broad governance".to_string(),
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
    pub fn do_not_cut_over(&self) -> [&str; 4] {
        [
            self.do_not_cut_over[0].as_str(),
            self.do_not_cut_over[1].as_str(),
            self.do_not_cut_over[2].as_str(),
            self.do_not_cut_over[3].as_str(),
        ]
    }
}

/// Pure input for evaluating the Phase 3C promotion gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionGateInput {
    satisfied_criteria: Vec<PromotionCriterion>,
    measurements: Vec<PromotionMeasurement>,
}

impl PromotionGateInput {
    /// Creates a promotion-gate input from satisfied criteria and measurements.
    ///
    /// Criteria are deduplicated and sorted; a later measurement for the same
    /// kind replaces an earlier one.
    #[must_use]
    pub fn new(
        satisfied_criteria: impl IntoIterator<Item = PromotionCriterion>,
        measurements: impl IntoIterator<Item = PromotionMeasurement>,
    ) -> Self {
        let mut criteria: Vec<PromotionCriterion> = satisfied_criteria.into_iter().collect();
        criteria.sort_unstable();
        criteria.dedup();

        let mut deduped: Vec<PromotionMeasurement> = Vec::new();
        for measurement in measurements {
            if let Some(existing) = deduped
                .iter_mut()
                .find(|existing| existing.kind() == measurement.kind())
            {
                *existing = measurement;
            } else {
                deduped.push(measurement);
            }
        }
        deduped.sort_unstable_by_key(PromotionMeasurement::kind);

        Self {
            satisfied_criteria: criteria,
            measurements: deduped,
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
                    .iter()
                    .find(|measurement| measurement.kind() == kind)
                    .cloned()
                    .unwrap_or_else(|| PromotionMeasurement::unavailable(kind))
                    .evaluate()
            })
            .collect::<Vec<_>>();

        let decision = if criteria.iter().all(PromotionCriterionStatus::satisfied)
            && measurements
                .iter()
                .all(PromotionMeasurementStatus::satisfied)
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionGateReport {
    decision: PromotionDecision,
    criteria: Vec<PromotionCriterionStatus>,
    measurements: Vec<PromotionMeasurementStatus>,
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

    /// Returns budget-evaluated measurement statuses in deterministic Phase 3C order.
    #[must_use]
    pub fn measurements(&self) -> &[PromotionMeasurementStatus] {
        &self.measurements
    }

    /// Returns the fallback recommendation attached to the report.
    #[must_use]
    pub const fn fallback_recommendation(&self) -> &FallbackRecommendation {
        &self.fallback_recommendation
    }
}
