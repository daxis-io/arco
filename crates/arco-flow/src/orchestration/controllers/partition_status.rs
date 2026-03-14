//! Partition status tracking and staleness computation.
//!
//! Per ADR-026, partition status separates:
//! - **Data freshness** (`last_materialization_*`): Only updated on successful materialization
//! - **Execution status** (`last_attempt_*`): Updated on every attempt
//!
//! Staleness is computed at query time based on:
//! 1. Freshness policy violations
//! 2. Upstream changes (upstream materialized after downstream)
//! 3. Code version changes

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::orchestration::compactor::fold::PartitionStatusRow;

/// Freshness policy for asset partitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshnessPolicy {
    /// Maximum allowed lag from last materialization in minutes.
    pub maximum_lag_minutes: u32,
}

impl Default for FreshnessPolicy {
    fn default() -> Self {
        Self {
            // Default: 24 hours
            maximum_lag_minutes: 60 * 24,
        }
    }
}

/// Result of staleness computation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StalenessResult {
    /// Whether the partition is considered stale.
    pub is_stale: bool,
    /// Reason for staleness (if stale).
    pub reason: Option<StalenessReason>,
    /// When the partition became stale (if applicable).
    pub stale_since: Option<DateTime<Utc>>,
}

/// Reason why a partition is considered stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StalenessReason {
    /// Partition has never been materialized.
    NeverMaterialized,
    /// Freshness policy violated (too old).
    FreshnessPolicy,
    /// Upstream partition materialized more recently.
    UpstreamChanged,
    /// One or more upstream partitions have never materialized.
    UpstreamNeverMaterialized,
    /// Code version changed since last materialization.
    CodeChanged,
}

/// Compute staleness for a partition using freshness policy.
///
/// # Arguments
/// - `partition`: The partition status row to check
/// - `policy`: The freshness policy to apply
/// - `current_code_version`: Current code version (e.g., git SHA)
/// - `now`: Current time for deadline computation
///
/// # Returns
/// Staleness result indicating if partition is stale and why.
#[must_use]
pub fn compute_staleness(
    partition: &PartitionStatusRow,
    policy: &FreshnessPolicy,
    current_code_version: Option<&str>,
    now: DateTime<Utc>,
) -> StalenessResult {
    // Check if never materialized
    let Some(last_mat) = partition.last_materialization_at else {
        return StalenessResult {
            is_stale: true,
            reason: Some(StalenessReason::NeverMaterialized),
            stale_since: None,
        };
    };

    // Check code version change
    if let (Some(current), Some(last)) = (
        current_code_version,
        partition.last_materialization_code_version.as_deref(),
    ) {
        if current != last {
            return StalenessResult {
                is_stale: true,
                reason: Some(StalenessReason::CodeChanged),
                stale_since: Some(now),
            };
        }
    }

    // Check freshness policy
    let deadline = last_mat + Duration::minutes(i64::from(policy.maximum_lag_minutes));
    if now > deadline {
        return StalenessResult {
            is_stale: true,
            reason: Some(StalenessReason::FreshnessPolicy),
            stale_since: Some(deadline),
        };
    }

    StalenessResult {
        is_stale: false,
        reason: None,
        stale_since: None,
    }
}

/// Compute staleness for a partition considering upstream dependencies.
///
/// This variant checks if any upstream partition materialized after the
/// downstream partition, making the downstream stale.
///
/// # Arguments
/// - `partition`: The downstream partition status row to check
/// - `upstreams`: Upstream partition status rows
/// - `policy`: Freshness policy to apply
/// - `current_code_version`: Current code version (e.g., git SHA)
/// - `now`: Current time for deadline computation
///
/// # Returns
/// Staleness result indicating if partition is stale and why.
#[must_use]
pub fn compute_staleness_with_upstreams(
    partition: &PartitionStatusRow,
    upstreams: &[PartitionStatusRow],
    policy: &FreshnessPolicy,
    current_code_version: Option<&str>,
    now: DateTime<Utc>,
) -> StalenessResult {
    // Check if never materialized
    let Some(last_mat) = partition.last_materialization_at else {
        return StalenessResult {
            is_stale: true,
            reason: Some(StalenessReason::NeverMaterialized),
            stale_since: None,
        };
    };

    // Check code version change
    if let (Some(current), Some(last)) = (
        current_code_version,
        partition.last_materialization_code_version.as_deref(),
    ) {
        if current != last {
            return StalenessResult {
                is_stale: true,
                reason: Some(StalenessReason::CodeChanged),
                stale_since: Some(now),
            };
        }
    }

    // Check for upstreams that never materialized
    if upstreams
        .iter()
        .any(|upstream| upstream.last_materialization_at.is_none())
    {
        return StalenessResult {
            is_stale: true,
            reason: Some(StalenessReason::UpstreamNeverMaterialized),
            stale_since: None,
        };
    }

    // Check if any upstream materialized after this partition
    for upstream in upstreams {
        if let Some(upstream_mat) = upstream.last_materialization_at {
            if upstream_mat > last_mat {
                return StalenessResult {
                    is_stale: true,
                    reason: Some(StalenessReason::UpstreamChanged),
                    stale_since: Some(upstream_mat),
                };
            }
        }
    }

    // Check freshness policy
    let deadline = last_mat + Duration::minutes(i64::from(policy.maximum_lag_minutes));
    if now > deadline {
        return StalenessResult {
            is_stale: true,
            reason: Some(StalenessReason::FreshnessPolicy),
            stale_since: Some(deadline),
        };
    }

    StalenessResult {
        is_stale: false,
        reason: None,
        stale_since: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_partition(
        asset_key: &str,
        partition_key: &str,
        last_mat_at: Option<DateTime<Utc>>,
        code_version: Option<&str>,
    ) -> PartitionStatusRow {
        PartitionStatusRow {
            tenant_id: "tenant".into(),
            workspace_id: "workspace".into(),
            asset_key: asset_key.into(),
            partition_key: partition_key.into(),
            last_materialization_run_id: last_mat_at.as_ref().map(|_| "run_123".into()),
            last_materialization_at: last_mat_at,
            last_materialization_code_version: code_version.map(ToString::to_string),
            last_attempt_run_id: None,
            last_attempt_at: None,
            last_attempt_outcome: None,
            stale_since: None,
            stale_reason_code: None,
            partition_values: HashMap::new(),
            delta_table: None,
            delta_version: None,
            delta_partition: None,
            execution_lineage_ref: None,
            row_version: "v1".into(),
        }
    }

    #[test]
    fn test_staleness_never_materialized() {
        let now = Utc::now();
        let partition = make_partition("analytics.daily", "2025-01-15", None, None);
        let policy = FreshnessPolicy::default();

        let result = compute_staleness(&partition, &policy, Some("v1"), now);

        assert!(result.is_stale);
        assert_eq!(result.reason, Some(StalenessReason::NeverMaterialized));
        assert!(result.stale_since.is_none());
    }

    #[test]
    fn test_staleness_computed_at_query_time() {
        let now = Utc::now();
        let partition = make_partition(
            "analytics.daily",
            "2025-01-15",
            Some(now - Duration::hours(25)),
            Some("v1"),
        );

        let policy = FreshnessPolicy {
            maximum_lag_minutes: 60 * 24, // 24 hours
        };

        let result = compute_staleness(&partition, &policy, Some("v1"), now);

        assert!(result.is_stale);
        assert_eq!(result.reason, Some(StalenessReason::FreshnessPolicy));
        assert!(result.stale_since.is_some());
    }

    #[test]
    fn test_staleness_fresh_partition() {
        let now = Utc::now();
        let partition = make_partition(
            "analytics.daily",
            "2025-01-15",
            Some(now - Duration::hours(1)),
            Some("v1"),
        );

        let policy = FreshnessPolicy {
            maximum_lag_minutes: 60 * 24, // 24 hours
        };

        let result = compute_staleness(&partition, &policy, Some("v1"), now);

        assert!(!result.is_stale);
        assert!(result.reason.is_none());
        assert!(result.stale_since.is_none());
    }

    #[test]
    fn test_staleness_detects_code_changed() {
        let now = Utc::now();
        let partition = make_partition(
            "analytics.daily",
            "2025-01-15",
            Some(now - Duration::hours(1)),
            Some("v1"),
        );

        let policy = FreshnessPolicy {
            maximum_lag_minutes: 60 * 24,
        };

        let result = compute_staleness(&partition, &policy, Some("v2"), now);

        assert!(result.is_stale);
        assert_eq!(result.reason, Some(StalenessReason::CodeChanged));
    }

    #[test]
    fn test_staleness_detects_upstream_changed() {
        let now = Utc::now();

        let downstream = make_partition(
            "analytics.summary",
            "2025-01",
            Some(now - Duration::hours(2)),
            Some("v1"),
        );

        let upstream = make_partition(
            "analytics.daily",
            "2025-01-15",
            Some(now - Duration::hours(1)), // More recent than downstream
            Some("v1"),
        );

        let policy = FreshnessPolicy::default();
        let result =
            compute_staleness_with_upstreams(&downstream, &[upstream], &policy, Some("v1"), now);

        assert!(result.is_stale);
        assert_eq!(result.reason, Some(StalenessReason::UpstreamChanged));
    }

    #[test]
    fn test_staleness_detects_upstream_never_materialized() {
        let now = Utc::now();

        let downstream = make_partition(
            "analytics.summary",
            "2025-01",
            Some(now - Duration::hours(2)),
            Some("v1"),
        );

        let upstream = make_partition("analytics.daily", "2025-01-15", None, Some("v1"));

        let policy = FreshnessPolicy::default();
        let result =
            compute_staleness_with_upstreams(&downstream, &[upstream], &policy, Some("v1"), now);

        assert!(result.is_stale);
        assert_eq!(
            result.reason,
            Some(StalenessReason::UpstreamNeverMaterialized)
        );
    }

    #[test]
    fn test_staleness_fresh_with_older_upstream() {
        let now = Utc::now();

        let downstream = make_partition(
            "analytics.summary",
            "2025-01",
            Some(now - Duration::hours(1)),
            Some("v1"),
        );

        let upstream = make_partition(
            "analytics.daily",
            "2025-01-15",
            Some(now - Duration::hours(2)), // Older than downstream
            Some("v1"),
        );

        let policy = FreshnessPolicy::default();
        let result =
            compute_staleness_with_upstreams(&downstream, &[upstream], &policy, Some("v1"), now);

        assert!(!result.is_stale);
        assert!(result.reason.is_none());
    }

    #[test]
    fn test_staleness_with_upstreams_checks_freshness_policy() {
        let now = Utc::now();

        let downstream = make_partition(
            "analytics.summary",
            "2025-01",
            Some(now - Duration::hours(30)),
            Some("v1"),
        );

        let upstream = make_partition(
            "analytics.daily",
            "2025-01-15",
            Some(now - Duration::hours(40)),
            Some("v1"),
        );

        let policy = FreshnessPolicy {
            maximum_lag_minutes: 60 * 24, // 24 hours
        };
        let result =
            compute_staleness_with_upstreams(&downstream, &[upstream], &policy, Some("v1"), now);

        assert!(result.is_stale);
        assert_eq!(result.reason, Some(StalenessReason::FreshnessPolicy));
    }
}
