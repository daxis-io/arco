//! Retention policy configuration.

use serde::{Deserialize, Serialize};

/// Retention policy for catalog artifacts.
///
/// Controls how long various artifacts are kept before garbage collection
/// can remove them. Sensible defaults are provided for typical workloads.
///
/// # Design Principles
///
/// - **Delay window**: All deletions have a minimum age to prevent racing
///   with active readers who may have cached old manifest pointers
/// - **Version retention**: Keep enough snapshot versions to allow rollback
/// - **Ledger retention**: Keep compacted events for replay/audit purposes
///
/// # Example
///
/// ```rust
/// use arco_catalog::gc::RetentionPolicy;
///
/// // Use defaults
/// let policy = RetentionPolicy::default();
///
/// // Or customize
/// let policy = RetentionPolicy {
///     keep_snapshots: 5,
///     delay_hours: 12,
///     ledger_retention_hours: 24,
///     max_age_days: 30,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetentionPolicy {
    /// Keep last N snapshots per domain (e.g., 10).
    ///
    /// Allows rollback to recent versions. Older versions are candidates
    /// for deletion after the delay window expires.
    pub keep_snapshots: u32,

    /// Minimum age (in hours) before deletion (e.g., 24).
    ///
    /// Prevents racing with active readers who may have cached old
    /// manifest pointers. Any artifact younger than this age is safe
    /// from garbage collection.
    pub delay_hours: u32,

    /// Ledger retention after compaction (in hours, e.g., 48).
    ///
    /// Compacted events are kept for this duration for replay/debugging/audit
    /// purposes. After this window, they can be deleted.
    pub ledger_retention_hours: u32,

    /// Maximum age for any artifact (in days, e.g., 90).
    ///
    /// Hard limit for compliance/cost control. Any artifact older than
    /// this is always a deletion candidate, regardless of other settings.
    pub max_age_days: u32,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            keep_snapshots: 10,
            delay_hours: 24,
            ledger_retention_hours: 48,
            max_age_days: 90,
        }
    }
}

impl RetentionPolicy {
    /// Creates a new retention policy with the specified values.
    #[must_use]
    pub const fn new(
        keep_snapshots: u32,
        delay_hours: u32,
        ledger_retention_hours: u32,
        max_age_days: u32,
    ) -> Self {
        Self {
            keep_snapshots,
            delay_hours,
            ledger_retention_hours,
            max_age_days,
        }
    }

    /// Creates a policy suitable for development/testing with short retention.
    ///
    /// - Keep 3 snapshots
    /// - 1 hour delay
    /// - 2 hour ledger retention
    /// - 7 day max age
    #[must_use]
    pub const fn development() -> Self {
        Self {
            keep_snapshots: 3,
            delay_hours: 1,
            ledger_retention_hours: 2,
            max_age_days: 7,
        }
    }

    /// Creates an aggressive policy for cost-sensitive workloads.
    ///
    /// - Keep 5 snapshots
    /// - 12 hour delay
    /// - 24 hour ledger retention
    /// - 30 day max age
    #[must_use]
    pub const fn aggressive() -> Self {
        Self {
            keep_snapshots: 5,
            delay_hours: 12,
            ledger_retention_hours: 24,
            max_age_days: 30,
        }
    }

    /// Creates a conservative policy for compliance-heavy workloads.
    ///
    /// - Keep 30 snapshots
    /// - 48 hour delay
    /// - 168 hour (7 day) ledger retention
    /// - 365 day max age
    #[must_use]
    pub const fn conservative() -> Self {
        Self {
            keep_snapshots: 30,
            delay_hours: 48,
            ledger_retention_hours: 168,
            max_age_days: 365,
        }
    }

    /// Validates the policy settings are reasonable.
    ///
    /// Returns an error message if validation fails.
    #[must_use]
    pub fn validate(&self) -> Option<String> {
        if self.keep_snapshots == 0 {
            return Some("keep_snapshots must be at least 1".to_string());
        }
        if self.delay_hours == 0 {
            return Some("delay_hours must be at least 1".to_string());
        }
        if self.max_age_days == 0 {
            return Some("max_age_days must be at least 1".to_string());
        }
        if self.ledger_retention_hours > self.max_age_days * 24 {
            return Some(format!(
                "ledger_retention_hours ({}) cannot exceed max_age_days ({} = {} hours)",
                self.ledger_retention_hours,
                self.max_age_days,
                self.max_age_days * 24
            ));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_policy() {
        let policy = RetentionPolicy::default();
        assert_eq!(policy.keep_snapshots, 10);
        assert_eq!(policy.delay_hours, 24);
        assert_eq!(policy.ledger_retention_hours, 48);
        assert_eq!(policy.max_age_days, 90);
    }

    #[test]
    fn test_development_policy() {
        let policy = RetentionPolicy::development();
        assert_eq!(policy.keep_snapshots, 3);
        assert_eq!(policy.delay_hours, 1);
    }

    #[test]
    fn test_aggressive_policy() {
        let policy = RetentionPolicy::aggressive();
        assert_eq!(policy.keep_snapshots, 5);
        assert_eq!(policy.max_age_days, 30);
    }

    #[test]
    fn test_conservative_policy() {
        let policy = RetentionPolicy::conservative();
        assert_eq!(policy.keep_snapshots, 30);
        assert_eq!(policy.max_age_days, 365);
    }

    #[test]
    fn test_validation_keep_snapshots() {
        let policy = RetentionPolicy {
            keep_snapshots: 0,
            ..Default::default()
        };
        assert!(policy.validate().is_some());
    }

    #[test]
    fn test_validation_delay_hours() {
        let policy = RetentionPolicy {
            delay_hours: 0,
            ..Default::default()
        };
        assert!(policy.validate().is_some());
    }

    #[test]
    fn test_validation_ledger_exceeds_max() {
        let policy = RetentionPolicy {
            ledger_retention_hours: 2500, // > 90 days * 24
            max_age_days: 90,
            ..Default::default()
        };
        assert!(policy.validate().is_some());
    }

    #[test]
    fn test_valid_policy() {
        let policy = RetentionPolicy::default();
        assert!(policy.validate().is_none());
    }

    #[test]
    fn test_serde_roundtrip() {
        let policy = RetentionPolicy::default();
        let json = serde_json::to_string(&policy).expect("serialize");
        let parsed: RetentionPolicy = serde_json::from_str(&json).expect("parse");
        assert_eq!(policy, parsed);
    }
}
