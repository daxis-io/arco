//! Quota management and fair scheduling for multi-tenant orchestration.
//!
//! This module provides:
//!
//! - [`QuotaManager`]: Trait for tenant quota enforcement
//! - [`DrrScheduler`]: Deficit round-robin scheduler for fair task distribution
//! - [`TenantQuota`]: Configuration for per-tenant resource limits
//!
//! ## Design Principles
//!
//! - **Fairness**: No tenant should monopolize scheduler capacity
//! - **Configurability**: Per-tenant quotas allow different SLAs
//! - **Burst handling**: DRR accumulates deficit allowing controlled bursts

pub mod drr;
pub mod memory;

use async_trait::async_trait;

use crate::error::Result;

/// Configuration for a tenant's resource quota.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantQuota {
    /// Maximum concurrent tasks for this tenant.
    pub max_concurrent_tasks: usize,
    /// Priority weight for DRR scheduling (higher = more capacity).
    /// Default: 1
    pub weight: u32,
}

impl Default for TenantQuota {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: 10,
            weight: 1,
        }
    }
}

impl TenantQuota {
    /// Creates a new tenant quota with the specified max concurrent tasks.
    #[must_use]
    pub const fn new(max_concurrent_tasks: usize) -> Self {
        Self {
            max_concurrent_tasks,
            weight: 1,
        }
    }

    /// Creates a quota with custom weight for DRR scheduling.
    #[must_use]
    pub const fn with_weight(max_concurrent_tasks: usize, weight: u32) -> Self {
        Self {
            max_concurrent_tasks,
            weight,
        }
    }
}

/// Result of a quota check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaDecision {
    /// Dispatch is allowed.
    Allowed,
    /// Dispatch is denied due to quota.
    Denied {
        /// Reason for denial.
        reason: QuotaDenialReason,
    },
}

impl QuotaDecision {
    /// Returns true if dispatch is allowed.
    #[must_use]
    pub const fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }
}

/// Reason for quota denial.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaDenialReason {
    /// Tenant has reached max concurrent task limit.
    MaxConcurrentTasks {
        /// Current number of running tasks.
        current: usize,
        /// Maximum allowed.
        limit: usize,
    },
    /// Tenant is unknown (no quota configured).
    UnknownTenant,
    /// Global capacity exhausted.
    GlobalCapacityExhausted,
}

impl std::fmt::Display for QuotaDenialReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaxConcurrentTasks { current, limit } => {
                write!(
                    f,
                    "max concurrent tasks reached: {current}/{limit}"
                )
            }
            Self::UnknownTenant => write!(f, "unknown tenant"),
            Self::GlobalCapacityExhausted => write!(f, "global capacity exhausted"),
        }
    }
}

/// Quota manager for enforcing tenant resource limits.
///
/// Implementations track active task counts per tenant and enforce
/// configurable limits to prevent resource monopolization.
///
/// ## Thread Safety
///
/// All methods are `Send + Sync` to support concurrent access from
/// multiple scheduler tasks.
#[async_trait]
pub trait QuotaManager: Send + Sync {
    /// Checks if a tenant can dispatch the specified number of tasks.
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - The tenant identifier
    /// * `task_count` - Number of tasks to dispatch
    ///
    /// # Returns
    ///
    /// - `QuotaDecision::Allowed` if the tenant has capacity
    /// - `QuotaDecision::Denied` with reason if quota would be exceeded
    async fn can_dispatch(&self, tenant_id: &str, task_count: usize) -> Result<QuotaDecision>;

    /// Atomically checks and reserves quota for the specified number of tasks.
    ///
    /// # Safety
    ///
    /// **Production implementations MUST override this method** to provide
    /// atomic check-and-reserve semantics. The default implementation composes
    /// `can_dispatch` + `record_dispatch` which has a TOCTOU race condition
    /// that can cause quota oversubscription under concurrent access.
    ///
    /// The default is provided only for simple single-threaded test scenarios.
    async fn try_dispatch(&self, tenant_id: &str, task_count: usize) -> Result<QuotaDecision> {
        let decision = self.can_dispatch(tenant_id, task_count).await?;
        if decision.is_allowed() {
            self.record_dispatch(tenant_id, task_count).await?;
        }
        Ok(decision)
    }

    /// Records that tasks have been dispatched for a tenant.
    ///
    /// This should be called after successful dispatch to update
    /// the active task count.
    async fn record_dispatch(&self, tenant_id: &str, task_count: usize) -> Result<()>;

    /// Records that tasks have completed for a tenant.
    ///
    /// This should be called when tasks reach terminal state to
    /// release quota capacity.
    async fn record_completion(&self, tenant_id: &str, task_count: usize) -> Result<()>;

    /// Gets the current active task count for a tenant.
    async fn active_tasks(&self, tenant_id: &str) -> Result<usize>;

    /// Gets the quota configuration for a tenant.
    async fn get_quota(&self, tenant_id: &str) -> Result<Option<TenantQuota>>;

    /// Sets the quota configuration for a tenant.
    async fn set_quota(&self, tenant_id: &str, quota: TenantQuota) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_quota_default() {
        let quota = TenantQuota::default();
        assert_eq!(quota.max_concurrent_tasks, 10);
        assert_eq!(quota.weight, 1);
    }

    #[test]
    fn tenant_quota_with_weight() {
        let quota = TenantQuota::with_weight(20, 3);
        assert_eq!(quota.max_concurrent_tasks, 20);
        assert_eq!(quota.weight, 3);
    }

    #[test]
    fn quota_decision_is_allowed() {
        assert!(QuotaDecision::Allowed.is_allowed());
        assert!(!QuotaDecision::Denied {
            reason: QuotaDenialReason::UnknownTenant
        }
        .is_allowed());
    }

    #[test]
    fn quota_denial_reason_display() {
        let reason = QuotaDenialReason::MaxConcurrentTasks {
            current: 10,
            limit: 10,
        };
        assert!(reason.to_string().contains("10/10"));
    }
}
