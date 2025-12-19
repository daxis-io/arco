//! In-memory quota manager implementation for testing.
//!
//! This module provides [`InMemoryQuotaManager`], a simple in-memory implementation
//! of the [`QuotaManager`] trait suitable for testing and development.
//!
//! ## Limitations
//!
//! - **NOT suitable for production**: No persistence, no cross-process coordination
//! - **Single-process only**: State is not shared across process boundaries

use std::collections::HashMap;
use std::sync::{PoisonError, RwLock};

use async_trait::async_trait;

use super::{QuotaDecision, QuotaDenialReason, QuotaManager, TenantQuota};
use crate::error::{Error, Result};

/// State for a single tenant.
#[derive(Debug, Clone)]
struct TenantState {
    /// The tenant's quota configuration.
    quota: TenantQuota,
    /// Current number of active (dispatched but not completed) tasks.
    active_tasks: usize,
}

/// In-memory quota manager for testing.
///
/// Provides a simple, thread-safe implementation of the [`QuotaManager`] trait
/// using `RwLock` for synchronization.
///
/// ## Example
///
/// ```rust
/// use arco_flow::quota::memory::InMemoryQuotaManager;
/// use arco_flow::quota::TenantQuota;
///
/// let manager = InMemoryQuotaManager::new();
/// // Configure quotas for tenants...
/// ```
#[derive(Debug)]
pub struct InMemoryQuotaManager {
    tenants: RwLock<HashMap<String, TenantState>>,
    /// Default quota for unknown tenants (if Some, allows unknown tenants).
    default_quota: Option<TenantQuota>,
}

impl Default for InMemoryQuotaManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Converts a lock poison error to a storage error.
fn poison_err<T>(_: PoisonError<T>) -> Error {
    Error::storage("quota manager lock poisoned")
}

impl InMemoryQuotaManager {
    /// Creates a new in-memory quota manager.
    ///
    /// Unknown tenants will be denied by default.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tenants: RwLock::new(HashMap::new()),
            default_quota: None,
        }
    }

    /// Creates a quota manager with a default quota for unknown tenants.
    ///
    /// This allows new tenants to be accepted with the default quota.
    #[must_use]
    pub fn with_default_quota(default_quota: TenantQuota) -> Self {
        Self {
            tenants: RwLock::new(HashMap::new()),
            default_quota: Some(default_quota),
        }
    }

    /// Pre-configures quotas for multiple tenants.
    #[must_use]
    pub fn with_quotas(quotas: impl IntoIterator<Item = (String, TenantQuota)>) -> Self {
        let tenants: HashMap<String, TenantState> = quotas
            .into_iter()
            .map(|(tenant_id, quota)| {
                (
                    tenant_id,
                    TenantState {
                        quota,
                        active_tasks: 0,
                    },
                )
            })
            .collect();

        Self {
            tenants: RwLock::new(tenants),
            default_quota: None,
        }
    }

    /// Gets the number of configured tenants.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned.
    pub fn tenant_count(&self) -> Result<usize> {
        let tenants = self.tenants.read().map_err(poison_err)?;
        Ok(tenants.len())
    }
}

#[async_trait]
impl QuotaManager for InMemoryQuotaManager {
    async fn can_dispatch(&self, tenant_id: &str, task_count: usize) -> Result<QuotaDecision> {
        let tenants = self.tenants.read().map_err(poison_err)?;

        let Some(state) = tenants.get(tenant_id) else {
            // Handle unknown tenant
            if self.default_quota.is_some() {
                // Unknown tenant with default quota - allow if under default limit
                let default = self
                    .default_quota
                    .as_ref()
                    .map_or(10, |q| q.max_concurrent_tasks);
                drop(tenants);
                return if task_count <= default {
                    Ok(QuotaDecision::Allowed)
                } else {
                    Ok(QuotaDecision::Denied {
                        reason: QuotaDenialReason::MaxConcurrentTasks {
                            current: 0,
                            limit: default,
                        },
                    })
                };
            }
            drop(tenants);
            return Ok(QuotaDecision::Denied {
                reason: QuotaDenialReason::UnknownTenant,
            });
        };

        let available = state.quota.max_concurrent_tasks.saturating_sub(state.active_tasks);

        if task_count <= available {
            drop(tenants);
            Ok(QuotaDecision::Allowed)
        } else {
            let current = state.active_tasks;
            let limit = state.quota.max_concurrent_tasks;
            drop(tenants);
            Ok(QuotaDecision::Denied {
                reason: QuotaDenialReason::MaxConcurrentTasks { current, limit },
            })
        }
    }

    async fn try_dispatch(&self, tenant_id: &str, task_count: usize) -> Result<QuotaDecision> {
        let mut tenants = self.tenants.write().map_err(poison_err)?;

        if let Some(state) = tenants.get_mut(tenant_id) {
            let limit = state.quota.max_concurrent_tasks;
            let allowed = state
                .active_tasks
                .checked_add(task_count)
                .is_some_and(|new_total| new_total <= limit);

            if allowed {
                state.active_tasks = state.active_tasks.saturating_add(task_count);
                return Ok(QuotaDecision::Allowed);
            }

            return Ok(QuotaDecision::Denied {
                reason: QuotaDenialReason::MaxConcurrentTasks {
                    current: state.active_tasks,
                    limit,
                },
            });
        }

        let Some(default_quota) = self.default_quota.clone() else {
            return Ok(QuotaDecision::Denied {
                reason: QuotaDenialReason::UnknownTenant,
            });
        };

        let limit = default_quota.max_concurrent_tasks;
        let allowed = task_count <= limit;
        if !allowed {
            return Ok(QuotaDecision::Denied {
                reason: QuotaDenialReason::MaxConcurrentTasks { current: 0, limit },
            });
        }

        tenants.insert(
            tenant_id.to_string(),
            TenantState {
                quota: default_quota,
                active_tasks: task_count,
            },
        );
        drop(tenants);

        Ok(QuotaDecision::Allowed)
    }

    async fn record_dispatch(&self, tenant_id: &str, task_count: usize) -> Result<()> {
        let mut tenants = self.tenants.write().map_err(poison_err)?;

        let state = if let Some(state) = tenants.get_mut(tenant_id) {
            state
        } else {
            let default_quota = self.default_quota.clone().ok_or_else(|| {
                Error::configuration(format!("unknown tenant: {tenant_id}"))
            })?;
            tenants.insert(
                tenant_id.to_string(),
                TenantState {
                    quota: default_quota,
                    active_tasks: 0,
                },
            );
            tenants
                .get_mut(tenant_id)
                .ok_or_else(|| Error::storage("failed to initialize tenant state"))?
        };

        state.active_tasks = state.active_tasks.saturating_add(task_count);
        drop(tenants);
        Ok(())
    }

    async fn record_completion(&self, tenant_id: &str, task_count: usize) -> Result<()> {
        let mut tenants = self.tenants.write().map_err(poison_err)?;

        if let Some(state) = tenants.get_mut(tenant_id) {
            state.active_tasks = state.active_tasks.saturating_sub(task_count);
        }
        drop(tenants);
        Ok(())
    }

    async fn active_tasks(&self, tenant_id: &str) -> Result<usize> {
        let tenants = self.tenants.read().map_err(poison_err)?;
        let count = tenants.get(tenant_id).map_or(0, |s| s.active_tasks);
        drop(tenants);
        Ok(count)
    }

    async fn get_quota(&self, tenant_id: &str) -> Result<Option<TenantQuota>> {
        let tenants = self.tenants.read().map_err(poison_err)?;
        let quota = tenants.get(tenant_id).map(|s| s.quota.clone());
        drop(tenants);
        Ok(quota)
    }

    async fn set_quota(&self, tenant_id: &str, quota: TenantQuota) -> Result<()> {
        let mut tenants = self.tenants.write().map_err(poison_err)?;

        tenants
            .entry(tenant_id.to_string())
            .and_modify(|s| s.quota = quota.clone())
            .or_insert_with(|| TenantState {
                quota,
                active_tasks: 0,
            });

        drop(tenants);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn allows_dispatch_under_quota() -> Result<()> {
        let manager = InMemoryQuotaManager::with_quotas(vec![(
            "tenant-1".to_string(),
            TenantQuota::new(10),
        )]);

        let decision = manager.can_dispatch("tenant-1", 5).await?;
        assert!(decision.is_allowed());

        Ok(())
    }

    #[tokio::test]
    async fn try_dispatch_reserves_quota() -> Result<()> {
        let manager = InMemoryQuotaManager::with_quotas(vec![(
            "tenant-1".to_string(),
            TenantQuota::new(5),
        )]);

        let decision = manager.try_dispatch("tenant-1", 3).await?;
        assert!(decision.is_allowed());
        assert_eq!(manager.active_tasks("tenant-1").await?, 3);

        let decision = manager.try_dispatch("tenant-1", 3).await?;
        assert!(!decision.is_allowed());
        assert_eq!(manager.active_tasks("tenant-1").await?, 3);

        Ok(())
    }

    #[tokio::test]
    async fn try_dispatch_unknown_tenant_default_quota() -> Result<()> {
        let manager = InMemoryQuotaManager::with_default_quota(TenantQuota::new(4));

        let decision = manager.try_dispatch("tenant-new", 2).await?;
        assert!(decision.is_allowed());
        assert_eq!(manager.active_tasks("tenant-new").await?, 2);

        Ok(())
    }

    #[tokio::test]
    async fn denies_dispatch_over_quota() -> Result<()> {
        let manager = InMemoryQuotaManager::with_quotas(vec![(
            "tenant-1".to_string(),
            TenantQuota::new(5),
        )]);

        // Record some active tasks
        manager.record_dispatch("tenant-1", 4).await?;

        // Try to dispatch more than available
        let decision = manager.can_dispatch("tenant-1", 3).await?;
        assert!(!decision.is_allowed());

        if let QuotaDecision::Denied { reason } = decision {
            assert!(matches!(
                reason,
                QuotaDenialReason::MaxConcurrentTasks { current: 4, limit: 5 }
            ));
        } else {
            panic!("Expected Denied");
        }

        Ok(())
    }

    #[tokio::test]
    async fn denies_unknown_tenant_without_default() -> Result<()> {
        let manager = InMemoryQuotaManager::new();

        let decision = manager.can_dispatch("unknown", 1).await?;
        assert!(!decision.is_allowed());

        if let QuotaDecision::Denied { reason } = decision {
            assert!(matches!(reason, QuotaDenialReason::UnknownTenant));
        } else {
            panic!("Expected Denied");
        }

        Ok(())
    }

    #[tokio::test]
    async fn record_dispatch_unknown_tenant_without_default_fails() {
        let manager = InMemoryQuotaManager::new();

        let result = manager.record_dispatch("unknown", 1).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn allows_unknown_tenant_with_default() -> Result<()> {
        let manager = InMemoryQuotaManager::with_default_quota(TenantQuota::new(5));

        let decision = manager.can_dispatch("new-tenant", 3).await?;
        assert!(decision.is_allowed());

        Ok(())
    }

    #[tokio::test]
    async fn record_dispatch_and_completion() -> Result<()> {
        let manager = InMemoryQuotaManager::with_quotas(vec![(
            "tenant-1".to_string(),
            TenantQuota::new(10),
        )]);

        assert_eq!(manager.active_tasks("tenant-1").await?, 0);

        manager.record_dispatch("tenant-1", 5).await?;
        assert_eq!(manager.active_tasks("tenant-1").await?, 5);

        manager.record_completion("tenant-1", 3).await?;
        assert_eq!(manager.active_tasks("tenant-1").await?, 2);

        Ok(())
    }

    #[tokio::test]
    async fn set_and_get_quota() -> Result<()> {
        let manager = InMemoryQuotaManager::new();

        assert!(manager.get_quota("tenant-1").await?.is_none());

        manager
            .set_quota("tenant-1", TenantQuota::with_weight(20, 3))
            .await?;

        let quota = manager.get_quota("tenant-1").await?;
        assert!(quota.is_some());
        let quota = quota.unwrap();
        assert_eq!(quota.max_concurrent_tasks, 20);
        assert_eq!(quota.weight, 3);

        Ok(())
    }

    #[tokio::test]
    async fn completion_doesnt_go_negative() -> Result<()> {
        let manager = InMemoryQuotaManager::with_quotas(vec![(
            "tenant-1".to_string(),
            TenantQuota::new(10),
        )]);

        // Record more completions than dispatches
        manager.record_completion("tenant-1", 100).await?;
        assert_eq!(manager.active_tasks("tenant-1").await?, 0);

        Ok(())
    }
}
