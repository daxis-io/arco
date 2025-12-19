# ADR-016: Tenant Quotas and Fairness

## Status
Accepted

## Context
Multi-tenant orchestration requires:
- **Resource isolation**: Prevent one tenant from monopolizing scheduler capacity
- **Fairness**: Ensure all tenants make progress, even under load
- **Burst handling**: Allow temporary quota overages without dropping work
- **Observability**: Track quota usage for capacity planning

Without quotas, a single tenant with many pending tasks could starve others.

## Decision
Implement a `QuotaManager` with Deficit Round-Robin (DRR) fairness:

### QuotaManager Trait
```rust
#[async_trait]
pub trait QuotaManager: Send + Sync {
    /// Check if a tenant can dispatch the specified number of tasks.
    async fn can_dispatch(&self, tenant_id: &str, task_count: usize) -> Result<QuotaDecision>;

    /// Atomically check and reserve quota for dispatch.
    async fn try_dispatch(&self, tenant_id: &str, task_count: usize) -> Result<QuotaDecision>;

    /// Record a successful dispatch (increments active task count).
    async fn record_dispatch(&self, tenant_id: &str, task_count: usize) -> Result<()>;

    /// Record task completion (frees quota).
    async fn record_completion(&self, tenant_id: &str, task_count: usize) -> Result<()>;

    /// Current active task count for a tenant.
    async fn active_tasks(&self, tenant_id: &str) -> Result<usize>;

    /// Get quota configuration for a tenant.
    async fn get_quota(&self, tenant_id: &str) -> Result<Option<TenantQuota>>;

    /// Set quota configuration for a tenant.
    async fn set_quota(&self, tenant_id: &str, quota: TenantQuota) -> Result<()>;
}
```

### Quota Model
```rust
pub struct TenantQuota {
    pub max_concurrent_tasks: usize, // Maximum concurrent dispatched tasks
    pub weight: u32,                 // DRR scheduling weight (quantum)
}
```

### DRR Fairness
When multiple tenants have pending work:
1. Each tenant gets a "quantum" of dispatch opportunities
2. Tenants with deficit (used less than their quantum) get priority
3. Round-robin across tenants at each scheduler tick
4. Starvation-free: every tenant eventually gets scheduled

### Default Quotas
- `max_concurrent_tasks`: 10
- `weight`: 1

Quotas are configurable per-tenant through configuration.

## Consequences

### Positive
- Prevents tenant starvation under load
- Predictable behavior with clear limits
- DRR provides fair scheduling without complex priority schemes
- Metrics expose quota usage for capacity planning

### Negative
- Adds latency to dispatch path (quota check)
- Configuration complexity for per-tenant quotas
- DRR may not be optimal for priority-based workloads
- Memory overhead for tracking per-tenant state
