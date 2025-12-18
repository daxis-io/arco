//! Deficit Round-Robin (DRR) scheduler for fair multi-tenant task distribution.
//!
//! DRR provides weighted fair scheduling by tracking a "deficit" counter for each
//! tenant. Each scheduling round:
//!
//! 1. Tenants with ready tasks have their deficit increased by their quantum
//! 2. Tasks are dispatched from the tenant with highest deficit
//! 3. Deficit decreases by 1 for each task dispatched
//!
//! This ensures:
//! - Long-term fairness: Each tenant gets capacity proportional to their weight
//! - Burst handling: Accumulated deficit allows controlled bursts
//! - Work conservation: Idle tenant capacity is redistributed

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::task::TaskExecution;

/// Entry for a tenant in the DRR scheduler.
#[derive(Debug, Clone)]
pub struct TenantEntry {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Current deficit counter.
    pub deficit: i64,
    /// Weight (quantum added per round).
    pub weight: u32,
    /// Ready tasks for this tenant.
    pub tasks: Vec<TaskExecution>,
}

impl TenantEntry {
    /// Creates a new tenant entry.
    #[must_use]
    pub fn new(tenant_id: impl Into<String>, weight: u32) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            deficit: 0,
            weight,
            tasks: Vec::new(),
        }
    }
}

// Implement ordering for the heap (highest deficit first)
impl PartialEq for TenantEntry {
    fn eq(&self, other: &Self) -> bool {
        self.deficit == other.deficit && self.tenant_id == other.tenant_id
    }
}

impl Eq for TenantEntry {}

impl PartialOrd for TenantEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TenantEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher deficit = higher priority (max-heap)
        // Tie-break by tenant_id for determinism
        self.deficit
            .cmp(&other.deficit)
            .then_with(|| self.tenant_id.cmp(&other.tenant_id))
    }
}

/// Result of selecting a task for dispatch.
#[derive(Debug, Clone)]
pub struct DispatchSelection {
    /// The selected task.
    pub task: TaskExecution,
    /// The tenant that owns the task.
    pub tenant_id: String,
}

/// Deficit Round-Robin scheduler for fair multi-tenant task distribution.
///
/// ## Algorithm
///
/// DRR maintains a deficit counter for each tenant. When selecting tasks:
///
/// 1. Add quantum (weight) to each tenant with ready tasks
/// 2. Select from tenant with highest deficit
/// 3. Decrement deficit by 1 for each dispatched task
///
/// ## Example
///
/// ```rust
/// use arco_flow::quota::drr::DrrScheduler;
///
/// let mut drr = DrrScheduler::new();
/// // Add ready tasks from multiple tenants...
/// // drr.add_ready_tasks("tenant-a", 2, tasks_a);
/// // drr.add_ready_tasks("tenant-b", 1, tasks_b);
/// //
/// // Select tasks fairly
/// // while let Some(selection) = drr.select_next() { ... }
/// ```
#[derive(Debug)]
pub struct DrrScheduler {
    /// Tenant entries by tenant ID.
    tenants: HashMap<String, TenantEntry>,
    /// Whether a scheduling round has been started.
    round_started: bool,
}

impl Default for DrrScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl DrrScheduler {
    /// Creates a new DRR scheduler.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tenants: HashMap::new(),
            round_started: false,
        }
    }

    /// Adds ready tasks for a tenant.
    ///
    /// If the tenant doesn't exist, creates an entry with the specified weight.
    /// Tasks should be pre-sorted by priority (lower = higher priority).
    pub fn add_ready_tasks(
        &mut self,
        tenant_id: impl Into<String>,
        weight: u32,
        tasks: Vec<TaskExecution>,
    ) {
        let tenant_id = tenant_id.into();
        let weight = weight.max(1); // Minimum weight of 1

        let entry = self
            .tenants
            .entry(tenant_id.clone())
            .or_insert_with(|| TenantEntry::new(tenant_id, weight));

        entry.tasks.extend(tasks);
        // Reset round state when new tasks arrive
        self.round_started = false;
    }

    /// Clears all tasks and deficits.
    pub fn clear(&mut self) {
        self.tenants.clear();
        self.round_started = false;
    }

    /// Returns the number of tenants with tasks.
    #[must_use]
    pub fn tenant_count(&self) -> usize {
        self.tenants.values().filter(|t| !t.tasks.is_empty()).count()
    }

    /// Returns the total number of ready tasks across all tenants.
    #[must_use]
    pub fn total_tasks(&self) -> usize {
        self.tenants.values().map(|t| t.tasks.len()).sum()
    }

    /// Starts a new scheduling round.
    ///
    /// Adds quantum to each tenant with ready tasks.
    fn start_round(&mut self) {
        for entry in self.tenants.values_mut() {
            if !entry.tasks.is_empty() {
                entry.deficit += i64::from(entry.weight);
            }
        }
        self.round_started = true;
    }

    /// Selects the next task to dispatch using DRR algorithm.
    ///
    /// Returns `None` if no tasks are available.
    #[must_use]
    pub fn select_next(&mut self) -> Option<DispatchSelection> {
        if !self.round_started {
            self.start_round();
        }

        // Find tenant with highest deficit that has tasks
        // Use deterministic selection by collecting and sorting
        let mut candidates: Vec<_> = self
            .tenants
            .values()
            .filter(|t| !t.tasks.is_empty())
            .collect();

        if candidates.is_empty() {
            return None;
        }

        // Sort by (deficit DESC, tenant_id ASC) for determinism
        candidates.sort_by(|a, b| {
            b.deficit
                .cmp(&a.deficit)
                .then_with(|| a.tenant_id.cmp(&b.tenant_id))
        });

        let selected_tenant_id = candidates.first()?.tenant_id.clone();

        // Take one task from the selected tenant
        let entry = self.tenants.get_mut(&selected_tenant_id)?;
        let task = entry.tasks.remove(0);
        entry.deficit -= 1;

        // If tenant has no more tasks and deficit <= 0, we can start a new round next time
        if entry.tasks.is_empty() {
            self.round_started = false;
        }

        Some(DispatchSelection {
            task,
            tenant_id: selected_tenant_id,
        })
    }

    /// Selects up to `count` tasks using DRR fairness.
    ///
    /// This is the primary interface for fair task selection.
    #[must_use]
    pub fn select_batch(&mut self, count: usize) -> Vec<DispatchSelection> {
        let mut selections = Vec::with_capacity(count);

        for _ in 0..count {
            match self.select_next() {
                Some(selection) => selections.push(selection),
                None => break,
            }
        }

        selections
    }

    /// Gets the current deficit for a tenant.
    #[must_use]
    pub fn get_deficit(&self, tenant_id: &str) -> i64 {
        self.tenants.get(tenant_id).map_or(0, |t| t.deficit)
    }
}

/// Builder for fair task selection that integrates DRR with task groups.
pub struct FairTaskSelector {
    drr: DrrScheduler,
}

impl Default for FairTaskSelector {
    fn default() -> Self {
        Self::new()
    }
}

impl FairTaskSelector {
    /// Creates a new fair task selector.
    #[must_use]
    pub fn new() -> Self {
        Self {
            drr: DrrScheduler::new(),
        }
    }

    /// Adds ready tasks grouped by tenant.
    ///
    /// Each entry is `(tenant_id, weight, tasks)`.
    pub fn add_tenant_tasks(&mut self, tenant_id: &str, weight: u32, tasks: Vec<TaskExecution>) {
        self.drr.add_ready_tasks(tenant_id, weight, tasks);
    }

    /// Selects up to `count` tasks fairly across tenants.
    #[must_use]
    pub fn select_fair(&mut self, count: usize) -> Vec<DispatchSelection> {
        self.drr.select_batch(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::TaskId;

    fn make_task(priority: i32) -> TaskExecution {
        TaskExecution::new(TaskId::generate()).with_metadata(
            crate::task_key::TaskKey::new(
                crate::plan::AssetKey::new("ns", "name"),
                crate::task_key::TaskOperation::Materialize,
            ),
            priority,
        )
    }

    #[test]
    fn drr_fair_distribution_equal_weights() {
        let mut drr = DrrScheduler::new();

        // Two tenants with equal weight, each with 4 tasks
        let tasks_a: Vec<_> = (0..4).map(|_| make_task(0)).collect();
        let tasks_b: Vec<_> = (0..4).map(|_| make_task(0)).collect();

        drr.add_ready_tasks("tenant-a", 1, tasks_a);
        drr.add_ready_tasks("tenant-b", 1, tasks_b);

        // Select 8 tasks
        let selections = drr.select_batch(8);
        assert_eq!(selections.len(), 8);

        // Count tasks per tenant
        let mut counts: HashMap<String, usize> = HashMap::new();
        for sel in &selections {
            *counts.entry(sel.tenant_id.clone()).or_default() += 1;
        }

        // With equal weights, each should get approximately equal share
        assert_eq!(counts.get("tenant-a"), Some(&4));
        assert_eq!(counts.get("tenant-b"), Some(&4));
    }

    #[test]
    fn drr_weighted_distribution() {
        let mut drr = DrrScheduler::new();

        // Tenant A has weight 2, Tenant B has weight 1
        let tasks_a: Vec<_> = (0..6).map(|_| make_task(0)).collect();
        let tasks_b: Vec<_> = (0..6).map(|_| make_task(0)).collect();

        drr.add_ready_tasks("tenant-a", 2, tasks_a);
        drr.add_ready_tasks("tenant-b", 1, tasks_b);

        // Select 6 tasks
        let selections = drr.select_batch(6);
        assert_eq!(selections.len(), 6);

        // Count tasks per tenant
        let mut counts: HashMap<String, usize> = HashMap::new();
        for sel in &selections {
            *counts.entry(sel.tenant_id.clone()).or_default() += 1;
        }

        // With 2:1 weights, tenant-a should get ~4 and tenant-b ~2
        let a_count = *counts.get("tenant-a").unwrap_or(&0);
        let b_count = *counts.get("tenant-b").unwrap_or(&0);

        // Allow some flexibility due to round boundaries
        assert!(a_count >= 3 && a_count <= 5, "tenant-a got {a_count}");
        assert!(b_count >= 1 && b_count <= 3, "tenant-b got {b_count}");
    }

    #[test]
    fn drr_deterministic_tie_breaking() {
        // Run the same scenario twice - should produce identical results
        fn run_scenario() -> Vec<String> {
            let mut drr = DrrScheduler::new();

            let tasks_a: Vec<_> = (0..2).map(|_| make_task(0)).collect();
            let tasks_b: Vec<_> = (0..2).map(|_| make_task(0)).collect();
            let tasks_c: Vec<_> = (0..2).map(|_| make_task(0)).collect();

            drr.add_ready_tasks("tenant-a", 1, tasks_a);
            drr.add_ready_tasks("tenant-b", 1, tasks_b);
            drr.add_ready_tasks("tenant-c", 1, tasks_c);

            drr.select_batch(6)
                .into_iter()
                .map(|s| s.tenant_id)
                .collect()
        }

        let run1 = run_scenario();
        let run2 = run_scenario();

        assert_eq!(run1, run2, "DRR selection should be deterministic");
    }

    #[test]
    fn drr_handles_empty_tenants() {
        let mut drr = DrrScheduler::new();

        // Tenant A has tasks, tenant B is empty
        let tasks_a: Vec<_> = (0..3).map(|_| make_task(0)).collect();
        drr.add_ready_tasks("tenant-a", 1, tasks_a);
        drr.add_ready_tasks("tenant-b", 1, Vec::new());

        let selections = drr.select_batch(5);
        assert_eq!(selections.len(), 3); // Only 3 tasks available

        // All should be from tenant-a
        for sel in &selections {
            assert_eq!(sel.tenant_id, "tenant-a");
        }
    }

    #[test]
    fn drr_work_conservation() {
        let mut drr = DrrScheduler::new();

        // Tenant A has many tasks, tenant B has few
        let tasks_a: Vec<_> = (0..10).map(|_| make_task(0)).collect();
        let tasks_b: Vec<_> = (0..2).map(|_| make_task(0)).collect();

        drr.add_ready_tasks("tenant-a", 1, tasks_a);
        drr.add_ready_tasks("tenant-b", 1, tasks_b);

        // Select more than tenant-b has
        let selections = drr.select_batch(12);
        assert_eq!(selections.len(), 12);

        // All 12 tasks should be selected (work conservation)
        let mut counts: HashMap<String, usize> = HashMap::new();
        for sel in &selections {
            *counts.entry(sel.tenant_id.clone()).or_default() += 1;
        }

        assert_eq!(counts.get("tenant-a"), Some(&10));
        assert_eq!(counts.get("tenant-b"), Some(&2));
    }

    #[test]
    fn fair_task_selector_interface() {
        let mut selector = FairTaskSelector::new();

        let tasks_a: Vec<_> = (0..3).map(|_| make_task(0)).collect();
        let tasks_b: Vec<_> = (0..3).map(|_| make_task(0)).collect();

        selector.add_tenant_tasks("tenant-a", 1, tasks_a);
        selector.add_tenant_tasks("tenant-b", 1, tasks_b);

        let selections = selector.select_fair(4);
        assert_eq!(selections.len(), 4);

        // Should be fairly distributed
        let mut counts: HashMap<String, usize> = HashMap::new();
        for sel in &selections {
            *counts.entry(sel.tenant_id.clone()).or_default() += 1;
        }

        assert_eq!(counts.get("tenant-a"), Some(&2));
        assert_eq!(counts.get("tenant-b"), Some(&2));
    }

    #[test]
    fn drr_deficit_tracking() {
        let mut drr = DrrScheduler::new();

        let tasks_a: Vec<_> = (0..5).map(|_| make_task(0)).collect();
        drr.add_ready_tasks("tenant-a", 3, tasks_a);

        // Before any selection
        assert_eq!(drr.get_deficit("tenant-a"), 0);

        // After first selection (round starts, adds quantum 3, then subtracts 1)
        let _ = drr.select_next();
        assert_eq!(drr.get_deficit("tenant-a"), 2);

        // After second selection
        let _ = drr.select_next();
        assert_eq!(drr.get_deficit("tenant-a"), 1);
    }
}
