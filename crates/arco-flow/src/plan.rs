//! Deterministic execution planning.
//!
//! Plans are generated from asset definitions and represent exactly
//! what will execute. Plans are:
//!
//! - **Deterministic**: Same inputs always produce the same plan
//! - **Serializable**: Can be stored and compared for debugging
//! - **Explainable**: Every task inclusion can be traced to a reason

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use arco_core::partition::PartitionKey;
use arco_core::{AssetId, TaskId};

use crate::dag::Dag;
use crate::error::{Error, Result};

/// Production guardrail: hard cap on tasks per plan.
const MAX_TASKS_PER_PLAN: usize = 10_000;

/// Asset identifier (namespace + name).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetKey {
    /// Namespace (e.g., "raw", "staging", "mart").
    pub namespace: String,
    /// Asset name within the namespace.
    pub name: String,
}

impl AssetKey {
    /// Creates a new asset key.
    #[must_use]
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    /// Returns the fully qualified name (namespace.name).
    ///
    /// Uses `.` separator for backward compatibility with existing code.
    #[must_use]
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }

    /// Returns the canonical string representation (namespace/name).
    ///
    /// Uses `/` separator per ADR-011 for deterministic identity.
    /// This is the preferred format for use in `TaskKey` and fingerprinting.
    #[must_use]
    pub fn canonical_string(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}

impl std::fmt::Display for AssetKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

/// Resource requirements for task execution.
///
/// All fields use integer types for cross-language determinism (ADR-010).
/// Float serialization differs between Rust and Python, so we use:
/// - `memory_bytes`: bytes (not fractional GB)
/// - `cpu_millicores`: 1/1000th of a CPU core (1000 = 1 CPU)
/// - `timeout_ms`: milliseconds (not fractional seconds)
///
/// **Serde defaults:** Each field has an explicit default function to ensure
/// partial deserialization uses sensible values (not 0).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequirements {
    /// Memory limit in bytes (unsigned - negative memory is invalid).
    #[serde(default = "default_memory_bytes")]
    pub memory_bytes: u64,

    /// CPU in millicores (1000 = 1 CPU core, 500 = 0.5 CPU).
    /// This avoids float serialization issues across languages.
    #[serde(default = "default_cpu_millicores")]
    pub cpu_millicores: u64,

    /// Maximum execution time in milliseconds.
    /// This avoids Duration serialization complexity.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

/// Default memory: 512 MB.
const fn default_memory_bytes() -> u64 {
    512 * 1024 * 1024
}

/// Default CPU: 1 core (1000 millicores).
const fn default_cpu_millicores() -> u64 {
    1000
}

/// Default timeout: 1 hour (3,600,000 ms).
const fn default_timeout_ms() -> u64 {
    3_600_000
}

impl Default for ResourceRequirements {
    fn default() -> Self {
        Self {
            memory_bytes: default_memory_bytes(),
            cpu_millicores: default_cpu_millicores(),
            timeout_ms: default_timeout_ms(),
        }
    }
}

impl ResourceRequirements {
    /// Creates a new `ResourceRequirements` with the given values.
    #[must_use]
    pub fn new(memory_bytes: u64, cpu_millicores: u64, timeout_ms: u64) -> Self {
        Self {
            memory_bytes,
            cpu_millicores,
            timeout_ms,
        }
    }

    /// Returns the timeout as a `Duration`.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}

/// Specification for a single task within a plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskSpec {
    /// Unique task identifier within the plan.
    pub task_id: TaskId,
    /// Asset this task materializes.
    pub asset_id: AssetId,
    /// Asset key (namespace.name).
    pub asset_key: AssetKey,
    /// Typed partition key (if partitioned asset).
    /// Uses `arco_core::partition::PartitionKey` for cross-language determinism.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<PartitionKey>,
    /// Upstream dependencies (task IDs that must complete first).
    #[serde(default)]
    pub upstream_task_ids: Vec<TaskId>,
    /// Topological stage for wave-based scheduling.
    /// 0 = roots (no dependencies), 1 = depends on stage 0, etc.
    /// Computed during plan building.
    #[serde(default)]
    pub stage: u32,
    /// Execution priority (lower = higher priority).
    #[serde(default)]
    pub priority: i32,
    /// Resource requirements.
    #[serde(default)]
    pub resources: ResourceRequirements,
}

/// Dependency edge in the plan graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DependencyEdge {
    /// Source task (upstream).
    pub source_task_id: TaskId,
    /// Target task (downstream).
    pub target_task_id: TaskId,
}

/// A deterministic execution plan.
///
/// Generated from asset definitions, a plan specifies exactly
/// which tasks will execute and in what order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    /// Unique plan identifier.
    pub plan_id: String,
    /// Tenant scope.
    pub tenant_id: String,
    /// Workspace scope.
    pub workspace_id: String,
    /// Plan creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Plan version for idempotency.
    pub version: u32,
    /// Hash of all inputs that produced this plan (for cache invalidation).
    pub input_hash: String,
    /// SHA-256 fingerprint of the plan spec (for quick equality).
    pub fingerprint: String,
    /// Ordered list of tasks (topologically sorted).
    pub tasks: Vec<TaskSpec>,
    /// Asset dependency graph (adjacency list).
    pub dependencies: Vec<DependencyEdge>,
}

impl Plan {
    /// Returns the number of tasks in the plan.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Returns true if the plan has no tasks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Returns the task spec for a given task ID.
    #[must_use]
    pub fn task(&self, task_id: TaskId) -> Option<&TaskSpec> {
        self.tasks.iter().find(|t| t.task_id == task_id)
    }

    /// Returns the task spec for a given task ID (alias for `task`).
    #[must_use]
    pub fn get_task(&self, task_id: &TaskId) -> Option<&TaskSpec> {
        self.tasks.iter().find(|t| &t.task_id == task_id)
    }

    /// Returns tasks with no dependencies (roots).
    #[must_use]
    pub fn root_tasks(&self) -> Vec<&TaskSpec> {
        self.tasks
            .iter()
            .filter(|t| t.upstream_task_ids.is_empty())
            .collect()
    }

    /// Returns tasks that depend on the given task ID.
    #[must_use]
    pub fn downstream_tasks(&self, task_id: &TaskId) -> Vec<&TaskSpec> {
        self.tasks
            .iter()
            .filter(|t| t.upstream_task_ids.contains(task_id))
            .collect()
    }
}

/// Builder for creating execution plans.
pub struct PlanBuilder {
    tenant_id: String,
    workspace_id: String,
    tasks: Vec<TaskSpec>,
    input_hash: Option<String>,
}

impl PlanBuilder {
    /// Creates a new plan builder.
    #[must_use]
    pub fn new(tenant_id: impl Into<String>, workspace_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            workspace_id: workspace_id.into(),
            tasks: Vec::new(),
            input_hash: None,
        }
    }

    /// Adds a task to the plan.
    #[must_use]
    pub fn add_task(mut self, task: TaskSpec) -> Self {
        self.tasks.push(task);
        self
    }

    /// Sets the input hash for cache invalidation.
    #[must_use]
    pub fn with_input_hash(mut self, hash: impl Into<String>) -> Self {
        self.input_hash = Some(hash.into());
        self
    }

    /// Builds the plan, validating dependencies and computing fingerprint.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Dependencies reference non-existent tasks
    /// - The dependency graph contains cycles
    #[tracing::instrument(
        skip(self),
        fields(
            tenant_id = %self.tenant_id,
            workspace_id = %self.workspace_id,
            task_count = self.tasks.len()
        )
    )]
    pub fn build(mut self) -> Result<Plan> {
        if self.tasks.len() > MAX_TASKS_PER_PLAN {
            return Err(Error::PlanTooLarge {
                task_count: self.tasks.len(),
                max_tasks: MAX_TASKS_PER_PLAN,
            });
        }

        // Collect all task IDs
        let task_ids: HashSet<_> = self.tasks.iter().map(|t| t.task_id).collect();

        // Validate all dependencies exist
        for task in &self.tasks {
            for dep_id in &task.upstream_task_ids {
                if !task_ids.contains(dep_id) {
                    return Err(Error::DependencyNotFound {
                        asset_key: format!("task {dep_id}"),
                    });
                }
            }
        }

        // Build DAG and check for cycles
        // Use NodeIndex-based API for type safety
        let mut dag: Dag<TaskId> = Dag::new();
        let mut id_to_idx: HashMap<TaskId, NodeIndex> = HashMap::new();

        for task in &self.tasks {
            let idx = dag.add_node(task.task_id);
            id_to_idx.insert(task.task_id, idx);
        }
        for task in &self.tasks {
            let to_idx =
                id_to_idx
                    .get(&task.task_id)
                    .copied()
                    .ok_or_else(|| Error::DagNodeNotFound {
                        node: task.task_id.to_string(),
                    })?;
            for dep_id in &task.upstream_task_ids {
                let from_idx =
                    id_to_idx
                        .get(dep_id)
                        .copied()
                        .ok_or_else(|| Error::DagNodeNotFound {
                            node: dep_id.to_string(),
                        })?;
                dag.add_edge(from_idx, to_idx)?;
            }
        }

        // Topological sort to verify acyclicity and get execution order
        let sorted_ids = dag.toposort()?;

        // Compute stages: stage = max(upstream stages) + 1, roots have stage 0
        let mut stages: HashMap<TaskId, u32> = HashMap::new();
        for id in &sorted_ids {
            let task = self
                .tasks
                .iter()
                .find(|t| &t.task_id == id)
                .ok_or_else(|| Error::TaskNotFound { task_id: *id })?;
            let stage = if task.upstream_task_ids.is_empty() {
                0
            } else {
                task.upstream_task_ids
                    .iter()
                    .filter_map(|dep| stages.get(dep))
                    .max()
                    .map_or(0, |max_stage| max_stage + 1)
            };
            stages.insert(*id, stage);
        }

        // Update tasks with computed stages
        for task in &mut self.tasks {
            task.stage = stages.get(&task.task_id).copied().unwrap_or(0);
        }

        // Reorder tasks by topological order
        let mut sorted_tasks = Vec::with_capacity(self.tasks.len());
        for id in sorted_ids {
            if let Some(task) = self.tasks.iter().find(|t| t.task_id == id) {
                sorted_tasks.push(task.clone());
            }
        }

        // Build dependency edges
        let dependencies: Vec<DependencyEdge> = sorted_tasks
            .iter()
            .flat_map(|task| {
                task.upstream_task_ids.iter().map(|dep_id| DependencyEdge {
                    source_task_id: *dep_id,
                    target_task_id: task.task_id,
                })
            })
            .collect();

        // Compute fingerprint from canonical JSON of tasks
        let fingerprint = compute_fingerprint(&sorted_tasks)?;

        // Generate plan ID
        let plan_id = ulid::Ulid::new().to_string();

        Ok(Plan {
            plan_id,
            tenant_id: self.tenant_id,
            workspace_id: self.workspace_id,
            created_at: Utc::now(),
            version: 1,
            input_hash: self.input_hash.unwrap_or_default(),
            fingerprint,
            tasks: sorted_tasks,
            dependencies,
        })
    }
}

/// Structural representation of a task for fingerprinting.
///
/// This excludes nondeterministic IDs (`task_id`, `asset_id`) and uses
/// `asset_key` for dependency references, ensuring the fingerprint is
/// stable across plan regenerations with the same structure.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StructuralTaskSpec {
    asset_key: AssetKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    partition_key: Option<PartitionKey>,
    upstream_asset_keys: Vec<String>,
    stage: u32,
    priority: i32,
    resources: ResourceRequirements,
}

/// Computes SHA-256 fingerprint of the plan spec using structural properties only.
///
/// The fingerprint excludes nondeterministic IDs (`task_id`, `asset_id`) and uses
/// `asset_key` for dependency references. This ensures the same logical plan
/// produces the same fingerprint regardless of generated IDs.
fn compute_fingerprint(tasks: &[TaskSpec]) -> Result<String> {
    // Build task_id -> asset_key mapping for dependency resolution
    let id_to_key: HashMap<TaskId, String> = tasks
        .iter()
        .map(|t| (t.task_id, t.asset_key.qualified_name()))
        .collect();

    // Convert to structural representation
    let structural: Vec<StructuralTaskSpec> = tasks
        .iter()
        .map(|t| {
            let mut upstream_keys: Vec<String> = t
                .upstream_task_ids
                .iter()
                .filter_map(|id| id_to_key.get(id).cloned())
                .collect();
            // Sort for determinism
            upstream_keys.sort();

            StructuralTaskSpec {
                asset_key: t.asset_key.clone(),
                partition_key: t.partition_key.clone(),
                upstream_asset_keys: upstream_keys,
                stage: t.stage,
                priority: t.priority,
                resources: t.resources.clone(),
            }
        })
        .collect();

    let json = serde_json::to_string(&structural).map_err(|e| Error::Serialization {
        message: format!("failed to serialize structural tasks: {e}"),
    })?;

    let hash = Sha256::digest(json.as_bytes());
    Ok(format!("sha256:{}", hex::encode(hash)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_key_canonical_string_uses_slash_separator() {
        let key = AssetKey::new("raw", "events");
        assert_eq!(key.canonical_string(), "raw/events");
        assert_eq!(key.qualified_name(), "raw.events");
    }

    #[test]
    fn plan_builder_creates_valid_plan() -> Result<()> {
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: TaskId::generate(),
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()?;

        assert_eq!(plan.tenant_id, "tenant");
        assert_eq!(plan.workspace_id, "workspace");
        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(plan.tasks[0].stage, 0);

        Ok(())
    }

    #[test]
    fn plan_fingerprint_is_deterministic() -> Result<()> {
        let task_id = TaskId::generate();
        let asset_id = AssetId::generate();

        let task = TaskSpec {
            task_id,
            asset_id,
            asset_key: AssetKey::new("raw", "events"),
            partition_key: None,
            upstream_task_ids: vec![],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        };

        let plan1 = PlanBuilder::new("tenant", "workspace")
            .add_task(task.clone())
            .build()?;

        let plan2 = PlanBuilder::new("tenant", "workspace")
            .add_task(task)
            .build()?;

        assert_eq!(plan1.fingerprint, plan2.fingerprint);

        Ok(())
    }

    #[test]
    fn plan_validates_dependencies_exist() {
        let result = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: TaskId::generate(),
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![TaskId::generate()],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn plan_sorts_tasks_topologically() -> Result<()> {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();
        let task_c = TaskId::generate();

        // Add tasks in wrong order: c depends on b, b depends on a
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_c,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("mart", "report"),
                partition_key: None,
                upstream_task_ids: vec![task_b],
                stage: 0, // Will be computed to 2
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0, // Will be computed to 0
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "cleaned"),
                partition_key: None,
                upstream_task_ids: vec![task_a],
                stage: 0, // Will be computed to 1
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()?;

        // Verify topological order
        let pos_a = plan
            .tasks
            .iter()
            .position(|t| t.task_id == task_a)
            .ok_or(Error::TaskNotFound { task_id: task_a })?;
        let pos_b = plan
            .tasks
            .iter()
            .position(|t| t.task_id == task_b)
            .ok_or(Error::TaskNotFound { task_id: task_b })?;
        let pos_c = plan
            .tasks
            .iter()
            .position(|t| t.task_id == task_c)
            .ok_or(Error::TaskNotFound { task_id: task_c })?;

        assert!(pos_a < pos_b, "task_a should come before task_b");
        assert!(pos_b < pos_c, "task_b should come before task_c");

        // Verify stages were computed
        assert_eq!(
            plan.task(task_a)
                .ok_or(Error::TaskNotFound { task_id: task_a })?
                .stage,
            0
        );
        assert_eq!(
            plan.task(task_b)
                .ok_or(Error::TaskNotFound { task_id: task_b })?
                .stage,
            1
        );
        assert_eq!(
            plan.task(task_c)
                .ok_or(Error::TaskNotFound { task_id: task_c })?
                .stage,
            2
        );

        Ok(())
    }

    #[test]
    fn plan_detects_cycles() {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();

        let result = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "a"),
                partition_key: None,
                upstream_task_ids: vec![task_b],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "b"),
                partition_key: None,
                upstream_task_ids: vec![task_a],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build();

        assert!(matches!(result, Err(Error::CycleDetected { .. })));
    }

    #[test]
    fn plan_builder_computes_stages() -> Result<()> {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();
        let task_c = TaskId::generate();

        // DAG: a -> b -> c
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "a"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "b"),
                partition_key: None,
                upstream_task_ids: vec![task_a],
                stage: 0, // Will be computed to 1
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_c,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("mart", "c"),
                partition_key: None,
                upstream_task_ids: vec![task_b],
                stage: 0, // Will be computed to 2
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()?;

        // Find tasks by ID and verify stages
        let stage_a = plan.task(task_a).map(|t| t.stage);
        let stage_b = plan.task(task_b).map(|t| t.stage);
        let stage_c = plan.task(task_c).map(|t| t.stage);

        assert_eq!(stage_a, Some(0)); // Root
        assert_eq!(stage_b, Some(1)); // Depends on stage 0
        assert_eq!(stage_c, Some(2)); // Depends on stage 1

        Ok(())
    }

    #[test]
    fn plan_builder_enforces_task_cap() {
        let mut builder = PlanBuilder::new("tenant", "workspace");

        for _ in 0..(MAX_TASKS_PER_PLAN + 1) {
            builder = builder.add_task(TaskSpec {
                task_id: TaskId::generate(),
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            });
        }

        let result = builder.build();
        assert!(matches!(
            result,
            Err(Error::PlanTooLarge {
                task_count,
                max_tasks
            }) if task_count == MAX_TASKS_PER_PLAN + 1 && max_tasks == MAX_TASKS_PER_PLAN
        ));
    }

    #[test]
    fn plan_fingerprint_is_structural() -> Result<()> {
        // Plan 1: DAG with a -> b -> c
        let task_a1 = TaskId::generate();
        let task_b1 = TaskId::generate();
        let task_c1 = TaskId::generate();

        let plan1 = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_a1,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b1,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "cleaned"),
                partition_key: None,
                upstream_task_ids: vec![task_a1],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_c1,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("mart", "report"),
                partition_key: None,
                upstream_task_ids: vec![task_b1],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()?;

        // Plan 2: Same structure, completely different IDs
        let task_a2 = TaskId::generate();
        let task_b2 = TaskId::generate();
        let task_c2 = TaskId::generate();

        let plan2 = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_a2,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b2,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "cleaned"),
                partition_key: None,
                upstream_task_ids: vec![task_a2],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_c2,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("mart", "report"),
                partition_key: None,
                upstream_task_ids: vec![task_b2],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()?;

        // Same structure = same fingerprint, even with different IDs
        assert_eq!(plan1.fingerprint, plan2.fingerprint);
        // But different plan IDs (generated)
        assert_ne!(plan1.plan_id, plan2.plan_id);

        Ok(())
    }
}
