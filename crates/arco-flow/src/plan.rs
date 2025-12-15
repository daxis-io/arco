//! Deterministic execution planning.
//!
//! Plans are generated from asset definitions and represent exactly
//! what will execute. Plans are:
//!
//! - **Deterministic**: Same inputs always produce the same plan
//! - **Serializable**: Can be stored and compared for debugging
//! - **Explainable**: Every task inclusion can be traced to a reason

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use arco_core::{AssetId, TaskId};

use crate::dag::Dag;
use crate::error::{Error, Result};

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
    #[must_use]
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }
}

impl std::fmt::Display for AssetKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

/// Resource requirements for task execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequirements {
    /// Memory limit in bytes.
    #[serde(default)]
    pub memory_bytes: i64,
    /// CPU cores (fractional allowed).
    #[serde(default)]
    pub cpu_cores: f64,
    /// Maximum execution time.
    #[serde(default, with = "humantime_serde")]
    pub timeout: Duration,
}

impl Default for ResourceRequirements {
    fn default() -> Self {
        Self {
            memory_bytes: 512 * 1024 * 1024, // 512 MB
            cpu_cores: 1.0,
            timeout: Duration::from_secs(3600), // 1 hour
        }
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
    /// Partition key (if partitioned asset).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<BTreeMap<String, String>>,
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
    pub fn build(mut self) -> Result<Plan> {
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
            let to_idx = id_to_idx.get(&task.task_id).copied().ok_or_else(|| {
                Error::DagNodeNotFound {
                    node: task.task_id.to_string(),
                }
            })?;
            for dep_id in &task.upstream_task_ids {
                let from_idx = id_to_idx.get(dep_id).copied().ok_or_else(|| {
                    Error::DagNodeNotFound {
                        node: dep_id.to_string(),
                    }
                })?;
                dag.add_edge(from_idx, to_idx)?;
            }
        }

        // Topological sort to verify acyclicity and get execution order
        let sorted_ids = dag.toposort()?;

        // Compute stages: stage = max(upstream stages) + 1, roots have stage 0
        let mut stages: HashMap<TaskId, u32> = HashMap::new();
        for id in &sorted_ids {
            let task = self.tasks.iter().find(|t| &t.task_id == id).ok_or_else(|| {
                Error::TaskNotFound { task_id: *id }
            })?;
            let stage = if task.upstream_task_ids.is_empty() {
                0
            } else {
                task.upstream_task_ids
                    .iter()
                    .filter_map(|dep| stages.get(dep))
                    .max()
                    .map(|max_stage| max_stage + 1)
                    .unwrap_or(0)
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

/// Computes SHA-256 fingerprint of the plan spec.
fn compute_fingerprint(tasks: &[TaskSpec]) -> Result<String> {
    let json = serde_json::to_string(tasks).map_err(|e| Error::Serialization {
        message: format!("failed to serialize tasks: {e}"),
    })?;

    let hash = Sha256::digest(json.as_bytes());
    Ok(format!("sha256:{}", hex::encode(hash)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_builder_creates_valid_plan() {
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
            .build()
            .unwrap();

        assert_eq!(plan.tenant_id, "tenant");
        assert_eq!(plan.workspace_id, "workspace");
        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(plan.tasks[0].stage, 0);
    }

    #[test]
    fn plan_fingerprint_is_deterministic() {
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
            .build()
            .unwrap();

        let plan2 = PlanBuilder::new("tenant", "workspace")
            .add_task(task)
            .build()
            .unwrap();

        assert_eq!(plan1.fingerprint, plan2.fingerprint);
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
    fn plan_sorts_tasks_topologically() {
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
            .build()
            .unwrap();

        // Verify topological order
        let pos_a = plan.tasks.iter().position(|t| t.task_id == task_a).unwrap();
        let pos_b = plan.tasks.iter().position(|t| t.task_id == task_b).unwrap();
        let pos_c = plan.tasks.iter().position(|t| t.task_id == task_c).unwrap();

        assert!(pos_a < pos_b, "task_a should come before task_b");
        assert!(pos_b < pos_c, "task_b should come before task_c");

        // Verify stages were computed
        assert_eq!(plan.task(task_a).unwrap().stage, 0);
        assert_eq!(plan.task(task_b).unwrap().stage, 1);
        assert_eq!(plan.task(task_c).unwrap().stage, 2);
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
    fn plan_builder_computes_stages() {
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
            .build()
            .unwrap();

        // Find tasks by ID and verify stages
        let stage_a = plan.task(task_a).map(|t| t.stage);
        let stage_b = plan.task(task_b).map(|t| t.stage);
        let stage_c = plan.task(task_c).map(|t| t.stage);

        assert_eq!(stage_a, Some(0)); // Root
        assert_eq!(stage_b, Some(1)); // Depends on stage 0
        assert_eq!(stage_c, Some(2)); // Depends on stage 1
    }
}
