# Part 3: Orchestration MVP Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement the orchestration domain (arco-flow) with DAG representation, task scheduling, materialization runner, and execution tracking following best practices and professional software engineering standards.

**Architecture:** The orchestration layer follows an event-sourced design with a deterministic planner that generates execution plans from asset definitions, a dependency-aware scheduler that respects topological order, and Tier 2 event persistence for execution tracking. All state transitions flow through a well-defined state machine.

**Tech Stack:** Rust 2024 edition, tokio async runtime, serde/serde_json for serialization, sha2 for plan fingerprinting, petgraph for DAG algorithms, chrono for timestamps, thiserror for errors.

---

## Overview

This plan implements Part 3 of the Arco platform - the Orchestration MVP. Building on Parts 1-2 (core infrastructure + catalog), this adds:

1. **DAG Representation** - Plan/TaskSpec with dependency graph, topological sort, fingerprinting
2. **Task Scheduling** - Dependency-aware task scheduling with state machine
3. **Materialization Runner** - Task execution lifecycle with retry support
4. **Execution Tracking** - Run/TaskExecution persistence via Tier 2 events

**Dependencies:** arco-core (IDs, storage, errors), arco-catalog (Asset definitions)

**Deliverables:**
- Extended `arco-core` with TaskId, MaterializationId
- Complete `arco-flow` crate with production-ready orchestration
- Integration tests validating cross-crate contracts
- Benchmarks for plan generation and scheduling

---

## Task 1: Add TaskId and MaterializationId to arco-core

**Files:**
- Modify: `crates/arco-core/src/id.rs`
- Modify: `crates/arco-core/src/lib.rs`

**Step 1: Write the failing test for TaskId**

Add to `crates/arco-core/src/id.rs`:

```rust
#[cfg(test)]
mod tests {
    // ... existing tests ...

    #[test]
    fn task_id_roundtrip() {
        let id = TaskId::generate();
        let s = id.to_string();
        let parsed: TaskId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn materialization_id_roundtrip() {
        let id = MaterializationId::generate();
        let s = id.to_string();
        let parsed: MaterializationId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn task_ids_are_unique() {
        let id1 = TaskId::generate();
        let id2 = TaskId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn materialization_ids_are_unique() {
        let id1 = MaterializationId::generate();
        let id2 = MaterializationId::generate();
        assert_ne!(id1, id2);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-core task_id_roundtrip`

Expected: FAIL with "cannot find type `TaskId`"

**Step 3: Write minimal implementation for TaskId**

Add to `crates/arco-core/src/id.rs` (after RunId):

```rust
/// A unique identifier for a task within a run.
///
/// Tasks are individual units of work that materialize a single asset
/// (or asset partition) within an orchestration run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(Ulid);

impl TaskId {
    /// Generates a new unique task ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(Ulid::new())
    }

    /// Creates a task ID from a raw ULID.
    #[must_use]
    pub const fn from_ulid(ulid: Ulid) -> Self {
        Self(ulid)
    }

    /// Returns the underlying ULID.
    #[must_use]
    pub const fn as_ulid(&self) -> Ulid {
        self.0
    }

    /// Returns the creation timestamp encoded in the ID.
    #[must_use]
    pub fn created_at(&self) -> chrono::DateTime<chrono::Utc> {
        let ms = self.0.timestamp_ms();
        let ms_i64 = i64::try_from(ms).unwrap_or(i64::MAX);
        chrono::DateTime::from_timestamp_millis(ms_i64).unwrap_or_else(chrono::Utc::now)
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for TaskId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ulid::from_string(s)
            .map(Self)
            .map_err(|e| Error::InvalidId {
                message: format!("invalid task ID '{s}': {e}"),
            })
    }
}

/// A unique identifier for a materialization.
///
/// Materializations represent a single successful execution of an asset
/// or asset partition, capturing the output files and metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MaterializationId(Ulid);

impl MaterializationId {
    /// Generates a new unique materialization ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(Ulid::new())
    }

    /// Creates a materialization ID from a raw ULID.
    #[must_use]
    pub const fn from_ulid(ulid: Ulid) -> Self {
        Self(ulid)
    }

    /// Returns the underlying ULID.
    #[must_use]
    pub const fn as_ulid(&self) -> Ulid {
        self.0
    }

    /// Returns the creation timestamp encoded in the ID.
    #[must_use]
    pub fn created_at(&self) -> chrono::DateTime<chrono::Utc> {
        let ms = self.0.timestamp_ms();
        let ms_i64 = i64::try_from(ms).unwrap_or(i64::MAX);
        chrono::DateTime::from_timestamp_millis(ms_i64).unwrap_or_else(chrono::Utc::now)
    }
}

impl fmt::Display for MaterializationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for MaterializationId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ulid::from_string(s)
            .map(Self)
            .map_err(|e| Error::InvalidId {
                message: format!("invalid materialization ID '{s}': {e}"),
            })
    }
}
```

**Step 4: Export new IDs in lib.rs**

Modify `crates/arco-core/src/lib.rs` to add exports:

```rust
pub use id::{AssetId, MaterializationId, RunId, TaskId};
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p arco-core`

Expected: All tests PASS

**Step 6: Commit**

```bash
git add crates/arco-core/src/id.rs crates/arco-core/src/lib.rs
git commit -m "feat(core): add TaskId and MaterializationId types

Adds strongly-typed ULID-based identifiers for tasks and materializations,
following the same patterns as AssetId and RunId."
```

---

## Task 2: Add Flow Error Types to arco-flow

**Files:**
- Create: `crates/arco-flow/src/error.rs`
- Modify: `crates/arco-flow/src/lib.rs`

**Step 1: Write the failing test for flow errors**

Create test in `crates/arco-flow/src/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_error_display() {
        let err = Error::CycleDetected {
            cycle: vec!["a".into(), "b".into(), "a".into()],
        };
        assert!(err.to_string().contains("cycle detected"));
    }

    #[test]
    fn task_error_display() {
        let err = Error::TaskNotFound {
            task_id: "01HYXYZ".into(),
        };
        assert!(err.to_string().contains("task not found"));
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-flow plan_error_display`

Expected: FAIL with "cannot find type `Error`"

**Step 3: Write minimal implementation**

Create `crates/arco-flow/src/error.rs`:

```rust
//! Error types for the orchestration domain.

use std::fmt;

/// The result type used throughout arco-flow.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in orchestration operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A cycle was detected in the dependency graph.
    #[error("cycle detected in dependency graph: {cycle:?}")]
    CycleDetected {
        /// The cycle path (asset keys or task IDs).
        cycle: Vec<String>,
    },

    /// A task was not found in the plan or run.
    #[error("task not found: {task_id}")]
    TaskNotFound {
        /// The task ID that was not found.
        task_id: String,
    },

    /// A run was not found.
    #[error("run not found: {run_id}")]
    RunNotFound {
        /// The run ID that was not found.
        run_id: String,
    },

    /// An invalid state transition was attempted.
    #[error("invalid state transition: {from} -> {to} ({reason})")]
    InvalidStateTransition {
        /// The current state.
        from: String,
        /// The attempted target state.
        to: String,
        /// The reason the transition is invalid.
        reason: String,
    },

    /// A dependency was not found.
    #[error("dependency not found: {asset_key}")]
    DependencyNotFound {
        /// The asset key of the missing dependency.
        asset_key: String,
    },

    /// A DAG node was not found (internal graph operation error).
    #[error("DAG node not found: {node}")]
    DagNodeNotFound {
        /// The node identifier (index or value).
        node: String,
    },

    /// Plan generation failed.
    #[error("plan generation failed: {message}")]
    PlanGenerationFailed {
        /// Description of the failure.
        message: String,
    },

    /// Task execution failed.
    #[error("task execution failed: {message}")]
    TaskExecutionFailed {
        /// Description of the failure.
        message: String,
    },

    /// A storage operation failed.
    #[error("storage error: {message}")]
    Storage {
        /// Description of the storage failure.
        message: String,
        /// The underlying cause, if any.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// A serialization error occurred.
    #[error("serialization error: {message}")]
    Serialization {
        /// Description of the serialization failure.
        message: String,
    },

    /// An error from arco-core.
    #[error("core error: {0}")]
    Core(#[from] arco_core::error::Error),
}

impl Error {
    /// Creates a new storage error.
    #[must_use]
    pub fn storage(message: impl Into<String>) -> Self {
        Self::Storage {
            message: message.into(),
            source: None,
        }
    }

    /// Creates a new storage error with a source.
    #[must_use]
    pub fn storage_with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Storage {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_error_display() {
        let err = Error::CycleDetected {
            cycle: vec!["a".into(), "b".into(), "a".into()],
        };
        assert!(err.to_string().contains("cycle detected"));
    }

    #[test]
    fn task_error_display() {
        let err = Error::TaskNotFound {
            task_id: "01HYXYZ".into(),
        };
        assert!(err.to_string().contains("task not found"));
    }
}
```

**Step 4: Update lib.rs to export error module**

Modify `crates/arco-flow/src/lib.rs`:

```rust
pub mod error;
pub mod plan;
pub mod run;
pub mod scheduler;

/// Prelude module for convenient imports.
pub mod prelude {
    pub use crate::error::{Error, Result};
    pub use crate::plan::Plan;
    pub use crate::run::Run;
    pub use crate::scheduler::Scheduler;
}
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p arco-flow`

Expected: All tests PASS

**Step 6: Commit**

```bash
git add crates/arco-flow/src/error.rs crates/arco-flow/src/lib.rs
git commit -m "feat(flow): add orchestration error types

Defines structured error types for the flow domain including cycle
detection, state transition errors, and task/run not found errors."
```

---

## Task 3: Implement DAG and Topological Sort

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Create: `crates/arco-flow/src/dag.rs`
- Modify: `crates/arco-flow/src/lib.rs`
- Modify: `crates/arco-flow/Cargo.toml`

**Step 1: Add petgraph dependency (workspace pattern)**

Add `petgraph` to the workspace root `Cargo.toml` (if not already present):

```toml
[workspace.dependencies]
# ... existing deps ...
petgraph = "0.7"
```

Then modify `crates/arco-flow/Cargo.toml` to use the workspace dependency:

```toml
[dependencies]
# ... existing deps ...
petgraph = { workspace = true }
```

**Step 2: Write the failing test for DAG**

Create test in `crates/arco-flow/src/dag.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_dag_has_no_nodes() {
        let dag: Dag<String> = Dag::new();
        assert_eq!(dag.node_count(), 0);
        assert!(dag.toposort().unwrap().is_empty());
    }

    #[test]
    fn single_node_dag() {
        let mut dag: Dag<String> = Dag::new();
        dag.add_node("a".into());
        let sorted = dag.toposort().unwrap();
        assert_eq!(sorted, vec!["a".to_string()]);
    }

    #[test]
    fn linear_dag_sorts_correctly() {
        let mut dag: Dag<String> = Dag::new();
        let a = dag.add_node("a".into());
        let b = dag.add_node("b".into());
        let c = dag.add_node("c".into());
        dag.add_edge(a, b).unwrap(); // a -> b
        dag.add_edge(b, c).unwrap(); // b -> c

        let sorted = dag.toposort().unwrap();
        // a must come before b, b must come before c
        let pos_a = sorted.iter().position(|x| x == "a").unwrap();
        let pos_b = sorted.iter().position(|x| x == "b").unwrap();
        let pos_c = sorted.iter().position(|x| x == "c").unwrap();
        assert!(pos_a < pos_b);
        assert!(pos_b < pos_c);
    }

    #[test]
    fn dag_detects_cycle() {
        let mut dag: Dag<String> = Dag::new();
        let a = dag.add_node("a".into());
        let b = dag.add_node("b".into());
        dag.add_edge(a, b).unwrap();
        dag.add_edge(b, a).unwrap(); // Creates cycle

        let result = dag.toposort();
        assert!(matches!(result, Err(crate::error::Error::CycleDetected { .. })));
    }

    #[test]
    fn dag_returns_upstream_dependencies_in_insertion_order() {
        let mut dag: Dag<String> = Dag::new();
        let a = dag.add_node("a".into());
        let b = dag.add_node("b".into());
        let c = dag.add_node("c".into());
        dag.add_edge(a, c).unwrap(); // a -> c
        dag.add_edge(b, c).unwrap(); // b -> c

        let upstream = dag.upstream(c).unwrap();
        // Exact ordering: a was inserted before b
        assert_eq!(upstream, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn dag_returns_downstream_dependents_in_insertion_order() {
        let mut dag: Dag<String> = Dag::new();
        let a = dag.add_node("a".into());
        let b = dag.add_node("b".into());
        let c = dag.add_node("c".into());
        dag.add_edge(a, b).unwrap();
        dag.add_edge(a, c).unwrap();

        let downstream = dag.downstream(a).unwrap();
        // Exact ordering: b was inserted before c
        assert_eq!(downstream, vec!["b".to_string(), "c".to_string()]);
    }

    #[test]
    fn roots_returns_nodes_in_insertion_order() {
        let mut dag: Dag<String> = Dag::new();
        let c = dag.add_node("c".into());
        let a = dag.add_node("a".into());
        let b = dag.add_node("b".into());
        dag.add_edge(a, c).unwrap();
        dag.add_edge(b, c).unwrap();

        let roots = dag.roots();
        // Insertion order: a, b (c has incoming edges so not a root)
        assert_eq!(roots, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn leaves_returns_nodes_in_insertion_order() {
        let mut dag: Dag<String> = Dag::new();
        let a = dag.add_node("a".into());
        let c = dag.add_node("c".into());
        let b = dag.add_node("b".into());
        dag.add_edge(a, b).unwrap();
        dag.add_edge(a, c).unwrap();

        let leaves = dag.leaves();
        // Insertion order: c, b (a has outgoing edges so not a leaf)
        assert_eq!(leaves, vec!["c".to_string(), "b".to_string()]);
    }

    #[test]
    fn toposort_is_deterministic_with_multiple_roots() {
        // When multiple valid orderings exist, toposort should be deterministic
        // by using insertion order as the tie-breaker
        let mut dag: Dag<String> = Dag::new();
        let a = dag.add_node("a".into());
        let b = dag.add_node("b".into());
        let c = dag.add_node("c".into());
        let d = dag.add_node("d".into());
        // a and b are independent roots, c depends on a, d depends on b
        dag.add_edge(a, c).unwrap();
        dag.add_edge(b, d).unwrap();

        // Run toposort multiple times, should always get same result
        let sorted1 = dag.toposort().unwrap();
        let sorted2 = dag.toposort().unwrap();
        let sorted3 = dag.toposort().unwrap();

        assert_eq!(sorted1, sorted2);
        assert_eq!(sorted2, sorted3);
        // Exact expected order based on insertion order tie-breaking
        assert_eq!(sorted1, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn toposort_determinism_independent_of_insertion_permutation() {
        // Build the same DAG structure with different insertion orders
        // The toposort result should respect the insertion order of THAT dag
        // but the overall algorithm must be deterministic given an insertion order

        // DAG structure: a -> c, b -> c (diamond top)
        //                c -> d

        // Build with order: a, b, c, d
        let mut dag1: Dag<String> = Dag::new();
        let a1 = dag1.add_node("a".into());
        let b1 = dag1.add_node("b".into());
        let c1 = dag1.add_node("c".into());
        let d1 = dag1.add_node("d".into());
        dag1.add_edge(a1, c1).unwrap();
        dag1.add_edge(b1, c1).unwrap();
        dag1.add_edge(c1, d1).unwrap();

        // Build with order: b, a, c, d (swapped a and b)
        let mut dag2: Dag<String> = Dag::new();
        let b2 = dag2.add_node("b".into());
        let a2 = dag2.add_node("a".into());
        let c2 = dag2.add_node("c".into());
        let d2 = dag2.add_node("d".into());
        dag2.add_edge(a2, c2).unwrap();
        dag2.add_edge(b2, c2).unwrap();
        dag2.add_edge(c2, d2).unwrap();

        let sorted1 = dag1.toposort().unwrap();
        let sorted2 = dag2.toposort().unwrap();

        // Both should be valid topological orderings
        // dag1: insertion order a,b,c,d -> expect [a, b, c, d]
        // dag2: insertion order b,a,c,d -> expect [b, a, c, d]
        assert_eq!(sorted1, vec!["a", "b", "c", "d"]);
        assert_eq!(sorted2, vec!["b", "a", "c", "d"]);

        // Each dag's toposort should be stable across multiple calls
        assert_eq!(dag1.toposort().unwrap(), sorted1);
        assert_eq!(dag2.toposort().unwrap(), sorted2);
    }
}
```

**Step 3: Run test to verify it fails**

Run: `cargo test -p arco-flow empty_dag_has_no_nodes`

Expected: FAIL with "cannot find type `Dag`"

**Step 4: Write minimal implementation**

Create `crates/arco-flow/src/dag.rs`:

```rust
//! Directed Acyclic Graph (DAG) for dependency management.
//!
//! This module provides a generic DAG implementation used for:
//! - Asset dependency graphs
//! - Task execution ordering
//! - Topological sorting for deterministic plan generation
//!
//! **Note:** This module is `pub(crate)` to preserve freedom to change internals.

use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::hash::Hash;

use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::Direction;

use crate::error::{Error, Result};

/// A directed acyclic graph for dependency management.
///
/// The DAG supports:
/// - Adding nodes and directed edges
/// - Topological sorting (for execution order)
/// - Querying upstream dependencies and downstream dependents
/// - Cycle detection
///
/// **API Note:** Methods accepting node references take `NodeIndex` for
/// type safety and to avoid String/&str coercion issues.
#[derive(Debug, Clone)]
pub(crate) struct Dag<T>
where
    T: Clone + Eq + Hash + Display,
{
    /// The underlying petgraph graph.
    graph: DiGraph<T, ()>,
    /// Map from node value to node index for fast lookup.
    index_map: HashMap<T, NodeIndex>,
    /// Insertion order for deterministic tie-breaking in toposort.
    insertion_order: Vec<NodeIndex>,
}

impl<T> Dag<T>
where
    T: Clone + Eq + Hash + Display,
{
    /// Creates a new empty DAG.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            index_map: HashMap::new(),
            insertion_order: Vec::new(),
        }
    }

    /// Returns the number of nodes in the DAG.
    #[must_use]
    pub(crate) fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Returns the number of edges in the DAG.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    /// Adds a node to the DAG.
    ///
    /// If the node already exists, this is a no-op.
    /// Returns the node index for use with other methods.
    pub(crate) fn add_node(&mut self, value: T) -> NodeIndex {
        if let Some(&idx) = self.index_map.get(&value) {
            return idx;
        }
        let idx = self.graph.add_node(value.clone());
        self.index_map.insert(value, idx);
        self.insertion_order.push(idx);
        idx
    }

    /// Adds a directed edge from `from` to `to`.
    ///
    /// Takes `NodeIndex` values returned from `add_node` for type safety.
    ///
    /// # Errors
    ///
    /// Returns an error if either node index is invalid.
    pub(crate) fn add_edge(&mut self, from: NodeIndex, to: NodeIndex) -> Result<()> {
        // Use node_weight() instead of indexing to avoid clippy::indexing_slicing
        let from_node = self.graph.node_weight(from).ok_or_else(|| Error::DagNodeNotFound {
            node: format!("index {}", from.index()),
        })?;
        let to_node = self.graph.node_weight(to).ok_or_else(|| Error::DagNodeNotFound {
            node: format!("index {}", to.index()),
        })?;

        // Validate nodes exist (the node_weight calls above do this)
        let _ = (from_node, to_node);

        self.graph.add_edge(from, to, ());
        Ok(())
    }

    /// Returns a topologically sorted list of nodes.
    ///
    /// Uses Kahn's algorithm with deterministic tie-breaking:
    /// when multiple nodes have zero in-degree, they are processed
    /// in insertion order for reproducible results.
    ///
    /// # Errors
    ///
    /// Returns an error if the graph contains a cycle.
    pub(crate) fn toposort(&self) -> Result<Vec<T>> {
        let node_count = self.graph.node_count();
        if node_count == 0 {
            return Ok(Vec::new());
        }

        // Compute in-degrees
        let mut in_degree: HashMap<NodeIndex, usize> = HashMap::with_capacity(node_count);
        for idx in self.graph.node_indices() {
            in_degree.insert(idx, 0);
        }
        for edge in self.graph.edge_references() {
            *in_degree.entry(edge.target()).or_insert(0) += 1;
        }

        // Initialize queue with nodes having zero in-degree, in insertion order
        let mut queue: VecDeque<NodeIndex> = self
            .insertion_order
            .iter()
            .filter(|&&idx| in_degree.get(&idx).copied().unwrap_or(0) == 0)
            .copied()
            .collect();

        let mut result = Vec::with_capacity(node_count);

        while let Some(idx) = queue.pop_front() {
            // Use node_weight() for clippy safety
            let node = self
                .graph
                .node_weight(idx)
                .ok_or_else(|| Error::DagNodeNotFound {
                    node: format!("index {}", idx.index()),
                })?
                .clone();
            result.push(node);

            // Collect neighbors and sort by insertion order for determinism
            let mut neighbors: Vec<NodeIndex> = self
                .graph
                .neighbors_directed(idx, Direction::Outgoing)
                .collect();

            // Sort by insertion order position for deterministic ordering
            neighbors.sort_by_key(|n| {
                self.insertion_order
                    .iter()
                    .position(|&i| i == *n)
                    .unwrap_or(usize::MAX)
            });

            for neighbor in neighbors {
                if let Some(deg) = in_degree.get_mut(&neighbor) {
                    *deg = deg.saturating_sub(1);
                    if *deg == 0 {
                        queue.push_back(neighbor);
                    }
                }
            }
        }

        // Cycle detection: if we didn't visit all nodes, there's a cycle
        if result.len() != node_count {
            // Find a node still with non-zero in-degree to report
            let cycle_node = in_degree
                .iter()
                .find(|(_, &deg)| deg > 0)
                .and_then(|(idx, _)| self.graph.node_weight(*idx))
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unknown".into());

            return Err(Error::CycleDetected {
                cycle: vec![cycle_node],
            });
        }

        Ok(result)
    }

    /// Returns the upstream dependencies of a node (nodes that point to it).
    ///
    /// Results are sorted by insertion order for determinism.
    ///
    /// # Errors
    ///
    /// Returns an error if the node index is invalid.
    pub(crate) fn upstream(&self, node: NodeIndex) -> Result<Vec<T>> {
        // Validate node exists
        self.graph
            .node_weight(node)
            .ok_or_else(|| Error::DagNodeNotFound {
                node: format!("index {}", node.index()),
            })?;

        let mut neighbors: Vec<NodeIndex> = self
            .graph
            .neighbors_directed(node, Direction::Incoming)
            .collect();

        // Sort by insertion order for deterministic results
        neighbors.sort_by_key(|n| {
            self.insertion_order
                .iter()
                .position(|&i| i == *n)
                .unwrap_or(usize::MAX)
        });

        Ok(neighbors
            .into_iter()
            .filter_map(|idx| self.graph.node_weight(idx).cloned())
            .collect())
    }

    /// Returns the downstream dependents of a node (nodes it points to).
    ///
    /// Results are sorted by insertion order for determinism.
    ///
    /// # Errors
    ///
    /// Returns an error if the node index is invalid.
    pub(crate) fn downstream(&self, node: NodeIndex) -> Result<Vec<T>> {
        // Validate node exists
        self.graph
            .node_weight(node)
            .ok_or_else(|| Error::DagNodeNotFound {
                node: format!("index {}", node.index()),
            })?;

        let mut neighbors: Vec<NodeIndex> = self
            .graph
            .neighbors_directed(node, Direction::Outgoing)
            .collect();

        // Sort by insertion order for deterministic results
        neighbors.sort_by_key(|n| {
            self.insertion_order
                .iter()
                .position(|&i| i == *n)
                .unwrap_or(usize::MAX)
        });

        Ok(neighbors
            .into_iter()
            .filter_map(|idx| self.graph.node_weight(idx).cloned())
            .collect())
    }

    /// Returns all nodes with no incoming edges (roots).
    ///
    /// Results are sorted by insertion order for determinism.
    #[must_use]
    pub(crate) fn roots(&self) -> Vec<T> {
        let mut root_indices: Vec<NodeIndex> = self
            .graph
            .node_indices()
            .filter(|&idx| {
                self.graph
                    .neighbors_directed(idx, Direction::Incoming)
                    .count()
                    == 0
            })
            .collect();

        // Sort by insertion order for deterministic results
        root_indices.sort_by_key(|n| {
            self.insertion_order
                .iter()
                .position(|&i| i == *n)
                .unwrap_or(usize::MAX)
        });

        root_indices
            .into_iter()
            .filter_map(|idx| self.graph.node_weight(idx).cloned())
            .collect()
    }

    /// Returns all nodes with no outgoing edges (leaves).
    ///
    /// Results are sorted by insertion order for determinism.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn leaves(&self) -> Vec<T> {
        let mut leaf_indices: Vec<NodeIndex> = self
            .graph
            .node_indices()
            .filter(|&idx| {
                self.graph
                    .neighbors_directed(idx, Direction::Outgoing)
                    .count()
                    == 0
            })
            .collect();

        // Sort by insertion order for deterministic results
        leaf_indices.sort_by_key(|n| {
            self.insertion_order
                .iter()
                .position(|&i| i == *n)
                .unwrap_or(usize::MAX)
        });

        leaf_indices
            .into_iter()
            .filter_map(|idx| self.graph.node_weight(idx).cloned())
            .collect()
    }

    /// Returns true if the node exists in the DAG.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn contains(&self, node: &T) -> bool {
        self.index_map.contains_key(node)
    }

    /// Returns the node index for a value, if it exists.
    #[must_use]
    pub(crate) fn get_index(&self, value: &T) -> Option<NodeIndex> {
        self.index_map.get(value).copied()
    }
}

impl<T> Default for Dag<T>
where
    T: Clone + Eq + Hash + Display,
{
    fn default() -> Self {
        Self::new()
    }
}

// Tests are defined in Step 2 above and included here for completeness
// Note: Tests use NodeIndex returned from add_node() for type safety
```

**Step 5: Update lib.rs to export dag module (pub(crate))**

Add to `crates/arco-flow/src/lib.rs`:

```rust
// Internal module - not exposed in public API
pub(crate) mod dag;
```

**Step 6: Run tests to verify they pass**

Run: `cargo test -p arco-flow`

Expected: All tests PASS

**Step 7: Commit**

```bash
git add crates/arco-flow/src/dag.rs crates/arco-flow/src/lib.rs crates/arco-flow/Cargo.toml
git commit -m "feat(flow): add DAG implementation with topological sort

Implements a generic directed acyclic graph for dependency management
using petgraph. Supports topological sorting, cycle detection, and
upstream/downstream queries."
```

---

## Task 4: Expand Plan and TaskSpec Types

**Files:**
- Modify: `crates/arco-flow/src/plan.rs`
- Modify: `crates/arco-flow/Cargo.toml`
- Modify: `Cargo.toml` (workspace root)

**Step 1: Write the failing test for expanded Plan**

Add tests to `crates/arco-flow/src/plan.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::{AssetId, TaskId};

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
        // Stage 0 = root (no dependencies)
        assert_eq!(plan.tasks[0].stage, 0);
    }

    #[test]
    fn plan_fingerprint_is_deterministic() {
        let task_id = TaskId::generate();
        let asset_id = AssetId::generate();

        let plan1 = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
                asset_id,
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let plan2 = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
                asset_id,
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
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
                upstream_task_ids: vec![TaskId::generate()], // Non-existent
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build();

        assert!(result.is_err());
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
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-flow plan_builder_creates_valid_plan`

Expected: FAIL with "cannot find struct `PlanBuilder`"

**Step 3: Write complete implementation**

Replace `crates/arco-flow/src/plan.rs`:

```rust
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
                Error::TaskNotFound {
                    task_id: id.to_string(),
                }
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
}
```

**Step 4: Add hex, sha2, and humantime-serde dependencies (workspace pattern)**

Add `humantime-serde` to the workspace root `Cargo.toml` (if not already present):

```toml
[workspace.dependencies]
# ... existing deps ...
humantime-serde = "1"
```

Then modify `crates/arco-flow/Cargo.toml`:

```toml
[dependencies]
# ... existing deps ...
hex = { workspace = true }
humantime-serde = { workspace = true }
sha2 = { workspace = true }
ulid = { workspace = true }
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p arco-flow`

Expected: All tests PASS

**Step 6: Commit**

```bash
git add crates/arco-flow/src/plan.rs crates/arco-flow/Cargo.toml
git commit -m "feat(flow): expand Plan and TaskSpec with full proto alignment

Implements PlanBuilder with dependency validation, topological sorting,
and deterministic fingerprinting. Adds AssetKey, ResourceRequirements,
and DependencyEdge types matching proto definitions."
```

---

## Task 5: Implement TaskState and TaskExecution

**Files:**
- Create: `crates/arco-flow/src/task.rs`
- Modify: `crates/arco-flow/src/lib.rs`

**Step 1: Write the failing test for TaskState**

Create test in `crates/arco-flow/src/task.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_state_full_lifecycle() {
        // Test the full state machine: PLANNED -> PENDING -> READY -> QUEUED -> DISPATCHED -> RUNNING -> SUCCEEDED
        let state = TaskState::Planned;
        assert!(state.can_transition_to(TaskState::Pending));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Running));

        let state = TaskState::Pending;
        assert!(state.can_transition_to(TaskState::Ready));
        assert!(state.can_transition_to(TaskState::Skipped));
        assert!(!state.can_transition_to(TaskState::Running));

        let state = TaskState::Ready;
        assert!(state.can_transition_to(TaskState::Queued));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Running));

        let state = TaskState::Queued;
        assert!(state.can_transition_to(TaskState::Dispatched));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Running));

        let state = TaskState::Dispatched;
        assert!(state.can_transition_to(TaskState::Running));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Succeeded));
    }

    #[test]
    fn task_state_transitions_running_to_terminal() {
        let state = TaskState::Running;
        assert!(state.can_transition_to(TaskState::Succeeded));
        assert!(state.can_transition_to(TaskState::Failed));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Pending));
    }

    #[test]
    fn task_execution_state_machine() {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        assert_eq!(exec.state, TaskState::Planned);

        exec.transition_to(TaskState::Pending).unwrap();
        exec.transition_to(TaskState::Ready).unwrap();
        exec.transition_to(TaskState::Queued).unwrap();
        assert!(exec.queued_at.is_some());

        exec.transition_to(TaskState::Dispatched).unwrap();
        assert!(exec.dispatched_at.is_some());

        exec.transition_to(TaskState::Running).unwrap();
        assert!(exec.started_at.is_some());

        exec.transition_to(TaskState::Succeeded).unwrap();
        assert_eq!(exec.state, TaskState::Succeeded);
        assert!(exec.completed_at.is_some());
    }

    #[test]
    fn task_execution_invalid_transition_fails() {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        // Cannot jump from PLANNED to SUCCEEDED
        let result = exec.transition_to(TaskState::Succeeded);
        assert!(result.is_err());
    }

    #[test]
    fn task_execution_heartbeat() {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id)
            .with_heartbeat_timeout(Duration::from_secs(30));

        // Progress to running state
        exec.transition_to(TaskState::Pending).unwrap();
        exec.transition_to(TaskState::Ready).unwrap();
        exec.transition_to(TaskState::Queued).unwrap();
        exec.transition_to(TaskState::Dispatched).unwrap();
        exec.transition_to(TaskState::Running).unwrap();

        // Initial heartbeat is set when transitioning to Running
        assert!(exec.last_heartbeat.is_some());
        assert!(!exec.is_heartbeat_stale()); // Fresh heartbeat

        // Record new heartbeat
        exec.record_heartbeat();
        assert!(!exec.is_heartbeat_stale());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-flow task_state_full_lifecycle`

Expected: FAIL with "cannot find type `TaskState`"

**Step 3: Write complete implementation**

Create `crates/arco-flow/src/task.rs`:

```rust
//! Task execution state and lifecycle management.
//!
//! This module provides:
//! - `TaskState`: The state machine for task execution
//! - `TaskExecution`: Execution tracking for a single task
//! - `TaskOutput`: Output metadata from successful execution
//! - `TaskError`: Error information from failed execution

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use arco_core::{MaterializationId, TaskId};

use crate::error::{Error, Result};

/// Default heartbeat timeout (5 minutes).
const DEFAULT_HEARTBEAT_TIMEOUT_SECS: u64 = 300;

/// Task execution state machine.
///
/// States follow a directed graph matching the orchestration design:
/// ```text
/// ┌─────────┐  run starts  ┌─────────┐  deps met   ┌───────┐  quota   ┌────────┐
/// │ PLANNED │─────────────►│ PENDING │────────────►│ READY │─────────►│ QUEUED │
/// └─────────┘              └─────────┘             └───────┘          └────────┘
///                               │                      │                   │
///                          upstream                    │              dispatched
///                          failed                      │                   │
///                               │                      │                   ▼
///                               ▼                      │             ┌────────────┐
///                          ┌─────────┐                 │             │ DISPATCHED │
///                          │ SKIPPED │                 │             └────────────┘
///                          └─────────┘                 │                   │
///                               ▲                      │               ack received
///                               │                      │                   │
///                               │                      │                   ▼
///                               │                      │              ┌─────────┐
///                               └──────────────────────┴──────────────│ RUNNING │
///                                       (cancelled)                   └─────────┘
///                                                                          │
///                                                            ┌─────────────┼─────────────┐
///                                                            │             │             │
///                                                            ▼             ▼             ▼
///                                                      ┌───────────┐ ┌──────────┐ ┌───────────┐
///                                                      │ SUCCEEDED │ │  FAILED  │ │ CANCELLED │
///                                                      └───────────┘ └──────────┘ └───────────┘
///                                                                          │
///                                                                     retry?
///                                                                          │
///                                                                          ▼
///                                                                    ┌────────────┐
///                                                                    │ RETRY_WAIT │
///                                                                    └────────────┘
///                                                                          │
///                                                                     backoff expires
///                                                                          │
///                                                                          ▼
///                                                                      ┌───────┐
///                                                                      │ READY │
///                                                                      └───────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskState {
    /// Exists in plan, scheduler hasn't evaluated yet.
    Planned,
    /// Scheduler evaluated, blocked on dependencies.
    Pending,
    /// Dependencies met, waiting for quota.
    Ready,
    /// Quota acquired, pushed to queue.
    Queued,
    /// Sent to worker, awaiting acknowledgment.
    Dispatched,
    /// Worker acknowledged, actively executing.
    Running,
    /// Completed successfully.
    Succeeded,
    /// Failed (may retry).
    Failed,
    /// Skipped (upstream failed).
    Skipped,
    /// Cancelled by user or system.
    Cancelled,
    /// Waiting for retry backoff.
    RetryWait,
}

impl TaskState {
    /// Returns true if this is a terminal state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Skipped | Self::Cancelled
        )
    }

    /// Returns true if this state allows retry.
    #[must_use]
    pub const fn is_retriable(&self) -> bool {
        matches!(self, Self::Failed | Self::RetryWait)
    }

    /// Returns true if the task is actively executing or pending execution.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Planned
                | Self::Pending
                | Self::Ready
                | Self::Queued
                | Self::Dispatched
                | Self::Running
                | Self::RetryWait
        )
    }

    /// Returns true if the transition from self to target is valid.
    #[must_use]
    pub fn can_transition_to(&self, target: Self) -> bool {
        match self {
            Self::Planned => matches!(target, Self::Pending | Self::Cancelled),
            Self::Pending => matches!(target, Self::Ready | Self::Skipped | Self::Cancelled),
            Self::Ready => matches!(target, Self::Queued | Self::Cancelled),
            Self::Queued => matches!(target, Self::Dispatched | Self::Cancelled),
            Self::Dispatched => matches!(target, Self::Running | Self::Cancelled),
            Self::Running => {
                matches!(target, Self::Succeeded | Self::Failed | Self::Cancelled)
            }
            Self::Failed => matches!(target, Self::RetryWait | Self::Cancelled),
            Self::RetryWait => matches!(target, Self::Ready | Self::Cancelled),
            Self::Succeeded | Self::Skipped | Self::Cancelled => false,
        }
    }

    /// Returns all valid target states from the current state.
    #[must_use]
    pub fn valid_transitions(&self) -> Vec<Self> {
        match self {
            Self::Planned => vec![Self::Pending, Self::Cancelled],
            Self::Pending => vec![Self::Ready, Self::Skipped, Self::Cancelled],
            Self::Ready => vec![Self::Queued, Self::Cancelled],
            Self::Queued => vec![Self::Dispatched, Self::Cancelled],
            Self::Dispatched => vec![Self::Running, Self::Cancelled],
            Self::Running => vec![Self::Succeeded, Self::Failed, Self::Cancelled],
            Self::Failed => vec![Self::RetryWait, Self::Cancelled],
            Self::RetryWait => vec![Self::Ready, Self::Cancelled],
            Self::Succeeded | Self::Skipped | Self::Cancelled => vec![],
        }
    }
}

impl Default for TaskState {
    fn default() -> Self {
        Self::Planned
    }
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Planned => write!(f, "PLANNED"),
            Self::Pending => write!(f, "PENDING"),
            Self::Ready => write!(f, "READY"),
            Self::Queued => write!(f, "QUEUED"),
            Self::Dispatched => write!(f, "DISPATCHED"),
            Self::Running => write!(f, "RUNNING"),
            Self::Succeeded => write!(f, "SUCCEEDED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Skipped => write!(f, "SKIPPED"),
            Self::Cancelled => write!(f, "CANCELLED"),
            Self::RetryWait => write!(f, "RETRY_WAIT"),
        }
    }
}

/// Task error categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskErrorCategory {
    /// Error in user asset code.
    UserCode,
    /// Schema mismatch or constraint violation.
    DataQuality,
    /// Network, storage, or timeout.
    Infrastructure,
    /// Invalid configuration or missing secrets.
    Configuration,
    /// Unknown error category.
    Unknown,
}

impl Default for TaskErrorCategory {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Task error information.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskError {
    /// Error category.
    pub category: TaskErrorCategory,
    /// Error message.
    pub message: String,
    /// Stack trace or detail (truncated).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Whether the error is retryable.
    pub retryable: bool,
}

impl TaskError {
    /// Creates a new task error.
    #[must_use]
    pub fn new(category: TaskErrorCategory, message: impl Into<String>) -> Self {
        Self {
            category,
            message: message.into(),
            detail: None,
            retryable: matches!(category, TaskErrorCategory::Infrastructure),
        }
    }

    /// Sets the error detail.
    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Sets whether the error is retryable.
    #[must_use]
    pub const fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }
}

/// Task output reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskOutput {
    /// Materialization ID for output tracking.
    pub materialization_id: MaterializationId,
    /// Output files.
    #[serde(default)]
    pub files: Vec<FileEntry>,
    /// Output row count.
    #[serde(default)]
    pub row_count: i64,
    /// Output size in bytes.
    #[serde(default)]
    pub byte_size: i64,
}

/// File entry for output tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileEntry {
    /// Storage path.
    pub path: String,
    /// File size in bytes.
    #[serde(default)]
    pub size_bytes: i64,
    /// Row count (for tabular data).
    #[serde(default)]
    pub row_count: i64,
    /// Content hash (SHA-256).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    /// File format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

/// Task execution metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskMetrics {
    /// Wall clock duration in milliseconds.
    #[serde(default)]
    pub duration_ms: i64,
    /// CPU time in milliseconds.
    #[serde(default)]
    pub cpu_time_ms: i64,
    /// Peak memory usage in bytes.
    #[serde(default)]
    pub peak_memory_bytes: i64,
    /// Bytes read from storage.
    #[serde(default)]
    pub bytes_read: i64,
    /// Bytes written to storage.
    #[serde(default)]
    pub bytes_written: i64,
}

/// Execution state for a single task within a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskExecution {
    /// Task being executed.
    pub task_id: TaskId,
    /// Execution state.
    pub state: TaskState,
    /// Attempt number (1-indexed, increments on retry).
    pub attempt: u32,
    /// Maximum retry attempts.
    pub max_attempts: u32,
    /// When the task was queued.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queued_at: Option<DateTime<Utc>>,
    /// When the task was dispatched to a worker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dispatched_at: Option<DateTime<Utc>>,
    /// When execution started (worker acknowledged).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// When execution completed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Last heartbeat from worker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat: Option<DateTime<Utc>>,
    /// Heartbeat timeout duration.
    #[serde(with = "humantime_serde", default = "default_heartbeat_timeout")]
    pub heartbeat_timeout: Duration,
    /// Worker that executed this task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    /// Output reference (if succeeded).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<TaskOutput>,
    /// Error information (if failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<TaskError>,
    /// Execution metrics.
    #[serde(default)]
    pub metrics: TaskMetrics,
}

fn default_heartbeat_timeout() -> Duration {
    Duration::from_secs(DEFAULT_HEARTBEAT_TIMEOUT_SECS)
}

impl TaskExecution {
    /// Creates a new task execution in PLANNED state.
    #[must_use]
    pub fn new(task_id: TaskId) -> Self {
        Self {
            task_id,
            state: TaskState::Planned,
            attempt: 1,
            max_attempts: 3,
            queued_at: None,
            dispatched_at: None,
            started_at: None,
            completed_at: None,
            last_heartbeat: None,
            heartbeat_timeout: default_heartbeat_timeout(),
            worker_id: None,
            output: None,
            error: None,
            metrics: TaskMetrics::default(),
        }
    }

    /// Creates a task execution with custom max attempts.
    #[must_use]
    pub const fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Creates a task execution with custom heartbeat timeout.
    #[must_use]
    pub const fn with_heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = timeout;
        self
    }

    /// Returns true if the task is in a terminal state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    /// Returns true if the task can be retried.
    #[must_use]
    pub fn can_retry(&self) -> bool {
        self.state.is_retriable() && self.attempt < self.max_attempts
    }

    /// Records a heartbeat from the worker.
    pub fn record_heartbeat(&mut self) {
        self.last_heartbeat = Some(Utc::now());
    }

    /// Returns true if the heartbeat is stale (exceeded timeout).
    ///
    /// A task is considered stale if:
    /// - It is in RUNNING or DISPATCHED state
    /// - AND no heartbeat has been received
    /// - OR the last heartbeat exceeds the timeout
    #[must_use]
    pub fn is_heartbeat_stale(&self) -> bool {
        if !matches!(self.state, TaskState::Running | TaskState::Dispatched) {
            return false;
        }

        match self.last_heartbeat {
            None => true, // No heartbeat yet = stale
            Some(last) => {
                let elapsed = Utc::now().signed_duration_since(last);
                elapsed > chrono::Duration::from_std(self.heartbeat_timeout)
                    .unwrap_or(chrono::Duration::max_value())
            }
        }
    }

    /// Transitions to a new state.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    pub fn transition_to(&mut self, target: TaskState) -> Result<()> {
        if !self.state.can_transition_to(target) {
            return Err(Error::InvalidStateTransition {
                from: self.state.to_string(),
                to: target.to_string(),
                reason: format!(
                    "valid transitions from {}: {:?}",
                    self.state,
                    self.state.valid_transitions()
                ),
            });
        }

        let now = Utc::now();

        // Update timestamps based on transition
        match target {
            TaskState::Queued => {
                self.queued_at = Some(now);
            }
            TaskState::Dispatched => {
                self.dispatched_at = Some(now);
            }
            TaskState::Running => {
                self.started_at = Some(now);
                // Record initial heartbeat when task starts
                self.last_heartbeat = Some(now);
            }
            TaskState::Ready => {
                // On retry, increment attempt counter
                if self.state == TaskState::RetryWait {
                    self.attempt += 1;
                }
            }
            TaskState::Succeeded | TaskState::Failed | TaskState::Skipped | TaskState::Cancelled => {
                self.completed_at = Some(now);
                if let Some(started) = self.started_at {
                    self.metrics.duration_ms = (now - started).num_milliseconds();
                }
            }
            _ => {}
        }

        self.state = target;
        Ok(())
    }

    /// Marks the task as succeeded with output.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    pub fn succeed(&mut self, output: TaskOutput) -> Result<()> {
        self.output = Some(output);
        self.transition_to(TaskState::Succeeded)
    }

    /// Marks the task as failed with error.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    pub fn fail(&mut self, error: TaskError) -> Result<()> {
        self.error = Some(error);
        self.transition_to(TaskState::Failed)
    }

    /// Marks the task as skipped (upstream failed).
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    pub fn skip(&mut self) -> Result<()> {
        self.transition_to(TaskState::Skipped)
    }

    /// Marks the task as cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    pub fn cancel(&mut self) -> Result<()> {
        self.transition_to(TaskState::Cancelled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_state_full_lifecycle() {
        // Test the full state machine: PLANNED -> PENDING -> READY -> QUEUED -> DISPATCHED -> RUNNING -> SUCCEEDED
        let state = TaskState::Planned;
        assert!(state.can_transition_to(TaskState::Pending));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Running));

        let state = TaskState::Pending;
        assert!(state.can_transition_to(TaskState::Ready));
        assert!(state.can_transition_to(TaskState::Skipped));
        assert!(!state.can_transition_to(TaskState::Running));

        let state = TaskState::Ready;
        assert!(state.can_transition_to(TaskState::Queued));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Running));

        let state = TaskState::Queued;
        assert!(state.can_transition_to(TaskState::Dispatched));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Running));

        let state = TaskState::Dispatched;
        assert!(state.can_transition_to(TaskState::Running));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Succeeded));
    }

    #[test]
    fn task_state_transitions_running_to_terminal() {
        let state = TaskState::Running;
        assert!(state.can_transition_to(TaskState::Succeeded));
        assert!(state.can_transition_to(TaskState::Failed));
        assert!(state.can_transition_to(TaskState::Cancelled));
        assert!(!state.can_transition_to(TaskState::Pending));
    }

    #[test]
    fn task_execution_state_machine() {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        assert_eq!(exec.state, TaskState::Planned);

        exec.transition_to(TaskState::Pending).unwrap();
        exec.transition_to(TaskState::Ready).unwrap();
        exec.transition_to(TaskState::Queued).unwrap();
        assert!(exec.queued_at.is_some());

        exec.transition_to(TaskState::Dispatched).unwrap();
        assert!(exec.dispatched_at.is_some());

        exec.transition_to(TaskState::Running).unwrap();
        assert!(exec.started_at.is_some());
        assert!(exec.last_heartbeat.is_some()); // Initial heartbeat set

        exec.transition_to(TaskState::Succeeded).unwrap();
        assert_eq!(exec.state, TaskState::Succeeded);
        assert!(exec.completed_at.is_some());
    }

    #[test]
    fn task_execution_invalid_transition_fails() {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        // Cannot jump from PLANNED to SUCCEEDED
        let result = exec.transition_to(TaskState::Succeeded);
        assert!(result.is_err());
    }

    #[test]
    fn task_execution_retry_increments_attempt() {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        // Progress to running then fail
        exec.transition_to(TaskState::Pending).unwrap();
        exec.transition_to(TaskState::Ready).unwrap();
        exec.transition_to(TaskState::Queued).unwrap();
        exec.transition_to(TaskState::Dispatched).unwrap();
        exec.transition_to(TaskState::Running).unwrap();
        exec.transition_to(TaskState::Failed).unwrap();
        exec.transition_to(TaskState::RetryWait).unwrap();

        assert_eq!(exec.attempt, 1);

        // RetryWait -> Ready increments attempt
        exec.transition_to(TaskState::Ready).unwrap();
        assert_eq!(exec.attempt, 2);
    }

    #[test]
    fn task_execution_succeed_with_output() {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        exec.transition_to(TaskState::Pending).unwrap();
        exec.transition_to(TaskState::Ready).unwrap();
        exec.transition_to(TaskState::Queued).unwrap();
        exec.transition_to(TaskState::Dispatched).unwrap();
        exec.transition_to(TaskState::Running).unwrap();

        let output = TaskOutput {
            materialization_id: MaterializationId::generate(),
            files: vec![],
            row_count: 1000,
            byte_size: 1024,
        };

        exec.succeed(output).unwrap();

        assert_eq!(exec.state, TaskState::Succeeded);
        assert!(exec.output.is_some());
    }

    #[test]
    fn task_execution_fail_with_error() {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id);

        exec.transition_to(TaskState::Pending).unwrap();
        exec.transition_to(TaskState::Ready).unwrap();
        exec.transition_to(TaskState::Queued).unwrap();
        exec.transition_to(TaskState::Dispatched).unwrap();
        exec.transition_to(TaskState::Running).unwrap();

        let error = TaskError::new(TaskErrorCategory::UserCode, "assertion failed");
        exec.fail(error).unwrap();

        assert_eq!(exec.state, TaskState::Failed);
        assert!(exec.error.is_some());
    }

    #[test]
    fn task_execution_heartbeat() {
        let task_id = TaskId::generate();
        let mut exec = TaskExecution::new(task_id)
            .with_heartbeat_timeout(Duration::from_secs(30));

        // Progress to running state
        exec.transition_to(TaskState::Pending).unwrap();
        exec.transition_to(TaskState::Ready).unwrap();
        exec.transition_to(TaskState::Queued).unwrap();
        exec.transition_to(TaskState::Dispatched).unwrap();
        exec.transition_to(TaskState::Running).unwrap();

        // Initial heartbeat is set when transitioning to Running
        assert!(exec.last_heartbeat.is_some());
        assert!(!exec.is_heartbeat_stale()); // Fresh heartbeat

        // Record new heartbeat
        exec.record_heartbeat();
        assert!(!exec.is_heartbeat_stale());
    }
}
```

**Step 4: Update lib.rs to export task module**

Add to `crates/arco-flow/src/lib.rs`:

```rust
pub mod task;
```

And update prelude:

```rust
pub mod prelude {
    pub use crate::error::{Error, Result};
    pub use crate::plan::{Plan, PlanBuilder, TaskSpec};
    pub use crate::run::Run;
    pub use crate::scheduler::Scheduler;
    pub use crate::task::{TaskExecution, TaskState};
}
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p arco-flow`

Expected: All tests PASS

**Step 6: Commit**

```bash
git add crates/arco-flow/src/task.rs crates/arco-flow/src/lib.rs
git commit -m "feat(flow): implement TaskState and TaskExecution

Adds complete task execution state machine with valid transitions,
TaskOutput, TaskError, TaskMetrics types. Supports retry logic with
attempt tracking."
```

---

## Task 6: Expand Run and RunState Types

**Files:**
- Modify: `crates/arco-flow/src/run.rs`

**Step 1: Write the failing test for expanded Run**

Add tests to `crates/arco-flow/src/run.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
    use crate::task::TaskExecution;
    use arco_core::{AssetId, TaskId};

    #[test]
    fn run_initializes_from_plan() {
        let task_id = TaskId::generate();
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
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

        let run = Run::from_plan(&plan, RunTrigger::manual("user@example.com"));

        assert_eq!(run.state, RunState::Pending);
        assert_eq!(run.tenant_id, "tenant");
        assert_eq!(run.task_executions.len(), 1);
    }

    #[test]
    fn run_state_transitions() {
        let state = RunState::Pending;
        assert!(state.can_transition_to(RunState::Running));
        assert!(!state.can_transition_to(RunState::Succeeded));
    }

    #[test]
    fn run_tracks_task_progress() {
        let task_id = TaskId::generate();
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
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

        let mut run = Run::from_plan(&plan, RunTrigger::manual("user"));

        assert_eq!(run.tasks_pending(), 1);
        assert_eq!(run.tasks_succeeded(), 0);

        run.get_task_mut(&task_id)
            .unwrap()
            .transition_to(crate::task::TaskState::Queued)
            .unwrap();
        run.get_task_mut(&task_id)
            .unwrap()
            .transition_to(crate::task::TaskState::Running)
            .unwrap();
        run.get_task_mut(&task_id)
            .unwrap()
            .transition_to(crate::task::TaskState::Succeeded)
            .unwrap();

        assert_eq!(run.tasks_pending(), 0);
        assert_eq!(run.tasks_succeeded(), 1);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-flow run_initializes_from_plan`

Expected: FAIL with "no method named `from_plan`"

**Step 3: Write complete implementation**

Replace `crates/arco-flow/src/run.rs`:

```rust
//! Execution run tracking.
//!
//! A run represents a single execution of a plan, capturing:
//!
//! - **Inputs**: What data was read
//! - **Outputs**: What data was produced
//! - **Timing**: When each task started and completed
//! - **State**: Current status and any errors

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use arco_core::{RunId, TaskId};

use crate::error::{Error, Result};
use crate::plan::Plan;
use crate::task::{TaskExecution, TaskState};

/// Run state machine states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunState {
    /// Created, waiting to start.
    Pending,
    /// Actively executing tasks.
    Running,
    /// All tasks completed successfully.
    Succeeded,
    /// One or more tasks failed.
    Failed,
    /// Being cancelled (waiting for in-flight tasks).
    Cancelling,
    /// Cancelled by user or system.
    Cancelled,
    /// Exceeded maximum duration.
    TimedOut,
}

impl RunState {
    /// Returns true if this is a terminal state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }

    /// Returns true if the transition from self to target is valid.
    #[must_use]
    pub fn can_transition_to(&self, target: Self) -> bool {
        match self {
            Self::Pending => matches!(target, Self::Running | Self::Cancelling | Self::Cancelled),
            Self::Running => matches!(
                target,
                Self::Succeeded | Self::Failed | Self::Cancelling | Self::TimedOut
            ),
            Self::Cancelling => matches!(target, Self::Cancelled),
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut => false,
        }
    }
}

impl Default for RunState {
    fn default() -> Self {
        Self::Pending
    }
}

impl std::fmt::Display for RunState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "PENDING"),
            Self::Running => write!(f, "RUNNING"),
            Self::Succeeded => write!(f, "SUCCEEDED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelling => write!(f, "CANCELLING"),
            Self::Cancelled => write!(f, "CANCELLED"),
            Self::TimedOut => write!(f, "TIMED_OUT"),
        }
    }
}

/// Trigger type enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TriggerType {
    /// User-initiated.
    Manual,
    /// Cron/schedule-based.
    Scheduled,
    /// Event-driven (e.g., file arrival).
    Sensor,
    /// Historical data backfill.
    Backfill,
}

impl Default for TriggerType {
    fn default() -> Self {
        Self::Manual
    }
}

/// How the run was triggered.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunTrigger {
    /// Trigger type.
    #[serde(rename = "type")]
    pub trigger_type: TriggerType,
    /// User who triggered (if manual).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triggered_by: Option<String>,
    /// Trigger timestamp.
    pub triggered_at: DateTime<Utc>,
    /// Associated schedule name (if scheduled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule_name: Option<String>,
}

impl RunTrigger {
    /// Creates a manual trigger.
    #[must_use]
    pub fn manual(user: impl Into<String>) -> Self {
        Self {
            trigger_type: TriggerType::Manual,
            triggered_by: Some(user.into()),
            triggered_at: Utc::now(),
            schedule_name: None,
        }
    }

    /// Creates a scheduled trigger.
    #[must_use]
    pub fn scheduled(schedule_name: impl Into<String>) -> Self {
        Self {
            trigger_type: TriggerType::Scheduled,
            triggered_by: None,
            triggered_at: Utc::now(),
            schedule_name: Some(schedule_name.into()),
        }
    }

    /// Creates a sensor trigger.
    #[must_use]
    pub fn sensor() -> Self {
        Self {
            trigger_type: TriggerType::Sensor,
            triggered_by: None,
            triggered_at: Utc::now(),
            schedule_name: None,
        }
    }

    /// Creates a backfill trigger.
    #[must_use]
    pub fn backfill(user: impl Into<String>) -> Self {
        Self {
            trigger_type: TriggerType::Backfill,
            triggered_by: Some(user.into()),
            triggered_at: Utc::now(),
            schedule_name: None,
        }
    }
}

/// A pipeline execution run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Run {
    /// Unique run identifier.
    pub id: RunId,
    /// Plan being executed.
    pub plan_id: String,
    /// Tenant scope.
    pub tenant_id: String,
    /// Workspace scope.
    pub workspace_id: String,
    /// Current state of the run.
    pub state: RunState,
    /// When the run was created.
    pub created_at: DateTime<Utc>,
    /// When the run started executing (if started).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// When the run completed (if completed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Task execution states.
    pub task_executions: Vec<TaskExecution>,
    /// Run-level labels.
    #[serde(default)]
    pub labels: HashMap<String, String>,
    /// Trigger information.
    pub trigger: RunTrigger,
}

impl Run {
    /// Creates a new run from a plan.
    #[must_use]
    pub fn from_plan(plan: &Plan, trigger: RunTrigger) -> Self {
        let task_executions = plan
            .tasks
            .iter()
            .map(|task| TaskExecution::new(task.task_id))
            .collect();

        Self {
            id: RunId::generate(),
            plan_id: plan.plan_id.clone(),
            tenant_id: plan.tenant_id.clone(),
            workspace_id: plan.workspace_id.clone(),
            state: RunState::Pending,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            task_executions,
            labels: HashMap::new(),
            trigger,
        }
    }

    /// Returns true if the run is in a terminal state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    /// Transitions to a new state.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    pub fn transition_to(&mut self, target: RunState) -> Result<()> {
        if !self.state.can_transition_to(target) {
            return Err(Error::InvalidStateTransition {
                from: self.state.to_string(),
                to: target.to_string(),
                reason: "invalid run state transition".into(),
            });
        }

        let now = Utc::now();

        match target {
            RunState::Running => {
                self.started_at = Some(now);
            }
            RunState::Succeeded
            | RunState::Failed
            | RunState::Cancelled
            | RunState::TimedOut => {
                self.completed_at = Some(now);
            }
            _ => {}
        }

        self.state = target;
        Ok(())
    }

    /// Returns the task execution for a given task ID.
    #[must_use]
    pub fn get_task(&self, task_id: &TaskId) -> Option<&TaskExecution> {
        self.task_executions.iter().find(|t| &t.task_id == task_id)
    }

    /// Returns mutable task execution for a given task ID.
    pub fn get_task_mut(&mut self, task_id: &TaskId) -> Option<&mut TaskExecution> {
        self.task_executions
            .iter_mut()
            .find(|t| &t.task_id == task_id)
    }

    /// Returns the count of tasks in pending state.
    #[must_use]
    pub fn tasks_pending(&self) -> usize {
        self.task_executions
            .iter()
            .filter(|t| t.state == TaskState::Pending)
            .count()
    }

    /// Returns the count of tasks in queued state.
    #[must_use]
    pub fn tasks_queued(&self) -> usize {
        self.task_executions
            .iter()
            .filter(|t| t.state == TaskState::Queued)
            .count()
    }

    /// Returns the count of tasks in running state.
    #[must_use]
    pub fn tasks_running(&self) -> usize {
        self.task_executions
            .iter()
            .filter(|t| t.state == TaskState::Running)
            .count()
    }

    /// Returns the count of succeeded tasks.
    #[must_use]
    pub fn tasks_succeeded(&self) -> usize {
        self.task_executions
            .iter()
            .filter(|t| t.state == TaskState::Succeeded)
            .count()
    }

    /// Returns the count of failed tasks.
    #[must_use]
    pub fn tasks_failed(&self) -> usize {
        self.task_executions
            .iter()
            .filter(|t| t.state == TaskState::Failed)
            .count()
    }

    /// Returns the count of skipped tasks.
    #[must_use]
    pub fn tasks_skipped(&self) -> usize {
        self.task_executions
            .iter()
            .filter(|t| t.state == TaskState::Skipped)
            .count()
    }

    /// Returns the count of cancelled tasks.
    #[must_use]
    pub fn tasks_cancelled(&self) -> usize {
        self.task_executions
            .iter()
            .filter(|t| t.state == TaskState::Cancelled)
            .count()
    }

    /// Returns tasks ready to execute (all dependencies satisfied).
    #[must_use]
    pub fn ready_tasks(&self, plan: &Plan) -> Vec<TaskId> {
        self.task_executions
            .iter()
            .filter(|exec| exec.state == TaskState::Pending)
            .filter(|exec| {
                // Get upstream task IDs from plan
                plan.get_task(&exec.task_id)
                    .map(|spec| {
                        spec.upstream_task_ids.iter().all(|dep_id| {
                            self.get_task(dep_id)
                                .map(|dep| dep.state == TaskState::Succeeded)
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false)
            })
            .map(|exec| exec.task_id)
            .collect()
    }

    /// Returns true if all tasks are in terminal states.
    #[must_use]
    pub fn all_tasks_terminal(&self) -> bool {
        self.task_executions.iter().all(|t| t.is_terminal())
    }

    /// Computes the final run state based on task states.
    ///
    /// Call this after all tasks are terminal to determine the run outcome.
    #[must_use]
    pub fn compute_final_state(&self) -> RunState {
        if !self.all_tasks_terminal() {
            return self.state;
        }

        if self.tasks_cancelled() > 0 {
            return RunState::Cancelled;
        }

        if self.tasks_failed() > 0 {
            return RunState::Failed;
        }

        RunState::Succeeded
    }

    /// Adds a label to the run.
    pub fn add_label(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.labels.insert(key.into(), value.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
    use arco_core::AssetId;

    #[test]
    fn run_initializes_from_plan() {
        let task_id = TaskId::generate();
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
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

        let run = Run::from_plan(&plan, RunTrigger::manual("user@example.com"));

        assert_eq!(run.state, RunState::Pending);
        assert_eq!(run.tenant_id, "tenant");
        assert_eq!(run.task_executions.len(), 1);
    }

    #[test]
    fn run_state_transitions() {
        let state = RunState::Pending;
        assert!(state.can_transition_to(RunState::Running));
        assert!(!state.can_transition_to(RunState::Succeeded));
    }

    #[test]
    fn run_tracks_task_progress() {
        let task_id = TaskId::generate();
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
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

        let mut run = Run::from_plan(&plan, RunTrigger::manual("user"));

        assert_eq!(run.tasks_pending(), 1);
        assert_eq!(run.tasks_succeeded(), 0);

        run.get_task_mut(&task_id)
            .unwrap()
            .transition_to(TaskState::Queued)
            .unwrap();
        run.get_task_mut(&task_id)
            .unwrap()
            .transition_to(TaskState::Running)
            .unwrap();
        run.get_task_mut(&task_id)
            .unwrap()
            .transition_to(TaskState::Succeeded)
            .unwrap();

        assert_eq!(run.tasks_pending(), 0);
        assert_eq!(run.tasks_succeeded(), 1);
    }

    #[test]
    fn run_ready_tasks_respects_dependencies() {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();

        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "cleaned"),
                partition_key: None,
                upstream_task_ids: vec![task_a],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let mut run = Run::from_plan(&plan, RunTrigger::manual("user"));

        // Only task_a should be ready initially
        let ready = run.ready_tasks(&plan);
        assert_eq!(ready.len(), 1);
        assert!(ready.contains(&task_a));

        // Mark task_a as succeeded
        run.get_task_mut(&task_a)
            .unwrap()
            .transition_to(TaskState::Queued)
            .unwrap();
        run.get_task_mut(&task_a)
            .unwrap()
            .transition_to(TaskState::Running)
            .unwrap();
        run.get_task_mut(&task_a)
            .unwrap()
            .transition_to(TaskState::Succeeded)
            .unwrap();

        // Now task_b should be ready
        let ready = run.ready_tasks(&plan);
        assert_eq!(ready.len(), 1);
        assert!(ready.contains(&task_b));
    }

    #[test]
    fn run_compute_final_state() {
        let task_id = TaskId::generate();
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
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

        let mut run = Run::from_plan(&plan, RunTrigger::manual("user"));

        run.get_task_mut(&task_id)
            .unwrap()
            .transition_to(TaskState::Queued)
            .unwrap();
        run.get_task_mut(&task_id)
            .unwrap()
            .transition_to(TaskState::Running)
            .unwrap();
        run.get_task_mut(&task_id)
            .unwrap()
            .transition_to(TaskState::Succeeded)
            .unwrap();

        assert_eq!(run.compute_final_state(), RunState::Succeeded);
    }
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -p arco-flow`

Expected: All tests PASS

**Step 5: Commit**

```bash
git add crates/arco-flow/src/run.rs
git commit -m "feat(flow): expand Run with full state machine and task tracking

Implements Run creation from Plan, RunState transitions, task progress
tracking, ready task computation respecting dependencies, and final
state computation. Adds RunTrigger with manual/scheduled/sensor/backfill types."
```

---

## Task 7: Implement Dependency-Aware Scheduler

**Files:**
- Modify: `crates/arco-flow/src/scheduler.rs`

**Step 1: Write the failing test for scheduler**

Add tests to `crates/arco-flow/src/scheduler.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
    use crate::run::RunTrigger;
    use arco_core::{AssetId, TaskId};

    #[test]
    fn scheduler_creates_run_from_plan() {
        let task_id = TaskId::generate();
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
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

        let scheduler = Scheduler::new(plan);
        let run = scheduler.create_run(RunTrigger::manual("user"));

        assert_eq!(run.state, RunState::Pending);
        assert_eq!(run.task_executions.len(), 1);
    }

    #[test]
    fn scheduler_returns_ready_tasks() {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();

        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "cleaned"),
                partition_key: None,
                upstream_task_ids: vec![task_a],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let scheduler = Scheduler::new(plan);
        let run = scheduler.create_run(RunTrigger::manual("user"));

        let ready = scheduler.get_ready_tasks(&run);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, task_a);
    }

    #[test]
    fn scheduler_skips_downstream_on_failure() {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();

        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "cleaned"),
                partition_key: None,
                upstream_task_ids: vec![task_a],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let scheduler = Scheduler::new(plan);
        let mut run = scheduler.create_run(RunTrigger::manual("user"));

        // Fail task_a
        run.get_task_mut(&task_a)
            .unwrap()
            .transition_to(TaskState::Queued)
            .unwrap();
        run.get_task_mut(&task_a)
            .unwrap()
            .transition_to(TaskState::Running)
            .unwrap();
        run.get_task_mut(&task_a)
            .unwrap()
            .transition_to(TaskState::Failed)
            .unwrap();

        // Process failure - should skip downstream
        scheduler.process_task_completion(&mut run, &task_a);

        let task_b_state = run.get_task(&task_b).unwrap().state;
        assert_eq!(task_b_state, TaskState::Skipped);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-flow scheduler_creates_run_from_plan`

Expected: FAIL with "no method named `create_run`"

**Step 3: Write complete implementation**

Replace `crates/arco-flow/src/scheduler.rs`:

```rust
//! Dependency-aware task scheduling.
//!
//! The scheduler executes plans with:
//!
//! - **Parallelism**: Independent tasks run concurrently
//! - **Dependency ordering**: Tasks wait for their dependencies
//! - **Fault tolerance**: Failed tasks skip downstream, continue independent branches

use std::collections::HashSet;

use arco_core::TaskId;

use crate::error::Result;
use crate::plan::{Plan, TaskSpec};
use crate::run::{Run, RunState, RunTrigger};
use crate::task::TaskState;

/// Configuration for the scheduler.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Maximum concurrent tasks.
    pub max_parallelism: usize,
    /// Whether to continue on task failure (skip downstream only).
    pub continue_on_failure: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_parallelism: 10,
            continue_on_failure: true,
        }
    }
}

/// Scheduler for executing plans.
///
/// The scheduler is responsible for:
/// - Creating runs from plans
/// - Determining which tasks are ready to execute
/// - Processing task completions and cascading effects
/// - Computing final run state
#[derive(Debug)]
pub struct Scheduler {
    plan: Plan,
    config: SchedulerConfig,
}

impl Scheduler {
    /// Creates a new scheduler for the given plan.
    #[must_use]
    pub fn new(plan: Plan) -> Self {
        Self {
            plan,
            config: SchedulerConfig::default(),
        }
    }

    /// Creates a scheduler with custom configuration.
    #[must_use]
    pub fn with_config(plan: Plan, config: SchedulerConfig) -> Self {
        Self { plan, config }
    }

    /// Returns the plan being scheduled.
    #[must_use]
    pub const fn plan(&self) -> &Plan {
        &self.plan
    }

    /// Returns the scheduler configuration.
    #[must_use]
    pub const fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    /// Creates a new run from the plan.
    #[must_use]
    pub fn create_run(&self, trigger: RunTrigger) -> Run {
        Run::from_plan(&self.plan, trigger)
    }

    /// Returns tasks that are ready to execute.
    ///
    /// A task is ready if:
    /// - It is in Pending state
    /// - All its upstream dependencies have Succeeded
    /// - We haven't exceeded max_parallelism (considering running tasks)
    #[must_use]
    pub fn get_ready_tasks(&self, run: &Run) -> Vec<&TaskSpec> {
        let running_count = run.tasks_running() + run.tasks_queued();
        let available_slots = self.config.max_parallelism.saturating_sub(running_count);

        if available_slots == 0 {
            return Vec::new();
        }

        let ready_ids = run.ready_tasks(&self.plan);

        // Sort by priority (lower = higher priority), then by task order in plan
        let mut ready_tasks: Vec<_> = ready_ids
            .iter()
            .filter_map(|id| self.plan.get_task(id))
            .collect();

        ready_tasks.sort_by(|a, b| {
            a.priority
                .cmp(&b.priority)
                .then_with(|| {
                    // Use plan order as tiebreaker
                    let pos_a = self.plan.tasks.iter().position(|t| t.task_id == a.task_id);
                    let pos_b = self.plan.tasks.iter().position(|t| t.task_id == b.task_id);
                    pos_a.cmp(&pos_b)
                })
        });

        ready_tasks.into_iter().take(available_slots).collect()
    }

    /// Processes task completion and cascades effects to downstream tasks.
    ///
    /// If a task fails and `continue_on_failure` is true, all downstream
    /// tasks that depend on it (directly or transitively) will be skipped.
    pub fn process_task_completion(&self, run: &mut Run, task_id: &TaskId) {
        let Some(execution) = run.get_task(task_id) else {
            return;
        };

        // If task failed or was cancelled, skip downstream tasks
        if matches!(execution.state, TaskState::Failed | TaskState::Cancelled) {
            if self.config.continue_on_failure {
                self.skip_downstream_tasks(run, task_id);
            }
        }
    }

    /// Skips all tasks that depend on the given task (directly or transitively).
    fn skip_downstream_tasks(&self, run: &mut Run, failed_task_id: &TaskId) {
        let mut to_skip: HashSet<TaskId> = HashSet::new();
        let mut frontier: Vec<TaskId> = vec![*failed_task_id];

        // BFS to find all transitive dependents
        while let Some(current) = frontier.pop() {
            for task in &self.plan.tasks {
                if task.upstream_task_ids.contains(&current) && !to_skip.contains(&task.task_id) {
                    to_skip.insert(task.task_id);
                    frontier.push(task.task_id);
                }
            }
        }

        // Skip all identified tasks
        for task_id in to_skip {
            if let Some(exec) = run.get_task_mut(&task_id) {
                if exec.state == TaskState::Pending {
                    let _ = exec.skip();
                }
            }
        }
    }

    /// Queues a task for execution.
    ///
    /// # Errors
    ///
    /// Returns an error if the task is not in Pending state.
    pub fn queue_task(&self, run: &mut Run, task_id: &TaskId) -> Result<()> {
        let exec = run
            .get_task_mut(task_id)
            .ok_or_else(|| crate::error::Error::TaskNotFound {
                task_id: task_id.to_string(),
            })?;

        exec.transition_to(TaskState::Queued)
    }

    /// Starts task execution.
    ///
    /// # Errors
    ///
    /// Returns an error if the task is not in Queued state.
    pub fn start_task(&self, run: &mut Run, task_id: &TaskId, worker_id: &str) -> Result<()> {
        let exec = run
            .get_task_mut(task_id)
            .ok_or_else(|| crate::error::Error::TaskNotFound {
                task_id: task_id.to_string(),
            })?;

        exec.transition_to(TaskState::Running)?;
        exec.worker_id = Some(worker_id.to_string());
        Ok(())
    }

    /// Checks if the run should transition to a terminal state.
    ///
    /// Returns the new state if a transition should occur.
    #[must_use]
    pub fn check_run_completion(&self, run: &Run) -> Option<RunState> {
        if !run.all_tasks_terminal() {
            return None;
        }

        Some(run.compute_final_state())
    }

    /// Starts the run (transitions from Pending to Running).
    ///
    /// # Errors
    ///
    /// Returns an error if the run is not in Pending state.
    pub fn start_run(&self, run: &mut Run) -> Result<()> {
        run.transition_to(RunState::Running)
    }

    /// Completes the run with the final state.
    ///
    /// # Errors
    ///
    /// Returns an error if the transition is invalid.
    pub fn complete_run(&self, run: &mut Run, final_state: RunState) -> Result<()> {
        run.transition_to(final_state)
    }

    /// Cancels the run and all pending tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if the run cannot be cancelled.
    pub fn cancel_run(&self, run: &mut Run) -> Result<()> {
        // First transition to Cancelling
        if run.state != RunState::Cancelling {
            run.transition_to(RunState::Cancelling)?;
        }

        // Cancel all pending and queued tasks
        for exec in &mut run.task_executions {
            if matches!(exec.state, TaskState::Pending | TaskState::Queued) {
                let _ = exec.cancel();
            }
        }

        // If no tasks are running, complete cancellation
        if run.tasks_running() == 0 {
            run.transition_to(RunState::Cancelled)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
    use arco_core::AssetId;

    #[test]
    fn scheduler_creates_run_from_plan() {
        let task_id = TaskId::generate();
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
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

        let scheduler = Scheduler::new(plan);
        let run = scheduler.create_run(RunTrigger::manual("user"));

        assert_eq!(run.state, RunState::Pending);
        assert_eq!(run.task_executions.len(), 1);
    }

    #[test]
    fn scheduler_returns_ready_tasks() {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();

        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "cleaned"),
                partition_key: None,
                upstream_task_ids: vec![task_a],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let scheduler = Scheduler::new(plan);
        let run = scheduler.create_run(RunTrigger::manual("user"));

        let ready = scheduler.get_ready_tasks(&run);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].task_id, task_a);
    }

    #[test]
    fn scheduler_skips_downstream_on_failure() {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();

        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "cleaned"),
                partition_key: None,
                upstream_task_ids: vec![task_a],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let scheduler = Scheduler::new(plan);
        let mut run = scheduler.create_run(RunTrigger::manual("user"));

        // Fail task_a
        run.get_task_mut(&task_a)
            .unwrap()
            .transition_to(TaskState::Queued)
            .unwrap();
        run.get_task_mut(&task_a)
            .unwrap()
            .transition_to(TaskState::Running)
            .unwrap();
        run.get_task_mut(&task_a)
            .unwrap()
            .transition_to(TaskState::Failed)
            .unwrap();

        // Process failure - should skip downstream
        scheduler.process_task_completion(&mut run, &task_a);

        let task_b_state = run.get_task(&task_b).unwrap().state;
        assert_eq!(task_b_state, TaskState::Skipped);
    }

    #[test]
    fn scheduler_respects_priority() {
        let task_low = TaskId::generate();
        let task_high = TaskId::generate();

        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_low,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "low"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 10, // Lower priority
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_high,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "high"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 1, // Higher priority
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let scheduler = Scheduler::new(plan);
        let run = scheduler.create_run(RunTrigger::manual("user"));

        let ready = scheduler.get_ready_tasks(&run);
        assert_eq!(ready.len(), 2);
        // Higher priority (lower number) should be first
        assert_eq!(ready[0].task_id, task_high);
        assert_eq!(ready[1].task_id, task_low);
    }

    #[test]
    fn scheduler_respects_max_parallelism() {
        let tasks: Vec<_> = (0..5)
            .map(|_| {
                TaskSpec {
                    task_id: TaskId::generate(),
                    asset_id: AssetId::generate(),
                    asset_key: AssetKey::new("raw", "task"),
                    partition_key: None,
                    upstream_task_ids: vec![],
                    stage: 0,
                    priority: 0,
                    resources: ResourceRequirements::default(),
                }
            })
            .collect();

        let mut builder = PlanBuilder::new("tenant", "workspace");
        for task in tasks {
            builder = builder.add_task(task);
        }
        let plan = builder.build().unwrap();

        let config = SchedulerConfig {
            max_parallelism: 2,
            continue_on_failure: true,
        };
        let scheduler = Scheduler::with_config(plan, config);
        let run = scheduler.create_run(RunTrigger::manual("user"));

        let ready = scheduler.get_ready_tasks(&run);
        assert_eq!(ready.len(), 2); // Limited by max_parallelism
    }

    #[test]
    fn scheduler_run_lifecycle() {
        let task_id = TaskId::generate();
        let plan = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
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

        let scheduler = Scheduler::new(plan);
        let mut run = scheduler.create_run(RunTrigger::manual("user"));

        // Start run
        scheduler.start_run(&mut run).unwrap();
        assert_eq!(run.state, RunState::Running);

        // Queue and run task
        scheduler.queue_task(&mut run, &task_id).unwrap();
        scheduler
            .start_task(&mut run, &task_id, "worker-1")
            .unwrap();
        run.get_task_mut(&task_id)
            .unwrap()
            .transition_to(TaskState::Succeeded)
            .unwrap();

        // Check completion
        let final_state = scheduler.check_run_completion(&run);
        assert_eq!(final_state, Some(RunState::Succeeded));

        // Complete run
        scheduler
            .complete_run(&mut run, RunState::Succeeded)
            .unwrap();
        assert_eq!(run.state, RunState::Succeeded);
    }

    #[test]
    fn scheduler_cancel_run() {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();

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
                asset_key: AssetKey::new("raw", "b"),
                partition_key: None,
                upstream_task_ids: vec![],
                stage: 0,
                priority: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let scheduler = Scheduler::new(plan);
        let mut run = scheduler.create_run(RunTrigger::manual("user"));

        scheduler.start_run(&mut run).unwrap();
        scheduler.cancel_run(&mut run).unwrap();

        assert_eq!(run.state, RunState::Cancelled);
        assert_eq!(run.get_task(&task_a).unwrap().state, TaskState::Cancelled);
        assert_eq!(run.get_task(&task_b).unwrap().state, TaskState::Cancelled);
    }
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -p arco-flow`

Expected: All tests PASS

**Step 5: Commit**

```bash
git add crates/arco-flow/src/scheduler.rs
git commit -m "feat(flow): implement dependency-aware scheduler

Adds Scheduler with ready task computation, priority sorting, max
parallelism limits, downstream skip on failure, and run lifecycle
management. Supports continue-on-failure mode for partial execution."
```

---

## Task 8: Add Runner Trait for Task Execution

**Files:**
- Create: `crates/arco-flow/src/runner.rs`
- Modify: `crates/arco-flow/src/lib.rs`

**Step 1: Write the failing test for runner**

Create test in `crates/arco-flow/src/runner.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{AssetKey, ResourceRequirements, TaskSpec};
    use crate::task::{TaskError, TaskErrorCategory, TaskOutput};
    use arco_core::{AssetId, MaterializationId, TaskId};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct CountingRunner {
        count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Runner for CountingRunner {
        async fn run(&self, _context: &RunContext, _task: &TaskSpec) -> TaskResult {
            self.count.fetch_add(1, Ordering::SeqCst);
            TaskResult::Succeeded(TaskOutput {
                materialization_id: MaterializationId::generate(),
                files: vec![],
                row_count: 100,
                byte_size: 1024,
            })
        }
    }

    #[tokio::test]
    async fn runner_executes_task() {
        let count = Arc::new(AtomicUsize::new(0));
        let runner = CountingRunner {
            count: count.clone(),
        };

        let context = RunContext {
            tenant_id: "tenant".into(),
            workspace_id: "workspace".into(),
            run_id: arco_core::RunId::generate(),
        };

        let task = TaskSpec {
            task_id: TaskId::generate(),
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("raw", "events"),
            partition_key: None,
            upstream_task_ids: vec![],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        };

        let result = runner.run(&context, &task).await;

        assert!(matches!(result, TaskResult::Succeeded(_)));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-flow runner_executes_task`

Expected: FAIL with "cannot find trait `Runner`"

**Step 3: Write complete implementation**

Create `crates/arco-flow/src/runner.rs`:

```rust
//! Task execution runner trait and implementations.
//!
//! The runner is responsible for executing individual tasks,
//! typically by delegating to worker processes or cloud functions.

use async_trait::async_trait;

use arco_core::RunId;

use crate::plan::TaskSpec;
use crate::task::{TaskError, TaskOutput};

/// Context for a task execution.
#[derive(Debug, Clone)]
pub struct RunContext {
    /// Tenant scope.
    pub tenant_id: String,
    /// Workspace scope.
    pub workspace_id: String,
    /// Run this task belongs to.
    pub run_id: RunId,
}

/// Result of a task execution.
#[derive(Debug)]
pub enum TaskResult {
    /// Task completed successfully.
    Succeeded(TaskOutput),
    /// Task failed with an error.
    Failed(TaskError),
    /// Task was cancelled.
    Cancelled,
}

impl TaskResult {
    /// Returns true if the task succeeded.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded(_))
    }

    /// Returns the output if succeeded.
    #[must_use]
    pub fn output(&self) -> Option<&TaskOutput> {
        match self {
            Self::Succeeded(output) => Some(output),
            _ => None,
        }
    }

    /// Returns the error if failed.
    #[must_use]
    pub fn error(&self) -> Option<&TaskError> {
        match self {
            Self::Failed(error) => Some(error),
            _ => None,
        }
    }
}

/// Trait for task execution.
///
/// Implementations can execute tasks locally, via cloud functions,
/// or through any other execution mechanism.
#[async_trait]
pub trait Runner: Send + Sync {
    /// Executes a task and returns the result.
    ///
    /// The implementation should:
    /// 1. Set up the execution environment
    /// 2. Execute the asset computation
    /// 3. Capture outputs and metrics
    /// 4. Return success with output or failure with error
    async fn run(&self, context: &RunContext, task: &TaskSpec) -> TaskResult;
}

/// A no-op runner for testing that immediately succeeds.
#[derive(Debug, Default)]
pub struct NoOpRunner;

#[async_trait]
impl Runner for NoOpRunner {
    async fn run(&self, _context: &RunContext, _task: &TaskSpec) -> TaskResult {
        TaskResult::Succeeded(TaskOutput {
            materialization_id: arco_core::MaterializationId::generate(),
            files: vec![],
            row_count: 0,
            byte_size: 0,
        })
    }
}

/// A runner that always fails with a configurable error.
#[derive(Debug)]
pub struct FailingRunner {
    error: TaskError,
}

impl FailingRunner {
    /// Creates a new failing runner with the given error.
    #[must_use]
    pub fn new(error: TaskError) -> Self {
        Self { error }
    }
}

#[async_trait]
impl Runner for FailingRunner {
    async fn run(&self, _context: &RunContext, _task: &TaskSpec) -> TaskResult {
        TaskResult::Failed(self.error.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{AssetKey, ResourceRequirements, TaskSpec};
    use crate::task::TaskErrorCategory;
    use arco_core::{AssetId, MaterializationId, TaskId};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct CountingRunner {
        count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Runner for CountingRunner {
        async fn run(&self, _context: &RunContext, _task: &TaskSpec) -> TaskResult {
            self.count.fetch_add(1, Ordering::SeqCst);
            TaskResult::Succeeded(TaskOutput {
                materialization_id: MaterializationId::generate(),
                files: vec![],
                row_count: 100,
                byte_size: 1024,
            })
        }
    }

    #[tokio::test]
    async fn runner_executes_task() {
        let count = Arc::new(AtomicUsize::new(0));
        let runner = CountingRunner {
            count: count.clone(),
        };

        let context = RunContext {
            tenant_id: "tenant".into(),
            workspace_id: "workspace".into(),
            run_id: arco_core::RunId::generate(),
        };

        let task = TaskSpec {
            task_id: TaskId::generate(),
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("raw", "events"),
            partition_key: None,
            upstream_task_ids: vec![],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        };

        let result = runner.run(&context, &task).await;

        assert!(matches!(result, TaskResult::Succeeded(_)));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn noop_runner_succeeds() {
        let runner = NoOpRunner;

        let context = RunContext {
            tenant_id: "tenant".into(),
            workspace_id: "workspace".into(),
            run_id: arco_core::RunId::generate(),
        };

        let task = TaskSpec {
            task_id: TaskId::generate(),
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("raw", "events"),
            partition_key: None,
            upstream_task_ids: vec![],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        };

        let result = runner.run(&context, &task).await;
        assert!(result.is_success());
    }

    #[tokio::test]
    async fn failing_runner_fails() {
        let error = TaskError::new(TaskErrorCategory::UserCode, "expected failure");
        let runner = FailingRunner::new(error);

        let context = RunContext {
            tenant_id: "tenant".into(),
            workspace_id: "workspace".into(),
            run_id: arco_core::RunId::generate(),
        };

        let task = TaskSpec {
            task_id: TaskId::generate(),
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("raw", "events"),
            partition_key: None,
            upstream_task_ids: vec![],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        };

        let result = runner.run(&context, &task).await;
        assert!(result.error().is_some());
    }
}
```

**Step 4: Update lib.rs to export runner module**

Add to `crates/arco-flow/src/lib.rs`:

```rust
pub mod runner;
```

And update prelude:

```rust
pub mod prelude {
    pub use crate::error::{Error, Result};
    pub use crate::plan::{Plan, PlanBuilder, TaskSpec};
    pub use crate::run::{Run, RunState};
    pub use crate::runner::{Runner, RunContext, TaskResult};
    pub use crate::scheduler::Scheduler;
    pub use crate::task::{TaskExecution, TaskState};
}
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p arco-flow`

Expected: All tests PASS

**Step 6: Commit**

```bash
git add crates/arco-flow/src/runner.rs crates/arco-flow/src/lib.rs
git commit -m "feat(flow): add Runner trait for task execution

Defines Runner trait for pluggable task execution, RunContext for
execution scope, TaskResult for outcomes. Includes NoOpRunner and
FailingRunner implementations for testing."
```

---

## Task 9: Add Execution Events for Tier 2 Persistence

**Files:**
- Create: `crates/arco-flow/src/events.rs`
- Modify: `crates/arco-flow/src/lib.rs`

**Design Note:** Events use a CloudEvents-compatible envelope wrapper for interoperability
with event streaming systems (Pub/Sub, Kafka, etc.). The envelope provides standard
attributes (id, source, type, specversion, time) while the data field contains the
typed execution event payload.

**Step 1: Write the failing test for events**

Create test in `crates/arco-flow/src/events.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::{RunId, TaskId};

    #[test]
    fn event_envelope_has_cloudevents_attributes() {
        let run_id = RunId::generate();
        let envelope = EventEnvelope::new(
            "tenant",
            "workspace",
            ExecutionEventData::RunStarted {
                run_id: run_id.clone(),
                plan_id: "plan-123".into(),
            },
        );

        // CloudEvents required attributes
        assert!(!envelope.id.is_empty());
        assert_eq!(envelope.specversion, "1.0");
        assert!(envelope.source.contains("arco"));
        assert!(envelope.event_type.starts_with("arco.flow."));
        assert!(envelope.time.is_some());

        // Arco-specific attributes
        assert_eq!(envelope.tenant_id, "tenant");
        assert_eq!(envelope.workspace_id, "workspace");
    }

    #[test]
    fn event_envelope_serializes_cloudevents_format() {
        let run_id = RunId::generate();
        let envelope = EventEnvelope::new(
            "tenant",
            "workspace",
            ExecutionEventData::RunStarted {
                run_id: run_id.clone(),
                plan_id: "plan-123".into(),
            },
        );

        let json = serde_json::to_string(&envelope).unwrap();

        // CloudEvents attributes are present
        assert!(json.contains("\"specversion\":\"1.0\""));
        assert!(json.contains("\"type\":\"arco.flow.run_started\""));
        assert!(json.contains("\"source\":"));
        assert!(json.contains("\"id\":"));
        assert!(json.contains("\"data\":"));
    }

    #[test]
    fn run_started_event_serializes() {
        let envelope = EventEnvelope::new(
            "tenant",
            "workspace",
            ExecutionEventData::RunStarted {
                run_id: RunId::generate(),
                plan_id: "plan-123".into(),
            },
        );

        let json = serde_json::to_string(&envelope).unwrap();
        assert!(json.contains("run_started"));
    }

    #[test]
    fn task_completed_event_serializes() {
        let envelope = EventEnvelope::new(
            "tenant",
            "workspace",
            ExecutionEventData::TaskCompleted {
                run_id: RunId::generate(),
                task_id: TaskId::generate(),
                state: TaskState::Succeeded,
                attempt: 1,
            },
        );

        let json = serde_json::to_string(&envelope).unwrap();
        assert!(json.contains("task_completed"));
    }

    #[test]
    fn event_idempotency_key_is_deterministic() {
        let run_id = RunId::generate();
        let task_id = TaskId::generate();

        let envelope1 = EventEnvelope::with_idempotency_key(
            "tenant",
            "workspace",
            ExecutionEventData::TaskCompleted {
                run_id: run_id.clone(),
                task_id: task_id.clone(),
                state: TaskState::Succeeded,
                attempt: 1,
            },
            format!("task-completed-{}-{}-1", run_id, task_id),
        );

        let envelope2 = EventEnvelope::with_idempotency_key(
            "tenant",
            "workspace",
            ExecutionEventData::TaskCompleted {
                run_id: run_id.clone(),
                task_id: task_id.clone(),
                state: TaskState::Succeeded,
                attempt: 1,
            },
            format!("task-completed-{}-{}-1", run_id, task_id),
        );

        // Same idempotency key for same logical event
        assert_eq!(envelope1.idempotency_key, envelope2.idempotency_key);
        // But different envelope IDs (each is unique)
        assert_ne!(envelope1.id, envelope2.id);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-flow event_envelope_has_cloudevents_attributes`

Expected: FAIL with "cannot find type `EventEnvelope`"

**Step 3: Write complete implementation**

Create `crates/arco-flow/src/events.rs`:

```rust
//! Execution events for Tier 2 persistence.
//!
//! This module provides CloudEvents-compatible event envelopes for run and task
//! state changes. Events are appended to the Tier 2 event log for eventual
//! consistency processing.
//!
//! ## CloudEvents Compatibility
//!
//! Event envelopes conform to the [CloudEvents v1.0 specification](https://cloudevents.io/):
//! - `id`: Unique event identifier (ULID)
//! - `source`: Event origin URI (`/arco/flow/{tenant}/{workspace}`)
//! - `specversion`: CloudEvents spec version ("1.0")
//! - `type`: Event type (`arco.flow.{event_name}`)
//! - `time`: Event timestamp (ISO 8601)
//! - `datacontenttype`: Content type of data ("application/json")
//! - `data`: The actual event payload
//!
//! ## Idempotency
//!
//! Events include an `idempotency_key` for deduplication. For the same logical
//! event (e.g., task completion), use a deterministic key derived from the
//! event's identity (run_id, task_id, attempt). Different envelope instances
//! with the same idempotency key represent the same logical event.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use arco_core::{MaterializationId, RunId, TaskId};

use crate::run::RunState;
use crate::task::{TaskError, TaskMetrics, TaskState};

/// CloudEvents-compatible event envelope for execution events.
///
/// Wraps execution event data with CloudEvents standard attributes for
/// interoperability with event streaming systems.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventEnvelope {
    // --- CloudEvents Required Attributes ---

    /// Unique event identifier (ULID).
    pub id: String,

    /// Event origin URI.
    /// Format: `/arco/flow/{tenant_id}/{workspace_id}`
    pub source: String,

    /// CloudEvents specification version.
    pub specversion: String,

    /// Event type.
    /// Format: `arco.flow.{event_name}` (e.g., `arco.flow.run_started`)
    #[serde(rename = "type")]
    pub event_type: String,

    // --- CloudEvents Optional Attributes ---

    /// Event timestamp (ISO 8601).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<DateTime<Utc>>,

    /// Content type of data field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datacontenttype: Option<String>,

    // --- Arco Extension Attributes ---

    /// Tenant scope.
    pub tenant_id: String,

    /// Workspace scope.
    pub workspace_id: String,

    /// Idempotency key for deduplication.
    /// Use a deterministic key for the same logical event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,

    // --- Event Data ---

    /// Event payload.
    pub data: ExecutionEventData,
}

impl EventEnvelope {
    /// Creates a new event envelope with auto-generated ID and timestamp.
    #[must_use]
    pub fn new(tenant_id: impl Into<String>, workspace_id: impl Into<String>, data: ExecutionEventData) -> Self {
        let tenant = tenant_id.into();
        let workspace = workspace_id.into();

        Self {
            id: Ulid::new().to_string(),
            source: format!("/arco/flow/{}/{}", tenant, workspace),
            specversion: "1.0".into(),
            event_type: format!("arco.flow.{}", data.event_name()),
            time: Some(Utc::now()),
            datacontenttype: Some("application/json".into()),
            tenant_id: tenant,
            workspace_id: workspace,
            idempotency_key: None,
            data,
        }
    }

    /// Creates a new event envelope with a custom idempotency key.
    #[must_use]
    pub fn with_idempotency_key(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        data: ExecutionEventData,
        idempotency_key: impl Into<String>,
    ) -> Self {
        let mut envelope = Self::new(tenant_id, workspace_id, data);
        envelope.idempotency_key = Some(idempotency_key.into());
        envelope
    }

    /// Returns the run ID associated with this event (if any).
    #[must_use]
    pub fn run_id(&self) -> Option<&RunId> {
        self.data.run_id()
    }

    /// Returns the task ID associated with this event (if any).
    #[must_use]
    pub fn task_id(&self) -> Option<&TaskId> {
        self.data.task_id()
    }
}

/// Execution event data payloads.
///
/// This enum contains the actual event data, wrapped by `EventEnvelope`
/// for CloudEvents compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum ExecutionEventData {
    /// A run has been created.
    RunCreated {
        /// Run identifier.
        run_id: RunId,
        /// Plan identifier.
        plan_id: String,
        /// Trigger type.
        trigger_type: String,
        /// Triggered by (user or schedule).
        #[serde(skip_serializing_if = "Option::is_none")]
        triggered_by: Option<String>,
    },

    /// A run has started executing.
    RunStarted {
        /// Run identifier.
        run_id: RunId,
        /// Plan identifier.
        plan_id: String,
    },

    /// A run has completed (succeeded, failed, or cancelled).
    RunCompleted {
        /// Run identifier.
        run_id: RunId,
        /// Final run state.
        state: RunState,
        /// Tasks succeeded count.
        tasks_succeeded: usize,
        /// Tasks failed count.
        tasks_failed: usize,
        /// Tasks skipped count.
        tasks_skipped: usize,
        /// Tasks cancelled count.
        tasks_cancelled: usize,
    },

    /// A task has been queued for execution.
    TaskQueued {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Attempt number.
        attempt: u32,
    },

    /// A task has been dispatched to a worker.
    TaskDispatched {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Attempt number.
        attempt: u32,
        /// Worker identifier.
        worker_id: String,
    },

    /// A task has started executing (worker acknowledged).
    TaskStarted {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Attempt number.
        attempt: u32,
        /// Worker identifier.
        worker_id: String,
    },

    /// A task has completed (succeeded, failed, skipped, or cancelled).
    TaskCompleted {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Final task state.
        state: TaskState,
        /// Attempt number.
        attempt: u32,
    },

    /// A task has produced output (on success).
    TaskOutput {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Materialization identifier.
        materialization_id: MaterializationId,
        /// Row count produced.
        row_count: i64,
        /// Bytes produced.
        byte_size: i64,
    },

    /// A task has failed with an error.
    TaskFailed {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Error information.
        error: TaskError,
        /// Attempt number.
        attempt: u32,
    },

    /// A task has been retried.
    TaskRetried {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Previous attempt number.
        previous_attempt: u32,
        /// New attempt number.
        new_attempt: u32,
    },

    /// Task heartbeat received.
    TaskHeartbeat {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Attempt number.
        attempt: u32,
    },

    /// Task execution metrics have been recorded.
    TaskMetricsRecorded {
        /// Run identifier.
        run_id: RunId,
        /// Task identifier.
        task_id: TaskId,
        /// Execution metrics.
        metrics: TaskMetrics,
    },
}

impl ExecutionEventData {
    /// Returns the event name (snake_case) for the CloudEvents type field.
    #[must_use]
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::RunCreated { .. } => "run_created",
            Self::RunStarted { .. } => "run_started",
            Self::RunCompleted { .. } => "run_completed",
            Self::TaskQueued { .. } => "task_queued",
            Self::TaskDispatched { .. } => "task_dispatched",
            Self::TaskStarted { .. } => "task_started",
            Self::TaskCompleted { .. } => "task_completed",
            Self::TaskOutput { .. } => "task_output",
            Self::TaskFailed { .. } => "task_failed",
            Self::TaskRetried { .. } => "task_retried",
            Self::TaskHeartbeat { .. } => "task_heartbeat",
            Self::TaskMetricsRecorded { .. } => "task_metrics_recorded",
        }
    }

    /// Returns the run ID associated with this event.
    #[must_use]
    pub fn run_id(&self) -> Option<&RunId> {
        match self {
            Self::RunCreated { run_id, .. }
            | Self::RunStarted { run_id, .. }
            | Self::RunCompleted { run_id, .. }
            | Self::TaskQueued { run_id, .. }
            | Self::TaskDispatched { run_id, .. }
            | Self::TaskStarted { run_id, .. }
            | Self::TaskCompleted { run_id, .. }
            | Self::TaskOutput { run_id, .. }
            | Self::TaskFailed { run_id, .. }
            | Self::TaskRetried { run_id, .. }
            | Self::TaskHeartbeat { run_id, .. }
            | Self::TaskMetricsRecorded { run_id, .. } => Some(run_id),
        }
    }

    /// Returns the task ID associated with this event (if any).
    #[must_use]
    pub fn task_id(&self) -> Option<&TaskId> {
        match self {
            Self::TaskQueued { task_id, .. }
            | Self::TaskDispatched { task_id, .. }
            | Self::TaskStarted { task_id, .. }
            | Self::TaskCompleted { task_id, .. }
            | Self::TaskOutput { task_id, .. }
            | Self::TaskFailed { task_id, .. }
            | Self::TaskRetried { task_id, .. }
            | Self::TaskHeartbeat { task_id, .. }
            | Self::TaskMetricsRecorded { task_id, .. } => Some(task_id),
            Self::RunCreated { .. } | Self::RunStarted { .. } | Self::RunCompleted { .. } => None,
        }
    }
}

/// Builder for creating event envelopes from run state.
pub struct EventBuilder;

impl EventBuilder {
    /// Creates a RunCreated event envelope.
    #[must_use]
    pub fn run_created(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        plan_id: impl Into<String>,
        trigger_type: impl Into<String>,
        triggered_by: Option<String>,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::RunCreated {
                run_id,
                plan_id: plan_id.into(),
                trigger_type: trigger_type.into(),
                triggered_by,
            },
        )
    }

    /// Creates a RunStarted event envelope.
    #[must_use]
    pub fn run_started(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        plan_id: impl Into<String>,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::RunStarted {
                run_id,
                plan_id: plan_id.into(),
            },
        )
    }

    /// Creates a RunCompleted event envelope.
    #[must_use]
    pub fn run_completed(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        state: RunState,
        tasks_succeeded: usize,
        tasks_failed: usize,
        tasks_skipped: usize,
        tasks_cancelled: usize,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::RunCompleted {
                run_id,
                state,
                tasks_succeeded,
                tasks_failed,
                tasks_skipped,
                tasks_cancelled,
            },
        )
    }

    /// Creates a TaskQueued event envelope.
    #[must_use]
    pub fn task_queued(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        attempt: u32,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskQueued {
                run_id,
                task_id,
                attempt,
            },
        )
    }

    /// Creates a TaskDispatched event envelope.
    #[must_use]
    pub fn task_dispatched(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        attempt: u32,
        worker_id: impl Into<String>,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskDispatched {
                run_id,
                task_id,
                attempt,
                worker_id: worker_id.into(),
            },
        )
    }

    /// Creates a TaskStarted event envelope.
    #[must_use]
    pub fn task_started(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        attempt: u32,
        worker_id: impl Into<String>,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskStarted {
                run_id,
                task_id,
                attempt,
                worker_id: worker_id.into(),
            },
        )
    }

    /// Creates a TaskCompleted event envelope.
    #[must_use]
    pub fn task_completed(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        state: TaskState,
        attempt: u32,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskCompleted {
                run_id,
                task_id,
                state,
                attempt,
            },
        )
    }

    /// Creates a TaskFailed event envelope.
    #[must_use]
    pub fn task_failed(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        error: TaskError,
        attempt: u32,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskFailed {
                run_id,
                task_id,
                error,
                attempt,
            },
        )
    }

    /// Creates a TaskOutput event envelope.
    #[must_use]
    pub fn task_output(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        materialization_id: MaterializationId,
        row_count: i64,
        byte_size: i64,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskOutput {
                run_id,
                task_id,
                materialization_id,
                row_count,
                byte_size,
            },
        )
    }

    /// Creates a TaskHeartbeat event envelope.
    #[must_use]
    pub fn task_heartbeat(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        run_id: RunId,
        task_id: TaskId,
        attempt: u32,
    ) -> EventEnvelope {
        EventEnvelope::new(
            tenant_id,
            workspace_id,
            ExecutionEventData::TaskHeartbeat {
                run_id,
                task_id,
                attempt,
            },
        )
    }
}

// Note: Step 1 tests (TDD tests) are shown above. The following tests verify
// the implementation is complete.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_started_envelope_serializes() {
        let envelope = EventBuilder::run_started(
            "tenant",
            "workspace",
            RunId::generate(),
            "plan-123",
        );

        let json = serde_json::to_string(&envelope).unwrap();

        // CloudEvents envelope attributes
        assert!(json.contains("\"specversion\":\"1.0\""));
        assert!(json.contains("\"type\":\"arco.flow.run_started\""));
        assert!(json.contains("/arco/flow/tenant/workspace"));

        // Data payload
        assert!(json.contains("run_started"));
    }

    #[test]
    fn task_completed_envelope_serializes() {
        let envelope = EventBuilder::task_completed(
            "tenant",
            "workspace",
            RunId::generate(),
            TaskId::generate(),
            TaskState::Succeeded,
            1,
        );

        let json = serde_json::to_string(&envelope).unwrap();

        assert!(json.contains("\"specversion\":\"1.0\""));
        assert!(json.contains("\"type\":\"arco.flow.task_completed\""));
        assert!(json.contains("\"SUCCEEDED\""));
    }

    #[test]
    fn event_builder_creates_valid_envelopes() {
        let run_id = RunId::generate();
        let task_id = TaskId::generate();

        let created = EventBuilder::run_created(
            "tenant",
            "workspace",
            run_id,
            "plan-1",
            "Manual",
            Some("user@example.com".into()),
        );
        assert_eq!(created.event_type, "arco.flow.run_created");
        assert_eq!(created.specversion, "1.0");

        let queued = EventBuilder::task_queued(
            "tenant",
            "workspace",
            run_id,
            task_id,
            1,
        );
        assert_eq!(queued.event_type, "arco.flow.task_queued");
        assert!(queued.id.len() == 26); // ULID length
    }

    #[test]
    fn envelopes_roundtrip_through_json() {
        let envelope = EventBuilder::run_completed(
            "tenant",
            "workspace",
            RunId::generate(),
            RunState::Succeeded,
            5,
            0,
            0,
            0,
        );

        let json = serde_json::to_string(&envelope).unwrap();
        let parsed: EventEnvelope = serde_json::from_str(&json).unwrap();

        assert_eq!(envelope.id, parsed.id);
        assert_eq!(envelope.event_type, parsed.event_type);
        assert_eq!(envelope.specversion, parsed.specversion);
    }

    #[test]
    fn event_data_extracts_ids() {
        let run_id = RunId::generate();
        let task_id = TaskId::generate();

        let data = ExecutionEventData::TaskStarted {
            run_id: run_id.clone(),
            task_id: task_id.clone(),
            attempt: 1,
            worker_id: "worker-1".into(),
        };

        assert_eq!(data.run_id(), &run_id);
        assert_eq!(data.task_id(), Some(&task_id));
    }

    #[test]
    fn heartbeat_event_creates_correctly() {
        let envelope = EventBuilder::task_heartbeat(
            "tenant",
            "workspace",
            RunId::generate(),
            TaskId::generate(),
            2,
        );

        assert_eq!(envelope.event_type, "arco.flow.task_heartbeat");
        matches!(envelope.data, ExecutionEventData::TaskHeartbeat { .. });
    }
}
```

**Step 4: Update lib.rs to export events module**

Add to `crates/arco-flow/src/lib.rs`:

```rust
pub mod events;
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p arco-flow`

Expected: All tests PASS

**Step 6: Commit**

```bash
git add crates/arco-flow/src/events.rs crates/arco-flow/src/lib.rs
git commit -m "feat(flow): add CloudEvents-compatible execution events

Implements EventEnvelope following CloudEvents v1.0 specification with
ULID-based event IDs, source URIs, and typed event data. Includes
ExecutionEventData enum with run and task lifecycle events (including
TaskDispatched, TaskHeartbeat for observability), plus EventBuilder
for ergonomic envelope creation. Events are designed for append-only
Tier 2 event log with idempotency key support for deduplication."
```

---

## Task 10: Integration Tests

**Files:**
- Create: `crates/arco-flow/tests/integration.rs`

**Step 1: Write integration test**

Create `crates/arco-flow/tests/integration.rs`:

```rust
//! Integration tests for arco-flow orchestration.

use arco_core::{AssetId, TaskId};
use arco_flow::events::EventBuilder;
use arco_flow::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
use arco_flow::run::{Run, RunState, RunTrigger};
use arco_flow::runner::{NoOpRunner, RunContext, Runner};
use arco_flow::scheduler::Scheduler;
use arco_flow::task::TaskState;

/// Test full orchestration lifecycle: plan -> run -> execute -> complete.
#[tokio::test]
async fn full_orchestration_lifecycle() {
    // Build a simple DAG: raw -> staging -> mart
    let task_raw = TaskId::generate();
    let task_staging = TaskId::generate();
    let task_mart = TaskId::generate();

    let plan = PlanBuilder::new("acme-corp", "production")
        .add_task(TaskSpec {
            task_id: task_raw,
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("raw", "events"),
            partition_key: None,
            upstream_task_ids: vec![],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .add_task(TaskSpec {
            task_id: task_staging,
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("staging", "events_cleaned"),
            partition_key: None,
            upstream_task_ids: vec![task_raw],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .add_task(TaskSpec {
            task_id: task_mart,
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("mart", "daily_summary"),
            partition_key: None,
            upstream_task_ids: vec![task_staging],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .build()
        .expect("plan should be valid");

    // Verify plan is topologically sorted
    let pos_raw = plan.tasks.iter().position(|t| t.task_id == task_raw).unwrap();
    let pos_staging = plan.tasks.iter().position(|t| t.task_id == task_staging).unwrap();
    let pos_mart = plan.tasks.iter().position(|t| t.task_id == task_mart).unwrap();
    assert!(pos_raw < pos_staging);
    assert!(pos_staging < pos_mart);

    // Create scheduler and run
    let scheduler = Scheduler::new(plan.clone());
    let mut run = scheduler.create_run(RunTrigger::manual("test@example.com"));

    // Start the run
    scheduler.start_run(&mut run).unwrap();
    assert_eq!(run.state, RunState::Running);

    // Create runner and context
    let runner = NoOpRunner;
    let context = RunContext {
        tenant_id: run.tenant_id.clone(),
        workspace_id: run.workspace_id.clone(),
        run_id: run.id,
    };

    // Execute tasks in dependency order
    let mut events = Vec::new();

    // Phase 1: raw task should be ready
    let ready = scheduler.get_ready_tasks(&run);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].task_id, task_raw);

    // Execute raw task
    scheduler.queue_task(&mut run, &task_raw).unwrap();
    events.push(EventBuilder::task_queued(&run.tenant_id, &run.workspace_id, run.id, task_raw, 1));

    scheduler.start_task(&mut run, &task_raw, "worker-1").unwrap();
    events.push(EventBuilder::task_started(&run.tenant_id, &run.workspace_id, run.id, task_raw, 1, "worker-1"));

    let result = runner.run(&context, &plan.get_task(&task_raw).unwrap()).await;
    assert!(result.is_success());

    run.get_task_mut(&task_raw)
        .unwrap()
        .succeed(result.output().unwrap().clone())
        .unwrap();
    events.push(EventBuilder::task_completed(&run.tenant_id, &run.workspace_id, run.id, task_raw, TaskState::Succeeded, 1));

    // Phase 2: staging task should now be ready
    let ready = scheduler.get_ready_tasks(&run);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].task_id, task_staging);

    // Execute staging task
    scheduler.queue_task(&mut run, &task_staging).unwrap();
    scheduler.start_task(&mut run, &task_staging, "worker-2").unwrap();
    let result = runner.run(&context, &plan.get_task(&task_staging).unwrap()).await;
    run.get_task_mut(&task_staging)
        .unwrap()
        .succeed(result.output().unwrap().clone())
        .unwrap();

    // Phase 3: mart task should now be ready
    let ready = scheduler.get_ready_tasks(&run);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].task_id, task_mart);

    // Execute mart task
    scheduler.queue_task(&mut run, &task_mart).unwrap();
    scheduler.start_task(&mut run, &task_mart, "worker-3").unwrap();
    let result = runner.run(&context, &plan.get_task(&task_mart).unwrap()).await;
    run.get_task_mut(&task_mart)
        .unwrap()
        .succeed(result.output().unwrap().clone())
        .unwrap();

    // Complete the run
    let final_state = scheduler.check_run_completion(&run).unwrap();
    assert_eq!(final_state, RunState::Succeeded);

    scheduler.complete_run(&mut run, final_state).unwrap();
    assert_eq!(run.state, RunState::Succeeded);
    assert_eq!(run.tasks_succeeded(), 3);

    // Add completion event
    events.push(EventBuilder::run_completed(
        &run.tenant_id,
        &run.workspace_id,
        run.id,
        run.state,
        run.tasks_succeeded(),
        run.tasks_failed(),
        run.tasks_skipped(),
        run.tasks_cancelled(),
    ));

    // Verify events were created
    assert!(!events.is_empty());
}

/// Test that downstream tasks are skipped when upstream fails.
#[tokio::test]
async fn skip_downstream_on_failure() {
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
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .add_task(TaskSpec {
            task_id: task_c,
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("mart", "c"),
            partition_key: None,
            upstream_task_ids: vec![task_b],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .build()
        .unwrap();

    let scheduler = Scheduler::new(plan);
    let mut run = scheduler.create_run(RunTrigger::manual("user"));

    scheduler.start_run(&mut run).unwrap();

    // Fail task_a
    scheduler.queue_task(&mut run, &task_a).unwrap();
    scheduler.start_task(&mut run, &task_a, "worker-1").unwrap();

    run.get_task_mut(&task_a)
        .unwrap()
        .fail(arco_flow::task::TaskError::new(
            arco_flow::task::TaskErrorCategory::UserCode,
            "test failure",
        ))
        .unwrap();

    // Process the failure - should cascade skips
    scheduler.process_task_completion(&mut run, &task_a);

    // Verify downstream tasks are skipped
    assert_eq!(run.get_task(&task_b).unwrap().state, TaskState::Skipped);
    assert_eq!(run.get_task(&task_c).unwrap().state, TaskState::Skipped);

    // Run should complete as failed
    let final_state = scheduler.check_run_completion(&run).unwrap();
    assert_eq!(final_state, RunState::Failed);
}

/// Test parallel execution of independent tasks.
#[tokio::test]
async fn parallel_independent_tasks() {
    let task_a = TaskId::generate();
    let task_b = TaskId::generate();
    let task_c = TaskId::generate();

    // DAG: a and b are independent, c depends on both
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
            asset_key: AssetKey::new("raw", "b"),
            partition_key: None,
            upstream_task_ids: vec![],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .add_task(TaskSpec {
            task_id: task_c,
            asset_id: AssetId::generate(),
            asset_key: AssetKey::new("mart", "c"),
            partition_key: None,
            upstream_task_ids: vec![task_a, task_b],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .build()
        .unwrap();

    let scheduler = Scheduler::new(plan);
    let run = scheduler.create_run(RunTrigger::manual("user"));

    // Both a and b should be ready initially
    let ready = scheduler.get_ready_tasks(&run);
    assert_eq!(ready.len(), 2);

    let ready_ids: Vec<_> = ready.iter().map(|t| t.task_id).collect();
    assert!(ready_ids.contains(&task_a));
    assert!(ready_ids.contains(&task_b));
}

/// Test plan fingerprint is stable.
#[test]
fn plan_fingerprint_stability() {
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

    // Same inputs should produce same fingerprint
    assert_eq!(plan1.fingerprint, plan2.fingerprint);
    // But different plan IDs (generated)
    assert_ne!(plan1.plan_id, plan2.plan_id);
}
```

**Step 2: Run integration tests**

Run: `cargo test -p arco-flow --test integration`

Expected: All tests PASS

**Step 3: Commit**

```bash
git add crates/arco-flow/tests/integration.rs
git commit -m "test(flow): add integration tests for orchestration lifecycle

Tests full DAG execution, failure cascading, parallel execution,
and plan fingerprint stability."
```

---

## Task 11: Run Full Test Suite and Verify

**Files:** None (verification only)

**Step 1: Run cargo check**

Run: `cargo check --workspace`

Expected: No errors

**Step 2: Run cargo clippy**

Run: `cargo clippy --workspace -- -D warnings`

Expected: No warnings

**Step 3: Run all tests**

Run: `cargo test --workspace`

Expected: All tests PASS

**Step 4: Run doc tests**

Run: `cargo test --doc -p arco-flow`

Expected: PASS

**Step 5: Commit if any fixes needed**

If clippy or tests revealed issues, fix them and commit:

```bash
git add -A
git commit -m "fix(flow): address clippy warnings and test failures"
```

---

## Summary

This plan implements Part 3 - Orchestration MVP with:

1. **Core Types** (Tasks 1-2): TaskId, MaterializationId, flow error types
2. **DAG Implementation** (Task 3): Generic DAG with petgraph, topological sort, cycle detection
3. **Plan & TaskSpec** (Task 4): Full proto alignment, fingerprinting, dependency validation
4. **Task Execution** (Task 5): State machine, TaskExecution, output/error tracking
5. **Run Management** (Task 6): Run lifecycle, state transitions, progress tracking
6. **Scheduler** (Task 7): Dependency-aware scheduling, priority, parallelism limits
7. **Runner Trait** (Task 8): Pluggable execution, test implementations
8. **Events** (Task 9): Tier 2 execution events for persistence
9. **Integration Tests** (Task 10): End-to-end lifecycle validation
10. **Verification** (Task 11): Full test suite, clippy, docs

Total: ~11 tasks with TDD approach (write test → run → implement → verify → commit)
