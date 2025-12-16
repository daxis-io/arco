//! Directed Acyclic Graph (DAG) for dependency management.
//!
//! This module provides a generic DAG implementation used for:
//! - Asset dependency graphs
//! - Task execution ordering
//! - Topological sorting for deterministic plan generation
//!
//! **Note:** This module is internal to `arco-flow` to preserve freedom to change internals.

use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::hash::Hash;

use petgraph::Direction;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;

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
pub struct Dag<T>
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
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            index_map: HashMap::new(),
            insertion_order: Vec::new(),
        }
    }

    /// Returns the number of nodes in the DAG.
    #[must_use]
    #[allow(dead_code)]
    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Returns the number of edges in the DAG.
    #[must_use]
    #[allow(dead_code)]
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    /// Adds a node to the DAG.
    ///
    /// If the node already exists, this is a no-op.
    /// Returns the node index for use with other methods.
    pub fn add_node(&mut self, value: T) -> NodeIndex {
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
    pub fn add_edge(&mut self, from: NodeIndex, to: NodeIndex) -> Result<()> {
        // Use node_weight() instead of indexing to avoid clippy::indexing_slicing
        self.graph
            .node_weight(from)
            .ok_or_else(|| Error::DagNodeNotFound {
                node: format!("index {}", from.index()),
            })?;
        self.graph
            .node_weight(to)
            .ok_or_else(|| Error::DagNodeNotFound {
                node: format!("index {}", to.index()),
            })?;

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
    pub fn toposort(&self) -> Result<Vec<T>> {
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
            // Find a node still with non-zero in-degree to report (deterministic).
            let cycle_node = self
                .insertion_order
                .iter()
                .find(|&&idx| in_degree.get(&idx).copied().unwrap_or(0) > 0)
                .and_then(|&idx| self.graph.node_weight(idx))
                .map_or_else(|| "unknown".to_string(), ToString::to_string);

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
    #[allow(dead_code)]
    pub fn upstream(&self, node: NodeIndex) -> Result<Vec<T>> {
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
    #[allow(dead_code)]
    pub fn downstream(&self, node: NodeIndex) -> Result<Vec<T>> {
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
    #[allow(dead_code)]
    pub fn roots(&self) -> Vec<T> {
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
    pub fn leaves(&self) -> Vec<T> {
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
    pub fn contains(&self, node: &T) -> bool {
        self.index_map.contains_key(node)
    }

    /// Returns the node index for a value, if it exists.
    #[must_use]
    #[allow(dead_code)]
    pub fn get_index(&self, value: &T) -> Option<NodeIndex> {
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
        assert!(matches!(result, Err(Error::CycleDetected { .. })));
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
        let _c = dag.add_node("c".into());
        let a = dag.add_node("a".into());
        let b = dag.add_node("b".into());
        dag.add_edge(a, _c).unwrap();
        dag.add_edge(b, _c).unwrap();

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
