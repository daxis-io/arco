//! Selection semantics and manifest-driven task planning.

use std::collections::{BTreeSet, HashMap, VecDeque};

use serde::{Deserialize, Serialize};
use sha2::Digest;

use super::events::TaskDef;

/// Selection closure options.
///
/// Parity-critical invariant:
/// - Downstream assets MUST NOT be included unless explicitly requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct SelectionOptions {
    /// Include upstream dependencies of the selection.
    #[serde(default)]
    pub include_upstream: bool,
    /// Include downstream dependents of the selection.
    #[serde(default)]
    pub include_downstream: bool,
}

impl SelectionOptions {
    /// Selection options with no expansion.
    #[must_use]
    pub fn none() -> Self {
        Self {
            include_upstream: false,
            include_downstream: false,
        }
    }
}

/// Minimal asset dependency graph.
///
/// Keys are canonicalized asset keys (e.g. `analytics.users`).
#[derive(Debug, Clone, Default)]
pub struct AssetGraph {
    upstream: HashMap<String, Vec<String>>,
    downstream: HashMap<String, Vec<String>>,
}

impl AssetGraph {
    /// Creates an empty asset dependency graph.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts an asset and its upstream dependencies.
    pub fn insert_asset(&mut self, asset_key: String, upstream_deps: Vec<String>) {
        self.upstream
            .insert(asset_key.clone(), upstream_deps.clone());
        self.downstream.entry(asset_key.clone()).or_default();

        for upstream in upstream_deps {
            self.downstream
                .entry(upstream)
                .or_default()
                .push(asset_key.clone());
        }
    }

    /// Returns upstream dependencies for `asset_key`.
    #[must_use]
    pub fn upstream_of(&self, asset_key: &str) -> &[String] {
        self.upstream
            .get(asset_key)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Returns downstream dependents for `asset_key`.
    #[must_use]
    pub fn downstream_of(&self, asset_key: &str) -> &[String] {
        self.downstream
            .get(asset_key)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Computes the closure of a selection.
    ///
    /// Returned set is deterministic (sorted).
    pub fn close_selection(&self, roots: &[String], options: SelectionOptions) -> BTreeSet<String> {
        let mut selected: BTreeSet<String> = roots.iter().cloned().collect();

        if options.include_upstream {
            let mut queue: VecDeque<String> = roots.iter().cloned().collect();
            while let Some(current) = queue.pop_front() {
                for upstream in self.upstream_of(&current) {
                    if selected.insert(upstream.clone()) {
                        queue.push_back(upstream.clone());
                    }
                }
            }
        }

        if options.include_downstream {
            let mut queue: VecDeque<String> = roots.iter().cloned().collect();
            while let Some(current) = queue.pop_front() {
                for downstream in self.downstream_of(&current) {
                    if selected.insert(downstream.clone()) {
                        queue.push_back(downstream.clone());
                    }
                }
            }
        }

        selected
    }
}

/// Canonicalizes an asset key string.
///
/// Accepts either `namespace.name` or `namespace/name`.
pub fn canonicalize_asset_key(input: &str) -> Result<String, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("asset key cannot be empty".to_string());
    }
    if trimmed != input {
        return Err("asset key cannot have leading/trailing whitespace".to_string());
    }
    if trimmed.chars().any(char::is_whitespace) {
        return Err("asset key cannot contain whitespace".to_string());
    }

    let has_slash = trimmed.contains('/');
    let has_dot = trimmed.contains('.');

    let (namespace, name) = if has_slash {
        let parts: Vec<&str> = trimmed.split('/').collect();
        if parts.len() != 2 {
            return Err(format!(
                "invalid asset key '{trimmed}': expected 'namespace/name'"
            ));
        }
        (parts[0], parts[1])
    } else if has_dot {
        let mut parts = trimmed.splitn(2, '.');
        (
            parts.next().unwrap_or_default(),
            parts.next().unwrap_or_default(),
        )
    } else {
        return Err(format!(
            "invalid asset key '{trimmed}': expected 'namespace.name' or 'namespace/name'"
        ));
    };

    if namespace.is_empty() || name.is_empty() {
        return Err(format!(
            "invalid asset key '{trimmed}': namespace and name must be non-empty"
        ));
    }

    if !is_valid_asset_namespace(namespace) {
        return Err(format!(
            "invalid asset key '{trimmed}': invalid namespace '{namespace}'"
        ));
    }
    if !is_valid_asset_name(name) {
        return Err(format!(
            "invalid asset key '{trimmed}': invalid name '{name}'"
        ));
    }

    Ok(format!("{namespace}.{name}"))
}

fn is_valid_asset_namespace(namespace: &str) -> bool {
    is_valid_asset_segment(namespace)
}

fn is_valid_asset_name(name: &str) -> bool {
    let segments: Vec<&str> = name.split('.').collect();
    if segments.is_empty() || segments.iter().any(|segment| segment.is_empty()) {
        return false;
    }

    segments
        .iter()
        .all(|segment| is_valid_asset_segment(segment))
}

fn is_valid_asset_segment(segment: &str) -> bool {
    let mut chars = segment.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Builds `TaskDef`s for a selection against a graph.
///
/// Dependencies are included only when both sides are present in the final planned set.
pub fn build_task_defs_for_selection(
    graph: &AssetGraph,
    roots: &[String],
    options: SelectionOptions,
    partition_key: Option<String>,
) -> Result<Vec<TaskDef>, String> {
    let mut canonical_roots = Vec::with_capacity(roots.len());
    for root in roots {
        canonical_roots.push(canonicalize_asset_key(root)?);
    }

    let planned_assets = graph.close_selection(&canonical_roots, options);

    let mut tasks: Vec<TaskDef> = planned_assets
        .iter()
        .map(|asset_key| {
            let mut depends_on: Vec<String> = graph
                .upstream_of(asset_key)
                .iter()
                .filter(|upstream| planned_assets.contains(*upstream))
                .cloned()
                .collect();
            depends_on.sort();

            TaskDef {
                key: asset_key.clone(),
                depends_on,
                asset_key: Some(asset_key.clone()),
                partition_key: partition_key.clone(),
                max_attempts: 3,
                heartbeat_timeout_sec: 300,
            }
        })
        .collect();

    tasks.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(tasks)
}

/// Computes a deterministic fingerprint for selection parameters.
///
/// Intended for run_key payload consistency and idempotency.
pub fn compute_selection_fingerprint(
    selection: &[String],
    options: SelectionOptions,
) -> Result<String, String> {
    #[derive(Serialize)]
    struct Payload {
        selection: Vec<String>,
        include_upstream: bool,
        include_downstream: bool,
    }

    let mut canonical: Vec<String> = selection
        .iter()
        .map(|s| canonicalize_asset_key(s))
        .collect::<Result<_, _>>()?;
    canonical.sort();
    canonical.dedup();

    let payload = Payload {
        selection: canonical,
        include_upstream: options.include_upstream,
        include_downstream: options.include_downstream,
    };

    let json = serde_json::to_vec(&payload).map_err(|e| format!("serialize: {e}"))?;
    let hash = sha2::Sha256::digest(&json);
    Ok(hex::encode(hash))
}
