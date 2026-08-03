//! Plan compiler implementations.

use std::error::Error as StdError;
use std::fmt;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::application::PlanningSnapshotToken;
use crate::orchestration::events::TaskDef;
use crate::planning::diagnostics::PlanDiagnostic;
use crate::planning::fingerprint::{CompilerVersion, LogicalTime, PlanFingerprint};
use crate::planning::selection::{
    AssetGraph, SelectionOptions, build_task_defs_for_selection, canonicalize_asset_key,
};

/// Compatibility run intent for orchestration run requests.
#[derive(Debug, Clone)]
pub struct RunIntent {
    /// Requested root asset selection.
    pub asset_selection: Vec<String>,
    /// Optional requested partition selection.
    pub partition_selection: Option<Vec<String>>,
    /// Selection closure options.
    pub selection_options: SelectionOptions,
}

impl RunIntent {
    /// Creates a run intent.
    #[must_use]
    pub fn new(
        asset_selection: Vec<String>,
        partition_selection: Option<Vec<String>>,
        selection_options: SelectionOptions,
    ) -> Self {
        Self {
            asset_selection,
            partition_selection,
            selection_options,
        }
    }

    /// Creates an intent that preserves the current run-bridge compatibility behavior.
    #[must_use]
    pub fn run_bridge_compatibility(
        asset_selection: Vec<String>,
        partition_selection: Option<Vec<String>>,
    ) -> Self {
        Self::new(
            asset_selection,
            partition_selection,
            SelectionOptions::none(),
        )
    }
}

/// Request to compile a run intent.
#[derive(Debug, Clone)]
pub struct CompileRequest {
    /// Correlation id for compile diagnostics and tracing.
    pub correlation_id: String,
    /// Run id for the compatibility event path.
    pub run_id: String,
    /// Plan id for the compatibility event path.
    pub plan_id: String,
    /// Planner-owned run intent.
    pub intent: RunIntent,
    /// Token naming the planning snapshot used for compilation.
    pub planning_snapshot_token: PlanningSnapshotToken,
    /// Explicit deterministic logical time for compilation.
    pub logical_time: LogicalTime,
}

/// Result of compiling a run intent.
#[derive(Debug, Clone)]
pub struct CompileResult {
    /// Compatibility task definitions for `PlanCreated`.
    pub tasks: Vec<TaskDef>,
    /// Diagnostics emitted during compilation.
    pub diagnostics: Vec<PlanDiagnostic>,
    /// Planning snapshot token used by the compiler.
    pub planning_snapshot_token: PlanningSnapshotToken,
    /// Deterministic fingerprint of the compiled plan.
    pub plan_fingerprint: PlanFingerprint,
    /// Compiler version that produced the result.
    pub compiler_version: CompilerVersion,
}

/// Compiler error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompileError {
    /// Error message.
    pub message: String,
}

impl CompileError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for CompileError {}

/// Planner-owned compiler from run intent to compatibility task definitions.
#[derive(Debug, Clone)]
pub struct PlanCompiler {
    compiler_version: CompilerVersion,
}

impl PlanCompiler {
    /// Creates a compiler with an explicit compiler version.
    #[must_use]
    pub fn new(compiler_version: CompilerVersion) -> Self {
        Self { compiler_version }
    }

    /// Creates the current run-bridge compatibility compiler.
    #[must_use]
    pub fn for_run_bridge_compatibility() -> Self {
        Self::new(CompilerVersion::run_bridge_compatibility())
    }

    /// Compiles a run intent.
    ///
    /// # Errors
    /// Returns an error if deterministic fingerprint material cannot be serialized.
    pub fn compile(&self, request: CompileRequest) -> Result<CompileResult, CompileError> {
        let CompileRequest {
            intent,
            planning_snapshot_token,
            logical_time,
            ..
        } = request;

        let normalized = normalize_run_bridge_intent(&intent);
        let mut diagnostics = normalized.diagnostics.clone();
        let tasks = compile_compatibility_tasks(&normalized, &mut diagnostics);
        let plan_fingerprint = compute_plan_fingerprint(
            &normalized,
            &planning_snapshot_token,
            &self.compiler_version,
            &logical_time,
            &tasks,
        )?;

        Ok(CompileResult {
            tasks,
            diagnostics,
            planning_snapshot_token,
            plan_fingerprint,
            compiler_version: self.compiler_version.clone(),
        })
    }
}

#[derive(Debug, Clone)]
struct NormalizedRunBridgeIntent {
    canonical_asset_selection: Vec<String>,
    invalid_asset_selection: Vec<String>,
    partition_selection: Option<Vec<String>>,
    partition_key: Option<String>,
    selection_options: SelectionOptions,
    diagnostics: Vec<PlanDiagnostic>,
}

fn normalize_run_bridge_intent(intent: &RunIntent) -> NormalizedRunBridgeIntent {
    let mut canonical_asset_selection = Vec::with_capacity(intent.asset_selection.len());
    let mut invalid_asset_selection = Vec::new();
    let mut diagnostics = Vec::new();

    for asset in &intent.asset_selection {
        match canonicalize_asset_key(asset) {
            Ok(canonical) => canonical_asset_selection.push(canonical),
            Err(error) => {
                invalid_asset_selection.push(asset.clone());
                diagnostics.push(PlanDiagnostic::warning(
                    "run_bridge_invalid_asset_selection",
                    format!("invalid run_bridge asset selection '{asset}': {error}"),
                ));
            }
        }
    }

    canonical_asset_selection.sort();
    canonical_asset_selection.dedup();
    invalid_asset_selection.sort();
    invalid_asset_selection.dedup();

    let partition_key = intent
        .partition_selection
        .as_deref()
        .and_then(|keys| match keys {
            [single] => Some(single.clone()),
            _ => None,
        });
    let partition_selection = intent.partition_selection.as_ref().map(|keys| {
        let mut normalized = keys.clone();
        normalized.sort();
        normalized.dedup();
        normalized
    });

    NormalizedRunBridgeIntent {
        canonical_asset_selection,
        invalid_asset_selection,
        partition_selection,
        partition_key,
        selection_options: intent.selection_options,
        diagnostics,
    }
}

fn compile_compatibility_tasks(
    normalized: &NormalizedRunBridgeIntent,
    diagnostics: &mut Vec<PlanDiagnostic>,
) -> Vec<TaskDef> {
    let mut graph = AssetGraph::new();
    for asset in &normalized.canonical_asset_selection {
        graph.insert_asset(asset.clone(), Vec::new());
    }

    if !normalized.canonical_asset_selection.is_empty() {
        match build_task_defs_for_selection(
            &graph,
            &normalized.canonical_asset_selection,
            normalized.selection_options,
            normalized.partition_key.as_deref(),
        ) {
            Ok(tasks) if !tasks.is_empty() => return tasks,
            Ok(_) => diagnostics.push(PlanDiagnostic::warning(
                "run_bridge_empty_task_selection",
                "run_bridge selection produced no compatibility tasks",
            )),
            Err(error) => diagnostics.push(PlanDiagnostic::warning(
                "run_bridge_selection_lowering_failed",
                format!("run_bridge selection lowering failed: {error}"),
            )),
        }
    }

    diagnostics.push(PlanDiagnostic::warning(
        "run_bridge_fallback_materialize_task",
        "run_bridge compatibility planning emitted fallback materialize task",
    ));
    vec![TaskDef {
        key: "materialize".to_string(),
        depends_on: Vec::new(),
        asset_key: None,
        partition_key: None,
        max_attempts: 3,
        heartbeat_timeout_sec: 300,
        requires_visible_output: false,
    }]
}

fn compute_plan_fingerprint(
    normalized: &NormalizedRunBridgeIntent,
    planning_snapshot_token: &PlanningSnapshotToken,
    compiler_version: &CompilerVersion,
    logical_time: &LogicalTime,
    tasks: &[TaskDef],
) -> Result<PlanFingerprint, CompileError> {
    #[derive(Serialize)]
    struct FingerprintPayload<'a> {
        intent: FingerprintIntent<'a>,
        planning_snapshot_token: &'a str,
        compiler_version: &'a str,
        logical_time: &'a str,
        tasks: Vec<FingerprintTask<'a>>,
    }

    #[derive(Serialize)]
    struct FingerprintIntent<'a> {
        asset_selection: &'a [String],
        invalid_asset_selection: &'a [String],
        partition_selection: &'a Option<Vec<String>>,
        include_upstream: bool,
        include_downstream: bool,
    }

    #[derive(Serialize)]
    struct FingerprintTask<'a> {
        key: &'a str,
        depends_on: Vec<&'a str>,
        asset_key: Option<&'a str>,
        partition_key: Option<&'a str>,
        max_attempts: u32,
        heartbeat_timeout_sec: u32,
        requires_visible_output: bool,
    }

    let mut fingerprint_tasks = tasks
        .iter()
        .map(|task| {
            let mut depends_on: Vec<&str> = task.depends_on.iter().map(String::as_str).collect();
            depends_on.sort_unstable();
            FingerprintTask {
                key: task.key.as_str(),
                depends_on,
                asset_key: task.asset_key.as_deref(),
                partition_key: task.partition_key.as_deref(),
                max_attempts: task.max_attempts,
                heartbeat_timeout_sec: task.heartbeat_timeout_sec,
                requires_visible_output: task.requires_visible_output,
            }
        })
        .collect::<Vec<_>>();
    fingerprint_tasks.sort_by(|left, right| left.key.cmp(right.key));

    let payload = FingerprintPayload {
        intent: FingerprintIntent {
            asset_selection: &normalized.canonical_asset_selection,
            invalid_asset_selection: &normalized.invalid_asset_selection,
            partition_selection: &normalized.partition_selection,
            include_upstream: normalized.selection_options.include_upstream,
            include_downstream: normalized.selection_options.include_downstream,
        },
        planning_snapshot_token: planning_snapshot_token.as_str(),
        compiler_version: compiler_version.as_str(),
        logical_time: logical_time.as_str(),
        tasks: fingerprint_tasks,
    };

    let bytes = serde_json::to_vec(&payload)
        .map_err(|error| CompileError::new(format!("serialize fingerprint material: {error}")))?;
    let hash = Sha256::digest(&bytes);
    Ok(PlanFingerprint::new(hex::encode(hash)))
}
