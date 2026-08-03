//! Application-level run planner.

use crate::application::planning_snapshot_provider::{
    CurrentInProcessPlanningSnapshotProvider, PlanningSnapshotProvider, PlanningSnapshotToken,
};
use crate::orchestration::events::TaskDef;
use crate::planning::{
    CompileError, CompileRequest, CompilerVersion, LogicalTime, PlanCompiler, PlanDiagnostic,
    PlanFingerprint, RunIntent,
};

/// Application service that plans run requests.
#[derive(Debug, Clone)]
pub struct RunPlanner<P = CurrentInProcessPlanningSnapshotProvider> {
    snapshot_provider: P,
    compiler: PlanCompiler,
}

impl Default for RunPlanner<CurrentInProcessPlanningSnapshotProvider> {
    fn default() -> Self {
        Self::new()
    }
}

impl RunPlanner<CurrentInProcessPlanningSnapshotProvider> {
    /// Creates a run planner with current in-process compatibility dependencies.
    #[must_use]
    pub fn new() -> Self {
        Self::with_snapshot_provider(CurrentInProcessPlanningSnapshotProvider)
    }
}

impl<P> RunPlanner<P>
where
    P: PlanningSnapshotProvider,
{
    /// Creates a run planner with an explicit snapshot provider.
    #[must_use]
    pub fn with_snapshot_provider(snapshot_provider: P) -> Self {
        Self {
            snapshot_provider,
            compiler: PlanCompiler::for_run_bridge_compatibility(),
        }
    }

    /// Plans a run request.
    ///
    /// # Errors
    /// Returns an error if the underlying compiler cannot produce a deterministic result.
    pub fn plan_run(&self, request: RunPlannerRequest) -> Result<PlannedRun, CompileError> {
        let intent = RunIntent::run_bridge_compatibility(
            request.asset_selection,
            request.partition_selection,
        );
        let snapshot = self.snapshot_provider.snapshot_for_intent(&intent);
        let result = self.compiler.compile(CompileRequest {
            correlation_id: request.correlation_id,
            run_id: request.run_id,
            plan_id: request.plan_id,
            intent,
            planning_snapshot_token: snapshot.token,
            logical_time: request.logical_time,
        })?;

        Ok(PlannedRun {
            tasks: result.tasks,
            diagnostics: result.diagnostics,
            planning_snapshot_token: result.planning_snapshot_token,
            plan_fingerprint: result.plan_fingerprint,
            compiler_version: result.compiler_version,
        })
    }
}

/// Request to plan a run.
#[derive(Debug, Clone)]
pub struct RunPlannerRequest {
    /// Correlation id for compile diagnostics and tracing.
    pub correlation_id: String,
    /// Run id for compatibility event emission.
    pub run_id: String,
    /// Plan id for compatibility event emission.
    pub plan_id: String,
    /// Requested root asset selection.
    pub asset_selection: Vec<String>,
    /// Optional requested partition selection.
    pub partition_selection: Option<Vec<String>>,
    /// Explicit deterministic logical time.
    pub logical_time: LogicalTime,
}

impl RunPlannerRequest {
    /// Creates a compatibility request for the run bridge path.
    #[must_use]
    pub fn run_bridge_compatibility(
        correlation_id: impl Into<String>,
        run_id: impl Into<String>,
        plan_id: impl Into<String>,
        asset_selection: Vec<String>,
        partition_selection: Option<Vec<String>>,
        logical_time: impl Into<String>,
    ) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            run_id: run_id.into(),
            plan_id: plan_id.into(),
            asset_selection,
            partition_selection,
            logical_time: LogicalTime::new(logical_time),
        }
    }
}

/// Planned run output for compatibility event emission.
#[derive(Debug, Clone)]
pub struct PlannedRun {
    /// Compatibility task definitions for `PlanCreated`.
    pub tasks: Vec<TaskDef>,
    /// Diagnostics emitted during planning.
    pub diagnostics: Vec<PlanDiagnostic>,
    /// Planning snapshot token used by the compiler.
    pub planning_snapshot_token: PlanningSnapshotToken,
    /// Deterministic compiled plan fingerprint.
    pub plan_fingerprint: PlanFingerprint,
    /// Compiler version that produced this plan.
    pub compiler_version: CompilerVersion,
}
