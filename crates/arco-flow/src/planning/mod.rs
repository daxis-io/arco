//! Planner-owned semantics for compiling run intents into executable task plans.

mod compiler;
mod diagnostics;
mod fingerprint;
pub mod selection;

pub use compiler::{CompileError, CompileRequest, CompileResult, PlanCompiler, RunIntent};
pub use diagnostics::{PlanDiagnostic, PlanDiagnosticLevel};
pub use fingerprint::{CompilerVersion, LogicalTime, PlanFingerprint};
pub use selection::{
    AssetGraph, SelectionOptions, build_task_defs_for_selection, canonicalize_asset_key,
    compute_selection_fingerprint,
};
