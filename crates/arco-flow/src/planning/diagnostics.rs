//! Planner diagnostics emitted during compilation.

use serde::{Deserialize, Serialize};

/// Severity for a planner diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanDiagnosticLevel {
    /// Informational diagnostic.
    Info,
    /// Warning diagnostic for compatibility fallbacks or degraded planning.
    Warning,
}

/// Diagnostic emitted while compiling a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDiagnostic {
    /// Stable machine-readable diagnostic code.
    pub code: String,
    /// Diagnostic severity.
    pub level: PlanDiagnosticLevel,
    /// Human-readable diagnostic message.
    pub message: String,
}

impl PlanDiagnostic {
    /// Creates an informational diagnostic.
    #[must_use]
    pub fn info(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            level: PlanDiagnosticLevel::Info,
            message: message.into(),
        }
    }

    /// Creates a warning diagnostic.
    #[must_use]
    pub fn warning(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            level: PlanDiagnosticLevel::Warning,
            message: message.into(),
        }
    }
}
