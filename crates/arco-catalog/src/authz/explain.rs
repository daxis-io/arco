//! Explain-access helpers.

use super::compiler::CompiledPermissionSet;
use super::decision::{AuthzDecision, AuthzRequest};

/// Explain-access response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessExplanation {
    /// Request ID.
    pub request_id: String,
    /// Enforcement decision.
    pub decision: AuthzDecision,
}

/// Evaluates and explains an access decision.
#[must_use]
pub fn explain_access(
    request: &AuthzRequest,
    compiled: &CompiledPermissionSet,
) -> AccessExplanation {
    AccessExplanation {
        request_id: request.request_id.clone(),
        decision: AuthzDecision::evaluate(request, compiled),
    }
}
