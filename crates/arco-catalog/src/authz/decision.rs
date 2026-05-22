//! Authorization decision evaluation.

use super::compiler::CompiledPermissionSet;
use super::privileges::Privilege;

/// Authorization outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionOutcome {
    /// Request is allowed.
    Allow,
    /// Request is denied.
    Deny,
}

/// Authorization request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzRequest {
    /// Stable principal ID.
    pub principal_id: String,
    /// Stable object ID.
    pub object_id: String,
    /// Object type.
    pub object_type: String,
    /// Requested privilege.
    pub privilege: Privilege,
    /// Request ID for audit correlation.
    pub request_id: String,
}

impl AuthzRequest {
    /// Creates an authorization request.
    #[must_use]
    pub fn new(
        principal_id: impl Into<String>,
        object_id: impl Into<String>,
        object_type: impl Into<String>,
        privilege: Privilege,
    ) -> Self {
        Self {
            principal_id: principal_id.into(),
            object_id: object_id.into(),
            object_type: object_type.into(),
            privilege,
            request_id: String::new(),
        }
    }

    /// Sets the request ID.
    #[must_use]
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = request_id.into();
        self
    }
}

/// Explainable evidence used by an authorization decision.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthzEvidence {
    /// Grant IDs that contributed to the decision.
    pub grant_ids: Vec<String>,
    /// Source object IDs that contributed to the decision.
    pub source_object_ids: Vec<String>,
    /// Source principals that contributed to the decision.
    pub source_principal_ids: Vec<String>,
}

/// Authorization decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzDecision {
    /// Decision outcome.
    pub outcome: DecisionOutcome,
    /// Stable safe reason code.
    pub reason_code: String,
    /// Safe human-readable reason.
    pub reason_message_safe: String,
    /// Evidence allowed in explain-access output.
    pub evidence: AuthzEvidence,
    /// Group snapshot version used by the compiled view.
    pub group_snapshot_version: Option<String>,
}

impl AuthzDecision {
    /// Evaluates an authorization request against a compiled permission set.
    #[must_use]
    pub fn evaluate(request: &AuthzRequest, compiled: &CompiledPermissionSet) -> Self {
        if !compiled.fresh {
            return deny(
                "stale_projection",
                "compiled permissions are stale or unavailable",
                compiled,
            );
        }
        if !known_object_type(&request.object_type) {
            return deny(
                "unknown_object_type",
                "object type is not supported",
                compiled,
            );
        }

        let matching_rows = compiled
            .rows_for_principal_object_privilege(
                &request.principal_id,
                &request.object_id,
                &request.object_type,
                request.privilege,
            )
            .collect::<Vec<_>>();
        if matching_rows.is_empty() {
            return deny(
                "absence_of_allow",
                "no compiled allow matched the request",
                compiled,
            );
        }

        let mut evidence = AuthzEvidence::default();
        for row in matching_rows {
            if let Some(grant_id) = &row.source_grant_id {
                evidence.grant_ids.push(grant_id.clone());
            }
            evidence
                .source_object_ids
                .push(row.source_object_id.clone());
            evidence
                .source_principal_ids
                .push(row.source_principal_id.clone());
        }
        evidence.grant_ids.sort();
        evidence.grant_ids.dedup();
        evidence.source_object_ids.sort();
        evidence.source_object_ids.dedup();
        evidence.source_principal_ids.sort();
        evidence.source_principal_ids.dedup();

        Self {
            outcome: DecisionOutcome::Allow,
            reason_code: "direct_or_inherited_grant".to_string(),
            reason_message_safe: "compiled allow matched the request".to_string(),
            evidence,
            group_snapshot_version: Some(compiled.group_snapshot_version.clone()),
        }
    }
}

fn deny(reason_code: &str, message: &str, compiled: &CompiledPermissionSet) -> AuthzDecision {
    AuthzDecision {
        outcome: DecisionOutcome::Deny,
        reason_code: reason_code.to_string(),
        reason_message_safe: message.to_string(),
        evidence: AuthzEvidence::default(),
        group_snapshot_version: Some(compiled.group_snapshot_version.clone()),
    }
}

fn known_object_type(object_type: &str) -> bool {
    matches!(
        object_type.to_ascii_uppercase().as_str(),
        "METASTORE"
            | "CATALOG"
            | "SCHEMA"
            | "TABLE"
            | "VIEW"
            | "VOLUME"
            | "STORAGE_CREDENTIAL"
            | "EXTERNAL_LOCATION"
            | "MANAGED_ROOT"
            | "FUNCTION"
            | "REGISTERED_MODEL"
            | "MODEL_VERSION"
            | "SYSTEM_TABLE"
    )
}
