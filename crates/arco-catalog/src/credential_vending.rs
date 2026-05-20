//! Credential vending decisions over authoritative storage governance state.

use std::time::Duration;

use chrono::Utc;
use ulid::Ulid;

use crate::error::Result;
use crate::metastore::events::LifecycleState;
use crate::storage_governance::{PathAuthorityKind, StorageGovernanceState};

/// Default requested credential TTL when the client omits one.
pub const DEFAULT_CREDENTIAL_TTL: Duration = Duration::from_secs(3600);

/// Credential vending operation requested by a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialOperation {
    /// Read data under a governed path.
    Read,
    /// Write data under a governed path.
    Write,
    /// List data under a governed path.
    List,
    /// Manage data under a governed path.
    Manage,
    /// Delete data under a governed path.
    Delete,
}

impl CredentialOperation {
    /// Parses UC-compatible path operation strings.
    #[must_use]
    pub fn from_uc_operation(value: &str) -> Option<Self> {
        match value {
            value
                if value.eq_ignore_ascii_case("READ")
                    || value.eq_ignore_ascii_case("PATH_READ") =>
            {
                Some(Self::Read)
            }
            value
                if value.eq_ignore_ascii_case("WRITE")
                    || value.eq_ignore_ascii_case("PATH_WRITE") =>
            {
                Some(Self::Write)
            }
            value if value.eq_ignore_ascii_case("LIST") => Some(Self::List),
            value if value.eq_ignore_ascii_case("MANAGE") => Some(Self::Manage),
            value if value.eq_ignore_ascii_case("DELETE") => Some(Self::Delete),
            _ => None,
        }
    }
}

/// Credential vending decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialDecision {
    /// Request may receive a scoped temporary credential.
    Allow,
    /// Request must not receive credentials.
    Deny,
}

impl CredentialDecision {
    /// Stable string form used in API responses.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
        }
    }
}

/// Request to vend temporary credentials for a governed path.
#[derive(Debug, Clone)]
pub struct CredentialVendingRequest {
    /// Principal requesting credentials.
    pub principal_id: String,
    /// Group snapshot version used for authorization context.
    pub groups_snapshot_version: String,
    /// Workspace execution/access context.
    pub workspace_id: String,
    /// Request ID for audit correlation.
    pub request_id: String,
    /// Requested path operation.
    pub operation: CredentialOperation,
    /// Requested storage path.
    pub requested_path: String,
    /// Requested credential TTL.
    pub requested_ttl: Duration,
    /// Client kind, for example `uc`.
    pub client_kind: String,
    /// Catalog or metastore projection version used by the caller.
    pub catalog_snapshot_version: String,
}

/// Result of a credential vending decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialVendingDecision {
    /// Allow/deny decision.
    pub decision: CredentialDecision,
    /// Stable reason code.
    pub reason_code: String,
    /// Cloud provider for allow decisions.
    pub provider: Option<String>,
    /// Credential kind for allow decisions.
    pub credential_kind: Option<String>,
    /// Authorized storage-governance object ID.
    pub authorized_object_id: Option<String>,
    /// Authorized storage path prefixes.
    pub authorized_path_prefixes: Vec<String>,
    /// Maximum TTL allowed for the minted credential.
    pub max_ttl: Duration,
    /// Expiration time in milliseconds since epoch.
    pub expires_at: i64,
    /// Stable audit event ID for the decision.
    pub audit_event_id: String,
}

/// Credential vending policy engine.
#[derive(Debug, Clone)]
pub struct CredentialVendingEngine {
    max_ttl: Duration,
}

impl Default for CredentialVendingEngine {
    fn default() -> Self {
        Self {
            max_ttl: Duration::from_secs(3600),
        }
    }
}

impl CredentialVendingEngine {
    /// Decides whether to vend temporary credentials for a governed path.
    ///
    /// # Errors
    ///
    /// Returns an error if path normalization or storage-governance lookup
    /// encounters invalid authoritative state.
    pub fn decide_path(
        &self,
        state: &StorageGovernanceState,
        request: &CredentialVendingRequest,
    ) -> Result<CredentialVendingDecision> {
        if !supports_operation(request.operation) {
            return Ok(deny("unsupported_operation", request.requested_ttl));
        }

        let Ok(path_decision) =
            state.authority_for_path(&request.workspace_id, &request.requested_path)
        else {
            return Ok(deny("path_not_governed", request.requested_ttl));
        };

        let provider = match path_decision.authority_kind {
            PathAuthorityKind::ExternalLocation => {
                let Some(location) = state.get_external_location(&path_decision.object_id) else {
                    return Ok(deny("path_not_governed", request.requested_ttl));
                };
                let Some(credential) = state.get_storage_credential(&location.credential_id)?
                else {
                    return Ok(deny("storage_credential_not_found", request.requested_ttl));
                };
                if credential.lifecycle_state != LifecycleState::Active {
                    return Ok(deny("storage_credential_not_active", request.requested_ttl));
                }
                Some(credential.cloud)
            }
            PathAuthorityKind::ManagedRoot => None,
        };

        Ok(CredentialVendingDecision {
            decision: CredentialDecision::Allow,
            reason_code: "allowed".to_string(),
            provider,
            credential_kind: Some("scoped_bearer".to_string()),
            authorized_object_id: Some(path_decision.object_id),
            authorized_path_prefixes: vec![normalize_prefix(&request.requested_path)],
            max_ttl: request.requested_ttl.min(self.max_ttl),
            expires_at: expires_at_ms(request.requested_ttl.min(self.max_ttl)),
            audit_event_id: Ulid::new().to_string(),
        })
    }
}

fn supports_operation(operation: CredentialOperation) -> bool {
    matches!(
        operation,
        CredentialOperation::Read | CredentialOperation::Write | CredentialOperation::List
    )
}

fn deny(reason_code: &str, max_ttl: Duration) -> CredentialVendingDecision {
    CredentialVendingDecision {
        decision: CredentialDecision::Deny,
        reason_code: reason_code.to_string(),
        provider: None,
        credential_kind: None,
        authorized_object_id: None,
        authorized_path_prefixes: Vec::new(),
        max_ttl,
        expires_at: expires_at_ms(max_ttl),
        audit_event_id: Ulid::new().to_string(),
    }
}

fn expires_at_ms(ttl: Duration) -> i64 {
    let millis = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
    Utc::now().timestamp_millis().saturating_add(millis)
}

fn normalize_prefix(path: &str) -> String {
    if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}
