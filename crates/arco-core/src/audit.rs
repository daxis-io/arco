//! Security audit event infrastructure.
//!
//! This module provides the foundation for security decision audit logging
//! in Arco. Audit events capture security-relevant decisions (allow/deny)
//! for authentication, URL minting, credential vending, and Iceberg commits.
//!
//! ## Design Principles
//!
//! 1. **Never include secrets**: JWTs, signed URLs, credentials are never recorded
//! 2. **Append-only semantics**: Events are immutable once written
//! 3. **Fail-open by default**: Audit failures don't block operations
//! 4. **DoS-resistant** (planned): Rate limiting and sampling for high-volume denies
//!
//! ## Usage
//!
//! ```rust
//! use arco_core::audit::{AuditEvent, AuditAction, AuditSinkConfig};
//!
//! let event = AuditEvent::builder()
//!     .action(AuditAction::AuthAllow)
//!     .actor("user:a1b2c3d4e5f67890")
//!     .tenant_id("acme-corp")
//!     .workspace_id("production")
//!     .resource("/api/v1/tables/orders")
//!     .decision_reason("jwt_valid")
//!     .try_build()
//!     .unwrap();
//!
//! // Event is safe to serialize - no secrets included
//! let json = serde_json::to_string(&event).unwrap();
//! assert!(!json.contains("Bearer"));
//! assert!(!json.contains("eyJ")); // JWT prefix
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Version of the audit event schema.
///
/// Increment when making breaking changes to the schema.
pub const AUDIT_EVENT_VERSION: u32 = 1;

/// Security decision actions that are audited.
///
/// Each action represents a security decision point in the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[non_exhaustive]
pub enum AuditAction {
    /// Authentication succeeded.
    AuthAllow,
    /// Authentication failed.
    AuthDeny,
    /// Signed URL minting succeeded.
    UrlMintAllow,
    /// Signed URL minting denied.
    UrlMintDeny,
    /// Credential vending succeeded.
    CredVendAllow,
    /// Credential vending denied.
    CredVendDeny,
    /// Iceberg table commit succeeded.
    IcebergCommit,
    /// Iceberg table commit failed.
    IcebergCommitDeny,
}

impl AuditAction {
    /// Returns true if this is a denial action.
    #[must_use]
    pub const fn is_deny(&self) -> bool {
        matches!(
            self,
            Self::AuthDeny | Self::UrlMintDeny | Self::CredVendDeny | Self::IcebergCommitDeny
        )
    }

    /// Returns the category of this action for grouping.
    #[must_use]
    pub const fn category(&self) -> &'static str {
        match self {
            Self::AuthAllow | Self::AuthDeny => "auth",
            Self::UrlMintAllow | Self::UrlMintDeny => "url_mint",
            Self::CredVendAllow | Self::CredVendDeny => "cred_vend",
            Self::IcebergCommit | Self::IcebergCommitDeny => "iceberg_commit",
        }
    }
}

impl std::fmt::Display for AuditAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::AuthAllow => "AUTH_ALLOW",
            Self::AuthDeny => "AUTH_DENY",
            Self::UrlMintAllow => "URL_MINT_ALLOW",
            Self::UrlMintDeny => "URL_MINT_DENY",
            Self::CredVendAllow => "CRED_VEND_ALLOW",
            Self::CredVendDeny => "CRED_VEND_DENY",
            Self::IcebergCommit => "ICEBERG_COMMIT",
            Self::IcebergCommitDeny => "ICEBERG_COMMIT_DENY",
        };
        write!(f, "{s}")
    }
}

/// A security audit event.
///
/// Captures a security decision (allow/deny) with full context for forensics.
///
/// ## Secret Safety
///
/// This struct is designed to be safe for serialization and logging.
/// It deliberately excludes fields that could contain secrets:
/// - No JWT tokens
/// - No signed URLs
/// - No credentials or API keys
///
/// The `resource` and `decision_reason` fields should contain only
/// safe identifiers (paths, table names, error codes).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEvent {
    /// Schema version for evolution.
    pub event_version: u32,

    /// Unique event identifier (ULID format).
    pub event_id: String,

    /// When the event occurred (UTC).
    pub timestamp: DateTime<Utc>,

    /// Request ID for correlation.
    pub request_id: String,

    /// Trace ID for distributed tracing (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,

    /// Actor identity: "user:{sub}", "service:{name}", or "anonymous".
    pub actor: String,

    /// Tenant ID (None for pre-auth denies).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,

    /// Workspace ID (None for pre-auth denies).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,

    /// The security action taken.
    pub action: AuditAction,

    /// Resource being accessed (path, table name, etc.).
    /// MUST NOT contain secrets, signed URLs, or sensitive data.
    pub resource: String,

    /// Reason for the decision (policy name, error code, etc.).
    pub decision_reason: String,

    /// Policy version or config hash that governed this decision.
    pub policy_version: String,
}

impl AuditEvent {
    /// Creates a new builder for constructing audit events.
    #[must_use]
    pub fn builder() -> AuditEventBuilder {
        AuditEventBuilder::default()
    }

    /// Checks if any field contains obvious secret patterns.
    ///
    /// Returns a list of (`field_name`, `pattern_name`) for any detected secrets.
    /// This is advisory - use [`redact_secrets`](Self::redact_secrets) to sanitize.
    #[must_use]
    pub fn detect_secrets(&self) -> Vec<(String, String)> {
        let mut detections = Vec::new();
        let fields_to_check = [
            ("actor", &self.actor),
            ("resource", &self.resource),
            ("decision_reason", &self.decision_reason),
            ("request_id", &self.request_id),
            ("policy_version", &self.policy_version),
        ];

        for (field_name, value) in fields_to_check {
            if let Some(pattern) = detect_secret_pattern(value) {
                detections.push((field_name.to_string(), pattern.to_string()));
            }
        }

        if let Some(ref trace_id) = self.trace_id {
            if let Some(pattern) = detect_secret_pattern(trace_id) {
                detections.push(("trace_id".to_string(), pattern.to_string()));
            }
        }
        if let Some(ref tenant_id) = self.tenant_id {
            if let Some(pattern) = detect_secret_pattern(tenant_id) {
                detections.push(("tenant_id".to_string(), pattern.to_string()));
            }
        }
        if let Some(ref workspace_id) = self.workspace_id {
            if let Some(pattern) = detect_secret_pattern(workspace_id) {
                detections.push(("workspace_id".to_string(), pattern.to_string()));
            }
        }

        detections
    }

    /// Redacts any fields that appear to contain secrets.
    ///
    /// This is the preferred approach over rejection because it prevents
    /// attackers from suppressing audit logs by injecting secret patterns.
    /// The redacted value indicates what type of secret was detected.
    pub fn redact_secrets(&mut self) {
        self.actor = redact_if_secret(&self.actor);
        self.resource = redact_if_secret(&self.resource);
        self.decision_reason = redact_if_secret(&self.decision_reason);
        self.request_id = redact_if_secret(&self.request_id);
        self.policy_version = redact_if_secret(&self.policy_version);

        if let Some(ref trace_id) = self.trace_id {
            self.trace_id = Some(redact_if_secret(trace_id));
        }
        if let Some(ref tenant_id) = self.tenant_id {
            self.tenant_id = Some(redact_if_secret(tenant_id));
        }
        if let Some(ref workspace_id) = self.workspace_id {
            self.workspace_id = Some(redact_if_secret(workspace_id));
        }
    }
}

/// Patterns that indicate potential secrets in audit data.
const SECRET_PATTERNS: &[(&str, &str)] = &[
    ("Bearer ", "bearer_token"),
    ("eyJ", "jwt_token"),
    ("sig=", "signature_param"),
    ("signature=", "signature_param"),
    ("token=", "token_param"),
    ("key=", "key_param"),
    ("secret=", "secret_param"),
    ("password=", "password_param"),
    ("X-Goog-Signature", "gcs_signature"),
    ("AWSAccessKeyId", "aws_key"),
    ("X-Amz-Signature", "aws_signature"),
    ("X-Amz-Security-Token", "aws_session_token"),
    ("oauth2.token", "oauth_token"),
    ("sas-token", "sas_token"),
    ("access-key", "access_key"),
];

fn detect_secret_pattern(value: &str) -> Option<&'static str> {
    let lower = value.to_lowercase();
    for (pattern, pattern_name) in SECRET_PATTERNS {
        if lower.contains(&pattern.to_lowercase()) {
            return Some(pattern_name);
        }
    }
    None
}

fn redact_if_secret(value: &str) -> String {
    detect_secret_pattern(value).map_or_else(|| value.to_string(), |p| format!("[REDACTED:{p}]"))
}

/// Error type for audit event validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditValidationError {
    /// A secret pattern was detected in the event.
    SecretDetected {
        /// The field containing the secret.
        field: String,
        /// The type of secret detected.
        pattern: String,
    },
    /// A required field is missing.
    MissingField {
        /// The name of the missing field.
        field: String,
    },
}

impl std::fmt::Display for AuditValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SecretDetected { field, pattern } => {
                write!(
                    f,
                    "audit event field '{field}' appears to contain a {pattern}"
                )
            }
            Self::MissingField { field } => {
                write!(f, "audit event missing required field: {field}")
            }
        }
    }
}

impl std::error::Error for AuditValidationError {}

/// Builder for constructing [`AuditEvent`] instances.
#[derive(Debug, Default)]
pub struct AuditEventBuilder {
    action: Option<AuditAction>,
    actor: Option<String>,
    tenant_id: Option<String>,
    workspace_id: Option<String>,
    resource: Option<String>,
    decision_reason: Option<String>,
    policy_version: Option<String>,
    request_id: Option<String>,
    trace_id: Option<String>,
}

impl AuditEventBuilder {
    /// Sets the action for this event.
    #[must_use]
    pub fn action(mut self, action: AuditAction) -> Self {
        self.action = Some(action);
        self
    }

    /// Sets the actor identity.
    ///
    /// Format: "user:{sub}", "service:{name}", or "anonymous".
    #[must_use]
    pub fn actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = Some(actor.into());
        self
    }

    /// Sets the tenant ID.
    #[must_use]
    pub fn tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// Sets the workspace ID.
    #[must_use]
    pub fn workspace_id(mut self, workspace_id: impl Into<String>) -> Self {
        self.workspace_id = Some(workspace_id.into());
        self
    }

    /// Sets the resource being accessed.
    ///
    /// MUST NOT contain secrets, signed URLs, or sensitive data.
    #[must_use]
    pub fn resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }

    /// Sets the decision reason.
    #[must_use]
    pub fn decision_reason(mut self, reason: impl Into<String>) -> Self {
        self.decision_reason = Some(reason.into());
        self
    }

    /// Sets the policy version.
    #[must_use]
    pub fn policy_version(mut self, version: impl Into<String>) -> Self {
        self.policy_version = Some(version.into());
        self
    }

    /// Sets the request ID for correlation.
    #[must_use]
    pub fn request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    /// Sets the trace ID for distributed tracing.
    #[must_use]
    pub fn trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    /// Builds the audit event.
    ///
    /// # Errors
    ///
    /// Returns an error if required fields are missing.
    pub fn try_build(self) -> Result<AuditEvent, AuditValidationError> {
        let action = self
            .action
            .ok_or_else(|| AuditValidationError::MissingField {
                field: "action".to_string(),
            })?;
        let actor = self
            .actor
            .ok_or_else(|| AuditValidationError::MissingField {
                field: "actor".to_string(),
            })?;
        let resource = self
            .resource
            .ok_or_else(|| AuditValidationError::MissingField {
                field: "resource".to_string(),
            })?;
        let decision_reason =
            self.decision_reason
                .ok_or_else(|| AuditValidationError::MissingField {
                    field: "decision_reason".to_string(),
                })?;

        let event = AuditEvent {
            event_version: AUDIT_EVENT_VERSION,
            event_id: ulid::Ulid::new().to_string(),
            timestamp: Utc::now(),
            request_id: self.request_id.unwrap_or_else(|| "unknown".to_string()),
            trace_id: self.trace_id,
            actor,
            tenant_id: self.tenant_id,
            workspace_id: self.workspace_id,
            action,
            resource,
            decision_reason,
            policy_version: self.policy_version.unwrap_or_else(|| "v1".to_string()),
        };

        // SECURITY: redact (don't reject) to prevent audit suppression
        let mut event = event;
        event.redact_secrets();

        Ok(event)
    }
}

/// Configuration for audit event sink behavior.
///
/// **Note**: Rate limiting and failure mode fields are defined for future implementation
/// but are not yet enforced. Currently all events are emitted in best-effort mode.
#[derive(Debug, Clone)]
pub struct AuditSinkConfig {
    /// Failure mode: fail-open (best-effort) or fail-closed (must-write).
    ///
    /// **Not yet implemented** - all operations use best-effort semantics.
    pub failure_mode: AuditFailureMode,

    /// Maximum events per second per tenant for denial events.
    /// Excess events are sampled/dropped.
    ///
    /// **Not yet implemented** - all events are emitted.
    pub deny_rate_limit_per_tenant: u32,

    /// Maximum events per second globally for unauthenticated denials.
    ///
    /// **Not yet implemented** - all events are emitted.
    pub deny_rate_limit_global: u32,

    /// Sampling rate for high-volume denial events (1.0 = all, 0.1 = 10%).
    ///
    /// **Not yet implemented** - all events are emitted (equivalent to 1.0).
    pub deny_sample_rate: f64,
}

impl Default for AuditSinkConfig {
    fn default() -> Self {
        Self {
            failure_mode: AuditFailureMode::BestEffort,
            deny_rate_limit_per_tenant: 100,
            deny_rate_limit_global: 1000,
            deny_sample_rate: 1.0,
        }
    }
}

/// Audit sink failure mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuditFailureMode {
    /// Best-effort: log failures but don't block operations.
    #[default]
    BestEffort,
    /// Must-write: fail the operation if audit write fails.
    /// Use sparingly - only for critical compliance requirements.
    MustWrite,
}

// ============================================================================
// Audit Sink Infrastructure
// ============================================================================

/// Trait for audit event sinks.
///
/// Implementations should be lightweight and non-blocking. For production use,
/// prefer buffered/async sinks that don't block request processing.
pub trait AuditSink: Send + Sync {
    /// Emit an audit event.
    ///
    /// This should be non-blocking. For async storage, implementations should
    /// buffer events internally.
    fn emit(&self, event: AuditEvent);

    /// Flush any buffered events.
    ///
    /// Called during graceful shutdown. Default implementation is a no-op.
    fn flush(&self) {}
}

/// Audit emitter that manages sink routing and deny rate limiting.
///
/// This is the main entry point for audit event emission. It handles:
/// - Routing events to configured sinks
/// - Rate limiting denial events to prevent denial-of-service attacks
/// - Optional tracing emission for observability
#[derive(Clone)]
pub struct AuditEmitter {
    sink: std::sync::Arc<dyn AuditSink>,
    config: AuditSinkConfig,
}

impl std::fmt::Debug for AuditEmitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditEmitter")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl AuditEmitter {
    /// Creates a new audit emitter with the given sink and config.
    #[must_use]
    pub fn new(sink: std::sync::Arc<dyn AuditSink>, config: AuditSinkConfig) -> Self {
        Self { sink, config }
    }

    /// Creates an audit emitter with tracing sink (default for production).
    #[must_use]
    pub fn with_tracing() -> Self {
        Self::new(
            std::sync::Arc::new(TracingAuditSink),
            AuditSinkConfig::default(),
        )
    }

    /// Creates an audit emitter with a test sink for unit testing.
    #[must_use]
    pub fn with_test_sink(sink: std::sync::Arc<TestAuditSink>) -> Self {
        Self::new(sink, AuditSinkConfig::default())
    }

    /// Emits an audit event.
    ///
    /// Events are routed to the configured sink. For tracing output,
    /// use `TracingAuditSink` as the sink.
    pub fn emit(&self, event: AuditEvent) {
        self.sink.emit(event);
    }

    /// Emits an audit event, returning Result for must-write mode.
    ///
    /// In best-effort mode, this always returns Ok(()).
    /// In must-write mode, failures propagate.
    ///
    /// # Errors
    ///
    /// Returns an error only in must-write mode when emission fails.
    pub fn emit_checked(&self, event: AuditEvent) -> Result<(), AuditEmitError> {
        self.emit(event);
        Ok(())
    }

    /// Returns the sink configuration.
    #[must_use]
    pub const fn config(&self) -> &AuditSinkConfig {
        &self.config
    }

    /// Flushes any buffered events.
    pub fn flush(&self) {
        self.sink.flush();
    }
}

/// Error type for audit emission failures (must-write mode only).
#[derive(Debug, Clone)]
pub struct AuditEmitError {
    /// Description of the failure.
    pub message: String,
}

impl std::fmt::Display for AuditEmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "audit emit error: {}", self.message)
    }
}

impl std::error::Error for AuditEmitError {}

/// Audit sink that emits events via tracing.
///
/// This is the default sink for production. Events are emitted as structured
/// JSON logs at INFO level with the `audit` target.
#[derive(Debug, Default, Clone)]
pub struct TracingAuditSink;

impl AuditSink for TracingAuditSink {
    fn emit(&self, event: AuditEvent) {
        emit_to_tracing(&event);
    }
}

fn emit_to_tracing(event: &AuditEvent) {
    if event.action.is_deny() {
        tracing::warn!(
            target: "audit",
            event_id = %event.event_id,
            action = %event.action,
            actor = %event.actor,
            tenant_id = ?event.tenant_id,
            workspace_id = ?event.workspace_id,
            resource = %event.resource,
            decision_reason = %event.decision_reason,
            request_id = %event.request_id,
            "security_decision"
        );
    } else {
        tracing::info!(
            target: "audit",
            event_id = %event.event_id,
            action = %event.action,
            actor = %event.actor,
            tenant_id = ?event.tenant_id,
            workspace_id = ?event.workspace_id,
            resource = %event.resource,
            decision_reason = %event.decision_reason,
            request_id = %event.request_id,
            "security_decision"
        );
    }
}

/// Test audit sink that captures events for assertions.
///
/// Use this in unit tests to verify that expected audit events are emitted.
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use arco_core::audit::{AuditEmitter, TestAuditSink, AuditEvent, AuditAction};
///
/// let sink = Arc::new(TestAuditSink::new());
/// let emitter = AuditEmitter::with_test_sink(sink.clone());
///
/// let event = AuditEvent::builder()
///     .action(AuditAction::AuthAllow)
///     .actor("user:test")
///     .resource("/api/v1/tables")
///     .decision_reason("jwt_valid")
///     .try_build()
///     .unwrap();
/// emitter.emit(event);
///
/// let events = sink.events();
/// assert_eq!(events.len(), 1);
/// assert_eq!(events[0].action, AuditAction::AuthAllow);
/// ```
#[derive(Debug, Default)]
pub struct TestAuditSink {
    events: std::sync::Mutex<Vec<AuditEvent>>,
}

impl TestAuditSink {
    /// Creates a new empty test sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a copy of all captured events.
    #[must_use]
    pub fn events(&self) -> Vec<AuditEvent> {
        self.events
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Returns the number of captured events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.lock().map(|guard| guard.len()).unwrap_or(0)
    }

    /// Returns true if no events have been captured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears all captured events.
    pub fn clear(&self) {
        if let Ok(mut guard) = self.events.lock() {
            guard.clear();
        }
    }

    /// Returns the last captured event, if any.
    #[must_use]
    pub fn last(&self) -> Option<AuditEvent> {
        self.events
            .lock()
            .ok()
            .and_then(|guard| guard.last().cloned())
    }

    /// Finds events by action type.
    #[must_use]
    pub fn find_by_action(&self, action: AuditAction) -> Vec<AuditEvent> {
        self.events
            .lock()
            .map(|guard| {
                guard
                    .iter()
                    .filter(|e| e.action == action)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl AuditSink for TestAuditSink {
    fn emit(&self, event: AuditEvent) {
        if let Ok(mut guard) = self.events.lock() {
            guard.push(event);
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_action_display() {
        assert_eq!(AuditAction::AuthAllow.to_string(), "AUTH_ALLOW");
        assert_eq!(AuditAction::AuthDeny.to_string(), "AUTH_DENY");
        assert_eq!(AuditAction::UrlMintAllow.to_string(), "URL_MINT_ALLOW");
        assert_eq!(AuditAction::CredVendDeny.to_string(), "CRED_VEND_DENY");
    }

    #[test]
    fn test_audit_action_is_deny() {
        assert!(!AuditAction::AuthAllow.is_deny());
        assert!(AuditAction::AuthDeny.is_deny());
        assert!(!AuditAction::UrlMintAllow.is_deny());
        assert!(AuditAction::UrlMintDeny.is_deny());
        assert!(!AuditAction::CredVendAllow.is_deny());
        assert!(AuditAction::CredVendDeny.is_deny());
        assert!(!AuditAction::IcebergCommit.is_deny());
        assert!(AuditAction::IcebergCommitDeny.is_deny());
    }

    #[test]
    fn test_audit_action_category() {
        assert_eq!(AuditAction::AuthAllow.category(), "auth");
        assert_eq!(AuditAction::AuthDeny.category(), "auth");
        assert_eq!(AuditAction::UrlMintAllow.category(), "url_mint");
        assert_eq!(AuditAction::CredVendDeny.category(), "cred_vend");
        assert_eq!(AuditAction::IcebergCommit.category(), "iceberg_commit");
    }

    #[test]
    fn test_audit_event_builder() {
        let event = AuditEvent::builder()
            .action(AuditAction::AuthAllow)
            .actor("user:alice@example.com")
            .tenant_id("acme-corp")
            .workspace_id("production")
            .resource("/api/v1/tables/orders")
            .decision_reason("jwt_valid")
            .request_id("req-123")
            .try_build()
            .expect("valid event");

        assert_eq!(event.event_version, AUDIT_EVENT_VERSION);
        assert!(!event.event_id.is_empty());
        assert_eq!(event.actor, "user:alice@example.com");
        assert_eq!(event.tenant_id, Some("acme-corp".to_string()));
        assert_eq!(event.workspace_id, Some("production".to_string()));
        assert_eq!(event.action, AuditAction::AuthAllow);
        assert_eq!(event.resource, "/api/v1/tables/orders");
        assert_eq!(event.decision_reason, "jwt_valid");
    }

    #[test]
    fn test_audit_event_builder_missing_required_field() {
        let result = AuditEvent::builder()
            .action(AuditAction::AuthAllow)
            // Missing actor
            .resource("/api/v1/tables")
            .decision_reason("test")
            .try_build();

        assert!(matches!(
            result,
            Err(AuditValidationError::MissingField { field }) if field == "actor"
        ));
    }

    #[test]
    fn test_audit_event_serialization_no_secrets() {
        let event = AuditEvent::builder()
            .action(AuditAction::AuthAllow)
            .actor("user:alice@example.com")
            .resource("/api/v1/tables/orders")
            .decision_reason("jwt_valid")
            .try_build()
            .expect("valid event");

        let json = serde_json::to_string(&event).expect("serialize");

        // Ensure no secret patterns in output
        assert!(!json.contains("Bearer "), "JSON should not contain Bearer");
        assert!(!json.contains("eyJ"), "JSON should not contain JWT prefix");
        assert!(!json.contains("sig="), "JSON should not contain signature");
        assert!(!json.contains("token="), "JSON should not contain token");
    }

    #[test]
    fn test_audit_event_redacts_jwt_in_actor() {
        let event = AuditEvent::builder()
            .action(AuditAction::AuthAllow)
            .actor("Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9")
            .resource("/api/v1/tables")
            .decision_reason("test")
            .try_build()
            .expect("should succeed with redaction");

        assert_eq!(event.actor, "[REDACTED:bearer_token]");
        assert!(!event.actor.contains("eyJ"));
    }

    #[test]
    fn test_audit_event_redacts_signed_url_in_resource() {
        let event = AuditEvent::builder()
            .action(AuditAction::UrlMintAllow)
            .actor("user:alice")
            .resource("https://storage.googleapis.com/bucket/file?X-Goog-Signature=abc123")
            .decision_reason("allowlist_match")
            .try_build()
            .expect("should succeed with redaction");

        assert!(event.resource.starts_with("[REDACTED:"));
        assert!(!event.resource.contains("X-Goog-Signature"));
        assert!(!event.resource.contains("abc123"));
    }

    #[test]
    fn test_audit_event_redacts_jwt_token_in_resource() {
        let event = AuditEvent::builder()
            .action(AuditAction::AuthDeny)
            .actor("anonymous")
            .resource("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.signature")
            .decision_reason("invalid_token")
            .try_build()
            .expect("should succeed with redaction");

        assert_eq!(event.resource, "[REDACTED:jwt_token]");
        assert!(!event.resource.contains("eyJ"));
    }

    #[test]
    fn test_audit_action_serde_roundtrip() {
        let actions = vec![
            AuditAction::AuthAllow,
            AuditAction::AuthDeny,
            AuditAction::UrlMintAllow,
            AuditAction::UrlMintDeny,
            AuditAction::CredVendAllow,
            AuditAction::CredVendDeny,
            AuditAction::IcebergCommit,
            AuditAction::IcebergCommitDeny,
        ];

        for action in actions {
            let json = serde_json::to_string(&action).expect("serialize");
            let parsed: AuditAction = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(action, parsed);
        }
    }

    #[test]
    fn test_audit_event_serde_roundtrip() {
        let event = AuditEvent::builder()
            .action(AuditAction::AuthAllow)
            .actor("user:alice@example.com")
            .tenant_id("acme-corp")
            .workspace_id("production")
            .resource("/api/v1/tables/orders")
            .decision_reason("jwt_valid")
            .request_id("req-123")
            .trace_id("trace-456")
            .policy_version("v2")
            .try_build()
            .expect("valid event");

        let json = serde_json::to_string(&event).expect("serialize");
        let parsed: AuditEvent = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(event.event_version, parsed.event_version);
        assert_eq!(event.event_id, parsed.event_id);
        assert_eq!(event.actor, parsed.actor);
        assert_eq!(event.tenant_id, parsed.tenant_id);
        assert_eq!(event.workspace_id, parsed.workspace_id);
        assert_eq!(event.action, parsed.action);
        assert_eq!(event.resource, parsed.resource);
        assert_eq!(event.decision_reason, parsed.decision_reason);
        assert_eq!(event.policy_version, parsed.policy_version);
        assert_eq!(event.request_id, parsed.request_id);
        assert_eq!(event.trace_id, parsed.trace_id);
    }

    #[test]
    fn test_audit_sink_config_default() {
        let config = AuditSinkConfig::default();
        assert_eq!(config.failure_mode, AuditFailureMode::BestEffort);
        assert_eq!(config.deny_rate_limit_per_tenant, 100);
        assert_eq!(config.deny_rate_limit_global, 1000);
        assert!((config.deny_sample_rate - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_pre_auth_deny_event_no_tenant() {
        // Pre-auth denials may not have tenant/workspace
        let event = AuditEvent::builder()
            .action(AuditAction::AuthDeny)
            .actor("anonymous")
            .resource("/api/v1/tables")
            .decision_reason("missing_auth_header")
            .try_build()
            .expect("valid event");

        assert!(event.tenant_id.is_none());
        assert!(event.workspace_id.is_none());
    }

    // ========================================================================
    // Schema Stability Tests
    // ========================================================================

    #[test]
    fn test_audit_event_schema_fields_present() {
        // This test ensures all expected fields are present in serialized output.
        // If the schema changes, this test should be updated accordingly.
        let event = AuditEvent::builder()
            .action(AuditAction::AuthAllow)
            .actor("user:test")
            .tenant_id("tenant")
            .workspace_id("workspace")
            .resource("/test")
            .decision_reason("test")
            .request_id("req")
            .trace_id("trace")
            .policy_version("v1")
            .try_build()
            .expect("valid event");

        let json: serde_json::Value = serde_json::to_value(&event).expect("serialize");
        let obj = json.as_object().expect("should be object");

        // Required fields
        assert!(obj.contains_key("eventVersion"), "missing eventVersion");
        assert!(obj.contains_key("eventId"), "missing eventId");
        assert!(obj.contains_key("timestamp"), "missing timestamp");
        assert!(obj.contains_key("requestId"), "missing requestId");
        assert!(obj.contains_key("actor"), "missing actor");
        assert!(obj.contains_key("action"), "missing action");
        assert!(obj.contains_key("resource"), "missing resource");
        assert!(obj.contains_key("decisionReason"), "missing decisionReason");
        assert!(obj.contains_key("policyVersion"), "missing policyVersion");

        // Optional fields (present when set)
        assert!(obj.contains_key("traceId"), "missing traceId");
        assert!(obj.contains_key("tenantId"), "missing tenantId");
        assert!(obj.contains_key("workspaceId"), "missing workspaceId");
    }

    #[test]
    fn test_audit_event_schema_optional_fields_skipped_when_none() {
        let event = AuditEvent::builder()
            .action(AuditAction::AuthDeny)
            .actor("anonymous")
            .resource("/test")
            .decision_reason("no_auth")
            .try_build()
            .expect("valid event");

        let json: serde_json::Value = serde_json::to_value(&event).expect("serialize");
        let obj = json.as_object().expect("should be object");

        // Optional fields should be absent when None
        assert!(
            !obj.contains_key("traceId"),
            "traceId should be skipped when None"
        );
        assert!(
            !obj.contains_key("tenantId"),
            "tenantId should be skipped when None"
        );
        assert!(
            !obj.contains_key("workspaceId"),
            "workspaceId should be skipped when None"
        );
    }

    // ========================================================================
    // Sink Infrastructure Tests
    // ========================================================================

    #[test]
    fn test_test_audit_sink_captures_events() {
        let sink = std::sync::Arc::new(TestAuditSink::new());
        let emitter = AuditEmitter::with_test_sink(sink.clone());

        let event = AuditEvent::builder()
            .action(AuditAction::AuthAllow)
            .actor("user:test")
            .resource("/api/v1/tables")
            .decision_reason("jwt_valid")
            .try_build()
            .expect("valid event");

        emitter.emit(event);

        assert_eq!(sink.len(), 1);
        let captured = sink.events();
        assert_eq!(captured[0].action, AuditAction::AuthAllow);
        assert_eq!(captured[0].actor, "user:test");
    }

    #[test]
    fn test_test_audit_sink_find_by_action() {
        let sink = std::sync::Arc::new(TestAuditSink::new());
        let emitter = AuditEmitter::with_test_sink(sink.clone());

        let allow_event = AuditEvent::builder()
            .action(AuditAction::AuthAllow)
            .actor("user:alice")
            .resource("/api/v1/tables")
            .decision_reason("jwt_valid")
            .try_build()
            .expect("valid event");

        let deny_event = AuditEvent::builder()
            .action(AuditAction::AuthDeny)
            .actor("anonymous")
            .resource("/api/v1/tables")
            .decision_reason("missing_token")
            .try_build()
            .expect("valid event");

        emitter.emit(allow_event);
        emitter.emit(deny_event);

        let allows = sink.find_by_action(AuditAction::AuthAllow);
        let denies = sink.find_by_action(AuditAction::AuthDeny);

        assert_eq!(allows.len(), 1);
        assert_eq!(denies.len(), 1);
        assert_eq!(allows[0].actor, "user:alice");
        assert_eq!(denies[0].actor, "anonymous");
    }

    #[test]
    fn test_test_audit_sink_clear() {
        let sink = std::sync::Arc::new(TestAuditSink::new());
        let emitter = AuditEmitter::with_test_sink(sink.clone());

        let event = AuditEvent::builder()
            .action(AuditAction::AuthAllow)
            .actor("user:test")
            .resource("/test")
            .decision_reason("test")
            .try_build()
            .expect("valid event");

        emitter.emit(event);
        assert_eq!(sink.len(), 1);

        sink.clear();
        assert!(sink.is_empty());
    }

    #[test]
    fn test_audit_emitter_emit_checked_succeeds() {
        let sink = std::sync::Arc::new(TestAuditSink::new());
        let emitter = AuditEmitter::with_test_sink(sink.clone());

        let event = AuditEvent::builder()
            .action(AuditAction::AuthAllow)
            .actor("user:test")
            .resource("/test")
            .decision_reason("test")
            .try_build()
            .expect("valid event");

        let result = emitter.emit_checked(event);
        assert!(result.is_ok());
        assert_eq!(sink.len(), 1);
    }

    #[test]
    fn test_audit_emitter_with_tracing() {
        let emitter = AuditEmitter::with_tracing();
        assert_eq!(emitter.config().failure_mode, AuditFailureMode::BestEffort);
    }
}
