//! Audit event helpers for Iceberg REST API.
//!
//! Provides convenience functions for emitting audit events with context
//! from Iceberg request handlers.

use arco_core::audit::{AuditAction, AuditEmitter, AuditEvent};

use crate::context::IcebergRequestContext;
use crate::state::TableInfo;

// Credential vending reasons
/// Credential vending succeeded.
pub const REASON_CRED_VEND_SUCCESS: &str = "credentials_vended";
/// Credential vending denied: provider not configured.
pub const REASON_CRED_VEND_DISABLED: &str = "vending_disabled";
/// Credential vending denied: access denied (authz failure).
pub const REASON_CRED_VEND_ACCESS_DENIED: &str = "access_denied";
/// Credential vending denied: provider error (internal/unavailable).
pub const REASON_CRED_VEND_PROVIDER_ERROR: &str = "provider_error";
/// Credential vending denied: unsupported delegation mode.
pub const REASON_CRED_VEND_UNSUPPORTED_DELEGATION: &str = "unsupported_delegation";

// Commit reasons
/// Commit succeeded.
pub const REASON_COMMIT_SUCCESS: &str = "commit_success";
/// Commit denied: requirements not met.
pub const REASON_COMMIT_REQUIREMENTS_FAILED: &str = "requirements_failed";
/// Commit denied: CAS conflict.
pub const REASON_COMMIT_CAS_CONFLICT: &str = "cas_conflict";
/// Commit denied: internal error.
pub const REASON_COMMIT_INTERNAL_ERROR: &str = "internal_error";
/// Commit denied: another commit already in progress.
pub const REASON_COMMIT_IN_PROGRESS: &str = "commit_in_progress";
/// Commit denied: cached failure from previous attempt.
pub const REASON_COMMIT_CACHED_FAILURE: &str = "cached_failure";
/// Commit denied: multi-table transactions not supported.
pub const REASON_COMMIT_MULTI_TABLE: &str = "multi_table_unsupported";

/// Formats a table identifier for audit resource field.
///
/// Format: `iceberg/table:{namespace}.{table}:{table_id}`
/// This format ensures no secrets are included while providing enough
/// context for audit trail analysis.
fn format_table_resource(table: &TableInfo) -> String {
    let namespace = table.ident.namespace.join(".");
    format!(
        "iceberg/table:{}.{}:{}",
        namespace, table.ident.name, table.table_id
    )
}

/// Formats a commit resource for audit.
///
/// Format: `iceberg/commit:{namespace}.{table}` or `iceberg/commit:{namespace}.{table}:seq={n}`
fn format_commit_resource(namespace: &str, table: &str, sequence_number: Option<i64>) -> String {
    sequence_number.map_or_else(
        || format!("iceberg/commit:{namespace}.{table}"),
        |seq| format!("iceberg/commit:{namespace}.{table}:seq={seq}"),
    )
}

/// Emits a credential vending allow audit event.
///
/// # Arguments
///
/// * `emitter` - The audit emitter to use
/// * `ctx` - Request context with tenant, workspace, and `request_id`
/// * `table` - Table info for which credentials were vended
pub fn emit_cred_vend_allow(
    emitter: &AuditEmitter,
    ctx: &IcebergRequestContext,
    table: &TableInfo,
) {
    let resource = format_table_resource(table);

    if let Ok(event) = AuditEvent::builder()
        .action(AuditAction::CredVendAllow)
        .actor(format!("tenant:{}", ctx.tenant))
        .tenant_id(&ctx.tenant)
        .workspace_id(&ctx.workspace)
        .resource(resource)
        .decision_reason(REASON_CRED_VEND_SUCCESS)
        .request_id(&ctx.request_id)
        .try_build()
    {
        emitter.emit(event);
    }
}

/// Emits a credential vending deny audit event.
///
/// # Arguments
///
/// * `emitter` - The audit emitter to use
/// * `ctx` - Request context with tenant, workspace, and `request_id`
/// * `table` - Table info for which credentials were denied
/// * `reason` - Reason for denial (use `REASON_CRED_VEND_*` constants)
pub fn emit_cred_vend_deny(
    emitter: &AuditEmitter,
    ctx: &IcebergRequestContext,
    table: &TableInfo,
    reason: &str,
) {
    let resource = format_table_resource(table);

    if let Ok(event) = AuditEvent::builder()
        .action(AuditAction::CredVendDeny)
        .actor(format!("tenant:{}", ctx.tenant))
        .tenant_id(&ctx.tenant)
        .workspace_id(&ctx.workspace)
        .resource(resource)
        .decision_reason(reason)
        .request_id(&ctx.request_id)
        .try_build()
    {
        emitter.emit(event);
    }
}

/// Emits an Iceberg commit success audit event.
///
/// # Arguments
///
/// * `emitter` - The audit emitter to use
/// * `tenant` - Tenant ID
/// * `workspace` - Workspace ID
/// * `request_id` - Request ID for correlation
/// * `namespace` - Table namespace
/// * `table` - Table name
/// * `sequence_number` - New sequence number after commit (None if unavailable)
pub fn emit_iceberg_commit(
    emitter: &AuditEmitter,
    tenant: &str,
    workspace: &str,
    request_id: &str,
    namespace: &str,
    table: &str,
    sequence_number: Option<i64>,
) {
    let resource = format_commit_resource(namespace, table, sequence_number);

    if let Ok(event) = AuditEvent::builder()
        .action(AuditAction::IcebergCommit)
        .actor(format!("tenant:{tenant}"))
        .tenant_id(tenant)
        .workspace_id(workspace)
        .resource(resource)
        .decision_reason(REASON_COMMIT_SUCCESS)
        .request_id(request_id)
        .try_build()
    {
        emitter.emit(event);
    }
}

/// Emits an Iceberg commit deny audit event.
///
/// # Arguments
///
/// * `emitter` - The audit emitter to use
/// * `tenant` - Tenant ID
/// * `workspace` - Workspace ID
/// * `request_id` - Request ID for correlation
/// * `namespace` - Table namespace
/// * `table` - Table name
/// * `reason` - Reason for denial (use `REASON_COMMIT_*` constants)
pub fn emit_iceberg_commit_deny(
    emitter: &AuditEmitter,
    tenant: &str,
    workspace: &str,
    request_id: &str,
    namespace: &str,
    table: &str,
    reason: &str,
) {
    let resource = format_commit_resource(namespace, table, None);

    if let Ok(event) = AuditEvent::builder()
        .action(AuditAction::IcebergCommitDeny)
        .actor(format!("tenant:{tenant}"))
        .tenant_id(tenant)
        .workspace_id(workspace)
        .resource(resource)
        .decision_reason(reason)
        .request_id(request_id)
        .try_build()
    {
        emitter.emit(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TableIdent;
    use arco_core::audit::TestAuditSink;
    use std::sync::Arc;

    fn make_test_ctx() -> IcebergRequestContext {
        IcebergRequestContext {
            tenant: "test-tenant".to_string(),
            workspace: "test-workspace".to_string(),
            request_id: "req-123".to_string(),
            idempotency_key: None,
        }
    }

    fn make_test_table_info() -> TableInfo {
        TableInfo {
            ident: TableIdent::new(vec!["sales".to_string()], "orders".to_string()),
            table_id: "table-uuid-123".to_string(),
            location: Some("gs://bucket/table".to_string()),
        }
    }

    #[test]
    fn test_emit_cred_vend_allow() {
        let sink = Arc::new(TestAuditSink::new());
        let emitter = AuditEmitter::with_test_sink(sink.clone());
        let ctx = make_test_ctx();
        let table = make_test_table_info();

        emit_cred_vend_allow(&emitter, &ctx, &table);

        let events = sink.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::CredVendAllow);
        assert_eq!(events[0].decision_reason, REASON_CRED_VEND_SUCCESS);
        assert!(events[0].resource.contains("sales.orders"));
        assert!(events[0].resource.contains("table-uuid-123"));
    }

    #[test]
    fn test_emit_cred_vend_deny() {
        let sink = Arc::new(TestAuditSink::new());
        let emitter = AuditEmitter::with_test_sink(sink.clone());
        let ctx = make_test_ctx();
        let table = make_test_table_info();

        emit_cred_vend_deny(&emitter, &ctx, &table, REASON_CRED_VEND_DISABLED);

        let events = sink.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::CredVendDeny);
        assert_eq!(events[0].decision_reason, REASON_CRED_VEND_DISABLED);
    }

    #[test]
    fn test_emit_iceberg_commit() {
        let sink = Arc::new(TestAuditSink::new());
        let emitter = AuditEmitter::with_test_sink(sink.clone());

        emit_iceberg_commit(
            &emitter,
            "test-tenant",
            "test-workspace",
            "req-456",
            "sales",
            "orders",
            Some(42),
        );

        let events = sink.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::IcebergCommit);
        assert_eq!(events[0].decision_reason, REASON_COMMIT_SUCCESS);
        assert!(events[0].resource.contains("sales.orders"));
        assert!(events[0].resource.contains("seq=42"));
    }

    #[test]
    fn test_emit_iceberg_commit_without_sequence() {
        let sink = Arc::new(TestAuditSink::new());
        let emitter = AuditEmitter::with_test_sink(sink.clone());

        emit_iceberg_commit(
            &emitter,
            "test-tenant",
            "test-workspace",
            "req-789",
            "sales",
            "orders",
            None,
        );

        let events = sink.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::IcebergCommit);
        assert!(events[0].resource.contains("sales.orders"));
        assert!(!events[0].resource.contains("seq="));
    }

    #[test]
    fn test_emit_iceberg_commit_deny() {
        let sink = Arc::new(TestAuditSink::new());
        let emitter = AuditEmitter::with_test_sink(sink.clone());

        emit_iceberg_commit_deny(
            &emitter,
            "test-tenant",
            "test-workspace",
            "req-789",
            "sales",
            "orders",
            REASON_COMMIT_CAS_CONFLICT,
        );

        let events = sink.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, AuditAction::IcebergCommitDeny);
        assert_eq!(events[0].decision_reason, REASON_COMMIT_CAS_CONFLICT);
    }

    #[test]
    fn test_format_table_resource_no_secrets() {
        let table = make_test_table_info();
        let resource = format_table_resource(&table);

        // Should contain identifiers
        assert!(resource.contains("sales.orders"));
        assert!(resource.contains("table-uuid-123"));

        // Should NOT contain storage location (could have signatures)
        assert!(!resource.contains("gs://"));
        assert!(!resource.contains("bucket"));
    }
}
