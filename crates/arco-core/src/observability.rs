//! Observability infrastructure for Arco.
//!
//! Per architecture docs: structured logging with consistent spans.
//! This module provides initialization helpers and span constructors
//! for consistent observability across all Arco components.

use std::sync::Once;
use tracing::Span;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

static INIT: Once = Once::new();

/// Log output format.
#[derive(Debug, Clone, Copy, Default)]
pub enum LogFormat {
    /// JSON structured logs (for production).
    Json,
    /// Pretty-printed logs (for development).
    #[default]
    Pretty,
}

/// Initializes the logging subsystem.
///
/// Call once at application startup. Safe to call multiple times;
/// subsequent calls are no-ops.
///
/// # Environment Variables
///
/// - `RUST_LOG`: Controls log levels (e.g., `info`, `arco_catalog=debug`)
///
/// # Example
///
/// ```rust
/// use arco_core::observability::{init_logging, LogFormat};
///
/// init_logging(LogFormat::Pretty);
/// ```
pub fn init_logging(format: LogFormat) {
    INIT.call_once(|| {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

        match format {
            LogFormat::Json => {
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(fmt::layer().json())
                    .init();
            }
            LogFormat::Pretty => {
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(fmt::layer().pretty())
                    .init();
            }
        }
    });
}

/// Creates a span for catalog operations with standard fields.
///
/// # Example
///
/// ```rust
/// use arco_core::observability::catalog_span;
///
/// let span = catalog_span("get_asset", "acme-corp", "production");
/// let _guard = span.enter();
/// // ... do catalog operation
/// ```
#[must_use]
pub fn catalog_span(operation: &str, tenant: &str, workspace: &str) -> Span {
    tracing::info_span!(
        "catalog",
        op = operation,
        tenant = tenant,
        workspace = workspace,
    )
}

/// Creates a span for orchestration operations.
///
/// # Example
///
/// ```rust
/// use arco_core::observability::orchestration_span;
///
/// let span = orchestration_span("execute_task", "run_abc123", "acme-corp", "production");
/// let _guard = span.enter();
/// // ... do orchestration operation
/// ```
#[must_use]
pub fn orchestration_span(operation: &str, run_id: &str, tenant: &str, workspace: &str) -> Span {
    tracing::info_span!(
        "orchestration",
        op = operation,
        run_id = run_id,
        tenant = tenant,
        workspace = workspace,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_logging_succeeds() {
        // Should not panic (uses Once internally)
        init_logging(LogFormat::Pretty);
        init_logging(LogFormat::Pretty); // Second call should be no-op
    }

    #[test]
    fn test_span_helper_creates_span() {
        let span = catalog_span("test_operation", "acme", "production");
        let _guard = span.enter();
        tracing::info!("test message in span");
    }

    #[test]
    fn test_orchestration_span_creates_span() {
        let span = orchestration_span("test_task", "run_123", "acme", "production");
        let _guard = span.enter();
        tracing::info!("orchestration message");
    }
}
