//! Observability infrastructure for Arco.
//!
//! Per architecture docs: structured logging with consistent spans.
//! This module provides initialization helpers and span constructors
//! for consistent observability across all Arco components.
//!
//! ## Log Redaction
//!
//! Sensitive data (credentials, signed URLs, tokens) should use the
//! [`Redacted`] wrapper to prevent accidental exposure in logs:
//!
//! ```rust
//! use arco_core::observability::Redacted;
//!
//! let secret_url = Redacted::new("https://storage.example.com?sig=abc123");
//! println!("{:?}", secret_url); // Prints: Redacted([REDACTED])
//! println!("{}", secret_url);   // Prints: [REDACTED]
//!
//! // Access the inner value when needed
//! let actual_url: &str = secret_url.expose();
//! ```

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

// ============================================================================
// Log Redaction
// ============================================================================

/// Wrapper for sensitive data that redacts output in Debug/Display.
///
/// Use this wrapper for any data that should not appear in logs:
/// - Signed URLs
/// - API keys and tokens
/// - Credentials
/// - PII
///
/// # Example
///
/// ```rust
/// use arco_core::observability::Redacted;
///
/// let signed_url = Redacted::new("https://storage.example.com?sig=secret123");
///
/// // Debug and Display both show [REDACTED]
/// assert_eq!(format!("{:?}", signed_url), "Redacted([REDACTED])");
/// assert_eq!(format!("{}", signed_url), "[REDACTED]");
///
/// // Access the actual value when needed
/// assert!(signed_url.expose().contains("secret123"));
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct Redacted<T>(T);

impl<T> Redacted<T> {
    /// Creates a new redacted wrapper around a value.
    #[must_use]
    pub const fn new(value: T) -> Self {
        Self(value)
    }

    /// Exposes the inner value.
    ///
    /// Use sparingly - the whole point of `Redacted` is to prevent
    /// accidental exposure. Only call this when you actually need
    /// the value (e.g., to pass to a storage client).
    #[must_use]
    pub fn expose(&self) -> &T {
        &self.0
    }

    /// Consumes the wrapper and returns the inner value.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::fmt::Debug for Redacted<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Redacted([REDACTED])")
    }
}

impl<T> std::fmt::Display for Redacted<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[REDACTED]")
    }
}

impl<T: Default> Default for Redacted<T> {
    fn default() -> Self {
        Self(T::default())
    }
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

    // ========================================================================
    // Redaction Tests
    // ========================================================================

    #[test]
    fn test_redacted_debug_hides_value() {
        let secret = Redacted::new("super_secret_token");
        let debug_output = format!("{:?}", secret);

        assert_eq!(debug_output, "Redacted([REDACTED])");
        assert!(!debug_output.contains("super_secret_token"));
    }

    #[test]
    fn test_redacted_display_hides_value() {
        let secret = Redacted::new("https://storage.example.com?sig=abc123");
        let display_output = format!("{}", secret);

        assert_eq!(display_output, "[REDACTED]");
        assert!(!display_output.contains("sig="));
        assert!(!display_output.contains("abc123"));
    }

    #[test]
    fn test_redacted_expose_reveals_value() {
        let secret_url = "https://storage.example.com?sig=abc123";
        let redacted = Redacted::new(secret_url);

        assert_eq!(*redacted.expose(), secret_url);
    }

    #[test]
    fn test_redacted_into_inner() {
        let secret_url = String::from("https://storage.example.com?sig=abc123");
        let redacted = Redacted::new(secret_url.clone());

        assert_eq!(redacted.into_inner(), secret_url);
    }

    #[test]
    fn test_redacted_clone() {
        let original = Redacted::new("secret");
        let cloned = original.clone();

        assert_eq!(original.expose(), cloned.expose());
    }

    #[test]
    fn test_redacted_default() {
        let redacted: Redacted<String> = Redacted::default();
        assert!(redacted.expose().is_empty());
    }

    #[test]
    fn test_redacted_equality() {
        let a = Redacted::new("secret");
        let b = Redacted::new("secret");
        let c = Redacted::new("different");

        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
