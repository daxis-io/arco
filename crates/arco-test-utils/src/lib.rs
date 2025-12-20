//! Shared test utilities for Arco integration tests.
//!
//! This crate provides:
//! - [`TracingMemoryBackend`]: In-memory storage with operation recording
//! - [`TestContext`]: Pre-configured test environment
//! - Factory functions for creating test data
//! - Custom assertion helpers
//!
//! # Example
//!
//! ```rust,ignore
//! use arco_test_utils::{TestContext, PlanFactory, assert_run_succeeded};
//!
//! #[tokio::test]
//! async fn test_example() {
//!     let ctx = TestContext::new();
//!     let (plan, _, _, _) = PlanFactory::linear_dag(&ctx.tenant_id, &ctx.workspace_id);
//!     // ... run test ...
//! }
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![warn(clippy::pedantic)]
#![allow(clippy::must_use_candidate)]
// Test utilities use expect/unwrap for cleaner test code - panics are acceptable in tests
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::missing_panics_doc)]

pub mod assertions;
pub mod fixtures;
pub mod http_signed_url;
pub mod simulation;
pub mod storage;

pub use assertions::*;
pub use fixtures::*;
pub use http_signed_url::*;
pub use simulation::*;
pub use storage::*;

/// Initialize test logging (call once per test module).
pub fn init_test_logging() {
    use tracing_subscriber::{EnvFilter, fmt};

    let _ = fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("arco=debug".parse().expect("valid directive")),
        )
        .with_test_writer()
        .try_init();
}
