//! Worker callback handlers per ADR-023.
//!
//! This module implements the worker callback contract:
//!
//! | Endpoint | Purpose |
//! |----------|---------|
//! | `/v1/tasks/{task_id}/started` | Worker began execution |
//! | `/v1/tasks/{task_id}/heartbeat` | Worker is still alive |
//! | `/v1/tasks/{task_id}/completed` | Task finished (success or failure) |
//!
//! ## Design Principles
//!
//! 1. **Framework-agnostic**: Handlers take parsed requests and return responses
//! 2. **Event emission**: All state changes go through the ledger
//! 3. **Attempt guard**: Verify attempt matches to prevent stale-worker corruption
//! 4. **Token validation**: Scoped bearer tokens for authentication

mod handlers;
mod types;

pub use handlers::*;
pub use types::*;
