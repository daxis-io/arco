//! Commit-related types for the Iceberg REST write path.
//!
//! This module contains types for the commit table endpoint including:
//! - Update requirements for optimistic concurrency
//! - Table updates (schema changes, snapshots, etc.)
//! - Request/response types
//!
//! The `CommitKey` type is in the `ids` module as it's also used elsewhere.
