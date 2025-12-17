//! Cross-crate integration test harness.
//!
//! The workspace root is a virtual workspace (no `[package]`), so repository-root `tests/` are
//! not discovered by Cargo. This crate exists solely to host workspace-level integration tests
//! that span multiple crates.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![warn(clippy::pedantic)]
#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]
