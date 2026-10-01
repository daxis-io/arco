# Test-only tenant grant admission ID and sequence plan

**Goal:** Reject a reused native event ID or reserved sequence before persisting a prepared tenant grant.

**Architecture:** Keep the existing one-pending-grant metastore authority transaction. During preparation, use the native ledger's persisted event list and `next_sequence` reservation view to reject stale identifiers. The ledger's immutable event path and sequence marker still decide races after preparation; ambiguous concurrent direct writers leave the pending grant fail-closed.

**Tech stack:** Rust `arco-catalog`, existing metastore ledger and control-state kernel, memory-backend integration tests.

## Steps

1. Add failing integration tests to `crates/arco-catalog/tests/tenant_catalog_paths.rs` for reused committed and aborted event IDs, stale and orphan-reserved sequences, and two independent probe writers.
2. Run the focused test and retain the expected rejection failure.
3. Add the smallest preflight validation in `crates/arco-catalog/src/authz/tenant_probe.rs`; reuse ledger helpers and keep the production route closed.
4. Run focused tests, core/catalog tests, rustdoc compile-fail, Clippy, formatting, default-feature check, and hygiene in this worktree's dedicated Cargo target. Record source/test hashes and failures.
5. DCO commit, push, and open a PR against main. Leave production multiwriter admission and provider fencing unqualified.
