# Remaining Security/Reliability Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement all remaining actionable findings from the audit in priority order across API, catalog reader, and compactor.

**Architecture:** Keep behavior-compatible defaults, add opt-in guards via config/env, and tighten validation/auth paths. Use targeted route-level improvements and minimal surface-area API changes.

**Tech Stack:** Rust, Axum, Tower, DataFusion, existing Arco crates.

---

### Task 1: URL Redaction Hardening (SEC-02)

**Files:**
- Modify: `crates/arco-api/src/redaction.rs`

**Steps:**
1. Add failing tests for cloud-style signed URL params (`X-Amz-*`, `X-Goog-*`, case-insensitive keys).
2. Verify tests fail.
3. Replace ad-hoc substring redaction with parsed query-parameter scrubbing.
4. Run redaction tests and verify pass.

### Task 2: Browser Path Validation Consistency + Mint Allowlist Reuse (SEC-03, PERF-02)

**Files:**
- Modify: `crates/arco-core/src/scoped_storage.rs`
- Modify: `crates/arco-catalog/src/reader.rs`
- Modify: `crates/arco-api/src/routes/browser.rs`

**Steps:**
1. Add failing browser route test for encoded traversal-like path that should be rejected before manifest reads.
2. Verify test fails.
3. Expose shared path validator from `ScopedStorage` and use it in browser route.
4. Add `CatalogReader` API that accepts precomputed allowlist and avoid recomputing in browser flow.
5. Run browser/query reader tests and verify pass.

### Task 3: API Metrics Gate + gRPC Signaling + Graceful Shutdown (SEC-04, ARCH-01, REL-01)

**Files:**
- Modify: `crates/arco-api/src/config.rs`
- Modify: `crates/arco-api/src/server.rs`
- Modify: `crates/arco-api/src/lib.rs`

**Steps:**
1. Add failing server tests for `/metrics` requiring secret when configured.
2. Verify tests fail.
3. Add `ARCO_METRICS_SECRET` config support and metrics auth gate.
4. Clarify gRPC status at startup (explicitly warn that gRPC listener is not active).
5. Add graceful shutdown to `serve()` via signal handling and `with_graceful_shutdown()`.
6. Run server/config tests and verify pass.

### Task 4: Compactor Correctness Hardening (CORR-02, CORR-03)

**Files:**
- Modify: `crates/arco-compactor/src/main.rs`

**Steps:**
1. Add failing tests for compaction-in-progress cleanup behavior and pending-no-flush cycle semantics.
2. Verify tests fail.
3. Introduce RAII guard for `compaction_in_progress` reset on all exits.
4. Differentiate cycle outcomes so pending/no-flush does not count as successful compaction heartbeat.
5. Run compactor tests and verify pass.

### Task 5: Rate Limiter Entry Eviction (PERF-03)

**Files:**
- Modify: `crates/arco-api/src/rate_limit.rs`
- Modify: `crates/arco-api/src/config.rs` (if needed for config defaults)

**Steps:**
1. Add failing tests for max tenant entry bound / stale entry eviction behavior.
2. Verify tests fail.
3. Implement bounded map + TTL-based pruning for limiter entries.
4. Run rate limit tests and verify pass.

### Task 6: Verify and Regressions

**Files:**
- N/A

**Steps:**
1. Run focused tests for each modified module.
2. Run `cargo test -p arco-api --lib`.
3. Run `cargo test -p arco-compactor --bin arco-compactor`.
4. Summarize outcomes and residual risks.
