# Batch 2 Feedback Remediation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the Batch 2 review feedback by hardening compactor retry/auth behavior and adding verification coverage for fail-closed semantics.

**Architecture:** Keep `CompactorClient` as the single point for auth/retry behavior, and enforce security constraints in `CompactorAuthConfig::validate()` so server startup fails fast. Extend CI matrix and audit artifacts to prove both feature-enabled and feature-disabled auth paths are verified.

**Tech Stack:** Rust (`reqwest`, `tokio`, `axum` test servers), GitHub Actions, existing release evidence/audit markdown+JSON artifacts.

---

### Task 1: Harden Retry Policy in Compactor Client

**Files:**
- Modify: `crates/arco-api/src/compactor_client.rs`

**Step 1: Write failing tests**
- Add a test that transport errors are retried once before surfacing an error.
- Add a test that 503 retry path performs measurable backoff (non-zero delay).

**Step 2: Run targeted tests to verify failures**

Run:
```bash
cargo test -p arco-api --tests compactor_client::tests::retries_transport_error_once_before_failing
cargo test -p arco-api --tests compactor_client::tests::retries_once_on_503_and_succeeds
```

Expected: at least one new test fails before implementation.

**Step 3: Implement minimal retry/backoff changes**
- Add retry classification for transport/timeouts.
- Add bounded exponential backoff + jitter between attempts.

**Step 4: Re-run targeted tests**

Run:
```bash
cargo test -p arco-api --tests compactor_client::tests::retries_transport_error_once_before_failing
cargo test -p arco-api --tests compactor_client::tests::retries_once_on_503_and_succeeds
```

Expected: pass.

### Task 2: Tighten Auth Safety and Add Coverage

**Files:**
- Modify: `crates/arco-api/src/config.rs`
- Modify: `crates/arco-api/src/compactor_client.rs`
- Modify: `crates/arco-api/src/server.rs`

**Step 1: Write failing tests**
- Add config validation test rejecting `metadata_url` unless debug posture is enabled.
- Add test proving explicit static token is not overridden by legacy URL userinfo.

**Step 2: Run targeted tests to verify failures**

Run:
```bash
cargo test -p arco-api --tests compactor_auth_debug_redacts_static_token
cargo test -p arco-api --tests compactor_client::tests::legacy_url_userinfo_does_not_override_explicit_static_bearer
```

Expected: new tests fail before implementation.

**Step 3: Implement minimal safety logic**
- Validate metadata URL usage (debug-only).
- Keep deprecation fallback but preserve explicit static bearer precedence.

**Step 4: Re-run targeted tests**

Run:
```bash
cargo test -p arco-api --tests test_static_bearer_mode_requires_token
cargo test -p arco-api --tests compactor_client::tests::legacy_url_userinfo_does_not_override_explicit_static_bearer
```

Expected: pass.

### Task 3: Expand Verification Matrix and Audit Artifacts

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `docs/audits/2026-02-12-prod-readiness/gate-tracker.json`
- Modify: `docs/audits/2026-02-12-prod-readiness/signal-ledger.md`
- Modify: `docs/audits/2026-02-12-prod-readiness/summary.md`

**Step 1: Add matrix coverage**
- Add an `arco-api` default-features test row to CI for non-`gcp` fail-closed path.
- Add the same command to gate tracker required command list.

**Step 2: Update audit evidence language**
- Update signal/evidence wording so fail-closed semantics are tied to explicit default-feature coverage.

**Step 3: Run targeted validation**

Run:
```bash
cargo test -p arco-api --tests
cargo test -p arco-api --all-features --tests
```

Expected: both command variants pass.

### Task 4: Final Verification

**Files:**
- No new code files.

**Step 1: Run final focused checks**

Run:
```bash
cargo fmt --all --check
cargo test -p arco-api --tests
cargo test -p arco-api --all-features --tests
```

Expected: all pass; if blocked by environment constraints, capture exact failure and why.

**Step 2: Summarize file-level changes and residual risk**
- Provide review-ready notes with exact file references.
