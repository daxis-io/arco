# Batches 0-3 Closure Evidence

Scope: local branch from refreshed `origin/main` at `446593d9ab207ca148d4aed510146657636a26e1`.

Batch 4 remains blocked until the Batch 0-3 issue matrix below is landed or the evidence-only issues are closed.

## Issue Matrix

| Issue | Status | Evidence |
| --- | --- | --- |
| #215 | already closed | PR #298 merged; live issue state is CLOSED. |
| #216 | ready to close with landed evidence | PR #298 merged; `cargo test -p arco-api --lib` covers L0-only system table query behavior. |
| #217 | ready to close with landed evidence | PR #298 merged; `cargo test -p arco-integration-tests --test user_acceptance_pipeline` passes. |
| #219 | ready to close with landed evidence | PR #298 merged; callback completion response coverage passes in `cargo test -p arco-flow --lib`. |
| #220 | ready to close with landed evidence | PR #298 merged; UAT planning/execution API coverage passes in `cargo test -p arco-api --lib`. |
| #222 | ready to close with landed evidence | PR #298 merged; UAT materialization lineage and partition assertions pass. |
| #223 | ready to close with landed evidence | PR #298 merged; deterministic UAT runner is split from local hygiene, and the runner smoke passes. |
| #224 | ready to close with landed evidence | PR #298 merged; actionlint runner smoke passes. |
| #225 | ready to close with landed evidence | PR #298 merged; UAT run state reaches SUCCEEDED after completion. |
| #226 | ready to close with landed evidence | PR #298 merged; code_version assertions pass in UAT and API route tests. |
| #227 | ready to close with landed evidence | PR #298 merged; UAT evidence validator smoke passes. |
| #229 | ready to close with landed evidence | PR #298 merged; backfill/sensor code_version route coverage passes. |
| #230 | ready to close with landed evidence | PR #298 merged; local evidence still shows link pressure is machine-resource-specific, not an accepted gate dependency. |
| #198 | ready to close with landed evidence | `cargo test -p arco-uc --test support_registry` passes. |
| #228 | fixed locally | Supported UAT lint policy is documented/enforced; stale runner test name was replaced; runner smoke passes. |
| #256 | ready to close with landed evidence | PR #301 merged; Python/Rust callback task ID alignment covered by API and integration tests. |
| #257 | ready to close with landed evidence | PR #301 merged; worker dispatch auth/e2e coverage passes. |
| #258 | ready to close with landed evidence | PR #301 merged; prefix-scoped timer/anti-entropy coverage passes. |
| #285 | ready to close with landed evidence | PR #301 merged; unknown-event tolerance coverage passes in flow tests. |
| #296 | ready to close with landed evidence | PR #301 merged; ADR uniqueness work is landed. |
| #250 | refuted with current evidence | `cargo test -p arco-flow anti_entropy` passes; retry wait and stale-running recovery are covered. |
| #255 | fixed locally | Added compaction row-amplification baseline/guard and documented current callback path findings. |
| #259 | refuted with current evidence | `cargo test -p arco-api --test control_plane_transactions_api commit_orchestration_batch` passes ambiguous append/compact repair cases. |
| #263 | fixed locally | Rejected/stale events no longer consume idempotency keys; rebuild regression passes. |
| #264 | fixed locally | Replay now sorts by event ID, not producer timestamp, with skew regressions. |
| #268 | fixed locally | Emitted cursors are opaque, sort-aware keyset cursors; legacy numeric cursors remain accepted. |
| #272 | fixed locally | Unreachable dispatch statuses were removed; legacy Parquet values map compatibly. |
| #273 | fixed locally | Task tokens include attempt_id, legacy unscoped tokens are rejected, and callback validators reject stale attempt callbacks. |
| #274 | fixed locally | Sweeper redispatch task IDs include repair-attempt scope. |
| #275 | fixed locally | Added deterministic multi-instance and clock-skew coverage; the opt-in nightly chaos workflow now runs that deterministic gate. |
| #281 | fixed locally | MemoryBackend CAS conformance now matches production expectations more closely. |
| #282 | fixed locally | Fold properties compare full normalized FoldState, not just counts. |

## Changed Files By Cluster

- #198/#228/#255 closure evidence and policy: `docs/plans/2026-06-20-issue-255-orchestration-compaction-architecture.md`, `scripts/run_user_acceptance_pipeline_uat.sh`, `docs/plans/2026-06-24-batches-0-3-status.md`, `crates/arco-flow/src/lib.rs`.
- #281 storage/CAS: `crates/arco-core/src/storage.rs`, `crates/arco-core/tests/storage_backend_conformance.rs`.
- #263/#264/#282 replay and fold correctness: `crates/arco-flow/src/orchestration/events/mod.rs`, `crates/arco-flow/src/orchestration/compactor/fold.rs`, `crates/arco-flow/src/orchestration/compactor/service.rs`, `crates/arco-flow/src/orchestration/state.rs`, `crates/arco-flow/tests/orchestration_rebuild_dr.rs`, `crates/arco-flow/tests/property_tests.rs`.
- #268 pagination/API: `crates/arco-api/src/routes/orchestration.rs`, `crates/arco-flow/src/orchestration/state.rs`.
- #272 dispatch state cleanup: `crates/arco-flow/src/orchestration/compactor/fold.rs`, `crates/arco-flow/src/orchestration/compactor/parquet_util.rs`, `crates/arco-flow/src/orchestration/controllers/anti_entropy.rs`, `crates/arco-flow/src/orchestration/controllers/dispatcher.rs`, `crates/arco-flow/src/orchestration/controllers/ready_dispatch.rs`, `crates/arco-flow/tests/runtime_observability_tests.rs`.
- #273/#274 dispatch token and redispatch security: `crates/arco-core/src/task_tokens.rs`, `crates/arco-api/src/routes/tasks.rs`, `crates/arco-flow/src/orchestration/callbacks/handlers.rs`, `crates/arco-flow/src/bin/arco_flow_dispatcher.rs`, `crates/arco-flow/src/bin/arco_flow_sweeper.rs`, `crates/arco-integration-tests/tests/orchestration_external_worker_e2e.rs`, `crates/arco-integration-tests/tests/user_acceptance_pipeline.rs`.
- #275 deterministic chaos replacement: `.github/workflows/nightly-chaos.yml`, `.github/workflows/release-sbom.yml`, `crates/arco-flow/tests/orchestration_correctness_tests.rs`.

## Verification Log

Passed:

- `cargo test -p arco-core --test storage_backend_conformance` (3 passed, 2 ignored cloud backends).
- `cargo test -p arco-flow --lib` (503 passed).
- `cargo test -p arco-flow --features test-utils --test property_tests` (10 passed).
- `cargo test -p arco-flow --test orchestration_rebuild_dr` (6 passed).
- `cargo test -p arco-flow --test orchestration_correctness_tests` (14 passed).
- `cargo test -p arco-flow --test runtime_observability_tests` (7 passed).
- `cargo test -p arco-flow --lib orchestration::compactor::fold::tests` (80 passed).
- `cargo test -p arco-core --test storage_backend_conformance` (4 passed, 2 cloud tests ignored).
- `cargo test -p arco-api --lib` (170 passed).
- `cargo test -p arco-api --lib test_jwt_task_token_validator_rejects_legacy_token_without_scope_claims` (1 passed after proving the legacy-token regression).
- `cargo test -p arco-api --lib test_paginate_keyset_cursor_resumes_after_anchor_disappears` (1 passed after proving the anchor-deletion regression).
- `cargo test -p arco-api --lib test_paginate_keyset_cursor_uses_descending_sort_tuple` (1 passed).
- `cargo test -p arco-flow --bin arco_flow_dispatcher --bin arco_flow_sweeper` (4 dispatcher tests and 5 sweeper tests passed).
- `cargo test -p arco-flow --bin arco_flow_sweeper redispatch_cloud_task_id_is_repair_scoped` (1 passed after proving the repair-attempt naming regression).
- `cargo test -p arco-core task_tokens` (7 matching tests passed).
- `cargo test -p arco-api --test control_plane_transactions_api commit_orchestration_batch` (10 matching tests passed).
- `cargo test -p arco-flow anti_entropy` (13 matching lib tests plus matching integration test passed).
- `cargo test -p arco-uc --test support_registry` (8 passed).
- `cargo test -p arco-integration-tests --test orchestration_external_worker_e2e` (1 passed).
- `cargo test -p arco-integration-tests --test user_acceptance_pipeline` (3 passed).
- `bash tools/test_user_acceptance_uat_runner.sh`.
- `scripts/run_user_acceptance_pipeline_uat.sh --deterministic --dry-run`.
- `scripts/run_user_acceptance_pipeline_uat.sh --with-hygiene --dry-run`.
- `scripts/run_user_acceptance_pipeline_uat.sh --with-hygiene` (full hygiene gate passed after disk pressure was cleared).
- `scripts/run_user_acceptance_pipeline_uat.sh --status`.
- `bash tools/test_actionlint_runner.sh`.
- `./scripts/run_actionlint.sh` (raw workflow validation passed with local `actionlint` installed).
- `bash tools/test_user_acceptance_evidence_validator.sh`.
- `cargo clippy --message-format short -p arco-flow --lib -- -D warnings`.
- `cargo fmt --check`.
- `git diff --check`.

Non-gate strict lint probes:

- `cargo clippy --message-format short -p arco-flow --lib --tests -- -D warnings` remains outside the #228 supported gate. After production clippy was made clean, the remaining failures are test/binary-target lint policy debt such as test `expect`/`panic`/indexing, ignored-test reasons, and legacy test style lints.
- Broad `cargo clippy --workspace --all-targets --all-features -- -D warnings` was not run because the supported UAT gate and production `arco-flow` strict clippy now pass, while the broader test-target policy remains intentionally non-gating for Batch 0-3 closure.

## Paste-Ready Comments

### #215

Ready to leave closed. Verified against current `origin/main` (`446593d9ab207ca148d4aed510146657636a26e1`): PR #298 is merged and live issue state is CLOSED. No local regression found in the Batch 0-3 verification pass.

### #216

Ready to close. Verified against current `origin/main` (`446593d9ab207ca148d4aed510146657636a26e1`): PR #298 landed the L0-only orchestration system-table coverage, and the current branch passes `cargo test -p arco-api --lib`.

### #217

Ready to close. Verified against current `origin/main` (`446593d9ab207ca148d4aed510146657636a26e1`): PR #298 landed the first-class cataloged orchestration UAT suite, and `cargo test -p arco-integration-tests --test user_acceptance_pipeline` passes.

### #219

Ready to close. Verified against current `origin/main` (`446593d9ab207ca148d4aed510146657636a26e1`): PR #298 landed worker completion response semantics coverage, and `cargo test -p arco-flow --lib` passes the callback handler tests.

### #220

Ready to close. Verified against current `origin/main` (`446593d9ab207ca148d4aed510146657636a26e1`): PR #298 landed the public API support needed by UAT planning/execution, and `cargo test -p arco-api --lib` passes.

### #222

Ready to close. Verified against current `origin/main` (`446593d9ab207ca148d4aed510146657636a26e1`): PR #298 landed UAT assertions for materialization lineage and partition status, and `cargo test -p arco-integration-tests --test user_acceptance_pipeline` passes.

### #223

Ready to close. Verified against current `origin/main` (`446593d9ab207ca148d4aed510146657636a26e1`): PR #298 split deterministic UAT from local hygiene. The local runner policy smoke passes with `bash tools/test_user_acceptance_uat_runner.sh`.

### #224

Ready to close. Verified against current `origin/main` (`446593d9ab207ca148d4aed510146657636a26e1`): PR #298 added the supported workflow-validation command. `bash tools/test_actionlint_runner.sh` and `./scripts/run_actionlint.sh` both pass locally.

### #225

Ready to close. Verified against current `origin/main` (`446593d9ab207ca148d4aed510146657636a26e1`): PR #298 covers the TRIGGERED-with-completed-counters gap. After the Batch 3 event-ID fix, `cargo test -p arco-integration-tests --test user_acceptance_pipeline` confirms the UAT run reaches `SUCCEEDED`.

### #226

Ready to close. Verified against current `origin/main` (`446593d9ab207ca148d4aed510146657636a26e1`): PR #298 landed manifest-triggered run `code_version` semantics, with UAT and API route coverage passing.

### #227

Ready to close. Verified against current `origin/main` (`446593d9ab207ca148d4aed510146657636a26e1`): PR #298 landed UAT evidence validation. `bash tools/test_user_acceptance_evidence_validator.sh` passes locally.

### #229

Ready to close. Verified against current `origin/main` (`446593d9ab207ca148d4aed510146657636a26e1`): PR #298 landed backfill/sensor requested-run `code_version` semantics. `cargo test -p arco-api --lib` and the UAT suite pass.

### #230

Ready to close as a documented local-resource issue. Verified against current `origin/main` (`446593d9ab207ca148d4aed510146657636a26e1`): PR #298 landed the hygiene split so UAT closure no longer depends on broad link-heavy local targets. After clearing the local disk-pressure condition, `scripts/run_user_acceptance_pipeline_uat.sh --with-hygiene` passes.

### #198

Ready to close. Current `origin/main` includes the UC support registry, and `cargo test -p arco-uc --test support_registry` passes all 8 tests, including explicit registry classification and structured 501 coverage.

### #228

Fixed locally. The supported policy is documented/enforced as deterministic UAT plus local shell/fmt/diff hygiene, without absorbing broad test-target clippy policy debt. I also replaced the stale removed event-priority test in `scripts/run_user_acceptance_pipeline_uat.sh` and made production `arco-flow` strict clippy clean. Evidence: `bash tools/test_user_acceptance_uat_runner.sh`, dry-runs for `--deterministic` and `--with-hygiene`, full `scripts/run_user_acceptance_pipeline_uat.sh --with-hygiene`, `cargo clippy --message-format short -p arco-flow --lib -- -D warnings`, `cargo fmt --check`, and `git diff --check` all pass.

### #256

Ready to close. Verified PR #301 is merged on current `origin/main`; current local verification passes `cargo test -p arco-api --lib` and `cargo test -p arco-integration-tests --test orchestration_external_worker_e2e`.

### #257

Ready to close. Verified PR #301 is merged on current `origin/main`; current local verification passes worker dispatch auth/callback coverage through `cargo test -p arco-api --lib` and `cargo test -p arco-integration-tests --test orchestration_external_worker_e2e`.

### #258

Ready to close. Verified PR #301 is merged on current `origin/main`; current local verification passes `cargo test -p arco-flow anti_entropy`.

### #285

Ready to close. Verified PR #301 is merged on current `origin/main`; current local verification passes `cargo test -p arco-flow --lib`, including unknown-event tolerance coverage.

### #296

Ready to close. Verified PR #301 is merged on current `origin/main`; no Batch 0-3 regression was found in the current verification pass.

### #250

Ready to close as refuted by current evidence. On current code, retry wait and stale-running recovery are covered by `cargo test -p arco-flow anti_entropy`, which passes. I did not find a remaining retry/zombie gap requiring a patch in this batch.

### #255

Fixed locally. I confirmed callbacks still use the compacting callback path and added a deterministic row-amplification guard, `callback_compaction_delta_rows_stay_bounded_with_existing_history`, plus documented the current baseline in `docs/plans/2026-06-20-issue-255-orchestration-compaction-architecture.md`. Evidence: `cargo test -p arco-flow --lib` passes.

### #259

Ready to close as refuted by current evidence. Current control-plane transaction tests already cover ambiguous append/compact repair behavior. Evidence: `cargo test -p arco-api --test control_plane_transactions_api commit_orchestration_batch` passes 10 matching tests.

### #263

Fixed locally. Rejected/stale events now record idempotency keys only when they actually apply. Evidence: `cargo test -p arco-flow --lib` and `cargo test -p arco-flow --test orchestration_rebuild_dr` pass.

### #264

Fixed locally. Compaction and rebuild replay now order by event ID rather than producer wall-clock timestamp, with skew regressions. I also fixed event ID generation to be monotonic for same-millisecond bursts so the event-ID ordering contract is safe for callback lifecycles. Evidence: `cargo test -p arco-flow --lib`, `cargo test -p arco-flow --test orchestration_rebuild_dr`, and `cargo test -p arco-integration-tests --test user_acceptance_pipeline` pass.

### #268

Fixed locally. Run and route list pagination now emit opaque keyset cursors while still accepting legacy numeric cursors as input. Route cursors encode the list sort tuple, so they can resume after the previous anchor row disappears and remain correct for descending time-ordered lists. Evidence: `cargo test -p arco-api --lib`, `cargo test -p arco-api --lib test_paginate_keyset_cursor_resumes_after_anchor_disappears`, `cargo test -p arco-api --lib test_paginate_keyset_cursor_uses_descending_sort_tuple`, and `cargo test -p arco-flow --lib` pass.

### #272

Fixed locally. The unreachable `DispatchStatus::Acked` and `DispatchStatus::Failed` states were removed from active state, with legacy Parquet values mapped compatibly. Evidence: `cargo test -p arco-flow --lib`, `cargo test -p arco-flow --test runtime_observability_tests`, and dispatcher/sweeper bin tests pass.

### #273

Fixed locally. Task tokens now include `attempt_id`, callback validators require it, legacy unscoped task tokens are rejected as missing scope, and stale attempt callbacks are rejected. Evidence: `cargo test -p arco-core task_tokens`, `cargo test -p arco-api --lib`, `cargo test -p arco-api --lib test_jwt_task_token_validator_rejects_legacy_token_without_scope_claims`, and the integration callback tests pass.

### #274

Fixed locally. Sweeper redispatch Cloud Task names include both repair row scope and a fresh repair-attempt ID, so repeated repair attempts for the same stale outbox row do not collide with earlier task names. Evidence: `cargo test -p arco-flow --bin arco_flow_sweeper` passes, including `redispatch_cloud_task_id_is_repair_scoped`; the narrower red/green command was `cargo test -p arco-flow --bin arco_flow_sweeper redispatch_cloud_task_id_is_repair_scoped`.

### #275

Fixed locally. The fake disabled scheduled chaos workflow was replaced with a manual workflow that checks out the repository, installs Rust, restores Cargo cache, and runs `cargo test -p arco-flow --test orchestration_correctness_tests`. Deterministic local coverage exercises multi-instance ready-dispatch convergence plus event clock skew. Evidence: `cargo test -p arco-flow --test orchestration_correctness_tests`, `bash tools/test_actionlint_runner.sh`, and `./scripts/run_actionlint.sh` pass.

### #281

Fixed locally. MemoryBackend CAS now uses backend-wide monotonic versions, malformed/missing CAS tokens fail closed, and conformance tests cover MemoryBackend plus object-store CAS boundaries. Evidence: `cargo test -p arco-core --test storage_backend_conformance` passes.

### #282

Fixed locally. Fold determinism properties now compare normalized full `FoldState` instead of only counts. Evidence: `cargo test -p arco-flow --features test-utils --test property_tests` passes.
