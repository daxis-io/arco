# Gate 2 Findings Refresh - 2026-02-12

- Gate: 2 (Storage, manifests, schema, invariants)
- Baseline commit: `e896506f3c936c266a21fe556a107d37bd7075b5`
- Prior baseline: `docs/audits/2025-12-30-prod-readiness/findings/gate-2-findings.md`

## Overall Status

`PARTIAL` (one previously open production signal closed; additional closure work remains)

## Signals Closed

1. **Search anti-entropy is now first-class bounded scan (not derived-only rebuild).**
   - Scan path uses `read_domain_watermark` for `Search` and regular bounded listing pass.
   - Code: `crates/arco-compactor/src/anti_entropy.rs:436`, `crates/arco-compactor/src/anti_entropy.rs:498`
   - Test: `crates/arco-compactor/src/anti_entropy.rs:824`
   - Evidence: `release_evidence/2026-02-12-prod-readiness/phase-0/current-head/command-logs/test_workspace_excl_flow_api.log`

## Signals Still Open

1. **Path canonicalization for non-catalog domains remains fragmented.**
   - Existing hardcoded path usage remains in flow/api/iceberg surfaces.
   - Examples: `crates/arco-flow/src/orchestration/ledger.rs`, `crates/arco-flow/src/outbox.rs`, `crates/arco-iceberg/src/pointer.rs`
   - Closure target: unified typed path APIs and callsite migration.

2. **Orchestration golden schema fixtures are not yet at catalog-equivalent rigor.**
   - Current orchestration schema tests exist but fixture-based compatibility gates are still missing.
   - Current tests: `crates/arco-flow/tests/orchestration_schema_tests.rs`
   - Closure target: committed golden fixtures + compatibility checks wired in CI.

3. **Failure-injection and property coverage needs explicit expansion for all targeted failure classes.**
   - Existing crash/CAS behavior tests exist in catalog/orchestration areas.
   - Closure target: deterministic tests explicitly covering CAS race, partial write windows, and compaction replay paths listed in the closure plan.

4. **Runbook updates for finalized invariants have not yet been refreshed for this audit cycle.**
   - Closure target: update and sign off operator checks in runbooks with linked drill evidence.

## Gate 2 Exit Conditions (for GO)

1. No unresolved path-canonicalization gaps in scoped modules.
2. Search anti-entropy bounded scan remains enforced by tests.
3. Orchestration schema fixtures + compatibility tests are committed and CI-enforced.
4. Deterministic invariant/failure-injection coverage meets plan matrix.
5. Runbook updates and evidence links are complete and reproducible.

