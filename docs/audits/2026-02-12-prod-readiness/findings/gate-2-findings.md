# Gate 2 Findings Refresh - 2026-02-12

- Gate: 2 (Storage, manifests, schema, invariants)
- Baseline commit: `e896506f3c936c266a21fe556a107d37bd7075b5`
- Prior baseline: `docs/audits/2025-12-30-prod-readiness/findings/gate-2-findings.md`

## Overall Status

`GO` (all local/code-only Gate 2 closure signals are closed for Batch 3)

## Signals Closed

1. **Search anti-entropy is now first-class bounded scan (not derived-only rebuild).**
   - Scan path uses `read_domain_watermark` for `Search` and regular bounded listing pass.
   - Code: `crates/arco-compactor/src/anti_entropy.rs:436`, `crates/arco-compactor/src/anti_entropy.rs:498`
   - Test: `crates/arco-compactor/src/anti_entropy.rs:824`
   - Evidence: `release_evidence/2026-02-12-prod-readiness/phase-0/current-head/command-logs/test_workspace_excl_flow_api.log`
2. **Orchestration schema golden fixtures and compatibility gates are now present.**
   - Contract test: `crates/arco-flow/tests/orchestration_schema_contracts.rs`
   - Golden fixtures: `crates/arco-flow/tests/golden_schemas/orchestration/`
   - Evidence: `release_evidence/2026-02-12-prod-readiness/phase-3/batch-3-head/command-logs/test_arco_flow_all_features.log`
3. **Typed non-catalog path canonicalization is complete in scoped flow/api/iceberg modules.**
   - Core typed paths expanded for flow/API/Iceberg: `crates/arco-core/src/flow_paths.rs`
   - Callsite migrations: `crates/arco-api/src/paths.rs`, `crates/arco-api/src/routes/manifests.rs`, `crates/arco-api/src/routes/orchestration.rs`, `crates/arco-iceberg/src/paths.rs`, `crates/arco-iceberg/src/pointer.rs`
   - Contract coverage: `crates/arco-core/tests/flow_paths_contracts.rs`
4. **Deterministic/property invariants now explicitly cover out-of-order, duplicate, and crash-recovery replay behavior.**
   - Property tests: `crates/arco-flow/tests/property_tests.rs`
   - Deterministic replay test: `crates/arco-flow/tests/orchestration_correctness_tests.rs`
   - Evidence: `release_evidence/2026-02-12-prod-readiness/phase-3/batch-3-head/command-logs/test_arco_flow_test_utils.log`, `release_evidence/2026-02-12-prod-readiness/phase-3/batch-3-head/command-logs/test_arco_flow_all_features.log`
5. **Failure-injection coverage now explicitly includes CAS race, partial writes, and compaction replay.**
   - CAS race: `crates/arco-iceberg/src/pointer.rs`
   - Partial write replay: `crates/arco-flow/src/outbox.rs`
   - Compaction replay after manifest publish failure: `crates/arco-flow/tests/orchestration_correctness_tests.rs`
   - Evidence: `release_evidence/2026-02-12-prod-readiness/phase-3/batch-3-head/command-logs/test_arco_flow_default.log`, `release_evidence/2026-02-12-prod-readiness/phase-3/batch-3-head/command-logs/test_arco_flow_all_features.log`
6. **Runbook/operator checks are refreshed with scriptable Batch 3 invariants.**
   - Runbook update: `docs/runbooks/metrics-catalog.md`
   - Evidence: `release_evidence/2026-02-12-prod-readiness/phase-3/batch-3-head/command-matrix-status.tsv`

## Signals Still Open

None (for local/code-only Batch 3 scope).

## Gate 2 Exit Conditions (for GO)

1. No unresolved path-canonicalization gaps in scoped modules.
2. Search anti-entropy bounded scan remains enforced by tests.
3. Orchestration schema fixtures + compatibility tests are committed and CI-enforced.
4. Deterministic invariant/failure-injection coverage meets plan matrix.
5. Runbook updates and evidence links are complete and reproducible.
