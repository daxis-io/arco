# G2-005 Evidence - Deterministic/Property Invariants (Out-of-Order, Duplicate, Crash)

- Signal: `G2-005`
- Ledger criterion: `Property + snapshot suites cover all listed invariants and pass.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md:30`)
- Status: `GO`

## Implementation Evidence

1. Property tests encode out-of-order/duplicate invariants and crash-replay convergence checks.
   - `crates/arco-flow/tests/property_tests.rs:560`
   - `crates/arco-flow/tests/property_tests.rs:577`
2. Deterministic correctness tests cover order independence and duplicate no-op behavior.
   - `crates/arco-flow/tests/orchestration_correctness_tests.rs:140`
   - `crates/arco-flow/tests/orchestration_correctness_tests.rs:175`

## Verification Evidence

1. Targeted invariant property tests passed.
   - `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` (`targeted_property_out_of_order_duplicate`, `targeted_property_crash_replay`)
2. Targeted orchestration correctness suite passed.
   - `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` (`targeted_orchestration_correctness_suite`)
3. Full flow test matrices passed with invariants active.
   - `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` (`matrix_test_flow_tests`, `matrix_test_flow_test_utils_tests`, `matrix_test_flow_all_features_tests`)
