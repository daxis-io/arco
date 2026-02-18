# G2-006 Evidence - Failure-Injection (CAS Race, Partial Writes, Compaction Replay)

- Signal: `G2-006`
- Ledger criterion: `Deterministic failure-injection tests added for all three failure classes.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md:31`)
- Status: `GO`

## Implementation Evidence

1. CAS race has explicit single-winner deterministic test in pointer store.
   - `crates/arco-iceberg/src/pointer.rs:793`
2. Outbox partial batch write failure recovery replay is covered.
   - `crates/arco-flow/src/outbox.rs:264`
3. Compaction replay recovery after manifest publish failure is covered.
   - `crates/arco-flow/tests/orchestration_correctness_tests.rs:555`

## Verification Evidence

1. Targeted CAS race failure-injection test passed.
   - `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` (`targeted_pointer_cas_race`)
2. Targeted partial-write replay recovery test passed.
   - `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` (`targeted_partial_write_recovery`, `targeted_partial_write_recovery_recheck`)
3. Targeted compaction replay correctness suite passed.
   - `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` (`targeted_orchestration_correctness_suite`)
