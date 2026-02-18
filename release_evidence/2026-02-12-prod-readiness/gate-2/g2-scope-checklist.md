# Gate 2 Scope Checklist (Locked)

- Gate: `2` (`Storage / manifest / schema / invariants`)
- Scope lock timestamp (UTC): `2026-02-18T20:10:00Z`
- Authoritative sources:
  - `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:66`
  - `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:70`
  - `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:79`
  - `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:228`
  - `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:26`
  - `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:27`
  - `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:28`
  - `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:29`
  - `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:30`
  - `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:31`
  - `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:32`
  - `docs/runbooks/metrics-catalog.md:30`

## Gate Closure Conditions

1. Gate 2 can remain `GO` only when required signals `G2-001..G2-007` are all `GO`.
   - Source: `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:70`, `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:219`
2. Gate 2 closure must include concrete reproducible evidence artifacts.
   - Source: `docs/audits/2026-02-12-prod-readiness/gate-tracker.json:79`, `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:26`
3. Runbook checks in `metrics-catalog.md` are in-scope closure evidence for `G2-007`.
   - Source: `docs/audits/2026-02-12-prod-readiness/signal-ledger.md:32`, `docs/runbooks/metrics-catalog.md:30`

## Signal Criteria and Pass/Fail Checks

| Signal | Authoritative Criterion (Source) | Pass Criteria | Fail Criteria | Code / Test / Doc Paths |
|---|---|---|---|---|
| `G2-001` | Unified typed path builders for non-catalog domains (`signal-ledger.md:26`) | Canonical typed path builders exist in core and are consumed by flow/api/iceberg call sites with stable contracts. | Path builders missing or call sites bypass typed helpers. | `crates/arco-core/src/flow_paths.rs`, `crates/arco-flow/src/paths.rs`, `crates/arco-api/src/paths.rs`, `crates/arco-iceberg/src/paths.rs`, `crates/arco-core/tests/flow_paths_contracts.rs` |
| `G2-002` | Replace hardcoded path literals in flow/api/iceberg modules (`signal-ledger.md:27`) | Runbook hardcoded-path scan returns no matches in scoped files. | Any scoped module contains disallowed hardcoded storage-path literals. | `crates/arco-flow/src/orchestration/ledger.rs`, `crates/arco-flow/src/outbox.rs`, `crates/arco-iceberg/src/pointer.rs`, `crates/arco-api/src/routes/manifests.rs`, `crates/arco-api/src/routes/orchestration.rs`, `docs/runbooks/metrics-catalog.md` |
| `G2-003` | Search anti-entropy as first-class bounded scan path (`signal-ledger.md:28`) | Search anti-entropy reads watermark and bounded-scan cursor progression test passes. | Search domain lacks bounded scan cursor progression or regression in test. | `crates/arco-compactor/src/anti_entropy.rs:436`, `crates/arco-compactor/src/anti_entropy.rs:824` |
| `G2-004` | Orchestration golden schema fixtures + compatibility tests (`signal-ledger.md:29`) | Golden fixtures are committed and schema compatibility contract test passes. | Fixtures absent/stale or compatibility contract fails. | `crates/arco-flow/tests/orchestration_schema_contracts.rs`, `crates/arco-flow/tests/golden_schemas/orchestration/` |
| `G2-005` | Deterministic/property tests for out-of-order/duplicate/crash invariants (`signal-ledger.md:30`) | Property invariants and deterministic correctness tests pass for out-of-order, duplicate, crash replay behavior. | Any invariant test fails or missing coverage for listed behavior classes. | `crates/arco-flow/tests/property_tests.rs`, `crates/arco-flow/tests/orchestration_correctness_tests.rs` |
| `G2-006` | Failure-injection for CAS race/partial writes/compaction replay (`signal-ledger.md:31`) | Deterministic tests pass for CAS race single-winner, partial write recovery replay, and compaction replay recovery. | Any of the three required failure-injection classes is missing or failing. | `crates/arco-iceberg/src/pointer.rs`, `crates/arco-flow/src/outbox.rs`, `crates/arco-flow/tests/orchestration_correctness_tests.rs` |
| `G2-007` | Runbooks updated with final invariants/operator checks (`signal-ledger.md:32`) | Gate 2 invariant check section exists and referenced commands execute successfully. | Runbook section missing/stale or commands fail. | `docs/runbooks/metrics-catalog.md:30`, `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` |

## Scope Conflict Check

- `gate-tracker.json` required signals (`G2-001..G2-007`) match `signal-ledger.md` Gate 2 rows exactly.
- No conflicting closure semantics were found between tracker, ledger, and runbook criteria.
- Scope conflict verdict: `NO-CONFLICT`.
