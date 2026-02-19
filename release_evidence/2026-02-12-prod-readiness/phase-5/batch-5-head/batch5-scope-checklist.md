# Batch 5 Scope Checklist (Table Read Operations)

- Batch: 5 (Tasks 5.1-5.4)
- Source plan: `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md`
- Source section: `Batch 5: Table Read Operations`
- Scope lock timestamp (UTC): 2026-02-16T01:45:33Z
- Scope status: `LOCKED`

## Acceptance Criteria Matrix

| ID | Acceptance criterion | Source line refs | Scope status | Implementation status | Test status | Evidence status | Notes |
|---|---|---|---|---|---|---|---|
| 5.1 | Implement list tables handler `GET /v1/{prefix}/namespaces/{namespace}/tables` including filtering to Iceberg tables and pagination behavior with tests. | `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:215`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:218`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:221`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:222`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:223`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:224` | LOCKED | COMPLETE | COMPLETE | COMPLETE | Impl: `crates/arco-iceberg/src/routes/tables.rs:114`. Tests: `crates/arco-iceberg/src/routes/tables.rs:1591`, `crates/arco-iceberg/src/routes/tables.rs:1652`. Fresh log: `.../command-logs/2026-02-16T024039Z-cargo-test-arco-iceberg.log`. |
| 5.2 | Implement `IcebergPointerStore` read operations `get(table_uuid)` and `exists(table_uuid)` with tests for existing/missing pointer behavior. | `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:230`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:234`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:235`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:238`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:239`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:240`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:241` | LOCKED | COMPLETE | COMPLETE | COMPLETE | Impl: `crates/arco-iceberg/src/pointer_store.rs:18`, `crates/arco-iceberg/src/pointer_store.rs:35`, `crates/arco-iceberg/src/pointer_store.rs:73`, export wiring in `crates/arco-iceberg/src/lib.rs:57`. Tests: `crates/arco-iceberg/src/pointer_store.rs:129`, `crates/arco-iceberg/src/pointer_store.rs:150`, `crates/arco-iceberg/src/pointer_store.rs:169`, `crates/arco-iceberg/src/pointer_store.rs:183`. Fresh logs: `.../command-logs/2026-02-16T024038Z-cargo-test-pointer-dyn.log`, `.../command-logs/2026-02-16T024039Z-cargo-test-arco-iceberg.log`. |
| 5.3 | Implement load table handler `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}` with ETag + If-None-Match handling and metadata-location response coverage in tests. | `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:246`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:249`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:252`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:253`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:254`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:255`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:256` | LOCKED | COMPLETE | COMPLETE | COMPLETE | Impl: `crates/arco-iceberg/src/routes/tables.rs:407`. Tests: `crates/arco-iceberg/src/routes/tables.rs:1672`, `crates/arco-iceberg/src/routes/tables.rs:1707`, `crates/arco-iceberg/src/routes/tables.rs:1729`, `crates/arco-iceberg/src/routes/tables.rs:1774`. Fresh log: `.../command-logs/2026-02-16T024039Z-cargo-test-arco-iceberg.log`. |
| 5.4 | Implement HEAD table handler `HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}` with tests including existing-table `200 + ETag`, non-existent `404`, and no-body response. | `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:262`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:265`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:268`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:269`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:270`, `docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:271` | BLOCKED-SCOPE | PARTIAL | PARTIAL | PARTIAL | Current branch/spec fixture behavior is `204` for existing tables without ETag (`crates/arco-iceberg/tests/fixtures/iceberg-rest-openapi.yaml:1174`, `crates/arco-iceberg/tests/fixtures/iceberg-rest-openapi.yaml:1182`) and handler currently returns `204` (`crates/arco-iceberg/src/routes/tables.rs:603`). Clarification required: should implementation follow plan step (`200 + ETag`) or official fixture/spec behavior (`204`)? |

## Evidence Summary

- Implementation evidence:
  - `crates/arco-iceberg/src/pointer_store.rs`
  - `crates/arco-iceberg/src/routes/tables.rs`
  - `crates/arco-iceberg/src/lib.rs`
- Test/log evidence:
  - `release_evidence/2026-02-12-prod-readiness/phase-5/batch-5-head/command-logs/2026-02-16T024038Z-cargo-test-pointer-dyn.log`
  - `release_evidence/2026-02-12-prod-readiness/phase-5/batch-5-head/command-logs/2026-02-16T024038Z-cargo-test-head-missing-pointer.log`
  - `release_evidence/2026-02-12-prod-readiness/phase-5/batch-5-head/command-logs/2026-02-16T024039Z-cargo-test-arco-iceberg.log`
  - `release_evidence/2026-02-12-prod-readiness/phase-5/batch-5-head/command-logs/2026-02-16T024042Z-jq-validate-gate-tracker.log`
  - `release_evidence/2026-02-12-prod-readiness/phase-5/batch-5-head/command-logs/2026-02-16T024042Z-checklist-blocked-scope-check.log`
- Command matrix linkage:
  - `release_evidence/2026-02-12-prod-readiness/phase-5/batch-5-head/command-matrix-status.tsv`

## Scope Clarification Required

- `Task 5.4` conflict:
  - Plan task step requires `HEAD existing table returns 200 + ETag` (`docs/plans/2025-12-22-iceberg-rest-phase-a-implementation.md:268`).
  - Current official spec fixture requires `204` for HEAD existence checks (`crates/arco-iceberg/tests/fixtures/iceberg-rest-openapi.yaml:1182`).
  - Current implementation follows fixture/spec (`crates/arco-iceberg/src/routes/tables.rs:603`) and OpenAPI compliance tests pass.
- Required decision:
  - Choose one: `A) plan-driven 200 + ETag` or `B) spec/fixture-driven 204`.
