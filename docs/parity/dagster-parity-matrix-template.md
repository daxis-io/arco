# Dagster Parity Matrix Template

This template is governed by:
- `docs/parity/dagster-parity-01-charter.md`

## Columns
Use a table (Markdown) or a machine-readable format (CSV/TSV). The minimum required columns:

- **Capability**: Dagster concept/feature (semantic behavior).
- **Arco Feature**: the corresponding Arco component(s).
- **Status**: Implemented | Partial | Designed | Planned | Exception.
- **Proof Level Required**: unit | integration | e2e | prod.
- **Evidence (Code)**: file paths and symbols.
- **Evidence (Tests)**: test file + test name.
- **Evidence (CI)**: CI job/command proving it runs.
- **Owner**: person/team.
- **Notes**: limits/SLOs, edge cases, open questions.

## Status Rules (summary)
- **Implemented** requires CI-gated proving tests.
- **Designed/Planned** must not be treated as shipped parity.
- **Exception** must point to `docs/audits/arco-flow-dagster-exception-planning.md`.

## Starter Rows (Examples)
These are intentionally narrow: they prove *event/idempotency semantics*, not full E2E workflows.

| Capability | Arco Feature | Status | Proof Level Required | Evidence (Code) | Evidence (Tests) | Evidence (CI) | Owner | Notes |
|---|---|---|---|---|---|---|---|---|
| Schedule tick idempotency key format | Orchestration events | Implemented | integration | `crates/arco-flow/src/orchestration/events/` | `crates/arco-flow/tests/orchestration_automation_tests.rs` | `.github/workflows/ci.yml` (arco-flow tests) | TBD | Proves deterministic idempotency keys only |
| Push sensor duplicate delivery dedupe | Sensor fold/idempotency keys | Implemented | integration | `crates/arco-flow/src/orchestration/compactor/` | `crates/arco-flow/tests/orchestration_sensor_tests.rs` | `.github/workflows/ci.yml` (arco-flow tests) | TBD | Does not prove Pub/Sub ingest |
| Layer-2 schema primary keys | Parquet row types | Implemented | integration | `crates/arco-flow/src/orchestration/compactor/` | `crates/arco-flow/tests/orchestration_schema_tests.rs` | `.github/workflows/ci.yml` (arco-flow tests) | TBD | Schema invariants only |

## Suggested Grouping
When expanding the matrix, group by domain:
- Scheduling
- Sensors
- Run identity (`run_key`)
- Partitions
- Backfills
- Partition status
- Staleness/reconciliation
- Operator UX (CLI/API)
- Production hardening
