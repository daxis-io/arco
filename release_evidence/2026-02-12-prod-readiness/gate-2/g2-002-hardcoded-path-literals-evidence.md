# G2-002 Evidence - Hardcoded Path Literals Removed in Flow/API/Iceberg Modules

- Signal: `G2-002`
- Ledger criterion: `No hardcoded path literals remain in targeted modules.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md:27`)
- Status: `GO`

## Implementation Evidence

1. Flow orchestration and outbox path writes consume typed path helpers.
   - `crates/arco-flow/src/orchestration/ledger.rs:83`
   - `crates/arco-flow/src/outbox.rs:110`
2. API manifest/backfill idempotency paths and manifest object paths consume helper functions.
   - `crates/arco-api/src/routes/manifests.rs:437`
   - `crates/arco-api/src/routes/manifests.rs:647`
   - `crates/arco-api/src/routes/orchestration.rs:1931`
3. Iceberg pointer listing/parsing uses canonical path helper behavior.
   - `crates/arco-iceberg/src/pointer.rs:583`

## Verification Evidence

1. Runbook hardcoded-path scan returned expected no-match result in targeted scoped files.
   - `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` (`targeted_no_hardcoded_path_literals`)
2. Flow/API matrix suites passed with path helper call sites active.
   - `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` (`matrix_test_flow_tests`, `matrix_test_api_all_features_tests`)
