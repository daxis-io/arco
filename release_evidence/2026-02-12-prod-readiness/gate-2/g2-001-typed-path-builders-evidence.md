# G2-001 Evidence - Unified Typed Path Builders (Non-Catalog Domains)

- Signal: `G2-001`
- Ledger criterion: `Flow/API/Iceberg path callsites migrated; hardcoded path debt removed.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md:26`)
- Status: `GO`

## Implementation Evidence

1. Canonical typed path APIs are defined centrally for flow, API, and Iceberg domains.
   - `crates/arco-core/src/flow_paths.rs:10`
   - `crates/arco-core/src/flow_paths.rs:54`
   - `crates/arco-core/src/flow_paths.rs:98`
2. Flow, API, and Iceberg modules route path construction through typed helpers.
   - `crates/arco-flow/src/paths.rs:6`
   - `crates/arco-api/src/paths.rs:7`
   - `crates/arco-iceberg/src/paths.rs:14`
3. Contract tests lock canonical path outputs for all three domains.
   - `crates/arco-core/tests/flow_paths_contracts.rs:9`

## Verification Evidence

1. Typed path contracts passed in targeted Gate 2 verification.
   - `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` (`targeted_flow_paths_contracts`)
2. Flow/API all-feature test matrices passed after targeted checks.
   - `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` (`matrix_test_flow_all_features_tests`, `matrix_test_api_all_features_tests`)
