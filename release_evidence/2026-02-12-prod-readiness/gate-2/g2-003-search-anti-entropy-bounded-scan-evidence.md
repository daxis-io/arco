# G2-003 Evidence - Search Anti-Entropy Bounded Scan Path

- Signal: `G2-003`
- Ledger criterion: `Search domain scan uses bounded listing + cursor progression test.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md:28`)
- Status: `GO`

## Implementation Evidence

1. Search anti-entropy pass reads domain watermark before bounded listing decisions.
   - `crates/arco-compactor/src/anti_entropy.rs:436`
2. Search anti-entropy bounded scan cursor progression test is present and scoped to search domain behavior.
   - `crates/arco-compactor/src/anti_entropy.rs:824`

## Verification Evidence

1. Targeted bounded-scan cursor test passed.
   - `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` (`targeted_search_anti_entropy_bounded_scan`)
2. Workspace all-features tests (excluding flow/api matrix command) passed.
   - `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` (`matrix_test_workspace_all_features_excl_flow_api`)
