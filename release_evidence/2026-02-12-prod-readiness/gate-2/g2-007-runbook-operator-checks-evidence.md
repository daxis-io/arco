# G2-007 Evidence - Runbook Final Invariants and Operator Checks

- Signal: `G2-007`
- Ledger criterion: `Runbook sections updated and drill-reviewed with evidence.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md:32`)
- Status: `GO`

## Implementation Evidence

1. Gate 2 invariant check section is present in runbook with explicit closure commands for canonicalization, invariants, and failure injection.
   - `docs/runbooks/metrics-catalog.md:30`
   - `docs/runbooks/metrics-catalog.md:34`
   - `docs/runbooks/metrics-catalog.md:52`
   - `docs/runbooks/metrics-catalog.md:61`

## Verification Evidence

1. Runbook Gate 2 section presence check passed.
   - `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` (`targeted_runbook_gate2_section`)
2. All runbook-aligned targeted checks passed (no hardcoded literals, deterministic/property invariants, failure injection).
   - `release_evidence/2026-02-12-prod-readiness/gate-2/verification-notes.md` (`targeted_no_hardcoded_path_literals`, `targeted_property_out_of_order_duplicate`, `targeted_property_crash_replay`, `targeted_partial_write_recovery`, `targeted_pointer_cas_race`)
3. Full command matrix passed and is captured in the Gate 2 command matrix TSV.
   - `release_evidence/2026-02-12-prod-readiness/gate-2/command-matrix-status.tsv`
