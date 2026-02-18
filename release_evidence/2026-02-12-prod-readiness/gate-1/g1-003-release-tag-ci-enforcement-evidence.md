# G1-003 Evidence - Release-Tag CI Enforcement for Changelog and Release Notes

- Signal: `G1-003`
- Ledger criterion: `Release-tag CI fails when changelog/release notes are incomplete.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md:24`)
- Status in this batch: `GO`

## Implementation Evidence

1. CI now triggers on release tags and has a dedicated release-tag discipline job.
   - `.github/workflows/ci.yml:3-8`
   - `.github/workflows/ci.yml:38-47`
2. Enforcement script validates changelog section/date, release-note structure, and absence of stub tokens.
   - `tools/check-release-tag-discipline.sh:50-53`
   - `tools/check-release-tag-discipline.sh:70-87`
   - `tools/check-release-tag-discipline.sh:89-114`
3. Release docs require running this check before tagging.
   - `RELEASE.md:22-35`

## Verification Evidence

1. Positive path passes on `v0.1.0` with complete release notes/changelog:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-targeted-release-tag-discipline-pass.log`
2. Negative path fails on intentionally incomplete release notes (missing required section), demonstrating fail-closed behavior:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-targeted-release-tag-discipline-expected-fail.log`
3. Workflow wiring proof for CI release-tag enforcement:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-full-ci-release-tag-enforcement-present.log`
