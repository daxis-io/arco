# G1-003 Evidence - Release-Tag CI Enforcement for Changelog and Release Notes

- Signal: `G1-003`
- Ledger criterion: `Release-tag CI fails when changelog/release notes are incomplete.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md:24`)
- Status in this batch: `GO`

## Implementation Evidence

1. CI workflow triggers on release tags and executes `Release Tag Discipline` for `refs/tags/v*`.
   - `.github/workflows/ci.yml:4-8`
   - `.github/workflows/ci.yml:38-49`
2. Discipline script fail-closes on missing/invalid changelog section and missing/incomplete release notes.
   - `tools/check-release-tag-discipline.sh:70-87`
   - `tools/check-release-tag-discipline.sh:89-114`
3. Release process requires running discipline checks before signed tagging.
   - `RELEASE.md:22-39`

## Verification Evidence (v0.1.4)

1. Positive path passes for complete release artifacts:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-18T185606Z-targeted_release_tag_discipline_pass_v0_1_4.log`
2. Negative path fails on intentionally incomplete release notes fixture (expected fail harness passes):
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-18T185607Z-targeted_release_tag_discipline_expected_fail_v0_1_4.log`
3. Tag-triggered CI check-run confirms `Release Tag Discipline` completed with `success` for tag commit:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/check-runs.json`
4. End-to-end gate hardening suite passes with current workflow/script wiring:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-18T185607Z-full_release_gate1_hardening_suite_v0_1_4.log`
