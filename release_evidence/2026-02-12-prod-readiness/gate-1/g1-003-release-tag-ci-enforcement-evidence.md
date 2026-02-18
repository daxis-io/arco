# G1-003 Evidence - Release-Tag CI Enforcement for Changelog and Release Notes

- Signal: `G1-003`
- Ledger criterion: `Release-tag CI fails when changelog/release notes are incomplete.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md`)
- Status: `GO`

## Implementation Evidence

1. CI workflow runs release-tag discipline on `refs/tags/v*` pushes.
   - `.github/workflows/ci.yml`
2. Discipline script fail-closes on missing/incomplete changelog or release-notes content.
   - `tools/check-release-tag-discipline.sh`
3. Release process requires discipline checks before signing/pushing release tags.
   - `RELEASE.md`

## Verification Evidence (`v0.1.4`)

1. Positive path passes for complete release artifacts.
   - `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` (`discipline_pass_v0_1_4`)
2. Negative path fails as expected with intentionally incomplete release notes.
   - `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` (`discipline_expected_fail_v0_1_4`)
3. CI `Release Tag Discipline` check-run is successful for the `v0.1.4` commit.
   - <https://github.com/daxis-io/arco/actions/runs/22153366241/job/64050488799>
   - `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` (`release_tag_discipline_check_run`)
