# G1-001 Evidence - Signed Release Tags and Provenance Pipeline

- Signal: `G1-001`
- Ledger criterion: `Signed tag and provenance evidence generated for a release tag.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md`)
- Status: `GO`

## Implementation Evidence

1. Release SBOM workflow hard-gates on release-tag discipline and validates annotated signed tags before SBOM publication.
   - `.github/workflows/release-sbom.yml`
2. Release SBOM workflow publishes build provenance attestation for the SBOM subjects.
   - `.github/workflows/release-sbom.yml`
3. Trust root for SSH tag verification is repository-pinned.
   - `.github/release-signers.allowed`
4. Collector records signature verification metadata in release evidence metadata.
   - `tools/collect_release_evidence.sh`

## Verification Evidence (`v0.1.4`)

1. Local signature verification succeeded against repo-pinned signers for `v0.1.4`.
   - `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` (`verify_signed_tag`)
2. Tag-triggered Release SBOM workflow succeeded.
   - <https://github.com/daxis-io/arco/actions/runs/22153366267>
3. SBOM job includes successful `Validate annotated signed release tag` and `Publish build provenance attestation for SBOM artifacts` steps.
   - `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` (`release_sbom_job_steps`)
4. CI run for the same commit has successful `Release Tag Discipline` check-run.
   - <https://github.com/daxis-io/arco/actions/runs/22153366241/job/64050488799>
   - `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` (`release_tag_discipline_check_run`)
