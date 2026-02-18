# G1-004 Evidence - SBOM Publication Linked to Release Artifact Retention

- Signal: `G1-004`
- Ledger criterion: `Published SBOM linked to release artifact and retained per policy.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md`)
- Status: `GO`

## Implementation Evidence

1. Release SBOM workflow builds SPDX + CycloneDX + checksum assets for release tags.
   - `.github/workflows/release-sbom.yml`
2. Retention policy is set in workflow artifact uploads.
   - `.github/workflows/release-sbom.yml`
3. Workflow attaches SBOM files to the GitHub release.
   - `.github/workflows/release-sbom.yml`
4. Workflow archives retained release evidence alongside SBOM artifact pack.
   - `.github/workflows/release-sbom.yml`

## Verification Evidence (`v0.1.4`)

1. Release SBOM workflow succeeded.
   - <https://github.com/daxis-io/arco/actions/runs/22153366267>
2. Release page includes all required SBOM assets.
   - <https://github.com/daxis-io/arco/releases/tag/v0.1.4>
   - `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` (`release_assets_v0_1_4`)
3. Workflow artifacts `release-sbom-v0.1.4` and `release-evidence-v0.1.4` are retained with explicit expiration metadata.
   - `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` (`release_sbom_artifacts`)
4. SBOM job step list confirms successful generation, upload, release attachment, and retained evidence upload.
   - `release_evidence/2026-02-12-prod-readiness/gate-1/verification-notes.md` (`release_sbom_job_steps`)
