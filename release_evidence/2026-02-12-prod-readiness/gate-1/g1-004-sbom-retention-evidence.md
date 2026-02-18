# G1-004 Evidence - SBOM Publication Linked to Release Artifact Retention

- Signal: `G1-004`
- Ledger criterion: `Published SBOM linked to release artifact and retained per policy.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md:25`)
- Status in this batch: `GO`

## Implementation Evidence

1. SBOM workflow emits SPDX + CycloneDX + checksum manifest per release tag.
   - `.github/workflows/release-sbom.yml:162-170`
2. Uploaded artifacts explicitly use 90-day retention policy.
   - `.github/workflows/release-sbom.yml:21`
   - `.github/workflows/release-sbom.yml:172-183`
   - `.github/workflows/release-sbom.yml:213-218`
3. SBOM files are attached to the GitHub release for the same tag.
   - `.github/workflows/release-sbom.yml:192-199`
4. Immutable evidence archive is produced and retained alongside SBOM artifact pack.
   - `.github/workflows/release-sbom.yml:201-218`

## Verification Evidence (v0.1.4)

1. Release SBOM workflow run succeeded for `v0.1.4`:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/release-sbom-run.json`
   - `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/release-sbom-run.log`
2. Step-level success confirms SBOM generation, artifact upload, attestation, release upload, and evidence archive:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/release-sbom-run-jobs.json`
3. Artifact retention metadata confirms retained artifacts with `expires_at` ~90 days after creation:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/release-sbom-run-artifacts.json`
4. GitHub release for `v0.1.4` contains all required SBOM assets:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/release-view.json`
5. Downloaded artifacts prove retained package contents and linkage to release tag outputs:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/downloaded-artifacts.txt`
   - `release_evidence/2026-02-12-prod-readiness/gate-1/release-assets/v0.1.4/release-sbom-v0.1.4/arco-v0.1.4.spdx.json`
   - `release_evidence/2026-02-12-prod-readiness/gate-1/release-assets/v0.1.4/release-sbom-v0.1.4/arco-v0.1.4.cyclonedx.json`
   - `release_evidence/2026-02-12-prod-readiness/gate-1/release-assets/v0.1.4/release-sbom-v0.1.4/arco-v0.1.4.sbom.sha256`
