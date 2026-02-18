# G1-001 Evidence - Signed Release Tags and Provenance Pipeline

- Signal: `G1-001`
- Ledger criterion: `Signed tag and provenance evidence generated for a release tag.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md:22`)
- Status in this batch: `GO`

## Implementation Evidence

1. Release SBOM workflow enforces signed-tag verification before publication and includes provenance attestation.
   - `.github/workflows/release-sbom.yml:128-145`
   - `.github/workflows/release-sbom.yml:184-191`
2. Release-tag CI discipline success is explicitly required before SBOM/provenance steps proceed.
   - `.github/workflows/release-sbom.yml:64-123`
3. Trust root for SSH tag verification is repository-pinned and immutable in source control.
   - `.github/release-signers.allowed:1`
4. Collector captures signature metadata and verification method in immutable pack metadata.
   - `tools/collect_release_evidence.sh:87-110`
   - `tools/collect_release_evidence.sh:152-163`

## Verification Evidence (v0.1.4)

1. Successful release SBOM/provenance run for signed tag `v0.1.4`:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/release-sbom-run.json`
   - `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/release-sbom-run.log`
2. Commit check-run shows `Release Tag Discipline` completed successfully for the same tag commit:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/check-runs.json`
3. SBOM job step conclusions include both signed-tag verification and provenance attestation as `success`:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/github-run/v0.1.4/release-sbom-run-jobs.json`
4. Downloaded retained artifact contains signed tag object and verification transcript:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/release-assets/v0.1.4/release-sbom-v0.1.4/arco-v0.1.4.tag-object.txt`
   - `release_evidence/2026-02-12-prod-readiness/gate-1/release-assets/v0.1.4/release-sbom-v0.1.4/arco-v0.1.4.tag-verify.txt`
5. Immutable collector metadata for `v0.1.4` records `SIGNATURE_VERIFIED=true` with `git-verify-tag` method:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/collector-packs/v0.1.4/24f2e2c0eb1f0c899ac4cb379292a14d5d0b4d3a-b26c3d872f82d4cee3252a0d373f0cc56e13997c/metadata.env`
