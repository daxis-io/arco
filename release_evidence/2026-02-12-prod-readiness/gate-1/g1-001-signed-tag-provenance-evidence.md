# G1-001 Evidence - Signed Release Tags and Provenance Pipeline

- Signal: `G1-001`
- Ledger criterion: `Signed tag and provenance evidence generated for a release tag.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md:22`)
- Status in this batch: `PARTIAL`

## Implementation Evidence

1. Release SBOM/provenance workflow is now release-tag driven and validates signed annotated tags.
   - `.github/workflows/release-sbom.yml:4-6`
   - `.github/workflows/release-sbom.yml:52-61`
2. Provenance attestation pipeline is wired with GitHub attestations + OIDC permissions.
   - `.github/workflows/release-sbom.yml:13-16`
   - `.github/workflows/release-sbom.yml:94-100`
3. Release process explicitly requires signed tags and documents the provenance workflow trigger.
   - `RELEASE.md:36-47`

## Verification Evidence

1. Local signed release tag material for `v0.1.0` (SSH signature block present):
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-targeted-signed-tag-prepared.log`
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-targeted-signed-tag-has-signature-material.log`
2. Immutable collector metadata captures signed tag identity and signature type:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/collector-packs/v0.1.0/352cba80c843fd9cf54b1ab169bd0ac081c51bde-dc3072977aef3b0572c58498bea6ceddab26fd22/metadata.env`
   - `release_evidence/2026-02-12-prod-readiness/gate-1/collector-packs/v0.1.0/352cba80c843fd9cf54b1ab169bd0ac081c51bde-dc3072977aef3b0572c58498bea6ceddab26fd22/tag-object.txt`
3. Workflow provenance controls are present in committed workflow:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-full-sbom-retention-and-provenance-present.log`

## Remaining Closure Gap

Local workspace cannot execute GitHub-hosted `actions/attest-build-provenance` for an origin-pushed release tag. Final `GO` for `G1-001` requires one successful remote run of `.github/workflows/release-sbom.yml` on a pushed signed tag, with run URL and attestation reference captured in Gate 1 evidence.
