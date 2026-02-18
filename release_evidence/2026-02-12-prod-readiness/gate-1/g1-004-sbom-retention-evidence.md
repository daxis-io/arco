# G1-004 Evidence - SBOM Publication Linked to Release Artifact Retention

- Signal: `G1-004`
- Ledger criterion: `Published SBOM linked to release artifact and retained per policy.` (`docs/audits/2026-02-12-prod-readiness/signal-ledger.md:25`)
- Status in this batch: `PARTIAL`

## Implementation Evidence

1. SBOM workflow generates SPDX + CycloneDX + checksum manifest per release tag.
   - `.github/workflows/release-sbom.yml:72-80`
2. Retention policy is explicit (`SBOM_RETENTION_DAYS=90`) and applied to uploaded SBOM/evidence artifacts.
   - `.github/workflows/release-sbom.yml:21`
   - `.github/workflows/release-sbom.yml:82-92`
   - `.github/workflows/release-sbom.yml:120-126`
3. SBOM files are linked to GitHub release assets for the same tag.
   - `.github/workflows/release-sbom.yml:102-109`
4. Release process documents retention policy and published asset set.
   - `RELEASE.md:65-73`
5. Immutable collector includes SBOM linkage metadata for the release tag.
   - `tools/collect_release_evidence.sh:131-137`
   - `release_evidence/2026-02-12-prod-readiness/gate-1/collector-packs/v0.1.0/352cba80c843fd9cf54b1ab169bd0ac081c51bde-dc3072977aef3b0572c58498bea6ceddab26fd22/sbom-artifact-linkage.txt`

## Verification Evidence

1. Workflow contains retention and publication wiring:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-full-sbom-retention-and-provenance-present.log`
2. Collector manifest proves linkage metadata and workflow snapshots are included in immutable evidence:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/command-logs/2026-02-16T024416Z-full-collector-manifest-verifies.log`

## Remaining Closure Gap

Local workspace cannot publish assets to GitHub Releases or emit retained GitHub-hosted artifacts/attestations. Final `GO` requires one successful remote run for a pushed signed tag with:

1. release asset URLs for `arco-<tag>.spdx.json`, `arco-<tag>.cyclonedx.json`, and `arco-<tag>.sbom.sha256`;
2. workflow artifact retention metadata (`retention-days=90`);
3. archived run URL and artifact IDs in Gate 1 evidence.
