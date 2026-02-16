# Gate 1 External Handoff Checklist

This checklist is required to close remaining external parts of `G1-001` and `G1-004`.

## Prerequisites

1. Push signed tag to origin (example: `v0.1.0` or next release tag):
   - `git push origin <tag>`
2. Ensure GitHub Actions permissions allow:
   - `contents: write`
   - `attestations: write`
   - `id-token: write`

## Required Remote Execution

1. Run `.github/workflows/release-sbom.yml` for the pushed signed tag.
2. Capture and archive:
   - workflow run URL
   - run ID
   - artifact IDs for `release-sbom-<tag>` and `release-evidence-<tag>`
3. Confirm release assets attached to the tag:
   - `arco-<tag>.spdx.json`
   - `arco-<tag>.cyclonedx.json`
   - `arco-<tag>.sbom.sha256`
4. Confirm artifact retention metadata is `90` days for uploaded artifacts.
5. Confirm provenance attestation exists for SBOM artifact subjects.

## Evidence Update Actions

1. Append run URL + IDs to:
   - `release_evidence/2026-02-12-prod-readiness/gate-1/g1-001-signed-tag-provenance-evidence.md`
   - `release_evidence/2026-02-12-prod-readiness/gate-1/g1-004-sbom-retention-evidence.md`
2. Re-evaluate statuses in:
   - `docs/audits/2026-02-12-prod-readiness/signal-ledger.md`
   - `docs/audits/2026-02-12-prod-readiness/gate-tracker.json`
   - `docs/audits/2026-02-12-prod-readiness/summary.md`
