# Release Process

This repository uses signed release tags, release-tag CI discipline checks, and retained SBOM/provenance artifacts.

## Versioning

- Use SemVer tags: `vMAJOR.MINOR.PATCH` (suffixes allowed, for example `v1.2.3-rc.1`).
- Keep `CHANGELOG.md` in [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.

## Required Release Inputs

For each release tag `vX.Y.Z`:

1. `CHANGELOG.md` must include `## [X.Y.Z] - YYYY-MM-DD` with at least one bullet.
2. `release_notes/vX.Y.Z.md` must exist and include:
   - `# Release Notes for vX.Y.Z`
   - `## Highlights`
   - `## Changelog Reference`
   - `## Verification`
3. Release notes must not contain placeholders (`TODO`, `TBD`, `PLACEHOLDER`, `XXX`).

Validate locally before tagging:

```bash
bash tools/check-release-tag-discipline.sh --tag vX.Y.Z
```

## Release Steps

1. Ensure CI is green on `main`.
2. Update versioned release notes (`release_notes/vX.Y.Z.md`).
3. Validate release-tag discipline:
   ```bash
   bash tools/check-release-tag-discipline.sh --tag vX.Y.Z
   ```
4. Create a signed, annotated tag:
   ```bash
   git tag -s vX.Y.Z -m "vX.Y.Z"
   ```
5. Push branch and tag:
   ```bash
   git push origin main
   git push origin vX.Y.Z
   ```
6. Pushing `vX.Y.Z` triggers:
   - `.github/workflows/ci.yml` release-tag discipline enforcement
   - `.github/workflows/release-sbom.yml` SBOM publication, provenance attestation, release asset upload, and immutable evidence pack archive

## Immutable Evidence Collector

Collect deterministic Gate 1 evidence for a release tag:

```bash
bash tools/collect_release_evidence.sh --tag vX.Y.Z
```

Output path is immutable and deterministic:

```text
release_evidence/2026-02-12-prod-readiness/gate-1/collector-packs/vX.Y.Z/<tag-object-sha>-<commit-sha>/
```

The collector refuses overwrites by default.

## SBOM Retention Policy

- Workflow artifact retention: 90 days (`actions/upload-artifact retention-days`).
- Published release assets: attached to GitHub release for the same tag.
- Artifacts produced per release tag:
  - `arco-vX.Y.Z.spdx.json`
  - `arco-vX.Y.Z.cyclonedx.json`
  - `arco-vX.Y.Z.sbom.sha256`
  - Provenance attestation via `actions/attest-build-provenance`

## Post-Release

- Record immutable collector output path in the gate evidence pack.
- Open/maintain `[Unreleased]` entries in `CHANGELOG.md` for subsequent work.
