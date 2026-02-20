# Release Process

This repository uses signed release tags, release-tag CI discipline checks, and SBOM/provenance publishing.

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

## Pre-Release Verification

Run from repository root unless noted:

```bash
cargo xtask repo-hygiene-check
cargo xtask adr-check
cargo xtask parity-matrix-check
cargo check --workspace --all-features
cargo test --workspace --all-features --exclude arco-flow --exclude arco-api
cd docs/guide && mdbook build
bash tools/check-release-tag-discipline.sh --tag vX.Y.Z
```

Also run targeted parity/API gates that mirror CI for `arco-api` and `arco-flow` before final tagging.

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
   - `.github/workflows/release-sbom.yml` for SBOM publication and provenance attestation
7. Ensure the SSH public key used for release-tag signing is present in `.github/release-signers.allowed` so CI can verify `git verify-tag`.

## Post-Release

1. Confirm release artifacts are accessible.
2. Re-open `[Unreleased]` in the changelog.
3. If this is the first clean release after OSS hard-cleanup, retire the temporary archive branch:
   ```bash
   git branch -D <archive-branch-name>
   git push origin --delete <archive-branch-name>
   ```

## Evidence Policy

Transient verification evidence belongs in CI artifacts and release assets, not long-lived tracked directories.
For evidence policy details, see `docs/guide/src/reference/evidence-policy.md`.
