# Release Process

This project follows semantic versioning and keeps a public changelog.

## Versioning

- Use SemVer: `MAJOR.MINOR.PATCH`
- Pre-release tags allowed (e.g., `0.1.0-alpha.1`)

## Pre-Release Checklist

1. Ensure CI is green on `main`.
2. Update `CHANGELOG.md` under `[Unreleased]` and cut a new version section.
3. Update version numbers:
   - `Cargo.toml`
   - `python/arco/pyproject.toml`
   - Any docs that mention versions
4. Verify docs build: `cargo doc --workspace --all-features --no-deps`
5. Run tests:
   - `cargo test --workspace --all-features`
   - Python tests in `python/arco/`
6. Generate SBOM (CI workflow `release-sbom.yml`).

## Release Steps

1. Create a release branch (optional): `release/vX.Y.Z`.
2. Commit version bumps + changelog.
3. Tag the release:
   ```bash
   git tag -s vX.Y.Z -m "vX.Y.Z"
   ```
4. Push branch + tag:
   ```bash
   git push origin release/vX.Y.Z
   git push origin vX.Y.Z
   ```
5. Create a GitHub Release with:
   - Changelog section for the version
   - SBOM artifact
   - Build/test evidence if needed

## Post-Release

- Verify published artifacts (if any) are accessible.
- Open a new `[Unreleased]` section in `CHANGELOG.md`.
