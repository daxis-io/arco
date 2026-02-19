# Release Process

Arco follows semantic versioning and publishes release notes for every tag.

## Versioning

- Use `MAJOR.MINOR.PATCH`.
- Pre-release tags are allowed when needed.

## Pre-Release Verification

Run from repository root unless noted:

```bash
cargo xtask repo-hygiene-check
cargo xtask adr-check
cargo xtask parity-matrix-check
cargo check --workspace --all-features
cargo test --workspace --all-features --exclude arco-flow --exclude arco-api
cd docs/guide && mdbook build
```

Also run targeted parity/API gates that mirror CI for `arco-api` and `arco-flow` before final tagging.

## Release Steps

1. Update `CHANGELOG.md`.
2. Update version metadata as needed.
3. Ensure CI is green on the release commit.
4. Create and push signed tag:
   ```bash
   git tag -s vX.Y.Z -m "vX.Y.Z"
   git push origin vX.Y.Z
   ```
5. Publish GitHub release notes with verification summary and relevant artifacts.

## Post-Release

1. Confirm release artifacts are accessible.
2. Re-open `[Unreleased]` in the changelog.
3. If this is the first clean release after OSS hard-cleanup, retire the temporary archive branch:
   ```bash
   git branch -D <archive-branch-name>
   git push origin --delete <archive-branch-name>
   ```

## Evidence Policy

Transient verification evidence belongs in CI artifacts and release assets, not long-lived tracked directories. See `docs/guide/src/reference/evidence-policy.md`.
