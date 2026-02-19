# Gate 1 Evidence

Gate 1 closure evidence for release discipline and provenance is intentionally slimmed to source-controlled proofs and immutable GitHub artifact URLs.

## Evidence Model

- Keep in-repo artifacts human-reviewable and small.
- Treat GitHub Actions run URLs, check-run URLs, release asset URLs, and artifact IDs as canonical immutable evidence.
- Do not commit bulk generated logs, downloaded artifact blobs, or copied API dumps.

## Scope + Signal Docs

- `g1-scope-checklist.md`
- `g1-001-signed-tag-provenance-evidence.md`
- `g1-002-immutable-collector-evidence.md`
- `g1-003-release-tag-ci-enforcement-evidence.md`
- `g1-004-sbom-retention-evidence.md`

## Verification Artifacts

- `verification-notes.md`
- `command-matrix-status.tsv`

## Canonical Remote Evidence (v0.1.4)

- Release SBOM workflow run: <https://github.com/daxis-io/arco/actions/runs/22153366267>
- CI run for the same tag commit: <https://github.com/daxis-io/arco/actions/runs/22153366241>
- `Release Tag Discipline` check-run job: <https://github.com/daxis-io/arco/actions/runs/22153366241/job/64050488799>
- Release page and SBOM assets: <https://github.com/daxis-io/arco/releases/tag/v0.1.4>
