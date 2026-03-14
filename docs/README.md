# Documentation Map

This directory holds Arco project documentation beyond the top-level project
files in repository root.

## Canonical Entry Points

- Quick project overview: `README.md`
- User/developer guide (mdBook source): `docs/guide/src/`
- High-level docs index: `docs/guide/src/reference/documentation-map.md`

## Documentation Areas

- Architecture decisions: `docs/adr/`
- Operations runbooks: `docs/runbooks/`
- Parity program tracking: `docs/parity/`
- Active audits: `docs/audits/`
- Engine compatibility notes: `docs/iceberg-engine-compatibility.md`

## Release and Governance Docs

These live at repository root for visibility:

- Release process: `RELEASE.md`
- Changelog: `CHANGELOG.md`
- Release notes: `release_notes/`
- Contribution/governance/community: `CONTRIBUTING.md`, `GOVERNANCE.md`,
  `COMMUNITY.md`, `SUPPORT.md`, `ROADMAP.md`, `CODE_OF_CONDUCT.md`

## Hygiene Rules

- Durable claims should reference code, tests, ADRs, and CI workflows.
- Transient logs/evidence belong in CI artifacts and release assets.
- Keep docs current; remove stale plans/evidence dumps from tracked tree.
