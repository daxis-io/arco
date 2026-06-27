# Documentation Map

Arco documentation is split by function so contributors can find the right
source of truth quickly.

## Start Here

- Project overview: `README.md`
- Guide and reference docs (this mdBook): `docs/guide/src/`
- Repo docs index: `docs/README.md`

## Architecture and Design

- ADR index and records: `docs/adr/README.md`
- Draft contract scaffolds: `docs/spec/README.md`
- Architecture concepts in mdBook:
  - `docs/guide/src/concepts/architecture.md`
  - `docs/guide/src/concepts/catalog.md`
  - `docs/guide/src/concepts/orchestration.md`
- Control-plane scope scorecard:
  - `docs/guide/src/reference/control-plane-scope.md`
- Production target architecture:
  - `docs/guide/src/reference/object-native-catalog-architecture.md`
- Metastore and workspace scope architecture:
  - `docs/guide/src/reference/metastore-scope-architecture.md`
- Catalog product contracts:
  - `docs/adr/adr-037-arco-catalog-product-surface.md`
  - `docs/adr/adr-038-catalog-threat-model.md`
  - `docs/adr/adr-039-catalog-consistency-model.md`
  - `docs/guide/src/reference/catalog-privilege-matrix.md`
  - `docs/guide/src/reference/catalog-api-contract.md`
  - `docs/guide/src/reference/schema-evolution-policy.md`
  - `docs/guide/src/reference/credential-vending-security.md`

## Operations and Readiness

- Runbooks: `docs/runbooks/`
- Active audits: `docs/audits/`
- Parity tracking and matrices: `docs/parity/`

## Release and Governance

- Release process: `RELEASE.md`
- Changelog: `CHANGELOG.md`
- Versioned release notes: `release_notes/`
- Community/governance:
  - `CONTRIBUTING.md`
  - `GOVERNANCE.md`
  - `COMMUNITY.md`
  - `SUPPORT.md`
  - `ROADMAP.md`
  - `CODE_OF_CONDUCT.md`

## Hygiene Policy

Transient evidence should not be tracked as long-lived repository content.
See `docs/guide/src/reference/evidence-policy.md` for policy details.

For repo-grounded "what is actually implemented" checks, use the control-plane
scope scorecard before summarizing architecture status.
