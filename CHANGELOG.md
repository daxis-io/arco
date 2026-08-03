# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Phase 3 state-store prototype gates: the `ArcoStateReader`/`ArcoStateStore`/`ArcoStateTxn` seam with capability-discovery-only current adapter, the deterministic `ModelStateStore` reference backend, the object-store control-store MVP (txlog + manifest + pointer-CAS), and the advisory prototype promotion gate (#316).
- Phase 4 shadow replay importer into an isolated `catalog-shadow` control-store domain plus opt-in internal comparison reads behind `ARCO_CATALOG_SHADOW_COMPARE_READS` (#317).
- Control-store projection outbox acknowledgements: first low-risk writable control-store domain with idempotent acks, token-pinned reads, and freshness/watermark-lag reporting (#319).
- Phase 6A path-governance metadata domain with canonical ancestor/descendant predicate model and deny-closed compiled-state readiness helpers (#318).
- External location and workspace/metastore binding metadata domains with atomic companion-path declarations and secret-free credential references (#320).
- Workspace snapshot export, roll-forward restore with REPAIR_REQUIRED journaling, and durable control-plane transaction handles with review-token workflow (#322).

All state-store program surfaces above are landed with CI-run test suites but are deliberately non-authoritative: crate-private with zero production callers, and the control-store prototype has not passed its Phase 3C promotion gate (see `docs/guide/src/reference/control-plane-scope.md`).

### Changed
- deps(rust): bumped `serde_with` from 3.16.1 to 3.21.0 (#321).

## [0.2.1] - 2026-06-27
### Added
- ADR-041 tiered object-storage orchestration event-log implementation, deterministic local pipeline UAT, and deployed-UAT readiness/provenance guardrails.
- Batch 7 catalog I/O and Tier-1 write-amplification baselines, flow-boundary guardrails, and refreshed crate/architecture documentation.

### Changed
- Promoted workspace crates, Python packages, generated OpenAPI metadata, release notes, and release-prep validation to `0.2.1`.
- Hardened orchestration dispatch, callback, retry, compaction, pagination, and unknown-event handling across API, flow, worker, and Python surfaces.
- Strengthened public API contracts for enum policy, idempotency, JSON casing, pagination, OpenAPI coverage, and compactor lock-race behavior.

### Fixed
- Closed catalog and Iceberg correctness gaps around idempotency markers, stale-marker takeover, credential-vending scope, orphaned snapshot recovery, and public protocol compatibility.
- Applied shared public-route rate limiting, internal-error redaction, thrift allocation bounds, and JWT feature-provider compatibility for Iceberg and Unity Catalog protocol surfaces.
- Preserved deployed UAT as an explicit live-proof gate instead of claiming completion from local or readiness-only evidence.

## [0.2.0] - 2026-05-31
### Added
- Catalog control-plane release surface covering scoped metastore mutations, catalog product APIs, Unity Catalog compatibility inventory, and system-table contracts.
- Orchestration control-plane contracts for root transactions, task-token callbacks, worker dispatch envelopes, and output visibility lifecycle coverage.
- Release gates for CI parity, repository hygiene, integrity checks, proto compatibility, and release-tag discipline.

### Changed
- Raised the supported Rust toolchain and local release-doctor expectation to Rust 1.88.
- Aligned the CI and local release-doctor Buf pin to 1.70.0.
- Promoted workspace crates, Python packages, generated OpenAPI metadata, and release inputs to `0.2.0`.
- Tightened release documentation around catalog governance, storage-governance compatibility, and release verification policy.

### Fixed
- Made Tier-1 catalog snapshot publication retry-safe by accepting byte-identical immutable snapshot collisions while rejecting divergent content.
- Reconciled repository hygiene checks with intentional legacy protobuf removal documentation and internal plan-to-plan references.
- Cleaned vendored Thrift whitespace so release diffs pass whitespace checks.

## [0.1.5] - 2026-04-11
### Added
- Server-side SQL query endpoint at `/api/v1/query` backed by DataFusion with Arrow IPC or JSON output.
- Control-plane transaction APIs and flow cutover wiring for orchestration callback workflows landed on `main`.
- Orchestration output visibility lifecycle coverage now spans the API, worker callbacks, and flow execution surfaces.
### Changed
- Promoted workspace, SDK, and release metadata to `0.1.5` while reserving planned protobuf-breaking contract changes for `2.0.0`.
- Refreshed `README.md`, `RELEASE.md`, and release-note templates to align with mdBook-first docs and CI/release artifact policy.
### Fixed
- Hardened orchestration compactor publication and control-plane repair publication paths on the release line.

## [0.1.4] - 2026-02-18
### Fixed
- Release SBOM now verifies signed release tags against repository-pinned SSH allowed signers (`.github/release-signers.allowed`) to avoid external GitHub-key registration dependency.

## [0.1.3] - 2026-02-18
### Fixed
- Release-tag signature verification now succeeds in GitHub-hosted SBOM workflow by using a verified tagger identity for signed release tags.

## [0.1.2] - 2026-02-18
### Fixed
- Release SBOM CI wait logic now polls commit check-runs for `Release Tag Discipline`, preventing false timeouts on tag releases.

## [0.1.1] - 2026-02-18
### Added
- Gate 1 release-discipline hardening: signed-tag checks, immutable release-evidence collection, and release-tag CI/SBOM enforcement artifacts.
## [0.1.0] - 2025-12-31
### Added
- Initial published baseline for the catalog MVP audit.
