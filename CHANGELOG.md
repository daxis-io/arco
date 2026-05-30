# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-05-28
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
