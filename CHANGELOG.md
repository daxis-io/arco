# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Server-side SQL query endpoint at `/api/v1/query` backed by DataFusion (Arrow IPC or JSON output).

## [0.1.2] - 2026-02-18
### Fixed
- Release SBOM CI wait logic now polls commit check-runs for `Release Tag Discipline`, preventing false timeouts on tag releases.

## [0.1.1] - 2026-02-18
### Added
- Gate 1 release-discipline hardening: signed-tag checks, immutable release-evidence collection, and release-tag CI/SBOM enforcement artifacts.

## [0.1.0] - 2025-12-31
### Added
- Initial published baseline for the catalog MVP audit.
