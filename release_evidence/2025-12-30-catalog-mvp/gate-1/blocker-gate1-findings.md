# Gate 1 Findings - Engineering System

Date: 2025-12-31 (Corrected)
Status: **DEFERRED** (release tag evidence pending)

## CORRECTION NOTICE

Previous version incorrectly stated "No CI workflows". This was an error.
Comprehensive CI workflows exist at `.github/workflows/`.

---

## 1.1 CI Parity and Dependency Pinning

### Current State: **GO**

**CI Workflows** (`.github/workflows/`)
- `ci.yml` - Main CI pipeline with 14 jobs:
  - `gates` - cargo xtask doctor, adr-check, verify-integrity
  - `check` - cargo check --workspace --all-features
  - `fmt` - cargo fmt --all --check
  - `clippy` - cargo clippy -D warnings
  - `test` - cargo test --workspace (split by crate)
  - `python-tests` - Python CLI API tests
  - `docs` - cargo doc with RUSTDOCFLAGS=-D warnings
  - `doc-tests` - cargo test --doc
  - `deny` - cargo deny check bans licenses sources
  - `deny-advisories` - cargo deny check advisories
  - `proto-check` - buf lint and buf breaking
  - `iam-smoke` - IAM smoke tests (main branch only)
  - `compile-negative` - UI/compile-fail tests
- `security-audit.yml` - Security scanning
- `release-sbom.yml` - SBOM generation for releases
- `nightly-chaos.yml` - Nightly chaos testing

**Dependency Pinning**
- `rust-toolchain.toml` pins Rust 1.85 with components
- CI uses `dtolnay/rust-toolchain@stable` with explicit `toolchain: 1.85`
- `Cargo.lock` present and committed
- `cargo-deny@0.18.9` pinned in CI
- `buf@1.47.2` pinned in CI

**CI Parity Evidence**
| Check | Local | CI |
|-------|-------|-----|
| cargo fmt | --check | --all --check |
| cargo clippy | -D warnings | -D warnings |
| cargo test | --workspace --all-features | --workspace --all-features (split) |
| cargo doc | -D warnings | RUSTDOCFLAGS=-D warnings |
| cargo deny | check | check bans licenses sources |
| buf lint | proto/ | proto/ |
| buf breaking | proto/ | proto/ (vs main) |

**Status**: GO - Comprehensive CI with pinned tooling

### Evidence
- `.github/workflows/ci.yml` - 226 lines, 14+ jobs
- `.github/workflows/security-audit.yml` - Security scanning
- `.github/workflows/release-sbom.yml` - SBOM generation
- `.github/workflows/nightly-chaos.yml` - Chaos testing
- `rust-toolchain.toml` - Rust 1.85 pinned
- `Cargo.lock` - Dependency lockfile

---

## 1.2 Supply Chain Governance

### Current State: **GO**

- `deny.toml` configured with license allowlist
- `cargo deny check` runs in CI (bans, licenses, sources)
- Security advisory checking (with continue-on-error for PRs)
- Dependabot configured (`.github/dependabot.yml`)

**Status**: GO - Supply chain governance in place

### Evidence
- `deny.toml` - Configured cargo-deny policies
- `.github/workflows/ci.yml:126-156` - cargo deny jobs
- `.github/dependabot.yml` - Dependabot configuration

---

## 1.3 Release Discipline

### Current State: **DEFERRED**

- `CHANGELOG.md` present (Keep a Changelog format)
- `RELEASE.md` present (minimal release process)
- `Cargo.toml` version: `0.1.0` (pre-release)
- `release-sbom.yml` exists and release process documented

### Blockers
1. **No version tagging evidence**: Cannot verify git tags exist
2. **Pre-1.0 status**: Version 0.1.0 indicates alpha/unstable

### Required Actions for MVP
- Tag first release (v0.1.0 or v1.0.0-alpha)

### Evidence
- `CHANGELOG.md` - Keep a Changelog format
- `RELEASE.md` - Release process documented
- `Cargo.toml` - version = "0.1.0"

---

## Summary

| Item | Status | Notes |
|------|--------|-------|
| 1.1 CI Parity | **GO** | 14-job CI pipeline, pinned tooling |
| 1.2 Supply Chain | **GO** | cargo deny + dependabot |
| 1.3 Release Discipline | **DEFERRED** | Release tag evidence pending; pre-1.0 status |

**Gate 1 Overall**: DEFERRED (release tag evidence pending)

---

## CI Job Reference

```yaml
# .github/workflows/ci.yml jobs:
jobs:
  gates:          # xtask doctor, adr-check, verify-integrity
  check:          # cargo check --workspace
  fmt:            # cargo fmt --all --check
  clippy:         # cargo clippy -D warnings
  test:           # cargo test (workspace + arco-flow split)
  python-tests:   # pytest integration tests
  docs:           # cargo doc -D warnings
  doc-tests:      # cargo test --doc
  deny:           # cargo deny (bans, licenses, sources)
  deny-advisories:# cargo deny advisories
  proto-check:    # buf lint + breaking
  iam-smoke:      # IAM smoke tests (main only)
  compile-negative:# UI/compile-fail tests
```
