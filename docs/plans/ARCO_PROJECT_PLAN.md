# Arco Project Plan

> High-level project planning for operational excellence, professional Rust engineering, and open source incubation.

**Version:** 1.0
**Status:** Draft
**Last Updated:** January 2025

---

## 1. Executive Summary

Arco is a **serverless lakehouse infrastructure** project developed by Daxis and open sourced to advance the data ecosystem. The platform unifies a file-native **catalog** and an execution-first **orchestration** layer into one operational metadata system. It stores metadata as immutable, queryable files on object storage and treats deterministic planning, replayable history, and explainability as product requirements, not optional tooling.

### Key Differentiators

| Differentiator | Description |
|----------------|-------------|
| **Metadata as files** | Parquet-first storage for catalog and operational metadata, optimized for direct SQL access |
| **Query-native reads** | Browser and server query engines read metadata directly (signed URLs + SQL), eliminating always-on read infrastructure |
| **Lineage-by-execution** | Lineage is captured from real runs (inputs/outputs/partitions), not inferred from SQL parsing |
| **Two-tier consistency** | Strong consistency for low-frequency DDL-like changes; eventual consistency for high-volume operational facts |
| **Tenant isolation by construction** | Isolation is enforced at storage layout, service boundaries, and test gates |
| **Production-proven** | Battle-tested in Daxis's multi-tenant analytics platform |

### Target Users and Use Cases

- **Platform operators** shipping a multi-tenant data platform that must scale to many idle tenants with near-zero baseline cost
- **Data engineers** needing fast discovery, reliable lineage, and debuggable executions across environments
- **Pipeline developers** authoring assets (SQL/Python/etc.) who need deterministic planning and local-to-prod parity
- **Organizations** seeking open, vendor-neutral catalog solutions built on the lakehouse ecosystem (Iceberg, Delta, Parquet)

### Success Metrics

| Category | Metric | Target |
|----------|--------|--------|
| Economics | Idle tenant cost | ~$0 at rest |
| Performance | Catalog discovery P95 | < 500ms |
| Performance | Tier 1 write operations | Predictable, bounded |
| Correctness | Execution lineage coverage | 100% |
| Correctness | Cross-tenant data leakage | Zero |
| Operability | Time-to-first-successful-asset (TTFSA) | < 30 minutes |
| Operability | Mean-time-to-debug (MTTD) | < 15 minutes |
| Operability | Local-to-prod parity | > 95% |

### Next Steps

- [ ] Publish a single "North Star" scorecard (economics, latency, correctness, operability, community) and wire CI + observability to report it
- [ ] Declare stability levels per crate and the compatibility policy before publishing any crates
- [ ] Establish the initial maintainers set, CODEOWNERS map, and review standards to prevent architecture drift

---

## 2. Repository Structure & Organization

### 2.1 Workspace Layout

```
arco/
├── Cargo.toml                     # Workspace root
├── crates/
│   ├── arco-core/                 # Core abstractions (types, storage traits, tenant context)
│   ├── arco-catalog/              # Catalog service (registry, lineage, search, writers, readers)
│   ├── arco-flow/                 # Orchestration engine (planning, scheduling, state machine)
│   ├── arco-api/                  # HTTP/gRPC composition layer (auth, routing, wiring)
│   ├── arco-compactor/            # Compaction binary (Cloud Function / job entrypoint)
│   ├── arco-proto/                # Protobuf definitions (prost-generated)
│   └── ...
├── proto/
│   └── arco/v1/                   # Canonical .proto files
├── python/
│   └── arco/                      # Python SDK (@asset decorator, CLI, worker runtime)
├── docs/
│   ├── adr/                       # Architecture Decision Records
│   ├── guide/                     # mdBook user guide
│   └── plans/                     # Design docs (historical)
├── examples/                      # Runnable examples (smoke tests for docs + APIs)
├── benches/                       # Criterion benchmarks
├── tools/                         # Dev tooling (release, lint, generators, xtask)
├── infra/                         # IaC and deployment templates (Terraform, manifests)
└── tests/
    └── integration/               # Cross-crate integration tests
```

### 2.1.1 Crate Boundary Principles

| Crate | Responsibility | Ownership |
|-------|----------------|-----------|
| `arco-core` | Shared domain primitives: tenant context, IDs, common error types, storage traits, canonical serialization helpers | Only crate allowed to define shared primitives |
| `arco-catalog` | Catalog write/read surfaces and durability model: Tier 1 snapshot/manifest lifecycle and Tier 2 event log + compaction pipeline | Catalog domain |
| `arco-flow` | Orchestration control-plane behavior: planning, scheduling, run state, and cross-language contracts shared with SDK/worker runtimes | Orchestration domain |
| `arco-api` | Thin composition layer: authn/z, config, server wiring, metrics, routing | No domain policy |

Cross-component interaction happens via explicitly versioned contracts (Rust types + protobuf/JSON schemas + integration tests). No implicit coupling via internal modules.

### 2.2 Crate Naming Conventions

| Pattern | Use Case | Example |
|---------|----------|---------|
| `arco-{name}` | Public API crates (publishable) | `arco-core`, `arco-catalog`, `arco-flow` |
| `arco-{name}-internal` | Internal implementation details (not published) | `arco-flow-store-internal` |
| `arco-{name}-test` | Test utilities for downstream crates | `arco-test`, `arco-catalog-test` |

Only crates with an explicit stability promise, `#![deny(missing_docs)]`, and a documented MSRV are publishable.

### 2.3 Module Organization Standards

**Public vs Internal Boundaries:**

- `src/lib.rs` exposes only stable modules; everything else is `pub(crate)` by default
- Internal implementation modules live under `src/internal/` and are never re-exported
- Cross-crate sharing happens via `arco-core` only; avoid "utility dumping" in other crates

**Re-export Patterns:**

- Use a `prelude` module per crate for ergonomic imports
- Use explicit `pub use` for stable symbols; avoid glob re-exports except for generated code

**Feature Flag Conventions:**

| Feature | Purpose |
|---------|---------|
| `default` | Minimal, production-safe defaults |
| `full` | All optional integrations enabled |
| `gcp` | Google Cloud Platform integrations |
| `aws` | Amazon Web Services integrations |
| `azure` | Azure integrations |
| `test-utils` | Testing utilities (never enabled by default) |

**Next Steps:**

- [ ] Create `crates/README.md` documenting crate responsibilities, stability level, and publishability
- [ ] Add `docs/adr/0001-repo-structure.md` documenting crate boundaries and the "no shared utilities outside arco-core" rule
- [ ] Add `cargo xtask` commands to validate workspace conventions (naming, feature policy, publishability)

---

## 3. Development Workflow & CI/CD

### 3.1 Branch Strategy

| Branch | Purpose | Protection Rules |
|--------|---------|------------------|
| `main` | Stable, always releasable | 2 approvals, passing CI, linear history, no force push, CODEOWNERS review |
| `feature/{description}` | Feature development | CI must pass before merge |
| `release/v{X.Y}` | Release stabilization | Maintainer approval, only stabilization/docs/release tooling |
| `hotfix/{description}` | Critical fixes | Expedited single-maintainer approval, postmortem required |

### 3.2 Commit Standards

- **Format**: [Conventional Commits](https://www.conventionalcommits.org/) v1.0.0
- **Types**: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `perf`, `ci`
- **Scopes**: Crate names (`catalog`, `flow`, `core`, `api`, `python`)
- **Breaking changes**: `feat!:` or `fix!:` with `BREAKING CHANGE:` footer
- **DCO sign-off**: Required (`Signed-off-by:` line via `git commit -s`)
- **Signed commits**: Required for maintainers and release tags
- **Commit template**: `.gitmessage` in repo, documented in `CONTRIBUTING.md`

### 3.3 CI Pipeline (GitHub Actions)

```yaml
# Fast feedback (merge blockers)
jobs:
  lint:        # cargo fmt --check, cargo clippy -- -D warnings
  test:        # cargo nextest run --workspace --all-features
  docs:        # cargo doc --workspace --no-deps --document-private-items
  msrv:        # cargo msrv verify
  python:      # pytest, mypy, ruff
  contracts:   # protobuf generation + schema validation

# Slower checks (scheduled + release branches)
jobs:
  audit:       # cargo deny check, cargo audit, cargo vet
  coverage:    # cargo llvm-cov --workspace --codecov (80% minimum)
  miri:        # cargo miri test (unsafe code paths, nightly)
  fuzz:        # cargo-fuzz targets
  bench:       # cargo bench (with regression detection via critcmp)
  release:     # cargo-release, crates.io publish (on tag)
```

### 3.4 Required Status Checks

| Check | Description | Blocking |
|-------|-------------|----------|
| `lint` | Zero clippy warnings, formatted code | Yes |
| `test` | All tests pass on Linux, macOS, Windows | Yes |
| `docs` | Documentation builds without warnings | Yes |
| `msrv` | Builds on minimum supported Rust version | Yes |
| `contracts` | Protobuf + schema validation passes | Yes |
| `audit` | No known vulnerabilities, all dependencies vetted | Yes |
| `coverage` | Minimum 80% line coverage | Yes |
| `miri` | Memory safety for unsafe code | Release only |
| `bench` | No performance regressions beyond threshold | Release only |

**CI Implementation Expectations:**

- Cache cargo registry, git dependencies, and build artifacts
- Split fast feedback from slower checks; gate merge on fast feedback
- Publish artifacts from CI (docs preview, SBOM, test reports) for traceability
- Feature-matrix CI job (`cargo hack`) to prevent "works only with default features" regressions

**Next Steps:**

- [ ] Stand up CI as a required gate before accepting external contributions
- [ ] Add feature-matrix CI job to prevent feature-flag regressions
- [ ] Add coverage reporting and publish coverage badges for transparency
- [ ] Configure Dependabot/Renovate for automated dependency updates

---

## 4. Code Quality Standards

### 4.1 Rust Edition & MSRV Policy

| Policy | Value | Rationale |
|--------|-------|-----------|
| Edition | 2024 | Latest stable features |
| MSRV | N-2 (1.75+) | Balance stability with ecosystem |
| MSRV bumps | Minor version only | Announced in CHANGELOG with migration guide |

### 4.2 Workspace Linting Configuration

```toml
# Cargo.toml (workspace root)
[workspace.lints.rust]
unsafe_code = "forbid"
missing_docs = "deny"
rust_2018_idioms = "deny"
trivial_casts = "deny"
unused_lifetimes = "deny"
unused_qualifications = "deny"

[workspace.lints.clippy]
all = "deny"
pedantic = "warn"
nursery = "warn"
cargo = "warn"
# Critical denies
unwrap_used = "deny"
expect_used = "deny"
panic = "deny"
indexing_slicing = "warn"
todo = "warn"
# Allow where justified (with comments)
module_name_repetitions = "allow"
must_use_candidate = "allow"
```

**Additional Rules:**

- `#![deny(unsafe_code)]` in crates where unsafe is not required
- Track TODOs via issues; do not merge new TODOs without an owner

### 4.3 Error Handling Standards

| Context | Approach |
|---------|----------|
| Library crates | `thiserror` with structured error types |
| Binary crates | `anyhow` with `.context()` chains |
| FFI boundaries | Explicit `Result` with error codes |
| Async code | No panics; propagate errors via `?` |

- Add context at every boundary crossing (I/O, parsing, network)
- No `.unwrap()` / `.expect()` in library code
- Panics reserved for truly unreachable states; must be documented

### 4.4 Documentation Standards

- `#![deny(missing_docs)]` in all published crates
- Doc examples must compile (`cargo test --doc`)
- Public APIs include runnable examples
- Complex modules have module-level documentation
- ADRs numbered and dated (`docs/adr/adr-NNN-*.md`)

**Required ADRs:**

- Storage layout and snapshot semantics
- Consistency guarantees and tiering
- Multi-tenancy boundaries and authorization model
- Deterministic planning guarantees
- Contract versioning and API stability policies

### 4.5 Testing Pyramid

| Level | Tool | Target | Required For |
|-------|------|--------|--------------|
| Unit | `cargo test` | 80%+ per crate | All crates |
| Integration | `cargo nextest` | Critical paths | Cross-crate contracts |
| Property | `proptest` | Edge cases | IDs, schema evolution, snapshot diffs |
| Fuzz | `cargo-fuzz` | Security | Parsers, serializers, untrusted inputs |
| Benchmark | `criterion` | Regression tracking | Performance-critical paths |

**Integration Test Requirements:**

- Cross-crate compatibility (catalog ↔ orchestration contracts)
- Multi-tenant isolation boundaries (path prefixes, auth checks, signed URL scope)
- Idempotent write paths and replay workflows

**Next Steps:**

- [ ] Add "testing pyramid" doc describing what belongs in unit vs integration vs e2e
- [ ] Create golden integration test harness that runs fully local (no cloud dependencies)
- [ ] Track coverage and benchmark deltas as release blockers for core crates

---

## 5. Dependency Management

### 5.1 Dependency Policy

| Criterion | Requirement |
|-----------|-------------|
| Size | Prefer small, focused crates over kitchen-sink dependencies |
| Maintenance | Active releases, clear ownership, responsive to issues |
| Security | Clean audit history, no unaddressed advisories |
| License | Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC, Zlib |
| Compatibility | Prefer `no_std` where it doesn't compromise ergonomics |

GPL/LGPL dependencies are not permitted in library crates.

### 5.2 Supply Chain Security

```toml
# deny.toml
[advisories]
vulnerability = "deny"
unmaintained = "warn"
yanked = "deny"

[licenses]
allow = ["MIT", "Apache-2.0", "BSD-2-Clause", "BSD-3-Clause", "ISC", "Zlib"]
copyleft = "deny"

[bans]
multiple-versions = "warn"
wildcards = "deny"
```

| Practice | Tool | Frequency |
|----------|------|-----------|
| Vulnerability scanning | `cargo audit` | Every CI run |
| Dependency review | `cargo vet` | Every new dependency |
| License compliance | `cargo deny` | Every CI run |
| SBOM generation | `cargo-sbom` | Every release |
| Signed releases | `cosign` | Every release |

### 5.3 Workspace Dependency Inheritance

```toml
# Cargo.toml (workspace root)
[workspace.dependencies]
tokio = { version = "1.40", features = ["full"] }
serde = { version = "1", features = ["derive"] }
thiserror = "2"
tracing = "0.1"
# Member crates use: tokio = { workspace = true }
```

- Commit `Cargo.lock` for binaries and workspace reproducibility
- Automate updates via Dependabot/Renovate with grouped patch updates
- Manual review required for new major versions
- Security updates prioritized

**Next Steps:**

- [ ] Create `deny.toml` and enforce it in CI from day one
- [ ] Add "approved dependencies" checklist to PR templates
- [ ] Track dependency freshness and advisories in weekly automation report

---

## 6. API Design Principles

### 6.1 Stability Guarantees

| Crate | Stability | SemVer | Notes |
|-------|-----------|--------|-------|
| `arco-core` | Stable after 1.0 | Strict | Foundation types |
| `arco-catalog` | Stable after 1.0 | Strict | Catalog domain |
| `arco-flow` | Stable after 1.0 | Strict | Orchestration domain |
| `arco-api` | Stable | HTTP/gRPC versioned | Composition layer |
| `arco-proto` | Wire-compatible | Protobuf evolution rules | Cross-language contracts |

### 6.2 Deprecation Policy

- Deprecated APIs marked with `#[deprecated(since = "X.Y", note = "...")]`
- Deprecated items remain for **2 minor versions** minimum (unless security-critical)
- Migration guide provided in changelog
- `RUSTFLAGS=-Awarnings` not permitted in CI

### 6.3 Breaking Change Policy

- Require an ADR for any breaking change
- Require migration guide and changelog entry
- Announce in release notes with upgrade path
- Strict SemVer compliance for published crates

### 6.4 Async Runtime Policy

| Principle | Implementation |
|-----------|----------------|
| Runtime-agnostic | Core types don't depend on specific runtime |
| Tokio default | `tokio` feature enabled by default for convenience |
| No blocking | All I/O operations are async; `spawn_blocking` for CPU-bound |
| Performance | `async-trait` allowed at boundaries; avoid in hot paths |

### 6.5 Serialization Standards

- All public types implement `Serialize`/`Deserialize`
- Protobuf for wire format (gRPC, queues)
- JSON for debugging and configuration
- Schema evolution via optional fields and `#[serde(default)]`
- Deterministic encoding: canonical ordering for maps/sets
- Stable snapshot formats to support replay and diff workflows

**Next Steps:**

- [ ] Add API review checklist (ergonomics, safety, error model, serialization stability)
- [ ] Publish stability matrix table in README for each crate
- [ ] Add contract tests between orchestration events and catalog ingestion

---

## 7. Security Practices

### 7.1 Security Policy (SECURITY.md)

| Element | Specification |
|---------|---------------|
| Disclosure | <security@daxis.io> for private vulnerability reports |
| Response SLA | Acknowledgment within 48 hours |
| Fix SLA | Patch within 90 days |
| Advisories | GitHub Security Advisories + RustSec |
| CVE assignment | Requested for confirmed vulnerabilities |

### 7.2 Supply Chain Security

| Practice | Tool | Frequency |
|----------|------|-----------|
| Vulnerability scanning | `cargo audit` | Every CI run |
| Dependency review | `cargo vet` | Every new dependency |
| License compliance | `cargo deny` | Every CI run |
| SBOM generation | `cargo-sbom` | Every release |
| Signed releases | `cosign` | Every release |
| Container scanning | Trivy/Grype | Every image build |

**Reproducible Builds Goal:**

- Hermetic CI builds
- Locked dependencies
- Documented build steps
- Verifiable artifacts

### 7.3 Code Security Standards

| Practice | Implementation |
|----------|----------------|
| No unsafe | `#![forbid(unsafe_code)]` where possible |
| Safety comments | `// SAFETY:` required for all unsafe blocks |
| Input validation | Validate at all trust boundaries |
| Secrets | Never commit; use env vars + secret managers |
| Timing attacks | Constant-time comparison for security-sensitive data |
| Secret scanning | Enabled in CI as merge blocker |

### 7.4 Infrastructure Security

- Signed commits required for releases
- Branch protection on `main` and `release/*`
- CODEOWNERS for security-sensitive paths
- Minimal CI permissions (principle of least privilege)
- Regular dependency audit cadence with maintainer rotation

**Next Steps:**

- [ ] Add secret scanning and dependency scanning as merge blockers
- [ ] Produce threat model ADR for multi-tenant isolation and signed URL access
- [ ] Establish regular dependency audit cadence with maintainer rotation

---

## 8. Performance Engineering

### 8.1 Benchmarking Strategy

| Benchmark Type | Tool | Location | Frequency |
|----------------|------|----------|-----------|
| Microbenchmarks | `criterion` | `benches/` | On demand + release |
| Scenario benchmarks | Custom harness | `tests/benchmarks/` | Release branches |
| Regression detection | `critcmp` | CI | On main + release |

**Scenario Benchmarks Required:**

- Common catalog discovery queries
- Snapshot read + "fresh read" merge paths
- Orchestration plan generation and state transitions

### 8.2 Performance Budgets

| Operation | P95 Target |
|-----------|------------|
| Catalog table lookup | < 50ms |
| Full catalog scan (1000 tables) | < 500ms |
| Lineage traversal (5 hops) | < 100ms |
| Event compaction (1000 events) | < 5s |
| Plan generation (100 tasks) | < 200ms |

### 8.3 Profiling Workflow

| Analysis Type | Tool |
|---------------|------|
| CPU profiling | `perf` + `flamegraph` |
| Memory profiling | `dhat` |
| Allocation tracking | Custom instrumentation |
| Async runtime | `tokio-console` |

### 8.4 Optimization Guidelines

- Profile before optimizing
- Document performance-critical paths with `// PERF:` comments
- Avoid premature optimization; prioritize correctness and debuggability
- Maintain `PERFORMANCE.md` with guidance and known hot paths

**Next Steps:**

- [ ] Create "performance lab" target that runs benchmarks + generates flamegraphs
- [ ] Add allocation regression checks for critical crates
- [ ] Establish performance triage process for reported issues

---

## 9. Open Source Governance

### 9.1 Licensing

| Element | Specification |
|---------|---------------|
| License | Apache-2.0 OR MIT (dual-license, user's choice) |
| Headers | SPDX identifier in all source files |
| Third-party | `NOTICE` file for attribution |
| Contributions | DCO sign-off required (no CLA) |

### 9.2 Contributor Ladder

| Role | Responsibilities | Requirements |
|------|------------------|--------------|
| Contributor | Submit PRs, report issues | Signed DCO |
| Reviewer | Review PRs, triage issues | Sustained contributions, maintainer nomination |
| Maintainer | Merge PRs, cut releases, architectural decisions | Demonstrated expertise, unanimous maintainer approval |
| Emeritus | Advisory, recognition | Stepped back from active maintenance |

### 9.3 Decision Making

| Decision Type | Process |
|---------------|---------|
| Minor decisions | Single maintainer approval |
| Significant changes | RFC process with 2-week community feedback period |
| Architectural decisions | Documented as ADRs, maintainer consensus |
| Disputes | Escalate to project lead; final decision documented |

### 9.4 Community Infrastructure

| Channel | Purpose |
|---------|---------|
| GitHub Discussions | Q&A, RFCs, announcements |
| Discord | Real-time chat, community support |
| Monthly community call | Roadmap updates, demos, feedback |
| `GOVERNANCE.md` | Decision-making process |
| `MAINTAINERS.md` | Current maintainers and emeritus |

### 9.5 Code Review Standards

| Change Type | Required Reviews |
|-------------|------------------|
| Standard changes | 1 approval |
| API/format changes | 2 approvals |
| Security/isolation changes | Security reviewer + maintainer |

**Review Turnaround SLAs:**

- Acknowledge PRs within 48 hours
- Provide actionable feedback or approval within 72 hours

### 9.6 Release Process

| Element | Implementation |
|---------|----------------|
| Versioning | Semantic versioning (strict) |
| Changelog | Automated via `git-cliff` |
| Release notes | Highlights, breaking changes, migration notes, security fixes |
| crates.io | `cargo publish --dry-run` in CI, publish in dependency order |
| Verification | Verify docs.rs builds post-publish |

**Next Steps:**

- [ ] Publish governance docs before accepting non-trivial external contributions
- [ ] Add maintainer rotation and escalation policy for stuck PRs
- [ ] Automate release checklists and publishing order to reduce manual risk

---

## 10. Documentation Strategy

### 10.1 Documentation Layers

| Layer | Tool | Audience | Location |
|-------|------|----------|----------|
| API Reference | rustdoc | Developers | docs.rs |
| User Guide | mdBook | Operators, users | arco.dev |
| Architecture | ADRs, design docs | Contributors | `docs/adr/` |
| Examples | Runnable code | Everyone | `examples/` |

### 10.2 Documentation Standards

| Standard | Enforcement |
|----------|-------------|
| Public APIs documented | `#![deny(missing_docs)]` in published crates |
| Examples compile | `cargo test --doc` in CI |
| Architecture diagrams | Mermaid (version-controlled) |
| Changelog | `git-cliff` automation |
| Migration guides | Required for breaking changes |

### 10.3 Documentation CI

- `cargo doc` builds without warnings
- Doc examples compile and run (`cargo test --doc`)
- mdBook builds and deploys on merge to main
- Link checker for external references

**Next Steps:**

- [ ] Create mdBook skeleton and publish automatically on releases
- [ ] Add doctest + "examples compile" CI jobs
- [ ] Add docs ownership map (CODEOWNERS) for review routing

---

## 11. Project Milestones

### Phase 0: Foundation (Pre-Alpha)

- [ ] Repository structure finalized
- [ ] CI/CD pipeline operational (all checks passing)
- [ ] Core abstractions defined (`arco-core` types)
- [ ] Contributing guide, DCO, Code of Conduct
- [ ] Initial architecture documentation (ADRs)
- [ ] Governance docs published

**Exit Criteria**: CI green, docs published, ready to accept contributions.

### Phase 1: Core Implementation (Alpha)

- [ ] Catalog MVP (Tier 1 writes, Parquet snapshots, basic reads)
- [ ] Orchestration MVP (DAG execution, state machine, planning)
- [ ] Cross-crate integration tests passing
- [ ] Initial benchmarks established
- [ ] Python SDK alpha (@asset decorator, CLI basics)
- [ ] Local development experience validated

**Exit Criteria**: End-to-end flow works locally, TTFSA < 30 min.

### Phase 2: Hardening (Beta)

- [ ] Third-party security audit
- [ ] Performance optimization pass (meet budgets)
- [ ] API stabilization review
- [ ] Documentation complete (guide + API docs)
- [ ] Deployment guides (GCP, AWS)
- [ ] Multi-tenant isolation tests comprehensive

**Exit Criteria**: Production-ready for Daxis, docs complete, no critical issues.

### Phase 3: Launch (1.0)

- [ ] Public announcement
- [ ] crates.io publish
- [ ] Production deployments (Daxis)
- [ ] Community launch (Discord, blog post)
- [ ] Conference talks / blog posts
- [ ] External contributors onboarded

**Exit Criteria**: Stable release, community active, adoption growing.

**Next Steps:**

- [ ] Convert each checkbox into tracked epic with acceptance criteria and owner
- [ ] Add exit criteria that map to success metrics in Section 1
- [ ] Treat documentation and operability as first-class deliverables in every phase

---

## 12. Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Scope creep | High | High | Clear MVP definition, phase gates, RFC process |
| API instability | Medium | High | Design reviews, deprecation policy, semver strict |
| Performance regressions | Medium | Medium | CI benchmarks, performance budgets, profiling |
| Security vulnerabilities | Low | Critical | Audits, cargo-vet, responsible disclosure |
| Contributor burnout | Medium | High | Sustainable pace, clear ownership, recognition |
| Low adoption | Medium | Medium | Developer experience focus, documentation, examples |
| Multi-tenant isolation regressions | Medium | Critical | Boundary tests, security reviews, least-privilege defaults |
| Format/schema drift | Medium | High | Contract tests, schema evolution rules, ADRs for format changes |
| Operational complexity | Medium | High | Runbooks, SLOs, observability-as-default |

**Next Steps:**

- [ ] Add standing "risk review" issue updated every release
- [ ] Require postmortems for any critical incident or security issue
- [ ] Track risks as labels in GitHub and tie mitigations to concrete PRs

---

## 13. Success Criteria

### Technical

| Criterion | Target |
|-----------|--------|
| CI checks | Pass on every commit |
| Test coverage | ≥ 80% |
| CVE response | No critical/high unpatched > 30 days |
| P95 latency | Within documented budgets |
| Documentation | 100% coverage for public APIs |
| TTFSA | < 30 minutes |
| MTTD | < 15 minutes |

### Community

| Criterion | Target |
|-----------|--------|
| Active contributors | ≥ 5 (excluding Daxis team) |
| Issue triage | Within 48 hours |
| PR review | Within 72 hours |
| Release cadence | Monthly (at minimum) |

### Adoption

| Criterion | Measurement |
|-----------|-------------|
| Visibility | GitHub stars growth trajectory |
| Downloads | crates.io weekly downloads |
| Production use | Deployments beyond Daxis |
| Engagement | Conference talks, blog posts, community contributions |

---

*This document is maintained by the Arco maintainers. For questions or suggestions, open an issue or discussion on GitHub.*
