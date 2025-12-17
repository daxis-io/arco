# Part 2 — Catalog MVP (arco-catalog) Production-Readiness & Completion Audit

Date: **2025-12-17**  
Auditor: Codex CLI (GPT-5.2)  

## Scope and evidence baseline

**Repo state**
- Git HEAD: `b8e48f8` (`test(integration): add Tier 2 walking skeleton tests`)
- Working tree: **DIRTY** (local modifications present; see `git status` for exact list)

**Plan under audit**
- The “Part 2: Catalog MVP (arco-catalog)” execution plan provided in the prompt (milestones 0–7 + DoD).
- Architecture references in-repo, used as the intended design source of truth where the plan aligns:
  - `docs/plans/ARCO_TECHNICAL_VISION.md:133` (two-tier model) and `docs/plans/ARCO_TECHNICAL_VISION.md:144` (four domain manifests)
  - `docs/adr/adr-001-parquet-metadata.md:1` (Parquet-first metadata)
  - Protobuf contracts, esp. Tier-2 event envelope: `proto/arco/v1/event.proto:1`

**What “done” means in this audit**
- A plan item is **Done** only if there is: implementation + tests/verification + operational artifacts where applicable (CI, deploy, runbooks, dashboards/alerts).
- “Implemented but not integrated” is **Partially done** (because it’s not shippable).

## Executed validation (actual runs)

**Green**
- `cargo check --workspace --all-features` ✅
- `cargo test --workspace --all-features` ✅
- `cargo clippy --workspace --all-features -- -D warnings` ✅
- `cargo fmt --all --check` ✅
- `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps` ✅

**Red / broken gates**
- `cargo deny check` ❌ (config parse failure in `deny.toml`; this breaks the CI “Cargo Deny” job and `cargo xtask ci`)
- `buf lint proto/` ❌ (Buf v1.28.1 rejects `proto/buf.yaml` `version: v2`; this breaks the CI “Proto Compatibility” job)
- `cargo xtask ci` ❌ (fails at `cargo deny check`)

## Evidence index (implementation touchpoints)

This is a quick “where is it implemented” index with the last commit that touched the primary artifact(s) (helpful for PR/commit traceability).

| Area | Primary evidence | Last commit (short) |
|---|---|---|
| Workspace + lint policy | `Cargo.toml:1` | `9a06b19` |
| CI workflow | `.github/workflows/ci.yml:1` | `3e33905` |
| Supply-chain config | `deny.toml:1` | `e42d95b` |
| Proto lint config | `proto/buf.yaml:1` | `20b3e27` |
| Infra (GCP bucket) | `infra/terraform/main.tf:1` | `e42d95b` |
| Scoped storage isolation | `crates/arco-core/src/scoped_storage.rs` | `ad3f16a` |
| Storage abstraction | `crates/arco-core/src/storage.rs` | `ad3f16a` |
| Multi-manifest model | `crates/arco-catalog/src/manifest.rs` | `9e52ef9` |
| Tier‑1 lock + CAS | `crates/arco-catalog/src/tier1_writer.rs`, `crates/arco-catalog/src/lock.rs` | `1cd5bbb`, `dcc630a` |
| Tier‑2 event writer | `crates/arco-catalog/src/event_writer.rs` | `dcc630a` |
| Tier‑2 compactor | `crates/arco-catalog/src/compactor.rs` | `9e52ef9` |
| Tier‑2 e2e/invariants tests | `crates/arco-test-utils/tests/tier2.rs` | `b8e48f8` |
| API server (placeholder) | `crates/arco-api/src/server.rs` | `829ff74` |
| Compactor binary (placeholder) | `crates/arco-compactor/src/main.rs` | `e42d95b` |

## Executive summary (production-readiness)

**Go / No-Go:** **NO-GO** for “Catalog MVP” as defined in the plan.  

**Primary reason:** The repo contains strong foundations (storage scoping, CAS, lock, Tier-2 compaction skeleton) but does **not** yet deliver the MVP product surface (catalog registry + lineage + REST API + browser signed-URL flow) nor production operations (deployments, SLOs, monitoring, runbooks, rollback). Several “quality system” gates are configured but **currently broken** (Buf, cargo-deny), which is incompatible with “launch quality”.

**Notable strengths (evidence-backed)**
- Strong workspace lint posture (`unsafe_code = forbid`, `missing_docs = deny`, clippy unwrap/expect/panic denies): `Cargo.toml:17`
- Multi-tenant/workspace **structural** storage isolation with traversal defenses: `crates/arco-core/src/scoped_storage.rs` (validated by tests)
- Tier-1 locking + CAS retry loop and contention tests: `crates/arco-catalog/src/lock.rs`, `crates/arco-catalog/src/tier1_writer.rs`, `crates/arco-catalog/tests/concurrent_writers.rs`
- Tier-2 invariants exercised end-to-end including reading produced Parquet: `crates/arco-test-utils/tests/tier2.rs`

**Top blockers (must-fix before any MVP “launch”)**
- Missing API surface (REST) and clients, missing browser signed URL minting and DuckDB-WASM read path.
- Tier-1 snapshot writer for Parquet catalog state is not implemented (core catalog has no Parquet state tables).
- CI “security scanning” and “proto compatibility” gates are broken (`deny.toml` + `proto/buf.yaml`).
- Infra/deploy/ops artifacts are minimal (bucket only; no IAM, services, monitoring, alerting, canary/rollback).

---

## 1) Plan → Implementation Traceability Matrix (Completion Audit)

Legend: **Status** = Done / Partial / Missing / Incorrect / Needs redesign  
Priority: **P0** must-fix pre-launch, **P1** next, **P2** later

### Milestone 0 — Program setup + engineering system

| Plan item | Implementation evidence | Status | Acceptance check (how verified) | Gaps & impact | Concrete fix (priority) |
|---|---|---:|---|---|---|
| Workspace structure (`arco-core`, `arco-catalog`, etc.) | `Cargo.toml:1`, `crates/` layout; `README.md:26` | Done | Repository inspection | N/A | N/A |
| CI red line: fmt/lint/tests required | `.github/workflows/ci.yml:1` defines `check/fmt/clippy/test/docs/deny/proto-check` | **Incorrect** | Ran local equivalents: Rust checks pass; **deny/buf fail** | “Required” checks can’t be relied on if they fail consistently; gate becomes bypassed or blocks all merges | Fix `deny.toml` to match cargo-deny schema **or pin cargo-deny version**; fix `proto/buf.yaml` vs Buf version in CI (P0) |
| Versioning + changelog discipline | No `CHANGELOG*` found; workspace version is `0.1.0` only: `Cargo.toml:14` | Missing | Repo scan | No release notes discipline; hard to manage breaking changes | Add `CHANGELOG.md` + release process; enforce via CI (P1) |
| ADR + decision log process | `docs/adr/README.md:1`, `docs/adr/adr-001-parquet-metadata.md:1` | Partial | Repo inspection | Only 1 ADR; key “locked decisions” from the plan are not recorded (IDs, manifests, security posture) | Add ADRs: ID format choice, manifest naming, Tier-2 ordering/watermarks, security posture A/B (P1) |
| “Architecture guardrails” PR checklist required | `.github/pull_request_template.md:1` has no architecture checklist | Missing | Repo inspection | Invariants can regress silently | Add checklist section (two-tier invariants, tenant scoping, CAS rules, schema evolution) (P1) |

### Milestone 1 — Storage layout + data model contracts (foundation)

| Plan item | Implementation evidence | Status | Acceptance check | Gaps & impact | Concrete fix (priority) |
|---|---|---:|---|---|---|
| Tenant storage layout abstraction (GCS/S3/local via abstraction) | Storage trait + Memory backend: `crates/arco-core/src/storage.rs`; scoped prefix enforcement: `crates/arco-core/src/scoped_storage.rs` | Partial | `cargo test` ✅; scoped isolation tests pass | Only in-memory backend; no production backend wiring to GCS/S3; “signed_url” is mock for MemoryBackend | Implement real backends via `object_store` and/or provider SDKs; add integration tests behind feature flags (P0/P1 depending on launch target) |
| Structural tenant isolation (prefix-based) | `ScopedStorage` prefix `tenant={tenant}/workspace={workspace}/...`: `crates/arco-core/src/scoped_storage.rs:1` | Done | `cargo test` ✅ (`test_tenant_workspace_isolation`) | Request-context enforcement is not present (no API/auth layer) | Implement tenant/workspace extraction in API layer; require scoped storage construction from auth context (P0) |
| Define Parquet schemas for `tables/columns/namespaces/lineage_edges` | No Parquet schema definitions for these domains in code | Missing | `rg` search in `crates/arco-catalog` | Core catalog state is not queryable as Parquet; plan’s primary differentiator not delivered | Define Arrow/Parquet schema modules + contract tests (golden schema) (P0) |
| Implement manifest model with 4 domain manifests | Multi-file manifests exist: `crates/arco-catalog/src/manifest.rs` (root + core + execution + lineage + governance) | Partial | `cargo test` ✅ (manifest unit tests) | Deviates from design naming/content: missing explicit `search` domain; `governance` appears unplanned for MVP; root manifest doesn’t include search pointer | Decide and codify final domain set (catalog/lineage/executions/search vs governance) via ADR; implement search manifest or rename governance (P1) |
| Event schema versioning (`event_type`, `event_version`, etc.) | Contract exists in proto: `proto/arco/v1/event.proto:1` but catalog code writes ad-hoc JSON without envelope/versioning: `crates/arco-catalog/src/event_writer.rs` | **Incorrect** | Code inspection + tests | Inability to evolve Tier-2 safely; compactor can’t reason about schema versions or idempotency keys consistently | Adopt `arco-proto::CatalogEvent` (or a shared Rust event envelope) for Tier-2 ledger writes; add version handling in compactor (P0) |
| Schema contract tests (golden files) | Some JSON golden fixtures for proto types: `crates/arco-proto/tests/golden_fixtures.rs`; no Parquet schema golden tests for catalog | Partial | `cargo test` ✅ | Missing Parquet contract guardrails (column types, required fields) | Add Parquet schema contract tests for each state table (P0) |

### Milestone 2 — Tier‑1 write path (strong consistency) + commit integrity

| Plan item | Implementation evidence | Status | Acceptance check | Gaps & impact | Concrete fix (priority) |
|---|---|---:|---|---|---|
| Tier‑1 write algorithm (lock → read → write snapshot → commit → atomic manifest swap) | Lock + CAS on core manifest: `crates/arco-catalog/src/tier1_writer.rs` + `crates/arco-catalog/src/lock.rs` | Partial | `cargo test` ✅ (Tier1Writer + concurrent writers tests) | No Tier‑1 Parquet snapshot writing; no atomic “snapshot becomes visible only via manifest pointer” for core catalog data | Implement Tier‑1 snapshot writer that writes Parquet state tables, then CAS-updates core manifest snapshot pointer/version (P0) |
| Concurrency tests for writers | `crates/arco-catalog/tests/concurrent_writers.rs` | Done | `cargo test` ✅ | Tests update only JSON manifest fields, not Parquet snapshot visibility | Add Tier‑1 snapshot visibility + crash/failure injection tests (P0) |
| Failure injection: crash between snapshot write and manifest CAS | Tier‑2 covers atomic publish semantics (`crates/arco-test-utils/tests/tier2.rs`); Tier‑1 snapshot path not present | Partial | `cargo test` ✅ | Tier‑1 has no equivalent failure-mode test; correctness risk under partial failures | Add a test backend that fails between snapshot write and manifest update; assert snapshot is not referenced until CAS (P0) |
| Commit integrity hash chain | Commit record type exists: `crates/arco-catalog/src/manifest.rs` + Tier1Writer writes commit records: `crates/arco-catalog/src/tier1_writer.rs` | Partial / Needs redesign | `cargo test` ✅ | Hash-chain implementation is inconsistent (two hashing schemes: `CommitRecord::compute_hash` vs Tier1Writer’s sha256 of JSON bytes). No periodic verifier tooling exists. | Choose one canonical commit hash scheme; add verifier tool (`xtask verify-integrity` or a small binary) and storage scan (P1) |

### Milestone 3 — Core catalog operations behind a service API (REST)

| Plan item | Implementation evidence | Status | Acceptance check | Gaps & impact | Concrete fix (priority) |
|---|---|---:|---|---|---|
| REST endpoints (`/namespaces`, `/tables`, `/lineage/...`) | API server stub only: `crates/arco-api/src/server.rs` | Missing | `cargo test` (no API tests exist) | No shippable product/API; cannot integrate clients or UI | Implement `axum` REST API + routing + request context; include OpenAPI generation (P0) |
| Typed clients (Rust + TS) matching API and error model | None | Missing | Repo scan | No safe consumption; no contract enforcement | Generate Rust client (e.g., `reqwest` + types) and TS SDK (OpenAPI generator) (P1) |
| OpenAPI spec checked in and tested | None | Missing | Repo scan | No contract tests; breaking changes undetected | Add OpenAPI 3.1 + CI contract tests (P0/P1) |
| Authn stub integrated (JWT/tenant context extraction) | None | Missing | Repo scan | Tenant isolation not enforced at API boundary | Implement auth middleware (even stubbed) that builds `ScopedStorage` from claims (P0) |

### Milestone 4 — Browser read path (DuckDB‑WASM) + signed URL API

| Plan item | Implementation evidence | Status | Acceptance check | Gaps & impact | Concrete fix (priority) |
|---|---|---:|---|---|---|
| Signed URL minting endpoint | None | Missing | Repo scan | Core “serverless read” differentiator absent | Add `/api/v1/browser/urls` in `arco-api` and enforce tenant scope + TTL + path allowlist (P0) |
| TS SDK + DuckDB‑WASM demo | None | Missing | Repo scan | No proof that browser-direct reads work | Add minimal web demo + SDK; validate against real object storage CORS & signed URLs (P1) |
| “Reads only mint URLs, not serve data” invariant | Not applicable (endpoint missing) | Missing | N/A | Risk of accidental “read proxy” architecture | Explicitly design endpoint as URL-only; add tests forbidding data-serving routes (P1) |

### Milestone 5 — Search MVP (token index)

| Plan item | Implementation evidence | Status | Acceptance check | Gaps & impact | Concrete fix (priority) |
|---|---|---:|---|---|---|
| Token postings + bucket partitioning | None | Missing | Repo scan | Search not available | Implement search index tables + manifest pointer + rebuild job (P2 unless required for MVP) |
| Perf tests on realistic sizes | None | Missing | Repo scan | No bounded-cost guarantee | Add criterion benches + targeted perf regression checks (P2) |

### Milestone 6 — Tier‑2 operational metadata (event log + compactor) + cloud deployment

| Plan item | Implementation evidence | Status | Acceptance check | Gaps & impact | Concrete fix (priority) |
|---|---|---:|---|---|---|
| Tier‑2 event append (“append-only + ack”) | `EventWriter` uses `DoesNotExist`: `crates/arco-catalog/src/event_writer.rs` | Done (library-level) | `cargo test` ✅ | Event envelope/versioning missing; domain validation missing | Switch to versioned envelope (`CatalogEvent`) and validate allowed domains (P0) |
| Compactor is sole Parquet writer; atomic publish; idempotent fold | `crates/arco-catalog/src/compactor.rs` + Tier‑2 integration tests `crates/arco-test-utils/tests/tier2.rs` | Partial | `cargo test` ✅ includes Parquet read loop | Current compactor watermark semantics are **not per-stream**; “domain” parameter shares a single `ExecutionManifest` watermark/snapshot; late events can be dropped due to lexicographic watermarking | Redesign manifest/watermarks: per-domain watermark and snapshot pointers; add lag window or enforce server-generated ULIDs; add retry/backoff on CAS conflicts (P0) |
| Deployment (Cloud Run / Functions / etc) | Terraform bucket only: `infra/terraform/main.tf` | Missing | Repo inspection | No production deploy path; no IAM boundaries | Add modules for service accounts, Cloud Run services, scheduler/triggers, least-privilege IAM, secret management (P0) |
| Compaction lag SLI under synthetic burst | No metrics/bench harness | Missing | Repo scan | Can’t prove freshness under load; no SLO enforcement | Add metrics (lag, throughput), load generator, and a CI/perf harness (P1) |
| Chaos test: kill compactor mid-run | Not present | Missing | Repo scan | Unknown recovery behavior on partial failures | Add failure injection test with storage faults; require idempotent recovery (P1) |

### Milestone 7 — Production hardening (SLOs, monitoring, runbooks, rollback)

| Plan item | Implementation evidence | Status | Acceptance check | Gaps & impact | Concrete fix (priority) |
|---|---|---:|---|---|---|
| Structured logs + correlation IDs | Logging helper exists: `crates/arco-core/src/observability.rs`; minimal tracing use in EventWriter | Partial | `cargo test` ✅ | No correlation IDs propagated (no API), no tracing/metrics exports | Add request ID + traceparent propagation in API; adopt OpenTelemetry for traces/metrics (P1) |
| Dashboards + alerts (API latency, compaction lag, errors) | None (design-only docs) | Missing | Repo scan | Not operable; no on-call posture | Add dashboards/alerts (Prometheus/Grafana or Cloud Monitoring) + SLOs (P0/P1 depending on launch) |
| Rollback procedure + drills | None | Missing | Repo scan | Unsafe to deploy changes; no “deploy compactor first” verification | Add staged deploy pipeline with canary + rollback runbook; verify in staging (P0) |
| Integrity tooling (manifest/commit chain verification) | Commit records exist; no verifier tool | Partial | `cargo test` ✅ | Silent corruption/tampering may go undetected | Add periodic verifier + alerting (P1) |
| Runbooks (compactor stuck, corruption, lock stuck) | None | Missing | Repo scan | On-call not possible | Add runbooks + playbooks + escalation paths (P0/P1) |

---

## 2) Architecture & Design Conformance Review

### High-impact deviations (callouts)

1) **IDs: plan/doc says UUID; implementation uses ULID**
- Evidence (intended): UUID referenced in `docs/plans/ARCO_TECHNICAL_VISION.md:448`
- Evidence (actual): ID types are ULID wrappers: `crates/arco-core/src/id.rs:33`
- Assessment: **Acceptable only if formalized** (ULIDs provide sortability and uniqueness), but it violates “locked decisions” as written.
- Recommendation: ADR to explicitly choose ULID vs UUID across all domains; update docs + proto types accordingly (P1).

2) **Tier‑2 event envelope exists in proto but is not used by arco-catalog**
- Intended: `proto/arco/v1/event.proto:1` defines `CatalogEvent` with `event_version`, `idempotency_key`, etc.
- Actual: `EventWriter` writes arbitrary JSON payloads without envelope/versioning: `crates/arco-catalog/src/event_writer.rs`
- Assessment: **Not acceptable for production**; breaks evolution and dedupe semantics.
- Recommendation: Implement Tier‑2 ledger writes/reads using the envelope; compactor must parse `event_type/event_version` and perform compatibility logic (P0).

3) **Storage layout helpers diverge from Tier‑2 implementation**
- `ScopedStorage::ledger_path(domain, date)` expects date partitioning, but `EventWriter` writes `ledger/{domain}/{event_id}.json` (no date): `crates/arco-core/src/scoped_storage.rs:158` vs `crates/arco-catalog/src/event_writer.rs:10`
- `ScopedStorage::state_path(domain, table)` suggests `state/{domain}/{table}/`, but compactor writes `state/{domain}/snapshot_*.parquet`: `crates/arco-core/src/scoped_storage.rs:170` vs `crates/arco-catalog/src/compactor.rs:158`
- Assessment: **Needs redesign** (or at least a single canonical layout) before adding more domains.
- Recommendation: Consolidate to one layout spec + enforce it via tests and helpers (P0).

4) **Tier‑1 “Parquet state + atomic publish” is not implemented**
- Current Tier‑1 is “manifest JSON mutation”, not “Parquet snapshot lifecycle”.
- Assessment: This is the **core missing MVP**.
- Recommendation: Implement Tier‑1 snapshot writer, schema contracts, and atomic publish semantics for core catalog Parquet tables (P0).

5) **Manifest naming/content deviates from design docs**
- Intended: manifest names and domains in `docs/plans/ARCO_TECHNICAL_VISION.md:144` (`catalog/lineage/executions/search`)
- Actual: `core/execution/lineage/governance` in `crates/arco-catalog/src/manifest.rs`
- Assessment: This is survivable for an MVP, but **must** be formalized before clients depend on paths and semantics.
- Recommendation: ADR to lock domain manifests + filenames, plus a migration story (P1).

### Consistency model implementation status (actual vs planned)

| Concern | Planned | Current implementation reality | Gap |
|---|---|---|---|
| Tier‑1 writes (DDL) | Lock + CAS + Parquet snapshot lifecycle | Lock + CAS exists, but only updates JSON manifest fields (no Parquet catalog snapshots) | Missing MVP core |
| Tier‑2 writes (high volume) | Append-only event log + compactor | Append-only JSON ledger + compactor produces Parquet for a single “materialization” record shape | Partial; not generalized and not versioned |
| Read path | Browser direct reads (DuckDB‑WASM) + server reads (DataFusion) | Tier‑2 tests read produced Parquet in-process; no API URL minting; no query engine integration | Missing product differentiator |

### Tier‑2 invariants compliance (what’s working)

Evidence via tests in `crates/arco-test-utils/tests/tier2.rs` (executed via `cargo test`):
- Invariant 1 (append-only ingest): ✅
- Invariant 2/sole Parquet writer: ✅
- Invariant 3/idempotent compaction (primary-key upsert semantics): ✅
- Invariant 4/atomic publish (manifest pointer is visibility gate): ✅
- Invariant 5/readers don’t need ledger: ✅ (note: only validated for the exercised path)

### Known correctness risks in Tier‑2 (needs redesign)

- **Watermarking based on lexicographic filename** can permanently skip late events if producers can generate older IDs or if ingestion allows client-supplied IDs without constraints.
- **Single watermark/snapshot in `ExecutionManifest`** is incompatible with `compact_domain(domain)` for multiple domains/streams.
- Compactor **does not retry CAS conflicts**; it errors out (`CatalogError::CasFailed`), which is operationally fragile.

### Data model correctness and evolution strategy

**Intended (per plan):**
- Core catalog state is stored as Parquet tables (`tables`, `columns`, `namespaces`, `lineage_edges`) with explicit schema evolution rules and contract tests.
- Tier‑2 events are versioned (`event_type`, `event_version`) with forward/backward compatibility and dedupe keys.

**Current:**
- Catalog domain model exists only as in-memory Rust structs (e.g., `Asset`), with no persisted Parquet “catalog tables”.
- Tier‑2 compactor materializes a single Parquet schema (materializations-like record) with no explicit evolution policy and no envelope-based versioning.
- Proto defines a robust `CatalogEvent` envelope, but it is not used by `arco-catalog`.

**Implication:** You currently cannot guarantee safe schema evolution or backward compatibility for the catalog MVP because the durable formats and compatibility rules are not implemented/enforced.

### Failure modes, ordering, duplication, and retries (key findings)

- **Tier‑1 error-path lock release**: `Tier1Writer::update` relies on `LockGuard` drop behavior on error; this is likely OK in Tokio contexts, but it is a reliability risk if future code calls it outside a runtime or adds long critical sections. Prefer explicit `release()` in all paths or tighter RAII patterns.
- **Late/out-of-order events**: current watermarking (`filename > watermark`) assumes monotonic event IDs. If producers can emit older IDs, late events can be skipped permanently. The proto contract explicitly discusses skew/late-event handling (`proto/arco/v1/event.proto:1`) but the implementation does not.
- **CAS conflicts in compaction**: current compactor returns `CasFailed` without backoff/retry, making “two compactors” an operational incident rather than a tolerable race.
- **Failure injection harness exists but isn’t used for the most critical scenario** (crash after Parquet write, before manifest CAS): `crates/arco-test-utils/src/storage.rs` supports injected failures, but there is no test that forces a mid-flight failure at the manifest update boundary.

### Multi-tenant isolation and boundary enforcement

- **Storage layer**: strong structural scoping exists (prefixing + traversal defenses) in `ScopedStorage`.
- **Service layer**: missing. There is no API/auth middleware that binds request identity → tenant/workspace → scoped storage.
- **Boundary enforcement risk**: without an API layer doing strict scoping, the strongest isolation guarantees are currently “by construction in library usage”, not “by enforcement at the boundary”.

---

## 3) Code Quality, Maintainability, Engineering Standards

**Strong points**
- Workspace-level lint discipline is unusually strong for an early repo (`Cargo.toml:17`).
- Testing includes invariants and integration “walking skeleton” for Tier‑2 (`crates/arco-test-utils/tests/tier2.rs`).
- `ScopedStorage` includes explicit input/path validation (good secure-by-default posture).

**Maintainability issues**
- `arco-catalog` has multiple public “writer” concepts with stubs (`CatalogWriter`, `CatalogReader`) not integrated with the implemented Tier‑1/Tier‑2 code (`Tier1Writer`, `EventWriter`, `Compactor`). This increases architectural ambiguity.
- Error types are inconsistent: `arco-catalog` has `CatalogError` but Tier‑1 uses `arco_core::Error`. Decide on one error model for the crate boundary.
- Several TODO stubs exist in user-facing crates (API server, compactor binary, writer/reader, benchmarks). For “MVP”, stubs must be eliminated or hidden behind feature flags.

---

## 4) Security Review (practical, production-oriented)

**What exists**
- Structural isolation via scoped prefixes and traversal defense: `crates/arco-core/src/scoped_storage.rs`
- No `unsafe` code in core crates; deny unwrap/expect/panic in clippy at workspace level.

**Major gaps (P0)**
- **No authn/authz enforcement layer**: tenant/workspace context is not derived from any verified identity in `arco-api`.
- **IAM is not defined**: Terraform only provisions a bucket, not least-privilege service accounts/roles.
- **Signed URL flow not implemented**: no controls around TTL, allowlisted paths, or tenant scoping at the minting boundary.
- **Rate limiting / quotas** absent: high risk of denial-of-wallet for signed URL minting and ledger writes once APIs exist.
- Supply chain scanning is configured but **broken** (`deny.toml`), which is a security governance gap.

**Additional production security requirements (recommended)**
- **IAM least privilege** (P0): separate service accounts for API and compactor; bucket permissions scoped to required prefixes; avoid broad “storage admin”.
- **Secrets handling** (P0/P1): use a secret manager (not env vars in plain text) for signing keys or cloud credentials; ensure logs never include signed URL query params; define rotation playbook.
- **Artifact integrity** (P1): generate SBOMs for release artifacts and sign images/binaries (e.g., Cosign/SLSA provenance) once you add deployments.
- **Abuse resistance** (P0): add rate limits and per-tenant quotas for URL minting and Tier‑2 ingest; add budget alerts to prevent denial-of-wallet.

---

## 5) Operational Excellence / SRE Readiness

**Current state:** Not SRE-ready.
- No metrics/traces export, no dashboards, no alerts, no runbooks, no rollback drills.
- No deployment artifacts for API/compactor; no “deploy compactor first” rule enforcement.

**Minimum bar to reach “launchable MVP”**
- Instrument API + compactor with metrics (latency, error rate, compaction lag) and traces with correlation IDs.
- Provide dashboards + actionable alerts.
- Provide runbooks for: compactor lag/backlog, CAS/lock contention, manifest corruption, rollback.

---

## 6) Testing & CI/CD Quality Gates

**What’s strong**
- Rust unit/integration tests pass and include meaningful invariants and failure-mode coverage for Tier‑2.
- CI includes fmt/clippy/test/docs/deny/proto-check jobs in `.github/workflows/ci.yml:1`.

**Critical gaps**
- Two CI gates are currently non-functional locally and likely in CI:
  - `cargo-deny` config schema mismatch (`deny.toml`) → breaks “deny” job and `cargo xtask ci`.
  - Buf config version mismatch (`proto/buf.yaml` and `proto/buf.gen.yaml` use `version: v2` while CI pins Buf 1.28.1) → breaks proto lint/breaking checks.
- No e2e tests covering “catalog registry → Parquet snapshot → browser read via signed URL” because the product surface is not implemented.
- No load/soak tests for compaction and query paths.

**Concrete CI gate fixes (P0)**
- **Buf**: convert `proto/buf.yaml` and `proto/buf.gen.yaml` to Buf `version: v1` format (or, if Buf v2 truly exists in your toolchain, pin CI and local tooling to that v2 release). Re-run `buf lint proto/` and `buf breaking proto/ ...` to confirm.
- **cargo-deny**: regenerate a compatible `deny.toml` using `cargo deny init` and then reapply your policies (license allow list, sources allowlist, bans, etc.). Confirm with `cargo deny check` and update `tools/xtask/src/main.rs` only once it is green.

---

## 7) Production readiness decision, ranked risks, punch list

### Decision
**NO-GO** for Catalog MVP launch as defined by the plan.

### Top risks (severity × probability)
1) **Missing API + signed URL + browser read path** (Critical × Certain)
2) **Tier‑1 core catalog Parquet snapshots not implemented** (Critical × Certain)
3) **Broken security/proto CI gates** (High × High)
4) **Tier‑2 event schema/versioning mismatch vs contracts** (High × High)
5) **Watermarking/domain semantics likely incorrect for multi-stream Tier‑2** (High × Medium)
6) **No deployment/IAM/observability/runbooks/rollback** (Critical × High)

### Prioritized punch list (with suggested owners and effort)

**P0 (must-fix before any MVP launch)**
- Implement Tier‑1 Parquet snapshot writer for core catalog state tables (tables/columns/namespaces/lineage_edges) + atomic publish via manifest pointer. (Owner: Catalog) (Effort: 1–3w)
- Implement `arco-api` REST surface + request context (tenant/workspace + trace/idempotency) + auth stub. (Owner: API) (Effort: 1–2w)
- Implement signed URL mint endpoint with strict allowlist + TTL + tenant scoping; validate with a minimal browser query demo. (Owner: API/Web) (Effort: 3–7d)
- Fix CI gates: update `deny.toml` for cargo-deny and align Buf config vs pinned version (`proto/buf.yaml`). (Owner: Platform) (Effort: 0.5–2d)
- Align Tier‑2 ledger format with `CatalogEvent` envelope (proto or shared Rust struct) and add versioning/compatibility tests. (Owner: Catalog) (Effort: 3–7d)
- Provide minimal infra for deploy (at least one environment): service accounts + least-privilege IAM + deployable API + deployable compactor trigger. (Owner: Infra) (Effort: 1–2w)

**P1 (next hardening)**
- Redesign Tier‑2 watermarks as per-domain/per-stream; add lag window or server-generated IDs to prevent late-event loss; add CAS retry/backoff for compactor. (Owner: Catalog) (Effort: 1–2w)
- Add observability: metrics/tracing (OpenTelemetry), dashboards, alerts, and SLO definitions for API + compaction lag. (Owner: SRE/Platform) (Effort: 1–2w)
- Add integrity verifier tooling (manifest + commit chain) + scheduled job + alerting. (Owner: Platform) (Effort: 3–7d)
- Add OpenAPI contract tests and generated clients (Rust + TS). (Owner: API) (Effort: 3–7d)

**P2 (post-MVP)**
- Search postings + bucket pruning + perf tests. (Owner: Catalog) (Effort: 1–3w)
- Load/soak tests for compaction and query paths; cost controls (lifecycle/retention) beyond bucket NEARLINE transition. (Owner: Platform) (Effort: 1–2w)

### How to validate after P0 fixes (explicit)
- CI: `cargo xtask ci` must be green (including cargo-deny) and `buf lint proto/` must pass.
- Tier‑1 e2e: REST write (create namespace/table/lineage edge) → Tier‑1 snapshot published → server query (DataFusion) reads Parquet → browser fetches signed URLs and DuckDB-WASM queries Parquet.
- Tier‑2 resilience: duplicate events + out-of-order events + compactor crash mid-run → no double-count + watermark correct + snapshot pointer atomic.
