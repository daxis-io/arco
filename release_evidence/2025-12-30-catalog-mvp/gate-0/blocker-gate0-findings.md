# Gate 0 Findings - Plan Coherence

Date: 2025-12-31
Status: PARTIAL (0.4 pending; ADR-first policy for plans)

## 0.1 Plan Internal Consistency

### Current State
- Technical Vision doc exists at `docs/plans/ARCO_TECHNICAL_VISION.md`
- Plans README exists at `docs/plans/README.md` listing 7 plan documents
- Canonical formats ADRs exist and are Accepted:
  - `docs/adr/adr-010-canonical-json.md`
  - `docs/adr/adr-011-partition-identity.md`
  - `docs/adr/adr-012-asset-key-format.md`

### Decision (ADR-first, Apache-style)
- `docs/plans/` remains gitignored by policy; ADRs are the canonical design record.
- Technical vision docs may remain internal; ADR references inside plans are optional for MVP.

### Status
- **GO** (policy decision for alpha/MVP)

### Evidence
- `.gitignore:55` contains `docs/plans/`
- `docs/plans/README.md` lists 7 planning docs with lifecycle states
- `docs/adr/README.md` lists 26 ADRs (most Accepted, one Superseded)

---

## 0.2 ADR-to-Code Conformance

### Current State
- 26 ADRs indexed in `docs/adr/README.md`
- Gate 2 findings (`docs/audits/2025-12-30-prod-readiness/findings/gate-2-findings.md`) contain partial ADR-to-code evidence for storage/manifests/correctness

### Status
- **GO** - ADR conformance matrix completed

### Evidence
- `release_evidence/2025-12-30-catalog-mvp/gate-0/adr-conformance-matrix.md` - full ADR-to-code mapping
- `docs/adr/README.md` - 26 ADRs listed
- `docs/audits/2025-12-30-prod-readiness/findings/gate-2-findings.md` - storage ADR evidence

---

## 0.3 Architecture Alignment

### Current State
- Core architecture concepts documented in Technical Vision:
  - Parquet-first metadata (ADR-001)
  - Two-tier write architecture (Tier-1 strong, Tier-2 eventual)
  - Multi-manifest domains (ADR-003)
  - Browser-direct reads via signed URLs + DuckDB-WASM
  - Server reads via DataFusion

### DataFusion Evidence (server + tests)
- DataFusion is present and validated in tests:
  - `crates/arco-test-utils/Cargo.toml:33` declares `datafusion = "46"`
  - `crates/arco-test-utils/tests/tier2.rs:1248` defines `datafusion_reads_parquet_bytes`
  - `release_evidence/2025-12-30-catalog-mvp/ci-logs/tier2-tests.txt` shows test passing
- Server read path implemented:
  - `crates/arco-api/src/routes/query.rs` (DataFusion handler)
  - `crates/arco-api/src/routes/mod.rs` (routing)
  - `crates/arco-api/src/openapi.rs` (OpenAPI)
  - `release_evidence/2025-12-30-catalog-mvp/ci-logs/arco-api-query-tests-2026-01-01-rerun4.txt` (query tests)

### Status
- **GO** - Server read path implemented; DataFusion tests pass

### Evidence
- `docs/adr/adr-027-datafusion-query-endpoint.md`
- `docs/audits/2025-12-30-prod-readiness/evidence/gate-2/06-datafusion-search.txt` - corrected evidence
- `crates/arco-test-utils/Cargo.toml:33` - `datafusion = "46"`
- `crates/arco-test-utils/tests/tier2.rs:1248` - `datafusion_reads_parquet_bytes` test
- `release_evidence/2025-12-30-catalog-mvp/ci-logs/tier2-tests.txt` - test passed
- `release_evidence/2025-12-30-catalog-mvp/ci-logs/arco-api-query-tests-2026-01-01-rerun4.txt` - query tests

---

## 0.4 Snippets Compile Rule

### Current State
- Planning docs contain extensive code fences (1800+ occurrences across 27 files)
- Only 1 explicit pseudocode label found: `docs/plans/2025-01-12-arco-unified-platform-design.md:469` ("// Compactor pseudocode")

### Blockers
- No snippet audit has been performed
- Most code blocks in plans are unlabeled (neither proven to compile nor marked as pseudocode)
- Plans are gitignored; snippet audit deferred until plans are tracked or published

### Required Actions
- If plans become tracked: audit all code fences in `docs/plans/*.md`
- For each: (a) verify it compiles/runs with evidence, OR (b) add explicit pseudocode label
- Document results in snippet audit report

### Evidence
- Code fence count by file (top files):
  - `2025-01-12-arco-orchestration-design-part2.md`: 242 fences
  - `2025-12-22-layer2-automation-execution-plan.md`: 199 fences
  - `2025-01-12-arco-unified-platform-design.md`: 166 fences
  - `ARCO_ARCHITECTURE_PART1_CORE.md`: 66 fences
  - `ARCO_TECHNICAL_VISION.md`: 60 fences

---

## Summary

| Item | Status | Primary Blocker |
|------|--------|-----------------|
| 0.1 | GO | ADR-first policy (plans internal) |
| 0.2 | GO | ADR conformance matrix completed |
| 0.3 | GO | Server read path implemented; DataFusion tests pass |
| 0.4 | NO-GO | No snippet audit performed (plans internal) |

Gate 0 overall: **PARTIAL**
