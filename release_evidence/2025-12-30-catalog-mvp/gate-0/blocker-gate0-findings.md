# Gate 0 Findings - Plan Coherence

Date: 2025-12-31
Status: GO (ADR-first policy for plans)

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
- Planning docs are internal and gitignored; ADRs are the canonical record
- ADR index notes plans may contain illustrative pseudocode

### Status
- **GO** (ADR-first policy; plans internal)

### Evidence
- `.gitignore:55` contains `docs/plans/`
- `docs/adr/README.md` includes the plans/pseudocode disclaimer
- `release_evidence/2025-12-30-catalog-mvp/gate-0/snippet-audit-summary.md` (historical scope assessment)

---

## Summary

| Item | Status | Primary Blocker |
|------|--------|-----------------|
| 0.1 | GO | ADR-first policy (plans internal) |
| 0.2 | GO | ADR conformance matrix completed |
| 0.3 | GO | Server read path implemented; DataFusion tests pass |
| 0.4 | GO | ADR-first policy (plans internal) |

Gate 0 overall: **GO**
