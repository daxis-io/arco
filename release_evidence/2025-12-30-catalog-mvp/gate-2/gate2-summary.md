# Gate 2 Summary - Storage, Manifests & Correctness

Date: 2025-12-31
Status: PARTIAL (core implemented, operational wiring pending)
Primary Reference: `docs/audits/2025-12-30-prod-readiness/findings/gate-2-findings.md`

## Summary Table

| Item | Status | Key Evidence |
|------|--------|--------------|
| 2.1 Storage layout | PARTIAL | Catalog paths canonical; flow/api/iceberg scattered |
| 2.2 Manifest model | PARTIAL | Root + domain manifests; search snapshot + compaction implemented |
| 2.3 Parquet schemas | PARTIAL | Golden schema tests + determinism tests pass |
| 2.4 Tier-1 invariants | PARTIAL | Lock/CAS/crash tests exist; search compaction tested |
| 2.5 Tier-2 invariants | PARTIAL | Tests pass; search anti-entropy remains derived |

## Gate 2 Overall: **PARTIAL / NO-GO for production**

## Key Blockers

### Operational Wiring Limitations (P0)
1. Search anti-entropy remains derived (no listing): `crates/arco-compactor/src/anti_entropy.rs:337`, `crates/arco-compactor/src/anti_entropy.rs:508`

### Evidence Corrections
- DataFusion evidence corrected 2025-12-31: DataFusion IS in tests (`crates/arco-test-utils/tests/tier2.rs:1248`)
- See: `docs/audits/2025-12-30-prod-readiness/evidence/gate-2/06-datafusion-search.txt`

## Test Evidence (All Pass)

| Test Suite | Log Path |
|------------|----------|
| Schema contracts | `ci-logs/schema-contracts-2026-01-01.txt` |
| Concurrent writers | `ci-logs/catalog-concurrent_writers.txt` |
| Tier-2 invariants | `ci-logs/tier2-tests.txt` |
| Browser E2E | `ci-logs/browser-e2e-tests.txt` |
| API integration | `ci-logs/api-integration-tests.txt` |
| OpenAPI contract | `ci-logs/openapi-contract-test.txt` |

## Recommendation

Gate 2 is **functionally complete for MVP catalog operations** but **NOT production-ready** due to:
1. Search anti-entropy remains derived (no listing)
2. Path canonicalization is still partial across non-catalog domains

For MVP release: Accept 2.1-2.4 with caveats; defer 2.5 operational hardening.
