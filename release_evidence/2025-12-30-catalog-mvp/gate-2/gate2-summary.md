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
| 2.5 Tier-2 invariants | PARTIAL | Tests pass; notification consumer/anti-entropy limitations |

## Gate 2 Overall: **PARTIAL / NO-GO for production**

## Key Blockers

### Operational Wiring Limitations (P0)
1. Compaction loop only flushes queued notifications (no listing); relies on `/internal/notify` or anti-entropy: `crates/arco-compactor/src/main.rs:675`, `crates/arco-compactor/src/main.rs:728`
2. Notification consumer fast-path supports Executions + Search; Catalog/Lineage require `/internal/sync-compact`: `crates/arco-compactor/src/notification_consumer.rs:411`, `crates/arco-compactor/src/notification_consumer.rs:465`
3. Anti-entropy job implemented with cursor + listing scan; Search uses derived rebuild (no listing) and requires list permission: `crates/arco-compactor/src/anti_entropy.rs:337`, `crates/arco-compactor/src/anti_entropy.rs:358`, `crates/arco-compactor/src/anti_entropy.rs:508`

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
1. Notification consumer limitations for Catalog/Lineage (requires `/internal/sync-compact`)
2. Anti-entropy relies on list permissions and search rebuild is derived (no search listing)

For MVP release: Accept 2.1-2.4 with caveats; defer 2.5 operational hardening.
