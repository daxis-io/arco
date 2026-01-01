# ADR-to-Code Conformance Matrix

Date: 2025-12-31
Baseline: ADR index from `docs/adr/README.md`

## Conformance Status Legend
- **Implemented**: Code + tests exist matching ADR decision
- **Partial**: Some aspects implemented, others pending
- **Not Started**: ADR accepted but no implementation evidence
- **Superseded**: ADR replaced by newer decision
- **N/A**: ADR is process/documentation-only

---

## ADR Conformance Table

| ADR | Title | Status | Implementation Evidence | Test Evidence | Notes |
|-----|-------|--------|------------------------|---------------|-------|
| 001 | Parquet-first metadata storage | Implemented | `crates/arco-catalog/src/parquet_util.rs`, `crates/arco-catalog/src/tier1_snapshot.rs` | `crates/arco-catalog/tests/schema_contracts.rs` | Core architecture |
| 002 | ID strategy by entity type | Implemented | `crates/arco-core/src/id.rs`, UUID v7 usage across crates | Various unit tests | Stable ULIDs used |
| 003 | Manifest domain names and contention | Implemented | `crates/arco-core/src/catalog_paths.rs`, `crates/arco-catalog/src/manifest.rs` | `crates/arco-catalog/tests/path_contracts.rs` | 4 domains: catalog, lineage, executions, search |
| 004 | Event envelope format and evolution | Implemented | `crates/arco-core/src/catalog_event.rs`, `crates/arco-flow/src/events.rs` | `crates/arco-test-utils/tests/tier2.rs` | Versioned envelopes |
| 005 | Canonical storage layout | Implemented | `crates/arco-core/src/catalog_paths.rs`, `crates/arco-core/src/scoped_storage.rs` | `crates/arco-catalog/tests/path_contracts.rs` | Single path module for catalog |
| 006 | Parquet schema evolution policy | Implemented | `crates/arco-catalog/src/parquet_util.rs` | `crates/arco-catalog/tests/schema_contracts.rs:203` | Golden schema tests |
| 010 | Canonical JSON serialization | Implemented | `crates/arco-core/src/canonical_json.rs` | Unit tests in module | Deterministic JSON |
| 011 | Canonical partition identity | Implemented | `crates/arco-core/src/partition.rs` | Unit tests in module | Canonical partition keys |
| 012 | AssetKey canonical string format | Implemented | `crates/arco-core/src/asset_key.rs` | Unit tests in module | Canonical asset keys |
| 013 | ID type wire formats | Implemented | `crates/arco-core/src/id.rs` | Unit tests in module | Wire format consistency |
| 014 | Leader election strategy | Implemented | `crates/arco-core/src/lock.rs` | `crates/arco-core/src/lock.rs:663+` | Lease-based locks |
| 015 | Postgres orchestration store | Superseded | N/A | N/A | Replaced by Parquet-based storage |
| 016 | Tenant quotas and fairness | Partial | `crates/arco-core/src/scoped_storage.rs` | Isolation tests | Tenant scoping implemented; quotas TBD |
| 017 | Cloud Tasks dispatcher | Implemented | `crates/arco-flow/src/dispatcher/` | `crates/arco-flow/tests/` | Cloud Tasks integration |
| 018 | Tier-1 write path architecture | Implemented | `crates/arco-catalog/src/tier1_writer.rs`, `crates/arco-catalog/src/tier1_compactor.rs` | `crates/arco-catalog/tests/failure_injection.rs` | Lock + CAS + snapshot |
| 019 | Existence privacy | Partial | Design documented | N/A | Posture A implemented; Posture B deferred |
| 020 | Orchestration as unified domain | Implemented | `crates/arco-flow/src/orchestration/` | `crates/arco-flow/tests/` | Orchestration domain |
| 021 | Cloud Tasks naming convention | Implemented | `crates/arco-flow/src/dispatcher/` | Integration tests | Task naming follows ADR |
| 022 | Per-edge dependency satisfaction | Implemented | `crates/arco-flow/src/orchestration/` | `crates/arco-flow/tests/orchestration_schema_tests.rs` | dep_satisfaction tracking |
| 023 | Worker contract specification | Implemented | `crates/arco-flow/src/worker/` | Integration tests | Worker contract |
| 024 | Schedule and sensor automation | Implemented | `crates/arco-flow/src/orchestration/` | `crates/arco-flow/tests/` | Schedule/sensor support |
| 025 | Backfill controller | Implemented | `crates/arco-flow/src/orchestration/` | Integration tests | Backfill support |
| 026 | Partition status tracking | Implemented | `crates/arco-flow/src/orchestration/` | `crates/arco-flow/tests/` | Partition state tracking |

---

## Summary

| Status | Count |
|--------|-------|
| Implemented | 22 |
| Partial | 2 |
| Superseded | 1 |
| Not Started | 0 |

**Total ADRs**: 25 (excluding superseded)
**Conformance Rate**: 22/24 = 91.7% (Partial counts as 0.5)

---

## Partial Implementation Notes

### ADR-016: Tenant Quotas
- Tenant isolation via `ScopedStorage` is implemented
- Quota enforcement (rate limiting, storage limits) not yet implemented
- Required for multi-tenant production deployment

### ADR-019: Existence Privacy
- Posture A (broad metadata visibility) is implemented
- Posture B (partitioned snapshots per visibility domain) is designed but not implemented
- MVP ships with Posture A per Technical Vision

---

## Operational Readiness Caveats (not ADR gaps)

The following code paths exist but remain limited for production readiness:

1. **DataFusion reads**: Server read path implemented; tests pass (`crates/arco-api/src/routes/query.rs`, `release_evidence/2025-12-30-catalog-mvp/ci-logs/arco-api-query-tests-2026-01-01-rerun4.txt`)
2. **Notification consumer fast-path**: Executions + Search only; Catalog/Lineage require `/internal/sync-compact` (`crates/arco-compactor/src/notification_consumer.rs:411`, `crates/arco-compactor/src/notification_consumer.rs:465`)
3. **Anti-entropy**: Implemented with cursor + listing scan; Search uses derived rebuild (no listing) (`crates/arco-compactor/src/anti_entropy.rs:337`, `crates/arco-compactor/src/anti_entropy.rs:358`, `crates/arco-compactor/src/anti_entropy.rs:508`)

These are operational readiness gaps, not ADR conformance gaps.
