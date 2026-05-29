# Runbook: Performance Baseline

## Status
Deferred (requires staging environment)

## Overview
Define and capture baseline performance for catalog lookup and query endpoints before a production release.

## Preconditions
- Staging environment with representative catalog data
- Bench runner with stable hardware
- Observability stack enabled (metrics + logs)

## Procedure
1. Warm caches with a single end-to-end run of catalog reads and SQL queries.
2. Run `cargo bench -p arco-catalog --bench catalog_lookup`.
3. Run the focused regression suite:

   ```bash
   cargo test -p arco-catalog --test protocol_invariants -- --nocapture
   cargo test -p arco-catalog --test authz_decisions -- --nocapture
   cargo test -p arco-catalog --test metastore_replay_publication -- --nocapture
   cargo test -p arco-uc --test credentials_authoritative -- --nocapture
   cargo test -p arco-uc --test storage_governance_authoritative -- --nocapture
   ```

4. Run a query load test against `/api/v1/query` with a fixed set of SQL statements.
5. Record P50/P95 latency and throughput.

## Local Release Signal
The `catalog_lookup` benchmark is the local release signal for the catalog read-performance slice. It must include cold and hot catalog read-model cases, including `cold_list_namespaces`, `hot_list_namespaces`, `cold_get_table_single`, `hot_get_table_single`, `hot_get_columns`, `hot_get_table_by_id`, and `large_hot_get_table_single`.

Staging load tests remain the production signal because they capture object-store latency, concurrency, and API runtime behavior that local benchmarks do not model.

## Evidence to capture
- Benchmark logs attached to CI artifacts or release assets
- Load test output and error rates
- Grafana dashboard snapshot for latency/CPU/memory
- Focused regression suite output for protocol, authz, projection, and credential vending checks

## Rollback
Not applicable (measurement only).
