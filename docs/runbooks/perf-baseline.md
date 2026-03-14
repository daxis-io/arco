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
3. Run a query load test against `/api/v1/query` with a fixed set of SQL statements.
4. Record P50/P95 latency and throughput.

## Evidence to capture
- Benchmark logs attached to CI artifacts or release assets
- Load test output and error rates
- Grafana dashboard snapshot for latency/CPU/memory

## Rollback
Not applicable (measurement only).
