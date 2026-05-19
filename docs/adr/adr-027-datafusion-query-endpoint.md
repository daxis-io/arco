# ADR-027: DataFusion Query Endpoint

## Status
Accepted

## Context
We need a server-side SQL query path for catalog snapshots to support audit and operational queries without standing up separate query infrastructure. The API must remain read-only and return efficient, structured results for clients.

## Decision
Add `POST /api/v1/query` backed by DataFusion. The handler:
- Validates queries are SELECT/CTE only
- Registers allowlisted catalog and lineage snapshot parquet files as legacy
  two-part schemas such as `catalog.namespaces` and `lineage.lineage_edges`
- Registers explicit tenant-visible `system.*` tables through the system-table
  allowlist
- Does not expose raw search postings such as `search.token_postings` or
  `system.search.token_postings`
- Executes the query and returns Arrow IPC stream by default, JSON when `format=json` or `Accept: application/json`

## Consequences
- Adds DataFusion/Arrow/Parquet dependencies to `arco-api`
- Results are materialized in memory per request; not intended for large scans
- Query surface is restricted to allowlisted snapshot/system data and read-only SQL
