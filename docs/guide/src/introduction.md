# Introduction

Arco is a serverless lakehouse infrastructure that unifies a file-native catalog with execution-first orchestration.

## Key Features

- **Metadata as files**: Parquet-first storage for catalog and operational metadata
- **Query-native reads**: Direct SQL access via signed URLs
- **Lineage-by-execution**: Real lineage from actual runs, not parsed SQL
- **Multi-tenant isolation**: Enforced at storage and service boundaries

## Engine Boundaries

Arco uses split services with hard boundaries:

- API/orchestration are control-plane services.
- DataFusion query routes are read-only (`SELECT`/`CTE`).
- Compactors own Parquet projection writes.
- Browser analytics use DuckDB-WASM via signed URLs.
- Task execution happens in external workers via canonical dispatch envelopes.

Current cycle non-goals: no in-process ETL engine and no Spark/dbt/Flink adapter implementation.

## Who is Arco for?

- Data platform teams building modern lakehouses
- Organizations needing unified catalog and orchestration
- Teams wanting operational metadata as queryable data
