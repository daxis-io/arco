# Introduction

Arco is a serverless lakehouse infrastructure that unifies a file-native catalog with execution-first orchestration.

## Key Features

- **Metadata as files**: Parquet-first storage for catalog and operational metadata
- **Engine-independent reads**: Published Parquet files and scoped signed URLs
- **Lineage-by-execution**: Real lineage from actual runs, not parsed SQL
- **Multi-tenant isolation**: Enforced at storage and service boundaries

## Engine Boundaries

Arco uses split services with hard boundaries:

- API/orchestration are control-plane services.
- Query execution runs in a client-supplied engine.
- Compactors own Parquet projection writes.
- Arco mints scoped URLs for published files; clients bring their own query engine.
- Task execution happens in external workers via canonical dispatch envelopes.

Pointer-first published state remains the source for reads. Arco publishes
operational projections; clients choose how to query them.

Current cycle non-goals: no in-process ETL engine and no Spark/dbt/Flink adapter implementation.

## Who is Arco for?

- Data platform teams building modern lakehouses
- Organizations needing unified catalog and orchestration
- Teams wanting operational metadata as queryable data
