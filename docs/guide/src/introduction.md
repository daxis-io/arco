# Introduction

Arco is a file-native lakehouse catalog with orchestration contracts. Operators
deploy its catalog API and bring their own query engine and task runtime.

## Key Features

- **Metadata as files**: Parquet-first storage for catalog and operational metadata
- **Engine-independent reads**: Published Parquet files and scoped signed URLs
- **Lineage-by-execution**: Real lineage from actual runs, not parsed SQL
- **Multi-tenant isolation**: Enforced at storage and service boundaries

## Engine Boundaries

Arco's current split deployment has hard ownership boundaries. The operator
chooses how to deploy the API and its supporting components:

- The API and Flow modules operate catalog and task state.
- Query execution runs in a client-supplied engine.
- Compactors own Parquet projection writes.
- The legacy catalog API mints scoped URLs for published files.
- Task execution happens in external workers via canonical dispatch envelopes.

Pointer-first published state remains the source for reads. Arco publishes
operational projections; clients choose how to query them.

Current cycle non-goals: no in-process ETL engine and no Spark/dbt/Flink adapter implementation.

## Who is Arco for?

- Data platform teams building modern lakehouses
- Organizations needing unified catalog and orchestration
- Teams wanting operational metadata as queryable data
