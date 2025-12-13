# ADR-001: Parquet-first Metadata Storage

## Status
Accepted

## Context
Arco needs to store catalog and operational metadata. Traditional approaches use databases, but this creates operational overhead and limits query flexibility.

## Decision
Store all metadata as immutable Parquet files on object storage:
- Catalog snapshots as partitioned Parquet
- Lineage events as append-only Parquet
- Query engines read directly via signed URLs

## Consequences

### Positive
- No database to operate
- Native SQL queryability
- Immutable audit trail
- Cost-effective at scale

### Negative
- Higher write latency than databases
- Requires compaction strategy
- Eventually consistent by default
