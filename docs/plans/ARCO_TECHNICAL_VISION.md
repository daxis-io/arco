# Arco Technical Vision Document

**Version:** 1.0
**Status:** Draft
**Last Updated:** January 2025

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Vision & Goals](#2-vision--goals)
3. [Core Concepts](#3-core-concepts)
4. [Features & Capabilities](#4-features--capabilities)
5. [High-Level Architecture](#5-high-level-architecture)
6. [Storage Design](#6-storage-design)
7. [Write Path Architecture](#7-write-path-architecture)
8. [Read Path Architecture](#8-read-path-architecture)
9. [Integration Points](#9-integration-points)
10. [Technical Decisions](#10-technical-decisions)
11. [Security & Multi-Tenancy](#11-security--multi-tenancy)
12. [Phased Roadmap](#12-phased-roadmap)
13. [Open Questions](#13-open-questions)
14. [Appendix](#14-appendix)

---

## 1. Executive Summary

### What is Arco?

Arco is a **serverless lakehouse infrastructure** project that provides:

- **Catalog**: A serverless data catalog storing metadata as Parquet files on object storage
- **Orchestration**: Pipeline orchestration with lineage-by-execution (via Servo integration)

Arco is the foundational infrastructure that powers Daxis, a serverless data analytics platform for SMBs.

### Key Differentiators

| Traditional Catalogs | Arco |
|---------------------|------|
| Always-on database (Postgres, MySQL) | Metadata as Parquet files on GCS/S3 |
| API server required for reads | Direct query via DuckDB-WASM (browser) or DataFusion (server) |
| Lineage inferred from SQL parsing | Lineage recorded at execution time |
| Complex deployment | Serverless, ~$0 at rest |

### The Tagline

> *"Arco: The arc of your data — catalog and orchestration, unified."*

---

## 2. Vision & Goals

### 2.1 Problem Statement

Modern data teams need:
1. **A data catalog** to discover, understand, and govern their data assets
2. **An orchestrator** to schedule and execute data pipelines
3. **Lineage tracking** to understand data flow and impact

Current solutions require:
- Always-on infrastructure (expensive for SMBs)
- Separate systems that don't talk to each other
- Complex deployment and maintenance

### 2.2 Vision

Build "The Iceberg of Catalogs" — apply the lakehouse philosophy to catalog infrastructure itself:

- **Metadata as files**: Store catalog state as Parquet on object storage
- **Query-native**: Read catalog directly with DuckDB/DataFusion, no API required
- **Serverless economics**: Pay only for what you use, ~$0 at rest
- **Unified**: Catalog and orchestration share the same metadata layer

### 2.3 Goals

| Goal | Success Metric |
|------|----------------|
| Serverless economics | <$1/month for idle tenants |
| Infinite read scale | Browser-direct queries, no API bottleneck |
| Complete lineage | 100% of pipeline executions tracked |
| Sub-second catalog queries | P95 < 500ms for table discovery |
| Multi-tenant isolation | Zero data leakage between tenants |

### 2.4 Non-Goals

- **Not a query engine**: Arco catalogs data; Daxis queries it
- **Not a storage layer**: Arco tracks tables; Delta/Iceberg stores them
- **Not a BI tool**: Arco provides metadata; dashboards consume it
- **Not replacing Iceberg/Delta**: Arco manages catalogs OF Iceberg/Delta tables

---

## 3. Core Concepts

### 3.1 Serverless Lakehouse Catalog

Instead of a database storing catalog metadata, Arco stores everything as Parquet files:

```
Traditional Catalog:
  App → API Server → PostgreSQL → Response

Arco Catalog:
  App → DuckDB-WASM → gs://bucket/catalog/*.parquet → Response
```

**Benefits:**
- No database to manage
- Infinite read parallelism (object storage)
- Time travel via snapshot versioning
- Query with standard SQL tools

### 3.2 Lineage-by-Execution

Traditional catalogs infer lineage by parsing SQL. Arco records lineage as it happens:

```
Traditional:
  SQL Query → Parse → Guess what tables/columns are related

Arco (via Servo):
  Execute Task → Record actual inputs/outputs → Store lineage edges
```

**Benefits:**
- 100% accurate (no parsing ambiguity)
- Captures runtime context (partitions processed, row counts)
- Works for any transformation (SQL, Python, Spark)

### 3.3 Two-Tier Write Architecture

Different catalog data has different write patterns:

| Tier | Data Type | Frequency | Consistency | Approach |
|------|-----------|-----------|-------------|----------|
| **Tier 1** | Tables, schemas, contracts | Low | Strong | Locking |
| **Tier 2** | Partitions, quality, executions | High | Eventual | Log + Compactor |

### 3.4 Multi-Manifest Strategy

Instead of a single manifest, Arco uses **four domain manifests** to reduce write contention:

| Manifest | Contents | Update Frequency |
|----------|----------|------------------|
| `catalog.manifest.json` | Tables, columns, namespaces | Low (DDL changes) |
| `lineage.manifest.json` | Nodes, edges | Medium (per-execution) |
| `executions.manifest.json` | Task runs, quality results | High (every pipeline run) |
| `search.manifest.json` | Postings, trigrams | Background rebuild |

This lets Servo write execution data without contending with catalog DDL.

### 3.5 Hash Chain Commits

Every commit includes a cryptographic hash of the previous commit, creating an immutable chain:

```json
{
  "commit_id": "00000042",
  "prev_commit_hash": "sha256:abc123...",
  "timestamp": "2025-01-15T10:00:05Z",
  "operations": [...]
}
```

**Benefits:**
- Tamper detection
- Gap detection (missing commits)
- Audit trail integrity

### 3.6 Tail Commits in Manifest

Manifests include a `tail` section pointing to commits not yet compacted:

```json
{
  "snapshot": { "version": 42, "path": "..." },
  "tail": {
    "from_commit_id": "00000040",
    "to_commit_id": "00000042",
    "commit_paths": ["commits/00000041.json", "commits/00000042.json"]
  }
}
```

Clients can do **incremental refresh** by replaying tail commits without reloading full snapshots.

### 3.7 Governance Postures

Arco supports three explicit security postures:

| Posture | Description | Implementation |
|---------|-------------|----------------|
| **A: Broad Visibility** | Metadata visible to all tenant users | Compiled grants for UI hints only |
| **B: Existence Privacy** | Users shouldn't see tables they can't access | Partitioned snapshots + IAM per visibility domain |
| **C: Encryption** | Objects encrypted at rest | Objects encrypted; key service for auth |

**MVP**: Posture A (metadata broadly visible)

**Architecture**: Designed to support Posture B when needed

### 3.8 Dual-Engine Reads

Arco supports two query engines for different contexts:

| Engine | Use Case | Deployment |
|--------|----------|------------|
| **DuckDB-WASM** | Browser-based catalog exploration | Client-side |
| **DataFusion** | Server-side catalog operations | Rust services |

Both read the same Parquet files — no synchronization needed.

---

## 4. Features & Capabilities

### 4.1 Catalog Features

#### 4.1.1 Table Registry
- Register tables (Delta, Iceberg, Parquet, CSV)
- Track table location, format, schema
- Namespace hierarchy (database.schema.table)

#### 4.1.2 Schema Management
- Column-level metadata (name, type, description)
- Schema evolution tracking
- PII/sensitivity classification

#### 4.1.3 Data Contracts
- Define expectations on tables
- Schema contracts (required columns, types)
- Quality contracts (freshness, row counts, custom checks)

#### 4.1.4 Search & Discovery (Progressive Architecture)

**Phase 1 (MVP)**: Token-based search
- Tokenize table/column names and descriptions on write
- Store in `search_postings` Parquet table partitioned by `token_bucket`
- Basic SQL query for matching

**Phase 2**: Fuzzy matching with trigrams
- Trigram table for typo tolerance
- Query: expand to trigrams → find candidate tokens → score by overlap

**Phase 3 (Optional)**: Semantic search
- Embeddings stored in Parquet
- ANN index artifact for scale (only if users demand)

#### 4.1.5 Lineage

- Table-level lineage (which tables feed which)
- Column-level lineage (which columns derive from which)
- Execution-level lineage (which pipeline run produced this data)

**Transform Fingerprint**: Each lineage edge includes a hash of the transformation definition, enabling deduplication when the same DAG runs repeatedly.

**Closure Constraints** (to avoid O(n²) explosion):

- Table-level closures only (not column-level) in Phase 1
- PII-scoped closures (only compute for PII-tagged columns)
- Max depth: 8 hops
- Use recursive CTEs at query time for on-demand traversal

### 4.2 Orchestration Features (via Servo)

#### 4.2.1 DAG Execution
- Define pipelines as directed acyclic graphs
- Task dependencies and ordering
- Retry and failure handling

#### 4.2.2 Partition-Aware Scheduling
- Track partition status (PENDING, RUNNING, COMPLETE, FAILED)
- Incremental processing (only process new partitions)
- Backfill support

#### 4.2.3 Execution History
- Record every pipeline run
- Task-level timing and status
- Resource usage metrics

#### 4.2.4 Quality Integration
- Run quality checks as pipeline tasks
- Record results in catalog
- Block downstream on quality failures

### 4.3 Governance Features

#### 4.3.1 Access Control
- Per-tenant isolation
- Role-based access within tenant
- Column-level masking (future)

#### 4.3.2 Audit Trail
- All catalog mutations logged
- Who changed what, when
- Immutable event history

#### 4.3.3 Data Classification
- PII tagging (SSN, email, phone, etc.)
- Sensitivity levels
- Propagation through lineage

---

## 5. High-Level Architecture

### 5.1 System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                              DAXIS                                   │
│                    (Serverless Analytics Platform)                   │
│                                                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │   Web UI    │  │  Query API  │  │  Admin API  │                  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                  │
│         │                │                │                          │
└─────────┼────────────────┼────────────────┼──────────────────────────┘
          │                │                │
          ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                              ARCO                                    │
│              (Serverless Lakehouse Infrastructure)                   │
│                                                                      │
│  ┌──────────────────────────┐  ┌──────────────────────────┐         │
│  │      ARCO CATALOG        │  │       ARCO FLOW          │         │
│  │  ────────────────────    │  │  ──────────────────      │         │
│  │  • Table Registry        │  │  • DAG Execution         │         │
│  │  • Schema Management     │  │  • Partition Tracking    │         │
│  │  • Lineage Storage       │  │  • Execution History     │         │
│  │  • Search Index          │  │  • Quality Integration   │         │
│  │  • Data Contracts        │  │                          │         │
│  └────────────┬─────────────┘  └────────────┬─────────────┘         │
│               │                              │                       │
│               └──────────────┬───────────────┘                       │
│                              │                                       │
│                              ▼                                       │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    STORAGE LAYER                              │   │
│  │  ──────────────────────────────────────────────────────────  │   │
│  │                                                               │   │
│  │   gs://bucket/tenant/_catalog/                                │   │
│  │   ├── core/           (Tier 1: Tables, schemas, lineage)     │   │
│  │   ├── events/         (Tier 2: Event log)                    │   │
│  │   └── operational/    (Tier 2: Partitions, quality, execs)   │   │
│  │                                                               │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
          │                              │
          ▼                              ▼
┌─────────────────────┐      ┌─────────────────────┐
│    DuckDB-WASM      │      │     DataFusion      │
│  (Browser Reads)    │      │   (Server Reads)    │
└─────────────────────┘      └─────────────────────┘
```

### 5.2 Component Breakdown

#### Arco Catalog (arco-catalog)
- **Purpose**: Store and manage data asset metadata
- **Language**: Rust
- **Key modules**:
  - `registry`: Table/column registration
  - `lineage`: Lineage edge storage and traversal
  - `search`: Full-text search index
  - `contracts`: Data contract definitions and validation

#### Arco Flow (arco-flow)
- **Purpose**: Pipeline orchestration with lineage recording
- **Language**: Rust
- **Key modules**:
  - `dag`: DAG definition and execution
  - `scheduler`: Task scheduling and dependencies
  - `executor`: Task execution runtime
  - `partitions`: Partition state management

#### Arco Core (arco-core)
- **Purpose**: Shared types and utilities
- **Language**: Rust
- **Key modules**:
  - `types`: Common data structures (Table, Column, LineageEdge)
  - `storage`: Storage abstraction (GCS, S3, local)
  - `auth`: Authentication and tenant context

---

## 6. Storage Design

### 6.1 Directory Structure

```
gs://bucket/{tenant}/_catalog/
│
├── core/                              # TIER 1: Core Catalog (Strong Consistency)
│   ├── lock.json                      # Lease file for write coordination
│   ├── manifest.json                  # Points to current snapshot version
│   ├── commits/                       # Immutable commit records
│   │   ├── 00000001.json
│   │   ├── 00000002.json
│   │   └── 00000042.json
│   └── snapshots/
│       ├── v41/
│       │   ├── tables.parquet
│       │   ├── columns.parquet
│       │   ├── namespaces.parquet
│       │   └── lineage_edges.parquet
│       └── v42/                       # Current version
│           ├── tables.parquet
│           ├── columns.parquet
│           ├── namespaces.parquet
│           └── lineage_edges.parquet
│
├── events/                            # TIER 2: Event Log (Append-Only)
│   ├── 2025-01-15/
│   │   ├── 10-00-00-servo-run-123.json
│   │   ├── 10-00-05-servo-run-123.json
│   │   └── 10-00-07-quality-check-456.json
│   └── 2025-01-16/
│       └── ...
│
├── operational/                       # TIER 2: Operational Snapshots
│   ├── watermark.json                 # Last processed event marker
│   └── snapshots/
│       └── v15/
│           ├── partitions.parquet
│           ├── partition_lineage.parquet
│           ├── quality_results.parquet
│           ├── executions.parquet
│           └── execution_tasks.parquet
│
├── search/                            # Search Index
│   └── fts_index.parquet              # Full-text search index
│
└── audit/                             # Audit Trail
    ├── 2025-01/
    │   └── audit_log.parquet
    └── 2025-02/
        └── audit_log.parquet
```

### 6.2 Parquet Schemas

#### tables.parquet
```
table_id: STRING (UUID)
namespace: STRING
name: STRING
location: STRING
format: STRING (DELTA | ICEBERG | PARQUET | CSV)
description: STRING (nullable)
owner: STRING (nullable)
created_at: TIMESTAMP
updated_at: TIMESTAMP
properties: MAP<STRING, STRING>
tags: LIST<STRING>
pii_columns: LIST<STRING>
row_count: INT64 (nullable)
size_bytes: INT64 (nullable)
last_modified: TIMESTAMP (nullable)
```

#### columns.parquet
```
column_id: STRING (UUID)
table_id: STRING (FK)
name: STRING
data_type: STRING
ordinal_position: INT32
is_nullable: BOOLEAN
description: STRING (nullable)
pii_type: STRING (nullable: SSN | EMAIL | PHONE | ADDRESS | ...)
sensitivity: STRING (nullable: PUBLIC | INTERNAL | CONFIDENTIAL | RESTRICTED)
created_at: TIMESTAMP
updated_at: TIMESTAMP
```

#### lineage_edges.parquet
```
edge_id: STRING (UUID)
source_table_id: STRING (nullable)
source_column_id: STRING (nullable)
target_table_id: STRING (nullable)
target_column_id: STRING (nullable)
transformation_type: STRING (DIRECT | AGGREGATE | EXPRESSION | CASE | CAST | WINDOW)
transformation_sql: STRING (nullable)
pipeline_id: STRING (nullable)
execution_id: STRING (nullable)
created_at: TIMESTAMP
```

#### partitions.parquet
```
partition_id: STRING (UUID)
table_id: STRING (FK)
partition_values: MAP<STRING, STRING>  # e.g., {"date": "2025-01-15", "region": "us-west"}
status: STRING (PENDING | RUNNING | COMPLETE | FAILED | STALE)
row_count: INT64 (nullable)
size_bytes: INT64 (nullable)
created_at: TIMESTAMP
updated_at: TIMESTAMP
last_successful_run: TIMESTAMP (nullable)
last_execution_id: STRING (nullable)
```

#### quality_results.parquet
```
result_id: STRING (UUID)
table_id: STRING (FK)
partition_id: STRING (nullable, FK)
check_name: STRING
check_type: STRING (FRESHNESS | ROW_COUNT | NULL_CHECK | CUSTOM_SQL | ...)
status: STRING (PASSED | FAILED | WARNING | SKIPPED)
expected_value: STRING (nullable)
actual_value: STRING (nullable)
message: STRING (nullable)
execution_id: STRING (nullable)
checked_at: TIMESTAMP
```

#### executions.parquet
```
execution_id: STRING (UUID)
pipeline_id: STRING
pipeline_name: STRING
status: STRING (PENDING | RUNNING | SUCCESS | FAILED | CANCELLED)
started_at: TIMESTAMP
completed_at: TIMESTAMP (nullable)
duration_ms: INT64 (nullable)
trigger_type: STRING (SCHEDULED | MANUAL | EVENT)
triggered_by: STRING (nullable)
error_message: STRING (nullable)
```

#### execution_tasks.parquet
```
task_id: STRING (UUID)
execution_id: STRING (FK)
task_name: STRING
status: STRING (PENDING | RUNNING | SUCCESS | FAILED | SKIPPED)
started_at: TIMESTAMP (nullable)
completed_at: TIMESTAMP (nullable)
duration_ms: INT64 (nullable)
retry_count: INT32
error_message: STRING (nullable)
input_tables: LIST<STRING>
output_tables: LIST<STRING>
input_partitions: LIST<STRING>
output_partitions: LIST<STRING>
rows_read: INT64 (nullable)
rows_written: INT64 (nullable)
bytes_read: INT64 (nullable)
bytes_written: INT64 (nullable)
```

### 6.3 Manifest Schema

```json
{
  "version": 2,
  "created_at": "2025-01-15T10:00:00Z",
  "core": {
    "snapshot_version": 42,
    "snapshot_path": "core/snapshots/v42/",
    "last_commit_id": "00000042"
  },
  "operational": {
    "snapshot_version": 15,
    "snapshot_path": "operational/snapshots/v15/",
    "watermark_event_id": "evt-abc123",
    "watermark_timestamp": "2025-01-15T09:59:55Z"
  },
  "search": {
    "index_version": 10,
    "index_path": "search/fts_index.parquet"
  }
}
```

### 6.4 Partitioning Strategy

Parquet files are partitioned by access pattern to enable efficient pruning:

| Data | Partition Key | Rationale |
|------|--------------|-----------|
| `tables.parquet` | `namespace_hash % 64` | Namespace browsing |
| `columns.parquet` | `table_id_hash % 256` | Schema drilldown |
| `lineage_edges.parquet` | `target_table_hash % 128` | "What feeds this table?" queries |
| `executions.parquet` | `date + task_id_hash` | Time-series + task lookup |
| `quality_results.parquet` | `table_id_hash + date` | Table health dashboards |
| `search_postings.parquet` | `token_bucket` | Keyword lookup |

---

## 7. Write Path Architecture

### 7.1 Tier 1: Core Catalog Writes (Strong Consistency)

For DDL-like operations: tables, schemas, core lineage, contracts.

```
┌─────────────────────────────────────────────────────────────────┐
│                     TIER 1 WRITE PATH                            │
│                                                                  │
│   Writer (Servo, API)                                            │
│        │                                                         │
│        ▼                                                         │
│   ┌─────────────────┐                                           │
│   │ Acquire Lock    │  ← GCS conditional write on lock.json     │
│   │ (lease: 30s)    │                                           │
│   └────────┬────────┘                                           │
│            │                                                     │
│            ▼                                                     │
│   ┌─────────────────┐                                           │
│   │ Read Manifest   │  ← Get current snapshot version           │
│   └────────┬────────┘                                           │
│            │                                                     │
│            ▼                                                     │
│   ┌─────────────────┐                                           │
│   │ Read Snapshot   │  ← Load current Parquet files             │
│   └────────┬────────┘                                           │
│            │                                                     │
│            ▼                                                     │
│   ┌─────────────────┐                                           │
│   │ Apply Changes   │  ← Modify in-memory state                 │
│   └────────┬────────┘                                           │
│            │                                                     │
│            ▼                                                     │
│   ┌─────────────────┐                                           │
│   │ Write Commit    │  ← Immutable commit record                │
│   │ (commits/N.json)│                                           │
│   └────────┬────────┘                                           │
│            │                                                     │
│            ▼                                                     │
│   ┌─────────────────┐                                           │
│   │ Write Snapshot  │  ← New Parquet files                      │
│   │ (snapshots/vN/) │                                           │
│   └────────┬────────┘                                           │
│            │                                                     │
│            ▼                                                     │
│   ┌─────────────────┐                                           │
│   │ Update Manifest │  ← Atomic pointer update                  │
│   └────────┬────────┘                                           │
│            │                                                     │
│            ▼                                                     │
│   ┌─────────────────┐                                           │
│   │ Release Lock    │                                           │
│   └─────────────────┘                                           │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

#### Lock File Schema
```json
{
  "holder": "servo-worker-abc123",
  "acquired_at": "2025-01-15T10:00:00Z",
  "expires_at": "2025-01-15T10:00:30Z",
  "generation": 12345
}
```

#### Commit Record Schema
```json
{
  "commit_id": "00000042",
  "timestamp": "2025-01-15T10:00:05Z",
  "parent_commit": "00000041",
  "author": "servo-pipeline-orders",
  "operations": [
    {
      "op": "create_table",
      "table_id": "tbl-123",
      "name": "orders_daily",
      "namespace": "analytics"
    },
    {
      "op": "add_lineage",
      "edge_id": "edge-456",
      "source_table": "raw.orders",
      "target_table": "analytics.orders_daily"
    }
  ],
  "snapshot_version": 42
}
```

### 7.2 Tier 2: Operational Writes (Eventual Consistency)

For high-frequency operations: partitions, quality results, execution events.

```
┌─────────────────────────────────────────────────────────────────┐
│                     TIER 2 WRITE PATH                            │
│                                                                  │
│   Writers (Servo tasks, quality checks, etc.)                    │
│        │                                                         │
│        ▼                                                         │
│   ┌─────────────────┐                                           │
│   │ Create Event    │  ← Build event object                     │
│   └────────┬────────┘                                           │
│            │                                                     │
│            ▼                                                     │
│   ┌─────────────────┐                                           │
│   │ Write to Log    │  ← events/YYYY-MM-DD/timestamp-source.json│
│   │ (fire & forget) │     No locking, no coordination           │
│   └────────┬────────┘                                           │
│            │                                                     │
│            │ (async, via GCS notification → Pub/Sub)             │
│            ▼                                                     │
│   ┌─────────────────┐                                           │
│   │ Cloud Function  │  ← Triggered on new events                │
│   │ (Compactor)     │                                           │
│   └────────┬────────┘                                           │
│            │                                                     │
│            ▼                                                     │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                  COMPACTION LOGIC                        │   │
│   │                                                          │   │
│   │  1. Read watermark (last processed event)                │   │
│   │  2. List new events since watermark                      │   │
│   │  3. Sort events by timestamp                             │   │
│   │  4. Load current operational snapshots                   │   │
│   │  5. Apply events to state:                               │   │
│   │     - partition_* → partitions.parquet                   │   │
│   │     - quality_* → quality_results.parquet                │   │
│   │     - execution_* → executions.parquet                   │   │
│   │  6. Write new snapshots                                  │   │
│   │  7. Update watermark (atomic)                            │   │
│   │  8. Archive processed events                             │   │
│   │                                                          │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

#### Event Schema
```json
{
  "event_id": "evt-abc123",
  "event_type": "partition_completed",
  "event_version": 1,
  "timestamp": "2025-01-15T10:00:05.123Z",
  "source": {
    "type": "servo",
    "execution_id": "exec-xyz789",
    "task_name": "transform_orders"
  },
  "payload": {
    "table_id": "tbl-orders",
    "partition_values": {"date": "2025-01-15"},
    "status": "COMPLETE",
    "row_count": 150000,
    "size_bytes": 45000000,
    "duration_ms": 12500
  }
}
```

#### Event Types
| Event Type | Payload | Resulting Update |
|------------|---------|------------------|
| `partition_created` | table_id, partition_values | Insert into partitions.parquet |
| `partition_completed` | partition_id, status, metrics | Update partitions.parquet |
| `partition_failed` | partition_id, error | Update partitions.parquet |
| `quality_check_passed` | table_id, check_name, values | Insert into quality_results.parquet |
| `quality_check_failed` | table_id, check_name, error | Insert into quality_results.parquet |
| `execution_started` | pipeline_id, execution_id | Insert into executions.parquet |
| `execution_completed` | execution_id, status, metrics | Update executions.parquet |
| `task_completed` | execution_id, task_name, metrics | Insert into execution_tasks.parquet |

#### Watermark Schema
```json
{
  "version": 15,
  "last_event_id": "evt-abc123",
  "last_timestamp": "2025-01-15T10:00:05.123Z",
  "events_processed": 1523,
  "compacted_at": "2025-01-15T10:00:10Z"
}
```

---

## 8. Read Path Architecture

### 8.1 DuckDB-WASM (Browser Reads)

For interactive catalog exploration in the Daxis UI.

```
┌─────────────────────────────────────────────────────────────────┐
│                    BROWSER READ PATH                             │
│                                                                  │
│   Daxis Web UI                                                   │
│        │                                                         │
│        ▼                                                         │
│   ┌─────────────────┐                                           │
│   │ DuckDB-WASM     │  ← Runs entirely in browser               │
│   │ (WebAssembly)   │                                           │
│   └────────┬────────┘                                           │
│            │                                                     │
│            ▼                                                     │
│   ┌─────────────────┐                                           │
│   │ HTTP Range Req  │  ← Fetch only needed Parquet chunks       │
│   │ (via CORS)      │                                           │
│   └────────┬────────┘                                           │
│            │                                                     │
│            ▼                                                     │
│   ┌─────────────────┐                                           │
│   │ GCS / CDN       │  ← Parquet files served directly          │
│   │ (signed URLs)   │                                           │
│   └─────────────────┘                                           │
│                                                                  │
└───────────────────────────────────��──────────────────────────────┘
```

**Example Queries:**
```sql
-- List all tables in a namespace
SELECT * FROM read_parquet('gs://bucket/tenant/_catalog/core/snapshots/v42/tables.parquet')
WHERE namespace = 'analytics';

-- Find tables with PII
SELECT t.name, t.pii_columns
FROM read_parquet('.../tables.parquet') t
WHERE cardinality(t.pii_columns) > 0;

-- Get lineage for a table
SELECT * FROM read_parquet('.../lineage_edges.parquet')
WHERE target_table_id = 'tbl-123';
```

### 8.2 DataFusion (Server Reads)

For server-side operations requiring fresh data or complex queries.

```
┌─────────────────────────────────────────────────────────────────┐
│                    SERVER READ PATH                              │
│                                                                  │
│   Arco API / Servo                                               │
│        │                                                         │
│        ▼                                                         │
│   ┌─────────────────┐                                           │
│   │ DataFusion      │  ← Rust-native query engine               │
│   │ (Rust)          │                                           │
│   └────────┬────────┘                                           │
│            │                                                     │
│            ├──── Fast Path: Read Parquet snapshots only         │
│            │                                                     │
│            └──── Fresh Path: Snapshot + event log tail          │
│                       │                                          │
│                       ▼                                          │
│               ┌─────────────────┐                               │
│               │ Apply pending   │                               │
│               │ events in-mem   │                               │
│               └─────────────────┘                               │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

**Read Modes:**

| Mode | Latency | Freshness | Use Case |
|------|---------|-----------|----------|
| **Fast** | ~50ms | Up to compaction lag | UI queries, discovery |
| **Fresh** | ~200ms | Real-time | Pipeline decisions, validation |

### 8.3 Search

Full-text search uses a pre-built index:

```sql
-- Search index structure
search/fts_index.parquet:
  entity_type: STRING (table | column | namespace)
  entity_id: STRING
  searchable_text: STRING  -- name + description + tags concatenated
  tokens: LIST<STRING>     -- tokenized for matching
```

**Query pattern:**
```sql
SELECT entity_type, entity_id, searchable_text
FROM read_parquet('.../fts_index.parquet')
WHERE searchable_text ILIKE '%customer%order%';
```

---

## 9. Integration Points

### 9.1 Daxis Integration

Arco is the metadata backbone for Daxis:

```
┌─────────────────────────────────────────────────────────────────┐
│                         DAXIS                                    │
│                                                                  │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│   │  Daxis UI    │    │ Daxis Query  │    │ Daxis Admin  │      │
│   │              │    │   Engine     │    │     API      │      │
│   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘      │
│          │                   │                   │               │
│          │ "What tables      │ "Resolve         │ "Create       │
│          │  exist?"          │  table path"     │  namespace"   │
│          │                   │                   │               │
│          ▼                   ▼                   ▼               │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                      ARCO CATALOG                        │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

**Integration Points:**
- **UI** → Arco (via DuckDB-WASM): Table discovery, lineage visualization
- **Query Engine** → Arco (via DataFusion): Table resolution, schema lookup
- **Admin API** → Arco (via Rust SDK): Namespace/table management

### 9.2 Servo Integration

Servo is the primary writer to Arco:

```
┌─────────────────────────────────────────────────────────────────┐
│                         SERVO                                    │
│                   (Orchestration Engine)                         │
│                                                                  │
│   Pipeline Execution                                             │
│        │                                                         │
│        ├─── Before task ──▶ Read partition status from Arco     │
│        │                    (Which partitions need processing?)  │
│        │                                                         │
│        ├─── Execute task ──▶ Run transformation                  │
│        │                                                         │
│        ├─── After task ───▶ Write to Arco:                       │
│        │                    • Partition completion event         │
│        │                    • Lineage edges (inputs → outputs)   │
│        │                    • Execution metrics                  │
│        │                    • Quality check results              │
│        │                                                         │
│        └─── On failure ───▶ Write failure event to Arco         │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

**Servo → Arco Data Flow:**

| Servo Event | Arco Update |
|-------------|-------------|
| Pipeline started | `execution_started` event |
| Task started | `task_started` event |
| Task completed | `task_completed` event + lineage edges |
| Partition written | `partition_completed` event |
| Quality check run | `quality_check_*` event |
| Pipeline completed | `execution_completed` event |

### 9.3 External Writers

Beyond Servo, other systems may write to Arco:

| Writer | What They Write | Tier |
|--------|-----------------|------|
| **Daxis Admin API** | Namespace/table creation, descriptions, tags | Tier 1 |
| **Data Quality Service** | Quality check results | Tier 2 |
| **Schema Registry** | Schema evolution events | Tier 1 |
| **Governance Tool** | PII classifications, policies | Tier 1 |
| **Profiler** | Column statistics, row counts | Tier 2 |

---

## 10. Technical Decisions

### 10.1 Why Parquet for Catalog Storage?

| Alternative | Why Not |
|-------------|---------|
| PostgreSQL | Always-on cost, single point of failure |
| SQLite on GCS | Write contention (download-modify-upload) |
| DynamoDB | Vendor lock-in, not query-native |
| Delta Lake for catalog | Circular dependency, over-engineered |

**Parquet wins because:**
- Query-native (DuckDB, DataFusion, Spark all read it)
- Columnar (efficient for analytical queries)
- Compressed (low storage cost)
- Immutable (snapshots = time travel)

### 10.2 Why Two-Tier Writes?

| If We Used Only Locking | If We Used Only Event Log |
|-------------------------|---------------------------|
| Core ops: Fine | Core ops: Eventual consistency breaks semantics |
| Partition updates: Bottleneck at scale | Partition updates: Perfect |

**Hybrid wins because:**
- Core catalog ops are low-frequency, need strong consistency
- Operational metadata is high-frequency, can tolerate lag

### 10.3 Why DuckDB-WASM + DataFusion?

| If We Used Only DuckDB-WASM | If We Used Only DataFusion |
|-----------------------------|----------------------------|
| Browser: Great | Browser: Can't run (no WASM) |
| Server: Overhead of WASM | Server: Native performance |

**Both wins because:**
- DuckDB-WASM = magic in browser (no API needed)
- DataFusion = native Rust for server operations

### 10.4 Why Not Iceberg/Delta for Catalog Files?

This was considered and rejected:

| Concern | Analysis |
|---------|----------|
| Circular dependency | Catalog depends on format that needs catalog |
| Bootstrap problem | How do you find the catalog? |
| Over-engineering | Catalog metadata is small (~MBs) |
| DuckDB-WASM compat | Plain Parquet "just works" |

**Decision:** Use plain Parquet + custom commit log. Borrow ideas from Delta (commit log, checkpoints) without the format dependency.

---

## 11. Security & Multi-Tenancy

### 11.1 Tenant Isolation

Each tenant has completely isolated storage:

```
gs://arco-catalog-prod/
├── tenant=acme/
│   └── _catalog/...
├── tenant=globex/
│   └── _catalog/...
└── tenant=initech/
    └── _catalog/...
```

**Isolation guarantees:**
- Separate GCS prefixes (no cross-tenant access)
- Tenant ID in every request context
- No shared state between tenants

### 11.2 Authentication

```
┌─────────────────────────────────────────────────────────────────┐
│                    AUTHENTICATION FLOW                           │
│                                                                  │
│   User/Service                                                   │
│        │                                                         │
│        ▼                                                         │
│   ┌─────────────────┐                                           │
│   │ Daxis Auth      │  ← JWT with tenant_id claim               │
│   │ (OAuth/OIDC)    │                                           │
│   └────────┬────────┘                                           │
│            │                                                     │
│            ▼                                                     │
│   ┌─────────────────┐                                           │
│   │ Arco Context    │  ← Extract tenant, validate permissions   │
│   └────────┬────────┘                                           │
│            │                                                     │
│            ▼                                                     │
│   ┌─────────────────┐                                           │
│   │ GCS Signed URLs │  ← Short-lived, tenant-scoped             │
│   └─────────────────┘                                           │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 11.3 Authorization Model

| Role | Permissions |
|------|-------------|
| **Viewer** | Read tables, columns, lineage |
| **Editor** | Viewer + edit descriptions, tags |
| **Admin** | Editor + create/drop tables, manage contracts |
| **Owner** | Admin + manage roles, tenant settings |

### 11.4 Data Classification

PII tracking propagates through lineage:

```
orders.customer_email (PII: EMAIL)
        │
        ▼ (lineage edge)
customer_summary.email_domain (PII: DERIVED_EMAIL)
```

**Classification levels:**
- `PUBLIC`: No restrictions
- `INTERNAL`: Company-internal only
- `CONFIDENTIAL`: Need-to-know basis
- `RESTRICTED`: Regulatory requirements (PII, PCI, HIPAA)

---

## 12. Phased Roadmap

### Phase 1: Core Catalog (v1.0)

**Goal:** Ship the foundational catalog with strong consistency.

**Features:**
- [ ] Table and column registry
- [ ] Namespace hierarchy
- [ ] Table-level lineage
- [ ] Basic search (ILIKE matching)
- [ ] Tier 1 write path (locking)
- [ ] DuckDB-WASM reads
- [ ] DataFusion reads
- [ ] Per-tenant isolation

**Storage:**
- `core/` directory only
- No event log yet

**Timeline:** Foundation release

---

### Phase 2: Operational Metadata (v1.1)

**Goal:** Add high-frequency operational data support.

**Features:**
- [ ] Tier 2 write path (event log + compactor)
- [ ] Partition tracking
- [ ] Execution history
- [ ] Quality check results
- [ ] Cloud Function compactor

**Storage:**
- Add `events/` and `operational/` directories

**Timeline:** After v1.0 stability

---

### Phase 3: Servo Integration (v1.2)

**Goal:** Deep integration with Servo orchestration.

**Features:**
- [ ] Lineage-by-execution
- [ ] Partition-aware scheduling reads
- [ ] Execution → catalog automatic writes
- [ ] Quality gates in pipelines

**Timeline:** After Servo MVP

---

### Phase 4: Advanced Features (v2.0)

**Goal:** Enterprise-grade capabilities.

**Features:**
- [ ] Column-level lineage from Servo
- [ ] Semantic search (vector embeddings)
- [ ] Data contracts with validation
- [ ] PII propagation through lineage
- [ ] Impact analysis queries
- [ ] Catalog versioning / time travel

**Timeline:** Based on customer demand

---

### Phase 5: Ecosystem (v2.x)

**Goal:** Broader integrations.

**Features:**
- [ ] dbt integration (models → catalog)
- [ ] Spark integration
- [ ] Airflow integration (for non-Servo users)
- [ ] REST API for external tools
- [ ] Webhook notifications

**Timeline:** Based on ecosystem needs

---

## 12.5 Key Locked Decisions

These architectural decisions are locked and should not be revisited without strong justification:

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **ID Strategy** | Stable ULIDs | Names can change; IDs should not |
| **Manifest Granularity** | 4 domain manifests | Reduces write contention |
| **Security Posture (MVP)** | Posture A (broad metadata) | Pragmatic start |
| **Security Architecture** | Designed for Posture B | Future-proof for existence privacy |
| **Lineage Granularity (MVP)** | Table-level | Avoid complexity early |
| **Lineage Granularity (Phase 2)** | Column-level | When customers need it |
| **Embedding Search** | No (Phase 3+ maybe) | Token search sufficient for MVP |
| **Write Patterns** | Bursty (pipeline runs) | Optimize compaction for batch arrivals |
| **Lineage Closures** | Bounded, table-level, PII-scoped only | Avoid O(n²) explosion |
| **Commit Integrity** | Hash chain | Tamper detection, gap detection |

---

## 13. Open Questions

### 13.1 Unresolved Decisions

| Question | Options | Status |
|----------|---------|--------|
| Search implementation | FTS in Parquet vs. external index | Leaning FTS in Parquet |
| Compactor deployment | Cloud Function vs. Cloud Run | Need to evaluate cold starts |
| Event batching | Per-event vs. micro-batch (100 events) | Need load testing |
| Snapshot retention | Keep all vs. rolling window | Depends on storage costs |

### 13.2 Future Considerations

| Topic | Notes |
|-------|-------|
| **Multi-region** | May need region-aware storage layout |
| **Real-time sync** | WebSocket for live catalog updates? |
| **Catalog federation** | Query across multiple tenant catalogs? |
| **Open source** | When/if to open source Arco |

### 13.3 Known Limitations

| Limitation | Mitigation |
|------------|------------|
| Eventual consistency in Tier 2 | Document expected lag (5-30s) |
| No cross-table transactions | Commit log is per-tenant atomic |
| Search is basic (v1) | Improve in v2 with vectors |
| Browser needs signed URLs | Short-lived URLs, refresh on expiry |

---

## 14. Appendix

### 14.1 Glossary

| Term | Definition |
|------|------------|
| **Arco** | The serverless lakehouse infrastructure project |
| **Arco Catalog** | The metadata catalog component |
| **Arco Flow** | The orchestration component (Servo integration) |
| **Commit** | An atomic set of catalog changes |
| **Compactor** | Cloud Function that processes event log → Parquet |
| **Event Log** | Append-only log of operational metadata changes |
| **Lineage Edge** | A relationship between source and target tables/columns |
| **Manifest** | JSON file pointing to current snapshot versions |
| **Partition** | A subset of table data (e.g., date=2025-01-15) |
| **Snapshot** | A point-in-time Parquet export of catalog state |
| **Tier 1** | Core catalog with strong consistency (locking) |
| **Tier 2** | Operational metadata with eventual consistency (event log) |
| **Watermark** | Marker indicating last processed event |

### 14.2 References

- [Apache Iceberg Spec](https://iceberg.apache.org/spec/)
- [Delta Lake Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [DuckDB-WASM](https://duckdb.org/docs/api/wasm/overview)
- [Apache DataFusion](https://datafusion.apache.org/)
- [GCS Conditional Requests](https://cloud.google.com/storage/docs/request-preconditions)

### 14.3 Comparison with Alternatives

| Feature | Arco | Unity Catalog | Polaris | AWS Glue |
|---------|------|---------------|---------|----------|
| Serverless | ✅ Full | ❌ Server required | ❌ Server required | ✅ Managed |
| Browser-direct reads | ✅ DuckDB-WASM | ❌ API only | ❌ API only | ❌ API only |
| Open format | ✅ Parquet | ❌ Proprietary | ✅ Iceberg REST | ❌ Proprietary |
| Lineage-by-execution | ✅ Via Servo | ❌ SQL parsing | ❌ SQL parsing | ❌ Limited |
| Multi-cloud | ✅ GCS/S3 | ❌ Databricks | ✅ Any | ❌ AWS only |
| Cost at rest | ~$0 | Server cost | Server cost | ~$0 |

---

*End of Technical Vision Document*
