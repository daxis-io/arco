# Arco Architecture Design Document
## Part 1: Core Architecture

**Version:** 1.0
**Status:** Draft
**Last Updated:** January 2025

---

## Table of Contents

1. [Overview](#1-overview)
2. [Crate Structure](#2-crate-structure)
3. [Storage Layer](#3-storage-layer)
4. [Data Models](#4-data-models)
5. [Write Path: Tier 1 (Core Catalog)](#5-write-path-tier-1-core-catalog)
6. [Write Path: Tier 2 (Operational Metadata)](#6-write-path-tier-2-operational-metadata)
7. [Read Path](#7-read-path)
8. [Consistency Model](#8-consistency-model)
9. [Concurrency Control](#9-concurrency-control)

---

## 1. Overview

This document provides the technical implementation details for Arco's core architecture. It covers the storage layer, data models, and the mechanics of reading and writing catalog metadata.

### 1.1 Design Principles

| Principle | Implementation |
|-----------|----------------|
| **Serverless-first** | No always-on servers; Cloud Functions for async work |
| **Query-native** | All data in Parquet; queryable by DuckDB/DataFusion |
| **Immutable snapshots** | Never mutate; always write new versions |
| **Append-only logs** | Events are immutable; compaction creates new snapshots |
| **Tenant isolation** | Complete storage separation per tenant |

### 1.2 Technology Stack

```
┌─────────────────────────────────────────────────────────┐
│                    APPLICATION LAYER                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
│  │ arco-catalog│  │  arco-flow  │  │  arco-core  │      │
│  │   (Rust)    │  │   (Rust)    │  │   (Rust)    │      │
│  └─────────────┘  └─────────────┘  └─────────────┘      │
├─────────────────────────────────────────────────────────┤
│                     QUERY ENGINES                        │
│  ┌─────────────────────┐  ┌─────────────────────┐       │
│  │    DataFusion       │  │    DuckDB-WASM      │       │
│  │  (Server-side)      │  │   (Browser-side)    │       │
│  └─────────────────────┘  └─────────────────────┘       │
├─────────────────────────────────────────────────────────┤
│                     STORAGE FORMAT                       │
│  ┌─────────────────────────────────────────────────┐    │
│  │              Apache Parquet                      │    │
│  │  • Columnar storage                              │    │
│  │  • Snappy compression                            │    │
│  │  • Row group size: 128MB                         │    │
│  └─────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────┤
│                   OBJECT STORAGE                         │
│  ┌─────────────────────┐  ┌─────────────────────┐       │
│  │   Google Cloud      │  │    Amazon S3        │       │
│  │     Storage         │  │   (future)          │       │
│  └─────────────────────┘  └─────────────────────┘       │
└─────────────────────────────────────────────────────────┘
```

---

## 2. Crate Structure

### 2.1 Workspace Layout

```
arco/
├── Cargo.toml                      # Workspace root
├── crates/
│   ├── arco-core/                  # Shared types and utilities
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── types/              # Core data types
│   │   │   │   ├── mod.rs
│   │   │   │   ├── table.rs
│   │   │   │   ├── column.rs
│   │   │   │   ├── namespace.rs
│   │   │   │   ├── lineage.rs
│   │   │   │   ├── partition.rs
│   │   │   │   ├── execution.rs
│   │   │   │   └── quality.rs
│   │   │   ├── storage/            # Storage abstraction
│   │   │   │   ├── mod.rs
│   │   │   │   ├── backend.rs
│   │   │   │   ├── gcs.rs
│   │   │   │   ├── s3.rs
│   │   │   │   └── local.rs
│   │   │   ├── tenant.rs           # Tenant context
│   │   │   └── error.rs            # Error types
│   │   └── Cargo.toml
│   │
│   ├── arco-catalog/               # Catalog operations
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── registry/           # Table/column registry
│   │   │   │   ├── mod.rs
│   │   │   │   ├── tables.rs
│   │   │   │   ├── columns.rs
│   │   │   │   └── namespaces.rs
│   │   │   ├── lineage/            # Lineage operations
│   │   │   │   ├── mod.rs
│   │   │   │   ├── edges.rs
│   │   │   │   └── traversal.rs
│   │   │   ├── search/             # Search index
│   │   │   │   ├── mod.rs
│   │   │   │   └── fts.rs
│   │   │   ├── writer/             # Write path
│   │   │   │   ├── mod.rs
│   │   │   │   ├── tier1.rs        # Core catalog writer
│   │   │   │   ├── tier2.rs        # Event log writer
│   │   │   │   ├── lock.rs         # Distributed locking
│   │   │   │   └── commit.rs       # Commit log
│   │   │   ├── reader/             # Read path
│   │   │   │   ├── mod.rs
│   │   │   │   ├── snapshot.rs
│   │   │   │   └── fresh.rs
│   │   │   └── compactor/          # Event compaction
│   │   │       ├── mod.rs
│   │   │       └── worker.rs
│   │   └── Cargo.toml
│   │
│   ├── arco-flow/                  # Orchestration (Servo integration)
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── dag.rs              # DAG definitions
│   │   │   ├── scheduler.rs        # Task scheduling
│   │   │   ├── executor.rs         # Task execution
│   │   │   ├── partitions.rs       # Partition tracking
│   │   │   └── lineage.rs          # Execution lineage
│   │   └── Cargo.toml
│   │
│   ├── arco-api/                   # REST API
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── routes/
│   │   │   ├── handlers/
│   │   │   └── middleware/
│   │   └── Cargo.toml
│   │
│   └── arco-compactor/             # Cloud Function for compaction
│       ├── src/
│       │   └── main.rs
│       └── Cargo.toml
│
├── docs/
└── tests/
    └── integration/
```

### 2.2 Crate Dependencies

```
┌─────────────────────────────────────────────────────────┐
│                                                          │
│                      arco-api                            │
│                         │                                │
│            ┌────────────┼────────────┐                   │
│            ▼            ▼            ▼                   │
│     arco-catalog    arco-flow   arco-compactor          │
│            │            │            │                   │
│            └────────────┼────────────┘                   │
│                         ▼                                │
│                    arco-core                             │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

### 2.3 Key Dependencies

```toml
# Cargo.toml (workspace)
[workspace.dependencies]
# Storage
object_store = { version = "0.11", features = ["gcp", "aws"] }

# Query engines
datafusion = "44"
arrow = "54"
parquet = { version = "54", features = ["async"] }

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Async runtime
tokio = { version = "1", features = ["full"] }

# Time
chrono = { version = "0.4", features = ["serde"] }

# IDs
uuid = { version = "1", features = ["v4", "serde"] }

# Error handling
thiserror = "2"
anyhow = "1"

# Tracing
tracing = "0.1"
tracing-subscriber = "0.3"
```

---

## 3. Storage Layer

### 3.1 Storage Backend Trait

```rust
// arco-core/src/storage/backend.rs

use async_trait::async_trait;
use bytes::Bytes;
use std::ops::Range;

/// Represents a conditional write precondition
#[derive(Debug, Clone)]
pub enum WritePrecondition {
    /// Object must not exist
    DoesNotExist,
    /// Object must have this generation/etag
    MatchesGeneration(i64),
    /// No precondition
    None,
}

/// Result of a conditional write
#[derive(Debug)]
pub enum WriteResult {
    /// Write succeeded
    Success { generation: i64 },
    /// Precondition failed
    PreconditionFailed { current_generation: i64 },
}

/// Metadata about a stored object
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub path: String,
    pub size: u64,
    pub generation: i64,
    pub last_modified: chrono::DateTime<chrono::Utc>,
    pub etag: Option<String>,
}

#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    /// Read entire object
    async fn get(&self, path: &str) -> Result<Bytes, StorageError>;

    /// Read byte range (for Parquet column chunks)
    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes, StorageError>;

    /// Write object with optional precondition
    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult, StorageError>;

    /// Delete object
    async fn delete(&self, path: &str) -> Result<(), StorageError>;

    /// List objects with prefix
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StorageError>;

    /// Check if object exists and get metadata
    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>, StorageError>;

    /// Generate signed URL for direct access
    async fn signed_url(&self, path: &str, expiry: std::time::Duration) -> Result<String, StorageError>;
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Object not found: {0}")]
    NotFound(String),

    #[error("Precondition failed")]
    PreconditionFailed,

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Storage error: {0}")]
    Internal(#[from] anyhow::Error),
}
```

### 3.2 GCS Implementation

```rust
// arco-core/src/storage/gcs.rs

use object_store::gcp::GoogleCloudStorage;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};

pub struct GcsBackend {
    store: GoogleCloudStorage,
    bucket: String,
    base_path: String,
}

impl GcsBackend {
    pub async fn new(bucket: &str, base_path: &str) -> Result<Self, StorageError> {
        let store = GoogleCloudStorage::new()
            .with_bucket_name(bucket)
            .build()?;

        Ok(Self {
            store,
            bucket: bucket.to_string(),
            base_path: base_path.to_string(),
        })
    }

    fn full_path(&self, path: &str) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}/{}", self.base_path, path))
    }
}

#[async_trait]
impl StorageBackend for GcsBackend {
    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult, StorageError> {
        let full_path = self.full_path(path);
        let payload = PutPayload::from(data);

        let options = match precondition {
            WritePrecondition::DoesNotExist => {
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                }
            }
            WritePrecondition::MatchesGeneration(gen) => {
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: None,
                        version: Some(gen.to_string()),
                    }),
                    ..Default::default()
                }
            }
            WritePrecondition::None => PutOptions::default(),
        };

        match self.store.put_opts(&full_path, payload, options).await {
            Ok(result) => {
                let generation = result
                    .version
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                Ok(WriteResult::Success { generation })
            }
            Err(object_store::Error::Precondition { .. }) => {
                // Get current generation for caller
                let meta = self.store.head(&full_path).await?;
                let current_gen = meta
                    .version
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                Ok(WriteResult::PreconditionFailed {
                    current_generation: current_gen,
                })
            }
            Err(e) => Err(StorageError::Internal(e.into())),
        }
    }

    async fn get(&self, path: &str) -> Result<Bytes, StorageError> {
        let full_path = self.full_path(path);
        let result = self.store.get(&full_path).await?;
        let bytes = result.bytes().await?;
        Ok(bytes)
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
        let full_path = self.full_path(path);
        let result = self.store.get_range(&full_path, range.start as usize..range.end as usize).await?;
        Ok(result)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StorageError> {
        use futures::TryStreamExt;

        let full_prefix = self.full_path(prefix);
        let stream = self.store.list(Some(&full_prefix));

        let objects: Vec<_> = stream
            .map_ok(|meta| ObjectMeta {
                path: meta.location.to_string(),
                size: meta.size as u64,
                generation: meta.version.and_then(|v| v.parse().ok()).unwrap_or(0),
                last_modified: meta.last_modified,
                etag: meta.e_tag,
            })
            .try_collect()
            .await?;

        Ok(objects)
    }

    // ... other methods
}
```

### 3.3 Tenant-Scoped Storage

```rust
// arco-core/src/storage/mod.rs

/// Storage scoped to a specific tenant
pub struct TenantStorage {
    backend: Arc<dyn StorageBackend>,
    tenant_id: String,
}

impl TenantStorage {
    pub fn new(backend: Arc<dyn StorageBackend>, tenant_id: &str) -> Self {
        Self {
            backend,
            tenant_id: tenant_id.to_string(),
        }
    }

    /// Get path within tenant's catalog directory
    fn catalog_path(&self, path: &str) -> String {
        format!("tenant={}/catalog/{}", self.tenant_id, path)
    }

    // Core catalog paths
    pub fn core_manifest_path(&self) -> String {
        self.catalog_path("core/manifest.json")
    }

    pub fn core_lock_path(&self) -> String {
        self.catalog_path("core/lock.json")
    }

    pub fn core_commit_path(&self, commit_id: &str) -> String {
        self.catalog_path(&format!("core/commits/{}.json", commit_id))
    }

    pub fn core_snapshot_path(&self, version: u64) -> String {
        self.catalog_path(&format!("core/snapshots/v{}", version))
    }

    // Event log paths
    pub fn event_path(&self, date: &str, event_id: &str) -> String {
        self.catalog_path(&format!("events/{}/{}.json", date, event_id))
    }

    pub fn events_prefix(&self, date: &str) -> String {
        self.catalog_path(&format!("events/{}/", date))
    }

    // Operational snapshot paths
    pub fn operational_watermark_path(&self) -> String {
        self.catalog_path("operational/watermark.json")
    }

    pub fn operational_snapshot_path(&self, version: u64) -> String {
        self.catalog_path(&format!("operational/snapshots/v{}", version))
    }

    // Delegate to backend
    pub async fn get(&self, path: &str) -> Result<Bytes, StorageError> {
        self.backend.get(path).await
    }

    pub async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult, StorageError> {
        self.backend.put(path, data, precondition).await
    }

    // ... other delegated methods
}
```

### 3.4 Directory Layout Constants

```rust
// arco-core/src/storage/paths.rs

pub mod paths {
    // Core catalog
    pub const CORE_DIR: &str = "core";
    pub const CORE_MANIFEST: &str = "core/manifest.json";
    pub const CORE_LOCK: &str = "core/lock.json";
    pub const CORE_COMMITS: &str = "core/commits";
    pub const CORE_SNAPSHOTS: &str = "core/snapshots";

    // Core snapshot files
    pub const TABLES_PARQUET: &str = "tables.parquet";
    pub const COLUMNS_PARQUET: &str = "columns.parquet";
    pub const NAMESPACES_PARQUET: &str = "namespaces.parquet";
    pub const LINEAGE_EDGES_PARQUET: &str = "lineage_edges.parquet";
    pub const CONTRACTS_PARQUET: &str = "contracts.parquet";

    // Event log
    pub const EVENTS_DIR: &str = "events";

    // Operational
    pub const OPERATIONAL_DIR: &str = "operational";
    pub const OPERATIONAL_WATERMARK: &str = "operational/watermark.json";
    pub const OPERATIONAL_SNAPSHOTS: &str = "operational/snapshots";

    // Operational snapshot files
    pub const PARTITIONS_PARQUET: &str = "partitions.parquet";
    pub const PARTITION_LINEAGE_PARQUET: &str = "partition_lineage.parquet";
    pub const QUALITY_RESULTS_PARQUET: &str = "quality_results.parquet";
    pub const EXECUTIONS_PARQUET: &str = "executions.parquet";
    pub const EXECUTION_TASKS_PARQUET: &str = "execution_tasks.parquet";

    // Search
    pub const SEARCH_DIR: &str = "search";
    pub const FTS_INDEX_PARQUET: &str = "search/fts_index.parquet";

    // Audit
    pub const AUDIT_DIR: &str = "audit";
}
```

---

## 4. Data Models

### 4.1 Core Types

```rust
// arco-core/src/types/table.rs

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableId(pub Uuid);

impl TableId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TableFormat {
    Delta,
    Iceberg,
    Parquet,
    Csv,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub id: TableId,
    pub namespace: String,
    pub name: String,
    pub location: String,
    pub format: TableFormat,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub properties: HashMap<String, String>,
    pub tags: Vec<String>,
    pub pii_columns: Vec<String>,
    pub row_count: Option<i64>,
    pub size_bytes: Option<i64>,
    pub last_modified: Option<DateTime<Utc>>,
}

impl Table {
    pub fn new(namespace: &str, name: &str, location: &str, format: TableFormat) -> Self {
        let now = Utc::now();
        Self {
            id: TableId::new(),
            namespace: namespace.to_string(),
            name: name.to_string(),
            location: location.to_string(),
            format,
            description: None,
            owner: None,
            created_at: now,
            updated_at: now,
            properties: HashMap::new(),
            tags: Vec::new(),
            pii_columns: Vec::new(),
            row_count: None,
            size_bytes: None,
            last_modified: None,
        }
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }
}
```

```rust
// arco-core/src/types/column.rs

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PiiType {
    Ssn,
    Email,
    Phone,
    Address,
    Name,
    DateOfBirth,
    CreditCard,
    BankAccount,
    IpAddress,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Sensitivity {
    Public,
    Internal,
    Confidential,
    Restricted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub id: ColumnId,
    pub table_id: TableId,
    pub name: String,
    pub data_type: String,
    pub ordinal_position: i32,
    pub is_nullable: bool,
    pub description: Option<String>,
    pub pii_type: Option<PiiType>,
    pub sensitivity: Option<Sensitivity>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

```rust
// arco-core/src/types/lineage.rs

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LineageEdgeId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransformationType {
    Direct,
    Aggregate,
    Expression,
    Case,
    Cast,
    Window,
    Join,
    Filter,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    pub id: LineageEdgeId,
    pub source_table_id: Option<TableId>,
    pub source_column_id: Option<ColumnId>,
    pub target_table_id: Option<TableId>,
    pub target_column_id: Option<ColumnId>,
    pub transformation_type: TransformationType,
    pub transformation_sql: Option<String>,
    /// Hash of task definition for deduplication when same DAG runs repeatedly
    pub transform_fingerprint: Option<String>,
    pub pipeline_id: Option<String>,
    pub execution_id: Option<String>,
    pub first_observed_at: DateTime<Utc>,
    pub last_observed_at: DateTime<Utc>,
    pub observation_count: u64,
    pub created_at: DateTime<Utc>,
}

impl LineageEdge {
    /// Create a table-level lineage edge
    pub fn table_edge(
        source: TableId,
        target: TableId,
        transformation: TransformationType,
    ) -> Self {
        Self {
            id: LineageEdgeId(Uuid::new_v4()),
            source_table_id: Some(source),
            source_column_id: None,
            target_table_id: Some(target),
            target_column_id: None,
            transformation_type: transformation,
            transformation_sql: None,
            pipeline_id: None,
            execution_id: None,
            created_at: Utc::now(),
        }
    }

    /// Create a column-level lineage edge
    pub fn column_edge(
        source_table: TableId,
        source_column: ColumnId,
        target_table: TableId,
        target_column: ColumnId,
        transformation: TransformationType,
    ) -> Self {
        Self {
            id: LineageEdgeId(Uuid::new_v4()),
            source_table_id: Some(source_table),
            source_column_id: Some(source_column),
            target_table_id: Some(target_table),
            target_column_id: Some(target_column),
            transformation_type: transformation,
            transformation_sql: None,
            pipeline_id: None,
            execution_id: None,
            created_at: Utc::now(),
        }
    }
}
```

### 4.2 Operational Types

```rust
// arco-core/src/types/partition.rs

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PartitionStatus {
    Pending,
    Running,
    Complete,
    Failed,
    Stale,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    pub id: PartitionId,
    pub table_id: TableId,
    pub partition_values: HashMap<String, String>,
    pub status: PartitionStatus,
    pub row_count: Option<i64>,
    pub size_bytes: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_successful_run: Option<DateTime<Utc>>,
    pub last_execution_id: Option<String>,
}

impl Partition {
    pub fn partition_spec(&self) -> String {
        self.partition_values
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("/")
    }
}
```

```rust
// arco-core/src/types/execution.rs

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionStatus {
    Pending,
    Running,
    Success,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TriggerType {
    Scheduled,
    Manual,
    Event,
    Backfill,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Execution {
    pub id: String,
    pub pipeline_id: String,
    pub pipeline_name: String,
    pub status: ExecutionStatus,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
    pub trigger_type: TriggerType,
    pub triggered_by: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskStatus {
    Pending,
    Running,
    Success,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTask {
    pub id: String,
    pub execution_id: String,
    pub task_name: String,
    pub status: TaskStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
    pub retry_count: i32,
    pub error_message: Option<String>,
    pub input_tables: Vec<String>,
    pub output_tables: Vec<String>,
    pub input_partitions: Vec<String>,
    pub output_partitions: Vec<String>,
    pub rows_read: Option<i64>,
    pub rows_written: Option<i64>,
    pub bytes_read: Option<i64>,
    pub bytes_written: Option<i64>,
}
```

```rust
// arco-core/src/types/quality.rs

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QualityCheckType {
    Freshness,
    RowCount,
    NullCheck,
    UniqueCheck,
    RangeCheck,
    CustomSql,
    SchemaMatch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QualityStatus {
    Passed,
    Failed,
    Warning,
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityResult {
    pub id: String,
    pub table_id: TableId,
    pub partition_id: Option<PartitionId>,
    pub check_name: String,
    pub check_type: QualityCheckType,
    pub status: QualityStatus,
    pub expected_value: Option<String>,
    pub actual_value: Option<String>,
    pub message: Option<String>,
    pub execution_id: Option<String>,
    pub checked_at: DateTime<Utc>,
}
```

### 4.3 Manifest and Commit Types

Arco uses **four domain manifests** to reduce write contention:
- `catalog.manifest.json` - Tables, columns, namespaces
- `lineage.manifest.json` - Lineage nodes and edges
- `executions.manifest.json` - Task runs, quality results
- `search.manifest.json` - Search postings and trigrams

```rust
// arco-core/src/types/manifest.rs

/// Tail commits not yet compacted into snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestTail {
    pub from_commit_id: String,
    pub to_commit_id: String,
    pub commit_paths: Vec<String>,
}

/// Snapshot metadata with integrity info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub snapshot_id: String,
    pub base_path: String,
    pub files: Vec<SnapshotFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotFile {
    pub logical: String,      // e.g., "tables"
    pub path: String,         // full path to parquet file
    pub rows: u64,
    pub checksum: String,     // sha256:...
}

/// Base manifest structure for all domain manifests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainManifest {
    pub manifest_type: String,
    pub format_version: u32,
    pub published_at: DateTime<Utc>,
    pub snapshot: SnapshotInfo,
    /// Tail commits not yet compacted (for incremental refresh)
    pub tail: Option<ManifestTail>,
    pub integrity: ManifestIntegrity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestIntegrity {
    pub manifest_checksum: String,
    pub signature: Option<String>,  // KMS signature (optional)
}

// Legacy combined manifest (for backward compatibility)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreManifest {
    pub snapshot_version: u64,
    pub snapshot_path: String,
    pub last_commit_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationalManifest {
    pub snapshot_version: u64,
    pub snapshot_path: String,
    pub watermark_event_id: String,
    pub watermark_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchManifest {
    pub index_version: u64,
    pub index_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogManifest {
    pub version: u32,
    pub created_at: DateTime<Utc>,
    pub core: CoreManifest,
    pub operational: OperationalManifest,
    pub search: Option<SearchManifest>,
}
```

```rust
// arco-core/src/types/commit.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum CatalogOperation {
    CreateNamespace {
        namespace_id: String,
        name: String,
        parent: Option<String>,
    },
    DropNamespace {
        namespace_id: String,
    },
    CreateTable {
        table_id: String,
        name: String,
        namespace: String,
        location: String,
        format: String,
    },
    DropTable {
        table_id: String,
    },
    UpdateTable {
        table_id: String,
        #[serde(flatten)]
        updates: HashMap<String, serde_json::Value>,
    },
    AddColumns {
        table_id: String,
        columns: Vec<Column>,
    },
    DropColumns {
        table_id: String,
        column_ids: Vec<String>,
    },
    AddLineage {
        edge_id: String,
        source_table_id: Option<String>,
        source_column_id: Option<String>,
        target_table_id: Option<String>,
        target_column_id: Option<String>,
        transformation_type: String,
    },
    DeleteLineage {
        edge_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit {
    pub commit_id: String,
    pub timestamp: DateTime<Utc>,
    pub parent_commit: Option<String>,
    /// SHA-256 hash of the previous commit for integrity chain
    pub prev_commit_hash: Option<String>,
    pub author: String,
    /// Client-provided ID for idempotency
    pub client_id: Option<String>,
    pub operations: Vec<CatalogOperation>,
    pub snapshot_version: u64,
    /// SHA-256 hash of this commit's content (for verification)
    pub checksum: String,
}

impl Commit {
    pub fn new(author: &str, operations: Vec<CatalogOperation>, snapshot_version: u64) -> Self {
        let mut commit = Self {
            commit_id: format!("{:08}", snapshot_version),
            timestamp: Utc::now(),
            parent_commit: if snapshot_version > 1 {
                Some(format!("{:08}", snapshot_version - 1))
            } else {
                None
            },
            prev_commit_hash: None, // Set by writer after reading previous commit
            author: author.to_string(),
            client_id: None,
            operations,
            snapshot_version,
            checksum: String::new(), // Computed after serialization
        };
        commit.checksum = commit.compute_checksum();
        commit
    }

    pub fn with_client_id(mut self, client_id: &str) -> Self {
        self.client_id = Some(client_id.to_string());
        self
    }

    pub fn with_prev_hash(mut self, hash: &str) -> Self {
        self.prev_commit_hash = Some(hash.to_string());
        self
    }

    fn compute_checksum(&self) -> String {
        use sha2::{Sha256, Digest};
        let content = serde_json::to_string(&self.operations).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(content.as_bytes());
        format!("sha256:{:x}", hasher.finalize())
    }
}
```

### 4.4 Event Types

```rust
// arco-core/src/types/event.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSource {
    #[serde(rename = "type")]
    pub source_type: String,
    pub execution_id: Option<String>,
    pub task_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum CatalogEvent {
    PartitionCreated {
        table_id: String,
        partition_id: String,
        partition_values: HashMap<String, String>,
    },
    PartitionCompleted {
        partition_id: String,
        status: String,
        row_count: Option<i64>,
        size_bytes: Option<i64>,
        duration_ms: Option<i64>,
    },
    PartitionFailed {
        partition_id: String,
        error: String,
    },
    QualityCheckPassed {
        table_id: String,
        partition_id: Option<String>,
        check_name: String,
        check_type: String,
        expected_value: Option<String>,
        actual_value: Option<String>,
    },
    QualityCheckFailed {
        table_id: String,
        partition_id: Option<String>,
        check_name: String,
        check_type: String,
        expected_value: Option<String>,
        actual_value: Option<String>,
        message: String,
    },
    ExecutionStarted {
        execution_id: String,
        pipeline_id: String,
        pipeline_name: String,
        trigger_type: String,
        triggered_by: Option<String>,
    },
    ExecutionCompleted {
        execution_id: String,
        status: String,
        duration_ms: i64,
        error_message: Option<String>,
    },
    TaskStarted {
        execution_id: String,
        task_id: String,
        task_name: String,
    },
    TaskCompleted {
        execution_id: String,
        task_id: String,
        task_name: String,
        status: String,
        duration_ms: i64,
        rows_read: Option<i64>,
        rows_written: Option<i64>,
        input_tables: Vec<String>,
        output_tables: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: String,
    pub event_version: u32,
    pub timestamp: DateTime<Utc>,
    pub source: EventSource,
    #[serde(flatten)]
    pub event: CatalogEvent,
}

impl EventEnvelope {
    pub fn new(source: EventSource, event: CatalogEvent) -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            event_version: 1,
            timestamp: Utc::now(),
            source,
            event,
        }
    }
}
```

### 4.5 Parquet Schema Definitions

```rust
// arco-catalog/src/schema.rs

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

pub fn tables_schema() -> Schema {
    Schema::new(vec![
        Field::new("table_id", DataType::Utf8, false),
        Field::new("namespace", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("location", DataType::Utf8, false),
        Field::new("format", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("owner", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new(
            "properties",
            DataType::Map(
                Arc::new(Field::new("entries", DataType::Struct(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ].into()), false)),
                false,
            ),
            true,
        ),
        Field::new("tags", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
        Field::new("pii_columns", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
        Field::new("row_count", DataType::Int64, true),
        Field::new("size_bytes", DataType::Int64, true),
        Field::new("last_modified", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
    ])
}

pub fn columns_schema() -> Schema {
    Schema::new(vec![
        Field::new("column_id", DataType::Utf8, false),
        Field::new("table_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::Int32, false),
        Field::new("is_nullable", DataType::Boolean, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("pii_type", DataType::Utf8, true),
        Field::new("sensitivity", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ])
}

pub fn lineage_edges_schema() -> Schema {
    Schema::new(vec![
        Field::new("edge_id", DataType::Utf8, false),
        Field::new("source_table_id", DataType::Utf8, true),
        Field::new("source_column_id", DataType::Utf8, true),
        Field::new("target_table_id", DataType::Utf8, true),
        Field::new("target_column_id", DataType::Utf8, true),
        Field::new("transformation_type", DataType::Utf8, false),
        Field::new("transformation_sql", DataType::Utf8, true),
        // Transform fingerprint for deduplication when same DAG runs repeatedly
        Field::new("transform_fingerprint", DataType::Utf8, true),
        Field::new("pipeline_id", DataType::Utf8, true),
        Field::new("execution_id", DataType::Utf8, true),
        Field::new("first_observed_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("last_observed_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("observation_count", DataType::UInt64, false),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ])
}

pub fn partitions_schema() -> Schema {
    Schema::new(vec![
        Field::new("partition_id", DataType::Utf8, false),
        Field::new("table_id", DataType::Utf8, false),
        Field::new(
            "partition_values",
            DataType::Map(
                Arc::new(Field::new("entries", DataType::Struct(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ].into()), false)),
                false,
            ),
            false,
        ),
        Field::new("status", DataType::Utf8, false),
        Field::new("row_count", DataType::Int64, true),
        Field::new("size_bytes", DataType::Int64, true),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("last_successful_run", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        Field::new("last_execution_id", DataType::Utf8, true),
    ])
}

pub fn executions_schema() -> Schema {
    Schema::new(vec![
        Field::new("execution_id", DataType::Utf8, false),
        Field::new("pipeline_id", DataType::Utf8, false),
        Field::new("pipeline_name", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("started_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("completed_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        Field::new("duration_ms", DataType::Int64, true),
        Field::new("trigger_type", DataType::Utf8, false),
        Field::new("triggered_by", DataType::Utf8, true),
        Field::new("error_message", DataType::Utf8, true),
    ])
}

pub fn quality_results_schema() -> Schema {
    Schema::new(vec![
        Field::new("result_id", DataType::Utf8, false),
        Field::new("table_id", DataType::Utf8, false),
        Field::new("partition_id", DataType::Utf8, true),
        Field::new("check_name", DataType::Utf8, false),
        Field::new("check_type", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("expected_value", DataType::Utf8, true),
        Field::new("actual_value", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("execution_id", DataType::Utf8, true),
        Field::new("checked_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ])
}
```

---

## 5. Write Path: Tier 1 (Core Catalog)

### 5.1 Distributed Lock Implementation

```rust
// arco-catalog/src/writer/lock.rs

use std::time::Duration;
use chrono::{DateTime, Utc};

const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);
const LOCK_RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_LOCK_RETRIES: u32 = 50;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockFile {
    pub holder: String,
    pub acquired_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub generation: i64,
}

#[derive(Debug)]
pub struct AcquiredLock {
    pub holder: String,
    pub generation: i64,
    pub expires_at: DateTime<Utc>,
}

pub struct DistributedLock {
    storage: Arc<TenantStorage>,
    holder_id: String,
    lease_duration: Duration,
}

impl DistributedLock {
    pub fn new(storage: Arc<TenantStorage>, holder_id: &str) -> Self {
        Self {
            storage,
            holder_id: holder_id.to_string(),
            lease_duration: DEFAULT_LEASE_DURATION,
        }
    }

    pub fn with_lease_duration(mut self, duration: Duration) -> Self {
        self.lease_duration = duration;
        self
    }

    /// Attempt to acquire the lock with retries
    pub async fn acquire(&self) -> Result<AcquiredLock, LockError> {
        let lock_path = self.storage.core_lock_path();

        for attempt in 0..MAX_LOCK_RETRIES {
            match self.try_acquire_once(&lock_path).await {
                Ok(lock) => return Ok(lock),
                Err(LockError::AlreadyHeld { expires_at, .. }) => {
                    // Check if lock is expired
                    if expires_at < Utc::now() {
                        // Try to steal expired lock
                        if let Ok(lock) = self.try_steal_expired(&lock_path).await {
                            return Ok(lock);
                        }
                    }

                    // Wait and retry
                    if attempt < MAX_LOCK_RETRIES - 1 {
                        tokio::time::sleep(LOCK_RETRY_DELAY).await;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Err(LockError::Timeout)
    }

    async fn try_acquire_once(&self, lock_path: &str) -> Result<AcquiredLock, LockError> {
        let now = Utc::now();
        let expires_at = now + chrono::Duration::from_std(self.lease_duration).unwrap();

        let lock_file = LockFile {
            holder: self.holder_id.clone(),
            acquired_at: now,
            expires_at,
            generation: 0, // Will be set by storage
        };

        let data = serde_json::to_vec_pretty(&lock_file)?;

        match self.storage.put(
            lock_path,
            data.into(),
            WritePrecondition::DoesNotExist,
        ).await? {
            WriteResult::Success { generation } => {
                Ok(AcquiredLock {
                    holder: self.holder_id.clone(),
                    generation,
                    expires_at,
                })
            }
            WriteResult::PreconditionFailed { .. } => {
                // Lock exists, check who holds it
                let existing = self.read_lock(lock_path).await?;
                Err(LockError::AlreadyHeld {
                    holder: existing.holder,
                    expires_at: existing.expires_at,
                })
            }
        }
    }

    async fn try_steal_expired(&self, lock_path: &str) -> Result<AcquiredLock, LockError> {
        let existing = self.read_lock(lock_path).await?;

        if existing.expires_at >= Utc::now() {
            return Err(LockError::AlreadyHeld {
                holder: existing.holder,
                expires_at: existing.expires_at,
            });
        }

        // Try to overwrite with generation check
        let now = Utc::now();
        let expires_at = now + chrono::Duration::from_std(self.lease_duration).unwrap();

        let new_lock = LockFile {
            holder: self.holder_id.clone(),
            acquired_at: now,
            expires_at,
            generation: 0,
        };

        let data = serde_json::to_vec_pretty(&new_lock)?;

        match self.storage.put(
            lock_path,
            data.into(),
            WritePrecondition::MatchesGeneration(existing.generation),
        ).await? {
            WriteResult::Success { generation } => {
                tracing::info!(
                    "Stole expired lock from {} (expired at {})",
                    existing.holder,
                    existing.expires_at
                );
                Ok(AcquiredLock {
                    holder: self.holder_id.clone(),
                    generation,
                    expires_at,
                })
            }
            WriteResult::PreconditionFailed { .. } => {
                Err(LockError::AlreadyHeld {
                    holder: "unknown".to_string(),
                    expires_at: Utc::now(),
                })
            }
        }
    }

    async fn read_lock(&self, lock_path: &str) -> Result<LockFile, LockError> {
        let data = self.storage.get(lock_path).await?;
        let lock: LockFile = serde_json::from_slice(&data)?;
        Ok(lock)
    }

    /// Release the lock
    pub async fn release(&self, lock: &AcquiredLock) -> Result<(), LockError> {
        let lock_path = self.storage.core_lock_path();

        // Verify we still hold the lock before deleting
        let current = self.read_lock(&lock_path).await?;

        if current.holder != lock.holder || current.generation != lock.generation {
            return Err(LockError::NotOwned);
        }

        self.storage.delete(&lock_path).await?;
        Ok(())
    }

    /// Extend the lease duration
    pub async fn extend(&self, lock: &mut AcquiredLock) -> Result<(), LockError> {
        let lock_path = self.storage.core_lock_path();

        let new_expires = Utc::now() + chrono::Duration::from_std(self.lease_duration).unwrap();

        let new_lock = LockFile {
            holder: lock.holder.clone(),
            acquired_at: Utc::now(),
            expires_at: new_expires,
            generation: 0,
        };

        let data = serde_json::to_vec_pretty(&new_lock)?;

        match self.storage.put(
            &lock_path,
            data.into(),
            WritePrecondition::MatchesGeneration(lock.generation),
        ).await? {
            WriteResult::Success { generation } => {
                lock.generation = generation;
                lock.expires_at = new_expires;
                Ok(())
            }
            WriteResult::PreconditionFailed { .. } => {
                Err(LockError::NotOwned)
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error("Lock already held by {holder} until {expires_at}")]
    AlreadyHeld {
        holder: String,
        expires_at: DateTime<Utc>,
    },

    #[error("Lock not owned by this holder")]
    NotOwned,

    #[error("Lock acquisition timed out")]
    Timeout,

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
```

### 5.2 Core Catalog Writer

```rust
// arco-catalog/src/writer/tier1.rs

use crate::writer::lock::{DistributedLock, AcquiredLock};
use crate::snapshot::{CatalogSnapshot, SnapshotWriter};

pub struct CoreCatalogWriter {
    storage: Arc<TenantStorage>,
    lock: DistributedLock,
}

impl CoreCatalogWriter {
    pub fn new(storage: Arc<TenantStorage>, writer_id: &str) -> Self {
        let lock = DistributedLock::new(storage.clone(), writer_id);
        Self { storage, lock }
    }

    /// Execute a transactional write to the core catalog
    pub async fn write<F, T>(&self, author: &str, f: F) -> Result<T, WriteError>
    where
        F: FnOnce(&mut CatalogSnapshot) -> Result<(T, Vec<CatalogOperation>), WriteError>,
    {
        // 1. Acquire lock
        let lock = self.lock.acquire().await?;

        // Use a guard to ensure lock is released
        let result = self.write_with_lock(&lock, author, f).await;

        // 3. Release lock (best effort)
        if let Err(e) = self.lock.release(&lock).await {
            tracing::warn!("Failed to release lock: {}", e);
        }

        result
    }

    async fn write_with_lock<F, T>(
        &self,
        lock: &AcquiredLock,
        author: &str,
        f: F,
    ) -> Result<T, WriteError>
    where
        F: FnOnce(&mut CatalogSnapshot) -> Result<(T, Vec<CatalogOperation>), WriteError>,
    {
        // 2. Read current manifest
        let manifest = self.read_manifest().await?;

        // 3. Load current snapshot
        let mut snapshot = self.load_snapshot(&manifest.core).await?;

        // 4. Apply changes
        let (result, operations) = f(&mut snapshot)?;

        if operations.is_empty() {
            return Ok(result);
        }

        // 5. Write new snapshot
        let new_version = manifest.core.snapshot_version + 1;
        let snapshot_path = self.storage.core_snapshot_path(new_version);

        self.write_snapshot(&snapshot_path, &snapshot).await?;

        // 6. Write commit record
        let commit = Commit::new(author, operations, new_version);
        let commit_path = self.storage.core_commit_path(&commit.commit_id);
        let commit_data = serde_json::to_vec_pretty(&commit)?;

        self.storage.put(
            &commit_path,
            commit_data.into(),
            WritePrecondition::DoesNotExist,
        ).await?;

        // 7. Update manifest (atomic pointer swing)
        let new_manifest = CatalogManifest {
            version: manifest.version,
            created_at: Utc::now(),
            core: CoreManifest {
                snapshot_version: new_version,
                snapshot_path: format!("core/snapshots/v{}", new_version),
                last_commit_id: commit.commit_id.clone(),
            },
            operational: manifest.operational,
            search: manifest.search,
        };

        self.write_manifest(&new_manifest).await?;

        tracing::info!(
            "Committed {} operations as commit {} (snapshot v{})",
            commit.operations.len(),
            commit.commit_id,
            new_version
        );

        Ok(result)
    }

    async fn read_manifest(&self) -> Result<CatalogManifest, WriteError> {
        let path = self.storage.core_manifest_path();

        match self.storage.get(&path).await {
            Ok(data) => {
                let manifest: CatalogManifest = serde_json::from_slice(&data)?;
                Ok(manifest)
            }
            Err(StorageError::NotFound(_)) => {
                // Initialize new catalog
                Ok(CatalogManifest::default())
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn write_manifest(&self, manifest: &CatalogManifest) -> Result<(), WriteError> {
        let path = self.storage.core_manifest_path();
        let data = serde_json::to_vec_pretty(manifest)?;

        // Note: We don't use preconditions here because we hold the lock
        self.storage.put(&path, data.into(), WritePrecondition::None).await?;
        Ok(())
    }

    async fn load_snapshot(&self, core: &CoreManifest) -> Result<CatalogSnapshot, WriteError> {
        if core.snapshot_version == 0 {
            return Ok(CatalogSnapshot::default());
        }

        let snapshot_path = &core.snapshot_path;

        // Load all parquet files
        let tables = self.read_parquet::<Table>(&format!("{}/tables.parquet", snapshot_path)).await?;
        let columns = self.read_parquet::<Column>(&format!("{}/columns.parquet", snapshot_path)).await?;
        let namespaces = self.read_parquet::<Namespace>(&format!("{}/namespaces.parquet", snapshot_path)).await?;
        let lineage_edges = self.read_parquet::<LineageEdge>(&format!("{}/lineage_edges.parquet", snapshot_path)).await?;

        Ok(CatalogSnapshot {
            tables,
            columns,
            namespaces,
            lineage_edges,
        })
    }

    async fn write_snapshot(&self, path: &str, snapshot: &CatalogSnapshot) -> Result<(), WriteError> {
        // Write each parquet file
        self.write_parquet(&format!("{}/tables.parquet", path), &snapshot.tables).await?;
        self.write_parquet(&format!("{}/columns.parquet", path), &snapshot.columns).await?;
        self.write_parquet(&format!("{}/namespaces.parquet", path), &snapshot.namespaces).await?;
        self.write_parquet(&format!("{}/lineage_edges.parquet", path), &snapshot.lineage_edges).await?;

        Ok(())
    }

    async fn read_parquet<T: DeserializeOwned>(&self, path: &str) -> Result<Vec<T>, WriteError> {
        // Implementation uses arrow-rs / parquet-rs
        // ... parquet reading logic
        todo!()
    }

    async fn write_parquet<T: Serialize>(&self, path: &str, data: &[T]) -> Result<(), WriteError> {
        // Implementation uses arrow-rs / parquet-rs
        // ... parquet writing logic
        todo!()
    }
}

/// In-memory representation of catalog state
#[derive(Debug, Default)]
pub struct CatalogSnapshot {
    pub tables: Vec<Table>,
    pub columns: Vec<Column>,
    pub namespaces: Vec<Namespace>,
    pub lineage_edges: Vec<LineageEdge>,
}

impl CatalogSnapshot {
    pub fn add_table(&mut self, table: Table) -> CatalogOperation {
        let op = CatalogOperation::CreateTable {
            table_id: table.id.0.to_string(),
            name: table.name.clone(),
            namespace: table.namespace.clone(),
            location: table.location.clone(),
            format: format!("{:?}", table.format),
        };
        self.tables.push(table);
        op
    }

    pub fn remove_table(&mut self, table_id: &TableId) -> Option<CatalogOperation> {
        if let Some(pos) = self.tables.iter().position(|t| &t.id == table_id) {
            self.tables.remove(pos);
            // Also remove associated columns and lineage
            self.columns.retain(|c| &c.table_id != table_id);
            self.lineage_edges.retain(|e| {
                e.source_table_id.as_ref() != Some(table_id) &&
                e.target_table_id.as_ref() != Some(table_id)
            });
            Some(CatalogOperation::DropTable {
                table_id: table_id.0.to_string(),
            })
        } else {
            None
        }
    }

    pub fn add_lineage_edge(&mut self, edge: LineageEdge) -> CatalogOperation {
        let op = CatalogOperation::AddLineage {
            edge_id: edge.id.0.to_string(),
            source_table_id: edge.source_table_id.as_ref().map(|id| id.0.to_string()),
            source_column_id: edge.source_column_id.as_ref().map(|id| id.0.to_string()),
            target_table_id: edge.target_table_id.as_ref().map(|id| id.0.to_string()),
            target_column_id: edge.target_column_id.as_ref().map(|id| id.0.to_string()),
            transformation_type: format!("{:?}", edge.transformation_type),
        };
        self.lineage_edges.push(edge);
        op
    }

    pub fn find_table_by_name(&self, namespace: &str, name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.namespace == namespace && t.name == name)
    }

    pub fn find_table_by_id(&self, id: &TableId) -> Option<&Table> {
        self.tables.iter().find(|t| &t.id == id)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("Lock error: {0}")]
    Lock(#[from] LockError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Table already exists: {0}")]
    TableExists(String),

    #[error("Validation error: {0}")]
    Validation(String),
}
```

### 5.3 High-Level Write API

```rust
// arco-catalog/src/registry/tables.rs

pub struct TableRegistry {
    writer: Arc<CoreCatalogWriter>,
}

impl TableRegistry {
    pub fn new(writer: Arc<CoreCatalogWriter>) -> Self {
        Self { writer }
    }

    /// Register a new table in the catalog
    pub async fn register_table(
        &self,
        namespace: &str,
        name: &str,
        location: &str,
        format: TableFormat,
        columns: Vec<ColumnDef>,
    ) -> Result<Table, WriteError> {
        self.writer.write("table-registry", |snapshot| {
            // Check if table already exists
            if snapshot.find_table_by_name(namespace, name).is_some() {
                return Err(WriteError::TableExists(format!("{}.{}", namespace, name)));
            }

            // Create table
            let table = Table::new(namespace, name, location, format);
            let table_id = table.id.clone();

            let mut ops = vec![snapshot.add_table(table.clone())];

            // Add columns
            for (i, col_def) in columns.into_iter().enumerate() {
                let column = Column {
                    id: ColumnId(Uuid::new_v4()),
                    table_id: table_id.clone(),
                    name: col_def.name,
                    data_type: col_def.data_type,
                    ordinal_position: i as i32,
                    is_nullable: col_def.is_nullable,
                    description: col_def.description,
                    pii_type: col_def.pii_type,
                    sensitivity: col_def.sensitivity,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                };

                ops.push(CatalogOperation::AddColumns {
                    table_id: table_id.0.to_string(),
                    columns: vec![column.clone()],
                });
                snapshot.columns.push(column);
            }

            Ok((table, ops))
        }).await
    }

    /// Drop a table from the catalog
    pub async fn drop_table(&self, namespace: &str, name: &str) -> Result<(), WriteError> {
        self.writer.write("table-registry", |snapshot| {
            let table = snapshot
                .find_table_by_name(namespace, name)
                .ok_or_else(|| WriteError::TableNotFound(format!("{}.{}", namespace, name)))?;

            let table_id = table.id.clone();

            let op = snapshot.remove_table(&table_id)
                .ok_or_else(|| WriteError::TableNotFound(format!("{}.{}", namespace, name)))?;

            Ok(((), vec![op]))
        }).await
    }

    /// Add lineage between tables
    pub async fn add_table_lineage(
        &self,
        source_table: &str,  // "namespace.table"
        target_table: &str,
        transformation: TransformationType,
    ) -> Result<LineageEdge, WriteError> {
        self.writer.write("table-registry", |snapshot| {
            let (src_ns, src_name) = parse_table_name(source_table)?;
            let (tgt_ns, tgt_name) = parse_table_name(target_table)?;

            let source = snapshot
                .find_table_by_name(src_ns, src_name)
                .ok_or_else(|| WriteError::TableNotFound(source_table.to_string()))?;

            let target = snapshot
                .find_table_by_name(tgt_ns, tgt_name)
                .ok_or_else(|| WriteError::TableNotFound(target_table.to_string()))?;

            let edge = LineageEdge::table_edge(
                source.id.clone(),
                target.id.clone(),
                transformation,
            );

            let op = snapshot.add_lineage_edge(edge.clone());

            Ok((edge, vec![op]))
        }).await
    }
}

fn parse_table_name(full_name: &str) -> Result<(&str, &str), WriteError> {
    let parts: Vec<&str> = full_name.split('.').collect();
    if parts.len() != 2 {
        return Err(WriteError::Validation(
            format!("Invalid table name '{}', expected 'namespace.table'", full_name)
        ));
    }
    Ok((parts[0], parts[1]))
}
```

---

## 6. Write Path: Tier 2 (Operational Metadata)

### 6.1 Event Writer

```rust
// arco-catalog/src/writer/tier2.rs

pub struct EventWriter {
    storage: Arc<TenantStorage>,
    source: EventSource,
}

impl EventWriter {
    pub fn new(storage: Arc<TenantStorage>, source: EventSource) -> Self {
        Self { storage, source }
    }

    /// Write an event to the log (fire-and-forget)
    pub async fn emit(&self, event: CatalogEvent) -> Result<String, EventError> {
        let envelope = EventEnvelope::new(self.source.clone(), event);
        let event_id = envelope.event_id.clone();

        // Generate path: events/YYYY-MM-DD/HH-MM-SS-event_id.json
        let date = envelope.timestamp.format("%Y-%m-%d").to_string();
        let time = envelope.timestamp.format("%H-%M-%S").to_string();
        let filename = format!("{}-{}.json", time, &event_id[..8]);

        let path = self.storage.event_path(&date, &filename);
        let data = serde_json::to_vec(&envelope)?;

        // Write without precondition (event IDs are unique)
        self.storage.put(&path, data.into(), WritePrecondition::None).await?;

        tracing::debug!("Emitted event {} to {}", event_id, path);

        Ok(event_id)
    }

    /// Emit a batch of events
    pub async fn emit_batch(&self, events: Vec<CatalogEvent>) -> Result<Vec<String>, EventError> {
        let mut event_ids = Vec::with_capacity(events.len());

        // Could parallelize this with futures::join_all
        for event in events {
            let id = self.emit(event).await?;
            event_ids.push(id);
        }

        Ok(event_ids)
    }

    /// Emit with idempotency - prevents duplicate events if Servo retries
    pub async fn emit_idempotent(
        &self,
        client_id: &str,
        event: CatalogEvent,
    ) -> Result<String, EventError> {
        // Check if already processed
        let idempotency_path = format!("_catalog/idempotency/{}.json", client_id);

        if let Ok(Some(_)) = self.storage.head(&idempotency_path).await {
            tracing::debug!("Event with client_id {} already processed, skipping", client_id);
            return Ok(client_id.to_string());
        }

        // Emit the event
        let event_id = self.emit(event).await?;

        // Record idempotency marker
        let marker = serde_json::json!({
            "client_id": client_id,
            "event_id": event_id,
            "processed_at": chrono::Utc::now()
        });
        let _ = self.storage.put(
            &idempotency_path,
            serde_json::to_vec(&marker)?.into(),
            WritePrecondition::DoesNotExist,
        ).await;

        Ok(event_id)
    }
}

/// Convenience methods for common events
impl EventWriter {
    pub async fn partition_completed(
        &self,
        partition_id: &str,
        row_count: Option<i64>,
        size_bytes: Option<i64>,
        duration_ms: Option<i64>,
    ) -> Result<String, EventError> {
        self.emit(CatalogEvent::PartitionCompleted {
            partition_id: partition_id.to_string(),
            status: "COMPLETE".to_string(),
            row_count,
            size_bytes,
            duration_ms,
        }).await
    }

    pub async fn partition_failed(
        &self,
        partition_id: &str,
        error: &str,
    ) -> Result<String, EventError> {
        self.emit(CatalogEvent::PartitionFailed {
            partition_id: partition_id.to_string(),
            error: error.to_string(),
        }).await
    }

    pub async fn quality_check_passed(
        &self,
        table_id: &str,
        check_name: &str,
        check_type: &str,
    ) -> Result<String, EventError> {
        self.emit(CatalogEvent::QualityCheckPassed {
            table_id: table_id.to_string(),
            partition_id: None,
            check_name: check_name.to_string(),
            check_type: check_type.to_string(),
            expected_value: None,
            actual_value: None,
        }).await
    }

    pub async fn execution_started(
        &self,
        execution_id: &str,
        pipeline_id: &str,
        pipeline_name: &str,
        trigger_type: &str,
    ) -> Result<String, EventError> {
        self.emit(CatalogEvent::ExecutionStarted {
            execution_id: execution_id.to_string(),
            pipeline_id: pipeline_id.to_string(),
            pipeline_name: pipeline_name.to_string(),
            trigger_type: trigger_type.to_string(),
            triggered_by: None,
        }).await
    }

    pub async fn execution_completed(
        &self,
        execution_id: &str,
        status: &str,
        duration_ms: i64,
        error_message: Option<String>,
    ) -> Result<String, EventError> {
        self.emit(CatalogEvent::ExecutionCompleted {
            execution_id: execution_id.to_string(),
            status: status.to_string(),
            duration_ms,
            error_message,
        }).await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EventError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
```

### 6.2 Event Compactor

```rust
// arco-catalog/src/compactor/worker.rs

use std::collections::HashMap;

pub struct EventCompactor {
    storage: Arc<TenantStorage>,
}

impl EventCompactor {
    pub fn new(storage: Arc<TenantStorage>) -> Self {
        Self { storage }
    }

    /// Run one compaction cycle
    pub async fn compact(&self) -> Result<CompactionResult, CompactionError> {
        // 1. Read current watermark
        let watermark = self.read_watermark().await?;

        // 2. List new events since watermark
        let events = self.list_events_since(&watermark).await?;

        if events.is_empty() {
            return Ok(CompactionResult {
                events_processed: 0,
                new_snapshot_version: watermark.version,
            });
        }

        tracing::info!("Processing {} new events", events.len());

        // 3. Load current operational state
        let mut state = self.load_operational_state(&watermark).await?;

        // 4. Apply events in order
        let sorted_events = self.sort_events(events);
        let mut last_event: Option<&EventEnvelope> = None;

        for event in &sorted_events {
            self.apply_event(&mut state, event)?;
            last_event = Some(event);
        }

        // 5. Write new snapshots
        let new_version = watermark.version + 1;
        self.write_operational_snapshot(new_version, &state).await?;

        // 6. Update watermark
        let new_watermark = Watermark {
            version: new_version,
            last_event_id: last_event.unwrap().event_id.clone(),
            last_timestamp: last_event.unwrap().timestamp,
            events_processed: watermark.events_processed + sorted_events.len() as u64,
            compacted_at: Utc::now(),
        };
        self.write_watermark(&new_watermark).await?;

        // 7. Optionally archive processed events
        // self.archive_events(&sorted_events).await?;

        Ok(CompactionResult {
            events_processed: sorted_events.len(),
            new_snapshot_version: new_version,
        })
    }

    async fn read_watermark(&self) -> Result<Watermark, CompactionError> {
        let path = self.storage.operational_watermark_path();

        match self.storage.get(&path).await {
            Ok(data) => {
                let watermark: Watermark = serde_json::from_slice(&data)?;
                Ok(watermark)
            }
            Err(StorageError::NotFound(_)) => {
                Ok(Watermark::default())
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn write_watermark(&self, watermark: &Watermark) -> Result<(), CompactionError> {
        let path = self.storage.operational_watermark_path();
        let data = serde_json::to_vec_pretty(watermark)?;
        self.storage.put(&path, data.into(), WritePrecondition::None).await?;
        Ok(())
    }

    async fn list_events_since(&self, watermark: &Watermark) -> Result<Vec<EventEnvelope>, CompactionError> {
        let mut all_events = Vec::new();

        // List events for today and yesterday (to handle day boundaries)
        let today = Utc::now().format("%Y-%m-%d").to_string();
        let yesterday = (Utc::now() - chrono::Duration::days(1)).format("%Y-%m-%d").to_string();

        for date in [&yesterday, &today] {
            let prefix = self.storage.events_prefix(date);
            let objects = self.storage.list(&prefix).await?;

            for obj in objects {
                let data = self.storage.get(&obj.path).await?;
                let event: EventEnvelope = serde_json::from_slice(&data)?;

                // Filter events after watermark
                if event.timestamp > watermark.last_timestamp {
                    all_events.push(event);
                }
            }
        }

        Ok(all_events)
    }

    fn sort_events(&self, mut events: Vec<EventEnvelope>) -> Vec<EventEnvelope> {
        events.sort_by(|a, b| {
            a.timestamp.cmp(&b.timestamp)
                .then_with(|| a.event_id.cmp(&b.event_id))
        });
        events
    }

    async fn load_operational_state(&self, watermark: &Watermark) -> Result<OperationalState, CompactionError> {
        if watermark.version == 0 {
            return Ok(OperationalState::default());
        }

        let snapshot_path = self.storage.operational_snapshot_path(watermark.version);

        let partitions = self.read_parquet(&format!("{}/partitions.parquet", snapshot_path)).await?;
        let quality_results = self.read_parquet(&format!("{}/quality_results.parquet", snapshot_path)).await?;
        let executions = self.read_parquet(&format!("{}/executions.parquet", snapshot_path)).await?;
        let execution_tasks = self.read_parquet(&format!("{}/execution_tasks.parquet", snapshot_path)).await?;

        Ok(OperationalState {
            partitions,
            quality_results,
            executions,
            execution_tasks,
        })
    }

    async fn write_operational_snapshot(
        &self,
        version: u64,
        state: &OperationalState,
    ) -> Result<(), CompactionError> {
        let snapshot_path = self.storage.operational_snapshot_path(version);

        self.write_parquet(&format!("{}/partitions.parquet", snapshot_path), &state.partitions).await?;
        self.write_parquet(&format!("{}/quality_results.parquet", snapshot_path), &state.quality_results).await?;
        self.write_parquet(&format!("{}/executions.parquet", snapshot_path), &state.executions).await?;
        self.write_parquet(&format!("{}/execution_tasks.parquet", snapshot_path), &state.execution_tasks).await?;

        Ok(())
    }

    fn apply_event(&self, state: &mut OperationalState, event: &EventEnvelope) -> Result<(), CompactionError> {
        match &event.event {
            CatalogEvent::PartitionCreated { table_id, partition_id, partition_values } => {
                state.partitions.push(Partition {
                    id: PartitionId(Uuid::parse_str(partition_id)?),
                    table_id: TableId(Uuid::parse_str(table_id)?),
                    partition_values: partition_values.clone(),
                    status: PartitionStatus::Pending,
                    row_count: None,
                    size_bytes: None,
                    created_at: event.timestamp,
                    updated_at: event.timestamp,
                    last_successful_run: None,
                    last_execution_id: event.source.execution_id.clone(),
                });
            }

            CatalogEvent::PartitionCompleted { partition_id, status, row_count, size_bytes, .. } => {
                if let Some(partition) = state.partitions.iter_mut()
                    .find(|p| p.id.0.to_string() == *partition_id)
                {
                    partition.status = match status.as_str() {
                        "COMPLETE" => PartitionStatus::Complete,
                        "FAILED" => PartitionStatus::Failed,
                        _ => PartitionStatus::Pending,
                    };
                    partition.row_count = *row_count;
                    partition.size_bytes = *size_bytes;
                    partition.updated_at = event.timestamp;
                    if partition.status == PartitionStatus::Complete {
                        partition.last_successful_run = Some(event.timestamp);
                    }
                    partition.last_execution_id = event.source.execution_id.clone();
                }
            }

            CatalogEvent::PartitionFailed { partition_id, error } => {
                if let Some(partition) = state.partitions.iter_mut()
                    .find(|p| p.id.0.to_string() == *partition_id)
                {
                    partition.status = PartitionStatus::Failed;
                    partition.updated_at = event.timestamp;
                }
            }

            CatalogEvent::QualityCheckPassed { table_id, partition_id, check_name, check_type, expected_value, actual_value } |
            CatalogEvent::QualityCheckFailed { table_id, partition_id, check_name, check_type, expected_value, actual_value, .. } => {
                let status = match &event.event {
                    CatalogEvent::QualityCheckPassed { .. } => QualityStatus::Passed,
                    CatalogEvent::QualityCheckFailed { .. } => QualityStatus::Failed,
                    _ => unreachable!(),
                };

                let message = match &event.event {
                    CatalogEvent::QualityCheckFailed { message, .. } => Some(message.clone()),
                    _ => None,
                };

                state.quality_results.push(QualityResult {
                    id: event.event_id.clone(),
                    table_id: TableId(Uuid::parse_str(table_id)?),
                    partition_id: partition_id.as_ref().map(|id| PartitionId(Uuid::parse_str(id).unwrap())),
                    check_name: check_name.clone(),
                    check_type: check_type.parse().unwrap_or(QualityCheckType::CustomSql),
                    status,
                    expected_value: expected_value.clone(),
                    actual_value: actual_value.clone(),
                    message,
                    execution_id: event.source.execution_id.clone(),
                    checked_at: event.timestamp,
                });
            }

            CatalogEvent::ExecutionStarted { execution_id, pipeline_id, pipeline_name, trigger_type, triggered_by } => {
                state.executions.push(Execution {
                    id: execution_id.clone(),
                    pipeline_id: pipeline_id.clone(),
                    pipeline_name: pipeline_name.clone(),
                    status: ExecutionStatus::Running,
                    started_at: event.timestamp,
                    completed_at: None,
                    duration_ms: None,
                    trigger_type: trigger_type.parse().unwrap_or(TriggerType::Manual),
                    triggered_by: triggered_by.clone(),
                    error_message: None,
                });
            }

            CatalogEvent::ExecutionCompleted { execution_id, status, duration_ms, error_message } => {
                if let Some(execution) = state.executions.iter_mut()
                    .find(|e| e.id == *execution_id)
                {
                    execution.status = match status.as_str() {
                        "SUCCESS" => ExecutionStatus::Success,
                        "FAILED" => ExecutionStatus::Failed,
                        "CANCELLED" => ExecutionStatus::Cancelled,
                        _ => ExecutionStatus::Failed,
                    };
                    execution.completed_at = Some(event.timestamp);
                    execution.duration_ms = Some(*duration_ms);
                    execution.error_message = error_message.clone();
                }
            }

            CatalogEvent::TaskStarted { execution_id, task_id, task_name } => {
                state.execution_tasks.push(ExecutionTask {
                    id: task_id.clone(),
                    execution_id: execution_id.clone(),
                    task_name: task_name.clone(),
                    status: TaskStatus::Running,
                    started_at: Some(event.timestamp),
                    completed_at: None,
                    duration_ms: None,
                    retry_count: 0,
                    error_message: None,
                    input_tables: vec![],
                    output_tables: vec![],
                    input_partitions: vec![],
                    output_partitions: vec![],
                    rows_read: None,
                    rows_written: None,
                    bytes_read: None,
                    bytes_written: None,
                });
            }

            CatalogEvent::TaskCompleted { execution_id, task_id, status, duration_ms, rows_read, rows_written, input_tables, output_tables, .. } => {
                if let Some(task) = state.execution_tasks.iter_mut()
                    .find(|t| t.id == *task_id && t.execution_id == *execution_id)
                {
                    task.status = match status.as_str() {
                        "SUCCESS" => TaskStatus::Success,
                        "FAILED" => TaskStatus::Failed,
                        "SKIPPED" => TaskStatus::Skipped,
                        _ => TaskStatus::Failed,
                    };
                    task.completed_at = Some(event.timestamp);
                    task.duration_ms = Some(*duration_ms);
                    task.rows_read = *rows_read;
                    task.rows_written = *rows_written;
                    task.input_tables = input_tables.clone();
                    task.output_tables = output_tables.clone();
                }
            }
        }

        Ok(())
    }

    // Parquet I/O helpers (similar to CoreCatalogWriter)
    async fn read_parquet<T: DeserializeOwned>(&self, path: &str) -> Result<Vec<T>, CompactionError> {
        todo!()
    }

    async fn write_parquet<T: Serialize>(&self, path: &str, data: &[T]) -> Result<(), CompactionError> {
        todo!()
    }
}

#[derive(Debug, Default)]
pub struct OperationalState {
    pub partitions: Vec<Partition>,
    pub quality_results: Vec<QualityResult>,
    pub executions: Vec<Execution>,
    pub execution_tasks: Vec<ExecutionTask>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Watermark {
    pub version: u64,
    pub last_event_id: String,
    pub last_timestamp: DateTime<Utc>,
    pub events_processed: u64,
    pub compacted_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct CompactionResult {
    pub events_processed: usize,
    pub new_snapshot_version: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum CompactionError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("UUID parse error: {0}")]
    UuidParse(#[from] uuid::Error),
}
```

---

## 7. Read Path

### 7.1 Snapshot Reader

```rust
// arco-catalog/src/reader/snapshot.rs

use datafusion::prelude::*;

pub struct CatalogReader {
    storage: Arc<TenantStorage>,
    ctx: SessionContext,
}

impl CatalogReader {
    pub async fn new(storage: Arc<TenantStorage>) -> Result<Self, ReaderError> {
        let ctx = SessionContext::new();

        // Register object store with DataFusion
        // ctx.runtime_env().register_object_store(...);

        Ok(Self { storage, ctx })
    }

    /// Get current manifest
    pub async fn manifest(&self) -> Result<CatalogManifest, ReaderError> {
        let path = self.storage.core_manifest_path();
        let data = self.storage.get(&path).await?;
        let manifest: CatalogManifest = serde_json::from_slice(&data)?;
        Ok(manifest)
    }

    /// List all tables
    pub async fn list_tables(&self) -> Result<Vec<Table>, ReaderError> {
        let manifest = self.manifest().await?;
        let path = format!(
            "{}/{}/{}",
            self.storage.base_path(),
            manifest.core.snapshot_path,
            paths::TABLES_PARQUET
        );

        let df = self.ctx.read_parquet(&path, ParquetReadOptions::default()).await?;
        let batches = df.collect().await?;

        // Convert Arrow batches to Table structs
        let tables = self.batches_to_tables(batches)?;
        Ok(tables)
    }

    /// List tables in a namespace
    pub async fn list_tables_in_namespace(&self, namespace: &str) -> Result<Vec<Table>, ReaderError> {
        let manifest = self.manifest().await?;
        let path = format!(
            "{}/{}/{}",
            self.storage.base_path(),
            manifest.core.snapshot_path,
            paths::TABLES_PARQUET
        );

        let df = self.ctx.read_parquet(&path, ParquetReadOptions::default()).await?
            .filter(col("namespace").eq(lit(namespace)))?;

        let batches = df.collect().await?;
        let tables = self.batches_to_tables(batches)?;
        Ok(tables)
    }

    /// Get a specific table by name
    pub async fn get_table(&self, namespace: &str, name: &str) -> Result<Option<Table>, ReaderError> {
        let manifest = self.manifest().await?;
        let path = format!(
            "{}/{}/{}",
            self.storage.base_path(),
            manifest.core.snapshot_path,
            paths::TABLES_PARQUET
        );

        let df = self.ctx.read_parquet(&path, ParquetReadOptions::default()).await?
            .filter(
                col("namespace").eq(lit(namespace))
                    .and(col("name").eq(lit(name)))
            )?;

        let batches = df.collect().await?;
        let tables = self.batches_to_tables(batches)?;
        Ok(tables.into_iter().next())
    }

    /// Get columns for a table
    pub async fn get_columns(&self, table_id: &str) -> Result<Vec<Column>, ReaderError> {
        let manifest = self.manifest().await?;
        let path = format!(
            "{}/{}/{}",
            self.storage.base_path(),
            manifest.core.snapshot_path,
            paths::COLUMNS_PARQUET
        );

        let df = self.ctx.read_parquet(&path, ParquetReadOptions::default()).await?
            .filter(col("table_id").eq(lit(table_id)))?
            .sort(vec![col("ordinal_position").sort(true, true)])?;

        let batches = df.collect().await?;
        let columns = self.batches_to_columns(batches)?;
        Ok(columns)
    }

    /// Get upstream lineage for a table
    pub async fn get_upstream_lineage(&self, table_id: &str) -> Result<Vec<LineageEdge>, ReaderError> {
        let manifest = self.manifest().await?;
        let path = format!(
            "{}/{}/{}",
            self.storage.base_path(),
            manifest.core.snapshot_path,
            paths::LINEAGE_EDGES_PARQUET
        );

        let df = self.ctx.read_parquet(&path, ParquetReadOptions::default()).await?
            .filter(col("target_table_id").eq(lit(table_id)))?;

        let batches = df.collect().await?;
        let edges = self.batches_to_lineage_edges(batches)?;
        Ok(edges)
    }

    /// Get downstream lineage for a table
    pub async fn get_downstream_lineage(&self, table_id: &str) -> Result<Vec<LineageEdge>, ReaderError> {
        let manifest = self.manifest().await?;
        let path = format!(
            "{}/{}/{}",
            self.storage.base_path(),
            manifest.core.snapshot_path,
            paths::LINEAGE_EDGES_PARQUET
        );

        let df = self.ctx.read_parquet(&path, ParquetReadOptions::default()).await?
            .filter(col("source_table_id").eq(lit(table_id)))?;

        let batches = df.collect().await?;
        let edges = self.batches_to_lineage_edges(batches)?;
        Ok(edges)
    }

    /// Search tables by name or description
    pub async fn search_tables(&self, query: &str) -> Result<Vec<Table>, ReaderError> {
        let manifest = self.manifest().await?;
        let path = format!(
            "{}/{}/{}",
            self.storage.base_path(),
            manifest.core.snapshot_path,
            paths::TABLES_PARQUET
        );

        let pattern = format!("%{}%", query.to_lowercase());

        let df = self.ctx.read_parquet(&path, ParquetReadOptions::default()).await?
            .filter(
                lower(col("name")).like(lit(&pattern))
                    .or(lower(col("description")).like(lit(&pattern)))
            )?;

        let batches = df.collect().await?;
        let tables = self.batches_to_tables(batches)?;
        Ok(tables)
    }

    // Conversion helpers
    fn batches_to_tables(&self, batches: Vec<RecordBatch>) -> Result<Vec<Table>, ReaderError> {
        // Convert Arrow RecordBatch to Vec<Table>
        todo!()
    }

    fn batches_to_columns(&self, batches: Vec<RecordBatch>) -> Result<Vec<Column>, ReaderError> {
        todo!()
    }

    fn batches_to_lineage_edges(&self, batches: Vec<RecordBatch>) -> Result<Vec<LineageEdge>, ReaderError> {
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}
```

### 7.2 Fresh Reader (Snapshot + Event Tail)

```rust
// arco-catalog/src/reader/fresh.rs

/// Reader that combines snapshot with uncommitted events for fresh reads
pub struct FreshReader {
    snapshot_reader: CatalogReader,
    storage: Arc<TenantStorage>,
}

impl FreshReader {
    pub fn new(snapshot_reader: CatalogReader, storage: Arc<TenantStorage>) -> Self {
        Self { snapshot_reader, storage }
    }

    /// Get partitions with pending event updates applied
    pub async fn get_partitions_fresh(&self, table_id: &str) -> Result<Vec<Partition>, ReaderError> {
        // 1. Read from operational snapshot
        let manifest = self.snapshot_reader.manifest().await?;
        let mut partitions = self.read_partitions_from_snapshot(&manifest.operational).await?;

        // 2. Read uncommitted events
        let watermark = self.read_watermark().await?;
        let events = self.read_events_since(&watermark).await?;

        // 3. Apply events to partition list
        for event in events {
            self.apply_partition_event(&mut partitions, &event);
        }

        // 4. Filter by table
        let filtered: Vec<_> = partitions.into_iter()
            .filter(|p| p.table_id.0.to_string() == table_id)
            .collect();

        Ok(filtered)
    }

    /// Get execution status with pending updates
    pub async fn get_execution_fresh(&self, execution_id: &str) -> Result<Option<Execution>, ReaderError> {
        let manifest = self.snapshot_reader.manifest().await?;
        let mut executions = self.read_executions_from_snapshot(&manifest.operational).await?;

        let watermark = self.read_watermark().await?;
        let events = self.read_events_since(&watermark).await?;

        for event in events {
            self.apply_execution_event(&mut executions, &event);
        }

        Ok(executions.into_iter().find(|e| e.id == execution_id))
    }

    async fn read_watermark(&self) -> Result<Watermark, ReaderError> {
        let path = self.storage.operational_watermark_path();
        match self.storage.get(&path).await {
            Ok(data) => Ok(serde_json::from_slice(&data)?),
            Err(StorageError::NotFound(_)) => Ok(Watermark::default()),
            Err(e) => Err(e.into()),
        }
    }

    async fn read_events_since(&self, watermark: &Watermark) -> Result<Vec<EventEnvelope>, ReaderError> {
        // Similar to compactor's list_events_since
        todo!()
    }

    async fn read_partitions_from_snapshot(&self, manifest: &OperationalManifest) -> Result<Vec<Partition>, ReaderError> {
        todo!()
    }

    async fn read_executions_from_snapshot(&self, manifest: &OperationalManifest) -> Result<Vec<Execution>, ReaderError> {
        todo!()
    }

    fn apply_partition_event(&self, partitions: &mut Vec<Partition>, event: &EventEnvelope) {
        // Same logic as compactor
    }

    fn apply_execution_event(&self, executions: &mut Vec<Execution>, event: &EventEnvelope) {
        // Same logic as compactor
    }
}
```

### 7.3 Signed URL Generation for Browser Access

```rust
// arco-catalog/src/reader/browser.rs

use std::time::Duration;

const DEFAULT_URL_EXPIRY: Duration = Duration::from_secs(3600); // 1 hour

pub struct BrowserAccessProvider {
    storage: Arc<TenantStorage>,
}

impl BrowserAccessProvider {
    pub fn new(storage: Arc<TenantStorage>) -> Self {
        Self { storage }
    }

    /// Generate signed URLs for browser-based DuckDB-WASM access
    pub async fn get_catalog_urls(&self) -> Result<CatalogUrls, ReaderError> {
        let manifest = self.storage.get(&self.storage.core_manifest_path()).await?;
        let manifest: CatalogManifest = serde_json::from_slice(&manifest)?;

        let snapshot_path = &manifest.core.snapshot_path;

        // Generate signed URLs for each parquet file
        let tables_url = self.storage.signed_url(
            &format!("{}/tables.parquet", snapshot_path),
            DEFAULT_URL_EXPIRY,
        ).await?;

        let columns_url = self.storage.signed_url(
            &format!("{}/columns.parquet", snapshot_path),
            DEFAULT_URL_EXPIRY,
        ).await?;

        let lineage_url = self.storage.signed_url(
            &format!("{}/lineage_edges.parquet", snapshot_path),
            DEFAULT_URL_EXPIRY,
        ).await?;

        // Operational snapshot URLs
        let op_snapshot_path = &manifest.operational.snapshot_path;

        let partitions_url = self.storage.signed_url(
            &format!("{}/partitions.parquet", op_snapshot_path),
            DEFAULT_URL_EXPIRY,
        ).await?;

        let quality_url = self.storage.signed_url(
            &format!("{}/quality_results.parquet", op_snapshot_path),
            DEFAULT_URL_EXPIRY,
        ).await?;

        let executions_url = self.storage.signed_url(
            &format!("{}/executions.parquet", op_snapshot_path),
            DEFAULT_URL_EXPIRY,
        ).await?;

        Ok(CatalogUrls {
            manifest_version: manifest.version,
            core_snapshot_version: manifest.core.snapshot_version,
            operational_snapshot_version: manifest.operational.snapshot_version,
            expires_at: Utc::now() + chrono::Duration::from_std(DEFAULT_URL_EXPIRY).unwrap(),
            urls: CatalogFileUrls {
                tables: tables_url,
                columns: columns_url,
                lineage_edges: lineage_url,
                partitions: partitions_url,
                quality_results: quality_url,
                executions: executions_url,
            },
        })
    }
}

#[derive(Debug, Serialize)]
pub struct CatalogUrls {
    pub manifest_version: u32,
    pub core_snapshot_version: u64,
    pub operational_snapshot_version: u64,
    pub expires_at: DateTime<Utc>,
    pub urls: CatalogFileUrls,
}

#[derive(Debug, Serialize)]
pub struct CatalogFileUrls {
    pub tables: String,
    pub columns: String,
    pub lineage_edges: String,
    pub partitions: String,
    pub quality_results: String,
    pub executions: String,
}
```

---

## 8. Consistency Model

### 8.1 Tier 1: Strong Consistency

```
┌─────────────────────────────────────────────────────────────────────┐
│                    TIER 1 CONSISTENCY MODEL                          │
│                                                                      │
│  Guarantee: Serializable writes, read-your-writes                   │
│                                                                      │
│  Mechanism:                                                          │
│  1. Single writer at a time (distributed lock)                      │
│  2. Read current snapshot before write                               │
│  3. Write new snapshot + commit atomically                           │
│  4. Update manifest pointer (atomic swap)                            │
│                                                                      │
│  Timeline:                                                           │
│                                                                      │
│  Writer A:  [acquire lock]──[read]──[write]──[release]              │
│                                                                      │
│  Writer B:  ─────────────[wait]─────────────[acquire]──[read]──...  │
│                                                                      │
│  Reader:    ────[read v1]────────────[read v2]─────────────────     │
│                               ▲                                      │
│                               │                                      │
│                     manifest updated                                 │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**Guarantees:**
- **Serializable**: All writes appear to execute in a total order
- **Read-your-writes**: After a write completes, subsequent reads see the update
- **External consistency**: If write A completes before write B starts, A precedes B

### 8.2 Tier 2: Eventual Consistency

```
┌─────────────────────────────────────────────────────────────────────┐
│                    TIER 2 CONSISTENCY MODEL                          │
│                                                                      │
│  Guarantee: Eventual consistency with bounded staleness             │
│                                                                      │
│  Mechanism:                                                          │
│  1. Writers append events (no coordination)                         │
│  2. Compactor processes events periodically                          │
│  3. Readers see snapshot state (may be stale)                        │
│                                                                      │
│  Timeline:                                                           │
│                                                                      │
│  Writer A:  [emit event]                                            │
│  Writer B:       [emit event]                                       │
│  Writer C:            [emit event]                                  │
│                                                                      │
│  Compactor: ────────────────────[process events]──[write snapshot]  │
│                                                                      │
│  Reader:    [read snapshot v1]────────────────────[read snapshot v2]│
│                                                    ▲                 │
│                                          now sees A, B, C           │
│                                                                      │
│  Staleness: ~5-30 seconds (configurable compaction frequency)       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**Guarantees:**
- **Eventual consistency**: All events eventually appear in snapshots
- **Causal ordering**: Events from same source maintain order
- **Bounded staleness**: Maximum delay = compaction interval + processing time

**Fresh Read Option:**
```
Reader:    [read snapshot]──[read event log tail]──[merge in memory]
                                                    ▲
                                            sees all events
```

### 8.3 Consistency Boundaries

| Operation | Tier | Consistency | Typical Latency |
|-----------|------|-------------|-----------------|
| Create table | 1 | Strong | 200-500ms |
| Update schema | 1 | Strong | 200-500ms |
| Add lineage edge | 1 | Strong | 200-500ms |
| Update partition status | 2 | Eventual | 50-100ms write, 5-30s visible |
| Record quality result | 2 | Eventual | 50-100ms write, 5-30s visible |
| Record execution | 2 | Eventual | 50-100ms write, 5-30s visible |

---

## 9. Concurrency Control

### 9.1 Lock Contention Handling

```rust
// Retry with exponential backoff
pub async fn acquire_with_backoff(lock: &DistributedLock) -> Result<AcquiredLock, LockError> {
    let mut delay = Duration::from_millis(50);
    let max_delay = Duration::from_secs(5);
    let max_attempts = 20;

    for attempt in 0..max_attempts {
        match lock.acquire().await {
            Ok(acquired) => return Ok(acquired),
            Err(LockError::AlreadyHeld { expires_at, .. }) => {
                if attempt < max_attempts - 1 {
                    // Wait with jitter
                    let jitter = rand::thread_rng().gen_range(0..50);
                    tokio::time::sleep(delay + Duration::from_millis(jitter)).await;
                    delay = std::cmp::min(delay * 2, max_delay);
                }
            }
            Err(e) => return Err(e),
        }
    }

    Err(LockError::Timeout)
}
```

### 9.2 Lease Extension for Long Operations

```rust
pub async fn with_lease_extension<F, T>(
    lock: &DistributedLock,
    mut acquired: AcquiredLock,
    operation: F,
) -> Result<T, WriteError>
where
    F: Future<Output = Result<T, WriteError>>,
{
    // Spawn background task to extend lease
    let (tx, mut rx) = tokio::sync::oneshot::channel();

    let lock_clone = lock.clone();
    let mut acquired_clone = acquired.clone();

    let extender = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut rx => break,
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    if let Err(e) = lock_clone.extend(&mut acquired_clone).await {
                        tracing::warn!("Failed to extend lease: {}", e);
                        break;
                    }
                }
            }
        }
    });

    // Execute operation
    let result = operation.await;

    // Stop extender
    let _ = tx.send(());
    let _ = extender.await;

    result
}
```

### 9.3 Event Ordering

Events within Tier 2 are ordered by:

1. **Timestamp** (primary): Event timestamp from source
2. **Event ID** (tiebreaker): UUID for deterministic ordering

```rust
fn order_events(events: &mut Vec<EventEnvelope>) {
    events.sort_by(|a, b| {
        match a.timestamp.cmp(&b.timestamp) {
            std::cmp::Ordering::Equal => a.event_id.cmp(&b.event_id),
            other => other,
        }
    });
}
```

For events from the same execution, the source includes sequence information:

```rust
pub struct EventSource {
    pub source_type: String,
    pub execution_id: Option<String>,
    pub task_name: Option<String>,
    pub sequence: Option<u64>,  // Ordering within execution
}
```

---

*End of Part 1: Core Architecture*

*See Part 2 for: Deployment, APIs, Monitoring, Error Handling, Performance*
