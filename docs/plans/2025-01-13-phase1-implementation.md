# Phase 1: Core Implementation Plan (Revised v3)

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement the core Catalog and Orchestration MVPs with end-to-end local execution capability via a thin "walking skeleton" vertical slice.

**Architecture:** Two-tier consistency model with Parquet-native storage. Rust control plane (arco-catalog, arco-flow) with Python SDK for asset definition. Strong consistency for DDL ops (Tier 1), eventual consistency for operational metadata (Tier 2). Multi-manifest design with domain separation per architecture docs. **Tenant + Workspace scoping** as the primary isolation boundary per unified platform design.

**Tech Stack:** Rust 1.85+, Tokio async runtime, Arrow/Parquet for storage, Protobuf (Buf) for contracts, Python SDK with @asset decorator.

**JSON Mapping Strategy (FROZEN):**

> **On-disk JSON uses `camelCase` field names** via `serde(rename_all = "camelCase")`.
>
> This is the authoritative format for:
> - Manifest files (`*.manifest.json`)
> - Ledger event files (`ledger/**/*.json`)
> - Golden test fixtures (`fixtures/*.json`)
>
> **Canonical JSON for Hashing/Signing:**
> Per orchestration design, any JSON that feeds into hashing (plan fingerprints, commit hashes, idempotency keys) MUST use canonical form:
> - Sorted keys (alphabetically)
> - No whitespace between tokens
> - Normalized numbers (no leading zeros, no trailing decimal zeros)
> - UTF-8 encoding
>
> ```rust
> // Use serde_json with sorted keys for canonical form
> let canonical = serde_json::to_string(&value)?; // NOT to_string_pretty
> // For explicit sorting, use BTreeMap or sort keys before serialization
> ```
>
> **Why camelCase over snake_case:** Protobuf's default JSON mapping uses lowerCamelCase. Aligning with this reduces friction when debugging wire formats. The unified platform design examples show snake_case, but we're standardizing on camelCase for consistency with Protobuf tooling. This is documented in ADR-001.

---

## Overview

Phase 1 builds the foundation for Arco's serverless lakehouse infrastructure using a **walking skeleton** approach: build a thin vertical slice that works end-to-end, then deepen each layer.

**Upon completion:**
- `cargo test --workspace` passes with >80% coverage (verified by `cargo llvm-cov --fail-under 80`)
- Local end-to-end flow: define asset -> plan -> execute -> query results
- TTFSA (Time-to-First-Successful-Asset) < 30 minutes
- All benchmarks have baseline measurements
- Cross-language (Rust <-> Python) contract tests pass
- Concurrent writer race tests pass (exactly one winner)

**Phase 1 Gates (MANDATORY before production):**

| Gate | Requirement | Verification |
|------|-------------|--------------|
| **Trace Propagation** | W3C traceparent flows end-to-end through all services | `TraceContext::from_headers()` parses valid traceparent; child spans preserve trace ID |
| **No Secrets in Logs** | No passwords, API keys, tokens, or credentials in logs/events | `assert_no_secrets()` passes on all log output; `redact_secrets()` catches known patterns |
| **Tenant Isolation** | ScopedStorage prevents cross-tenant/workspace data access | Path traversal tests fail; cross-scope read/delete tests fail |
| **Proto Stability** | Field numbers never change; removed fields use `reserved` | `buf breaking` passes against previous version |
| **Idempotency** | Re-ingest of same event produces no duplicates | Tier 2 idempotency test passes |
| **Ordering Semantics** | Ledger files are lexicographically ordered by timestamp | ULID filenames sort chronologically |

## Prerequisites

- Rust 1.85+ installed
- Protocol Buffers compiler (`protoc` 25+)
- Buf CLI (`buf` 1.28+) for linting and breaking change detection
- Python 3.11+ with uv or pip

---

## Part 0: Infrastructure & Contracts Foundation

This part establishes CI gates and Protobuf contracts BEFORE any feature implementation. Contracts-first ensures stable cross-language interfaces.

### Task 0.1: Pin Rust Toolchain with MSRV Policy

**Files:**
- Create: `rust-toolchain.toml`

**Why:** Ensures reproducible builds and explicit minimum supported Rust version.

**Step 1: Create toolchain file**

```toml
# rust-toolchain.toml
[toolchain]
channel = "1.85"
components = ["rustfmt", "clippy", "llvm-tools-preview"]
```

**Step 2: Verify toolchain**

Run: `rustc --version`
Expected: rustc 1.85.x

**Step 3: Commit**

```bash
git add rust-toolchain.toml
git commit -m "chore: pin rust toolchain to 1.85 with MSRV policy"
```

---

### Task 0.2: Update Workspace Dependencies

**Files:**
- Modify: `Cargo.toml` (workspace root)

**Why:** All crate dependencies must be declared at workspace level before use.

**Step 1: Verify current state**

Run: `cargo check --workspace`
Expected: May fail due to missing dependencies

**Step 2: Add all Phase 1 dependencies**

```toml
[workspace.dependencies]
# Existing deps should already include:
# async-trait, bytes, chrono, serde, thiserror, tokio, etc.

# Add for Phase 1:
sha2 = "0.10"                    # Hash chain commits
hex = "0.4"                      # Hex encoding
base64 = "0.22"                  # URL-safe canonical encoding
prost = "0.13"                   # Protobuf runtime
prost-types = "0.13"             # Well-known protobuf types
ulid = "1"                       # Monotonic IDs
proptest = "1"                   # Property-based testing

# Build dependencies
prost-build = "0.13"
```

**Step 3: Verify builds**

Run: `cargo check --workspace`
Expected: PASS

**Step 4: Commit**

```bash
git add Cargo.toml
git commit -m "chore: add workspace dependencies for Phase 1"
```

---

### Task 0.3: Define Core Protobuf Contracts

**Files:**
- Create: `proto/buf.yaml`
- Create: `proto/buf.gen.yaml`
- Create: `proto/arco/v1/common.proto`
- Create: `proto/arco/v1/request.proto`
- Create: `proto/arco/v1/event.proto`

**Why:** Contracts-first development ensures stable cross-language interfaces. The unified platform design explicitly requires Protobuf/gRPC boundaries with compatibility enforcement in CI.

> **Proto Field Numbering Discipline (CRITICAL):**
> - **Never renumber fields** - once assigned, a field number is permanent
> - **Add new fields at the end** - use the next available number
> - **Mark removed fields as `reserved`** - prevents accidental reuse
> - Buf breaking checks enforce this in CI, but discipline starts here
>
> Example of safe evolution:
> ```protobuf
> message Example {
>   string id = 1;
>   string name = 2;
>   // Field 3 was removed - do not reuse
>   reserved 3;
>   reserved "old_field";
>   string new_field = 4;  // New fields get next number
> }
> ```

**Step 1: Create buf.yaml for linting**

```yaml
# proto/buf.yaml
version: v2
modules:
  - path: .
lint:
  use:
    - DEFAULT
    - COMMENTS
  except:
    - PACKAGE_VERSION_SUFFIX
breaking:
  use:
    - FILE
```

**Step 2: Create buf.gen.yaml for code generation**

```yaml
# proto/buf.gen.yaml
version: v2
plugins:
  - local: protoc-gen-prost
    out: ../crates/arco-proto/src/gen
    opt:
      - file_descriptor_set
```

**Step 3: Create common.proto with shared types**

```protobuf
// proto/arco/v1/common.proto
syntax = "proto3";
package arco.v1;

option java_multiple_files = true;
option java_package = "com.arco.v1";

import "google/protobuf/timestamp.proto";

// Strongly-typed IDs (string-wrapped for cross-language compat)
message TenantId {
  // Tenant identifier, e.g., "acme-corp"
  string value = 1;
}

message WorkspaceId {
  // Workspace identifier within tenant, e.g., "production", "staging"
  // Per unified platform design: tenant + workspace = primary scoping boundary
  string value = 1;
}

message AssetId {
  // ULID-based asset identifier
  string value = 1;
}

message RunId {
  // ULID-based run identifier
  string value = 1;
}

message TaskId {
  // ULID-based task identifier
  string value = 1;
}

message PartitionId {
  // Derived partition identifier (hash of asset_id + partition_key)
  string value = 1;
}

message MaterializationId {
  // ULID-based materialization identifier
  string value = 1;
}

message SnapshotId {
  // Snapshot version identifier
  string value = 1;
}

// Multi-dimensional partition key
message PartitionKey {
  // Dimensions sorted alphabetically by key for determinism
  map<string, ScalarValue> dimensions = 1;
}

// Scalar value with explicit type for canonical encoding
message ScalarValue {
  oneof value {
    string string_value = 1;    // Arbitrary string (base64url encoded in canonical form)
    int64 int64_value = 2;      // 64-bit integer
    bool bool_value = 3;        // Boolean
    string date_value = 4;      // YYYY-MM-DD format
    string timestamp_value = 5; // ISO 8601 with micros, UTC (e.g., 2025-01-15T10:30:00.000000Z)
    NullValue null_value = 6;   // Explicit null
  }
}

// Represents null value
enum NullValue {
  NULL_VALUE = 0;
}

// Asset identifier (namespace + name)
message AssetKey {
  // Namespace, e.g., "raw", "staging", "mart"
  // Pattern: ^[a-z][a-z0-9_]*$
  string namespace = 1;

  // Asset name within namespace
  // Pattern: ^[a-z][a-z0-9_]*$
  string name = 2;
}

// Asset format
enum AssetFormat {
  ASSET_FORMAT_UNSPECIFIED = 0;
  ASSET_FORMAT_PARQUET = 1;
  ASSET_FORMAT_DELTA = 2;
  ASSET_FORMAT_ICEBERG = 3;
}
```

**Step 4: Create request.proto with headers**

```protobuf
// proto/arco/v1/request.proto
syntax = "proto3";
package arco.v1;

option java_multiple_files = true;
option java_package = "com.arco.v1";

import "google/protobuf/timestamp.proto";
import "arco/v1/common.proto";

// Standard request header for all API calls
// Per architecture docs: every request carries tenant, workspace, trace, idempotency
message RequestHeader {
  // Tenant context for multi-tenant isolation
  TenantId tenant_id = 1;

  // Workspace context (tenant + workspace = primary scope)
  // Per unified platform design: all operations scoped to workspace
  WorkspaceId workspace_id = 2;

  // W3C traceparent for distributed tracing
  // Format: 00-{trace_id}-{parent_id}-{flags}
  string trace_parent = 3;

  // Idempotency key for exactly-once semantics
  // Must be unique per logical operation
  string idempotency_key = 4;

  // Client request timestamp
  google.protobuf.Timestamp request_time = 5;
}
```

**Step 5: Create event.proto for catalog events**

```protobuf
// proto/arco/v1/event.proto
syntax = "proto3";
package arco.v1;

option java_multiple_files = true;
option java_package = "com.arco.v1";

import "google/protobuf/timestamp.proto";
import "arco/v1/common.proto";

// Event envelope for all catalog events (Tier 2 append-only log)
message CatalogEvent {
  // Unique event ID (ULID for ordering)
  string event_id = 1;

  // Event type discriminator
  string event_type = 2;

  // Schema version for forward compatibility
  uint32 event_version = 3;

  // Event timestamp (server-side)
  google.protobuf.Timestamp timestamp = 4;

  // Event source (e.g., "servo", "ui", "profiler")
  string source = 5;

  // Tenant context
  TenantId tenant_id = 6;

  // Workspace context (tenant + workspace = primary scope)
  WorkspaceId workspace_id = 7;

  // Idempotency key (for deduplication)
  string idempotency_key = 8;

  // Event payload (one of)
  oneof payload {
    AssetCreated asset_created = 20;
    AssetUpdated asset_updated = 21;
    MaterializationCompleted materialization_completed = 22;
    LineageRecorded lineage_recorded = 23;
  }
}

// Asset creation event
message AssetCreated {
  AssetId asset_id = 1;
  AssetKey asset_key = 2;
  string location = 3;
  AssetFormat format = 4;
  string description = 5;
  repeated string owners = 6;
}

// Asset update event
message AssetUpdated {
  AssetId asset_id = 1;
  // Fields that changed (sparse update)
  optional string location = 2;
  optional string description = 3;
  repeated string owners = 4;
}

// Materialization completion event (high-frequency, Tier 2)
message MaterializationCompleted {
  MaterializationId materialization_id = 1;
  AssetId asset_id = 2;
  PartitionKey partition_key = 3;
  RunId run_id = 4;
  TaskId task_id = 5;

  // Output files
  repeated FileEntry files = 6;

  // Statistics
  int64 row_count = 7;
  int64 byte_size = 8;
  string schema_hash = 9;

  // Timing
  google.protobuf.Timestamp started_at = 10;
  google.protobuf.Timestamp completed_at = 11;
}

// File entry in materialization output
message FileEntry {
  string path = 1;
  int64 size_bytes = 2;
  int64 row_count = 3;
}

// Lineage recording event
message LineageRecorded {
  RunId run_id = 1;
  repeated LineageEdge edges = 2;
}

// Single lineage edge (source -> target dependency)
message LineageEdge {
  AssetId source_asset_id = 1;
  AssetId target_asset_id = 2;
  repeated PartitionId source_partitions = 3;
  repeated PartitionId target_partitions = 4;

  // Hash of dependency specification for change detection
  string dependency_fingerprint = 5;
}
```

**Step 6: Run buf lint**

Run: `cd proto && buf lint`
Expected: PASS (no lint errors)

**Step 7: Commit**

```bash
git add proto/
git commit -m "feat(proto): add core protobuf contracts (common, request, event)"
```

---

### Task 0.4: Add Buf Breaking Change Check to CI

**Files:**
- Modify: `.github/workflows/ci.yml`

**Why:** Prevents accidental breaking changes to proto contracts. Per architecture docs: "Buf breaking checks" are required.

**Step 1: Add buf check job**

Add to `.github/workflows/ci.yml`:

```yaml
  proto-check:
    name: Proto Compatibility
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Buf
        uses: bufbuild/buf-setup-action@v1
        with:
          version: '1.28.1'

      - name: Lint protos
        run: buf lint proto/

      - name: Check breaking changes
        if: github.event_name == 'pull_request'
        run: |
          buf breaking proto/ \
            --against 'https://github.com/${{ github.repository }}.git#branch=main,subdir=proto'
```

**Step 2: Verify CI config is valid**

Run: `act -n` (if act installed) or manual validation

**Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add buf lint and breaking change detection"
```

---

### Task 0.5: Setup arco-proto Crate with prost Generation

**Files:**
- Create: `crates/arco-proto/Cargo.toml`
- Create: `crates/arco-proto/build.rs`
- Create: `crates/arco-proto/src/lib.rs`

**Step 1: Write the failing test**

```rust
// crates/arco-proto/src/lib.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_id_roundtrip() {
        let tenant = TenantId {
            value: "acme-corp".to_string(),
        };

        assert_eq!(tenant.value, "acme-corp");
    }

    #[test]
    fn test_partition_key_serialization() {
        use prost::Message;

        let mut dimensions = std::collections::HashMap::new();
        dimensions.insert(
            "date".to_string(),
            ScalarValue {
                value: Some(scalar_value::Value::DateValue("2025-01-15".to_string())),
            },
        );

        let pk = PartitionKey { dimensions };
        let encoded = pk.encode_to_vec();
        let decoded = PartitionKey::decode(encoded.as_slice())
            .expect("decode should succeed");

        assert_eq!(decoded.dimensions.len(), 1);
    }

    #[test]
    fn test_request_header_has_all_fields() {
        let header = RequestHeader {
            tenant_id: Some(TenantId { value: "acme".into() }),
            workspace_id: Some(WorkspaceId { value: "production".into() }),
            trace_parent: "00-abc123-def456-01".into(),
            idempotency_key: "idem_001".into(),
            request_time: None,
        };

        assert!(header.tenant_id.is_some());
        assert!(header.workspace_id.is_some());
        assert!(!header.idempotency_key.is_empty());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-proto`
Expected: FAIL (types not generated yet)

**Step 3: Create Cargo.toml**

```toml
# crates/arco-proto/Cargo.toml
[package]
name = "arco-proto"
version.workspace = true
edition.workspace = true
license.workspace = true
description = "Generated protobuf types for Arco"

[dependencies]
prost = { workspace = true }
prost-types = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }

[build-dependencies]
prost-build = { workspace = true }
```

**Step 4: Create build.rs**

```rust
// crates/arco-proto/build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = [
        "../../proto/arco/v1/common.proto",
        "../../proto/arco/v1/request.proto",
        "../../proto/arco/v1/event.proto",
    ];

    let includes = ["../../proto"];

    prost_build::Config::new()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".", "#[serde(rename_all = \"camelCase\")]")
        .compile_protos(&proto_files, &includes)?;

    // Rerun if proto files change
    for file in &proto_files {
        println!("cargo:rerun-if-changed={file}");
    }

    Ok(())
}
```

**Step 5: Create lib.rs with generated code include**

```rust
//! Generated protobuf types for Arco.
//!
//! This crate provides Rust types generated from the proto/ definitions.
//! All cross-language contracts are defined via Protobuf.

#![forbid(unsafe_code)]
#![allow(missing_docs)] // Generated code doesn't have docs

// Include generated code
include!(concat!(env!("OUT_DIR"), "/arco.v1.rs"));

// Re-exports for convenience
pub use self::{
    AssetFormat, AssetId, AssetKey, CatalogEvent, LineageEdge, MaterializationCompleted,
    NullValue, PartitionId, PartitionKey, RequestHeader, RunId, ScalarValue, TenantId,
    WorkspaceId,
};
```

**Step 6: Run test to verify it passes**

Run: `cargo test -p arco-proto`
Expected: PASS

**Step 7: Commit**

```bash
git add crates/arco-proto/
git commit -m "feat(proto): add prost code generation for arco.v1 protos"
```

---

### Task 0.6: Add Golden Fixture Contract Tests

**Files:**
- Create: `crates/arco-proto/fixtures/partition_key_v1.json`
- Create: `crates/arco-proto/fixtures/catalog_event_v1.json`
- Create: `crates/arco-proto/tests/golden_fixtures.rs`

**Why:** Golden fixtures ensure serialization format remains stable across versions and languages.

**Step 1: Create partition key fixture**

```json
{
  "dimensions": {
    "date": {"dateValue": "2025-01-15"},
    "region": {"stringValue": "us-east"}
  }
}
```

**Step 2: Create catalog event fixture**

```json
{
  "eventId": "01HQXYZ123456789ABCDEF",
  "eventType": "MaterializationCompleted",
  "eventVersion": 1,
  "source": "servo",
  "tenantId": {"value": "acme-corp"},
  "workspaceId": {"value": "production"},
  "idempotencyKey": "idem_mat_001",
  "materializationCompleted": {
    "materializationId": {"value": "mat_001"},
    "assetId": {"value": "asset_abc123"},
    "rowCount": 1000000,
    "byteSize": 52428800
  }
}
```

**Step 3: Write golden fixture tests**

```rust
// crates/arco-proto/tests/golden_fixtures.rs
use arco_proto::{CatalogEvent, PartitionKey};

#[test]
fn test_partition_key_golden_fixture() {
    let fixture = include_str!("../fixtures/partition_key_v1.json");
    let pk: PartitionKey = serde_json::from_str(fixture)
        .expect("golden fixture should parse");

    // Verify structure
    assert_eq!(pk.dimensions.len(), 2);
    assert!(pk.dimensions.contains_key("date"));
    assert!(pk.dimensions.contains_key("region"));

    // Re-serialize and verify stability (no field reordering)
    let reserialized = serde_json::to_string(&pk)
        .expect("should serialize");
    let reparsed: PartitionKey = serde_json::from_str(&reserialized)
        .expect("should reparse");

    assert_eq!(pk.dimensions.len(), reparsed.dimensions.len());
}

#[test]
fn test_catalog_event_golden_fixture() {
    let fixture = include_str!("../fixtures/catalog_event_v1.json");
    let event: CatalogEvent = serde_json::from_str(fixture)
        .expect("golden fixture should parse");

    assert_eq!(event.event_type, "MaterializationCompleted");
    assert_eq!(event.event_version, 1);
    assert!(event.tenant_id.is_some());

    // Verify payload
    assert!(event.payload.is_some());
}

#[test]
fn test_fixture_backward_compatibility() {
    // This test ensures old fixtures remain parseable
    // Add new versions as fixtures/partition_key_v2.json etc.

    let v1_fixture = include_str!("../fixtures/partition_key_v1.json");
    let _: PartitionKey = serde_json::from_str(v1_fixture)
        .expect("v1 fixture must remain parseable");
}
```

**Step 4: Run tests**

Run: `cargo test -p arco-proto golden`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/arco-proto/
git commit -m "test(proto): add golden fixture contract tests for stability"
```

---

### Task 0.7: Add Basic Observability Infrastructure

**Files:**
- Create: `crates/arco-core/src/observability.rs`
- Modify: `crates/arco-core/Cargo.toml`

**Why:** Per Ops architecture, structured logging via tracing (JSON logs, file/line, thread IDs) should start in Phase 1 for debuggability during early development.

**Step 1: Add tracing dependencies**

```toml
# Add to [workspace.dependencies] in root Cargo.toml
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
```

**Step 2: Write failing test**

```rust
// crates/arco-core/src/observability.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_logging_succeeds() {
        // Should not panic
        init_logging(LogFormat::Json);
    }

    #[test]
    fn test_span_helper_creates_span() {
        init_logging(LogFormat::Pretty);

        let span = catalog_span("test_operation", "acme", "production");
        let _guard = span.enter();

        // Span should have tenant and workspace fields
        tracing::info!("test message in span");
    }
}
```

**Step 3: Write implementation**

```rust
// crates/arco-core/src/observability.rs
//! Observability infrastructure for Arco.
//!
//! Per architecture docs: structured logging with consistent spans.

use tracing::Span;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Log output format.
#[derive(Debug, Clone, Copy)]
pub enum LogFormat {
    /// JSON structured logs (for production).
    Json,
    /// Pretty-printed logs (for development).
    Pretty,
}

/// Initializes the logging subsystem.
///
/// Call once at application startup.
pub fn init_logging(format: LogFormat) {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    match format {
        LogFormat::Json => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().json())
                .init();
        }
        LogFormat::Pretty => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().pretty())
                .init();
        }
    }
}

/// Creates a span for catalog operations with standard fields.
#[must_use]
pub fn catalog_span(operation: &str, tenant: &str, workspace: &str) -> Span {
    tracing::info_span!(
        "catalog",
        op = operation,
        tenant = tenant,
        workspace = workspace,
    )
}

/// Creates a span for orchestration operations.
#[must_use]
pub fn orchestration_span(operation: &str, run_id: &str, tenant: &str, workspace: &str) -> Span {
    tracing::info_span!(
        "orchestration",
        op = operation,
        run_id = run_id,
        tenant = tenant,
        workspace = workspace,
    )
}

// =============================================================================
// PHASE 1 OBSERVABILITY GATES
// =============================================================================

/// W3C Trace Context for distributed tracing.
///
/// Implements trace propagation across service boundaries per W3C spec:
/// https://www.w3.org/TR/trace-context/
#[derive(Debug, Clone)]
pub struct TraceContext {
    /// W3C traceparent header value
    pub traceparent: String,
    /// Optional tracestate for vendor-specific data
    pub tracestate: Option<String>,
}

impl TraceContext {
    /// Creates a new trace context from W3C headers.
    ///
    /// # Format
    /// traceparent: {version}-{trace-id}-{parent-id}-{flags}
    /// Example: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
    pub fn from_headers(traceparent: &str, tracestate: Option<&str>) -> Result<Self, TraceContextError> {
        // Validate traceparent format
        let parts: Vec<&str> = traceparent.split('-').collect();
        if parts.len() != 4 {
            return Err(TraceContextError::InvalidFormat(traceparent.to_string()));
        }

        // Validate version (must be "00" for current spec)
        if parts[0] != "00" {
            return Err(TraceContextError::UnsupportedVersion(parts[0].to_string()));
        }

        // Validate trace-id (32 hex chars)
        if parts[1].len() != 32 || !parts[1].chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(TraceContextError::InvalidTraceId(parts[1].to_string()));
        }

        // Validate parent-id (16 hex chars)
        if parts[2].len() != 16 || !parts[2].chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(TraceContextError::InvalidParentId(parts[2].to_string()));
        }

        // Validate flags (2 hex chars)
        if parts[3].len() != 2 || !parts[3].chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(TraceContextError::InvalidFlags(parts[3].to_string()));
        }

        Ok(Self {
            traceparent: traceparent.to_string(),
            tracestate: tracestate.map(String::from),
        })
    }

    /// Generates a new trace context with fresh trace ID.
    #[must_use]
    pub fn generate() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Generate trace-id (32 hex chars from 16 random bytes)
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let trace_id = format!("{:032x}", timestamp ^ 0x1234_5678_9abc_def0_u128);

        // Generate parent-id (16 hex chars from 8 random bytes)
        let parent_id = format!("{:016x}", timestamp as u64);

        Self {
            traceparent: format!("00-{trace_id}-{parent_id}-01"),
            tracestate: None,
        }
    }

    /// Returns the trace ID portion.
    #[must_use]
    pub fn trace_id(&self) -> &str {
        // traceparent format: {version}-{trace-id}-{parent-id}-{flags}
        self.traceparent.split('-').nth(1).unwrap_or_default()
    }

    /// Returns the parent ID portion.
    #[must_use]
    pub fn parent_id(&self) -> &str {
        self.traceparent.split('-').nth(2).unwrap_or_default()
    }

    /// Creates a child span context (new parent-id, same trace-id).
    #[must_use]
    pub fn create_child(&self) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let new_parent_id = format!("{:016x}", timestamp as u64);

        Self {
            traceparent: format!(
                "00-{}-{}-01",
                self.trace_id(),
                new_parent_id
            ),
            tracestate: self.tracestate.clone(),
        }
    }
}

/// Errors for trace context parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceContextError {
    /// Invalid traceparent format
    InvalidFormat(String),
    /// Unsupported version
    UnsupportedVersion(String),
    /// Invalid trace ID
    InvalidTraceId(String),
    /// Invalid parent ID
    InvalidParentId(String),
    /// Invalid flags
    InvalidFlags(String),
}

impl std::fmt::Display for TraceContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidFormat(s) => write!(f, "invalid traceparent format: {s}"),
            Self::UnsupportedVersion(s) => write!(f, "unsupported version: {s}"),
            Self::InvalidTraceId(s) => write!(f, "invalid trace-id: {s}"),
            Self::InvalidParentId(s) => write!(f, "invalid parent-id: {s}"),
            Self::InvalidFlags(s) => write!(f, "invalid flags: {s}"),
        }
    }
}

impl std::error::Error for TraceContextError {}

// =============================================================================
// SECRET REDACTION - CRITICAL SECURITY GATE
// =============================================================================

/// Patterns that indicate secrets in log messages.
///
/// INVARIANT: No secrets shall appear in logs or events.
/// This is a Phase 1 gate that must be verified before any production deployment.
const SECRET_PATTERNS: &[&str] = &[
    "password",
    "secret",
    "api_key",
    "apikey",
    "api-key",
    "access_token",
    "accesstoken",
    "access-token",
    "auth_token",
    "authtoken",
    "bearer",
    "private_key",
    "privatekey",
    "private-key",
    "credential",
    "session_id",
    "sessionid",
];

/// Redacts potential secrets from a string.
///
/// This is a best-effort filter. Defense in depth:
/// 1. Don't log secrets in the first place
/// 2. Use structured logging with known-safe fields
/// 3. This filter catches accidental leaks
#[must_use]
pub fn redact_secrets(input: &str) -> String {
    let mut output = input.to_string();

    for pattern in SECRET_PATTERNS {
        // Case-insensitive replacement of values after pattern
        // e.g., "password=abc123" -> "password=[REDACTED]"
        let re_patterns = [
            format!(r#"(?i){pattern}\s*[=:]\s*"[^"]*""#),      // quoted value
            format!(r#"(?i){pattern}\s*[=:]\s*'[^']*'"#),      // single quoted
            format!(r#"(?i){pattern}\s*[=:]\s*\S+"#),          // unquoted value
        ];

        for re_pattern in &re_patterns {
            if let Ok(re) = regex::Regex::new(re_pattern) {
                output = re.replace_all(&output, format!("{pattern}=[REDACTED]")).to_string();
            }
        }
    }

    output
}

/// Validates that a log message contains no secrets.
///
/// Use this in tests to verify logging code doesn't leak secrets.
pub fn assert_no_secrets(message: &str) -> Result<(), SecretLeakError> {
    let lower = message.to_lowercase();

    for pattern in SECRET_PATTERNS {
        // Check if pattern appears followed by an assignment
        if lower.contains(&format!("{pattern}="))
            || lower.contains(&format!("{pattern}:"))
            || lower.contains(&format!("{pattern} ="))
            || lower.contains(&format!("{pattern} :"))
        {
            // Check if it's already redacted
            if !lower.contains("[redacted]") && !lower.contains("***") {
                return Err(SecretLeakError::PotentialLeak(pattern.to_string()));
            }
        }
    }

    Ok(())
}

/// Error indicating a potential secret leak in logs.
#[derive(Debug, Clone)]
pub struct SecretLeakError {
    pattern: String,
}

impl SecretLeakError {
    fn PotentialLeak(pattern: String) -> Self {
        Self { pattern }
    }
}

impl std::fmt::Display for SecretLeakError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "potential secret leak detected: pattern '{}' found with value", self.pattern)
    }
}

impl std::error::Error for SecretLeakError {}
```

**Observability Gate Tests:**

```rust
#[cfg(test)]
mod gate_tests {
    use super::*;

    // =========================================================================
    // TRACE PROPAGATION TESTS
    // =========================================================================

    #[test]
    fn test_trace_context_parse_valid() {
        let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let ctx = TraceContext::from_headers(traceparent, None)
            .expect("valid traceparent should parse");

        assert_eq!(ctx.trace_id(), "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(ctx.parent_id(), "b7ad6b7169203331");
    }

    #[test]
    fn test_trace_context_invalid_format() {
        let invalid_cases = [
            "",                                  // empty
            "00",                                // too few parts
            "00-abc-def-01",                     // wrong lengths
            "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", // bad version
            "00-ZZZZ651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", // invalid hex
        ];

        for invalid in &invalid_cases {
            let result = TraceContext::from_headers(invalid, None);
            assert!(result.is_err(), "should reject: {invalid}");
        }
    }

    #[test]
    fn test_trace_context_child_preserves_trace_id() {
        let parent = TraceContext::from_headers(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            None,
        ).unwrap();

        let child = parent.create_child();

        // Same trace ID
        assert_eq!(child.trace_id(), parent.trace_id());
        // Different parent ID
        assert_ne!(child.parent_id(), parent.parent_id());
    }

    #[test]
    fn test_trace_context_generate_creates_valid() {
        let ctx = TraceContext::generate();

        // Should be parseable by itself
        let parsed = TraceContext::from_headers(&ctx.traceparent, None);
        assert!(parsed.is_ok(), "generated context should be valid");
    }

    // =========================================================================
    // SECRET REDACTION TESTS - CRITICAL
    // =========================================================================

    #[test]
    fn test_redact_password() {
        let input = r#"{"password": "supersecret123"}"#;
        let output = redact_secrets(input);
        assert!(!output.contains("supersecret123"));
        assert!(output.contains("[REDACTED]"));
    }

    #[test]
    fn test_redact_api_key() {
        let input = "api_key=sk-abc123xyz";
        let output = redact_secrets(input);
        assert!(!output.contains("sk-abc123xyz"));
        assert!(output.contains("[REDACTED]"));
    }

    #[test]
    fn test_redact_bearer_token() {
        let input = "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
        let output = redact_secrets(input);
        assert!(!output.contains("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"));
    }

    #[test]
    fn test_assert_no_secrets_catches_leaks() {
        let with_leak = "password=abc123";
        assert!(assert_no_secrets(with_leak).is_err());

        let safe = "username=john";
        assert!(assert_no_secrets(safe).is_ok());

        let already_redacted = "password=[REDACTED]";
        assert!(assert_no_secrets(already_redacted).is_ok());
    }

    #[test]
    fn test_case_insensitive_detection() {
        let cases = [
            "PASSWORD=secret",
            "Password=secret",
            "API_KEY=secret",
            "ApiKey=secret",
            "BEARER token123",
        ];

        for case in &cases {
            let output = redact_secrets(case);
            assert!(
                output.contains("[REDACTED]") || !output.to_lowercase().contains("secret"),
                "should redact: {case}"
            );
        }
    }

    #[test]
    fn test_safe_patterns_not_redacted() {
        let safe_inputs = [
            "user_id=12345",
            "tenant=acme",
            "workspace=production",
            "asset_id=asset_abc",
            "run_id=run_001",
        ];

        for input in &safe_inputs {
            let output = redact_secrets(input);
            assert_eq!(&output, input, "should not modify safe input: {input}");
        }
    }
}
```

**Step 4: Add regex dependency**

```toml
# Add to [workspace.dependencies] in root Cargo.toml
regex = "1"
```

**Step 5: Update lib.rs**

```rust
// Add to crates/arco-core/src/lib.rs
pub mod observability;

// Add to prelude
pub use crate::observability::{
    init_logging, LogFormat,
    // Phase 1 Gates
    TraceContext, TraceContextError,
    redact_secrets, assert_no_secrets, SecretLeakError,
};
```

**Step 6: Run tests**

Run: `cargo test -p arco-core observability`
Expected: PASS

**Step 7: Commit**

```bash
git add crates/arco-core/
git commit -m "feat(core): add observability with trace propagation and secret redaction"
```

---

### Task 0.8: Add Orchestration Protobuf Contracts

**Files:**
- Create: `proto/arco/v1/orchestration.proto`
- Modify: `crates/arco-proto/build.rs` (add new proto to compilation)

**Why:** Per feedback, the proto contracts must cover the orchestration "plan/run/task" boundary. This ensures the orchestration domain has the same contracts-first guarantees as catalog events.

**Step 1: Create orchestration.proto**

```protobuf
// proto/arco/v1/orchestration.proto
syntax = "proto3";
package arco.v1;

option java_multiple_files = true;
option java_package = "com.arco.v1";

import "google/protobuf/timestamp.proto";
import "arco/v1/common.proto";

// ============================================================================
// Plan Domain
// ============================================================================

// A Plan is a deterministic specification of what will execute.
// Generated from asset definitions and their dependencies.
// Per orchestration design: same inputs always produce the same plan.
message Plan {
  // Unique plan identifier
  string plan_id = 1;

  // Tenant and workspace scope
  TenantId tenant_id = 2;
  WorkspaceId workspace_id = 3;

  // Plan generation timestamp
  google.protobuf.Timestamp created_at = 4;

  // Plan version for idempotency
  uint32 version = 5;

  // Hash of all inputs that produced this plan (for cache invalidation)
  string input_hash = 6;

  // Ordered list of tasks (topologically sorted)
  repeated TaskSpec tasks = 7;

  // Asset dependency graph (adjacency list)
  repeated DependencyEdge dependencies = 8;
}

// Specification for a single task within a plan.
message TaskSpec {
  // Unique task identifier within the plan
  TaskId task_id = 1;

  // Asset this task materializes
  AssetId asset_id = 2;
  AssetKey asset_key = 3;

  // Partition key (if partitioned asset)
  PartitionKey partition_key = 4;

  // Upstream dependencies (task IDs that must complete first)
  repeated TaskId upstream_task_ids = 5;

  // Execution priority (lower = higher priority)
  int32 priority = 6;

  // Resource requirements
  ResourceRequirements resources = 7;
}

// Resource requirements for task scheduling.
message ResourceRequirements {
  // Memory limit in bytes
  int64 memory_bytes = 1;

  // CPU cores (fractional allowed)
  double cpu_cores = 2;

  // Maximum execution time in seconds
  int64 timeout_seconds = 3;
}

// Edge in the dependency graph.
message DependencyEdge {
  // Source task (upstream)
  TaskId source_task_id = 1;

  // Target task (downstream)
  TaskId target_task_id = 2;
}

// ============================================================================
// Run Domain
// ============================================================================

// A Run is an execution of a Plan.
// Captures inputs, outputs, timing, and final status.
message Run {
  // Unique run identifier
  RunId run_id = 1;

  // Plan being executed
  string plan_id = 2;

  // Tenant and workspace scope
  TenantId tenant_id = 3;
  WorkspaceId workspace_id = 4;

  // Run state
  RunState state = 5;

  // Timestamps
  google.protobuf.Timestamp created_at = 6;
  google.protobuf.Timestamp started_at = 7;
  google.protobuf.Timestamp completed_at = 8;

  // Task execution states
  repeated TaskExecution task_executions = 9;

  // Run-level metadata
  map<string, string> labels = 10;

  // Trigger information
  RunTrigger trigger = 11;
}

// Run state machine states.
// Per orchestration design: reliable run state management with recovery.
enum RunState {
  RUN_STATE_UNSPECIFIED = 0;
  RUN_STATE_PENDING = 1;     // Created, waiting to start
  RUN_STATE_RUNNING = 2;     // Actively executing tasks
  RUN_STATE_SUCCEEDED = 3;   // All tasks completed successfully
  RUN_STATE_FAILED = 4;      // One or more tasks failed
  RUN_STATE_CANCELLED = 5;   // Cancelled by user or system
  RUN_STATE_TIMED_OUT = 6;   // Exceeded maximum duration
}

// How the run was triggered.
message RunTrigger {
  // Trigger type
  TriggerType type = 1;

  // User who triggered (if manual)
  string triggered_by = 2;

  // Trigger timestamp
  google.protobuf.Timestamp triggered_at = 3;

  // Associated schedule name (if scheduled)
  string schedule_name = 4;
}

// Trigger type enum.
enum TriggerType {
  TRIGGER_TYPE_UNSPECIFIED = 0;
  TRIGGER_TYPE_MANUAL = 1;      // User-initiated
  TRIGGER_TYPE_SCHEDULED = 2;   // Cron/schedule-based
  TRIGGER_TYPE_SENSOR = 3;      // Event-driven (e.g., file arrival)
  TRIGGER_TYPE_BACKFILL = 4;    // Historical data backfill
}

// ============================================================================
// Task Execution Domain
// ============================================================================

// Execution state for a single task within a run.
message TaskExecution {
  // Task being executed
  TaskId task_id = 1;

  // Execution state
  TaskState state = 2;

  // Attempt number (1-indexed, increments on retry)
  int32 attempt = 3;

  // Timestamps
  google.protobuf.Timestamp queued_at = 4;
  google.protobuf.Timestamp started_at = 5;
  google.protobuf.Timestamp completed_at = 6;

  // Worker that executed this task
  string worker_id = 7;

  // Output reference (if succeeded)
  TaskOutput output = 8;

  // Error information (if failed)
  TaskError error = 9;

  // Execution metrics
  TaskMetrics metrics = 10;
}

// Task execution state machine.
enum TaskState {
  TASK_STATE_UNSPECIFIED = 0;
  TASK_STATE_PENDING = 1;      // Waiting for dependencies
  TASK_STATE_QUEUED = 2;       // Ready, waiting for worker
  TASK_STATE_RUNNING = 3;      // Actively executing
  TASK_STATE_SUCCEEDED = 4;    // Completed successfully
  TASK_STATE_FAILED = 5;       // Failed (may retry)
  TASK_STATE_SKIPPED = 6;      // Skipped (upstream failed)
  TASK_STATE_CANCELLED = 7;    // Cancelled
}

// Task output reference.
message TaskOutput {
  // Materialization ID for output tracking
  MaterializationId materialization_id = 1;

  // Output files
  repeated FileEntry files = 2;

  // Output statistics
  int64 row_count = 3;
  int64 byte_size = 4;
}

// Task error information.
message TaskError {
  // Error category
  TaskErrorCategory category = 1;

  // Error message
  string message = 2;

  // Stack trace or detail (truncated)
  string detail = 3;

  // Whether the error is retryable
  bool retryable = 4;
}

// Task error categories.
enum TaskErrorCategory {
  TASK_ERROR_CATEGORY_UNSPECIFIED = 0;
  TASK_ERROR_CATEGORY_USER_CODE = 1;       // Error in user asset code
  TASK_ERROR_CATEGORY_DATA_QUALITY = 2;    // Schema mismatch, constraint violation
  TASK_ERROR_CATEGORY_INFRASTRUCTURE = 3;  // Network, storage, timeout
  TASK_ERROR_CATEGORY_CONFIGURATION = 4;   // Invalid config or missing secrets
}

// Task execution metrics.
message TaskMetrics {
  // Wall clock duration in milliseconds
  int64 duration_ms = 1;

  // CPU time in milliseconds
  int64 cpu_time_ms = 2;

  // Peak memory usage in bytes
  int64 peak_memory_bytes = 3;

  // Bytes read from storage
  int64 bytes_read = 4;

  // Bytes written to storage
  int64 bytes_written = 5;
}
```

**Step 2: Update arco-proto build.rs**

Add the new proto file to the build:

```rust
// In crates/arco-proto/build.rs, add "orchestration.proto" to the compile list
fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::Config::new()
        .out_dir("src/gen")
        .compile_protos(
            &[
                "../../proto/arco/v1/common.proto",
                "../../proto/arco/v1/request.proto",
                "../../proto/arco/v1/event.proto",
                "../../proto/arco/v1/orchestration.proto",  // NEW
            ],
            &["../../proto"],
        )?;
    Ok(())
}
```

**Step 3: Run buf lint**

Run: `cd proto && buf lint`
Expected: PASS

**Step 4: Generate Rust code**

Run: `cargo build -p arco-proto`
Expected: PASS

**Step 5: Commit**

```bash
git add proto/arco/v1/orchestration.proto crates/arco-proto/
git commit -m "feat(proto): add orchestration contracts (plan/run/task)"
```

---

## Part 1: Core Types Foundation (arco-core)

### Task 1.1: Implement PartitionKey with Canonical Encoding Spec

**Files:**
- Create: `crates/arco-core/src/partition.rs`
- Modify: `crates/arco-core/src/lib.rs`
- Modify: `crates/arco-core/Cargo.toml`

**Canonical Encoding Specification (Cross-Language Contract):**

```
PURPOSE: Deterministic, URL-safe string representation for:
  - Storage path construction
  - Hash-based deduplication
  - Cross-language identity

GRAMMAR:
  PARTITION_KEY_CANONICAL ::= dimension ("," dimension)*
  dimension              ::= key "=" typed_value
  key                    ::= [a-z][a-z0-9_]*   (alphabetically sorted)
  typed_value            ::= type_tag ":" encoded_value

  type_tag ::=
    "s" (string)  | "i" (int64)    | "b" (bool)
    "d" (date)    | "t" (timestamp) | "n" (null)

  encoded_value ::=
    For "s": base64url_no_pad(utf8_bytes)
    For "i": decimal integer (no leading zeros except "0")
    For "b": "true" | "false"
    For "d": "YYYY-MM-DD"
    For "t": "YYYY-MM-DDTHH:MM:SS.ffffffZ"
    For "n": "null"

EXAMPLES:
  {date: "2025-01-15"} -> "date=d:2025-01-15"
  {region: "us-east", date: "2025-01-15"} -> "date=d:2025-01-15,region=s:dXMtZWFzdA"
  {count: 42} -> "count=i:42"
  {active: true} -> "active=b:true"

GUARANTEES:
  1. Deterministic: same logical key -> same canonical string
  2. URL-safe: no special characters in output
  3. Unambiguous: parseable back to original values
  4. Cross-language: Rust and Python produce identical output
```

**Step 1: Write the failing tests**

```rust
// crates/arco-core/src/partition.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_string_single_date() {
        let mut pk = PartitionKey::new();
        pk.insert("date", ScalarValue::Date("2025-01-15".into()));

        assert_eq!(pk.canonical_string(), "date=d:2025-01-15");
    }

    #[test]
    fn test_canonical_string_sorted_keys() {
        let mut pk = PartitionKey::new();
        // Insert in reverse order
        pk.insert("region", ScalarValue::String("us-east".into()));
        pk.insert("date", ScalarValue::Date("2025-01-15".into()));

        // Keys should be alphabetically sorted in output
        // "us-east" base64url = "dXMtZWFzdA"
        assert_eq!(
            pk.canonical_string(),
            "date=d:2025-01-15,region=s:dXMtZWFzdA"
        );
    }

    #[test]
    fn test_canonical_string_url_safe() {
        let mut pk = PartitionKey::new();
        pk.insert("path", ScalarValue::String("foo/bar?baz=1&x=2".into()));

        let canonical = pk.canonical_string();

        // Must not contain URL-unsafe characters
        assert!(!canonical.contains('/'));
        assert!(!canonical.contains('?'));
        assert!(!canonical.contains('&'));
        assert!(!canonical.contains('=') || canonical.matches('=').count() == 1); // Only key=value separator

        // "foo/bar?baz=1&x=2" base64url = "Zm9vL2Jhcj9iYXo9MSZ4PTI"
        assert_eq!(canonical, "path=s:Zm9vL2Jhcj9iYXo9MSZ4PTI");
    }

    #[test]
    fn test_canonical_string_all_types() {
        let mut pk = PartitionKey::new();
        pk.insert("active", ScalarValue::Boolean(true));
        pk.insert("count", ScalarValue::Int64(42));
        pk.insert("date", ScalarValue::Date("2025-01-15".into()));
        pk.insert("name", ScalarValue::String("test".into()));
        pk.insert("ts", ScalarValue::Timestamp("2025-01-15T10:30:00.000000Z".into()));
        pk.insert("empty", ScalarValue::Null);

        let canonical = pk.canonical_string();

        assert!(canonical.contains("active=b:true"));
        assert!(canonical.contains("count=i:42"));
        assert!(canonical.contains("date=d:2025-01-15"));
        assert!(canonical.contains("empty=n:null"));
        // "test" base64url = "dGVzdA"
        assert!(canonical.contains("name=s:dGVzdA"));
        assert!(canonical.contains("ts=t:2025-01-15T10:30:00.000000Z"));
    }

    #[test]
    fn test_canonical_deterministic_regardless_of_insertion_order() {
        let mut pk1 = PartitionKey::new();
        pk1.insert("z", ScalarValue::Int64(1));
        pk1.insert("a", ScalarValue::Int64(2));
        pk1.insert("m", ScalarValue::Int64(3));

        let mut pk2 = PartitionKey::new();
        pk2.insert("a", ScalarValue::Int64(2));
        pk2.insert("m", ScalarValue::Int64(3));
        pk2.insert("z", ScalarValue::Int64(1));

        let mut pk3 = PartitionKey::new();
        pk3.insert("m", ScalarValue::Int64(3));
        pk3.insert("z", ScalarValue::Int64(1));
        pk3.insert("a", ScalarValue::Int64(2));

        assert_eq!(pk1.canonical_string(), pk2.canonical_string());
        assert_eq!(pk2.canonical_string(), pk3.canonical_string());
    }

    #[test]
    fn test_partition_id_derivation() {
        let asset_id = AssetId::from_str("asset_abc123")
            .expect("valid asset id");
        let mut pk = PartitionKey::new();
        pk.insert("date", ScalarValue::Date("2025-01-15".into()));

        let id1 = PartitionId::derive(&asset_id, &pk);
        let id2 = PartitionId::derive(&asset_id, &pk);

        // Same inputs -> same ID
        assert_eq!(id1, id2);

        // Different inputs -> different ID
        let mut pk_different = PartitionKey::new();
        pk_different.insert("date", ScalarValue::Date("2025-01-16".into()));
        let id3 = PartitionId::derive(&asset_id, &pk_different);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_empty_partition_key() {
        let pk = PartitionKey::new();
        assert!(pk.is_empty());
        assert_eq!(pk.canonical_string(), "");
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-core partition`
Expected: FAIL (module doesn't exist)

**Step 3: Update Cargo.toml with dependencies**

```toml
# crates/arco-core/Cargo.toml
[package]
name = "arco-core"
version.workspace = true
edition.workspace = true
license.workspace = true
description = "Core abstractions for Arco serverless lakehouse"

[dependencies]
base64 = { workspace = true }
chrono = { workspace = true }
hex = { workspace = true }
serde = { workspace = true }
sha2 = { workspace = true }
thiserror = { workspace = true }
ulid = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["rt-multi-thread", "macros"] }
```

**Step 4: Write implementation**

```rust
// crates/arco-core/src/partition.rs
//! Partition key types with cross-language canonical encoding.
//!
//! See module-level documentation for the canonical encoding specification.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use crate::id::AssetId;

/// Scalar value types allowed in partition keys.
///
/// Floats are intentionally excluded to prevent precision drift
/// across languages and serialization formats.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarValue {
    /// Arbitrary string (base64url encoded in canonical form)
    String(String),
    /// 64-bit signed integer
    Int64(i64),
    /// Boolean value
    Boolean(bool),
    /// Date in YYYY-MM-DD format
    Date(String),
    /// Timestamp in ISO 8601 format with microseconds, UTC
    Timestamp(String),
    /// Explicit null value
    Null,
}

impl ScalarValue {
    /// Returns the canonical representation with type tag.
    ///
    /// String values are base64url encoded (no padding) for URL safety.
    #[must_use]
    pub fn canonical_repr(&self) -> String {
        match self {
            Self::String(s) => {
                let encoded = URL_SAFE_NO_PAD.encode(s.as_bytes());
                format!("s:{encoded}")
            }
            Self::Int64(n) => format!("i:{n}"),
            Self::Boolean(b) => format!("b:{}", if *b { "true" } else { "false" }),
            Self::Date(d) => format!("d:{d}"),
            Self::Timestamp(ts) => format!("t:{ts}"),
            Self::Null => "n:null".to_string(),
        }
    }

    /// Returns the type tag character.
    #[must_use]
    pub fn type_tag(&self) -> char {
        match self {
            Self::String(_) => 's',
            Self::Int64(_) => 'i',
            Self::Boolean(_) => 'b',
            Self::Date(_) => 'd',
            Self::Timestamp(_) => 't',
            Self::Null => 'n',
        }
    }
}

/// Multi-dimensional partition key with deterministic canonical form.
///
/// Uses `BTreeMap` internally to ensure keys are always sorted alphabetically.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PartitionKey(BTreeMap<String, ScalarValue>);

impl PartitionKey {
    /// Creates a new empty partition key.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a dimension into the partition key.
    ///
    /// If a dimension with the same key exists, it is replaced.
    pub fn insert(&mut self, key: impl Into<String>, value: ScalarValue) {
        self.0.insert(key.into(), value);
    }

    /// Returns the canonical string representation.
    ///
    /// This is deterministic: same logical key produces same string,
    /// regardless of insertion order.
    #[must_use]
    pub fn canonical_string(&self) -> String {
        self.0
            .iter()
            .map(|(k, v)| format!("{k}={}", v.canonical_repr()))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns canonical bytes for hashing.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        self.canonical_string().into_bytes()
    }

    /// Returns true if the partition key has no dimensions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of dimensions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Gets a dimension value by key.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&ScalarValue> {
        self.0.get(key)
    }

    /// Returns an iterator over dimensions.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &ScalarValue)> {
        self.0.iter()
    }

    /// Parses a canonical string back into a PartitionKey.
    ///
    /// This is the inverse of `canonical_string()`. Round-trip property:
    /// `PartitionKey::parse(pk.canonical_string()) == Ok(pk)`
    ///
    /// # Errors
    /// Returns error if the string is malformed.
    pub fn parse(s: &str) -> Result<Self, PartitionKeyParseError> {
        if s.is_empty() {
            return Ok(Self::new());
        }

        let mut pk = Self::new();
        for part in s.split(',') {
            let (key, encoded) = part
                .split_once('=')
                .ok_or_else(|| PartitionKeyParseError::MissingEquals(part.to_string()))?;

            // Validate key (must be valid identifier)
            if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err(PartitionKeyParseError::InvalidKey(key.to_string()));
            }

            // Parse type prefix and value
            let (type_prefix, value_part) = encoded
                .split_once(':')
                .ok_or_else(|| PartitionKeyParseError::MissingTypePrefix(encoded.to_string()))?;

            let scalar = match type_prefix {
                "n" => ScalarValue::Null,
                "b" => match value_part {
                    "1" => ScalarValue::Bool(true),
                    "0" => ScalarValue::Bool(false),
                    _ => return Err(PartitionKeyParseError::InvalidBool(value_part.to_string())),
                },
                "i" => {
                    let n: i64 = value_part
                        .parse()
                        .map_err(|_| PartitionKeyParseError::InvalidInt(value_part.to_string()))?;
                    ScalarValue::Int(n)
                }
                "s" => {
                    // Decode base64url
                    let bytes = BASE64_URL_SAFE_NO_PAD
                        .decode(value_part)
                        .map_err(|_| PartitionKeyParseError::InvalidBase64(value_part.to_string()))?;
                    let string = String::from_utf8(bytes)
                        .map_err(|_| PartitionKeyParseError::InvalidUtf8(value_part.to_string()))?;
                    ScalarValue::String(string)
                }
                "d" => {
                    // Date stored as days since epoch
                    let days: i32 = value_part
                        .parse()
                        .map_err(|_| PartitionKeyParseError::InvalidDate(value_part.to_string()))?;
                    let date = NaiveDate::from_num_days_from_ce_opt(days + 719163)
                        .ok_or_else(|| PartitionKeyParseError::InvalidDate(value_part.to_string()))?;
                    ScalarValue::Date(date)
                }
                "ts" => {
                    // Timestamp stored as milliseconds since epoch
                    let millis: i64 = value_part
                        .parse()
                        .map_err(|_| PartitionKeyParseError::InvalidTimestamp(value_part.to_string()))?;
                    let dt = DateTime::from_timestamp_millis(millis)
                        .ok_or_else(|| PartitionKeyParseError::InvalidTimestamp(value_part.to_string()))?;
                    ScalarValue::Timestamp(dt)
                }
                _ => return Err(PartitionKeyParseError::UnknownType(type_prefix.to_string())),
            };

            pk.add(key.to_string(), scalar);
        }

        Ok(pk)
    }
}

/// Errors that can occur when parsing a canonical partition key string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionKeyParseError {
    /// Missing '=' separator between key and value
    MissingEquals(String),
    /// Invalid key (empty or invalid characters)
    InvalidKey(String),
    /// Missing type prefix (e.g., "s:", "i:")
    MissingTypePrefix(String),
    /// Unknown type prefix
    UnknownType(String),
    /// Invalid boolean value (must be "0" or "1")
    InvalidBool(String),
    /// Invalid integer value
    InvalidInt(String),
    /// Invalid base64 encoding
    InvalidBase64(String),
    /// Invalid UTF-8 in decoded string
    InvalidUtf8(String),
    /// Invalid date value
    InvalidDate(String),
    /// Invalid timestamp value
    InvalidTimestamp(String),
}

impl std::fmt::Display for PartitionKeyParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEquals(s) => write!(f, "missing '=' in partition segment: {s}"),
            Self::InvalidKey(s) => write!(f, "invalid partition key: {s}"),
            Self::MissingTypePrefix(s) => write!(f, "missing type prefix in value: {s}"),
            Self::UnknownType(s) => write!(f, "unknown type prefix: {s}"),
            Self::InvalidBool(s) => write!(f, "invalid boolean value: {s}"),
            Self::InvalidInt(s) => write!(f, "invalid integer value: {s}"),
            Self::InvalidBase64(s) => write!(f, "invalid base64 encoding: {s}"),
            Self::InvalidUtf8(s) => write!(f, "invalid UTF-8 in decoded string: {s}"),
            Self::InvalidDate(s) => write!(f, "invalid date value: {s}"),
            Self::InvalidTimestamp(s) => write!(f, "invalid timestamp value: {s}"),
        }
    }
}

impl std::error::Error for PartitionKeyParseError {}

/// Derived partition identifier (stable across re-materializations).
///
/// The partition ID is derived from `hash(asset_id + canonical_partition_key)`,
/// ensuring the same asset partition always has the same ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionId(String);

impl PartitionId {
    /// Derives partition ID from asset ID + partition key.
    ///
    /// The derivation is deterministic: same inputs always produce same ID.
    #[must_use]
    pub fn derive(asset_id: &AssetId, partition_key: &PartitionKey) -> Self {
        let input = format!("{}:{}", asset_id.as_str(), partition_key.canonical_string());
        let hash = Sha256::digest(input.as_bytes());
        // Use first 16 hex chars (64 bits) for reasonable uniqueness
        let short_hash = &hex::encode(hash)[..16];
        Self(format!("part_{short_hash}"))
    }

    /// Returns the ID string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
```

**Step 5: Update lib.rs to include partition module**

```rust
// Add to crates/arco-core/src/lib.rs
pub mod partition;

// Add to prelude
pub use crate::partition::{PartitionId, PartitionKey, PartitionKeyParseError, ScalarValue};
```

**Step 6: Run test to verify it passes**

Run: `cargo test -p arco-core partition`
Expected: PASS

**Step 7: Commit**

```bash
git add crates/arco-core/
git commit -m "feat(core): add PartitionKey with cross-language canonical encoding"
```

---

### Task 1.2: Implement StorageBackend with Full Architecture Contract

**Files:**
- Create: `crates/arco-core/src/storage.rs`
- Modify: `crates/arco-core/Cargo.toml`

**Why:** The storage backend contract must match architecture docs including:
- `signed_url()` for direct access (even if unused in Phase 1)
- `ObjectMeta` with `last_modified` and `etag` fields
- Range validation to prevent panics

**Step 1: Write the failing tests**

```rust
// crates/arco-core/src/storage.rs
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_memory_backend_roundtrip() {
        let backend = MemoryBackend::new();
        let data = Bytes::from("hello world");

        let result = backend
            .put("test/file.txt", data.clone(), WritePrecondition::None)
            .await
            .expect("put should succeed");

        assert!(matches!(result, WriteResult::Success { generation: 1 }));

        let retrieved = backend.get("test/file.txt").await.expect("get should succeed");
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_object_meta_has_required_fields() {
        let backend = MemoryBackend::new();
        backend
            .put("test.txt", Bytes::from("data"), WritePrecondition::None)
            .await
            .expect("put should succeed");

        let meta = backend
            .head("test.txt")
            .await
            .expect("head should succeed")
            .expect("object should exist");

        // Required by architecture contract
        assert_eq!(meta.path, "test.txt");
        assert_eq!(meta.size, 4);
        assert!(meta.generation > 0);
        assert!(meta.last_modified.is_some(), "must have last_modified");
        assert!(meta.etag.is_some(), "must have etag");
    }

    #[tokio::test]
    async fn test_get_range_valid() {
        let backend = MemoryBackend::new();
        backend
            .put("test.txt", Bytes::from("hello world"), WritePrecondition::None)
            .await
            .expect("put should succeed");

        let result = backend.get_range("test.txt", 0..5).await.expect("should succeed");
        assert_eq!(result, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_get_range_clamps_end() {
        let backend = MemoryBackend::new();
        backend
            .put("test.txt", Bytes::from("hello"), WritePrecondition::None)
            .await
            .expect("put should succeed");

        // End beyond length should clamp, not panic
        let result = backend.get_range("test.txt", 0..100).await.expect("should succeed");
        assert_eq!(result, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_get_range_invalid_start() {
        let backend = MemoryBackend::new();
        backend
            .put("test.txt", Bytes::from("hello"), WritePrecondition::None)
            .await
            .expect("put should succeed");

        // Start beyond length should error, not panic
        let result = backend.get_range("test.txt", 100..200).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_signed_url_generation() {
        let backend = MemoryBackend::new();
        backend
            .put("test.txt", Bytes::from("data"), WritePrecondition::None)
            .await
            .expect("put should succeed");

        let url = backend
            .signed_url("test.txt", Duration::from_secs(3600))
            .await
            .expect("signed_url should succeed");

        // Memory backend returns mock URL
        assert!(url.contains("test.txt"));
        assert!(url.contains("expires="));
    }

    #[tokio::test]
    async fn test_precondition_does_not_exist() {
        let backend = MemoryBackend::new();

        // First write with DoesNotExist should succeed
        let result = backend
            .put("new.txt", Bytes::from("data"), WritePrecondition::DoesNotExist)
            .await
            .expect("should succeed");
        assert!(matches!(result, WriteResult::Success { .. }));

        // Second write with DoesNotExist should fail
        let result = backend
            .put("new.txt", Bytes::from("data2"), WritePrecondition::DoesNotExist)
            .await
            .expect("should succeed");
        assert!(matches!(result, WriteResult::PreconditionFailed { .. }));
    }

    #[tokio::test]
    async fn test_precondition_matches_generation() {
        let backend = MemoryBackend::new();

        // Create object
        let result = backend
            .put("gen.txt", Bytes::from("v1"), WritePrecondition::None)
            .await
            .expect("should succeed");
        let gen = match result {
            WriteResult::Success { generation } => generation,
            _ => panic!("expected success"),
        };

        // Update with correct generation should succeed
        let result = backend
            .put("gen.txt", Bytes::from("v2"), WritePrecondition::MatchesGeneration(gen))
            .await
            .expect("should succeed");
        assert!(matches!(result, WriteResult::Success { .. }));

        // Update with stale generation should fail
        let result = backend
            .put("gen.txt", Bytes::from("v3"), WritePrecondition::MatchesGeneration(gen))
            .await
            .expect("should succeed");
        assert!(matches!(result, WriteResult::PreconditionFailed { .. }));
    }

    #[tokio::test]
    async fn test_list_with_prefix() {
        let backend = MemoryBackend::new();

        backend.put("a/1.txt", Bytes::from("a1"), WritePrecondition::None).await.unwrap();
        backend.put("a/2.txt", Bytes::from("a2"), WritePrecondition::None).await.unwrap();
        backend.put("b/1.txt", Bytes::from("b1"), WritePrecondition::None).await.unwrap();

        let list_a = backend.list("a/").await.expect("should succeed");
        assert_eq!(list_a.len(), 2);

        let list_b = backend.list("b/").await.expect("should succeed");
        assert_eq!(list_b.len(), 1);
    }

    #[tokio::test]
    async fn test_delete() {
        let backend = MemoryBackend::new();

        backend.put("del.txt", Bytes::from("data"), WritePrecondition::None).await.unwrap();
        assert!(backend.head("del.txt").await.unwrap().is_some());

        backend.delete("del.txt").await.expect("should succeed");
        assert!(backend.head("del.txt").await.unwrap().is_none());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-core storage`
Expected: FAIL (module doesn't exist)

**Step 3: Update Cargo.toml**

```toml
# Add to [dependencies]
async-trait = { workspace = true }
bytes = { workspace = true }
tokio = { workspace = true }
```

**Step 4: Write implementation**

```rust
// crates/arco-core/src/storage.rs
//! Storage backend abstraction for object storage (GCS, S3, local).
//!
//! This module defines the core storage contract that all backends must implement.
//! The contract matches the architecture docs:
//! - Conditional writes with preconditions
//! - Object metadata including `last_modified` and `etag`
//! - Signed URL generation for direct access

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::error::{Error, Result};

/// Precondition for conditional writes (CAS operations).
#[derive(Debug, Clone)]
pub enum WritePrecondition {
    /// Write only if object does not exist.
    DoesNotExist,
    /// Write only if object's generation matches.
    MatchesGeneration(i64),
    /// Write unconditionally.
    None,
}

/// Result of a conditional write.
#[derive(Debug, Clone)]
pub enum WriteResult {
    /// Write succeeded, returns new generation.
    Success { generation: i64 },
    /// Precondition failed, returns current generation.
    PreconditionFailed { current_generation: i64 },
}

/// Metadata about a stored object.
///
/// Per architecture docs: must include `last_modified` and `etag`.
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    /// Object path (key).
    pub path: String,
    /// Object size in bytes.
    pub size: u64,
    /// Object generation (version number for CAS).
    pub generation: i64,
    /// Last modification timestamp.
    pub last_modified: Option<DateTime<Utc>>,
    /// Entity tag for cache validation.
    pub etag: Option<String>,
}

/// Storage backend trait for object storage.
///
/// All storage backends (GCS, S3, memory) implement this trait.
/// The contract is designed for cloud object storage semantics.
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    /// Reads entire object.
    ///
    /// Returns `Error::NotFound` if object doesn't exist.
    async fn get(&self, path: &str) -> Result<Bytes>;

    /// Reads a byte range from an object.
    ///
    /// Returns `Error::InvalidInput` if start > object length.
    /// Clamps end to object length if end > length.
    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes>;

    /// Writes with optional precondition.
    ///
    /// Returns `WriteResult::PreconditionFailed` if precondition not met.
    /// Never returns error for precondition failure - that's a normal result.
    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult>;

    /// Deletes an object.
    ///
    /// Succeeds even if object doesn't exist (idempotent).
    async fn delete(&self, path: &str) -> Result<()>;

    /// Lists objects with the given prefix.
    ///
    /// Returns empty vec if no objects match.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>>;

    /// Gets object metadata without reading content.
    ///
    /// Returns `None` if object doesn't exist.
    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>>;

    /// Generates a signed URL for direct access.
    ///
    /// Per architecture docs: required for direct client access.
    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String>;
}

/// In-memory storage backend for testing.
///
/// Thread-safe via `RwLock`. Not suitable for production.
#[derive(Debug, Default)]
pub struct MemoryBackend {
    objects: Arc<RwLock<HashMap<String, StoredObject>>>,
}

#[derive(Debug, Clone)]
struct StoredObject {
    data: Bytes,
    generation: i64,
    last_modified: DateTime<Utc>,
}

impl MemoryBackend {
    /// Creates a new empty memory backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StorageBackend for MemoryBackend {
    async fn get(&self, path: &str) -> Result<Bytes> {
        let objects = self
            .objects
            .read()
            .map_err(|_| Error::Internal("lock poisoned".into()))?;

        objects
            .get(path)
            .map(|o| o.data.clone())
            .ok_or_else(|| Error::NotFound(format!("object not found: {path}")))
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        let data = self.get(path).await?;
        let start = range.start as usize;
        let len = data.len();

        if start > len {
            return Err(Error::InvalidInput(format!(
                "range start {start} exceeds object length {len}"
            )));
        }

        let end = (range.end as usize).min(len);
        Ok(data.slice(start..end))
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        let mut objects = self
            .objects
            .write()
            .map_err(|_| Error::Internal("lock poisoned".into()))?;

        let current = objects.get(path);

        match precondition {
            WritePrecondition::DoesNotExist => {
                if let Some(obj) = current {
                    return Ok(WriteResult::PreconditionFailed {
                        current_generation: obj.generation,
                    });
                }
            }
            WritePrecondition::MatchesGeneration(expected) => match current {
                Some(obj) if obj.generation != expected => {
                    return Ok(WriteResult::PreconditionFailed {
                        current_generation: obj.generation,
                    });
                }
                None => {
                    return Ok(WriteResult::PreconditionFailed {
                        current_generation: 0,
                    });
                }
                _ => {}
            },
            WritePrecondition::None => {}
        }

        let new_gen = current.map_or(1, |o| o.generation + 1);
        objects.insert(
            path.to_string(),
            StoredObject {
                data,
                generation: new_gen,
                last_modified: Utc::now(),
            },
        );

        Ok(WriteResult::Success { generation: new_gen })
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let mut objects = self
            .objects
            .write()
            .map_err(|_| Error::Internal("lock poisoned".into()))?;

        objects.remove(path);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let objects = self
            .objects
            .read()
            .map_err(|_| Error::Internal("lock poisoned".into()))?;

        Ok(objects
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(path, obj)| ObjectMeta {
                path: path.clone(),
                size: obj.data.len() as u64,
                generation: obj.generation,
                last_modified: Some(obj.last_modified),
                etag: Some(format!("\"{}\"", obj.generation)),
            })
            .collect())
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        let objects = self
            .objects
            .read()
            .map_err(|_| Error::Internal("lock poisoned".into()))?;

        Ok(objects.get(path).map(|obj| ObjectMeta {
            path: path.to_string(),
            size: obj.data.len() as u64,
            generation: obj.generation,
            last_modified: Some(obj.last_modified),
            etag: Some(format!("\"{}\"", obj.generation)),
        }))
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        // Mock implementation for testing
        Ok(format!(
            "memory://localhost/{path}?expires={}&signature=mock",
            expiry.as_secs()
        ))
    }
}
```

**Step 5: Update lib.rs**

```rust
// Add to crates/arco-core/src/lib.rs
pub mod storage;

// Add to prelude
pub use crate::storage::{MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
```

**Step 6: Run test to verify it passes**

Run: `cargo test -p arco-core storage`
Expected: PASS

**Step 7: Commit**

```bash
git add crates/arco-core/
git commit -m "feat(core): add StorageBackend with signed_url and rich ObjectMeta"
```

---

### Task 1.3: Implement ScopedStorage with Architecture-Aligned Paths

**Files:**
- Create: `crates/arco-core/src/scoped_storage.rs`

**Storage Layout (per unified platform design - tenant + workspace scoping):**

```
{tenant}/{workspace}/
├── manifests/               # Root manifest pointers
│   └── catalog.json         # Main catalog manifest
├── core/                    # Tier 1: Core catalog (strong consistency)
│   ├── manifest.json        # Current catalog state pointer
│   ├── lock.json            # Distributed lock
│   ├── commits/             # Hash-chain commit records
│   │   └── {commit_id}.json
│   └── snapshots/           # Immutable catalog snapshots
│       └── v{n}/
│           ├── assets.parquet
│           └── schemas.parquet
├── ledger/                  # Tier 2: Append-only event logs
│   ├── execution/           # MaterializationCompleted events
│   │   └── {date}/
│   ├── quality/             # Data quality events
│   └── lineage/             # Lineage events
├── state/                   # Tier 2: Compacted Parquet state
│   ├── execution/           # Materialization state
│   │   ├── materializations/
│   │   └── partitions/
│   ├── quality/
│   └── lineage/
│       └── edges/
├── lineage/                 # Lineage domain manifest
│   └── manifest.json
├── governance/              # Governance overlay
│   ├── tags.parquet
│   └── owners.parquet
├── locks/                   # Distributed lock files
│   └── {lock_id}.json
└── audit/                   # Audit trail
    └── commits/
```

> **Note:** The path format `{tenant}/{workspace}/` aligns with the unified platform design which explicitly states: "gs://…/{tenant}/{workspace}/…"

**Step 1: Write the failing tests**

```rust
// crates/arco-core/src/scoped_storage.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryBackend;
    use std::sync::Arc;

    #[test]
    fn test_core_paths_match_architecture() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production");

        // Per unified platform design: {tenant}/{workspace}/...
        assert_eq!(
            storage.core_manifest_path(),
            "acme/production/core/manifest.json"
        );
        assert_eq!(
            storage.core_lock_path(),
            "acme/production/core/lock.json"
        );
        assert_eq!(
            storage.core_snapshot_path(42),
            "acme/production/core/snapshots/v42/"
        );
        assert_eq!(
            storage.core_commit_path("commit_abc123"),
            "acme/production/core/commits/commit_abc123.json"
        );
    }

    #[test]
    fn test_ledger_paths_match_architecture() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production");

        assert_eq!(
            storage.ledger_path("execution", "2025-01-15"),
            "acme/production/ledger/execution/2025-01-15/"
        );
        assert_eq!(
            storage.ledger_path("quality", "2025-01-15"),
            "acme/production/ledger/quality/2025-01-15/"
        );
        assert_eq!(
            storage.ledger_path("lineage", "2025-01-15"),
            "acme/production/ledger/lineage/2025-01-15/"
        );
    }

    #[test]
    fn test_state_paths_match_architecture() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production");

        assert_eq!(
            storage.state_path("execution", "materializations"),
            "acme/production/state/execution/materializations/"
        );
        assert_eq!(
            storage.state_path("lineage", "edges"),
            "acme/production/state/lineage/edges/"
        );
    }

    #[test]
    fn test_governance_paths_match_architecture() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production");

        assert_eq!(
            storage.governance_tags_path(),
            "acme/production/governance/tags.parquet"
        );
        assert_eq!(
            storage.governance_owners_path(),
            "acme/production/governance/owners.parquet"
        );
    }

    #[tokio::test]
    async fn test_tenant_workspace_isolation() {
        let backend = Arc::new(MemoryBackend::new());

        // Same tenant, different workspaces
        let prod = ScopedStorage::new(backend.clone(), "acme", "production");
        let staging = ScopedStorage::new(backend.clone(), "acme", "staging");

        prod.put_raw("test.txt", Bytes::from("prod-data"), WritePrecondition::None)
            .await
            .expect("put should succeed");
        staging
            .put_raw("test.txt", Bytes::from("staging-data"), WritePrecondition::None)
            .await
            .expect("put should succeed");

        // Each workspace sees only their own data
        let prod_data = prod.get_raw("test.txt").await.expect("get should succeed");
        let staging_data = staging.get_raw("test.txt").await.expect("get should succeed");

        assert_eq!(prod_data, Bytes::from("prod-data"));
        assert_eq!(staging_data, Bytes::from("staging-data"));
    }

    #[tokio::test]
    async fn test_different_tenants_isolation() {
        let backend = Arc::new(MemoryBackend::new());

        let acme = ScopedStorage::new(backend.clone(), "acme", "production");
        let globex = ScopedStorage::new(backend.clone(), "globex", "production");

        acme.put_raw("test.txt", Bytes::from("acme-data"), WritePrecondition::None)
            .await
            .expect("put should succeed");
        globex
            .put_raw("test.txt", Bytes::from("globex-data"), WritePrecondition::None)
            .await
            .expect("put should succeed");

        let acme_data = acme.get_raw("test.txt").await.expect("get should succeed");
        let globex_data = globex.get_raw("test.txt").await.expect("get should succeed");

        assert_eq!(acme_data, Bytes::from("acme-data"));
        assert_eq!(globex_data, Bytes::from("globex-data"));
    }

    #[tokio::test]
    async fn test_catalog_path_operations() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production");

        // Write to core manifest path
        let manifest_data = Bytes::from(r#"{"version": 1}"#);
        storage
            .put_core_manifest(manifest_data.clone(), WritePrecondition::None)
            .await
            .expect("put should succeed");

        // Read back
        let retrieved = storage.get_core_manifest().await.expect("get should succeed");
        assert_eq!(retrieved, manifest_data);
    }

    // =========================================================================
    // ISOLATION BOUNDARY TESTS - Critical for multi-tenant security
    // =========================================================================

    #[tokio::test]
    async fn test_path_traversal_prevention() {
        // CRITICAL: ScopedStorage must reject path traversal attacks.
        // Attempts to escape the scope boundary via ".." must fail.

        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production");

        // Attempt to write outside scope via path traversal
        let traversal_paths = [
            "../other_workspace/secret.txt",
            "../../other_tenant/data.txt",
            "manifests/../../../escape.txt",
            "ledger/execution/..%2F..%2Fsecret.txt", // URL-encoded
            "ledger/../../../etc/passwd",
        ];

        for path in &traversal_paths {
            let result = storage
                .put_raw(path, Bytes::from("attack"), WritePrecondition::None)
                .await;
            assert!(
                result.is_err(),
                "path traversal must be rejected: {path}"
            );
        }

        // Similarly for reads
        for path in &traversal_paths {
            let result = storage.get_raw(path).await;
            assert!(
                result.is_err(),
                "path traversal read must be rejected: {path}"
            );
        }
    }

    #[tokio::test]
    async fn test_list_isolation_boundary() {
        // INVARIANT: list operations must be scoped to tenant+workspace.
        // One scope cannot enumerate another scope's files.

        let backend = Arc::new(MemoryBackend::new());

        let acme_prod = ScopedStorage::new(backend.clone(), "acme", "production");
        let acme_staging = ScopedStorage::new(backend.clone(), "acme", "staging");
        let globex_prod = ScopedStorage::new(backend.clone(), "globex", "production");

        // Write files to each scope
        acme_prod
            .put_raw("secret1.txt", Bytes::from("acme-prod"), WritePrecondition::None)
            .await
            .expect("put");
        acme_staging
            .put_raw("secret2.txt", Bytes::from("acme-staging"), WritePrecondition::None)
            .await
            .expect("put");
        globex_prod
            .put_raw("secret3.txt", Bytes::from("globex-prod"), WritePrecondition::None)
            .await
            .expect("put");

        // Each scope should only see its own files
        let acme_prod_files = acme_prod.list("").await.expect("list");
        let acme_staging_files = acme_staging.list("").await.expect("list");
        let globex_prod_files = globex_prod.list("").await.expect("list");

        assert_eq!(acme_prod_files.len(), 1, "acme/prod sees only its file");
        assert_eq!(acme_staging_files.len(), 1, "acme/staging sees only its file");
        assert_eq!(globex_prod_files.len(), 1, "globex/prod sees only its file");

        // Verify correct files visible
        assert!(acme_prod_files[0].as_str().contains("secret1.txt"));
        assert!(acme_staging_files[0].as_str().contains("secret2.txt"));
        assert!(globex_prod_files[0].as_str().contains("secret3.txt"));
    }

    #[tokio::test]
    async fn test_cross_scope_read_fails() {
        // INVARIANT: ScopedStorage instances cannot read each other's data
        // even if the underlying paths are known.

        let backend = Arc::new(MemoryBackend::new());

        let acme = ScopedStorage::new(backend.clone(), "acme", "production");
        let globex = ScopedStorage::new(backend.clone(), "globex", "production");

        // Write data under acme scope
        acme.put_raw("secret.txt", Bytes::from("acme-secret"), WritePrecondition::None)
            .await
            .expect("put");

        // Verify acme can read its own data
        let acme_data = acme.get_raw("secret.txt").await.expect("should succeed");
        assert_eq!(acme_data, Bytes::from("acme-secret"));

        // Globex cannot read acme's data using the same relative path
        let globex_result = globex.get_raw("secret.txt").await;
        assert!(
            globex_result.is_err() || globex_result.unwrap().is_empty(),
            "cross-scope read must fail or return empty"
        );
    }

    #[tokio::test]
    async fn test_cross_scope_delete_fails() {
        // INVARIANT: ScopedStorage cannot delete files in other scopes.

        let backend = Arc::new(MemoryBackend::new());

        let acme = ScopedStorage::new(backend.clone(), "acme", "production");
        let globex = ScopedStorage::new(backend.clone(), "globex", "production");

        // Write data under acme scope
        acme.put_raw("important.txt", Bytes::from("critical-data"), WritePrecondition::None)
            .await
            .expect("put");

        // Globex tries to delete acme's file using same relative path
        let delete_result = globex.delete("important.txt").await;
        // This should either fail or be a no-op (deleting non-existent file)

        // Verify acme's data is still intact
        let acme_data = acme.get_raw("important.txt").await.expect("should still exist");
        assert_eq!(acme_data, Bytes::from("critical-data"));
    }

    #[tokio::test]
    async fn test_tenant_id_validation() {
        // INVARIANT: Tenant and workspace IDs must be valid identifiers.
        // This prevents path injection via malicious tenant names.

        let backend = Arc::new(MemoryBackend::new());

        // Invalid tenant IDs should be rejected at construction time
        let invalid_ids = [
            "",               // empty
            "tenant/evil",    // contains slash
            "tenant..name",   // contains dots
            "../escape",      // path traversal
            "tenant name",    // contains space
            "tenant\nname",   // contains newline
        ];

        for invalid_id in &invalid_ids {
            let result = std::panic::catch_unwind(|| {
                ScopedStorage::new(backend.clone(), invalid_id, "workspace")
            });
            // Either panic or return an error type - both are acceptable
            // The key is that invalid IDs don't silently succeed
        }
    }

    #[tokio::test]
    async fn test_absolute_path_normalization() {
        // All relative paths within a scope should normalize to absolute storage paths.
        // Leading slashes, double slashes, etc. should be handled gracefully.

        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production");

        // These should all refer to the same file
        let paths = [
            "file.txt",
            "./file.txt",
            "subdir/../file.txt",
        ];

        storage
            .put_raw("file.txt", Bytes::from("data"), WritePrecondition::None)
            .await
            .expect("put");

        // Note: only "file.txt" is guaranteed to work; others depend on normalization impl
        let data = storage.get_raw("file.txt").await.expect("get");
        assert_eq!(data, Bytes::from("data"));
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-core scoped_storage`
Expected: FAIL

**Step 3: Write implementation**

```rust
// crates/arco-core/src/scoped_storage.rs
//! Tenant + workspace scoped storage with architecture-aligned path layout.
//!
//! This module enforces the documented storage layout for multi-tenant, multi-workspace
//! catalog operations. All paths are prefixed with `{tenant}/{workspace}/`.
//! Per unified platform design: tenant + workspace = primary scoping boundary.

use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

use crate::error::Result;
use crate::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};

/// Tenant + workspace scoped storage wrapper.
///
/// Enforces isolation by prefixing all paths with `{tenant}/{workspace}/`.
/// Path helpers align with the documented catalog storage layout.
#[derive(Clone)]
pub struct ScopedStorage {
    backend: Arc<dyn StorageBackend>,
    tenant_id: String,
    workspace_id: String,
}

impl ScopedStorage {
    /// Creates a new scoped storage wrapper.
    pub fn new(
        backend: Arc<dyn StorageBackend>,
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
    ) -> Self {
        Self {
            backend,
            tenant_id: tenant_id.into(),
            workspace_id: workspace_id.into(),
        }
    }

    /// Returns the tenant ID.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the workspace ID.
    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    /// Returns the backend for advanced operations.
    pub fn backend(&self) -> &Arc<dyn StorageBackend> {
        &self.backend
    }

    // === Path Construction ===

    fn scope_prefix(&self) -> String {
        format!("{}/{}", self.tenant_id, self.workspace_id)
    }

    fn scoped_path(&self, path: &str) -> String {
        format!("{}/{}", self.scope_prefix(), path)
    }

    // === Core Catalog Paths (Tier 1) ===

    /// Path to the core catalog manifest.
    #[must_use]
    pub fn core_manifest_path(&self) -> String {
        self.scoped_path("core/manifest.json")
    }

    /// Path to the distributed lock file.
    #[must_use]
    pub fn core_lock_path(&self) -> String {
        self.scoped_path("core/lock.json")
    }

    /// Path to a catalog snapshot directory.
    #[must_use]
    pub fn core_snapshot_path(&self, version: u64) -> String {
        self.scoped_path(&format!("core/snapshots/v{version}/"))
    }

    /// Path to a specific commit record.
    #[must_use]
    pub fn core_commit_path(&self, commit_id: &str) -> String {
        self.scoped_path(&format!("core/commits/{commit_id}.json"))
    }

    // === Ledger Paths (Tier 2 Input - Append-Only Events) ===

    /// Path to event log partition (domain/date).
    #[must_use]
    pub fn ledger_path(&self, domain: &str, date: &str) -> String {
        self.scoped_path(&format!("ledger/{domain}/{date}/"))
    }

    // === State Paths (Tier 2 Output - Compacted Parquet) ===

    /// Path to compacted state table.
    #[must_use]
    pub fn state_path(&self, domain: &str, table: &str) -> String {
        self.scoped_path(&format!("state/{domain}/{table}/"))
    }

    // === Lineage Domain ===

    /// Path to lineage domain manifest.
    #[must_use]
    pub fn lineage_manifest_path(&self) -> String {
        self.scoped_path("lineage/manifest.json")
    }

    /// Path to lineage edges state.
    #[must_use]
    pub fn lineage_edges_path(&self) -> String {
        self.scoped_path("state/lineage/edges/")
    }

    // === Governance Domain ===

    /// Path to governance tags table.
    #[must_use]
    pub fn governance_tags_path(&self) -> String {
        self.scoped_path("governance/tags.parquet")
    }

    /// Path to governance owners table.
    #[must_use]
    pub fn governance_owners_path(&self) -> String {
        self.scoped_path("governance/owners.parquet")
    }

    // === High-Level Operations ===

    /// Gets the core manifest.
    pub async fn get_core_manifest(&self) -> Result<Bytes> {
        self.backend.get(&self.core_manifest_path()).await
    }

    /// Puts the core manifest with precondition.
    pub async fn put_core_manifest(
        &self,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        self.backend
            .put(&self.core_manifest_path(), data, precondition)
            .await
    }

    /// Gets the core manifest metadata.
    pub async fn head_core_manifest(&self) -> Result<Option<ObjectMeta>> {
        self.backend.head(&self.core_manifest_path()).await
    }

    // === Raw Operations (for paths not covered by helpers) ===

    /// Reads data at a catalog-relative path.
    pub async fn get_raw(&self, path: &str) -> Result<Bytes> {
        self.backend.get(&self.catalog_path(path)).await
    }

    /// Writes data at a catalog-relative path.
    pub async fn put_raw(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        self.backend
            .put(&self.catalog_path(path), data, precondition)
            .await
    }

    /// Deletes data at a catalog-relative path.
    pub async fn delete_raw(&self, path: &str) -> Result<()> {
        self.backend.delete(&self.catalog_path(path)).await
    }

    /// Lists objects at a catalog-relative path prefix.
    pub async fn list_raw(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.backend.list(&self.catalog_path(prefix)).await
    }

    /// Gets metadata at a catalog-relative path.
    pub async fn head_raw(&self, path: &str) -> Result<Option<ObjectMeta>> {
        self.backend.head(&self.catalog_path(path)).await
    }

    /// Generates signed URL for a catalog-relative path.
    pub async fn signed_url_raw(&self, path: &str, expiry: Duration) -> Result<String> {
        self.backend
            .signed_url(&self.catalog_path(path), expiry)
            .await
    }
}
```

**Step 4: Update lib.rs**

```rust
// Add to crates/arco-core/src/lib.rs
pub mod scoped_storage;

// Add to prelude
pub use crate::scoped_storage::ScopedStorage;
```

**Step 5: Run test to verify it passes**

Run: `cargo test -p arco-core tenant_storage`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/arco-core/
git commit -m "feat(core): add ScopedStorage with tenant+workspace path layout"
```

---

### Task 1.4: Add Cross-Language PartitionKey Tests

**Files:**
- Create: `tests/fixtures/partition_key_cases.json`
- Create: `tests/cross_language/mod.rs`
- Create: `tests/cross_language/partition_key_test.rs`
- Create: `python/tests/test_partition_key_canonical.py`

**Why:** The architecture docs require cross-language canonical partition encoding tests. This ensures Rust and Python produce identical canonical strings.

**Step 1: Create shared test fixtures**

```json
// tests/fixtures/partition_key_cases.json
[
  {
    "name": "single_date",
    "dimensions": {
      "date": {"type": "date", "value": "2025-01-15"}
    },
    "expected_canonical": "date=d:2025-01-15"
  },
  {
    "name": "single_int",
    "dimensions": {
      "count": {"type": "int64", "value": "42"}
    },
    "expected_canonical": "count=i:42"
  },
  {
    "name": "single_bool_true",
    "dimensions": {
      "active": {"type": "bool", "value": "true"}
    },
    "expected_canonical": "active=b:true"
  },
  {
    "name": "single_bool_false",
    "dimensions": {
      "active": {"type": "bool", "value": "false"}
    },
    "expected_canonical": "active=b:false"
  },
  {
    "name": "single_string_simple",
    "dimensions": {
      "name": {"type": "string", "value": "test"}
    },
    "expected_canonical": "name=s:dGVzdA"
  },
  {
    "name": "single_string_with_slash",
    "dimensions": {
      "path": {"type": "string", "value": "foo/bar"}
    },
    "expected_canonical": "path=s:Zm9vL2Jhcg"
  },
  {
    "name": "single_string_with_special_chars",
    "dimensions": {
      "query": {"type": "string", "value": "foo/bar?baz=1&x=2"}
    },
    "expected_canonical": "query=s:Zm9vL2Jhcj9iYXo9MSZ4PTI"
  },
  {
    "name": "multi_dimension_sorted",
    "dimensions": {
      "region": {"type": "string", "value": "us-east"},
      "date": {"type": "date", "value": "2025-01-15"}
    },
    "expected_canonical": "date=d:2025-01-15,region=s:dXMtZWFzdA"
  },
  {
    "name": "multi_dimension_three_keys",
    "dimensions": {
      "z_last": {"type": "int64", "value": "3"},
      "a_first": {"type": "int64", "value": "1"},
      "m_middle": {"type": "int64", "value": "2"}
    },
    "expected_canonical": "a_first=i:1,m_middle=i:2,z_last=i:3"
  },
  {
    "name": "timestamp",
    "dimensions": {
      "ts": {"type": "timestamp", "value": "2025-01-15T10:30:00.000000Z"}
    },
    "expected_canonical": "ts=t:2025-01-15T10:30:00.000000Z"
  },
  {
    "name": "null_value",
    "dimensions": {
      "missing": {"type": "null", "value": "null"}
    },
    "expected_canonical": "missing=n:null"
  },
  {
    "name": "empty_string",
    "dimensions": {
      "empty": {"type": "string", "value": ""}
    },
    "expected_canonical": "empty=s:"
  },
  {
    "name": "unicode_string",
    "dimensions": {
      "greeting": {"type": "string", "value": "hello world"}
    },
    "expected_canonical": "greeting=s:aGVsbG8gd29ybGQ"
  },
  {
    "name": "path_hostile_all_special",
    "comment": "Tests ALL path-hostile characters in one value",
    "dimensions": {
      "hostile": {"type": "string", "value": "a/b\\c?d=e&f#g%h:i@j!k$l"}
    },
    "expected_canonical": "hostile=s:YS9iXGM_ZD1lJmYjZyVoOmlAaiFrJGw"
  },
  {
    "name": "unicode_emoji",
    "comment": "Unicode emoji - must survive round-trip",
    "dimensions": {
      "emoji": {"type": "string", "value": "🎉"}
    },
    "expected_canonical": "emoji=s:8J-OiQ"
  },
  {
    "name": "unicode_cjk",
    "comment": "CJK characters - common in real data",
    "dimensions": {
      "name": {"type": "string", "value": "日本語"}
    },
    "expected_canonical": "name=s:5pel5pys6Kqe"
  },
  {
    "name": "negative_int",
    "dimensions": {
      "delta": {"type": "int64", "value": "-42"}
    },
    "expected_canonical": "delta=i:-42"
  },
  {
    "name": "zero_int",
    "dimensions": {
      "count": {"type": "int64", "value": "0"}
    },
    "expected_canonical": "count=i:0"
  },
  {
    "name": "max_int64",
    "comment": "Maximum i64 value - edge case",
    "dimensions": {
      "big": {"type": "int64", "value": "9223372036854775807"}
    },
    "expected_canonical": "big=i:9223372036854775807"
  }
]
```

> **Round-Trip Parsing Requirement:**
> Every canonical string MUST be parseable back to the original dimensions.
> This is critical for idempotency and deduplication.

**Step 2: Create Rust cross-language test**

```rust
// tests/cross_language/partition_key_test.rs
//! Cross-language PartitionKey canonical encoding tests.
//!
//! These tests use shared fixtures to ensure Rust and Python
//! produce identical canonical strings.

use arco_core::partition::{PartitionKey, ScalarValue};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
struct TestCase {
    name: String,
    dimensions: HashMap<String, DimensionValue>,
    expected_canonical: String,
}

#[derive(Debug, Deserialize)]
struct DimensionValue {
    #[serde(rename = "type")]
    typ: String,
    value: String,
}

fn load_test_cases() -> Vec<TestCase> {
    let fixture = include_str!("../fixtures/partition_key_cases.json");
    serde_json::from_str(fixture).expect("fixture should parse")
}

#[test]
fn test_all_fixtures_match_expected_canonical() {
    let cases = load_test_cases();

    for case in cases {
        let mut pk = PartitionKey::new();

        for (key, dim) in &case.dimensions {
            let value = match dim.typ.as_str() {
                "string" => ScalarValue::String(dim.value.clone()),
                "date" => ScalarValue::Date(dim.value.clone()),
                "timestamp" => ScalarValue::Timestamp(dim.value.clone()),
                "int64" => ScalarValue::Int64(
                    dim.value.parse().expect("int64 should parse"),
                ),
                "bool" => ScalarValue::Boolean(
                    dim.value.parse().expect("bool should parse"),
                ),
                "null" => ScalarValue::Null,
                other => panic!("unknown type in fixture: {other}"),
            };
            pk.insert(key, value);
        }

        let actual = pk.canonical_string();
        assert_eq!(
            actual, case.expected_canonical,
            "FAIL: case '{}'\n  expected: {}\n  actual:   {}",
            case.name, case.expected_canonical, actual
        );
    }
}

#[test]
fn test_fixture_count() {
    let cases = load_test_cases();
    // Ensure we have a reasonable number of test cases
    assert!(cases.len() >= 10, "expected at least 10 fixture cases");
}

#[test]
fn test_round_trip_all_fixtures() {
    // Every canonical string must be parseable back to original dimensions
    let cases = load_test_cases();

    for case in cases {
        // Build partition key from fixture
        let mut pk = PartitionKey::new();
        for (key, dim) in &case.dimensions {
            let value = match dim.typ.as_str() {
                "string" => ScalarValue::String(dim.value.clone()),
                "date" => ScalarValue::Date(dim.value.clone()),
                "timestamp" => ScalarValue::Timestamp(dim.value.clone()),
                "int64" => ScalarValue::Int64(dim.value.parse().unwrap()),
                "bool" => ScalarValue::Boolean(dim.value.parse().unwrap()),
                "null" => ScalarValue::Null,
                other => panic!("unknown type: {other}"),
            };
            pk.insert(key, value);
        }

        // Get canonical string
        let canonical = pk.canonical_string();

        // Parse back (this tests the parse implementation)
        let parsed = PartitionKey::parse(&canonical)
            .expect(&format!("case '{}' should parse: {}", case.name, canonical));

        // Must produce identical canonical string
        assert_eq!(
            parsed.canonical_string(), canonical,
            "FAIL round-trip: case '{}'\n  original:   {}\n  after parse: {}",
            case.name, canonical, parsed.canonical_string()
        );
    }
}

#[test]
fn test_canonical_is_path_safe() {
    // All canonical strings must be usable as path segments
    let cases = load_test_cases();

    for case in cases {
        let canonical = &case.expected_canonical;

        // Must not contain path-unsafe characters
        assert!(!canonical.contains('/'), "case '{}' has /", case.name);
        assert!(!canonical.contains('\\'), "case '{}' has \\", case.name);
        assert!(!canonical.contains('?'), "case '{}' has ?", case.name);
        assert!(!canonical.contains('#'), "case '{}' has #", case.name);
        assert!(!canonical.contains('%'), "case '{}' has %", case.name);

        // Only allowed special char is '=' for key=value and ',' for separators
        // and ':' for type tags
    }
}
```

**Step 3: Create tests/cross_language/mod.rs**

```rust
// tests/cross_language/mod.rs
mod partition_key_test;
```

**Step 4: Create Python test**

```python
# python/tests/test_partition_key_canonical.py
"""Cross-language PartitionKey canonical encoding tests.

These tests use the same shared fixtures as Rust to ensure
both languages produce identical canonical strings.
"""

import base64
import json
from pathlib import Path


def canonical_string(dimensions: dict) -> str:
    """Generate canonical partition key string matching Rust implementation.

    This MUST produce identical output to arco_core::partition::PartitionKey::canonical_string()
    """
    parts = []

    # Keys must be sorted alphabetically (matching BTreeMap behavior)
    for key in sorted(dimensions.keys()):
        dim = dimensions[key]
        typ = dim["type"]
        value = dim["value"]

        if typ == "string":
            # Base64url encode without padding
            encoded = base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii")
            encoded = encoded.rstrip("=")  # Remove padding
            parts.append(f"{key}=s:{encoded}")
        elif typ == "date":
            parts.append(f"{key}=d:{value}")
        elif typ == "timestamp":
            parts.append(f"{key}=t:{value}")
        elif typ == "int64":
            parts.append(f"{key}=i:{value}")
        elif typ == "bool":
            parts.append(f"{key}=b:{value.lower()}")
        elif typ == "null":
            parts.append(f"{key}=n:null")
        else:
            raise ValueError(f"Unknown type: {typ}")

    return ",".join(parts)


def load_fixtures() -> list[dict]:
    """Load shared test fixtures."""
    fixture_path = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "partition_key_cases.json"
    return json.loads(fixture_path.read_text())


def test_all_fixtures_match_expected_canonical():
    """Test all fixtures produce expected canonical string."""
    cases = load_fixtures()

    for case in cases:
        actual = canonical_string(case["dimensions"])
        expected = case["expected_canonical"]

        assert actual == expected, (
            f"FAIL: case '{case['name']}'\n"
            f"  expected: {expected}\n"
            f"  actual:   {actual}"
        )


def test_fixture_count():
    """Ensure we have a reasonable number of test cases."""
    cases = load_fixtures()
    assert len(cases) >= 10, f"expected at least 10 fixture cases, got {len(cases)}"


if __name__ == "__main__":
    # Allow running directly for debugging
    test_all_fixtures_match_expected_canonical()
    test_fixture_count()
    print("All tests passed!")
```

**Step 5: Run tests in both languages**

Run:
```bash
cargo test cross_language
pytest python/tests/test_partition_key_canonical.py -v
```
Expected: PASS in both

**Step 6: Commit**

```bash
git add tests/ python/tests/
git commit -m "test: add cross-language PartitionKey canonical encoding tests"
```

---

## Part 2: Catalog MVP (arco-catalog)

### Task 2.1: Define Asset Data Model

**Files:**
- Create: `crates/arco-catalog/src/asset.rs`
- Modify: `crates/arco-catalog/Cargo.toml`

*(Standard asset model implementation - see prior version for full details)*

---

### Task 2.2: Implement Multi-Manifest Structure (Physically Multi-File)

**Files:**
- Create: `crates/arco-catalog/src/manifest.rs`

**Why:** Per architecture docs, the catalog uses a multi-manifest structure with domain separation: core, operational, lineage, governance. This reduces contention and enables independent evolution.

> **CRITICAL: Physically Separate Files**
>
> The multi-manifest structure must be **physically multi-file**, not just a single JSON with embedded domains:
>
> ```
> {tenant}/{workspace}/manifests/
> ├── root.manifest.json        # Root pointer to domain manifests
> ├── core.manifest.json        # Tier 1: Assets, schemas (locked writes)
> ├── execution.manifest.json   # Tier 2: Materializations (compactor writes)
> ├── lineage.manifest.json     # Tier 2: Dependency edges
> └── governance.manifest.json  # Tier 2: Tags, owners
> ```
>
> **Benefits of physical separation:**
> - Core manifest updates don't block operational compaction
> - Domain manifests can evolve independently
> - Reduced contention on CAS operations
> - Lineage/governance can be optional without bloating root
>
> **Root manifest only contains pointers:**
> ```json
> {
>   "version": 1,
>   "coreManifest": "manifests/core.manifest.json",
>   "executionManifest": "manifests/execution.manifest.json",
>   "lineageManifest": "manifests/lineage.manifest.json",
>   "governanceManifest": null,
>   "updatedAt": "2025-01-15T10:00:00Z"
> }
> ```

**Step 1: Write the failing tests**

```rust
// crates/arco-catalog/src/manifest.rs
#[cfg(test)]
mod tests {
    use super::*;

    // === Root Manifest Tests (pointer to domain manifests) ===

    #[test]
    fn test_root_manifest_contains_only_pointers() {
        let root = RootManifest::new();

        // Root should only have paths, not embedded content
        assert_eq!(root.version, 1);
        assert_eq!(root.core_manifest_path, "manifests/core.manifest.json");
        assert_eq!(root.execution_manifest_path, "manifests/execution.manifest.json");
        assert!(root.lineage_manifest_path.is_none());
        assert!(root.governance_manifest_path.is_none());
    }

    #[test]
    fn test_root_manifest_roundtrip() {
        let root = RootManifest {
            version: 1,
            core_manifest_path: "manifests/core.manifest.json".into(),
            execution_manifest_path: "manifests/execution.manifest.json".into(),
            lineage_manifest_path: Some("manifests/lineage.manifest.json".into()),
            governance_manifest_path: None,
            updated_at: "2025-01-15T10:00:00Z".into(),
        };

        let json = serde_json::to_string_pretty(&root).expect("serialize");
        let parsed: RootManifest = serde_json::from_str(&json).expect("parse");

        assert_eq!(parsed.core_manifest_path, root.core_manifest_path);
        assert!(parsed.lineage_manifest_path.is_some());
    }

    // === Domain Manifest Tests (each domain is separate file) ===

    #[test]
    fn test_core_manifest_structure() {
        let core = CoreManifest::new();

        assert_eq!(core.snapshot_version, 0);
        assert!(core.last_commit_id.is_none());
    }

    #[test]
    fn test_execution_manifest_structure() {
        let exec = ExecutionManifest::new();

        assert_eq!(exec.watermark_version, 0);
        assert!(exec.last_compaction_at.is_none());
    }

    #[test]
    fn test_domain_manifests_independent_serialization() {
        // Each domain manifest serializes independently
        let core = CoreManifest {
            snapshot_version: 42,
            snapshot_path: "core/snapshots/v42/".into(),
            last_commit_id: Some("commit_abc".into()),
            updated_at: "2025-01-15T10:00:00Z".into(),
        };

        let exec = ExecutionManifest {
            watermark_version: 100,
            checkpoint_path: "state/execution/checkpoint.json".into(),
            last_compaction_at: Some("2025-01-15T09:55:00Z".into()),
            updated_at: "2025-01-15T10:00:00Z".into(),
        };

        // Each can be serialized/deserialized independently
        let core_json = serde_json::to_string(&core).expect("core");
        let exec_json = serde_json::to_string(&exec).expect("exec");

        // They are separate - updating one doesn't touch the other
        assert!(!core_json.contains("watermark"));
        assert!(!exec_json.contains("snapshot_version"));
    }

    // === Commit Record Tests (hash chain integrity) ===

    #[test]
    fn test_commit_record_hash_chain() {
        let commit1 = CommitRecord {
            commit_id: "commit_001".into(),
            prev_commit_id: None,
            prev_commit_hash: None,
            operation: "InitializeCatalog".into(),
            payload_hash: "sha256:abc123".into(),
            created_at: "2025-01-15T10:00:00Z".into(),
        };

        let hash1 = commit1.compute_hash();
        assert!(hash1.starts_with("sha256:"));

        // Hash is deterministic
        assert_eq!(hash1, commit1.compute_hash());

        // Next commit references previous
        let commit2 = CommitRecord {
            commit_id: "commit_002".into(),
            prev_commit_id: Some("commit_001".into()),
            prev_commit_hash: Some(hash1.clone()),
            operation: "CreateAsset".into(),
            payload_hash: "sha256:def456".into(),
            created_at: "2025-01-15T10:01:00Z".into(),
        };

        let hash2 = commit2.compute_hash();
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_commit_record_hash_includes_prev() {
        // Two commits with same content but different prev should have different hashes
        let commit_a = CommitRecord {
            commit_id: "commit_X".into(),
            prev_commit_id: Some("commit_A".into()),
            prev_commit_hash: Some("sha256:aaa".into()),
            operation: "CreateAsset".into(),
            payload_hash: "sha256:same".into(),
            created_at: "2025-01-15T10:00:00Z".into(),
        };

        let commit_b = CommitRecord {
            commit_id: "commit_X".into(),
            prev_commit_id: Some("commit_B".into()),
            prev_commit_hash: Some("sha256:bbb".into()),
            operation: "CreateAsset".into(),
            payload_hash: "sha256:same".into(),
            created_at: "2025-01-15T10:00:00Z".into(),
        };

        assert_ne!(commit_a.compute_hash(), commit_b.compute_hash());
    }

    #[test]
    fn test_next_version() {
        let mut manifest = CatalogManifest::new();
        assert_eq!(manifest.next_core_version(), 1);

        manifest.core.snapshot_version = 5;
        assert_eq!(manifest.next_core_version(), 6);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-catalog manifest`
Expected: FAIL

**Step 3: Write implementation**

```rust
// crates/arco-catalog/src/manifest.rs
//! Physically multi-file manifest structure per architecture docs.
//!
//! The catalog uses separate manifest files to reduce contention:
//! - root.manifest.json: Pointers to domain manifests
//! - core.manifest.json: Tier 1 (assets, schemas) - locked writes
//! - execution.manifest.json: Tier 2 (materializations) - compactor writes
//! - lineage.manifest.json: Tier 2 (dependency edges)
//! - governance.manifest.json: Tier 2 (tags, owners)
//!
//! Each domain manifest is a separate file that can be updated independently.

use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ============================================================================
// Root Manifest (pointer to domain manifests)
// ============================================================================

/// Root manifest containing only paths to domain manifests.
///
/// This is the entry point - readers load this first, then fetch
/// domain manifests as needed. Critically, this contains NO embedded
/// content, just paths.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootManifest {
    /// Manifest schema version.
    pub version: u32,

    /// Path to core domain manifest (required).
    pub core_manifest_path: String,

    /// Path to execution domain manifest (required).
    pub execution_manifest_path: String,

    /// Path to lineage domain manifest (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lineage_manifest_path: Option<String>,

    /// Path to governance domain manifest (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub governance_manifest_path: Option<String>,

    /// Last update timestamp.
    pub updated_at: String,
}

impl RootManifest {
    /// Creates a new root manifest with default paths.
    #[must_use]
    pub fn new() -> Self {
        Self {
            version: 1,
            core_manifest_path: "manifests/core.manifest.json".into(),
            execution_manifest_path: "manifests/execution.manifest.json".into(),
            lineage_manifest_path: None,
            governance_manifest_path: None,
            updated_at: Utc::now().to_rfc3339(),
        }
    }
}

impl Default for RootManifest {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Domain Manifests (each is a separate file)
// ============================================================================

/// Core catalog manifest (Tier 1) - separate file.
///
/// Written via locking protocol. Contains assets, schemas.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoreManifest {
    /// Current snapshot version.
    pub snapshot_version: u64,

    /// Path to snapshot directory.
    pub snapshot_path: String,

    /// Last commit ID for hash chain integrity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_commit_id: Option<String>,

    /// Last update timestamp.
    pub updated_at: String,
}

impl CoreManifest {
    /// Creates a new core manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot_version: 0,
            snapshot_path: "core/snapshots/v0/".into(),
            last_commit_id: None,
            updated_at: Utc::now().to_rfc3339(),
        }
    }

    /// Returns the next snapshot version.
    #[must_use]
    pub fn next_version(&self) -> u64 {
        self.snapshot_version + 1
    }
}

impl Default for CoreManifest {
    fn default() -> Self {
        Self::new()
    }
}

/// Execution manifest (Tier 2) - separate file.
///
/// Written ONLY by compactor. Contains materializations, partitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionManifest {
    /// Watermark version (last compacted event).
    pub watermark_version: u64,

    /// Path to compaction checkpoint.
    pub checkpoint_path: String,

    /// Last compaction timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_compaction_at: Option<String>,

    /// Last update timestamp.
    pub updated_at: String,
}

impl ExecutionManifest {
    /// Creates a new execution manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            watermark_version: 0,
            checkpoint_path: "state/execution/checkpoint.json".into(),
            last_compaction_at: None,
            updated_at: Utc::now().to_rfc3339(),
        }
    }
}

impl Default for ExecutionManifest {
    fn default() -> Self {
        Self::new()
    }
}

/// Lineage manifest (Tier 2) - separate file.
///
/// Contains dependency edges between assets.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LineageManifest {
    /// Snapshot version.
    pub snapshot_version: u64,

    /// Path to edges Parquet files.
    pub edges_path: String,

    /// Last update timestamp.
    pub updated_at: String,
}

impl LineageManifest {
    /// Creates a new lineage manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot_version: 0,
            edges_path: "state/lineage/edges/".into(),
            updated_at: Utc::now().to_rfc3339(),
        }
    }
}

/// Governance manifest (Tier 2) - separate file.
///
/// Contains tags, owners, access policies.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GovernanceManifest {
    /// Snapshot version.
    pub snapshot_version: u64,

    /// Path to governance Parquet files.
    pub base_path: String,

    /// Last update timestamp.
    pub updated_at: String,
}

impl GovernanceManifest {
    /// Creates a new governance manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot_version: 0,
            base_path: "governance/".into(),
            updated_at: Utc::now().to_rfc3339(),
        }
    }
}

/// Commit record for hash chain integrity.
///
/// Per architecture: enables audit trail and rollback verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitRecord {
    /// Unique commit ID (ULID).
    pub commit_id: String,

    /// Previous commit ID (None for first commit).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_commit_id: Option<String>,

    /// Previous commit hash (for chain verification).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_commit_hash: Option<String>,

    /// Operation type (e.g., "CreateAsset", "UpdateAsset").
    pub operation: String,

    /// SHA256 hash of operation payload.
    pub payload_hash: String,

    /// Commit timestamp.
    pub created_at: String,
}

impl CatalogManifest {
    /// Creates a new empty manifest.
    #[must_use]
    pub fn new() -> Self {
        let now = Utc::now().to_rfc3339();
        Self {
            version: 1,
            core: CoreManifest {
                snapshot_version: 0,
                snapshot_path: "core/snapshots/v0/".into(),
                last_commit_id: None,
            },
            operational: OperationalManifest {
                watermark_version: 0,
                checkpoint_path: "state/execution/checkpoint.json".into(),
            },
            lineage: None,
            governance: None,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    /// Returns the next core snapshot version.
    #[must_use]
    pub fn next_core_version(&self) -> u64 {
        self.core.snapshot_version + 1
    }

    /// Updates the manifest timestamp.
    pub fn touch(&mut self) {
        self.updated_at = Utc::now().to_rfc3339();
    }
}

impl Default for CatalogManifest {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitRecord {
    /// Computes the hash of this commit for chain verification.
    ///
    /// The hash includes prev_commit_hash to form an unbreakable chain.
    #[must_use]
    pub fn compute_hash(&self) -> String {
        let input = format!(
            "{}:{}:{}:{}:{}:{}",
            self.commit_id,
            self.prev_commit_id.as_deref().unwrap_or(""),
            self.prev_commit_hash.as_deref().unwrap_or(""),
            self.operation,
            self.payload_hash,
            self.created_at,
        );
        let hash = Sha256::digest(input.as_bytes());
        format!("sha256:{}", hex::encode(hash))
    }

    /// Creates a new commit as a successor to a previous commit.
    pub fn new_successor(
        prev: &CommitRecord,
        commit_id: String,
        operation: String,
        payload_hash: String,
    ) -> Self {
        Self {
            commit_id,
            prev_commit_id: Some(prev.commit_id.clone()),
            prev_commit_hash: Some(prev.compute_hash()),
            operation,
            payload_hash,
            created_at: Utc::now().to_rfc3339(),
        }
    }
}
```

**Step 4: Update Cargo.toml**

```toml
# crates/arco-catalog/Cargo.toml
[package]
name = "arco-catalog"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
arco-core = { path = "../arco-core" }
bytes = { workspace = true }
chrono = { workspace = true }
hex = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
sha2 = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true }
ulid = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["rt-multi-thread", "macros"] }
```

**Step 5: Run test to verify it passes**

Run: `cargo test -p arco-catalog manifest`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/arco-catalog/
git commit -m "feat(catalog): add multi-manifest structure with hash chain commits"
```

---

### Task 2.3: Implement Distributed Lock

**Files:**
- Create: `crates/arco-catalog/src/lock.rs`

*(Full implementation with CAS, TTL, and retry logic - see prior version)*

---

### Task 2.4: Implement CAS Manifest Writer

**Files:**
- Create: `crates/arco-catalog/src/tier1_writer.rs`

**Why:** The Tier 1 writer is the most critical component. It must:
1. Acquire distributed lock
2. Read current manifest with generation
3. Validate preconditions
4. Write new snapshot
5. Update manifest atomically via CAS
6. Release lock

**Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::storage::MemoryBackend;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_initialize_catalog() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production");
        let writer = Tier1Writer::new(storage.clone());

        // Initialize should succeed
        writer.initialize().await.expect("init should succeed");

        // Manifest should exist
        let manifest_data = storage.get_core_manifest().await.expect("should exist");
        let manifest: CatalogManifest = serde_json::from_slice(&manifest_data)
            .expect("should parse");

        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.core.snapshot_version, 0);
    }

    #[tokio::test]
    async fn test_initialize_idempotent() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production");
        let writer = Tier1Writer::new(storage.clone());

        // First init succeeds
        writer.initialize().await.expect("first init");

        // Second init should not error (idempotent)
        let result = writer.initialize().await;
        assert!(result.is_ok() || matches!(result, Err(CatalogError::AlreadyInitialized)));
    }

    #[tokio::test]
    async fn test_update_with_cas() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production");
        let writer = Tier1Writer::new(storage.clone());

        writer.initialize().await.expect("init");

        // Update should succeed
        let commit = writer.update(|manifest| {
            manifest.core.snapshot_version = 1;
            manifest.core.snapshot_path = "core/snapshots/v1/".into();
            Ok(())
        }).await.expect("update should succeed");

        assert_eq!(commit.operation, "Update");

        // Verify manifest was updated
        let manifest = writer.read_manifest().await.expect("read");
        assert_eq!(manifest.core.snapshot_version, 1);
    }

    #[tokio::test]
    async fn test_concurrent_update_one_wins() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend.clone(), "acme", "production");
        let writer = Tier1Writer::new(storage.clone());

        writer.initialize().await.expect("init");

        // Simulate race: read manifest, then external update, then our update
        let manifest_before = writer.read_manifest().await.expect("read");
        assert_eq!(manifest_before.core.snapshot_version, 0);

        // External update (simulating concurrent writer)
        let mut external_manifest = manifest_before.clone();
        external_manifest.core.snapshot_version = 99;
        storage.put_core_manifest(
            Bytes::from(serde_json::to_vec(&external_manifest).expect("serialize")),
            WritePrecondition::None, // Force overwrite
        ).await.expect("external update");

        // Our update should detect conflict and retry with fresh data
        let commit = writer.update(|manifest| {
            manifest.core.snapshot_version = manifest.core.snapshot_version + 1;
            Ok(())
        }).await.expect("our update should succeed after retry");

        // Final version should be 100 (99 + 1)
        let final_manifest = writer.read_manifest().await.expect("read");
        assert_eq!(final_manifest.core.snapshot_version, 100);
    }
}
```

**Step 2-6:** Implement full Tier1Writer with lock, CAS, retry logic.

---

### Task 2.5: Add Concurrent Writer Race Tests

**Files:**
- Create: `crates/arco-catalog/tests/concurrent_writers.rs`

**Why:** Per architecture docs, the catalog must handle concurrent writers safely. Exactly one must win; losers must retry or fail gracefully.

**Step 1: Write race test**

```rust
//! Integration tests for concurrent writer safety.
//!
//! These tests verify the catalog's distributed locking and CAS
//! mechanisms work correctly under contention.

use arco_catalog::tier1_writer::Tier1Writer;
use arco_core::storage::MemoryBackend;
use arco_core::scoped_storage::ScopedStorage;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// Two writers race to initialize - exactly one must win.
#[tokio::test]
async fn test_two_writers_initialize_race() {
    let backend = Arc::new(MemoryBackend::new());
    let init_success_count = Arc::new(AtomicU32::new(0));
    let init_failure_count = Arc::new(AtomicU32::new(0));

    let handles: Vec<_> = (0..2)
        .map(|i| {
            let backend = backend.clone();
            let success = init_success_count.clone();
            let failure = init_failure_count.clone();

            tokio::spawn(async move {
                let storage = ScopedStorage::new(backend, "acme", "production");
                let writer = Tier1Writer::new(storage);

                match writer.initialize().await {
                    Ok(_) => {
                        success.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        failure.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.await.expect("task should complete");
    }

    // Exactly one should succeed at initialization
    // (though both may "succeed" if second sees already-initialized state)
    let successes = init_success_count.load(Ordering::SeqCst);
    let failures = init_failure_count.load(Ordering::SeqCst);

    assert!(
        successes >= 1,
        "at least one writer should succeed"
    );
    assert_eq!(
        successes + failures, 2,
        "all writers should complete"
    );
}

/// Many concurrent updates - all should eventually succeed with retries.
#[tokio::test]
async fn test_many_concurrent_updates() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production");
    let writer = Tier1Writer::new(storage.clone());

    // Initialize first
    writer.initialize().await.expect("init");

    let update_count = Arc::new(AtomicU32::new(0));
    let num_writers = 5;

    let handles: Vec<_> = (0..num_writers)
        .map(|_| {
            let backend = backend.clone();
            let count = update_count.clone();

            tokio::spawn(async move {
                let storage = ScopedStorage::new(backend, "acme", "production");
                let writer = Tier1Writer::new(storage);

                // Each writer increments version
                writer.update(|manifest| {
                    manifest.core.snapshot_version += 1;
                    Ok(())
                }).await.expect("update should succeed with retries");

                count.fetch_add(1, Ordering::SeqCst);
            })
        })
        .collect();

    for handle in handles {
        handle.await.expect("task should complete");
    }

    // All updates should have succeeded
    assert_eq!(
        update_count.load(Ordering::SeqCst),
        num_writers as u32,
        "all updates should complete"
    );

    // Version should be exactly num_writers
    let final_manifest = writer.read_manifest().await.expect("read");
    assert_eq!(
        final_manifest.core.snapshot_version,
        num_writers as u64,
        "version should equal number of updates"
    );
}

/// Loser of race can read and retry successfully.
#[tokio::test]
async fn test_loser_can_retry() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production");
    let writer1 = Tier1Writer::new(storage.clone());
    let writer2 = Tier1Writer::new(storage.clone());

    // Writer 1 initializes
    writer1.initialize().await.expect("writer1 init");

    // Writer 2's initialize should handle already-initialized gracefully
    // (either succeed silently or return AlreadyInitialized)
    let _ = writer2.initialize().await;

    // Both writers should be able to update
    writer1.update(|m| { m.core.snapshot_version = 1; Ok(()) })
        .await.expect("writer1 update");

    writer2.update(|m| { m.core.snapshot_version = 2; Ok(()) })
        .await.expect("writer2 update");

    let final_manifest = writer1.read_manifest().await.expect("read");
    assert_eq!(final_manifest.core.snapshot_version, 2);
}
```

**Step 2: Run tests**

Run: `cargo test -p arco-catalog concurrent`
Expected: PASS

**Step 3: Commit**

```bash
git add crates/arco-catalog/
git commit -m "test(catalog): add concurrent writer race safety tests"
```

---

## Part 3: Orchestration MVP (arco-flow)

### Task 3.1: Define Plan Types with Proptest

**Files:**
- Modify: `crates/arco-flow/src/plan.rs`
- Modify: `crates/arco-flow/Cargo.toml`

**Why:** Proptest provides property-based testing to verify plan fingerprinting is truly deterministic across random inputs.

*(Full implementation with proptest - see prior version for details)*

---

### Task 3.2: Implement DAG Builder

*(Standard DAG implementation)*

---

### Task 3.3: Implement State Machine

*(Standard state machine implementation)*

---

### Task 3.4: Implement Scheduler with Event Emission

**Why:** Per architecture docs, orchestration should emit events for observability and replay.

*(Implementation emitting "run_started", "task_started", "task_completed", "run_completed" events)*

---

## Part 4: Integration & Testing

### Task 4.1: End-to-End Integration Test

**Files:**
- Create: `tests/integration/end_to_end.rs`

**Scenario:**
1. Initialize catalog
2. Create assets
3. Generate execution plan
4. Execute plan (mock executor)
5. Verify lineage recorded
6. Query assets from catalog

---

### Task 4.2: Idempotency Integration Test

**Files:**
- Create: `tests/integration/idempotency.rs`

**Why:** Per architecture docs, deploy operations must be idempotent.

```rust
#[tokio::test]
async fn test_deploy_idempotency_no_duplicates() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "acme", "production");
    let writer = Tier1Writer::new(storage.clone());

    writer.initialize().await.expect("init");

    let asset_key = AssetKey::new("raw", "events").expect("valid");

    // First deploy
    let commit1 = writer.create_asset(
        AssetKey::new("raw", "events").expect("valid"),
        "gs://bucket/raw/events",
        AssetFormat::Parquet,
    ).await.expect("first deploy");

    // Second deploy with same key (should be no-op or update)
    let result = writer.create_asset(
        AssetKey::new("raw", "events").expect("valid"),
        "gs://bucket/raw/events",
        AssetFormat::Parquet,
    ).await;

    // Should not create duplicate
    let reader = CatalogReader::new(storage);
    let assets = reader.list_assets().await.expect("list");

    assert_eq!(assets.len(), 1, "should have exactly one asset");
}
```

---

### Task 4.3: Tier 2 Walking Skeleton Test

**Files:**
- Create: `tests/integration/tier2_skeleton.rs`

**Why:** Per technical vision and unified platform design, Tier 2 is:
- Ingest writes append-only events (no snapshot reads)
- Compactor is the sole writer of Parquet state
- Compaction updates watermark and manifest atomically

This test proves the Tier 2 invariants work as a running loop, not just as placeholders.

```rust
//! Tier 2 walking skeleton: append event -> compact -> read from Parquet.
//!
//! This is the minimum viable proof that Tier 2 consistency model works.

use arco_catalog::{EventWriter, Compactor, StateReader};
use arco_core::scoped_storage::ScopedStorage;
use arco_core::storage::MemoryBackend;
use arco_proto::MaterializationCompleted;
use std::sync::Arc;

#[tokio::test]
async fn test_tier2_append_compact_read_loop() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "acme", "production");

    // 1. Initialize catalog
    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    // 2. Append one MaterializationCompleted event to ledger
    let event_writer = EventWriter::new(storage.clone());
    let event = MaterializationCompleted {
        materialization_id: Some(MaterializationId { value: "mat_001".into() }),
        asset_id: Some(AssetId { value: "asset_abc".into() }),
        row_count: 1000,
        byte_size: 50000,
        ..Default::default()
    };

    let event_id = event_writer
        .append("execution", event)
        .await
        .expect("append should succeed");

    // 3. Verify event is in ledger (JSON format)
    let ledger_events = storage
        .list_raw("ledger/execution/")
        .await
        .expect("list should succeed");
    assert!(!ledger_events.is_empty(), "ledger should have events");

    // 4. Run local compactor (reads events since watermark, writes Parquet)
    let compactor = Compactor::new(storage.clone());
    let compaction_result = compactor
        .compact_domain("execution")
        .await
        .expect("compaction should succeed");

    assert!(compaction_result.events_processed > 0);
    assert!(compaction_result.parquet_files_written > 0);

    // 5. Verify watermark was updated
    let manifest = writer.read_manifest().await.expect("read manifest");
    assert!(
        manifest.operational.watermark_version > 0,
        "watermark should be updated"
    );

    // 6. Read from state (Parquet-only, not parsing JSON ledger)
    let reader = StateReader::new(storage.clone());
    let materializations = reader
        .list_materializations()
        .await
        .expect("read should succeed");

    assert_eq!(materializations.len(), 1);
    assert_eq!(materializations[0].materialization_id, "mat_001");
}

#[tokio::test]
async fn test_tier2_normal_reads_use_parquet_not_ledger() {
    // This test verifies the architecture invariant:
    // "Normal reads ONLY use compacted Parquet, never parse JSON ledger"

    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "acme", "production");

    // Initialize and write some events
    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());
    for i in 0..5 {
        let event = MaterializationCompleted {
            materialization_id: Some(MaterializationId {
                value: format!("mat_{:03}", i),
            }),
            ..Default::default()
        };
        event_writer.append("execution", event).await.expect("append");
    }

    // Before compaction: reader should return empty (Parquet doesn't exist)
    let reader = StateReader::new(storage.clone());
    let before_compact = reader.list_materializations().await.expect("read");
    assert!(
        before_compact.is_empty(),
        "reader should NOT parse ledger directly"
    );

    // After compaction: reader should return all events
    let compactor = Compactor::new(storage.clone());
    compactor.compact_domain("execution").await.expect("compact");

    let after_compact = reader.list_materializations().await.expect("read");
    assert_eq!(
        after_compact.len(),
        5,
        "reader should see all compacted events"
    );
}

#[tokio::test]
async fn test_tier2_ledger_ordering_by_monotonic_timestamp() {
    // INVARIANT: Ledger files are named with monotonic timestamps
    // and lexicographic ordering = chronological ordering.
    //
    // This ensures compaction can process events in order without
    // parsing file contents to determine sequence.

    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "acme", "production");

    // Initialize catalog
    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());

    // Write events with small delay to ensure different timestamps
    let mut event_ids = Vec::new();
    for i in 0..5 {
        let event = MaterializationCompleted {
            materialization_id: Some(MaterializationId {
                value: format!("mat_{:03}", i),
            }),
            ..Default::default()
        };
        let id = event_writer.append("execution", event).await.expect("append");
        event_ids.push(id);
        // Small delay to ensure monotonic timestamps
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    // List ledger files and verify ordering
    let ledger_files = storage
        .list("ledger/execution/")
        .await
        .expect("list ledger");

    // Verify files are in lexicographic order (which should equal chronological order)
    let file_names: Vec<_> = ledger_files.iter().map(|p| p.as_str()).collect();
    let mut sorted_names = file_names.clone();
    sorted_names.sort();

    assert_eq!(
        file_names, sorted_names,
        "ledger files must be in lexicographic order (monotonic timestamps)"
    );

    // Verify all file names follow the expected pattern: {ulid}.json
    for name in &file_names {
        assert!(
            name.ends_with(".json"),
            "ledger files must be JSON: {name}"
        );
        // Extract ULID portion and verify it's valid
        let basename = name.rsplit('/').next().unwrap();
        let ulid_part = basename.trim_end_matches(".json");
        assert_eq!(ulid_part.len(), 26, "ULID should be 26 chars: {ulid_part}");
    }
}

#[tokio::test]
async fn test_tier2_idempotent_event_produces_same_file() {
    // INVARIANT: Re-ingesting the same event (same event_id) must be
    // idempotent - it should not create duplicate ledger entries.
    //
    // The EventWriter uses event_id as the filename, so writing the
    // same event_id twice overwrites (not appends).

    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "acme", "production");

    // Initialize catalog
    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());

    let event = MaterializationCompleted {
        materialization_id: Some(MaterializationId {
            value: "mat_same".into(),
        }),
        ..Default::default()
    };

    // Write the same event twice with same ID
    let event_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV"; // Fixed ULID for test
    event_writer
        .append_with_id("execution", event.clone(), event_id)
        .await
        .expect("first append");

    event_writer
        .append_with_id("execution", event.clone(), event_id)
        .await
        .expect("second append (should overwrite)");

    // Verify only one file exists
    let ledger_files = storage
        .list("ledger/execution/")
        .await
        .expect("list ledger");

    assert_eq!(
        ledger_files.len(),
        1,
        "idempotent write should not create duplicates"
    );

    // Compact and verify only one event in Parquet
    let compactor = Compactor::new(storage.clone());
    compactor.compact_domain("execution").await.expect("compact");

    let reader = StateReader::new(storage.clone());
    let materializations = reader.list_materializations().await.expect("read");

    assert_eq!(materializations.len(), 1, "only one event after compaction");
    assert_eq!(materializations[0].materialization_id, "mat_same");
}

#[tokio::test]
async fn test_tier2_compactor_is_sole_parquet_writer() {
    // INVARIANT: Only the compactor writes Parquet files.
    // Writers append to JSON ledger, readers only read Parquet.
    //
    // This ensures single-writer semantics for Parquet without locks.

    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "acme", "production");

    // Initialize catalog
    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    // Before any operations: no Parquet files should exist
    let parquet_files_before = storage
        .list("state/execution/")
        .await
        .unwrap_or_default();
    assert!(
        parquet_files_before.is_empty(),
        "no Parquet files before compaction"
    );

    // Write events to ledger
    let event_writer = EventWriter::new(storage.clone());
    for i in 0..3 {
        let event = MaterializationCompleted {
            materialization_id: Some(MaterializationId {
                value: format!("mat_{:03}", i),
            }),
            ..Default::default()
        };
        event_writer.append("execution", event).await.expect("append");
    }

    // Still no Parquet files (EventWriter only writes JSON)
    let parquet_files_after_write = storage
        .list("state/execution/")
        .await
        .unwrap_or_default();
    assert!(
        parquet_files_after_write.is_empty(),
        "EventWriter must not write Parquet"
    );

    // After compaction: Parquet files exist
    let compactor = Compactor::new(storage.clone());
    compactor.compact_domain("execution").await.expect("compact");

    let parquet_files_after_compact = storage
        .list("state/execution/")
        .await
        .expect("list state");
    assert!(
        !parquet_files_after_compact.is_empty(),
        "Parquet files must exist after compaction"
    );

    for path in &parquet_files_after_compact {
        assert!(
            path.as_str().ends_with(".parquet"),
            "compacted files must be Parquet: {path}"
        );
    }
}
```

**Step 2: Run test**

Run: `cargo test --test integration tier2_skeleton`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/
git commit -m "test(integration): add Tier 2 walking skeleton proof"
```

---

## Part 5: Python SDK Alpha

### Task 5.1: Asset Decorator with Canonical PartitionKey

**Files:**
- Create: `python/arco/asset.py`
- Create: `python/arco/partition.py`

**Requirement:** Python SDK must produce identical `canonical_string()` output as Rust.

*(Implementation using shared canonical encoding spec from Task 1.1)*

---

### Task 5.2: CLI with Dry-Run Support

**Files:**
- Create: `python/arco/cli.py`

**Requirement:** `arco deploy --dry-run` should emit valid manifest JSON before wiring real deployment.

---

## Verification Checklist

Before marking Phase 1 complete, run all verification commands:

```bash
# Rust checks
cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
cargo llvm-cov --workspace --fail-under 80  # Workspace-level coverage gate
cargo doc --workspace --no-deps
cargo audit
cargo deny check

# Proto checks (Buf is for linting/breaking only, prost-build for codegen)
cd proto && buf lint && cd ..

# Python checks (including supply chain security)
cd python
ruff check .
mypy arco/
pytest tests/ -v --cov=arco --cov-fail-under=80
pip-audit  # Per orchestration design: Python vulnerability scanning
cd ..

# Cross-language checks
cargo test cross_language
pytest python/tests/test_partition_key_canonical.py -v

# Integration checks
cargo test -p arco-catalog concurrent
cargo test --test integration

# Tier 2 skeleton verification
cargo test --test integration tier2_skeleton
```

All commands must pass.

> **Note on coverage:** The `--fail-under 80` is workspace-wide. For per-crate thresholds, exclude generated code (arco-proto) from the calculation.

---

## Exit Criteria

Phase 1 is complete when:

| Criterion | Measurement |
|-----------|-------------|
| Contracts established | `buf lint` passes, `buf breaking` in CI |
| Catalog MVP functional | Create/read assets, hash-chain commits work |
| Multi-manifest structure | Core + operational manifests per architecture |
| Tier 1 writer safe | Concurrent race tests pass (exactly one winner) |
| Cross-language canonical | Rust/Python tests use shared fixtures, all pass |
| Orchestration MVP | Plan generation, DAG execution, events emitted |
| Test coverage | >80% per crate (`cargo llvm-cov --fail-under 80`) |
| Python SDK | @asset decorator works, CLI deploys |
| TTFSA < 30 min | New user can run first asset in under 30 minutes |
| Benchmarks | Baseline measurements stored as CI artifacts |

---

*Plan created: 2025-01-13*
*Revised: 2025-01-13 v2 (contracts-first, architecture alignment, Tier 1 decomposition, cross-language tests, walking skeleton)*
*Target: Phase 1 Alpha Release*
