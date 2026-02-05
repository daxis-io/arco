# Arco Unified Platform Architecture
## Servo + Catalog Integration Design

**Status:** Draft
**Date:** 2025-01-12
**Authors:** Architecture Team

---

## Executive Summary

This document defines the end-to-end architecture for Arco, a unified data platform where **Servo** (orchestration) and **Catalog** (data product hub) form a single coherent system. The design ensures:

- **$0 at rest**: Pure Parquet-on-GCS architecture—no Postgres, no managed databases
- **Asset-first mental model**: Data products are the central concept
- **Deterministic planning**: Same inputs produce the same execution plan
- **Event-sourced truth**: All state is derived from immutable events (stored as Parquet)
- **Multi-tenant isolation**: Complete separation at every layer via GCS path prefixes
- **Quality as enforcement**: Checks gate downstream; quarantine is first-class
- **Observable by default**: Traces, metrics, and audit events end-to-end
- **Browser-direct reads**: DuckDB-WASM queries Parquet directly from GCS via signed URLs

**Integration Pattern**: Synchronous per-tenant writes with eventual consistency for reads. Servo writes directly to tenant GCS paths; Catalog reads from the same paths. No outbox, no message queue—single source of truth in GCS.

---

## Table of Contents

- [Section A: Glossary + Unified Mental Model](#section-a-glossary--unified-mental-model)
- [Section B: Source-of-Truth Matrix](#section-b-source-of-truth-matrix)
- [Section C: End-to-End Architecture](#section-c-end-to-end-architecture)
- [Section D: Integration Contracts](#section-d-integration-contracts)
- [Section E: Catalog Model for Orchestration](#section-e-catalog-model-for-orchestration)
- [Section F: Deterministic Planning Using Catalog](#section-f-deterministic-planning-using-catalog)
- [Section G: Multi-Tenancy, Security, and Governance](#section-g-multi-tenancy-security-and-governance)
- [Section H: Observability + Explainability](#section-h-observability--explainability)
- [Section I: Reliability + Consistency Guarantees](#section-i-reliability--consistency-guarantees)
- [Section J: MVP Cut + Roadmap](#section-j-mvp-cut--roadmap)
- [Section K: Open Questions + Risks](#section-k-open-questions--risks)

---

## Section A: Glossary + Unified Mental Model

### A.1 Core Terms

| Term | Definition | System of Record |
|------|------------|------------------|
| **Tenant** | Isolated customer account. All data is tenant-scoped. No cross-tenant access. | Both (shared identity) |
| **Workspace** | Logical environment within a tenant (dev, staging, prod). | Servo |
| **Asset** | A data product definition—code + schema + dependencies + checks. The unit of orchestration and cataloging. | Servo (definition), Catalog (metadata) |
| **AssetKey** | Canonical identifier: `{namespace}.{name}` (e.g., `analytics.orders_daily`). | Both (shared) |
| **AssetId** | Stable ULID assigned at first deploy. Survives renames. | Both (shared) |
| **Dataset** | The physical data an asset produces. One asset → one dataset (1:1). | Catalog |
| **Partition** | A subset of a dataset defined by partition key dimensions (e.g., `{date: "2025-01-15", tenant: "acme"}`). | Both |
| **PartitionId** | Stable identifier: `hash(asset_id + canonical_partition_key)`. | Both (shared) |
| **Materialization** | A specific version of a partition's data. Immutable. Points to files on storage. | Catalog (pointer), Servo (provenance) |
| **MaterializationId** | Unique identifier for a materialization event (ULID). | Servo (generated) |
| **Version** | A pointer to a specific materialization of a partition. Partitions have version history. | Catalog |
| **VersionId** | Sequential identifier within a partition (e.g., `v_00042`). | Catalog |
| **Code Version** | A deployed revision of asset definitions. Tied to git SHA or artifact digest. | Servo |
| **Run** | A single execution of a plan. Contains multiple tasks. | Servo |
| **RunId** | Unique identifier for a run (ULID). | Servo |
| **Task** | A unit of work within a run—materializing one partition of one asset. | Servo |
| **TaskId** | Unique identifier for a task within a run. | Servo |
| **Plan** | A deterministic execution graph. Same inputs → same plan. First-class artifact. | Servo |
| **PlanId** | Unique identifier; includes fingerprint of spec for equality checking. | Servo |
| **Snapshot** | A point-in-time view of catalog state. Immutable. Hash-chained. | Catalog |
| **SnapshotId** | Unique identifier for a catalog snapshot. | Catalog |
| **Contract** | Schema and quality expectations on an asset. Enforced at deploy and runtime. | Catalog (Governance Domain) |
| **Check** | A quality assertion (row count, nulls, freshness, custom SQL). | Servo (execution), Catalog (results) |
| **Lineage Edge** | A dependency between assets (A → B means A feeds B). | Catalog (derived from Servo events) |
| **SLA** | Freshness or availability expectation on an asset. | Catalog (Governance Domain) |

### A.2 Term Mapping: Servo ↔ Catalog

```
┌─────────────────────────────────────────────────────────────────┐
│                    UNIFIED MENTAL MODEL                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   USER PERSPECTIVE (Data Product)                                │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │  "orders_daily" asset                                    │   │
│   │    - Defined in code (Python SDK)                        │   │
│   │    - Has schema, dependencies, checks                    │   │
│   │    - Produces partitioned data                           │   │
│   │    - Has owners, tags, SLAs                              │   │
│   └─────────────────────────────────────────────────────────┘   │
│                              │                                   │
│              ┌───────────────┴───────────────┐                  │
│              ▼                               ▼                   │
│   ┌─────────────────────┐       ┌─────────────────────────┐     │
│   │       SERVO         │       │        CATALOG          │     │
│   │   (Orchestration)   │       │    (Data Product Hub)   │     │
│   ├─────────────────────┤       ├─────────────────────────┤     │
│   │ AssetDefinition     │──────►│ Asset (metadata)        │     │
│   │ PartitionKey        │──────►│ Partition (versions)    │     │
│   │ Materialization     │──────►│ MaterializationRef      │     │
│   │ Run/Task            │──────►│ Execution history       │     │
│   │ CheckResult         │──────►│ Quality results         │     │
│   │ TaskInputs/Outputs  │──────►│ Lineage edges           │     │
│   └─────────────────────┘       └─────────────────────────┘     │
│                                                                  │
│   Servo WRITES execution facts → Catalog PROJECTS views         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### A.3 Identity Rules

```rust
/// AssetKey: namespace.name (case-sensitive, validated)
pub struct AssetKey {
    pub namespace: String,  // ^[a-z][a-z0-9_]*$
    pub name: String,       // ^[a-z][a-z0-9_]*$
}

impl AssetKey {
    pub fn canonical(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }
}

/// AssetId: Stable ULID, assigned once at first deploy, never changes
/// Survives renames of AssetKey
pub struct AssetId(Ulid);

impl AssetId {
    pub fn new() -> Self {
        Self(Ulid::new())  // Time-ordered for indexing
    }
}

/// PartitionId: Derived deterministically from asset + partition key
/// Stable across re-materializations of the same partition
pub struct PartitionId(String);

impl PartitionId {
    pub fn new(asset_id: &AssetId, partition_key: &PartitionKey) -> Self {
        let canonical_key = partition_key.canonical_string();
        let hash = sha256(&format!("{}:{}", asset_id.0, canonical_key));
        Self(format!("part_{}", &hash[..16]))
    }
}

/// PartitionKey: Multi-dimensional, canonically ordered by key name
pub struct PartitionKey(BTreeMap<String, ScalarValue>);

impl PartitionKey {
    /// CRITICAL: Canonical encoding for partition keys
    ///
    /// This encoding MUST be stable across:
    /// - Different languages (Python SDK, Rust, TypeScript)
    /// - Different versions of the same language
    /// - Serialization round-trips
    ///
    /// Format: type-tagged canonical JSON with stable ordering
    pub fn canonical_string(&self) -> String {
        // Use canonical JSON with explicit type tags
        let entries: Vec<String> = self.0.iter()
            .map(|(k, v)| format!("{}={}", k, v.canonical_repr()))
            .collect();
        entries.join(",")
    }
}

impl ScalarValue {
    /// Canonical representation for each scalar type
    /// These rules prevent "ghost partitions" from encoding drift
    pub fn canonical_repr(&self) -> String {
        match self {
            // Strings: JSON-escaped, quoted
            ScalarValue::String(s) => format!("s:{}", json_escape(s)),

            // Integers: decimal, no leading zeros
            ScalarValue::Int64(n) => format!("i:{}", n),
            ScalarValue::Int32(n) => format!("i:{}", n),

            // Floats: PROHIBITED in partition keys (precision issues)
            ScalarValue::Float64(_) => panic!("Float partition keys are not allowed"),

            // Booleans: lowercase
            ScalarValue::Boolean(b) => format!("b:{}", if *b { "true" } else { "false" }),

            // Dates: ISO 8601, date only (YYYY-MM-DD)
            ScalarValue::Date(d) => format!("d:{}", d.format("%Y-%m-%d")),

            // Timestamps: ISO 8601 with microseconds, UTC, Z suffix
            ScalarValue::Timestamp(ts) => format!("t:{}", ts.format("%Y-%m-%dT%H:%M:%S%.6fZ")),

            // Null: explicit
            ScalarValue::Null => "n:null".to_string(),
        }
    }
}

// Example canonical partition keys:
// { "date": Date(2025-01-15), "region": "us-east" }
// → "date=d:2025-01-15,region=s:us-east"
//
// { "year": 2025, "month": 1 }
// → "month=i:1,year=i:2025"  (sorted by key name)

/// MaterializationId: Unique per materialization event
/// Generated by Servo, used as idempotency key in Catalog
pub struct MaterializationId(Ulid);

/// VersionId: Catalog-specific, sequential within a partition
pub struct VersionId(String);  // e.g., "v_00042"

/// EdgeId: Derived from source + target + dependency semantics
/// Stable for deduplication
pub struct EdgeId(String);

impl EdgeId {
    pub fn new(
        source: &AssetId,
        target: &AssetId,
        fingerprint: &DependencyFingerprint
    ) -> Self {
        let hash = sha256(&format!("{}:{}:{}", source, target, fingerprint));
        Self(format!("edge_{}", &hash[..16]))
    }
}

/// DependencyFingerprint: Captures semantic identity of a dependency
/// Includes partition mapping and transform to avoid false deduplication
pub struct DependencyFingerprint(String);

impl DependencyFingerprint {
    pub fn new(
        source: &AssetId,
        target: &AssetId,
        partition_mapping: &PartitionMapping,
        transform_fingerprint: &TransformFingerprint,
        parameter_name: &str,
    ) -> Self {
        let hash = sha256(&format!(
            "{}:{}:{}:{}:{}",
            source, target, partition_mapping.canonical(),
            transform_fingerprint, parameter_name
        ));
        Self(hash)
    }
}
```

---

## Section B: Source-of-Truth Matrix

### B.1 Ownership Map

| Entity / Field | Owner | Write Path | Read Path | Consistency | Evolution Strategy |
|----------------|-------|------------|-----------|-------------|-------------------|
| **Asset definition** (code, deps, checks) | Servo | SDK deploy → Servo API | Servo API, Catalog sync | Strong | Schema versioning |
| **Asset metadata** (id, key, schema version) | Servo | Deploy event | Catalog projection | Eventual (< 5s) | Additive fields |
| **Owners, tags, description** | Catalog (Governance) | Admin API / UI | Catalog API | Strong | Merge semantics |
| **Data contracts** | Catalog (Governance) | Admin API | Servo (deploy validation) | Strong | Contract versioning |
| **Schema versions** | Both | Servo (infers), Catalog (registers) | Catalog API | Strong | Compatibility rules |
| **Partition strategy** | Servo | Asset definition | Catalog sync | Eventual | Breaking = new asset |
| **Code version / artifact** | Servo | Deploy | Servo API | Strong | Immutable artifacts |
| **Run state** | Servo | Scheduler/Workers | Servo API, Catalog sync | Strong (Servo) | Event replay |
| **Task state, retries, heartbeats** | Servo | Workers | Servo API | Strong | Projection rebuild |
| **Materialization records** | Both | Servo (creates) → Catalog (stores) | Catalog API | Eventual (< 5s) | Append-only |
| **Partition versions** | Catalog | Servo sync → Catalog | Catalog API | Eventual | Version history |
| **File locations (URIs)** | Catalog | Servo sync | Catalog API, Query engines | Eventual (< 5s) | Immutable per version |
| **Lineage edges** | Catalog | Derived from Servo events | Catalog API | Eventual (< 60s) | Projection rebuild |
| **Column lineage** | Catalog | Derived / annotated | Catalog API | Eventual | Additive |
| **Quality results** | Both | Servo (executes) → Catalog | Catalog API | Eventual (< 30s) | Append-only |
| **Gating decisions** | Servo | Check execution | Catalog sync | Eventual | Event log |
| **Quarantine status** | Both | Servo (sets) → Catalog | Both | Eventual | State machine |
| **SLA definitions** | Catalog (Governance) | Admin API | Servo (planning) | Strong | Versioned |
| **Observed freshness** | Catalog | Computed from materializations | Catalog API | Eventual | Derived view |
| **Health status** | Catalog | Computed | Catalog API | Eventual | Derived view |

### B.2 Storage Architecture (Two-Tier Write Model)

The storage architecture uses a **two-tier write model**:

1. **Strong writes** (DDL-like ops): Direct commit protocol with conditional writes for core catalog operations
2. **Event-log writes** (high-frequency): Append-only log directory + serverless compactor for operational metadata

#### Why Two Tiers?

- **Object storage is fast at writing lots of small objects concurrently**—not at in-place updates
- **Core catalog ops** (tables/namespaces/schema/contracts) need strong consistency—use direct commit
- **High-frequency operational metadata** (partitions/quality/executions/lineage) can tolerate seconds of lag—use event log → compactor

#### Key Insight: "Append-Only Log" = Directory of Immutable Files

**Important**: Object storage doesn't support multiple writers appending to the same file. So our "append-only log" is actually a **directory of immutable event files**:

```
ledger/execution/2025-01-15/
  2025-01-15T10:00:00Z-servo-run-abc123.json    # Writer 1
  2025-01-15T10:00:05Z-servo-run-def456.json    # Writer 2
  2025-01-15T10:00:07Z-profiler-xyz789.json     # Writer 3
```

**Pattern**:
- Each writer creates its own new object with a unique name (`{timestamp}-{source}-{uuid}.json`)
- The compactor treats the entire prefix as a logical log
- Events are ordered by timestamp (with tie-breaking on event ID)
- This gives us "log semantics" without any coordination between writers

**Conceptual Model**:
```
catalog_state = fold(core_commits) + fold(events_since_last_snapshot)
```

#### Storage Layout

```
gs://bucket/{tenant}/{workspace}/
├── manifests/                         # Per-domain manifests (low churn)
│   ├── root.manifest.json             # Points to latest of each domain
│   ├── catalog_core.manifest.json     # Assets, schemas, contracts
│   ├── execution.manifest.json        # Materializations, partitions
│   ├── quality.manifest.json          # Check results
│   └── lineage.manifest.json          # Edges, executions
│
├── ledger/                            # Append-only event segments
│   ├── catalog/                       # Core catalog events (low vol)
│   │   └── {ulid}.json                # TableCreated, SchemaChanged
│   ├── execution/                     # Materialization events (high vol)
│   │   └── {date}/{timestamp}-{uuid}.json    # MaterializationCompleted
│   ├── quality/                       # Check result events
│   │   └── {date}/{timestamp}-{uuid}.json    # CheckExecuted
│   └── lineage/                       # Lineage events
│       └── {date}/{timestamp}-{uuid}.json    # LineageRecorded
│
├── state/                             # Compacted Parquet (read-optimized)
│   ├── catalog/                       # Core catalog state
│   │   ├── assets/
│   │   │   └── ns_hash={h}/assets.parquet
│   │   └── schemas/
│   │       └── asset_hash={h}/schemas.parquet
│   ├── execution/                     # Execution state
│   │   ├── materializations/
│   │   │   └── date={d}/asset_hash={h}/data.parquet
│   │   └── partitions/
│   │       └── asset_hash={h}/partitions.parquet
│   ├── quality/                       # Quality results
│   │   └── date={d}/asset_hash={h}/results.parquet
│   └── lineage/                       # Lineage state
│       ├── edges/
│       │   └── src_hash={h}/edges.parquet
│       └── executions/
│           └── year={y}/month={m}/data.parquet
│
├── governance/                        # Governance overlay
│   ├── tags.parquet                   # Tags (code + UI sources)
│   ├── owners.parquet                 # Owners (code + UI sources)
│   └── slas.parquet                   # SLA definitions
│
└── data/                              # User data (materialized)
    └── {namespace}/{asset}/
        └── {partition_key}/
            └── {materialization_id}/  # Version by mat_id (not sequential)
                └── part-{n}.parquet
```

#### Write Path: Ledger + Compactor (LSM-style)

| Tier | Operations | Volume | Write Path | Consistency |
|------|------------|--------|------------|-------------|
| **Core Catalog** | TableCreated, SchemaChanged, ContractUpdated | ~10–100/day | Ledger → immediate compaction → manifest publish | Strong |
| **Execution** | MaterializationCompleted | ~1K–100K/day | Ledger → batch compaction (~10s) → manifest publish | Eventual (< 10s) |
| **Quality** | CheckExecuted | ~1K–10K/day | Ledger → batch compaction (~10s) → manifest publish | Eventual (< 10s) |
| **Lineage** | LineageRecorded | ~100–10K/day | Ledger → batch compaction (~10s) → manifest publish | Eventual (< 10s) |

**Tier 1: Core Catalog (Strong Consistency)**

```
1. Append event to ledger/catalog/{ulid}.json
2. Compactor runs immediately (or < 1s microbatch)
3. Update catalog_core.manifest.json (CAS on generation)
4. ACK to writer after manifest publish
```

**Tier 2: Execution / Quality / Lineage (Eventual)**

```
1. Writer appends to ledger/{domain}/{date}/{timestamp}-{uuid}.json
2. ACK immediately (fire-and-forget)
3. Compactor wakes every ~10s (GCS notification / timer)
4. Compactor: list events since watermark → sort → fold → write Parquet
5. Update {domain}.manifest.json (CAS on generation)
6. Move processed events to ledger/{domain}/processed/
```

#### Event Schema (JSON)

All events share a common envelope with domain-specific payloads:

```typescript
// Event envelope (all events)
interface CatalogEvent {
  event_id: string;           // ULID - globally unique
  event_type: string;         // e.g., "MaterializationCompleted"
  event_version: number;      // Schema version for evolution
  timestamp: string;          // ISO 8601
  source: string;             // e.g., "servo", "ui", "profiler"
  tenant_id: string;
  workspace_id: string;

  // Idempotency - events must be either:
  // - Uniquely identifiable (event_id) so duplicates can be ignored
  // - Or commutative (set status, increment counter)
  idempotency_key: string;    // Usually same as event_id

  payload: EventPayload;
}

// Execution domain events
interface MaterializationCompleted {
  materialization_id: string;   // ULID - the version identity
  asset_id: string;
  partition_key: Record<string, string>;
  run_id: string;
  task_id: string;
  files: FileEntry[];
  row_count: number;
  byte_size: number;
  schema_hash: string;
  started_at: string;
  completed_at: string;
}

// Quality domain events
interface CheckExecuted {
  check_id: string;
  asset_id: string;
  partition_id: string;
  materialization_id: string;
  check_name: string;
  check_type: string;           // "row_count", "not_null", "freshness", "custom"
  passed: boolean;
  severity: "info" | "warning" | "error" | "critical";
  expected_value?: string;
  actual_value?: string;
  message: string;
  gating_action: "continue" | "warn" | "block" | "quarantine";
}

// Lineage domain events
interface LineageRecorded {
  run_id: string;
  task_id: string;
  edges: LineageEdge[];
}

interface LineageEdge {
  source_asset_id: string;
  target_asset_id: string;
  source_partitions: string[];    // Partition IDs consumed
  target_partitions: string[];    // Partition IDs produced
  dependency_fingerprint: string; // For deduplication
}
```

#### Compactor Logic (Serverless Worker)

The compactor is a Cloud Run / Cloud Function triggered by GCS notifications or a timer:

```typescript
// Compactor pseudocode
async function compactDomain(tenant: string, workspace: string, domain: string) {
  // 1. Read watermark (last processed position)
  const watermark = await readWatermark(tenant, workspace, domain);

  // 2. List all events since watermark
  const eventFiles = await listObjects(
    `gs://bucket/${tenant}/${workspace}/ledger/${domain}/`,
    { after: watermark.lastProcessedFile }
  );

  if (eventFiles.length === 0) return; // Nothing to do

  // 3. Sort by timestamp (filename prefix ensures lexicographic = chronological)
  eventFiles.sort((a, b) => a.name.localeCompare(b.name));

  // 4. Load and parse events
  const events: CatalogEvent[] = [];
  for (const file of eventFiles) {
    const content = await readObject(file.path);
    const event = JSON.parse(content);

    // Deduplicate by idempotency_key
    if (!seenKeys.has(event.idempotency_key)) {
      events.push(event);
      seenKeys.add(event.idempotency_key);
    }
  }

  // 5. Load current state snapshot
  const currentState = await loadCurrentSnapshot(tenant, workspace, domain);

  // 6. Fold events into state
  for (const event of events) {
    applyEvent(currentState, event);
  }

  // 7. Write updated Parquet files
  await writeParquetState(tenant, workspace, domain, currentState);

  // 8. Update manifest (CAS with generation match)
  await updateManifest(tenant, workspace, domain, {
    snapshotId: newSnapshotId,
    generation: currentGeneration + 1,
  });

  // 9. Update watermark
  await writeWatermark(tenant, workspace, domain, {
    lastProcessedFile: eventFiles[eventFiles.length - 1].name,
    lastProcessedAt: new Date().toISOString(),
    eventsProcessed: events.length,
  });

  // 10. Move processed events to archive (optional, for audit)
  for (const file of eventFiles) {
    await moveObject(file.path, file.path.replace('/ledger/', '/ledger_processed/'));
  }
}
```

**Compactor Deployment (GCP)**:

```yaml
# Cloud Run triggered by Pub/Sub (GCS notifications)
trigger:
  eventType: google.cloud.storage.object.v1.finalized
  eventFilters:
    - attribute: bucket
      value: arco-catalog
    - attribute: name
      value: "*/ledger/*"  # Only ledger writes trigger compaction

# Or: Cloud Scheduler for batchy approach (lower cost, higher latency)
schedule: "*/10 * * * *"  # Every 10 seconds via Cloud Tasks
```

#### Six Non-Negotiable Invariants

These invariants must hold for correctness and scalability:

**Invariant 1 — Ingest is "append-only + ack"**

The Catalog ingest path must NEVER:
- Read current snapshot state
- Mutate existing Parquet
- Compute derived "current pointers"

It must ONLY:
- Validate/auth the request
- Write event(s) to ledger (micro-batched segments)
- Optionally enqueue a compaction trigger
- Return ack

**Invariant 2 — Compactor is the sole writer of Parquet state**

Only compaction jobs write:
- `assets.parquet`, `materializations.parquet`, `partitions.parquet`, etc.
- "Latest/current" pointer tables
- Search and lineage projections

**Invariant 3 — Compaction is idempotent**

If a compaction run crashes and reruns, it must NOT produce:
- Duplicate materialization rows
- Double-counts in "execution_count"
- Inconsistent "latest" pointers

Achieved via: primary keys + upsert/merge semantics + durable watermarks per (topic, shard).

**Invariant 4 — Publish is atomic**

A new snapshot/delta is NOT "visible" until the manifest CAS succeeds. Readers see a consistent view.

**Invariant 5 — Readers never need the ledger**

Standard reads: Parquet snapshot only.
Near-real-time reads: Parquet snapshot + Parquet L0 deltas (NOT JSON events).

Browser never parses event JSON. All read paths are Parquet-only.

**Invariant 6 — No bucket listing dependency for correctness**

Normal path: notification-driven (Pub/Sub triggers compaction).
Anti-entropy: periodic listing job for gap detection, NOT the consumer loop.

#### Read Path: Standard vs Near-Real-Time

**Standard Read Path (Parquet snapshots)**:

Most queries read from compacted Parquet—fast and cheap:

```
1. Browser requests signed URLs from API
   POST /api/catalog/signed-urls
   Body: { "tenant": "acme", "workspace": "prod" }

2. API generates time-limited signed URLs for:
   - manifests/root.manifest.json
   - state/{domain}/**/*.parquet (based on query scope)

3. DuckDB-WASM in browser:
   a. Fetch root.manifest.json → get domain manifest pointers
   b. Fetch relevant manifests → get Parquet file refs
   c. Query Parquet with partition pruning

4. SQL executes entirely in browser—no server round-trips
```

**Near-Real-Time Read Path (snapshot + L0 Parquet deltas)**:

For "give me the absolute latest" queries, read base snapshot + uncommitted L0 deltas:

```typescript
async function readWithL0Deltas(tenant: string, workspace: string, domain: string) {
  // 1. Fetch manifest (includes base snapshot + L0 delta refs)
  const manifest = await fetchManifest(tenant, workspace, domain);

  // 2. Read base snapshot Parquet files
  const baseData = await queryParquet(manifest.base_snapshot_files);

  // 3. Read L0 delta Parquet files (bounded: max 50 files or 256MB)
  // These are mini-compacted Parquet, NOT raw JSON events
  const deltaData = await queryParquet(manifest.l0_delta_files);

  // 4. Union base + deltas with deduplication by primary key
  const liveState = unionWithDedup(baseData, deltaData, manifest.primary_key);

  return liveState;
}
```

**L0 Delta Management:**

```typescript
interface DomainManifest {
  format_version: number;
  published_at: string;
  tenant_id: string;
  workspace_id: string;

  // Base snapshot (fully compacted)
  base_snapshot: {
    snapshot_id: string;
    files: ParquetFileRef[];
    row_count: number;
    min_position: number;
    max_position: number;
  };

  // L0 deltas (recent, not yet merged into base)
  l0_deltas: ParquetFileRef[];

  // Hard caps - when exceeded, compactor merges L0 → new base
  l0_limits: {
    max_files: 50;        // Max L0 Parquet files
    max_bytes: 268435456; // 256MB
    max_age_seconds: 3600; // 1 hour
  };
}

interface ParquetFileRef {
  path: string;
  checksum_sha256: string;
  row_count: number;
  byte_size: number;
  min_position: number;
  max_position: number;
}
```

**Key invariant**: Browser never parses JSON events. L0 deltas are Parquet files produced by micro-compaction (every ~10s), not raw ledger events.

**When to use which**:

| Use Case | Read Path | Latency |
|----------|-----------|---------|
| Catalog browsing UI | Standard (Parquet) | < 100ms |
| Lineage exploration | Standard (Parquet) | < 100ms |
| "Did my run complete?" | Near-real-time (+ tail) | < 1s |
| Planning snapshot | Standard (Parquet) | < 100ms |
| Quality dashboard | Standard (Parquet) | < 100ms |

#### Sharding Strategy

| Table | Partition Key | Shards | Rationale |
|-------|---------------|--------|-----------|
| `assets.parquet` | `namespace_hash % 16` | 16 | Namespace grouping |
| `schemas.parquet` | `asset_id_hash % 16` | 16 | Per-asset versions |
| `columns.parquet` | `asset_id_hash % 16` | 16 | Per-asset columns |
| `materializations.parquet` | `date + asset_hash % 8` | Daily × 8 | Time + asset pruning |
| `partitions.parquet` | `asset_hash % 16` | 16 | Per-asset partitions |
| `quality/results.parquet` | `date + asset_hash % 8` | Daily × 8 | Time-series by asset |
| `lineage/edges.parquet` | `src_asset_hash % 8` | 8 | Fast upstream traversal |

**Resharding Plan (Future-Proofing):**

Shard counts are encoded in manifests to enable smooth migration:

```typescript
interface ShardingConfig {
  // Encoded in manifest - readers use this, not hard-coded values
  asset_namespace_buckets: 16;
  asset_id_buckets: 16;
  materialization_asset_buckets: 8;
  lineage_src_buckets: 8;

  // Logical bucket strategy (256 logical, physical packing varies)
  logical_bucket_bits: 8;  // 256 logical buckets (00-ff)
}
```

**Many logical buckets, few physical files:**
- Define 256 logical buckets (hash % 256)
- Write only populated buckets
- Compact small buckets together as needed
- Readers prune by bucket; physical packing can change over time
- Avoids painful "reshard migrations"

**Column metadata sharding:**
`columns.parquet` is partitioned by `asset_id_hash` to prevent "one huge file" as schema count grows.

### B.3 Field-Level Authority

For fields that could be written by either system:

| Field | Servo Authority | Governance Authority | Conflict Resolution |
|-------|-----------------|---------------------|---------------------|
| `asset.description` | From code docstring | UI override | Governance wins if set |
| `asset.owners` | From `@asset(owners=[...])` | UI additions | Union of both |
| `asset.tags` | From `@asset(tags={...})` | UI additions | Union, dedupe by key |
| `partition.status` | Execution state | Manual override (rare) | Manual wins |

---

## Section C: End-to-End Architecture

### C.1 System Context Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              ARCO PLATFORM                                    │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌─────────────┐     ┌─────────────────────────────────────────────────────┐ │
│  │  Python SDK │     │                    SERVO                             │ │
│  │  + CLI      │────►│              (Orchestration Engine)                  │ │
│  └─────────────┘     │                                                      │ │
│        │             │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐   │ │
│        │             │  │  Deploy  │  │Execution │  │     Asset        │   │ │
│        │             │  │  Service │  │ Service  │  │    Service       │   │ │
│        │             │  └────┬─────┘  └────┬─────┘  └────────┬─────────┘   │ │
│        │             │       │             │                  │             │ │
│        │             │  ┌────▼─────────────▼──────────────────▼─────────┐  │ │
│        │             │  │              CORE DOMAIN                       │  │ │
│        │             │  │  ┌─────────┐ ┌──────────┐ ┌────────────────┐  │  │ │
│        │             │  │  │ Planner │ │Scheduler │ │ State Machine  │  │  │ │
│        │             │  │  └─────────┘ └──────────┘ └────────────────┘  │  │ │
│        │             │  └────────────────────┬──────────────────────────┘  │ │
│        │             │                       │                              │ │
│        │             │  ┌────────────────────▼──────────────────────────┐  │ │
│        │             │  │             GCS LEDGER + COMPACTOR             │  │ │
│        │             │  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐  │  │ │
│        │             │  │  │ Ledger │ │  State │ │Compactor│             │  │  │ │
│        │             │  │  └────────┘ └────────┘ └─────────┘             │  │ │
│        │             │  └───────────────────────────────────────────────┘  │ │
│        │             └─────────────────────────────────────────────────────┘ │
│        │                                    │                                │
│        │                                    │  Direct GCS writes             │
│        │                                    │  (ledger append)               │
│        │                                    ▼                                │
│        │             ┌─────────────────────────────────────────────────────┐ │
│        │             │                   CATALOG                            │ │
│        └────────────►│             (Data Product Hub)                       │ │
│                      │                                                      │ │
│                      │  ┌──────────────────┐  ┌──────────────────────────┐ │ │
│                      │  │ Execution Domain │  │   Governance Domain      │ │ │
│                      │  │ (Servo writes)   │  │   (API/UI writes)        │ │ │
│                      │  │                  │  │                          │ │ │
│                      │  │ • materializations│ │ • tags, owners           │ │ │
│                      │  │ • versions       │  │ • contracts              │ │ │
│                      │  │ • lineage        │  │ • SLAs                   │ │ │
│                      │  │ • quality        │  │ • descriptions           │ │ │
│                      │  │ • history        │  │ • policies               │ │ │
│                      │  └────────┬─────────┘  └────────────┬─────────────┘ │ │
│                      │           │                         │               │ │
│                      │           └────────────┬────────────┘               │ │
│                      │                        ▼                             │ │
│                      │  ┌─────────────────────────────────────────────┐    │ │
│                      │  │            STORAGE LAYER (GCS)              │    │ │
│                      │  │                                             │    │ │
│                      │  │  gs://bucket/{tenant}/{workspace}/          │    │ │
│                      │  │  ├── manifests/*.manifest.json              │    │ │
│                      │  │  ├── ledger/{domain}/*.json                 │    │ │
│                      │  │  ├── state/{domain}/**/*.parquet            │    │ │
│                      │  │  └── data/{namespace}/{asset}/*.parquet     │    │ │
│                      │  └─────────────────────────────────────────────┘    │ │
│                      └──────────────────────────────────────────────────────┘ │
│                                         │                                     │
│                      ┌──────────────────┼──────────────────┐                 │
│                      ▼                  ▼                  ▼                 │
│               ┌─────────────┐   ┌─────────────┐   ┌─────────────┐           │
│               │ DuckDB-WASM │   │ DataFusion  │   │   Admin     │           │
│               │  (Browser)  │   │  (Server)   │   │    UI       │           │
│               └─────────────┘   └─────────────┘   └─────────────┘           │
│                                                                               │
├───────────────────────────────────────────────────────────────────────────────┤
│                          EXTERNAL COMPONENTS                                  │
│                                                                               │
│   ┌───────────────┐    ┌───────────────┐    ┌───────────────┐               │
│   │  Cloud Tasks  │    │   Cloud Run   │    │    Pub/Sub    │               │
│   │   (Queue)     │    │   (Workers)   │    │   (Events)    │               │
│   └───────────────┘    └───────────────┘    └───────────────┘               │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
```

### C.2 Trust Boundaries

```
┌─────────────────────────────────────────────────────────────────┐
│                     TRUST BOUNDARIES                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  BOUNDARY 1: User → Platform                                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  • AuthN: OIDC / API keys                               │    │
│  │  • AuthZ: RBAC per tenant                               │    │
│  │  • TLS: All external traffic                            │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  BOUNDARY 2: Servo ↔ Catalog                                    │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  • Service accounts (not user tokens)                   │    │
│  │  • Signed payloads (HMAC)                               │    │
│  │  • Tenant context propagated and validated              │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  BOUNDARY 3: Servo → Workers                                    │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  • Presigned callback URLs (no worker auth needed)      │    │
│  │  • Task envelope signed by Servo                        │    │
│  │  • Workers have GCS write access to output paths only   │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  BOUNDARY 4: Catalog → Storage                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  • Service account per tenant (or shared + RLS)         │    │
│  │  • Signed URLs for browser reads                        │    │
│  │  • Conditional writes for concurrency control           │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### C.3 End-to-End Flows

#### Flow 1: Deploy

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  SDK/CLI │     │  Servo   │     │  Outbox  │     │ Catalog  │
└────┬─────┘     └────┬─────┘     └────┬─────┘     └────┬─────┘
     │                │                │                │
     │ 1. Build manifest               │                │
     │ (parse @asset decorators)       │                │
     │────────────────────────────────►│                │
     │                │                │                │
     │ 2. Deploy(manifest)             │                │
     │───────────────►│                │                │
     │                │                │                │
     │                │ 3. Validate    │                │
     │                │ - Schema compat│                │
     │                │ - Contract check                │
     │                │                │                │
     │                │ 4. Store deployment             │
     │                │ (single txn):  │                │
     │                │ - Write DeploymentCreated event │
     │                │ - Update deployments projection │
     │                │ - Insert outbox record          │
     │                │────────────────►│                │
     │                │                │                │
     │◄───────────────│                │                │
     │ 5. DeployResponse               │                │
     │    (deployment_id)              │                │
     │                │                │                │
     │                │                │ 6. Outbox worker
     │                │                │ reads pending   │
     │                │                │────────────────►│
     │                │                │                │
     │                │                │ 7. Catalog sync:│
     │                │                │ - Register asset│
     │                │                │ - Store schema  │
     │                │                │ - Mark delivered│
     │                │                │◄────────────────│
     │                │                │                │
```

#### Flow 2: Trigger Run & Plan

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Trigger │     │  Servo   │     │ Catalog  │     │ Scheduler│
└────┬─────┘     └────┬─────┘     └────┬─────┘     └────┬─────┘
     │                │                │                │
     │ 1. TriggerRun(asset, partition) │                │
     │───────────────►│                │                │
     │                │                │                │
     │                │ 2. Fetch metadata snapshot      │
     │                │───────────────►│                │
     │                │                │                │
     │                │◄───────────────│                │
     │                │ 3. Snapshot (materializations,  │
     │                │    schemas, check results)      │
     │                │                │                │
     │                │ 4. Generate plan               │
     │                │ (deterministic):               │
     │                │ - Resolve dependencies         │
     │                │ - Build task DAG               │
     │                │ - Assign task IDs              │
     │                │ - Compute fingerprint          │
     │                │                │                │
     │                │ 5. Store run   │                │
     │                │ (single txn):  │                │
     │                │ - RunCreated event             │
     │                │ - PlanGenerated event          │
     │                │ - Update runs projection       │
     │                │                │                │
     │◄───────────────│                │                │
     │ 6. TriggerResponse              │                │
     │    (run_id, plan summary)       │                │
     │                │                │                │
     │                │ 7. Notify scheduler            │
     │                │────────────────────────────────►│
     │                │                │                │
```

#### Flow 3: Task Execution

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│Scheduler │     │  Queue   │     │  Worker  │     │  Servo   │
└────┬─────┘     └────┬─────┘     └────┬─────┘     └────┬─────┘
     │                │                │                │
     │ 1. Check ready tasks            │                │
     │ (deps met, quota available)     │                │
     │                │                │                │
     │ 2. Dispatch to queue            │                │
     │───────────────►│                │                │
     │                │                │                │
     │ 3. Update state│                │                │
     │ READY → QUEUED │                │                │
     │────────────────────────────────────────────────►│
     │                │                │                │
     │                │ 4. Worker pulls task           │
     │                │───────────────►│                │
     │                │                │                │
     │                │                │ 5. Validate envelope
     │                │                │ (signature, expiry)
     │                │                │                │
     │                │                │ 6. ACK start   │
     │                │                │───────────────►│
     │                │                │                │
     │                │                │ 7. State update│
     │                │                │ DISPATCHED→RUNNING
     │                │                │                │
     │                │                │ 8. Execute:    │
     │                │                │ - Read inputs  │
     │                │                │ - Transform    │
     │                │                │ - Write Parquet│
     │                │                │ - Run checks   │
     │                │                │                │
     │                │                │ 9. Heartbeats  │
     │                │                │───────────────►│
     │                │                │ (periodic)     │
     │                │                │                │
     │                │                │ 10. Report result
     │                │                │───────────────►│
     │                │                │ TaskResult:    │
     │                │                │ - outcome      │
     │                │                │ - materialization
     │                │                │ - check results│
     │                │                │ - lineage      │
     │                │                │                │
```

#### Flow 4: Catalog Update (Ledger + Compactor)

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Servo   │     │   GCS    │     │Compactor │     │   GCS    │
│ (Worker) │     │ (Ledger) │     │(Cloud Run│     │ (State)  │
└────┬─────┘     └────┬─────┘     └────┬─────┘     └────┬─────┘
     │                │                │                │
     │ 1. Task completes               │                │
     │                │                │                │
     │ 2. Write event to ledger        │                │
     │ (fire-and-forget)               │                │
     │ ledger/execution/{date}/{ts}-{uuid}.json               │
     │───────────────►│                │                │
     │                │                │                │
     │ 3. ACK to caller (immediate)    │                │
     │◄───────────────│                │                │
     │                │                │                │
     │                │ 4. GCS notification triggers   │
     │                │ Compactor (or timer)           │
     │                │───────────────►│                │
     │                │                │                │
     │                │                │ 5. List events since watermark
     │                │◄───────────────│                │
     │                │                │                │
     │                │                │ 6. Sort by (timestamp, event_id)
     │                │                │                │
     │                │                │ 7. Fold events into state:
     │                │                │ - Update materializations table
     │                │                │ - Update partition versions
     │                │                │ - Upsert lineage edges
     │                │                │ - Append lineage executions
     │                │                │                │
     │                │                │ 8. Write Parquet updates
     │                │                │───────────────►│
     │                │                │                │
     │                │                │ 9. CAS publish manifest
     │                │                │ (if-generation-match)
     │                │                │───────────────►│
     │                │                │                │
     │                │                │ 10. Update watermark
     │                │                │───────────────►│
     │                │                │                │
     │                │                │ 11. Archive processed events
     │                │◄───────────────│                │
     │                │                │                │
```

**Key differences from outbox pattern:**

| Aspect | Old (Outbox) | New (Ledger + Compactor) |
|--------|--------------|--------------------------|
| Write ACK | After Catalog sync API returns | Immediate (after ledger append) |
| Consistency | Per-event sync (hot path) | Batch compaction (~10s lag) |
| Contention | Per-event CAS on root pointer | Per-batch CAS on domain manifest |
| Idempotency | TTL cache (fragile) | Table primary keys (durable) |
| Replay | Re-run outbox from checkpoint | Re-process ledger from position |

#### Flow 5: Discovery (Browser Query)

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│ Browser  │     │   API    │     │ Catalog  │     │   GCS    │
└────┬─────┘     └────┬─────┘     └────┬─────┘     └────┬─────┘
     │                │                │                │
     │ 1. Get signed URLs              │                │
     │───────────────►│                │                │
     │                │                │                │
     │                │ 2. Generate signed URLs         │
     │                │ (short-lived, tenant-scoped)   │
     │                │                │                │
     │◄───────────────│                │                │
     │ 3. URLs for:   │                │                │
     │ - catalog.json │                │                │
     │ - manifest paths                │                │
     │                │                │                │
     │ 4. DuckDB-WASM: fetch catalog.json              │
     │─────────────────────────────────────────────────►│
     │                │                │                │
     │◄─────────────────────────────────────────────────│
     │ 5. Root pointer (snapshot ref)  │                │
     │                │                │                │
     │ 6. DuckDB-WASM: fetch snapshot  │                │
     │─────────────────────────────────────────────────►│
     │                │                │                │
     │◄─────────────────────────────────────────────────│
     │ 7. Asset manifest refs          │                │
     │                │                │                │
     │ 8. DuckDB-WASM: fetch asset manifest            │
     │─────────────────────────────────────────────────►│
     │                │                │                │
     │◄─────────────────────────────────────────────────│
     │ 9. Partition versions, file paths               │
     │                │                │                │
     │ 10. DuckDB-WASM: query data     │                │
     │ (SQL over Parquet)              │                │
     │─────────────────────────────────────────────────►│
     │                │                │                │
```

---

## Section D: Integration Contracts

> **MVP Note:** For MVP, all contracts use **JSON over HTTP** (REST APIs). Protobuf/gRPC can be added in Phase 2 for performance-critical paths. The schemas below show the logical structure; MVP implementation uses equivalent JSON.

### D.1 Synchronous APIs (JSON/REST for MVP)

#### Catalog APIs (called by Servo)

**MVP**: REST endpoints returning JSON. **Phase 2**: Optional gRPC for high-throughput paths.

```typescript
// GET /api/v1/catalog/snapshot
// Returns metadata snapshot for planning

interface GetMetadataSnapshotRequest {
  tenant_id: string;
  workspace_id: string;
  asset_ids?: string[];  // Empty = all assets
  manifest_generation?: number;  // For cache validation
}

interface MetadataSnapshot {
  snapshot_id: string;
  manifest_generation: number;
  as_of: string;  // ISO-8601 timestamp
  assets: AssetMetadata[];
  latest_materializations: MaterializationRef[];
  schemas: SchemaRef[];
}

interface MaterializationRef {
  asset_id: string;
  partition_id: string;
  materialization_id: string;  // ULID - the true version identifier
  version_number?: number;     // Human-friendly, derived during compaction
  file_paths: string[];
  row_count: number;
  byte_size: number;
  schema_hash: string;
  created_at: string;  // ISO-8601
  created_by_run_id: string;
  quality_status: "unknown" | "passed" | "warned" | "failed" | "quarantined";
}
```

#### Servo APIs (called by Catalog or external systems)

```typescript
// POST /api/v1/runs/trigger
interface TriggerRunRequest {
  tenant_id: string;
  workspace_id: string;
  asset_keys: string[];
  partition_filter?: Record<string, string>;
  mode: "standard" | "force" | "dry_run";
  idempotency_key?: string;
}

// GET /api/v1/runs/{run_id}
interface Run {
  run_id: string;
  tenant_id: string;
  workspace_id: string;
  status: "pending" | "running" | "completed" | "failed" | "cancelled";
  plan_id: string;
  tasks: TaskSummary[];
  created_at: string;
  started_at?: string;
  completed_at?: string;
}

// GET /api/v1/assets/{asset_id}/explain
interface ExplainAssetResponse {
  asset_id: string;
  asset_key: string;
  status: AssetStatus;
  freshness: FreshnessExplanation;
  last_run: RunSummary;
  health: HealthSummary;
  recommendations: Recommendation[];
}
```

### D.2 Ledger Events (JSON for MVP)

With the ledger + compactor model, events are written as JSON files to GCS:

```
ledger/execution/{date}/{timestamp}-{uuid}.json
ledger/quality/{date}/{timestamp}-{uuid}.json
ledger/lineage/{date}/{timestamp}-{uuid}.json
```

#### Event Envelope (CloudEvents-compatible JSON)

```typescript
// All events follow CloudEvents structure
interface LedgerEvent {
  // CloudEvents metadata
  id: string;                    // ULID
  source: string;                // "servo/{tenant_id}/{workspace_id}"
  type: string;                  // Event type (see below)
  specversion: "1.0";
  time: string;                  // ISO-8601
  datacontenttype: "application/json";

  // Routing
  tenant_id: string;
  workspace_id: string;

  // Idempotency (for compactor deduplication)
  idempotency_key: string;

  // Ordering (for deterministic replay)
  sequence?: number;             // Per-stream sequence (optional)
  stream_id?: string;            // e.g., "run:{run_id}"

  // Correlation
  correlation_id?: string;       // Run ID for related events
  causation_id?: string;         // Event that caused this one

  // Payload
  data: MaterializationCompleted | LineageRecorded | CheckExecuted | RunCompleted | DeploymentCreated;
}
```

#### Event Types (JSON)

```typescript
// Event: MaterializationCompleted
// Written to: ledger/execution/{date}/{timestamp}-{uuid}.json
interface MaterializationCompleted {
  type: "materialization_completed";
  materialization_id: string;    // ULID - primary key for idempotency
  asset_id: string;
  asset_key: string;
  partition_id: string;
  partition_key: Record<string, string>;

  run_id: string;
  task_id: string;

  files: FileEntry[];
  row_count: number;
  byte_size: number;

  schema_hash: string;
  schema_arrow_base64?: string;  // Arrow IPC schema (base64 encoded)

  started_at: string;            // ISO-8601
  completed_at: string;          // ISO-8601
}

interface FileEntry {
  path: string;
  size_bytes: number;
  row_count: number;
  stats?: FileStats;             // Optional inline stats
}

// Event: LineageRecorded
// Written to: ledger/lineage/{date}/{timestamp}-{uuid}.json
interface LineageRecorded {
  type: "lineage_recorded";
  run_id: string;
  task_id: string;

  edges: LineageEdgeRecord[];
}

interface LineageEdgeRecord {
  source_asset_id: string;
  target_asset_id: string;
  edge_id: string;               // Derived from source + target + fingerprint
  dependency_fingerprint: string;
  transform_fingerprint: string;

  // Partition-level detail
  source_partitions: PartitionRef[];
  target_partitions: PartitionRef[];

  // Column lineage (Phase 2)
  column_lineage?: ColumnLineage;
}

// Event: Check executed
message CheckExecuted {
  string check_id = 1;
  string asset_id = 2;
  string partition_id = 3;
  string run_id = 4;
  string task_id = 5;
  string materialization_id = 6;

  string check_name = 10;
  string check_type = 11;
  CheckResult result = 12;

  GatingDecision gating_decision = 20;
}

message CheckResult {
  bool passed = 1;
  Severity severity = 2;
  string expected_value = 3;
  string actual_value = 4;
  string message = 5;
}

enum GatingDecision {
  GATING_DECISION_UNSPECIFIED = 0;
  GATING_DECISION_PASSED = 1;
  GATING_DECISION_WARNED = 2;
  GATING_DECISION_BLOCKED = 3;
  GATING_DECISION_QUARANTINED = 4;
}
```

### D.3 Idempotency (Durable Table Keys)

**Critical**: Idempotency must be **durable and structural**, not TTL-based caches. TTL caches break in:
- Outbox retries beyond TTL
- Replays/rebuilds (months later)
- Disaster recovery/backfills

**Solution**: Make tables naturally idempotent via primary keys:

```rust
// Idempotency by table primary keys - no separate cache needed

impl CompactorState {
    /// Materializations: keyed by (tenant, materialization_id)
    /// Duplicate = no-op (first write wins)
    pub fn apply_materialization(&mut self, event: &MaterializationCompleted) {
        let key = (event.tenant_id.clone(), event.materialization_id.clone());

        // Idempotent: if exists, skip (first write wins for materializations)
        if self.materializations.contains_key(&key) {
            return;
        }

        self.materializations.insert(key, Materialization {
            materialization_id: event.materialization_id.clone(),
            asset_id: event.asset_id.clone(),
            partition_id: event.partition_id.clone(),
            partition_key: event.partition_key.clone(),
            files: event.files.clone(),
            row_count: event.row_count,
            byte_size: event.byte_size,
            schema_hash: event.schema_hash.clone(),
            created_at: event.completed_at,
            run_id: event.run_id.clone(),
            task_id: event.task_id.clone(),
        });
    }

    /// Partitions: keyed by (tenant, partition_id)
    /// Duplicate = update (last write wins for current pointer)
    pub fn apply_partition_update(&mut self, event: &MaterializationCompleted) {
        let key = (event.tenant_id.clone(), event.partition_id.clone());

        // Last-write-wins for partition current pointer
        // (but version history is append-only)
        self.partitions.entry(key)
            .or_insert_with(|| Partition::new(&event.asset_id, &event.partition_id))
            .update_current(&event.materialization_id);
    }

    /// Lineage edges: keyed by (tenant, edge_id)
    /// Duplicate = update metadata (last_seen, execution_count)
    pub fn apply_lineage_edge(&mut self, edge: &LineageEdge) {
        let key = (edge.tenant_id.clone(), edge.edge_id.clone());

        self.lineage_edges.entry(key)
            .and_modify(|e| {
                e.last_seen_at = edge.observed_at;
                e.execution_count += 1;
            })
            .or_insert_with(|| LineageEdgeState::from(edge));
    }

    /// Quality results: keyed by (tenant, check_id, materialization_id)
    /// Duplicate = no-op (checks are immutable per materialization)
    pub fn apply_quality_result(&mut self, event: &CheckExecuted) {
        let key = (
            event.tenant_id.clone(),
            event.check_id.clone(),
            event.materialization_id.clone(),
        );

        if self.quality_results.contains_key(&key) {
            return; // Already recorded
        }

        self.quality_results.insert(key, QualityResult::from(event));
    }
}
```

**Primary Key Summary:**

| Table | Primary Key | Duplicate Behavior |
|-------|-------------|-------------------|
| `materializations` | `(tenant_id, materialization_id)` | Ignore (first write wins) |
| `partitions` | `(tenant_id, partition_id)` | Update current pointer |
| `lineage_edges` | `(tenant_id, edge_id)` | Update metadata |
| `lineage_executions` | `(tenant_id, run_id, task_id, edge_id)` | Ignore (first write wins) |
| `quality_results` | `(tenant_id, check_id, materialization_id)` | Ignore (first write wins) |

This ensures:
- **Replay safety**: Re-processing months of events produces identical state
- **No external dependencies**: No Redis/cache needed for correctness
- **Deterministic**: Same events → same state, regardless of timing

### D.4 Ordering Guarantees

| Scope | Ordering | Guarantee | Implementation |
|-------|----------|-----------|----------------|
| Per-run | Total order | Events within a run are ordered | Sequence number per run stream |
| Per-asset | Causal | Materializations for same partition are ordered | Sequence per (asset_id, partition_id) |
| Per-task | Total order | Task events are strictly ordered | Single producer |
| Global | None | No global ordering guarantee | Catalog handles out-of-order |

```rust
// Ordering implementation

pub struct EventOrdering {
    /// Run-level sequence (monotonic within run)
    pub run_sequence: u64,

    /// Asset-partition sequence (monotonic per partition)
    pub partition_sequence: u64,

    /// Servo global position (monotonic, from Postgres sequence)
    /// This is the best anchor for catalog watermarks
    pub servo_global_position: u64,
}

impl CatalogEventHandler {
    pub async fn handle_event(&self, event: &ServoEvent) -> Result<()> {
        // Check for out-of-order within stream
        let stream_key = &event.stream_id;
        let last_seq = self.get_last_sequence(stream_key).await?;

        if event.sequence <= last_seq {
            // Out of order or duplicate
            if event.sequence == last_seq {
                return Ok(()); // Duplicate, ignore
            }
            // Log warning, still process (idempotent handlers will skip)
            warn!(
                "Out of order event: stream={}, expected>{}, got={}",
                stream_key, last_seq, event.sequence
            );
        }

        // Process event (idempotent - safe to reprocess)
        self.process(event).await?;

        // IMPORTANT: Only advance sequence, never regress
        // This prevents out-of-order events from corrupting the watermark
        let new_seq = std::cmp::max(last_seq, event.sequence);
        self.update_sequence(stream_key, new_seq).await?;

        Ok(())
    }
}
```

**Best Practice: Use Servo's Global Position as Watermark Anchor**

For catalog snapshot binding and compaction boundaries, use Servo's Postgres global event position (monotonic sequence) rather than timestamps:

```rust
/// Include Servo's global position in every outbox payload
pub struct OutboxPayload {
    pub event: ServoEvent,
    /// Servo's global event position (from Postgres sequence)
    /// This is monotonic and survives clock skew
    pub servo_position: u64,
}

/// Catalog uses this for:
/// - Checkpoint watermarks (resume from position X)
/// - Snapshot inclusion ranges (min/max position included)
/// - Compaction boundaries (process events up to position Y)
pub struct CatalogWatermark {
    pub last_processed_position: u64,
    pub last_processed_at: DateTime<Utc>,
}
```

**Position Semantics (Precise Definition):**

| Question | Answer |
|----------|--------|
| What is "global position"? | Monotonic Postgres sequence per (tenant, workspace) across ALL Servo events |
| Scope? | Per tenant+workspace (not global across tenants) |
| Survives restore/replay? | Yes - Postgres sequence is durable |
| What if events arrive out of order? | Process anyway (idempotent); watermark = max(current, event.position) |

**Checkpoint Granularity:**

Checkpoints are tracked per `(tenant, workspace, domain)`:

```typescript
interface CheckpointState {
  tenant_id: string;
  workspace_id: string;
  domain: string;  // "execution", "quality", "lineage"
  last_processed_position: number;
  last_processed_at: string;
  events_since_snapshot: number;
}
```

**Gap Detection and Recovery:**

If compactor advances from position 1000 → 1300 but positions 1001–1299 never arrived:

```typescript
// Anti-entropy job (runs hourly)
async function detectGaps(tenant: string, workspace: string, domain: string) {
  const checkpoint = await getCheckpoint(tenant, workspace, domain);
  const ledgerSegments = await listLedgerSegments(tenant, workspace, domain, {
    since: checkpoint.last_processed_position - 1000, // Look back
  });

  // Find gaps in position sequence
  const positions = ledgerSegments.map(s => s.max_position).sort();
  const gaps = findGaps(positions, checkpoint.last_processed_position);

  if (gaps.length > 0) {
    // Re-request from Servo or flag for investigation
    await alertGapDetected(tenant, workspace, domain, gaps);
  }
}
```

**Normal path**: Pub/Sub triggers compaction (notification-driven).
**Anti-entropy**: Hourly listing job detects gaps, NOT the consumer loop.

**Derived Current Pointers (Not Imperative):**

"Current" partition pointers are derived during compaction, NOT updated imperatively per event:

```rust
impl Compactor {
    fn derive_current_pointers(&self, materializations: &[Materialization]) -> HashMap<PartitionId, MaterializationId> {
        // Group by partition
        let by_partition: HashMap<PartitionId, Vec<&Materialization>> = materializations
            .iter()
            .into_group_map_by(|m| m.partition_id.clone());

        // For each partition, find the materialization with max position
        by_partition
            .into_iter()
            .map(|(partition_id, mats)| {
                let current = mats.iter()
                    .max_by_key(|m| m.servo_position)
                    .expect("partition has at least one materialization");
                (partition_id, current.materialization_id.clone())
            })
            .collect()
    }
}
```

This ensures current pointers are:
- Derived from materializations table + ordering rule (max position)
- Deterministic on replay
- Never corrupted by out-of-order events

### D.5 Failure Handling (Ledger Model)

With the ledger + compactor model, failure handling is significantly simpler:

**Write Path Failures:**
- Ledger append failures → GCS retry (automatic, exponential backoff)
- No complex DLQ because events are immutable once written

**Compactor Failures:**
- Compaction failures → retry on next trigger (events remain in ledger)
- CAS manifest failures → retry with fresh read (optimistic concurrency)
- Invalid events → log error, skip, continue (events are auditable)

```rust
/// Compactor failure handling
impl Compactor {
    pub async fn run_with_retry(&self) -> Result<CompactionResult> {
        let mut attempts = 0;
        let max_attempts = 3;

        loop {
            match self.run().await {
                Ok(result) => return Ok(result),
                Err(CompactionError::ManifestCasConflict) => {
                    // Another compactor updated manifest; retry with fresh read
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(CompactionError::MaxRetriesExceeded);
                    }
                    // Exponential backoff: 100ms, 200ms, 400ms
                    tokio::time::sleep(Duration::from_millis(100 * (1 << attempts))).await;
                }
                Err(CompactionError::InvalidEvent { event_id, reason }) => {
                    // Log and skip invalid event (don't block compaction)
                    error!("Invalid event {}: {}", event_id, reason);
                    metrics::increment_counter!("compaction_invalid_events");
                    // Continue with remaining events
                }
                Err(e) => return Err(e),
            }
        }
    }
}
```

### D.6 Idempotency (Durable Table Keys)

**No DLQ or Reconciliation needed** because:

1. **Ledger is append-only**: Events are never lost; can replay from any position
2. **Compactor is idempotent**: Re-processing the same events produces the same state
3. **Table primary keys enforce uniqueness**: Duplicates are no-ops

**Idempotency by Design:**

| Table | Primary Key | Duplicate Behavior |
|-------|-------------|-------------------|
| `materializations` | `(tenant_id, materialization_id)` | Ignore (already exists) |
| `partitions` | `(tenant_id, partition_id)` | Update (last write wins) |
| `quality_results` | `(tenant_id, check_id, materialization_id)` | Ignore |
| `lineage_edges` | `(tenant_id, edge_id)` | Update metadata |
| `lineage_executions` | `(tenant_id, run_id, task_id, edge_id)` | Ignore |

```rust
/// Idempotent upsert for materializations
impl CompactorState {
    pub fn apply_materialization(&mut self, event: &MaterializationEvent) {
        let key = (event.tenant_id.clone(), event.materialization_id.clone());

        // Idempotent: if exists, skip (first write wins for materializations)
        if self.materializations.contains_key(&key) {
            return;
        }

        self.materializations.insert(key, Materialization {
            asset_id: event.asset_id.clone(),
            partition_id: event.partition_id.clone(),
            files: event.files.clone(),
            row_count: event.row_count,
            byte_size: event.byte_size,
            created_at: event.completed_at,
            created_by_run_id: event.run_id.clone(),
            // Note: no sequential version_id; use materialization_id for ordering
        });
    }
}
```

**Replay and Rebuild:**

The ledger can be replayed from any position to rebuild state:

```rust
/// Rebuild catalog state from ledger
pub async fn rebuild_from_ledger(
    storage: &dyn ObjectStore,
    tenant_id: &TenantId,
    workspace: &Workspace,
    from_position: Option<EventPosition>,
) -> Result<CatalogState> {
    let mut state = CatalogState::default();

    // List all events from position (or beginning)
    let events = storage.list_events(tenant_id, workspace, from_position).await?;

    // Sort by (timestamp, event_id) for deterministic order
    let sorted = events.into_iter()
        .sorted_by_key(|e| (e.timestamp, e.event_id.clone()))
        .collect::<Vec<_>>();

    // Fold all events into state (idempotent)
    for event in sorted {
        state.apply(&event)?;
    }

    Ok(state)
}
```

---

## Section E: Catalog Model for Orchestration

### E.1 Asset Definitions and Versions

```rust
// Asset metadata in Catalog (derived from Servo deployments)

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetMetadata {
    /// Stable identifier (survives renames)
    pub asset_id: AssetId,

    /// Current key (namespace.name)
    pub asset_key: AssetKey,

    /// Previous keys (for alias resolution)
    pub key_history: Vec<AssetKeyHistory>,

    /// Current schema version
    pub current_schema_version: SchemaVersion,

    /// Partition strategy
    pub partition_spec: PartitionSpec,

    /// Dependencies (for lineage context)
    pub dependencies: Vec<DependencySpec>,

    /// Code version that defined this asset
    pub defined_by_code_version: CodeVersionId,

    /// First deployed
    pub created_at: DateTime<Utc>,

    /// Last updated
    pub updated_at: DateTime<Utc>,

    /// Health summary (computed)
    pub health: AssetHealth,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetKeyHistory {
    pub key: AssetKey,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub renamed_by_deployment: DeploymentId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    pub version: u32,
    pub schema_hash: String,
    pub arrow_schema: Vec<u8>,          // Arrow IPC format
    pub columns: Vec<ColumnMetadata>,
    pub registered_at: DateTime<Utc>,
    pub registered_by_run: RunId,
    pub compatibility: SchemaCompatibility,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    /// Stable column ID (survives renames)
    pub column_id: ColumnId,
    pub name: String,
    pub data_type: ArrowDataType,
    pub nullable: bool,
    pub description: Option<String>,
    pub pii_classification: Option<PiiType>,
    pub added_in_version: u32,
}
```

### E.2 Partitions and Materializations

```rust
// Partition version model (immutable versions with retention)

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionManifest {
    pub partition_id: PartitionId,
    pub asset_id: AssetId,
    pub partition_key: PartitionKey,

    /// Current version pointer
    pub current_version_id: VersionId,

    /// Recent versions (bounded, for quick time-travel)
    pub recent_versions: Vec<PartitionVersion>,

    /// Retention policy
    pub retention_policy: RetentionPolicy,

    /// Stats across all versions
    pub stats: PartitionStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionVersion {
    pub version_id: VersionId,
    pub materialization_id: MaterializationId,
    pub run_id: RunId,

    /// When this version was created
    pub created_at: DateTime<Utc>,

    /// Schema at time of materialization
    pub schema_version: u32,

    /// Version status
    pub status: VersionStatus,

    /// Quality status
    pub quality_status: QualityStatus,

    /// Aggregate stats (for partition-level pruning)
    pub stats: VersionStats,

    /// File entries
    pub files: Vec<FileEntry>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum VersionStatus {
    Current,    // Active version
    Retained,   // Within retention window
    Expired,    // Soft-deleted, awaiting GC
    Deleted,    // Hard-deleted
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum QualityStatus {
    Unknown,      // No checks run
    Passed,       // All checks passed
    Warned,       // Non-blocking issues
    Failed,       // Blocking checks failed
    Quarantined,  // Manually quarantined
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Keep versions for at least this long
    pub min_retention_days: u32,  // Default: 7

    /// Always keep at least N versions
    pub min_versions: u32,        // Default: 2

    /// Never keep more than N versions
    pub max_versions: u32,        // Default: 100

    /// Grace period before hard delete
    pub gc_grace_period: Duration, // Default: 24h
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    pub size_bytes: u64,
    pub row_count: u64,

    /// Inline stats (for file-level pruning)
    /// Only present if partition has < 10 files (stats blobs get big fast)
    pub stats: Option<FileStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStats {
    pub column_stats: HashMap<String, ColumnStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    pub min: Option<ScalarValue>,
    pub max: Option<ScalarValue>,
    pub null_count: u64,
    pub distinct_count: Option<u64>,
}
```

### E.3 Schema/Contract Versions

```rust
// Schema compatibility enforcement

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SchemaCompatibility {
    /// No compatibility checking
    None,
    /// New schema can read old data
    Backward,
    /// Old schema can read new data
    Forward,
    /// Both directions
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityRules {
    /// Allowed changes under current compatibility mode
    pub allowed_changes: Vec<AllowedChange>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AllowedChange {
    AddNullableColumn,
    AddColumnWithDefault,
    WidenNumericType,
    RenameColumn,       // With stable column ID
    RemoveColumn,       // Backward only
    ChangeOptionalToRequired, // Forward only
}

impl SchemaValidator {
    pub fn check_compatibility(
        &self,
        old_schema: &SchemaVersion,
        new_schema: &SchemaVersion,
        mode: SchemaCompatibility,
    ) -> Result<CompatibilityResult> {
        let mut changes = Vec::new();
        let mut violations = Vec::new();

        // Check each change
        for new_col in &new_schema.columns {
            let old_col = old_schema.columns.iter()
                .find(|c| c.column_id == new_col.column_id);

            match old_col {
                None => {
                    // New column added
                    if new_col.nullable {
                        changes.push(SchemaChange::AddNullableColumn(new_col.name.clone()));
                    } else {
                        violations.push(CompatibilityViolation::AddedRequiredColumn {
                            column: new_col.name.clone(),
                        });
                    }
                }
                Some(old) => {
                    // Check for type changes
                    if old.data_type != new_col.data_type {
                        if self.is_type_widening(&old.data_type, &new_col.data_type) {
                            changes.push(SchemaChange::WidenType {
                                column: new_col.name.clone(),
                                from: old.data_type.clone(),
                                to: new_col.data_type.clone(),
                            });
                        } else {
                            violations.push(CompatibilityViolation::IncompatibleTypeChange {
                                column: new_col.name.clone(),
                                from: old.data_type.clone(),
                                to: new_col.data_type.clone(),
                            });
                        }
                    }

                    // Check for rename
                    if old.name != new_col.name {
                        changes.push(SchemaChange::RenameColumn {
                            from: old.name.clone(),
                            to: new_col.name.clone(),
                        });
                    }
                }
            }
        }

        // Check for removed columns
        for old_col in &old_schema.columns {
            let exists = new_schema.columns.iter()
                .any(|c| c.column_id == old_col.column_id);

            if !exists {
                match mode {
                    SchemaCompatibility::Backward | SchemaCompatibility::Full => {
                        changes.push(SchemaChange::RemoveColumn(old_col.name.clone()));
                    }
                    _ => {
                        violations.push(CompatibilityViolation::RemovedColumn {
                            column: old_col.name.clone(),
                        });
                    }
                }
            }
        }

        if violations.is_empty() {
            Ok(CompatibilityResult::Compatible { changes })
        } else {
            Ok(CompatibilityResult::Incompatible { violations })
        }
    }
}
```

### E.4 Lineage Model

```rust
// Three-layer lineage model

// Layer 1: Edge Graph (stable topology)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    pub edge_id: EdgeId,
    pub source_asset: AssetId,
    pub target_asset: AssetId,
    pub edge_type: EdgeType,
    pub dependency_fingerprint: DependencyFingerprint,
    pub transform_fingerprint: TransformFingerprint,
    pub column_lineage: Option<ColumnLineage>,
}

// Layer 2: Edge Metadata (slowly changing)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeMetadata {
    pub edge_id: EdgeId,
    pub first_seen_run: RunId,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_run: RunId,
    pub last_seen_at: DateTime<Utc>,
    pub execution_count: u64,
    pub owner: Option<String>,
    pub description: Option<String>,
}

// Layer 3: Executions (append-only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageExecution {
    pub execution_id: ExecutionId,
    pub edge_id: EdgeId,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub source_materializations: Vec<MaterializationRef>,
    pub target_materialization: MaterializationId,
    pub source_partitions: Vec<PartitionRef>,
    pub target_partitions: Vec<PartitionRef>,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub rows_read: u64,
    pub rows_written: u64,
}

// Column-level lineage (Phase 2)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnLineage {
    pub capture_method: CaptureMethod,
    pub confidence: Confidence,

    /// Coverage: percentage of output columns mapped with confidence >= Medium
    /// Critical for trust: users need to know "how complete is this lineage?"
    pub coverage: f64,

    /// Human-readable reason for confidence level
    /// E.g., "sql_resolved", "schema_heuristic", "user_override", "runtime_verified"
    pub confidence_reason: ConfidenceReason,

    pub mappings: Vec<ColumnMapping>,

    /// Columns in output schema that could not be mapped
    pub unmapped_columns: Vec<ColumnRef>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConfidenceReason {
    /// SQL analysis with deterministic column resolution
    SqlResolved,
    /// Schema diff heuristic (same name/type)
    SchemaHeuristic,
    /// User-provided override annotation
    UserOverride,
    /// Runtime plan tracing verified
    RuntimeVerified,
    /// Polars/Spark plan introspection
    PlanIntrospection,
    /// Mixed sources with degraded confidence
    Mixed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMapping {
    /// Target column using stable ColumnId (survives renames)
    pub target_column: ColumnRef,
    pub source_columns: Vec<ColumnDependency>,
    pub transform_type: ColumnTransformType,
    /// Per-mapping confidence (can differ from overall)
    pub mapping_confidence: Confidence,
}
```

### E.4.1 Stable Column Identity

Column identity is a **first-class dependency** for safe renames and long-lived evolution:

```rust
/// Stable column identifier that survives renames and reorders
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnId(String);

impl ColumnId {
    /// Generate new ColumnId for a newly added column
    pub fn new() -> Self {
        Self(Ulid::new().to_string())
    }

    /// ColumnId is stored in schema registry, NOT ephemeral lineage results
    /// This enables:
    /// - Renamed columns: preserve ColumnId if mapping is known
    /// - Dropped columns: ColumnId becomes inactive (still referenced historically)
    /// - Added columns: new ColumnId generated
}

/// Column evolution tracking in schema registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnEvolutionRecord {
    pub column_id: ColumnId,
    pub name_history: Vec<ColumnNameChange>,
    pub status: ColumnStatus,
    pub first_seen_schema_version: u32,
    pub last_seen_schema_version: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnNameChange {
    pub old_name: String,
    pub new_name: String,
    pub changed_in_version: u32,
    pub change_type: ColumnChangeType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ColumnChangeType {
    /// Rename via annotation or governed rename process
    GovernedRename,
    /// Inferred rename via schema diff heuristic
    InferredRename,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ColumnStatus {
    Active,
    Inactive, // Dropped but still referenced historically
}
```

### E.4.2 SQL Analysis: Name Resolution Complexity

SQL lineage accuracy hinges on **robust name resolution**. Common pitfalls:

| Pattern | Challenge | Resolution Strategy |
|---------|-----------|---------------------|
| CTEs, nested SELECTs | Scope tracking | Build scope stack during parse |
| Table aliases | Ambiguous column refs | Maintain alias→table map |
| `SELECT *` with JOINs | Implicit column expansion | Resolve against source schemas |
| `USING(...)` / `NATURAL JOIN` | Implicit join columns | Expand to explicit column refs |
| Computed expressions | Expression dependencies | Recursively trace through AST |
| `CASE` expressions | Conditional dependencies | All branches contribute to lineage |

**Confidence assignment:**

- **High**: Deterministically resolved column references to source schema
- **Medium**: Resolved with some ambiguity (e.g., unqualified column in multi-table query)
- **Low**: Could not fully resolve; schema-diff fallback or annotation required

```rust
impl SqlLineageAnalyzer {
    pub fn analyze(&self, sql: &str, input_schemas: &[Schema]) -> ColumnLineageResult {
        let ast = sqlparser::parse(sql)?;
        let mut resolver = ColumnResolver::new(input_schemas);

        // Build scope stack for nested queries
        let scopes = self.build_scope_stack(&ast);

        // Resolve each output column
        let mut mappings = Vec::new();
        let mut unmapped = Vec::new();

        for output_col in self.extract_output_columns(&ast) {
            match resolver.resolve(&output_col, &scopes) {
                Resolution::Definite(sources) => {
                    mappings.push(ColumnMapping {
                        target_column: output_col.to_ref(),
                        source_columns: sources,
                        transform_type: self.infer_transform(&output_col),
                        mapping_confidence: Confidence::High,
                    });
                }
                Resolution::Ambiguous(candidates) => {
                    // Degrade to Medium confidence
                    mappings.push(ColumnMapping {
                        target_column: output_col.to_ref(),
                        source_columns: candidates,
                        transform_type: ColumnTransformType::Unknown,
                        mapping_confidence: Confidence::Medium,
                    });
                }
                Resolution::Failed => {
                    unmapped.push(output_col.to_ref());
                }
            }
        }

        let coverage = mappings.len() as f64 /
            (mappings.len() + unmapped.len()) as f64;

        ColumnLineageResult {
            capture_method: CaptureMethod::StaticAnalysis,
            confidence: self.aggregate_confidence(&mappings),
            confidence_reason: ConfidenceReason::SqlResolved,
            coverage,
            mappings,
            unmapped_columns: unmapped,
        }
    }
}
```

### E.4.3 DataFrame Lineage: Plan Introspection Over AST

**Key insight**: Static analysis of generic Python DataFrame code is brittle.
Prefer **plan introspection** for specific engines:

| Engine | Strategy | Quality |
|--------|----------|---------|
| **Polars** | Leverage lazy plan / expression tree | High (deterministic) |
| **Spark SQL** | Import SQL strings or lineage artifacts | High (reuse SQL analyzer) |
| **dbt** | Import lineage from dbt artifacts | High (authoritative) |
| **Pandas** | Schema-diff + optional annotation | Low (fallback only) |

```rust
/// Polars plan introspection for column lineage
pub struct PolarsLineageExtractor;

impl PolarsLineageExtractor {
    /// Extract column dependencies from Polars LazyFrame plan
    /// This is MUCH more reliable than Python AST analysis
    pub fn extract_from_plan(plan_json: &str) -> Result<ColumnLineage> {
        let plan: PolarsLogicalPlan = serde_json::from_str(plan_json)?;

        let mut mappings = Vec::new();

        for output_col in &plan.output_schema.columns {
            let deps = self.trace_column_dependencies(&plan, &output_col.name);

            mappings.push(ColumnMapping {
                target_column: ColumnRef::new(&output_col.column_id, &output_col.name),
                source_columns: deps,
                transform_type: self.infer_transform_from_expr(&output_col.expr),
                mapping_confidence: Confidence::High,
            });
        }

        ColumnLineage {
            capture_method: CaptureMethod::PlanIntrospection,
            confidence: Confidence::High,
            confidence_reason: ConfidenceReason::PlanIntrospection,
            coverage: 1.0,
            mappings,
            unmapped_columns: vec![],
        }
    }
}
```

### E.4.4 Runtime Tracing: Plan Tracing, Not Row-Level

Runtime tracing observes **structural dependency signals**, never values:

**DO trace:**

- Columns referenced in expressions
- Join keys, filter predicates, group keys
- "This output column was derived from expression X"

**DO NOT trace:**

- Actual data values (privacy + cost)
- Row-level provenance (out of scope)

```rust
/// Runtime plan tracer (opt-in audit mode)
pub struct PlanTracer {
    /// Sampling rate to bound overhead (e.g., 0.01 = 1%)
    sample_rate: f64,
    /// Per-run toggle
    enabled_for_run: bool,
}

impl PlanTracer {
    /// Trace column dependencies at execution time
    /// Returns structural signals only, never values
    pub fn trace_execution(&self, ctx: &ExecutionContext) -> Option<TracedLineage> {
        if !self.enabled_for_run || !self.should_sample() {
            return None;
        }

        let mut traced = TracedLineage::default();

        // Observe expression evaluation (columns only)
        for expr in ctx.evaluated_expressions() {
            traced.add_dependency(
                expr.output_column_id(),
                expr.input_column_ids(),
                expr.expression_type(),
            );
        }

        // Observe join keys
        for join in ctx.join_operations() {
            traced.add_join_dependency(
                join.output_columns(),
                join.left_keys(),
                join.right_keys(),
            );
        }

        Some(traced)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracedLineage {
    pub dependencies: Vec<TracedDependency>,
    pub join_dependencies: Vec<TracedJoinDependency>,
    /// Structural signals only, no values
    pub expression_types: Vec<ExpressionType>,
}
```

### E.5 Health Model

```rust
// Asset health (computed view)

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetHealth {
    /// Overall health status
    pub status: HealthStatus,

    /// Freshness
    pub freshness: FreshnessHealth,

    /// Quality
    pub quality: QualityHealth,

    /// Last run outcome
    pub last_run: LastRunHealth,

    /// Computed at
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,    // All good
    Warning,    // Some issues, not critical
    Unhealthy,  // Critical issues
    Unknown,    // Not enough data
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessHealth {
    /// Time since last successful materialization
    pub last_materialized_at: Option<DateTime<Utc>>,

    /// SLA expectation (if defined)
    pub sla: Option<FreshnessSla>,

    /// Is within SLA?
    pub within_sla: Option<bool>,

    /// Staleness
    pub staleness: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityHealth {
    /// Recent check pass rate
    pub pass_rate: f64,

    /// Number of recent checks
    pub check_count: u64,

    /// Any quarantined partitions?
    pub has_quarantined: bool,

    /// Latest check results
    pub recent_results: Vec<CheckResultSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastRunHealth {
    pub run_id: RunId,
    pub outcome: RunOutcome,
    pub completed_at: DateTime<Utc>,
    pub duration: Duration,
    pub tasks_total: u32,
    pub tasks_succeeded: u32,
    pub tasks_failed: u32,
}

impl AssetHealth {
    pub fn compute(
        asset_id: &AssetId,
        materializations: &[PartitionVersion],
        quality_results: &[CheckResult],
        runs: &[RunSummary],
        sla: Option<&FreshnessSla>,
    ) -> Self {
        let freshness = Self::compute_freshness(materializations, sla);
        let quality = Self::compute_quality(quality_results);
        let last_run = Self::compute_last_run(runs);

        let status = match (&freshness, &quality, &last_run) {
            (f, q, r) if f.within_sla == Some(false) => HealthStatus::Unhealthy,
            (_, q, _) if q.has_quarantined => HealthStatus::Unhealthy,
            (_, _, r) if r.outcome == RunOutcome::Failed => HealthStatus::Warning,
            (_, q, _) if q.pass_rate < 0.95 => HealthStatus::Warning,
            _ => HealthStatus::Healthy,
        };

        Self {
            status,
            freshness,
            quality,
            last_run,
            computed_at: Utc::now(),
        }
    }
}
```

---

## Section F: Deterministic Planning Using Catalog

### F.1 Metadata Snapshot

```rust
// Snapshot for deterministic planning

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanningSnapshot {
    /// Unique ID for this snapshot
    pub snapshot_id: MetadataSnapshotId,

    /// Timestamp (for debugging)
    pub as_of: DateTime<Utc>,

    /// Asset definitions (from latest deployment)
    pub assets: HashMap<AssetId, AssetDefinition>,

    /// Latest materializations per partition
    pub materializations: HashMap<PartitionId, MaterializationRef>,

    /// Schema versions
    pub schemas: HashMap<AssetId, SchemaVersion>,

    /// Recent check results (for gating decisions)
    pub check_results: HashMap<PartitionId, Vec<CheckResult>>,

    /// Quarantine status
    pub quarantined: HashSet<PartitionId>,

    /// SLAs (for priority calculation)
    pub slas: HashMap<AssetId, FreshnessSla>,
}

impl PlanningSnapshot {
    /// Get latest good materialization for a partition
    /// (not quarantined, checks passed)
    pub fn latest_good(&self, partition_id: &PartitionId) -> Option<&MaterializationRef> {
        if self.quarantined.contains(partition_id) {
            return None;
        }

        let mat = self.materializations.get(partition_id)?;

        // Check quality status
        if let Some(checks) = self.check_results.get(partition_id) {
            let has_blocking_failure = checks.iter()
                .any(|c| !c.passed && c.severity == Severity::Error);

            if has_blocking_failure {
                return None;
            }
        }

        Some(mat)
    }
}
```

### F.2 Snapshot Immutability

```rust
// Snapshot persistence and retrieval

impl CatalogSnapshotStore {
    /// Create a new planning snapshot
    /// Returns snapshot ID for later reference
    pub async fn create_snapshot(
        &self,
        tenant_id: &TenantId,
        request: &SnapshotRequest,
    ) -> Result<PlanningSnapshot> {
        // Read current catalog state
        let current = self.read_current_state(tenant_id).await?;

        // Build snapshot
        let snapshot = PlanningSnapshot {
            snapshot_id: MetadataSnapshotId::new(),
            as_of: Utc::now(),
            assets: current.assets.clone(),
            materializations: current.latest_materializations(),
            schemas: current.schemas.clone(),
            check_results: current.recent_check_results(),
            quarantined: current.quarantined_partitions(),
            slas: current.slas.clone(),
        };

        // Persist snapshot (immutable)
        let path = self.snapshot_path(&snapshot.snapshot_id);
        self.object_store.put(&path, snapshot.serialize()?).await?;

        Ok(snapshot)
    }

    /// Retrieve a snapshot by ID
    pub async fn get_snapshot(
        &self,
        snapshot_id: &MetadataSnapshotId,
    ) -> Result<PlanningSnapshot> {
        let path = self.snapshot_path(snapshot_id);
        let bytes = self.object_store.get(&path).await?;
        PlanningSnapshot::deserialize(&bytes)
    }
}
```

### F.2.1 Snapshot Binding Semantics

Users want "the world as it existed when run R47 finished", not just "what R47 touched".

**The subtle pitfall**: If `as_of_run` is implemented purely as timestamp lookup:

1. Resolve `run.completed_at`
2. Find `snapshot_as_of(completed_at)`

...you can accidentally return a snapshot that **predates** the ingestion/visibility of
that run's outputs (especially with async outbox delivery + periodic snapshot compaction).

**Solution**: Track event offsets and run→snapshot mappings explicitly.

```rust
/// Extended snapshot metadata for reproducibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub snapshot_id: MetadataSnapshotId,
    pub created_at: DateTime<Utc>,

    /// Event stream position: min event offset included in this snapshot
    pub min_event_offset_included: u64,

    /// Event stream position: max event offset included in this snapshot
    pub max_event_offset_included: u64,

    /// Hash chain link for integrity verification
    pub previous_snapshot_id: Option<MetadataSnapshotId>,
    pub hash: String,
}

/// Run-to-snapshot mapping for exact reproducibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSnapshotMapping {
    pub run_id: RunId,

    /// First snapshot where ALL of this run's outputs are visible
    /// Populated once all materializations from the run are ingested
    pub first_visible_snapshot_id: Option<MetadataSnapshotId>,

    /// Run completion timestamp (for fallback)
    pub completed_at: DateTime<Utc>,

    /// Status of run visibility in catalog
    pub ingestion_status: RunIngestionStatus,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum RunIngestionStatus {
    /// All materializations from this run are visible in catalog
    FullyIngested,
    /// Some materializations still pending (async delivery in progress)
    PartiallyIngested,
    /// Run completed but no materializations yet visible
    PendingIngestion,
}

/// Snapshot binding options
#[derive(Debug, Clone)]
pub enum SnapshotBinding {
    /// Latest snapshot
    Current,
    /// Snapshot at a specific timestamp
    AsOf(DateTime<Utc>),
    /// Snapshot where run outputs are visible (preferred for reproducibility)
    AsOfRun(RunId),
    /// Specific snapshot by ID
    SnapshotId(MetadataSnapshotId),
}

impl CatalogSnapshotStore {
    /// Resolve snapshot binding to a concrete snapshot
    pub async fn resolve_binding(
        &self,
        tenant_id: &TenantId,
        binding: &SnapshotBinding,
    ) -> Result<SnapshotResolution> {
        match binding {
            SnapshotBinding::Current => {
                let snapshot = self.get_current_snapshot(tenant_id).await?;
                Ok(SnapshotResolution::Resolved(snapshot))
            }

            SnapshotBinding::AsOf(timestamp) => {
                let snapshot = self.find_snapshot_as_of(tenant_id, *timestamp).await?;
                Ok(SnapshotResolution::Resolved(snapshot))
            }

            SnapshotBinding::AsOfRun(run_id) => {
                // First, check for explicit run→snapshot mapping
                let mapping = self.get_run_snapshot_mapping(tenant_id, run_id).await?;

                match mapping.ingestion_status {
                    RunIngestionStatus::FullyIngested => {
                        // Best case: we have exact snapshot where run is visible
                        let snapshot_id = mapping.first_visible_snapshot_id
                            .expect("FullyIngested must have snapshot_id");
                        let snapshot = self.get_snapshot(&snapshot_id).await?;
                        Ok(SnapshotResolution::Resolved(snapshot))
                    }

                    RunIngestionStatus::PartiallyIngested => {
                        // Run not fully ingested yet - return warning with fallback
                        let fallback = self.find_snapshot_as_of(
                            tenant_id,
                            mapping.completed_at,
                        ).await?;

                        Ok(SnapshotResolution::PartialWithWarning {
                            snapshot: fallback,
                            warning: format!(
                                "Run {} not fully ingested into catalog. \
                                 Using timestamp fallback. Some outputs may be missing.",
                                run_id
                            ),
                        })
                    }

                    RunIngestionStatus::PendingIngestion => {
                        // Run just completed, nothing ingested yet
                        Ok(SnapshotResolution::NotYetAvailable {
                            run_id: run_id.clone(),
                            completed_at: mapping.completed_at,
                            retry_after: Duration::from_secs(5),
                        })
                    }
                }
            }

            SnapshotBinding::SnapshotId(id) => {
                let snapshot = self.get_snapshot(id).await?;
                Ok(SnapshotResolution::Resolved(snapshot))
            }
        }
    }

    /// Update run→snapshot mapping when all outputs are visible
    pub async fn mark_run_fully_ingested(
        &self,
        tenant_id: &TenantId,
        run_id: &RunId,
        snapshot_id: &MetadataSnapshotId,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE run_snapshot_mappings
             SET first_visible_snapshot_id = $1,
                 ingestion_status = 'fully_ingested'
             WHERE tenant_id = $2 AND run_id = $3"
        )
        .bind(snapshot_id.as_str())
        .bind(tenant_id.as_str())
        .bind(run_id.as_str())
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[derive(Debug)]
pub enum SnapshotResolution {
    /// Exact snapshot resolved
    Resolved(PlanningSnapshot),

    /// Snapshot resolved but with warning (run not fully ingested)
    PartialWithWarning {
        snapshot: PlanningSnapshot,
        warning: String,
    },

    /// Run outputs not yet available in any snapshot
    NotYetAvailable {
        run_id: RunId,
        completed_at: DateTime<Utc>,
        retry_after: Duration,
    },
}
```

### F.2.2 Run Outputs Query (Strict Forensic Mode)

For strict forensic queries ("only what run R47 produced"), provide a separate API:

```rust
impl CatalogReader {
    /// Get ONLY the outputs produced by a specific run
    /// This is forensic mode - returns exactly what that run wrote
    pub async fn run_outputs(
        &self,
        tenant_id: &TenantId,
        run_id: &RunId,
    ) -> Result<RunOutputs> {
        // Query materializations table filtered by run_id
        let materializations = sqlx::query_as::<_, MaterializationRecord>(
            "SELECT * FROM materializations
             WHERE tenant_id = $1 AND run_id = $2"
        )
        .bind(tenant_id.as_str())
        .bind(run_id.as_str())
        .fetch_all(&self.pool)
        .await?;

        Ok(RunOutputs {
            run_id: run_id.clone(),
            materializations: materializations.into_iter().map(Into::into).collect(),
        })
    }
}
```

### F.3 Plan Determinism

```rust
// Deterministic plan generation

impl Planner {
    /// Generate execution plan
    /// INVARIANT: Same inputs → same plan
    pub fn generate_plan(
        &self,
        request: &ExecutionRequest,
        snapshot: &PlanningSnapshot,
        code_version: &CodeVersion,
    ) -> Result<ExecutionPlan> {
        // Determinism anchors
        let spec = ExecutionPlanSpec {
            version: PLAN_SPEC_VERSION,
            code_version_id: code_version.id.clone(),
            metadata_snapshot_id: snapshot.snapshot_id.clone(),
            trigger: request.trigger.clone(),
            request: request.clone(),
            tasks: Vec::new(),
            timeout: request.timeout.unwrap_or(self.config.default_timeout),
            max_parallelism: request.max_parallelism.unwrap_or(self.config.default_parallelism),
            failure_policy: request.failure_policy.clone(),
            estimates: PlanEstimates::default(),
        };

        // Resolve target assets and partitions
        let targets = self.resolve_targets(request, snapshot)?;

        // Build dependency graph
        let graph = self.build_dependency_graph(&targets, snapshot)?;

        // Generate tasks (deterministic ordering)
        let tasks = self.generate_tasks(&graph, snapshot, code_version)?;

        // Compute fingerprint
        let fingerprint = self.compute_fingerprint(&spec, &tasks);

        let plan = ExecutionPlan {
            header: PlanHeader {
                id: PlanId::new(),
                run_id: RunId::new(),
                tenant_id: request.tenant_id.clone(),
                workspace_id: request.workspace_id.clone(),
                created_at: Utc::now(),
                planner_version: env!("CARGO_PKG_VERSION").to_string(),
            },
            spec: ExecutionPlanSpec { tasks, ..spec },
            fingerprint,
        };

        Ok(plan)
    }

    /// Compute deterministic fingerprint of plan spec
    fn compute_fingerprint(
        &self,
        spec: &ExecutionPlanSpec,
        tasks: &[PlannedTask],
    ) -> String {
        let mut hasher = Sha256::new();

        // Include all determinism-relevant fields
        hasher.update(spec.code_version_id.as_bytes());
        hasher.update(spec.metadata_snapshot_id.as_bytes());
        hasher.update(&spec.trigger.canonical_bytes());

        // Sort tasks by ID for determinism
        let mut sorted_tasks = tasks.to_vec();
        sorted_tasks.sort_by_key(|t| t.task_id.clone());

        for task in sorted_tasks {
            hasher.update(task.asset_key.canonical().as_bytes());
            hasher.update(task.partition_key.canonical_string().as_bytes());
            for dep in &task.depends_on {
                hasher.update(dep.as_bytes());
            }
        }

        format!("plan_{}", hex::encode(hasher.finalize()))
    }

    /// Generate tasks with deterministic IDs
    fn generate_tasks(
        &self,
        graph: &DependencyGraph,
        snapshot: &PlanningSnapshot,
        code_version: &CodeVersion,
    ) -> Result<Vec<PlannedTask>> {
        let mut tasks = Vec::new();
        let mut task_ids = HashMap::new();

        // Topological sort (deterministic)
        for node in graph.topological_sort()? {
            let task_id = TaskId::deterministic(
                &node.asset_id,
                &node.partition_key,
                &code_version.id,
            );

            let depends_on: Vec<TaskId> = node.dependencies.iter()
                .map(|dep| task_ids.get(dep).cloned().unwrap())
                .collect();

            let task = PlannedTask {
                task_id: task_id.clone(),
                asset_key: node.asset_key.clone(),
                partition_key: node.partition_key.clone(),
                code_version_id: code_version.id.clone(),
                resources: self.estimate_resources(&node),
                priority: self.calculate_priority(&node, snapshot),
                depends_on,
                stage: node.stage,
                inputs: self.resolve_inputs(&node, snapshot)?,
                output_uri: self.generate_output_uri(&node, code_version),
            };

            task_ids.insert(node.id.clone(), task_id);
            tasks.push(task);
        }

        Ok(tasks)
    }
}

impl TaskId {
    /// Generate deterministic task ID
    pub fn deterministic(
        asset_id: &AssetId,
        partition_key: &PartitionKey,
        code_version_id: &CodeVersionId,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(asset_id.as_bytes());
        hasher.update(partition_key.canonical_string().as_bytes());
        hasher.update(code_version_id.as_bytes());
        Self(format!("task_{}", &hex::encode(hasher.finalize())[..16]))
    }
}
```

---

## Section G: Multi-Tenancy, Security, and Governance

### G.1 Authentication/Authorization Strategy

```rust
// AuthN/AuthZ across both systems

#[derive(Debug, Clone)]
pub struct AuthContext {
    /// Authenticated identity
    pub identity: Identity,

    /// Tenant scope
    pub tenant_id: TenantId,

    /// Roles within tenant
    pub roles: Vec<Role>,

    /// Workspace scope (optional)
    pub workspace_id: Option<WorkspaceId>,

    /// Request metadata
    pub request_id: String,
    pub trace_id: String,
}

#[derive(Debug, Clone)]
pub enum Identity {
    /// Human user (OIDC)
    User { user_id: String, email: String },

    /// API key
    ApiKey { key_id: String, name: String },

    /// Service account (internal)
    Service { service_name: String },

    /// Worker (task execution)
    Worker { worker_id: String, task_id: TaskId },
}

#[derive(Debug, Clone, Copy)]
pub enum Role {
    Viewer,    // Read catalog, view runs
    Editor,    // Deploy assets, trigger runs
    Admin,     // Manage contracts, SLAs
    Owner,     // Manage roles, tenant settings
}

// Permission checks
impl AuthContext {
    pub fn can_read_asset(&self, asset_id: &AssetId) -> bool {
        // All roles can read
        !self.roles.is_empty()
    }

    pub fn can_deploy(&self) -> bool {
        self.roles.iter().any(|r| matches!(r, Role::Editor | Role::Admin | Role::Owner))
    }

    pub fn can_trigger_run(&self) -> bool {
        self.roles.iter().any(|r| matches!(r, Role::Editor | Role::Admin | Role::Owner))
    }

    pub fn can_manage_contracts(&self) -> bool {
        self.roles.iter().any(|r| matches!(r, Role::Admin | Role::Owner))
    }

    pub fn can_manage_roles(&self) -> bool {
        self.roles.iter().any(|r| matches!(r, Role::Owner))
    }
}
```

### G.2 RBAC Model

```
┌─────────────────────────────────────────────────────────────────┐
│                     RBAC PERMISSION MATRIX                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Permission              │ Viewer │ Editor │ Admin │ Owner     │
│  ────────────────────────┼────────┼────────┼───────┼───────    │
│  View assets             │   ✓    │   ✓    │   ✓   │   ✓       │
│  View runs               │   ✓    │   ✓    │   ✓   │   ✓       │
│  View lineage            │   ✓    │   ✓    │   ✓   │   ✓       │
│  View quality results    │   ✓    │   ✓    │   ✓   │   ✓       │
│  Deploy assets           │        │   ✓    │   ✓   │   ✓       │
│  Trigger runs            │        │   ✓    │   ✓   │   ✓       │
│  Cancel runs             │        │   ✓    │   ✓   │   ✓       │
│  Edit tags/descriptions  │        │   ✓    │   ✓   │   ✓       │
│  Manage data contracts   │        │        │   ✓   │   ✓       │
│  Manage SLAs             │        │        │   ✓   │   ✓       │
│  Quarantine partitions   │        │        │   ✓   │   ✓       │
│  Manage roles            │        │        │       │   ✓       │
│  Manage API keys         │        │        │       │   ✓       │
│  Tenant settings         │        │        │       │   ✓       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### G.3 Tenant Isolation

```rust
// Row-level security implementation

// Postgres RLS (Servo)
const RLS_POLICY: &str = r#"
-- Enable RLS on all tenant-scoped tables
ALTER TABLE events ENABLE ROW LEVEL SECURITY;
ALTER TABLE runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;

-- Create isolation policies
CREATE POLICY tenant_isolation_events ON events
    USING (tenant_id = current_setting('app.tenant_id'));

CREATE POLICY tenant_isolation_runs ON runs
    USING (tenant_id = current_setting('app.tenant_id'));

CREATE POLICY tenant_isolation_tasks ON tasks
    USING (tenant_id = current_setting('app.tenant_id'));

-- Connection setup function
CREATE FUNCTION set_tenant_context(p_tenant_id TEXT) RETURNS VOID AS $$
BEGIN
    PERFORM set_config('app.tenant_id', p_tenant_id, false);
END;
$$ LANGUAGE plpgsql;
"#;

// Connection pool with tenant context
impl TenantAwarePool {
    pub async fn acquire(&self, tenant_id: &TenantId) -> Result<TenantConnection> {
        let conn = self.pool.acquire().await?;

        // Set tenant context
        sqlx::query("SELECT set_tenant_context($1)")
            .bind(tenant_id.as_str())
            .execute(&mut *conn)
            .await?;

        Ok(TenantConnection { conn, tenant_id: tenant_id.clone() })
    }
}

// GCS path isolation (Catalog)
impl CatalogStorage {
    fn tenant_prefix(&self, tenant_id: &TenantId) -> String {
        format!("gs://{}/{}/", self.bucket, tenant_id)
    }

    fn validate_path(&self, tenant_id: &TenantId, path: &str) -> Result<()> {
        let prefix = self.tenant_prefix(tenant_id);
        if !path.starts_with(&prefix) {
            return Err(SecurityError::CrossTenantAccess {
                expected_tenant: tenant_id.clone(),
                actual_path: path.to_string(),
            }.into());
        }
        Ok(())
    }
}
```

### G.4 Secrets Policy

```rust
// Secrets handling (never in logs/events/catalog)

#[derive(Debug)]
pub struct SecretRef {
    /// Reference ID (not the actual secret)
    pub secret_id: String,

    /// Secret manager path
    pub path: String,

    /// Version (for rotation)
    pub version: String,
}

impl SecretRef {
    /// Resolve to actual value (at runtime only)
    pub async fn resolve(&self, secrets_client: &SecretsClient) -> Result<SecretValue> {
        secrets_client.access(&self.path, &self.version).await
    }
}

// Redaction in logs
impl std::fmt::Display for TaskRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskRequest {{ task_id: {}, asset: {}, secrets: [REDACTED] }}",
            self.task_id, self.asset_key)
    }
}

// Secrets never in events
impl MaterializationCompleted {
    pub fn validate(&self) -> Result<()> {
        // Ensure no secrets leaked into event
        for file in &self.files {
            if file.path.contains("secret") || file.path.contains("credential") {
                return Err(ValidationError::PotentialSecretLeak {
                    field: "file.path",
                }.into());
            }
        }
        Ok(())
    }
}
```

### G.5 Audit Logging

```rust
// Comprehensive audit trail

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub event_id: String,
    pub timestamp: DateTime<Utc>,
    pub tenant_id: TenantId,

    /// Who performed the action
    pub actor: AuditActor,

    /// What was done
    pub action: AuditAction,

    /// What was affected
    pub resource: AuditResource,

    /// Request context
    pub request_id: String,
    pub trace_id: String,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,

    /// Before/after state (for changes)
    pub old_value: Option<serde_json::Value>,
    pub new_value: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditActor {
    pub identity_type: String,  // "user", "api_key", "service"
    pub identity_id: String,
    pub email: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditAction {
    // Deployments
    DeployAssets,
    RollbackDeployment,

    // Runs
    TriggerRun,
    CancelRun,

    // Governance
    UpdateContract,
    UpdateSla,
    QuarantinePartition,
    UnquarantinePartition,

    // Access control
    CreateApiKey,
    RevokeApiKey,
    UpdateRoles,

    // Admin
    UpdateTenantSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditResource {
    pub resource_type: String,  // "asset", "run", "contract", etc.
    pub resource_id: String,
    pub resource_name: Option<String>,
}

// Audit log storage (append-only, tamper-evident)
impl AuditLogger {
    pub async fn log(&self, event: AuditEvent) -> Result<()> {
        // Compute hash chain
        let prev_hash = self.get_last_hash(&event.tenant_id).await?;
        let event_hash = self.compute_hash(&event, &prev_hash);

        let record = AuditRecord {
            event,
            prev_hash,
            hash: event_hash.clone(),
        };

        // Append to audit log
        self.append(&record).await?;

        // Update hash pointer
        self.update_last_hash(&record.event.tenant_id, &event_hash).await?;

        Ok(())
    }

    fn compute_hash(&self, event: &AuditEvent, prev_hash: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(prev_hash.as_bytes());
        hasher.update(serde_json::to_vec(event).unwrap());
        hex::encode(hasher.finalize())
    }
}
```

---

## Section H: Observability + Explainability

### H.1 Trace Propagation

```rust
// End-to-end tracing (OpenTelemetry)

use opentelemetry::{
    global,
    trace::{Span, SpanKind, Tracer, TracerProvider},
    Context, KeyValue,
};

// Trace context propagation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceContext {
    pub traceparent: String,  // W3C Trace Context
    pub tracestate: String,
    pub baggage: String,
}

impl TraceContext {
    pub fn from_current() -> Self {
        let cx = Context::current();
        Self {
            traceparent: extract_traceparent(&cx),
            tracestate: extract_tracestate(&cx),
            baggage: extract_baggage(&cx),
        }
    }

    pub fn inject(&self) -> Context {
        inject_context(self)
    }
}

// Span structure for a run
// Root: run
//   ├── task: orders_daily@{date=2025-01-15}
//   │   ├── read_inputs
//   │   │   └── read: raw_orders@{date=2025-01-15}
//   │   ├── execute_transform
//   │   ├── write_output
//   │   │   └── write: orders_daily@{date=2025-01-15}
//   │   └── run_checks
//   │       ├── check: row_count
//   │       └── check: not_null
//   └── catalog_sync

impl RunExecutor {
    pub async fn execute(&self, run: &Run) -> Result<RunResult> {
        let tracer = global::tracer("servo");

        // Root span for entire run
        let mut run_span = tracer
            .span_builder(format!("run:{}", run.run_id))
            .with_kind(SpanKind::Internal)
            .with_attributes(vec![
                KeyValue::new("run.id", run.run_id.to_string()),
                KeyValue::new("tenant.id", run.tenant_id.to_string()),
                KeyValue::new("run.tasks_total", run.tasks.len() as i64),
            ])
            .start(&tracer);

        let cx = Context::current_with_span(run_span);

        // Execute tasks (scheduler handles ordering)
        let result = self.execute_tasks(&run, cx.clone()).await;

        // Record outcome
        run_span.set_attribute(KeyValue::new("run.outcome", result.outcome.to_string()));
        run_span.end();

        result
    }
}

impl TaskExecutor {
    pub async fn execute(&self, task: &TaskRequest, parent_cx: Context) -> Result<TaskResult> {
        let tracer = global::tracer("servo-worker");

        // Task span
        let mut task_span = tracer
            .span_builder(format!("task:{}@{}", task.asset_key, task.partition_key))
            .with_kind(SpanKind::Consumer)
            .with_attributes(vec![
                KeyValue::new("task.id", task.task_id.to_string()),
                KeyValue::new("asset.key", task.asset_key.canonical()),
                KeyValue::new("partition.key", task.partition_key.canonical_string()),
            ])
            .start_with_context(&tracer, &parent_cx);

        let cx = Context::current_with_span(task_span);

        // Read inputs
        let inputs = self.read_inputs(&task, cx.clone()).await?;

        // Execute transform
        let output = self.execute_transform(&task, inputs, cx.clone()).await?;

        // Write output
        let materialization = self.write_output(&task, output, cx.clone()).await?;

        // Run checks
        let check_results = self.run_checks(&task, &materialization, cx.clone()).await?;

        task_span.set_attribute(KeyValue::new("task.rows_written", materialization.row_count as i64));
        task_span.set_attribute(KeyValue::new("task.checks_passed",
            check_results.iter().filter(|c| c.passed).count() as i64));
        task_span.end();

        Ok(TaskResult {
            task_id: task.task_id.clone(),
            outcome: TaskOutcome::Succeeded,
            materialization: Some(materialization),
            check_results,
            ..Default::default()
        })
    }
}
```

### H.2 Metrics

```rust
// Key metrics (Prometheus format)

use prometheus::{
    register_counter_vec, register_gauge_vec, register_histogram_vec,
    CounterVec, GaugeVec, HistogramVec,
};

lazy_static! {
    // Run metrics
    static ref RUNS_TOTAL: CounterVec = register_counter_vec!(
        "servo_runs_total",
        "Total runs by outcome",
        &["tenant_id", "workspace_id", "outcome"]
    ).unwrap();

    static ref RUN_DURATION: HistogramVec = register_histogram_vec!(
        "servo_run_duration_seconds",
        "Run duration in seconds",
        &["tenant_id", "workspace_id"],
        vec![1.0, 5.0, 15.0, 60.0, 300.0, 900.0, 3600.0]
    ).unwrap();

    // Task metrics
    static ref TASKS_TOTAL: CounterVec = register_counter_vec!(
        "servo_tasks_total",
        "Total tasks by outcome",
        &["tenant_id", "asset_namespace", "outcome"]
    ).unwrap();

    static ref TASK_DURATION: HistogramVec = register_histogram_vec!(
        "servo_task_duration_seconds",
        "Task duration in seconds",
        &["tenant_id", "asset_namespace"],
        vec![1.0, 5.0, 15.0, 60.0, 300.0, 900.0]
    ).unwrap();

    // Quality metrics
    static ref CHECKS_TOTAL: CounterVec = register_counter_vec!(
        "servo_checks_total",
        "Total quality checks by result",
        &["tenant_id", "check_type", "result"]
    ).unwrap();

    // Catalog sync metrics
    static ref CATALOG_SYNC_DURATION: HistogramVec = register_histogram_vec!(
        "servo_catalog_sync_duration_seconds",
        "Catalog sync latency",
        &["tenant_id", "event_type"],
        vec![0.1, 0.5, 1.0, 2.0, 5.0]
    ).unwrap();

    static ref CATALOG_SYNC_LAG: GaugeVec = register_gauge_vec!(
        "servo_catalog_sync_lag_seconds",
        "Time since oldest unsynced event",
        &["tenant_id"]
    ).unwrap();

    // Health metrics
    static ref ASSET_FRESHNESS: GaugeVec = register_gauge_vec!(
        "arco_asset_freshness_seconds",
        "Time since last successful materialization",
        &["tenant_id", "asset_key"]
    ).unwrap();

    static ref ASSET_QUALITY_PASS_RATE: GaugeVec = register_gauge_vec!(
        "arco_asset_quality_pass_rate",
        "Recent check pass rate",
        &["tenant_id", "asset_key"]
    ).unwrap();
}
```

### H.3 SLI/SLO Definitions

```yaml
# SLI/SLO definitions

slis:
  # Run scheduling latency
  - name: run_scheduling_latency
    description: Time from TriggerRun to first task dispatched
    metric: histogram_quantile(0.95, servo_run_scheduling_latency_seconds)

  # Task success rate
  - name: task_success_rate
    description: Percentage of tasks that succeed
    metric: sum(servo_tasks_total{outcome="succeeded"}) / sum(servo_tasks_total)

  # Catalog sync freshness
  - name: catalog_sync_freshness
    description: Time for materialization to appear in Catalog
    metric: histogram_quantile(0.95, servo_catalog_sync_duration_seconds)

  # API availability
  - name: api_availability
    description: Percentage of successful API requests
    metric: sum(rate(http_requests_total{status!~"5.."}[5m])) / sum(rate(http_requests_total[5m]))

slos:
  - sli: run_scheduling_latency
    target: 10s
    window: 30d

  - sli: task_success_rate
    target: 99.5%
    window: 30d

  - sli: catalog_sync_freshness
    target: 5s
    window: 30d

  - sli: api_availability
    target: 99.9%
    window: 30d
```

### H.4 Explainability UX

```rust
// "Explain This Asset" feature

pub struct ExplainAssetResponse {
    pub asset: AssetSummary,
    pub status: AssetStatus,
    pub freshness: FreshnessExplanation,
    pub quality: QualityExplanation,
    pub lineage: LineageExplanation,
    pub recent_activity: Vec<ActivityEvent>,
    pub recommendations: Vec<Recommendation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessExplanation {
    pub last_materialized_at: Option<DateTime<Utc>>,
    pub staleness: Option<Duration>,
    pub sla: Option<FreshnessSla>,
    pub within_sla: Option<bool>,

    /// Why is it stale?
    pub stale_reason: Option<StaleReason>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StaleReason {
    /// No runs triggered
    NoRecentRuns {
        last_run_at: Option<DateTime<Utc>>,
    },

    /// Run failed
    RunFailed {
        run_id: RunId,
        failed_at: DateTime<Utc>,
        error: String,
    },

    /// Upstream stale
    UpstreamStale {
        upstream_asset: AssetKey,
        upstream_staleness: Duration,
    },

    /// Quality check failed
    QualityBlocked {
        check_name: String,
        failed_at: DateTime<Utc>,
    },

    /// Manually quarantined
    Quarantined {
        quarantined_by: String,
        quarantined_at: DateTime<Utc>,
        reason: String,
    },
}

impl ExplainService {
    pub async fn explain_asset(
        &self,
        asset_key: &AssetKey,
    ) -> Result<ExplainAssetResponse> {
        let asset = self.catalog.get_asset(asset_key).await?;
        let runs = self.servo.recent_runs_for_asset(&asset.asset_id, 10).await?;
        let quality = self.catalog.quality_summary(&asset.asset_id).await?;
        let lineage = self.catalog.lineage_context(&asset.asset_id).await?;

        // Determine status
        let status = self.compute_status(&asset, &runs, &quality);

        // Explain freshness
        let freshness = self.explain_freshness(&asset, &runs, &lineage).await?;

        // Generate recommendations
        let recommendations = self.generate_recommendations(&status, &freshness, &quality);

        Ok(ExplainAssetResponse {
            asset: asset.summary(),
            status,
            freshness,
            quality: self.explain_quality(&quality),
            lineage: self.explain_lineage(&lineage),
            recent_activity: self.recent_activity(&asset.asset_id, &runs).await?,
            recommendations,
        })
    }

    fn generate_recommendations(
        &self,
        status: &AssetStatus,
        freshness: &FreshnessExplanation,
        quality: &QualitySummary,
    ) -> Vec<Recommendation> {
        let mut recs = Vec::new();

        if let Some(StaleReason::UpstreamStale { upstream_asset, .. }) = &freshness.stale_reason {
            recs.push(Recommendation {
                priority: Priority::High,
                action: format!("Fix upstream asset: {}", upstream_asset),
                reason: "This asset is stale because an upstream dependency is stale".into(),
            });
        }

        if quality.pass_rate < 0.95 {
            recs.push(Recommendation {
                priority: Priority::Medium,
                action: "Review failing quality checks".into(),
                reason: format!("Quality pass rate is {:.1}%", quality.pass_rate * 100.0),
            });
        }

        recs
    }
}
```

---

## Section I: Reliability + Consistency Guarantees

### I.1 Consistency Model

| Operation | Guarantee | Mechanism |
|-----------|-----------|-----------|
| Servo event write | Exactly-once | Postgres ACID + idempotency key |
| Servo projection update | Exactly-once | Same transaction as event |
| Outbox enqueue | Exactly-once | Same transaction as event |
| Catalog sync | Effectively-once | Idempotency key (materialization_id) |
| Catalog snapshot update | Serializable | Conditional write (generation match) |

### I.2 Transaction Boundaries

```rust
// Servo: Event + Projection + Outbox in single transaction

impl EventStore {
    pub async fn append_with_sync(
        &self,
        event: &Event,
        projection_updates: &[ProjectionUpdate],
        catalog_sync: &CatalogSyncPayload,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        // 1. Append event
        sqlx::query(
            "INSERT INTO events (event_id, tenant_id, stream_type, stream_id, sequence, ...)
             VALUES ($1, $2, $3, $4, $5, ...)"
        )
        .bind(&event.id)
        .bind(&event.tenant_id)
        // ... other fields
        .execute(&mut *tx)
        .await?;

        // 2. Update projections
        for update in projection_updates {
            match update {
                ProjectionUpdate::Run(run) => {
                    sqlx::query("UPDATE runs SET state = $1, ... WHERE run_id = $2")
                        .bind(&run.state)
                        .bind(&run.run_id)
                        .execute(&mut *tx)
                        .await?;
                }
                ProjectionUpdate::Task(task) => {
                    sqlx::query("UPDATE tasks SET state = $1, ... WHERE task_id = $2")
                        .bind(&task.state)
                        .bind(&task.task_id)
                        .execute(&mut *tx)
                        .await?;
                }
            }
        }

        // 3. Insert outbox record
        sqlx::query(
            "INSERT INTO event_outbox (event_id, payload, status, next_attempt_at)
             VALUES ($1, $2, 'pending', NOW())"
        )
        .bind(&event.id)
        .bind(serde_json::to_vec(catalog_sync)?)
        .execute(&mut *tx)
        .await?;

        // Commit all together
        tx.commit().await?;

        Ok(())
    }
}
```

### I.3 Rebuild Strategy

```rust
// Projection rebuild from events

impl ProjectionRebuilder {
    /// Rebuild all projections from event log
    pub async fn rebuild_all(&self, tenant_id: &TenantId) -> Result<RebuildReport> {
        let mut report = RebuildReport::default();

        // Clear existing projections
        self.clear_projections(tenant_id).await?;

        // Stream events in order
        let mut event_stream = self.event_store
            .stream_events(tenant_id, 0)
            .await?;

        while let Some(event) = event_stream.next().await {
            let event = event?;

            // Apply to projection
            match event.event_type.as_str() {
                "RunCreated" => {
                    let payload: RunCreated = event.deserialize_payload()?;
                    self.apply_run_created(&payload).await?;
                }
                "TaskStateChanged" => {
                    let payload: TaskStateChanged = event.deserialize_payload()?;
                    self.apply_task_state_changed(&payload).await?;
                }
                // ... other event types
                _ => {}
            }

            report.events_processed += 1;
        }

        report.completed_at = Utc::now();
        Ok(report)
    }
}

// Catalog view rebuild from Servo events
impl CatalogRebuilder {
    /// Rebuild catalog from Servo event stream
    pub async fn rebuild_from_servo(
        &self,
        tenant_id: &TenantId,
        from_position: u64,
    ) -> Result<RebuildReport> {
        let mut report = RebuildReport::default();

        // Stream events from Servo
        let mut event_stream = self.servo_client
            .stream_events(tenant_id, from_position)
            .await?;

        while let Some(event) = event_stream.next().await {
            let event = event?;

            // Process catalog-relevant events
            match &event.data {
                ServoEventData::MaterializationCompleted(mat) => {
                    self.process_materialization(mat).await?;
                    report.materializations_processed += 1;
                }
                ServoEventData::LineageRecorded(lin) => {
                    self.process_lineage(lin).await?;
                    report.lineage_edges_processed += 1;
                }
                ServoEventData::CheckExecuted(check) => {
                    self.process_check(check).await?;
                    report.checks_processed += 1;
                }
                _ => {}
            }

            report.events_processed += 1;
        }

        Ok(report)
    }
}
```

### I.4 Disaster Recovery

```rust
// Backup and recovery procedures

pub struct BackupConfig {
    /// Servo event log backup
    pub servo_backup: ServoBackupConfig,

    /// Catalog snapshot backup
    pub catalog_backup: CatalogBackupConfig,
}

pub struct ServoBackupConfig {
    /// Backup to GCS
    pub destination: String,  // gs://backups/servo/

    /// Backup frequency
    pub schedule: CronSchedule,  // Every 6 hours

    /// Retention
    pub retention_days: u32,  // 30 days

    /// Include projections (for faster recovery)
    pub include_projections: bool,
}

pub struct CatalogBackupConfig {
    /// Catalog is already on GCS, just need cross-region copy
    pub cross_region_bucket: String,

    /// Snapshot retention
    pub snapshot_retention: RetentionPolicy,
}

impl DisasterRecovery {
    /// Full recovery procedure
    pub async fn recover(&self, backup_timestamp: DateTime<Utc>) -> Result<RecoveryReport> {
        let mut report = RecoveryReport::default();

        // 1. Restore Servo event log
        let servo_backup = self.find_servo_backup(backup_timestamp).await?;
        self.restore_servo_events(&servo_backup).await?;
        report.servo_events_restored = servo_backup.event_count;

        // 2. Rebuild Servo projections
        self.rebuild_servo_projections().await?;

        // 3. Restore Catalog (from GCS backup)
        let catalog_backup = self.find_catalog_backup(backup_timestamp).await?;
        self.restore_catalog(&catalog_backup).await?;
        report.catalog_snapshots_restored = catalog_backup.snapshot_count;

        // 4. Replay any events after backup
        let replay_count = self.replay_events_after(backup_timestamp).await?;
        report.events_replayed = replay_count;

        // 5. Reconcile
        let reconciliation = self.reconcile().await?;
        report.discrepancies_fixed = reconciliation.fixed_count;

        Ok(report)
    }
}
```

### I.5 Data Retention

| Data | Hot Retention | Cold Retention | Archive |
|------|---------------|----------------|---------|
| Servo events | 90 days | 1 year (Parquet on GCS) | Forever (compliance) |
| Servo runs/tasks | 90 days | Cascade with events | - |
| Catalog snapshots | 5 most recent | 30 days | - |
| Catalog partition versions | Per retention policy | Per retention policy | - |
| Lineage executions | 90 days | 1 year (Parquet) | Aggregated forever |
| Quality results | 90 days | 1 year | Aggregated forever |
| Audit logs | 90 days | 7 years | Forever |

---

## Section J: MVP Cut + Roadmap

### J.1 MVP Scope

**Goal**: Minimal integration that feels unified—deploy once, see in catalog, track lineage.

#### MVP Features

| Component | Feature | Status |
|-----------|---------|--------|
| **Servo** | Asset deployment | ✓ |
| **Servo** | Run/task execution | ✓ |
| **Servo** | Event sourcing | ✓ |
| **Servo** | Outbox for catalog sync | MVP |
| **Catalog** | Asset metadata storage | MVP |
| **Catalog** | Partition version tracking | MVP |
| **Catalog** | Table-level lineage | MVP |
| **Catalog** | Basic quality results | MVP |
| **Integration** | Servo → Catalog sync | MVP |
| **Integration** | Catalog snapshot for planning | MVP |

#### MVP Non-Features (Deferred)

| Feature | Reason | Phase |
|---------|--------|-------|
| Column-level lineage | Complexity | Phase 2 |
| Schema compatibility enforcement | Complexity | Phase 2 |
| Advanced SLAs | Need usage patterns | Phase 2 |
| Semantic search | Not critical | Phase 3 |
| Cross-asset time-travel joins | Complex UX | Phase 2 |
| Runtime lineage tracing | Performance overhead | Phase 3 |

### J.2 Three-Phase Rollout

#### Phase 1: Foundation (MVP)

**Duration**: 6-8 weeks

**Deliverables**:
1. Servo outbox implementation
2. Catalog sync handler
3. Basic partition manifest storage
4. Table-level lineage derivation
5. Asset health computation
6. Basic "explain asset" UI

**Acceptance Criteria**:
- [ ] Deploy asset via SDK, see in Catalog within 5s
- [ ] Run completes, materialization visible in Catalog within 5s
- [ ] Lineage edge appears after task with dependencies completes
- [ ] Asset health reflects last run outcome
- [ ] Reconciliation job runs and reports zero discrepancies

#### Phase 2: Production Hardening

**Duration**: 4-6 weeks

**Deliverables**:
1. Schema compatibility checking
2. Full version history with time-travel
3. Run-scoped snapshot queries
4. Column-level lineage (static analysis)
5. Data contracts enforcement
6. SLA monitoring and alerting

**Acceptance Criteria**:
- [ ] Breaking schema change blocked at deploy
- [ ] `as_of` and `as_of_run` queries work correctly
- [ ] Column lineage visible for SQL transforms
- [ ] Contract violations block downstream
- [ ] SLA breaches generate alerts

#### Phase 3: Enterprise Features

**Duration**: 4-6 weeks

**Deliverables**:
1. Column lineage (DataFrame analysis)
2. Runtime lineage tracing (opt-in)
3. Semantic search
4. Impact analysis tools
5. Advanced governance (Posture B)
6. Multi-region support

**Acceptance Criteria**:
- [ ] Column lineage for Polars/Pandas transforms
- [ ] "What uses this column?" query works
- [ ] Search finds assets by description/tags
- [ ] Impact analysis shows all downstream dependencies
- [ ] Existence privacy for sensitive assets

### J.3 Migration Strategy

```rust
// Migration from current state to unified model

pub struct MigrationPlan {
    pub phases: Vec<MigrationPhase>,
}

pub struct MigrationPhase {
    pub name: String,
    pub steps: Vec<MigrationStep>,
    pub rollback: Vec<RollbackStep>,
    pub validation: Vec<ValidationCheck>,
}

impl MigrationRunner {
    pub async fn run_phase(&self, phase: &MigrationPhase) -> Result<MigrationResult> {
        // Pre-validation
        for check in &phase.validation {
            if !self.validate(check).await? {
                return Err(MigrationError::PreValidationFailed(check.name.clone()));
            }
        }

        // Execute steps
        for step in &phase.steps {
            match self.execute_step(step).await {
                Ok(_) => continue,
                Err(e) => {
                    // Rollback
                    for rollback in &phase.rollback {
                        self.execute_rollback(rollback).await?;
                    }
                    return Err(e);
                }
            }
        }

        // Post-validation
        for check in &phase.validation {
            if !self.validate(check).await? {
                return Err(MigrationError::PostValidationFailed(check.name.clone()));
            }
        }

        Ok(MigrationResult::Success)
    }
}
```

---

## Section L: Cost Model

### L.1 Target: $0 at Rest (Pure GCS)

The ledger + compactor architecture achieves **$0 at rest** for catalog metadata:

| Component | Old Design | New Design | Monthly Cost (10 tenants) |
|-----------|-----------|-----------|---------------------------|
| **Postgres** | Cloud SQL (db-f1-micro) | None | $0 (was ~$25) |
| **Event Store** | Postgres tables | GCS JSON files | ~$0.02/GB |
| **Catalog State** | Postgres + GCS hybrid | GCS Parquet only | ~$0.02/GB |
| **Compactor** | N/A (sync writes) | Cloud Run (on-demand) | ~$5-20 |
| **API Server** | Always-on Cloud Run | On-demand Cloud Run | ~$10-30 |

**Estimated total for 10 SMB tenants:** $20-55/month (vs ~$385/month with Postgres)

### L.2 Cost Breakdown by Operation

| Operation | Cost Driver | Estimate |
|-----------|------------|----------|
| **Ledger append** | GCS Class A ops ($0.05/10K) | ~$0.50/month per 100K events |
| **Compaction** | Cloud Run CPU + GCS reads/writes | ~$0.10 per compaction run |
| **Browser query** | GCS Class B ops ($0.004/10K) | ~$0.04/month per 10K queries |
| **Signed URL generation** | Cloud Run CPU | ~$0.001 per request |

### L.3 Storage Growth Model

```
Per tenant, per workspace, per month:

Ledger (JSON events):
- ~100 bytes/event average
- 10K events/month = 1 MB/month
- Archived after compaction (can delete after 30 days)

State (Parquet):
- Assets: ~1 KB per asset × 100 assets = 100 KB
- Materializations: ~500 bytes × 1K/month = 500 KB
- Lineage: ~200 bytes/edge × 500 edges = 100 KB
- Quality: ~1 KB/result × 5K/month = 5 MB

Total catalog metadata: ~6 MB/month/workspace (negligible)
User data: varies (not included in catalog cost)
```

### L.4 Scaling Characteristics

| Scale | Tenants | Events/day | Compactions/day | Est. Monthly Cost |
|-------|---------|------------|-----------------|-------------------|
| **Starter** | 1-10 | 1K | 24 | $20-55 |
| **Growth** | 10-50 | 10K | 100 | $50-150 |
| **Scale** | 50-200 | 100K | 500 | $150-500 |
| **Enterprise** | 200+ | 1M+ | Custom | Custom |

**Key insight:** Cost scales with write activity (events), not with data size or tenant count.

---

## Section K: Open Questions + Risks

### K.1 Top 10 Architectural Risks

| # | Risk | Likelihood | Impact | Mitigation |
|---|------|------------|--------|------------|
| 1 | Compaction lag causes stale reads during planning | Medium | High | Include manifest generation in plan; validate freshness |
| 2 | Compactor contention under high load | Low | Medium | Per-domain compactors, debouncing, backpressure |
| 3 | Schema evolution breaks existing queries | Medium | High | Strict compatibility checking, migration tools |
| 4 | Column lineage inference has false positives | High | Medium | Confidence scoring, manual override (Phase 2) |
| 5 | Large Parquet files slow browser queries | Medium | Medium | Sharding by hash, partition pruning |
| 6 | Cross-tenant data leakage | Low | Critical | GCS path isolation, audit logging, penetration testing |
| 7 | Manifest corruption undetected | Low | High | Checksums in manifest, periodic integrity checks |
| 8 | Many small ledger files overwhelm GCS | Low | Medium | Batch writes, periodic archival/cleanup |
| 9 | Event schema evolution breaks compactor | Medium | High | Schema versioning, forward compatibility |
| 10 | Time-travel queries return inconsistent results | Low | High | Manifest-based consistency, clear semantics |

### K.2 Open Questions

| Question | Options | Proposed Default | Status |
|----------|---------|------------------|--------|
| How to handle orphaned GCS objects? | GC job vs lazy cleanup | GC job (24h delay) | Open |
| Max partition versions before compaction? | 10, 50, 100 | 100 | Proposed |
| Lineage edge retention for deleted assets? | Delete, archive, keep | Archive 1 year | Open |
| Column ID stability across renames? | Stable IDs vs name-based | Stable IDs (Phase 2) | Proposed |
| Governance field merge semantics? | Last-write-wins vs union | Field-specific | Open |
| Catalog consistency during Servo upgrade? | Pause sync vs continue | Continue with reconciliation | Proposed |

### K.3 Architecture Decision Records (ADRs)

#### ADR-001: Hybrid Integration Pattern

**Decision**: Use hybrid push/pull integration where Servo pushes critical facts via outbox and Catalog derives detailed views.

**Context**: Need to balance consistency (planning needs accurate data) with decoupling (systems should evolve independently).

**Consequences**:
- (+) Strong consistency for planning-critical data
- (+) Loose coupling for historical/analytical data
- (-) Two integration paths to maintain
- (-) Must handle out-of-order events

---

#### ADR-002: Immutable Partition Versions

**Decision**: Materializations create immutable versions; retention policy governs cleanup.

**Context**: Need time-travel for debugging and auditing; storage costs must be bounded.

**Consequences**:
- (+) Time-travel queries work naturally
- (+) Easy rollback to previous version
- (-) Storage grows with frequency of updates
- (-) Need GC mechanism

---

#### ADR-003: Event-Sourced Servo with Projection Rebuild

**Decision**: Servo stores events as source of truth; projections are rebuildable.

**Context**: Need auditability, debugging, and ability to fix bugs by replaying events.

**Consequences**:
- (+) Complete audit trail
- (+) Can fix projection bugs without data loss
- (+) Natural integration with Catalog (event subscription)
- (-) Query performance depends on projection freshness
- (-) Storage for events + projections

---

#### ADR-004: Catalog as Derived View

**Decision**: Catalog execution data is derived from Servo events, not independently written.

**Context**: Prevent drift between orchestration state and catalog state.

**Consequences**:
- (+) Single source of truth (Servo events)
- (+) Catalog can be rebuilt from events
- (-) Catalog freshness depends on sync latency
- (-) Servo must emit all relevant events

---

#### ADR-005: Three-Layer Lineage Model

**Decision**: Separate lineage into edges (topology), metadata (governance), and executions (history).

**Context**: Different query patterns have different cardinalities and update frequencies.

**Consequences**:
- (+) Fast graph traversal (small edge set)
- (+) Complete forensics (full execution history)
- (+) Governance queries (metadata layer)
- (-) Three storage locations to maintain
- (-) Must keep layers consistent

---

#### ADR-006: Schema Compatibility Enforcement

**Decision**: Enforce backward compatibility by default; breaking changes require explicit migration.

**Context**: Schema is a contract between data producers and consumers.

**Consequences**:
- (+) Prevents accidental breaking changes
- (+) Queries across schema versions work automatically
- (-) Friction for legitimate breaking changes
- (-) Need migration tooling

---

### K.4 Completeness Checklist

- [x] Glossary defines all terms consistently
- [x] Source of truth is unambiguous for each entity
- [x] Integration contract specifies all events and APIs
- [x] Idempotency keys defined for all sync operations
- [x] Failure modes and recovery documented
- [x] Ordering guarantees specified
- [x] Reconciliation strategy defined
- [x] Multi-tenancy enforced at all layers
- [x] Secrets never in events/logs
- [x] Audit trail covers all mutations
- [x] Trace propagation end-to-end
- [x] SLIs/SLOs defined
- [x] MVP scope clearly bounded
- [x] Migration strategy documented
- [x] Top risks identified with mitigations
- [x] Open questions have proposed defaults

---

## Recommended ADR Set

The following Architecture Decision Records (ADRs) should be published alongside this design document to capture key decisions and their rationale:

### ADR-001: Servo→Catalog Delivery Pattern

**Decision**: Use outbox pattern with idempotency keys and async delivery.

**Context**: Servo needs to sync materialization events to Catalog reliably without tight coupling.

**Consequences**:

- (+) Servo commits are atomic (event + projection + outbox in one transaction)
- (+) Catalog delivery is eventually guaranteed via retries/replay
- (+) Idempotency via `materialization_id` handles duplicates
- (-) Catalog view may lag Servo by seconds under normal operation

### ADR-002: Partition Versioning and Retention

**Decision**: Immutable partition versions with time-based retention + minimum version floor.

**Context**: Need to support time-travel queries while bounding storage costs.

**Consequences**:

- (+) Time-travel queries work naturally with `as_of` semantics
- (+) Retention is predictable and configurable per asset
- (+) Minimum version floor prevents accidental data loss
- (-) Storage grows with update frequency; requires GC infrastructure

### ADR-003: Snapshot Binding and `as_of_run` Semantics

**Decision**: Track `run→first_visible_snapshot_id` mapping explicitly; fall back to timestamp with warning.

**Context**: Users want reproducible "world as of run completion" semantics, but async delivery creates visibility gaps.

**Consequences**:

- (+) Exact reproducibility when run is fully ingested
- (+) Graceful degradation with explicit warning when partially ingested
- (+) Forensic mode (`run_outputs`) provides strict filtering
- (-) Requires additional bookkeeping for run→snapshot mapping

### ADR-004: Tiered Statistics for Partition Pruning

**Decision**: Partition-level stats always in manifest; file-level stats conditional on file count.

**Context**: WASM clients need efficient pruning without downloading excessive metadata.

**Consequences**:

- (+) Manifest size bounded regardless of file count
- (+) Partition-level pruning works for all queries
- (+) File-level stats available when beneficial (threshold: 10 files)
- (-) Some queries may over-scan when file count exceeds threshold

### ADR-005: Schema Evolution Compatibility

**Decision**: Enforce backward compatibility by default; breaking changes require explicit migration.

**Context**: Downstream consumers need stable schemas; breaking changes cause pipeline failures.

**Consequences**:

- (+) Safe evolution (add nullable columns, widen types) is frictionless
- (+) Breaking changes are blocked unless explicitly approved
- (+) Stable `ColumnId` survives renames for lineage continuity
- (-) Some legitimate changes require migration workflow

### ADR-006: Lineage Storage Model

**Decision**: Three-layer model: edges (stable topology), metadata (slowly changing), executions (append-only).

**Context**: Need to support four query patterns: ancestry, impact, governance, forensics.

**Consequences**:

- (+) Each layer optimized for its access pattern
- (+) Ancestry/impact queries fast (edge layer)
- (+) Historical queries possible (execution layer)
- (-) More complex than single-table model

### ADR-007: Column-Level Lineage Capture

**Decision**: Layered capture with explicit precedence: annotation > static analysis > schema diff > runtime tracing.

**Context**: Accurate column lineage requires multiple strategies; confidence must be communicated.

**Consequences**:

- (+) "Batteries included" via static analysis for SQL
- (+) Plan introspection for Polars/Spark provides high accuracy
- (+) Coverage and confidence_reason fields enable trust calibration
- (+) User overrides provide escape hatch for complex cases
- (-) Generic Python DataFrame code falls back to low-confidence schema diff

### ADR-008: Security Posture (Existence Privacy)

**Decision**: MVP uses Posture A (all metadata visible within tenant). Enterprise mode requires Posture B with partitioned snapshots.

**Context**: The catalog-as-files model requires explicit decisions about what metadata is visible to whom.

**Posture A (MVP) — All metadata visible within tenant:**

- Signed URLs grant access to entire tenant's catalog files
- DuckDB-WASM queries can see all assets, partitions, lineage
- No per-asset ACLs; tenant boundary is the only access control
- Suitable for most SMB use cases where all users see all data

**Posture B (Enterprise) — Existence privacy:**

- Users shouldn't learn that restricted assets exist
- Requires partitioned snapshots by visibility domain
- Per-user/per-group signed URLs only expose authorized shards
- Root manifest must not reveal hidden assets

**Consequences:**

- (+) MVP is simple: one set of signed URLs per tenant
- (+) Browser-direct reads work efficiently for Posture A
- (-) Posture B requires additional infrastructure:
  - Visibility domain partitioning in storage layout
  - Per-user shard selection at signing time
  - Query engine (or proxy) enforcement for server-side reads
- (-) Migration from A to B requires re-partitioning existing data

**Implementation notes for Posture B (Phase 3):**

```
gs://bucket/{tenant}/{workspace}/
├── state/
│   ├── visibility={public}/       # All users see
│   │   └── assets/...
│   ├── visibility={restricted}/   # ACL-checked
│   │   └── assets/...
│   └── visibility={confidential}/ # Need-to-know
│       └── assets/...
```

**Row filters in browser are NOT enforcement.** True enforcement requires:
- Object-store permissions (different signed URLs per visibility)
- Or query engine proxy that filters before returning results
- Or client-side encryption with per-group keys

### ADR-009: Catalog Storage Model (Ledger + LSM Compaction)

**Decision**: Use append-only ledger + LSM-style compaction for all catalog writes.

**Context**: The original design used per-event sync writes to Parquet, which creates contention and scaling issues.

**New model:**

1. **Ledger layer**: Append-only event segments in GCS (JSON files)
2. **Compaction layer**: Serverless compactor folds events into Parquet
3. **Manifest layer**: Per-domain manifests with CAS publish

**Consequences:**

- (+) Write scalability: many writers, no coordination
- (+) Replay capability: rebuild state from any position
- (+) Reduced contention: batch CAS vs per-event CAS
- (+) Simpler idempotency: table keys, not TTL caches
- (-) Eventual consistency for operational metadata (~10s lag)
- (-) More moving parts (compactor job)

### ADR-010: Version Identity (Materialization-Based)

**Decision**: `materialization_id` (ULID) is the true version identifier. Sequential `v_00042` style version numbers are derived during compaction, not allocated transactionally.

**Context**: Sequential version ID allocation requires read-modify-write serialization, which conflicts with the append-only ledger pattern.

**Consequences:**

- (+) No transactional allocation during writes
- (+) Idempotency is natural (ULID is globally unique)
- (+) Time-ordering is built into ULID
- (+) Replay/rebuild produces consistent version numbers
- (-) Human-friendly version numbers require computation
- (-) Version numbers may have gaps if events are skipped

**Implementation:**

```rust
// During compaction, compute human-friendly version numbers
pub fn compute_version_numbers(materializations: &[Materialization]) -> Vec<(MaterializationId, u32)> {
    materializations
        .iter()
        .sorted_by_key(|m| m.materialization_id.timestamp())  // ULID timestamp
        .enumerate()
        .map(|(i, m)| (m.materialization_id.clone(), (i + 1) as u32))
        .collect()
}
```

---

## Appendix C: Servo North Star Alignment

This section documents how the Arco unified platform architecture aligns with the Servo planning document's goals, principles, and technical standards.

### C.1 Alignment Summary

| Area | Alignment Status | Notes |
|------|------------------|-------|
| Event-sourced core + replayability | ✅ Strong | Append-only log → derived projections matches vision |
| Time-travel + snapshot semantics | ✅ Strong | Enables "if it ran, you can explain it" |
| Partition identity + backfills | ✅ Strong | Stable identity + version history + retention |
| Quality as enforcement | ✅ Strong (with refinement) | Gates/quarantine/policies; needs severity vs outcome separation |
| Lineage model | ✅ Strong | Stable edges + observed facts + column lineage roadmap |
| Integration posture | ✅ Strong | "Integrate with, don't replace" via OpenLineage export |
| Engineering standards | ✅ Strong | OpenTelemetry, API standards, code quality gates |

### C.2 Arco Catalog Scope Statement

**Critical alignment**: The Servo planning document is explicit that Servo should "integrate with catalogs like DataHub rather than compete as a full enterprise catalog."

**Arco's role is precisely scoped as:**

1. **Execution catalog / metastore** — the operational truth needed for:
   - Fast reads during planning
   - Time-travel debugging
   - Materialization discovery
   - Cross-asset snapshot consistency

2. **Light governance overlay** — minimal metadata for Servo-native workflows:
   - Owners, tags, descriptions (union with code-defined values)
   - Data contracts (schema policy, SLAs)
   - Quality results and health status

3. **Integration surface** — NOT a replacement for enterprise catalogs:
   - OpenLineage export for DataHub/Atlan/Collibra integration
   - Lineage facts are first-class exports, not locked in
   - Discovery UX is "good enough for Servo workflows," not competing with full catalog products

**What Arco is NOT:**

- A replacement for DataHub, Atlan, or enterprise data catalogs
- A general-purpose metadata store for non-Servo assets
- A standalone governance platform

This scope ensures alignment with "integrate with, don't replace" while providing the execution-layer capabilities Servo requires for deterministic planning and time-travel debugging.

### C.3 Check Severity vs Gating Outcome Semantics

**Problem identified**: Risk of conflating check severity (how bad is the result?) with execution semantics (task failure vs success) and gating semantics (what to do about it).

**Solution**: Explicit three-layer model:

```rust
/// Layer 1: Check Severity (observed fact)
/// "How bad is this result?"
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CheckSeverity {
    Info,       // Informational, no action needed
    Warning,    // Potential issue, should investigate
    Error,      // Significant issue, likely needs attention
    Critical,   // Severe issue, requires immediate action
}

/// Layer 2: Check Result (observed fact)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub check_id: CheckId,
    pub passed: bool,
    pub severity: CheckSeverity,
    pub expected_value: Option<String>,
    pub actual_value: Option<String>,
    pub message: String,
}

/// Layer 3: Gating Policy (configurable per asset/check)
/// "What should we do when a check of this severity fails?"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatingPolicy {
    /// Mapping from severity to gating action
    pub severity_actions: HashMap<CheckSeverity, GatingAction>,

    /// Default action if severity not mapped
    pub default_action: GatingAction,
}

/// Gating Action (outcome of policy application)
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum GatingAction {
    /// Continue execution, no impact
    Continue,

    /// Log warning but continue
    Warn,

    /// Block downstream tasks from using this output
    BlockDownstream,

    /// Quarantine the output (available but marked unsafe)
    Quarantine,

    /// Fail the task (and potentially the run)
    FailTask,

    /// Fail the entire run immediately
    FailRun,
}

/// Gating Decision (computed from policy + result)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatingDecision {
    pub check_result: CheckResult,
    pub policy_applied: GatingPolicy,
    pub action_taken: GatingAction,
    pub reason: String,
}

impl GatingPolicy {
    /// Apply policy to check result
    pub fn apply(&self, result: &CheckResult) -> GatingDecision {
        let action = if result.passed {
            GatingAction::Continue
        } else {
            self.severity_actions
                .get(&result.severity)
                .copied()
                .unwrap_or(self.default_action)
        };

        GatingDecision {
            check_result: result.clone(),
            policy_applied: self.clone(),
            action_taken: action,
            reason: format!(
                "Check {} {} with severity {:?} → action {:?}",
                result.check_id,
                if result.passed { "passed" } else { "failed" },
                result.severity,
                action
            ),
        }
    }
}

/// Example enterprise policies
impl GatingPolicy {
    /// Strict policy: errors block, criticals fail
    pub fn strict() -> Self {
        Self {
            severity_actions: [
                (CheckSeverity::Info, GatingAction::Continue),
                (CheckSeverity::Warning, GatingAction::Warn),
                (CheckSeverity::Error, GatingAction::BlockDownstream),
                (CheckSeverity::Critical, GatingAction::FailRun),
            ].into(),
            default_action: GatingAction::Warn,
        }
    }

    /// Lenient policy: only criticals block
    pub fn lenient() -> Self {
        Self {
            severity_actions: [
                (CheckSeverity::Info, GatingAction::Continue),
                (CheckSeverity::Warning, GatingAction::Continue),
                (CheckSeverity::Error, GatingAction::Warn),
                (CheckSeverity::Critical, GatingAction::BlockDownstream),
            ].into(),
            default_action: GatingAction::Continue,
        }
    }

    /// Quarantine policy: failures quarantine but don't block
    pub fn quarantine() -> Self {
        Self {
            severity_actions: [
                (CheckSeverity::Info, GatingAction::Continue),
                (CheckSeverity::Warning, GatingAction::Continue),
                (CheckSeverity::Error, GatingAction::Quarantine),
                (CheckSeverity::Critical, GatingAction::Quarantine),
            ].into(),
            default_action: GatingAction::Continue,
        }
    }
}
```

This separation enables enterprise requirements:

- "ERROR blocks downstream but doesn't fail the run"
- "CRITICAL fails the run immediately"
- "WARN logs but continues"
- "Quarantine output but keep audit trail"

### C.4 Dual Deployment Strategy

**Non-negotiable**: Managed (SaaS) and enterprise self-hosted modes must share the same API/UX; only adapters differ.

**Adapter trait requirements:**

```rust
/// Queue adapter (Cloud Tasks / SQS / local)
#[async_trait]
pub trait TaskQueue: Send + Sync {
    async fn enqueue(&self, task: TaskEnvelope) -> Result<TaskHandle>;
    async fn acknowledge(&self, handle: &TaskHandle) -> Result<()>;
    async fn extend_deadline(&self, handle: &TaskHandle, duration: Duration) -> Result<()>;
}

/// Scheduler adapter (Cloud Scheduler / EventBridge / cron)
#[async_trait]
pub trait ScheduleTrigger: Send + Sync {
    async fn create_schedule(&self, spec: ScheduleSpec) -> Result<ScheduleId>;
    async fn update_schedule(&self, id: &ScheduleId, spec: ScheduleSpec) -> Result<()>;
    async fn delete_schedule(&self, id: &ScheduleId) -> Result<()>;
    async fn list_schedules(&self, filter: ScheduleFilter) -> Result<Vec<Schedule>>;
}

/// Object store adapter (GCS / S3 / Azure / local)
#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn get(&self, path: &str) -> Result<Bytes>;
    async fn put(&self, path: &str, data: Bytes) -> Result<()>;
    async fn put_conditional(&self, path: &str, data: Bytes, generation: u64) -> Result<()>;
    async fn delete(&self, path: &str) -> Result<()>;
    async fn list(&self, prefix: &str) -> Result<BoxStream<ObjectMeta>>;
}

/// Secrets adapter (Secret Manager / Vault / env)
#[async_trait]
pub trait SecretsManager: Send + Sync {
    async fn get_secret(&self, path: &str, version: Option<&str>) -> Result<SecretValue>;
}
```

**Catalog I/O paths**: All storage operations use the ObjectStore trait, ensuring GCS/S3/Azure/local work in both deployment modes.

### C.5 Integration Contract as First-Class API Surface

**Requirement**: The Servo↔Arco boundary must be a versioned, stable contract with backward compatibility guarantees.

**Contract specifications:**

```protobuf
// catalog/v1/ingest.proto
// Versioned "Execution Catalog Ingest" contract

syntax = "proto3";
package catalog.v1;

option go_package = "github.com/arco/catalog/v1";

// Service for ingesting execution events from Servo
service CatalogIngestService {
  // Sync a materialization event (idempotent)
  rpc SyncMaterialization(SyncMaterializationRequest)
      returns (SyncMaterializationResponse);

  // Sync lineage from a run
  rpc SyncLineage(SyncLineageRequest)
      returns (SyncLineageResponse);

  // Sync quality check results
  rpc SyncQualityResults(SyncQualityResultsRequest)
      returns (SyncQualityResultsResponse);

  // Batch sync (for replay/rebuild)
  rpc SyncBatch(stream SyncBatchRequest)
      returns (SyncBatchResponse);
}

// All requests include standard headers
message RequestHeader {
  string request_id = 1;
  string tenant_id = 2;
  string trace_parent = 3;  // W3C Trace Context
  string idempotency_key = 4;
}
```

**Compatibility enforcement in CI:**

```yaml
# .github/workflows/contract-check.yml
name: Contract Compatibility Check

on:
  pull_request:
    paths:
      - 'proto/**'

jobs:
  breaking-change-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: bufbuild/buf-setup-action@v1
      - name: Check for breaking changes
        run: buf breaking --against 'https://github.com/arco/proto.git#branch=main'

  golden-fixture-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run contract tests
        run: cargo test --package servo-catalog-contract
```

**Contract test suite**: A dedicated test package that runs Servo + Arco together:

```rust
// tests/integration/servo_arco_contract_test.rs

#[tokio::test]
async fn test_materialization_sync_idempotent() {
    let (servo, arco) = setup_test_harness().await;

    let event = MaterializationCompleted {
        materialization_id: "mat_123".into(),
        // ...
    };

    // First sync succeeds
    let result1 = arco.sync_materialization(&event).await;
    assert!(result1.is_ok());

    // Duplicate sync is idempotent
    let result2 = arco.sync_materialization(&event).await;
    assert!(result2.is_ok());
    assert_eq!(result1.unwrap().version_id, result2.unwrap().version_id);
}

#[tokio::test]
async fn test_lineage_edge_deduplication() {
    let (servo, arco) = setup_test_harness().await;

    // Sync same lineage twice
    let edge = LineageEdgeRecord { /* ... */ };
    arco.sync_lineage(&edge).await.unwrap();
    arco.sync_lineage(&edge).await.unwrap();

    // Should have exactly one edge
    let edges = arco.list_edges(&edge.target_asset_id).await.unwrap();
    assert_eq!(edges.len(), 1);
}
```

### C.6 Success Metrics Alignment

**Target metrics from Servo planning doc:**

| Metric | Target | Architecture Support |
|--------|--------|---------------------|
| TTFSA (Time to First Successful Asset) | < 30 min | SDK simplicity, local-to-prod parity |
| MTTD (Mean Time to Debug) | < 15 min | Time-travel, snapshot binding, explain UX |
| Local-to-prod parity | > 95% | Dual deployment adapters, same API surface |

**Features enabling MTTD < 15 min:**

1. **"Explain This Asset" UX** (Section H.4):
   - Asset status and health summary
   - Freshness explanation with root cause
   - Quality check history and trends
   - Lineage context (upstream/downstream)
   - Actionable recommendations

2. **"What Changed?" diffs:**

```rust
/// Compare two snapshots or versions
pub struct SnapshotDiff {
    pub from_snapshot: MetadataSnapshotId,
    pub to_snapshot: MetadataSnapshotId,

    pub added_assets: Vec<AssetKey>,
    pub removed_assets: Vec<AssetKey>,
    pub modified_assets: Vec<AssetModification>,

    pub added_partitions: Vec<PartitionRef>,
    pub removed_partitions: Vec<PartitionRef>,
    pub modified_partitions: Vec<PartitionModification>,

    pub schema_changes: Vec<SchemaChange>,
    pub lineage_changes: Vec<LineageChange>,
}

impl CatalogReader {
    /// Diff two snapshots
    pub async fn diff_snapshots(
        &self,
        from: &MetadataSnapshotId,
        to: &MetadataSnapshotId,
    ) -> Result<SnapshotDiff> {
        // ...
    }

    /// Diff partition versions
    pub async fn diff_versions(
        &self,
        partition_id: &PartitionId,
        from_version: &VersionId,
        to_version: &VersionId,
    ) -> Result<VersionDiff> {
        // ...
    }
}
```

3. **Failure classification + remediation hints:**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureClassification {
    pub category: FailureCategory,
    pub severity: FailureSeverity,
    pub is_transient: bool,
    pub remediation_hints: Vec<RemediationHint>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FailureCategory {
    // Data issues
    SchemaViolation,
    DataQualityFailure,
    MissingDependency,

    // Infrastructure issues
    ResourceExhausted,
    Timeout,
    StorageError,

    // Code issues
    TransformError,
    ConfigurationError,

    // External issues
    UpstreamFailure,
    RateLimited,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemediationHint {
    pub action: String,
    pub command: Option<String>,
    pub documentation_url: Option<String>,
}
```

---

## Appendix D: Production-Ready Conformance Checklist

Use this checklist in design reviews to ensure features stay aligned with Servo principles and technical standards.

### D.1 Reliability and Correctness

- [ ] **Idempotency**: All sync operations have idempotency keys (materialization_id, event_id)
- [ ] **Transactional outbox**: Event + projection + outbox in single transaction
- [ ] **Replay capability**: Catalog can be rebuilt from Servo event stream
- [ ] **Snapshot reproducibility**: Reads include snapshot_id; same ID → same data
- [ ] **Tenant isolation**: RLS enforced; no cross-tenant data paths exist

### D.2 Standards and Interoperability

- [ ] **REST APIs**: OpenAPI 3.1 spec, RFC7807 errors, cursor pagination
- [ ] **gRPC APIs**: Protobuf with buf compatibility checks
- [ ] **Versioned APIs**: Breaking changes require version bump
- [ ] **OpenLineage export**: Lineage facts exportable in OpenLineage format
- [ ] **Trace propagation**: W3C Trace Context (traceparent) propagated end-to-end

### D.3 Security and Compliance

- [ ] **Row-Level Security**: Postgres RLS on all tenant-scoped tables
- [ ] **Secrets handling**: Never in events, logs, or catalog; only SecretRef
- [ ] **Audit trail**: All mutations logged with actor, timestamp, before/after
- [ ] **Hash-chain integrity**: Audit logs and snapshots are tamper-evident
- [ ] **Control/data plane separation**: User code runs in isolated worker environment

### D.4 Professional Engineering Quality

- [ ] **Rust quality gates**: `cargo fmt`, `cargo clippy` (no warnings), coverage > 80%
- [ ] **Python quality gates**: `ruff`, `mypy`, `pytest`, coverage > 80%
- [ ] **Schema evolution**: Backward compatible by default; breaking requires migration
- [ ] **Contract tests**: Servo↔Arco integration tests in CI
- [ ] **Observability**: Traces, logs, metrics, lineage events emitted for every operation

### D.5 UX and Debuggability

- [ ] **Explain capability**: Every asset/run can be explained with root cause
- [ ] **Diff capability**: Can diff snapshots, versions, schemas
- [ ] **Failure hints**: Errors include classification and remediation suggestions
- [ ] **Time-travel**: `as_of` queries work for debugging past state
- [ ] **Lineage traversal**: Upstream/downstream queries < 100ms for typical graphs

### D.6 Catalog Acceptance Test Suite

These tests validate the catalog invariants and should be automated in CI:

**1. At-Least-Once Delivery (Idempotency)**
```
Test: Replay same ledger segment 10x
Expected: No duplicate rows in projections
Validates: Primary key idempotency
```

**2. Out-of-Order Processing**
```
Test: Deliver ledger segments in random order
Expected: Projection matches ordered-by-position result
Validates: Idempotent handlers, max() watermark logic
```

**3. Gap Detection**
```
Test: Skip a range of positions (1001-1299)
Expected: Anti-entropy job detects and flags gaps
Validates: Gap detection, alerting
```

**4. Crash Recovery**
```
Test: Kill compactor mid-run, restart
Expected: Rerun produces identical state
Validates: Atomic publish, checkpoint durability
```

**5. Manifest CAS Conflicts**
```
Test: Two compactors race to publish
Expected: Exactly one wins; loser retries safely
Validates: CAS semantics, retry logic
```

**6. Workspace Isolation**
```
Test: Attempt cross-workspace read/write
Expected: Blocked at signing + path validation
Validates: Workspace prefix enforcement
```

**7. Browser Query Pruning**
```
Test: Search for one namespace
Expected: Downloads only that namespace's shards (not all)
Validates: Sharding, partition pruning
```

**8. L0 Delta Limits**
```
Test: Generate > 50 L0 delta files
Expected: Compactor merges to new base snapshot
Validates: L0 cap enforcement
```

**9. Derived Current Pointers**
```
Test: Process materializations out of order
Expected: Current pointer always points to max(position)
Validates: Derived (not imperative) current logic
```

**10. Canonical Partition Key Encoding**
```
Test: Same partition key from Python SDK and Rust
Expected: Identical PartitionId generated
Validates: Cross-language canonical encoding
```

---

## Appendix A: Proto File Index

| File | Contents |
|------|----------|
| `servo/v1/common.proto` | Shared types (IDs, timestamps) |
| `servo/v1/task.proto` | TaskRequest, TaskResult, TaskEvent |
| `servo/v1/asset.proto` | AssetDefinition, dependencies |
| `servo/v1/plan.proto` | ExecutionPlan, PlannedTask |
| `servo/v1/state.proto` | TaskState, RunState |
| `servo/v1/event.proto` | Event envelope, event types |
| `servo/v1/service.proto` | gRPC service definitions |
| `catalog/v1/service.proto` | Catalog service definitions |
| `catalog/v1/sync.proto` | Sync event payloads |

---

## Appendix B: Storage Layout Reference

**Updated for Ledger + Compactor Architecture (No Postgres)**

```
gs://bucket/{tenant}/{workspace}/
│
├── manifests/                              # Per-domain manifests
│   ├── root.manifest.json                  # Points to latest of each domain
│   ├── catalog_core.manifest.json          # Assets, schemas, contracts
│   ├── execution.manifest.json             # Materializations, partitions
│   ├── quality.manifest.json               # Check results
│   └── lineage.manifest.json               # Edges, executions
│
├── ledger/                                 # Append-only event log (JSON)
│   ├── catalog/                            # Core catalog events
│   │   └── {ulid}.json                     # TableCreated, SchemaChanged
│   ├── execution/                          # Materialization events
│   │   └── {timestamp}-{uuid}.json         # MaterializationCompleted
│   ├── quality/                            # Check result events
│   │   └── {timestamp}-{uuid}.json         # CheckExecuted
│   └── lineage/                            # Lineage events
│       └── {timestamp}-{uuid}.json         # LineageRecorded
│
├── state/                                  # Compacted Parquet (read-optimized)
│   ├── catalog/
│   │   ├── assets/
│   │   │   └── ns_hash={h}/assets.parquet
│   │   ├── schemas/
│   │   │   └── asset_hash={h}/schemas.parquet
│   │   └── contracts/
│   │       └── contracts.parquet
│   ├── execution/
│   │   ├── materializations/
│   │   │   └── date={d}/asset_hash={h}/data.parquet
│   │   └── partitions/
│   │       └── asset_hash={h}/partitions.parquet
│   ├── quality/
│   │   └── date={d}/asset_hash={h}/results.parquet
│   └── lineage/
│       ├── edges/
│       │   └── src_hash={h}/edges.parquet
│       └── executions/
│           └── year={y}/month={m}/data.parquet
│
├── governance/                             # Governance overlay (direct writes)
│   ├── tags.parquet
│   ├── owners.parquet
│   ├── descriptions.parquet
│   └── slas.parquet
│
└── data/                                   # User data (materialized outputs)
    └── {namespace}/{asset}/
        └── {partition_key}/
            └── {materialization_id}/       # Version by mat_id (ULID)
                └── part-{n}.parquet
```

**Key changes from previous design:**

| Component | Old | New |
|-----------|-----|-----|
| Servo state | Postgres tables | GCS ledger + state |
| Catalog sync | Outbox → sync API | Direct GCS writes |
| Event store | Postgres events table | `ledger/` JSON files |
| Projections | Postgres views | `state/` Parquet files |
| Version identity | Sequential `v_00042` | `materialization_id` (ULID) |
| Workspace isolation | Not in path | `{workspace}/` in path |
| Manifest structure | Single root pointer | Per-domain manifests |

---

*End of Design Document*
