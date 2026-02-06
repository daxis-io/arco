# arco-flow Production-Grade Remediation Plan (v2)

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close all audit gaps in arco-flow to achieve production-grade orchestration with canonical determinism, distributed correctness, and operational excellence.

**Architecture:** Four-gate approach: (A) Determinism via canonical JSON + semantic TaskKey + stable fingerprints + deterministic DAG ordering, (B) State machine observability via TransitionReason + idempotent transitions + two-timeout heartbeats, (C) Distributed correctness via Store trait + Postgres CAS + leader election, (D) Production features via quotas/fairness + Cloud Tasks dispatcher + metrics/runbooks.

**Tech Stack:** Rust 1.85+, serde_json, sha2, sqlx (Postgres), tokio, tracing, proptest

---

## ⚠️ MUST-FIX DECISIONS (Resolve Before Coding)

These ADRs must be written and approved before implementation begins. The plan contains blocking contradictions that need resolution.

### ADR-010: Canonical JSON Serialization

**Decision Required:** Single-mode canonical JSON with float rejection.

**Context:**
- `arco_core::canonical_json` already exists and rejects floats
- Cross-language (Rust + Python) signing requires byte-identical output
- Float stringification differs between languages

**Decision:**
- There is ONE canonical JSON mode: `to_canonical_bytes()`
- Floats are rejected everywhere (returns `CanonicalJsonError::FloatNotAllowed`)
- All numeric values must be integers (millis, millicores, bytes)
- ResourceRequirements uses `u64` fields (memory_bytes, cpu_millicores, timeout_ms)

**File:** `docs/adr/010-canonical-json.md`

---

### ADR-011: Canonical Partition Identity

**Decision Required:** Use `arco_core::partition::PartitionKey` as the ONLY canonical partition identity.

**Context:**
- `arco_core::partition::PartitionKey` already exists with typed values and URL-safe encoding
- The plan's `canonical_partition_key(HashMap<String, String>)` is a DUPLICATE format
- Having two partition formats will cause cross-component bugs

**Decision:**
- `PartitionKey` (typed, BTreeMap-backed) is THE canonical partition identity
- Remove/deprecate JSON-based `canonical_partition_key()` from `canonical_json.rs`
- TaskKey uses `Option<PartitionKey>` not `Option<String>`
- Cross-language golden vectors use `PartitionKey::canonical_string()` format

**File:** `docs/adr/011-partition-identity.md`

---

### ADR-012: AssetKey Canonical String Format

**Decision Required:** Choose ONE canonical string format for AssetKey identity.

**Context:**
- Current `Display` impl: `namespace.name` (dot separator)
- Plan proposes: `namespace/name` (slash separator)
- Some docs use: `namespace:name` (colon separator)

**Decision:**
- `Display` remains `namespace.name` for human readability
- Add explicit `AssetKey::canonical_string()` returning `namespace/name` for hashing/sorting
- Fingerprinting, TaskKey, and DAG ordering use `canonical_string()`

**File:** `docs/adr/012-asset-key-format.md`

---

### ADR-013: ID Type Wire Formats

**Decision Required:** Reconcile Rust ID types with proto documentation.

**Context:**
- Rust: `AssetId` is UUID v7, `TaskId/RunId/MaterializationId/EventId` are ULID
- Proto docs (common.proto line 23): Claims AssetId is ULID-based
- Golden tests need deterministic, correctly-typed IDs

**Decision:**
- Document correct wire formats: AssetId = UUID v7 string, others = ULID string
- Update proto documentation to match Rust types
- Golden tests use correct formats: `AssetId::from_str("uuid-v7-here")`, `TaskId::from_str("ulid-here")`

**File:** `docs/adr/013-id-wire-formats.md`

---

### ADR-014: Leader Election Strategy

**Decision Required:** Choose ONE leader election mechanism.

**Context:**
- Plan mentions both advisory locks AND row-based lease table
- These are different approaches with different semantics

**Decision (choose one):**
- **Option A:** Postgres advisory locks (requires dedicated connection per scheduler)
- **Option B:** Row-based lease with TTL (single table, CAS updates)

Recommend: **Option B** (row-based lease) for simpler connection management.

**File:** `docs/adr/014-leader-election.md`

---

### ADR-015: Postgres Orchestration Store

**Decision Required:** Confirm Postgres as orchestration store (not ledger-only).

**Context:**
- Design mentions "Postgres for live correctness" vs "GCS ledger for history"
- Store trait implementation needs this clarified

**Decision:**
- Postgres is the authoritative store for Run/Task state during execution
- GCS ledger is append-only audit trail (events, not state)
- Store trait operations are Postgres-backed

**File:** `docs/adr/015-postgres-orchestration-store.md`

---

## Gate A: Determinism (PRs 1-5)

### PR-1: Harden Existing Canonical JSON Module

**Goal:** Verify and harden the existing `arco_core::canonical_json` module. Add stronger float rejection tests and cross-language golden vectors.

**Note:** The module ALREADY EXISTS. This PR is verification + hardening, not creation.

---

### Task 1.1: Verify existing canonical_json implementation

**Files:**
- Review: `crates/arco-core/src/canonical_json.rs` (already exists)

**Step 1: Verify module already exists and exports correctly**

```bash
cargo test -p arco-core canonical_json
```

Expected: Tests pass (module already implemented per ADR-010)

**Step 2: Review current implementation**

The existing implementation should have:
- `to_canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, CanonicalJsonError>`
- `to_canonical_string<T: Serialize>(value: &T) -> Result<String, CanonicalJsonError>`
- Float rejection via `CanonicalJsonError::FloatNotAllowed`

If these exist, proceed to Task 1.2. If not, implement them.

---

### Task 1.2: Add stronger float rejection tests

**Files:**
- Modify: `crates/arco-core/src/canonical_json.rs`

**Step 1: Add explicit float rejection tests**

Add to the tests module:

```rust
    #[test]
    fn rejects_explicit_floats() {
        // ADR-010: All floats must be rejected
        let cases = vec![
            json!({"x": 1.25}),
            json!({"x": 1e3}),
            json!({"x": 0.1}),
            json!({"nested": {"f": 3.14159}}),
            json!([1.0, 2.0, 3.0]),
        ];

        for case in cases {
            let result = to_canonical_string(&case);
            // serde_json may coerce 1e3 to integer, so check both outcomes
            match result {
                Err(CanonicalJsonError::FloatNotAllowed) => { /* expected */ }
                Ok(s) => {
                    // If it succeeded, verify no decimal points
                    assert!(!s.contains('.'), "Float leaked through: {s}");
                }
            }
        }
    }

    #[test]
    fn rejects_nan_and_infinity() {
        // NaN/Inf are not valid JSON numbers. serde_json rejects them during `to_value()`.
        #[derive(Serialize)]
        struct Wrap {
            x: f64,
        }

        assert!(to_canonical_string(&Wrap { x: f64::NAN }).is_err());
        assert!(to_canonical_string(&Wrap { x: f64::INFINITY }).is_err());
    }
```

**Step 2: Run tests**

```bash
cargo test -p arco-core canonical_json::tests::rejects_explicit_floats
```

Expected: PASS

**Step 3: Commit**

```bash
git add crates/arco-core/src/canonical_json.rs
git commit -m "test(arco-core): add explicit float rejection tests for canonical JSON"
```

---

### Task 1.3: Add cross-language golden vector tests

**Files:**
- Create: `crates/arco-core/tests/canonical_json_golden.rs`
- Create: `crates/arco-core/tests/golden/canonical_json_vectors.json`

**Step 1: Create golden vectors file**

```json
{
  "vectors": [
    {
      "name": "simple_object_sorted_keys",
      "input": {"tenant": "acme", "date": "2025-01-15"},
      "expected_canonical": "{\"date\":\"2025-01-15\",\"tenant\":\"acme\"}"
    },
    {
      "name": "nested_object",
      "input": {"b": {"d": 2, "c": 1}, "a": 0},
      "expected_canonical": "{\"a\":0,\"b\":{\"c\":1,\"d\":2}}"
    },
    {
      "name": "array_preserves_order",
      "input": [3, 2, 1],
      "expected_canonical": "[3,2,1]"
    },
    {
      "name": "unicode_string",
      "input": {"emoji": "🎉", "chinese": "中文"},
      "expected_canonical": "{\"chinese\":\"中文\",\"emoji\":\"🎉\"}"
    },
    {
      "name": "escaped_string",
      "input": {"s": "a\"b\nc"},
      "expected_canonical": "{\"s\":\"a\\\"b\\nc\"}"
    },
    {
      "name": "negative_integer",
      "input": {"n": -42},
      "expected_canonical": "{\"n\":-42}"
    },
    {
      "name": "large_integer",
      "input": {"big": 9223372036854775807},
      "expected_canonical": "{\"big\":9223372036854775807}"
    }
  ],
  "python_reference": "json.dumps(input, sort_keys=True, separators=(',', ':'), ensure_ascii=False)"
}
```

**Step 2: Create golden vector test**

```rust
// crates/arco-core/tests/canonical_json_golden.rs

//! Golden vector tests for cross-language canonical JSON verification.
//!
//! These tests ensure Rust produces identical output to the Python reference:
//! `json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)`

use arco_core::canonical_json::to_canonical_string;
use serde_json::Value;

#[derive(Debug, serde::Deserialize)]
struct VectorFile {
    vectors: Vec<Vector>,
}

#[derive(Debug, serde::Deserialize)]
struct Vector {
    name: String,
    input: Value,
    expected_canonical: String,
}

/// Golden vectors that must match across Rust and Python implementations.
///
/// Source of truth is `tests/golden/canonical_json_vectors.json`, which can also
/// be consumed by a Python verifier script.
#[test]
fn canonical_json_golden_vectors() -> Result<(), Box<dyn std::error::Error>> {
    let file: VectorFile =
        serde_json::from_str(include_str!("golden/canonical_json_vectors.json"))?;

    for vector in file.vectors {
        let canonical = to_canonical_string(&vector.input)?;
        assert_eq!(canonical, vector.expected_canonical, "vector: {}", vector.name);
    }

    Ok(())
}
```

**Step 3: Run golden tests**

```bash
cargo test -p arco-core --test canonical_json_golden
```

Expected: PASS

**Step 4: Commit**

```bash
git add crates/arco-core/tests/
git commit -m "test(arco-core): add cross-language golden vectors for canonical JSON"
```

---

### Task 1.4: Remove or deprecate canonical_partition_key function

**Files:**
- Modify: `crates/arco-core/src/canonical_json.rs`

**Context:** The `canonical_partition_key()` function duplicates functionality from `arco_core::partition::PartitionKey`. Per ADR-011, PartitionKey is THE canonical partition identity.

**Step 1: Deprecate the function (don't remove yet for backward compatibility)**

```rust
/// Produces a canonical partition key string from dimension key-value pairs.
///
/// **DEPRECATED:** Use `arco_core::partition::PartitionKey` instead.
/// This function exists for backward compatibility but will be removed.
/// The typed `PartitionKey` provides better safety and cross-language guarantees.
#[deprecated(
    since = "0.2.0",
    note = "Use arco_core::partition::PartitionKey::canonical_string() instead"
)]
#[must_use = "canonical partition key should be used for identity"]
pub fn canonical_partition_key<S: std::hash::BuildHasher>(
    dimensions: &HashMap<String, String, S>,
) -> Result<String, CanonicalJsonError> {
    to_canonical_string(dimensions)
}
```

**Step 2: Update any callers to use PartitionKey**

Search for usages and update them to use `PartitionKey`.

**Step 3: Commit**

```bash
git add crates/arco-core/src/canonical_json.rs
git commit -m "refactor(arco-core): deprecate canonical_partition_key in favor of PartitionKey"
```

---

### Task 1.5: Verify CI gates

**Files:**
- None (verification only)

**Step 1: Run full test suite**

```bash
cargo test -p arco-core
```

Expected: All tests PASS

**Step 2: Run clippy**

```bash
cargo clippy -p arco-core -- -D warnings
```

Expected: No warnings

**Step 3: Commit PR-1 completion**

```bash
git add -A
git commit -m "chore(arco-core): PR-1 complete - canonical JSON hardening"
```

---

## PR-2: TaskKey Semantic Identity with Type-Safe PartitionKey

**Goal:** Define deterministic semantic identity for tasks using typed `PartitionKey`, not raw strings.

**Starting point note:** `crates/arco-flow/src/task_key.rs` exists today but currently stores `partition_key` as a raw string. This PR migrates it to `Option<PartitionKey>` and updates all call sites.

---

### Task 2.1: Add TaskOperation enum

**Files:**
- Modify: `crates/arco-flow/src/task_key.rs`
- Modify: `crates/arco-flow/src/lib.rs`

**Step 1: Create TaskOperation enum**

```rust
// crates/arco-flow/src/task_key.rs

//! Semantic task identity for deterministic ordering and deduplication.
//!
//! `TaskKey` provides a stable identity for tasks based on their semantic
//! meaning (asset + partition + operation), independent of generated IDs.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use arco_core::partition::PartitionKey;
use crate::plan::AssetKey;

/// The type of operation a task performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskOperation {
    /// Produce asset data (default).
    Materialize,
    /// Run data quality checks.
    Check,
    /// Historical data backfill chunk.
    Backfill,
}

impl Default for TaskOperation {
    fn default() -> Self {
        Self::Materialize
    }
}

impl std::fmt::Display for TaskOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Materialize => write!(f, "materialize"),
            Self::Check => write!(f, "check"),
            Self::Backfill => write!(f, "backfill"),
        }
    }
}

impl PartialOrd for TaskOperation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaskOperation {
    fn cmp(&self, other: &Self) -> Ordering {
        // Stable ordering: Materialize < Check < Backfill
        fn rank(op: &TaskOperation) -> u8 {
            match op {
                TaskOperation::Materialize => 0,
                TaskOperation::Check => 1,
                TaskOperation::Backfill => 2,
            }
        }
        rank(self).cmp(&rank(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_operation_ordering_is_stable() {
        assert!(TaskOperation::Materialize < TaskOperation::Check);
        assert!(TaskOperation::Check < TaskOperation::Backfill);
    }

    #[test]
    fn task_operation_default_is_materialize() {
        assert_eq!(TaskOperation::default(), TaskOperation::Materialize);
    }
}
```

**Step 2: Export from lib.rs**

Add to `crates/arco-flow/src/lib.rs`:

```rust
pub mod task_key;
```

**Step 3: Run test**

```bash
cargo test -p arco-flow task_key
```

Expected: All tests PASS

**Step 4: Commit**

```bash
git add crates/arco-flow/src/task_key.rs crates/arco-flow/src/lib.rs
git commit -m "feat(arco-flow): add TaskOperation enum with stable ordering"
```

---

### Task 2.2: Add TaskKey struct with typed PartitionKey

**Files:**
- Modify: `crates/arco-flow/src/task_key.rs`

**Step 1: Add TaskKey struct**

Add after TaskOperation in `crates/arco-flow/src/task_key.rs`:

```rust
/// Semantic identity for a task.
///
/// Used for:
/// - Deterministic topological sort tie-breaking
/// - Stable task ordering before fingerprinting
/// - Duplicate detection (same TaskKey = same logical task)
///
/// Does NOT include `task_id` (which is generated and non-deterministic).
///
/// **Type Safety:** Uses `Option<PartitionKey>` (not `Option<String>`) to ensure
/// partition keys are always in canonical form. This prevents "two canonical formats" bugs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskKey {
    /// The asset this task operates on.
    pub asset_key: AssetKey,

    /// Canonical partition key (if partitioned).
    /// Uses typed `PartitionKey` for cross-language determinism (ADR-011).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<PartitionKey>,

    /// The operation type.
    pub operation: TaskOperation,
}

impl TaskKey {
    /// Creates a new TaskKey for a non-partitioned asset.
    #[must_use]
    pub fn new(asset_key: AssetKey, operation: TaskOperation) -> Self {
        Self {
            asset_key,
            partition_key: None,
            operation,
        }
    }

    /// Creates a new TaskKey with a partition key.
    #[must_use]
    pub fn with_partition(
        asset_key: AssetKey,
        partition_key: PartitionKey,
        operation: TaskOperation,
    ) -> Self {
        Self {
            asset_key,
            partition_key: Some(partition_key),
            operation,
        }
    }

    /// Returns the canonical string representation for sorting/hashing.
    ///
    /// Format: `{namespace}/{name}[{partition_canonical}]:{operation}`
    /// - Uses `/` separator per ADR-012
    /// - Partition uses `PartitionKey::canonical_string()` format
    /// - Includes operation to distinguish Materialize vs Check for same asset
    #[must_use]
    pub fn canonical_string(&self) -> String {
        let base = match &self.partition_key {
            Some(pk) => format!(
                "{}/{}[{}]",
                self.asset_key.namespace,
                self.asset_key.name,
                pk.canonical_string()
            ),
            None => format!("{}/{}", self.asset_key.namespace, self.asset_key.name),
        };
        format!("{}:{}", base, self.operation)
    }
}

impl PartialOrd for TaskKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaskKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // Order by: namespace, name, partition canonical string, operation
        self.asset_key
            .namespace
            .cmp(&other.asset_key.namespace)
            .then_with(|| self.asset_key.name.cmp(&other.asset_key.name))
            .then_with(|| {
                let self_pk = self.partition_key.as_ref().map(PartitionKey::canonical_string);
                let other_pk = other.partition_key.as_ref().map(PartitionKey::canonical_string);
                self_pk.cmp(&other_pk)
            })
            .then_with(|| self.operation.cmp(&other.operation))
    }
}

impl std::fmt::Display for TaskKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Use canonical_string() directly (operation already included)
        write!(f, "{}", self.canonical_string())
    }
}
```

**Step 2: Add tests**

Add to tests module:

```rust
    #[test]
    fn task_key_ordering_is_deterministic() {
        let key_a = TaskKey::new(
            AssetKey::new("raw", "events"),
            TaskOperation::Materialize,
        );
        let key_b = TaskKey::new(
            AssetKey::new("staging", "events"),
            TaskOperation::Materialize,
        );

        // raw < staging (lexicographic)
        assert!(key_a < key_b);
    }

    #[test]
    fn task_key_with_partition_orders_correctly() {
        use arco_core::partition::{PartitionKey, ScalarValue};

        let mut pk1 = PartitionKey::new();
        pk1.insert("date", ScalarValue::Date("2025-01-01".into()));

        let mut pk2 = PartitionKey::new();
        pk2.insert("date", ScalarValue::Date("2025-01-02".into()));

        let key_a = TaskKey::with_partition(
            AssetKey::new("raw", "events"),
            pk1,
            TaskOperation::Materialize,
        );
        let key_b = TaskKey::with_partition(
            AssetKey::new("raw", "events"),
            pk2,
            TaskOperation::Materialize,
        );

        // Same asset, different partition: 01 < 02
        assert!(key_a < key_b);
    }

    #[test]
    fn task_key_operation_breaks_ties() {
        let materialize = TaskKey::new(
            AssetKey::new("raw", "events"),
            TaskOperation::Materialize,
        );
        let check = TaskKey::new(
            AssetKey::new("raw", "events"),
            TaskOperation::Check,
        );

        // Same asset: Materialize < Check
        assert!(materialize < check);
    }

    #[test]
    fn task_key_display_format() {
        use arco_core::partition::{PartitionKey, ScalarValue};

        let mut pk = PartitionKey::new();
        pk.insert("date", ScalarValue::Date("2025-01-01".into()));

        let key = TaskKey::with_partition(
            AssetKey::new("raw", "events"),
            pk,
            TaskOperation::Materialize,
        );

        // Format: namespace/name[partition_canonical]:operation
        let display = key.to_string();
        assert!(display.starts_with("raw/events["));
        assert!(display.contains("date=d:2025-01-01"));
        assert!(display.ends_with("]:materialize"));
    }

    #[test]
    fn task_key_canonical_string_without_partition() {
        let key = TaskKey::new(
            AssetKey::new("staging", "cleaned"),
            TaskOperation::Check,
        );

        assert_eq!(key.canonical_string(), "staging/cleaned:check");
    }

    #[test]
    fn task_key_operation_included_in_canonical() {
        let materialize = TaskKey::new(
            AssetKey::new("raw", "events"),
            TaskOperation::Materialize,
        );
        let check = TaskKey::new(
            AssetKey::new("raw", "events"),
            TaskOperation::Check,
        );

        // Same asset, different operation = different canonical string
        assert_ne!(materialize.canonical_string(), check.canonical_string());
        assert_eq!(materialize.canonical_string(), "raw/events:materialize");
        assert_eq!(check.canonical_string(), "raw/events:check");
    }
```

**Step 3: Run tests**

```bash
cargo test -p arco-flow task_key
```

Expected: All tests PASS

**Step 4: Commit**

```bash
git add crates/arco-flow/src/task_key.rs
git commit -m "feat(arco-flow): add TaskKey with typed PartitionKey and deterministic ordering"
```

---

### Task 2.3: Close partition identity end-to-end (TaskSpec uses PartitionKey)

**Goal:** Ensure there is exactly ONE partition identity format in `arco-flow`: `arco_core::partition::PartitionKey` (ADR-011). This eliminates the lingering `BTreeMap<String, String>` partition representation in plan/task specs.

**Files:**
- Modify: `crates/arco-flow/src/plan.rs`
- Modify: `crates/arco-flow/tests/` (call sites)
- Modify: `crates/arco-flow/benches/` (call sites)

**Step 1: Migrate TaskSpec.partition_key to typed PartitionKey**

- Change:
  - `TaskSpec.partition_key: Option<BTreeMap<String, String>>`
  - to:
  - `TaskSpec.partition_key: Option<arco_core::partition::PartitionKey>`

**Step 2: Update all call sites**

- Most call sites already use `partition_key: None`; update compilation errors accordingly.

**Step 3: Update fingerprint normalization expectations**

- PR-4 fingerprinting must treat partition identity via `PartitionKey::canonical_string()` (usually via `TaskKey::canonical_string()`).
- Do not fingerprint ad-hoc JSON maps for partition identity.

**Step 4: Add a focused test**

- Add a unit test that:
  - Builds a `TaskSpec` with a non-empty `PartitionKey`
  - Verifies the plan fingerprint changes if the partition canonical string changes

**Step 5: Commit**

```bash
git add crates/arco-flow/src/plan.rs crates/arco-flow/tests crates/arco-flow/benches
git commit -m "refactor(arco-flow): use PartitionKey for TaskSpec partition identity"
```

---

### Task 2.4: Add AssetKey::canonical_string() method

**Files:**
- Modify: `crates/arco-flow/src/plan.rs`

**Context:** Per ADR-012, `Display` uses `.` but canonical identity uses `/`.

**Step 1: Add canonical_string method**

Add to `AssetKey` impl in `crates/arco-flow/src/plan.rs`:

```rust
impl AssetKey {
    // ... existing methods ...

    /// Returns the canonical string representation for sorting/hashing.
    ///
    /// Format: `namespace/name` (ADR-012)
    ///
    /// **Note:** `Display` uses `.` for human readability, but `canonical_string()`
    /// uses `/` for hashing and sorting consistency.
    #[must_use]
    pub fn canonical_string(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}
```

**Step 2: Add test**

```rust
    #[test]
    fn asset_key_canonical_vs_display() {
        let key = AssetKey::new("raw", "events");

        // Display uses dot (human readable)
        assert_eq!(key.to_string(), "raw.events");

        // Canonical uses slash (for hashing/sorting)
        assert_eq!(key.canonical_string(), "raw/events");
    }
```

**Step 3: Commit**

```bash
git add crates/arco-flow/src/plan.rs
git commit -m "feat(arco-flow): add AssetKey::canonical_string() per ADR-012"
```

---

### Task 2.5: Export TaskKey from prelude and verify CI

**Files:**
- Modify: `crates/arco-flow/src/lib.rs`

**Step 1: Update prelude**

```rust
pub mod prelude {
    pub use crate::error::{Error, Result};
    pub use crate::outbox::{EventSink, InMemoryOutbox, LedgerWriter};
    pub use crate::plan::{AssetKey, Plan, PlanBuilder, TaskSpec};
    pub use crate::run::{Run, RunState};
    pub use crate::runner::{RunContext, Runner, TaskResult};
    pub use crate::scheduler::Scheduler;
    pub use crate::task::{TaskExecution, TaskState};
    pub use crate::task_key::{TaskKey, TaskOperation};
}
```

**Step 2: Run full test suite**

```bash
cargo test -p arco-flow
```

Expected: All tests PASS

**Step 3: Commit**

```bash
git add crates/arco-flow/src/lib.rs
git commit -m "chore(arco-flow): PR-2 complete - TaskKey semantic identity"
```

---

## PR-3: ResourceRequirements Integer-Only (Already Done)

**Status:** This PR is already implemented based on the file modifications shown.

**Verification:**

The `ResourceRequirements` struct in `crates/arco-flow/src/plan.rs` now uses:
- `memory_bytes: u64`
- `cpu_millicores: u64`
- `timeout_ms: u64`

The proto in `proto/arco/v1/orchestration.proto` uses:
- `uint64 memory_bytes = 1;`
- `uint64 cpu_millicores = 2;`
- `uint64 timeout_ms = 3;`

**Note on Proto Compatibility:**

If any clients are deployed that use the old float-based `cpu_cores` field:
1. `cpu_millicores` reuses field number `2` but changes the wire type (`double` → `uint64`), which is **wire-incompatible**
2. Old clients may fail to parse the message (wire-type mismatch), or silently drop the field depending on the implementation
3. This is safe only if nothing is deployed yet that depends on the old schema

If backward compatibility is required:
- Do **not** reuse field numbers with a different wire type
- Prefer `ResourceRequirementsV2` or add new fields with new field numbers (and keep the old ones deprecated)

**Task 3.1: Verify ResourceRequirements uses integers**

```bash
cargo test -p arco-flow resource
```

Expected: PASS (already implemented)

---

## PR-4: Plan Fingerprint with NormalizedPlanSpec

**Goal:** Implement deterministic plan fingerprinting using TaskKey for identity.

---

### Task 4.1: Create NormalizedPlanSpec struct

**Files:**
- Modify: `crates/arco-flow/src/plan.rs`

**Step 1: Add NormalizedPlanSpec**

```rust
use arco_core::canonical_json::to_canonical_bytes;
use sha2::{Sha256, Digest};

/// Normalized representation of a plan for fingerprinting.
///
/// Excludes non-deterministic fields (plan_id, created_at) and uses
/// TaskKey for stable task identity instead of generated TaskIds.
#[derive(Debug, Clone, Serialize)]
struct NormalizedPlanSpec {
    /// Spec version for future evolution
    spec_version: u8,
    /// Tenant identifier
    tenant_id: String,
    /// Workspace identifier
    workspace_id: String,
    /// Tasks sorted by TaskKey canonical string
    tasks: Vec<NormalizedTaskSpec>,
}

/// Normalized task specification.
#[derive(Debug, Clone, Serialize)]
struct NormalizedTaskSpec {
    /// Task identity (deterministic)
    task_key: String,
    /// Upstream dependencies as TaskKey canonical strings (sorted)
    upstream_keys: Vec<String>,
    /// Priority (semantic scheduling intent)
    priority: i32,
    /// Resource requirements
    resources: NormalizedResources,
}

/// Normalized resource requirements.
#[derive(Debug, Clone, Serialize)]
struct NormalizedResources {
    memory_bytes: u64,
    cpu_millicores: u64,
    timeout_ms: u64,
}

impl NormalizedPlanSpec {
    /// Creates a normalized spec from a Plan.
    ///
    /// # Errors
    ///
    /// Returns error if a task references a non-existent upstream task.
    fn from_plan(plan: &Plan) -> Result<Self, Error> {
        // Build TaskId -> TaskKey mapping
        let mut id_to_key: std::collections::HashMap<TaskId, String> =
            std::collections::HashMap::new();

        for task in &plan.tasks {
            // TaskKey includes partition identity (PartitionKey::canonical_string) and operation.
            // TODO: once TaskSpec has an explicit operation, use that instead of Materialize.
            let task_key = match &task.partition_key {
                Some(pk) => crate::task_key::TaskKey::with_partition(
                    task.asset_key.clone(),
                    pk.clone(),
                    crate::task_key::TaskOperation::Materialize,
                ),
                None => crate::task_key::TaskKey::new(
                    task.asset_key.clone(),
                    crate::task_key::TaskOperation::Materialize,
                ),
            };
            id_to_key.insert(task.task_id, task_key.canonical_string());
        }

        // Build normalized tasks
        let mut normalized_tasks: Vec<NormalizedTaskSpec> = Vec::new();

        for task in &plan.tasks {
            let task_key = id_to_key.get(&task.task_id)
                .ok_or_else(|| Error::PlanGenerationFailed {
                    message: format!("task {} not in id_to_key map", task.task_id),
                })?
                .clone();

            // Resolve upstream task IDs to TaskKeys
            let mut upstream_keys: Vec<String> = Vec::new();
            for upstream_id in &task.upstream_task_ids {
                let upstream_key = id_to_key.get(upstream_id)
                    .ok_or_else(|| Error::PlanGenerationFailed {
                        message: format!(
                            "task {} references missing upstream {}",
                            task.task_id, upstream_id
                        ),
                    })?;
                upstream_keys.push(upstream_key.clone());
            }
            // Sort for determinism
            upstream_keys.sort();

            normalized_tasks.push(NormalizedTaskSpec {
                task_key,
                upstream_keys,
                priority: task.priority,
                resources: NormalizedResources {
                    memory_bytes: task.resources.memory_bytes,
                    cpu_millicores: task.resources.cpu_millicores,
                    timeout_ms: task.resources.timeout_ms,
                },
            });
        }

        // Sort tasks by canonical key for determinism
        normalized_tasks.sort_by(|a, b| a.task_key.cmp(&b.task_key));

        Ok(Self {
            spec_version: 1,
            tenant_id: plan.tenant_id.clone(),
            workspace_id: plan.workspace_id.clone(),
            tasks: normalized_tasks,
        })
    }
}
```

**Step 2: Update fingerprint computation**

Update the `compute_fingerprint` function (or add it):

```rust
impl Plan {
    /// Computes a deterministic fingerprint for the plan.
    ///
    /// The fingerprint is stable across:
    /// - Different generated TaskIds/PlanIds
    /// - Different task insertion orders
    /// - Identical logical plans always produce identical fingerprints
    ///
    /// Format: `v1:sha256:{hex_digest}`
    fn compute_fingerprint(&self) -> Result<String, Error> {
        let normalized = NormalizedPlanSpec::from_plan(self)?;

        let canonical_bytes = to_canonical_bytes(&normalized)
            .map_err(|e| Error::PlanGenerationFailed {
                message: format!("canonical JSON serialization failed: {e}"),
            })?;

        let mut hasher = Sha256::new();
        hasher.update(&canonical_bytes);
        let hash = hasher.finalize();

        Ok(format!("v1:sha256:{}", hex::encode(hash)))
    }
}
```

**Step 3: Add test for fingerprint stability**

```rust
    #[test]
    fn fingerprint_stable_across_task_insertion_order() -> Result<(), Error> {
        use std::str::FromStr;

        // Use deterministic IDs for testing
        let task_id_1 = TaskId::from_str("01HQXYZ123456789ABCDEFGHJK")?;
        let task_id_2 = TaskId::from_str("01HQXYZ123456789ABCDEFGHJL")?;
        let asset_id_1 = AssetId::from_str("0193a7b8-c9d0-7000-8000-000000000001")?;
        let asset_id_2 = AssetId::from_str("0193a7b8-c9d0-7000-8000-000000000002")?;

        // Build plan with tasks in order: 1, 2
        let plan1 = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_id_1,
                asset_id: asset_id_1,
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_id_2,
                asset_id: asset_id_2,
                asset_key: AssetKey::new("staging", "cleaned"),
                partition_key: None,
                upstream_task_ids: vec![task_id_1],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .build()?;

        // Build plan with tasks in REVERSE order: 2, 1
        let plan2 = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id: task_id_2,
                asset_id: asset_id_2,
                asset_key: AssetKey::new("staging", "cleaned"),
                partition_key: None,
                upstream_task_ids: vec![task_id_1],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_id_1,
                asset_id: asset_id_1,
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .build()?;

        // Fingerprints should be identical
        assert_eq!(plan1.fingerprint, plan2.fingerprint);

        Ok(())
    }

    #[test]
    fn fingerprint_fails_on_missing_upstream() {
        let task_id = TaskId::generate();
        let missing_upstream = TaskId::generate();

        let result = PlanBuilder::new("tenant", "workspace")
            .add_task(TaskSpec {
                task_id,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![missing_upstream], // Not in plan!
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .build();

        assert!(result.is_err());
    }
```

**Step 4: Commit**

```bash
git add crates/arco-flow/src/plan.rs
git commit -m "feat(arco-flow): deterministic fingerprinting with NormalizedPlanSpec"
```

---

## PR-5: DAG Determinism + Cycle Path Reporting

**Goal:** Make DAG toposort insertion-order-independent and report full cycle paths.

---

### Task 5.1: Fix DAG toposort to use stable ordering

**Files:**
- Modify: `crates/arco-flow/src/dag.rs`

**Context:** The current implementation uses insertion order for tie-breaking, which makes the result depend on how tasks were added. For fingerprinting, we need insertion-order-independence.

**Step 1: Add key-based ordering option**

The DAG should accept a key function for deterministic ordering:

```rust
use std::collections::BTreeSet;

impl<T> Dag<T>
where
    T: Clone + Eq + Hash + Display,
{
    /// Returns a topologically sorted list of nodes with deterministic ordering.
    ///
    /// Uses a caller-provided key function for tie-breaking when multiple nodes
    /// have zero in-degree. This makes the result independent of insertion order.
    ///
    /// **Important:** The key must be semantic (e.g., `TaskKey::canonical_string()`),
    /// not a generated ID (`TaskId`), otherwise the "deterministic" order will still
    /// change across plan regenerations.
    ///
    /// **Complexity:** O((V+E) log V) due to BTreeSet operations.
    ///
    /// # Errors
    ///
    /// Returns an error if the graph contains a cycle.
    pub fn toposort_by_key<K, F>(&self, key_fn: F) -> Result<Vec<T>>
    where
        K: Ord,
        F: Fn(&T) -> K,
    {
        let node_count = self.graph.node_count();
        if node_count == 0 {
            return Ok(Vec::new());
        }

        // Compute in-degrees
        let mut in_degree: HashMap<NodeIndex, usize> = HashMap::with_capacity(node_count);
        for idx in self.graph.node_indices() {
            in_degree.insert(idx, 0);
        }
        for edge in self.graph.edge_references() {
            *in_degree.entry(edge.target()).or_insert(0) += 1;
        }

        // Use BTreeSet<(K, usize)> for deterministic ordering; usize is the NodeIndex.
        // The key controls tie-breaking, and the index avoids collisions.
        let mut ready: BTreeSet<(K, usize)> = BTreeSet::new();

        for idx in self.graph.node_indices() {
            if in_degree.get(&idx).copied().unwrap_or(0) == 0 {
                if let Some(value) = self.graph.node_weight(idx) {
                    ready.insert((key_fn(value), idx.index()));
                }
            }
        }

        let mut result = Vec::with_capacity(node_count);

        while let Some((_, idx)) = ready.pop_first() {
            let idx = NodeIndex::new(idx);
            let node = self
                .graph
                .node_weight(idx)
                .ok_or_else(|| Error::DagNodeNotFound {
                    node: format!("index {}", idx.index()),
                })?
                .clone();
            result.push(node);

            for neighbor in self.graph.neighbors_directed(idx, Direction::Outgoing) {
                if let Some(deg) = in_degree.get_mut(&neighbor) {
                    *deg = deg.saturating_sub(1);
                    if *deg == 0 {
                        if let Some(value) = self.graph.node_weight(neighbor) {
                            ready.insert((key_fn(value), neighbor.index()));
                        }
                    }
                }
            }
        }

        // Cycle detection
        if result.len() != node_count {
            return Err(self.find_cycle_path()?);
        }

        Ok(result)
    }
}
```

**Step 2: Add cycle path finding (iterative, not recursive)**

```rust
impl<T> Dag<T>
where
    T: Clone + Eq + Hash + Display,
{
    /// Finds a cycle path using iterative DFS with color marking.
    ///
    /// **Complexity:** O(V+E), no recursion depth risk.
    fn find_cycle_path(&self) -> Result<Error> {
        // Color marking: 0 = white (unvisited), 1 = gray (in progress), 2 = black (done)
        let mut color: HashMap<NodeIndex, u8> = HashMap::new();
        let mut parent: HashMap<NodeIndex, NodeIndex> = HashMap::new();

        for idx in self.graph.node_indices() {
            color.insert(idx, 0);
        }

        // Start DFS from each unvisited node
        for start in self.graph.node_indices() {
            if color.get(&start).copied().unwrap_or(0) != 0 {
                continue;
            }

            let mut stack = vec![start];

            while let Some(node) = stack.last().copied() {
                match color.get(&node).copied().unwrap_or(0) {
                    0 => {
                        // Mark as in-progress
                        color.insert(node, 1);

                        // Push neighbors
                        for neighbor in self.graph.neighbors_directed(node, Direction::Outgoing) {
                            match color.get(&neighbor).copied().unwrap_or(0) {
                                0 => {
                                    parent.insert(neighbor, node);
                                    stack.push(neighbor);
                                }
                                1 => {
                                    // Found cycle! Build path
                                    let mut cycle_path = vec![neighbor];
                                    let mut current = node;
                                    while current != neighbor {
                                        cycle_path.push(current);
                                        current = match parent.get(&current) {
                                            Some(&p) => p,
                                            None => break,
                                        };
                                    }
                                    cycle_path.push(neighbor);
                                    cycle_path.reverse();

                                    let cycle_names: Vec<String> = cycle_path
                                        .iter()
                                        .filter_map(|&idx| self.graph.node_weight(idx).map(|v| v.to_string()))
                                        .collect();

                                    return Ok(Error::CycleDetected { cycle: cycle_names });
                                }
                                _ => { /* black, skip */ }
                            }
                        }
                    }
                    1 => {
                        // Done with this node
                        color.insert(node, 2);
                        stack.pop();
                    }
                    _ => {
                        stack.pop();
                    }
                }
            }
        }

        // No cycle found (shouldn't happen if called from toposort failure)
        Ok(Error::CycleDetected { cycle: vec!["unknown".to_string()] })
    }
}
```

**Step 3: Add tests for insertion-order independence**

```rust
    #[test]
    fn toposort_by_key_regardless_of_insertion_order() -> Result<()> {
        // Build same logical DAG with different insertion orders

        // Order 1: a, b, c
        let mut dag1: Dag<String> = Dag::new();
        let a1 = dag1.add_node("a".into());
        let b1 = dag1.add_node("b".into());
        let c1 = dag1.add_node("c".into());
        dag1.add_edge(a1, c1)?;
        dag1.add_edge(b1, c1)?;

        // Order 2: c, b, a (reverse)
        let mut dag2: Dag<String> = Dag::new();
        let c2 = dag2.add_node("c".into());
        let b2 = dag2.add_node("b".into());
        let a2 = dag2.add_node("a".into());
        dag2.add_edge(a2, c2)?;
        dag2.add_edge(b2, c2)?;

        // Order 3: b, a, c
        let mut dag3: Dag<String> = Dag::new();
        let b3 = dag3.add_node("b".into());
        let a3 = dag3.add_node("a".into());
        let c3 = dag3.add_node("c".into());
        dag3.add_edge(a3, c3)?;
        dag3.add_edge(b3, c3)?;

        // All should produce the same deterministic result
        let sorted1 = dag1.toposort_by_key(|v| v.clone())?;
        let sorted2 = dag2.toposort_by_key(|v| v.clone())?;
        let sorted3 = dag3.toposort_by_key(|v| v.clone())?;

        assert_eq!(sorted1, sorted2);
        assert_eq!(sorted2, sorted3);

        // Expected: a, b (alphabetically), then c (depends on both)
        assert_eq!(sorted1, vec!["a", "b", "c"]);

        Ok(())
    }

    #[test]
    fn cycle_detection_reports_full_path() -> Result<()> {
        let mut dag: Dag<String> = Dag::new();
        let a = dag.add_node("a".into());
        let b = dag.add_node("b".into());
        let c = dag.add_node("c".into());
        dag.add_edge(a, b)?;
        dag.add_edge(b, c)?;
        dag.add_edge(c, a)?; // Creates cycle: a -> b -> c -> a

        let result = dag.toposort_by_key(|v| v.clone());

        match result {
            Err(Error::CycleDetected { cycle }) => {
                // Should contain all nodes in the cycle
                assert!(cycle.len() >= 3, "Cycle path should have at least 3 nodes");
                // First and last should be the same (cycle completed)
                assert_eq!(cycle.first(), cycle.last());
            }
            _ => panic!("Expected CycleDetected error"),
        }

        Ok(())
    }
```

**Step 4: Commit**

```bash
git add crates/arco-flow/src/dag.rs
git commit -m "feat(arco-flow): insertion-order-independent toposort with cycle path reporting"
```

---

## Gate B: State Machine Observability (PRs 6-8)

### PR-6: TransitionReason Enum

**Goal:** Add explicit reasons for all state transitions.

---

### Task 6.1: Add TransitionReason enum

**Files:**
- Modify: `crates/arco-flow/src/task.rs`

**Step 1: Add enum**

```rust
/// Reason for a task state transition.
///
/// Every state transition must have an explicit reason for:
/// - Auditing and debugging
/// - Metrics and alerting
/// - Replay and recovery decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionReason {
    // --- Happy path ---
    /// Run started, task moved from Planned to Pending
    RunStarted,
    /// All upstream dependencies completed successfully
    DependenciesSatisfied,
    /// Task added to dispatch queue
    EnqueuedForDispatch,
    /// Task sent to worker
    DispatchedToWorker,
    /// Worker acknowledged task receipt
    WorkerAcknowledged,
    /// Worker began execution
    ExecutionStarted,
    /// Task completed successfully
    ExecutionSucceeded,

    // --- Failure path ---
    /// Worker reported failure
    ExecutionFailed,
    /// Task exceeded timeout
    TimedOut,
    /// Worker missed heartbeat deadline
    HeartbeatTimeout,
    /// Worker missed dispatch-ack deadline
    DispatchAckTimeout,
    /// Upstream task failed
    UpstreamFailed,

    // --- Recovery path ---
    /// Task queued for retry after failure
    RetryScheduled,
    /// Retry timer expired, task ready again
    RetryTimerExpired,

    // --- Cancellation path ---
    /// User requested cancellation
    UserCancelled,
    /// System cancelled (e.g., run timeout)
    SystemCancelled,
    /// Parent run was cancelled
    RunCancelled,

    // --- Skip path ---
    /// Skipped because upstream failed
    SkippedDueToUpstreamFailure,
    /// Skipped because asset is up-to-date
    SkippedUpToDate,
}

impl std::fmt::Display for TransitionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Use snake_case for logging consistency
        let s = serde_json::to_string(self).unwrap_or_else(|_| "unknown".to_string());
        // Remove quotes from JSON string
        write!(f, "{}", s.trim_matches('"'))
    }
}
```

**Step 2: Add run-level transition reasons**

```rust
/// Reason for a run state transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunTransitionReason {
    /// Run created
    Created,
    /// Run started executing
    Started,
    /// All tasks completed successfully
    AllTasksSucceeded,
    /// One or more tasks failed
    TasksFailed,
    /// User requested cancellation
    UserCancelled,
    /// System requested cancellation
    SystemCancelled,
    /// Run exceeded maximum duration
    TimedOut,
}
```

**Step 3: Update TaskExecution to track transition reason**

Add to `TaskExecution`:

```rust
    /// Reason for the most recent state transition.
    pub last_transition_reason: Option<TransitionReason>,

    /// Timestamp of the most recent state transition.
    pub last_transition_at: Option<DateTime<Utc>>,
```

**Step 4: Commit**

```bash
git add crates/arco-flow/src/task.rs
git commit -m "feat(arco-flow): add TransitionReason enum for state machine observability"
```

---

### PR-7: Two-Timeout Heartbeat Model

**Goal:** Implement dispatch-ack timeout and running heartbeat timeout separately.

---

### Task 7.1: Add timeout fields to TaskExecution

**Files:**
- Modify: `crates/arco-flow/src/task.rs`

**Step 1: Add timeout tracking fields**

```rust
/// Execution state for a single task within a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecution {
    // ... existing fields ...

    /// When the task was dispatched to a worker.
    /// Used for dispatch-ack timeout calculation.
    pub dispatched_at: Option<DateTime<Utc>>,

    /// Most recent heartbeat from the worker.
    /// Used for running heartbeat timeout calculation.
    pub last_heartbeat: Option<DateTime<Utc>>,

    /// When retry should be attempted (if in RetryWait state).
    pub retry_at: Option<DateTime<Utc>>,
}
```

**Step 2: Add timeout configuration**

```rust
/// Timeout configuration for task execution.
#[derive(Debug, Clone, Copy)]
pub struct TaskTimeoutConfig {
    /// Time for worker to acknowledge dispatch (default: 30s).
    /// If exceeded, task is requeued for dispatch.
    pub dispatch_ack_timeout: Duration,

    /// Time between heartbeats during execution (default: 60s).
    /// If exceeded, task is marked as zombie and may be requeued.
    pub heartbeat_timeout: Duration,

    /// Maximum execution time for a task (default: from ResourceRequirements).
    pub max_execution_time: Duration,
}

impl Default for TaskTimeoutConfig {
    fn default() -> Self {
        Self {
            dispatch_ack_timeout: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(60),
            max_execution_time: Duration::from_secs(3600),
        }
    }
}
```

**Step 3: Add zombie detection**

```rust
impl TaskExecution {
    /// Returns true if this task is a "zombie" (missed heartbeat deadline).
    ///
    /// Zombie tasks should be requeued for dispatch.
    pub fn is_zombie(&self, now: DateTime<Utc>, config: &TaskTimeoutConfig) -> bool {
        match self.state {
            TaskState::Dispatched => {
                // Check dispatch-ack timeout
                if let Some(dispatched_at) = self.dispatched_at {
                    let deadline = dispatched_at + chrono::Duration::from_std(config.dispatch_ack_timeout)
                        .unwrap_or_else(|_| chrono::Duration::seconds(30));
                    now > deadline
                } else {
                    false
                }
            }
            TaskState::Running => {
                // Check heartbeat timeout
                if let Some(last_hb) = self.last_heartbeat {
                    let deadline = last_hb + chrono::Duration::from_std(config.heartbeat_timeout)
                        .unwrap_or_else(|_| chrono::Duration::seconds(60));
                    now > deadline
                } else {
                    // No heartbeat ever received - use dispatched_at as fallback
                    if let Some(dispatched_at) = self.dispatched_at {
                        let deadline = dispatched_at + chrono::Duration::from_std(config.heartbeat_timeout)
                            .unwrap_or_else(|_| chrono::Duration::seconds(60));
                        now > deadline
                    } else {
                        false
                    }
                }
            }
            _ => false,
        }
    }
}
```

**Step 4: Add tests**

```rust
    #[test]
    fn zombie_detection_dispatched_state() {
        let config = TaskTimeoutConfig::default();
        let now = Utc::now();

        let mut task = TaskExecution::new(TaskId::generate());
        task.state = TaskState::Dispatched;
        task.dispatched_at = Some(now - chrono::Duration::seconds(60)); // 60s ago

        // dispatch_ack_timeout is 30s, so this should be zombie
        assert!(task.is_zombie(now, &config));

        // But not if dispatched recently
        task.dispatched_at = Some(now - chrono::Duration::seconds(10));
        assert!(!task.is_zombie(now, &config));
    }

    #[test]
    fn zombie_detection_running_state() {
        let config = TaskTimeoutConfig::default();
        let now = Utc::now();

        let mut task = TaskExecution::new(TaskId::generate());
        task.state = TaskState::Running;
        task.last_heartbeat = Some(now - chrono::Duration::seconds(120)); // 120s ago

        // heartbeat_timeout is 60s, so this should be zombie
        assert!(task.is_zombie(now, &config));

        // But not if heartbeat was recent
        task.last_heartbeat = Some(now - chrono::Duration::seconds(30));
        assert!(!task.is_zombie(now, &config));
    }
```

**Step 5: Commit**

```bash
git add crates/arco-flow/src/task.rs
git commit -m "feat(arco-flow): two-timeout heartbeat model with zombie detection"
```

---

### PR-8: Idempotent Terminal Transitions

**Goal:** Make transitions to terminal states idempotent, keyed by (run_id, task_id, attempt).

---

### Task 8.1: Add attempt tracking and idempotent transitions

**Files:**
- Modify: `crates/arco-flow/src/task.rs`

**Step 1: Ensure attempt field exists**

```rust
/// Execution state for a single task within a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecution {
    // ... existing fields ...

    /// Current attempt number (1-indexed).
    /// Increments on retry.
    pub attempt: u32,
}
```

**Step 2: Add idempotent transition logic**

```rust
impl TaskExecution {
    /// Attempts to transition to a terminal state idempotently.
    ///
    /// Returns `Ok(true)` if transition was applied.
    /// Returns `Ok(false)` if already in terminal state (idempotent no-op).
    /// Returns `Err` if transition is invalid.
    ///
    /// **Key invariant:** Late/duplicate callbacks for attempt N cannot
    /// affect attempt N+1's state.
    pub fn try_terminal_transition(
        &mut self,
        target: TaskState,
        reason: TransitionReason,
        attempt: u32,
    ) -> Result<bool, Error> {
        // Validate target is terminal
        if !target.is_terminal() {
            return Err(Error::InvalidTransition {
                from: self.state,
                to: target,
            });
        }

        // Reject callbacks for old attempts
        if attempt < self.attempt {
            // Late callback from previous attempt - ignore
            return Ok(false);
        }

        // Already in terminal state?
        if self.state.is_terminal() {
            // Idempotent: same state = no-op
            if self.state == target {
                return Ok(false);
            }
            // Different terminal state = error (shouldn't happen)
            return Err(Error::InvalidTransition {
                from: self.state,
                to: target,
            });
        }

        // Apply transition
        self.state = target;
        self.last_transition_reason = Some(reason);
        self.last_transition_at = Some(Utc::now());

        Ok(true)
    }
}

impl TaskState {
    /// Returns true if this is a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Succeeded | TaskState::Failed | TaskState::Skipped | TaskState::Cancelled
        )
    }
}
```

**Step 3: Add tests**

```rust
    #[test]
    fn terminal_transition_is_idempotent() {
        let mut task = TaskExecution::new(TaskId::generate());
        task.state = TaskState::Running;
        task.attempt = 1;

        // First transition succeeds
        let result = task.try_terminal_transition(
            TaskState::Succeeded,
            TransitionReason::ExecutionSucceeded,
            1,
        );
        assert!(result.is_ok());
        assert!(result.unwrap()); // true = applied
        assert_eq!(task.state, TaskState::Succeeded);

        // Duplicate transition is no-op
        let result = task.try_terminal_transition(
            TaskState::Succeeded,
            TransitionReason::ExecutionSucceeded,
            1,
        );
        assert!(result.is_ok());
        assert!(!result.unwrap()); // false = already terminal, no-op
    }

    #[test]
    fn late_callback_for_old_attempt_is_ignored() {
        let mut task = TaskExecution::new(TaskId::generate());
        task.state = TaskState::Running;
        task.attempt = 2; // We're on attempt 2

        // Late callback from attempt 1 should be ignored
        let result = task.try_terminal_transition(
            TaskState::Succeeded,
            TransitionReason::ExecutionSucceeded,
            1, // Old attempt!
        );
        assert!(result.is_ok());
        assert!(!result.unwrap()); // false = ignored
        assert_eq!(task.state, TaskState::Running); // State unchanged
    }
```

**Step 4: Commit**

```bash
git add crates/arco-flow/src/task.rs
git commit -m "feat(arco-flow): idempotent terminal transitions keyed by attempt"
```

---

## Gate C: Distributed Correctness (PRs 9-11)

### PR-9: Store Trait + InMemoryStore (Split from LeaderElector)

**Goal:** Define storage abstraction for runs/tasks. Leader election is a separate trait.

---

### Task 9.1: Create Store trait (without leader election)

**Files:**
- Create: `crates/arco-flow/src/store/mod.rs`
- Create: `crates/arco-flow/src/store/memory.rs`

**Step 1: Create Store trait**

```rust
// crates/arco-flow/src/store/mod.rs

//! Pluggable storage for orchestration state.
//!
//! The Store trait defines the persistence layer for runs and tasks.
//! Leader election is handled separately by `LeaderElector`.

pub mod memory;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use arco_core::{RunId, TaskId};

use crate::error::Result;
use crate::run::Run;
use crate::task::{TaskExecution, TaskState, TransitionReason};

/// Result of a compare-and-swap operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasResult {
    /// Operation succeeded.
    Success,
    /// Entity not found.
    NotFound,
    /// State didn't match expected value.
    StateMismatch { actual: TaskState },
    /// Version conflict (concurrent modification).
    VersionConflict { actual: u64 },
}

/// Storage abstraction for orchestration state.
///
/// Implementations must provide:
/// - Durability appropriate for the deployment (in-memory for tests, Postgres for prod)
/// - CAS semantics for state transitions
/// - Efficient queries for scheduler operation
#[async_trait]
pub trait Store: Send + Sync {
    // --- Run Operations ---

    /// Gets a run by ID.
    async fn get_run(&self, run_id: &RunId) -> Result<Option<Run>>;

    /// Saves a run (insert or update).
    async fn save_run(&self, run: &Run) -> Result<()>;

    // --- Task State Operations (CAS) ---

    /// Atomically transitions task state if current state matches expected.
    ///
    /// This is the core primitive for distributed correctness:
    /// - Prevents double-dispatch
    /// - Ensures exactly-once state transitions
    async fn cas_task_state(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        expected_state: TaskState,
        target_state: TaskState,
        reason: TransitionReason,
    ) -> Result<CasResult>;

    // --- Scheduler Query Operations ---

    /// Gets all tasks in a specific state for a run.
    async fn get_tasks_by_state(
        &self,
        run_id: &RunId,
        state: TaskState,
    ) -> Result<Vec<TaskExecution>>;

    /// Gets tasks that are zombies (missed heartbeat/dispatch-ack deadlines).
    async fn get_zombie_tasks(
        &self,
        run_id: &RunId,
        now: DateTime<Utc>,
    ) -> Result<Vec<TaskId>>;

    /// Gets tasks whose retry timer has expired.
    async fn get_retry_eligible_tasks(
        &self,
        run_id: &RunId,
        now: DateTime<Utc>,
    ) -> Result<Vec<TaskId>>;

    /// Gets READY tasks grouped by tenant with deterministic ordering.
    ///
    /// Returns (tenant_id, Vec<TaskExecution>) pairs for fair scheduling.
    async fn get_ready_tasks_by_tenant(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<(String, Vec<TaskExecution>)>>;
}
```

**Step 2: Create InMemoryStore**

```rust
// crates/arco-flow/src/store/memory.rs

//! In-memory store implementation for testing.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arco_core::{RunId, TaskId};

use super::{CasResult, Store};
use crate::error::Result;
use crate::run::Run;
use crate::task::{TaskExecution, TaskState, TransitionReason, TaskTimeoutConfig};

/// In-memory store for testing.
///
/// **NOT suitable for production** - no durability, no cross-process coordination.
#[derive(Debug, Default)]
pub struct InMemoryStore {
    runs: Arc<RwLock<HashMap<RunId, Run>>>,
    timeout_config: TaskTimeoutConfig,
}

impl InMemoryStore {
    /// Creates a new in-memory store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a store with custom timeout configuration.
    #[must_use]
    pub fn with_timeout_config(config: TaskTimeoutConfig) -> Self {
        Self {
            runs: Arc::new(RwLock::new(HashMap::new())),
            timeout_config: config,
        }
    }
}

#[async_trait]
impl Store for InMemoryStore {
    async fn get_run(&self, run_id: &RunId) -> Result<Option<Run>> {
        let runs = self.runs.read().unwrap();
        Ok(runs.get(run_id).cloned())
    }

    async fn save_run(&self, run: &Run) -> Result<()> {
        let mut runs = self.runs.write().unwrap();
        runs.insert(run.id, run.clone());
        Ok(())
    }

    async fn cas_task_state(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        expected_state: TaskState,
        target_state: TaskState,
        reason: TransitionReason,
    ) -> Result<CasResult> {
        let mut runs = self.runs.write().unwrap();

        let Some(run) = runs.get_mut(run_id) else {
            return Ok(CasResult::NotFound);
        };

        let Some(task) = run.get_task_mut(task_id) else {
            return Ok(CasResult::NotFound);
        };

        if task.state != expected_state {
            return Ok(CasResult::StateMismatch { actual: task.state });
        }

        task.state = target_state;
        task.last_transition_reason = Some(reason);
        task.last_transition_at = Some(Utc::now());

        Ok(CasResult::Success)
    }

    async fn get_tasks_by_state(
        &self,
        run_id: &RunId,
        state: TaskState,
    ) -> Result<Vec<TaskExecution>> {
        let runs = self.runs.read().unwrap();

        let Some(run) = runs.get(run_id) else {
            return Ok(vec![]);
        };

        Ok(run
            .task_executions
            .iter()
            .filter(|t| t.state == state)
            .cloned()
            .collect())
    }

    async fn get_zombie_tasks(
        &self,
        run_id: &RunId,
        now: DateTime<Utc>,
    ) -> Result<Vec<TaskId>> {
        let runs = self.runs.read().unwrap();

        let Some(run) = runs.get(run_id) else {
            return Ok(vec![]);
        };

        Ok(run
            .task_executions
            .iter()
            .filter(|t| t.is_zombie(now, &self.timeout_config))
            .map(|t| t.task_id)
            .collect())
    }

    async fn get_retry_eligible_tasks(
        &self,
        run_id: &RunId,
        now: DateTime<Utc>,
    ) -> Result<Vec<TaskId>> {
        let runs = self.runs.read().unwrap();

        let Some(run) = runs.get(run_id) else {
            return Ok(vec![]);
        };

        Ok(run
            .task_executions
            .iter()
            .filter(|t| {
                t.state == TaskState::RetryWait
                    && t.retry_at.map_or(false, |r| r <= now)
            })
            .map(|t| t.task_id)
            .collect())
    }

    async fn get_ready_tasks_by_tenant(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<(String, Vec<TaskExecution>)>> {
        let runs = self.runs.read().unwrap();

        let Some(run) = runs.get(run_id) else {
            return Ok(vec![]);
        };

        // For in-memory, just return single tenant
        let ready_tasks: Vec<TaskExecution> = run
            .task_executions
            .iter()
            .filter(|t| t.state == TaskState::Ready)
            .cloned()
            .collect();

        if ready_tasks.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![(run.tenant.clone(), ready_tasks)])
        }
    }
}
```

**Step 3: Export store module**

Add to `crates/arco-flow/src/lib.rs`:

```rust
pub mod store;
```

**Step 4: Run tests and commit**

```bash
cargo test -p arco-flow store
git add crates/arco-flow/src/store/
git commit -m "feat(arco-flow): Store trait with CAS semantics (separated from leader election)"
```

---

### PR-10: LeaderElector Trait

**Goal:** Define leader election abstraction separately from storage.

---

### Task 10.1: Create LeaderElector trait

**Files:**
- Create: `crates/arco-flow/src/leader.rs`

```rust
// crates/arco-flow/src/leader.rs

//! Leader election for single-scheduler coordination.
//!
//! Only one scheduler instance should be processing tasks at a time.
//! The leader elector provides distributed coordination.

use async_trait::async_trait;
use std::time::Duration;

use crate::error::Result;

/// Leader election abstraction.
///
/// Implementations:
/// - `RowBasedLeader`: Uses a database row with TTL (recommended)
/// - `AdvisoryLockLeader`: Uses Postgres advisory locks (requires dedicated connection)
#[async_trait]
pub trait LeaderElector: Send + Sync {
    /// Attempts to acquire leadership.
    ///
    /// Returns `true` if this instance is now the leader.
    async fn try_acquire(&self, scheduler_id: &str, lease_duration: Duration) -> Result<bool>;

    /// Renews the leadership lease.
    ///
    /// Returns `true` if renewal succeeded (still leader).
    /// Returns `false` if lost leadership (another instance took over).
    async fn renew(&self, scheduler_id: &str, lease_duration: Duration) -> Result<bool>;

    /// Releases leadership voluntarily.
    async fn release(&self, scheduler_id: &str) -> Result<()>;

    /// Checks if this instance is currently the leader.
    async fn is_leader(&self, scheduler_id: &str) -> Result<bool>;
}

/// In-memory leader elector for testing.
#[derive(Debug, Default)]
pub struct InMemoryLeaderElector {
    leader: std::sync::RwLock<Option<(String, std::time::Instant)>>,
}

impl InMemoryLeaderElector {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl LeaderElector for InMemoryLeaderElector {
    async fn try_acquire(&self, scheduler_id: &str, lease_duration: Duration) -> Result<bool> {
        let mut leader = self.leader.write().unwrap();
        let now = std::time::Instant::now();

        match &*leader {
            Some((current_id, expires)) if current_id != scheduler_id && *expires > now => {
                // Another scheduler holds the lock
                Ok(false)
            }
            _ => {
                // Lock is free or expired, or we already hold it
                *leader = Some((scheduler_id.to_string(), now + lease_duration));
                Ok(true)
            }
        }
    }

    async fn renew(&self, scheduler_id: &str, lease_duration: Duration) -> Result<bool> {
        let mut leader = self.leader.write().unwrap();

        match &*leader {
            Some((current_id, _)) if current_id == scheduler_id => {
                *leader = Some((scheduler_id.to_string(), std::time::Instant::now() + lease_duration));
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn release(&self, scheduler_id: &str) -> Result<()> {
        let mut leader = self.leader.write().unwrap();

        if let Some((current_id, _)) = &*leader {
            if current_id == scheduler_id {
                *leader = None;
            }
        }

        Ok(())
    }

    async fn is_leader(&self, scheduler_id: &str) -> Result<bool> {
        let leader = self.leader.read().unwrap();
        let now = std::time::Instant::now();

        match &*leader {
            Some((current_id, expires)) if current_id == scheduler_id && *expires > now => {
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}
```

**Step 2: Export and commit**

```bash
git add crates/arco-flow/src/leader.rs
git commit -m "feat(arco-flow): LeaderElector trait (separated from Store)"
```

---

### PR-11: Postgres Store + Row-Based Leader (Optional, feature-gated)

**Goal:** Production-grade Postgres implementation.

**Note:** This requires sqlx setup and is feature-gated.

**Files:**
- Modify: `crates/arco-flow/Cargo.toml`
- Create: `crates/arco-flow/src/store/postgres.rs`
- Create: `crates/arco-flow/src/leader/postgres.rs`
- Create: `crates/arco-flow/migrations/001_create_flow_tables.sql`

(Detailed implementation omitted for brevity - follows standard sqlx patterns with CAS via `UPDATE ... WHERE version = $expected RETURNING ...`)

---

## Gate D: Production Features (PRs 12-15)

### PR-12: QuotaManager + DRR Fairness

**Acceptance Criteria:**
- [ ] `QuotaManager` trait with `can_dispatch(tenant_id, task_count)` method
- [ ] `DrrScheduler` implements deficit round-robin fairness across tenants
- [ ] Configurable per-tenant quotas (max concurrent tasks)
- [ ] Tests verify fair scheduling under contention

---

### PR-13: TaskQueue Trait + Cloud Tasks Dispatcher

**Acceptance Criteria:**
- [ ] `TaskQueue` trait with `enqueue(TaskEnvelope, EnqueueOptions)` method
- [ ] `CloudTasksDispatcher` implementation for Google Cloud Tasks (feature-gated; optional)
- [ ] Idempotency key = `{run_id}/{task_id}/{attempt}` (prevents duplicate dispatch)
- [ ] Retry policy configuration with exponential backoff + jitter (integration pending)
- [ ] Tests verify idempotent dispatch behavior

---

### PR-14: Metrics + Tracing Instrumentation

**Acceptance Criteria:**
- [ ] Prometheus metrics:
  - `arco_flow_tasks_total{from_state, to_state, tenant}` counter
  - `arco_flow_task_duration_seconds{operation, state}` histogram
  - `arco_flow_scheduler_tick_duration_seconds` histogram
  - `arco_flow_active_runs{tenant}` gauge
- [ ] Tracing spans for scheduler tick, task dispatch, state transitions
- [ ] Structured logging with run_id, task_id context

---

### PR-15: Runbooks + Operational Documentation

**Acceptance Criteria:**
- [ ] Runbook: "Scheduler is not making progress"
- [ ] Runbook: "Task stuck in Dispatched state"
- [ ] Runbook: "High task failure rate"
- [ ] Dashboard JSON for Grafana
- [ ] Alert rules for SLOs (e.g., P99 task latency > 5min)

---

## ADR Schedule

| ADR | When | File |
|-----|------|------|
| ADR-010: Canonical JSON | Before PR-1 | `docs/adr/010-canonical-json.md` |
| ADR-011: Partition Identity | Before PR-2 | `docs/adr/011-partition-identity.md` |
| ADR-012: AssetKey Format | Before PR-2 | `docs/adr/012-asset-key-format.md` |
| ADR-013: ID Wire Formats | Before PR-4 | `docs/adr/013-id-wire-formats.md` |
| ADR-014: Leader Election | Before PR-10 | `docs/adr/014-leader-election.md` |
| ADR-015: Postgres Store | Before PR-11 | `docs/adr/015-postgres-orchestration-store.md` |
| ADR-016: Tenant Quotas | Before PR-12 | `docs/adr/016-quotas-and-fairness.md` |
| ADR-017: Cloud Tasks Dispatch | Before PR-13 | `docs/adr/017-cloud-tasks-dispatcher.md` |

---

## Verification Checkpoints

### After Gate A (PRs 1-5)
- [ ] `cargo test --workspace` passes
- [ ] Cross-language golden vectors match Python output
- [ ] Fingerprint is stable across task insertion order
- [ ] DAG toposort is insertion-order independent
- [ ] Cycle errors include full path

### After Gate B (PRs 6-8)
- [ ] TransitionReason in all state change events
- [ ] Two-timeout zombie detection works correctly
- [ ] Idempotent terminal transitions (duplicate callbacks safe)
- [ ] Late callbacks for old attempts are ignored

### After Gate C (PRs 9-11)
- [ ] CAS transitions prevent double-dispatch
- [ ] Leader election works with 2 instances
- [ ] Store and LeaderElector are separate traits

### After Gate D (PRs 12-15)
- [ ] Quotas enforced per tenant
- [ ] Cloud Tasks dispatcher with idempotency
- [ ] Metrics exported to Prometheus
- [ ] Runbooks documented

---

## Timeline Estimate

- **Gate A (PRs 1-5):** 5-7 days
- **Gate B (PRs 6-8):** 3-4 days
- **Gate C (PRs 9-11):** 5-7 days
- **Gate D (PRs 12-15):** 4-6 days

**Total:** ~17-24 engineering days

---

## Known Constraints

1. **Proto backward compatibility:** The ResourceRequirements change may break existing clients. Consider v2 message if needed.
2. **Cross-language testing:** Golden vectors require Python implementation verification.
3. **Postgres dependency:** Gate C requires Postgres setup for integration tests.
4. **Cloud Tasks dependency:** PR-13 requires GCP project for e2e tests.
