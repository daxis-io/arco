# Common Proto Hard Cut Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Hard-cut `proto/arco/v1/common.proto` and adjacent proto contracts to stronger, schema-validated shapes while the project is still alpha.

**Architecture:** Keep the package at `arco.v1`, replace the structurally wrong fields now, and update the Rust proto crate so generated types still have stable serde behavior. Use local vendoring for Protovalidate annotations so both `buf` and `prost_build` compile the same schema graph without network-dependent build logic.

**Tech Stack:** Protobuf, Buf, Protovalidate annotations, Rust `prost_build`, `pbjson-types`, `serde`, Cargo tests

---

### Task 1: Capture the Hard-Cut Contract in Tests

**Files:**
- Modify: `crates/arco-proto/src/lib.rs`
- Modify: `crates/arco-proto/tests/golden_fixtures.rs`
- Create: `crates/arco-proto/fixtures/partition_key_v2.json`
- Create: `crates/arco-proto/fixtures/file_entry_v2.json`

**Step 1: Write the failing tests**

Add tests that assert:
- `PartitionKey` serde uses an ordered `dimensions` array of `{ key, value }` objects.
- `ScalarValue` serde accepts typed `date`, typed `timestamp`, `doubleValue`, `bytesValue`, and JSON `null`.
- `FileEntry` serde accepts `uint64`-sized counters, typed digest metadata, and enum-based file format.

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-proto`
Expected: FAIL because the current generated schema still exposes map-based partitions, stringly typed scalar date/timestamp values, and legacy file metadata fields.

**Step 3: Keep the assertions minimal**

Avoid adding helper abstractions before the schema exists. The failure should point directly at the missing contract shape.

**Step 4: Re-run the focused crate tests after each schema change**

Run: `cargo test -p arco-proto`
Expected: The new tests move from compile/runtime failure toward green as the proto changes land.

### Task 2: Make the Proto Toolchain Support the New Contract

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/arco-proto/Cargo.toml`
- Modify: `crates/arco-proto/build.rs`
- Create: `proto/buf/validate/validate.proto`

**Step 1: Vendor the validation schema**

Copy the official `buf/validate/validate.proto` into `proto/buf/validate/validate.proto` with its license header intact so both Buf and `prost_build` resolve the same import path locally.

**Step 2: Add serde-capable well-known types**

Add `pbjson-types` as a workspace dependency and wire `crates/arco-proto/build.rs` to map:
- `.google.protobuf.Timestamp` to `::pbjson_types::Timestamp`
- `.google.protobuf.NullValue` to `::pbjson_types::NullValue`

**Step 3: Expand serde derives for new generated types**

Update `build.rs` so the new messages/enums introduced in `common.proto` derive `serde` where needed:
- `PartitionDimension`
- `DateValue`
- `FileHash`
- `HashAlgorithm`
- `FileFormat`

**Step 4: Run the crate tests to verify the toolchain still compiles**

Run: `cargo test -p arco-proto`
Expected: FAIL only on the schema contract tests until the proto definitions are updated.

### Task 3: Hard-Cut the Schema in `common.proto`

**Files:**
- Modify: `proto/arco/v1/common.proto`

**Step 1: Replace comment-only invariants with validation annotations**

Import:
- `google/protobuf/timestamp.proto`
- `buf/validate/validate.proto`

Encode validation for:
- UUIDv7 `AssetId`
- ULID `RunId`, `TaskId`, and `MaterializationId`
- `AssetKey` namespace/name pattern and length
- non-empty `FileEntry.path`
- non-negative numeric fields
- enum `defined_only` / non-zero where required

**Step 2: Replace unordered partition maps with ordered dimensions**

Change:
- `PartitionKey.dimensions` from `map<string, ScalarValue>` to `repeated PartitionDimension dimensions = 1`

Add:
- `message PartitionDimension { string key = 1; ScalarValue value = 2; }`

**Step 3: Strengthen scalar values**

Replace the old stringly typed date/timestamp members with:
- `DateValue date = 4`
- `google.protobuf.Timestamp timestamp = 5`
- `google.protobuf.NullValue null_value = 6`

Add:
- `double double_value = 7`
- `bytes bytes_value = 8`

**Step 4: Strengthen file metadata**

Replace free-form file metadata with:
- `uint64 size_bytes`
- `uint64 row_count`
- `FileHash content_digest`
- `FileFormat file_format`

Add message/enum definitions for:
- `FileHash`
- `HashAlgorithm`
- `FileFormat`

Use a message-level CEL rule on `FileHash` so SHA-256 digests must be exactly 32 bytes.

### Task 4: Tighten Adjacent Non-Negative Proto Fields

**Files:**
- Modify: `proto/arco/v1/event.proto`
- Modify: `proto/arco/v1/orchestration.proto`

**Step 1: Change output counters to unsigned types**

Update these fields from `int64` to `uint64`:
- `MaterializationCompleted.row_count`
- `MaterializationCompleted.byte_size`
- `TaskOutput.row_count`
- `TaskOutput.byte_size`
- `TaskMetrics.duration_ms`
- `TaskMetrics.cpu_time_ms`
- `TaskMetrics.peak_memory_bytes`
- `TaskMetrics.bytes_read`
- `TaskMetrics.bytes_written`

**Step 2: Keep semantics consistent with the rest of the contract**

Do not change fields where signed values still make sense, such as task priority.

### Task 5: Verify the Hard Cut End-to-End

**Files:**
- Modify as required by generated/test fallout only

**Step 1: Run focused verification**

Run: `cargo test -p arco-proto`
Expected: PASS

Run: `buf lint proto/`
Expected: PASS

Run: `buf build proto -o /tmp/arco-common-proto-hard-cut.binpb`
Expected: PASS

**Step 2: Check the worktree diff**

Run: `git status --short`
Expected: Only the intended schema, build, fixture, and test changes are present.

**Step 3: Summarize intentional breakage**

Document in the final response that this is a deliberate alpha hard cut in `arco.v1`, so previous JSON fixtures and serialized wire payloads are not preserved.
