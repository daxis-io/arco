# ADR-032 Hardening Follow-up Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the highest-priority ADR-032 gaps in orchestration compaction by enforcing lock-backed fencing, making L0 artifact writes immutable, adding manifest succession checks, and aligning stale docs with the live architecture.

**Architecture:** Keep the change set scoped to the orchestration compactor and adjacent manifest/docs surfaces. Add failing tests around lock validation and L0 overwrite rejection first, then harden the service path by validating the actual lock object, retrying on immutable-write conflicts, and moving snapshot+pointer publish onto the shared helper with explicit succession checks.

**Tech Stack:** Rust (`arco-flow`, `arco-core`, `arco-catalog` docs), object-storage CAS semantics, repo docs under `docs/plans/`.

---

### Task 1: Add failing orchestration compactor tests

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/service.rs`

**Step 1: Write fencing tests**

- Add tests proving `compact_events_fenced()`:
  - rejects a missing/expired lock with `Error::FencingLockUnavailable`
  - rejects a stale fencing token with `Error::StaleFencingToken`
  - accepts the canonical lock path when the current lock holder sequence matches

**Step 2: Write immutable L0 tests**

- Add tests proving:
  - `write_parquet_file()` rejects overwrite attempts via `DoesNotExist`
  - compaction retries and succeeds when the first L0 object write hits a synthetic precondition failure

**Step 3: Run focused tests to verify RED**

Run: `cargo test -p arco-flow fencing -- --nocapture`

Expected: failure because the current implementation ignores `lock_path` and still writes L0 files with `WritePrecondition::None`.

### Task 2: Harden orchestration compactor semantics

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/service.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/manifest.rs`

**Step 1: Enforce actual lock-backed fencing**

- Validate the supplied `lock_path` matches `orchestration_compaction_lock_path()`.
- Read the distributed lock object.
- Reject missing/expired lock holders as `Error::FencingLockUnavailable`.
- Reject mismatched sequence numbers as `Error::StaleFencingToken`.

**Step 2: Make L0 artifact writes immutable**

- Change Parquet writes to `WritePrecondition::DoesNotExist`.
- Surface precondition failures as retryable L0 conflicts.
- Retry from a fresh manifest/new delta id when the first immutable write loses a race.

**Step 3: Add manifest succession validation**

- Add orchestration validator(s) for:
  - monotonic `manifest_id`
  - monotonic `epoch`
  - matching `parent_pointer_hash`
- Use these checks during pointer publish so stale or tampered chains fail before visibility changes.

**Step 4: Use shared snapshot+pointer publish helper**

- Replace the local publish transaction code with `arco_core::publish::publish_snapshot_pointer_transaction`.
- Preserve current `Visible` vs `PersistedNotVisible` behavior and retry classification.

### Task 3: Align docs with the implementation

**Files:**
- Modify: `crates/arco-catalog/src/lib.rs`
- Modify: `docs/adr/adr-032-immutable-manifest-pointers.md`

**Step 1: Update stale catalog crate docs**

- Remove the old `Tier1Writer::update()` example as the primary write path.
- Document the ledger + sync-compactor path and note `Tier1Writer` is legacy/compatibility only.

**Step 2: Update ADR status**

- Change ADR-032 from `Proposed` to the correct post-implementation state if the code now matches the committed protocol for catalog + the hardened orchestration slice.

### Task 4: Verify the slice

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/service.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/manifest.rs`
- Modify: `crates/arco-catalog/src/lib.rs`
- Modify: `docs/adr/adr-032-immutable-manifest-pointers.md`

**Step 1: Run targeted orchestration compactor tests**

Run: `cargo test -p arco-flow orchestration::compactor::service -- --nocapture`

Expected: PASS

**Step 2: Run crate tests for touched docs-adjacent crates when needed**

Run: `cargo test -p arco-flow --lib -- --nocapture`

Expected: PASS or an unrelated known failure that must be reported explicitly.
