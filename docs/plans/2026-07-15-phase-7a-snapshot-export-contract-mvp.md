# Phase 7A Snapshot/Export Contract MVP Implementation Plan

> **Execution requirements:** Use `test-driven-development` for every
> behavior, keep code mutations with one worktree owner, and use two ordered
> independent read-only review gates.

**Goal:** Define canonical retained workspace-snapshot/export records, stable
persisted authority references, deterministic fail-closed GC reachability, and
an exact safe `system.catalog.snapshots` projection without adding workflow
services or changing authority.

**Architecture:** Add a pure contract module whose constructors/codecs validate
scope, IDs, ordering, paths, checksums, compatibility, and retention lifecycle.
Add a separate persisted-authority adapter implemented by the control-store MVP
and explicitly unsupported by `CurrentStateStore`. Build a deterministic GC
protection graph before deletion and register only a manifest-selected,
exact-schema safe Parquet projection.

**Tech Stack:** Rust 2024, serde/serde_json/serde_jcs, chrono, SHA-256, ULID,
`BTreeMap`/`BTreeSet`, Arrow/Parquet, DataFusion, object-store CAS, and Cargo.

---

## Scope And Hard Non-Goals

Allowed implementation files:

- `docs/plans/2026-07-15-phase-7-execution-program.md`
- `docs/plans/2026-07-15-phase-7a-snapshot-export-contract-mvp.md`
- `crates/arco-catalog/src/workspace_snapshot.rs`
- `crates/arco-catalog/tests/workspace_snapshot_contracts.rs`
- `crates/arco-catalog/src/gc/reachability.rs`
- `crates/arco-catalog/src/gc/mod.rs`
- `crates/arco-catalog/src/gc/collector.rs`
- `crates/arco-catalog/src/state_store.rs`
- `crates/arco-catalog/src/state_store/control_mvp.rs`
- `crates/arco-catalog/tests/state_store_current_adapter.rs`
- `crates/arco-catalog/tests/state_store_control_mvp.rs`
- `crates/arco-catalog/src/parquet_util.rs`
- `crates/arco-catalog/src/lib.rs`
- `crates/arco-api/src/system_tables.rs`
- `crates/arco-api/tests/system_tables_api.rs`

Do not add snapshot/export creation services, restore operations, journals,
durable handles, routes, protobufs, CLI/SQL commands, grants, catalog DDL,
credential-vending changes, new authority roots, or ordinary catalog-compactor
publication of `snapshots.parquet`.

## Contract Decisions

### Record envelope and identity

- Every durable record has `record_type` and `version: 1`.
- Decode validates the discriminator/version before constructing a record.
- Unknown additive v1 fields are accepted; unsupported versions/types fail.
- Canonical encoding uses `serde_jcs` after full validation.
- IDs are caller-suppliable for retry and must be `snap_`, `exp_`, or `pin_`
  followed by exactly one valid 26-character ULID.

### Scope and authority references

- `WorkspaceScope` contains nonblank `tenant_id` and `workspace_id`.
- Every domain authority reference repeats the exact workspace scope and has a
  unique canonical domain name.
- `PersistedAuthorityReference` stores backend implementation, typed scope,
  `checkpoint` or `state_token`, manifest ID, logical sequence, canonical
  relative manifest/checkpoint paths, `sha256:<64 lowercase hex>` digests, and
  an absolute retention deadline.
- `StateToken` and `CheckpointToken` remain private and have no serde derives.
- `PersistedAuthorityAdapter` is separate from `ArcoStateAdmin` so the
  deterministic model backend is not forced to fabricate storage references.

### Projection, archive, and object references

- A projection watermark contains projection name, source domain, included
  authority sequence, and a checksum-bearing manifest reference.
- An event archive is explicitly `empty` or an inclusive start/end range with
  a checksum-bearing archive-manifest reference; start must not exceed end.
- Required objects contain canonical relative paths, byte size, typed kind, and
  prefixed SHA-256 digest.
- Paths reject absolute forms, `.`/`..`, empty segments, backslashes, control
  characters, and normalized duplicates. Equal content at distinct paths is
  valid.
- Compatibility references are read-only and must name an identical path and
  digest present in required objects.
- Relocation records only that paths are relative to the caller's export root;
  no provider URI, root URI, credential, or secret is persisted.

### Retention

- Snapshots and exports are immutable.
- Retention uses immutable `RetentionPinRevision` records and one CAS-selected
  `RetentionPinLatest` selector.
- Renewal can only extend an active pin.
- Expired or released pins cannot be renewed or revived.
- Release is idempotent.
- Structural validation always precedes expiry evaluation.

### System projection

`WorkspaceSnapshotCatalogRecord` exposes exactly:

```text
snapshot_id
record_version
created_at
retained_until
retention_status
domain_count
parent_snapshot_id
has_legacy_compatibility
```

The API must reject a selected `snapshots.parquet` whose schema differs from
this exact safe schema. The table is available only when the current catalog
manifest lists the file. A physically present but unselected file is invisible.

## Task 1: Write Snapshot And Export Contract Tests First

**Files:**

- Create: `crates/arco-catalog/tests/workspace_snapshot_contracts.rs`
- Create later: `crates/arco-catalog/src/workspace_snapshot.rs`
- Modify later: `crates/arco-catalog/src/lib.rs`

**Step 1: Add the first failing public-contract test**

Import the wished-for public API and build a valid v1 snapshot containing two
out-of-order domain authority refs, projection watermarks, archives, required
objects, and one compatibility artifact. Assert:

- validation returns a canonical domain-sorted value;
- canonical JSON round-trips byte-for-byte;
- token internals do not appear in encoded JSON;
- an additive unknown v1 field decodes successfully.

**Step 2: Run red**

```bash
cargo test -p arco-catalog --test workspace_snapshot_contracts -- --nocapture
```

Expected: compile failure because the contract module/types do not exist.
Capture the output before adding production code.

**Step 3: Add one failing test per validation rule**

Cover unsupported type/version, malformed prefixed IDs, blank/mismatched scope,
duplicate domains, invalid archive ranges, invalid/duplicate object paths,
invalid digests, missing compatibility targets, provider/root relocation data,
and immutable pin renewal/release rules.

Run the focused test after each test group and confirm it fails for the intended
missing behavior rather than test setup.

**Step 4: Implement the minimum contract module**

Create public, documented types and validated constructors/codecs in
`workspace_snapshot.rs`. Use private fields plus accessors where accepting raw
public fields would bypass invariants. Use `BTreeMap`/sorted vectors for stable
domain/object order. Keep generic helpers such as ID, digest, and relative-path
validation private.

The public surface must include the snapshot/export records, workspace scope,
domain authority refs, projection watermark, archive cut, checksum/required
object refs, relocation policy, pin revision/latest selector, retention status,
and canonical encode/decode functions.

Register/re-export the module from `lib.rs` without exposing token constructors.

**Step 5: Run green and refactor**

```bash
cargo test -p arco-catalog --test workspace_snapshot_contracts -- --nocapture
cargo fmt --all --check
```

Expected: every contract test passes and formatting is clean.

## Task 2: Add Stable Persisted-Authority References Test-First

**Files:**

- Modify: `crates/arco-catalog/src/state_store.rs`
- Modify: `crates/arco-catalog/src/state_store/control_mvp.rs`
- Modify: `crates/arco-catalog/tests/state_store_current_adapter.rs`
- Modify: `crates/arco-catalog/tests/state_store_control_mvp.rs`
- Modify: `crates/arco-catalog/src/lib.rs`

**Step 1: Add failing control-MVP tests**

Test `PersistedAuthorityAdapter` with a committed `StateToken` and a created
`CheckpointToken`. Assert the returned records contain the stable
implementation, repeated scope, reference kind, manifest ID/sequence, expected
relative paths, prefixed raw-object digests, and retention deadline. Resolve
both references and read committed data.

Add corrupt implementation, scope, kind/optional-field coherence, path,
manifest/checkpoint digest, manifest/sequence, and expired-reference cases.

**Step 2: Add a failing current-adapter test**

Assert persist/resolve operations return `UnsupportedOperation`, and add a JSON
compile/runtime opacity assertion showing neither opaque token implements a
serialization path.

**Step 3: Run red**

```bash
cargo test -p arco-catalog --test state_store_current_adapter -- --nocapture
cargo test -p arco-catalog --test state_store_control_mvp -- --nocapture
```

Expected: compile failures for the missing adapter/reference API.

**Step 4: Implement the minimum adapter**

In `state_store.rs`, add documented serializable `PersistedAuthorityKind` and
`PersistedAuthorityReference` values plus an async `PersistedAuthorityAdapter`
trait. Keep constructors validated and fields private. Implement all adapter
methods as unsupported for `CurrentStateStore`.

In `control_mvp.rs`, implement conversion and resolution using directly
computed paths and raw object reads. Validate the selected manifest/checkpoint
and hash their exact stored bytes. Resolution must revalidate every persisted
field before constructing private token values or returning a retained reader.

**Step 5: Run green and opacity checks**

```bash
cargo test -p arco-catalog --test state_store_current_adapter -- --nocapture
cargo test -p arco-catalog --test state_store_control_mvp -- --nocapture
cargo test -p arco-catalog --doc state_store -- --nocapture
```

Expected: new adapter tests and existing compile-fail token doctests pass.

## Task 3: Build Fail-Closed Deterministic Reachability Test-First

**Files:**

- Create: `crates/arco-catalog/src/gc/reachability.rs`
- Modify: `crates/arco-catalog/src/gc/mod.rs`
- Modify: `crates/arco-catalog/src/gc/collector.rs`

**Step 1: Add failing pure reachability tests in the new module**

Tests must be module-local so this exact filter executes them:

```bash
cargo test -p arco-catalog gc::reachability -- --nocapture
```

Cover deterministic results under permuted input; unconditional current-head
protection; active checkpoint/state-token, root-token, snapshot, export,
review-cut, projection, archive, required-object, and compatibility protection;
expired/released pins no longer protecting targets; and validation errors for
malformed pins, ambiguous lifecycle, missing active targets, corrupt refs, bad
paths/digests, and unsupported versions.

**Step 2: Capture red**

Run the filter and confirm nonzero tests fail because the protection builder is
missing or incomplete.

**Step 3: Implement the pure graph**

Use `BTreeMap`/`BTreeSet` and a caller-supplied `now`. Return one validated
`ProtectionSet` with exact-object and prefix checks. Validate all root records
before checking expiry and before returning candidates.

**Step 4: Add failing collector integration tests**

Using a recording memory backend, prove:

- malformed retained inventory returns `Err` and performs zero deletes;
- each active root category removes its objects from every candidate class;
- current heads are rechecked before a delete;
- listing order cannot change the report;
- dry-run and mutation mode share the same protection semantics.

**Step 5: Integrate before the first delete**

Build/validate the complete protection set once before candidate generation in
`collect_dry_run` and `collect`. Inventory every candidate class before the
first mutation. Thread the set through orphan, ledger, old-version, and
delete-prefix paths. Preserve a fresh current-head recheck immediately before
delete to cover pointer movement. Root-validation errors must bypass
`run_phase` and abort the run.

Sort every background listing before traversal. Do not change request-time
correctness paths to list storage.

**Step 6: Run green**

```bash
cargo test -p arco-catalog gc::reachability -- --nocapture
cargo test -p arco-catalog gc::collector -- --nocapture
cargo test -p arco-catalog gc -- --nocapture
```

Expected: every filter runs nonzero tests and passes.

## Task 4: Add The Safe Snapshot Projection And Manifest-Selected Table

**Files:**

- Modify: `crates/arco-catalog/src/parquet_util.rs`
- Modify: `crates/arco-catalog/src/lib.rs`
- Modify: `crates/arco-api/src/system_tables.rs`
- Modify: `crates/arco-api/tests/system_tables_api.rs`

**Step 1: Add failing encoder and API tests**

Add catalog tests for exact schema, deterministic bytes, millisecond
timestamps, checked domain-count conversion, and absence of all private fields.

Add API tests whose names contain `snapshot` and prove:

- every approved column is queryable;
- `SELECT *` returns exactly the approved key set;
- authority/checkpoint, creator, checksum, path, archive, and relocation columns
  are rejected;
- a physically present but unselected file is unavailable;
- a selected file succeeds with list operations configured to fail;
- a selected file with an extra column is rejected before registration.

**Step 2: Capture red**

```bash
cargo test -p arco-api --test system_tables_api snapshot -- --nocapture
```

Expected: the filter runs nonzero tests and fails because the table is not
allowlisted/registered.

**Step 3: Implement the encoder and allowlist**

Add `WorkspaceSnapshotCatalogRecord`, `workspace_snapshot_schema()`, and
`write_workspace_snapshots()` using the existing `write_single_batch` pattern.
Use only the eight approved columns.

Add the `snapshots.parquet` allowlist entry. Before registering that one table,
compare the decoded Arrow schema to the exact catalog snapshot schema and fail
closed on extra, missing, reordered, nullable, or type-mismatched columns. Do
not add the artifact to ordinary catalog snapshot writers or legacy fallback
paths.

**Step 4: Run green**

```bash
cargo test -p arco-api --test system_tables_api snapshot -- --nocapture
cargo test -p arco-api --test system_tables_api -- --nocapture
cargo check -p arco-api
```

Expected: the snapshot filter runs nonzero tests; all system-table tests and API
check pass.

## Task 5: Verify, Commit, And Review The Slice

**Step 1: Run the full 7A focused gate**

```bash
cargo test -p arco-catalog --test workspace_snapshot_contracts -- --nocapture
cargo test -p arco-catalog gc::reachability -- --nocapture
cargo test -p arco-catalog --test state_store_current_adapter -- --nocapture
cargo test -p arco-catalog --test state_store_control_mvp -- --nocapture
cargo test -p arco-api --test system_tables_api snapshot -- --nocapture
cargo check -p arco-catalog
cargo check -p arco-api
cargo fmt --all --check
git diff --check
```

Expected: all commands exit zero and every filtered test command runs nonzero
tests.

**Step 2: Audit scope**

```bash
git status --short
git diff --stat
git diff -- crates/arco-catalog/src/state_store.rs \
  crates/arco-catalog/src/state_store/control_mvp.rs
```

Confirm opaque tokens remain private, `CurrentStateStore` remains unsupported,
no service/restore/handle/route/proto/CLI/DDL code exists, no public projection
is written, and no authority owner changed.

**Step 3: Commit one slice**

```bash
git add docs/plans/2026-07-15-phase-7-execution-program.md \
  docs/plans/2026-07-15-phase-7a-snapshot-export-contract-mvp.md \
  crates/arco-catalog/src/workspace_snapshot.rs \
  crates/arco-catalog/tests/workspace_snapshot_contracts.rs \
  crates/arco-catalog/src/gc/reachability.rs \
  crates/arco-catalog/src/gc/mod.rs \
  crates/arco-catalog/src/gc/collector.rs \
  crates/arco-catalog/src/state_store.rs \
  crates/arco-catalog/src/state_store/control_mvp.rs \
  crates/arco-catalog/tests/state_store_current_adapter.rs \
  crates/arco-catalog/tests/state_store_control_mvp.rs \
  crates/arco-catalog/src/parquet_util.rs \
  crates/arco-catalog/src/lib.rs \
  crates/arco-api/src/system_tables.rs \
  crates/arco-api/tests/system_tables_api.rs
git commit -m "feat(catalog): define snapshot export contracts"
```

**Step 4: Run ordered fresh reviews**

Record `BASE_SHA=origin/main` and `HEAD_SHA=HEAD`. Dispatch a fresh
spec-compliance reviewer first. Fix blockers and amend until approved. Then
dispatch a fresh code-quality reviewer, fix Important/Critical findings, amend,
rerun the focused gate, and re-review the final SHA.

**Exit gate:** canonical v1 records round-trip; opaque tokens are never
serialized; malformed roots abort GC without deletion; all required active
root categories protect their objects; the snapshots projection is exact-safe,
read-only, and manifest-selected; and the worktree is clean with no 7B, 7C,
7D, transport, or authority change.
