# Phase 7B Workspace Snapshot And Export Implementation Plan

> **Execution requirements:** Use `test-driven-development` for every
> behavior, keep code mutations with one worktree owner, and use two ordered
> independent read-only review gates.

**Goal:** Implement explicitly configured, direct-addressed workspace snapshot
and export services that checkpoint every configured domain, publish only
validated immutable retention records, verify every referenced object without
listing, and provide a complete read-only restore preflight.

**Architecture:** Add one deep catalog service module around `ScopedStorage`, a
deterministic `WorkspaceDomainRegistry`, and four mandatory capabilities per
domain: `ArcoStateStore`, `PersistedAuthorityAdapter`, projection-watermark
provider, and event-archive provider. Reuse the Phase 7A canonical records and
state-reference adapter. Centralize retention paths so GC and request-time
services address identical objects. Treat export as a verified immutable
manifest over objects relative to the caller-supplied scoped storage root; it
does not copy or publish public Parquet. All request-time reads are exact-path
reads, and the latest pin selector is the only retention-visibility boundary.
Snapshot/export finalization and mutating GC share one workspace-scoped lease
to claim a durable exact-path mutation epoch. After one post-claim lease proof,
the in-flight epoch is the exclusion boundary through every storage mutation;
foreign holders abort without mutation even after lease expiry. Epoch numbers
come only from the durable record's CAS sequence, not the lease fencing token,
so deleting a stale released lock cannot wedge future retention work.

**Tech Stack:** Rust 2024, async-trait, `Arc`, `BTreeMap`/`BTreeSet`, chrono,
SHA-256, ULID, `ScopedStorage`, object-store conditional writes, and Cargo.

**Original slice base:** `acd94512744416dadc88d5ed96596b090e33af31`

**Refreshed slice parent:** `003a9dc15a911209c5c25106f7ec6834a68b0db2`

---

## Scope And Hard Non-Goals

Allowed files:

- `docs/plans/2026-07-15-phase-7b-workspace-snapshot-export.md`
- `crates/arco-catalog/src/workspace_snapshot.rs`
- `crates/arco-catalog/src/workspace_snapshot_service.rs`
- `crates/arco-catalog/src/retention_coordination.rs`
- `crates/arco-catalog/src/gc/reachability.rs`
- `crates/arco-catalog/src/gc/collector.rs`
- `crates/arco-catalog/src/gc/mod.rs`
- `crates/arco-catalog/src/state_store.rs`
- `crates/arco-catalog/src/state_store/control_mvp.rs`
- `crates/arco-catalog/src/lib.rs`
- `crates/arco-catalog/tests/state_store_control_mvp.rs`
- `crates/arco-catalog/tests/workspace_snapshot_contracts.rs`
- `crates/arco-catalog/tests/workspace_snapshot_services.rs`
- `crates/arco-core/src/lock.rs`

The core lock file is a narrow safety prerequisite: its renewal operation must
bind content to an earlier HEAD version, validate holder plus fencing sequence,
reject expired ownership, and CAS that exact version. The state-store files are
limited to an explicit-time persisted-reference resolution seam so restore
preflight never consults ambient wall-clock time.

Do not add an API route, protobuf, CLI/SQL command, catalog DDL, grant or
credential-vending change, public Parquet publication, root/current-pointer
write, restore mutation, restore journal, durable transaction handle, retry
policy, new authority implementation, or Phase 8 work. Do not modify Cargo
manifests.

The service may write only:

- control-store checkpoint objects produced by the configured state store;
- immutable snapshot or export records under `retention/`;
- immutable pin revisions and their conditional latest selectors; and
- the versioned `retention/coordination/mutation-epoch.json` exclusion record;
- the ephemeral `locks/workspace-retention-gc.lock.json` coordination lease; and
- a future explicitly configured projection-outbox surface, if one exists.

The current generic `ArcoStateStore` surface cannot stage
`ControlMvpProjectionOutboxRecord`. Phase 7B must not downcast a transaction,
fabricate a no-op provider, or add a second visibility mechanism merely to emit
outbox data. No outbox write is required in this slice.

## Committed 7A Seams To Reuse

- `workspace_snapshot.rs` owns validated scope, authority, projection, archive,
  required-object, compatibility, snapshot/export, and pin record types.
- `state_store.rs` keeps opaque tokens private and separates
  `ArcoStateStore` from `PersistedAuthorityAdapter`.
- `ControlMvpStateStore` checkpoints current authority and resolves stable
  checkpoint references by exact path and checksum.
- `ScopedStorage::{get_raw, head_raw, put_raw}` supplies scope-safe direct
  operations. `list` is a separate method and is forbidden on request paths.
- `WritePrecondition::DoesNotExist` plus winner readback supplies immutable
  create-if-absent. `MatchesVersion` supplies selector CAS where an existing
  selector is legitimately advanced in a later slice.
- GC already consumes snapshot, export, pin revision, and pin selector paths.
  Move those path constructors into `workspace_snapshot.rs` and make both GC
  and the service call the same validated helpers.

## Service Contract Decisions

### Explicit domain configuration

Add async provider traits with no defaults:

- `ProjectionWatermarkProvider` returns the complete projection cut for one
  captured domain authority, including every explicitly required projection
  object and any read-only compatibility artifact.
- `EventArchiveProvider` returns exactly one `DomainEventArchive` for the
  captured domain, plus every explicitly required archive object.

Provider result types must contain Phase 7A validated values rather than raw
paths or digests. Empty projections are explicit. The archive provider must
return `empty` or `inclusive`; absence is not a fallback.

`WorkspaceDomainBinding` requires, with no `Option` and no `Default`:

- an exact `StateScope`;
- `Arc<dyn ArcoStateStore>`;
- `Arc<dyn PersistedAuthorityAdapter>`;
- `Arc<dyn ProjectionWatermarkProvider>`; and
- `Arc<dyn EventArchiveProvider>`.

`WorkspaceDomainRegistry` owns a `WorkspaceScope` and a `BTreeMap` of bindings.
Construction rejects an empty registry, duplicate domain, invalid state scope,
or tenant/workspace/domain mismatch. Iteration is canonical by domain name.
Production construction therefore cannot silently fall back to fabricated
providers.

### Direct identity and immutable publication

Requests carry caller-supplied canonical IDs and deterministic timestamps:

- snapshot creation: `snapshot_id`, `pin_id`, `created_at`, `retained_until`,
  and optional `parent_snapshot_id`;
- export creation: `export_id`, `pin_id`, `snapshot_id`, explicit
  `source_pin_id`, `created_at`, and `retained_until`.

Phase 7A constructors reject malformed or noncanonical IDs. A generated-ID
convenience constructor may use an uppercase ULID, but the core operation must
accept caller IDs for retry.

Each immutable snapshot record repeats its target `pin_id`. Each immutable
export record repeats both its target `pin_id` and source snapshot
`source_pin_id`. These direct-addressed bindings are required version-1 fields,
validated as canonical pin IDs, and are part of immutable retry identity. A
same record ID with any different bound pin fails before another root is
written.

Before checkpointing, `create_snapshot` directly gets
`retention/snapshots/{snapshot_id}.json`. A valid existing record with the same
scope and immutable request identity is returned as an idempotent retry only
after its selected pin is validated. It must not create new random checkpoint
objects. A same-ID semantic conflict fails with `PreconditionFailed`.

The first attempt uses two phases:

1. Validate the complete registry and check every domain capability before the
   first checkpoint. Every binding must support checkpoints and retained reads.
2. In canonical domain order, checkpoint, convert the opaque checkpoint token
   immediately through that binding's adapter, validate implementation and
   full scope, collect provider cuts, directly read and hash every referenced
   object, and build the complete record in memory.

No snapshot or pin record is written until every domain and provider succeeds.
Failed checkpoints/providers may leave unreachable checkpoint objects, but no
retained root or authority publication.

Publication order is:

1. immutable snapshot/export record;
2. immutable revision-1 pin record targeting it; and
3. create-if-absent latest-pin selector.

Every immutable write uses `DoesNotExist`, reads the winner on collision,
accepts byte-identical content, and rejects different bytes. If the bound
latest selector already exists, retry loads and validates its complete immutable
revision chain, target, revision-1 request semantics, and active status without
writing pin objects. A legitimately advanced active pin is therefore accepted
only while its selected deadline does not exceed the immutable record and
authority cut's usable retention deadline.
Revision 1 is published only when the bound selector is absent. 7B itself does
not renew or release pins. A known, completed partial publication may be retried
after complete closure revalidation. A crash, cancellation, or uncertain
storage result leaves the durable mutation epoch in flight and intentionally
blocks automatic retry pending explicit operator recovery; 7B never clears it
from age, lease expiry, or matching operation identity.

Snapshot and export operations reject a retention deadline at or before their
entry wall clock, including existing-record retries. Caller-controlled
`created_at` cannot revive an elapsed request.

### GC and finalization coordination

Mutating GC acquires the workspace retention/GC lease, atomically claims
`retention/coordination/mutation-epoch.json`, re-proves the lease once, and then
holds the durable `IN_FLIGHT` epoch through protection inventory and every
delete. Dry-run GC is epoch-free and read-only. Snapshot creation claims before
its first checkpoint; retries and exports claim before final closure
revalidation and publication. Every coordinated write/delete completes before
the owner CAS-settles the exact epoch version to `IDLE`. A foreign or malformed
`IN_FLIGHT` epoch or claim CAS loss aborts with zero product mutation. An absent
record claims epoch 1; a validated `IDLE(n)` record claims checked `n + 1` under
the exact observed-version CAS. Exhaustion at `u64::MAX` fails closed without
rewriting the record. Lease fencing sequence remains lease-local and never
numbers the durable epoch, because force-break or stale-lock deletion can
recreate it at 1. Unknown write/delete outcomes and cancellation remain
`IN_FLIGHT`; they are never TTL-cleared. Per-mutation lease renewal is forbidden
because the epoch, not an expired lease, excludes a newer holder while the
original owner finishes already-authorized mutations.

For every active snapshot or export root, GC validates the complete selected
pin chain and requires revision 1 to match the immutable target record's pin
ID, target, creation time, and initial retention deadline. An alias pin or a
substituted revision 1 aborts GC before deletion. Later valid active revisions
remain eligible protection roots.

### Required object closure

The service must include and verify the complete explicit object closure:

- authority manifest and checkpoint paths from each persisted checkpoint
  reference;
- projection manifest/artifact paths returned by the provider;
- event archive manifest paths returned by the provider; and
- the exact source snapshot record, which must appear exactly once at its
  canonical path with `SnapshotRecord` kind, exact byte size, and the digest of
  its stored bytes; and
- compatibility artifacts, which must also occur in required objects with the
  identical digest and `LegacyCompatibility` kind.

For each object, use exact `get_raw`, hash the exact bytes, compare any supplied
digest, and record the exact byte size. Duplicate paths with disagreeing
metadata fail closed. Never infer an object by prefix or storage listing.

### Get and export

`get_snapshot(snapshot_id)` and `get_export(export_id)` validate the typed ID,
format one canonical path, read it directly, and use the Phase 7A validating
codec. Not-found stays typed and no raw backend text is returned.

`export_snapshot` first performs a direct idempotent lookup by `export_id`.
When that record exists, immutable request and pin-binding conflicts fail
before any read of a caller-supplied alternate source pin. Both first creation
and exact retry then load the source snapshot by `snapshot_id`, require the
explicit source pin ID to equal the snapshot record's immutable binding, and
directly validate the complete selected pin chain. Revision 1 must match the
bound pin ID, snapshot target, creation time, and initial retention deadline;
the latest revision must still be active at the operation wall clock and retain
the same target. It validates record version, scope, retention, relocation, and
pin state. It re-reads and hashes every explicitly required snapshot object,
validates all authority/projection/archive and compatibility cross-references,
adds the exact source snapshot record, and constructs a new export manifest
with
`RelocationPolicy::relative_to_caller_export_root()`. The runtime
`ScopedStorage` is the caller-supplied export root. Its provider/bucket URI is
never serialized. Export verifies and records; it does not copy objects, write
old compatibility paths, or publish Parquet/current roots.

First creation and exact retry use the same deterministic source-derivation
routine. A retry decodes its stored export and compares the known v1 semantics
against that freshly derived manifest before any target-pin write. A stored
manifest that omits or changes any source domain, projection, archive, required
object, compatibility artifact, or source-record evidence fails closed even
when it is internally valid. Comparison is typed rather than raw-byte equality,
so additive unknown top-level v1 fields remain compatible.

Add only the missing safe snapshot/export accessors needed by the service:
bound pin IDs, scope, creation time, retention deadline, and relocation policy.

### Read-only restore preflight

`preflight_restore` accepts a typed snapshot-or-export source ID, its explicit
retention pin ID, expected workspace scope, and caller-supplied `now`. It
loads the record first and rejects a caller pin ID that differs from the
record's immutable target binding before reading that alternate pin. For the
bound ID, it directly validates the complete selected chain and exact
revision-1 record semantics without listing. Missing, corrupt, released,
expired, and wrong-target pin state is classified in the report. It returns a
deterministic report with sorted, deduplicated issues:

- `Missing`
- `Corrupt`
- `Expired`
- `Incompatible`
- `OutOfScope`

Check the record envelope and scope first. A scope mismatch returns only a
redacted `OutOfScope` issue before any bound-pin or artifact probe. In-scope
preflight then checks retention, the bounded selected-pin chain, every required
object for presence, exact size, and SHA-256, then every domain binding and
authority implementation; then call the matching adapter's
`resolve_persisted_reference_at(reference, now)` to verify stable authority
data. The legacy adapter method delegates with `Utc::now()`. An authority
expired at the supplied time is reported only as `Expired`, not
`Incompatible`. A permission or backend outage is an operation error, not
`Missing`.

Issue details expose safe record/domain/category identifiers only. They omit
provider URIs, object content, checksums, opaque tokens, and raw backend errors.
Preflight performs no `put`, `delete`, `list`, checkpoint, transaction, or
provider-capture call. It never writes or repairs old compatibility paths.

## Task 1: Add Shared Paths And Missing Accessors Test-First

**Files:**

- Modify: `crates/arco-catalog/tests/workspace_snapshot_services.rs`
- Modify: `crates/arco-catalog/src/workspace_snapshot.rs`
- Modify: `crates/arco-catalog/src/gc/reachability.rs`
- Modify if required: `crates/arco-catalog/src/gc/collector.rs`

**Step 1: Add the first failing service contract test**

Import wished-for validated snapshot/export/pin path helpers and assert exact
canonical paths. Assert malformed IDs are rejected before path formatting.
Assert the safe export getters return the validated source values.

**Step 2: Capture red**

```bash
cargo test -p arco-catalog --test workspace_snapshot_services paths -- --nocapture
```

Expected: compile failure because the shared path/service surface does not
exist. The filter must later run nonzero tests.

**Step 3: Implement minimum shared helpers**

Move path construction out of GC into `workspace_snapshot.rs` behind validated
functions. Add the export getters. Update reachability/collector imports and
prove the existing GC tests remain green.

## Task 2: Implement The Explicit Registry And Provider Boundary

**Files:**

- Create: `crates/arco-catalog/src/workspace_snapshot_service.rs`
- Modify: `crates/arco-catalog/src/lib.rs`
- Modify: `crates/arco-catalog/tests/workspace_snapshot_services.rs`

**Step 1: Add failing registry tests**

Cover canonical order, empty/duplicate bindings, state-scope mismatch, mandatory
provider construction, and a binding whose state store lacks checkpoints.

**Step 2: Capture red**

```bash
cargo test -p arco-catalog --test workspace_snapshot_services registry -- --nocapture
```

Expected: compile failures for the missing registry/provider types.

**Step 3: Implement the minimum registry**

Use private fields, validated constructors, and `BTreeMap`. Do not implement
`Default`, an empty provider, or a production mock. Expose only canonical
iteration and exact-domain lookup needed by create/export/preflight.

## Task 3: Create A Workspace Snapshot Without Publishing Authority

**Files:**

- Modify: `crates/arco-catalog/src/workspace_snapshot_service.rs`
- Modify: `crates/arco-catalog/tests/workspace_snapshot_services.rs`

**Step 1: Add one failing behavior at a time**

Tests must prove:

- every configured domain is checkpointed in canonical order;
- capability denial across any binding occurs before the first checkpoint;
- adapter implementation/scope/domain mismatch aborts before publication;
- projection/archive provider failure leaves snapshot record and pin selector
  absent;
- all authority/provider objects are direct-read, byte-sized, and hashed;
- the record, pin revision, and selector use only approved `retention/` paths;
- a same-ID retry returns the existing record without another checkpoint;
- byte-identical concurrent winners succeed and different bytes conflict; and
- no public Parquet, manifest pointer, root token, authority pointer, or old
  compatibility path is written.

Use recording/fault-injection state stores, adapters, providers, and a storage
backend. Do not depend only on `ControlMvpStateStore`, because the order and
failure assertions need deterministic spies.

**Step 2: Capture red for each behavior**

```bash
cargo test -p arco-catalog --test workspace_snapshot_services create_ -- --nocapture
```

**Step 3: Implement minimal create/publication logic**

Keep capability gating, capture, record building, immutable writes, and pin
publication as separate private functions. Validate the complete in-memory
record before the first `put`. On collision, compare canonical exact bytes.

## Task 4: Add Direct Get And Verified Export

**Files:**

- Modify: `crates/arco-catalog/src/workspace_snapshot_service.rs`
- Modify: `crates/arco-catalog/tests/workspace_snapshot_services.rs`

**Step 1: Add denied-list and export failure tests**

Use a backend whose `list` always records and fails. Prove direct get and export
succeed with zero list calls. Cover missing object, size mismatch, checksum
mismatch, expired source, incompatible version/relocation, scope mismatch,
unknown domain/implementation, conflicting retry, and read-only old-path
compatibility. Verify no object copying or compatibility write occurs.
Seed an internally valid one-domain same-ID export for a valid two-domain
source snapshot, remove its target selector/revision, and prove retry rejects
the source-divergent cut before any target-pin write. Preserve a retry whose
manifest is truly source-derived and whose target pin has a valid later active
revision.

**Step 2: Capture red**

```bash
cargo test -p arco-catalog --test workspace_snapshot_services export_ -- --nocapture
cargo test -p arco-catalog --test workspace_snapshot_services no_list -- --nocapture
```

**Step 3: Implement direct get/export**

Follow only IDs and references contained in validated records. Hash every
required object's exact bytes. Revalidate cross-reference closure before
encoding. Publish export record and revision-1 export pin through the same
immutable helper used by snapshot creation.

## Task 5: Add Complete Read-Only Restore Preflight

**Files:**

- Modify: `crates/arco-catalog/src/workspace_snapshot_service.rs`
- Modify: `crates/arco-catalog/tests/workspace_snapshot_services.rs`

**Step 1: Add failing report tests**

Create one deterministic case for each issue category and one aggregate case
that returns multiple sorted issues. Prove malformed/corrupt records fail
closed, safe reports redact sensitive data, backend permission/outage errors
remain operation errors, and adapter resolution occurs only for matching
bindings.

Use a recording backend/provider set to assert zero writes, deletes, lists,
checkpoints, transactions, provider captures, and old-path mutations.

**Step 2: Capture red**

```bash
cargo test -p arco-catalog --test workspace_snapshot_services preflight_ -- --nocapture
```

**Step 3: Implement the minimum report**

Use public issue/report types with private fields and safe accessors. Sort by
category, domain, and safe identifier. Continue after artifact-level issues so
the report is complete; stop only for malformed envelope or non-classifiable
backend failure.

## Review Hardening Amendment

The fresh reviews add six release blockers to this same amendable 7B
commit:

1. Coordinate snapshot/export finalization with mutating GC and prove continuous
   lease ownership before every unconditioned mutation. Add a core stale-holder
   takeover regression, deterministic GC/publication interleaving, acquisition
   failure, lease-loss, and zero-stale-delete tests.
2. Revalidate an existing snapshot/export record's complete object closure
   under coordination before crash recovery may recreate its active pin.
3. Enforce operation wall-clock expiry for snapshot retries and first/retry
   exports, independent of caller timestamps.
4. Require explicit source pin IDs for export and restore preflight. Reuse one
   exact-path complete-chain validator for request-time service and GC.
5. Add explicit-time persisted-authority resolution and use preflight's supplied
   time. Expired authority references must produce only `Expired`.
6. Bind target pin identity into every snapshot/export record and source pin
   identity into every export. Reject same-record retries with different pin
   identities without writing a second root. When the bound selector exists,
   validate its complete chain, target, immutable revision-1 semantics, and
   active status; accept legitimate later active revisions without rewriting
   revision 1 or the selector. Enforce the same record-bound revision-1
   semantics for export source pins, restore preflight, and active GC roots.
   Validate an existing export's immutable source-pin identity before reading
   any caller-supplied alternate pin.
7. Replace the check-then-act lease-only protocol with one canonical v1 durable
   mutation epoch shared by snapshot/export and every mutating GC run. Claim it
   atomically under the lease, re-prove once, keep it `IN_FLIGHT` through all
   mutations, and settle only after every mutation returns. Foreign, malformed,
   or uncertain epochs fail closed without automatic recovery.
8. Reject a selected pin whose latest retention deadline exceeds the immutable
   snapshot/export and authority cut's usable deadline in creation/retry,
   export source validation, restore preflight, and GC protection.
9. Check decoded record scope before probing the bound pin so out-of-scope
   preflight returns one redacted issue with zero pin reads.
10. Bound complete selected-pin traversal at 1,024 revisions and reject an
    oversized selector before reading any revision object.
11. Derive first and retry exports through one routine from the exact source
    snapshot bytes and fully re-hashed retained cut. Require exactly one
    canonical `SnapshotRecord`, and compare decoded known v1 semantics before
    recreating a missing target pin while preserving additive v1 fields.
12. Allocate durable mutation epochs exclusively from the exact previous epoch
    record: absent to 1 and validated `IDLE(n)` to checked `n + 1`. Prove that a
    released lock deleted and recreated at lease sequence 1 still claims durable
    epoch 2, while overflow, malformed/in-flight state, and CAS loss remain
    fail-closed.

## Task 6: Verify, Commit, And Review The Slice

**Step 1: Run the full 7B gate**

```bash
cargo test -p arco-catalog --test workspace_snapshot_services -- --nocapture
cargo test -p arco-catalog --test workspace_snapshot_contracts -- --nocapture
cargo test -p arco-catalog gc::reachability -- --nocapture
cargo test -p arco-catalog gc -- --nocapture
cargo test -p arco-catalog --test state_store_control_mvp -- --nocapture
cargo test -p arco-catalog --test state_store_current_adapter -- --nocapture
cargo test -p arco-core lock::tests -- --nocapture
cargo check -p arco-catalog
cargo clippy -p arco-catalog --lib -- -D warnings
cargo clippy -p arco-core --all-features -- -D warnings
cargo fmt --all --check
git diff --check
git diff --check 003a9dc15a911209c5c25106f7ec6834a68b0db2...HEAD
```

Every filtered command must execute nonzero relevant tests.

**Step 2: Audit scope and writes**

```bash
git status --short
git diff --stat 003a9dc15a911209c5c25106f7ec6834a68b0db2
git diff --name-only 003a9dc15a911209c5c25106f7ec6834a68b0db2
rg -n "\.list\(|snapshots\.parquet|manifest_root|current\.pointer|begin_txn" \
  crates/arco-catalog/src/workspace_snapshot_service.rs
```

Confirm every request-time service path is direct-addressed; any source-level
`list` match is absent. Confirm no public projection, authority/root pointer,
transaction, route, proto, CLI/SQL, grant, DDL, or credential-vending change.

**Step 3: Stage narrowly and commit once**

```bash
git add docs/plans/2026-07-15-phase-7b-workspace-snapshot-export.md \
  crates/arco-catalog/src/retention_coordination.rs \
  crates/arco-catalog/src/workspace_snapshot.rs \
  crates/arco-catalog/src/workspace_snapshot_service.rs \
  crates/arco-catalog/src/gc/reachability.rs \
  crates/arco-catalog/src/gc/collector.rs \
  crates/arco-catalog/src/gc/mod.rs \
  crates/arco-catalog/src/state_store.rs \
  crates/arco-catalog/src/state_store/control_mvp.rs \
  crates/arco-catalog/src/lib.rs \
  crates/arco-catalog/tests/state_store_control_mvp.rs \
  crates/arco-catalog/tests/workspace_snapshot_contracts.rs \
  crates/arco-catalog/tests/workspace_snapshot_services.rs \
  crates/arco-core/src/lock.rs
git commit -m "feat(catalog): implement workspace snapshot exports"
```

Do not amend or squash Phase 7A.

**Step 4: Run ordered fresh reviews**

Record `BASE_SHA=003a9dc15a911209c5c25106f7ec6834a68b0db2` and the
new `HEAD_SHA`. Dispatch a fresh spec-compliance reviewer first. Fix every
correctness, safety, authority-boundary, and scope blocker by amending only the
7B commit; rerun the gate and spec review until approved. Then dispatch a fresh
code-quality reviewer. Fix Important/Critical findings, amend, reverify, and
re-review the final SHA.

**Exit gate:** explicit configuration has no fallback providers; every first
creation checkpoints every domain and publishes nothing until the complete cut
validates; exact retries are safe and conflicts fail; get/export/preflight use
no listing; a durable epoch excludes mutating GC and finalization across lease
expiry, while uncertain mutation outcomes remain fail-closed and in flight;
crash recovery revalidates closure; explicit source pins and authority time are
validated deterministically; immutable record-to-pin bindings reject alias
creation, retry, preflight, and GC roots without probing alternate pin paths,
while valid advanced selected revisions within the immutable cut deadline
remain retry-safe; export
re-hashes every object, requires its exact canonical source snapshot record,
rejects source-divergent same-ID retries before target-pin publication, and
writes no
old/public/authority path; preflight reports missing, corrupt, expired,
incompatible, and out-of-scope artifacts without mutation; the worktree is
clean at exactly two Phase 7 commits and contains no restore, handle, transport,
or Phase 8 work.
