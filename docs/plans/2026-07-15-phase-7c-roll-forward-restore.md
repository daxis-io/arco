# Phase 7C Roll-Forward Restore Implementation Plan

> **Execution requirements:** Use `test-driven-development` for every
> behavior, keep code mutations with one worktree owner, and run fresh
> independent read-only spec-compliance and code-quality review gates before
> advancing to Phase 7D.

**Goal:** Restore one domain or an explicitly selected workspace snapshot/export
cut by publishing strictly newer state-store authority, with a durable exact-path
journal that makes every crash point and partial multi-domain result recoverable
without rolling back or duplicating a visible participant.

**Architecture:** Add one deep restore workflow module behind a small
`WorkspaceRestoreService` interface. The module reuses Phase 7B direct-addressed
source validation, requires an explicitly configured restore adapter at the
state-store seam, and persists immutable attempt plans before it invokes any
participant mutation. The first production adapter is a Control MVP adapter
that deterministically plans transaction and manifest identities, publishes
through the existing current-pointer CAS, and proves completion by exact-path
manifest-lineage inspection. Workspace restore is a canonical-order,
repairable sequential workflow; its final immutable read manifest is opt-in
metadata and is never a new public root or distributed-transaction claim.

**Tech Stack:** Rust 2024, async-trait, `Arc`, `BTreeMap`/`BTreeSet`, chrono,
serde/JCS JSON, SHA-256, `ScopedStorage`, object-store create-if-absent and CAS,
the existing `ArcoStateStore`/Control MVP transaction model, and Cargo.

**Original slice base:** `2af20b9b6bd36ca46211683599a180fd6eb0ec45`

**Refreshed slice parent:** `1c75799a5450ce2a551af89c099a5f01717b8c18`

---

## Provenance And Feasibility Gate

This plan is the first and only Phase 7C change at authoring time. It must be
committed in the same Phase 7C commit but must predate every production and test
change in that commit. Before starting Task 1, capture:

```bash
git status --short --branch
git diff --name-only 1c75799a5450ce2a551af89c099a5f01717b8c18
```

Expected before implementation: this plan is the only changed path.

**Feasibility verdict: proceed only through an explicit restore-participant
adapter.** The current generic `ArcoStateStore` interface is insufficient by
itself. `begin_txn` returns an opaque transaction, normal Control MVP
transaction/manifest IDs include a random ULID, and the generic interface has
no durable outcome-inspection operation. Therefore a crash after pointer CAS
but before journal advancement cannot be distinguished generically from a
failed attempt. Treating `request_id` as idempotency would be false: today it
does not determine the transaction or manifest identity.

Phase 7C is feasible only if all of the following are implemented and proven:

1. `WorkspaceDomainBinding` receives an explicit restore-participant adapter;
   absence is unsupported, never a fabricated fallback.
2. Control MVP gets a concrete adapter whose immutable attempt plan determines
   its transaction ID, manifest ID, exact paths, expected checksums, base
   pointer version, and resulting sequence before mutation.
3. Recovery directly reads the planned transaction, planned manifest, current
   pointer, and current manifest lineage. It never lists and never trusts the
   mutable journal alone as proof of domain visibility.
4. A planned transaction found in current manifest lineage is complete even if
   a later writer has advanced the current pointer. A new immutable attempt is
   legal only after exact inspection proves the old plan `Superseded`: current
   checksum-valid lineage excludes the planned transaction and the observed
   base-pointer version is irreversibly lost. Partial visibility, `APPLYING`, a
   generic participant failure, or storage uncertainty never authorizes a new
   attempt; helpers adopt and resume the same deterministic active attempt.
5. `CurrentStateStore` remains unsupported and does not implement the adapter.

If any of those proofs would require making `StateToken` serializable, adding a
generic “assume committed” hook, changing normal transaction retry policy,
listing state-store objects, or fabricating catalog/orchestration root receipts,
stop at the committed Phase 7B head. Do not create the Phase 7C commit.

## One-Commit Scope

Allowed files:

- `docs/plans/2026-07-15-phase-7c-roll-forward-restore.md`
- `crates/arco-catalog/src/lib.rs`
- `crates/arco-catalog/src/state_store.rs`
- `crates/arco-catalog/src/state_store/control_mvp.rs`
- `crates/arco-catalog/src/retention_coordination.rs`
- `crates/arco-catalog/src/workspace_snapshot_service.rs`
- Create `crates/arco-catalog/src/workspace_restore.rs`
- `crates/arco-catalog/tests/state_store_control_mvp.rs`
- Create `crates/arco-catalog/tests/workspace_snapshot_restore.rs`

No Cargo manifest change is expected. Do not modify `arco-core`, `arco-api`,
`arco-proto`, GC, public Parquet/system tables, protobuf, CLI, SQL, DDL, grants,
credential vending, transaction retry policy, or production catalog/
orchestration mutation authority. If implementation needs another path, stop
and amend this plan before touching it.

**Safety amendment discovered during implementation:** an exact active-pin read
immediately before participant apply is not, by itself, a linearization point
against the crate-owned mutating GC path. Add a `WorkspaceRestoreApply`
retention-mutation kind and hold the existing distributed retention lock plus
durable mutation epoch from the final source/pin validation through the
participant pointer CAS and durable journal receipt (or durable
`REPAIR_REQUIRED` result). This serializes restore with production GC and with
future legitimate pin mutators that use the same protocol. It does not claim
to serialize unsupported raw object-store writes that bypass the crate-owned
protocol. A failed/cancelled coordination epoch must fail closed; do not weaken
GC coordination or infer liveness from an abandoned `IN_FLIGHT` epoch.
In particular, when a participant apply returns an error and exact inspection
still reports only nonterminal `Ready`, retain the matching epoch `IN_FLIGHT`
and the journal in operator-repair state. Automatic recovery must neither retry
that plan nor publish a replacement until exact inspection proves `Visible` or
`Superseded`; only that terminal proof may settle the epoch and continue.

All work is one amendable commit:

```text
feat(catalog): add roll-forward snapshot restore
```

Do not make intermediate commits. Preserve red/green command output for the
final report.

## Committed 7B Seams To Reuse

- `WorkspaceSnapshotService::preflight_restore` already validates the source
  envelope, workspace scope, bound active pin and complete pin chain, every
  required object, every compatibility reference, and every persisted domain
  authority through exact reads.
- `RestoreSource` carries an explicit snapshot/export ID and explicit immutable
  target pin ID. Restore must not accept an unpinned source.
- `WorkspaceDomainRegistry` is canonical by domain name and has no fallback
  providers.
- Every source `DomainAuthorityReference` contains a validated
  `PersistedAuthorityReference`; opaque `CheckpointToken` and `StateToken`
  values are not serialized.
- `ControlMvpStateStore` already publishes immutable transaction and manifest
  objects, then makes them visible only through current-pointer CAS.
- A Control MVP manifest contains the full ordered transaction-reference
  lineage. That lineage is the durable recovery evidence for a restore whose
  journal update was interrupted.
- `ScopedStorage::{get_raw, head_raw, put_raw}` provides exact-path reads and
  conditional writes. `list` and `list_meta` are forbidden in every restore
  request and recovery path.

Refactor Phase 7B helpers only enough to share a fully validated internal
restore cut. Do not weaken or duplicate its preflight rules.

## Hard Semantic Decisions

### Roll-forward, never rollback

For a Control MVP participant, “restore” means:

- load and verify the retained checkpoint authority named by the source cut;
- read the current base authority;
- calculate the deterministic key-value delta from current state to the
  retained state;
- stage puts for retained live values and deletes for current live keys absent
  from the retained cut;
- commit that delta as a new immutable transaction and new immutable manifest;
  and
- publish the new manifest only through the existing current-pointer CAS.

An empty delta still commits a new transaction. The resulting logical sequence
must equal `current_sequence + 1` and must be strictly greater than both the
current and retained source sequences. If the retained source sequence is
ahead of the current sequence, or sequence increment overflows, preflight fails
before journal or domain mutation.

Historical transaction, manifest, checkpoint, snapshot, export, required
object, and compatibility paths are read-only. Restore never overwrites or
deletes them. A reader pinned to an old snapshot must return the same bytes
before and after restore.

The Control MVP projection outbox is append-only history, not restored mutable
state. Preserve the current outbox and append one deterministic typed restore
notice in the new transaction so projectors can rebuild from the restored
authority. Do not copy or truncate historical outbox entries. The restored
key-value authority must equal the retained reader; audit/outbox history remains
monotonic.

### Explicit omissions

Add exactly:

```rust
pub enum OmittedDomainPolicy {
    Omit,
    Reject,
}
```

It has no `Default` and no serde default.

- `RestoreWorkspaceToSnapshot` always requires one of these values.
- `Reject` requires exact equality between configured registry domains and
  source-cut domains.
- `Omit` restores every source-cut domain for which the registry has an exact
  binding, leaves configured domains absent from the source untouched, and
  persists their canonical names in `omitted_domains`.
- A source-cut domain absent from the registry is incompatible under both
  policies; it is not silently omitted.
- Omitted domains contribute no carried-forward token, manifest, root entry,
  provider output, or compatibility path. Carry-forward is not offered.
- `RestoreDomainToSnapshot` names exactly one source domain. Other domains are
  outside that operation, not silently carried into its read manifest.

### Existing root transactions are not generic restore transactions

The current `RootTxRecord`, `RootTxManifest`, and `RootTxReceipt` describe
genuine catalog DDL and orchestration batch commits produced by the existing
control-plane transaction implementation. The audited current interfaces do
not contain a typed state-replacement operation for either domain. A generic
Control MVP manifest is not a catalog/orchestration commit receipt, even when
its domain string happens to be `catalog` or `orchestration`.

Therefore this slice must not construct or write `RootTxRecord`,
`RootTxManifest`, `RootTxReceipt`, `DomainCommit`,
`transactions/root/*`, or `commits/root/*` for state-store restore. The
root-eligible participant set is empty until a genuine typed restore operation
exists in the existing catalog/orchestration transaction module. If such a
participant is added later, it must invoke that existing mutation module and
reuse its unmodified records and receipts; it may not translate a generic
state-store manifest into them.

Phase 7C instead describes configured state-store domains honestly as a
canonical-order, repairable sequential workflow. Its final
`WorkspaceRestoreReadManifest` is an opt-in pinned read cut over already-visible
participant manifests. It is not a `RootTxManifest`, is not selected by any
production current/root pointer, and does not claim atomic cross-domain
visibility. Keep the existing root protocol tests green to prove reuse rather
than redefinition.

## Restore-Participant Seam

Add a separate `StateRestoreParticipant` interface in `state_store.rs`; do not
add restore methods to `ArcoStateStore`:

```rust
#[async_trait]
pub trait StateRestoreParticipant: Send + Sync {
    fn implementation(&self) -> &'static str;
    fn scope(&self) -> &StateScope;
    fn restore_binding_identity(&self) -> StateStoreBindingIdentity;

    async fn plan_restore(
        &self,
        source: &PersistedAuthorityReference,
        identity: &RestoreAttemptIdentity,
        now: DateTime<Utc>,
    ) -> Result<PersistedRestoreParticipantPlan>;

    async fn inspect_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
    ) -> Result<RestoreParticipantInspection>;

    async fn apply_restore(
        &self,
        plan: &PersistedRestoreParticipantPlan,
        now: DateTime<Utc>,
    ) -> Result<RestoreParticipantInspection>;
}
```

The exact names may follow repository style, but preserve this three-operation
interface and these invariants:

- `plan_restore` is read-only and returns complete durable evidence.
- `inspect_restore` is read-only, exact-path, and returns `Ready`, `Visible`,
  or `Superseded`; ambiguous/corrupt evidence is an error.
- `apply_restore` accepts only a persisted plan, recomputes and verifies its
  exact candidate bytes, uses immutable writes plus the existing pointer CAS,
  and returns the same inspection vocabulary.
- `Visible` carries an in-memory opaque `StateToken` plus serializable stable
  manifest evidence. The token itself never enters a record.
- `Superseded` means the planned candidate never entered current lineage after
  its base lost CAS. It never means the adapter may overwrite the winner.
- `restore_binding_identity` is an opaque, non-serializable, process-local
  identity for the backing storage authority. The binding's state store must
  expose the same identity; implementation and scope strings alone are not
  sufficient.

Add a `roll_forward_restore()` capability flag only to Control MVP. Preserve
all existing capability methods. `CurrentStateStore` and the deterministic
model do not gain the flag and do not implement `StateRestoreParticipant`.

Add an explicit `ControlMvpRestoreParticipant` adapter wrapping a
`ControlMvpStateStore`. `WorkspaceDomainBinding` gains an optional adapter slot
only through an explicit builder such as:

```rust
pub fn with_restore_participant(
    self,
    participant: Arc<dyn StateRestoreParticipant>,
) -> Result<Self>;
```

The existing constructor continues to mean “snapshot/export capable, restore
unsupported.” The builder rejects scope, domain, implementation, capability,
missing binding identity, or backing-authority identity mismatch. Restore
preflight requires `Some(adapter)` for every participating domain before it
writes anything. There is no no-op adapter.

## Control MVP Durable Attempt Plan

`PersistedRestoreParticipantPlan` is a typed enum. In this slice it has one
production variant, `ControlMvp(ControlMvpRestorePlan)`. Do not use arbitrary
JSON payloads or a backend type-name/downcast convention.

The version-1 `ControlMvpRestorePlan` contains at least:

- exact implementation and `StateScope`;
- `restore_id`, positive `attempt`, and canonical domain;
- source authority manifest ID, logical sequence, exact path, and SHA-256;
- source checkpoint path and SHA-256;
- current base manifest ID/sequence (or explicit empty-base marker);
- the stable object-store version observed for the current pointer;
- SHA-256 of the exact raw base-pointer bytes bound to that observed version
  (or an explicit absent-pointer marker), separately from the candidate;
- deterministic transaction ID, transaction path, and exact planned SHA-256;
- deterministic candidate manifest ID, manifest path, and exact planned
  SHA-256;
- deterministic candidate current-pointer payload SHA-256;
- resulting logical sequence; and
- deterministic restore-outbox record ID and safe source/result sequence
  metadata.

It must not contain source key/value bytes, opaque tokens, provider roots,
credentials, raw backend errors, or serialized trait objects. The adapter
re-reads the exact source/base manifests and transaction objects and recomputes
candidate bytes; the persisted checksums bind that recomputation.

Derive deterministic IDs from a canonical SHA-256 of workspace scope, domain,
restore ID, attempt number, source manifest evidence, and base manifest
evidence. Do not call `Ulid::new()` in the restore planning/apply path. Keep
normal `begin_control_txn` behavior unchanged.

Load the current pointer as a version-bound observation: HEAD, GET, HEAD again,
and retry if the versions differ. A plan must never bind bytes fetched after a
different HEAD version. Bound retries and fail closed on instability.

`apply_restore` follows this exact protocol:

1. Revalidate plan structure, implementation, scope, canonical paths, and
   checksums.
2. Revalidate the source authority and active source retention at the actual
   mutation decision time.
3. Re-read the exact source and planned base objects, recompute the source/base
   states, delta, restore outbox record, transaction bytes, manifest bytes, and
   pointer bytes, and require every planned checksum to match.
4. Call `inspect_restore`. If `Visible`, return the existing token/evidence. If
   `Superseded`, return it without writing. Continue only from `Ready`.
5. Put the planned transaction and manifest with `DoesNotExist`; accept only a
   byte-identical existing winner.
6. CAS the existing current pointer from the exact planned base version. For an
   empty base use `DoesNotExist`.
7. On success or any uncertain/CAS result, inspect again. Return `Visible` only
   when the planned transaction reference occurs in a checksum-valid manifest
   lineage selected by the current pointer. If a later manifest contains that
   reference, it remains `Visible`. If the current lineage excludes it and the
   base changed, return `Superseded`.

Never CAS from a newly observed winner inside the same attempt. A superseded
attempt leaves its immutable, unreachable transaction/manifest artifacts and
the previous winner visible. Repair creates attempt `n + 1` from a newly
preflighted base only after durable exact inspection proves supersession.
Ready, uncertain, and partially completed participant plans always retain their
originating participant attempt identity and exact bytes. The workspace
attempt is an aggregate generation. If any unfinished participant is proven
`Superseded`, aggregate `n + 1` contains newly planned bytes only for those
superseded participants and carries every other unfinished Ready/uncertain plan
byte-for-byte, including its older participant-attempt identity and digest.
Because every candidate byte is deterministic, any recovery helper may safely
resume a carried Ready plan; immutable writes converge and pointer CAS makes at
most one candidate visible.

Before every apply, a helper stable-reads the journal and requires lifecycle
`APPLYING`, the exact active aggregate attempt/path/digest, and the domain's
exact originating participant attempt plus participant-plan digest. If any
metadata changed, it re-enters recovery rather than applying from stale memory.
An aggregate-1 helper stops after aggregate 2 is selected. An aggregate-2 helper
inspects carried plans again and adopts a carried plan that raced to visibility
just before aggregate selection. After a superseded participant is newly
planned, its old executor cannot publish because its exact base-pointer CAS
version has already been proven lost.

## Restore Record Layout And Validation

Use canonical uppercase `rst_<26-character ULID>` restore IDs and positive,
checked attempt/revision numbers. Path helpers validate before formatting:

```text
transactions/restores/{restore_id}/request.json
transactions/restores/{restore_id}/attempts/{attempt:020}.plan.json
transactions/restores/{restore_id}/journal.json
transactions/restores/{restore_id}/read.manifest.json
```

Request-time and recovery code formats these exact paths. It never enumerates
`transactions/restores/`.

All records carry `record_type` plus `version: 1`, typed workspace scope, and
the restore ID. Reject unsupported versions, invalid IDs, unsafe paths,
duplicate/unsorted domains, unknown lifecycle strings, inconsistent scope,
sequence overflow, and cross-record digest/attempt mismatches before acting on
them.

Do not rely on `BTreeMap` deserialization to canonicalize malformed wire input.
Decode participant and domain collections first as ordered raw entry vectors,
reject duplicate or non-increasing names, then build the in-memory map. Add
raw-wire tests for duplicate and reversed encodings.

### Immutable request

`WorkspaceRestoreRequestRecord` binds retry identity:

- restore target: `domain { domain }` or `workspace`;
- source kind, source ID, and source pin ID;
- workspace scope;
- caller request timestamp;
- explicit workspace `OmittedDomainPolicy` (absent only for domain restore);
  and
- canonical request SHA-256.

The same restore ID with different source, pin, scope, target domain, timestamp,
or omission policy fails with `PreconditionFailed`. Immutable create-if-absent
accepts only byte-identical or typed-semantic-equivalent retry content.

### Immutable attempt plan

`WorkspaceRestoreAttemptPlan` is an aggregate and contains:

- restore ID, attempt number, request digest, and source record digest;
- canonical participants, each encoded as domain, originating participant
  attempt, exact participant-plan digest, and typed persisted plan;
- canonical explicit `omitted_domains`;
- the active source retention deadline used by preflight; and
- its own canonical SHA-256 recorded by the journal.

Every participant includes the source `PersistedAuthorityReference` evidence
for that exact domain. Phase 7C accepts only
`PersistedAuthorityKind::Checkpoint`; `StateToken` authority is rejected before
any write because the recovery protocol requires exact checkpoint path and
checksum evidence. Attempt 1 plans every required participant before the first
journal write. Repair revalidates every required participant and every prior
completed receipt before any new attempt-plan or journal write. A replacement
aggregate contains every unfinished participant: participants whose old plans
were durably proven `Superseded` receive new plans whose participant-attempt
equals the new aggregate attempt; all Ready/uncertain participants carry their
old exact plan, origin attempt, and digest unchanged. Completed participants
remain journal receipt evidence and are not aggregate-plan entries. An inner
participant attempt must be positive and no greater than its aggregate attempt.
Failed revalidation or superseded-participant planning writes no new aggregate
plan and no journal revision.

### CAS journal

`WorkspaceRestoreJournal` contains:

- restore ID, scope, request path/digest, monotonically increasing journal
  revision, and lifecycle;
- active attempt number, exact attempt-plan path, and plan digest;
- the immutable canonical required and omitted domain sets;
- per-required-domain state: `PLANNED` with exact originating participant
  attempt and participant-plan digest, or `VISIBLE` with immutable stable
  participant receipt;
- safe last failure category (`CAS_LOST`, `PARTICIPANT_FAILED`, or
  `STORAGE_UNCERTAIN`) without raw backend text; and
- optional frozen finalization time and exact read-manifest path/digest.

Lifecycle transitions are:

```text
PREPARED -> APPLYING -> FINALIZING -> VISIBLE
                      -> REPAIR_REQUIRED
PREPARED -> REPAIR_REQUIRED         (adopted durable orphan proven invalid before apply)
REPAIR_REQUIRED -> APPLYING          (same active plan when still Ready/uncertain)
REPAIR_REQUIRED -> APPLYING          (new plan only after proven Superseded)
FINALIZING -> VISIBLE
```

`VISIBLE` is terminal. `FINALIZING` is allowed only when every required domain
has a validated visible receipt. Any persisted `APPLYING` journal with a strict
nonempty subset of visible participants is interpreted as repair-required on
load and must be CAS-marked `REPAIR_REQUIRED` before another participant
mutation. There is no cross-domain abort or undo after any participant is
visible. The direct `PREPARED -> REPAIR_REQUIRED` edge is limited to adopting
an already-durable orphan attempt whose full participant inspection proves a
superseded or mixed visible/ready aggregate before any participant apply. A
fresh in-memory attempt still requires every participant `Ready` and writes no
restore metadata if that prepublication check fails.

Bind journal bytes to their CAS version with HEAD/GET/HEAD stability checks.
Every update increments the revision, validates the complete transition, and
uses `MatchesVersion`. On CAS loss, reload and merge only identical participant
receipts. Conflicting evidence is an invariant violation. On an uncertain write
outcome, direct-read the journal and compare the intended revision/semantics
before deciding whether to retry.

### Immutable final read manifest

`WorkspaceRestoreReadManifest` contains only:

- restore ID, source identity, scope, request digest, and frozen finalization
  timestamp;
- canonical participant stable manifest evidence (implementation, domain,
  manifest ID/path/SHA-256, logical sequence, transaction ID, and attempt);
- explicit omitted domain names; and
- `publication_mode: sequential_repairable`.

It contains no opaque token, provider URI, secret, source key/value bytes,
compatibility path, raw error, or claim of atomic visibility. Runtime results
may return opaque `StateToken`s in memory; persisted records may not.

Finalization is two-step and deterministic:

1. After every participant is proven visible, CAS the journal to `FINALIZING`
   and freeze the timestamp plus exact read-manifest bytes/digest.
2. Put the immutable read manifest, accepting only an identical winner, then
   CAS the journal to terminal `VISIBLE`.

A crash in `FINALIZING` reconstructs the exact same bytes from the frozen
journal and finishes. No production root/current pointer selects this manifest;
callers opt in by addressing the restore ID directly.

## End-To-End Workflow

Expose a small interface:

```rust
pub struct WorkspaceRestoreService { /* storage + Phase 7B source module */ }

impl WorkspaceRestoreService {
    pub fn new(storage: ScopedStorage, registry: WorkspaceDomainRegistry)
        -> Result<Self>;

    pub async fn restore_domain_to_snapshot(
        &self,
        request: &RestoreDomainToSnapshot,
    ) -> Result<WorkspaceRestoreOutcome>;

    pub async fn restore_workspace_to_snapshot(
        &self,
        request: &RestoreWorkspaceToSnapshot,
    ) -> Result<WorkspaceRestoreOutcome>;

    pub async fn recover_restore(
        &self,
        restore_id: &str,
    ) -> Result<WorkspaceRestoreOutcome>;

    pub async fn get_restore(
        &self,
        restore_id: &str,
    ) -> Result<WorkspaceRestoreOutcome>;
}
```

`WorkspaceRestoreOutcome` returns safe status, canonical completed/pending/
omitted domain names, optional final manifest, and in-memory tokens available
in the current process. It never returns raw storage errors or serialized
tokens. Expected partial/superseded outcomes return `REPAIR_REQUIRED`; malformed
records and backend outages remain typed errors.

For every new attempt or repair attempt:

1. Direct-read any existing immutable request, attempt plan, journal, and final
   manifest by restore ID. A terminal visible retry validates and returns the
   final manifest without participant preflight because it performs no
   mutation.
2. Run Phase 7B source preflight and require a ready report. Re-read the source
   record/pin and keep the validated internal cut in memory.
3. Resolve omission policy and canonical participants. Require every source
   domain to have the exact registry binding. Require every participant to have
   a matching explicit restore adapter and restore capability.
4. Inspect every previously completed receipt and every persisted unfinished
   plan in canonical domain order. Existing `Ready` or uncertain participants
   reuse their exact persisted plan; do not call `plan_restore` for them.
   `plan_restore` is called only for attempt 1 or for a participant whose prior
   exact plan was proven `Superseded` while preparing aggregate attempt `n + 1`.
   Build that new aggregate from newly planned superseded domains plus every
   unfinished Ready/uncertain domain carried byte-for-byte with its origin
   attempt/digest. Finish all participant/receipt/source preflight before any
   request/plan/journal/domain write. If any participant fails, return with zero
   new mutation.
5. Construct and validate the complete request/attempt/journal bytes in memory.
6. Create or verify the immutable request and immutable attempt plan, then
   create/CAS the durable journal to `PREPARED`/`APPLYING`. The journal must be
   durably readable before the first participant apply call.
7. For each unfinished participant in canonical order, inspect first. Reuse a
   visible result; apply only a `Ready` plan. Immediately before apply,
   stable-read the journal and require lifecycle `APPLYING`, the exact active
   aggregate attempt/path/digest, and that domain's exact originating
   participant attempt/digest in `PLANNED` state. Multiple helpers adopt the
   same plan and may race identical immutable writes. A helper that observes an
   aggregate change re-enters recovery. The new aggregate helper reinspects
   carried plans so it can adopt a just-visible old-origin result. After
   visibility, CAS the exact stable receipt into the journal before moving to
   the next participant.
8. If inspection returns `Superseded`, leave the prior current-pointer winner
   visible, mark the journal `REPAIR_REQUIRED`, and stop. Only this durable
   evidence permits retry to write attempt `n + 1`; it never retargets attempt
   `n`. An old attempt cannot later publish because its planned base CAS is lost.
9. On participant error after any visible receipt, inspect once for durable
   evidence, CAS the journal to `REPAIR_REQUIRED`, and return. Ready or uncertain
   participants keep the same attempt and are adopted by recovery. Never roll
   back or replay a participant whose planned transaction occurs in current
   lineage, and never create a second empty-delta publication for it.
10. Only after all required participants are proven visible, perform the
    `FINALIZING` protocol and return `VISIBLE`.

Before every participant apply, directly revalidate the source pin and source
authority at the actual current time. If retention expires or is released
after an earlier participant became visible, stop in `REPAIR_REQUIRED`; do not
write old paths or pretend the workflow completed.

## Task 1: Add Record And Path Contracts Test-First

**Files:**

- Create: `crates/arco-catalog/tests/workspace_snapshot_restore.rs`
- Create: `crates/arco-catalog/src/workspace_restore.rs`
- Modify: `crates/arco-catalog/src/lib.rs`

**Step 1: Write failing contracts**

Add tests for all four exact paths, canonical `rst_` IDs, positive attempts,
request retry identity, no default omission policy, canonical domain ordering,
record round trips, unsupported versions, unsafe paths, invalid journal
transitions, and token-free serialized JSON.

**Step 2: Capture red**

```bash
cargo test -p arco-catalog --test workspace_snapshot_restore restore_record_contracts -- --nocapture
```

Expected: compile failure because restore contracts/path helpers do not exist.

**Step 3: Implement only the validated records/codecs/path helpers**

Use private wire structs and validating decode functions. Keep lifecycle fields
private and expose safe read-only accessors.

**Step 4: Capture green**

Run the same command. Expected: at least one matching test executes and passes.

## Task 2: Add The Explicit Adapter Seam And Deterministic Control MVP Plan

**Files:**

- Modify: `crates/arco-catalog/src/state_store.rs`
- Modify: `crates/arco-catalog/src/state_store/control_mvp.rs`
- Modify: `crates/arco-catalog/src/workspace_snapshot_service.rs`
- Modify: `crates/arco-catalog/src/lib.rs`
- Modify: `crates/arco-catalog/tests/state_store_control_mvp.rs`
- Modify: `crates/arco-catalog/tests/workspace_snapshot_restore.rs`

**Step 1: Write failing capability/configuration tests**

Prove Control MVP advertises restore only through its explicit adapter, binding
rejects adapter scope/implementation mismatch and a same-scope adapter backed
by different storage, a binding without the adapter is restore-unsupported,
and `CurrentStateStore` remains unsupported.

**Step 2: Capture red**

```bash
cargo test -p arco-catalog --test workspace_snapshot_restore restore_adapter -- --nocapture
```

Expected: compile failure for the missing adapter/capability interface.

**Step 3: Add failing deterministic-plan tests**

Create a retained checkpoint, advance current state, and assert two planning
calls with identical `RestoreAttemptIdentity` produce the same typed plan,
transaction ID/path/digest, manifest ID/path/digest, pointer payload digest,
outbox record ID, and result sequence. Assert the plan has no source values or
opaque tokens and performs zero writes/lists.

Also assert an ahead-of-current source sequence, malformed source reference,
wrong scope/implementation, unstable pointer observation, expired reference,
`PersistedAuthorityKind::StateToken`, and sequence overflow fail before writes.
The StateToken-reference case must prove zero restore/state-store writes.

**Step 4: Capture red**

```bash
cargo test -p arco-catalog --test state_store_control_mvp restore_plan -- --nocapture
```

Expected: compile failure for the missing Control MVP plan operation.

**Step 5: Implement the minimum read-only planner**

Do not change normal transaction ID generation. Reuse Control MVP replay and
encoding internals to compute deterministic candidate bytes without writing.

**Step 6: Capture green**

Run both filters. Expected: nonzero matching tests pass.

## Task 3: Implement Control MVP Apply And Exact-Path Recovery Test-First

**Files:**

- Modify: `crates/arco-catalog/src/state_store/control_mvp.rs`
- Modify: `crates/arco-catalog/tests/state_store_control_mvp.rs`

**Step 1: Write failing roll-forward tests**

Prove restore changes the live keyspace to the retained cut, deletes keys absent
from that cut, preserves old token/checkpoint reads, appends one typed restore
outbox notice, and returns a token/manifest whose logical sequence is strictly
newer than current and source.

**Step 2: Capture red**

```bash
cargo test -p arco-catalog --test state_store_control_mvp restore_apply -- --nocapture
```

Expected: failure because apply/inspection are not implemented.

**Step 3: Add crash/CAS tests before implementation**

Use storage wrappers to stop after transaction write, after manifest write, and
after pointer CAS. Prove identical retry resumes each point. Advance the pointer
with a foreign transaction before restore CAS and prove restore returns
`Superseded` without overwriting that winner. Advance once more after a
successful restore and prove lineage inspection still reports the restore
`Visible`.

**Step 4: Capture red**

```bash
cargo test -p arco-catalog --test state_store_control_mvp restore_recovery -- --nocapture
```

**Step 5: Implement immutable writes, existing pointer CAS, and lineage inspection**

Never infer visibility from the existence of the planned manifest alone.

**Step 6: Capture green and run the complete state-store regression**

```bash
cargo test -p arco-catalog --test state_store_control_mvp restore -- --nocapture
cargo test -p arco-catalog --test state_store_control_mvp -- --nocapture
```

Expected: every filtered test executes; the full existing suite stays green.

## Task 4: Prove All-Participant Preflight And Explicit Omissions

**Files:**

- Modify: `crates/arco-catalog/src/workspace_snapshot_service.rs`
- Modify: `crates/arco-catalog/src/workspace_restore.rs`
- Modify: `crates/arco-catalog/tests/workspace_snapshot_restore.rs`

**Step 1: Write failing no-mutation preflight tests**

Use two canonical domains and make the later domain fail for each of: missing
restore adapter, unsupported capability, corrupt/missing/expired source,
out-of-scope authority, adapter plan failure, and source pin release. Assert no
write occurs under `transactions/restores/` or either domain state-store prefix.

Use a backend whose list methods fail the test and prove successful preflight
uses exact paths only.

**Step 2: Capture red**

```bash
cargo test -p arco-catalog --test workspace_snapshot_restore preflight_before_mutation -- --nocapture
```

Expected: compile/failing assertions because the restore workflow is absent.

**Step 3: Write omission-policy tests**

Prove `Reject` fails with zero writes when configured domains exceed source
domains. Prove `Omit` records the absent configured domain canonically and never
reads or writes it during participant execution. Prove a source domain missing
from configuration fails under both policies. Prove the domain-only operation
plans exactly its named source domain.

**Step 4: Capture red**

```bash
cargo test -p arco-catalog --test workspace_snapshot_restore omitted_domain_policy -- --nocapture
```

**Step 5: Implement the internal validated cut and full preflight phase**

Return the cut from the same checks that power Phase 7B preflight; do not create
a second weaker source validator.

**Step 6: Capture green**

Run both filters and the existing preflight regression:

```bash
cargo test -p arco-catalog --test workspace_snapshot_services preflight -- --nocapture
```

## Task 5: Persist The Journal Before The First Participant Commit

**Files:**

- Modify: `crates/arco-catalog/src/workspace_restore.rs`
- Modify: `crates/arco-catalog/tests/workspace_snapshot_restore.rs`

**Step 1: Write failing publication-order/idempotency tests**

Record backend operations and require immutable request, immutable attempt plan,
and a durably readable `APPLYING` journal before the first Control MVP
transaction write. Assert same-ID exact retry reuses them and same-ID semantic
conflict writes nothing new.

**Step 2: Capture red**

```bash
cargo test -p arco-catalog --test workspace_snapshot_restore journal_precedes_domain_commit -- --nocapture
```

**Step 3: Add journal CAS tests**

Prove stable HEAD/GET/HEAD binding, revision increments, legal transitions,
winner reconciliation, unknown-write readback, conflicting receipt rejection,
bounded retry on unstable versions, and journal-revision exhaustion before any
participant apply.

**Step 4: Capture red**

```bash
cargo test -p arco-catalog --test workspace_snapshot_restore journal_cas -- --nocapture
```

**Step 5: Implement immutable write helpers and the CAS journal module**

Keep all lifecycle validation behind the journal module interface rather than
spreading raw record writes across the workflow.

**Step 6: Capture green**

Run both filters. Expected: nonzero matching tests pass.

## Task 6: Implement Sequential Workspace Restore And Repair

**Files:**

- Modify: `crates/arco-catalog/src/workspace_restore.rs`
- Modify: `crates/arco-catalog/tests/workspace_snapshot_restore.rs`

**Step 1: Write the canonical-order success test**

Restore two domains supplied in reverse configuration order. Assert preflight
completes for both before the first mutation, commit order is canonical, each
result is strictly newer, historical source reads remain immutable, and no
final read manifest exists until both are visible.

**Step 2: Capture red**

```bash
cargo test -p arco-catalog --test workspace_snapshot_restore workspace_restore_success -- --nocapture
```

**Step 3: Write partial-failure and crash-window tests**

Cover:

- first participant visible, second participant fails;
- participant pointer CAS succeeds, journal receipt CAS fails;
- process stops after journal `APPLYING` but before participant apply;
- attempt is superseded by a foreign pointer winner;
- journal reaches `FINALIZING` before final-manifest write;
- final manifest write succeeds before terminal journal CAS; and
- source retention expires between participants.

Assert partial visibility becomes `REPAIR_REQUIRED`; recovery inspects exact
completed evidence, never undoes or duplicates it, adopts the exact same plan
for Ready/uncertain work, and writes a new immutable attempt only for unfinished
work proven `Superseded`. Add explicit regressions for two helpers racing the
same Ready plan, crash after domain visibility before journal receipt, helper
adoption of that exact plan, partial visibility creating no `n + 1`, an old
attempt being unable to publish after `n + 1`, and no duplicate empty-delta
publication. Also make completed-receipt revalidation and unfinished-participant
planning fail during repair and assert zero new attempt-plan writes and zero
journal revision.

Add a three-domain aggregate regression: A is already visible, B attempt 1 is
proven superseded, and C attempt 1 remains Ready/uncertain. Aggregate attempt 2
must omit completed A, create only B participant plan 2, carry C participant
plan 1 byte-for-byte with its original digest, and publish B then C in canonical
order exactly once. An aggregate-1 helper must fail its journal fence; the
aggregate-2 helper must adopt C if C raced visible before the aggregate change.
Also pause an initially unpublished identical invocation while another helper
publishes the same restore to `VISIBLE`; the loser must re-read and adopt that
terminal winner instead of returning a stale preflight or plan-readiness error.

**Step 4: Capture red**

```bash
cargo test -p arco-catalog --test workspace_snapshot_restore workspace_restore_recovery -- --nocapture
```

**Step 5: Implement the workflow loop and recovery loop**

Return a safe repair-required outcome for expected partial/CAS cases. Preserve
typed infrastructure errors when durable state cannot be read or updated.

**Step 6: Capture green**

Run both filters. Expected: nonzero matching tests pass.

## Task 7: Finalize The Opt-In Read Manifest And Authority Boundaries

**Files:**

- Modify: `crates/arco-catalog/src/workspace_restore.rs`
- Modify: `crates/arco-catalog/tests/workspace_snapshot_restore.rs`

**Step 1: Write failing finalization tests**

Assert final manifest publication occurs only after every required participant
receipt is revalidated, terminal retry is read-only/idempotent, and corrupt or
conflicting manifest bytes fail closed. Assert omitted domains have names only
and no carried authority.

**Step 2: Write forbidden-write/redaction tests**

Capture all puts/deletes and assert restore never writes:

- the source snapshot/export record;
- source pin revisions/selectors;
- source checkpoint or source manifest paths;
- any required-object or legacy-compatibility path;
- public Parquet or catalog system-table paths;
- `transactions/root/*` or `commits/root/*`; or
- any non-participating domain pointer.

Serialize every persisted restore record and assert it contains no opaque token,
provider root/URI, credential-like field, key/value payload, raw backend error,
or false “atomic”/distributed-transaction marker.

**Step 3: Capture red**

```bash
cargo test -p arco-catalog --test workspace_snapshot_restore final_read_manifest -- --nocapture
cargo test -p arco-catalog --test workspace_snapshot_restore restore_authority_boundaries -- --nocapture
```

**Step 4: Implement minimum finalization and safe accessors**

Do not add a route, public selector, or system table.

**Step 5: Capture green and run the whole new suite**

```bash
cargo test -p arco-catalog --test workspace_snapshot_restore -- --nocapture
```

Expected: every restore test passes with no ignored critical crash case.

## Task 8: Focused And Full Verification

Run in this order:

```bash
cargo test -p arco-catalog --test workspace_snapshot_restore -- --nocapture
cargo test -p arco-catalog --test state_store_control_mvp restore -- --nocapture
cargo test -p arco-catalog --test state_store_control_mvp -- --nocapture
cargo test -p arco-catalog --test workspace_snapshot_services preflight -- --nocapture
cargo test -p arco-catalog --test workspace_snapshot_services -- --nocapture
cargo test -p arco-catalog --test workspace_snapshot_contracts -- --nocapture
cargo test -p arco-catalog gc::reachability -- --nocapture
cargo test -p arco-catalog
cargo check -p arco-catalog
cargo clippy -p arco-catalog --all-features -- -D warnings
cargo check -p arco-api
cargo test -p arco-api --test control_plane_transactions_api -- --nocapture
cargo test -p arco-api --test root_transaction_protocol -- --nocapture
cargo test -p arco-core --test control_plane_transaction_contracts -- --nocapture
cargo test -p arco-core --test control_plane_transaction_paths_contracts -- --nocapture
cargo fmt --all --check
git diff --check
git diff --check 1c75799a5450ce2a551af89c099a5f01717b8c18...HEAD
```

Each filtered command must execute at least one relevant test. No required
failure may be classified away as an environment issue without concrete
evidence. Because no manifests or guide content should change, do not run
dependency or mdBook checks for this slice.

Audit request-time no-list and authority scope before staging:

```bash
rg -n '\.(list|list_meta)\(' \
  crates/arco-catalog/src/workspace_restore.rs \
  crates/arco-catalog/src/state_store/control_mvp.rs
rg -n 'RootTxRecord|RootTxManifest|RootTxReceipt|transactions/root|commits/root' \
  crates/arco-catalog/src/workspace_restore.rs \
  crates/arco-catalog/src/state_store/control_mvp.rs
```

Expected: zero matches in the new restore implementation for both audits.
Existing unrelated Control MVP code need not be mechanically rewritten.

## Task 9: Narrow Staging, One Commit, And Ordered Reviews

Confirm only the allowlist changed:

```bash
git status --short
git diff --name-only 1c75799a5450ce2a551af89c099a5f01717b8c18
```

Stage exact paths only and commit:

```bash
git add \
  docs/plans/2026-07-15-phase-7c-roll-forward-restore.md \
  crates/arco-catalog/src/lib.rs \
  crates/arco-catalog/src/retention_coordination.rs \
  crates/arco-catalog/src/state_store.rs \
  crates/arco-catalog/src/state_store/control_mvp.rs \
  crates/arco-catalog/src/workspace_snapshot_service.rs \
  crates/arco-catalog/src/workspace_restore.rs \
  crates/arco-catalog/tests/state_store_control_mvp.rs \
  crates/arco-catalog/tests/workspace_snapshot_restore.rs
git commit -m "feat(catalog): add roll-forward snapshot restore"
```

Record base/head SHAs and dispatch a fresh spec-compliance reviewer against this
plan and exact diff. The reviewer must specifically verify:

- plan file predates implementation changes;
- no participant/journal mutation precedes complete preflight;
- omission policy is explicit and carry-forward absent;
- source and recovery reads are exact-path/no-list;
- Control MVP plans are deterministic and visibility is proven by lineage;
- CAS loss never overwrites the previous winner;
- partial progress is durably repairable and completed participants are not
  replayed;
- old snapshots/exports/checkpoints/compatibility artifacts remain immutable;
- the final read manifest appears only after all required domains succeed;
- `CurrentStateStore` remains unsupported;
- generic state-store manifests are never relabeled as root transactions; and
- no production authority, route, grant, DDL, retry-policy, or Phase 8 change
  entered the slice.

Fix every correctness, safety, recovery, and scope blocker in the same commit,
rerun the complete verification list, amend, and request a fresh spec review of
the new SHA until approved. Then dispatch a fresh code-quality reviewer. Fix
Important/Critical findings, amend, reverify, and re-review.

Require a clean worktree before writing the Phase 7D plan.

## Exit Gate And Mandatory Stop Conditions

Phase 7C is complete only when tests prove all of the following:

- every participant and source artifact preflights before any new journal or
  domain mutation;
- domain and workspace restore publish strictly newer Control MVP authority;
- `Omit` and `Reject` are explicit, and omitted domains contribute no authority;
- immutable attempt evidence survives every tested crash window;
- crash after commit/before journal recovers through exact lineage evidence;
- same-scope adapters backed by a different storage authority are rejected;
- revision exhaustion is rejected before participant authority mutation;
- an unpublished identical retry adopts a concurrently published terminal
  winner;
- CAS loss leaves the other winner visible and requires a new attempt;
- partial visibility is `REPAIR_REQUIRED`, and retry continues without undo or
  duplicate participant publication;
- historical snapshots, exports, manifests, checkpoints, required objects, and
  compatibility paths are unchanged;
- the final opt-in read manifest is immutable and appears only after all
  required participants succeed;
- no request/recovery path lists;
- no opaque token is serialized;
- no generic restore constructs existing root-transaction records or claims a
  distributed transaction; and
- the one commit passes both independent review gates and the complete focused
  verification list.

Stop at Phase 7B instead of forcing a partial 7C implementation if any of these
conditions holds:

1. deterministic Control MVP candidate bytes cannot be reproduced from exact
   persisted source/base evidence;
2. a visible planned transaction cannot be distinguished from an unreachable
   immutable artifact without listing;
3. a completed participant would need to be replayed or undone to recover;
4. source retention/compatibility would require writes to old paths;
5. a generic participant requires fabricated root records or a downcast from
   `ArcoStateStore`;
6. `CurrentStateStore` or any missing capability would need a fallback;
7. journal CAS cannot bind bytes to the exact observed version; or
8. the final read manifest would need to become a production current/root
   selector to be useful.

Do not start Phase 7D while any required command is unrun/failing, either review
is unapproved, the Phase 7C worktree is dirty, or safe recovery is only asserted
rather than proven.
