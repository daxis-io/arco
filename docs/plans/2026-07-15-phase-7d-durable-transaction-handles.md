# Phase 7D Durable Transaction Handles Implementation Plan

> **Execution requirements:** Use `test-driven-development` for every
> behavior, keep code mutations with one worktree owner, and run fresh
> independent read-only spec-compliance and code-quality review gates before
> the final Phase 7 verification matrix.

**Goal:** Add a durable, direct-addressed control-plane handle above the
existing catalog, orchestration, and root transaction executors. A handle may
stage only supported typed mutations, freeze them for review, execute them
with deterministic low-level identities, and recover partial or uncertain
visibility without undoing an already-visible participant.

**Architecture:** Extend the existing shared control-plane transaction module
with a distinct handle record and exact-path namespace, then implement the
high-level state machine as a child module of the existing API transaction
service. Handles reuse the existing typed mutation executors and their
`Prepared`/`Visible`/`Aborted` records as evidence; they do not introduce a
second low-level transaction protocol. The only tenant-visible projection is a
safe Parquet encoder that is registered when, and only when, the current
catalog manifest selects `transactions.parquet`.

**Tech stack:** Rust 2024, serde/JCS JSON, SHA-256, UUID v4 entropy, ULIDs,
chrono, `ScopedStorage`, create-if-absent immutable objects, object-store CAS,
Arrow/Parquet, and Cargo.

**Original slice base:** `b0e8dee0edba2a587b4c4bdd7a23512f4b789747`

**Refreshed slice parent:** `1e295851a7aeff54bfcc2889f57274cbb54f5ce7`

---

## Provenance Gate

This plan must be the first and only Phase 7D change at authoring time. It is
committed in the same amendable Phase 7D commit but predates all production and
test changes. Before Task 1, capture:

```bash
git status --short --branch
git diff --name-only 1e295851a7aeff54bfcc2889f57274cbb54f5ce7
```

Expected before implementation: only this plan path is changed.

All work is one amendable commit:

```text
feat(control-plane): add durable transaction handles
```

Do not make an intermediate commit. Preserve genuine red and green output for
the final report.

## One-Commit Scope

Allowed files:

- `docs/plans/2026-07-15-phase-7d-durable-transaction-handles.md`
- `crates/arco-core/src/control_plane_transactions.rs`
- `crates/arco-core/tests/control_plane_transaction_paths_contracts.rs`
- `crates/arco-api/src/control_plane_transactions.rs`
- `crates/arco-api/tests/control_plane_transactions_api.rs`
- `crates/arco-api/tests/root_transaction_protocol.rs`
- `crates/arco-api/tests/visible_contracts.rs`
- Create `crates/arco-api/src/control_plane_transactions/handles.rs`
- Create `crates/arco-api/src/control_plane_transactions/handles_tests.rs`
- `crates/arco-catalog/src/parquet_util.rs`
- Create `crates/arco-catalog/tests/transaction_handle_contracts.rs`
- `crates/arco-api/src/system_tables.rs`
- `crates/arco-api/tests/system_tables_api.rs`
- `crates/arco-catalog/src/tier1_compactor.rs`
- `crates/arco-catalog/src/tier1_writer.rs`
- `crates/arco-catalog/src/write_options.rs`
- `crates/arco-catalog/src/writer.rs`
- `crates/arco-flow/src/orchestration/compactor/manifest.rs`
- `crates/arco-flow/src/orchestration/compactor/reconciler.rs`
- `crates/arco-flow/src/orchestration/compactor/service.rs`
- `crates/arco-flow/src/orchestration/ledger.rs`
- `crates/arco-flow/src/orchestration/state.rs`
- `crates/arco-flow/tests/orchestration_rebuild_dr.rs`
- `crates/arco-flow/tests/orchestration_parity_gates_m1.rs`
- `crates/arco-flow/tests/orchestration_parity_gates_m2.rs`
- `crates/arco-flow/tests/property_tests.rs`

No Cargo manifest change is expected. Do not change protobuf or generated
code, HTTP/gRPC routes, CLI commands, SQL/DDL surface, grants, credential
vending, public authority rules, catalog/orchestration retry policy, snapshot
or restore contracts, or GC. If another path is required, stop and amend this
plan before changing it.

### Review-hardening scope reconciliation

The reconciled implementation files above are required only for the
frozen-handle recovery rules in this plan:

- `crates/arco-catalog/src/tier1_writer.rs` owns the immutable, exact-addressed
  catalog transaction event intent, collision validation, direct selected
  publication proof, and legacy manifest-chain recovery. Exact recovery cannot
  remain in the API files because only the Tier-1 writer owns the event path,
  catalog watermark, pointer-selected commit snapshot, manifest lineage, and
  pointer-version evidence needed to distinguish an unpublished event from an
  already-visible catalog mutation.
- `crates/arco-catalog/src/tier1_compactor.rs` records the immutable manifest ID
  and a sorted, unique `(event_id, SHA-256(raw event bytes))` witness for every
  event in the compacted batch on each new row in the existing cumulative
  `commits.parquet` snapshot. The row and file checksum are selected by the
  same catalog pointer CAS as the immutable manifest, so this is a direct
  publication witness inside existing catalog authority rather than a new
  authority object. Exact recovery cannot synthesize this witness after the
  fact because only the compactor knows the candidate manifest ID and exact
  event bytes before the pointer CAS. Recording every batch member is required
  because the existing notification consumer may compact a crash-orphaned
  frozen event together with later events.
- `crates/arco-catalog/src/write_options.rs` carries an optional private
  `CatalogTransactionIdentity` into the existing writer. The option is set only
  after a durable handle has frozen and bound the participant; its absent
  default preserves legacy catalog behavior.
- `crates/arco-catalog/src/writer.rs` selects transaction-owned event-intent
  publication and exact recovery only when that private frozen identity is
  present. All non-handle callers continue through the existing event append
  and retry path, so this amendment does not redesign the public catalog retry
  policy.
- `crates/arco-flow/src/orchestration/ledger.rs` owns canonical event
  publication and the duplicate-event rule required by same-ID durable-append
  recovery. New events are written as JCS JSON. An existing same-ID JSON object
  is parsed and canonicalized so a semantically identical legacy encoding is
  idempotent, while malformed JSON or a different canonical value fails closed.
  Treating divergent event authority as a successful duplicate would let an
  unreviewed event satisfy a frozen transaction.
- `crates/arco-flow/src/orchestration/compactor/manifest.rs` adds an optional,
  additive exact-batch witness to the existing immutable orchestration
  manifest authority. Each entry binds a canonical ledger path to the SHA-256
  digest of its canonical event payload. Endpoints and count alone cannot
  distinguish different middle events in a multi-event batch, so exact
  durable-handle verification requires this existing-authority witness.
- `crates/arco-flow/src/orchestration/compactor/service.rs` populates that
  witness only when the existing compactor publishes a changed manifest
  revision. Before folding, it also requires every caller-supplied object path
  to equal the canonical ledger path derived from the parsed event; otherwise
  the manifest could falsely claim that it processed canonical authority while
  consuming identical bytes from another object. Repeated references to that
  same immutable canonical path are deduplicated before reads and folding so
  legacy notification replay remains idempotent; a same-ID object with
  divergent bytes still fails closed at ledger publication. The witness is
  selected by the same manifest-pointer CAS and adds no independent write,
  retry path, or mutation authority.
- `crates/arco-flow/src/orchestration/compactor/reconciler.rs` changes only its
  internal test manifest fixture to initialize the additive field; it adds no
  production reconciliation behavior.
- `crates/arco-flow/src/orchestration/state.rs` owns the narrow validation
  contract that exact-reads the current pointer and its cycle-checked
  predecessor chain, proves the claimed target manifest was selected, and
  finds the exact event witness at or before that target revision. API
  recovery therefore does not depend on compactor record fields directly and
  cannot accept a checksum-valid orphan manifest. The validator uses no
  listing and adds no mutation authority, retry behavior, or public transport
  surface.
- `crates/arco-flow/tests/orchestration_rebuild_dr.rs` keeps the existing
  disaster-recovery fixtures consistent with the production ledger path
  contract by deriving each stored event path from its parsed event. Exact
  compactor path validation cannot be proven across rebuild entry points by
  the service unit test alone, and hard-coded fixture dates would exercise an
  invalid authority shape rather than legacy rebuild compatibility.
- `crates/arco-flow/tests/orchestration_parity_gates_m1.rs` and
  `crates/arco-flow/tests/orchestration_parity_gates_m2.rs` keep existing
  out-of-order dispatch and sensor-reload parity fixtures on the same
  timestamp-derived ledger paths as production append. Those CI-only parity
  entry points are excluded from the aggregate flow command and otherwise
  retain stale hard-coded dates for non-ULID event identifiers, so the
  canonical-path recovery contract cannot be verified across the complete CI
  matrix without updating them. This changes fixture addressing only and does
  not change controller, compactor, or legacy retry behavior.
- `crates/arco-flow/tests/property_tests.rs` makes the existing compaction
  replay properties store their ULID-bearing events at the same canonical
  paths production append derives. The property suite spans duplicate and
  crash replay entry points that the service and rebuild regressions do not.
- `crates/arco-api/tests/visible_contracts.rs` owns endpoint-level regression
  coverage for the existing catalog and orchestration transaction routes. It
  must preserve their existing marker-first cached-visible recovery because the
  private handle unit suite cannot prove that legacy protobuf callers retain
  deterministic same-identity recovery and no-list behavior through the
  router. This regression coverage adds no route, protobuf field, retry
  authority, or public surface.

Catalog event intents and their recovery are therefore constrained to private
frozen durable-handle execution; handle-shaped caller syntax alone never
enables them. The orchestration duplicate check strengthens validation at the
existing append boundary without allocating replacement IDs or changing
legacy retry eligibility. Frozen direct and root handle staging additionally
reject repeated canonical orchestration event paths before persisting a staged
mutation. The compactor intentionally reduces repeated exact notification
paths to one event, while frozen finalization binds the reviewed event count
and unique manifest witness; accepting a duplicate into the immutable handle
would therefore make an otherwise successful low-level publication impossible
to finalize or recover. This pre-write validation applies only to the private
handle seam and leaves legacy notification replay deduplication unchanged.
None of these files may add a new mutation authority, route, protobuf, CLI or
SQL surface, grant, credential-vending capability, public projection writer,
request-time listing, or Phase 8 behavior.

The catalog boundary enforces that constraint with an opaque capability, not a
caller-settable identity tuple. Capability issuance exact-reads the canonical
handle, per-ordinal identity authority, immutable staged bytes and digest,
idempotency claim, and exact low-level transaction record in the writer's
tenant/workspace scope. It accepts the direct catalog participant identity or
the catalog child of a frozen root participant only when the staged catalog
operation hashes to the claimed reviewed request. Each catalog transaction
method independently recomputes that same canonical request hash before it can
publish a transaction-owned event intent; a capability for operation A cannot
publish or recover operation B under A's transaction ID. The opaque capability
retains the typed request reconstructed from the exact staged bytes instead of
discarding it after hash validation. Each private event intent binds the exact
canonical base-manifest ID and path, the SHA-256 digest of those immutable
manifest bytes, and a digest of typed event semantics derived independently
from the retained request and that base state. Collision and recovery validate
all six supported catalog variants before event creation, intent revision, or
compaction: request-controlled fields and base-inherited row fields must match
exactly, resolved catalog/schema IDs and normalized table fields must match the
bound base, and runtime UUIDs and timestamps must be structurally valid. A
self-consistent operation-B envelope carrying operation A's request hash or a
self-declared digest therefore fails before event or manifest publication.
Collision adoption additionally requires the stored winner's exact base
witness and independently derived semantic binding to equal the candidate
built from the stable current catalog head. Recovery first searches
pointer-selected catalog authority for an already-published intent. New
`commits.parquet` rows carry an additive nullable immutable manifest ID. The
same row carries an additive nullable canonical per-event checksum witness.
The Tier-1 writer verifies the current manifest's exact snapshot path, selected
`commits.parquet` size, checksum, row count, unique matching event witness,
exact manifest ID and bytes, exact intent-event digest, commit ID, snapshot
version, publication time, and fencing token. New witnessed rows therefore do
not depend on the compacted ledger event remaining available after publication.
This makes new-handle publication verification constant-object-I/O even after
arbitrarily many later manifests and avoids holding the catalog lock across
history traversal. A legacy matching commit row without the additive witness
falls back to an uncapped, cycle-checked manifest walk bounded by the intent's
exact base; that read-only walk completes before lock acquisition, and the
lock-held phase rechecks the direct selected witness before mutation. When the
head has advanced past an unpublished intent, recovery applies the retained
typed request to the stable current state before any intent CAS or event
publication. A compatible unrelated advance CAS-rebinds the private intent to
the current manifest bytes and semantic digest, allocating a successor event
ID only when the watermark has overtaken the prior ID. A changed resolved ID,
target precondition, or base-inherited field fails closed before intent
revision, event publication, or compaction, so an orphaned full-row update
cannot roll back a later writer.
This base witness and semantic digest strengthen the existing private intent;
they are not a new mutation authority or public surface. Cached-only visible
evidence may issue a read-only validation capability so the existing exact
record materialization join remains valid, but mutation and recovery require
an exact clean `Prepared` record whose marker and complete, sorted, unique
identity-claim set match the staged mutation. An exact or cached `Visible`
record may issue only a read-only capability, and malformed, aborted,
repair-pending, or marker-divergent authority cannot authorize a catalog
mutation. The writer's private version-1 staged-operation mirror accepts
additive fields at every nested supported variant while still rejecting
unknown tags and unsupported versions, including during commit and recovery.
The low-level transaction append helper is crate-private, and legacy callers
with no capability retain the existing event append and retry path.

## Existing Primitives To Reuse

- `ControlPlaneTxStatus` remains exactly `Prepared`, `Visible`, and `Aborted`.
  A handle lifecycle is a separate high-level status and never adds a proto or
  low-level status.
- `ControlPlaneTransactionService` already validates typed catalog DDL and
  orchestration events, claims deterministic idempotency records, publishes
  immutable transaction artifacts, records durable repair evidence, and
  adopts visible retries.
- Root transactions already derive deterministic participant idempotency keys
  and reconcile catalog/orchestration receipts. Refactor their inline typed
  executor only enough for handles to call the same implementation.
- `ControlPlaneTxPaths::{idempotency,record,root_super_manifest,
  root_commit_receipt,orchestration_commit_receipt}` are the only low-level
  paths used for recovery. Request-time handle operations never call `list` or
  `list_meta`.
- `CatalogReader::get_mintable_paths` returns only paths selected by the
  current catalog manifest. System-table registration must keep using this
  selection and must not discover handle rows by listing `transactions/`.

## Durable Contract

### IDs and paths

Handle IDs are canonical path-safe `hdl_`-prefixed ULIDs. The handle namespace
is separate from low-level records:

```text
transactions/handles/{handle_id}/handle.json
transactions/handles/{handle_id}/mutations/{ordinal:020}.json
transactions/handles/{handle_id}/identities/{ordinal:020}.json
```

Path builders validate the handle ID and positive ordinal before returning a
path. Persisted paths must equal the canonical builder output. Digests use
`sha256:<64 lowercase hex>`.

### Versioned records

Every handle, staged-mutation, and per-ordinal identity-authority record has a
stable `record_type` and `version: 1`. Version 1 accepts additive unknown fields
and rejects unknown record types, unsupported versions, unknown enum variants,
invalid timestamps, malformed IDs/paths/digests, duplicate or noncanonical
mutation references or identity reservations, and inconsistent lifecycle
evidence.

`ControlPlaneHandleRecord` contains only durable coordination metadata:

- handle ID, typed tenant/workspace scope, positive CAS revision, lifecycle;
- created, updated, expiry, and optional prepared/committing/visible/terminal
  timestamps;
- canonical immutable mutation references and digests;
- the SHA-256 verifier for the review token;
- deterministic low-level participant identity and safe status/receipt
  evidence sufficient for exact recovery; and
- a bounded typed failure category, never a raw error string.

It never contains a plaintext review token, caller identity, opaque payload,
credential, secret, storage-provider URI, arbitrary metadata, low-level read
token, or mutation body.

Each staged mutation is an immutable versioned typed enum. Supported variants
are exactly:

- an existing catalog DDL mutation represented by explicit fields;
- a validated runtime orchestration batch represented by typed
  `OrchestrationEvent` values; and
- a root commit containing only those two executable variants.

Metastore and scoped-metastore root mutations, including storage credentials
and grants, are unsupported. Raw JSON, protobuf bytes/hex, unknown tags,
arbitrary key/value payloads, and credential-bearing URI/userinfo/query data
are rejected before immutable publication. Byte-identical create-if-absent
retries succeed; a different object at the same staged path is a conflict.

### Review token

Creation generates a token from two independent UUID v4 values and returns the
plaintext exactly once. Only `sha256:<64 lowercase hex>` is persisted. Get and
all retries return no plaintext token. Mutating operations verify the supplied
token in constant-time style over fixed-length digest bytes. Debug output,
errors, records, staged bytes, low-level records, and system-table rows must
not contain the token. If create receives an ambiguous write response, it
exact-reads the generated handle path without listing. A byte-identical stored
record is adopted with the same still-in-memory one-time token, absence permits
a fresh create-if-absent attempt, and divergent stored state fails closed.

### Lifecycle and TTL

The legal forward path is:

```text
OPEN -> PREPARING -> PREPARED -> COMMITTING -> VISIBLE
```

Legal exits are `ABORTED`, `EXPIRED`, or `REPAIR_REQUIRED`:

- only `OPEN` may add staged mutation references;
- prepare CASes `OPEN -> PREPARING`, exact-reads and validates every immutable
  staged object in ordinal order, freezes deterministic low-level identities,
  then CASes `PREPARING -> PREPARED`;
- commit verifies the review token and unexpired TTL, CASes
  `PREPARED -> COMMITTING` before invoking any low-level executor, and records
  each visible participant through another handle CAS;
- all participants visible yields `VISIBLE`;
- a proven partial or uncertain visibility state yields `REPAIR_REQUIRED`;
- recovery exact-reads durable low-level idempotency, transaction, and receipt
  evidence and either adopts it or re-enters the same deterministic executor;
  it never allocates a replacement low-level identity;
- abort and expiry are allowed only before any participant can be visible or
  visibility is uncertain. They are forbidden from `COMMITTING`,
  `REPAIR_REQUIRED`, or `VISIBLE`;
- `VISIBLE`, `ABORTED`, and `EXPIRED` are terminal. Recovery is the only exit
  from `REPAIR_REQUIRED` and never performs undo.

An expired pre-visibility handle transitions to `EXPIRED`; it cannot be
revived. TTL checks use an explicit caller-supplied clock in tests. CAS loss
reloads and validates the winner; a byte-equivalent replay adopts the winner,
and conflicting state fails closed.

## Failure And Recovery Rules

Before the first low-level write, the handle durably contains the complete
canonical mutation set and deterministic idempotency identities needed for
recovery. Crash tests cover:

1. immutable mutation written before its handle-reference CAS;
2. handle in `PREPARING` before and after staged-object validation;
3. handle in `PREPARED` before commit;
4. handle in `COMMITTING` before a low-level claim;
5. a low-level `Prepared` record or durable append without visibility;
6. low-level visibility before the receipt is copied into the handle;
7. one participant visible before a later participant fails; and
8. final handle CAS loss after all low-level participants are visible.

Low-level `Visible` is adopted as visible evidence. Low-level `Prepared` with
repair evidence or any partial/uncertain participant set yields
`REPAIR_REQUIRED` until recovery reconciles it. Low-level `Aborted` may permit
a pre-visibility terminal abort only when exact inspection proves that no
participant is visible or uncertain. A root record alone never proves its
catalog/orchestration participants are invisible.

Stop rather than commit if any crash can make a participant visible before its
deterministic recovery identity is durable, recovery requires listing, a
partially visible handle can abort/expire, or a retry can duplicate a visible
mutation.

## Review-Hardening Clarifications

The following requirements make the existing durability, secrecy, and
recovery contracts explicit. They narrow ambiguous or permissive wording
above; they do not expand Phase 7D's surface or change its non-goals.

- Handle records, staged mutations, and identity authorities use canonical JCS
  JSON bytes. Exact reads compare the original bytes with canonicalized parsed
  v1 JSON before typed decoding, so pretty-printed or key-reordered authority
  fails closed while canonical additive v1 fields remain accepted. Immutable
  retry identity is independent of map insertion order or serializer process.
- Orchestration event publication and every reviewed-event byte comparison use
  canonical JCS JSON. Duplicate same-ID events accept a semantically equal
  legacy JSON encoding only after canonical comparison; malformed or
  canonically divergent values fail closed. Durable-append recovery therefore
  remains stable across map insertion order and service reconstruction without
  weakening event identity.
- Frozen catalog authorization recomputes the complete sorted, unique direct
  or root-child claim set from the exact staged mutation and requires it to
  equal the path- and scope-bound identity authority. It rejects missing,
  extra, duplicate, legacy-overlapping, domain-kind-divergent, or
  mutation-reference-divergent claims.
- Frozen catalog intent creation, collision adoption, and recovery retain the
  typed staged request and validate the event realization against the exact
  immutable base-manifest bytes and loaded base state. The intent's semantic
  digest is independently recomputed only after the typed variant, target,
  patch, resolved IDs, normalized fields, full inherited row, column set, and
  runtime ID/time structure match. Copying a reviewed request hash onto a
  different but internally valid event is corrupt and cannot publish an event
  or manifest.
- A collision winner must match the freshly derived current-base witness and
  semantic binding. Recovery may roll an unpublished intent forward across an
  unrelated catalog advance only after all six typed realizations also validate
  against the stable current state and a CAS rebinds the intent's base witness
  and digest. Stale resolved IDs, target state, or inherited full-row fields
  fail before the CAS and before event or manifest publication. Legacy writers
  without a frozen capability retain their existing retry behavior.
- Catalog mutation or recovery authority requires a consistent idempotency
  marker and exact clean `Prepared` transaction record. Exact or cached
  `Visible` authority is validation-only; `Aborted`, repair-pending, malformed,
  cached/exact-divergent, or concurrently changed authority cannot mutate.
  Race tests gate the writer's exact transaction-record read and finalization
  write so an invalid winner is retained as evidence without repairing the
  marker. The writer's mirrored v1 decoder accepts additive fields in nested
  catalog variants through prepare, commit, and recovery.
- Each participant is bound to its handle ID, ordinal, domain, mutation kind,
  deterministic request ID, idempotency key, and exact `request_hash`. A
  marker, transaction record, or receipt is adoptable only when every bound
  identity field matches.
- An exact-addressed handle load binds the decoded `handle_id` to the requested
  canonical handle path before any record is returned or CAS target is
  derived. A valid record copied beneath another handle path is corrupt
  authority and fails closed without a write.
- The legacy ownership fence parses the canonical handle ID, positive ordinal,
  and optional root-child domain, then exact-reads the scoped per-ordinal
  authority. When a handle intent exists, it also exact-reads the matching
  handle record, canonical mutation reference, checksum-bearing staged bytes,
  and typed staged record. A direct claim conflicts only when its domain and
  kind match that staged mutation; a root-child claim conflicts only when that
  staged root contains the requested child domain. An absent handle, unused
  ordinal, or absent root-child domain retains legacy retry behavior after its
  eligible exact identity is durably reserved. A referenced but missing or
  corrupt authority or staged object fails closed.
- Staging and a legacy claim for an as-yet-unused handle ordinal arbitrate
  through one versioned, path-bound CAS authority record under the handle's
  private `identities/` namespace. The record carries an optional immutable
  handle intent containing the exact staged reference, digest, and sorted
  direct/root-child claim set, plus sorted unique legacy identity
  reservations. Staging may install or identically retry its intent only when
  no legacy reservation overlaps its exact claim set; a legacy caller must
  durably reserve its exact domain, kind, and key before publishing a
  low-level marker, and conflicts when an existing handle intent owns that
  identity. Reservations for an absent root child or another unclaimed
  domain may coexist with a staged intent. A fully canonical legacy
  domain/kind/key tuple may install its scoped reservation before the handle
  record exists, closing the create-and-stage race; a malformed lookalike or a
  root-child/domain-kind mismatch does not enter this private namespace and
  retains legacy behavior. Before first intent publication, staging
  exact-checks every generated pre-feature marker without listing. Missing,
  corrupt, noncanonical, or path-divergent authority or staged bytes fail
  closed.
- Immutable staged bytes are published before their handle intent, and the
  intent is published before the handle mutation reference. Therefore a pause,
  ambiguous response, or service reconstruction at either boundary retains a
  single durable winner: an intent without its handle reference still blocks
  overlapping legacy claims and can complete on an identical stage retry,
  while a legacy reservation without its low-level marker still blocks an
  overlapping stage. Lease expiry, lock fencing, and process lifetime are not
  part of this ownership decision. Handle-shaped syntax alone remains
  non-authoritative: only a successfully persisted, path- and scope-bound CAS
  reservation participates in arbitration, including when it predates the
  exact handle record.
- Recovery and finalization exact-read and cross-check the marker, transaction
  record, required result receipt, and any present audit receipt for every
  participant, including one cached as `Visible` in the handle. Cached handle
  evidence alone never proves visibility. This exact reinspection also runs
  before an already-terminal `VISIBLE` handle is returned by commit or
  recovery, and immediately after an executor response before that response is
  journaled as durable evidence.
- A cached low-level `VISIBLE` marker is never adopted directly. Recovery may
  materialize a missing exact transaction record, or advance an exact
  non-visible `PREPARED` or finalize-fallback `ABORTED` predecessor, only
  through the existing typed replay primitive; the predecessor must carry no
  visibility result, while divergent visible results and unverifiable catalog
  audit artifacts fail closed.
- Before the first low-level write, reserve enough checked CAS-revision
  capacity to journal every remaining participant result and the final
  `VISIBLE` or `REPAIR_REQUIRED` transition. Potential revision overflow fails
  before mutation.
- TTL is evaluated before idempotent prepare or abort adoption. Once any
  low-level claim exists, the handle cannot become `ABORTED` or `EXPIRED`;
  exact inspection must retain it as `REPAIR_REQUIRED`, even when the claim
  currently says `Aborted`.
- The persisted per-ordinal identity authority is the non-expiring ownership
  fence for canonical handle participant identities. Its handle intent is
  accepted only when an exact handle and exact staged mutation prove the same
  reference, digest, and claim set. Its legacy reservation is accepted only
  for an eligible canonical domain/kind/key tuple and current workspace scope,
  but may predate the handle itself; malformed and mismatched tuples retain
  existing public retry behavior. Frozen execution begins only after the
  handle CAS makes `COMMITTING` or `REPAIR_REQUIRED` visible, so abort/expiry
  and first low-level claim publication cannot both win their race.
- Durable-handle root recovery reconciles the frozen root participant and its
  recorded root transaction ID in place; it must not replace that transaction
  ID. Catalog and orchestration claims derived beneath a handle-owned root use
  the same private frozen policy; an aborted or ambiguous child is never
  replaced. Existing legacy root retry behavior remains unchanged for callers
  outside durable handles.
- Handle-owned low-level keys are not eligible for the legacy fresh-ID retry
  path. Handle ownership is conveyed by a private frozen-claim execution mode
  only after staged-participant binding succeeds; the syntax of a caller-owned
  request or idempotency key never changes legacy retry behavior. A frozen
  participant with a journaled transaction ID and missing marker fails closed
  without reconstructing or replacing that marker. A repair-pending
  orchestration durable append resumes through the existing same-ID executor,
  while an aborted non-root claim remains `REPAIR_REQUIRED` instead of being
  silently replaced.
- A discovered low-level claim prevents abort or expiry and is durably retained
  as `REPAIR_REQUIRED`, including when discovery races the pre-commit handle
  states. An exact aborted root participant may be CAS-rearmed and recovered in
  place under its frozen transaction ID when no child claim exists; non-root
  aborted participants are never retried automatically.
- If the visible marker and exact visible transaction record differ only in
  `repair_pending`, reconciliation preserves `true` and rewrites both copies.
  The joined `repair_pending=true` authority is typed-validated before either
  copy changes, so a missing orchestration or root audit is accepted only from
  that explicit repair state in either write order. Any other visible-record
  divergence fails closed.
- Frozen-handle low-level visibility finalization and cached-visible
  reconciliation publish the exact transaction record first. CAS loss or an
  ambiguous write response exact-reads and validates the immutable record
  winner; only that winner may then repair the idempotency marker. A losing
  handle candidate never becomes marker authority and never overwrites a
  different exact visible winner.
- Legacy endpoint finalization remains outside that private frozen-handle
  policy. Its immediate marker publication preserves the existing two
  cross-request recovery directions: a failed exact-record write leaves a
  cached visible marker that can materialize the same transaction record, while
  a failed marker write leaves the exact visible record that can repair the
  marker. Both catalog and orchestration replays retain their deterministic
  transaction identity and use no listing. This compatibility path cannot
  authorize frozen-handle execution or bypass the handle's typed winner
  validation.
- Handle reconciliation treats exact-record publication/adoption and marker
  repair as separate steps. After any exact-record CAS result, including a
  concurrent same-owner winner, it applies the complete domain-specific typed
  authority proof to the actual exact winner (recursively for root children)
  before the idempotency marker may change. A typed-invalid race winner is
  retained at its exact path as external evidence but is never cached into the
  marker; recovery fails closed and leaves the marker byte-for-byte unchanged.
- Cached or exact visible evidence must prove its domain-specific typed
  authority before generic reconciliation mutates an exact transaction record
  or idempotency marker. A typed-invalid cached candidate therefore cannot
  replace a same-owner `PREPARED` or `ABORTED` exact record, cannot create a
  missing exact record, and cannot repair a marker from an exact visible
  record. This applies recursively to every root child before root authority is
  adopted; rejection leaves the exact path and marker byte-for-byte unchanged.
- Concurrent root recovery rechecks exact visibility after acquiring the root
  lock. Once one recovery finalizes a root receipt, another recovery adopts
  that immutable result and cannot generate a second receipt for the same root
  transaction ID.
- Root recovery adopts an existing tx-scoped super-manifest only after proving
  its frozen transaction and domain set, a nonzero fencing token, and a
  publication time compatible with the prepared root claim. Invalid adopted
  manifest authority fails before the exact root record or marker is made
  visible.
- Exact visible-root proof reads the canonical tx-scoped super-manifest and
  every explicitly staged child marker, transaction record, typed result, and
  applicable audit receipt. The super-manifest path, root transaction and
  fencing identity, publication time, domain set, and per-domain
  commit/manifest references must match the root record and receipt. Missing,
  corrupt, or divergent authority evidence fails closed without listing.
- Exact visibility proof for direct catalog and orchestration participants
  applies the same typed authority validation as root children: canonical lock
  and fencing identity, ULID commit/event identity where applicable, a
  parseable fixed-width `u64` manifest ID, and the exact domain read token.
  Current Tier-1 catalog writes do not persist `CommitRecord` objects, so a
  catalog audit is optional; if present, its exact canonical path and
  `commit_id` must match. Orchestration and root audit receipts are required
  unless the immutable record explicitly retains a repair-pending
  missing-audit state. Receipt transaction and visibility timestamps alone are
  never sufficient.
- A discovered root-child claim with a missing root marker is retained as
  durable `REPAIR_REQUIRED` evidence without fabricating a root transaction
  ID. The handle journal records the child uncertainty in a core-valid shape,
  so abort, expiry, commit, and recovery cannot leave the handle in a
  preterminal state merely because the parent claim is absent.
- `commit_handle` never enters or exits `REPAIR_REQUIRED`; only
  `recover_handle` may drive repair. The drive mode is explicit, so a normal
  commit that loses a handle CAS to newly discovered `REPAIR_REQUIRED`
  evidence stops instead of continuing participant execution. A concurrent
  `VISIBLE` handle winner is exact-verified and returned without another CAS,
  revision increment, or timestamp rewrite.
- A repair-pending orchestration child beneath a handle-owned root remains a
  recoverable frozen claim when it has the exact durable-append identity.
  Recovery re-enters the existing root executor with the same root and child
  transaction IDs; it never collapses that child to irrecoverable uncertainty
  or allocates replacement IDs.
- A non-visible transaction carrying `visible_at` or a result is malformed and
  cannot enter repair or any mutation path. Concurrent repair of one
  orchestration durable append serializes finalization, exact-adopts an already
  visible winner, and never rewrites that winner with a newly generated
  receipt.
- Every location-like staged value rejects URI userinfo and any query or
  fragment. Opaque absolute URIs are rejected because their credential
  authority cannot be inspected; provider locations must use a hierarchical
  URI or a nonempty credential-free relative path. Blank values and absolute
  filesystem forms such as `/` and `/path` are rejected. This applies to
  catalog locations, orchestration event sources, and task-completion callback
  output paths.
  Staging also rejects a mutation that reflects the plaintext review token,
  and no validation error may echo that token.
- Each public staging wrapper exact-loads the path-bound handle and verifies
  the review token before parsing or validating any caller payload, then
  revalidates the token inside the common stage CAS path. Therefore malformed
  catalog, orchestration, or root payloads paired with a wrong well-formed
  token always return `FORBIDDEN`, perform no write or listing, and cannot echo
  payload contents.
- Review-token candidates must match `review_` followed by exactly 64
  lowercase hexadecimal characters before hashing or scanning staged bytes.
  A malformed candidate is always `FORBIDDEN`, even if a corrupt verifier
  would match its digest, and cannot cause a write or a zero-window scan.
  Catalog, orchestration, and root handle conversion/validation failures use
  bounded handle-specific messages; incoming and persisted orchestration
  partition keys are validated without reflecting their raw value or a review
  token embedded in it.

## Safe System Projection

Add `TransactionHandleCatalogRecord`, `transaction_handle_schema()`, and
`write_transaction_handles()` in `parquet_util.rs`. Canonical deterministic
rows expose only:

- `handle_id`, record version, lifecycle;
- creation, update, expiry, and nullable prepared/committing/visible/terminal
  timestamps; and
- mutation count and visible mutation count.

Rows omit the review-token verifier, actor/request/idempotency data, mutation
kinds and payloads, staged paths and digests, low-level transaction/receipt/
manifest/read-token data, failure detail, and provider/storage information.

Allowlist `system.catalog.transactions` only as `transactions.parquet`. Apply
an exact safe-schema validator before registration. A physically present but
manifest-unselected file is invisible; an allowlisted selected file with an
extra or incompatible column fails closed. The handle service never writes
this Parquet file or any catalog manifest/current pointer.

The additive catalog publication witnesses in physical `commits.parquet` are
internal correctness metadata, not tenant-visible system-table columns.
Registration of `system.catalog.commits` must project the exact pre-Phase-7D
nine-column schema and reject direct queries for `manifest_id` or event
witnesses. This preserves the existing SQL surface while allowing the
pointer-selected snapshot to carry private recovery proof.

## TDD Tasks

### Task 1: Shared paths and record state machine

Write failing core contract tests for canonical IDs/paths, v1 additive
round-trip, corrupt-wire rejection, canonical references, all legal/illegal
transitions, and proof that low-level statuses remain unchanged. Capture red:

```bash
cargo test -p arco-core --test control_plane_transaction_paths_contracts handle -- --nocapture
```

Implement the minimum core contract and rerun green.

### Task 2: Typed staging, review secrecy, CAS, and TTL

Write failing internal API tests for typed-only staging, credential and opaque
payload denial, immutable retry/conflict, ordinal canonicalization, concurrent
CAS, token returned once/verifier-only/redacted debug, lifecycle transitions,
TTL, abort/expire boundaries, exact get, and zero request-time listing.
Capture red:

```bash
cargo test -p arco-api control_plane_transactions::handles_tests -- --nocapture
```

Implement immutable staged records and the handle state machine only through
`PREPARED`, then rerun green.

### Task 3: Commit and exact-path recovery

Add failing tests for every listed crash point, deterministic low-level
identity reuse, existing `Prepared`/`Visible`/`Aborted` mapping, partial
visibility to `REPAIR_REQUIRED`, recovery without undo or duplication, final
CAS-winner adoption, terminal immutability, all six typed catalog event
realizations, and operation-B intent collisions/recovery under operation A's
request hash before event or manifest publication. Add lower-level regressions
rejecting an unselected immutable orchestration manifest, rejecting a
three-event batch whose endpoints and count match but whose middle event
differs, and accepting an exact historical witness in the current selected
manifest lineage. Add a compactor regression rejecting an in-scope event
loaded from a noncanonical caller-supplied path before any manifest is
published, plus an orphaned description-only update followed by a visible
location-only update, the same stale-base collision, and current-base changes
across all six typed variants. Recovery must preserve compatible unrelated-watermark
roll-forward while stale inherited state fails before intent revision, event
publication, or compaction. Add a selected-publication regression with more
than 10,000 later immutable manifests, a backend observation proving no deep
manifest read occurs while the catalog lock is held, and bounded-I/O
regressions proving frozen intent construction does not read unrelated domain
manifests and collision adoption does not reload the same catalog state after
validation. Add a regression proving terminal verification still succeeds
after the exact ledger event is absent, another proving a crash-orphaned
frozen event is recovered when an existing background-style multi-event
compaction publishes it below the batch maximum, and an API regression proving
the internal witness columns remain absent from `system.catalog.commits`.
Intent construction derives its event ID from the already stable catalog base
watermark. Refactor the existing root typed executor only as required; preserve
transport request validation and low-level retry behavior. Capture red with
the focused API test plus:

```bash
cargo test -p arco-catalog catalog_transaction_ -- --nocapture
cargo test -p arco-catalog reviewed_catalog_request_validates_all_six_event_realizations -- --nocapture
cargo test -p arco-flow selected_orchestration_publication -- --nocapture
```

Then implement and rerun green.

### Task 4: Safe Parquet contract

Write failing catalog tests for exact schema, deterministic bytes, lifecycle
and count validation, malformed IDs/timestamps, and forbidden-column absence:

```bash
cargo test -p arco-catalog --test transaction_handle_contracts -- --nocapture
```

Implement the safe row encoder and rerun green.

### Task 5: Manifest-selected system table

Write failing API tests proving explicit allowlisting, selected registration,
unselected invisibility, exact-schema failure on an extra column, safe query
columns, deny-list queries, and no listing. Capture red:

```bash
cargo test -p arco-api --test system_tables_api transaction -- --nocapture
```

Implement the allowlist/schema gate and rerun green.

### Task 6: Slice regression, scope audit, and commit

Run:

```bash
cargo fmt --all --check
cargo check -p arco-core
cargo check -p arco-catalog
cargo check -p arco-api
cargo test -p arco-core --test control_plane_transaction_contracts -- --nocapture
cargo test -p arco-core --test control_plane_transaction_paths_contracts -- --nocapture
cargo test -p arco-catalog catalog_transaction_ -- --nocapture
cargo test -p arco-catalog reviewed_catalog_request_validates_all_six_event_realizations -- --nocapture
cargo test -p arco-catalog --test transaction_handle_contracts -- --nocapture
cargo test -p arco-api control_plane_transactions::handles_tests -- --nocapture
cargo test -p arco-api --test control_plane_transactions_api -- --nocapture
cargo test -p arco-api --test root_transaction_protocol -- --nocapture
cargo test -p arco-api --test system_tables_api -- --nocapture
cargo test -p arco-proto --test control_plane_transactions -- --nocapture
git diff --check
```

Audit the diff for `.list(`, `list_meta`, plaintext tokens, credentials,
protobuf/generated files, routes, retry-policy changes, public Parquet writes,
and unexpected paths. Stage only the allowed files and commit with the required
message.

## Review Gate

After the commit, dispatch fresh spec-compliance and code-quality reviewers
against exact base `1e295851a7aeff54bfcc2889f57274cbb54f5ce7` and the Phase 7D
head. Correct every correctness, safety, or scope blocker; amend the single
commit; rerun focused and regression verification; then dispatch fresh reviews
against the amended exact SHA. Advance to the final matrix only after both
reviewers approve and the worktree is clean.

## Exit Gate

Phase 7D is complete only when tests prove legal transitions, TTL, all crash
points, retry idempotency, high-entropy review-token secrecy, partial visibility
and recovery, faithful low-level status mapping, request-time no-list behavior,
and safe manifest-selected `system.catalog.transactions`. Existing transaction
and root-protocol tests must remain green. No transport, protobuf tag, CLI, SQL,
grant, DDL, authority, vending, or retry-policy expansion may exist.
