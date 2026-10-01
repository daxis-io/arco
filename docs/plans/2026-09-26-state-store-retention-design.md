# State-store retention design — authority format 9

Status: design accepted by the owner on 2026-09-26 (four sections reviewed in
turn). Implementation is sequenced below and is not started by this document.
Depends on the remediation branch that adds the control-store worker and the
metric emitters (PR #436).

## Decisions

| Question | Decision |
|---|---|
| Where do catalog audit records live? | Projection only. No audit row is written to the authoritative KV; the audit record rides the projection intent payload (as today) and is materialized into an append-only `system.catalog.audit` Parquet projection with its own retention. |
| How long is a keyed request replayable with its original response? | 24 hours from `occurredAtMs` (when the adapter accepted the request). Restore never restores receipts. |
| How is the physical purge versioned? | New on-disk authority format 9. Old binaries fail closed on format-9 roots. No conversion from format 7 (no production root exists). Format 8 remains the test-only bounded-directory format. |

## What retention fixes and what it does not

The design bounds the authority KV to current logical state plus a short tail:
receipts for 24 hours, outbox rows until acknowledged, and tombstones until no
retained reader can observe the key live (the 30-day token and checkpoint
floor, plus any snapshot or export pin). Audit rows leave the KV.

At the pilot rate (1,209,600 mutations per week, 80/10/5/5 update, rename,
create, drop) this leaves roughly 200k receipts, up to ~260k tombstones over 30
days, and a few thousand catalog rows: under half a million rows, inside the
1M-row restore scanner, and restore skips receipt rows outright. Growth becomes
bounded on every format, including authority 8, whose restore and GC also carry
the whole log today.

It does not fix per-commit cost. Format 7 replays and hashes the entire retained
state on every commit; half a million rows is on the order of 250 MB decoded per
commit, which caps a single root near one commit per second. Retention is
therefore necessary for any pilot and sufficient for low-rate roots; the 25
mutation/s burst hour still needs bounded commits (authority 8's purpose) or a
smaller target. The throughput question is handed to the real-provider
measurement with a state of about 200k rows; this design makes no throughput
claim.

## Kernel mechanism (authority format 9)

Format 9 is format 7 plus the following. Layout, exact-version head CAS, writer
epoch fencing, reclamation generation, history root, and the hash domains are
unchanged.

### Wall-clock stamps

Manifests and transaction objects gain `committed_at_ms` (i64, milliseconds
since the Unix epoch, taken at rendering time). Retention today is judged by
backend object age; the horizon needs a sequence-to-time mapping that readers
can verify from the artifacts themselves. The stamp is informational for
ordering: logical order remains the sequence.

Stamps are monotone along ancestry on every render path (commit, restore,
maintenance): a child's stamp is `max(render clock, parent stamp)`, and the
ancestry walker rejects a child stamped before its parent.

*Amendment (2026-09-26, after Package C review):* one manifest per commit means
a plain ancestry walk cannot span 30 days at the pilot rate (millions of
manifests against a 4,096-hop budget). Every format-9 manifest therefore also
carries `age_anchor: Option<{ manifest_id, manifest_sha256, sequence,
committed_at_ms }>`, computed at render from the parent: if the child's stamp
falls in a later hour bucket than the parent's, the anchor is the parent (the
last manifest of the previous bucket); otherwise the child inherits the
parent's anchor. Following anchors steps back at least one hour per hop, so the
30-day floor is reached in at most about 721 authenticated reads. The walker
checks the anchor rule as a transition invariant and authenticates each anchor
by its recorded digest, sequence and stamp.

### Per-row expiry

KV segment rows gain a nullable `expires_at_ms` column (segment format 2). A
writer sets it through a new transaction method `put_with_expiry(key, value,
expires_at_ms)`; plain `put` leaves it null. Expiry is a purge-eligibility
hint, not a read filter: point reads, scans, witnesses, and checksums are
unchanged, so an expired row stays visible until a horizon rewrite drops it.
This keeps commit validation deterministic (no wall clock in witness
evaluation) and gives the guarantee "replayable for at least 24 hours".

### `RetentionHorizon` maintenance transition

The durable maintenance worker gains a second job kind beside consolidation.
A horizon job renders new L1 shards from the parent manifest's state omitting:

- rows whose `expires_at_ms` plus a one-hour safety margin is before the
  worker's clock at preparation; and
- tombstones whose deleting sequence is at or below `horizon_sequence`.

Live rows are never purged by the horizon. Outbox rows are never purged by the
horizon (see below). The worker computes `horizon_sequence` as the minimum of:

1. the sequence of the last manifest of the newest hour bucket whose last
   manifest is stamped at or below the token retention floor plus a one-hour
   clock-skew margin (30 days + 1 h; token validity and GC judge age by
   backend object time, the walk by writer stamps). This is conservative by
   up to one bucket, since anchors are the last manifest of each bucket. It
   is found by following `age_anchor` links from the current manifest (no
   listing). If an anchor has been collected by GC, its recorded stamp and
   sequence still decide: a recorded stamp at or below the floor yields that
   sequence; a recorded stamp above the floor is corruption and fails closed.
   GC holds manifest objects for the token retention plus the same margin,
   so a collected anchor is never above the floor;
2. every sequence pinned by an active snapshot or export root, streamed by the
   same retained-root inventory GC uses; and
3. every checkpoint's sequence while its own retention (`min_retention_seconds`,
   floored at 30 days) has not elapsed.

If any of those inputs is unavailable or invalid, the job fails closed without
rendering.

### Certificate and equivalence rule

The new manifest carries:

```text
retention_horizon {
  encoding_version: 1,
  horizon_sequence,
  purge_cutoff_ms,
  pinned_evidence: [ { kind, id, sequence } ],
  purged_rows_sha256,        // digest over the ordered purged rows (key, generation, tombstone, expires_at_ms)
  purged_counts: { expired_rows, tombstones },
}
```

Logical sequence and history root are unchanged; layout generation advances;
the full-state checksum becomes the checksum of the pruned state. The
equivalence rule for this transition kind is "parent state minus the certified
purged set equals the new state". The worker verifies it before its
exact-version head CAS and binds the certificate into the manifest exactly as
consolidation evidence is bound today. Ancestry walkers accept a horizon
transition when the certificate binds to its parent's state checksum, the
equivalence evidence names the parent's physical root, and the sequence and
history root are unchanged; manifests carry no row totals, so walkers cannot
check the purged counts against them. As with consolidation evidence, this is
verification by independent code at rewrite time, not by an independent
party; readers cannot recompute the identity after the purged rows are gone.

### Outbox rows

The existing acknowledge-then-trim saga already folds trimmed outbox rows out
of L1 at replay. The control-store worker enables it for the catalog consumer
after each drain (retire acknowledged acks in the ack root, then commit exact
incarnation trims in the catalog root under the cooperative writer epoch). The
operator endpoint's refusal of catalog trims stays; only the worker trims.
The trim goes through a fixed-consumer path
(`ProjectionOutboxWorker::trim_fixed_consumer`, exposed as
`CatalogProjectionMaterializer::trim_once`) that installs no binding
metadata in the catalog root, because the fixed-consumer drain refuses a
root carrying it.

## Catalog adapter

- `stage_commit_records` stops writing the audit row (key tag 4) and writes the
  receipt with `put_with_expiry(occurredAtMs + 24 h)`, where `occurredAtMs`
  is when the adapter accepted the request. The audit record still
  becomes the projection intent payload, unchanged.
- `load_receipt` is untouched; an expired-but-unpurged receipt still
  short-circuits a replay, which is the safe direction.
- The audit-key absent precondition added by PR #436 is removed with the row;
  the family-qualified operation id stays because the intent id depends on it.

## Audit projection

On each drained intent the catalog materializer appends the decoded audit
record to a day-partitioned `system.catalog.audit` Parquet artifact before it
acknowledges, so artifact-before-ack ordering guarantees no audit is lost when
the outbox row is later trimmed. Retention is a projection setting (default
400 days) enforced by the control-store worker's audit sweep at the end of its
GC phase (`CatalogProjectionMaterializer::expire_audit_partitions`;
`ARCO_CONTROL_STORE_AUDIT_RETENTION_DAYS`, at least 30 days). Arco hosts no
SQL catalog: `system.catalog.audit` is this design's name for a published
Parquet projection, not a registered table, and it exists only under a
control-bound catalog root. A restore emits its own audit record for the
restore mutation and does not resurrect historical audit rows; step 4
delivered the restore-emitted record (see "Restore and checkpoints").

*As implemented (step 3):* one immutable single-row file per acknowledged
intent at
`control/v1/projections/catalog-audit/dt=YYYY-MM-DD/{source_logical_sequence:020}-{intent_id}.parquet`,
written after the snapshot files and before the snapshot manifest. See the
"As implemented" section of `2026-09-27-state-store-retention-step3-adapter.md`.

*As implemented (step 4):* every `:` of an operation id becomes `-` in the
file name, because Hadoop-style path readers reject `:`. Intent ids contain
no `:`, so their names are unchanged. A restore's row is filed at
`dt=YYYY-MM-DD/{result_logical_sequence:020}-restore-{restore_id}-{attempt}-{domain}.parquet`.

## Restore and checkpoints

- Restore plan 7 landed in step 1. It binds the format-9 transaction shape
  and pins the candidate's `committed_at_ms`; versions 1 through 6 are
  supersession-only.
- A restore never restores idempotency receipts (tag 3) and leaves none
  behind. After a restore a client replay must re-apply, not be answered from
  a receipt taken before it. This changes the rendered restore bytes, so it
  needs a new plan version: step 4 moved restore plans to version 8.
- *As implemented (step 4):* the exclusion is a restore key policy held by
  the restore participant, not a catalog rule inside the kernel.
  `RestoreKeyPolicy` holds at most 16 canonical excluded key prefixes
  (non-empty, sorted, deduplicated, and without any prefix a shorter one
  covers) and a `sha256:<hex>` digest over them.
  `ControlMvpRestoreParticipant::with_key_policy` sets it; the default,
  `RestoreKeyPolicy::none()`, follows the plain rules for every key. The
  catalog policy, `catalog_restore_key_policy()`, excludes tag 3 only, and
  `catalog_restore_participant(store)` builds the catalog participant with
  it (it refuses a store outside the `catalog` domain). Tag 4 (residual
  pre-step-3 audit rows) is not excluded: those rows are the only record of
  those audits and follow the plain rules.
- Under a policy a format-9 restore follows three rules. Excluded source
  rows are dropped before the restore source scan charges its row and byte
  budget, so they are never put. Every excluded key live in the current
  state or in the candidate parent is deleted at the restore sequence. Every
  other key follows the plain rules. A keyed request replayed after the
  restore therefore re-executes, as after its receipt expired.
- Empty-base rule. With no head pointer the writes are diffed against an
  empty current state but land on the candidate parent, which is the source
  lineage. A filter on the source scan alone would leave that lineage's
  receipts in place, so the excluded deletes are taken from the candidate
  parent: the source lineage's receipts are tombstoned too.
- Plan 8 binds the policy digest (`restore_key_policy_sha256`, required).
  Plan 7 joins the supersession-only versions: it inspects `Superseded`,
  even when its restore already landed, and apply refuses it without
  writes. A plan bound to another policy inspects `Superseded`, so the
  driver replans it, rather than failing as a byte mismatch. The policy
  governs rendering, not recognition: a landed restore inspects `Visible`
  whatever the inspecting participant's policy. The bounded (format 8)
  restore cannot honour a policy, so its planning and advance refuse a
  non-empty one with `UnsupportedOperation`.
- Capacity. Before step 4 the source scan charged receipts against its
  64 MiB decoded budget. At about 564 B per receipt row that budget held
  about 119k receipts, below the ~180k live at the pilot rate, so a
  pilot-rate restore failed at plan time. Receipts no longer charge it.
  Their deletes share the restore's single L0 segment with every other
  restore write and the notice; its 512 KiB index limit binds at roughly
  390k live receipts. A restore that does not fit fails `Validation` naming
  the restore and its put, delete and excluded-key delete counts, and HEAD
  does not move. Each restore also turns every live receipt into a
  tombstone at the restore sequence. Tombstones carry no expiry hint, so the
  horizon purges them only once it passes the restore sequence (at least
  30 days): up to ~180k extra retained rows at the pilot rate.
- Restore projection and audit row. Before step 4 the restore commit's one
  outbox record, the restore notice `restore:{restore_id}:{attempt}:{domain}`
  (payload `ControlMvpRestoreNotice`), was not a `ProjectionIntentV1`. The
  catalog materializer quarantined it `INVALID_PROJECTION_INTENT`. The
  quarantine was terminal, the record was never acknowledged or trimmed, and
  no snapshot was published at the restore sequence. Step 3 did not cause
  this. *As implemented (step 4):* the materializer recognises every
  `restore:` record before the intent decode and authenticates it with
  `ControlMvpStateStore::resolve_restore_notice_source`. It then publishes it
  like a mutation: the restored catalog state as the snapshot at the
  restore's own result manifest, one audit row for the restore, then the
  snapshot manifest. The notice is acknowledged after that and trimmed. The row's
  operation id is the notice record id; its family is `restore_domain`, its
  actor `restore`, its request digest the SHA-256 hex of the notice payload,
  its occurrence the result manifest's `committed_at_ms`, and its logical
  sequence and authority manifest those of the restore's result. A
  malformed notice, or one the authenticated lineage refutes, is
  quarantined `INVALID_RESTORE_NOTICE`; a publication failure is
  quarantined `INCOMPATIBLE_PROJECTION_INTENT`. A notice quarantined before
  step 4 keeps its `INVALID_PROJECTION_INTENT` disposition and is never
  processed again; resolving it is an operator follow-up. A restore commit
  wakes no post-commit drain, so its notice is materialized on the next
  drain.
- Checkpoints carry the manifest's `retention_horizon`, and
  `validate_source` requires it to equal the source manifest's. Step 1
  copied it in `prepare_checkpoint` only. Step 4 makes
  `prepare_bounded_checkpoint` (the mixed workspace capture path) copy it
  too, so every capture path carries the certificate and a capture no
  longer fails when a format-9 domain's HEAD is a horizon manifest.
- A key live in the source cut and absent in current is written at the
  restore sequence (existing rule), so a purged tombstone can never be
  resurrected as an old generation.
- A checkpoint's `min_retention_seconds` participates in `horizon_sequence`, so
  a long-lived checkpoint holds the horizon back rather than being violated.

## Failure cases

| Case | Behaviour |
|---|---|
| Horizon rewrite loses its head CAS | Regenerates under the new generation, like consolidation. |
| A pin is published between horizon computation and CAS | A new pin references the current head, whose sequence is at or above the computed horizon, so it cannot lower the bound; the exact-version CAS still regenerates the job if the head moved. |
| Pin inventory unreadable or malformed | Job fails closed; nothing rendered. |
| Anchor object collected while its stamp is still above the floor | Cannot happen while writer stamps do not run ahead of object-store time: GC holds manifests for the token retention plus the skew margin, one margin longer than tokens, so any collected anchor is at or below the floor and decides by its record. A writer clock ahead of the object store by δ reopens a δ-wide band, which fails closed. |
| Worker clock ahead of artifact stamps | The horizon walk uses `committed_at_ms` from artifacts; expiry purge carries a one-hour safety margin. |
| Expired receipt read before purge | Returned as a valid receipt (replay short-circuits). Safe. |
| Reader pinned before the rewrite | Reads its own manifest's L1 and still sees the tombstone. |
| Transaction pinned after the rewrite | Observes absence; its commit validates against the same pinned replay. Deterministic. |
| Older binary opens a format-9 root | `UnsupportedAuthorityFormat` at open; no partial read. |
| Keyed request retried after its receipt was purged | Re-executes under the same deterministic operation id. While the earlier projection intent is still retained in the outbox, staging fails closed with a projection-intent conflict (`AlreadyExists { entity: "projection intent" }`) and commits nothing, unless the command fails first; after the worker drains and trims that intent, the retry commits and stages a fresh outbox incarnation. A quarantined intent is never acknowledged, so never trimmed: a transient quarantine cause clears on a later drain, but a persistent one (for example divergent artifact bytes) keeps that (family, key) returning the conflict (409) until the quarantine is resolved; operators check the drain's `quarantined_records`. Audit identity downstream is `(operation_id, logical_sequence)`. A per-call operation id would remove the conflict; that is an owner decision. |

## Verification

All tests are differential against pre-change behaviour.

- Kernel: format-9 fixtures replace the format-7 ones. A horizon rewrite over a
  state with expired receipts, live rows, purgeable and non-purgeable
  tombstones, and unacked outbox rows must drop exactly the certified set.
  Forged certificates (wrong horizon, missing pin, altered purged digest, purge
  of a live row, purge of a tombstone above the horizon) fail closed at open
  and at ancestry walk. Retained-token reads before the rewrite still see the
  tombstone; transactions after it see absence. The model oracle gains an
  expiry column and a horizon operation; the 32-seed reclamation and
  maintenance models gain horizon operations in their families.
- Adapter: no audit key is ever written; receipt replay works inside 24 hours;
  after a rewrite purged the receipt, the replay re-applies; restore never
  restores a receipt.
- Projection: audit rows land in Parquet before ack; a drain interrupted after
  the artifact and before the ack re-materializes idempotently; the
  control-store worker's audit sweep ages audit partitions by the configured
  retention.

## Gaps found during step 3

- **Catalog snapshot directories are never collected.** The audit projection
  text above originally relied on a projection GC that "already ages
  superseded snapshots". None exists. Every acknowledged catalog intent
  publishes a full catalog snapshot under
  `control/v1/projections/catalog-parquet/`, and neither the kernel GC
  (scoped to `control/v1/domains/<domain>/`), the legacy GC (`snapshots/`),
  nor the audit sweep deletes it, so these directories grow by one full
  snapshot per mutation without bound. Follow-up: collect superseded
  snapshot directories.
- **Semantic retry acceptance for projection Parquet is deferred.** A
  redelivered audit file, like the snapshot files, is accepted only when its
  bytes are identical, and Parquet bytes are stable only within one build of
  the `parquet` crate. Accepting an existing file whose decoded rows match
  was due no later than step 4, because restore-emitted audit records might
  be written without snapshot files. Step 4 re-scoped it out: a restore
  writes its snapshot files before its audit file, exactly like a mutation,
  so that trigger does not exist. The library-upgrade hazard itself is
  older than step 4 (snapshot files compare bytes too) and stays a
  follow-up with no step attached. Until it lands, drain the catalog outbox
  before deploying a different `parquet`/`arrow` version; the API (post-commit
  and operator drains) and the worker must run the same version.

## Gaps found during step 4

- **An intent quarantined at publication is re-walked on every drain and
  can stall every drain.** The drain hands every unacknowledged record to
  the materializer, quarantined or not. An intent quarantined
  `INCOMPATIBLE_PROJECTION_INTENT` at publication (for example for different
  bytes at an artifact path) is therefore resolved again on every drain: an
  authenticated ancestry walk from the current head back to its source
  manifest, then a publication that fails again. The walk is capped at
  4,096 manifests. Once the intent's source is further behind the head than
  that, the walk fails `AmbiguousAuthorityOutcome` ("authenticated ancestry
  resolution budget exhausted"), which is retryable, so every drain aborts
  at that record and no later record is materialized, acknowledged or
  trimmed. At the pilot rate (about two catalog commits a second) the cap is
  reached within about 35 minutes. This predates step 4; step 4 exempts only
  `restore:` records, which are never processed again once quarantined. The
  same cap applies to any record left undrained for more than 4,096 catalog
  manifests. Follow-up: the quarantine-resolution path.
- **Restore notices quarantined before step 4 stay quarantined.** They keep
  `INVALID_PROJECTION_INTENT`, are never acknowledged or trimmed, and have
  no snapshot at the restore sequence and no audit row. Later mutations
  materialize normally. Resolving them needs the same operator path.
- **No production composition registers restore participants.** Restore is
  reached from tests, benches and the S3 qualification binary; a production
  composition must register `catalog_restore_participant` for the catalog
  domain.

## Capacity acceptance

Re-run the Gate 7 capacity probe on format 9 with the pilot mutation mix for one
synthetic week, with the horizon job running on every worker invocation:

- retained rows stay under 500k;
- restore of the current cut fits the existing scanner and one restore L0
  segment, including its receipt deletes;
- the operation-cost harness reports per-commit replay bytes at that state
  size. That number is an input to the real-provider throughput decision, not a
  pass/fail criterion here.

## Sequencing

Each step is its own reviewable change, landed in order after PR #436:

1. Kernel: format 9 (`committed_at_ms`, `expires_at_ms`, `put_with_expiry`), the
   `RetentionHorizon` transition, certificate validation, fixtures, oracle.
   Landed (PR #439).
2. Worker: the horizon job kind and the catalog outbox trim after drain.
   Landed (PR #442).
3. Adapter: receipt expiry, audit-row removal, and the `system.catalog.audit`
   projection with its retention. Landed (PR #444); the restore-emitted
   audit record moved to step 4.
4. Restore: the restore key policy and restore plan 8 (a restore never
   restores receipts), restore notices materialized with a restore audit
   row, and the horizon certificate on every checkpoint capture path.
   Landed (this change). Restore plan 7 and the certificate on the main
   capture path had landed in step 1. Semantic retry acceptance for
   projection Parquet was re-scoped out (see "Gaps found during step 3").
5. Capacity probe and report.

No conversion step exists: the pilot root is seeded fresh on format 9.

## Out of scope

Bounded commits (authority 8), the Step-3 durable-restore branch, provider
throughput, and any change to retention floors (7-day orphan, 30-day token and
checkpoint) or to the 16/32 L0 thresholds.
