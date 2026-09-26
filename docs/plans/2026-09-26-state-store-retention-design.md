# State-store retention design — authority format 9

Status: design accepted by the owner on 2026-09-26 (four sections reviewed in
turn). Implementation is sequenced below and is not started by this document.
Depends on the remediation branch that adds the control-store worker and the
metric emitters (PR #436).

## Decisions

| Question | Decision |
|---|---|
| Where do catalog audit records live? | Projection only. No audit row is written to the authoritative KV; the audit record rides the projection intent payload (as today) and is materialized into an append-only `system.catalog.audit` Parquet projection with its own retention. |
| How long is a keyed request replayable with its original response? | 24 hours from staging. Restore never restores receipts. |
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

*Amendment (2026-09-27, after Package C review):* one manifest per commit means
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

1. the sequence of the newest manifest whose `committed_at_ms` is older than
   the token retention floor plus a one-hour clock-skew margin (30 days + 1 h;
   token validity and GC judge age by backend object time, the walk by writer
   stamps), found by following `age_anchor` links from the current manifest
   (no listing). If an anchor has been collected by GC, its recorded stamp and
   sequence still decide: a recorded stamp at or below the floor yields that
   sequence; a recorded stamp above the floor is corruption and fails closed;
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
transition when the certificate binds to its parent, the sequence and history
root are unchanged, and the purged counts are consistent with the parent's and
child's row totals. As with consolidation evidence, this is verification by
independent code at rewrite time, not by an independent party; readers cannot
recompute the identity after the purged rows are gone.

### Outbox rows

The existing acknowledge-then-trim saga already folds trimmed outbox rows out
of L1 at replay. The control-store worker enables it for the catalog consumer
after each drain (retire acknowledged acks in the ack root, then commit exact
incarnation trims in the catalog root under the cooperative writer epoch). The
operator endpoint's refusal of catalog trims stays; only the worker trims.

## Catalog adapter

- `stage_commit_records` stops writing the audit row (key tag 4) and writes the
  receipt with `put_with_expiry(staging time + 24 h)`. The audit record still
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
400 days) enforced by the projection GC that already ages superseded
snapshots. The table is registered only when a control root is bound. A restore
emits its own audit record for the restore mutation and does not resurrect
historical audit rows.

## Restore and checkpoints

- The restore source scan skips receipt rows (tag 3) entirely. After a restore a
  client replay must re-apply, not be answered from a receipt taken before it.
- Restore plans move to version 7 to bind the format-9 transaction shape;
  versions 1 through 6 remain supersession-only.
- Checkpoints carry the manifest's `retention_horizon`. A key live in the source
  cut and absent in current is written at the restore sequence (existing rule),
  so a purged tombstone can never be resurrected as an old generation.
- A checkpoint's `min_retention_seconds` participates in `horizon_sequence`, so
  a long-lived checkpoint holds the horizon back rather than being violated.

## Failure cases

| Case | Behaviour |
|---|---|
| Horizon rewrite loses its head CAS | Regenerates under the new generation, like consolidation. |
| A pin is published between horizon computation and CAS | Caught by the retention-epoch fence and the exact-version CAS; the job regenerates. |
| Pin inventory unreadable or malformed | Job fails closed; nothing rendered. |
| Worker clock ahead of artifact stamps | The horizon walk uses `committed_at_ms` from artifacts; expiry purge carries a one-hour safety margin. |
| Expired receipt read before purge | Returned as a valid receipt (replay short-circuits). Safe. |
| Reader pinned before the rewrite | Reads its own manifest's L1 and still sees the tombstone. |
| Transaction pinned after the rewrite | Observes absence; its commit validates against the same pinned replay. Deterministic. |
| Older binary opens a format-9 root | `UnsupportedAuthorityFormat` at open; no partial read. |

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
  the artifact and before the ack re-materializes idempotently; projection GC
  ages audit partitions by the configured retention.

## Capacity acceptance

Re-run the Gate 7 capacity probe on format 9 with the pilot mutation mix for one
synthetic week, with the horizon job running on every worker invocation:

- retained rows stay under 500k;
- restore of the current cut fits the existing scanner;
- the operation-cost harness reports per-commit replay bytes at that state
  size. That number is an input to the real-provider throughput decision, not a
  pass/fail criterion here.

## Sequencing

Each step is its own reviewable change, landed in order after PR #436:

1. Kernel: format 9 (`committed_at_ms`, `expires_at_ms`, `put_with_expiry`), the
   `RetentionHorizon` transition, certificate validation, fixtures, oracle.
2. Worker: the horizon job kind and the catalog outbox trim after drain.
3. Adapter: receipt expiry, audit-row removal, and the `system.catalog.audit`
   projection with its retention.
4. Restore plan 7 and checkpoint horizon.
5. Capacity probe and report.

No conversion step exists: the pilot root is seeded fresh on format 9.

## Out of scope

Bounded commits (authority 8), the Step-3 durable-restore branch, provider
throughput, and any change to retention floors (7-day orphan, 30-day token and
checkpoint) or to the 16/32 L0 thresholds.
