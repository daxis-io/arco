# Authority 9 retention contract, encoding 1

Retention step 1 writes authority format 9, segment/directory format 2 and
restore-plan format 7. Format 9 is format 7 (see
[integrity encoding 1](state-store-integrity-format-v1.md) and
[block format 1](state-store-block-format-v1.md)) plus wall-clock stamps, a
per-row expiry hint, an age-anchor chain and a certified `RetentionHorizon`
maintenance transition. Layout, exact-version HEAD CAS, writer-epoch fencing,
reclamation generation, the history root, the existing hash domain tags (one
tag, `arco/control-v1/retention-purge`, is added), the retention
floors (7-day orphan, 30-day token and checkpoint) and the 16/32 L0 thresholds
are unchanged. Point reads, scans and witnesses are unchanged. The design is
`2026-09-26-state-store-retention-design.md` (accepted, including the
2026-09-26 age-anchor amendment).

## Versions

`CONTROL_MVP_FORMAT_VERSION` is 9 on the head pointer, manifests, transactions
and checkpoints; `SEGMENT_FORMAT_VERSION` is 2 on segment directories and in
every owning state reference; `RESTORE_PLAN_VERSION` is 7. Restore plans 1
through 6 decode as supersession-only: they can be inspected and superseded
but never applied. Retained-token reads accept a manifest header of 9 or 8,
where 8 is the test-only bounded-directory authority; any other header returns
`UnsupportedAuthorityFormat`. Durable maintenance descriptors bind authority
version 9 and segment/directory version 2.

Every canonical hash keeps encoding version 1 and the format-7 framing, with
`u32(authority format = 9)` in place of 7:

```
bytes(fixed domain tag)
u32(encoding version = 1)
bytes("arco-state-control-mvp")
u32(authority format = 9)
bytes(exact tenant UTF-8)
bytes(exact workspace UTF-8)
bytes(exact domain UTF-8)
```

Because the format number is framed, every digest changed with the bump:
genesis, mutation, history step, manifest layout, checkpoint layout and the
new purged-rows digest. There is no conversion from format 7 and no dual
reader; format-7 pointers, manifests, transactions and checkpoints and
eight-column format-1 segments fail closed (see "Old binaries" below).

## Stamps and monotonicity

Manifests and transaction objects carry `committed_at_ms: i64`, wall-clock
milliseconds since the Unix epoch taken at render. Validation requires a
positive value on both. The stamp is informational for ordering (logical
order remains the sequence); retention reads it as the sequence-to-time
mapping. It is in no canonical digest: the envelope checksum of the manifest
or transaction authenticates it.

Stamps never run backwards along ancestry. A commit stamps its child
`max(render clock, parent stamp)`. A maintenance publication stamps
`max(descriptor.created_at, parent stamp)`, so a pending attempt reconstructs
the same bytes and a job clock up to 24 h older than the parent HEAD cannot
regress the stamp. A restore candidate rejects a stamp before its candidate
parent. The ancestry walker rejects a child stamped before its parent as an
invalid transition. Restore plan 7 pins the `committed_at_ms` its format-9
candidate bytes carry (required and positive); legacy plans must not carry
one.

## Segment format 2 and the expiry hint

Segment format 2 adds a ninth Arrow column, `expires_at_ms: Int64, nullable`,
after `origin_sequence`. The eight format-1 columns keep their names, types,
nullability and order, and preflight requires exactly nine field nodes and the
exact schema. Directories persist `formatVersion` 2, and each owning state
reference encodes `u32 segment format 2, u32 directory format 2`, so the
segment format is bound into the physical root.

`ArcoStateTxn::put_with_expiry(key, value, expires_at_ms)` stages a live value
with a hint; a plain `put` or `delete` of the same key clears it. The one rule
every committed hint satisfies (`expiry_hint_is_valid`, shared by transaction
replay, segment decoding and the deterministic model) is: absent, or a
positive instant on a live KV row. Tombstones, outbox rows and trims never
carry one, and a non-positive value is a `Validation` error before staging.
The bounded format-8 path rejects the hint before staging ("expiry is not
supported on bounded authority"), and its mutation verifier rejects any
candidate KV row carrying one.

The hint is a purge-eligibility hint only. Point reads, scans, point and range
witnesses, range preconditions and predicate inputs ignore it: an expired row
stays visible until a horizon rewrite drops it, so commit validation has no
wall clock in it. Two places bind the hint:

- The full-state checksum. Each `ReplayStateDigestEntry` carries
  `expires_at_ms`, serialized only when present, so hint-free states keep
  their format-9 checksums.
- The mutation digest. After each sorted entry's `optional(value)` comes
  `optional_i64(expires_at_ms)`: one byte 0 when absent, or one byte 1
  followed by the 8-byte big-endian signed value.

L1 rendering, durable maintenance and restore renders carry the hint row for
row. The restore source scan returns every live row of the materialized
checkpoint cut with its hint, and `restore_writes` treats a hint-only
difference as a difference to reproduce.

## Age anchors

Every non-genesis format-9 manifest carries `age_anchor: Option<AgeAnchorV1 {
manifest_id, manifest_sha256, sequence, committed_at_ms }>`. The render rule
(`age_anchor_for_child`) buckets stamps by hour (`committed_at_ms` divided by
3,600,000, Euclidean): a child stamped in a later bucket than its parent
records the parent (the last manifest of the previous bucket); otherwise it
inherits the parent's anchor; genesis has none. The rule is applied on every
render path: commit, restore candidate, consolidation and horizon candidates.

Manifest validation requires an anchor to name a valid immutable id, a raw
digest and a positive stamp, and to come from an earlier bucket of the
manifest's own ancestry: a parent must exist, `anchor.sequence <=
logical_sequence`, `anchor.committed_at_ms <= committed_at_ms`, and the
anchor's bucket is strictly earlier. The ancestry walker checks the render
rule as a transition invariant: the child's anchor must equal exactly what the
rule derives from the authenticated parent (the parent's record across a
bucket, the parent's anchor within one). Following anchors steps back at
least one hour per hop, so the 30-day floor is reached in at most about 721
authenticated reads regardless of commit rate. The anchor is in no canonical
digest; the manifest envelope authenticates it, and each anchor is
authenticated at use by its recorded digest, sequence and stamp.

## Retention-horizon certificate

A manifest published by a `RetentionHorizon` transition carries
`retention_horizon`; mutations, consolidations and restores carry none:

```text
retention_horizon {
  encoding_version: 1,
  horizon_sequence: u64,
  purge_cutoff_ms: i64,
  pinned_evidence: [ { kind, id, sequence } ],
  parent_state_checksum_sha256,
  purged_rows_sha256,
  purged_counts: { expired_rows: u64, tombstones: u64 },
}
```

Structural validation, run by manifest and checkpoint validation against the
carrying logical sequence: encoding 1; both digests are 64-character lowercase
hex; `horizon_sequence <= logical_sequence`; `purge_cutoff_ms > 0`; every
evidence entry names a kind in `manifest_age | snapshot | export | checkpoint`
by a valid immutable id, with `sequence >= horizon_sequence`. A manifest with
a certificate must also carry rewrite equivalence evidence. Checkpoints copy
the source manifest's certificate at creation and validate it on read.

`purged_rows_sha256` uses tag `arco/control-v1/retention-purge` and the
standard framing, then `u64(row count)` and, per purged row in strictly
increasing binary key order, `bytes(key) u64(generation) u8(tombstone)
optional_i64(expires_at_ms)`. Rows out of key order or duplicated are an
invariant violation. Readers cannot recompute this digest after the rows are
gone: as with consolidation evidence, the identity "parent state minus the
certified purged set equals the new state" is verified by independent code at
rewrite time, not by an independent party.

## The three ancestry transitions

The bounded ancestry walker (4,096 manifests, 64 MiB, one manifest at a time)
accepts exactly three parent-to-child transitions. In all three the child's
history root continues the parent's (the child's `parent_history_root` equals
the parent's `history_root`), the child's stamp is at least the parent's, and
the child's anchor follows the render rule; anything else is "invalid
authenticated ancestry transition".

1. Mutation: `parent.logical_sequence + 1 == child.logical_sequence`, the same
   layout generation, the child carries neither equivalence evidence nor a
   certificate, and the parent's successor anchor digest equals the child's
   predecessor digest.
2. Consolidation: the same sequence, layout generation + 1, the child's state
   checksum equals the parent's, no certificate, the child's equivalence
   source physical root equals the parent's physical root, and the parent's
   maintenance intent names the child's layout generation.
3. Horizon: the same sequence, layout generation + 1, the child carries a
   certificate whose `parent_state_checksum_sha256` equals the parent's state
   checksum (the child's own checksum is the pruned state's), and the child's
   equivalence source physical root equals the parent's physical root. No
   maintenance intent is required: the horizon runs on the worker's schedule.

## The horizon job

`MaintenanceKind` is `CONSOLIDATION` (the default) or `RETENTION_HORIZON`. A
horizon descriptor additionally binds the admitted `HorizonInputs {
horizon_sequence, purge_cutoff_ms, pinned_evidence }` and the plan's purge
summary (the pruned render-cut checksum, the purged digest and the counts).
The kind and the inputs are bound into the render seed; the purge summary is
bound into the job identity (the descriptor digest) but not the seed. Their
presence must agree with the kind. `DurableMaintenanceWorker::prepare_horizon_at(now)`
computes the inputs, replays the head and admits a plan; it returns no plan
when the root has no head or nothing is eligible (zero purged counts), and it
needs no maintenance intent. The prepared job then uses the same `start_at`,
`advance_at`, `resume_at`, `publish_at` and `abandon_at` lifecycle as
consolidation. `prepare_at` (consolidation) never purges.

### Inputs

`purge_cutoff_ms` is `now - 1 h` (`CONTROL_MVP_RETENTION_CLOCK_SKEW_MS`) and
must be positive. `horizon_sequence` is the minimum over one evidence entry
per kind, each at that kind's lowest sequence. Any unreadable or invalid input
is an error and nothing is rendered.

1. `manifest_age`, the age bound. The floor is `now - 30 d - 1 h`: token
   validity and GC judge age by backend object time, the walk by writer
   stamps, and the margin keeps a still-valid token's manifest from being
   treated as past the floor. If the head's stamp is at or below the floor
   the bound is the head's sequence. Otherwise the walk follows `age_anchor`
   links from the head under a hop budget of 1,024 and a byte budget of
   1,024 x 1 MiB. Each anchor is loaded with its recorded digest as the
   expected checksum, and its sequence and stamp must equal the record. The
   first manifest stamped at or below the floor is the bound. An anchor GC
   already collected decides by its record only when the record is at or
   below the floor; a missing anchor recorded above the floor, an exhausted
   budget, or a record its manifest contradicts fails closed. A chain that
   ends above the floor yields bound 0, citing the newest manifest examined.
2. `snapshot` and `export`: every active pin streamed by the retained-root
   inventory GC uses, skipping maintenance roots, for authorities naming this
   scope. Each pinned manifest is loaded with its recorded checksum and must
   carry the reference's sequence.
3. `checkpoint`: the `checkpoints/` inventory, paged like GC. A checkpoint is
   retained while its object age is within `max(min_retention_seconds, 30 d)`
   (`checkpoint_retained_at`, shared with GC; an object without a timestamp is
   always retained). Each retained checkpoint's source manifest is loaded and
   the checkpoint is validated against it.

### Purge predicate

Over the render cut's replayed KV map, a row is purged when its generation is
at or below the cut's logical sequence and either it is a tombstone with
`generation <= horizon_sequence`, or it is a live row with
`expires_at_ms < purge_cutoff_ms`. Live rows without a hint, unexpired rows,
tombstones above the horizon and rows a later suffix wrote are never
candidates. Outbox rows and trims are copied unchanged. Plan pages render the
pruned state, so ordinals and digests line up with the admitted summary.

### Pre-CAS verification

Publication first checks compatibility: the reclamation generation and the
layout generation are unchanged, and the head still owns the descriptor's
source states and extends its suffix without a restore transaction. The
completed L1 outputs must reproduce the pruned render-cut checksum at the
cut's sequence; the suffix transactions after the cut are then applied. The
publisher replays the current head afresh, recomputes the purge with the
admitted inputs bounded to the cut's sequence, and requires the recomputed
purged digest and counts to equal the admitted plan's. Otherwise the job is
refused by `publish_at` with `PreconditionFailed` ("retention horizon purged set was
superseded by later commits"), because a suffix rewrote a purged key since
preparation; the driver abandons it and prepares again. The candidate state
must then equal the pruned parent exactly, by value and by checksum.

### Published evidence

The candidate keeps the parent's logical sequence and history root; its
history anchor moves to the render cut (`HistoryAnchor { sequence:
render.logical_sequence, root: render.history_root }`), as for consolidation.
The candidate keeps the parent's logical sequence and history
root, advances the layout generation, sets `state_checksum_sha256` to the
pruned state's checksum, binds the certificate with
`parent_state_checksum_sha256` equal to the parent's checksum, and carries
`equivalence` (encoding 2) naming the parent as its source, with a
`render_source` whose `state_checksum_sha256` is the pruned render-cut
checksum. Stamp and anchor follow the rules above. A consolidation candidate
clears any certificate it would inherit from its cloned parent. The exact
captured HEAD version CAS still governs publication: a lost CAS regenerates
the candidate under the new head.

Exclusivity with consolidation: both kinds share the head's layout generation,
and activation claims the workspace retention mutation epoch under the
retention lock, so of two concurrently running jobs at most one publishes;
the other fails its compatibility check with a precondition error and is
abandoned like any consumed job. `Superseded` keeps its meaning: source
compatibility was consumed by a different publication and the descriptor's
24 h lifetime has expired; before that a consumed attempt returns to
`ReadyToPublish` and regenerates.

## Old binaries

The format checks are symmetric. This binary rejects a format-7 head pointer,
manifest, transaction or checkpoint at pointer load, envelope validation,
transaction metadata load and the retained-token format witness, before any
typed payload is followed; a format-7 binary rejects format 9 at the same
points. A format-9 root is therefore unreadable and unwritable by an older
binary, and recovery is roll-forward, never a partial read. Segment format 1
(eight columns) fails preflight, and restore plans 1 through 6 are
supersession-only. No production root existed on format 7, so no conversion
exists: the pilot root is seeded fresh on format 9.

## Out of scope for step 1

Not implemented by this step, stated so the contract is not read as claiming
them: the catalog adapter still writes the audit row and plain receipts
(`put_with_expiry` has no adapter caller); the `system.catalog.audit`
projection and audit-row removal; the catalog outbox trim after a drain; a
restore that skips receipt rows (today the restore source scan returns every
live row of the checkpoint cut and re-stages receipts with their hint); and
the scheduled worker binary invoking the horizon job kind (it prepares
consolidation only). These are steps 2 to 4 of the design.

## Vectors and fixtures

`../reports/2026-09-26-format9-canonical-vectors.json` pins the format-9
preimages and digests (genesis; empty, one-entry and expiring mutations and
history steps; manifest and checkpoint layouts). It is generated by the
implementation under test (`generate_format_canonical_vectors`), so it is a
regression pin only; independent regeneration, as the format-7 vectors had,
is a follow-up. `../reports/2026-09-06-gate3-canonical-vectors.json` is the
independently produced format-7 set and is historical. Fixtures live in
`crates/arco-catalog/tests/fixtures/control_mvp_authority_v9/` (legacy-scope
manifest, transaction and checkpoint) and
`crates/arco-catalog/tests/fixtures/control_mvp_restore_plans/`.
