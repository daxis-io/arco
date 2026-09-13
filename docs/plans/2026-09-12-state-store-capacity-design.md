# State-store capacity follow-up — September 12, 2026

Status: local architecture investigation and executable feasibility checks. This is a
new follow-up to the frozen Gate7 candidate, not an amendment that makes Gate7 pass.
No cloud resource, production authority, routing, or retention policy is changed.

## Source and contract

The isolated worktree reconstructs all1,096 files of Gate7 checkpoint05 from base
92fd19f11a547ece5004ac94cad83a3527471812. The initial manifest digest is
079a7b5251ad06d34c90aa9cff12c54975ef2dab300a9ded31843a0474824924.
The original Gate7 worktree and all recovery archives remain untouched. The runtime
cache-reuse handoff0da54a1c remains a separate, unintegrated candidate.

This follow-up resolves the capacity architecture and tests its storage assumptions.
Production protocol implementation requires a new qualification contract: the existing
instruction to preserve authority7 and restore-plan6 is not silently relaxed.
The original operation limits, record retention, single authority, exact HEAD CAS,
history validation, outbox ordering/incarnations, cache ownership and fencing remain
requirements. No records are discarded, no root rotation or cross-root commit is used.

## Problem traced through the actual code

1. Each semantic catalog mutation adds an idempotency receipt and audit record in
   `catalog_authority::stage_commit_records`. The selected pilot needs1,209,600
   mutations and therefore at least2,419,200 retained rows, plus objects, indexes,
   acknowledgement records and pending outbox records.
2. `restore_source_values` calls a scanner limited to1,000,000 rows /64MiB and
   collects every visible source row into a BTreeMap.
3. `load_stable_restore_base` replays the full current root; missing-HEAD restore
   also replays source lineage. `restore_writes` builds the complete difference.
4. `render_restore_candidate` puts all differences in one L0 limited to64MiB and
   1,000,000 rows (including outbox rows). Transaction JSON contains references; its
   writes/outbox fields are serde-skipped. The4MiB JSON cap is NOT a restore-value
   cap. A below-scan-limit source can still require a larger difference against
   a disjoint current state. A paged source scan alone cannot fix that barrier.
5. `lazy::TransactionBase::materialize_for_commit` still calls full replay.
   `ReplayState::checksum` clones every key/value and encodes the complete digest
   JSON before hashing. A restore-only change leaves ordinary writes proportional
   to total retained state. Cache reuse does not change this asymptotic work.
6. L1 already supports multiple bounded immutable shards. However the existing
   snapshot renderer first builds every row/shard in memory; durable maintenance
   planning has bounded units and global validation. Physical sharding alone is
   not a bound on full-root replay or hashing.

## Alternatives and decision

- Raise aggregate limits: rejected. It moves failure into the single-L0 limit,
  peak allocations, startup/replay cost, or write latency without bounding work.
- Expire or externalize receipts/audits: rejected for this contract. It changes
  idempotency/audit restoration and retained-read semantics; external storage also
  needs an atomic reachability/retention protocol. Compression does not solve rows.
- Partition authenticated logical state and prepare restore incrementally: selected.
  Reuse existing immutable segment/index encoding and the existing durable-work
  mechanisms, but make logical authentication and publication reference bounded
  pages instead of a full-state vector or transaction-sized restore diff.

This choice requires a new authority representation. A new restore job layered on
unchanged format7 would leave the ordinary-commit and history-validation problems.
The proposal below is a protocol design, not an already supported wire format.

## Selected architecture

### Authenticated state directory

A single HEAD references an immutable root that binds scope, logical sequence,
writer epoch, reclamation generation, history root, a KV directory root, and two atomically bound outbox index roots
(active record ID and delivery order). Directory pages have fixed encoded-byte and child-count bounds, ordered
nonoverlapping key intervals, authenticated child hashes, row counts and logical
content digests. Bound counts and bytes before allocation. Large directories split;
all references form a closed finite tree with validated depth and traversal budgets.

Use a persistent ordered tree whose identity is its exact hash-bound structure.
Equivalent physical layouts may have different tree roots; equivalence must be
proved by the ordered logical stream, not assumed from matching tree shape.
Preserve explicit tombstones and generation semantics. The active-ID outbox index maps
record ID to its exact incarnation (origin sequence, within-transaction ordinal) and
payload digest. The delivery-order index maps (origin sequence, ordinal, record ID)
to that incarnation and payload. Both roots and equal active counts are bound by the
same manifest. A bijection is established at genesis/conversion and preserved by each
verified transition: staging proves ID absence and inserts both entries; trim proves
the exact existing origin/ID and removes both; restaging after trim creates a new
incarnation and order position. Every transition proves the cross-index correspondence
for all touched records; count equality alone is insufficient. Ordered reads validate
the matching active-ID proof for each emitted record. No full-ID scan or full-vector
sort is allowed. Restore preserves both current indexes (or source indexes in the
absent-HEAD mode) and atomically appends the notice to both. GC and retention trace
both roots. Required tests include a large ordered outbox, duplicate IDs across pages,
trim/restage, and corruption/omission in only one index.

A narrow commit authenticates the HEAD and affected search paths, checks point/range
witnesses against the exact pinned root, changes bounded leaves, hashes their ancestor
paths and publishes one successor HEAD by version CAS. Unchanged subtrees are retained
by reference. Logical history binds the preceding logical history root and the canonical mutation
digest, excluding physical directory roots and page boundaries. Each manifest separately
binds its physical directory roots and an authenticated transition certificate. A normal
commit certificate proves the exact pinned prior physical root, unchanged subtree
references, affected key/interval witnesses and the expected replacement leaves. The
verifier checks complete interval coverage at every replaced ancestor; a matching
mutation digest alone never authorizes an arbitrary replacement directory. Range predicates require an authenticated gap/interval witness, including empty
ranges, so insertions or deletion/recreation cannot escape conflict detection.

A physical maintenance transition has no logical mutation and does not advance history.
Its certificate binds both physical roots, equal logical sequence/history, and a gap-free
ordered comparison of all selected old/new intervals (including tombstones/generations
and outbox ordering/origins). Unselected subtrees must be identical by reference. The
certificate and its pages are hash-bound by the new manifest, not folded into logical
history. Retained ancestry validation follows these certificates, and cannot treat
matching sequence/history alone as equivalence. The full ordered logical stream digest
may be computed during maintenance/audit; it is not replaced with an ad hoc commutative
hash or recomputed by every narrow writer. Required vectors include the same stream
partitioned into different physical pages: distinct physical roots, equal logical
history, verified equivalent content; changing one value/generation/origin must fail.

Full logical verification remains available as a streaming recovery/audit operation;
it ceases to be a full-root allocation on every narrow commit. This is a substantive
change to the format7 integrity contract and must be independently qualified.

### Restore preparation and final publication

Freeze the source checkpoint and current authority (raw digest, object version, writer
and reclamation fences). A restore job pins both roots under the existing retention
rules. Merge source-visible KV and current KV in key order through bounded pages:

- Equal visible values preserve the current generation.
- Changed or newly visible values receive the single restore result sequence.
- A current visible key absent from source becomes a result-sequence tombstone.
- Existing current tombstones absent from source remain; no resurrection or loss of ABA
  evidence is permitted. Source-only tombstones are not imported as visible values.
- With a present HEAD, current ordered outbox/incarnations survive; append exactly
  one restore notice. Do not rewind acknowledgements or copy an obsolete source
  outbox. With an absent HEAD, explicitly preserve authority7/restore-plan6 source-lineage behavior:
  the source lineage supplies inherited outbox/history and existing source tombstones
  (with their original generations); source-visible values are written at the new
  restore sequence and the result is newer than that source. This is a distinct verified preparation mode, not an empty-current
  shortcut through the present-HEAD rule.

Each completed unit persists deterministic input/output digests, last exclusive key,
row/byte accounting and the exact next cursor before advancement. Keep source and
current page caches and output ownership bounded; restart resumes only from verified
progress. The restore job binds every output page into its result directory. A receipt
commits to the ordered merge relation; per-unit checking alone cannot certify omitted
or duplicated key intervals, so the final validator also proves complete gap-free
coverage and exact aggregate counts across the page chain.

No prepared page is visible through HEAD. Finalization publishes one logical restore
transition with the result directory roots, the complete authenticated preparation
receipt and the one outbox notice. The record stays below the existing transaction
metadata ceiling by referencing the result directory and preparation receipt. Format7
already references L0 payload; this design removes the single-L0 payload restriction. Exactly
one conditional HEAD replacement establishes visibility. A changed current HEAD or
fence supersedes the job; do not rebase its pages onto another current root.

Lost publication responses use exact persisted candidate identity and authenticated
ancestry to distinguish visible, superseded and unresolved outcomes. Cleanup follows
reconciliation and retention, never a timeout guess. Keep the24-hour execution,
eight-day maintenance retention, seven-day orphan eligibility and30-day protected
checkpoint/token rules. A preparation exceeding its bound fails with backpressure;
job chaining cannot silently extend those deadlines or reset timestamps.

### Compatibility and rollout

Reserve a future authority format and restore-plan version only when implementing the
new contract (candidate names: authority8 / restore-plan7). Leave format7 readers and
restore-plan6 behavior intact. New readers must read and authenticate old roots;
old readers must fail closed on new roots. Transition requires an explicit per-root
conversion: stream and verify the old logical state, prepare the new directory,
then CAS its single authoritative HEAD with a history-bound conversion record.
No dual authority, dual write, automatic migration or routing change is included.
Old retained roots remain readable and protected until their existing disposition.

## Frozen local feasibility checks (before measurements)

Use Rust1.88, one sequential Cargo queue, a separate APFS-cloned target and disabled
incremental/debug info. No existing target is removed. Record source/tool/log digests,
raw durations, resource usage and failures outside the worktree.

1. Keep the original Gate7 capacity regression unchanged and reproduce it.
2. Verify a value-heavy restore serializes compact transaction metadata and succeeds,
   preserving the failed4MiB-payload hypothesis as diagnostic evidence. Separately
   demonstrate the single-L0 barrier with disjoint source/current keys using small
   injected row limits through actual commits/checkpoint/planning. Keep production
   defaults unchanged and label the scaled boundary test accurately.
3. Encode and decode2,419,200 unique retained inventory rows using the production L1
   codec in bounded batches. Derive values from actual emitted receipt/audit records;
   label this as a synthetic encoding fixture, not1,209,600 executed catalog mutations
   or the full80/10/5/5 workload. Preserve actual key shape and typed JSON record shape.
4. Record total decoded/encoded/index bytes, every shard's rows/bytes and hash, maximum
   batch rows/bytes, duration, and fresh decode equality for every row. Keep production
   segment limits unchanged; assert every output fits the existing half-capacity limits.
5. Reject a corrupted production block/index and retain evidence of zero silent drops.

This experiment tests physical encoding feasibility. It does not prove full-root
restore/replay/maintenance performance, a bounded authenticated-directory implementation,
or pilot eligibility. The final design must retain these distinctions.

## Implementation sequence and acceptance

1. Specify directory/hash domains, bounds, gap witnesses, compatibility and conversion
   vectors; implement immutable-directory reader/writer with forged-boundary, omission,
   duplicate, cross-scope and allocation-budget tests.
2. Implement bounded ordinary commits against authenticated paths, with eager-format7
   oracle comparison for points, ranges, tombstones, conflicts and ordered outbox.
   Require work and peak memory to scale with touched pages, not retained-row count.
3. Implement durable restore preparation, restart/supersession/fences, coverage receipt,
   one logical publication and exact uncertainty reconciliation. Differentially check
   every accepted restore against the existing small-state logical oracle.
4. Integrate reachability, maintenance equivalence, protected roots and GC. Test expired
   and superseded jobs, retained readers, source replacement and crash recovery at all
   publication/deletion boundaries. Do not reuse old equivalence certificates for new
   roots without a defined verification rule.
5. Run a real full-scale local root with the complete pilot mutation mix, acknowledged
   projections and retained history. Measure current/older-cut restore, commit, replay,
   maintenance, cache ownership and growth before binding a new provider manifest.
6. Implement the full pilot workload/cohort/SLO verifier and rerun affected local lanes.
   Real-S3 and168-hour evidence are later, separately authorized execution phases.

No ceiling or SLO is declared satisfied by this design. Cloud compute stays torn down.

## Amendment before capacity measurements

The initial4MiB restore-payload hypothesis was falsified by actual production rendering.
The failed test and superseded design are retained externally. The corrected experiment
measures compact transaction metadata and tests the actual single-L0 capacity boundary.
No production behavior was changed to force either outcome.

Review clarification: absent-HEAD restore inherits source tombstones/outbox/history.
Physical directory hashes are excluded from logical history; an independently verified,
hash-bound transition/equivalence certificate authorizes every physical root change.
These are required protocol semantics, not optional implementation details.

## Proposed operation bounds for the new protocol

These are design targets for the new contract, not changes to format7 defaults:

- Directory pages: at most64KiB encoded,128children, depth8; validate balance,
  monotone nonoverlapping intervals, counts and lengths before recursion/allocation.
- Logical leaves: existing64KiB block target and256KiB block maximum. A narrow
  write rewrites touched blocks and ancestor pages; it must not rewrite an entire
  32MiB packed maintenance container just to update a small record.
- Physical packing: retain64MiB segment maximum and existing32MiB maintenance
  output target; logical leaves may reference authenticated blocks in packed segments.
  Repacking changes physical references under a verified equivalence transition.
- Per-request mutation, read and cache ownership budgets stay at their existing
  ceilings. Directory/path/read-proof accounting is included, not hidden in a new
  uncharged pool. Excessive touched paths or proof bytes produce typed backpressure.
- Restore units reuse at most64MiB selected-input bytes and32MiB packed-output target;
  job pages and progress are bounded. A final certificate root references bounded
  proof pages; loading that root must not deserialize the complete job. All units
  together retain the24-hour execution deadline and original retention timestamps.

A fixed current-HEAD binding intentionally makes a job superseded by a competing
mutation. Large restore therefore needs a controlled quiet period or repeated attempts
with explicit liveness limits; this design does not promise completion while writers
continuously change the base. No silent rebase, extended deadline, or pilot-SLO exemption
is introduced. Reads continue against retained roots. Quiescence/admission behavior
must be explicit in the implementation/runbook and full-scale acceptance evidence.

Measurement amendment: the initial synthetic fixture used a shortened manifest ID.
Retain that run as superseded size evidence. The final fixture uses the production
manifest-ID shape and asserts its length matches each actual emitted template before
encoding. This changes the synthetic measurement input, not production behavior.
