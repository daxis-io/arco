# State store architecture and design audit — 2026-09-23

Target: `origin/main` @ `59dde89a` (PR #435), reviewed in a detached worktree at
`.worktrees/statestore-audit-20260923`. The local `main` checkout is 65 commits
behind origin and was not audited. Method: design-document review, then seven
parallel static code audits (commit/fencing protocol, deployed posture, catalog
adapter, maintenance/GC/retention, isolation and read path, authority 8,
projection pipeline). Every P1 below was re-verified in code by the lead
reviewer. Nothing was edited, built for effect, or committed; one
`cargo clippy -p arco-api --lib --bins -- -D warnings` run passed clean
(issue #425's lint claim is stale on this head).

## Verdict

The kernel's core invariants are well built and hold up under adversarial
reading: exact-version head CAS, immutable checksum-bound artifacts, writer
epoch fencing, reclamation-generation GC fencing, digest-authenticated reads,
bounded Arrow preflight, typed root scopes, and a narrow storage capability.
Isolation is sound by pin-then-CAS.

The architecture is not deployable as it stands, and the program is optimising
the wrong layer. Three independent unbounded-growth vectors (idempotency
receipts and audit rows, the never-trimmed catalog outbox, and tombstones that
are never reclaimed) meet hard scan and job caps, so every domain eventually
wedges. Nothing in a deployed binary runs layout maintenance, GC, or the
projection drain, so the 32-segment backpressure wall is reached after 32
commits on the catalog root and after roughly 16 materialised intents on the
acknowledgement root. The Terraform sole-writer grant protects a prefix the
kernel does not write. Alerts reference metrics nothing emits. The authority-8
directory work (about 70K lines across main and an unlanded branch) reduces
per-commit replay cost but does not reduce row growth, and it is now a
prerequisite for a pilot that the production format cannot pass.

Two protocol defects are P1 on their own: a lost-response writer-authority
claim can be adopted by two concurrent claimers, and reusing one
`Idempotency-Key` across operation families silently overwrites an audit
record.

## What is sound (verified)

- Base pinning binds pointer version, writer epoch and reclamation generation
  from one head/get/head verified read; commit CASes on that exact version;
  observations are validated against the re-authenticated pinned replay
  (`control_mvp.rs:798-855`, `lazy.rs:180-256`, `control_mvp.rs:4840-4877`).
- Epoch exact-match at begin and commit; `u64::MAX` rejected at every entry
  including the pointer validator choke point (`control_mvp.rs:3446-3454`, `:6198`).
- Commit reconciliation on transport error accepts only exact pointer bytes or
  an exact transaction reference in bounded visible lineage; otherwise
  `AmbiguousAuthorityOutcome` (`control_mvp.rs:4896-4936`, `:1969-2076`).
- Single production Arrow decode entry with length, SHA-256, flatbuffer
  verifier caps, exact schema, one batch, block arithmetic, then a
  `catch_unwind`ed reader (`control_mvp.rs:7433-7636`, `:7881-7946`).
- Aug-2026 P2s fixed: L1 duplicate keys (`>=` at `:7934`, `:8205`, `:7737`,
  `:1104`), null-origin trim rows (`:6707-6714`), restore `checkpoint_interval`
  persisted in plan v6 (`:3661-3675`), claim ambiguous readback added
  (`:597-616`, but see P1-3), request_id validation mostly closed.
- No unconditional put, delete, or listing on the kernel request path; every
  write goes through `ScopedAuthorityStore::put` with `DoesNotExist` or
  `MatchesVersion`. Listing is confined to the GC worker's lifecycle handle.
- Provider conditional-write mapping is real and the single-attempt
  conditional client is in all three production constructors
  (`arco-storage-object-store/src/lib.rs:296-298`, `arco-storage-s3/src/lib.rs:148-151`,
  `arco-storage-gcs/src/lib.rs:39`, `arco-storage-azure/src/lib.rs:49`).
- Range witnesses cover gaps and tombstones with length-prefixed hashing;
  scan pages are pinned to one token and cursors are AES-GCM sealed with scope
  and manifest witness; error paths on unseal are uniform.
- Read cache keys bind scope, backend identity, owning reference digests and
  versions; no lock is held across an await; cross-tenant sharing is
  impossible by construction.
- Projection saga ordering is correct: artifact, then ack-root status, then
  ack, then (non-catalog only) trim with exact incarnation validation; no
  intent-loss path was found; projections are not an enforcement input
  anywhere.
- Operator endpoints are default-off, posture-gated, and group-gated with
  fail-closed tests.

## P1 findings (all CONFIRMED in code)

### P1-1. Deployed IAM protects a prefix the kernel never writes

The kernel base prefix is `control/v1/domains/{domain}`
(`control_mvp.rs:2526-2528`), plus `control/directory/v1/...`
(`directory.rs:209,274`) and `{base}/maintenance/...`, all under the
`tenant=…/workspace=…/` scope. Terraform grants the API sole-writer only where
the path `startsWith("state-store/")` (`infra/terraform/iam_conditions.tf:49,149-162`);
its comments assert a `state-store/control-mvp/{domain}` layout that exists
only in stale test fixtures. No service account has any grant matching
`control/`. Enabling the control root or the operator endpoints on the
deployed service would have every kernel write denied; the "sole writer"
invariant is enforced against the wrong string, and the xtask guard
(`tools/xtask/tests/terraform_iam.rs:109-160`) only checks that literal.

### P1-2. No deployed component runs maintenance, GC, or the projection drain

`DurableMaintenanceWorker::new` / `ControlMvpMaintenanceWorker::new` are
constructed only by benches, tests, the `gc/collector.rs` test module, and the
one-shot S3 qualification binary; `arco-api`, `arco-compactor`, `arco-cli`
contain none. Commit refuses at 32 unconsolidated L0 segments in production
mode (`control_mvp.rs:4767-4773`). Every ack-root write is itself a control
transaction (two per materialised intent: `projection_outbox_acks.rs:275,446,491`),
so the acknowledgement domain wedges after about 16 intents, the catalog
domain after 32 commits. `drain_once` is driven only by a fail-open
`tokio::spawn` after each commit (`catalog_authority.rs:86-105`) on a
`min_instances = 0`, `cpu_idle = true` service, and by a manual operator POST.
Terraform schedules jobs for the legacy compactor and flow only. ADR-043's
projection lag objective (p99 ≤ 10 s) is neither achievable nor measurable
with shipped components. Legacy GC and repair deliberately discard control
closures and never delete under `control/` (`gc/collector.rs:706-715`,
`reconciler.rs:1925-1960`), so control/v1 orphans are never collected.

### P1-3. A lost-response writer-authority claim can be adopted by two claimers

`claim_writer_authority` builds `claimed = ControlMvpPointer { writer_epoch: epoch+1, ..pointer }`
with no claimant identity (`control_mvp.rs:570-573`). On a transport error it
reads the head back and adopts the epoch if the bytes equal `claimed_bytes`
(`:597-616`). Two writers pinning the same pointer produce byte-identical
claims; if A's CAS lands and B's single-attempt PUT is lost in transit, B's
readback matches A's bytes and B also adopts epoch N+1. Both then pass
`validate_publication_epoch` at every begin and commit; neither fences the
other. Commit avoids this because `manifest_id` embeds a nonce; the claim
does not. The existing test covers a single claimer only. Fix: add a claim
nonce or claimant id to the pointer so readback can match only the caller's
own write.

### P1-4. Cross-family `Idempotency-Key` reuse overwrites an audit record

`freeze_mutation` derives `operation_id = "op-" + sha256(key)[..32]` from the
key alone, while `receipt_key` includes the family
(`catalog_authority.rs:2575-2600`, `:3577`, `:3584`). Key K used first for
`create_schema` and then for `register_table` misses the receipt (different
family), applies the second command, and `txn.put(audit_key("op-K"))`
replaces the first audit row with no absent precondition. The projection
intent id also equals the operation id, so `ensure_outbox_id_available`
(`lazy.rs:760-774`) returns a nonsensical 409 "already exists: projection
intent" while the intent is retained, and succeeds silently once the intent
is trimmed. The legacy API path keys idempotency on operation plus hash and
would have caught this; the control path returns before that check
(`routes/catalogs.rs:373-390`).

### P1-5. Three unbounded-growth vectors meet hard caps

1. Receipts and audits: two permanent KV rows and at least ~1 KiB per
   mutation, never pruned (`catalog_authority.rs:2801-2839`; tags 3/4 have no
   delete path). Unkeyed requests still write a receipt under a ULID no
   client can present. The 1M-row / 64 MiB restore scanner
   (`state_store.rs:2651-2690`) fails the pilot at 1.2M mutations.
2. Catalog outbox: trim is refused for the catalog domain
   (`routes/control_store.rs:190-194`) and the fixed-consumer drain never
   trims, so every intent (payload = full audit record) is re-rendered into
   every L1 and reloaded by every drain and every `projection_status` query;
   ack rows are permanent and `acknowledged_event_ids` scans the whole prefix
   through a 1M-row cap that errors rather than truncates. Roughly 200K
   intents before the drain and the system table fail permanently.
3. Tombstones: `apply_tx` never removes a key; `from_replay` and
   `segment_rows_for_state` emit every tombstone into L1
   (`control_mvp.rs:5895-5908`, `:6314-6330`, `:6894-6907`; `maintenance.rs:234-241`).
   Sharding at half caps is recursive, so the binding ceiling is the durable
   maintenance job (`MAX_UNITS = 256`, `MAX_PLAN_BYTES = 8 MiB`,
   `maintenance.rs:26-28`): on the order of 8–20M distinct keys ever written
   per domain, after which no plan can be prepared and the domain is
   permanently backpressured.

The chosen remedy (authority 8) bounds per-commit work by touched pages but
does not reduce any of these row counts; restore still carries the whole log.

### P1-6. Maintenance and GC cannot keep up with the pilot workload

Maintenance is a full rewrite: `prepare` replays all L1 shards plus L0s and
streams every key into fresh shards; publication replays current head again,
reads back every output page, then CASes on a `head_version` captured before
the replay (`maintenance.rs:1281-1287`, `2141-2192`, `2497-2506`); a commit in
that window consumes one of 16 submissions and any reclamation fence is fatal
to the job (`:1295-1305`). With intent at 16 L0s and hard stop at 32, the
pilot's 25 mutation/s hour gives 0.64 s to finish a consolidation whose own
benchmark needs about 57 serial requests for a one-key state
(`docs/reports/2026-09-04-state-store-vnext-progress.md:171`). Steady state
is stop-and-go at roughly 16 commits per cycle, degrading with retained
bytes. GC re-walks the whole inventory and every sub-30-day manifest closure
per 256-candidate page without memoisation (`control_mvp.rs:3117-3270`), and
aborts the page whenever the head advances (`:2917-2926`); GC pages and
maintenance jobs mutually starve under any write load. ADR-043's own escape
hatch ("maintenance cannot remain ahead of writes → stop for a new ADR") is
triggered by the implementation as written.

### P1-7. Monitoring exists on paper only

`infra/monitoring/alerts.yaml:397-554` defines seven control-store and
projection alerts against `arco_state_store_*` series; `metrics.rs:94-100`
says none has a production emitter, and no `counter!/gauge!/histogram!` call
uses them. There is no metric for `MaintenanceBackpressure`,
`AmbiguousAuthorityOutcome`, control/v1 CAS conflicts, maintenance failure, or
projection lag as a histogram. A seven-day soak cannot be evidenced.

## P2 findings

- **No durable cutover marker.** Authority selection is in-memory env config
  (`catalog_authority.rs:340-346,397-405`; `config.rs:876-905`). Nothing in
  `writer.rs`, `sync_compactor.rs`, or `tier1_writer.rs` refuses a root that
  has a `control/v1` head; a replica missing the env vars serves stale legacy
  reads and accepts legacy DDL for the pilot root. The hard-cut doc's "no
  dual writer" relies entirely on IAM, which P1-1 shows is misconfigured.
- **Retry budget exhaustion returns 409 with no `Retry-After`**
  (`catalog_authority.rs:4129-4135`; `arco-api/src/error.rs:335`) although
  nothing conflicted; `CasFailed` from the begin-time head pin
  (`control_mvp.rs:846-850`) bypasses the loop entirely. Jitter is a
  deterministic 5–15 ms (`:4126`); each lost attempt orphans a tx, L0 and
  manifest for seven days.
- **O(workspace) reads.** Non-page `list_schemas`/`list_tables` scan all
  objects of a kind and filter in memory (`catalog_authority.rs:1451-1456,1482-1487`);
  `scan_prefix` has no aggregate cap; page lists do up to 1,000 point gets
  each re-pinning HEAD (`:1206-1216`; `control_mvp.rs:4975-4982`); `get_table`
  is five sequential pins.
- **Projection drains race and redo work.** One full-catalog Parquet snapshot
  per intent, one unserialised `tokio::spawn` per commit per replica; a lost
  ack-root CAS to an unrelated key aborts the whole pass
  (`projection_outbox_acks.rs:491-530`). Quarantined intents disappear from
  `system.catalog.projection_status` once a later success lands
  (`:1835-1837`) and are re-executed on every pass.
- **`assert_range_empty` is permanently unsatisfiable once any key in the
  range has ever existed** (`lazy.rs:856-897,1172-1178`;
  `control_mvp.rs:6033`), while the only caller's own scan ignores
  tombstones (`path_governance_metadata.rs:349-362`). Latent today because
  that module is dead code, but it is the intended Phase-6 path-overlap check.
- **Stuck retention epochs.** An unresolved maintenance-activation PUT leaves
  the workspace epoch in flight; only `ControlGc` is auto-adopted
  (`retention_coordination.rs:723-731`); recovery requires the lost job id or
  a Rust-only function with no CLI/HTTP surface; the runbook kind list omits
  `maintenance_root_publish`. Workspace snapshots force a second full L1
  render per domain and any error (including typed backpressure) wedges the
  finalize epoch (`workspace_snapshot_service.rs:1434-1441`).
- **No provisioning for the control root or cursor key** anywhere in
  `infra/`, `scripts/`, `.github/`, or Dockerfiles; the 32-byte cursor key
  has no Secret Manager resource, doc, or rotation story (rotation breaks
  every outstanding cursor; there is no dual-key window).
- **GCS conditional-write semantics unverified** for this kernel (generation
  vs ETag mapping in object_store 0.11.2); the deployed posture is GCS while
  the GA target is S3, and the scorecard records "provider-qualified: No".
- **Docs contradict code.** Runbooks describe `state-store/control-mvp/…/txlog`
  and `current.pointer.json`; `CHANGELOG.md:17` says "zero production
  callers"; `docs/spec/README.md:44-45` says "prototype-approved only" while
  ADR-043 is Accepted; ADR-043 and the scorecard call it the "v4 kernel"
  while the on-disk format is 7; Terraform comments assert a layout and
  wiring state the code left behind.
- **Fault-injection tests still partially blind** (`control_mvp.rs:8939-8975`
  discards the result and never checks the fault flag; `:11240-11274`;
  `:10805-10835`).

## P3 findings (selected)

- `request_id` may contain `%`, passing `TxnOptions::validate` but failing at
  the first artifact PUT with a storage error instead of a validation error
  (`state_store.rs:2101-2114`).
- A provably lost CAS under transport error is reported ambiguous
  (`control_mvp.rs:4896-4936`), so the API loop stops instead of retrying.
- Claim CASes on a version from a separate `head()` and discards the pinned
  version (`:555-560`).
- Missing head reads as an empty catalog on the public reader path
  (`:4974-4976`, `:5007-5009`); a misrouted or 403→None backend reports zero
  objects instead of failing closed.
- Memo-evicted re-reads are not cross-checked against the recorded witness
  (`lazy.rs:483-496`); commit validation walks all keys per observation
  (`lazy.rs:504-541`, `control_mvp.rs:6037-6050`).
- Continuation tokens have no TTL; after GC the failure is
  `NotFound{entity:"object"}`, indistinguishable from "catalog not found";
  no `TokenNotRetained` exists.
- Read-cache ledger uses bare `-=` under a poison-recovering lock with
  overflow checks off in release.
- v1 `execute` passes `Utc::now()` to `apply_command` instead of the frozen
  timestamp, so replays are not timestamp-deterministic (`catalog_authority.rs:4103`).
- "Production layout" is inferred from `checkpoint_interval == 32`
  (`control_mvp.rs:2368`, `4767`); any other value silently switches to
  inline L1 rendering and disables the backpressure gate.
- Operator refusals other than the group check emit no deny record
  (`routes/control_store.rs:185-194`); `X-Groups` grants operator authority
  in dev+debug; `AckOnlyProjectionHandler` is wired for every non-catalog
  domain and mutates the source root's sequence.
- On a control root, `system.catalog.*` tables other than `projection_status`
  are absent; the projection currently has no reader.
- `catch_unwind` drops the panic payload into a fixed string (`control_mvp.rs:7900-7914`).

## Authority 8 (bounded directory) assessment

The directory itself is carefully done: interval coverage is verified at every
replaced ancestor with canonical-layout enforcement (`directory/update.rs:63-127,320-451`),
unchanged subtrees are kept by reference, row-within-fence chains are closed
reader-side, gap and empty-range witnesses exist, and the outbox bijection is
checked for every touched record (`bounded.rs:1251-1423`). Structural capacity
is ample (2.4M rows ≈ 25K leaves at depth 3). The prepared-candidate CAS and
reconciliation path returns Committed/Superseded/Unresolved without re-CAS.

What is wrong with it is scope and readiness, not the tree:

- **P1 — Zero production reachability and no 7→8 path.** Every entry into
  bounded code is gated on `authority_format == 8`, whose only setter is the
  `test-utils` constructor (`control_mvp.rs:389,405-413`); the modules carry
  `#[allow(dead_code)]` (`:131-149`). Missing for any production root:
  conversion, restore-plan 7 and bounded restore (branch only), maintenance
  (`maintenance.rs:640,1389,2259` pin version 7), GC and reachability for
  the six new prefixes (GC rejects 8 at `:2838/2876`), a V2 projection
  worker (`projection_outbox_acks.rs` only calls V1), checkpoint/export for
  8 (rejected at `:5138,5217,5270`), and a production constructor.
- **P1 — The Step-3 restore branch is a divergent copy, not a delta.** A dry
  `git merge-tree` against origin/main reports 14 conflicts in 11 files, six
  of them add/add on whole files (`bounded.rs`, `bounded/synthetic.rs`,
  `directory/update.rs`, `logical_v2.rs`, `physical.rs`, `authority8_tests.rs`);
  the branch has zero uses of #435's `RootStorage` seam, so its 32 new files
  must be re-plumbed. This is a rewrite, and it was never CI-run on a head
  that includes #428/#431/#435.
- **P1 — Format-8 prefix scans fail above 16 leaves.** `Base::range`
  returns `MaintenanceBackpressure` whenever the directory page has a
  continuation or the boundary witnesses exceed `MAX_SELECTED_BLOCKS = 16`
  (`bounded.rs:2018-2022,2049-2053`); a continuation only moves the lower
  bound while the range still extends to the prefix end, so every page of a
  larger prefix fails. At ~97 rows per 64 KiB leaf, any prefix above ~1,600
  rows is unlistable through `list_tables_page`. The Step-2 matrix used at
  most 1,170 bootstrap commits and never hit this.
- **P1 — Unbounded immutable-object growth with no GC.** About 25 objects and
  ~300 KB per commit (segments, indexes, per-block descriptors, external
  `/keys/` fences, one page per rewritten ancestor per role, projection
  source, transition, transaction, manifest, prepared candidate); the report's
  own memory-backend inventory reached 598K–764K objects for ~30K commits,
  with 30,194 unattached prepared candidates per axis. Extrapolated to the
  pilot: ~30M objects, ~360 GB, none reclaimable until Step 4 exists.
- **P2 — The transition certificate is verified only by the writer before
  its own CAS** (`verify_candidate_transition`, `bounded.rs:994-1162`, called
  from `commit_bounded` and a test-utils export). Readers, recovery, GC and
  restore only check that the transition's new root equals the manifest root
  (`validate_manifest_artifacts` `:481-590`; `validate_prepared_candidate`
  `:591-755`), and no reader recomputes logical history from rows. Format 7
  re-derives and compares the state checksum on every retained read; format 8
  trusts the writer. The docs call this an "independent transition verifier";
  it is independent code, not an independent party.
- **P2 — Receipt keys always exceed the 64-byte inline fence** so every
  receipt-leaf fence is an external object read; the report measured 7,927
  range GETs and 12,637 directory references for one request at 2.4M rows on
  an in-memory backend. No object-store latency numbers exist.
- **P3** — genesis/conversion bijection witness is count-only
  (`synthetic.rs:395-470`); `read_checkpoint` and `projection_outbox_at` lack
  the format guard and fail closed only by accident; twelve hash-domain tags
  and roughly ten versioned encodings now coexist.

Assessment: authority 8 exists because receipts and audits are kept forever
in the authoritative KV. Receipts are read only for idempotent replay; audit
rows have no reader anywhere in `crates/` and are already handed to the
outbox as the projection payload (`catalog_authority.rs:2793-2797`). A bounded
receipt window (there is a 90-day precedent in `arco-iceberg/src/gc.rs:43`)
plus audits in an append-only projection root removes nearly all of the 2.4M
rows from authoritative state, leaving catalog objects and name indexes that
plausibly fit format 7's scanner, or at worst need a paged restore scanner.
The design rejected that in one paragraph against ~5.8K production lines on
main, ~64K on the branch, a second authority format, a new restore-plan
version, and unwritten GC, maintenance, conversion and reader-side
verification.

## Design assessment

**Root cause 1: retention policy, not physical format.** The capacity design
(`docs/plans/2026-09-12-state-store-capacity-design.md`) rejected expiring or
externalising receipts and audits on the grounds that it changes restore and
retained-read semantics and needs an atomic reachability protocol. Neither
holds: idempotency receipts are contractually a bounded window (the API layer
already has `ARCO_IDEMPOTENCY_STALE_TIMEOUT_SECS`), a prune by
`logical_sequence` is itself a CAS commit, and the audit record is already
atomically bound into the outbox, which is the natural hand-off to an
append-only archive or projection root (the vision doc's own "event archive
boundaries" that snapshots pin). The same applies to the catalog outbox
(trim after ack) and to tombstones (a horizon at max(token, checkpoint)
retention, with witness semantics adjusted). Fixing retention removes the
pilot blocker on format 7; authority 8 then becomes an optimisation rather
than a prerequisite.

**Root cause 2: the physical lifecycle was built as library code with
in-memory gate evidence, never as a deployed component.** Maintenance, GC, and
projection drain are qualified by deterministic schedules and independent
oracles, which is good, but no binary owns them, no scheduler triggers them,
no metric reports them, IAM does not permit them, and runbooks describe a
layout that no longer exists. Everything downstream of the kernel is on paper.

**Root cause 3: single-head optimistic CAS plus full-rewrite maintenance.**
One head per root, losers regenerate every artifact, and consolidation
rewrites the whole L1 every 16 commits. The critical path per commit on real
S3 is four PUTs plus a CAS; the design has never been measured on a live
provider. The escape hatch in ADR-043 exists precisely for this and should be
exercised with a measurement, not by adding format complexity.

**Root cause 4: process weight and drift.** Formats 4→5→6→7 in August and
September with no migration and a hard rollback boundary; 8 in flight with no
7→8 conversion; the Step-3 restore branch (13 commits, 57 code files, 63,922
insertions) is based on `fa87b28f`, before #428/#431/#435, and #431 is the
squash of the same bounded work; 64 micro-step plan docs on that branch; 16 GB
of untracked `evidence/` in the main checkout (not gitignored) and about 15 GB
of build outputs across 48 worktrees (4 prunable) on a disk with 2.5 GiB free;
`control_mvp.rs` is 12,103 lines with a 333-line `commit_inner` and a full
v1/v2 duplication of the execute, receipt, audit, and notifier paths where v2
is unreachable in production.

## Recommendations, in order

1. **Re-decide retention before any more authority-8 work.** Bounded
   idempotency window; audit and intent payloads to an append-only archive or
   projection root via the outbox; trim the catalog outbox after ack; define
   a tombstone horizon. Re-run the Gate-7 capacity probe on format 7.
2. **Ship the runtime the design assumes.** One scheduled, always-on worker
   (Cloud Run job now, Lambda schedule later) that runs drain, maintenance,
   and GC for every control domain; real emitters for the reserved metrics
   plus backpressure, ambiguity, and CAS-conflict counters; then the existing
   alerts become meaningful.
3. **Fix the three protocol/adapter P1s.** Claim nonce in the pointer; fold
   the family into `operation_id` or add an absent precondition on the audit
   key; derive the Terraform prefix and the xtask guard from
   `ControlMvpPaths::base_prefix()` (and decide on `control/directory/`).
4. **Measure real S3 on one root for one hour** (tens of dollars) before
   further format investment; if sustained winners per second are below the
   target, take the ADR-043 escape hatch and decide per-domain heads or
   explicit sharding rather than more directory machinery.
5. **Land or kill the restore branch; freeze formats.** Rebase onto #435 or
   abandon; implement 7→8 conversion before any further bump; add a durable
   cutover marker so the legacy writer refuses a root with a control/v1 head.
6. **Modularise the kernel.** Split `control_mvp.rs` into publication,
   restore, GC, segment codec, and head modules; separate a `synthetic`
   feature from `test-utils`; delete or activate the dead Phase-6 modules;
   remove the v2 duplication until it is reachable.
7. **Docs and hygiene.** Single format-version registry; fix runbooks,
   CHANGELOG, Terraform comments, spec headers, and the "v4 kernel" naming;
   gitignore `evidence/`; prune worktrees and build outputs; keep evidence
   archives outside the checkout.

## Refuted or not re-reported

Per the August 2026 audit memory, the following were not re-raised: CAS-loser
`PreconditionFailed` own-bytes reconciliation (provider item), "ADR-043 gates
stated nowhere", restore reverting outbox consumer binding, checkpoint
re-encode convergence and unfenced checkpoint minting. A lost control-GC
DELETE does not wedge the retention epoch (`retention_coordination.rs:568-583`).
Ambiguous outcomes are never re-executed by the adapter loop; the kernel
reconciles before declaring ambiguity.
