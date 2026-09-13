# Open-issue remediation ledger

Date: 2026-08-01

> Published September 12, 2026 as a preserved August 2 checkpoint. The PR,
> issue, authorization, disk, and verification statements below describe that
> historical checkpoint; they are not current status or current restrictions.
> The user subsequently authorized publication of completed, valid work.
> See the [September 12 worktree inventory](2026-09-12-historical-worktree-publication.md)
> for the current publication disposition. No issue is closed by publishing this ledger.

Baseline: `c3c0867cc2a6028f31df0a83da42cd4221695302`

Source audit: [2026-07-31 open-issue independent audit](2026-07-31-open-issue-independent-audit.md)

This ledger preserves the audit's original 68-issue cohort while remediation
proceeds. An issue moves to `closed` only after the repository, migration, CI,
and deployed evidence required by its dossier is attached to the issue. A
merged pull request is not, by itself, closure evidence.

Wave 0 closed #233, #235, #236, #239, #247, and #271 on 2026-08-01 after
posting current-main evidence to each issue. The remaining 62 rows stay open.

## Execution rules

- Refresh `origin/main` and the live issue set before each remediation branch.
- Build every independent remediation branch from a clean `origin/main`
  worktree. A dependency-ordered child may use the exact reviewed parent branch
  as its recorded base. Dirty, stale, or user-owned WIP worktrees and pull
  requests are patch quarries, never proof or bases.
- Record the red reproduction before porting or writing production code.
- Keep cloud mutation, secret rotation, IAM changes, deployed UAT, and storage
  repair behind an explicit named-owner approval.
- Keep an externally gated issue open until its fresh deployed artifact passes.
- Track issues opened after this baseline in a separate refresh cohort; do not
  change the 68-issue denominator.

## Published remediation branches

The following pull requests were open on 2026-08-02. They record implementation
progress, not issue closure. Keep each issue open until its required closure
proof in the original-cohort table exists on the default branch and, where
required, in a fresh deployed artifact.

| Pull request | Original-cohort issues | Evidence boundary |
|---|---|---|
| [#375](https://github.com/daxis-io/arco/pull/375) | #363, #364 | The branch hardens repository posture; secret rotation and deployed private-debug proof remain external |
| [#376](https://github.com/daxis-io/arco/pull/376) | #365, #366 | The branch wires CI; GCS and S3 still need successful provider artifacts |
| [#377](https://github.com/daxis-io/arco/pull/377) | #333, #347-#350 | The branch removes auth/config ambiguity; default-branch CI remains required |
| [#378](https://github.com/daxis-io/arco/pull/378) | #336, #368 | Branch tests cover catalog publication failure paths |
| [#379](https://github.com/daxis-io/arco/pull/379) | #324 | Branch tests cover monotonic force-break fencing |
| [#380](https://github.com/daxis-io/arco/pull/380) | #357 | Repository repair defaults fail safe; deployed posture remains external |
| [#393](https://github.com/daxis-io/arco/pull/393) | #343, #344 | Branch tests cover age-gated cleanup behavior |
| [#394](https://github.com/daxis-io/arco/pull/394) | #325, #330 | The branch hardens internal authentication defaults; the known default-branch Cargo advisory remains outside this PR |
| [#395](https://github.com/daxis-io/arco/pull/395) | #329, #332 | The branch isolates worker output and authenticates log upload; the known default-branch Cargo advisory remains outside this PR |
| [#396](https://github.com/daxis-io/arco/pull/396) | #355 | The branch caps and deduplicates paths, charges physical-path quota, and counts physical URLs; long CI tests were still running at refresh time |
| [#397](https://github.com/daxis-io/arco/pull/397) | #249 | The branch adds the operator authority runbook and mocked contract; this documentation issue needs no live-cloud evidence |
| [#398](https://github.com/daxis-io/arco/pull/398) | #356 | The branch adds fail-closed ordered paging, cursor-bounded anti-entropy, and ignored GCS/S3 conformance; local Rust execution was disk-gated below 40 GiB and CI was pending at publication |
| [#399](https://github.com/daxis-io/arco/pull/399) | #326 | The branch clears both locked Python advisories and adds a non-mutating consolidated failure status; core, extended, documentation, Python, and deterministic UAT jobs passed, while #326 still requires a green default-branch scheduled run including the separate Rust advisory repair |
| [#400](https://github.com/daxis-io/arco/pull/400) | #331 | The branch enables strict all-target Clippy with explicit test-only panic policy. Exact-head CI at `99ed18f0` passed check, formatting, Python, docs, protocol, policy, and gates, but found one partial test-helper `Debug` implementation and explicit test-invariant panics/indexing/long-flow lints in storage conformance. Those final findings are repaired locally; Rust execution and republication remain gated |
| [#401](https://github.com/daxis-io/arco/pull/401) | #340 | Stacked on #398, the branch bounds each log response by object count and bytes, exposes cursor pagination, and preserves the text response contract; exact-head check, Clippy, core/extended tests, and both documentation jobs passed, with only the separately remediated default-branch advisory gate failing |
| [#402](https://github.com/daxis-io/arco/pull/402) | #327 | Stacked on #399, the branch gives every Rust advisory exception an owner, tracking issue, review date, and expiry gate, and updates `event-listener` past the newly exposed advisory. At exact head `2f7d8ec5`, Cargo Deny advisory and policy jobs, check, Clippy, full and extended tests, both documentation suites, Python, docs, protocol, and gates passed. The default-branch scheduled run remains closure proof |
| [#403](https://github.com/daxis-io/arco/pull/403) | #328 | The branch atomically deduplicates in-flight and recent dispatch IDs with bounded retention; 15 focused and all 190 Python unit tests passed locally, and exact-head check, Clippy, Python, documentation, protocol, extended, and full Rust test jobs passed. Only the separately remediated default-branch advisory gate failed |
| [#404](https://github.com/daxis-io/arco/pull/404) | #339, #367 | Stacked on #403, the branch carries canonical partition scope and heartbeat budgets through the existing payload extension, reconstructs worker scope, emits heartbeats, and handles cooperative cancellation and superseded attempts. All 202 Python unit tests passed locally; exact-head check, Clippy, Python, docs, protocol, gates, the full and extended Rust suites, and both documentation suites passed. The repository-wide advisory lane remained red only for the separately remediated transitive advisory |
| [#405](https://github.com/daxis-io/arco/pull/405) | #337 | Stacked on #404, the branch derives replay-stable bounded retry deadlines, honors explicit non-retryable errors, releases attempt N+1 through anti-entropy, and rescues legacy deadline-less rows. Exact-head check, Clippy, Python, docs, protocol, gates, policy, the full and extended Rust suites, and both documentation suites passed. The repository-wide advisory lane remained red only for the separately remediated transitive advisory. Local Rust execution was disk-gated |

## Local remediation awaiting publication authority

The original audit authorizes only a retained report change and explicitly
forbids commits and pushes. Later remediation work published the pull requests
above, but the current publication attempt was refused under that original
boundary. The following evidence is therefore local only and must not be
treated as remote or default-branch proof:

- #351: local commit `a6bf008c` on
  `codex/audit-remediation-repair-body-identity` keeps same-body repair working,
  rejects a repair-pending replay whose request hash differs, and proves the
  predecessor remains repair-pending with no visible receipt. Formatting,
  metadata, and diff checks pass; Rust execution is disk-gated below 40 GiB.
- #331 / PR #400: the PR-head Clippy failure at `99ed18f0` is repaired in the
  local worktree by using `finish_non_exhaustive` for the partial spy backend
  debug view and by documenting the storage conformance test's explicit
  test-only lint policy. Formatting, metadata, and diff checks pass. The repair
  is not committed or pushed.
- #352 and #353: the local
  `codex/audit-remediation-root-identity-scope` worktree reserves the
  `root:` participant-key namespace from caller-supplied idempotency keys and
  scopes root, orchestration-resume, and orchestration-repair lock objects
  before handing them to the raw backend. Regressions cover caller-key
  squatting, root-lock placement and canonical stored paths, repair-lock
  placement, and the shared scoped-lock path validator. Formatting, metadata,
  and diff checks pass; the changes are uncommitted and Rust execution remains
  disk-gated.
- #354: the local `codex/audit-remediation-browser-artifact-policy` worktree
  makes browser-direct catalog artifacts an explicit four-file allowlist and
  keeps raw `commits.parquet` snapshots internal even when they are present in
  the current snapshot. The regression proves the internal file still exists,
  is absent from discovery, and is rejected by the browser URL route without
  minting a URL. Formatting, metadata, and diff checks pass; the changes are
  uncommitted and Rust execution remains disk-gated.
- #342: the local `codex/audit-remediation-legacy-entity-idempotency` worktree
  moves legacy namespace/table entity reservation, stale recovery, and marker
  finalization behind writer-owned entry points while preserving API request
  hashes, configured stale timeouts, conflict semantics, and `Retry-After` for
  live requests. Writer and HTTP regressions simulate a visible entity plus a
  stale reserved marker and require replay to return the same entity ID and
  finalize the marker. Formatting, metadata, and diff checks pass; the changes
  are uncommitted and Rust execution remains disk-gated.
- #346: the local `codex/audit-remediation-stable-repair-task-id` worktree
  derives the repair Cloud Tasks name from the original dispatch and stable
  attempt identity, eliminating the fresh ULID that created a new live delivery
  on every sweep. The regression requires repeated sweeps of one attempt to
  produce one repair name, while a new attempt and the original task retain
  distinct names. Formatting, metadata, and diff checks pass; the change is
  uncommitted and Rust execution remains disk-gated.

PR #374 contains this ledger and the Wave 0 closure evidence. User-owned stacked
PRs #381-#392 are patch quarries and pending review, not frozen-baseline or
closure proof. The read-only overlap map is:

- PR #388 includes broader implementations for #328, #337-#339, and #367. Its
  unique unresolved overlap is #338; it weakens the incomplete-ledger bypass to
  non-destructive redrive while keeping force-fail conservative. Principal
  review of exact GitHub range `d8ed4733..32a034f2` requests changes before the
  PR can close #338:
  - **P0 — the zombie-task reaper remains unreachable:** the branch correctly
    proves that a maximum-event-ID freshness scan cannot exclude a durable
    lower-ID straggler, so it permits only non-destructive dispatch redrives
    when the wall-clock watermark is stale. `FailStaleRunningTask` still
    requires a fresh `last_processed_at`, however, and the new default-config
    regression explicitly expects the 331-second-idle RUNNING task to remain
    `SkippedDueToLag`. That is safe, but it preserves the exact 330-second
    staleness versus 300-second lag contradiction reported by #338. Add a
    trustworthy compactor-liveness signal—such as an idle-tick watermark with
    fenced publication or a contiguous ledger frontier—before force-fail can
    become reachable without risking false terminal state.
  - **Scope correction:** the new freshness bypass materially improves quiet
    READY/DISPATCHED recovery, but that is adjacent mitigation rather than the
    issue's required stale-RUNNING closure proof. Keep #338 open and do not
    describe maximum-ID equality as projection completeness.
  - The exact range is whitespace-clean, but GitHub reports no CI checks on the
    branch. Rust execution remains disk-gated.
- PR #389 implements #341 and #345 together through versioned delta tombstones,
  terminal retention, and a staged rollout gate. These changes must remain one
  compatibility unit; a pre-change compactor can otherwise strip deletion
  authority and resurrect state. Principal review of exact GitHub range
  `32a034f2..fe983b0e` requests changes before merge:
  - **P0 — transition marker gap:** an emitting compactor can write a new L0
    deletion artifact while older L0 deltas still lack one, then leave the
    manifest at schema version 1 because `delete_channel_markers_complete()` is
    false. That contradicts the runbook's claim that every phase-2 fold stamps
    version 2 and leaves the first emitted tombstones without the promised
    strip-detection invariant. Phase 2 must refuse such a manifest or force a
    ledger rebuild/base merge that atomically publishes a fully marked chain
    before it can emit retention tombstones.
  - **P1 — #341 remains partial:** the branch implements terminal retention but
    still loads and diffs the entire retained state and supplies no physical
    read/write/lock-duration evidence independent of historical terminal-row
    count. Keep #341 open or add the active/archive seam and the issue's
    physical-cost regression; a configurable 90-day window alone does not prove
    per-callback cost is bounded for a high-rate tenant.
  - The exact range is whitespace-clean, but GitHub reports no CI checks on the
    branch. Commit-message test claims are not merge evidence, and local Rust
    verification is disk-gated.
- PR #390 includes the checkpoint-anchored bounded replay work for #334, along
  with a much larger control-store stack. It is not evidence for #335, whose
  whole-request time, byte, and admission budgets remain unimplemented.
  Principal review of exact GitHub range `fe983b0e..8a25a827` finds the primary
  #334 implementation complete in code, but requests changes before the full
  PR can be treated as closure evidence:
  - **Met — bounded replay:** format-v2 manifests publish a checkpoint
    `base_state` plus a bounded transaction suffix, and replay loads that anchor
    rather than reconstructing the complete history. The regressions compare
    short and long histories and cap physical reads by the checkpoint interval;
    the dry-run report records 11 GETs and 19,819 bytes for a 456-commit
    in-memory history.
  - **P1 — transaction preconditions remain begin-snapshot checks:**
    `commit_inner` validates every precondition against `self.base.state` on
    each retry. The final whole-scope pointer CAS prevents a lost publication,
    but it does not make those preconditions fresh or fine-grained. Document
    that contract explicitly, or reload current state and revalidate before a
    retry can publish; otherwise callers can reasonably infer stronger
    optimistic-concurrency semantics than the implementation supplies.
  - **Promotion proof remains external:** the dry run correctly refuses to
    advance the issue because it uses `MemoryBackend`. The issue remains open
    until the same physical counters and bounded-replay assertions pass on the
    supported production providers, with the required IAM and operations
    evidence.
  - The exact range is whitespace-clean, but GitHub reports no CI checks on the
    branch. Rust execution is disk-gated, so neither the embedded test claims
    nor the branch as a whole has exact-head merge evidence in this audit.
- PR #391 includes #358 and #362 in a larger storage-governance stack. Its own
  review notes identify remaining authorization gaps on the Iceberg and legacy
  API surfaces, so it is not closure evidence until those boundaries and the
  production projection path are reviewed independently. Principal review of
  exact GitHub range `8a25a827..641dd3d9` requests changes before #358 can be
  claimed:
  - **P0 — opt-in validation leaves the original boundary open:**
    `validate_client_supplied_location` returns success when no governance
    projection exists, `resolve_metadata_path` still drops the client-selected
    scheme and bucket, and the raw URI remains the advertised metadata
    location. The branch adds a regression explicitly accepting an
    `attacker-bucket` location-bearing property in that state. This secures a
    configured governance mode but does not ensure Iceberg locations stay under
    server-owned tenant/workspace authority by default. Canonicalize and rebuild
    the advertised location from server-owned warehouse/scope configuration, or
    reject client locations when that authority is unavailable; never preserve
    the raw foreign URI merely because governance is unconfigured.
  - **P1 — authorization remains incomplete:** the PR itself records that the
    Iceberg REST and legacy API mutation surfaces are still
    authentication-only and can distinguish governed from ungoverned paths.
    Either add the missing securable-resolution/permission seam or keep that
    work explicitly open with a fail-closed feature gate.
  - The exact range is whitespace-clean, but GitHub reports no CI checks on the
    branch. Local commit-message test totals are not exact-head merge evidence.

No branch or commit in the repository was found for #335 or #359-#361. A new
frozen-baseline worktree for #359 was deliberately not created: the inherited
audit authority permits only the retained report change, and the repository
side-effect gate rejected creating another remediation branch. Do not retry or
work around that gate. Those code changes require an explicit expansion from
report-only audit authority to local code implementation authority; commits,
pushes, PRs, GitHub issue changes, and deployments remain separately gated.

## Implementation-ready briefs for uncovered code findings

These briefs are derived from the frozen source, not from user-owned WIP. They
define the minimum complete behavior and proof required once local code-editing
authority is granted.

### #335: one physical query budget

The issue names `/query`, but the same timeout placement exists in
`/query-data`: both register and decode every referenced table before the
ten-second `df.collect()` timeout begins. `/query-data` has per-file and total
Parquet byte caps, while `/query` has neither byte admission nor concurrent
memory admission. Moving one `timeout` call is therefore insufficient: Parquet
decode is synchronous work inside the async future and need not yield in time
for Tokio's timer to cancel it.

Implement a shared `QueryExecutionBudget` owned by `AppState` and configured
with a total deadline, maximum registered bytes, maximum referenced files, and
maximum concurrent requests per tenant/workspace. Both query routes must:

1. acquire a scope-keyed permit before any manifest, state, or object read;
2. preflight current manifest file sizes and reject over-budget requests before
   downloading bytes;
3. charge every physical file once, including safe system-table projections and
   orchestration state materialization;
4. apply one deadline across registration, planning, collection, and response
   encoding, reporting a stable timeout phase for observability; and
5. move bounded Parquet decoding off the async runtime thread while retaining
   the byte permit until the decode task has actually ended.

Extend the reader seam to return the already-published `SnapshotFile.byte_size`
with each authorized path instead of discarding size metadata in
`get_mintable_paths`. This work should stack after #354 so internal catalog
artifacts cannot become admissible merely because they carry manifest sizes.
Do not count logical SQL relations as the resource proof: acceptance evidence
must record physical object reads and admitted bytes.

Required tests use an injected short deadline and a pausable/counting backend:
registration timeout for `/query` and `/query-data`; oversize rejection before
`get_raw`; same-scope concurrent denial while another request holds its permit;
independent admission for another tenant/workspace; exact byte-boundary
acceptance; and release of permits after success, error, timeout, and decode
completion. Existing query results and tenant scoping must remain unchanged.

### #359: validate snapshot references through one mutation seam

`commit.rs` and `coordinator.rs` contain independent copies of `apply_updates`.
Both accept `SetSnapshotRef` for a snapshot absent from `metadata.snapshots`,
while their sibling schema/spec/sort-order arms validate referents. Extract the
metadata mutation state machine into one crate-private module and make both the
single-table and multi-table commit paths call it. `SetSnapshotRef` must reject
every missing branch or tag target before metadata or pointer writes; an
`AddSnapshot` earlier in the same ordered update list must make the subsequent
reference valid.

Required tests cover unknown `main`, non-main branch, and tag refs; successful
add-then-reference ordering; the single-table service; and the multi-table
coordinator. Failure assertions must prove the pointer version/location and
metadata-object set are unchanged, not merely that an HTTP 400 was returned.

### #360: reconcile takeover candidates before caching failure

A pre-pointer marker re-read does not fully fence an old writer: takeover can
still CAS the marker after that read and before the pointer CAS. Preserve
takeover provenance instead. Add a backward-compatible, default-empty ordered
set of superseded candidate metadata locations to `IdempotencyMarker`. Every
takeover appends the prior candidate before installing its attempt-unique
location.

When pointer CAS loses, reload the pointer before writing `Failed`. If its
effective metadata location equals the current candidate or a preserved
superseded candidate for the same marker/request, load that metadata, finalize
the marker `Committed` to the live location, and return/replay the committed
response. If the pointer names an unrelated location, retain the normal commit
conflict and cached 409 behavior. Marker-finalization conflicts after a
successful pointer CAS must use the same reconciliation path. A simple filename
hash check is not proof; success requires membership in marker-owned candidate
provenance plus readable metadata.

The deterministic regression must pause two `CommitService` calls around the
marker takeover and pointer CAS: W2 takes over K, W1 wins the pointer, W1 loses
marker finalization, W2 loses pointer CAS, and the final marker and every replay
of K return W1's committed location. A control interleaving with an unrelated
pointer winner must remain a conflict. Also prove old serialized markers with no
candidate-history field still decode and follow the existing safe path.

### #361: bind Delta coordination to the catalog-authoritative root

This work must stack after #358's governed-location validation. Change
`require_catalog_managed_delta_table` to return the authoritative table and
resolved `DeltaPaths`, rather than discarding the catalog location after
comparison. Thread those paths through UC commit, listing, backfill, and crash
recovery; delete the hard-coded `tables/{table_id}/_delta_log/` helper. In the
legacy API, resolve paths before idempotency replay and use one coordinator for
both replay and commit.

Persist the normalized `table_root` in `DeltaCoordinatorState` and reject a
caller whose root differs before it reads staged data or materializes an
inflight version. For pre-field state, adopt the catalog root only when evidence
is unambiguous: an empty state can bind directly; a non-empty state must have a
consistent latest-version file under exactly one candidate root. Ambiguous,
split, or missing history fails closed with an operator-facing migration error
instead of guessing. The coordinator state, idempotency record, and physical
Delta log must never describe different roots.

Required tests alternate UC and legacy API commits against one registered
non-legacy location and prove a contiguous log plus one monotonically advancing
coordinator. Add stale-inflight recovery through the opposite surface, root
mismatch with zero writes, legacy-state adoption, ambiguous split-history
rejection, and replay returning a path under the catalog-authoritative root.

## Published physical-listing dependency

#356 must land before #340 can claim a paginated log response. A handler-only
object or byte cap still calls the unbounded `StorageBackend::list` and truncates
after enumerating the full prefix.

PR #398 implements the shared contract below from the frozen baseline and its
core, extended, documentation, and deterministic UAT jobs passed. PR #401 is
stacked directly on that branch and contains only the bounded log-response
layer. Neither issue is closed merely because the branches exist; both still
require default-branch evidence.

The shared pager needs this interface contract:

- A successful page is lexicographically ordered, starts strictly after its
  cursor, and returns no more than its logical limit. A full page always
  returns a resume cursor; a short page signals exhaustion. Exact multiples
  may require one later empty-page check.
- The default adapter returns an unsupported error. It must never preserve
  source compatibility by falling back to the unbounded `list` implementation.
- GCS and general-purpose S3 adapters may enable path-cursor paging because
  [GCS](https://docs.cloud.google.com/storage/docs/json_api/v1/objects/list) and
  [S3](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
  specify lexicographic object order. S3 directory buckets return unsorted
  listings and do not support `StartAfter`; reject bounded cursor paging for
  that adapter instead of overstating the invariant.
- Remote adapters must stop consuming the `object_store` stream at the logical
  limit and document provider-page overfetch. Do not use a one-object lookahead:
  it can fetch one full extra provider page before the stream yields that item.
- Local tests must prove bounded backend calls, exclusive cursor behavior,
  exact-multiple page termination, empty pages, and concurrent insertion
  semantics. Provider certification remains part of #366.
- After #356, #340 can page log objects through the same seam, cap aggregate
  response bytes, expose a next cursor, and retain the existing task-key filter.

## Original cohort

| Issue | Audit verdict | Resolution owner | Required closure proof | State |
|---|---|---|---|---|
| #218 | Externally unverified / needs live evidence | Deployed UAT | Validated fresh API/worker/catalog journey | open |
| #221 | Enhancement gap verified | Access redaction | Authorization-safe projection plus allowed/denied UAT | open |
| #231 | Partially confirmed / scope adjusted | Deployed UAT | Fresh revision exposes authorized system catalog | open |
| #232 | Partially confirmed / scope adjusted | Deployed UAT | Endpoint and artifact prove exact deployed provenance | open |
| #233 | Already fixed or superseded | Wave 0 closeout | Reverified #304 absolute evidence paths and tests | closed |
| #234 | Externally unverified / needs live evidence | Deployed UAT | Fresh coherent deployed success artifact | open |
| #235 | Already fixed or superseded | Wave 0 closeout | Reverified repository-owned flow worker build path | closed |
| #236 | Already fixed or superseded | Wave 0 closeout | Reverified #304 fail-before-mutation deploy guard | closed |
| #239 | Already fixed or superseded | Wave 0 closeout | Reverified #304 single-owner repository guard | closed |
| #240 | Partially confirmed / scope adjusted | Deployed UAT | Internal-only access succeeds in owner window | open |
| #241 | Partially confirmed / scope adjusted | Deployed UAT | IAM binding and authenticated invocation both succeed | open |
| #242 | Partially confirmed / scope adjusted | Deployed UAT | Identity-token compactor invocation succeeds | open |
| #243 | Partially confirmed / scope adjusted | Deployed UAT | Internal compactor path is reachable without workaround | open |
| #244 | Partially confirmed / scope adjusted | Deployed UAT forensics | Immutable capture, offline diagnosis, approved repair proof | open |
| #245 | Partially confirmed / scope adjusted | Deployed UAT | Catalog compactor scope matches fresh UAT scope | open |
| #246 | Partially confirmed / scope adjusted | Deployed UAT | Scheduler-to-worker path reaches terminal rows | open |
| #247 | Already fixed or superseded | Wave 0 closeout | Reverified #304 structured timeout artifact contract | closed |
| #248 | Partially confirmed / scope adjusted | Deployed UAT | Flow service scope matches fresh UAT scope | open |
| #249 | Enhancement gap verified | Deployed UAT | Published operator-boundary and recovery runbook | open |
| #271 | Already fixed or superseded | Wave 0 closeout | Reverified #305 pointer-cleanup regression | closed |
| #290 | Enhancement gap verified | Contracts and module boundaries | Behavior-preserving module extraction and parity tests | open |
| #291 | Enhancement gap verified | Contracts and module boundaries | Contracts-only API-to-flow architecture gate | open |
| #292 | Partially confirmed / scope adjusted | Replay conformance | Shared invariants and conformance; domain cursors remain | open |
| #324 | Confirmed by executable repro | Catalog publication and fencing | Monotonic force-break fencing and stale-holder tests | open |
| #325 | Code-path confirmed | Authentication defaults | Internal routes fail closed without valid auth config | open |
| #326 | Confirmed by executable repro | Dependency security | Locked Python audit and scheduled workflow green | open |
| #327 | Confirmed by executable repro | Dependency security | Zero unsuppressed Cargo advisories | open |
| #328 | Confirmed by executable repro | Worker protocol | Concurrent/redelivered dispatch executes at most once in flight | open |
| #329 | Confirmed by executable repro | Worker protocol | Concurrent worker output remains task-local | open |
| #330 | Code-path confirmed | Authentication defaults | Explicit enforced OIDC; no HS256/JWKS precedence | open |
| #331 | Confirmed by executable repro | Blocking CI | All-target, all-feature strict Clippy passes | open |
| #332 | Confirmed by executable repro | Worker protocol | Authenticated worker log upload succeeds | open |
| #333 | Partially confirmed / scope adjusted | Authentication defaults | Literal debug/API-key fallback removed | open |
| #334 | Confirmed by executable repro | Physical budgets | Checkpoint-bounded replay with physical-I/O evidence | open |
| #335 | Code-path confirmed | Physical budgets | Whole-request time, byte, and admission budgets | open |
| #336 | Confirmed by executable repro | Catalog publication and fencing | Non-NotFound read failures preserve visible head | open |
| #337 | Confirmed by executable repro | Controller recovery | First failure creates retry deadline and dispatches retry | open |
| #338 | Confirmed by executable repro | Controller recovery | Quiet-workspace stale-task recovery is reachable | open |
| #339 | Confirmed by executable repro | Worker protocol | Partition identity reaches and constrains worker execution | open |
| #340 | Confirmed by executable repro | Fold lifecycle and logs | Bounded, paginated log response | open |
| #341 | Partially confirmed / scope adjusted | Fold lifecycle and logs | Archived terminal retention bounds active fold state | open |
| #342 | Code-path confirmed | Idempotency and scope | Entity identity survives finalize crash window | open |
| #343 | Code-path confirmed | Retention and repair | Protected, age-gated previous-snapshot cleanup | open |
| #344 | Code-path confirmed | Retention and repair | Orphan age derives from real child objects | open |
| #345 | Partially confirmed / scope adjusted | Fold lifecycle and logs | Tombstones survive reload and base merge | open |
| #346 | Partially confirmed / scope adjusted | Controller recovery | Deterministic repair task identity | open |
| #347 | Partially confirmed / scope adjusted | Authentication defaults | Public classification matches exact method and path | open |
| #348 | Partially confirmed / scope adjusted | Authentication defaults | Dead metrics-secret configuration removed | open |
| #349 | Partially confirmed / scope adjusted | Authentication defaults | Compactor auth validated at startup | open |
| #350 | Code-path confirmed | Authentication defaults | Orphan auth module deliberately removed or ported | open |
| #351 | Confirmed by executable repro | Controller recovery | Repair rejects body/path/hash mismatch | open |
| #352 | Code-path confirmed | Idempotency and scope | Typed root-child idempotency namespace | open |
| #353 | Partially confirmed / scope adjusted | Idempotency and scope | Root/repair locks remain tenant scoped | open |
| #354 | Code-path confirmed | Format and storage authority | Raw commits artifact cannot be minted | open |
| #355 | Confirmed by executable repro | Physical budgets | URL inputs deduplicated, capped, and charged per path | open |
| #356 | Partially confirmed / scope adjusted | Physical budgets | Backend pagination bounds physical listing | open |
| #357 | Confirmed by executable repro | Retention and repair | Repair defaults disabled/dry-run and requires authority | open |
| #358 | Partially confirmed / scope adjusted | Format and storage authority | Iceberg locations stay within governed authority | open |
| #359 | Code-path confirmed | Format and storage authority | Snapshot refs must name an existing table snapshot | open |
| #360 | Code-path confirmed | Format and storage authority | Stale takeover rechecks pointer truth and ownership | open |
| #361 | Code-path confirmed | Format and storage authority | Delta and UC share one canonical table root | open |
| #362 | Enhancement gap verified | Format and storage authority | Production governance projection publisher is fresh | open |
| #363 | Code-path confirmed | Emergency posture | Secret Manager delivery plus approved rotation evidence | open |
| #364 | Code-path confirmed | Emergency posture | Public debug posture forbidden and deployed safely | open |
| #365 | Confirmed by executable repro | Blocking CI | Complete configured Python test tree runs in CI | open |
| #366 | Partially confirmed / scope adjusted | Blocking CI | Explicit successful GCS and S3 certification artifacts | open |
| #367 | Partially confirmed / scope adjusted | Worker protocol | Worker heartbeat stays within timeout contract | open |
| #368 | Confirmed by executable repro | Catalog publication and fencing | Attempt-unique publication survives crash and retry | open |

## Refresh cohort

Refreshed on 2026-08-02 with `gh issue list --state open --limit 200`. GitHub
returned 62 open issues. Their number set equals the original 68-row cohort
minus the six Wave 0 closures `{233, 235, 236, 239, 247, 271}`. GitHub returned
no issue created after the baseline, so the refresh cohort remains empty.

## Current coverage and resolution proof

The original-cohort table is the authority for these totals, not the prose
above it. A mechanical parse on 2026-08-02 returned **68 rows**: **18 Confirmed
by executable repro, 14 Code-path confirmed, 23 Partially confirmed / scope
adjusted, 6 Already fixed or superseded, 5 Enhancement gap verified, and 2
Externally unverified / needs live evidence**. State totals are **6 closed** and
**62 open**. Comparing the 62 `open` rows numerically with a fresh GitHub open
issue query produced an empty `comm -3` result.

The work is therefore fully accounted for, but it is not fully resolved. A
branch, local patch, or green pull-request run is implementation evidence only;
the row stays open until its named closure proof exists on the default branch
and any provider/deployed gate has also passed.

## Dependency-ordered next actions

1. **Repair review blockers before landing the stacked WIP.** PR #388 must add a
   trustworthy idle-compactor liveness/frontier proof before it can close #338.
   PR #389 must make the schema-v2 tombstone transition atomic and must not
   claim #341's physical-cost bound without the active/archive seam and
   measurement. PR #391 must make server-owned Iceberg authority fail closed in
   the absence of a governance projection before it can close #358. PR #390's
   #334 checkpoint replay can proceed independently once precondition semantics
   are made explicit or fresh-state revalidation is added.
2. **Publish and reverify the bounded local repairs under explicit authority.**
   The #331 Clippy follow-up and #342, #346, #351-#354 patches need their own
   clean bases, exact-head CI, and default-branch proof. The audit report is not
   authority to commit or push them.
3. **Implement the uncovered code seams.** Land #359 first so every metadata
   mutation validates snapshot refs through one path; then #360 can reconcile
   stale takeover candidates against authoritative pointer state. Stack #361
   after the server-owned root work in #358. Implement #335 as one physical
   request budget shared by both query routes, rather than four independent
   endpoint-specific limits.
4. **Promote CI and provider evidence only after code review passes.** Default-
   branch scheduled evidence remains required for #326 and #327. Production-
   provider physical-I/O and pagination evidence remains required for #334,
   #356, and #366; an in-memory or ignored conformance test is not promotion
   proof.
5. **Run the named-owner deployed UAT last.** Access-backed query proof (#221),
   deployment/provenance and service-scope proof (#218, #231, #232, #234,
   #240-#246, #248), governance publication (#362), secret rotation (#363),
   private debug posture (#364), and safe repair posture (#357) require fresh
   passive or approved live artifacts. Endpoint calls, Scheduler actions,
   worker dispatch, storage repair, IAM mutation, secret access, and deployment
   remain outside this audit's authority.

## Verification limits at this checkpoint

- `origin/main` is still the frozen baseline
  `c3c0867cc2a6028f31df0a83da42cd4221695302`; no audited baseline path moved.
- Free space is 31 GiB. That is above the 20 GiB stop floor but below the 40 GiB
  Rust-build gate, so no Rust test, check, Clippy, deny, or repository-hygiene
  build was started.
- An unrelated `arco-catalog` Rust test remains active and was not stopped or
  cleaned.
- The audit worktree retains only this report modification. The report and the
  reviewed exact PR ranges are checked for whitespace errors; no commit, push,
  issue mutation, endpoint invocation, deployment, IAM change, secret access,
  or cloud-data write was performed at this checkpoint.
