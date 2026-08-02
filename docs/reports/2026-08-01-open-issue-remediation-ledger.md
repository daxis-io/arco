# Open-issue remediation ledger

Date: 2026-08-01

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
- Build every remediation branch from a clean `origin/main` worktree. Dirty or
  stale worktrees and pull requests are patch quarries, never proof or bases.
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

PR #374 contains this ledger and the Wave 0 closure evidence. User-owned stacked
PRs #381-#392 overlap several other rows; treat them as patch quarries and
pending review, not as frozen-baseline or closure proof.

## Queued physical-listing dependency

#356 must land before #340 can claim a paginated log response. A handler-only
object or byte cap still calls the unbounded `StorageBackend::list` and truncates
after enumerating the full prefix.

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
