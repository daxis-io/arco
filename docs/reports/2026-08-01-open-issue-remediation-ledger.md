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

No post-baseline issues are included here. Add them only after comparing the
current live issue-number set with the original table above.
