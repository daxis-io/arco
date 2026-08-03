# Open-Issue Independent Audit — 2026-07-31

## Scope and epistemic status

This report audits every issue that was open in `daxis-io/arco` at the initial
snapshot on 2026-07-31, excluding pull requests. The issue body, comments, labels,
and suggested fix are treated as hypotheses. A verdict is based on independent
inspection of `origin/main`, executable tests or bounded repro harnesses where
safe, repository history, active pull requests, and read-only deployed-state
evidence where credentials permit.

The frozen local baseline is
`c3c0867cc2a6028f31df0a83da42cd4221695302` (`Add workspace snapshot export,
restore, and durable transaction handles (#322)`, authored 2026-07-26). The audit
worktree is detached at that commit. The divergent dirty root and all pre-existing
worktrees were left untouched. Findings describe that baseline unless a dossier
explicitly records a later refresh commit.

No production code, API, schema, type, runtime behavior, infrastructure, IAM,
cloud data, GitHub issue/label/comment state, commit, branch, push, or deployment
is changed by this audit. This report is the only retained repository change.

## Method and verdict vocabulary

For each issue the audit extracts a falsifiable claim and prerequisites, locates
the implementation independently from repository concepts and tests, attempts a
safe reproduction, evaluates redundancy/history/WIP, and only then compares the
result with the reporter's analysis. Destructive paths use temporary storage or
code-path proof; deployed claims use read-only inspection only.

Verdicts used in this report:

- **Confirmed by executable repro**
- **Code-path confirmed**
- **Partially confirmed / scope adjusted**
- **Refuted**
- **Already fixed or superseded**
- **Duplicate**
- **Enhancement gap verified**
- **Externally unverified / needs live evidence**
- **Insufficient information**

## Baseline inventory

- Initial open-issue set: **68**, exactly `#324`–`#368` plus `#218`, `#221`,
  `#231`–`#236`, `#239`–`#249`, `#271`, and `#290`–`#292`.
- Initial open pull requests: **8** — `#300`, `#309`, `#311`–`#315`, and `#323`.
  None declares an issue-closing reference in its body. PRs are context, not audit
  subjects.
- Initial issue snapshot: 68 bodies, 68 timelines, 156 comments across 22 issues,
  and 112 cross-reference events. The temporary snapshot is outside the worktree.
- CI at the baseline commit: the 2026-07-26 `CI`, `CI Extended`, and CodeQL push
  runs passed. The scheduled `ADR-034 GCS Conformance` run passed on 2026-07-31;
  the scheduled `Security Audit` run failed on 2026-07-31 and on preceding days.
- Local baseline gates: `cargo xtask repo-hygiene-check` and `cargo xtask
  adr-check` both passed from the detached audit worktree.
- Toolchain: Rust/Cargo 1.88.0, Python 3.14.5, uv 0.9.7, Terraform 1.12.2,
  gcloud 561.0.0, GitHub CLI 2.96.0, jq 1.7.1, Git 2.50.1, macOS arm64.
- Disk gate: 61 GiB was available before worktree creation and about 58 GiB
  after creating the dedicated `/tmp/arco-open-issue-audit-target`, satisfying
  the 40 GiB creation floor. Rust compilation later consumed about 15.3 GiB;
  that audit-owned target was cleaned immediately. On 2026-08-01 free space
  nevertheless fell to 17 GiB, below the mandatory 20 GiB stop floor, so all
  build/test and further audit activity stopped. A process-aware cleanup then
  removed only inactive Git-ignored Rust targets in retained Arco worktrees and
  stale temporary build targets. Active builds, source, Git state, and whole
  worktrees were preserved. Free space reached 135 GiB and the audit resumed.

## Executive summary

The audit covers **68 of 68** issues. Matrix-derived verdict totals are **18
Confirmed by executable repro, 14 Code-path confirmed, 23 Partially confirmed /
scope adjusted, 6 Already fixed or superseded, 5 Enhancement gap verified, and
2 Externally unverified / needs live evidence**. There were no refuted,
duplicate, or insufficient-information verdicts. “Partial” is not shorthand for
“probably true”: it records a proven mechanism with a narrower consequence,
intentional architectural difference, or missing live/deployed precondition.

The most urgent proven findings are #368's deterministic
catalog-compaction wedge, #336's silent catalog-loss path, #324's fencing reset,
and #364's mismatch between the deployed dev API's debug posture and public
invocation metadata.
Security severity was reduced for #333, #347, #348, and #358 because current
guards or deployment defaults narrow the reported consequence; #364 was raised
because passive deployed metadata contradicted the issue's safety caveat.

Work stopped when free disk fell to **17 GiB**, below the plan's mandatory
20 GiB floor. After scoped cleanup restored **135 GiB**, work resumed. At the
next-session gate 55 GiB was available and no active Rust build was found, so
the 40 GiB build threshold was initially satisfied. During final passive work,
free space fell to 22 GiB and an unrelated Rust test began building in the
retained `control-store-idempotency-grants-ddl-pilots` worktree. It was not
stopped or cleaned; no new audit Rust target or Rust-backed final hygiene run
was started.

Final refresh on 2026-08-01 found `origin/main` still exactly
`c3c0867cc2a6028f31df0a83da42cd4221695302`; no audited path changed and no
reproduction required rerunning. The live open-issue list still contains exactly
the frozen 68-number set: missing `[]`, additions `[]`. Therefore there is no
closure cohort and no newly opened refresh cohort. The open-PR set changed to
`#300`, `#309`, `#315`, `#323`, and `#369`-`#372`; these remain WIP context only.
Notably, #372 removes locked `click 8.3.1` and upgrades
`pydantic-settings` to 2.14.2, directly overlapping #326, but it is not merged
and its locked audit has not replaced the baseline repro.

## Coverage matrix

| Issue | Batch | Initial severity/category | Verdict | Recommended disposition |
|---:|---|---|---|---|
| #368 | Critical/high | critical bug/catalog | Confirmed by executable repro | Keep open; fix first |
| #326 | Critical/high | high dependencies | Confirmed by executable repro | Keep open; merge dependency repair |
| #328 | Critical/high | high reliability | Confirmed by executable repro | Keep open |
| #336 | Critical/high | high bug/catalog | Confirmed by executable repro | Keep open; critical boundary |
| #337 | Critical/high | high bug/orchestration | Confirmed by executable repro | Keep open; correct #250 closure |
| #338 | Critical/high | high reliability | Confirmed by executable repro | Keep open |
| #339 | Critical/high | high bug/partitioning | Confirmed by executable repro | Keep open |
| #340 | Critical/high | high reliability | Confirmed by executable repro | Keep open |
| #341 | Critical/high | high performance | Partially confirmed / scope adjusted | Keep open; measure terminal-only fixture |
| #325 | Security | medium security | Code-path confirmed | Keep open; fail closed when config is absent |
| #327 | Security | medium security | Confirmed by executable repro | Keep open or record accountable exceptions |
| #330 | Security | medium security | Code-path confirmed | Keep open; enforce verified OIDC |
| #333 | Security | low security | Partially confirmed / scope adjusted | Re-scope to unreachable fallback hardening |
| #347 | Security | low security | Partially confirmed / scope adjusted | Re-scope to exact-path classification hardening |
| #348 | Security | low security | Partially confirmed / scope adjusted | Close security claim; track dead config separately |
| #349 | Security | low reliability/auth | Partially confirmed / scope adjusted | Merge deployment-auth scope into #242; retain startup validation gap |
| #352 | Security | low security | Code-path confirmed | Keep open at low severity |
| #354 | Security | low security | Code-path confirmed | Keep open; define raw-artifact policy |
| #358 | Security | medium security | Partially confirmed / scope adjusted | Keep open as latent boundary; lower severity pending live evidence |
| #363 | Security | medium security | Code-path confirmed | Keep open; migrate secret delivery and rotate out of band |
| #364 | Security | medium security | Code-path confirmed | Keep open; urgent deployed-posture review, raise severity |
| #271 | Catalog/storage | unlabeled bug | Already fixed or superseded | Close as fixed by #305 |
| #324 | Catalog/storage | medium reliability | Confirmed by executable repro | Keep open; raise severity |
| #342 | Catalog/storage | medium reliability | Code-path confirmed | Keep open |
| #343 | Catalog/storage | medium reliability | Code-path confirmed | Keep open; make prerequisite to #357 |
| #344 | Catalog/storage | low bug | Code-path confirmed | Keep open |
| #345 | Catalog/storage | medium bug | Partially confirmed / scope adjusted | Keep open; narrow to quiet post-base window |
| #353 | Catalog/storage | medium reliability | Partially confirmed / scope adjusted | Keep open; availability/governance scope only |
| #357 | Catalog/storage | medium reliability | Confirmed by executable repro | Keep open; default disabled/dry-run |
| #359 | Catalog/storage | low bug | Code-path confirmed | Keep open as latent correctness guard |
| #360 | Catalog/storage | medium reliability | Code-path confirmed | Keep open; add deterministic interleaving test |
| #361 | Catalog/storage | medium bug | Code-path confirmed | Keep open; latent until UC is enabled |
| #362 | Catalog/storage | medium reliability | Enhancement gap verified | Keep open; wire publisher before enabling vending |
| #329 | Orchestration | medium bug | Confirmed by executable repro | Keep open; isolate or serialize capture |
| #332 | Orchestration | medium bug | Confirmed by executable repro | Keep open |
| #346 | Orchestration | medium reliability | Partially confirmed / scope adjusted | Keep open; deterministic repair identity |
| #351 | Orchestration | medium bug | Confirmed by executable repro | Keep open; reject mismatched repair body |
| #356 | Orchestration | medium performance | Partially confirmed / scope adjusted | Keep open; add physical listing benchmark |
| #367 | Orchestration | medium reliability | Partially confirmed / scope adjusted | Keep open; add worker heartbeat loop |
| #221 | CI/architecture | enhancement | Enhancement gap verified | Keep open; access projection contract first |
| #290 | CI/architecture | enhancement | Enhancement gap verified | Keep open; staged behavior-preserving extraction |
| #291 | CI/architecture | enhancement | Enhancement gap verified | Keep open; define contracts-only seam |
| #292 | CI/architecture | enhancement | Partially confirmed / scope adjusted | Keep open; share invariants/tests, not cursors |
| #331 | CI/architecture | low testing | Confirmed by executable repro | Keep open; settle all-target lint policy |
| #334 | CI/architecture | medium performance | Confirmed by executable repro | Keep open as promotion blocker |
| #335 | CI/architecture | medium reliability | Code-path confirmed | Keep open as request-budget reliability |
| #350 | CI/architecture | low maintainability | Code-path confirmed | Delete or deliberately port dead module |
| #355 | CI/architecture | medium performance | Confirmed by executable repro | Keep open; cap, deduplicate, and charge per path |
| #365 | CI/architecture | low testing | Confirmed by executable repro | Keep open as testing hygiene |
| #366 | CI/architecture | medium testing | Partially confirmed / scope adjusted | Keep open; make skipped gates explicit and add S3 |
| #218 | Deployed UAT | enhancement | Externally unverified / needs live evidence | Keep open; validated deployed success artifact required |
| #231 | Deployed UAT | unlabeled | Partially confirmed / scope adjusted | Keep open until fresh revision proves query visibility |
| #232 | Deployed UAT | enhancement | Partially confirmed / scope adjusted | Keep open until endpoint/evidence proves deployed provenance |
| #233 | Deployed UAT | bug | Already fixed or superseded | Close as fixed by #304 |
| #234 | Deployed UAT | enhancement | Externally unverified / needs live evidence | Keep open; current environment is not ready |
| #235 | Deployed UAT | unlabeled | Already fixed or superseded | Close as fixed by repo-owned worker path |
| #236 | Deployed UAT | bug | Already fixed or superseded | Close as fixed by #304 deploy preflight/direct refresh |
| #239 | Deployed UAT | bug | Already fixed or superseded | Close repo guard; retain operator discipline |
| #240 | Deployed UAT | enhancement | Partially confirmed / scope adjusted | Keep open; internal-only access remains unproved |
| #241 | Deployed UAT | bug | Partially confirmed / scope adjusted | IAM fixed; keep open until authenticated invocation succeeds |
| #242 | Deployed UAT | bug | Partially confirmed / scope adjusted | Code/config fixed; keep open for live invocation proof |
| #243 | Deployed UAT | bug | Partially confirmed / scope adjusted | Public-ingress workaround observed; internal path unresolved |
| #244 | Deployed UAT | bug | Partially confirmed / scope adjusted | Detection/log confirmed; stored-data cause unverified |
| #245 | Deployed UAT | bug/terraform | Partially confirmed / scope adjusted | Repo wiring fixed; live scope is currently mismatched |
| #246 | Deployed UAT | bug | Partially confirmed / scope adjusted | Historical symptom confirmed; Scheduler currently uninspectable |
| #247 | Deployed UAT | bug | Already fixed or superseded | Close as fixed by #304 structured failure evidence |
| #248 | Deployed UAT | unlabeled | Partially confirmed / scope adjusted | Keep open; passive snapshot confirms live scope mismatch |
| #249 | Deployed UAT | enhancement | Enhancement gap verified | Keep open; document local SDK versus cloud mutation boundary |

## Detailed dossiers

Each dossier records the tested commit/environment, claim and preconditions,
expected versus actual behavior, exact evidence command or harness, code path,
confidence and severity, dependency/WIP overlap, recommendation, and secondary
observations. Dossiers are added batch by batch.

### Critical and high-severity claims

#### #368 — Crashed compaction attempt directory wedges catalog DDL

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** The
  critical severity is justified: one crash-window artifact prevents all later
  Tier-1 catalog compactions for the workspace until manual storage repair.
- **Tested:** `c3c0867`, macOS arm64, Rust 1.88.0, `MemoryBackend` wrapped by the
  existing single-shot `FailingBackend`; no live storage was touched.
- **Claim/preconditions:** after v(N+1) snapshot files are written but before its
  immutable manifest is put, a restarted process chooses the same manifest id and
  attempt directory. Expected recovery is a fresh publish attempt; actual retries
  collide with the abandoned `commits.parquet` whose time/ULID-derived bytes differ.
- **Executable evidence:** a temporary test in
  `crates/arco-catalog/tests/failure_injection.rs` injected the v1 manifest write
  failure, then acquired fresh locks for retries 2 and 3. Both returned
  `snapshot file already exists with different content` at the same
  `snapshots/catalog/v1/attempts/000...001/commits.parquet`; the visible manifest
  remained v0. Command: `CARGO_TARGET_DIR=/tmp/arco-open-issue-audit-target cargo
  test -p arco-catalog --test failure_injection audit_ -- --nocapture` (2/2
  audit tests passed). The temporary test was removed.
- **Code path:** `tier1_compactor.rs` derives `snapshot_attempt_dir` from
  `next_available_manifest_id`, which probes only manifest JSON; `tier1_snapshot.rs`
  fails closed on different existing bytes; `ReservedCatalogCommit` is local to a
  single invocation and `next_commit_ulid`/`published_at` change after restart.
- **Dependencies/WIP:** no open PR closes it. The user-owned `audit-fixes`
  worktree changes the compactor and failure-injection tests, so a fix appears in
  progress, but that worktree is not evidence about `origin/main`. #343/#344/#357
  govern when and whether abandoned snapshot artifacts can be reclaimed.
- **Recommendation:** keep open and repair before other catalog correctness work.
  Use unique per-attempt directories plus a separately age-guarded orphan GC rule;
  retain the exact crash-window regression.

#### #326 — Nightly Security Audit permanently red

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** The issue's
  count is stale only because the failure continued: 42 consecutive scheduled
  non-successes from 2026-06-20 through 2026-07-31, after a 2026-06-19 success.
- **Tested:** live read-only GitHub Actions state at baseline commit `c3c0867` and
  the locked `python/arco` environment under CPython 3.11.14.
- **Expected versus actual:** the nightly should be an actionable green/red
  security signal. The latest run `30608858368` failed only the
  `python-audit (python/arco)` leg; `cargo-deny` and `python-audit (python)` passed.
- **Executable evidence:** `uv run --locked --extra dev --with pip-audit==2.10.0
  pip-audit` exited 1 with exactly two findings: `click 8.3.1`
  (`PYSEC-2026-2132`, fixed in 8.3.3) and `pydantic-settings 2.14.1`
  (`GHSA-4xgf-cpjx-pc3j`, fixed in 2.14.2). `gh run view 30608858368
  --log-failed` showed the same two rows and exit 1.
- **Code/config path:** `.github/workflows/security-audit.yml` runs the locked
  audit in `python/arco`; `python/arco/uv.lock` pins both affected versions.
- **Dependencies/WIP:** at the final refresh, PR #300 upgrades
  `pydantic-settings` only, while new PR #372 removes locked `click 8.3.1` through
  the Typer update and upgrades `pydantic-settings` to 2.14.2, overlapping both
  findings. The uncommitted `audit-fixes` worktree also overlaps Python
  dependency/workflow work. None is baseline proof. The issue body mistakenly
  says “Related: #326”; the advisory-suppression companion is #327.
- **Recommendation:** keep open until the locked repro and a fresh scheduled or
  manually dispatched unchanged workflow are green. Preserve high severity for
  the loss of monitoring signal, even though the individual advisories are not
  RCE-class in this worker.

#### #328 — Python worker redelivery executes a dispatch twice

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** High
  severity is appropriate because the supported long-running worker path permits
  concurrent duplicate side effects.
- **Tested:** `c3c0867`, locked Python 3.11 environment, real loopback
  `DispatchHTTPServer` on an ephemeral port; no Cloud Tasks or external callback
  endpoint was used.
- **Claim/preconditions:** two deliveries with the same `dispatch_id` reach the
  worker before the first finishes. Expected is one execution and prompt ack of
  the duplicate; actual is two executions, two started/completed callback pairs,
  two concurrent handler threads, and HTTP 200 only after execution finishes.
- **Executable evidence:** a temporary three-test harness called the same
  `WorkerDispatchEnvelope` twice and observed two asset calls/two completion
  callbacks; sent two concurrent identical `/dispatch` requests and observed
  `max_active == 2`; and measured both acknowledgements after the injected 200 ms
  task delay. Command: `/tmp/arco-open-issue-audit-pyenv/bin/pytest
  tests/unit/test_audit_high_repros.py -vv` (3/3 passed outside the sandbox after
  sandboxed loopback bind returned `EPERM`). The harness was removed.
- **Code path:** `WorkerDispatchEnvelope.from_dict` requires but only stores
  `dispatch_id`; `DispatchHandler.do_POST` calls `handle_dispatch` synchronously;
  `DispatchHTTPServer` uses `ThreadingMixIn`; `DispatchWorker` has no in-flight or
  completed-id registry.
- **Dependencies/WIP:** interacts with #346 sweeper redispatch and #337/#338
  recovery. The `audit-fixes` worktree changes `worker/server.py`; no open PR
  closes the issue.
- **Recommendation:** keep open. Add bounded durable/in-memory dedup semantics and
  decouple acknowledgement from asset runtime; test redelivery concurrency and
  completed-id retention explicitly.

#### #336 — `catalogs.parquet` read errors publish catalog loss

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** This is at
  the high/critical boundary: the repro produced silent durable metadata loss;
  reachability in GCS still depends on a read failure escaping lower-level retry.
- **Tested:** `c3c0867` with the existing `FailingBackend` injecting a one-shot
  storage error on the visible snapshot's `catalogs.parquet`.
- **Expected versus actual:** the next compaction should abort and leave the
  visible catalog intact. Actual compaction returned success, published a new
  snapshot, and `CatalogReader::list_catalogs()` returned `[]`, deleting the
  previously registered `audit-keeper` from the read model.
- **Executable evidence:** the same temporary failure-injection command recorded
  `catalogs_after_transient_read_failure=[]`; both audit tests passed because the
  harness asserted the observed bad behavior. No live bucket was used.
- **Code path:** `tier1_state::load_catalog_state` maps every error for only
  `catalogs.parquet` to `Vec::new()`, while the other four snapshot files propagate
  non-not-found errors. `Tier1Compactor` folds onto that state, writes a complete
  snapshot, and publishes it through an otherwise valid manifest/pointer CAS.
- **Dependencies/WIP:** the `audit-fixes` worktree narrows this catch-all and adds
  a regression; no open PR closes the issue. Manifest checksum/row-count
  verification would provide stronger defense than the minimal error match.
- **Recommendation:** keep open, raise to critical if repository taxonomy treats
  any reproducible silent data loss as critical, and require the injected-read
  regression plus visible-head preservation before closure.

#### #337 — first task failure is stranded in `RetryWait`

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** High
  severity is justified: the default `max_attempts = 3` makes first failure a
  nonterminal run wedge rather than a surfaced failure.
- **Tested:** `c3c0867`, `arco-flow` unit path with a `RetryWait` row at attempt 1,
  `max_attempts = 3`, `retry_not_before = None`, and fresh watermarks.
- **Expected versus actual:** anti-entropy should create attempt-2 dispatch state;
  actual `AntiEntropySweeper::scan` returned `[]`.
- **Executable evidence:** temporary unit test command
  `CARGO_TARGET_DIR=/tmp/arco-open-issue-audit-target cargo test -p arco-flow
  --features test-utils --lib audit_ -- --nocapture` passed 2/2 audit tests and
  printed `retry_wait_none_repairs=[]`. The temporary code was removed.
- **Code path:** failed `TaskFinished` selects `RetryWait` but does not set a
  deadline or emit a timer. `check_retry_wait_task` returns for `None` through
  `is_none_or`; `set_retry_not_before` is reachable only after a Retry
  `TimerRequested`; `retry_timer_internal_id` has no production caller.
- **History/dependencies/WIP:** #250 closed on 2026-06-27 citing PR #303 and
  14 anti-entropy tests, but those tests supplied a deadline and did not exercise
  bootstrap creation. #337 correctly reopens the substance rather than duplicating
  an active issue. It compounds #338/#367 and #341. `audit-fixes` overlaps this
  path; no open PR closes it.
- **Recommendation:** keep open and correct #250's closure record in eventual
  release notes. Require an end-to-end failed callback -> backoff -> attempt-2
  dispatch test rather than direct invocation of the retry handler.

#### #338 — zombie reaper is masked by its compaction-lag guard

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** The
  shipped 300-second lag budget is crossed before the 300+30-second stale-task
  threshold in a quiet workspace.
- **Tested:** `c3c0867`, `AntiEntropySweeper::with_defaults`, one running task whose
  last heartbeat and workspace watermark were both 400 seconds old.
- **Expected versus actual:** expected `FailStaleRunningTask`; actual was exactly
  `SkippedDueToLag { compaction_lag_secs: 400 }`.
- **Executable evidence:** the same two-test `arco-flow` command printed
  `quiet_workspace_zombie_repairs=[SkippedDueToLag ... 400]` and passed the
  audit assertion.
- **Code path:** `scan` applies the workspace-wide lag guard before task repair;
  `DEFAULT_MAX_COMPACTION_LAG` is five minutes; `running_task_is_stale` adds 30
  seconds to the default 300-second heartbeat timeout; empty compaction batches do
  not advance `last_processed_at`.
- **Dependencies/WIP:** worsened by #367 (the reference worker emits no
  heartbeats); #337 feeds failed stale tasks into another wedge. The
  `audit-fixes` worktree changes this controller; no open PR closes it.
- **Recommendation:** keep open. Make freshness evidence task-relative or ensure
  idle compactor health is independently observable, and preserve a physically
  possible quiet-workspace regression.

#### #339 — partition scope never reaches worker execution

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** High
  severity is justified because success records a partition materialization the
  worker did not execute.
- **Tested:** `c3c0867`, locked Python harness using an envelope payload containing
  `{"partition":{"date":"2026-01-01"}}`.
- **Expected versus actual:** expected `AssetContext.partition_key` to contain the
  requested dimension; actual dimensions were `{}` while the envelope retained
  the supplied payload.
- **Executable evidence:** the temporary Python high-repro suite's partition test
  passed by asserting the bad behavior. Source searches independently confirmed
  no partition field in `WorkerDispatchEnvelope`/proto and empty payloads in both
  Rust producers.
- **Code path:** server-side planning/fold rows carry `partition_key`, the dispatch
  contract does not; Rust dispatcher/sweeper use `{}` payloads; Python
  `_execute_asset` constructs `PartitionKey()` and never reads envelope payload.
- **Dependencies/WIP:** `audit-fixes` changes the worker contract, both Rust
  producers, Python parsing, fixtures, and integration tests; no open PR closes
  it. Partition materialization validation should also fail closed if scope is
  absent.
- **Recommendation:** keep open. Version the dispatch contract, populate and parse
  the canonical partition encoding, and require an end-to-end partitioned run
  where the asset-observed key equals the catalog-recorded key.

#### #340 — run-log aggregation is unbounded

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** High
  reliability severity is reasonable; auth and workspace scoping limit callers,
  but one authorized request has no allocation or read-amplification ceiling.
- **Tested:** `c3c0867`, real Axum test router with `MemoryBackend`, bounded
  fixtures of 225 small objects and five 2 MiB objects.
- **Expected versus actual:** expected pagination/truncation or a response cap.
  Actual returned all 225 listed objects (225 framed entries) and materialized a
  10,486,090-byte response from 10,485,760 stored bytes.
- **Executable evidence:** temporary integration command
  `cargo test -p arco-api --test audit_run_logs_unbounded -- --nocapture` passed
  2/2 audit tests and printed physical object/byte counts. The test was removed.
- **Code path:** `get_run_logs` lists the full prefix, sorts all paths, performs
  one `get_raw` per path, and appends each whole chunk to one `String`.
  `RunLogsQuery` has only `task_key`; the 2 MiB cap is per upload, not aggregate.
- **Dependencies/WIP:** #341 and retry/zombie defects increase retained attempts.
  The `audit-fixes` worktree adds object/byte caps and a regression file; no open
  PR closes the issue.
- **Recommendation:** keep open. Bound object reads and aggregate bytes before
  allocation, add cursor/limit semantics or streaming, and test physical reads as
  well as response length.

#### #341 — orchestration history grows the compaction working set

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  unbounded retained rows and base-rewrite growth; medium for the exact
  terminal-only measurement.** Keep the issue open.
- **Tested:** `c3c0867`, `MemoryBackend`, 30 independently compacted run/plan
  fixtures. The fixture intentionally bounded execution but created planned,
  nonterminal rows; the exact terminal-only variant was not rerun after the
  dedicated build target crossed the mandatory disk stop floor.
- **Expected versus actual:** a bounded working set should not grow with irrelevant
  history. At base merges after 10/20/30 compactions, retained runs and tasks were
  10/20/30; physical `runs.parquet` bytes grew 5,085 -> 5,694 -> 6,297 and
  `tasks.parquet` 8,241 -> 9,141 -> 10,027.
- **Executable evidence:** temporary unit command `cargo test -p arco-flow
  --features test-utils --lib audit_terminal_history_grows_base_rewrite_bytes
  -- --nocapture` passed and printed the three measurements. The test name
  overstated its fixture (planned history, not terminal history); this report
  corrects that scope rather than inheriting the name.
- **Code-path evidence for the terminal claim:** the only retention constants and
  `.retain` sweeps in the compactor cover `sensor_evals` and `idempotency_keys`.
  `load_current_state` reads every base/L0 run and task, and base merge writes the
  complete maps. No production removal path exists for terminal run/task rows.
- **History/dependencies/WIP:** #255 closed citing PR #303's bounded per-callback
  delta-row test; that test does not bound base reads or rewrites. #345 must be
  resolved before relying on delta tombstones for retention. `audit-fixes` adds
  terminal retention and measurement work; no open PR closes #341.
- **Recommendation:** keep open but amend the acceptance evidence to require a
  terminal-only N-scaling fixture, physical get/put bytes, and deletion parity
  across base/L0 rebuild. Preserve high severity as a promotion/operability
  blocker, not as an immediate outage at small scale.

### Security, authentication, and supply chain

This batch used a deliberately passive boundary: source/config inspection,
dependency-audit tooling, GitHub metadata, and read-only Cloud Run service/IAM
descriptions only. No deployed URL was requested, no authentication input was
constructed, no secret value was retrieved, and no exploit-style harness was
run. Consequences that require those actions remain code-path or external-evidence
claims rather than executable confirmations.

#### #325 — flow-compactor internal routes are unauthenticated when OIDC config is absent

- **Verdict:** **Code-path confirmed**. **Confidence: high for the local default,
  medium for exposure.** Medium defense-in-depth severity is appropriate.
- **Tested/claim:** at `c3c0867`, source and Terraform inspection with no internal
  OIDC variables. Expected internal routes to fail closed; actual
  `InternalOidcConfig::from_env()` returns `None`, and
  `crates/arco-flow/src/bin/arco_flow_compactor.rs::build_router` installs the
  compact, rebuild, and reconcile routes without middleware.
- **Evidence:** `rg -n 'InternalOidcConfig|build_router|ARCO_INTERNAL_AUTH'
  crates/arco-core crates/arco-flow infra/terraform`; read-only `gcloud run
  services describe` and IAM-policy reads showed the deployed dev compactor has
  no internal-auth env names. Its ingress metadata is broad, but invocation IAM
  is limited to two service accounts; no URL request was made.
- **Dependencies/WIP:** the separate API compactor wrapper does not cover all
  three flow-compactor routes. The user-owned `audit-fixes` worktree overlaps
  internal auth; it is not baseline evidence.
- **Recommendation:** keep open and require an explicit authenticated mode at
  startup. Preserve Cloud Run IAM as a separate outer control and add a
  configuration-matrix test. Secondary observation: the issue should distinguish
  route-level absence from currently observed IAM reachability.

#### #327 — `deny.toml` suppresses live `quick-xml` advisories without lifecycle metadata

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** This is a
  medium supply-chain/process gap; inspected reachability does not establish a
  high-impact exploit in Arco's trusted object-store clients.
- **Tested/claim:** `c3c0867`, Cargo 1.88.0 and cargo-deny. Expected suppressions
  to be tracked or expired; actual `deny.toml` ignores RUSTSEC-2026-0194 and
  RUSTSEC-2026-0195 while the lockfile contains `quick-xml` 0.37.5 and 0.38.4.
- **Executable evidence:** `cargo deny check advisories` passed with the shipped
  config. The same command with a temporary config that retained the repository
  advisory policy but removed only the ignore list exited 1 and reported both
  advisories for both locked versions. The temporary config was deleted.
- **Code/dependencies/WIP:** the versions enter through `object_store` and the
  `opendal`/Iceberg graph. No open PR declares closure; dependency-related PRs and
  `audit-fixes` overlap portions of the graph.
- **Recommendation:** keep open until versions are upgraded, or replace bare
  ignores with an accountable exception containing owner, rationale, affected
  surface, and review-by date. #326 should be fixed first so the nightly signal is
  useful.

#### #330 — internal OIDC is report-only by default and gives HS256 precedence

- **Verdict:** **Code-path confirmed**. **Confidence: high.** Medium severity is
  appropriate for an internal-boundary fail-open configuration.
- **Tested/claim:** `c3c0867`, passive source/config matrix. Expected configured
  verification failures to deny by default; actual `InternalOidcConfig` defaults
  `enforce` to false, middleware admits a failed verification in that mode, and
  `verify_token` selects a configured shared secret before JWKS.
- **Evidence/code path:** `sed -n '38,280p'
  crates/arco-core/src/internal_oidc.rs` plus caller searches in both compactors.
  No token was minted or submitted and no service endpoint was contacted.
- **Dependencies/WIP:** shares the absent-config boundary with #325. The
  `audit-fixes` worktree changes internal-auth configuration; no open PR closes
  the issue.
- **Recommendation:** keep open. Make enabled authentication fail closed, require
  an explicit development-only report mode, and prevent silent algorithm-source
  precedence. Test the posture matrix without live services.

#### #333 — worker's literal token fallback is not normally reachable from HTTP dispatch

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high.** The
  literal fallback exists, but the reported request path is guarded earlier;
  security severity should be reduced to low hardening/maintainability.
- **Tested/claim:** at `c3c0867`, expected worker startup to reject missing token
  configuration. `DispatchWorker.__init__` does fall back from task token to API
  key and then a literal development value, but
  `WorkerDispatchEnvelope.from_dict` rejects an absent/empty per-dispatch token,
  so a normal HTTP dispatch cannot use the final fallback.
- **Evidence:** `rg -n '_select_task_token|fallback_task_token|task_token'
  python/arco/src/arco_flow/worker python/arco/tests/unit`; existing tests exercise
  the helper fallback but do not prove an externally reachable acceptance path.
  No callback request was sent.
- **Dependencies/WIP:** `audit-fixes` changes worker task-token behavior. #363 is
  the stronger secret-delivery issue.
- **Recommendation:** re-scope to removing unreachable/development fallback code
  and validating startup configuration, or close once dead-code behavior is
  removed. Do not retain the original security consequence without a reachable
  path.

#### #347 — public-route classification uses suffix matching, but protected handlers fail closed

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  classification, medium-high for current handler closure.** Reduce to low
  hardening.
- **Tested/claim:** `c3c0867`, source and route-graph inspection. The Iceberg and
  Unity Catalog helpers use `ends_with` for public documentation/config paths,
  so the classifier is broader than an exact route match. However current
  non-public handlers require a nonoptional authenticated extension and fail
  closed; the feature routes are disabled by default in Terraform.
- **Evidence/code path:** `rg -n 'ends_with.*openapi|ends_with.*v1/config|Extension'
  crates/arco-api crates/arco-iceberg crates/arco-uc infra/terraform`. No crafted
  path was submitted to a local or deployed server.
- **Dependencies/WIP:** the issue overlaps general route-auth posture but is not a
  duplicate of #325. No open PR closes it.
- **Recommendation:** re-scope the title and acceptance criteria to exact-path
  classification and shared public-route rate limiting. Close the current data
  access claim unless a nonoptional-context bypass is independently shown.

#### #348 — API metrics-secret config is dead, but the documented route is intentionally unauthenticated

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high.** The
  dead configuration claim is true; the asserted data-leak/security posture is
  not supported by the repository's deployment contract.
- **Tested/claim:** at `c3c0867`, `Config.metrics_secret` is parsed and documented
  as a gate, but the API metrics handler takes no request state or header and
  never reads it. Runbooks intentionally permit unauthenticated metrics only in
  development/private posture and prescribe 404 for public posture; Terraform
  does not set the field.
- **Evidence:** `rg -n 'metrics_secret|serve_metrics|/metrics' crates/arco-api
  docs infra/terraform`; metric definitions were inspected for tenant-identifying
  labels. No metrics endpoint was requested.
- **Dependencies/WIP:** the separate compactor implements a real metrics-secret
  gate, which likely caused the misleading API config to persist.
- **Recommendation:** close the security claim and separately remove the dead
  field/documentation or implement the documented contract. Retain low
  maintainability severity unless deployment policy is changed.

#### #349 — compactor-auth startup validator is dead; deployment-auth scope overlaps #242

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  the dead validator, medium for deployed impact.** Low reliability/auth severity
  is appropriate locally.
- **Tested/claim:** `c3c0867`, source, Terraform, and passive service metadata.
  `CompactorAuthConfig::validate()` has tests but no production caller, and the
  local default auth mode is `none`; the first authenticated call does still
  reject a configured static mode with no token. Terraform does not define a
  compactor-auth-mode variable, while the repair script explicitly selects a GCP
  identity token.
- **Evidence:** `rg -n 'CompactorAuthConfig|\.validate\(|COMPACTOR_AUTH'
  crates/arco-api infra scripts`. Read-only service description was used only to
  compare configuration names; no compaction call was made.
- **Dependencies/WIP:** #242 already tracks API-to-IAM-protected compactor
  authentication and should own the deployment path. `audit-fixes` overlaps API
  auth configuration.
- **Recommendation:** merge the deployment-auth portion into #242; keep or
  re-scope #349 to mandatory startup validation and explicit mode selection.

#### #352 — derived root idempotency keys share the caller-key namespace

- **Verdict:** **Code-path confirmed**. **Confidence: high.** Low severity is
  appropriate: impact is confined to a workspace peer that can predict another
  operation's key and already possesses mutation authority.
- **Tested/claim:** at `c3c0867`, code-path inspection showed
  `root_participant_metadata` derives a root key by string concatenation into the
  same storage namespace used by caller-supplied idempotency keys. Expected
  reserved derived identities to be unrepresentable by callers; raw caller keys
  are not validated against that prefix.
- **Evidence:** repository searches and direct inspection of transaction
  idempotency reservation/lookup code; no collision input was submitted and no
  storage row was written.
- **Dependencies/WIP:** handle-owned identities have stronger guards, making this
  a legacy-root inconsistency rather than a system-wide absence. No open PR
  declares closure.
- **Recommendation:** keep open at low severity. Use a typed/separate namespace
  and add a local `MemoryBackend` regression when disk headroom permits.

#### #354 — signed-file allowlist includes raw commit snapshots

- **Verdict:** **Code-path confirmed**. **Confidence: high for artifact access,
  medium for sensitivity.** Low security/governance severity is appropriate.
- **Tested/claim:** `c3c0867`, projection, snapshot-manifest, and signed-file
  allowlist inspection. The safe `system.catalog.commits` projection drops
  private columns, but Tier-1 manifests enumerate raw `commits.parquet`, and the
  browser file allowlist admits every selected snapshot file after scope and
  membership checks.
- **Evidence:** `rg -n 'system.catalog.commits|commits.parquet|signed_url|allowlist'
  crates`; no signed URL was minted or requested. The extra raw fields are
  principally witness/event identifiers and digests; `manifest_id` is already
  exposed through other metadata.
- **Dependencies/WIP:** shares publication-policy questions with #359–#362, not
  an authentication bypass. No open PR closes it.
- **Recommendation:** keep open, define whether raw snapshot artifacts are public
  within a tenant/workspace, and either restrict the allowlist to projected
  artifacts or document the raw schema as an authorized interface.

#### #358 — Iceberg metadata locations are normalized for storage but echoed externally

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  normalization/echo behavior, low-medium for an external reader consequence.**
  Treat as a latent low-to-medium boundary, not a confirmed exfiltration path.
- **Tested/claim:** at `c3c0867`, `resolve_metadata_path` discards URI scheme and
  bucket for server-side scoped storage, while table metadata/pointers preserve
  the client-provided location. Server writes remain constrained by scoped
  storage; GCS credential vending is denied; the server does not itself follow
  the external location.
- **Evidence:** `rg -n 'resolve_metadata_path|metadata_location|location'
  crates/arco-iceberg infra/terraform`; CRUD and credential-vending feature
  defaults were inspected. No external location was submitted or dereferenced.
- **Dependencies/WIP:** Iceberg CRUD is disabled in the inspected default
  Terraform posture. No open PR closes the issue.
- **Recommendation:** keep open as a latent validation gap, lower severity until
  an authorized integration proves external-engine behavior, and constrain
  canonical locations before enabling CRUD.

#### #363 — task-signing secret is committed and injected as direct environment data

- **Verdict:** **Code-path confirmed**. **Confidence: high for repository and
  Terraform behavior; deployed storage remains passively unverified.** Medium
  secret-management severity is justified.
- **Tested/claim:** at `c3c0867`, Terraform assigns the task-token variable
  directly to API, dispatcher, and sweeper environment entries rather than using
  a secret reference. The dev tfvars file contains a nonempty committed value.
- **Redacted evidence:** `infra/terraform/environments/arco-testing-dev.tfvars:22`
  is present, 26 bytes, SHA-256 prefix `c99327e90cf0`; the value is intentionally
  omitted. `infra/terraform/cloud_run.tf` has three direct `value =
  var.task_token_secret` assignments. No secret API or runtime value was read.
- **Dependencies/WIP:** #333 concerns fallback selection, while this issue owns
  secret custody. `audit-fixes` overlaps Terraform/task-token code; no open PR
  closes the issue.
- **Recommendation:** keep open. Move delivery to a managed secret reference,
  remove committed secret material, and rotate through the authorized operator
  process. This audit performs none of those state-changing actions.

#### #364 — deployed dev API combines debug authentication with public invocation

- **Verdict:** **Code-path confirmed with passive deployed-state corroboration**.
  **Confidence: high.** Raise from medium to high/critical operational priority
  for the inspected testing service, despite it not being a production project.
- **Tested/claim:** `c3c0867` plus read-only Cloud Run description/IAM policy in
  project `arco-testing-20260320`, region `us-central1`. Terraform enables debug
  for non-public dev posture; debug request handling trusts caller-supplied scope
  metadata instead of token claims.
- **Expected versus actual:** the issue's own caveat expected internal ingress/no
  public invoker and possibly a placeholder image. Passive metadata instead
  showed the dev API with debug enabled, `api_public=false`, broad ingress,
  `allUsers` invocation, and a real Arco API image. No service URL was requested
  and no request headers or token were constructed.
- **Evidence/code path:** read-only `gcloud run services describe` and `gcloud run
  services get-iam-policy`, limited to configuration booleans, image identity,
  ingress, and IAM principals; `rg -n 'ARCO_DEBUG|api_public|debug' crates/arco-api
  infra/terraform` for the local path.
- **Dependencies/WIP:** this is a deployed-posture interaction, not just a code
  default. It overlaps #330 only conceptually; `audit-fixes` changes debug/task
  auth configuration.
- **Recommendation:** keep open and request immediate operator review through the
  normal change process. Gate debug independently from environment name, forbid
  it when public invocation is enabled, and add a Terraform policy test. The
  audit did not change IAM, deployment, or service state.

### Catalog, storage, projection, and transaction safety

#### #271 — UC table drop leaving Iceberg pointer orphans

- **Verdict:** **Already fixed or superseded**. **Confidence: high.** The open
  issue describes behavior that is absent from the tested baseline.
- **Tested/claim:** `c3c0867`, macOS arm64, Rust 1.88.0, `MemoryBackend` unit
  path. Expected an Iceberg UC drop to remove both the catalog row and pointer;
  actual code does so after recovering the dropped table identity.
- **Executable/history evidence:** `cargo test -p arco-uc
  delete_iceberg_table_removes_pointer -- --nocapture` passed. `git blame` traces
  the cleanup and regression to `aa7303ba3` (`Fix catalog protocol compatibility
  gaps (#305)`, 2026-06-27), which is an ancestor of the baseline.
- **Code path/WIP:** `crates/arco-uc/src/routes/tables.rs::delete_table` filters
  the dropped row by Iceberg format, derives `IcebergPaths::pointer_path`, and
  deletes it through scoped storage. No active WIP is needed to establish this.
- **Recommendation:** close as fixed by #305 and cite the passing test. Secondary
  observation: pointer deletion is best-effort after catalog commit, so a storage
  deletion failure can still leave a logged orphan for later GC; that narrower
  residual is not the unconditional gap alleged here.

#### #324 — `force_break` resets the fencing sequence

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** Raise from
  the label's medium severity to high because the documented recovery operation
  can wedge a mature workspace and violates monotonic fencing.
- **Tested/claim:** `c3c0867`, `MemoryBackend`. Expected a post-break holder to
  have a token above the old holder; actual `force_break` deletes the record and
  fresh acquisition starts again at sequence 1.
- **Executable evidence:** `cargo test -p arco-core
  lock::tests::test_force_break -- --nocapture` and `cargo test -p arco-catalog
  durable_epoch_advances_after_the_released_lock_record_is_deleted --
  --nocapture` both passed. The latter explicitly asserts the recreated lease is
  sequence 1 while demonstrating why retention uses a separate durable epoch.
- **Code path/dependencies/WIP:** Tier-1 writer stamps the lease sequence into the
  durable pointer and rejects a writer behind that epoch. `audit-fixes` changes
  `lock.rs`; no open PR closes the issue.
- **Recommendation:** keep open and repair before relying on operator force-break.
  Preserve the sequence under CAS and separate the durable writer epoch from the
  lease lifecycle; test both availability and stale-holder fencing.

#### #342 — namespace/table idempotency does not reserve entity identity

- **Verdict:** **Code-path confirmed**. **Confidence: high.** Medium severity is
  appropriate. Executing the exact finalize gap requires killing a request
  between durable creation and marker finalization, so no process-kill repro was
  attempted.
- **Tested/claim:** at `c3c0867`, route/writer/idempotency-state inspection showed
  `create_namespace` and legacy `register_table` claim markers in the API but do
  not call the writer's reserve-entity protocol. After stale takeover they
  re-execute, receive `AlreadyExists`, and cache the 409 as `Failed`.
- **Evidence:** `rg -n 'check_idempotency|reserve_idempotency_entity|StaleReserved|finalize_failed'
  crates/arco-api/src/routes crates/arco-catalog/src/{writer,idempotency}.rs`.
  The three catalog/schema-aware sibling operations do reserve and recover,
  making the asymmetry independently visible.
- **Dependencies/WIP:** marker GC bounds the duration but does not repair the
  wrong response. No open PR closes the issue; the broad `audit-fixes` worktree
  does not change the two affected API route files.
- **Recommendation:** keep open. Move both legacy operations to reserve/recover
  semantics and add a fault-injected finalize-gap regression before closure.

#### #343 — Full repair deletes recent prior snapshot files

- **Verdict:** **Code-path confirmed**. **Confidence: high.** Medium reliability
  severity is justified; current-head protection prevents permanent loss of the
  visible snapshot, but readers of a just-resolved prior version can fail.
- **Tested/claim:** `c3c0867`, passive destructive-path review. Expected age,
  retention-count, and mutation-epoch guards before deletion; actual
  `Reconciler::repair_with_scope(Full)` protects only exact current paths and
  versions above the visible head, then directly deletes older files.
- **Evidence/code path:** `sed -n '240,440p' crates/arco-catalog/src/reconciler.rs`
  compared with `gc/collector.rs` and `gc/policy.rs`. No live or local object was
  deleted for this repro because the branch is already unambiguous and the
  plan treats unsafe deletion paths as code-path evidence.
- **Dependencies/WIP:** #357 makes this path automatic by default; #344 is the
  separate nonfunctional orphan-GC path. `audit-fixes` changes the reconciler and
  compactor.
- **Recommendation:** keep open and make it a prerequisite to any enforce-mode
  rollout. Apply a minimum age, retention/pin protection, and the same distributed
  retention authority used by GC.

#### #344 — orphaned-snapshot GC HEADs a non-object directory prefix

- **Verdict:** **Code-path confirmed**. **Confidence: high.** Low bug severity is
  correct: cleanup is inert rather than destructive.
- **Tested/claim:** at `c3c0867`, `find_orphaned_snapshots` returns version
  directory strings ending in `/`; `gc_orphaned_snapshots` runs deletion only
  inside `if let Ok(Some(meta)) = head_raw(&dir)`. Object stores expose files,
  not synthetic prefix metadata, so the body is skipped.
- **Evidence:** direct inspection of `gc/collector.rs:320-394`,
  `extract_snapshot_version_dir`, and storage backend `head_raw` contracts. The
  report command was `rg -n 'find_orphaned_snapshots|gc_orphaned_snapshots|head_raw'
  crates/arco-catalog/src/gc crates/arco-core/src/storage.rs`.
- **Dependencies/WIP:** #368 creates precisely the abandoned attempt artifacts
  this phase should eventually reclaim. `audit-fixes` changes the collector.
- **Recommendation:** keep open. Determine age from the newest/oldest child object
  as policy requires, then call the already fenced `delete_prefix`; add physical
  list/head/delete accounting.

#### #345 — L0 deltas cannot represent row deletion

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  resurrection, medium for repeat-dispatch consequence.** Medium severity remains
  reasonable, but “forever” applies primarily to a quiet post-base-merge window.
- **Tested/claim:** `c3c0867`, fold/delta/reload/controller path inspection.
  `insert_changed` iterates only current rows, `delta_from_states` has no deletion
  channel, and `merge_states` is an upsert union; a row pruned from current state
  therefore reappears when a non-base L0 delta is reloaded.
- **Evidence:** `sed -n '1504,1855p'
  crates/arco-flow/src/orchestration/compactor/service.rs` plus the existing
  in-memory-only prune test at `fold.rs:5176`. No external task was dispatched.
- **Scope/dependencies/WIP:** sensor/idempotency retention is re-applied during
  load, and an active tenant can self-heal at the next base merge. Actual duplicate
  execution additionally depends on external task-name dedup expiry. #328 covers
  worker redelivery once delivered. `audit-fixes` changes compactor service.
- **Recommendation:** keep open with narrowed wording. Add explicit tombstones or
  deleted-key sets and a publish-delta-reload regression that starts immediately
  after a base merge.

#### #353 — root transaction lock bypasses tenant/workspace scoping

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  path and contention, medium-low for deployed IAM behavior.** Keep medium
  reliability severity, but remove information-disclosure/corruption claims.
- **Tested/claim:** at `c3c0867`, expected the root lock path to pass through
  `ScopedStorage`; actual `execute_claimed_root` passes the constant
  `locks/root.lock.json` to the raw backend, coupling all scopes using one bucket.
- **Evidence:** direct inspection of `ControlPlaneTxPaths::root_lock`,
  `control_plane_transactions.rs::execute_claimed_root`, and catalog's correctly
  scoped lock construction. No lock was created. Domain-level commits retain
  their own scoped fencing, so stale root ownership alone does not prove corrupt
  publication.
- **Dependencies/WIP:** passive Terraform review suggests the deployed conditional
  IAM grant may reject this bucket-root path; that remains externally unverified
  and belongs with deployed-UAT findings. `audit-fixes` changes the transaction
  service.
- **Recommendation:** keep open. Construct all root/repair lock paths through the
  request scope and add a two-scope `MemoryBackend` concurrency test.

#### #357 — destructive repair automation is enabled by default

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** Medium
  reliability severity is appropriate; #343 determines the concrete deletion
  consequence.
- **Tested/claim:** `c3c0867`, isolated compactor unit test. Expected absent
  configuration to disable or dry-run repair; actual default is `Enforce`,
  `Full`, every 300 seconds for catalog, lineage, and search.
- **Executable evidence:** `cargo test -p arco-compactor
  test_repair_automation_config_defaults_to_enforce_full_scope -- --nocapture`
  passed, pinning the unsafe default. Terraform independently defaults the same
  mode/scope, so deployed configuration does not neutralize it.
- **Dependencies/WIP:** #343 must land before enforce mode is safe. `audit-fixes`
  changes compactor configuration and Terraform.
- **Recommendation:** keep open. Default to disabled or dry-run, require explicit
  enforcement configuration, and add policy tests that reject enforce/full when
  age/retention authority is absent.

#### #359 — Iceberg snapshot refs accept an absent snapshot id

- **Verdict:** **Code-path confirmed**. **Confidence: high for invalid metadata,
  medium for external-engine symptom.** Low severity is appropriate because
  writes are disabled in repository deployment defaults.
- **Tested/claim:** at `c3c0867`, both single-table and multi-table `apply_update`
  implementations validate schema/spec/sort-order referents but copy a snapshot
  ref and main snapshot id without checking `metadata.snapshots`.
- **Evidence:** direct comparison of the match arms in
  `crates/arco-iceberg/src/{commit,coordinator}.rs`; no malformed request was sent
  to a server. External engine rejection is inferred, not executed.
- **Dependencies/WIP:** Iceberg write mode is false and unset in Terraform.
  `audit-fixes` changes both duplicated implementations.
- **Recommendation:** keep open as a latent correctness guard. Centralize update
  application and validate snapshot membership before either metadata write path.

#### #360 — stale marker takeover can cache failure after a commit lands

- **Verdict:** **Code-path confirmed**. **Confidence: high for the interleaving,
  medium for practical timing.** Medium reliability severity is reasonable but
  latent while Iceberg writes remain disabled.
- **Tested/claim:** `c3c0867`, idempotency/pointer CAS state-machine inspection.
  Takeover replaces the marker version without fencing the first writer; the
  first can still win the pointer CAS, fail marker finalization, and the takeover
  can then lose the pointer CAS and finalize the marker as cached failure.
- **Evidence:** `sed -n '240,470p;680,770p'
  crates/arco-iceberg/src/commit.rs`. The exact two-writer timing was not executed;
  it needs deterministic pause points rather than wall-clock sleeps.
- **Dependencies/WIP:** marker GC ignores already-Failed reconciliation, and the
  default service request timeout is absent. `audit-fixes` changes commit logic.
- **Recommendation:** keep open. Fence pointer publication on current marker
  ownership or reconcile pointer truth before caching failure; require a
  controlled two-writer interleaving regression.

#### #361 — Delta endpoints share coordinator state but use different log roots

- **Verdict:** **Code-path confirmed**. **Confidence: high.** The correctness
  defect is severe if both endpoints are enabled, but repository deployment
  posture leaves Unity Catalog disabled; assess as medium latent risk rather than
  current high impact.
- **Tested/claim:** at `c3c0867`, API commit paths resolve the registered table
  location and construct `DeltaPaths`, while the UC facade uses
  `DeltaCommitCoordinator::new` and a hard-coded legacy log prefix. Both retain
  table-id-keyed coordinator state, allowing one version sequence across two log
  directories.
- **Evidence:** direct path-constructor comparison in
  `crates/arco-api/src/routes/delta.rs`, `crates/arco-uc/src/routes/delta_commits.rs`,
  `crates/arco-delta/src/coordinator.rs`, and `arco-core/src/flow_paths.rs`. No
  Delta table was mutated.
- **Dependencies/WIP:** UC is disabled by default/unset in Terraform.
  `audit-fixes` changes both routes, the coordinator, and integration tests.
- **Recommendation:** keep open. Make the canonical table root part of persisted
  coordinator identity and derive both commit/list paths from the catalog record;
  fail closed on a mismatched existing coordinator.

#### #362 — no production metastore projection publisher

- **Verdict:** **Enhancement gap verified**. **Confidence: high.** Medium feature
  completeness/reliability severity is appropriate; enforcement fails closed.
- **Tested/claim:** at `c3c0867`, expected a deployed binary to publish
  `metastore_projection.pointer.json`; actual callers of
  `publish_metastore_projection_set` exist only in tests. Credential routes
  require a fresh projection and return unavailable when it is missing/stale.
- **Evidence:** repository-wide `rg -n 'publish_metastore_projection_set\(' .
  --glob '!**/target/**'` found the definition plus catalog/UC tests, with no API,
  compactor, flow, or other production caller. Terraform does not enable UC.
- **Dependencies/WIP:** support metadata already labels credential vending
  partial. `audit-fixes` changes publication code and tests, but is not baseline
  behavior.
- **Recommendation:** keep open. Wire an event-driven or safely periodic publisher
  before enabling credential vending and test freshness after every metastore
  append; preserve the current fail-closed read behavior.

### Orchestration and worker runtime

#### #329 — threaded worker stdout capture crosses task boundaries

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** Medium
  correctness severity is justified, with confidentiality impact if one worker
  serves more than one tenant.
- **Tested/claim:** `c3c0867`, locked Python 3.11.14 environment, two real
  `DispatchWorker.handle_dispatch` calls on coordinated threads. Expected each
  upload to contain only its asset's output; actual task B's captured stdout
  contained task A's marker because both contexts reassign process-global
  `sys.stdout`.
- **Executable evidence:** temporary `/tmp` harness command
  `/tmp/arco-open-issue-audit-pyenv/bin/pytest
  /tmp/test_audit_orchestration_repros.py -vv` passed 2/2 after fixture-only
  import/contract corrections. The concurrency assertion proved the crossed line;
  the harness was removed.
- **Code path/dependencies/WIP:** `server.py` combines
  `contextlib.redirect_stdout` with `ThreadingMixIn`; #328 makes concurrent copies
  of one dispatch possible. `audit-fixes` changes the worker server.
- **Recommendation:** keep open. Use task-local logging/capture or deliberately
  serialize asset execution, and retain a two-task isolation regression.

#### #332 — worker log upload omits dispatch authentication

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** Medium
  observability severity is appropriate; the endpoint fails closed, so this is
  not an access-control bypass.
- **Tested/claim:** at `c3c0867`, a real `ArcoFlowApiClient` with no API key called
  `upload_logs` through a captured request method. Expected the dispatch token to
  authorize the upload; actual request headers contained no `Authorization`
  field, and the method has no token/callback-URL parameters.
- **Evidence:** the same temporary Python command passed the missing-header
  assertion. `DispatchWorker` passes task token and callback URL to started and
  completed callbacks, but not to `upload_logs`; its `ApiError` is only printed
  locally. The harness was removed.
- **Dependencies/WIP:** blocks useful evidence for deployed-UAT #247.
  `audit-fixes` changes both `client.py` and `server.py`.
- **Recommendation:** keep open. Thread the dispatch-scoped token and validated
  callback base URL through log upload, and surface upload failure structurally.

#### #346 — sweeper creates a fresh task name for repeated repair

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  name instability, medium-low for duplicate live execution.** Medium latent
  reliability severity is appropriate.
- **Tested/claim:** `c3c0867`, focused Rust unit. Expected repeated repair of one
  outbox epoch to reuse a Cloud Tasks identity; actual helper includes a fresh
  ULID and the test requires two same-epoch repair ids to differ.
- **Executable evidence:** `cargo test -p arco-flow --bin arco_flow_sweeper
  redispatch_cloud_task_id_is_repair_scoped -- --nocapture` passed. No Cloud Task
  was created and no worker endpoint was called.
- **Scope/dependencies/WIP:** outbox time is refreshed, task-start pruning ends
  repair, queue delivery retries often finish before the stale threshold, and
  committed environments do not deploy the sweeper. Actual duplication also
  depends on an original delivery remaining live and #328 worker dedup absence.
  `audit-fixes` changes sweeper/anti-entropy code.
- **Recommendation:** keep open with narrowed consequence. Make repair identity
  deterministic for a stable repair epoch or advance/fence the attempt.

#### #351 — repair-pending orchestration accepts a different request body

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** Medium-to-
  high correctness severity is justified: the legacy path can return success for
  an event it never appended, though reachability is one repair window for an
  authenticated caller.
- **Tested/claim:** `c3c0867`, `MemoryBackend`, real API router and loopback
  compactor stub. The existing regression first stored event id ending `...01`,
  then retried the same idempotency key with `...02`; repair returned a visible
  one-event receipt while using only the original stored path.
- **Executable evidence:** `cargo test -p arco-api --test
  control_plane_transactions_api
  commit_orchestration_batch_repairs_ambiguous_append_from_stored_event_paths --
  --nocapture` passed exactly as described.
- **Code path/dependencies/WIP:** `claim_idempotency` bypasses hash mismatch for
  `RepairPending`; legacy `Immediate` policy disables stored-event identity
  comparison. Frozen-handle, root, and catalog paths reject or validate the
  mismatch. `audit-fixes` changes transaction code/tests.
- **Recommendation:** keep open. A changed request hash must return conflict;
  repair should validate stored paths and bytes against the submitted batch under
  every policy. Reverse the existing test to require rejection.

#### #356 — anti-entropy processing cap does not cap physical listing

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  full-list allocation, insufficient for a specific OOM threshold.** Keep medium
  performance severity but do not claim an observed outage.
- **Tested/claim:** `c3c0867`, implementation and backend contract review.
  `run_pass` calls `ScopedStorage::list_meta`, whose backend `list` fully collects
  the prefix; only afterwards does it sort, seek by `last_path`, and `take(limit)`.
  Thus `max_objects_per_run` bounds comparisons/enqueues, not listed objects.
- **Evidence:** `sed -n '340,395p' crates/arco-compactor/src/anti_entropy.rs`,
  `scoped_storage.rs:577-601`, and `storage.rs` `try_collect::<Vec<_>>()`. Existing
  “bounded” tests assert processed counts/config arithmetic and do not count
  physical pages, objects, or bytes. No cloud listing was performed.
- **Scope/dependencies/WIP:** passes are idle-only, serialized, one workspace at a
  time, and downstream work is bounded. The report does not confirm the issue's
  2M-object/OOM estimate without measurement. `audit-fixes` changes anti-entropy.
- **Recommendation:** keep open. Add a paginated storage contract and a benchmark
  that records list pages, returned metadata bytes, peak allocation, and growth
  across bounded fixture sizes.

#### #367 — reference Python worker emits no heartbeats

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  missing heartbeat loop and stale classification, medium for final retry impact.**
  Keep medium runtime reliability severity.
- **Tested/claim:** at `c3c0867`, source/call-graph inspection found a working
  `task_heartbeat` client method and contract tests, but no call from the worker.
  Anti-entropy falls back from absent `last_heartbeat_at` to `started_at` and
  classifies a task stale after its configured 300 seconds plus 30-second grace.
- **Evidence:** `rg -n 'heartbeat|task_started|task_completed'
  python/arco/src/arco_flow/worker python/arco/src/arco_flow/client.py` and direct
  anti-entropy/sweeper inspection. No seven-minute task or deployed sweep ran.
- **Scope/dependencies/WIP:** #338 can mask the reaper in a quiet workspace; #337
  prevents normal retry bootstrap; a late completion can move the same attempt
  from `RetryWait` to success. Current deployed smoke worker finishes quickly and
  committed environments do not enable the sweeper. `audit-fixes` overlaps both
  worker and controller paths.
- **Recommendation:** keep open with corrected interaction wording. Carry the
  heartbeat timeout in the dispatch contract, emit bounded periodic heartbeats,
  honor cancellation, and make late completion/redelivery handling idempotent.

### CI, performance, maintainability, and architecture

#### #221 — row-level query redaction lacks an access-backed contract

- **Verdict:** **Enhancement gap verified**. **Confidence: high.** This is a
  prerequisite/design gap, not a currently exploitable query defect: the access
  tables whose rows would drive redaction are intentionally not queryable.
- **Frozen-baseline evidence:** `system_tables.rs` explicitly rejects
  `system.access.{grants,compiled_permissions,audit,auth_denies,credential_mints}`
  until authoritative projections exist. ADR-035 lists those tables as deferred,
  and ADR-037 requires explicit allowlisting, redaction, workspace scoping, and
  freshness watermarks before a system table is complete. Current UAT proves
  workspace isolation for catalog/orchestration rows, which is a different
  contract from row-level grant filtering.
- **History/dependencies/WIP:** the Batch 7 report deliberately deferred #221
  until access-backed projections land. The broad `audit-fixes` WIP changes
  `system_tables.rs`, but unmerged WIP is not baseline proof and does not remove
  the missing authority/schema dependency.
- **Recommendation:** keep open. Define authoritative access mutations and safe
  projections, the caller-to-row authorization rule, deny and inference cases,
  and freshness behavior before extending deterministic UAT.

#### #290 — orchestration, catalog writer, and fold modules have no staged extraction seam

- **Verdict:** **Enhancement gap verified**. **Confidence: high for the
  maintainability condition, not a runtime-failure claim.** File size alone is
  not a defect, but these files combine protocol DTOs, validation, persistence,
  orchestration, and response construction behind one change surface.
- **Evidence:** at `c3c0867`, `wc -l` measured 9,235 lines in
  `routes/orchestration.rs`, 9,115 in `writer.rs`, and 7,169 in `fold.rs`. A
  simple anchored declaration scan (including implementation blocks) found 251,
  70, and 69 matches respectively. History shows repeated cross-cutting edits,
  including #298, #301-#303, #305-#306, and #322, rather than a stable facade
  shielding those implementations.
- **Scope/dependencies/WIP:** the proposed work is architectural debt reduction;
  the audit did not attribute a production incident to module length. The
  `audit-fixes` WIP touches all three modules and many adjacent controller files,
  reinforcing change-collision risk but not proving its changes correct.
- **Recommendation:** keep open. Freeze behavioral tests, then extract by owned
  seams: route DTO/validation groups, catalog normalization and transaction
  helpers, and fold row schemas/merge helpers. Keep moves separate from behavior
  changes and preserve small stable facades.

#### #291 — `arco-api` consumes flow implementation types rather than a contracts-only seam

- **Verdict:** **Enhancement gap verified**. **Confidence: high.** The boundary
  drift is real, but this issue requests a design and enforcement seam rather
  than repairing a demonstrated runtime failure.
- **Evidence:** `crates/README.md` says cross-crate interaction must use explicit,
  versioned contracts. Production API modules directly import flow compactor,
  ledger, event, state, controller, and row types from
  `control_plane_transactions.rs`, `routes/orchestration.rs`, `routes/tasks.rs`,
  `routes/manifests.rs`, `system_tables.rs`, `orchestration_compaction.rs`, and
  `server.rs`. Examples include `FoldState`, `MicroCompactor`, `RunRow`,
  `OrchestrationEvent`, `LedgerWriter`, and concrete controller types.
- **History/dependencies/WIP:** API and flow have evolved together across the
  orchestration parity/correctness series, so a hard split without first naming
  the service contract would be destabilizing. `audit-fixes` overlaps most
  import sites and flow internals, but does not establish a landed boundary.
- **Recommendation:** keep open. Introduce versioned request/response, state-read,
  and compaction service contracts; keep fold rows and storage implementations
  private; then add an architecture test that permits only the named contract
  modules across the API-to-flow boundary.

#### #292 — replay protocols differ intentionally as well as accidentally

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high.**
  Four replay/watermark implementations exist, but current architecture
  explicitly documents domain-specific ordering and late-event semantics. A
  forced single cursor implementation would violate those constraints.
- **Evidence:** ADR-039's replay table distinguishes Tier-1 explicit event-path
  replay, Tier-2 listed/explicit ledger replay with late-event policy,
  orchestration's committed-versus-visible watermarks, and numeric-sequence
  metastore publication. It states that these surfaces share immutable,
  CAS-selected publication but do not share one replay cursor, and prohibits
  substituting flow or metastore watermarks across domains without migration
  proof.
- **Scope/dependencies/WIP:** the issue is correct that shared vocabulary and
  equivalence obligations are not centrally enforced. The broad WIP touches
  catalog writers and flow compaction, but its overlap is not evidence that
  cross-domain cursor consolidation is safe.
- **Recommendation:** keep open with revised acceptance criteria. Standardize
  invariant names, replay-cut/freshness reporting, property and conformance
  tests, and migration proof obligations. Consolidate an implementation only
  when two domains demonstrate identical ordering, late-event, visibility, and
  rebuild semantics.

#### #331 — CI omits all-target Clippy coverage

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** Low
  testing severity remains appropriate; the cited three lint errors were only a
  stale subset of the current blocker inventory.
- **Tested/actual:** at `c3c0867`, the exact workflow command
  `cargo clippy --workspace --all-features -- -D warnings` passed in 10m28s.
  Adding the omitted `--all-targets` failed. Its first workspace failure was six
  `clippy::print_stdout` diagnostics in
  `arco-worker-contract/examples/embedded_protocol.rs`; focused
  `cargo clippy -p arco-proto --all-targets -- -D warnings` reproduced the three
  reported panic lints plus two `unnecessary_wraps` and one
  `cast_possible_wrap` diagnostic.
- **Scope/dependencies/WIP:** this proves the declared workspace lint policy is
  not the enforced test/example/bench policy; it does not imply that idiomatic
  assertion panics are product defects. `audit-fixes` adds a second all-targets
  job with a debt-exclusion list.
- **Recommendation:** keep open. Decide explicit target-specific allowances,
  inventory the full debt, add `--all-targets` with a visible burn-down, and
  prevent new exclusions from silently expanding.

#### #334 — Control MVP replay and manifest cost grow with history

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** Medium
  promotion-blocker severity is appropriate; this prototype has no production
  caller on the frozen baseline.
- **Executable evidence:** temporary counting instrumentation in
  `state_store_control_mvp.rs` ran under
  `cargo test -p arco-catalog --test state_store_control_mvp
  audit_replay_and_manifest_growth_are_linear_in_history -- --nocapture` and was
  then removed. Across commits 1-8, begin-time GETs were 0, 3, 4, 5, 6, 7, 8,
  and 9; manifest bytes were 638, 853, 1,014, 1,175, 1,336, 1,497, 1,658, and
  1,819. A read after eight commits made 10 GETs, and cumulative manifest bytes
  written were 9,990.
- **Code path/scope/WIP:** `replay_manifest` starts from default state and loads
  every transaction reference; each manifest embeds the cumulative reference
  list. `ControlMvpStateStore` remains a test/promotion slice. `audit-fixes` adds
  a bounded-replay regression and materialized base-snapshot design, which is
  WIP only.
- **Recommendation:** keep open as a promotion gate. Start replay from a selected
  checkpoint/base snapshot, bound suffix references and object reads, preserve
  historical reads, and state whether conflict detection is fine-grained or
  whole-scope CAS.

#### #335 — query timeout begins after snapshot/system-table registration

- **Verdict:** **Code-path confirmed**. **Confidence: high for excluded request
  work, medium for a concrete exhaustion threshold.** This is request-budget
  reliability, not an access-control vulnerability.
- **Evidence:** `routes/query.rs` validates an allowlisted SQL relation set,
  creates tenant/workspace-scoped storage, then awaits
  `register_snapshot_tables` and `register_system_tables`. Only the later
  `df.collect()` is wrapped in the ten-second `QUERY_TIMEOUT`. Object reads,
  Parquet decode, registration, and query planning therefore do not consume the
  advertised timeout budget. No unbounded live query or deployed endpoint was
  exercised.
- **Mitigations/scope/WIP:** SQL length, relation allowlisting, row count, and
  response size are bounded, and scoped storage preserves isolation. The audit
  has not measured the issue's OOM scenario or established a safe byte threshold.
  `audit-fixes` wraps registration/planning/collection together and adds
  `query_registration_timeout.rs`; this is overlap, not baseline evidence.
- **Recommendation:** keep open. Apply one wall-clock budget to the entire
  request, then add independently observable registration bytes and a scoped
  concurrency/memory admission policy.

#### #350 — orphaned JWT verifier is outside the crate module tree

- **Verdict:** **Code-path confirmed**. **Confidence: high.** This is low
  maintainability and auditability debt, not a live authentication weakness.
- **Evidence:** `crates/arco-api/src/auth.rs` defines a JWKS-backed `JwtVerifier`,
  but `lib.rs` declares no `mod auth`, and repository search found no
  `crate::auth`, `auth::`, or `JwtVerifier` consumer outside the file. Rust does
  not parse it during normal check/lint. The live bearer-token path is in
  `context.rs` and accepts configured HS256 or RS256 material; internal OIDC has
  a separate verifier.
- **Scope/history/WIP:** the issue's statement that the orphan would not compile
  is a strong source inference from missing config/context fields, not an
  executed compiler result. Secret Manager's `latest` reference permits
  revision-based rotation, although the live path lacks overlapping-key/JWKS
  rotation. `audit-fixes` deletes `auth.rs`.
- **Recommendation:** delete the orphan, or deliberately port and wire a tested
  JWKS contract. Do not credit dead safeguards in threat models; if orphaned
  source is a recurring risk, add a narrow module-reachability check.

#### #355 — duplicate signed-URL paths scale physical signing work

- **Verdict:** **Confirmed by executable repro**. **Confidence: high for linear
  work, medium for deployed denial-of-wallet impact.** Keep medium
  performance/availability severity.
- **Executable evidence:** a temporary local-only counting backend wrapped
  `MemoryBackend`; no external signer or deployed endpoint was called. Under
  `cargo test -p arco-api --test browser_e2e
  audit_duplicate_paths_scale_physical_signing_calls -- --nocapture`, request
  sizes 1, 8, 64, and 256 produced exactly 1, 8, 64, and 256 physical signing
  calls and response entries. Instrumentation was removed.
- **Scope/mitigations/WIP:** the JSON body limit is only an implicit coarse cap;
  allowlist membership, tenant auth, and per-request rate limiting do not
  deduplicate or charge per path. Path count is logged/audited, correcting the
  issue's invisibility claim; the metric still increments once per request.
  Whether deployed GCS uses remote `signBlob` and its exact cost remains
  externally unverified. `audit-fixes` adds caps/deduplication tests and code.
- **Recommendation:** keep open. Set a deliberate maximum, deduplicate before
  signing, charge and meter physical paths, and separately verify the deployed
  signer/IAM mode through approved passive configuration evidence.

#### #365 — one hermetic Python integration file is unreachable from CI

- **Verdict:** **Confirmed by executable repro**. **Confidence: high.** Keep low
  testing-hygiene severity; the original cross-language consequence was
  overstated.
- **Evidence:** `ci.yml` runs `tests/unit` and only
  `tests/integration/test_cli_api.py`; no workflow or script names
  `tests/integration/test_e2e.py`. Running
  `/tmp/arco-open-issue-audit-pyenv/bin/pytest
  tests/integration/test_e2e.py -vv` from `python/arco` collected and passed all
  8 tests in 1.13s.
- **Scope/mitigations/WIP:** CI-run unit tests already cover most discovery,
  manifest, CLI, and the true Python/Rust canonical-JSON contract. The unique
  value is narrower end-to-end on-disk deploy/validate and combined discovery
  behavior; its “fingerprint stable” assertion compares two same-process builds,
  not a Rust golden. `audit-fixes` switches CI to bare pytest and adds collection
  coverage.
- **Recommendation:** keep open. Run the configured `tests` tree or explicitly
  include the whole integration directory, and add a guard that new test files
  cannot be silently omitted.

#### #366 — credentialed IAM/backend conformance can pass without executing

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high.**
  The skipped-green gap is current; normal CI still compiles the feature-gated
  tests and has meaningful declarative/in-process coverage, so the issue's
  “nothing covers the invariant” framing is too broad.
- **Frozen-baseline evidence:** listing the ignored `iam_smoke` suite found six
  tests, including API allow/deny checks for commits, ledger, locks, and state.
  `ci.yml` reports success after an echo when its key/bucket pair is absent.
  The GCS workflow has a separate OIDC/key/skip matrix. The frozen tree contains
  no equivalent scheduled S3 workflow.
- **Fresh passive observation:** scheduled run `30689674883` on 2026-08-01 used
  baseline SHA `c3c0867` and concluded success in about 29 seconds, but its log
  selected `auth_mode=skip` and printed that the test bucket and GCP auth were
  missing. Thus no real GCS CAS/precondition test executed. No live IAM or
  storage test was run by this audit.
- **Mitigations/WIP:** normal all-feature test/check compiles `iam_smoke`;
  Terraform tests cover conditional prefix bindings; in-process conformance
  covers backend-agnostic put logic. The backend-specific cloud error mapping
  and deployed IAM bindings still need credentialed proof. `audit-fixes` makes
  unrun gates prominent/optionally required and adds an S3 workflow.
- **Recommendation:** keep open or split by invariant. Make “configured and ran”
  an explicit required artifact, retain credential-free negative prefix tests,
  add scheduled S3 conformance, and fail half-configured environments. Execute
  the ignored cloud suites only in an approved disposable fixture project.

### Deployed UAT and live operator boundaries

This batch distinguishes repository capability from deployed truth. PR #304
(`4f421510`) landed the main repo-side guardrails and passed its CI, extended,
Terraform, docs, and security checks. Those checks do not substitute for a
deployed success artifact. Fresh passive observations below were collected on
2026-08-01 from project `arco-testing-20260320`, region `us-central1`, without
calling a service endpoint, reading bucket objects, or changing cloud state.

#### #218 — live-gated external-worker acceptance proof

- **Verdict:** **Externally unverified / needs live evidence**. **Confidence:
  high that the harness exists; no confidence claim that the deployed pipeline
  currently passes.**
- **Frozen baseline:** the ignored deployed UAT now drives public API/catalog
  setup, manifest/run creation, real worker dispatch/callbacks, system-table
  queries, provenance capture, and success/failure evidence. The runner keeps it
  separate from deterministic CI, requires explicit env/provenance, and validates
  only fresh `deployed_api_worker_*.json` success artifacts.
- **Historical/current evidence:** live durable-storage proof succeeded in June,
  but the last deployed run timed out with one root task `READY` and two
  `BLOCKED`; no accepted success artifact was produced. The current passive
  snapshot is still incoherent: old dirty images, mixed tenant/workspace scope,
  mostly public ingress, and Scheduler metadata unavailable because billing is
  disabled.
- **WIP overlap:** the dirty `batch4-live-uat` worktree is the pre-merge precursor
  to #304; `audit-fixes` changes worker and UAT paths further. Neither is deployed
  proof.
- **Safe follow-up:** after an authorized project owner restores Scheduler
  billing, reserve one `ARCO_DEPLOY_OWNER`, run
  `repair-deployed-uat-prereqs.sh --dry-run --run-live-deployed` and review it,
  then run the same wrapper in an approved mutation window with
  `--status-output-dir`. Accept only a fresh artifact that passes
  `validate_user_acceptance_evidence.sh --require-kind deployed_api_worker` and
  matches the expected image, SHA, revision, tenant, and workspace.

#### #231 — deployed `system.catalog` query visibility

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  the historical stale-deployment failure and frozen code path; externally
  unverified for the current deployed response.**
- **Frozen baseline:** `/api/v1/query` allowlists and registers
  `system.catalog.{catalogs,namespaces,tables,columns,commits,snapshots,transactions}`
  from manifest-selected artifacts. Local API/UAT tests exercise those tables,
  so the June “table not found” response is not the behavior of `c3c0867` under
  equivalent storage/config.
- **Historical/current evidence:** the reported deployment failed planning for
  `system.catalog.tables` before provenance was available. Current Cloud Run
  metadata identifies a June `02234a2-dirty-*` API image, not the frozen audit
  commit. The audit did not invoke `/api/v1/query`, so it cannot tell whether the
  current revision exposes the table.
- **WIP overlap:** `batch4-live-uat` and `audit-fixes` both modify the UAT/query
  proof surface; baseline registration is already landed independently.
- **Recommendation/follow-up:** keep open until a provenance-pinned deployed gate
  creates a uniquely named table, queries it through `system.catalog.tables`,
  records namespace/column rows, and validates the fresh success artifact. Do
  not diagnose a current query bug from the stale June response alone.

#### #232 — API build provenance for deployed triage

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  the repository contract and passive env wiring, externally unverified for an
  endpoint response tied to an accepted run.**
- **Frozen baseline:** `GET /version` returns service/package/code version, Git
  SHA, image, and Cloud Run revision with explicit `unknown` defaults. Terraform
  and direct revision refresh wire the build fields, and the UAT runner rejects
  missing, unknown, or unexpected provenance before writing evidence.
- **Historical/current evidence:** the old deployment originally returned 404.
  Current passive metadata now includes `ARCO_CODE_VERSION`, `ARCO_GIT_SHA`, and
  `ARCO_API_IMAGE`, but they describe a June dirty image; endpoint invocation was
  deliberately not performed. Cloud Run supplies `K_REVISION` at runtime, which
  cannot be confirmed from the static env list alone.
- **WIP overlap:** both Batch 4 worktrees and `audit-fixes` touch server/UAT
  provenance paths; #304 is the frozen-baseline authority.
- **Recommendation/follow-up:** keep open until a fresh pinned revision returns
  all six non-unknown fields through the approved UAT access path and the exact
  values appear in a validated success or diagnostic failure artifact.

#### #233 — evidence paths depended on Cargo's working directory

- **Verdict:** **Already fixed or superseded** by #304. **Confidence: high.**
- **Frozen baseline:** `uat_evidence_dir()` converts relative configuration to
  `${ROOT_DIR}/...` before creating the freshness marker, passing the path to
  Cargo, and invoking the validator. Shell regressions assert the same absolute
  path for durable and deployed modes. The accepted historical durable run also
  validated an artifact in the repo-root target directory after this change.
- **Current deployed relevance:** no live service or storage access is necessary
  to prove path identity; a future live gate can still fail for unrelated
  readiness reasons.
- **WIP overlap/recommendation:** the dirty Batch 4 precursor contains the same
  family of changes. Close #233 against merge `4f421510`; retain the runner test
  so Cargo cwd cannot regress.

#### #234 — redeploy and capture deployed API/worker evidence

- **Verdict:** **Externally unverified / needs live evidence**. **Confidence:
  high that the issue remains incomplete.**
- **Frozen baseline:** #304 supplies provenance checks, single-owner locking,
  repair dry-runs, strict readiness, status snapshots, post-update verification,
  and optional continuation into the live evidence-producing gate.
- **Historical/current evidence:** every recorded June pass stopped before a
  valid `deployed_api_worker_*.json`. On 2026-08-01 all seven relevant services
  shared the same stale historical owner label, but their images/scopes/ingress
  were a mixed historical environment rather than a coherent baseline deployment.
  Scheduler listing failed `BILLING_DISABLED`; retained Cloud logs contained no
  current dispatcher/Scheduler proof.
- **WIP overlap:** `batch4-live-uat` is a dirty historical execution worktree;
  `audit-fixes` contains later worker/auth changes. No worktree proves deployed
  completion.
- **Safe follow-up:** use the #218 sequence, beginning with billing/readiness and
  a reviewed dry run. Record initial/final status snapshots in the owner window,
  verify 100% traffic and exact image/env after every refresh, then run the full
  gate and validate the fresh artifact. Do not reuse June artifacts or current
  owner labels as proof of exclusive present ownership.

#### #235 — deployable flow worker ownership

- **Verdict:** **Already fixed or superseded** on the frozen baseline.
  **Confidence: high.**
- **Frozen baseline:** the repository owns
  `crates/arco-api/src/bin/arco_flow_worker.rs`; the Cloud Run image builder
  accepts `arco_flow_worker`, and xtask coverage asserts the expected build
  substitution while rejecting unknown binaries. Baseline CI compiled/tested
  the merged worker path.
- **Scope/current evidence:** the final ownership differs from the issue's early
  proposed `arco-flow`/separate-crate location, but it satisfies the operational
  choice. Current Cloud Run metadata names an `arco-flow-worker` image, although
  it is a later dirty June image and not acceptance proof.
- **WIP overlap/recommendation:** `audit-fixes` further changes the worker binary
  and protocol. Close the binary-ownership decision; track protocol/runtime
  changes in their specific issues rather than reopening #235.

#### #236 — Terraform plan against unmanaged live resources

- **Verdict:** **Already fixed or superseded** by #304. **Confidence: high for
  the deploy safety contract, not for adoption of existing state.**
- **Frozen baseline:** `deploy.sh` initializes Terraform, compares expected live
  Cloud Run/job/service-account/role/bucket resources with `terraform state
  list`, and stops before plan/apply with explicit import commands when a live
  object is unmanaged. A supported direct revision-refresh path handles the
  existing-dev-service case and verifies owner/image/env/latest-ready/traffic
  immediately and again at the end.
- **Current evidence:** the audit did not inspect Terraform state or plan/apply;
  those are unnecessary to verify the fail-before-mutation logic and outside the
  report-only boundary.
- **WIP overlap/recommendation:** the dirty Batch 4 precursor contains the
  pre-merge implementation. Close #236 as repo-fixed; importing/adopting a
  specific environment remains an authorized operator task, not an audit action.

#### #239 — single-owner deployed-UAT mutation window

- **Verdict:** **Already fixed or superseded** for the repository guardrail.
  **Confidence: high.** Human/process compliance remains operational.
- **Frozen baseline:** non-dry-run deploy, revision refresh, Scheduler repair,
  and combined prerequisite repair require a label-safe `ARCO_DEPLOY_OWNER` and
  a shared local lock. They reject mismatched live owner labels, keep the lock
  across repair plus optional UAT, capture pre/post status, and perform a final
  verification sweep for mid-run drift.
- **Current passive evidence:** all seven services carry the same historical
  owner label, but a persistent label cannot prove no other operator is active
  now; it only lets the scripts detect conflicting labels.
- **WIP overlap/recommendation:** Batch 4 WIP contains the original lock work.
  Close the repo-control issue, preserve the one-owner runbook, and require human
  coordination plus process/port checks before each mutation window.

#### #240 — internal-only Cloud Run access for deployed UAT

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high that
  the repository has a bounded proxy workflow; high that internal-only
  end-to-end reachability is not demonstrated.**
- **Frozen baseline:** the runner supports explicit `gcloud run services proxy`,
  records access/ingress mode, validates `/version` before evidence, traps proxy
  cleanup, and offers a read-only preflight. Revision refresh also supports a
  bounded `FLOW_COMPACTOR_INGRESS` selection with post-change verification.
- **Historical/current evidence:** June comments record that the local proxy
  could not reach internal API/flow-compactor paths; temporarily public ingress
  moved the gate forward. Current passive metadata shows API, catalog compactor,
  flow compactor, dispatcher, sweeper, and worker ingress `all`; only timer
  ingest remains internal. Thus the environment does not prove the desired
  internal-only workflow.
- **WIP overlap/recommendation:** both Batch 4 and `audit-fixes` overlap Cloud Run
  config. Keep open. In an approved owner window, first test the documented proxy
  preflight against internal ingress; if platform networking still blocks
  API-to-compactor calls, use a designed VPC/internal path. A temporary-public
  fallback must capture before/after ingress and restore it in the same window.

#### #241 — API service account invoker bindings

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high that
  declarative and current IAM bindings are present; externally unverified for a
  successful authenticated request.**
- **Frozen baseline:** Terraform grants the API service account
  `roles/run.invoker` separately on catalog and flow compactors, with focused
  Terraform tests. This fixes the original binding omission.
- **Current passive evidence:** both live IAM policies include
  `arco-api-dev@arco-testing-20260320.iam.gserviceaccount.com`. IAM membership
  alone does not attach an ID token and therefore cannot prove that the original
  Cloud Run 403 is gone.
- **WIP overlap/recommendation:** `audit-fixes` further modifies IAM and
  compactor auth. Keep open only for the live half or merge that half into #242:
  after readiness is coherent, use the normal API namespace/table operation in
  the approved UAT—never a crafted auth probe—and require absence of 403 plus a
  provenance-pinned result.

#### #242 — API sync-compactor identity-token authentication

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  code/config, externally unverified for current invocation.**
- **Frozen baseline:** `CompactorClient` supports none, static bearer, and GCP
  metadata identity-token modes; the GCP audience defaults to the service base
  URL, not `/internal/sync-compact`. Server construction passes
  `config.compactor_auth`, config parsing redacts tokens, Terraform/refresh set
  `ARCO_COMPACTOR_AUTH_MODE=gcp_id_token`, and tests cover headers/metadata.
- **Current passive evidence:** the API revision declares `gcp_id_token`, and
  #241's IAM bindings are present. The audit did not invoke either compactor, so
  token acquisition, audience acceptance, route reachability, and 403 removal
  remain unproved for this old revision.
- **WIP overlap/recommendation:** `audit-fixes` changes this client again. Keep
  open until the standard deployed UAT operation reaches both compactors with a
  pinned revision; capture only status/request IDs, never token material.

#### #243 — API-to-flow-compactor reachability

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  the historical symptom and public-ingress workaround, low for current
  internal-path behavior.**
- **Frozen baseline:** the client appends `/compact`, derives the ID-token
  audience from the service URL, and the service registers `POST /compact`.
  Deploy tooling can explicitly select flow-compactor ingress and verifies the
  resulting revision.
- **Historical/current evidence:** June's Google-frontend 404 did not reach
  service logs. A later single-owner pass with flow-compactor ingress `all`
  recorded `/compact` request IDs, so URL/route code was not the enduring cause.
  Current flow-compactor ingress is still `all`; retained logs returned no fresh
  evidence. Internal-only reachability therefore remains unresolved.
- **WIP overlap/recommendation:** Batch 4 WIP and `audit-fixes` touch this path.
  Keep open as an ingress/networking design issue, not a generic missing route.
  Follow #240's owner-window procedure and prove receipt in passive service logs
  while keeping token values and endpoint probing out of the audit.

#### #244 — cyclic orchestration manifest chain in dev storage

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  cycle detection and historical log recurrence; externally unverified for the
  stored-data cause and present state.**
- **Frozen baseline:** reconciler traversal tracks visited paths and returns
  `cycle detected in orchestration manifest chain at {path}` rather than looping
  forever. Tests cover manifest-chain traversal and failure reporting.
- **Historical/current evidence:** issue evidence records repeated June Cloud Run
  log occurrences at `state/orchestration/manifest.json`, including a recurrence
  after the later UAT window. The fresh retained-log query returned no rows,
  consistent with log retention; it does not refute the occurrence. Per the
  audit boundary, no live orchestration object was read and the cycle's data
  cause was not inspected.
- **WIP overlap/recommendation:** `audit-fixes` changes compactor/reconciler
  paths. Keep open. In an authorized storage-repair window, prefer a new isolated
  UAT tenant/workspace; separately copy the affected chain to immutable evidence,
  analyze it offline, and repair only from an approved runbook with before/after
  pointer evidence. Never overwrite the live pointer during diagnosis.

#### #245 — catalog-compactor UAT scope alignment

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high that
  repo wiring exists and current deployed scope is wrong; externally unverified
  for a successful aligned mutation.**
- **Frozen baseline:** Terraform wires catalog compactor tenant/workspace from
  explicit variables, direct refresh propagates the selected scope, and runner
  readiness checks compare every service against the requested UAT identity.
- **Historical/current evidence:** the issue's lock-path mismatch was observed
  live, later passed after a scoped refresh, and is therefore configuration
  dependent. The 2026-08-01 passive snapshot shows catalog compactor now at
  `tenant-proof2-20260604/workspace-proof2-20260604`, not the default UAT scope.
- **WIP overlap/recommendation:** both user WIP trees change compactor Terraform.
  Keep open for deployed alignment. Review `refresh-cloud-run-revisions.sh
  --scope-only --dry-run`, then in the single-owner window align it with all flow
  services and rerun strict readiness before any catalog mutation.

#### #246 — READY task was not dispatched

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high for
  the historical live symptom; insufficient current evidence to select one
  Scheduler/dispatcher cause.**
- **Frozen baseline:** Terraform defines minute dispatcher and five-minute
  sweeper jobs with POST `/run`, service-account OIDC, and base-service audiences.
  The runner fails preflight on missing/paused jobs, `/run` audience mistakes,
  wrong service account/method, or scope drift. Narrow repair scripts can resume
  only paused jobs and repair only target metadata under the owner lock.
- **Historical/current evidence:** the 2026-06-04 run recorded one `READY` root
  and no dispatch/worker activity; later passive status found dispatcher audience
  incorrectly included `/run` and the sweeper was paused. On 2026-08-01 Scheduler
  list/describe failed `BILLING_DISABLED`, and current dispatcher/Scheduler log
  queries returned no retained rows. Billing is now the first proven readiness
  blocker; the original exact delivery failure is not re-confirmed.
- **WIP overlap/recommendation:** Batch 4 WIP contains the repair/preflight
  precursor; `audit-fixes` changes controllers too. Keep open. After billing is
  restored, read-only describe both jobs; review the repair dry-run; repair in one
  owner window; then require Scheduler success logs, dispatcher `/run`, Cloud
  Tasks enqueue, worker receipt, and terminal task rows in the UAT artifact.

#### #247 — structured deployed-UAT timeout evidence

- **Verdict:** **Already fixed or superseded** by #304. **Confidence: high.**
- **Frozen baseline:** after a run ID exists, failure/timeout handling writes a
  sanitized `deployed_api_worker_failure_*.json` with provenance, access mode,
  scope, run identity, last run/task/dependency rows, stage/error, and timestamps;
  the returned error prints its path. The validator recognizes this kind but
  does not let it satisfy the success-kind requirement. Focused tests cover
  writing, path reporting, schema, and secret/token rejection.
- **Current relevance/WIP:** no live failure was triggered by this audit.
  `audit-fixes` changes related worker logging, but #247's structured artifact
  contract is already landed. Close it and retain the distinction between
  diagnostic failure evidence and acceptance success.

#### #248 — deployed flow-service scope mismatch

- **Verdict:** **Partially confirmed / scope adjusted**. **Confidence: high.**
- **Frozen baseline:** strict status and full-run preflight inspect catalog
  compactor, flow compactor, dispatcher, sweeper, timer ingest, and worker env
  scope before proxy, evidence, or Cargo work. The combined repair wrapper can
  align scope under the owner lock and verifies every updated service twice.
- **Historical/current evidence:** historical status found a mixed UAT/proof2
  deployment. The fresh passive snapshot still finds catalog compactor, flow
  compactor, dispatcher, sweeper, and worker on
  `tenant-proof2-20260604/workspace-proof2-20260604`; only timer ingest is on
  `arco-uat-tenant/arco-uat-workspace`. This directly confirms current
  not-ready state without calling the services.
- **WIP overlap/recommendation:** both WIP trees touch the same env wiring. Keep
  open. Use the same reviewed scope-only dry-run and single-owner repair as #245,
  require strict readiness to show all six services aligned, and only then run
  the deployed gate.

#### #249 — sandboxed Cloud SDK versus live-mutation authority

- **Verdict:** **Enhancement gap verified**. **Confidence: high.** Repo scripts
  enforce the mutation boundary, but the durable operator documentation does not
  plainly explain why passive `gcloud` may still require local filesystem
  escalation.
- **Frozen/current evidence:** `ARCO_DEPLOY_OWNER`, owner-label checks, locks,
  dry-run, preflight-only, strict status, and status-output artifacts distinguish
  observation from mutation operationally. The Batch 4 plan mentions that local
  Cloud SDK credentials can be unavailable, but it does not document credential
  DB/cache reads/writes, sandbox approval wording, or the rule that such local
  access is not itself GCP mutation. This audit reproduced the distinction:
  read-only `gcloud` needed sandbox escalation, while no cloud state changed.
- **WIP overlap/recommendation:** historical Batch 4 comments contain the missing
  guidance but comments are not a durable runbook. Keep open. Add a short operator
  section defining passive command classes, local SDK side effects, prohibited
  mutating commands without an owner window, status snapshot provenance, proxy
  port/process checks, and the exact `--require-live-deployed-ready` handoff.

## Shared-root-cause clusters and dependency order

| Root-cause cluster | Representative issues | Audit conclusion |
|---|---|---|
| Publication identity, visibility, and repair are not one lifecycle | #336, #343-#345, #357, #359-#362, #368 | Immutable files and CAS pointers are individually guarded, but attempt identity, deletion/retention, late events, projection publication, and recovery are not governed by one fail-safe lifecycle. The same gap creates wedges, silent loss, resurrection, stale markers, and missing authority. |
| Retry identity is inconsistent across API, controller, queue, and worker | #324, #328, #337-#338, #342, #346, #351-#352, #360, #367 | Idempotency/fencing is often correct inside one component but loses request hashes, entity identity, attempt identity, heartbeat ownership, or monotonic fencing at the next boundary. |
| Request-level limits do not bound physical work | #334-#335, #340-#341, #355-#356 | Timeouts, row limits, and per-request quotas are applied after registration/listing or without charging object reads, bytes, signings, and retained state. Observability must count physical operations, not only logical requests. |
| Security posture depends on optional/dead configuration and deployment drift | #325, #330, #333, #347-#350, #354, #358, #363-#364 | Several reported “bypasses” are narrowed by handler guards, but real defaults remain too permissive or misleading: report-only OIDC, dead validators/config, raw-artifact policy, direct secret injection, and a public debug deployment. |
| Green automation can mean “did not exercise the invariant” | #326-#327, #331, #365-#366 | Security, all-target lint, Python integration, IAM, and backend conformance have failure modes that are skipped, suppressed, or unreachable while the visible gate is green or chronically red. |
| Deployed acceptance lacks one coherent authority snapshot | #218, #231-#249 | Repository guardrails are substantially better after #304, but images, ingress, service scope, Scheduler state, IAM/auth, provenance, and evidence must be pinned in one owner window. Mixed snapshots cannot prove the user journey. |
| Architectural seams lag the domain model | #221, #290-#292, #362 | Access-backed redaction and projection publication are missing capabilities; API/flow coupling and monoliths make correctness changes collide; replay domains need shared invariants rather than an indiscriminate shared cursor. |

Recommended dependency order:

1. **Contain unsafe deployed and repair posture.** Review #364 immediately; stop
   public debug invocation, move/rotate #363's secret through an authorized
   process, and make #357 repair dry-run/disabled by default before exercising
   recovery.
2. **Restore catalog publication safety.** Fix #368 attempt identity, #336
   fail-closed catalog reads, and #324 fencing first; then correct #343/#344
   retention/GC so recovery cannot delete current evidence. Resolve #359-#361
   before enabling more Iceberg/Delta mutation surface, and wire #362 before
   credential vending.
3. **Repair orchestration/worker attempt ownership.** Address #328, #329, #332,
   #337, #338, #351, and #367 as one dispatch-to-terminal lifecycle. Add #345
   deletion semantics and #346 deterministic repair identity before relying on
   sweeper recovery.
4. **Install physical work budgets.** Bound #340/#341 state and logs, #335 full
   query time/memory, #355 signing operations, and #356 listing pages/bytes.
   Treat #334 as a hard promotion gate for the control MVP.
5. **Make CI truth-preserving.** Clear #326, give #327 accountable expiry, add
   #331 all-target coverage, execute #365, and make #366 distinguish configured,
   executed, and skipped cloud gates.
6. **Re-establish one deployed-UAT owner snapshot.** Only after the correctness
   and gate work above, restore Scheduler billing, align service scope and
   ingress, verify IAM plus ID-token auth, isolate or repair #244's state, and run
   #218/#234 to a provenance-pinned artifact.
7. **Deepen architecture after behavior is fenced.** Define #221's access
   contract and #291's service contracts, then stage #290 extractions and #292
   cross-domain conformance vocabulary without mixing moves with behavior fixes.

### Material severity and scope corrections

- **Raise/keep urgent:** #368 is a deterministic workspace-wide DDL wedge;
  #336 is a fail-open catalog-loss boundary; #324's fencing reset is more serious
  than its medium label; and #364 deserves high operational priority because the
  passive deployment contradicted the issue's internal/no-public-invoker caveat.
- **Keep high but distinguish signal from exploitability:** #326 is high because
  the security signal has been unusable for weeks; the two dependency advisories
  themselves are not RCE-class. New PR #372 is relevant WIP, not a fix until its
  locked audit and scheduled workflow pass after merge.
- **Lower security consequence:** #333's literal fallback is not reached by the
  normal HTTP dispatch path; #347's protected handlers still require context;
  #348 is dead config around an intentionally public metrics route; #349 is
  mainly startup validation plus duplicate deployed-auth scope; #358 is a latent
  location-validation boundary while CRUD/vending defaults are disabled.
- **Reclassify rather than escalate:** #335 is request-budget reliability, not
  tenant isolation; #350 is dead-code/auditability debt, not a live JWT flaw;
  #334 is a prototype promotion blocker, not current production latency; #365 is
  CI hygiene with narrower unique coverage than reported.
- **Do not overclaim measured scale:** #341 proves retained-state/base-rewrite
  growth but its executable fixture was nonterminal; #356 proves full-list
  allocation but not the issue's 2M-object OOM estimate; #355 proves linear
  signing work but not the current deployed signBlob mode or quota cost.

### WIP overlap at final refresh

- The user-owned `audit-fixes` worktree is broadly dirty and overlaps most
  current findings: compaction/reconciliation, worker retries/logging/heartbeats,
  query budgets, signed URLs, CI gates, IAM/conformance, and deployed UAT. It is
  evidence of active work only; no dossier treats it as baseline behavior.
- `batch4-live-uat` remains a dirty, behind-main precursor to merged PR #304.
  `batch4-live-uat-20260625` also contains later uncommitted worker/auth/IAM
  changes. Neither worktree is a deploy snapshot.
- Open PR #372 overlaps both Python advisories in #326; #300 overlaps only
  `pydantic-settings`. PR #370 updates action pins, not #366's skip semantics.
  PR #369 changes Rust dependency locks but leaves the audited `quick-xml 0.37.5`
  occurrences. Open dependency PRs require their own locked verification.
- `origin/main` did not move from `c3c0867`, so none of this WIP changed a frozen
  verdict or triggered a reproduction rerun.

### Claims intentionally left externally unverified

- **Full live acceptance:** #218/#234 have no validated deployed success
  artifact. Consequently fresh-revision query visibility (#231), endpoint-bound
  provenance (#232), internal-only access (#240), authenticated compactor calls
  (#241-#243), aligned catalog/flow scope (#245/#248), and READY-to-worker
  dispatch (#246) are not accepted as complete.
- **Stored-data cause:** #244's cycle detector and historical log occurrence are
  confirmed, but the live object chain, cause, and repair outcome were not read or
  changed.
- **Credentialed cloud semantics:** #353's live bucket-root IAM rejection,
  #355's actual GCS signer/signBlob path and quota, and #366's real GCS/S3
  precondition plus IAM smoke behavior were not actively exercised. The latest
  GCS workflow explicitly skipped; S3 has no frozen-baseline workflow.
- **Security consequences requiring hostile or secret-bearing input:** no live or
  local server was sent a crafted #325/#330/#347/#354/#358 request; no secret
  value or secret-manager payload was retrieved for #363/#364. Those verdicts
  rely on code/config and passive metadata, not exploit execution.
- **Scale ceilings:** the exact terminal-only #341 fixture, #356's proposed
  multi-million-object/OOM threshold, and a safe global memory threshold for
  #335 were not measured. Their mechanisms are proven; their claimed outage
  sizes are not.
- **Current Scheduler/log state:** billing disabled prevented Scheduler
  list/describe on 2026-08-01, and retained dispatcher/Scheduler/cycle log queries
  returned no rows. Historical evidence is preserved, but absence after retention
  is not evidence of repair.

## Prioritized action list

### P0 — contain loss, wedges, and exposed dev posture

1. Fix #368, #336, and #324 with deterministic crash/fencing regressions.
2. Disable or dry-run #357 by default; correct #343/#344 before enabling cleanup.
3. Review and close #364's public-debug deployment boundary immediately; migrate
   and rotate #363 through approved secret-management operations.
4. Fix #328 worker dedup/ownership and #337 first-failure retry bootstrap before
   trusting redelivery or sweeper recovery.

### P1 — restore reliable execution and truthful gates

1. Complete the orchestration lifecycle group: #329, #332, #338, #345-#346,
   #351, and #367, with task-local logs, authenticated uploads, tombstones,
   deterministic repair identity, request-hash checks, and heartbeats.
2. Add physical budgets and metrics for #340-#341, #335, #355-#356; keep #334
   blocked from promotion until replay and manifest work are bounded.
3. Repair #326/#327 and merge only after fresh locked/scheduled proof; add #331,
   #365, and explicit configured-versus-skipped #366 gates.
4. Resolve #359-#362 and define #354/#358 artifact/location policy before
   widening Iceberg, Delta, signed-file, or credential-vending capability.

### P1 live gate — only in an authorized single-owner window

1. Restore project billing so Scheduler can be inspected, then run the strict
   read-only readiness snapshot.
2. Review the combined repair dry run; align all service scope, owner, images,
   ingress, Scheduler state/audience, and IAM/auth without concurrent sessions.
3. Prefer a fresh UAT tenant/workspace over mutating #244's suspect state.
4. Run #218/#234 once, retain initial/final status plus structured failure if it
   fails, and accept success only after validator/provenance equality.

### P2 — simplify and document after correctness is stable

1. Close repo-fixed #233, #235, #236, #239, and #247 with merge evidence; merge
   #241's live half into #242 if separate issue ownership no longer helps.
2. Remove dead #348/#350 configuration/source or wire it deliberately; make
   #325/#330 authentication startup fail closed.
3. Define #221 access projections/redaction, #291 contracts-only state APIs, and
   #292 shared replay invariants; then stage #290 module extractions.
4. Add #249's durable Cloud SDK/sandbox/operator-boundary documentation and keep
   proxy/owner/status evidence instructions copy-pasteable.

## Final verification

- `git fetch origin main` left `origin/main`, audit `HEAD`, and their merge base
  equal to `c3c0867cc2a6028f31df0a83da42cd4221695302`; the changed-path set was
  empty.
- Live GitHub set comparison returned `live_count=68`, `initial_count=68`,
  `missing_from_live=[]`, `new_live_issues=[]`, `equal=true`.
- Independent report parsing found 68 unique matrix rows and 68 unique dossier
  headings with no set difference. Verdict totals recomputed to 18 executable,
  14 code-path, 23 partial, 6 fixed, 5 enhancement, and 2 external.
- `git diff --check` exited zero for tracked state. Because the report is
  intentionally untracked, a separate trailing-whitespace scan over the report
  returned no matches. The unresolved-marker scan returned no actionable
  matches.
- `git status --short --branch` showed detached `HEAD` plus only
  `?? docs/reports/2026-07-31-open-issue-independent-audit.md`. No production
  source, config, test, generated file, index entry, commit, or branch changed.
- No repository Markdown-lint or non-building doc-link checker was present for
  this standalone report. The structural/set/whitespace checks above are the
  applicable non-building documentation checks.
- Baseline `cargo xtask repo-hygiene-check` and `cargo xtask adr-check` had passed
  before the final report pass. Fresh Rust-backed hygiene was **not rerun**:
  final recorded free space was 22 GiB, below the audit's 40 GiB target
  creation/rebuild
  gate, while an unrelated Rust test was actively building in the retained
  `control-store-idempotency-grants-ddl-pilots` worktree. That process was not
  stopped or cleaned. This is a disk-gated omission, not a passing fresh Rust
  verification claim.
