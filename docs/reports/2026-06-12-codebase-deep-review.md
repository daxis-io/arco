# Arco codebase deep review — critical, prioritized assessment

**Date:** 2026-06-12
**Branch reviewed:** `chore/docs-updates` (including uncommitted ADR-041 working-tree changes)
**Scope:** Full workspace (~196k LOC, 12 crates + xtask), weighted toward data-loss/corruption, storage-concurrency correctness, tenant isolation, and orchestration state-machine bugs.

## Method & confidence

Produced by a multi-agent review: ~30 scoped reviewers swept the workspace across architecture, correctness, API design, performance, testing, security, and maintainability, then independent adversarial verifiers attempted to *refute* each load-bearing finding by tracing it end-to-end.

- **Wave 1** (catalog correctness, dependencies/supply-chain, + gap rounds: Python worker, Terraform/IAM, orchestration clock/singleton assumptions): 17 confirmed, 3 refuted.
- **Wave 2** (flow orchestration, flow compactor, API correctness, Iceberg/UC + GC, Rust security surface, API design, perf/testing, architecture/maintainability): 50 findings; the 16 highest-impact independently verified — 14 confirmed, 2 refuted.

Confidence labels below:
- **Confirmed** = traced end-to-end by a separate agent trying to refute it.
- **Unverified** = single-reviewer claim, cites real lines, but not independently re-checked (verification budget exhausted on session limits). Treat as leads, not facts. Base rate: of the 16 verified, 14 held.
- **Refuted** = checked and did not hold up; listed so they aren't re-chased.

---

## The 5 highest-impact issues

### 1. Orchestration retry & zombie recovery is never bootstrapped — failed tasks and crashed workers wedge forever
**`crates/arco-flow/src/bin/arco_flow_automation_reconciler.rs:200`** — Confirmed (verified twice), HIGH

Failed-with-attempts-remaining tasks move to `RetryWait` (`fold.rs:1850`). Recovery from `RetryWait` needs a `Retry` timer; zombie reaping of crashed-worker `Running` tasks needs a `HeartbeatCheck` timer. **No deployed binary ever emits the initial `TimerRequested` for either.** The only producer (`reschedule_timer_event`) is reachable solely from `process_fired_timers`, which is filtered to already-`Fired` timers — a consumer with no producer. The anti-entropy sweeper ignores both states (`anti_entropy.rs:191-214`, with a test pinning that `Running` is ignored), and the legacy in-process `Scheduler` that handles retries is wired into no binary. Worse: `fold_run_cancel_requested` skips `Running` tasks, so even manual run-cancel won't unwedge a crashed-worker zombie.

**Fix:** Extend `AntiEntropySweeper` to (a) redispatch `RetryWait` tasks past backoff via `DispatchRequested` attempt+1, and (b) fail `Running` tasks whose `last_heartbeat_at`/`started_at` exceeds `heartbeat_timeout_sec + grace`. Add a test: fail attempt 1 of a 2-attempt task, assert a second dispatch. **First confirm whether this is a known-unbuilt slice** — the dead handlers look production-ready and no TODO/ADR acknowledges the gap.

### 2. Catalog reconciler can delete a live commit's in-flight snapshot files, corrupting the published catalog
**`crates/arco-catalog/src/reconciler.rs:257`** — Confirmed (verified twice), HIGH — plus two sibling tier1 bugs

`check()` marks any file under `snapshots/{domain}/` not referenced by the *current* manifest as repairable — including v(N+1) files an in-flight commit wrote before its pointer CAS. `repair_with_scope(Full)` deletes them, protected only by the currently-published manifest's paths, with no age grace. The `/internal/sync-compact` handler doesn't take the in-process compaction lock, so repair runs concurrently with commits. The `arco-compactor` binary runs this **Enforce/Full every 300s by default** (`main.rs:124-137`, matched by Terraform). A delete landing in the commit window leaves the pointer CAS succeeding against a manifest referencing **deleted Parquet** — a `Critical`, non-repairable `MissingSnapshot` state needing manual ledger recovery.

Siblings in `tier1_compactor.rs`:
- **`:970`** — the replay watermark filter compares non-monotonic event ULIDs (`EventId::generate()` = plain `Ulid::new()`). A DDL whose event ULID sorts below the prior watermark (cross-node clock skew or same-ms random regression) is silently treated as already-applied, returns success, and **the entity never exists** — acknowledged-write data loss. The codebase already solved this for *commit* ULIDs (`next_commit_ulid`) but never for event IDs.
- **`:412`** — a crash between snapshot write and pointer publish leaves orphaned v(N+1) files; every later DDL recomputes `next_version=N+1`, collides, and fails → catalog-wide DDL outage until repair (~300s). Indefinite if repair is disabled or no compactor stays alive.

**Fix:** Reconciler — add a deletion grace window (skip candidates whose `last_modified`, currently discarded in `list_files`, is younger than `max(lock TTL, ~15 min)`); never auto-delete versions *above* the current manifest in Full scope. Watermark — make ledger event IDs monotonic against the published watermark in `append_ledger_event`, or switch from ULID-ordering to membership in `state.commits`. Orphan-wedge — make the snapshot dir unique per attempt (include the reserved commit ULID), or delete the provably-unpublished orphans under the held lock and retry.

### 3. GCS credential vending hands the client a bucket-wide read-write OAuth token — tenant isolation break
**`crates/arco-iceberg/src/credentials/gcs.rs:226`** — Confirmed (verified), HIGH

With `iceberg.enable_credential_vending` on and `X-Iceberg-Access-Delegation: vended-credentials`, `vended_credentials` returns the **platform service account's raw OAuth2 token** (`devstorage.read_write`) verbatim. No downscoping — no STS Credential Access Boundary, no token exchange, no per-tenant SA — and `prefix` is only an advisory client hint. Since all tenants share one bucket under `tenant=.../workspace=.../` prefixes and the SA has bucket-wide access, **any tenant given vended credentials can read and write every other tenant's objects.** Reachable on read paths (`load_table` → `maybe_vended_credentials`), and the `delegation` field is ignored, so a read delegation still yields a read-write token. Contradicts the repo's own `credential-vending-security.md`.

Mitigated only by being opt-in (default false) + `gcp`-feature-gated — hence HIGH not CRITICAL.

**Fix:** Don't vend the bare SA token. Use GCS downscoped tokens via STS Credential Access Boundary bound to `tenant={t}/workspace={w}/` (and read-only scope for read delegations), or per-tenant SA/HMAC credentials. At minimum reject vending when the table location prefix isn't strictly within the requesting tenant's scope.

### 4. Every orchestration event triggers a synchronous full-state compaction under one global lock, with no run/task pruning
**`crates/arco-api/src/orchestration_compaction.rs:270`** — Confirmed (verified), HIGH

Each worker callback (including **per-heartbeat** events) writes one event via `append_event_and_compact`: acquire the single workspace-wide compaction lock → download the full base snapshot + all L0 deltas (~17 Parquet tables) → clone the entire state → fold **one** event → diff the entire state → write a delta and CAS-publish (full base rewrite every 10 deltas). Only `sensor_evals` and `idempotency_keys` are pruned; **runs, tasks, dep_satisfaction, timers, and dispatch_outbox are never removed.** Base-snapshot size — thus per-event cost — grows monotonically with all-time run count. No in-process cache.

Throughput ≈ 1/(full-state RTT + parse + diff + write), serialized by the global lock. Correctness tail: once callback latency approaches `heartbeat_timeout_sec` (default 60), the sweeper declares live tasks stuck → spurious redispatch. Worsened by the dead-outbox finding (`fold.rs:2142`).

**Fix (three independent levers):** (1) batch events per workspace (the ADR-041 L0 inbox bundles are the right vehicle; heartbeats should hit a lightweight side-channel, not the folded ledger); (2) version-pinned in-process `FoldState` cache keyed by manifest revision, revalidated by pointer ETag; (3) terminal-state retention so old runs/tasks/outbox rows leave the base snapshot. Coordinate with the ADR-041 slice.

### 5. Uncommitted ADR-041 work has hard land-blockers
Confirmed (verified), HIGH cluster — fix **before** committing

- **`python/arco/.../worker/server.py`** — the Python worker never reads `callback_task_id`, but the in-flight Rust dispatcher/sweeper now mint the callback token with subject = the `rt_`-prefixed `callback_task_id`, and the API validator requires `claims.task_id == url task_id` (`tasks.rs:376`). In production posture **every `started`/`completed` callback fails with `task_id_mismatch` and runs hang.** Masked locally by the debug validator. The new Rust worker handles it correctly — the two implementations have diverged.
- **`server.py:300` / `cli/main.py:277`** — `/dispatch` is **unauthenticated** and the CLI defaults `--host` to `0.0.0.0`. Anyone with network reach can POST a crafted envelope to execute arbitrary registered asset code, with attacker-chosen `callback_base_url` and `task_token` (falls back to the API key) — RCE + SSRF + key exfiltration. It also never validates envelope `tenant_id`/`workspace_id` against its own scope (the Rust worker does).
- **`infra/terraform/iam_conditions.tf:295`** — the working-tree diff **dropped the `startsWith("state/anti_entropy/")` condition** from the anti-entropy SA's write binding. That SA already has read-all + bucket list; it now has **bucket-wide create/delete/update across all tenants.** Separately, `iam.tf:270` gives `flow_timer_ingest` unconditional bucket-wide `objectUser` — the exact Gate-5 violation the file claims to have removed.

**Fix:** Align the Python worker to `arco_flow_worker.rs` (read `callback_task_id`, require an inbound shared secret with constant-time compare, default `--host` to `127.0.0.1`, validate envelope scope). Restore the IAM prefix condition and prefix-scope the timer-ingest grant.

---

## Per-area breakdown

### Correctness & edge cases

**Confirmed (verified):**
- **`control_plane_transactions.rs:540`** (HIGH) — a timed-out/5xx sync-compact is recorded as a definite `Abort`, but the commit may have published at t=29.9s with a lost response. The DDL becomes visible while `GetCatalogTransaction` reports `Aborted`; a same-key retry re-executes and returns **409 for the caller's own committed write**. Fix: distinguish definite (4xx/pre-append) from ambiguous (timeout/transport/5xx-after-append) failures; leave ambiguous as `Prepared` + `repair_pending`, verify by re-reading the manifest watermark; treat retry-time `AlreadyExists` for the same request_hash as replayed success.
- **`routes/orchestration.rs:2222`** (HIGH) — `create_backfill` appends+folds `BackfillCreated` *before* claiming the idempotency record. Two concurrent same-key requests both commit distinct backfills; the loser returns the winner's response but **its orphan backfill still executes all chunks in parallel** (fold ignores `client_request_id`; chunk run_keys embed the unique `backfill_id`). Fix: claim the idempotency record before appending, mirroring `ControlPlaneTransactionService`'s two-phase pattern.
- **`arco-iceberg/src/commit.rs:382`** (HIGH) — stale-marker takeover collides with the orphaned metadata file: the path is deterministic, so the recovery retry re-derives the path the crashed attempt wrote, hits `PreconditionFailed`, and **caches a false 409** for a commit that never landed. `arco-delta`'s coordinator handles this correctly (payload-equality). Fix: on `PreconditionFailed` in the takeover path, read/verify the existing file and proceed to pointer CAS, or make the location attempt-unique.
- **`callbacks/handlers.rs:647`** (HIGH→verifier MEDIUM) — `handle_task_completed` writes `TaskFinished` then `TaskOutputVisibilityChanged` as two non-atomic compactions; if the second fails, the retry hits the terminal-state guard (409) before re-emitting, so visibility state is **permanently lost** (a `requires_visible_output` run reports `Succeeded` even if the worker reported publication `Failed`). Fix: emit both in one batch via the existing `append_all` path, minding fold order.
- **`fold.rs:1140`** (MEDIUM) — `record_idempotency_key` runs *before* arm applicability, so a rejected event consumes the exact key its legitimate successor carries — silently, for 30 days. Fix: record the key only when the arm applied, or add discriminators (attempt_id, outcome) to the affected keys.
- **`compactor/service.rs:515`** (MEDIUM) — live fold runs in lock-acquisition order, but `rebuild_from_ledger_manifest` replays sorted by wall-clock `timestamp` (separately sampled from `event_id`). For order-sensitive CAS arms (`fold_sensor_evaluated`, `fold_backfill_state_changed`), a DR rebuild can produce state that **disagrees with the live projection**, including never-triggered runs. Fix: use `event_id` (ULID) as the canonical sort key everywhere, or persist a per-batch sequence; property-test fold determinism across batch splits.
- **Idempotency caching (wave 1, MEDIUM ×2):** `writer.rs:738` permanently caches transient `409`/`412` (lock contention, CAS exhaustion) as `Failed`, poisoning retries. `writer.rs:1144` — a crash after commit but before marker finalize converts a success into a permanently cached `AlreadyExists` 409. Fix: only finalize markers for deterministic terminal errors; for transient ones delete/leave-InProgress so retry can take over.
- **`gc.rs:495`** (MEDIUM) — `check_in_progress_action` inverts recovery: when the pointer already references the marker's new location (commit succeeded, crashed before finalize), it returns `MarkFailedThenDelete` and writes a false 409 — the opposite of `commit.rs:298-321`. Latent only because GC is unwired. Fix: finalize as `Committed`.

**Refuted (don't chase):**
- Schedule re-enable / A→B→A "silently dropped" — routes override the content-fingerprint key with a unique key right after `OrchestrationEvent::new`; an e2e test pins create→disable→enable. (The fingerprint *default* is a latent footgun for future callers, worth cleanup.)
- `commit_root_transaction` non-atomic — ADR-034 scopes cross-domain atomicity to root-token readers; the root token never becomes visible on abort; the path is documented and test-pinned.
- Ledger rebuild floor dropping crash-orphaned/skewed events — producers ack only after a *visible* fold, so excluded orphans were never acknowledged and re-derive identically.

**Unverified (single-source):** offset pagination cursors skip/duplicate under concurrent mutation (`orchestration.rs:1886`); Iceberg deterministic requirement failures leave the marker `InProgress` → 10-min 503 lockouts (`commit.rs:353`); `RemoveSnapshots` leaves stale snapshot-log entries Java engines may reject (`commit.rs:870`); UC table drop leaves an orphaned Iceberg pointer (`uc/routes/tables.rs:502`).

### Concurrency & state-machine hazards
- **`fold.rs:2142`** (Confirmed, MEDIUM) — `DispatchStatus::Acked`/`Failed` are **never set by the fold**; the controller branches handling them are dead, and outbox rows park in `Created` forever (unbounded growth, re-scanned every tick). Fix: implement the transitions + terminal pruning, or delete the dead states/branches.
- **`task_tokens.rs:150`** (Confirmed, MEDIUM) — `attempt_id` is `deterministic_attempt_id(dispatch_id)`, computable by any worker, and task tokens carry no attempt binding. A stale/compromised attempt-1 worker can compute attempt-2's `attempt_id` and POST a callback for an attempt it never ran. Fix: add `attempt` to `TaskTokenClaims`, mint per-dispatch.
- **Sweeper redispatch** (wave 1, MEDIUM) — `arco_flow_sweeper.rs:216` reuses the byte-identical Cloud Tasks task name, so within the dedup window the "repair" is a silent no-op that still writes false `DispatchEnqueued` events. Fix: salt the redispatch name with a deterministic repair epoch. (Verifier: duplicate events share an idempotency key and are dropped by the fold → misleading telemetry + bounded liveness delay, not corruption.)
- **No multi-instance / clock-skew tests** (wave 1, MEDIUM) — nothing interleaves two dispatcher/sweeper ticks against shared storage or injects producer clock skew, despite cross-instance behavior being load-bearing. `nightly-chaos.yml` (the suite that would cover this) is **fully disabled via `if: false` on every job**, and the cargo features it invokes don't exist — scaffolding masquerading as a gate.

### Security
- GCS credential vending (#3) is the headline.
- **Iceberg/UC routers bypass the rate limiter** (`server.rs:606`, unverified MEDIUM).
- **Internal storage error strings reflected verbatim in Iceberg/UC 500 bodies** (`iceberg/src/error.rs:242`, unverified LOW).
- **Vendored thrift `read_bytes`** (wave 1, unverified LOW) — backport omits upstream's `max_string_size` guard; `vec![0u8; len as usize]` allows a ~4 GiB allocation from an untrusted length. Only matters if externally-sourced Parquet metadata is ever decoded.
- **Supply chain otherwise strong** (verified): `deny.toml` strict, `cargo deny` green + CI-enforced + nightly, all GitHub Actions SHA-pinned, pure-rustls, one well-understood build script, signed-tag release pipeline with SBOM + provenance. The thrift backport is a faithful minimal port of the upstream GHSA fix.
- **Rust auth/scoping surface verified clean:** JWT pins algorithm (HS256 xor RS256), `nbf`/issuer/audience enforced in prod, tenant/workspace from verified claims (never headers); task tokens re-bound to URL task_id + tenant; `ScopedStorage::validate_path` rejects `..`/absolute/`%`/backslash/control chars; DataFusion restricted to single SELECT over allowlisted tenant-scoped tables; signed URLs allowlist-validated with a hard 1h TTL. IAM is the real exposure, not the app layer.

### Performance (unverified — single-reviewer, plausible)
- **`reader.rs:244`** — every API request builds a fresh `CatalogReader`, pays ~8 manifest GETs (all four domains even for catalog-only ops), downloads full snapshot Parquet for point lookups; the only cache is per-instance and never hits in Cloud Run. (The 2026-05-22 catalog read performance plan suggests this is known.)
- **`tier1_compactor.rs:399`** — catalog DDL rewrites the entire domain snapshot per commit; `commits.parquet` grows unboundedly and is fully rewritten every DDL → O(N²) cumulative write.
- The per-event compaction architecture (#4) is the verified one.

### Testing
- **`storage.rs:559`** (Confirmed, MEDIUM) — `MemoryBackend` (46 test files) diverges from production object stores: version tokens restart at 1 after delete+recreate (ABA — a stale token *succeeds* a CAS in tests but gets 412 on GCS); CAS-update-of-missing returns clean `PreconditionFailed` (vs S3/local-fs 404→`NotFound`); malformed tokens silently become "never matches." Fencing behavior for delete+recreate protocols (reconciler, GC, marker lifecycles) is validated against semantics production doesn't have. Fix: make tokens globally unique (ULID/monotonic), add the three missing conformance cases, run the suite against `object_store` LocalFileSystem in CI. **The conformance harness itself is a strength** — same CAS contract against MemoryBackend, object_store InMemory, and (opt-in) real GCS, including a barrier-coordinated two-writer race.
- **Fold-determinism property tests are weak** (`property_tests.rs:199`, unverified MEDIUM) — they compare only collection *counts* over 4 hardcoded same-timestamp events, so content-level nondeterminism in the 6k-line fold is invisible.
- **`commit_flow.rs:162`** (unverified) — Iceberg commit tests don't pin any single-table conflict case.

### API design (unverified — single-reviewer)
- **No `#[non_exhaustive]` on any public error/contract enum** (`arco-core/src/error.rs:13`) — every downstream `match` is a breaking-change tripwire pre-1.0; `Error` carries duplicate `NotFound` variants.
- **Orchestration event enum has no unknown-variant tolerance** (`events/mod.rs:229`, Confirmed→verifier MEDIUM) — the durable ledger contract is a tagged enum parsed with a hard `?`; one unrecognized `type` tag fails the whole compaction batch permanently. arco-api (writer) and the flow compactor deploy separately, so writer-before-reader skew during a rolling deploy is normal, and ADR-041 is adding event kinds. Fix: add an `Unknown { type, raw }` catch-all that fold skips/quarantines; mark `#[non_exhaustive]`; add a version-skew fold test.
- Idempotency conventions diverge per route; `trigger_run` ignores the `Idempotency-Key` header (`orchestration.rs:3818`). JSON casing splits camelCase vs snake_case inside one `/api/v1` (`tables.rs:76`). Catalog list endpoints are unbounded arrays while orchestration uses cursors (`catalogs.rs:69`). Compactor 409→412 rewrite conflates lock races with If-Match failures (`compactor_client.rs:157`).
- **Strengths:** proto breaking-change detection is genuinely wired (buf breaking + frozen baseline, self-verified by xtask); field reservation discipline consistent; ADR-041 `L0InboxBundle::new` is a model parse-don't-validate constructor.

### Architecture & maintainability (unverified — single-reviewer)
- **God modules:** `routes/orchestration.rs` (8,187 lines, 153 top-level items) owns run-lifecycle domain logic that belongs in arco-flow; `writer.rs` (6.2k) and `fold.rs` (6.1k) are single-file monoliths.
- **Boundary-rule violation:** arco-api consumes arco-flow compactor internals (`FoldState`/`MicroCompactor`/row types) as its de facto state API (`tasks.rs:43`) — contradicting `crates/README.md`'s "contracts only" rule.
- **Three divergent ledger-replay/watermark protocols have drifted** (`compactor.rs:280`): Tier-2 has late-event detection, Tier-1 doesn't, the flow compactor uses a third scheme.
- **~6k lines of dead orchestration code** still exported/compiled/CI-run (`lib.rs:90`: legacy-scheduler, `leader/`, `quota/`), with backing ADRs still Accepted.
- **Docs that lie:** `scoped_storage.rs:226` legacy helpers say "v0.2.x: Removed" — violated at v0.2.0. `crates/README.md` omits 6 of 12 crates and calls arco-compactor a Cloud Function (deployed as Cloud Run). Duplicate ADR-032 (two Accepted; adr-check doesn't enforce uniqueness).

---

## Genuine strengths (do not touch)

- **The pointer-CAS publish protocol with layered fencing** — immutable snapshots + a single CAS-updated pointer; fencing token validated up front, revalidated before publish, embedded in the pointer; CAS is the source of truth even if the lock fails. Best-engineered part of the system.
- `validate_succession` defense-in-depth; `put_state_if_absent` idempotent-write semantics.
- The idempotency module (UUIDv7 + RFC-8785 JCS hashing + stale-takeover with race detection).
- Lock-takeover binding to object version (avoids the read-then-overwrite race).
- Watermark-freshness gates that fail *safe* across all controllers.
- Schema-evolution-tolerant Parquet readers.
- Cross-language canonical-JSON / partition-key golden-vector parity tests.
- The storage-backend conformance harness (multi-backend CAS contract incl. two-writer race).

---

## What couldn't be verified (don't treat as fact)

- **Whether finding #1 is a deliberately-unbuilt slice** vs. a regression. Confirm with the orchestration roadmap owner before investing.
- **Reachability of several findings depends on deployment config** not inspectable from code: credential vending defaults off; whether non-default worker queues are used; deploy ordering of arco-api vs the compactor; actual heartbeat interval.
- **The ~34 unverified wave-2 findings** are single-reviewer claims (verification budget exhausted on session limits). Leads, not facts.
- **Object-store error semantics on real GCS/S3** (the `MemoryBackend` divergence) are inferred; the GCS conformance test that would settle it is `#[ignore]`-gated.
- **`commit_table` requires a UUIDv7 `Idempotency-Key` header** — vanilla Iceberg REST clients (Spark/Trino/PyIceberg) don't send it, which would 400 all standard-client commits. Confirm if open-client compat is a goal.

---

## Bottom line

The storage-commit core (pointer-CAS + fencing) and the Rust security/supply-chain posture are genuinely well-built — protect them. Risk concentrates in (1) the orchestration state machine, where recovery machinery is wired but never triggered and the per-event compaction model won't scale, (2) the catalog reconciler's lack of deletion safety, and (3) the in-flight ADR-041 work, which has commit-blocking bugs in the Python worker, dispatch auth, and IAM scoping. Recommended sequence: gate the ADR-041 commit on #5; schedule #1 and #2 as the next correctness work; redesign the per-event compaction architecture (#4) alongside the L0-inbox slice rather than patching it.
