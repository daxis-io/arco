# Retention step 2: worker horizon job and catalog outbox trim — implementation plan

> **Execution contract:** implement package-by-package with TDD, one implementer per package, a two-stage (spec, then quality) review after each package, and no package starting before the previous one is approved. Design: `2026-09-26-state-store-retention-design.md` (accepted); step 1 landed as PR #439 (authority format 9).

**Goal:** The scheduled control-store worker runs the `RetentionHorizon` maintenance job on every invocation and trims acknowledged catalog projection outbox records after each drain, with the outcome of both visible in summary logs and metrics. No adapter, projection, or restore-semantics changes (steps 3 and 4).

**Architecture:** The kernel already admits, constructs and publishes horizon jobs (`DurableMaintenanceWorker::prepare_horizon_at`) and already trims acknowledged outbox records (`ProjectionOutboxWorker::trim_acked`, ack-domain retirement first, then an exact-incarnation source trim). Step 2 wires both into `crates/arco-api/src/bin/arco_control_store_worker.rs`: the maintenance phase drives at most one consolidation job and then at most one horizon job per domain per run, sharing the persisted-identity recovery path, and a new `trim` phase runs between the drain and GC. The kernel gains only read accessors (job kind, purged counts) and three metric emitters.

**Tech stack:** Rust 1.88, Tokio, `metrics` crate, `tracing`; existing worker test harness (`MemoryBackend`, `SilentNotifier`).

**Ground rules for every package**
- Worktree `/Users/ethanurbanski/arco/.worktrees/state-store-retention-20260926`, branch `feat/state-store-retention-step2` (based on `origin/main` at 0240ecbe). Never `cd` elsewhere. Never `git stash`. Never push.
- Export `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`. Run cargo in the foreground with filters. NEVER run the whole arco-catalog suite; ALWAYS append `-- --skip older_cut_outbox_model --skip independent_reclamation_model --skip durable_maintenance_model_32_seeds --skip 32_seeds` to arco-catalog `--lib` and reclamation-schedule runs (the lead runs the long models once at `CARGO_PROFILE_TEST_OPT_LEVEL=1` at the end).
- TDD; never weaken an assertion.
- Stage new docs with `git add` before `cargo xtask repo-hygiene-check` (it scans tracked files only). Tracked files must not contain the tokens the hygiene check bans (agent or vendor names) or a literal plans-directory path outside that directory.
- Commit per task with `git add <files>`; end messages with the session's standard attribution trailer.
- Worker tests cannot inject the kernel clock (`arco-core`'s `test-utils` feature is not enabled for `arco-api` tests), so worker-level horizon tests exercise the expiry purge (`put_with_expiry` with a stamp two hours in the past; the purge cutoff is now minus one hour) and never the 30-day age bound, which the kernel's own tests and the durable model cover.
- The operator endpoint's refusal of catalog trims (`crates/arco-api/src/routes/control_store.rs`, "source-domain rebind and trim are unsupported") stays exactly as is. Only the worker trims the catalog outbox.

---

## Package A — kernel accessors and metric emitters (Task 1)

### Task 1: expose job kind and purged counts; emit publication, purge and trim metrics

**Files:** `crates/arco-catalog/src/state_store/control_mvp/maintenance.rs` (`PreparedMaintenance` :~1568, `MaintenanceProgress` :~1600, `LoadedJob::progress`, `publish_at` :~2950), `crates/arco-catalog/src/state_store/control_mvp.rs` (`ControlMvpMaintenanceOutcome` :~4700), `crates/arco-catalog/src/state_store.rs` and `crates/arco-catalog/src/lib.rs` (re-export `MaintenanceKind`), `crates/arco-catalog/src/metrics.rs`, `crates/arco-catalog/src/state_store/projection_outbox_acks.rs` (`trim_acked` :1657), `crates/arco-catalog/src/state_store/control_mvp/maintenance/tests.rs`.

Steps:
1. Failing tests first (kernel `maintenance::tests`): (a) `prepare_at` on a root with a pending consolidation intent yields a plan whose `kind()` is `MaintenanceKind::Consolidation` and `prepare_horizon_at` on a root with an expired row yields `MaintenanceKind::RetentionHorizon`; (b) `start_at`/`resume_at`/`advance_at` progress carries the same `kind`; (c) `publish_at` of a horizon job returns an outcome whose `kind()` is `RetentionHorizon` and whose `purged_counts()` equals the published manifest's certificate counts as an `(expired_rows, tombstones)` pair, while a consolidation outcome reports `Consolidation` and `None`. Run by name; expect FAIL.
2. Implement: `PreparedMaintenance::kind(&self) -> MaintenanceKind`; add `pub kind: MaintenanceKind` to `MaintenanceProgress` (set in `LoadedJob::progress` from the descriptor); add `kind` and `purged_counts: Option<(u64, u64)>` to `ControlMvpMaintenanceOutcome` with `const fn` accessors, filled at the point `publish_at` builds the outcome from the selected manifest's `retention_horizon` certificate. Re-export `MaintenanceKind` alongside `MaintenanceProgress` in `state_store.rs` and `lib.rs`. Give `MaintenanceKind` a `pub const fn as_str(&self) -> &'static str` returning `"consolidation"` / `"retention_horizon"` (snake_case, matching the worker's log and metric label vocabulary; serde stays SCREAMING_SNAKE_CASE on the wire).
3. Metrics (`metrics.rs`, follow the existing `STATE_STORE_*` block and `register_metrics`): `STATE_STORE_MAINTENANCE_PUBLISHED = "arco_state_store_maintenance_published_total"` (labels `domain`, `kind`), `STATE_STORE_RETENTION_PURGED_ROWS = "arco_state_store_retention_purged_rows_total"` (labels `domain`, `reason` in {`expired`, `tombstone`}), `STATE_STORE_OUTBOX_TRIMMED_RECORDS = "arco_control_store_outbox_trimmed_records_total"` (labels `domain`, `consumer`; this is the name the outbox module's docs already promise at `projection_outbox_acks.rs:80`). Emitters `record_maintenance_published(domain, kind)`, `record_retention_purged_rows(domain, expired, tombstones)` (skip zero increments), `record_outbox_trimmed(domain, consumer, n)`. Emit sites: in `publish_at` immediately after the exact head CAS is confirmed selected (both kinds; purged rows only for horizon), and in `trim_acked` after the source commit succeeds. Extend the metrics module's header comment that lists code-owned names.
4. Run: `cargo test -p arco-catalog --features test-utils --lib maintenance --locked -- --skip 32_seeds --skip durable_maintenance_model_32_seeds`, `--lib projection_outbox_acks`, `--lib metrics`, `--test state_store_control_mvp --locked`. Commit: `feat(catalog): expose maintenance job kind and purge counts; emit publication, purge and trim metrics`.

---

## Package B — worker runs the horizon job (Task 2)

### Task 2: consolidation then horizon, one job each per domain per run, shared recovery

**Files:** `crates/arco-api/src/bin/arco_control_store_worker.rs` (`DomainMaintenanceSummary` :~362, `drive_maintenance` :~872, `maintain_domain` :~929, `recover_selected_job` :~745, `publish_job` :~677, `run_once` :~1014, tests :~1182), `docs/runbooks/control-store-worker.md` (only the parts Package D does not cover: none; leave docs to Package D).

Behaviour:
- `maintain_domain` returns `Vec<DomainMaintenanceSummary>` (one entry per job driven, each with `kind`). Sequence per domain: (1) if a persisted identity exists, recover/resume it exactly as today (the record is kind-agnostic; the resumed progress reports the kind) and finish it; (2) otherwise, or after the recovered job finished with `Published`/`Terminal`/`Idle`, prepare a consolidation job with `prepare_at` and drive it; (3) when the consolidation step ended `Idle` or `Published` and no persisted identity remains, prepare a horizon job with `prepare_horizon_at`, persist the same `SelectedJobRecord` shape, start, advance and publish it. A `Deferred` or `Exhausted` consolidation ends the domain's phase for this run (the horizon waits; the persisted record must be finished first). At most one horizon job per domain per run.
- `DomainMaintenanceSummary` gains `kind: &'static str` (from `MaintenanceKind::as_str`), `purged_expired_rows: Option<u64>`, `purged_tombstones: Option<u64>` (from the publication outcome); `log()` emits them. `MaintenanceOutcome::Idle` for the horizon means "no purgeable row".
- `RunSummary.maintenance` holds every entry (up to two per domain). The `run` log line is unchanged.
- Recovery of a persisted horizon job uses the existing `recover_selected_job`; `summary.kind` is set from the resumed `MaintenanceProgress.kind` (or the recovered plan's kind).

Steps:
1. Failing tests first (worker `tests`, MemoryBackend): (a) `run_once_publishes_a_horizon_that_drops_expired_rows`: seed the catalog store with a live row, a row `put_with_expiry(now − 2 h)` and a row `put_with_expiry(now + 1 day)`; run; assert the catalog maintenance entries contain one with `kind == "retention_horizon"`, `outcome == Published`, `purged_expired_rows == Some(1)`, `purged_tombstones == Some(0)`; a fresh `ControlMvpStateStore` read at the current token returns `None` for the expired key and the values for the other two; a second run reports the horizon `Idle`. (b) `run_once_runs_consolidation_then_horizon_in_one_run`: seed 17 plain commits (so a consolidation intent is pending) plus an expired row; run; assert the catalog entries are `[consolidation Published, retention_horizon Published]` in that order and the L0 count dropped. (c) `run_once_resumes_a_persisted_horizon_job`: prepare a horizon plan through the kernel, `persist_selected_job`, `start_at`, then run; assert one catalog entry with `kind == "retention_horizon"`, `recovered == true`, `outcome == Published`, and the record is cleared. (d) update `run_once_is_idle_on_a_consolidated_root` to assert both entries idle. Adjust the `domain_summary` helper to take a kind. Run by name; expect FAIL.
2. Implement per the behaviour above. Keep `finish_job`, `advance_until_ready`, `publish_job` kind-agnostic; `publish_job` copies `purged_counts()` into the summary.
3. Run: `cargo test -p arco-api --bin arco_control_store_worker --locked`, `cargo test -p arco-api --test control_store_operator_api --locked`. Commit: `feat(api): control-store worker runs the retention horizon job after consolidation`.

---

## Package C — catalog outbox trim after the drain (Task 3)

### Task 3: `trim` phase between drain and GC

**Files:** `crates/arco-api/src/bin/arco_control_store_worker.rs` (new `TrimOutcome`, `TrimSummary`, `trim_catalog_outbox`, `RunSummary.trim`, `run_once`), tests.

Behaviour:
- After the drain phase (whatever its outcome) and before GC, `trim_catalog_outbox(storage)` builds `ProjectionOutboxWorker::new(storage, "catalog", CATALOG_PARQUET_PROJECTION_CONSUMER_ID)` with the default cooperative writer epoch and calls `trim_acked()`.
- Outcomes (`snake_case` serde names, `as_str`, covered by `outcome_log_names_match_their_serde_names`): `ok` (a trim commit landed: `trimmed_records > 0`, `trim_sequence`), `idle` (nothing acknowledged and untrimmed; no commit), `deferred` (`MaintenanceBackpressure`, `PreconditionFailed` or `CasFailed`: acknowledged records stay trimmable next run; log at warn with the error). Any other error fails the phase (`summary.fail("trim", "catalog", ..)`).
- `TrimSummary { outcome, trimmed_records, trim_sequence: Option<u64>, elapsed_ms }` with `log()` at `phase = "trim"`, `domain = "catalog"`.
- `is_deferrable` is not widened for maintenance; the trim phase matches `MaintenanceBackpressure` explicitly like the drain does.

Steps:
1. Failing tests first: (a) `run_once_trims_acknowledged_catalog_records_after_the_drain`: seed three catalog intents; run; assert `drain.drained_records == 3`, `trim.outcome == Ok`, `trim.trimmed_records == 3`, `trim.trim_sequence` is `Some`, `pending_records == 0`, and the source outbox is empty (read it through the kernel: `ControlMvpStateStore::current_projection_outbox` if public, otherwise a second `trim_acked` reports no trimmed ids and a `ProjectionOutboxWorker::backlog` shows nothing pending and nothing acknowledged-but-present); a second run reports `trim.outcome == Idle` with no new catalog sequence. (b) `run_once_trim_is_idle_when_nothing_is_acknowledged`: seed plain commits without intents; run; `trim.outcome == Idle` and the catalog logical sequence is unchanged by the trim. (c) `run_once_defers_the_trim_under_catalog_backpressure`: bring the catalog domain to the 32-segment backpressure threshold after acknowledging intents (seed intents, drain them through the materializer directly, then plain commits up to the refusal), run with `maintenance_max_advances: 0` so consolidation is `Exhausted` and the trim's source commit hits `MaintenanceBackpressure`; assert `trim.outcome == Deferred` and no failure recorded. If the threshold cannot be reached without tripping backpressure earlier in the run, replace (c) by a unit test of the outcome classifier with a constructed `MaintenanceBackpressure` error and say so in the commit message. Run by name; expect FAIL.
2. Implement per the behaviour above.
3. Run: `cargo test -p arco-api --bin arco_control_store_worker --locked`, `cargo test -p arco-api --test control_store_operator_api --locked` (the catalog-trim refusal test must still pass). Commit: `feat(api): control-store worker trims acknowledged catalog outbox records after the drain`.

---

## Package D — docs (Task 4)

### Task 4: runbook, changelog, design status

**Files:** `docs/runbooks/control-store-worker.md`, `docs/runbooks/state-store-replay-budget.md`, `CHANGELOG.md`, `docs/plans/2026-09-26-state-store-retention-design.md` (sequencing status only), `docs/plans/2026-09-26-state-store-retention-step2-worker.md` ("As implemented" section).

Steps:
1. Runbook `control-store-worker.md`: "What it does" becomes five phases (epoch, maintenance = consolidation then retention horizon per domain, drain, trim, GC) with the horizon's inputs (30-day age bound plus one-hour skew, snapshot/export pins, retained checkpoints; expired rows and unobservable tombstones only; live and outbox rows never); the summary-log table gains `kind`, `purged_expired_rows`, `purged_tombstones` on the maintenance row and a `trim` row (`outcome`, `trimmed_records`, `trim_sequence`, `elapsed_ms`); outcome vocabularies for `kind` and `trim`; the metric names from Package A; Known limitations gains: the trim adds one catalog L0 segment per run that trimmed something (folded by the next consolidation); a horizon job is refused when a later commit rewrote a purged key (`deferred`, retried next run); writer clocks more than one hour behind object-store time defeat the skew margin; the age bound is exercised only by kernel tests until a root has 30 days of history.
2. `state-store-replay-budget.md`: note that acknowledged outbox records are now trimmed by the worker, so outbox growth no longer contributes to replay bytes beyond one run's backlog.
3. CHANGELOG (Unreleased/Added): worker runs the retention horizon and trims acknowledged catalog outbox records; the three metrics.
4. Design doc: mark step 2 landed in the sequencing list (one line).
5. Add an "As implemented" section to this plan recording deviations. `git add` the docs, then `cargo xtask repo-hygiene-check`, `cargo xtask adr-check` if present, and a markdown lint if the repo has one. Commit: `docs: control-store worker horizon and trim phases`.

---

## Final verification (lead)

`cargo test -p arco-api --bin arco_control_store_worker --locked`; `cargo test -p arco-api --test control_store_operator_api --locked`; `cargo test -p arco-catalog --features test-utils --lib --locked -- --skip older_cut_outbox_model --skip independent_reclamation_model --skip durable_maintenance_model_32_seeds --skip 32_seeds`; `--test state_store_control_mvp`; `--test state_store_reclamation_schedules` (skips); `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; `cargo fmt --all -- --check`; `git diff --check`; `cargo xtask repo-hygiene-check`; the 32-seed durable model once at `CARGO_PROFILE_TEST_OPT_LEVEL=1`.

---

## As implemented

- Package B: the persisted `SelectedJobRecord` carries an optional `kind`
  (`#[serde(default)]`, the kernel's `MaintenanceKind` wire form) so every
  replay log line and failure string names the job kind even when the kernel
  yields no progress, as on a deferred replay. Records written before the
  field existed decode with `kind: None`, are replayed exactly as before and
  are labelled from the resumed progress; the Package B text above that calls
  the record kind-agnostic predates this. Per domain and run the worker drives
  at most one consolidation and one retention horizon; a replayed job fills
  the slot of its own kind, and the horizon follows a consolidation slot only
  when it ended `idle` or `published`.
