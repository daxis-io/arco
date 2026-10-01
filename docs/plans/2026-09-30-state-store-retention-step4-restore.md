# Retention step 4: restore excludes receipts, restore projection and audit, checkpoint certificate — implementation plan

> **Execution contract:** implement package-by-package with TDD, one implementer per package, a two-stage (spec, then quality) review after each package, and no package starting before the previous one is approved. Design: `2026-09-26-state-store-retention-design.md` (accepted). Steps 1 (PR #439), 2 (PR #442) and 3 (PR #444) are on `main`.

**Goal:** A format-9 restore never restores idempotency receipts and leaves none behind, so a replay after a restore re-applies; the catalog projection materializes every restore (a snapshot at the restore sequence plus one audit row for the restore) instead of quarantining it; checkpoints carry the horizon certificate on every capture path.

**Survey findings this plan rests on (main at f4f827b7):**
- Restore plan version 7 already exists (step 1 pinned `committed_at_ms`). Changing rendered restore bytes requires a new plan version, or in-flight v7 plans fail with "cannot reproduce deterministic candidate bytes" instead of being superseded.
- The format-9 restore source is a checkpoint's materialized state (`restore_source_values`, `live_entries_bounded`); writes are computed by `restore_writes(source, current)` and every write lands at the restore sequence. Expiry hints are reproduced. No domain hook exists to exclude keys.
- Receipts today: a receipt in current but not in the source is deleted; one present in both with identical bytes and hint is kept and keeps answering replays (the design forbids this); one in the source but purged in current is resurrected. Empty-base trap: when the head pointer is absent, writes are diffed against an empty `current` but applied over `candidate_parent` (the source lineage), so source-lineage receipts would survive a scan-only filter.
- Pre-existing defect: the restore commit's single outbox record (`restore:{id}:{attempt}:{domain}`, payload `ControlMvpRestoreNotice`) is not a `ProjectionIntentV1`, so `CatalogProjectionMaterializer::process` quarantines it as `INVALID_PROJECTION_INTENT`; the quarantine is sticky-terminal, the record is never acknowledged or trimmed, and no snapshot is published at the restore sequence. Step 3 did not cause this.
- Checkpoints carry `retention_horizon` and `validate_source` requires it to equal the manifest's, except `prepare_bounded_checkpoint`, which hard-codes `None` (test-only mixed capture path; capture fails on a horizon HEAD).
- Every restore source is pinned (checkpoint retention or a retained root), so no restore can resurrect a key at an old generation.
- No production composition registers restore participants yet; restore is reached from tests, benches and the S3 qualification binary.

**Scope decision on semantic Parquet retry acceptance.** Step 3 deferred "accept an existing projection Parquet file whose decoded rows match" to no later than step 4 because restore-emitted audit records might be written without snapshot files. Package B writes the restore's snapshot files before its audit file, exactly like a mutation, so the audit comparison is never the first to fire and the step-4 trigger disappears. The library-upgrade hazard itself is pre-existing (snapshot files compare bytes too, and the legacy tier-1 compactor shares `put_state_if_absent`); it stays a separate follow-up.

**Tech stack:** Rust 1.88, Tokio, Arrow/Parquet; kernel tests in `crates/arco-catalog/tests/state_store_control_mvp.rs` and `control_mvp.rs` unit tests; catalog tests in `crates/arco-catalog/tests/catalog_authority_control_v1.rs`; workspace restore tests in `crates/arco-catalog/tests/workspace_snapshot_restore.rs`.

**Ground rules for every package**
- Worktree `/Users/ethanurbanski/arco/.worktrees/state-store-retention-20260926`, branch `feat/state-store-retention-step4` (based on `origin/main` f4f827b7). Never `cd` elsewhere. Never `git stash`. Never push. Never amend.
- Export `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`. Foreground cargo with filters. The shell is zsh: a variable holding several flags is not word-split, so spell flags out or use bash. NEVER run the whole arco-catalog suite; append `-- --skip older_cut_outbox_model --skip independent_reclamation_model --skip durable_maintenance_model_32_seeds --skip 32_seeds` to `--lib` runs. Exhaustive models are `#[ignore]` (weekly CI); the lead runs `durable_maintenance_model_32_seeds` once with `--ignored` at `CARGO_PROFILE_TEST_OPT_LEVEL=1`.
- TDD; never weaken an assertion. A test that encodes the old receipt-restoring behaviour is updated and the commit message says so.
- CI gates `RUSTDOCFLAGS=-D warnings cargo doc`: never intra-doc-link a `pub(crate)` or private item from a public doc.
- Stage new docs with `git add` before `cargo xtask repo-hygiene-check` (bans agent/vendor names and a literal plans-directory path outside that directory).
- Commit per task with `git add <files>`; end messages with the session's standard attribution trailer.
- The restore notice wire shape, the projection intent contract, catalog key tags, and the bounded (format 8) restore are unchanged.

---

## Package A — kernel restore key exclusion, plan version 8 (Task 1)

**Files:** `crates/arco-catalog/src/state_store/control_mvp.rs` (`RESTORE_PLAN_VERSION` ~:181 and the supersession arm ~:3480-3517 / ~:4726-4760; `ControlMvpRestoreParticipant` ~:5097; `restore_source_values` ~:3054; `live_entries_bounded` ~:9828; `restore_writes` ~:3081; `render_restore_candidate` ~:3109; `build_restore_plan` ~:3324; the plan wire struct and its validation ~:4700-5060; `inspect_restore` ~:6740; `apply_restore` ~:6808), `crates/arco-catalog/src/catalog_authority.rs` (a catalog restore participant constructor), `crates/arco-catalog/tests/fixtures/control_mvp_restore_plans/` (a v7 fixture), tests.

Behaviour:
- `pub struct RestoreKeyPolicy` (kernel, public, documented): an ordered, deduplicated set of excluded key prefixes (non-empty prefixes; at most a small fixed number, e.g. 16), with a canonical digest (`sha256` over a tagged framing of the sorted prefixes; tag e.g. `arco/control-v1/restore-key-policy`). `RestoreKeyPolicy::none()` is the default.
- `ControlMvpRestoreParticipant::with_key_policy(self, policy) -> Self`. A participant without a policy behaves exactly as today except that its plans are version 8 with the empty-policy digest.
- Rendering with a policy: (1) source rows whose key starts with an excluded prefix are dropped before `live_entries_bounded` counts rows and bytes (so receipts never consume the scanner's row/byte budget); (2) every excluded-prefix key that is live (not tombstoned) in `stable.current.state` OR in `stable.candidate_parent.state` gets `StagedWrite::Delete` at the restore sequence (closes the empty-base trap); (3) everything else follows the existing rules.
- `RESTORE_PLAN_VERSION = 8`; the plan binds `restore_key_policy_sha256` (required for v8); version 7 joins the supersession-only arm exactly as v6 did (inspect returns `Superseded`, apply refuses without writes). Add a literal v7 fixture (`v7_last_before_restore_key_policy.json`) captured from the current code before the change, mirroring `v6_last_before_format9.json`.
- Catalog: `pub fn catalog_restore_key_policy() -> RestoreKeyPolicy` (excludes the receipt prefix `[IDEMPOTENCY_KEY_TAG]`) and `ControlCatalogAuthority::restore_participant(&self) -> ControlMvpRestoreParticipant` (or a free constructor over a `ControlMvpStateStore`; whichever the existing participant construction makes natural) that applies it. Tag 4 (residual pre-step-3 audit rows) is NOT excluded: those rows are the only record of pre-step-3 audits and follow the normal restore rules.

Steps:
1. Failing tests first (kernel, `tests/state_store_control_mvp.rs`, templates `restore_apply_rolls_forward_and_preserves_historical_checkpoint` ~:2198 and `restore_empty_current_base_extends_source_lineage_and_retries_idempotently` ~:3159): (a) with a policy excluding prefix `[3]`: a receipt-like key present in both source and current with identical bytes and hint is deleted; one only in current is deleted; one only in the source (or purged in current) is not restored; non-excluded keys follow the old rules exactly; the restore L0 rows match; (b) empty base: the source lineage's excluded keys are tombstoned in the candidate; (c) the excluded rows do not count against `live_entries_bounded` (a source whose non-excluded rows fit but whose total would exceed a small test bound succeeds; use the test-utils bound override if one exists, otherwise test the counting function directly); (d) the plan binds the policy digest: a plan built with one policy is `Superseded` (or rejected) when inspected/applied by a participant with a different policy, never a byte-mismatch invariant violation; (e) the literal v7 fixture is supersession-only (inspect `Superseded`, apply refuses without writes); (f) a participant without a policy produces v8 plans that restore exactly as before (re-run the existing restore suite unchanged). Catalog (`tests/catalog_authority_control_v1.rs`): (g) `restore_never_restores_receipts_and_a_replay_reapplies`: create a catalog with a keyed request, checkpoint, patch it with a keyed request, restore from the checkpoint through the catalog restore participant (follow the kernel restore tests for the checkpoint reference and identity plumbing), then assert the tag-3 prefix is empty, the catalog is back to its checkpoint state, and replaying the first keyed request re-executes (observable conflict or effect; pin which). Run each by name; confirm FAIL.
2. Implement.
3. Lanes: `--test state_store_control_mvp`, `--lib control_mvp -- <skips>`, `--test workspace_snapshot_restore`, `--test workspace_snapshot_services`, `--test catalog_authority_control_v1`, `--test state_store_reclamation_schedules -- <skips>`, `--lib integrity`, `--lib maintenance -- --skip 32_seeds --skip durable_maintenance_model_32_seeds`, clippy both crates `-D warnings`, doc gate, fmt. Commit: `feat(catalog): restore key policy excludes receipts; restore plan version 8`.

---

## Package B — catalog projection materializes restores (Task 2)

**Files:** `crates/arco-catalog/src/state_store/control_mvp.rs` (expose a decoder for the restore notice: `pub fn decode_control_mvp_restore_notice(record: &ControlMvpProjectionOutboxRecord) -> Option<Result<ControlMvpRestoreNoticeView>>` or equivalent public read-only view; a way to resolve the restore's result manifest and a read token at `result_logical_sequence`), `crates/arco-catalog/src/catalog_authority.rs` (`CatalogProjectionMaterializer::process`/`materialize`, audit row construction for restores), tests.

Behaviour:
- In `process`, before the `ProjectionIntentV1` decode, recognise a restore notice: record id `restore:{restore_id}:{attempt}:{domain}`, payload decoding exactly as `ControlMvpRestoreNotice`, `domain == "catalog"`, record origin sequence == `result_logical_sequence`, id components consistent with the payload. A record that looks like a restore notice but fails any check is quarantined `INVALID_RESTORE_NOTICE` (a new code) with the existing warn log.
- Materialize a recognised notice like a mutation: resolve the catalog state at `result_logical_sequence` (the restored state) through the kernel's authenticated ancestry (a retained manifest at that sequence; fail retryable if it is not yet visible, non-retryable if it is provably absent), write the catalog snapshot files into `control/v1/projections/catalog-parquet/{seq:020}-{manifest_id}/`, then one audit file at the usual path with a restore row, then the manifest; then record success; the generic worker acknowledges.
- Restore audit row (a `CatalogAuditRow`, no new schema columns): `record_version = 1`, `operation_id = <record id>`, `operation_family = "restore_domain"`, `request_digest = sha256 hex of the notice payload bytes`, `actor = "restore"`, `occurred_at_ms = committed_at_ms of the result manifest`, `logical_sequence = result_logical_sequence`, `authority_manifest_id = result manifest id`, `logical_commit_id = None`. Path `dt=<day of occurred_at_ms>/{seq:020}-{record_id}.parquet` (record ids may contain `:`; confirm `ScopedStorage::validate_path` accepts it and object names allow it).
- The existing sticky quarantine of a restore notice on roots restored before this change is not migrated automatically: document the operator action (the terminal status names `INVALID_PROJECTION_INTENT`; resolving it is the existing quarantine-resolution follow-up).

Steps:
1. Failing tests first (`tests/catalog_authority_control_v1.rs`): (a) `a_catalog_restore_is_materialized_with_a_restore_audit_row`: mutate, checkpoint, mutate, restore through the catalog participant, drain; assert no quarantine, the status is not terminal, a snapshot manifest exists at the restore sequence whose catalogs match the checkpoint state, exactly one restore audit file with the fields above, and the notice record is acknowledged (trim removes it); (b) a later mutation still materializes and the status stays healthy; (c) a forged notice (wrong domain, sequence mismatch, malformed payload with a `restore:` id) is quarantined `INVALID_RESTORE_NOTICE` and later work proceeds; (d) redelivery of a restore notice (fail the ack as in `redelivered_intent_rewrites_an_identical_audit_artifact`) rewrites identical bytes. Unit tests for the restore audit row builder and notice decoder. Run by name; confirm FAIL.
2. Implement.
3. Lanes: `--test catalog_authority_control_v1`, `--lib catalog_authority -- --skip 32_seeds`, `--test state_store_control_mvp`, `--test workspace_snapshot_restore`, `--test runtime_reuse_cost`, `cargo test -p arco-api --bin arco_control_store_worker`, `cargo test -p arco-api --test control_store_operator_api`, clippy, doc gate, fmt. Commit: `feat(catalog): materialize catalog restores with a restore audit row instead of quarantining them`.

---

## Package C — checkpoint certificate on every capture path (Task 3)

**Files:** `crates/arco-catalog/src/state_store/control_mvp.rs` (`prepare_bounded_checkpoint` ~:2191-2214), tests in `control_mvp.rs`/`integrity/tests.rs`/`tests/workspace_snapshot_services.rs`.

Behaviour: `prepare_bounded_checkpoint` copies `manifest.retention_horizon` like `prepare_checkpoint` does.

Steps:
1. Failing tests first: (a) a checkpoint taken while HEAD is a horizon manifest carries the certificate and passes `validate_source`; a checkpoint whose certificate is removed or altered fails `validate_source` (fail closed); (b) the mixed bounded capture (`mixed_authority8_and_v7_capture_uses_a_bounded_v7_checkpoint_source`, `tests/workspace_snapshot_services.rs` ~:1644) succeeds when the format-9 domain's HEAD is a horizon manifest (drive a horizon through the kernel with an expired row first); (c) a restore from a checkpoint taken at a horizon HEAD succeeds. Run by name; confirm FAIL where applicable.
2. Implement.
3. Lanes: `--test workspace_snapshot_services`, `--test state_store_control_mvp`, `--lib integrity`, `--lib control_mvp -- <skips>`, clippy, doc gate, fmt. Commit: `fix(catalog): bounded checkpoint capture carries the retention horizon certificate`.

---

## Package D — docs (Task 4)

**Files:** `docs/plans/2026-09-26-state-store-retention-design.md` ("Restore and checkpoints": plan 7 landed in step 1, the receipt rule needs plan 8; the key policy; the restore audit row; the empty-base rule; the pre-existing restore-notice quarantine and its fix; Sequencing: step 3 landed (PR #444), step 4 landed (this change); the semantic Parquet acceptance re-scope), `docs/plans/state-store-retention-format-v1.md` (restore plan 8, the policy digest), `docs/guide/src/reference/system-catalog.md` (restore audit rows: family `restore_domain`, actor `restore`, operation id = notice record id), `docs/runbooks/control-store-worker.md` (restore projections now materialize; operator action for restore notices quarantined before this change), `docs/runbooks/state-store-restore-repair-required.md` if it describes restore effects on receipts or projections, `CHANGELOG.md`, this plan's "As implemented" section.

Steps: edit, `git add`, `cargo xtask repo-hygiene-check`, `cargo xtask adr-check`, `mdbook build docs/guide`. Commit: `docs: restore excludes receipts, restore projection and audit, checkpoint certificate`.

---

## Final verification (lead)

`cargo test -p arco-catalog --features test-utils --lib --locked -- <skips>`; `--lib` default features with skips; integration lanes `catalog_authority_control_v1`, `state_store_control_mvp`, `workspace_snapshot_restore`, `workspace_snapshot_services`, `state_store_lazy_transactions`, `state_store_segment_contract`, `control_bounded_parity`, `control_bounded_cost`, `runtime_reuse_cost`, `state_store_intent_contract`, `state_store_reclamation_schedules` (skips); `cargo test -p arco-api --locked`; `cargo test -p arco-core --locked`; `cargo test -p xtask --locked`; workspace clippy `-D warnings`; workspace doc gate; fmt; diff-check; hygiene; adr-check; mdbook; the 32-seed durable model once with `--ignored` at opt-level 1.

## Follow-ups (not in this step)

1. Semantic retry acceptance for projection Parquet (see the scope decision above); until then, drain the catalog outbox before deploying a different `parquet`/`arrow` version.
2. Nothing collects per-intent catalog snapshot directories under `control/v1/projections/catalog-parquet/` (found in step 3).
3. A production composition that registers restore participants (with the catalog key policy) does not exist yet.
4. Resolution path for terminal projection quarantines (including restore notices quarantined before this change).
