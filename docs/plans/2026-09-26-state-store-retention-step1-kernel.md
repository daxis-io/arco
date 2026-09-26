# Retention step 1: kernel authority format 9 — implementation plan

> **Execution contract:** implement package-by-package with TDD, one implementer per package, a two-stage (spec, then quality) review after each package, and no package starting before the previous one is approved. Design: `2026-09-26-state-store-retention-design.md` (accepted).

**Goal:** Ship authority format 9 in the kernel: wall-clock stamps, per-row expiry, and a certified `RetentionHorizon` maintenance transition, with fixtures, oracle and models updated. No adapter, projection, restore-semantics, or worker-binary changes (those are steps 2 to 4).

**Architecture:** Format 9 is format 7 plus `committed_at_ms` on manifests and transactions, a nullable `expires_at_ms` segment column (segment format 2), a `retention_horizon` manifest field, and a second durable-maintenance job kind that renders L1 without purge-eligible rows under a certificate the ancestry walker can bind. Every hash already frames the authority format number, so all digests change with the bump; fixtures and canonical vectors are regenerated. Reads and witnesses are untouched.

**Tech stack:** Rust 1.88, Arrow IPC 54, serde, Tokio; existing fixture driver, `LogicalOracle`, `ModelStateStore`, 32-seed models.

**Ground rules for every package**
- Worktree `/Users/ethanurbanski/arco/.worktrees/state-store-retention-20260926`, branch `feat/state-store-retention-design` (based on the PR #436 branch). Never `cd` elsewhere. Never `git stash`. Never push.
- Export `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`. Run cargo in the foreground with filters. NEVER run the whole crate suite; ALWAYS append `-- --skip older_cut_outbox_model --skip independent_reclamation_model --skip durable_maintenance_model_32_seeds --skip 32_seeds` to `--lib` and reclamation-schedule runs (those models take hours unoptimized; the lead runs them once at `CARGO_PROFILE_TEST_OPT_LEVEL=1` at the end).
- TDD; never weaken an assertion. A test that encodes format-7-only behaviour is updated, and the commit message says so.
- Stage new docs with `git add` before `cargo xtask repo-hygiene-check` (it scans tracked files only).
- Commit per task with `git add <files>`; end messages with the session's standard attribution trailer.
- Retention floors (7-day orphan, 30-day token/checkpoint), L0 thresholds (16/32), hash domain tags, and the public `ArcoStateReader` surface do not change. `ArcoStateTxn` gains exactly one method.

---

## Package A — format identity (Task 1)

### Task 1: bump to authority 9, stamp manifests/transactions, restore plan 7, regenerate fixtures

**Files:** `crates/arco-catalog/src/state_store/control_mvp.rs` (`CONTROL_MVP_FORMAT_VERSION` :177, `RESTORE_PLAN_VERSION` :168 and the supersession `match` ~:3718-3760, `authenticated_token_format` `matches!(.., 7 | 8)` :271, `ControlMvpManifest` :6470ff, `ControlMvpTxObject` ~:6900ff, every manifest/tx render site that sets `format_version`), `control_mvp/integrity.rs` (framing binds the const automatically; docs), `control_mvp/integrity/tests.rs` (fixture and vector paths :171, :304-324, :414), `crates/arco-catalog/tests/fixtures/control_mvp_authority_v9/*` (new; the format-7 fixture directory is removed), `docs/reports/2026-09-06-gate3-canonical-vectors.json` → new `docs/reports/2026-09-26-format9-canonical-vectors.json`, `crates/arco-catalog/tests/fixtures/control_mvp_restore_plans/*` (add a v7 current plan fixture; v2 stays), `docs/runbooks/state-store-restore-repair-required.md` (format numbers).

Steps:
1. Failing tests first: (a) in `control_mvp.rs` tests, a head pointer, manifest and transaction envelope with `format_version: 7` must fail closed at `load_pinned_pointer`, `ControlMvpManifest::validate`, and tx metadata load with the existing mismatch errors; (b) a committed manifest and its transaction object carry `committed_at_ms` within `[staging_time, now]`; (c) a restore plan with `version: 6` decodes as supersession-only (`inspect` returns `Superseded`, `apply` refuses without writes), a `version: 7` plan applies. Run each by name; expect FAIL.
2. Implement: `CONTROL_MVP_FORMAT_VERSION = 9`; `authenticated_token_format` accepts `9 | 8`; add `committed_at_ms: i64` to `ControlMvpManifest` and `ControlMvpTxObject` (set from `cost::now()`-equivalent wall clock at render; validate `> 0`); `RESTORE_PLAN_VERSION = 7` with `6` added to the supersession-only arm (keep 1..=5 handling; `checkpoint_interval`/`observed_reclamation_generation`/`transaction_ref` requirements stay "current version only"); maintenance `Descriptor.authority_version` and bounded checks follow the const automatically.
3. Regenerate fixtures: add a `#[ignore]`d generator test next to the existing `generate_golden_schemas` that writes `control_mvp_authority_v9/{legacy_workspace_manifest,legacy_workspace_transaction,legacy_workspace_checkpoint}.json` preserving the legacy (unversioned) `StateScope` shape those tests exercise; point `integrity/tests.rs:304-324` at v9. Add an `#[ignore]`d vector generator that writes `2026-09-26-format9-canonical-vectors.json` from the Rust implementation and point `canonical_hashes_match_independent_binary_vectors` at it. Record in the file's header comment that these vectors are Rust-generated and that independent regeneration is a follow-up (the format-7 vectors were independently produced).
4. Run: `--lib integrity --locked`, `--lib control_mvp::tests --locked` (with skips), `--test state_store_control_mvp --locked`, `--test workspace_snapshot_restore --locked`, `--test state_store_segment_contract --locked`, `--test control_bounded_parity --locked`, `--test catalog_authority_control_v1 --locked`, `cargo test -p arco-api --test control_store_operator_api --locked`, `cargo test -p arco-api --bin arco_control_store_worker --locked`. Fix every format-7 literal a test hard-codes (list them in the commit).
5. Update the restore runbook's format numbers. Commit: `feat(catalog): authority format 9 with committed_at stamps and restore plan 7`.

---

## Package B — per-row expiry (Task 2)

### Task 2: `expires_at_ms` through write, segment, replay, checksum, digest, model and oracle

**Files:** `crates/arco-catalog/src/state_store.rs` (`ArcoStateTxn` trait ~:2853: add `async fn put_with_expiry(&mut self, key: &[u8], value: Bytes, expires_at_ms: i64) -> Result<()>`; doc: expiry is a purge-eligibility hint, never a read filter), `control_mvp/lazy.rs` (`StagedWrite::Put` gains `expires_at_ms: Option<i64>`; `stage_write` accounting +8 bytes; `put_with_expiry` impl; format-8 `Bounded` base rejects it with `Validation("expiry is not supported on bounded authority")`), `control_mvp.rs` (`ControlMvpWriteEntry` +`expires_at_ms: Option<i64>` :6909; `ControlMvpSegmentRow` +field; Arrow schema :7461-7468 gains `Field::new("expires_at_ms", DataType::Int64, true)` as the ninth column; `SEGMENT_FORMAT_VERSION = 2`; `preflight_arrow_segment` exact-schema check; `encode_segment`, `decode_segment_batch`, `decode_block_rows` nodes/buffers count (`nodes == 9`); `segment_rows_for_tx` :7125; `segment_rows_for_state` :6989; `state_object_from_segment_rows`; `hydrate_from_segment_rows` :6750; `StoredValue` +`expires_at_ms`; `apply_tx` copies it; `ReplayStateDigestEntry` +`expires_at_ms` so the full-state checksum covers it; `from_replay` :6409), `control_mvp/integrity.rs` (`mutation_digest`: after `optional_bytes(value)` append `optional u64 expires_at_ms`; the design keeps encoding version 1 because the framing already binds format 9), `state_store/model.rs` (`ModelWrite`/`StoredValue` +expiry; `ModelTxn::put_with_expiry`; model checksum/digest includes it), `tests/support/integrity_oracle.rs` (kv value gains expiry; `range_empty`/`range_witness` unchanged), `control_mvp/bounded/*` and `physical.rs` (the extra column must decode; format-8 rows always carry null), `docs/plans/state-store-block-format-v1.md` (segment format 2 section).

Steps:
1. Failing tests: (a) `tests/state_store_segment_contract.rs`: a committed segment has nine columns with `expires_at_ms` nullable Int64 and index `format_version == 2`; an eight-column (format-1) segment is rejected at preflight; (b) `tests/state_store_control_mvp.rs`: `put_with_expiry` round-trips through L0 replay, a consolidation (fixture driver `consolidate_pending`), a checkpoint read, and a restore render with the expiry preserved and the value readable; a plain `put` yields null expiry; two states differing only in one row's expiry have different full-state checksums and different mutation digests; (c) `tests/state_store_model.rs` + `tests/state_store_lazy_transactions.rs`: model and control agree with expiry in the operation mix; (d) `authority8_tests.rs`: `put_with_expiry` on a bounded store is a `Validation` error and stages nothing.
2. Implement per the file list. Reads (`get`, `scan`, `read_at`, witnesses, `range_has_entries`) do not look at expiry.
3. Run the same lanes as Task 1 plus `--test state_store_model`, `--test state_store_lazy_transactions`, `--lib lazy`, `--lib bounded`, `--lib physical`, `--test control_bounded_cost`.
4. Commit: `feat(catalog): per-row expiry hint on state-store writes (segment format 2)`.

---

## Package C — the retention horizon (Tasks 3 and 4)

### Task 3: certificate type, manifest field, validators, ancestry rule

**Files:** `control_mvp/integrity.rs` (new `pub(super) struct RetentionHorizonV1 { encoding_version: u32, horizon_sequence: u64, purge_cutoff_ms: i64, pinned_evidence: Vec<PinnedSequenceV1 { kind: String, id: String, sequence: u64 }>, parent_state_checksum_sha256: String, purged_rows_sha256: String, purged_counts: PurgedCountsV1 { expired_rows: u64, tombstones: u64 } }` with `validate()`: encoding 1, valid digests, `horizon_sequence <= logical_sequence` (passed in), kinds ∈ {snapshot, export, checkpoint, manifest_age}), `control_mvp.rs` (`ControlMvpManifest` +`retention_horizon: Option<RetentionHorizonV1>` with `#[serde(default, skip_serializing_if)]`; `ControlMvpCheckpoint` +same as pass-through; `ControlMvpManifest::validate` calls the certificate validator; `resolve_ancestor_bounded` ~:2070-2090: add a third accepted transition `horizon`: `manifest.logical_sequence == child.sequence && manifest.layout_generation + 1 == child.layout && child.retention_horizon.is_some() && cert.parent_state_checksum_sha256 == manifest.state_checksum_sha256 && child.equivalence.source_physical_root == manifest.physical_root`; no `maintenance_intent` requirement; `AncestorTransition` carries `retention_horizon`; the purged-rows digest function `purged_rows_digest(rows: impl Iterator<Item=&ControlMvpSegmentRow>) -> String` (canonical: tag `arco/control-v1/retention-purge`, then per row `bytes(key) u64(generation) u8(tombstone) optional u64(expires)` in key order)).

Steps:
1. Failing tests in `control_mvp.rs`/`integrity/tests.rs`: a manifest with a well-formed certificate validates; certificates with an invalid digest, `horizon_sequence > logical_sequence`, unknown pin kind, or negative cutoff are rejected; an ancestry walk over a hand-built parent → child pair where the child has a horizon certificate but (i) a mismatched `parent_state_checksum`, (ii) a changed logical sequence, or (iii) a changed history root fails with "invalid authenticated ancestry transition", while the well-formed pair is accepted. Build the pair by committing, then rendering a child manifest through a test-only helper that reuses the maintenance renderer with an injected purge set (or construct the manifest literal and publish it with the test storage's raw put).
2. Implement. Run `--lib integrity`, `--lib control_mvp::tests` (skips), `--test state_store_control_mvp`.
3. Commit: `feat(catalog): retention-horizon certificate and ancestry rule for authority 9`.

### Task 4: `RetentionHorizon` durable-maintenance job kind

**Files:** `control_mvp/maintenance.rs` (`Descriptor` +`kind: MaintenanceKind` (`#[serde(default)]`, `Consolidation | RetentionHorizon`) and +`horizon: Option<HorizonInputs { horizon_sequence, purge_cutoff_ms, pinned_evidence }>`; new `pub async fn prepare_horizon_at(&self, now) -> Result<Option<PreparedMaintenance>>`; `PreparedPlan::build` gains an optional purge predicate and accumulates `purged_rows_sha256` + counts into the plan pages/descriptor; `submit_publication` ~:2195-2260 renders `retention_horizon: Some(cert)` and sets `equivalence.state_checksum_sha256` to the pruned state's checksum with `render_source` as today; before the head CAS, recompute the parent replay, apply the recorded predicate, and require the resulting checksum and purged digest to equal the rendered ones; `compatible()` unchanged; consolidation (`prepare_at`) never purges), `control_mvp.rs` (a helper the worker calls to compute inputs: `retention_horizon_inputs(&self, lifecycle: &ScopedStorage, now) -> Result<HorizonInputs>`: (1) bounded ancestry walk from the current manifest following `base_manifest_id`/`parent_manifest_sha256` until `committed_at_ms <= now − 30 d`, taking that manifest's `logical_sequence` (if the walk exhausts without reaching 30 days, horizon = 0, i.e. nothing purgeable by age); (2) `RetainedAuthorityRoots::new(lifecycle, now)` streamed for snapshot/export pins whose authorities name this scope, taking each pinned manifest's `logical_sequence`; (3) the `checkpoints/` inventory with the same age and `min_retention_seconds` rule GC uses (~:3180-3215), taking each retained checkpoint's `logical_sequence`; `horizon_sequence = min(all)`; `purge_cutoff_ms = now − 1 h`). Purge predicate: `row.expires_at_ms.is_some_and(|e| e < purge_cutoff_ms) || (row.tombstone && row.generation <= horizon_sequence)`; outbox and trim rows are never candidates.

Steps:
1. Failing tests (use `DurableMaintenanceWorker` directly on `MemoryBackend` plus the fixture driver): (a) mixed state: an expired receipt, an unexpired receipt, a live row, a tombstone below the horizon, a tombstone above it, and an unacked outbox row → after `prepare_horizon_at` + `start_at`/`advance_at`/`publish_at` exactly the expired receipt and the low tombstone are gone; counts and digest in the certificate match; logical sequence and history root unchanged; layout generation +1; (b) a retained token taken before the rewrite still reads the tombstone generation (`assert_generation`) and the expired receipt; a transaction begun after sees absence; (c) a snapshot pin at sequence p keeps tombstones with deleting sequence > p and purges those ≤ p; a checkpoint with `min_retention_seconds` beyond 30 days holds the horizon at its sequence; (d) `prepare_horizon_at` returns `None` when nothing is eligible; (e) CAS loss during publication regenerates under the new generation (reuse the reclamation-schedule harness pattern); (f) tampering: alter the rendered certificate's `purged_rows_sha256` in storage → subsequent `read_at` over that manifest and the ancestry walk fail closed; (g) `LogicalOracle` gains an `expires_at` per row and a `horizon(now, pinned)` operation, and the `durable_maintenance_model_32_seeds_of_128_operations` family list gains `RetentionHorizon`; run the model with `--skip` per the ground rules but execute one reduced seed inline (e.g. 2 seeds × 32 ops) as a named test.
2. Implement. Run `--lib maintenance` (skips), `--lib control_mvp::tests` (skips), `--test state_store_control_mvp`, `--test state_store_reclamation_schedules` (skips), `--test workspace_snapshot_restore`, `--test workspace_snapshot_services`.
3. Commit: `feat(catalog): retention-horizon maintenance job that purges expired rows and unobservable tombstones`.

---

## Package D — contracts and docs (Task 5)

### Task 5: format documents, ADR wording, changelog, capacity smoke

**Files:** new `docs/plans/state-store-retention-format-v1.md` (authority 9 contract: stamps, segment format 2 column, certificate encoding, transition rule, purge predicate, horizon inputs, what old binaries do), `docs/plans/state-store-integrity-format-v1.md` and `state-store-block-format-v1.md` (one-paragraph pointers to the new document; do not rewrite history), `docs/adr/adr-043-s3-state-token-authority.md` ("on-disk authority format 7" → 9; add one sentence to invariant 9 that retention-horizon maintenance may remove expired rows and unobservable tombstones without advancing logical sequence), `CHANGELOG.md` (Unreleased entries), `docs/runbooks/state-store-replay-budget.md` (mention the horizon job), a capacity smoke test in `crates/arco-catalog/src/state_store/control_mvp/capacity_design.rs` (`#[ignore]`d full size stays; add a small non-ignored test: 2,000 receipts with expiry + 500 deletes → after one horizon run past the cutoff, retained rows equal the live rows plus unexpired receipts).

Steps: write docs; stage them; `cargo xtask repo-hygiene-check`; `cargo xtask adr-check`; `mdbook build docs/guide` if installed; `git diff --check`; run the smoke test; commit: `docs: authority format 9 retention contract and changelog`.

---

## Final verification (lead)

```sh
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
cargo test -p arco-catalog --features test-utils --lib --tests --locked -- --skip older_cut_outbox_model --skip independent_reclamation_model --skip durable_maintenance_model_32_seeds --skip 32_seeds
cargo test -p arco-catalog --lib --tests --locked -- <same skips>
CARGO_PROFILE_TEST_OPT_LEVEL=1 cargo test -p arco-catalog --features test-utils --test state_store_reclamation_schedules --locked -- older_cut_outbox_model independent_reclamation_model durable_maintenance_model
cargo test -p arco-api --locked
cargo test -p arco-core --locked
cargo check --workspace --all-targets --locked
cargo clippy -p arco-catalog --features test-utils --lib --tests --benches --locked -- -D warnings
cargo clippy -p arco-api --all-targets --locked -- -D warnings
cargo fmt --all -- --check && git diff --check && cargo xtask repo-hygiene-check && cargo xtask adr-check
```

---

## As implemented (2026-09-26)

Packages A through D landed on this branch (bf3694da, f526db45, f0702f29,
7e8f326a, 6484e5dc, 7c9aae9b, e8eee31e, 16587f2d, d589ec82, then the Package D docs
commit). The resulting contract is `state-store-retention-format-v1.md`.
Deviations from the plan above:

- **Restore plan 7 pins the stamp.** The plan-7 wire format carries
  `committed_at_ms` (required, positive; legacy plans must not carry it) so a
  pending restore reconstructs its format-9 candidate bytes exactly, instead
  of stamping at apply time. `render_restore_candidate` rejects a stamp before
  the candidate parent.
- **No `v7_current.json` restore-plan fixture.** The fixture convention is
  "last shape an older revision wrote", so Package A added
  `v6_last_before_format9.json` (captured from pre-bump code) instead; the
  current field set is pinned by `a_v6_plan_over_a_matching_source_is_superseded_and_never_applied`.
- **Maintenance stamps are `max(descriptor.created_at, parent stamp)`**, and
  commit and restore stamps are `max(clock, parent stamp)`; the ancestry
  walker rejects a child stamped before its parent. The plan only said "set
  from the wall clock at render".
- **Restore source scan.** `live_entries_bounded` is synchronous and serves
  only the materialized checkpoint cut restore resolves; a manifest-backed
  reader is an invariant violation rather than an unbounded replay. Receipts
  are not skipped (that is design step 4); they are re-staged with their hint.
- **Age-anchor chain instead of the per-commit walk.** Task 4's walk over
  `base_manifest_id` cannot span 30 days at the pilot rate, so every manifest
  carries `age_anchor` (`AgeAnchorV1`, hour buckets), the walker checks the
  render rule as a transition invariant, and the age bound follows anchors
  under a 1,024-hop budget plus a matching byte budget. The floor is
  `now - 30 d - 1 h` (`CONTROL_MVP_RETENTION_CLOCK_SKEW_MS`), not
  `now - 30 d`; the purge cutoff is `now - 1 h` as planned. The input helper
  is `ControlMvpMaintenanceWorker::retention_horizon_inputs` in
  `maintenance.rs`, not a method on the store.
- **Bound-0 evidence shape.** When the chain ends above the floor the
  certificate still cites one `manifest_age` entry, the newest manifest
  examined, with sequence 0, so `pinned_evidence` is never empty. Evidence is
  one entry per kind at that kind's lowest sequence, and
  `validate_horizon_inputs` requires every cited sequence to be at or above
  the horizon.
- **Digest encoding of the hint.** The mutation and purged-rows digests
  encode `expires_at_ms` as `optional_i64` (one byte 0, or 1 plus the 8-byte
  big-endian signed value), not the `optional u64` written above, and the
  purged-rows digest frames the row count first.
- **Model family renumbering.** The durable model has 20 families (12 when
  not durable): `RetentionHorizon` is family 12, family 19 runs a two-cycle
  age scenario over 32-day jumps, and the forced, intent-less consolidations
  are gone (the model commits tracked filler until the head carries a
  maintenance intent, then calls `prepare_at`). Family 0 writes an expiring
  `receipt` row, and the run pins `FixedInputs::at(now)` so stamps follow
  model time. The reduced inline run is
  `durable_maintenance_model_reduced_two_seeds_of_thirty_two_operations`
  (2 seeds x 32 operations).
- **Vectors.** `2026-09-26-format9-canonical-vectors.json` is generated by the
  Rust implementation and is a regression pin only; independent regeneration
  is a follow-up, as the file's header records.
