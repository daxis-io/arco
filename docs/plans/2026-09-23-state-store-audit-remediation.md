# State Store Audit Remediation (defects + plumbing) Implementation Plan

> **Execution contract:** implement task-by-task with TDD, one implementer per package, and a two-stage (spec, then quality) review after each package before the next dependent package starts.

**Goal:** Close the confirmed P1/P2 code defects and the missing deployment plumbing from `docs/reports/2026-09-23-state-store-architecture-audit.md` without changing authority format 7, restore-plan 6, or the retention contract.

**Architecture:** Kernel and adapter fixes stay inside `crates/arco-catalog` and preserve every existing wire format (one optional, serde-defaulted pointer field is the only persisted change). Deployment plumbing adds one scheduled Cloud Run job binary under the API service account (the sole `control/` writer), makes Terraform and the xtask guard derive the protected prefix from code, and wires real emitters for the reserved `arco_state_store_*` metrics. Docs are corrected to describe the layout and wiring that exist.

**Tech Stack:** Rust 1.88, Tokio, serde, `metrics` 0.24, Terraform (GCP), Cloud Run jobs + Cloud Scheduler, mdBook docs.

**Ground rules for every task**
- Work only in `/Users/ethanurbanski/arco/.worktrees/state-store-audit-fixes-20260923` on branch `fix/state-store-audit-20260923`.
- TDD: write the failing test first, run it, then implement. Never weaken an existing assertion to make a test pass; if an existing test encodes the old behaviour that this plan deliberately changes, update it and say so in the commit message.
- Build with `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0` (disk is tight). Run focused tests with `cargo test -p arco-catalog --features test-utils --lib --tests --locked <filter>`; only run the whole crate suite at the end of your package.
- Commit after each task with `git add <your files only>`; if `index.lock` exists, wait and retry (other agents share this worktree). Never `git stash`. Never push.
- Do not change authority/restore/segment/continuation format versions, retention floors, L0 thresholds, or any hash domain.

---

## Package K1 — protocol P1s (sequential; owns `control_mvp.rs`, `catalog_authority.rs`, `tests/state_store_control_mvp.rs`, `tests/catalog_authority_control_v1.rs`)

### Task 1: Writer-authority claim must reconcile only its own write (P1-3)

**Files:**
- Modify: `crates/arco-catalog/src/state_store/control_mvp.rs:6167-6220` (`ControlMvpPointer` + `validate_versioned`), `:538-620` (`claim_writer_authority`), every `ControlMvpPointer {` literal (`:2423`, `:2963`, `:4169`, `:4828`, `control_mvp/maintenance.rs:2257`, `control_mvp/bounded.rs:834`, `control_mvp/eager_reference.rs:113`)
- Test: `crates/arco-catalog/tests/state_store_control_mvp.rs` (near `landed_writer_claim_with_lost_response_adopts_exact_claimed_epoch`, ~line 2138)

**Step 1: Write the failing test**

Add a backend that parks the first pointer PUT *before* applying it, and returns a transport error *without* applying it once released:

```rust
struct GatedDroppedPointerWriteBackend {
    inner: Arc<dyn StorageBackend>,
    armed: AtomicBool,
    fired: AtomicBool,
    current_pointer: String,
    parked: Notify,
    release: Notify,
}
impl GatedDroppedPointerWriteBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self { /* like GatedPointerWriteThenErrorBackend::new */ }
    fn arm(&self) { self.fired.store(false, SeqCst); self.armed.store(true, SeqCst); }
    async fn wait_until_parked(&self) { while !self.fired.load(SeqCst) { self.parked.notified().await; } }
    fn release_dropped_write(&self) { self.release.notify_one(); }
    fn fault_fired(&self) -> bool { self.fired.load(SeqCst) }
}
// StorageBackend::put: if path.ends_with(current_pointer) && armed.swap(false):
//   fired.store(true); parked.notify_one(); release.notified().await;
//   return Err(arco_core::Error::storage("injected dropped pointer write"));
// every other method delegates to inner.
```

Test:

```rust
#[tokio::test]
async fn concurrent_writer_claims_with_lost_response_cannot_both_adopt_the_epoch() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(GatedDroppedPointerWriteBackend::new(inner));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store_a = store(storage.clone());
    let store_b = store(storage);
    commit_value(&store_a, b"catalog/default", "v1").await;

    backend.arm();
    let claim_b = tokio::spawn(store_b.claim_writer_authority());
    backend.wait_until_parked().await;                 // B read epoch 0 and its PUT is parked
    let claimed_a = store_a.claim_writer_authority().await.expect("A's claim lands");
    assert_eq!(1, claimed_a.writer_epoch());
    backend.release_dropped_write();                   // B's PUT is lost in transit

    let result_b = claim_b.await.expect("claim task");
    assert!(backend.fault_fired(), "the dropped pointer write must fire");
    let error = result_b.expect_err("B never published its claim and must not adopt A's epoch");
    assert!(format!("{error:?}").starts_with("AmbiguousAuthorityOutcome"), "unexpected: {error:?}");
}
```

**Step 2: Run it** — `cargo test -p arco-catalog --features test-utils --test state_store_control_mvp concurrent_writer_claims --locked`. Expected: FAIL — B returns `Ok` with epoch 1 (byte-identical readback).

**Step 3: Implement**
- Add to `ControlMvpPointer`: `#[serde(default, skip_serializing_if = "Option::is_none")] claim_id: Option<String>,`. In `validate_versioned`, if `Some(id)` require `integrity::valid_immutable_id(id)`.
- In `claim_writer_authority`, build `claimed = ControlMvpPointer { writer_epoch: claimed_epoch, claim_id: Some(Ulid::new().to_string().to_ascii_lowercase()), ..pointer }` (check `valid_immutable_id` accepts lowercase ULIDs; if not, use the same id shape `tx_id` uses). Readback now matches only this claimer's bytes. Keep the `..pointer` form in `fence_reclamation` (`:2963`) so a fence preserves whatever is in HEAD; set `claim_id: None` in every other explicit literal (commit, restore, maintenance, bounded, eager reference).
- Format-8 branch: same field; `bounded.rs:834` literal gets `claim_id: None`.

**Step 4: Run** the new test and `cargo test -p arco-catalog --features test-utils --test state_store_control_mvp claim --locked`, then `--test workspace_snapshot_restore --locked` and `--lib control_mvp --locked` (restore plans hash rendered pointer bytes; they must stay deterministic). Expected: all PASS.

**Step 5: Commit** — `fix(catalog): bind writer-authority claims to a claim id so lost-response readback cannot adopt a concurrent claim`.

### Task 2: Idempotency operation id must include the operation family (P1-4)

**Files:**
- Modify: `crates/arco-catalog/src/catalog_authority.rs:2575-2600` (`freeze_mutation`), `:2801-2839` (`stage_commit_records`), `:2781-2799` (`_v2`)
- Test: `crates/arco-catalog/tests/catalog_authority_control_v1.rs`

**Step 1: Failing test** (use the file's `scoped_storage()`/`scope()` helpers and the `CatalogAuthority::control_v1_bound` construction used by neighbouring tests; `WriteOptions` has `idempotency_key`):

```rust
#[tokio::test]
async fn reusing_an_idempotency_key_across_operation_families_keeps_both_audit_records() {
    // create catalog "c" without a key; then create_schema(c, "s") with key K; then
    // register_table(c, s, "t") with the SAME key K.
    // Expect: both succeed (no AlreadyExists / 409), and scanning the audit key tag
    // (byte 4) through a read_at(current token) yields two distinct records.
}
```
Use `ArcoStateReader::scan(ScanRequest::new([4u8]))` on the underlying store (`authority.store()` accessor exists? if not, construct `ControlMvpStateStore::new(storage, scope)` directly) and assert two entries.

**Step 2: Run** — expected FAIL with `AlreadyExists { entity: "projection intent" .. }` (409).

**Step 3: Implement**
- `operation_id`: `format!("op-{}", &sha256_hex(format!("{family}\0{key}").as_bytes())[..32])` when a key is present (unkeyed path unchanged).
- In both `stage_commit_records*`, call `txn.assert_absent(&audit_key(&frozen.operation_id)).await?` before the audit `put` (fail closed if a collision ever recurs).

**Step 4: Run** the new test, then `cargo test -p arco-catalog --features test-utils --test catalog_authority_control_v1 --locked` and `--lib catalog_authority --locked`. Expected PASS. Any test that hard-codes an `op-<hash>` for a keyed request must be updated (state why in the commit).

**Step 5: Commit** — `fix(catalog): derive control catalog operation ids from family and idempotency key`.

---

## Package K2 — isolation, retry, and validation P2s (sequential after K1; owns `lazy.rs`, `model.rs`, `state_store.rs`, `catalog_authority.rs` retry loop, `arco-api/src/error.rs`, `control_mvp.rs` test module)

### Task 3: `assert_range_empty` ignores tombstones (P2)

**Files:**
- Modify: `crates/arco-catalog/src/state_store/control_mvp/lazy.rs:840-897` (`range_evidence`), `:675-684` (`validate`), `crates/arco-catalog/src/state_store/control_mvp.rs:6033` (`range_has_entries`), `crates/arco-catalog/src/state_store/model.rs:369` (`range_has_entries`), trait doc `crates/arco-catalog/src/state_store.rs:2896-2901`, doc line `docs/plans/state-store-integrity-format-v1.md:152`
- Test: `crates/arco-catalog/src/state_store/control_mvp/lazy/tests.rs:192` (flip expectation), new test in `crates/arco-catalog/tests/state_store_control_mvp.rs`

**Step 1: Failing test** — commit `a/b`, delete `a/b`, begin txn, `assert_range_empty(KeyRange::new(b"a/", b"a0"))` must be `Ok`, commit must succeed; then re-create `a/b` in another txn concurrently before commit and assert the first commit fails `CasFailed`/`PreconditionFailed` (witness still covers the tombstone). Run: expected FAIL ("cannot assert a non-empty control MVP range").

**Step 2: Implement** — `present` counts only rows with `!value.tombstone` (both bounded and streaming branches; the bounded branch is `rows.iter().any(|(_, v)| !v.tombstone)`); `validate` compares `rows.iter().any(live) != *present`; `ReplayState::range_has_entries` and model `range_has_entries` filter `!value.tombstone`. Flip `lazy/tests.rs:192` to `.is_ok()`. Update the trait doc ("Tombstoned keys are not entries; the recorded witness still covers them, so a concurrent resurrection conflicts at commit.") and the integrity doc sentence.

**Step 3: Run** `--lib lazy --locked`, `--test state_store_lazy_transactions --locked`, `--test state_store_model --locked`, `--test state_store_control_mvp range --locked`, `--lib path_governance --locked`. Expected PASS.

**Step 4: Commit** — `fix(catalog): range-empty assertions ignore tombstones while keeping the tombstone witness`.

### Task 4: Conflict retries cover the begin-time head pin and back off; exhaustion carries Retry-After (P2)

**Files:**
- Modify: `crates/arco-catalog/src/catalog_authority.rs:4079-4215` (`execute`, `execute_v2`), `crates/arco-api/src/error.rs:335`
- Test: `crates/arco-catalog/tests/catalog_authority_control_v1.rs`; `crates/arco-api/src/error.rs` unit tests

**Step 1: Failing tests** — (a) wrap the storage in a backend whose `head()` on the pointer returns a fresh version for the first 3 calls (see `UnstablePointerHeadBackend` in `tests/state_store_control_mvp.rs`) then stabilises: a catalog mutation must succeed instead of returning `CasFailed("HEAD pin retry budget exhausted")`. (b) `ApiError::from(CatalogError::CasFailed{..})` response must carry `Retry-After: 1` and status 409.

**Step 2: Implement** — in both loops, match `begin_control_txn` like commit: `Err(CatalogError::CasFailed{..}) if started.elapsed() < RETRY_BUDGET => { backoff; continue }`. Backoff: `let base = (5u64 << attempt.min(6)).min(200); let jitter = base + (u64::from(Ulid::new().random() as u32) % base); sleep(jitter ms)`. API: `CatalogError::CasFailed { message } => Self::conflict(message).with_retry_after(1)`.

**Step 3: Run** `--test catalog_authority_control_v1 --locked`; `cargo test -p arco-api --lib error --locked`. Expected PASS.

**Step 4: Commit** — `fix(catalog): retry head-pin conflicts inside the catalog budget with backoff and advertise Retry-After on exhaustion`.

### Task 5: Fault-injection tests prove the fault fired; `%` rejected in request ids (P2/P3)

**Files:**
- Modify: `crates/arco-catalog/src/state_store/control_mvp.rs:8939-8975`, `:11240-11274`, `:10805-10835` (tests), `crates/arco-catalog/src/state_store.rs:2101-2114` (`validate_scope_component`)
- Test: `crates/arco-catalog/src/state_store.rs` unit tests (find `validate_scope_component`/`TxnOptions` tests)

**Steps** — (a) In each of the three tests, after the faulted operation assert the injected flag was consumed: `assert!(!backend.lose_head_response.load(Ordering::SeqCst), "the lost head response must fire")` (the backends `swap(false)` when they fire), same for `lose_checkpoint_response`; in the `fail_readback=false` branch assert the lost response fired as well. (b) Add a failing unit test that `TxnOptions::default().with_request_id("a%2fb").validate()` is `Err(Validation)`, then add `'%'` to the rejected characters and the message. Run `--lib state_store --locked` and `--lib control_mvp::tests --locked`. Commit — `test(catalog): assert injected faults fire; reject percent in request ids`.

---

## Package K3 — projection drain hygiene and metrics (sequential after K2; owns `catalog_authority.rs` notifier, `projection_outbox_acks.rs`, `metrics.rs`, `control_mvp.rs` emit sites, `infra/monitoring/alerts.yaml`)

### Task 6: Serialize and coalesce post-commit drains; acknowledge() retries an unrelated CAS loss (P2)

**Files:**
- Modify: `crates/arco-catalog/src/catalog_authority.rs:85-108` (`CatalogProjectionDrainNotifier`), `crates/arco-catalog/src/state_store/projection_outbox_acks.rs:491-530` (`acknowledge`)
- Test: `crates/arco-catalog/tests/catalog_authority_control_v1.rs`, `crates/arco-catalog/src/state_store/projection_outbox_acks.rs` tests

**Steps** — (a) Notifier: add `gate: Arc<tokio::sync::Mutex<()>>` and `pending: Arc<AtomicBool>` to the struct (constructed once per authority; clones share). `notify`: `pending.store(true)`; spawn `{ let _g = gate.lock().await; while pending.swap(false, SeqCst) { drain_once().await ... } }`. Test: fire 8 notifies concurrently against a counting materializer backend (count `manifest.json` PUT attempts) and assert at most 2 drains ran and the outbox is fully acked. (b) `acknowledge`: loop up to `STATUS_CAS_ATTEMPTS`; on `CasFailed` with no visible receipt, retry a fresh txn; only after exhausting return the `CasFailed`. Test: two acks for different records racing on the ack-root pointer both succeed. Commit — `fix(catalog): coalesce projection drains and retry unrelated ack pointer races`.

### Task 7: Quarantined intents stay visible in projection status (P2)

**Files:** `crates/arco-catalog/src/state_store/projection_outbox_acks.rs:1820-1845` (`merge_projection_status`) and its tests; `crates/arco-api/src/system_tables.rs` (surface unchanged).

**Steps** — Failing test: quarantine seq 5, then record success at seq 6 → status `failure_state()` must still start with `terminal:`. Implement: a current `terminal:*` failure state is retained across later successes (terminal records are never retried; only an operator can resolve them). Document in the field doc comment. Run `--lib projection_outbox_acks --locked`. Commit — `fix(catalog): keep terminal projection quarantine visible in materialization status`.

### Task 8: Real emitters for the reserved state-store metrics, plus backpressure/ambiguity counters (P1-7)

**Files:**
- Modify: `crates/arco-catalog/src/metrics.rs` (add `STATE_STORE_MAINTENANCE_BACKPRESSURE = "arco_state_store_maintenance_backpressure_total"`, `STATE_STORE_AMBIGUOUS_OUTCOMES = "arco_state_store_ambiguous_outcomes_total"`, `STATE_STORE_L0_SEGMENTS = "arco_state_store_l0_segments"` gauge; describe them; add helpers `record_state_store_cas_publish(domain, outcome)`, `record_state_store_replay(domain, secs, bytes)`, `record_state_store_integrity_failure(domain, artifact)`, `record_state_store_backpressure(domain)`, `record_state_store_ambiguous(domain)`, `record_state_store_l0_segments(domain, n)`, `record_projection_publish(domain, consumer)`, `record_projection_watermark(domain, consumer, lag, age_secs)`; delete the "no production emitter" note)
- Emit in `crates/arco-catalog/src/state_store/control_mvp.rs`: `commit_inner` head-CAS match (`:4896-4956`): publish attempt + outcome `success|cas_lost|stale_epoch|transport|ambiguous`; L0 gauge = `tx_refs.len()` and backpressure counter at `:4767-4773`; `replay_manifest` (`:915`) duration + bytes (sum of `segment_size_bytes` over base states and tx refs); `validate_raw_checksum` (`:8559`) failure → integrity counter with `artifact=context` (domain comes from the caller; if unavailable use `"unknown"`); `ambiguous_authority_outcome` helper (`:8829`) → ambiguous counter (add a `domain: &str` parameter or emit at the nine call sites).
- Emit in `crates/arco-catalog/src/state_store/projection_outbox_acks.rs`: `record_projection_success` → publish counter; `projection_watermark_lag_for` (`:785`) → lag/age gauges.
- `infra/monitoring/alerts.yaml`: remove the stale "no production emitter" notes; fix `current.pointer.json`/`txlog` wording to `head/current.json`/`transactions/`; add `ArcoControlStoreMaintenanceBackpressure` (any increase over 10m → page, runbook state-store-replay-budget) and `ArcoControlStoreAmbiguousOutcomes` (any increase over 1h → warn, runbook state-store-cas-publish-failure); add a `arco_state_store_l0_segments > 24` warning.

**Test:** install `metrics_util::debugging::DebuggingRecorder` (check `metrics-util` is a dev-dependency; if not add it) in one unit test that performs a commit and asserts `arco_state_store_cas_publish_total{domain="catalog"}` == 1 and the L0 gauge == 1. Run `--lib metrics --locked` and `--lib control_mvp --locked`. Commit — `feat(catalog): emit state-store CAS, replay, integrity, backpressure, ambiguity and projection metrics`.

---

## Package W — scheduled control-store worker (parallel with K1; owns `crates/arco-api/src/bin/arco_control_store_worker.rs`, `crates/arco-api/Cargo.toml`, `infra/terraform/cloud_run_job.tf`, `infra/terraform/variables.tf`, `scripts/build-cloud-run-image.sh`, `scripts/deploy.sh`, `.github/workflows/terraform-plan-validate.yml`, `docs/runbooks/control-store-worker.md`)

### Task 9: Worker binary

Read first: `crates/arco-api/src/bin/arco_flow_worker.rs` (env/config/logging style), `crates/arco-storage-s3/src/bin/qualification.rs:432-495` (maintenance/GC orchestration), `crates/arco-catalog/src/state_store/control_mvp/maintenance.rs:1335-1360,1439,1635,1773,2363` (`prepare_at/start_at/resume_at/advance_at/publish_at`), `crates/arco-catalog/src/catalog_authority.rs:866-930` (`CatalogProjectionMaterializer`), `crates/arco-catalog/src/state_store/projection_outbox_acks.rs:98` (ack domain const).

Binary `arco_control_store_worker` (register in `crates/arco-api/Cargo.toml` `[[bin]]` if the crate lists bins explicitly). Env: `ARCO_STORAGE_BUCKET`, `ARCO_CATALOG_CONTROL_V1_TENANT_ID`, `ARCO_CATALOG_CONTROL_V1_WORKSPACE_ID`, `ARCO_CONTROL_STORE_MAINTENANCE_BINDING` (base64, exactly 32 bytes; required; documented as a per-deployment secret that must never change for a root), `ARCO_CONTROL_STORE_GC_MAX_PAGES` (default 16), `ARCO_CONTROL_STORE_MAINTENANCE_MAX_ADVANCES` (default 4096). Behaviour per invocation (`--once` is the only mode; the job exits 0 on success, non-zero on any typed error):
1. `storage = ScopedStorage::new(arco_storage::from_bucket(bucket)?, tenant, workspace)?`.
2. Catalog drain: `CatalogProjectionMaterializer::new(storage.clone())?.drain_once().await?`; log the report; `status()` → emit `record_projection_watermark`.
3. For each domain in `["catalog", PROJECTION_OUTBOX_ACK_DOMAIN]`: build `DurableMaintenanceWorker::new(storage.clone(), StateScope::new(tenant, workspace, domain), binding)?`; `prepare_at(now)` → if `Some(plan)`: `start_at(&plan, now)`, then `resume_at(job_id)` and `advance_at` until `ReadyToPublish` (bounded by the max-advances env), then `publish_at`. Treat `Superseded`/`Consumed` as "retry next run" (log, exit 0). Persist nothing; if `prepare_at` refuses because a job is already in flight, log and continue (read its error variant to decide).
4. GC per domain: `ControlMvpMaintenanceWorker::new(storage.clone(), scope)?.collect_gc_page_at(now, vec![], cursor)` up to max pages; log the plan/outcome counts. Cursor is not persisted (document as a known cost limitation).
5. One JSON summary log line per phase (`phase`, `domain`, `outcome`, counts, elapsed ms).

Unit test (in the bin, `#[cfg(test)]`): with `MemoryBackend`, seed 16 commits into `catalog` through `ControlCatalogAuthority` (or raw store), run the worker function once, assert a maintenance job published (`pending_intent()` is `None` afterwards) and the projection status advanced. Run `cargo test -p arco-api --bin arco_control_store_worker --locked`.

### Task 10: Terraform job + scheduler + build/deploy wiring

- `variables.tf`: `control_store_worker_image` (default `""`), `control_store_tenant_id`, `control_store_workspace_id` (default `""`), `control_store_worker_schedule` (default `"*/5 * * * *"`), `control_store_maintenance_binding_secret` (Secret Manager secret id, default `""`).
- `cloud_run_job.tf`: `google_cloud_run_v2_job "control_store_worker"` with `count = local.control_store_worker_enabled ? 1 : 0` (all four inputs non-empty), `service_account = google_service_account.api.email` (the sole `control/` writer — do NOT use the compactor SA), env from the vars, binding from `secret_key_ref`; `google_cloud_run_v2_job_iam_member` invoker like the compactor job; `google_cloud_scheduler_job` trigger gated on `var.background_automation_enabled`. Comment that the 5-minute cadence cannot meet ADR-043's 10 s p99 lag objective and that queue-driven wake is separate cutover work.
- `scripts/build-cloud-run-image.sh:36,95` add `arco_control_store_worker`; `scripts/deploy.sh` add the tfvar plumbing mirroring `flow_worker_image`; `.github/workflows/terraform-plan-validate.yml:99` add `-var="control_store_worker_image=example.com/arco-control-store-worker:test"` plus tenant/workspace/secret test values.
- Run `terraform fmt -check` and `terraform validate` if the CLI is available (`terraform -chdir=infra/terraform validate` after `init -backend=false`); otherwise state that it was not run.
- New runbook `docs/runbooks/control-store-worker.md`: what the job does, env vars, how to run it manually (`gcloud run jobs execute`), how to read its summary logs, and the secret rotation rule for the binding (never rotate for a live root).

Commit — `feat(api): add scheduled control-store worker job for projection drain, maintenance and GC`.

---

## Package T — IAM prefix truth (parallel; owns `infra/terraform/iam_conditions.tf`, `tools/xtask/tests/terraform_iam.rs`, `crates/arco-core/src/storage_keys.rs`, new `crates/arco-catalog/tests/state_store_layout_contract.rs`)

### Task 11

1. Failing test `crates/arco-catalog/tests/state_store_layout_contract.rs`: `ControlMvpPaths::new("catalog").base_prefix()` and `.current_pointer()` start with `arco_core::storage_keys::CONTROL_STATE_OBJECT_PREFIX`; also the directory prefix literal `"control/directory/v1/"` (see `control_mvp/directory.rs:209`) starts with it. Add `pub const CONTROL_STATE_OBJECT_PREFIX: &str = "control/";` to `arco-core/src/storage_keys.rs` with a doc comment naming both layouts.
2. `iam_conditions.tf`: replace `state_store_object_prefix = "state-store/"` with `control_store_object_prefix = "control/"`; keep the resource name `api_write_state_store` (avoid a Terraform state move) but update its description and every comment block (`:21`, `:42-49`, `:120-149`) to describe `tenant={t}/workspace={w}/control/v1/domains/{domain}/…` and `control/directory/v1/…`, and to say the API service is the sole writer and the scheduled control-store worker runs under the same service account. Note in the comment that `state-store/` is retired and no code writes it.
3. `tools/xtask/tests/terraform_iam.rs:109-160`: rename the test `control_store_prefix_has_exactly_one_writer`; assert the local equals `arco_core::storage_keys::CONTROL_STATE_OBJECT_PREFIX` (xtask already depends on arco-core); the structural walk must look for `startsWith` arguments starting with that constant; keep the single-writer assertion.
4. Run `cargo test -p arco-catalog --test state_store_layout_contract --locked`, `cargo test -p xtask --test terraform_iam --locked` (from `tools/xtask`), `terraform fmt -check`. Commit — `fix(infra): grant the API sole-writer IAM on the control/ prefix the state kernel actually writes`.

---

## Package D — documentation truth (parallel; owns `docs/runbooks/state-store-*.md`, `CHANGELOG.md`, `docs/spec/README.md`, `docs/adr/adr-043-s3-state-token-authority.md`, `docs/guide/src/reference/control-plane-scope.md`, `.gitignore`, `docs/guide/src/reference/catalog-authority-hard-cut.md`)

### Task 12

1. All seven `docs/runbooks/state-store-*.md`: replace `state-store/control-mvp/{domain}/txlog/…` with `control/v1/domains/{domain}/transactions/…`, `current.pointer.json` with `head/current.json`, `segments/…` and `checkpoints/…` per `ControlMvpPaths` (`control_mvp.rs:2526-2570`); fix the sole-writer sentence in `state-store-cas-publish-failure.md:51-53` to reference the `control/` grant; replace every "Honest status as of 2026-07-30 … no production callers" paragraph with a dated 2026-09-23 status: route-wired behind `ARCO_CATALOG_CONTROL_V1_*` for one exact root, default legacy, not provider-qualified, drain/maintenance/GC run by the `arco-control-store-worker` job (link the new runbook), metrics emitted as of this change; replace private-symbol references (`ControlMvpStateStore::token`, `ControlMvpRestorePlan::validate`) with the public surfaces (`ArcoStateAdmin::checkpoint`, `/internal/control-store/*` operator endpoints, `ARCO_CONTROL_STORE_OPERATOR_*`).
2. `CHANGELOG.md:17`: replace the "crate-private with zero production callers" paragraph with an accurate statement (route-wired, default-disabled, one exact root, not authoritative on any deployed root) and add Unreleased entries for this remediation.
3. `docs/spec/README.md:44-45`: "The control-store path is accepted by ADR-043 as the target authority for `control/v1`; it is not yet provider-qualified or authoritative on any deployed root."
4. `docs/adr/adr-043-s3-state-token-authority.md`: "The current v4 kernel" → "The current kernel (fourth revision, on-disk authority format 7)"; add one sentence under "Cutover and qualification" noting the scheduled worker and metric emitters exist but queue-driven wake remains cutover work.
5. `docs/guide/src/reference/control-plane-scope.md:34`: "v4 ordered L1 shards" → "format-7 ordered L1 shards"; add the worker/metrics columns' truth.
6. `.gitignore`: add `evidence/` with a comment (audit and qualification evidence is archived outside the tree).
7. Run `cargo xtask adr-check` if it exists and the mdBook build (`mdbook build docs/guide` if installed); `git diff --check`. Commit — `docs: describe the control/v1 layout, wiring state and worker that actually exist`.

---

## Final verification (lead)

```sh
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
cargo test -p arco-catalog --features test-utils --lib --tests --locked
cargo test -p arco-api --locked
cargo test -p arco-core --locked
(cd tools/xtask && cargo test --test terraform_iam --locked)
cargo clippy -p arco-catalog --features test-utils --lib --tests --locked -- -D warnings
cargo clippy -p arco-api --all-targets --locked -- -D warnings
cargo fmt --all -- --check
git diff --check
```
