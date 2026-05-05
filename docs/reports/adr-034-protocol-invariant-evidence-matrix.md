# ADR-034 Protocol Invariant Evidence Matrix

Date: April 9, 2026

Scope: repo-side evidence for ADR-034 protocol invariants and backend conformance. This matrix is
about direct proof in-tree. It is not a rollout go/no-go substitute for the existing PI/runbook
documents.

## Verification Status

Directly re-run in this workspace after landing the new shared/core/catalog/flow suites:

```bash
cargo test -p arco-core --test storage_backend_conformance -- --nocapture
cargo test -p arco-catalog --test protocol_invariants -- --nocapture
cargo test -p arco-flow --test orchestration_protocol_invariants -- --nocapture
cargo test -p arco-api --test root_transaction_protocol -- --nocapture
cargo test -p arco-api --test visible_contracts -- --nocapture
cargo test -p arco-compactor -- --nocapture
```

Observed results:

- `arco-core` storage conformance: passed on `MemoryBackend` and `ObjectStoreBackend(InMemory)`;
  GCS remained ignored/env-gated
- `arco-catalog` protocol invariants: passed
- `arco-flow` orchestration protocol invariants: passed
- `arco-api` root replay protocol: passed
- `arco-api` visible contracts: passed
- `arco-compactor` operational smoke and handler boundary tests: passed
- Fresh `CARGO_TARGET_DIR=target-review cargo check -p arco-api --lib`: passed

## Invariant Matrix

| Invariant | Direct test evidence | Owning lane | Status |
|---|---|---|---|
| `StorageBackend` create-if-absent, CAS, `head()`, and exact-one race semantics hold through Arco abstractions | `memory_backend_satisfies_storage_conformance`, `object_store_memory_backend_satisfies_storage_conformance`, `gcs_backend_satisfies_storage_conformance` in `crates/arco-core/tests/storage_backend_conformance.rs` | PR CI for memory/object-store-memory; scheduled/manual GCS workflow for live cloud | Landed |
| Visible publish CAS has one winner and visible head is only old-or-winner | `visible_publish_race_keeps_pointer_at_old_or_winner_only` in `crates/arco-core/tests/publish_protocol_contract.rs` | PR CI | Landed |
| Persisted durability never reports visible success on CAS loss | `persisted_publish_conflict_returns_persisted_not_visible_without_advancing_pointer` in `crates/arco-core/tests/publish_protocol_contract.rs`; `publish_manifest_reports_persisted_not_visible_on_cas_loss` in `crates/arco-flow/src/orchestration/compactor/service.rs` | PR CI / flow crate tests | Landed |
| Immutable artifacts reject overwrite reuse | `immutable_snapshot_write_rejects_overwrite_attempts` in `crates/arco-core/tests/publish_protocol_contract.rs` | PR CI | Landed |
| Stale fencing holders are rejected for lock handoff and release never clobbers newer holders | `expired_lock_takeover_race_has_single_winner_and_monotonic_fencing`, `stale_guard_release_never_clobbers_newer_lock_holder` in `crates/arco-core/tests/lock_protocol_contract.rs` | PR CI | Landed |
| Catalog ordinary reads are pointer-first and do zero `list()` calls | `ordinary_catalog_reads_never_list_storage` in `crates/arco-catalog/tests/protocol_invariants.rs` | PR CI | Landed |
| Catalog root-token reads are pinned and do zero `list()` calls | `root_token_catalog_reads_never_list_storage` in `crates/arco-catalog/tests/protocol_invariants.rs` | PR CI | Landed |
| Catalog stale writer fencing is rejected without moving visible head | `tier1_writer_rejects_stale_fencing_and_leaves_visible_head_unchanged` in `crates/arco-catalog/tests/protocol_invariants.rs` | PR CI | Landed |
| Catalog fenced sync compaction rejects stale holders without moving visible head | `tier1_sync_compaction_rejects_stale_fencing_and_leaves_visible_head_unchanged` in `crates/arco-catalog/tests/protocol_invariants.rs` | PR CI | Landed |
| Catalog CAS loss leaves previous visible head unchanged even if loser immutable artifacts persist | `pointer_cas_loss_leaves_visible_head_unchanged_even_if_new_snapshot_persists` in `crates/arco-catalog/tests/protocol_invariants.rs` | PR CI | Landed |
| Catalog reconcile never deletes pointer-targeted current references and current-head-only repair skips generic cleanup | `repair_skips_pointer_targeted_snapshot_paths`, `repair_with_current_head_scope_skips_generic_cleanup_issues` in `crates/arco-catalog/src/reconciler.rs` | Catalog crate tests | Existing direct proof retained |
| Orchestration ordinary reads do zero `list()` calls and never read the ledger | `ordinary_orchestration_reads_never_list_or_read_ledger` in `crates/arco-flow/tests/orchestration_protocol_invariants.rs` | PR CI / flow tests | Landed |
| Orchestration root-token reads do zero `list()` calls and never read the ledger | `root_token_orchestration_reads_never_list_or_read_ledger` in `crates/arco-flow/tests/orchestration_protocol_invariants.rs` | PR CI / flow tests | Landed |
| Orchestration fenced races are deterministic and stale holders are rejected | `compact_events_fenced_retries_after_concurrent_manifest_snapshot_conflict`, `compact_events_fenced_rejects_stale_lock_holder` in `crates/arco-flow/src/orchestration/compactor/service.rs` | Flow crate tests | Existing direct proof retained |
| Orchestration reconcile protects current pointer targets plus reachable manifest/base/L0 history | `check_preserves_reachable_manifest_history_and_artifacts`, `repair_keeps_reachable_manifest_history_and_artifacts` in `crates/arco-flow/src/orchestration/compactor/reconciler.rs` | Flow crate tests | Existing direct proof retained |
| API visible-success paths remain replayable after visible finalize/cache repair and post-visibility pointer readback failures | `apply_catalog_ddl_replays_from_cached_visible_record_after_finalize_write_failure`, `apply_catalog_ddl_keeps_visible_success_when_pointer_readback_fails`, `commit_orchestration_batch_keeps_visible_success_when_pointer_readback_fails` in `crates/arco-api/tests/visible_contracts.rs` | `arco-api` test lane | Landed |
| Controller/API orchestration compaction rejects `visibility_status != "visible"` | `require_visible_compaction_rejects_persisted_not_visible_results` in `crates/arco-api/src/orchestration_compaction.rs` | `arco-api` unit test lane | Landed |
| Root replay repairs the missing tx-record durable side from visible idempotency cache without listing | `replay_repairs_missing_root_tx_record_from_visible_idempotency_without_listing` in `crates/arco-api/tests/root_transaction_protocol.rs` | `arco-api` test lane | Landed |
| Root replay repairs the missing visible idempotency durable side from the visible tx record without listing | `replay_repairs_missing_visible_idempotency_from_root_tx_record_without_listing` in `crates/arco-api/tests/root_transaction_protocol.rs` | `arco-api` test lane | Landed |
| Compactor reconcile defaults stay on full repair scope | `test_reconcile_request_defaults_to_full_scope`, `test_reconcile_endpoint_defaults_to_full_scope`, `test_reconcile_endpoint_full_scope_repairs_cleanup_items` in `crates/arco-compactor/src/main.rs` | `arco-compactor` test lane | Landed |
| Anti-entropy remains the only list-authorized compactor path | `test_sync_compact_handler_does_not_require_list_permission`, `test_anti_entropy_handler_is_the_only_list_authorized_path` in `crates/arco-compactor/src/main.rs` | `arco-compactor` test lane | Landed |

## Execution Lanes

| Lane | Command / workflow |
|---|---|
| Core backend conformance in PR CI | `cargo test -p arco-core --test storage_backend_conformance -- --nocapture` |
| Live-cloud GCS gate | `.github/workflows/adr-034-gcs-conformance.yml` |
| Catalog invariants | `cargo test -p arco-catalog --test protocol_invariants -- --nocapture` |
| Orchestration invariants | `cargo test -p arco-flow --test orchestration_protocol_invariants -- --nocapture` |
| API root protocol | `cargo test -p arco-api --test root_transaction_protocol -- --nocapture` |
| API visible-only contracts | `cargo test -p arco-api --test visible_contracts -- --nocapture` |
| Compactor operational smoke | `cargo test -p arco-compactor -- --nocapture` |

## Notes

- The orchestration legacy-manifest fallback remains compatibility-only and is not counted as a
  correctness boundary in this matrix.
- Ordinary-read zero-list proof covers public catalog/orchestration readers and root-token reads,
  not anti-entropy/reconcile/rebuild paths.
- The GCS lane is intentionally scheduled/manual and not part of blocking PR CI.
- The GCS workflow now prefers OIDC via `vars.GCP_WORKLOAD_IDENTITY_PROVIDER` and
  `vars.GCP_SERVICE_ACCOUNT`, with the legacy JSON key path retained only as a fallback.
