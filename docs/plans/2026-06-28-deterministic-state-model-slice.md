# Deterministic State Model Slice

**Status:** Phase 3A child plan.

**Base:** `e64d19e docs: add phase 2 contract conformance slice`.

## Scope

Add a deterministic in-crate reference state-store model under
`arco-catalog::state_store`. The model exists to make the state-store contract
observable and testable before any production authority changes.

## Owned Files

- `docs/plans/2026-06-28-deterministic-state-model-slice.md`
- `crates/arco-catalog/src/state_store.rs`
- `crates/arco-catalog/src/state_store/model.rs`
- `crates/arco-catalog/src/lib.rs`
- `crates/arco-catalog/tests/state_store_model.rs`

## Implementation Shape

- Add `state_store::model` as a reference implementation module.
- Add `ModelStateStore` implementing `ArcoStateReader`, `ArcoStateAdmin`, and
  `ArcoStateStore`.
- Add `ModelTxn` implementing `ArcoStateTxn`.
- Use deterministic folded key/value state in `BTreeMap<Vec<u8>, StoredValue>`.
- Assign monotonic logical sequences only after successful commit
  revalidation.
- Record point, range, and predicate input-set witnesses that fail closed when
  concurrent transactions change the observed generations.
- Store committed model records containing deterministic logical events and
  folded writes.
- Provide replay helpers that rebuild folded state from committed records and
  ignore identical repeated records.
- Provide deterministic explanation accessors for tests and later conformance
  users.

## Non-Goals

- Do not route production reads or writes through `ModelStateStore`.
- Do not add a new crate.
- Do not add public constructors for `StateToken` or `CheckpointToken`.
- Do not create object-store manifests, transaction objects, checkpoints,
  pointer CAS, segments, or bounded replay.
- Do not change `CatalogWriter`, Unity Catalog routes, compaction, ledger,
  governance writes, or API behavior.

## TDD Order

1. `accepted_commits_advance_logical_sequence_once`
2. `failed_precondition_revalidation_does_not_advance_sequence`
3. `point_precondition_failure_fails_closed`
4. `range_empty_precondition_failure_fails_closed`
5. `range_unchanged_precondition_failure_fails_closed`
6. `predicate_input_set_revalidation_catches_conflicting_writes`
7. `replay_from_committed_events_equals_folded_kv_state`
8. `idempotent_replay_is_stable`
9. `failed_transactions_publish_no_partial_state`

## Verification

Run:

```bash
cargo fmt --check
cargo test -p arco-catalog --test state_store_model
cargo test -p arco-catalog --test state_store_current_adapter
cargo check -p arco-catalog
git diff --check
```
