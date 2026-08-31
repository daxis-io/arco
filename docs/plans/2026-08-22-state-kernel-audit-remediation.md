# State Kernel Audit Remediation Implementation Plan

**Goal:** Close every material finding from the fresh Phase 1 state-kernel audit without expanding into the deferred AWS, route-cutover, or Phase 2-6 program.

**Architecture:** Arrow segments remain physically sorted and checksum-bound, but logical outbox order is encoded explicitly. L0 Arrow becomes the sole mutation payload while JSON transactions retain metadata and the L0 reference. The hard-cut restore contract advances to a new current version and recognizes older plans only well enough to terminate them without writes. Serialized intent evidence remains public, but only the state store may mint `StateToken` values.

**Tech Stack:** Rust, Tokio, Arrow IPC 54, Serde JSON, in-memory `StorageBackend` fault injection, Cargo test/Clippy/format gates.

**Authority boundary:** Work only in `/Users/ethanurbanski/arco/.worktrees/s3-lambda-state-kernel-v1`. Do not commit, push, create a PR, deploy, mutate cloud state, or clean unrelated files without separate authorization.

---

### Task 1: Preserve logical outbox order across sorted L1 segments

**Files:**
- Modify: `crates/arco-catalog/tests/state_store_control_mvp.rs`
- Modify: `crates/arco-catalog/src/state_store/control_mvp.rs`

1. Add a regression that stages reverse-lexical outbox IDs, crosses an interval-one L1 anchor, opens current/token/checkpoint readers, commits a successor, and asserts replay order is unchanged.
2. Run the exact test and confirm it fails with a replay checksum mismatch or reordered outbox.
3. Add a logical ordinal column to segment rows. Keep physical sorting, validate unique contiguous outbox ordinals, and reconstruct outbox state by ordinal.
4. Re-run the focused regression and the full control-MVP suite.

### Task 2: Give the new restore layout a real version transition

**Files:**
- Modify: `crates/arco-catalog/tests/state_store_control_mvp.rs`
- Modify: `crates/arco-catalog/tests/workspace_snapshot_restore.rs`
- Modify: `crates/arco-catalog/src/state_store/control_mvp.rs`

1. Add `literal_old_layout_restore_plans_are_superseded_without_writes` for direct participant inspection/application and `workspace_restore_recovery_migrates_v1_and_v2_participant_plans_and_replans_them` for the actual v1/v2 workspace recovery path; require supersession followed by a current-version replan.
2. Run them and confirm current-path validation rejects the old fixtures.
3. Advance the current restore-plan version. Validate legacy plans using only their safe scope/identity/source invariants before returning `Superseded`; never apply or migrate them.
4. Re-run focused restore and workspace recovery suites.

### Task 3: Preserve opaque StateToken provenance

**Files:**
- Modify: `crates/arco-catalog/src/state_store.rs`
- Modify: `crates/arco-catalog/tests/state_store_intent_contract.rs`

1. Change the external contract tests to require source scope, logical sequence, and manifest evidence without constructing a `StateToken` from deserialized JSON.
2. Confirm the old public reconstruction API is the only way those tests compile today.
3. Remove public token reconstruction from both intent types and expose read-only source evidence accessors instead.
4. Re-run the intent contract tests and workspace compilation.

### Task 4: Make L0 Arrow the authoritative mutation payload

**Files:**
- Modify: `crates/arco-catalog/tests/state_store_segment_contract.rs`
- Modify: `crates/arco-catalog/src/state_store/control_mvp.rs`

1. Add a test committing a large value and projection intent, asserting the transaction JSON contains metadata and the L0 reference but no mutation/outbox payload fields, while replay remains exact.
2. Run it and confirm the payload is duplicated in JSON.
3. Stop serializing changes in `ControlMvpTxObject`. Decode and validate L0 rows into a hydrated transaction before applying replay. Keep restore rendering deterministic and checksum-bound.
4. Re-run segment, control, checkpoint, and restore suites.

### Task 5: Publish real Arrow IPC batch offsets

**Files:**
- Modify: `crates/arco-catalog/tests/state_store_segment_contract.rs`
- Modify: `crates/arco-catalog/src/state_store/control_mvp.rs`

1. Extend the index test to parse the Arrow footer and require every stored record-batch offset to equal the footer block offset, be nonzero, and be in bounds.
2. Run it and confirm the current constant zero fails.
3. Extract record-batch block offsets from the finalized IPC footer during index construction and recompute them during validation.
4. Re-run segment corruption and deterministic restore tests.

### Task 6: Close remaining recovery and malformed-artifact risks

**Files:**
- Modify: `crates/arco-catalog/tests/state_store_control_mvp.rs`
- Modify: `crates/arco-catalog/src/state_store/control_mvp.rs`

1. Add a normal-commit fault test where the head write succeeds and the transport returns an error; require readback reconciliation to return the committed outcome.
2. Add the checksum-coherent malformed-segment suite in `state_store_segment_contract.rs` (`checksum_coherent_malformed_arrow_cases_fail_closed_without_panics` and `checksum_coherent_null_origin_l0_trim_fails_closed_without_a_panic`) plus failure injection at transaction, L0, L0 index, L1, L1 index, manifest, and head writes.
3. Confirm each regression fails for the intended reason.
4. Add checksum-bound footer/schema/feature/block preflight before Arrow decoding, bounded row-count checks, and normal-head exact-byte plus visible-lineage reconciliation.
5. Re-run every focused control/restore suite.

### Task 7: Align ADR claims and verify the complete remediation

**Files:**
- Modify: `docs/adr/adr-043-s3-state-token-authority.md`

1. Label CAS retry budgets and index-pruned point/prefix reads as cutover requirements until implemented; describe the now-small JSON transaction accurately.
2. Run `cargo fmt --all -- --check`, `git diff --check`, `cargo xtask adr-check`, strict `arco-catalog` Clippy, all catalog tests, and `cargo test --workspace --quiet`.
3. Inspect the final diff and status. Do not commit or publish without authorization.
