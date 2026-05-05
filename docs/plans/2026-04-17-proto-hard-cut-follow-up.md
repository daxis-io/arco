# Proto Hard-Cut Follow-Up Implementation Plan

**Goal:** Finish the highest-value proto hard-cut follow-up work by locking the protobuf JSON policy, realigning the public catalog read models to `catalog/schema/table`, and making the orchestration contract boundary explicit without rewriting unrelated runtime internals.

**Architecture:** Treat the current `proto/arco/*/v1` tree as the authoritative post-cut surface, then tighten it in place. Start with policy and low-blast-radius schema changes that are mostly exercised through `arco-proto` and transaction tests, then update the small number of adapters and tests that depend on those shapes. Keep runtime behavior stable unless a failing contract test forces a code change.

**Tech Stack:** Protobuf, Buf, `prost`/`tonic`, Rust, `serde`, `cargo test`

---

### Task 1: Lock the Protobuf JSON policy

**Files:**
- Modify: `buf.yaml`
- Modify: `proto/STYLE.md`
- Modify: `README.md`
- Test: `crates/arco-proto/tests/golden_fixtures.rs`

**Step 1: Write the failing test**

Add a test in `crates/arco-proto/tests/golden_fixtures.rs` that round-trips a representative control-plane/catalog payload through `serde_json` and asserts the JSON field names are part of the supported contract.

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-proto --test golden_fixtures -- --nocapture`
Expected: FAIL because the current tests do not yet cover the declared JSON contract.

**Step 3: Write minimal implementation**

- Change `buf.yaml` breaking mode from `FILE` to `WIRE_JSON`.
- Update `proto/STYLE.md` to state that generated protobuf JSON is an official contract and that breaking checks must preserve JSON field compatibility from the new baseline onward.
- Update the repository README section for protobuf/codegen to say JSON support is intentional, not incidental.

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-proto --test golden_fixtures -- --nocapture`
Expected: PASS

### Task 2: Realign catalog public read models to `catalog/schema/table`

**Files:**
- Modify: `proto/arco/catalog/v1/catalog.proto`
- Modify: `crates/arco-proto/tests/control_plane_transactions.rs`
- Modify: `crates/arco-proto/tests/golden_fixtures.rs`

**Step 1: Write the failing test**

Add tests that assert:
- `Catalog` serializes with `catalog`, not `id`/`name`
- `Schema` serializes with `catalog` and `schema`, not `id`/`catalogId`/`name`
- `Table` serializes with `catalog`, `schema`, and `table`

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-proto --test control_plane_transactions -- --nocapture`
Expected: FAIL because the current messages still expose the old id-based field names.

**Step 3: Write minimal implementation**

Update `proto/arco/catalog/v1/catalog.proto` so the public read models use the canonical public nouns:
- `Catalog.catalog`
- `Schema.catalog`, `Schema.schema`
- `Table.catalog`, `Table.schema`, `Table.table`

Keep the existing mutation operation family for this slice unless a compiler error proves a production adapter depends on the old read-model shape. If fields are removed, reserve their numbers/names inside the post-cut file instead of silently reusing them.

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-proto --test control_plane_transactions -- --nocapture`
Expected: PASS

### Task 3: Make the orchestration contract boundary explicit

**Files:**
- Modify: `proto/arco/orchestration/v1/orchestration.proto`
- Modify: `proto/arco/controlplane/v1/transactions.proto`
- Modify: `crates/arco-proto/tests/control_plane_transactions.rs`
- Modify the control-plane transactions implementation plan.

**Step 1: Write the failing test**

Add/extend contract tests that prove:
- the event envelope is intentionally public cross-process input for control-plane commits
- callback output and published output remain distinct
- any JSON escape-hatch field that remains public is explicitly justified

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-proto --test control_plane_transactions -- --nocapture`
Expected: FAIL if the tests require a clearer contract declaration than the schema/docs currently provide.

**Step 3: Write minimal implementation**

- Add schema comments documenting that `OrchestrationEventEnvelope` is the supported cross-process event contract for transaction commits.
- Add comments documenting that HTTP/UI layers may map richer runtime states into smaller public enums elsewhere.
- If feasible without breaking runtime call sites, split the proto file into read/callback vs event files in a later slice; do not do it in the same commit if it adds noise without behavior change.

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-proto --test control_plane_transactions -- --nocapture`
Expected: PASS

### Task 4: Verify adapters and transaction tests after schema changes

**Files:**
- Modify as needed: `crates/arco-api/src/control_plane_transactions.rs`
- Modify as needed: `crates/arco-api/src/grpc_transactions_tests.rs`
- Modify as needed: `crates/arco-api/tests/control_plane_transactions_api.rs`

**Step 1: Write the failing test**

Do not write new adapter logic first. Re-run the existing transaction and gRPC suites after the proto edits and use the failures as the red phase for any required adapter fixes.

**Step 2: Run test to verify it fails**

Run:
- `cargo test -p arco-api --test control_plane_transactions_api -- --nocapture`
- `cargo test -p arco-api grpc_transactions_tests -- --nocapture`

Expected: FAIL only if the schema changes exposed a real adapter dependency.

**Step 3: Write minimal implementation**

Update only the affected adapters/helpers to match the new schema. Avoid broad refactors in the transaction runtime.

**Step 4: Run test to verify it passes**

Run the same commands again and confirm both suites pass.

### Task 5: Final verification

**Files:**
- No new files unless verification exposes a missing doc/test

**Step 1: Run targeted verification**

Run:
- `cargo test -p arco-proto --test golden_fixtures -- --nocapture`
- `cargo test -p arco-proto --test control_plane_transactions -- --nocapture`
- `cargo test -p arco-api --test control_plane_transactions_api -- --nocapture`
- `cargo test -p arco-api grpc_transactions_tests -- --nocapture`
- `buf lint`

**Step 2: Evaluate gaps**

If verification passes, record the remaining intentionally deferred slices:
- shrinking `arco.common.v1`
- broader catalog mutation/read-model unification
- deeper orchestration public/internal event surface reduction
- new-post-cut Buf baseline freezing in CI
