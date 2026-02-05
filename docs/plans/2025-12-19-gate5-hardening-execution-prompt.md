# Gate 5 Hardening Execution Prompt

> **For Claude:** Use `superpowers:executing-plans` to implement this plan. Apply patches from the delta document before implementing each task.

## Execution Context

**Primary Plan:** `docs/plans/2025-12-18-gate5-hardening.md`
**Authoritative Patches:** `docs/plans/2025-12-19-gate5-hardening-plan-patch.md`

**Rule:** Where the patch doc differs from the main plan, the patch doc is authoritative. Apply each patch's changes before implementing the corresponding task.

---

## Engineering Standards

### Code Quality Requirements

1. **Type Safety First**
   - Use Rust's type system to make illegal states unrepresentable
   - Prefer compile-time guarantees over runtime checks
   - Every typed key (`LedgerKey`, `StateKey`, etc.) encodes invariants

2. **Test-Driven Development**
   - Write failing test BEFORE implementation
   - Run test to verify it fails for the right reason
   - Implement minimal code to pass
   - Refactor with tests green
   - Commit after each green cycle

3. **Compile-Time Enforcement**
   - Compile-negative tests via trybuild MUST actually fail
   - No commented-out assertions
   - If a method shouldn't exist, prove it with a compile error

4. **Documentation**
   - Every public API has doc comments explaining WHY, not just WHAT
   - Critical invariants documented with `# Safety` or `# Panics` sections
   - Link to ADRs for architectural decisions

### Security Standards

1. **Capability-Based Security**
   - Components receive only the capabilities they need
   - No "god object" backends passed around
   - Trait bounds encode permission boundaries

2. **Defense in Depth**
   - Code-level: Trait separation prevents wrong method calls
   - IAM-level: Prefix-scoped permissions as infrastructure enforcement
   - Runtime-level: Fencing tokens detect stale actors

3. **Unforgeable Tokens**
   - `pub(crate)` constructors for security-critical types
   - Only the lock module can mint `FencingToken`
   - Only `PermitIssuer` (from `LockGuard`) can mint `PublishPermit`

### Verification Requirements

1. **Every Task Must Have**
   - At least one test that would catch regression
   - Verification command to run before marking complete
   - Clean `cargo clippy` and `cargo test` output

2. **Integration Points**
   - IAM changes: Smoke test proves enforcement
   - Trait changes: Compile-negative test proves separation
   - Manifest changes: Succession validation test proves monotonicity

---

## Execution Phases

### Phase 1: Critical Path (Tasks 1-5)

**Objective:** Remove 90% of Gate 5 risk via mechanical enforcement.

#### Pre-Flight Checks

```bash
# Ensure clean starting state
cargo test --workspace
cargo clippy --workspace -- -D warnings
git status  # Should be clean
```

#### Task 1: Tier-1 Write Path + ADR-018

**Apply Patches:**
- Patch 8: Sync compaction via RPC with explicit event keys (no listing)
- Patch 11: Supersede older path conventions in ADR

**Implementation Order:**
1. Create ADR-018 documenting the decision
2. Refactor `Tier1Writer` to append ledger events only
3. Create sync compaction RPC handler in compactor
4. Update `Tier1Writer.update()` to call compactor RPC
5. Verify with test: API code path contains no Parquet writes

**Verification:**
```bash
cargo test -p arco-catalog
# Verify no ParquetWriter usage in tier1_writer.rs
grep -r "ParquetWriter" crates/arco-catalog/src/tier1_writer.rs && echo "FAIL: Still writing Parquet" || echo "PASS"
```

#### Task 2: Split Storage Traits + Typed Keys

**Apply Patches:**
- Patch 2: Split `PutStore` → `LedgerPutStore` + `StatePutStore`
- Patch 3: Update manifest keys to `*.manifest.json` naming
- Patch 7: Add `MetaStore` for artifact verification

**Implementation Order:**
1. Create typed keys module with `RootManifestKey`, `DomainManifestKey`
2. Create `ReadStore` trait (get, get_range only)
3. Create `LedgerPutStore` trait (API only)
4. Create `StatePutStore` trait (Compactor only)
5. Create `CasStore` trait (manifest CAS only)
6. Create `ListStore` trait (anti-entropy only)
7. Create `MetaStore` trait (artifact head checks)
8. Add compile-negative tests (MUST FAIL)
9. Update `ScopedStorage` accessors

**Verification:**
```bash
cargo test -p arco-core compile_negative
# All compile-negative tests should pass (meaning they fail to compile)
```

#### Task 3: PublishPermit with Real Distributed Fencing

**Apply Patches:**
- Patch 4: Make `FencingToken` constructor `pub(crate)`
- Patch 5: Add `fencing_token` field to manifest, enforce monotonicity
- Patch 6: Add `commit_ulid` to manifest

**Implementation Order:**
1. Create `FencingToken` with `pub(crate)` constructor
2. Create `PermitIssuer` that takes `&LockGuard`
3. Create `PublishPermit` with private fields and TTL
4. Create `Publisher` as sole publish API
5. Update `LockGuard` to expose `permit_issuer()`
6. Add `fencing_token` and `commit_ulid` to manifest
7. Enforce fencing token monotonicity at publish time
8. Add real stale holder test

**Critical Test:**
```rust
#[tokio::test]
async fn test_stale_holder_cannot_publish_even_with_fresh_version() {
    // Holder A gets token=1, loses lock
    // Holder B gets token=2, publishes successfully
    // Holder A reads FRESH manifest version (after B's publish)
    // Holder A tries to publish with stale token but fresh version
    // This MUST FAIL because token=1 < prev.token=2
}
```

**Verification:**
```bash
cargo test -p arco-core test_stale_holder
cargo test -p arco-catalog publish_permit
```

#### Task 4: IAM Prefix Scoping

**Apply Patches:**
- Patch 1: Remove `api_write_manifests` (compactor is sole publisher)
- Patch 9: Split compactor into fast-path SA (no list) and anti-entropy SA

**Implementation Order:**
1. Create `iam_conditions.tf` with strict `startsWith` conditions
2. Remove bucket-wide grants from `iam.tf`
3. Create IAM smoke test using typed keys
4. Add smoke test to CI workflow

**IAM Validation:**
```bash
cd infra/terraform
terraform plan -var-file=environments/dev.tfvars
# Review: No bucket-wide objectUser grants
# Review: API has NO manifest write permission
# Review: Compactor fast-path has NO list permission
```

#### Task 5: Rollback Detection

**Apply Patches:**
- Patch 7: Use `MetaStore::head_state` for artifact verification

**Implementation Order:**
1. Add `parent_hash` to manifest (sha256 of raw bytes)
2. Implement `compute_manifest_hash(raw_bytes)`
3. Implement `validate_succession` with all checks:
   - `snapshot_version` must not decrease
   - `commit_ulid` must be greater (lexicographic)
   - `fencing_token` must be >= previous
   - `parent_hash` must match raw bytes
4. Update `verify_snapshot_artifacts` to use `MetaStore`
5. Add rollback detection tests

**Verification:**
```bash
cargo test -p arco-catalog rollback
cargo test -p arco-catalog test_pointer_regression
cargo test -p arco-catalog test_parent_hash
```

---

### Phase 2: Architecture (Task 6)

#### Task 6: Compactor Split

**Implementation Order:**
1. Create `NotificationConsumer` (processes explicit paths, no list)
2. Create `AntiEntropyJob` (cursor-based, bounded per run)
3. Wire into compactor main with separate entry points
4. Add tests for notification-only convergence

**Verification:**
```bash
cargo test -p arco-compactor notification
cargo test -p arco-compactor anti_entropy
```

---

### Phase 3: Operations (Tasks 7-8)

#### Task 7: Backpressure

**Apply Patches:**
- Patch 10: Use watermark positions, not list counts

**Implementation Order:**
1. Create `BackpressureState` with position-based lag calculation
2. Implement soft/hard threshold evaluation
3. Add `retry_after` to rejection responses
4. Integrate into API handlers

#### Task 8: Search Tombstones

**Implementation Order:**
1. Create `SearchTombstone` struct with expiry
2. Add tombstone events for delete/rename
3. Add hot-token skew test

---

### Phase 4: Compliance (Task 9)

#### Task 9: ADR-019 Existence Privacy

**Implementation Order:**
1. Create ADR documenting Option 1 (partitioned snapshots + IAM)
2. Document migration path

---

### Phase 5: CI/Observability (Tasks 10-12)

#### Tasks 10-12: Simulation, CI Tiers, Metrics

**Implementation Order:**
1. Create deterministic simulation harness
2. Configure CI tiers (fast, nightly, E2E)
3. Add metrics catalog and dashboards

---

## Commit Standards

### Commit Message Format

```
<type>(<scope>): <summary>

<body explaining WHY>

<breaking changes if any>

Gate 5: <specific requirement addressed>
```

### Types
- `feat`: New feature
- `fix`: Bug fix
- `refactor`: Code change that neither fixes nor adds
- `test`: Adding tests
- `docs`: Documentation only
- `chore`: Build/tooling changes

### Example

```
feat(core): split storage into prefix-scoped capability traits

Implement Gate 5 trait separation so readers literally cannot
call write/list/head methods at compile time.

BREAKING: PutStore split into LedgerPutStore + StatePutStore.

Gate 5: #2 - Steady-state readers have no list/write capability.
```

---

## Review Checkpoints

### After Each Task

1. **Tests Pass:**
   ```bash
   cargo test --workspace
   ```

2. **Lints Clean:**
   ```bash
   cargo clippy --workspace -- -D warnings
   ```

3. **Compile-Negative Tests Verify:**
   ```bash
   cargo test -p arco-core compile_negative
   ```

4. **No Regressions:**
   - Run full test suite before committing
   - Check that existing functionality still works

### After Phase 1

**Comprehensive Verification:**

```bash
# 1. All unit tests pass
cargo test --workspace

# 2. No clippy warnings
cargo clippy --workspace -- -D warnings

# 3. Compile-negative tests work
cargo test -p arco-core compile_negative

# 4. Coverage check (optional)
cargo llvm-cov --workspace

# 5. Doc tests pass
cargo test --doc --workspace
```

**Manual Review Checklist:**

- [ ] `FencingToken::from_lock_sequence` is `pub(crate)`
- [ ] `PermitIssuer::from_lock` takes `&LockGuard` reference
- [ ] Manifest has `fencing_token`, `commit_ulid`, `parent_hash`
- [ ] `validate_succession` checks all three monotonicity fields
- [ ] IAM has NO `api_write_manifests` binding
- [ ] Smoke tests use typed keys and `LedgerPutStore`
- [ ] No `put_raw` or `WritePrecondition::None` in API code

---

## Failure Recovery

### If Tests Fail

1. **Do NOT mark task complete**
2. Investigate root cause
3. Fix the issue
4. Create NEW commit (never amend pushed commits)
5. Re-run verification

### If IAM Smoke Tests Fail

1. Check Terraform state matches expected
2. Verify service account credentials
3. Check IAM condition expressions
4. Test with `gcloud` CLI to isolate issue

### If Compile-Negative Tests Pass (Bad)

This means the code SHOULD fail to compile but doesn't:
1. Verify test file has uncommented forbidden calls
2. Check trait bounds are correctly restrictive
3. Ensure no blanket impls are leaking methods

---

## Success Criteria

**Gate 5 is complete when:**

| Requirement | Verification |
|-------------|--------------|
| Trait separation enforced at compile time | `compile_negative/*.rs` tests pass |
| Typed keys prevent prefix bugs | All storage calls use typed keys |
| PublishPermit is unforgeable | `pub(crate)` constructors verified |
| Real distributed fencing | `test_stale_holder_cannot_publish_even_with_fresh_version` passes |
| IAM enforces sole writer | `test_api_cannot_write_state` passes in deployed env |
| Rollback detection works | `test_pointer_regression_rejected` passes |
| Raw byte hashing | `test_parent_hash_detects_concurrent_modification` passes |
| Torn-write prevention | `test_mid_write_crash_never_referenced` passes |
| Backpressure uses positions | No listing in backpressure calculation |
| Compactor fast-path no list | Separate SA with no list permission |

---

## Execution Command

To begin execution, use:

```
/superpowers:execute-plan
```

Or invoke the skill directly:

```
Use superpowers:executing-plans to implement docs/plans/2025-12-18-gate5-hardening.md
with authoritative patches from docs/plans/2025-12-19-gate5-hardening-plan-patch.md
```

**Critical:** Always apply the relevant patch before implementing each task. The patch document is the authoritative source for code changes.
