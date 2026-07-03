# Object-Store Control-Store MVP Slice

Goal: validate the object-store authority path while the old ledger plus synchronous compactor path remains production authority.

Base: current `origin/main` plus the Phase 3A deterministic state-store model
commit in this PR-prep branch.

This slice adds an internal `arco-state-control-mvp` backend inside `arco-catalog::state_store`. It writes immutable transaction objects and control manifests through existing object-store conditional-write primitives, publishes visibility only through pointer CAS, and serves `StateToken` plus minimal `CheckpointToken` reads by manifest-reachable replay.

## Scope

- Add `crates/arco-catalog/src/state_store/control_mvp.rs`.
- Keep all paths under `state-store/control-mvp/{domain}/`.
- Re-export `ControlMvpStateStore` and MVP helper types from `state_store` and `lib.rs`.
- Extend `ArcoStateReader` with `read_checkpoint(CheckpointToken)`.
- Return explicit `UnsupportedOperation` from current/model stores where checkpoint reads are unsupported.
- Do not touch production writer, reader, compactor, or API routing.

## Storage Protocol

- Transaction object: `txlog/{tx_id}.json`.
- Manifest object: `manifests/{manifest_id}.json`.
- Current pointer: `current.pointer.json`.
- Checkpoint object: `checkpoints/{checkpoint_id}.json`.
- Immutable transaction, manifest, and checkpoint objects use `WritePrecondition::DoesNotExist`.
- Pointer publication uses `DoesNotExist` for first publish or `MatchesVersion` for compare-and-swap updates.
- JSON envelopes carry SHA-256 checksums; corrupt or mismatched artifacts fail closed.

## Commit Protocol

- `begin_control_txn` captures current pointer version and manifest-selected folded state.
- Commit validates staged preconditions against the captured base.
- Commit writes the transaction object, writes the candidate manifest, CAS-publishes the pointer, then returns one `StateToken`.
- CAS loss returns `CatalogError::CasFailed`.
- Losing transaction and manifest artifacts are physical orphans and are never visible.

## Read Protocol

- Current reads resolve only `current.pointer.json` to selected manifest to selected transaction references.
- `read_at(StateToken)` loads the deterministic manifest path named by the token's `authority_manifest_id`, validates scope, sequence, and checksums, and replays only manifest-selected transaction refs.
- `checkpoint()` writes an immutable checkpoint record for the current manifest.
- `read_checkpoint()` opens the same retained manifest reader.
- Expiry and garbage collection behavior are deferred to later Phase 3B child slices.

## Projection Outbox

- Add an MVP-only transaction method to stage `ControlMvpProjectionOutboxRecord`.
- Expose current and token-pinned outbox reads that replay only records selected by visible manifests.

## Tests

- Add `crates/arco-catalog/tests/state_store_control_mvp.rs`.
- Cover immutable transaction and manifest writes followed by pointer CAS publication.
- Cover one returned visible `StateToken` per successful commit.
- Cover CAS loss leaving old state and old outbox visible only.
- Cover losing transaction and manifest artifacts remaining invisible without pointer-selected manifest reachability.
- Cover `read_at(StateToken)` resolving retained manifest state.
- Cover manifest-reachable replay folding to expected key-value state.
- Cover projection outbox records becoming visible only after the selecting manifest is visible.
- Cover checksum and corrupt-artifact failure failing closed.
- Cover no request-time correctness path calling object-store listing.
- Update `state_store_current_adapter.rs` for unsupported `read_checkpoint`.

## Verification

Baseline before implementation:

```bash
git status --short --branch
git log --oneline --decorate -4
cargo fmt --check
cargo test -p arco-catalog --test state_store_model
cargo test -p arco-catalog --test state_store_current_adapter
```

Final verification:

```bash
cargo fmt --check
cargo test -p arco-catalog --test state_store_model
cargo test -p arco-catalog --test state_store_current_adapter
cargo test -p arco-catalog --test state_store_control_mvp
cargo check -p arco-catalog
git diff --check
```

## Non-Goals

- Do not implement Phase 3C promotion, performance, or fallback gates.
- Do not implement Phase 4 shadow replay.
- Do not add custom segment formats.
- Do not move catalog DDL, grants, credential vending, governance writes, or production reads/writes to the MVP.
- Do not implement writer-epoch lease loss, token expiry, checkpoint expiry, or GC retention unless a narrow helper falls out without widening this slice.
