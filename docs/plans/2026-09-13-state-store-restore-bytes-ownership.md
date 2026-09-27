# Step 3 restore Bytes backing ownership contract

2026-09-13. Frozen after independent root review. Runtime implementation and behavioral proof remain pending. It supersedes the FIFO receipt idea retained in `step3-bytes-backing-ownership-proposal.md`; that approach is rejected because an ambient queue cannot bind one exact response across cancellation or concurrent reads.

## Scope and conclusion

Add one additive classified range-read API for **authority-8 Workspace restore physical reads**. Both the bounded restore merge-unit reader and final-stream microchunk reader use it for selected V8 directory/index/descriptor/payload and receipt ranges. The V8 restore reader must not issue a bare `get` or bare `get_range` for such a physical object. Workspace request/journal/plan/progress/lock/seal/control records stay on their current control routes.

Ordinary catalog operations, ordinary authority-8 catalog reads/writes outside Workspace restore, and all V7 paths remain on the current `Bytes` APIs. There is no StorageBackend-wide resident-memory claim and no provider qualification in this change.

No existing response hook can supply this fact. `StorageBackend::{get,get_range}` return only `Bytes` (`crates/arco-core/src/storage.rs:111-122`) and `ScopedStorage` forwards only that value (`crates/arco-core/src/scoped_storage.rs:497-500,725-728`). `ScopedStorage::backend()` exposes `Arc<dyn StorageBackend>` (`:190-194`), so a concrete fixture side channel cannot safely travel with an exact response. Allocation counters likewise do not identify a pre-existing backing reached through a `Bytes` clone.

## Additive core API and compatibility

In `crates/arco-core/src/storage.rs`, define non-serialized Rust-only types next to `StorageBackend`:

```rust
#[derive(Debug)]
pub struct ClassifiedBytes {
    pub bytes: Bytes,
    pub ownership: BytesBackingOwnership,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BytesBackingOwnership {
    Unknown,
    BackendOriginShared { held_len: usize },
    NewRequestOwned { actual_capacity: usize },
    CopiedRequestOwned { actual_capacity: usize },
}
```

Then add the default method after `get_range`:

```rust
async fn get_range_with_ownership(
    &self,
    path: &str,
    range: Range<u64>,
) -> Result<ClassifiedBytes> {
    Ok(ClassifiedBytes {
        bytes: self.get_range(path, range).await?,
        ownership: BytesBackingOwnership::Unknown,
    })
}
```

This leaves the existing `get_range` signature, every existing implementer, all codecs, and all wire bytes unchanged. The default deliberately preserves the backend's ordinary range semantics and returns `Unknown`; it neither derives a capacity from `Bytes::len()` nor inspects `Bytes` internals. `ClassifiedBytes` must not derive serde and is never persisted.

Add an inherent method to the existing scoped type:

```rust
impl ScopedStorage {
    pub async fn get_range_with_ownership(
        &self,
        path: &str,
        range: Range<u64>,
    ) -> Result<ClassifiedBytes> {
        Self::validate_path(path)?;
        self.backend
            .get_range_with_ownership(&self.scoped_path(path), range)
            .await
    }
}
```

Also override the `StorageBackend for ScopedStorage` implementation's new method to call `ScopedStorage::get_range_with_ownership(self, path, range).await`. This matches the current structure: the inherent API owns path validation and the trait implementation delegates, just as the existing trait `get` delegates to `get_raw` and `get_range` validates/forwards (`scoped_storage.rs:718-728`). It preserves the response's class rather than recreating a bare `Bytes`.

Authority-8 physical readers do not access `ScopedStorage` directly. They hold `ScopedAuthorityStore`, whose private `storage: ScopedStorage` field is the existing narrow capability boundary (`crates/arco-core/src/authority_storage.rs:39-40,43-109`). Add only this matching method there:

```rust
pub async fn get_range_with_ownership(
    &self,
    path: &str,
    range: Range<u64>,
) -> Result<ClassifiedBytes> {
    self.storage.get_range_with_ownership(path, range).await
}
```

It delegates through the private scoped storage exactly as `ScopedAuthorityStore::get_range` does (`authority_storage.rs:76-84`); it must not expose a backend handle, take an unscoped path, or give control-MVP code any broader authority capability.

## Concrete truthful classifications

`MemoryBackend` overrides only the new range method:

```rust
async fn get_range_with_ownership(...) -> Result<ClassifiedBytes> {
    let bytes = self.get_range(path, range).await?;
    Ok(ClassifiedBytes {
        ownership: BytesBackingOwnership::BackendOriginShared {
            held_len: bytes.len(),
        },
        bytes,
    })
}
```

The statement is intentionally about **origin**, not a promise that the inventory keeps the object forever. At response creation, `MemoryBackend::get` clones the stored `Bytes`, and `get_range` slices that clone (`crates/arco-core/src/storage.rs:270-305`). The returned handle therefore shares a backing origin with the stored object. If that object is later deleted, the held handle may be its last ref; its report remains `backend_origin_shared` for the entire handle lifetime. It reports the exact visible held span and never reports a request-owned backing capacity, even where the stored input had excess `Vec` capacity.

`CountingBackend`, used by the synthetic cost fixture, wraps `MemoryBackend` (`crates/arco-catalog/benches/support/control_cost.rs:322-341`). Its new range method must use the exact same retry, injected-failure, pause, allocation and read-counter behavior as current `get_range` (`:598-650`) and forward the final successful `ClassifiedBytes` from `inner.get_range_with_ownership`. Refactor both public range methods through one private counted implementation if needed; they may not produce different storage-attempt counts. Test fault wrappers that delegate to `MemoryBackend` explicitly forward the new method to preserve its known origin class. Any wrapper that synthesizes a response either gives one truthful explicit class or inherits the default `Unknown`.

A class is a **trusted adapter contract**. The runtime's `actual_capacity >= len` check catches an impossible under-report but cannot establish actual capacity by itself. An adapter may state `NewRequestOwned` or `CopiedRequestOwned` only when it observed allocation capacity at the allocation/copy site. Existing provider adapters should initially inherit `Unknown`; a later override requires separate adapter evidence. This proposal does not qualify a provider.

## Runtime ownership wrapper

The runtime may not admit a `ClassifiedBytes` and return a bare `Bytes`, because that loses the release/lifetime accounting. The V8 restore physical reader instead converts it to a private non-`Clone` wrapper retained through decode and any carry:

```rust
struct AccountedBytes {
    bytes: Bytes,
    class: AccountedBytesClass,
    handle: OwnershipHandle,
}
```

`OwnershipHandle` holds an `Arc<Mutex<RestoreOwnershipLedger>>` plus a recorded class/amount. It is created only after the ledger accepts the response and decrements the matching current-live total in `Drop`. This uses existing standard-library ownership primitives only. `AccountedBytes` exposes only `fn as_slice(&self) -> &[u8]` for decoders and hashers: it exposes neither `&Bytes` nor `into_bytes`, so a consumer cannot create an untracked `Bytes` clone. A deliberate retained copy must allocate a new `Vec`/`BytesMut`, record the observed capacity as request-owned, and construct a separate `AccountedBytes`. The wrapper itself is the carry entry when a response crosses an await or microchunk boundary.

`decode_block_rows` already accepts `&[u8]` and returns owned `ControlMvpSegmentRow` data. Its row decoder copies keys and values with `to_vec()` (`crates/arco-catalog/src/state_store/control_mvp.rs:8921-8976`), while the transient Arrow `RecordBatch` remains local to the decode function (`:8645-8711`). The proposal therefore permits dropping an `AccountedBytes` immediately after successful `decode_block_rows` only because no decoded row aliases its input. Any future decoder output that borrows or holds an input buffer must instead retain the same `AccountedBytes`/ownership handle until every alias drops; a test must prove that behavior before it is used in restore.

Admission consumes `ClassifiedBytes` and validates before decoding, hashing, retaining, or another I/O:

```rust
match response.ownership {
    Unknown => fail_closed(),
    BackendOriginShared { held_len } if held_len == response.bytes.len() => {
        ledger.reserve_backend_origin_shared(held_len)?
    }
    NewRequestOwned { actual_capacity }
    | CopiedRequestOwned { actual_capacity }
        if actual_capacity >= response.bytes.len() => {
        ledger.reserve_request_owned(actual_capacity)?
    }
    _ => fail_closed(),
}
```

`BackendOriginShared` contributes to current/maximum `backend_origin_shared_held_bytes` and its exact lifetime. It is separate from request-owned live/carry and cache-pool totals. A buffer copied by restore is a request-owned allocation and remains charged as such even if the original was backend-origin shared. Every successful/error/cancellation result must leave both the request-owned and shared-handle ledgers at zero after wrappers drop; a final report with an unknown response or unbalanced handle is nonpassing.

The same private V8 restore `read_physical_range` helper is used by normal bounded merge units and final-stream microchunks. This is required because the frozen ordinary-unit peak also demands capacity-based carry accounting. Final stream keeps its separate ledger/phase reporting; ordinary merge units keep their existing 64 MiB admission. Neither route changes ordinary catalog/V7 behavior.

## Required red/green tests

1. **Default unknown is rejected at runtime.** A backend that implements only existing `get_range` supplies `Unknown`. The red proves old V8 restore physical read could decode or issue another I/O; green rejects before both and before output/progress/seal/prepared/HEAD publication.
2. **Merge-unit and final-stream routing.** Instrument a V8 restore that executes one normal physical merge read and one final microchunk range read. Both must reach the classified helper; a structural regression test rejects bare range calls in the V8 restore physical reader. An ordinary V7 restore and ordinary catalog request retain existing `get_range` behavior.
3. **Memory full and strict range classification.** Seed `MemoryBackend` with a `Bytes` from an over-capacity `Vec`. Both scope-relative full and strict-range classified reads through `ScopedAuthorityStore` return `BackendOriginShared { held_len == bytes.len() }`, never owned capacity. A traversal path still fails at the authority boundary. Delete the backend object after the response and retain the wrapper through a poll; the shared-handle ledger remains correct until wrapper drop without claiming the inventory still retains the object.
4. **Scoped forwarding and fixture forwarding.** A `ScopedStorage` classified read validates the relative path and preserves exact class. `CountingBackend` produces the same retries, failure result, read bytes and class on classified versus bare range reads. A delegated fault wrapper keeps MemoryBackend's class.
5. **Owned/copy capacity and invalid claims.** A test-only truthful owned/copy adapter reserves capacity greater than visible length and the ledger charges that capacity. Separate malformed responses with `actual_capacity < len` or shared `held_len != len` fail before decoding/next I/O. Passing `capacity >= len` alone is not treated as a proof in production; only the test adapter's explicit allocation-site contract enables the test.
6. **No ambient pairing under cancellation/concurrency.** Start two V8 classified range reads for distinct objects, cancel one after it returns, and retain the other. Its `AccountedBytes` remains paired with its own class and releases exactly once. The cancelled response cannot alter the other handle's ledger.
7. **No provider claim.** A production-shaped backend without an override receives the default `Unknown`; both V8 merge and final range admissions fail closed and evidence identifies unknown backing rather than silently charging zero.
8. **Decoded output has no input alias.** Retain an `AccountedBytes` during `decode_block_rows`, drop it after the returned rows are used across a poll, and verify the ownership handle releases once while every decoded key/value remains valid. This regression binds the existing `to_vec()` behavior; any decoder that instead exposes an input alias must retain the handle.

## Boundaries retained

This proposal provides response-backing classification only for authority-8 Workspace restore physical range reads. It does not classify ordinary storage reads, replace the allocator's conservative request-allocation measurement, solve hostile `ObjectMeta`/`WriteResult` pre-return allocation, or move cache-pool ownership into request-owned memory. It adds no dependency, generic backend wrapper, mandatory V7 caller capability, source wire change, provider qualification, deployment, or production claim.
