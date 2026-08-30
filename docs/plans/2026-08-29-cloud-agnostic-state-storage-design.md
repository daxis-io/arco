# Cloud-Agnostic State Storage Design

## Status

Accepted for implementation on `codex/s3-lambda-state-kernel-v1` after the
Phase 1 second-audit remediation.

## Problem

The `control/v1` state algorithm is written against `ScopedStorage`, but cloud
provider construction currently lives in `arco-core`. That makes the authority
algorithm portable in practice while leaving the crate graph, feature graph,
and qualification ownership cloud-coupled. `arco-core` imports the
`object_store` provider implementations, selects providers from bucket schemes,
and carries GCS, S3, and Azure behavior in one implementation. The catalog crate
also declares a direct `object_store` dependency even though its state-store
implementation uses the Arco storage interface.

Cloud agnosticism means the durable format and state transition algorithm do
not name or select a provider. It does not mean that every provider is
qualified. Each provider must independently prove the conditional-write,
version-token, consistency, and recovery semantics required by the kernel.

## Selected architecture

Keep one state-kernel implementation and introduce provider adapters at the
storage seam:

```text
arco-catalog control/v1 state kernel
        |
        v
arco-core provider-neutral storage interface
        |
        +-- arco-storage-object-store
        |      shared object_store translation
        |
        +-- arco-storage-s3
        +-- arco-storage-gcs
        +-- arco-storage-azure
        |
        +-- arco-core MemoryBackend for deterministic tests

arco-storage
        runtime-only provider selection and composition
```

`arco-core` retains the provider-neutral `StorageBackend`, opaque version
tokens, conditional-write outcomes, `MemoryBackend`, and tenant/workspace
scoping. It loses cloud builders, scheme routing, cloud-specific comments, and
its production dependency on `object_store`.

`arco-storage-object-store` translates `object_store` reads, metadata,
conditional writes, listing, and signing into the Arco interface without
selecting a cloud. Each provider crate owns its builder, credentials,
provider-specific capability decisions, and live qualification test. The
`arco-storage` composition crate selects one provider from deployment
configuration and returns `Arc<dyn StorageBackend>`; runtime crates depend on
that composition layer rather than teaching `arco-core` about provider schemes.

The control-state kernel receives a narrow `ScopedAuthorityStore` view with
only read, metadata, create-if-absent, and compare-and-swap operations. Listing,
deletion, signed URLs, and unconditional overwrite are not exposed through that
interface. Existing callers continue to construct it from `ScopedStorage`, so
fault-injection and compatibility tests remain useful without provider logic
entering the catalog crate.

## Provider contract

Every provider adapter must preserve these observable semantics:

1. Create-if-absent never overwrites an existing immutable object.
2. Compare-and-swap accepts only the exact current opaque version.
3. Successful conditional writes return a nonempty version that differs from
   the replaced version.
4. A stale version, including one retained across delete and recreate, cannot
   become valid again.
5. A precondition loss is a normal typed outcome, not a transport error.
6. Transport errors remain distinguishable so the state kernel can reconcile
   a potentially landed authority-head write.
7. Provider-internal retries cannot silently turn an unknown conditional-write
   outcome into a claimed failure or success.
8. Ordered paging and signing are provider capabilities outside the authority
   commit interface and may fail closed when a provider cannot satisfy them.

Repository conformance proves adapter behavior only for deterministic and
local substitutes. S3, GCS, and Azure remain separately qualified by ignored,
credentialed live tests and provider evidence. S3 remains the first GA target;
the artifact format and state algorithm are not S3-specific.

The provider builders disable upstream automatic request retries. This is
intentionally broader than conditional writes because the upstream clients do
not expose a separate retry policy for the authority write path. Callers own
bounded retry, and head writers reconcile a surfaced transport error before
claiming a conditional outcome. A future provider-internal retry mode requires
separate ambiguity and recovery qualification before it can be enabled.

## Alternatives considered

### Extract the entire catalog state kernel immediately

Rejected for this slice. The current state interface owns catalog errors,
workspace-restore records, projection intents, and catalog-specific recovery
traits. Moving those types at the same time as provider isolation would create
a large compatibility migration unrelated to cloud selection. A dedicated
state-kernel crate remains a valid later extraction once catalog-domain
ownership is separated deliberately.

### Keep one feature-gated provider implementation in `arco-core`

Rejected. The current provider features do not isolate dependencies, provider
selection remains core policy, and independent qualification ownership remains
unclear.

### Duplicate the state store per provider

Rejected. Transaction rendering, Arrow validation, bounded replay, restore,
and ambiguity reconciliation are provider-independent invariants. Duplicating
them would multiply correctness and audit surfaces.

## Compatibility and migration

- `StorageBackend`, `WritePrecondition`, `WriteResult`, `MemoryBackend`, and
  `ScopedStorage` retain their existing public contracts.
- Runtime construction changes from `ObjectStoreBackend::from_bucket` to the
  `arco-storage` composition interface.
- Tests that need a generic in-memory `object_store` adapter import it
  from `arco-storage-object-store`.
- The catalog removes its unused direct `object_store` dependency.
- ADR-043 is clarified to distinguish a provider-neutral `control/v1` format
  from the S3-first GA qualification target.
- No provider is promoted or certified by this refactor.

## Acceptance

- Architecture tests fail if `arco-core` or `arco-catalog` regain a production
  `object_store` dependency or provider constructors.
- Each provider crate implements `StorageBackend` and owns an ignored live
  conformance entry point.
- The runtime factory routes `s3://`/`s3a://`, `gs://`/`gcs://`, and
  `az://`/`azure://` without a provider branch in `arco-core` or the catalog.
- The control-state implementation uses only its scoped authority-storage
  interface.
- Existing catalog state-store contracts and the workspace test gate pass.
