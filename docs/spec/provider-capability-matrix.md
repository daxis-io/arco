> Status: Draft Phase 2 contract scaffold.
> Implementation status: Fixture-level conformance only.
> Architecture status: The control-store path is prototype-approved only, not accepted production architecture.
> Compatibility status: Provider write certification is not a public API guarantee.

# Provider Capability Matrix

This draft defines the evidence required before an object-store provider can be
used for production control-plane writes. It does not enable any production
provider by itself.

## Certification States

- `test-only`: used by local, memory, fake, or fixture providers. It can be
  write-enabled for tests without representing production safety.
- `not-certified`: no production write authority. This is the default for
  production providers until evidence is reviewed.
- `read-only-certified`: addressed reads are acceptable for read paths, audit,
  or migration, but the provider must not publish mutation-visible roots.
- `write-certified`: production writes are allowed only when every required
  evidence category is present and the CAS semantics are native or proven-safe.

## Required Write Evidence

A production provider can be `write-certified` or `write_enabled` only when it
has evidence for all of these properties:

- conditional object create;
- conditional pointer replace / compare-and-swap;
- stable version tokens returned by successful writes and usable by later CAS;
- addressed read-after-write for newly written objects and pointer heads;
- checksum or corruption detection for fetched authority objects;
- bounded retry and timeout policy;
- request-time correctness that does not depend on object listing.

The evidence must be provider-specific. A generic statement that the cloud API
supports a feature is not enough unless the Arco adapter maps that feature into
the control-plane error, token, and retry contracts.

## Ambiguous CAS

Ambiguous CAS includes any provider mode where:

- overwrite prevention is inferred from listing;
- etags, generations, or version IDs cannot be reused as stable compare tokens;
- a stale conditional write can appear successful;
- successful writes do not return a durable fence for the next update;
- cross-region or replicated metadata behavior is unknown for mutation-visible
  roots.

Providers with ambiguous CAS semantics must remain write-disabled and must not
be marked `write-certified`. They may be `not-certified` or, if addressed reads
have independent evidence, `read-only-certified`.

## Fixture Contract

The fixture at `fixtures/provider-capability-matrix-v1.json` is the current
machine-checked draft. The conformance test requires representative provider
coverage, requires production write-enabled providers to have CAS, stable
version-token, retry/timeout, checksum, and read-after-write evidence, and
rejects write-enabled ambiguous-CAS providers and listing-dependent request-time
correctness.

Live provider certification is a follow-up slice. This document and fixture do
not grant live production authority.

## Code-Level Provider Follow-Up

This Phase 2 slice intentionally uses a fixture-level matrix rather than
certifying live S3, GCS, Azure Blob, or S3-compatible adapters. Before any
production provider becomes write-enabled, add provider-specific tests for:

- conditional create by exact key;
- conditional pointer replacement using a stable version token, generation,
  etag, or equivalent fence;
- addressed read-after-write for newly written objects and pointer heads;
- checksum or corruption detection before authority use;
- bounded retry and timeout mapping for mutation-visible operations;
- duplicate retry and stale writer epoch behavior;
- proof that request-time correctness does not depend on object listing.

If any provider cannot prove native or proven-safe conditional replace, keep it
read-only, disabled, or not certified for mutation-visible roots.
