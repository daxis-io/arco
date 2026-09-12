# Bounded state directory — implementation slice 1

Date: September 12, 2026. Status: implemented and locally verified as a crate-internal
component. It is not connected to catalog publication. The overall capacity fix,
Gate 7 passage, provider testing and the 168-hour pilot remain incomplete.

## What changed

Added an immutable directory reader and streaming writer over logical block references.
A root binds ordered, nonoverlapping block intervals through scope-bound page hashes.
The reader supports candidate-block point lookup, half-open range scans, exact-root/
query pagination, and reopening from encoded root bytes. Unknown versions, malformed
metadata, corrupt objects and exceeded budgets fail closed.

Short fence keys live inside authenticated pages; longer fences use hash-bound objects.
The writer packs pages by both child count and worst-case fence-read bytes. It retains
at most one bounded page per level and uses existing scoped immutable matching writes.
An interrupted builder requires a restart; deterministic rebuilding reconciles objects
already created. No authority HEAD, routing, production cache, retention rule, or existing
format version was changed, and no dependency was added.

The final component contract is in
[the implementation plan](../plans/2026-09-12-state-directory-implementation.md).
Its amendments retain pre-release corrections; amendment 4 defines the final inline
wire representation. Earlier draft wire bytes are preserved as superseded evidence.

## Effective bounds and formats

| Property | Final component |
|---|---|
| Directory codec | Version 1, separate from authority 7 / restore-plan 6 |
| Root / child reference | 285 / 245 bytes |
| Page ceiling / full 128-child page | 65,536 / 31,403 bytes |
| Maximum levels / children per page | 8 / 128 |
| Page plus all external fence probes | At most 4 MiB; pack with fewer children when needed |
| Per-read ceilings | 4,096 object probes and 64 MiB probe bytes; caller may lower |
| Fence representation | Inline through 64 bytes; external through 256 KiB |
| Referenced block lengths | Through 256 KiB for multiple rows; existing singleton exception through 64 MiB |
| Scan result | At most 128 block references; continuation binds exact root and both range bounds |

Object hashes length-frame scope components, object kind and raw bytes. Inline mode is
selected only by length; nonzero padding, alternate representations and mismatched
hashes are rejected. Probes include an extra byte to detect oversized replacements.
Short-key lookups read only directory pages. No additional cache is involved.

Read budgets are fail-only guards. On exhaustion, no partial successful result escapes;
charged I/O stays charged. Row-limit pagination provides progress. Root decoding validates
structure and scope; the enclosing authority must authenticate that root before treating
it as authoritative. Page hashes are physical identities and do not replace logical history.

## Verification and measurements

- **515 catalog library tests passed**, with the pre-existing full-size capacity experiment
  ignored in the ordinary suite. All 18 new directory tests also passed explicitly.
- Catalog library/tests Clippy passed with warnings denied. Workspace formatting and
  incremental whitespace hygiene passed. Rust 1.88, locked offline dependencies, a
  separate cloned target, and disabled incremental/dev/test debug information were used.
- Tests cover multilevel construction, all ordered results across pagination, points and
  gaps, exact range endpoints, empty/reversed ranges, root/query-bound cursors, unknown
  versions, all scope components, duplicate/overlapping inputs, row overflow, malformed
  counts/levels/lengths, omitted/reordered children, and inconsistent parent summaries.
- Same-handle reads reject changed, deleted and oversized page/key objects. Controlled
  tests cover lost responses after an immutable PUT lands, poisoned-builder restart,
  matching/mismatching readback, and competing identical builders synchronized by a barrier.
- Canonical wire bytes are checked against an independent Python struct/hashlib vector.
  Key lengths 0, 1, 64 and 65, trailing-zero keys, wrong digests and nonzero padding are tested.
- Existing Arrow L1 bytes decode unchanged, including a real oversized singleton containing
  a 300 KiB value. An actual oversized-key encode is rejected by the existing index limit.
  That proves the fence bound covers existing L1 KV endpoints; it does not prove all future
  transaction/outbox key mappings or root conversion.
- Tests include a 16,385-block directory, an actual eight-level page chain, and a synthetic
  exact-depth-capacity boundary without allocating 128^8 leaves.

The final three-level point probe over 16,385 short-key block references performed
**3 object reads** with **79,416 bytes maximum measured allocation**.
The superseded external-short-key representation needed 521 reads and 38,432 peak bytes;
inlining exchanges modest page memory for fewer object requests. These are local component
measurements, not provider latency or catalog SLO qualification.

The maximum-fence fixture used 39 probes and 9,441,517 probe bytes,
below the 64 MiB ceiling. Hostile count decoding peaked at
38 allocated bytes before rejection. Allocation measurement surrounds the
individual request/decoder, excluding construction and the MemoryBackend's already
resident dataset; it is neither whole-process RSS nor a production cache ownership ledger.
Raw measurements are in `directory-sealed.log`.

## Review and preserved failures

The read-only audit is retained as `directory-audit.md`. Demonstrated issues were fixed
with failing regressions first: empty-range results, valid-but-unreadable maximum-fence
pages, exact depth-boundary finalization, and oversized-singleton compatibility. A separate
read-count regression drove inline short fences. Original failures, contract revisions,
pre-inline codec/vector/source, compilation and lint diagnostics remain preserved.

The earlier capacity candidate is unchanged in all 1,099 files. This slice changes one
existing file only to declare the directory module, and adds the module, tests and documents.
Existing authority algorithms and the separate runtime-cache handoff remain untouched.

## Source and recovery

Worktree: `/Users/ethanurbanski/arco/.worktrees/state-store-directory-20260912-01`. Branch:
`codex/state-store-directory-20260912-01`. Git base:
`92fd19f11a547ece5004ac94cad83a3527471812`. Input candidate manifest:
`11af6ae7f7649040bf4796da52729f01fb744000db3145cfdbe673ce2951f98b`.

Evidence: `/private/tmp/arco-state-directory-20260912-01`. `final-code-inputs.json` binds production/test/dependency inputs;
command JSON and raw logs retain exact invocations and resource accounting. For a
credential-free rerun with a separate target:

```sh
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo +1.88 test --offline --locked -p arco-catalog --lib --features test-utils control_mvp::directory::tests -- --nocapture
```

The recovery archive contains complete candidate source, exact Git base snapshot, binary
patch, source/base/member manifests, audit and evidence; it excludes Git metadata, build
targets and credentials. External `closeout.json` records the final archive checksum and
compressed-member verification plus reconstruction from both Git and the archive's own
base snapshot. Prior archives and failures remain preserved. Source stays uncommitted
and unstaged. This slice ran no cloud commands and created no compute.

## Next integration slice

The returned block digest/length does not locate a block inside a packed segment or
verify its decoded values. Next, bind physical block locations and semantic endpoint/count
validation, then implement bounded authenticated catalog commits and transition certificates.
Keep old-root and outbox-key conversion explicit. Durable restore, maintenance/equivalence,
retention/GC, full pilot-scale workload, combined cache-source qualification, provider runs
and the elapsed seven-day pilot follow. None is established by this component alone.
