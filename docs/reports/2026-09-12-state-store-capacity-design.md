# State-store capacity design and local feasibility — September 12, 2026

Status: architecture follow-up and local feasibility complete. Production capacity
remediation is not implemented. Gate 7 remains nonpassing; no provider run or pilot
is implied by these results. No cloud commands were run during this follow-up.

## Outcome

The current restore scanner cannot admit the pilot's minimum 2,419,200 retained
receipt/audit rows. Streaming that scanner alone would not fix restore: the entire
source/current difference must also fit one L0. Ordinary lazy commits still replay
and hash full retained state. Raising limits would leave these costs unbounded.

The selected design replaces full-state authentication on narrow operations with
bounded authenticated directory paths, prepares large restores in durable bounded
units, and publishes one result through the existing single-HEAD CAS model. Logical
history remains separate from physical layout. Two atomically bound outbox indexes
support active-ID uniqueness and ordered delivery. Retention and operation limits
remain requirements. See [the design](../plans/2026-09-12-state-store-capacity-design.md)
for the exact invariants, proposed bounds, compatibility, and implementation sequence.

This needs a new authority/restore contract and explicit root conversion. It cannot
be represented as a behavior-preserving authority 7 / restore-plan 6 patch. Neither
new wire format nor conversion is implemented in this follow-up.

## Exact source and scope

- Worktree: `/Users/ethanurbanski/arco/.worktrees/state-store-vnext-gate7-capacity-20260912-01`.
- Branch: `codex/state-store-vnext-gate7-capacity-20260912-01`.
- Git base: `92fd19f11a547ece5004ac94cad83a3527471812`.
- Reconstructed Gate 7 checkpoint 05: 1,096 files, manifest SHA-256
  `079a7b5251ad06d34c90aa9cff12c54975ef2dab300a9ded31843a0474824924`.
- Incremental changes: one test-only capacity module, its test module declaration,
  a test-only helper using private production catalog encoders, and this report/design.
- Production APIs, encodings, limits and dependencies are unchanged relative to that
  checkpoint. The inherited Gate 7 diff remains uncommitted and unstaged too.
- The original Gate 7 snapshot was independently rechecked for exact paths, bytes,
  sizes and modes. Runtime cache commit `0da54a1c` remains unintegrated.

## Measured physical encoding

The fixture first executes two actual catalog mutations to obtain emitted typed
receipt and audit records. It then generates unique keys and production-shaped JSON
identities for the synthetic inventory. These are not 1,209,600 executed mutations
or a complete history-bearing catalog root. The sampled receipt and audit need not
belong to the same operation family. No compression or retention reduction is used.

| Measurement | Corrected run |
|---|---:|
| Synthetic rows | 2,419,200 |
| Production L1 shards | 100 |
| Decoded key/value bytes | 1,363,416,192 |
| Encoded segment bytes | 1,525,627,602 |
| Encoded index bytes | 17,969,615 |
| Largest shard rows | 27,776 |
| Largest segment bytes | 15,548,022 |
| Largest index bytes | 195,368 |
| Maximum accounted input batch bytes | 16,777,202 |
| Encode/decode experiment elapsed seconds | 311.445 |
| Command maximum resident bytes (`time -l`) | 2,086,633,472 |

Every shard used unchanged production half-capacity limits. Every decoded row,
including generation, ordinal, tombstone and origin fields, matched its input.
Ordered input/output key/value hashes matched. Corrupted segment and index bytes
were rejected by the production decoder. Raw per-shard sizes, key bounds, checksums
and durations are retained in `physical-layout.json`.

The batch accounting adds key/value lengths and 128 bytes per row; it is not heap,
cache ownership, or RSS accounting. Command RSS may include compilation and is not
a production-process bound. This single local run is a codec feasibility measurement,
not a five-run performance qualification or a provider cost forecast. Outputs were
decoded and checked then discarded; the evidence retains hashes rather than segment
payloads. The fixture is reproducible from source and actual sample records.

## Checks and retained failures

- Final catalog library suite: 497 passed, 0 failed; the full-size experiment is
  ignored in the ordinary suite and passed separately with explicit invocation.
- Original capacity regression reproduced unchanged: one-million-row / 64 MiB
  scanner bound still prevents the proposed pilot inventory.
- A value-heavy restore applied successfully: 1,572,928 decoded bytes, 1,598,418
  L0 bytes, and only 1,310 bytes of transaction JSON metadata.
- A disjoint source/current restore passed source scanning but exceeded a deliberately
  injected 32-row L0 limit with 48 KV writes and one notice; current `Validation`
  was returned and HEAD remained unchanged. Production defaults were unchanged.
- Catalog library/tests Clippy with warnings denied, workspace formatting, and
  whitespace hygiene passed. Rust 1.88.0, Python 3.11.14, locked offline dependencies,
  no incremental compilation, and no dev/test debug information were used.
- Preserved diagnostics: sandbox `time -l` resource-accounting denial despite a
  passing test, a test compilation error, the falsified 4 MiB restore-payload
  hypothesis, an initially incorrect expected error variant, and the superseded
  physical run with shortened synthetic manifest IDs. A first recovery reconstruction
  caught Git archive emitting group-writable modes under its default tar umask;
  the replacement archive explicitly uses 0022 and verifies modes against Git.
  The failed archive and diagnostic remain preserved. No production behavior was
  changed to force these tests to pass.

A fresh-context read-only audit raised history/physical-root, outbox ordering and
absent-HEAD inheritance findings. The design now specifies their resolutions;
`architecture-audit.md` preserves original findings and final dispositions. Those
are design resolutions, not implemented protocol proofs.

## Evidence and recovery

Evidence directory: `/private/tmp/arco-gate7-capacity-20260912-01`. It contains raw command logs/results, superseded contracts,
fixture measurements, audit, complete source/base manifests, incremental delta, and
binary patch. `run.py` records exact commands and build environment. For example,
set `ARCO_GATE7_CAPACITY_DESIGN_REPORT` to a new output path and run:

```sh
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo +1.88 test --offline --locked -p arco-catalog --lib --features test-utils \
  pilot_inventory_round_trips_through_bounded_production_l1_shards \
  -- --ignored --nocapture
```

The separate recovery archive contains complete candidate source, an exact Gate 6
base snapshot, the binary patch, and evidence, excluding build targets, Git metadata
and credentials. External `closeout.json` records the final archive checksum, actual
compressed-member verification, and reconstruction results from both Git and the
archive's own base. The earlier Gate 7 recovery archive remains preserved.

## Remaining work

1. Implement and qualify the new bounded directory/commit/restore protocol, including
   format conversion, maintenance, retention, uncertainty reconciliation and GC.
2. Exercise a real full-scale root with the full mutation mix and measure restore,
   replay, narrow writes, reads, ownership and growth against the frozen bounds.
3. Finish the pilot workload/cohort/SLO acceptance engine, integrate the separate
   cache-reuse candidate if selected, and rerun affected combined-source lanes.
4. Only after those prerequisites pass, prepare new exact-source provider/pilot
   execution packets. Real S3, 168 elapsed hours, remote CI on combined source,
   deployment and cutover remain separate and uncompleted.

Cloud compute remains at its previously verified teardown disposition; this
follow-up did not recreate or query it. Stored S3 evidence was preserved. No claim
of zero total AWS storage cost follows from terminating compute.
