# Restore payload decoder admission

2026-09-14. Additive Step 3 contract. Authority 7, the Arrow/index wire bytes,
and the closed production constructors retain their existing behavior.

## Admitted calculation

For an authenticated descriptor let P be its selected encoded block length,
N its row count, and S the compiled size of ControlMvpSegmentRow. Require
0 < P <= 64 MiB, 0 < N <= 1,000,000, and P <= 256 KiB when N > 1.
The numerical qualification here is Rust 1.88.0, the locked dependencies,
and the 64-bit target whose retained layout/source proof establishes S <= 96.

Before the checksum, footer/message verifier, Arrow reader, row copies, or
outbox validation, reserve the checked cumulative requested-allocation bound:

```
D(P,N,role) = 8 MiB + C*P + N*S + 378
             + (16*P + 1,024*N + 4,096 if role == active_id, otherwise 0)
C = 7 when N == 1; C = 5 otherwise.
```

All arithmetic is checked. Malformed descriptors, unsupported ABI, overflow,
or a reservation above the remaining applicable live/cumulative allowance
return typed backpressure before invoking the decoder. The admitted amount
excludes the separately classified response and all starting carried state;
those remain charged in their existing ledgers. Actual cumulative allocation
counts shrink the held reservation after decoding and remain an overrun
witness. An underestimate stops the invocation, records the arriving bytes,
and cannot support publication. The calculation is admission, never a claim
that the allocator itself prevents every failed or external allocation.

The 5P term bounds footer plus reader body/metadata (P), disjoint alignment
copies (P + at most 6*63 bytes), copied key/value bytes (P), and the two
hexadecimal endpoints (2P). A singleton may render its one key twice, hence
7P. Both Binary arrays pass the pinned Arrow reader's full offset validation
before row access, so the sum of their copied ranges cannot exceed their
disjoint values buffers. The row vector allocates exactly N*S. Active-ID
validation adds the separately derived sequential typed JSON and canonical
encoding bound 16P + 1,024*N + 4,096; values in KV remain opaque bytes.

K=8 MiB includes the fixed successful Arrow owner graph and detailed error
formatting, including invalid footer/message data before schema admission.
The fixed schema, eight nodes/eighteen buffers, positive-buffer disjointness,
role minima, and normal Arrow validation remain mandatory. Caught panics,
allocator failure, poisoned ledgers, unknown response ownership, and counter
overflow remain nonpassing. No duplicate Binary-offset parser is added.

## Evidence and scope

The compiled layout probe reports S=96, fixed schema allocations 9,824 bytes,
reader headers 2,632 bytes, fixed error live bound 383,488 bytes and fixed
error cumulative bound 701,312 bytes. The combined fixed cumulative term is
713,768 bytes. Those are compiler layouts applied to source-derived counts,
not measured scratch maxima. The source binding covers locked Arrow,
FlatBuffers, serde/serde_json/bytes and pinned Rust allocation/formatting
sources; source or dependency drift requires requalification before a claim.

The pre-existing final-step accounting keeps cumulative decoder allocations,
returned owned capacities, and initial external plus ledger carry. It cannot
recover admission by dropping a previous decoded value in the same step.

This slice adds the payload reservation to the private synthetic reader.
Metadata and output allocations still need their own a-priori admission, and
oversized-singleton phase routing still needs its explicit one-payload
microchunk. This formula does not expand an ordinary unit or standard final
step, relabel an operation, or enable native restore advancement. A legal
singleton whose reservation cannot fit the current explicit route returns
backpressure until the separately frozen singleton route is implemented.
The native merge driver, durable receipts/progress, complete final coverage,
publication/recovery, and large-workflow evidence remain required.

The payload reservation replaces the observational all-remaining reservation;
it is never added on top of it. It uses the same minimum of the live ledger
allowance and, for final verification, cumulative allocation plus starting
carry allowance. The classified payload is already owned before admission,
and admission precedes checksum/preflight/reader work.

Retained qualification inputs before this amendment:

- Source fingerprint: `1828899eda2f1232761ee8fb595f57d580b51940a726852b4bcd0ebf13490ebf`.
- `native_decoder_source_drift_guard_03.py`: `90627611c98dd86f996981aaf806bb83d26fef2e90f6bafd9506f38596857b89`.
- `native-decoder-source-drift-binding-02.json`: `40f77a92e80d882d03b319fcbcdf0e0fb9b484d10c4d61c79cac57fee66ceb63`.
- `native-decoder-layout-probe-root-03.json`: `5f24b75aec8287a2c618041f4b27c0bf8763df05f0d51d1ab9f0c0eaa4378bb4`.
- `native-decoder-source-and-layout-evidence-root-02.json`: `04e3e95aa62ba967303fc7b482cede28b2234eb7be1faca59066b06a443c450e`.
- `step3-final-cumulative-layout-checkpoint-root-01.json`: `41613ce2e9e113a36427b62f2fc7eff0a5607ae3c64f3d37a2545bcb8dfc03c8`.

These inputs establish the pre-change derivation. The implemented reservation
still requires compiled behavioral red/green, actual decoder overrun checks,
and final source-bound verification. They do not establish the full native
restore contract.
