# Restore Plan7 and unit-record wire contract

**Status: frozen after independent root review.** These are implementation requirements, not compiler, runtime, budget, or complete-restore proof.

Inputs: durable-restore contract SHA-256
`1e21182e7d3f1bdb1c590568c6e9676e10299348bf5fa714fc13003dd1fa40ac`; logical codec
`8c0c57d111a6e24848a08b47f5cbefb8faf83815b5be9c26c1e2fc4df25ead56`; final-stream admission
`ac4d815436e2c90678c938c1d930117aceb63d4dc8df321125ecb7a39bdfeccc`; invocation clarification
`a36561b4f7d90a7b8986aa5e7d270b4d06bec27b2516425e6ce41c153cc08d24`.

## Common encoding

Every new V1 record is a fixed `#[serde(deny_unknown_fields)]` struct with `record_type` and
`version = 1`; Plan7 is the explicitly versioned V7 exception. All are written as `serde_jcs`.
Readers decode the bounded raw object, validate every semantic/path/size field, re-encode JCS,
and require exact raw-byte equality. This rejects duplicate keys, defaults, unknown fields, and
alternate binary spellings.

All SHA-256 fields are lowercase `sha256:` hex. The frozen logical codec alone uses its specified
ordered binary primitives. New physical identity/body/chain preimages below use a tagged JCS body
struct. New outer-record binary fields use unpadded base64url and must round-trip exactly; new
outer times use `{seconds:i64,nanoseconds:u32}` with nanoseconds below one billion. Existing
nested `PersistedAuthorityReference`, `RestoreAttemptIdentity`, and `DurableAuthorityBinding`
codecs are preserved exactly and are not globally re-encoded by this proposal.

`tag_bytes("name")` means the ASCII bytes of `name` followed by exactly one byte `0x00`. The
printed spelling never contributes a reverse-solidus or ASCII `0` byte. Every new sum type uses
Serde's internally tagged form `#[serde(tag = "kind", rename_all = "snake_case")]`, with no
`content` wrapper: `start` is `{ "kind":"start" }` and a struct variant places all its named
fields beside `kind`. New option fields are exactly JSON `null` or their stated object. No new
record has an untagged or externally tagged enum.

Plan, selector, progress, receipt and final seal are each at most 4 MiB encoded; the separate
prepared record is at most 1 MiB. Control records use the exact-sized stable helper. Final-stream
receipt reads use the frozen fixed `4 MiB + 1` probe, including its charged HEAD/range read; they
are not silently changed to exact-sized control reads. Immutable writes require exact-byte
collision readback; selector writes use the observed object-version CAS.

## Wrapper and Plan7

Preserve `PersistedRestoreParticipantPlan::ControlMvp(ControlMvpRestorePlan)` and its raw
`RestoreParticipantPlanRecord` wrapper exactly. Add only its tagged sibling:

```text
{ "plan_kind": "control_mvp_v7", ...ControlMvpRestorePlanV7... }
```

The existing `plan_wire` remains the exact JCS value and `plan_sha256 = sha256(JCS(plan_wire))`.
That raw Plan7 digest binds every progress record while leaving V6 bytes unchanged. Plan7 exposes
common `source()` and `identity()` accessors after explicit variant dispatch.

`ControlMvpRestorePlanV7` fields:

| Group | Fields |
| --- | --- |
| Envelope | `record_type="control_mvp_restore_plan"`, `version=7`, `implementation="arco-state-control-mvp"`, `scope`, `identity: RestoreAttemptIdentity` |
| Trusted source | `source: PersistedAuthorityReference` (authority8 StateToken only), `source_reference_sha256`, `source_manifest: ImmutableObjectWitness`, encoded source KV/active-id/delivery-order roots, source logical sequence/history digest, source retention deadline |
| Trusted composition | exact canonical `durable_authority_binding: DurableAuthorityBinding` |
| Request window | `workspace_request_sha256`, `requested_at`, `execution_deadline`, `source_deadline` |
| Target/base | `mode: present|absent`, `target: PresentTargetWitness|AbsentTargetWitness`, base logical sequence/history digest and encoded KV/active-id/delivery-order roots |
| Logical result | `result_logical_sequence`, `restore_request_digest`, `logical_commit_id`, `restore_notice_intent_id`, `restore_notice_payload_b64` |
| Physical selection | `owner_generation`, `candidate_seed_sha256`, `candidate_id` |

`ImmutableObjectWitness` is `{path,byte_size,sha256}`. The path is canonical for the plan scope,
size is positive and within its role limit, and authenticated bytes must hash exactly when used. It
witnesses the raw source manifest without duplicating a potentially 4 MiB manifest inside Plan7.
The roots are exact encoded directory roots, not hashes alone.

`PresentTargetWitness` is `{current_pointer_path,current_pointer_raw_b64,current_pointer_sha256,
current_pointer_version,writer_epoch,reclamation_generation,manifest:ImmutableObjectWitness}`.
It has a nonempty version and its decoded raw pointer must name the copied base roots/history.
`AbsentTargetWitness` is `{current_pointer_path,absence_marker="does_not_exist",
observed_writer_epoch,observed_reclamation_generation,source_parent_writer_epoch,
source_parent_reclamation_generation}`. It permits only a later DoesNotExist HEAD CAS and derives
base roots/history from source; it never means a present empty pointer. The observed external
fences are exact planning witnesses, not zero/default/max-substituted source values. Planning and
final verification reject either observed fence below its authenticated source-parent fence, and
the existing child-parent verifier must prove monotonic manifest fences before candidate output is
accepted. The candidate seed commits the exact observed absent witness.

Validate `execution_deadline == requested_at + 24h` with checked arithmetic,
`source_deadline == source_retention_deadline`, and exact equality to the immutable workspace
request's timestamp. Validate the logical fields by recomputing the frozen logical codec from the
same source/base witnesses and original request time. The binding is compared with trusted
composition configuration, never an Arc identity or a default.

`candidate_seed_sha256` is deliberately independent of Plan7 raw digest and candidate ID, avoiding
a cycle. `owner_generation` is exactly `identity.attempt()` serialized as u64: it is the immutable
selected participant attempt, never a lock/epoch generation. The seed is:

```text
SHA256(tag_bytes("arco/control-v2/restore-candidate-v1") || JCS(CandidateSeedBodyV1))
```

`CandidateSeedBodyV1` has exactly these fields (JCS determines their canonical name order): `scope`, `identity`,
`workspace_request_sha256`, `durable_authority_binding`, `source_reference_sha256`,
`source_manifest_sha256`, `source_kv_root_b64`, `source_active_id_root_b64`,
`source_delivery_order_root_b64`, `source_logical_sequence`, `source_history_sha256`,
`requested_at`, `execution_deadline`, `source_deadline`, `mode`, `target_witness_digest`,
`base_kv_root_b64`, `base_active_id_root_b64`, `base_delivery_order_root_b64`,
`base_logical_sequence`, `base_history_sha256`, `result_logical_sequence`,
`restore_request_digest`, `logical_commit_id`, `restore_notice_intent_id`,
`restore_notice_payload_sha256`, and `owner_generation`. `target_witness_digest` is the tagged
digest below, and the payload hash must equal the decoded Plan7 payload. The body excludes
`candidate_seed_sha256`, candidate ID, Plan7 raw digest, receipts/progress/seals, and live
lock/epoch. The tag plus exact JCS body replaces an unspecified second binary codec.

`target_witness_digest` is `SHA256(tag_bytes("arco/control-v2/restore-target-witness-v1") ||
JCS(TargetWitnessBodyV1))`, where the body is the exact selected present/absent witness.
`candidate_id` is lowercase hex of the seed. The Plan7 raw digest subsequently binds it, but the
seed contains neither candidate ID, Plan7 raw digest, receipts/progress/seals, nor live lock/epoch.

## Deterministic paths and output IDs

Under the configured Control MVP base prefix, use only these paths. `C` is candidate ID, `N` and
`O` are decimal u64 exactly 20 digits with zero padding, and `P` is decimal u32:

```text
restore/v7/C/selector.json
restore/v7/C/progress/N.json                 # state after N receipts
restore/v7/C/receipts/O.json
restore/v7/C/final-seal.json
restore/v7/C/prepared.json
```

All components are generated bounded ASCII, never caller-concatenated. An output segment/state ID
for ordinal `O`, output part `P`, role byte `R` is lowercase hex of:

```text
SHA256(tag_bytes("arco/control-v2/restore-output-v1") || JCS(OutputIdBodyV1))
```

`OutputIdBodyV1` has exactly `{candidate_seed_sha256,ordinal,part,role}`. This keeps output
selection in the same tagged-JCS identity family as the seed, receipt body, and chain; production
descriptor/index/block codecs remain their existing codecs.

Existing production descriptor/index/block codecs and content-addressed directory paths remain
unchanged. Thus a retry writes the same IDs and bytes or receives typed immutable conflict; it
cannot pick a new ID after cancellation or a lost response.

## Receipt

`ControlMvpRestoreReceiptV1` at `receipts/O.json` contains:

```text
record_type="control_mvp_restore_receipt"; version=1
plan_sha256; identity; owner_generation; ordinal
predecessor_receipt_sha256; predecessor_chain_sha256
before: MergeCursor; after: MergeCursor
singleton_before: SingletonState; singleton_after: SingletonState
source_inputs: [InputWitness]; current_inputs: [InputWitness]; outputs: [OutputWitness]
prefix_cumulative_counts: CumulativeSemanticCounts; counts: SemanticCounts
receipt_body_sha256; chain_sha256
```

`mode` and `SingletonState.pending.phase` are bare-string enums: mode is exactly `"present"` or
`"absent"`; phase is exactly `"compare_source"`, `"compare_current"`, or `"emit"`. Every other
new sum follows the common internally tagged `kind` rule. `MergeCursor` has `global: GlobalCut`
plus `source: SideCursor` and `current: SideCursor`. `GlobalCut` is exactly `{kind:"start"}`,
`{kind:"end"}`, or `{kind:"after",key_b64url}`: it owns no root and no directory position. Each
`SideCursor` is `{kind:"start"}`, `{kind:"end"}`, or
`{kind:"after",key_b64url,position:DirectoryPosition}`.
No byte successor exists. `DirectoryPosition` names its role and the matching exact Plan7 root,
has at most eight `{page_sha256,child_index}` entries, and ends with exact leaf
`{first_b64url,last_b64url,rows,bytes,digest}`. Validation rereads every named directory page,
checks child index/fences/digest/root scope, and proves the `after` key is inside the named leaf or
at its exact exhausted boundary. Both before and after cursors are retained.

For example, source leaf `[a,m]` and current leaf `[n,z]` may advance global `start -> after(m)`
while source is `after(m)` and current remains `start`; the next receipt proves the gap
`(m,n)` by retaining current's unchanged position and its first fence `n`. A misaligned source
`[a,z]` / current `[b,c]` must retain both side positions while global advances only to the
earliest proved common cut. Empty sides use `end`; they never invent a one-root global position.

`SingletonState` is not just an enum. Under that tag rule it is exactly one of:

```text
{kind:"none"}
{kind:"pending",key_b64url,source:null|SingletonValueWitness,
 current:null|SingletonValueWitness,phase:"compare_source"|"compare_current"|"emit"}
{kind:"complete",key_b64url}
```

`SingletonValueWitness` is `{generation, tombstone, value_length, value_sha256,
descriptor:ImmutableObjectWitness, index:ImmutableObjectWitness, block, row_ordinal}`; payload
bytes are never persisted. `block` is the same authenticated descriptor/index/block witness used
by an input. Legal transitions are `none -> pending(compare_source)`, pending compare-source to
compare-current, compare-current to emit, emit to complete, then complete to none for the next
key. A global cut may move past that singleton key only in `complete`; every phase rereads and
charges payload bytes as required.

`InputWitness` is `{role,root_b64,directory_leaf,descriptor:ImmutableObjectWitness,
index:ImmutableObjectWitness,block:{offset,length,sha256,rows,min_key_b64url,max_key_b64url}}`.
`OutputWitness` is separate: `{role="kv",output_id,part,
directory_leaf:{first_key_b64url,last_key_b64url,rows,bytes,digest},
descriptor:ImmutableObjectWitness,index:ImmutableObjectWitness,
block:{offset,length,sha256,rows,min_key_b64url,max_key_b64url}}`. It has no `root_b64`, directory
path, or other reference to the candidate root, which does not exist while units are written.
Input lists are strictly ordered and authenticate their pinned exact roots, leaves,
descriptor/index/block fences, and decoded logical rows before use. Output lists are strictly
ordered by canonical leaf key and part; the final Builder verifies their descriptors/indexes/blocks
and constructs the candidate root from those ordered output leaves only after the terminal scan.

`SemanticCounts` is only deterministic u64 work: `source_leaves`, `current_leaves`,
`input_encoded_bytes`, `decoded_blocks`, `decoded_rows`, `output_blocks`,
`output_encoded_bytes`, and `mutations`, plus `reservation: UnitReservationV1`. It is
independently recomputed and limited by frozen ordinary/singleton admission. `UnitReservationV1`
is not an opaque number: `standard` is exactly `{kind:"standard",combined_leaf_limit:16,
decode_limit:64,input_byte_limit:67108864,output_block_limit:32,packed_output_byte_limit:33554432}`.
`singleton` is exactly `{kind:"singleton",phase,authenticated_payload_bytes:P,
input_byte_limit:67108864,segment_byte_limit:67108864,scratch_base_bytes:67108864,
scratch_payload_multiplier:24}` and requires the matching singleton witness/phase. Actual I/O,
response allocation, cache, retry and wall-time facts never enter a receipt.

`receipt_body_sha256` is `SHA256(tag_bytes("arco/control-v2/restore-receipt-body-v1") ||
JCS(ReceiptBodyV1))`, where `ReceiptBodyV1` is every listed receipt field except
`receipt_body_sha256` and `chain_sha256`. `chain_sha256` is:

```text
SHA256(tag_bytes("arco/control-v2/restore-receipt-chain-v1") || JCS(ReceiptChainBodyV1))
```

`ReceiptChainBodyV1` has exactly `plan_sha256`, `owner_generation`, `ordinal`,
`predecessor_chain_sha256`, and `receipt_body_sha256`.

`genesis_receipt_raw_sha256` is
`SHA256(tag_bytes("arco/control-v2/restore-receipt-none-v1") || JCS(GenesisReceiptNoneBodyV1))`,
where the body has exactly `{plan_sha256,identity,owner_generation}`. `genesis_chain_sha256` is
`SHA256(tag_bytes("arco/control-v2/restore-receipt-genesis-chain-v1") ||
JCS(GenesisChainBodyV1))`, whose body has exactly `{plan_sha256,identity,owner_generation,
genesis_receipt_raw_sha256}`. Receipt ordinal zero must name those values as its predecessor raw
and chain digests. Later ordinals must equal the authenticated direct predecessor's raw/chain
digests. This avoids a raw receipt/chain cycle and rejects substitution, reordering, or changed
selected plan before progress can advance.

## Progress, selector, and recovery

No receipt-reference collection exists. Each deterministic per-ordinal receipt is read with the
fixed <=4 MiB-plus-one final-stream probe. Final traversal derives `receipts/0.json` through
`receipts/(next_ordinal-1).json` in ascending order and retains only one decoded receipt at a time.
The raw receipt digest is `SHA256` of its exact stored JCS bytes; the predecessor raw digest plus
terminal chain/count proves the forward sequence without a collection layer or reverse traversal.

`RestoreProgressV1` at `progress/N.json` means exactly N selected receipts. Fields are:

```text
record_type="control_mvp_restore_progress"; version=1
plan_sha256; identity; owner_generation
next_ordinal=N; receipt_count=N; last_receipt: Option<ReceiptRef>; chain_sha256
cursor: MergeCursor; singleton_state: SingletonState; terminal: bool
cumulative_counts: CumulativeSemanticCounts
```

Genesis `progress/0.json` is immutable with the Plan7 genesis chain, no receipt, and
`singleton_state=none`, and `terminal=false`. Terminal is true only after a receipt has
global/source/current cursors at end and its `singleton_after` is either `none` or
`complete {key_b64url}`; it must never be `pending`. Thus an ordinary or empty final unit can
terminate with `none`, while a final singleton unit retains the completed key. An empty merge
writes one Start-to-End receipt. There is no second terminal-key field: the optional completed key
is represented solely by `singleton_state` being `none` or `complete {key_b64url}`.

`ReceiptRef` is exactly `{path,raw_sha256}` and path must be the deterministic ordinal path.
For every N, `progress/N` is derived only from `progress/N-1` and `receipts/(N-1)`: the receipt
before cursor and singleton state equal the predecessor progress values, its after cursor and
singleton state equal the new progress values, and its predecessor raw/chain digests equal the
direct predecessor. Genesis requires every side and global cursor to be `start`; terminal requires
every side and global cursor to be `end`. These are local continuity checks only: they reject an
immediate skipped, repeated, or cross-root cursor transition, but do not authenticate an unseen
prefix of the receipt chain.

`CumulativeSemanticCounts` has the same checked u64 dimensions as `SemanticCounts` except the
per-receipt reservation. Genesis is zero. Every receipt body additionally carries
`prefix_cumulative_counts: CumulativeSemanticCounts`: ordinal zero's prefix is zero; every later
ordinal's prefix must equal the selected direct predecessor progress totals. Each new progress
proves `cumulative_counts == receipt.prefix_cumulative_counts + receipt.counts` field by field
with checked addition. A resumed receipt's prefix is an untrusted selected-prefix claim: local
checks establish only that immediate arithmetic edge. At terminal,
`cumulative_counts.mutations` is the declared mutation count passed to
`logical_v2::RestoreHistory::new` before the first streamed logical change. The required
same-invocation final scan independently recomputes every cumulative field and requires equality;
it is not replaced by local metadata validation.

`RestoreProgressSelectorV1` is the only mutable record at `selector.json`:

```text
record_type="control_mvp_restore_progress_selector"; version=1
plan_sha256; identity; owner_generation; current_progress_path; current_progress_sha256
```

It stores no semantic counters. Its storage version is the selection generation. Before one
advance, the bounded local validation reads exactly selector, its selected `progress/N`, that
progress's `receipts/(N-1)` when non-genesis, and the proposed `receipts/N` collision-or-write
object; it never reads an older progress chain. It validates selected-plan/owner equality, the
selected last-receipt path/raw digest, selected chain equal direct-receipt chain, prior receipt
prefix-plus-count totals equal selected progress totals, proposed predecessor raw/chain equality,
proposed prefix totals equal selected progress totals, and proposed after cursor/singleton state
and totals form `progress/(N+1)`. It then writes
exact immutable receipt/progress bytes and CASes the selector from its observed version. A loser
re-reads selector and accepts only exactly the same selected progress digest. It never merges,
rebases, or adopts another attempt.

For genesis, local validation reads no prior receipt; it instead requires null `last_receipt`, the
explicit genesis chain digest, zero cumulative totals, and a proposed ordinal-zero prefix of zero.

The terminal tuple is current progress's `(next_ordinal,chain_sha256,receipt_count)`. The
same-invocation final scan begins at Plan7 genesis, authenticates every deterministic receipt
ordinal forward through that terminal tuple, and requires exact ordinal/predecessor/terminal-tuple
equality. While streaming it, the scan validates canonical source/current/output rows and their
cursor transitions. Only this full scan proves complete prefix coverage and supplies the
independent semantic-total recomputation; it retains one receipt at a time and needs no separate
preliminary pass.

### Cancellation and lost response before preparation

Live retention epoch, lock holder, lease renewal time, operation-response count, and clock are
not persisted in Plan7/receipt/progress/selector bytes. A retry under a new live epoch must
derive identical immutable bytes from the same selected plan.

If receipt/progress/selector work is canceled or a response is lost, recovery first performs
the selected Plan7 bounded inspection and authenticates the selector/current/direct predecessor.
It may settle an existing IN_FLIGHT workspace apply coordination only after this proves exact
nonterminal selected progress or exact terminal candidate state. Generic legacy `Ready` is never
safe retry proof. Exact immutable receipt/progress collisions are acceptable only when bytes,
plan, owner, ordinal, predecessor, and cursor chain all match; missing/corrupt/ambiguous progress
leaves the selected attempt RepairRequired.

Recovery also HEADs the deterministic `prepared.json` through the control ledger. Its authenticated
absence permits selected nonterminal retry only after the above proof and active source/deadline
gate. Once a prepared descriptor may exist, any cancellation/lost response changes the route to
read-only exact-candidate reconciliation; it never resends a candidate HEAD CAS, even under a new
lock epoch.

## Final seal and prepared-once boundary

`ControlMvpRestoreFinalSealV1` at `final-seal.json` is immutable:

```text
record_type="control_mvp_restore_final_seal"; version=1
plan_sha256; identity; owner_generation; candidate_id
terminal {next_ordinal,receipt_count,chain_sha256}
source_witness_digest; target_witness_digest; output_kv_root_b64
logical {restore_request_digest,logical_commit_id,result_sequence,
         restore_history_sha256,mutation_count,restore_notice_payload_sha256}
candidate {transaction:ImmutableObjectWitness,manifest:ImmutableObjectWitness,
           candidate_head_sha256,candidate_head_byte_size}
coverage_sha256
```

`coverage_sha256` is `SHA256(tag_bytes("arco/control-v2/restore-coverage-v1") ||
JCS(CoverageBodyV1))`. `CoverageBodyV1` has exactly Plan7 raw digest, terminal tuple,
source/target witness digests, output root, ordered final logical/history outputs, candidate object
witnesses, and recomputed semantic totals. Actual final-stream totals, retry/failure facts,
response ownership, cache ownership, live epoch, lease time, and clock belong only in the
monotonic runtime ledger and execution evidence. They are not fields of the fixed-path immutable
seal, coverage digest, logical identity, manifest certificate, or prepared descriptor. A seal
never grants publication authority.

`RestorePreparedV1` at `prepared.json` is a separate <=1 MiB restore-prepared record, not an
extension of an existing descriptor. It has `record_type="control_mvp_restore_prepared"`,
`version=1`, `plan_sha256`, `raw_plan_witness: RawPlanWitnessV1`, identity, owner generation,
candidate ID, final-seal path/digest, original target witness digest and exact original target HEAD
raw bytes/hash/size/version (or exact absent witness), terminal tuple, candidate
manifest/transaction witnesses, exact candidate HEAD bytes/hash/size, and `coverage_sha256`.
`RawPlanWitnessV1` is `{workspace_attempt:ImmutableObjectWitness, participant_domain,
participant_attempt, plan_wire_sha256}`. Validation rereads the authenticated immutable workspace
attempt, selects exactly that domain and attempt, JCS-encodes its preserved `plan_wire`, and
requires `plan_wire_sha256 == plan_sha256`. It therefore binds the original raw Plan7 and its
original exact target HEAD witness without reconstructing either from typed fields. The record
contains no lock token, epoch, lease expiry, operation time, or mutable progress pointer.

Only the private same-invocation capability writes seal then `RestorePreparedV1` after full
verification. It holds exact candidate HEAD bytes outside persisted records, re-fences the current
lock/epoch/selected plan, and performs one conditional present-version or absent-DoesNotExist
HEAD CAS. A re-run before prepared under a new epoch must reproduce the same output blocks,
manifest, restore certificate, seal, and descriptor bytes. Candidate manifest certificates bind
Plan7/terminal chain/output roots but never final-seal or prepared hashes; the seal binds manifest
and transaction after they exist. This one-way ordering prevents a self-cycle.

If `prepared.json` may exist, restart is exact read-only candidate reconciliation. It never sends
HEAD CAS again. A persisted seal, candidate ID, descriptor, or matching certificate cannot create
the private capability.

## Required first-implementation invariants

1. Reject Plan7 unless raw JCS, source/binding/window/target witnesses, candidate seed, logical
   codec fields, and stable owner generation all validate.
2. Reject receipt/progress/selector substitutions across plan, scope, participant attempt, owner,
   ordinal, predecessor, cursor, or candidate. Partial chains cannot mint authority.
3. Same selected plan under a new live epoch produces equal receipt/progress/output/manifest
   bytes. Changed target fences supersede instead of rebasing; replacement attempts get new
   physical paths and cannot adopt old progress.
4. Finalization streams the authenticated selected terminal tuple forward, independently
   recomputes coverage/output/counts/logical history/certificate, then creates a private
   capability. Prepared/seal presence never reconstructs it.
5. V6 values and workspace attempt bytes remain exactly unchanged. Plan7 accepts authenticated8
   retained sources only and rejects checkpoint/export/cross-format input before candidate I/O.
6. The restore certificate uses its distinct restore-kind transaction/transition references. No
   Plan7/unit/finalizer path materializes an ordinary full-job `Vec<Write>`; logical history is
   streamed with the frozen restore-history codec and physical output remains descriptor-witnessed.
