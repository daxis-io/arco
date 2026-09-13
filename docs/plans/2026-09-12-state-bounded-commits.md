# Step 2: authenticated blocks and bounded catalog commits

Status: frozen implementation acceptance contract, revision 1. No passing claim.
Base: e29a5f6a3f99c4050a78a794a85c7e125a421a12.
Authority 8 is explicit synthetic/test construction only; production remains 7.
Directory version 1 wire bytes remain unchanged. Restore-plan remains 6; no 7.
Runtime-cache commit 0da54a1ca5101e5d523b18f7aa116eb4ace059b8 is excluded.
Subsequent PR 429/430 changes require coordination and a new source identity.

## Authority and physical authentication

One existing scoped HEAD path. Incompatible current formats return
UnsupportedAuthorityFormat. Pin with metadata-before, bounded pointer read,
metadata-after: require equal nonempty versions and exact matching sizes.
Retry changed versions at most three times; exhausted attempts are unresolved.
Corruption, missing selected bytes and unavailable evidence never imply genesis.
Validate scope, format, fences and manifest raw digest before accepting a root.
Retained reads check their private digest witness then dispatch on manifest format.

Authority 8 wraps every directory root with role and
leaf_encoding=physical_descriptor_v1. Bare directory roots do not grant authority.
Opaque leaf hashes bind descriptors with scope/role; exact segment and index
identities, lengths, digests and immutable versions; checked block offset,
encoded length, digest, record kind, endpoints and row count. Use existing scoped
paths and production Arrow/index codecs. Authenticate descriptors and indexes
before exclusions. Validate selected versions and lengths around range reads.
Decoded ordering, endpoints, generations, tombstones, ordinals and payload
semantics must agree. Cross-role substitution and incomplete index coverage fail.

Directory pages <=65536 bytes, fanout <=128, depth <=8; existing object/fence
probe ceilings remain. Multirow blocks <=262144 bytes; valid singletons retain
the existing 67108864-byte segment ceiling. Charge singleton payload/scratch
separately from directory metadata. Preserve request/cache ownership ceilings.

## Logical identity and V2 envelopes

Freeze stable operation identity, generated IDs and timestamps before retries.
Logical commit ID hashes scope, prior logical history, next sequence, stable
operation identity, family and request digest. Exclude retry count, HEAD version,
fences and physical artifact IDs. V2 receipt/audit/projection envelopes have
logical commit identity and sequence, deterministic encoding and no physical
manifest identity. Separate logical-history codec v2 hashes exact opaque KV
bytes, generations, deletes, ordered outbox additions and exact-incarnation trims.
Physical provenance is authenticated by the transition certificate. Construct
candidate KV root, projection-source descriptors referring to that root, then
outbox entries referring to those descriptors to avoid a manifest hash cycle.
Preserve public V1 results/notifiers. Explicit V2 result/notification paths require
an injected V2 notifier. V1 entry points reject V2 intents.

## Mutation, proof, delivery and publication

Extend concrete transaction/catalog execution; reuse command interpretation,
object/name-index encoders, receipt/audit staging and frozen command retries.
No ordinary format-8 commit may enter materialize_for_commit, full replay,
full-state checksum or streaming directory construction. Path-copy only selected
blocks and affected ancestors; locally split with existing byte targets. Preserve
unchanged references. Prune empty outbox branches/collapse roots. Repacking and
occupancy rebalancing are Step 4.

Authenticate points and complete range coverage at the pinned root, including
empty ranges, insertion gaps and boundaries. Preserve tombstone generations and
never-present distinction; excessive predicates return typed backpressure.
An independent transition verifier accounts for each old child slot exactly once
as unchanged or authorized replacement. New leaves equal old authenticated rows
plus canonical mutation. Reject omitted siblings, duplicate paths, extra leaves,
changed untouched subtrees and invalid split/collapse coverage.

One manifest binds KV, active-ID and delivery-order roots. Streaming synthetic
genesis verifies the paired-index bijection. Stage requires ID absence; trim
requires exact incarnation in both indexes. No trim/restage in one transaction;
later restaging uses a new incarnation. Bounded delivery pagination verifies
active-ID and physical source descriptor for every record. No complete-ID scan
or whole-outbox sort in narrow work.

Persist immutable prepared-candidate descriptor before CAS: original HEAD bytes,
version/fences and exact transaction/proof/manifest/candidate HEAD identities.
Recovery uses retained candidate ID; existence does not establish commitment.
Exactly one conditional HEAD publication per candidate. Known conflict reexecutes
the frozen catalog command with new state-dependent decisions and atomic semantic
records. Reconciliation distinguishes committed, conflict, superseded, unresolved.
Bounded authenticated ancestry is exceptional recovery work. Missing evidence,
budget exhaustion or cancellation never becomes assumed retry-safe failure.
Unsupported new-format checkpoint, restore, conversion, maintenance, reclamation
and old projection workers fail closed.

## Behavioral evidence

Before each behavior change retain a compiled behavioral failure; compilation
failure alone is not red evidence. Catalog/schema/table commands must match the
independent small-state oracle for objects, name/ID indexes, generations,
tombstones, receipts, audit, sequence and ordered outbox. Independently check V2
history and published-root physical provenance. Cover point/range/empty/gap
conflicts, delete/recreate, stale generations, frozen-command reexecution and
exact idempotency replay/conflict. Cover forged transitions, omitted/duplicate
intervals, extra replacements, corrupt locators/indexes/objects, cross-scope/role
substitutions, malformed Arrow metadata and corruption in either outbox index.
Barrier-controlled writers exercise stale writer/reclamation fences, cancellation,
restart and before/after/lost-response schedules, including reconciliation after
HEAD advances. Retained tokens/cursors remain pinned through commit/trim/restage;
unattached candidates cannot mint readable authority. Cache disabled/default/
pressure modes cover changed/deleted warm objects, cancellation ownership,
existing pool ceilings and zero accounting underestimates. Identical frozen
mutations over differently partitioned equivalent fixtures must produce equal
logical IDs/history and distinct physical candidates; provenance tampering fails.

## Frozen measurement matrix and ceilings

Independent retained receipt/audit row axis: 4096,65536,1048576,2419200.
Independent active ordered outbox axis: 128,4096,65536.
Use production-shaped V2 records and verified paired indexes in explicit streaming
synthetic genesis. Label construction separately from actual measured mutations;
this is not 1209600 executed pilot mutations. Fixed scenarios: create catalog,
create schema, register table, patch existing catalog, rename table, drop table,
idempotent replay, stale-generation conflict, ordered outbox page, exact trim.
For each scenario/size/cache mode run five repetitions of 200 measured operations.
Freeze generated operations before timing; creation scenarios use distinct IDs.
Each standard scenario declares T<=16 selected/touched blocks including predicate
boundaries; exceeding T is nonpassing, not permission to expand the bound.
For actual depth D: directory-page reads and writes each <=4D(T+1), block decodes
<=4T, rewritten blocks <=2T. Full replay/checksum/streaming-builder ordinary
inputs must equal zero. Standard request peak owned bytes <=67108864; cache pools
are separate. Oversized singleton cases report payload-dependent scratch ceiling
before measurement. No singleton result counts as standard-fixture acceptance.
Count all physical reads/writes/metadata HEADs/CAS attempts and bytes, decoded
rows, hashed bytes, directory references processed and peak owned allocations.
Include external-fence probes, descriptors, proof validation, failures and retries.
Missing counters or mandatory proof are nonpassing.

## Verification, handoff and publication

Rust 1.88, locked dependencies, incremental off, dev/test debug off, own target
and evidence; coordinate sequential Cargo ownership and preserve other artifacts.
Run affected catalog/protocol/integrity/outbox/fault/cache and format-7 lanes,
strict Clippy, formatting, documentation and hygiene including untracked files.
Preserve baseline failures; prerequisite repairs require owner coordination.
Fresh-context read-only correctness/security audit receives contract, full diff,
dependency comparison, exact source manifest, test inventory and raw evidence.
Fix demonstrated in-scope P0/P1/P2 with red/green evidence and rerun final source.
Update capacity report and Step 3 handoff for restore preparation, merge/coverage
proofs, provenance, reconciliation, deadlines/fences. Identify Step 4 directory,
descriptor, proof and projection-source reachability for conversion/retention/GC.
Archive complete source, pinned base, reconstructable delta, manifests/evidence
with hashes; exclude credentials/build trees/Git metadata, verify members/modes
and reconstruction. Only after validation/audit DCO-sign commits, push this new
branch and open draft PR against main with PR 429 dependency and PR 430 exclusion.
No measured commit amendments or merges. No provider, deployment or cutover claim.
Substantive amendments are recorded and hashed before affected measurements.

## Amendment 1: physical descriptor encoding, before resolver implementation

physical_descriptor_v1 is deterministic serde JSON of a deny-unknown-fields
struct, in declared field order: encoding_version (1), scope, role, segment,
segment_version, index_version, block. Roles encode as kv, active_id, delivery_order.
Scope, segment and block reuse existing production structs/codecs. The descriptor
object path is the scoped control-v1 domain prefix plus physical/descriptors/
and its lowercase SHA-256 raw digest plus .json. Its raw digest occupies the
unchanged directory-v1 leaf digest; leaf bytes still denotes encoded block length.
The descriptor bound is the existing 1 MiB control JSON ceiling. Root wrappers
must independently declare role and physical_descriptor_v1 before resolution.

Segment and index objects retain their existing scoped paths. Descriptor loading
checks scope/role and owning leaf endpoints/count/length before selecting data.
Selected segment/index HEADs must equal their descriptor's nonempty exact versions
and lengths both before and after the reads. Full production index validation
precedes selection; exactly one index block must equal the descriptor block.
Descriptor object substitution cannot change the raw digest. The existing block
preflight/decoder authenticates raw bytes and decoded metadata; the resolver also
checks reference sequence, KV generations/tombstones and role-specific records.
No old worker gains permission to interpret either outbox role from these types.

## Amendment 2: V2 logical codec, before implementation

Binary preimages begin a u64-BE-length-framed ASCII domain tag, u32-BE encoding
version 2, and separately u64-BE-length-framed tenant/workspace/domain bytes.
Tags: arco/control-v2/logical-commit-id, arco/control-v2/history-genesis,
arco/control-v2/history-step. Digest fields are validated lowercase 64-hex and
encoded as their decoded 32 bytes. Commit identity then encodes preceding history,
nonzero next sequence (u64 BE), stable operation ID, family and request digest.
Operation/intent/family identifiers use existing immutable-identifier validation.

History encodes preceding history, sequence, logical commit ID, canonical unique
writes sorted by raw key, ordered additions, and exact-incarnation trims. Counts
are u64 BE. Writes encode framed key (including valid empty keys), nonzero
generation <= sequence, kind byte 0 delete or 1 value, then framed opaque value
bytes for kind 1. Ordinary commit verification additionally requires result-sequence
generations. Additions preserve caller order with exact 0..N ordinals, matching
scope/sequence/logical commit ID, and validated nonempty opaque payloads. Trims
sort by record ID bytes and origin sequence, reject duplicate incarnations,
validate origin sequence in 1..=sequence, and encode ID, origin and exact ordinal.
The ordinary verifier separately proves live incarnation and forbids trim/restage.

ProjectionIntentV2 deterministic JSON field order is contractVersion (2), intentId,
projectionKind, sourceScope, sourceLogicalSequence, logicalCommitId, ordinal,
payload (byte array). Unknown fields and invalid versions/IDs/scope/sequence/digest/
payload are rejected on decoding. No physical field participates in these codecs.

## Amendment 3: canonical directory transition layout, before verification

An ordinary path-copy transition has one canonical physical layout. Starting from
the authenticated old pages, each affected leaf slot is resolved once against the
immutable old leaf list, then the canonical mutation is applied. Affected ancestors
retain their old child order, substituting only the recursively rewritten slots;
their child sequences are greedily partitioned by the existing fanout and
fence-probe byte limits. Empty branches are removed and a single-child root is
collapsed exactly as the updater does. The verifier independently reconstructs
that layout from the old pages and edits, using the shared directory-v1 page codec
only to derive exact wire references. Its expected root must equal the candidate
root. It may not accept an alternate wrapping, repartitioning, or novel untouched
subtree merely because the final logical leaves are equivalent. This is not an
occupancy-rebalancing or repacking policy; those remain deferred to Step 4.


## Amendment 4: authority-8 manifests and prepared candidates

Manifest8 uses deterministic deny-unknown-fields JSON with format_version 8,
implementation, scope, manifest_id, logical_sequence, logical_history, authenticated
parent manifest identity/digest, writer_epoch, reclamation_generation, and KV,
active-ID, delivery-order wrapped roots. Each wrapper specifies role,
leaf_encoding physical_descriptor_v1, and exact directory_root_hex. The manifest
also binds candidate transaction and transition-certificate references by raw digest.
All roles are mandatory and scope checked. Pointer8 retains the pointer field
shape and binds the raw manifest digest; pinned pointer fences dominate manifest fences.

PreparedCandidateV1 is immutable deterministic deny-unknown-fields JSON, record_type
arco_control_bounded_prepared_candidate, encoding_version 1, scope, candidate_id,
original_head, candidate_tx, candidate_manifest, candidate_head, and three role
transitions. It preserves original HEAD raw bytes/digest/version/fences/manifest
(or explicit initially absent genesis), exact candidate bytes and reference digests.
It is stored at control/v1/domains/{domain}/prepared/{candidate_id}.json using
DoesNotExist before one conditional HEAD publication. The descriptor is bounded
by the existing 1 MiB JSON ceiling; transaction payloads remain external references.
Root transitions preserve exact old/new roots and canonical ordered bounded edits.
Verification authenticates old/new physical rows and canonical mutation semantics
independently, then requires the exact canonical structural transition.

Recovery by retained candidate identity does not infer commitment from artifacts.
Exact authenticated candidate HEAD or bounded authenticated ancestry proves
Committed. Exact original HEAD is Unresolved after uncertain/cancelled publication;
Conflict requires trusted definitive precondition-failure evidence. Superseded
requires an authenticated conflicting successor at candidate sequence and complete
bounded ancestry to the original authority. Missing/corrupt evidence or exhausted
budgets remains Unresolved. Candidate CAS is never retried.


## Amendment 5: paired V2 outbox physical payloads

Active-ID OUTBOX rows use UTF-8 record ID keys and deterministic deny-unknown JSON
with camelCase recordId, originSequence, ordinal, sourceDescriptorSha256, payload.
Payload is a byte array containing the exact serialized ProjectionIntentV2 envelope;
outer fields match that envelope and the physical row. The source descriptor raw
digest authenticates scope, logical identity/sequence, and the candidate KV root.
Delivery-order OUTBOX keys are fixed 20-digit sequence, slash, fixed 20-digit ordinal,
slash, record ID; value bytes are the same ASCII source-descriptor SHA-256.
Both physical row kinds have generation zero, tombstone false, exact incarnation
origin and ordinal, and logical_sequence equal to the physical segment materialization
sequence. The origin may precede that sequence. Delivery reads authenticate the
active entry and physical source descriptor before returning a record.


## Amendment 6: complete transition certificates and opaque-value references

Transition8 fields are format_version 8, scope, transaction_id, roles. Roles is an
exact three-entry role-checked array; each RoleTransition has role, old_root_hex,
new_root_hex, edits. Each EditProof has old (optional LeafProof) and new (ordered
LeafProof array). LeafProof contains first_hex, last_hex, rows, bytes, digest_hex.
The verifier authenticates referenced descriptors and old/new production block rows
itself, validates canonical mutations and exact directory layout, and runs before CAS.
The aggregate unique selected/touched leaf count across all roles and predicate
predecessor/successor boundaries is at most 16.

Transaction8 writes are ordered by raw key and contain key, generation, delete,
value_sha256 (omitted for deletes). The value digest is the raw SHA-256 of opaque
value bytes. Candidate KV descriptors in the transition identify the exact new
blocks containing these values. Verification recovers and authenticates the exact
values before computing logical-history-v2 bytes; transaction, manifest, and prepared
JSON never embed full KV values. Transaction8 also contains ordered V2 additions
and sorted exact-incarnation trims.

Manifest8 additionally references a projection source when additions exist.
ProjectionSource8 contains encoding_version 1, scope, logical_sequence,
logical_commit_id, kv_root_hex. Its path is derived from its raw SHA-256 beneath the
scoped projection-sources prefix. Historical active and delivery entries resolve
this digest independently of the latest manifest. Transaction and three-role
transition references remain raw-SHA-256 authenticated by the manifest.


## Amendment 7: independent logical-operation identity validation

Transaction8 additionally contains a required operation object with operation_id,
family, and request_digest. The transition verifier independently recomputes the
logical commit ID from that frozen tuple, scope, prior logical history, and next
sequence before validating logical-history bytes. A merely well-formed digest,
even when paired with a recomputed history hash, is insufficient provenance.

Bounded work accounting preserves historical 36-slot phase reports. A separate
per-phase BoundedWork record tracks decoded_rows (all rows submitted to a decode
attempt, conservatively including a rejected batch), transition_proof_rows,
selected_blocks, rewritten_blocks, directory_references (node encode/decode
attempts, including root summaries), and streaming_builder_inputs. Direct SHA-256
paths charge exact framed input lengths into the existing SHA counters. Failed
operations retain their charges. Missing bounded counters remain nonpassing.

## Amendment 9: independent axis combinations and singleton scratch

The measurement configurations are exactly (retained receipt/audit rows, active
outbox records): (4096,128), (65536,128), (1048576,128), (2419200,128),
(4096,4096), and (4096,65536). The shared baseline occurs once. These are two
independent sweeps, not a Cartesian product. Each configuration uses the ten
previously frozen scenarios, five repetitions of 200 operations, and all three
cache modes. Default uses the existing default configuration; disabled removes
the cache; pressure uses 1048576 metadata bytes and 4194304 decoded bytes.
Report fixture construction, bootstrap, warmup and executed measured commands
separately. Failed operations and retry work remain in the measured request.

Oversized-singleton probes use opaque payload lengths 307200, 8388608 and
62914560 bytes, subject to the unchanged 67108864 encoded segment ceiling.
The declared request scratch ceiling for those probes is 16777216 + 12P bytes,
where P is the exact supplied payload length; directory metadata retains its
unchanged limits. Report actual encoded sizes, payload length and measured peak
for every cache mode. These probes never satisfy the standard 64 MiB request
acceptance lane. A payload that cannot fit the existing encoded segment ceiling
must fail with typed capacity backpressure before publication.

## Amendment 10: explicit streaming synthetic genesis

Manifest8 distinguishes `kind` values `transaction` and `synthetic_genesis`.
Transaction manifests require transaction and transition references and no
genesis_witness; synthetic genesis manifests require genesis_witness and no
transaction or transition references. No ordinary transition is exempted from
the bounded edit proof. Genesis has no parent, may begin at a declared positive
logical sequence, and is admitted only by the explicit test/synthetic constructor
when the scoped HEAD is absent. Later transaction manifests advance that declared
sequence by one and carry the authenticated genesis manifest as their parent.

The raw-digest genesis witness is deterministic deny-unknown-fields JSON:
record_type `arco_control_synthetic_genesis`, encoding_version 1, scope, fixture_id,
logical_sequence, logical_history, three wrapped roots, and per-role row/block
counts. Its scoped path is `synthetic-genesis/{fixture_id}.json`. The constructor
streams ordered exact logical KV rows and V2 intents, verifies every produced
descriptor/index/block against those inputs before publication, and derives both
outbox indexes from the same exact-incarnation input. Generated intents are ordered
by both active ID and delivery key; inputs not satisfying both orders are rejected.
The witness is an explicit synthetic construction attestation, never evidence of
executed historical catalog commands. Readers authenticate its digest and all
scope/root/count/history bindings without replaying the inventory. An unattached
witness cannot mint a retained token. Publication remains one prepared-candidate
conditional HEAD write from observed absence.

Fixture logical history uses codec v2 framing under the domain
`arco/control-v2/synthetic-genesis`: framed domain, u32 version 2, framed scope,
u64 logical sequence, u64 KV count, exact ordered KV tuples using the existing
history-step key/generation/delete/value encoding, then u64 intent count and exact
ordered V2 intent tuples using the existing logical encoding. There are no trims.
The incremental encoder checks declared counts and order; it never collects the
complete inventory. Physical partitioning, fixture ID, object versions and root
digests do not enter this logical hash. Paired-index physical provenance is bound
after the candidate KV root exists. Different valid block partitions of identical
inputs must produce equal fixture history and later logical commit identities.

## Amendment 11: retained genesis intent incarnations

Synthetic genesis accepts active V2 intents from historical origins: source scope
must match, origin sequence must be positive and at most the declared genesis
sequence, and each envelope must satisfy the V2 codec. Logical IDs may vary with
origin. Active inventories may retain gaps in ordinal numbering after trims;
genesis does not apply the contiguous-ordinal rule for new transaction additions.
Both strict active-ID order and strict delivery tuple order remain mandatory.
The constructor writes projection sources for the exact origin/logical-ID pairs,
each bound to the completed candidate KV root, and derives both indexes from the
same intent stream. Ordinary commits retain the stricter current-sequence and
contiguous-addition rules. No historical intent implies a historical command was
executed by this synthetic fixture constructor.

## Amendment 12: prepared synthetic candidates

PreparedCandidateV1 retains its frozen record type, encoding version, original
HEAD witness, exact candidate manifest/HEAD bytes and digests, and single-CAS
publication rules. It adds `kind` matching Manifest8 and optional
`candidate_genesis`. Transaction candidates require candidate_tx and the three
transition proofs, with no candidate_genesis. Synthetic candidates require the
genesis witness reference, no candidate_tx, and no ordinary transition proofs.
Every mutually exclusive field combination is validated before publication and
again during recovery. Both kinds share the same publication and reconciliation
implementation; an existing prepared descriptor alone never establishes commitment.

## Amendment 13: measured fixture reset and recovery reads

The capacity harness builds each axis fixture once on a privately owned in-memory
backend. Between scenario repetitions it may conditionally replace only that
backend's scoped HEAD with saved authenticated fixture bytes. Reset is test setup,
outside request measurement, and records old/new object versions and reset counts.
Immutable objects retain their original versions. Unreachable candidates remain
in the backend and are reported separately from active inventory. No production
restore capability is introduced. Cache construction and warmup are labeled setup;
each measured command records its frozen request identity before its timer starts.

Candidate reconciliation performs reads only, including when original or current
HEAD is absent. Computing the canonical empty directory reference must not persist
an empty page. Normal first publication persists that page as candidate work.

## Amendment 14: conservative request allocation acceptance

The existing allocator counts deallocations of pre-existing warmed objects against
the current poll. Its net-live watermark can therefore underestimate request
ownership. The retained regression holds 4 KiB across a poll, evicts a pre-existing
16 KiB object, then allocates another 8 KiB; the old calculation reports only 8 KiB.

`peak_owned_bytes` now denotes a conservative upper bound: cumulative allocator
bytes attributed to request polls, not an exact resident-allocation peak. The
separately labeled `net_poll_peak_bytes` is diagnostic only. Acceptance applies
the unchanged 64 MiB ceiling to the conservative upper bound; exceeding it remains
nonpassing pending a qualified exact ownership measurement. The measured future
clones frozen owned command inputs inside its allocation scope while retaining
the original outside, so owned input copies enter this bound. Cache pools remain
separately owned and reported. Oversized singleton payloads remain a separate lane.

Independent post-publication tuple export and oracle verification are separately
labeled verification work. Ordinary measured commands include their mandatory
internal transition verifier. Additional exporter work must not be substituted
for missing internal proof counters. Scaling ceilings use the declared T=16,
with actual selected predecessor/predicate blocks independently required <=16.

## Amendment 15: one bounded recovery evidence budget

Exceptional candidate reconciliation may inspect at most 32 manifests in the
current authenticated ancestry and consume at most 64 MiB of encoded object
evidence. One request-local byte budget starts before the prepared descriptor
read and covers candidate/original manifests, transaction and transition bytes,
projection/genesis witnesses, current HEAD bytes including retries, and ancestry
artifacts. Metadata HEAD calls remain separately counted physical operations.

Every bounded JSON read reserves its maximum probe length before issuing a GET;
an insufficient remaining budget returns unresolved without issuing that GET.
After a successful read, unused reservation is refunded. Failed or cancelled
reads retain their reservation conservatively. This may return unresolved when
a final small object would fit but its maximum probe cannot be admitted. It must
never read first and discover budget exhaustion afterward. The injected smaller
limits in tests exercise this same reader and reconciliation implementation.

## Amendment 16: oversized singleton conservative allocation bound

The retained 8 MiB singleton measurement failed the original 16 MiB + 12P
ceiling: cumulative allocation was 176,616,192 bytes versus 117,440,512.
The rewrite succeeded with zero full replay and full-state checksum calls.
Inspection identifies payload copies in Arrow builders/render buffers, authenticated
range decoding, local predecessor/replacement materialization, and the independent
transition verifier. The earlier 12P allowance did not cover their cumulative
allocations. The diagnostic net poll watermark is not qualified ownership proof.

For oversized singletons only, freeze a conservative cumulative allocation ceiling
of 16 MiB + 24P, where P is the replaced payload length. This includes request-owned
input and the bounded rewrite/proof and codec buffers; directory work and configured
cache pools are separately reported. The standard fixture ceiling stays 64 MiB.
This amendment does not establish compliance: all three payload sizes in all three
cache modes must be measured against it. Preserve the 12P failure as nonpassing
historical evidence. Every singleton JSONL row records command outcome separately
from ceiling acceptance before assertions.

## Amendment 17: scaling execution profile and evidence transport

The full fixed 180,000-request matrix uses Rust 1.88 test-profile optimization
level 3, incremental compilation disabled, and dev/test debug information disabled.
Earlier unoptimized behavioral and preflight runs retain their original profile
and are not mixed into optimized latency summaries. Work and allocation ceilings,
fixture shapes, repetitions and operation counts do not change. An optimized
preflight must pass before full execution. Compiler profile is recorded with each
measurement command and source identity.

Raw JSONL is streamed through a privately owned FIFO to a standard-library gzip
writer to preserve every operation and phase without requiring the entire
uncompressed evidence on disk. The writer retains EOF through the child process
lifetime, drains before completion, and records compressed and decompressed byte
counts/checksums. Decompression must recover every original JSONL byte. This changes
evidence storage only; serialization remains outside the measured request future.

## Amendment 18: trusted conflict classification boundary

The four publication classifications use two evidence channels. A directly
observed conditional HEAD `PreconditionFailed` returns `CatalogError::CasFailed`
and is the trusted conflict classification authorizing frozen-command reexecution.
Candidate-ID-only restart reconciliation returns committed, superseded or unresolved
from bounded authenticated evidence. It cannot recreate a lost conditional-write
response and therefore does not expose an unreachable Conflict enum variant.

No unauthenticated rejection side-object may establish a retry-safe conflict. A
hash over public prepared bytes cannot authenticate that the provider returned a
precondition failure. Missing evidence, cancellation or transport failure remain
unresolved unless authenticated current state/ancestry proves a terminal outcome.
Tests retain the direct definitive-CAS-failure classification and prove that restart
reconciliation without that response never fabricates it. This API clarification
does not add a publication attempt or change any persisted authority format.


### Amendment 19 — optimized compatibility verification and evidence sealing

The opt-level-0 full package run retained 910 completed passing tests before the
long legacy Gate 7 model was explicitly interrupted after 3,748.98 seconds. A
one-second stack sample showed active replay and software SHA computation. This
interrupted run is incomplete evidence, not a passing full suite. The complete
compatibility suite will rerun with Rust 1.88 test opt-level 3, debug assertions
and overflow checks explicitly enabled, locked dependencies, incremental zero,
and dev/test debug information zero. The previously frozen matrix remains
opt-level 3 with the same assertions/checks enabled; axes, operations and ceilings
are unchanged. Singleton opt-level-0 results remain separately labeled.

Measured publishing samples must bind their recorded operation ID and request
digest to the authenticated published tuple. Ordered-page reads are exempt from
that mutation identity comparison. The earlier preflight's placeholder digests
for stale-conflict and exact trim are retained as nonpassing identity evidence.

Lossless JSONL capture must fail promptly on output failure and reject empty,
malformed or unterminated streams. Matrix acceptance requires exact operation and
phase identities, including construction, reset, bootstrap, warmup, operation
reset, summary and final artifact inventory, with analyzer/transport/gzip digests
bound together. Sealing requires the complete matrix, measured/final code hashes,
profile, and final audit. Reviewed evidence is cloned into a private immutable
snapshot and scanned before archive creation; base and delta are scanned too.
The unchanged pinned API test signing fixture is admitted only by its exact path
and SHA-256. Validation precedes exclusive final archive publication, preserving
failed attempts and every prior archive.


### Amendment 20 — bounded reuse of authenticated directory fences

The all-size optimized preflight failed at request 95: rename-table over 2,419,200
retained rows and 128 active records succeeded but allocated a conservative
69,866,436 bytes, exceeding the unchanged 64 MiB standard ceiling. It read 19,745
external directory fences for only 622 distinct immutable keys. Full-page sibling
validation, floor selection and independent rewrite/proof walks repeatedly fetched
the same authenticated endpoint bytes. This was depth/fanout work, not full replay;
the failure remains nonpassing evidence.

Each directory ReadBudget may retain at most 1,024 authenticated external fence
entries and 128 KiB of copied payload, with entry storage plus payload statically
bounded below 256 KiB. Allocation is lazy, scope/digest/declared length bind reuse,
and admission follows the existing bounded read and raw digest validation. A full
budget cache or oversized fence falls back to authenticated reads. Physical misses
and failures retain existing probe charges; reuse performs no physical read.
Fresh read budgets in the independent verifier remain independent of the updater.
Every sibling boundary still participates in ordering and coverage validation.
This operation-local scratch applies in all cache modes and is charged to request
ownership; configured cache pools and directory-v1 bytes are unchanged. No request
or probe ceiling is raised and no PR #430 runtime cache work is incorporated.

### Amendment 21 — bounded insertion in the in-memory fixture provider

The first full optimized matrix stopped at request 22,576 (4,096 retained rows,
128 active records, pressure cache, register-table, repetition 3, ordinal 176).
The command and proof succeeded, but conservative allocation was 98,513,779 bytes.
Adjacent requests used approximately 13.58 MB with the same selected-block work.
A compiled reduced regression proves that the testing-only MemoryBackend HashMap
reallocates its whole retained object table during one tiny PUT: insertion 7,169
allocated 1,327,243 bytes. The full run crossed a larger table growth boundary,
adding approximately 84.93 MB. The failed full stream and reduced red remain
nonpassing evidence; no allocation is subtracted or relabeled as passing.

Use the standard library BTreeMap for MemoryBackend's object inventory. This
existing backend is explicitly documented as testing-only and unsuitable for
production. Its storage API, conditional version semantics, payload ownership,
locks, and list-page contract remain unchanged; insertion allocates local tree
nodes instead of replacing the full inventory table. The measured request still
includes all backend and protocol allocations. No preallocation, provider-memory
exclusion, candidate deletion, request ceiling change, or dependency is introduced.
Retain a regression requiring each tiny PUT across 4,096 through 9,096 objects to
allocate at most 16 KiB, then rerun core storage and catalog compatibility checks,
all-size preflight, and the complete 180,000-request matrix on the changed source.
Prior HashMap latency observations must remain separately labeled.

### Amendment 22 — lossless evidence compression and disk admission

The second full matrix was deliberately interrupted after 37,319 complete requests
to preserve disk headroom. Its 6,441,076,343 raw bytes occupy 952,138,293 gzip bytes;
growth on the 65,536-row axis projects beyond available disk for the remaining
axes plus the required archive. The owned test process alone received SIGTERM.
The partial stream, command exit, verified gzip roundtrip and intervention receipt
remain incomplete evidence; they do not qualify the full matrix.

Use the installed Python 3.14.5 standard-library compression.zstd with libzstd
1.5.7, compression level 3 and window_log 25 (32 MiB), in the existing lossless
FIFO transport outside measured request futures. The decoder admits at most that
window. This replaces gzip's 32 KiB history window for final JSONL evidence and
introduces no Rust dependency or protocol/source behavior change. Preserve every
raw JSONL byte, operation/phase record, raw and compressed digest, byte/line count,
roundtrip check, exclusive artifact name, failure/cancellation check, and exact
analyzer/transport/Cargo/source binding. Readers retain support for historical gzip
failures; the final qualification gate requires the new codec and frozen settings.

Retain a same-byte gzip/Zstandard compression probe and transport failure tests,
then repeat the all-axis preflight and complete fixed 180,000-request matrix.
Source and compiler profile remain held during each run. No request count, proof,
64 MiB ceiling, fixture, cache mode, or ownership accounting is reduced. Compression
memory/CPU is evidence transport work outside requests and is not a provider claim.
All earlier gzip latency/evidence runs remain separately labeled and nonqualifying
for the final Zstandard matrix. The source archive still contains complete source,
pinned base, reconstructable delta, admitted evidence and checked member modes.
