# Gate 7 state-store qualification — 2026-09-10

Status: **Gate 7 is incomplete and nonpassing.** This is a partial local qualification
candidate. The selected pilot is blocked by retained-row capacity. The bounded provider
runner and Linux supervisor are implemented; provider execution still needs the
provisioned environment and its approved execution manifest. The full pilot workload
engine remains unimplemented. No remote CI, real S3, cohort preparation,
pilot, deployment or cutover has run. The candidate remains uncommitted and unstaged.

## Pinned source and execution

The fresh worktree starts at Gate 6 commit
`92fd19f11a547ece5004ac94cad83a3527471812`, tree
`9a1a5abd6f22a89077d6e44c0d8490d20ad34131`. Its 1,013 files matched the supplied
manifest in paths, bytes, sizes and modes; both manifest and prior recovery archive
SHA-256 matched the frozen contract. The actual `git fetch --no-prune origin`
succeeded without remote-ref movement. The initial sandbox denial is retained.
The root's two untracked paths, all previous worktrees, targets and archives remain.
One source writer owns this candidate and one sequential Cargo build queue uses new targets.
No production state-store behavior or format has changed.

## Confirmed pilot capacity blocker

The pilot requires `7 × (3,600 × 25 + 23 × 3,600) = 1,209,600` semantic mutations.
`stage_commit_records` retains one receipt and one audit row for each new successful
catalog mutation. Thus receipt/audit retention alone requires 2,419,200 visible rows.
The production `restore_source_values` scanner has unchanged aggregate limits of
1,000,000 rows and 67,108,864 decoded key/value bytes. The row prerequisite fails
regardless of compression, cache behavior or projection throughput.

The runnable `gate7_capacity` test executes actual catalog create/update operations,
reads their four serialized receipt/audit records, and passes a generated inventory
to the same production bounded scanner used by restore. Both the byte-bound probe
using actual record values and the row-bound probe using empty values return
`MaintenanceBackpressure`. The sample's smallest record value is 434 bytes; repeating
that sample with shorter eight-byte keys produces 1,069,286,400 decoded bytes. This
is a sample-based growth projection, not a universal lower bound for every mutation
mix, physical stored-byte forecast, or a measured full pilot root.

This probe proves the selected pilot's restore-capacity conflict. It does not prove
full-scale replay/maintenance viability. No limits are raised, records discarded,
roots rotated or retention redesigned. Architectural follow-up must reconcile the
required receipt/audit retention and bounded restore/replay/maintenance contract,
then receive a new qualification contract before the pilot can proceed.

## Verified local inventory

The workspace test lane passed 2,113 test executions across 135 suites, with 42
explicitly ignored tests. Counts overlap with focused lanes and are not a deduplicated
inventory. The full catalog-only lane earlier passed 943 executions. The final model
and shared fixture sources remained stable during the workspace run; changed S3 CLI
and reader-cohort schedule inputs received their own affected reruns. Python and
documentation changes during that run are recorded as source differences.

- Original 32×128 maintenance and 32×64 reclamation models, 85 remote-write schedules,
  and logical-clock recovery regression passed.
- All 192 new cases passed: seeds 33–64 × three cache configurations × two physical
  layouts, each with 128 generated operations and all ten actual operation families.
  Deterministic setup adds 64 encoded rows and materializes a current-manifest-owned
  layout before the generated sequence. The independent oracle authenticates that
  layout; observed indexes have five blocks at 32 KiB and one at 256 KiB.
- All 96 controlled schedules passed on the clarified contract. The mode axis is the
  retained catalog reader, explicitly warmed before each boundary with exact cache
  capacities checked and pre/post ledgers retained. Maintenance, acknowledgement and
  GC capabilities preserve their existing ownership; GC stays uncached. The pressure
  configuration is not a claim that this small fixture induced eviction.
- All 86 explicit maintenance scaling cases, 15 resume schedules and three lifecycle
  schedules passed. Raw measurements retain operation costs and phase accounting.
- The subprocess interruption regression kills a worker at an armed pause-before
  boundary and verifies durable seed/mode/block/step/family/fault/request evidence.
- Python admission regressions pass for elapsed-time lower bounds, clock discontinuity,
  checkpoint corruption/loss/drift, resource and cache ownership ceilings, externally
  bound packet digests, phase ceilings, stop-request acknowledgement, and cold sample
  cardinality/ledgers. These helper tests do not establish a full pilot supervisor.

The API/Flow tests and parity/legacy matrices, API feature checks, deterministic UAT,
Python SDK, protocol lint/breaking comparison, mdBook and warning-free workspace docs,
Clippy, formatting, dependency policy, ADR/engine/parity/integrity checks and repository
hygiene passed locally. Dependency policy uses cargo-deny 0.18.9 and the preserved
pinned advisory snapshot; it is not a fresh online advisory qualification. External
workflow records retain commands, exit codes, source-before/after differences and raw
log hashes. The API run overlapped documentation edits only; affected source inputs
were unchanged. No remote CI status is implied.

The Python support now includes a pure durable-window transition and read-only Linux
`recover` decision. Heartbeats bind run/attempt/source/configuration/boot identities,
exact sequence, clocks, immutable attempt start, prior-attempt digest, raw chunk
headers and chain heads, outstanding publication state and an explicit workload-verifier
verdict. Recovery checks the last raw header against the checkpoint and recomputes its
chain head; an envelope checksum alone is insufficient. An unplanned gap above 60 seconds,
clock discontinuity, drift, reboot, lost sequence/chain, or unproven workload interval
rejects that heartbeat and resets the window while retaining prior-attempt metadata.
The next baseline must explicitly declare the next attempt. Terminal stop preserves
elapsed accounting, requires zero ambiguity, and prevents that run from resuming. Planned individual-worker
restart counts only with affirmative continued-workload evidence. A 604,800-second
window is labelled `window_complete`; `pilot_qualified` remains false. Recovery
credits no downtime and cannot launch workers. Deterministic tests cover calendar
false positives, exact elapsed boundaries, failures, and durable checkpoint round
trips. This is independently useful supervision logic, not the missing workload
engine, systemd lifecycle integration or end-to-end pilot qualification.

Each model operation has two identical diagnostic annotations: the oracle stream and
its live backend stream. `scripts/gate7_qualification.py models <evidence-directory>`
requires exactly two matching annotations for each unique step 0–127, checks declared
family counts against those unique steps, and retains both line locations and the raw
trace hash. Its regression rejects missing, third or conflicting annotations. The
index counts **24,576 distinct generated operations**, not 49,152 annotation lines.
Raw files are never rewritten. Live and final traces intentionally coexist to preserve
interruption provenance and oracle annotations. Earlier interrupted/failed attempts
remain nonqualifying and retained.

## S3 adapter evidence

The qualification executable provides inventory, MemoryBackend scenarios and contained
loopback scenarios through the actual `S3StorageBackend`. The shared conformance helper
runs beneath `ScopedStorage`. The final loopback passed with 295,027 transport requests,
one synthetic tenant/workspace namespace, zero rejected paths, a maximum 1,000 objects
per listing response, and 6,000 independent cold point/scan observations. Those samples
cover five repetitions × three cache modes × two operations × 200 observations, with
unique handles, initial and terminal ledgers, and nearest-rank p50/p99 validation.

Coverage includes conditional storage conformance, 1,001-object bounded listing,
actual catalog/projection acknowledgement and materialized equality at a watermark,
catalog metadata restoration from an older cut, retained reads, changed/deleted warm
objects in three modes, maintenance/reclamation/restart, and injected HTTP outcomes.
Each 403/404/408/409/412/503 conditional PUT was observed exactly once; the read-retry
probe issued two GETs. Lost response after application and delayed application before
reconciliation both retained uncertainty until exact readback, then completed cleanup.
Injected failures are labelled as local client/HTTP fixture evidence. The 408 case is
an HTTP timeout response, not proof of a socket-timeout deadline. Simulated nine-day
retention is not provider elapsed-time evidence.

`s3-loopback-final-01` failed because its test incorrectly expected an explicit 409
conditional conflict to remain an uncertain error; its 404 token also failed locally
before reaching HTTP. The corrected probe uses an actual adapter-issued encoded token
and checks the appropriate conflict outcome. `s3-loopback-final-02` passed and retains
all raw requests/outcomes. No production adapter behavior changed. See
[AWS conditional-write behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
and [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html).

The local durations do not establish provider SLO acceptance or replace the complete
Gate 6 HEAD/decode/hash/validation/allocation comparison. The Gate 6 archive's 9,886
payload members were independently checked in compressed form. Source-applicability
checks verify 88 unchanged cache/measurement inputs, at the earlier local seal. The provider continuation adds a default-preserving
S3 constructor refactor and an opt-in patch to the same dependency version; affected
adapter, dependency and qualification checks are rerun and bound separately. Its
original five-run/200-cold-observation raw evidence and prior qualification limits
remain preserved. Discarded handles' cache ledgers are never summed into RSS.

## External execution packets

Both execution phases remain blocked packets. Approved read-only AWS diagnostics on
September 11 verified account `012832591253`, the Ohio region, the existing bucket's
ownership/encryption/versioning/lifecycle, and an existing VPC/subnet/AMI. No dedicated
EC2 host or runtime role exists. The existing bucket's seven-day lifecycle is unsuitable
for the pilot's protected cohorts; its configuration remains unchanged. The selected
new bucket, runtime role/profile and host require provisioning. User authorization
covers creating prerequisites; a new SCP exception and either execution phase each
require their own concrete approval. The diagnostic-only SCP exception was restored
to its exact original digest after inspection. No resources or provider traffic were
created by those diagnostics.

The bounded runner binds the complete exported source inventory, binary patch, contract,
build inputs, executable, supervisor, AWS CLI and scenario inventory. It validates the
actual instance-role identity, owns an atomic operation journal and conservatively
charges retries before dispatch. A dedicated listing client uses HTTP/1, connection
close and no idle pool through a TLS-transparent admission proxy. Its 100,000 prepaid
connection allowance bounds hidden pagination and retries; it is not an observed HTTP
request count. The exact pinned `object_store` 0.11.2 source has one opt-in no-redirect
patch; ordinary constructors retain their defaults. Credential-free TLS tests prove
that 307/308 conditional writes issue exactly one physical PUT.

`scripts/gate7_provider.py` owns that proxy and the worker in a verified systemd unit.
It bounds operation, transport and supervision evidence, persists heartbeat and stop
state outside the checkout, rejects source/checkpoint drift, and distinguishes process
termination from reconciled publication. Linux lifecycle tests cover unsafe pre-start hooks, graceful stop, post-start unit drift, source drift,
deadline expiry, ordinary and detached surviving children, supervisor crash, evidence
overflow and checkpoint loss. These use a fake worker with no AWS access; they prove
supervision behavior, not integrated provider execution or performance acceptance.

Current AWS price observations and conservative phase calculations are retained with
the external packets. Each phase still requires a concrete host/executable/credential
and permission binding before traffic. No image publication or workflow dispatch is
included.

## Remaining mandatory work and recovery

The full-scale production restore/replay/maintenance growth fixture and durable pilot
workload/supervision acceptance remain missing. The pilot support provides admission,
durable-window transitions and recovery/status/stop checks; it has no 168-hour workload
engine, complete workload verifier, cohort preparation or aggregate SLO verdict. Rust
advertises five bounded provider repetitions and refuses pilot execution. Passing
provider helper and lifecycle tests does not close the pilot omissions.

A confirmed capacity conflict requires architectural follow-up under a new contract;
this slice does not raise limits, discard receipts/audits, rotate roots or redesign
retention. Provider execution additionally needs the verified existing environment,
Linux-target executable/build identity, complete conservative request/cost admission,
and its own approved packet. No approval is requested for a blocked packet.

The dated contract, report and soak runbook travel with the candidate. External
`CLOSEOUT-01.json` and `RECOVERY-PROOF-01.json` record final check/audit bindings and the
actual compressed-member plus two-base reconstruction proof after source freeze.
The recovery archive preserves the complete candidate, exact Gate 6 base, binary
patch, tested local executable, raw evidence/failures and prior Gate 6 archive;
credentials, Git metadata, build trees and reconstruction scratch are excluded.
Recovery integrity is distinct from Gate 7 acceptance.


## September 11 provider continuation closure

The immutable TLS rehearsal completed five repetitions and 6,000 independent cold
observations. Seven separate TLS fault probes passed, including listing retry,
redirect, truncated body and connection-cap cases plus conditional PUT 307/308.
The preceding TLS attempt retained all scenario results but failed terminal source
identity after an executable rebuild; it remains failed. Rehearsals now copy and
bind an immutable executable before starting. These are loopback results, not S3
performance qualification.

Python 3.11 passed 26 control tests. Ten Linux systemd lifecycle cases passed with
fake workers and zero AWS traffic, including rejection of a changed stop hook before
issuing systemctl stop. The Linux executable's real supervisor/parent validation
reached a deliberately substituted credential CLI and rejected its empty response
before constructing an S3 backend. No credential acquisition was exercised.

The corrected setup packet fixes one instance, 128 GiB gp3, 3,000 IOPS and 125 MiB/s.
Closed request admission rejects count, device, performance and other unexpected
launch fields; SCP conditions also constrain the supported volume parameters. The
reviewed provider accounting requires a $19 fixed reserve, six microdollars per
admitted request and 44 picodollars per submitted byte, bound to the price forecast.
The generic runner's lower validation floors alone do not establish priced admission.
The forecast is $23.42 for the measured request upper bound, below the $25 planning
ceiling. It assumes manual host stop; the supervisor stops its workload, not EC2.
No independent host shutdown mechanism has been installed or tested. A failed stop
can exceed the forecast, so hard total-cost admission remains unproved. Provider
execution is blocked until independent host shutdown and stopped-state proof, actual
host, billing and permission prerequisites are verified.

Automatic approval review rejected sending the proposed IAM/SCP documents to AWS
Access Analyzer. No call executed. Policy validation and the new temporary setup
SCPs therefore remain separate approval steps. Earlier packets are retained and
explicitly superseded; the latest source-bound checkpoint is the review authority.
