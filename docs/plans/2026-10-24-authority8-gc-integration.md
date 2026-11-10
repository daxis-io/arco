# Authority-8 GC integration and transport qualification

Scope approved on 2026-10-24: integrate source manifest
`c5ee4a47ec0de4348a4b810b26134946291553191b29d71a5ecd1160376144f7`
onto main `7eb60dcf1e8a3f3906b238d45ea66cf436ffd93b`, close local CI,
qualify process restart and local transport accounting, then reduce remaining
requests and serial latency. Preserve the frozen candidate and all receipts.
No publication, credentialed provider run or active deletion. Cleanup is limited
to the separately authorized inactive shared incremental cache with a fresh
recursive writer check; other artifacts remain preserved.

## Integration

The complete 36-file patch applies without conflict and matches every retained
file hash. Main already updates rustls-webpki, Tokio and webpki-roots; its current
advisory check passes. Its license check rejects tiny-keccak 2.0.2, reached through
Iceberg's credential dependency. A license allowance is restricted to that exact
existing version, using the existing cargo-deny exception mechanism; no global
CC0 allowance, dependency or lockfile update is needed.

## Local qualification design

1. Preserve the existing graph verifier, root schedules, fixed filter, worker,
   freshness fences and all 4,096-operation/64-MiB invoices. Reuse the existing
   qualification binary, bounded journal and numeric-loopback endpoint checks.
   A new GC rehearsal must not silently expand the historical provider packet.
2. A retained, test-only storage image includes object bytes and exact opaque
   versions. Separate OS processes interrupt, reopen and rebuild the verifier;
   neither a stored completion receipt nor serialized filter restores trust.
   The independent fixture oracle compares exact candidate membership. Corrupt
   images/proofs and changed roots must fail closed. This is test storage, not a
   production membership index or new authority protocol.
3. Count actual local HTTP methods/paths/statuses/bytes/attempts at the server,
   below logical adapters and retries. Cover bounded LIST, range GET, conditional
   writes, failures, cancellation and replay. Only dummy credentials and numeric
   loopback addresses are admitted; no real provider identity is inferred.

## Performance alternatives and selection criteria

The previous code does HEAD then range GET even for authenticated bounded probes.
The installed object_store 0.11.2 GetOptions/GetRange contract already clamps a
nonempty range to EOF. First test replacing that preliminary HEAD with the native
bounded GET. Preserve Arco's empty/EOF/inverted/missing-object behavior through
bounded metadata fallback where necessary, and bound response collection before
copying untrusted bodies. Authentication and final physical-version checks stay.

If that does not meet the serial latency gate, compare bounded prefetch of
independent descriptor probes and concurrent endpoint HEADs against a new persisted
index. Prefer the existing authenticated graph with fixed small concurrency and
prepaid operations/ownership. Do not add a service, unbounded cache or persisted
membership index. Root-churn invalidation and complete restart remain unchanged.

## Acceptance thresholds frozen before new implementation

- Keep all prior frozen workload axes and limits: one page <=99,365 logical calls,
  full sweep <=250,000 calls, <=150/200 MiB GET bodies respectively, <=256 KiB
  attempted proof and <=128 KiB persisted proof. Every invocation stays within
  4,096 charged operations and 64 MiB admitted ownership.
- Subsequent pages do zero protected-graph GETs and <=512 logical calls on the
  frozen scope. Restart is one cold verification per process, independent of page
  count; exact oracle and negative evidence checks remain mandatory.
- Successful nonempty native range read: one object-store/HTTP GET and zero
  preliminary HEADs, bounded body collection, unchanged byte-range semantics.
  Failures and fallback requests must be visible in transport accounting.
- Investigate >=10x provider-request reduction against the retained 1,443,385
  successful no-retry request model, rather than relabeling logical calls.
- Deterministic 25-ms request critical-path model for cold page and full sweep
  must fit the unchanged 1,800-second seed admission to pass the latency gate.
  Any concurrency savings must come from actual bounded execution groups; count
  all attempts and replay. Retain failures; do not extend the deadline.
- Run the unchanged large cold and complete sweep fixtures on final integrated
  source, relevant regressions, required local CI, format, strict workspace
  Clippy and an independent effective-diff audit including untracked files.

The original 100x target remains infeasible for the current independently stored
descriptor representation; this work does not qualify active maintenance,
retained-root release, the whole Step-4 collection oracle, active deletion or
production. An opt-in provider packet must still name the exact qualified
source/executable, namespace and request/byte/time/abort/cleanup limits.


## Implemented local continuation

The measured warm path has an atomic `next_marked_gc_plan_page` entry point: one
before/after root fence plus exact final candidate HEAD checks, without the extra
pair needed between two separately callable invocations. The original two-step
API remains and shares admission/final-version validation. The exact small-state
oracle first failed its total-page call ceiling on the old path, then passed the
atomic path; no fence is reused across separate invocations.

The cold walk prepays at most eight descriptor probes through the same invocation
invoice, then authenticates each original descriptor/index and joins its two
physical endpoint HEADs. A safe processed prefix can yield at existing soft
limits; at most eight authenticated leaves stay in the opaque frontier. Pending
leaves prevent root completion and are included in resident/staged ownership.
Large keys reduce lookahead. No serialized frontier/filter restores trust.
Named bounded-work counters record actual probe/head requests and execution groups;
the round-trip model subtracts only the overlap those fixed groups permit.

The shared adapter uses native bounded GetOptions (vendored object_store 0.11.2,
`src/util.rs:193-253`) rather than a preliminary HEAD. Bounded stream collection
checks range and exact body length, grows fallibly only with observed chunks, and
never allocates from an advertised size alone. Adversarial huge-meta/short-body,
oversized-body, EOF and missing/inverted range tests preserve the shared contract.

The GC-only rehearsal extends the existing manifest/journal while preserving the
historical real-provider admission. It binds current source HEAD, full source
inventory and executable, admits only numeric loopback and dummy credentials,
and denies DELETE, mutable authority writes and payload GET in measured stages.
Real-provider validation rejects GC fields before traffic. The local fixture
retains exact bytes and opaque ETag versions across killed/restarted processes;
its image and all HTTP attempts/status/header/body sizes are retained. It is not
qualification of versioned S3, S3 throughput or a production provider command.
CI runs the deterministic rehearsal and retains its evidence.

The scope still excludes live provider execution and active authority-8 deletion.
A future large provider packet must reconcile conservative journal reservations
(including prepaid LIST connections and worst-case retries/fallback HEADs) with
its request cap before admission; actual loopback counts are not a waiver of those
reservations. The historical provider executable is not silently promoted to an
active GC command.
