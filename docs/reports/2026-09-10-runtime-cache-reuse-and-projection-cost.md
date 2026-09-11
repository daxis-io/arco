# Runtime cache reuse and projection catch-up

Status: implementation, corrected local measurement acceptance, independent review and audit follow-up complete; approved for local commit.

The isolated implementation starts at Gate 6 `92fd19f11a547ece5004ac94cad83a3527471812`. The root checkout and the separately owned Gate 7 worktree are preserved. External execution evidence is under `/private/tmp/arco-runtime-reuse-20260910-01`; `ownership.md` identifies the branch, candidate, targets, owned paths and initial state. `qualification-02/source-before.json` and `source-after.json` bind the measurement window to the Rust source. Baseline extraction verifies every original file outside the explicit test overlay.

## Behavior

Registry clones share a slot for each configured root. Control roots divide the existing 32 MiB metadata and 128 MiB decoded budgets equally, leaving integer remainders unused. Legacy roots receive no cache capacity. The first retained handle keeps its exact backend, physical root and scope; an incompatible backend reads directly without replacing it. Unfundable shares read directly after ordinary constructor validation. Locks cover only selection and initialization.

Authority construction continues to run for every request, including its ordinary default-cache validation; that temporary cache is dropped before registry attachment. The measured request costs include this construction. The budget bounds retained caches in one registry, including the existing reservation and live-lease accounting. It excludes transient constructors, request-owned data, backend buffers and other registries; it is not a process RSS bound. Direct state-store construction is unchanged, as are Gate 6 authentication, freshness and ownership checks.

The interruption regression exposed an existing projection retry defect. Publishing a manifest and then failing the first status write caused the next attempt to quarantine the intent because `published_at` changed. Retry now preserves the first publication timestamp while comparing every other manifest field and all immutable file bytes. Conflicting contents still quarantine; the original manifest is never rewritten. Per-intent reconstruction, publication, acknowledgement and historical watermarks remain intact.

## Reproduce

Use a dedicated target and a new evidence directory. The runner prebuilds both executables and alternates candidate/base order across five pairs. It is sequential and never runs provider qualification:

```sh
python3 scripts/runtime_reuse_qualification.py run \
  --evidence /private/tmp/arco-runtime-reuse-new-run \
  --target /private/tmp/arco-runtime-reuse-target
python3 scripts/runtime_reuse_qualification.py compare \
  /private/tmp/arco-runtime-reuse-new-run
python3 -m unittest discover -s scripts/tests -p test_runtime_reuse_qualification.py
```

Runtime measurements use five repetitions, 200 independently cold point and scan requests per repetition, and 40 warm requests per operation, for both 8 and 64 tables with four columns. Server registry configuration is outside the interval; request authority construction, fresh authority resolution, reader opening and result copies are inside it. Baseline and candidate load byte-identical saved storage images and compare full table records against fixture expectations. Cold p50 must be at most 1.25 times Gate 6, p99 at most 1.5, and allocated bytes at most twice Gate 6. Fully admitted warm requests require zero eligible payload reads and decodes, fresh HEAD work and zero reservation underestimates.

Projection fixtures have 8 or 64 tables, four columns each and 1, 8 or 16 pending metadata updates. Setup projection work and maintenance complete before measurement. Each of five repetitions measures an ordinary drain and a drain interrupted after immutable publication, followed by recovery and three fresh drain invocations modelling repeated notifications. If acknowledgement writes hit existing backpressure, the driver runs bounded durable maintenance and retries, counting that work in the measured total and a separate maintenance stage. The driver independently decodes all five Parquet files at every pending source watermark, checks full records and manifest checksums/counts, and checks acknowledgement completion and status. Saved images freeze inputs across repetitions and schedules.

Opt-in test instrumentation partitions discovery/ack lookup, source reconstruction, encoding/immutable publication, and status/ack writes. Storage counters record attempts and bytes; allocations count future polls, and stage elapsed time includes suspension. The total additionally includes construction, intent parsing, loop overhead and measurement bookkeeping. HEAD bytes count native metadata storage, not provider wire bytes. No production measurement is enabled by default.

## Results

The accepted evidence is `qualification-02/acceptance.json`. All 4,000 cold and 800 warm observations **per source** pass logical parity and the applicable work checks. Maximum cold p50 is **1.014x**, p99 **1.037x**, and allocation bytes **1.000850x** Gate 6. Warm eligible payload fetches and metadata/block decodes are zero. Retained ledgers show zero reservation underestimates, active loads and participants at completion; an underestimate also propagates an error, so successful requests cover earlier cold handles. The largest recorded representative metadata/decoded ownership peaks are 203,781 / 351,100 bytes, well within their fixed shares.

Values below are medians of five per-repetition percentiles, in milliseconds. HEAD counts are attempts per request.

| Tables | Request | Gate 6 p50 / p99 | Candidate p50 / p99 | Candidate HEADs |
| ---: | --- | ---: | ---: | ---: |
| 8 | cold point | 12.367 / 13.662 | 12.539 / 13.805 | 155 |
| 8 | cold scan | 13.451 / 14.433 | 13.425 / 14.963 | 149 |
| 8 | warm point | 12.398 / 13.286 | 8.783 / 9.460 | 132 |
| 8 | warm scan | 13.403 / 14.032 | 7.643 / 8.182 | 119 |
| 64 | cold point | 16.217 / 17.190 | 16.110 / 17.191 | 21 |
| 64 | cold scan | 14.832 / 15.644 | 14.599 / 15.644 | 18 |
| 64 | warm point | 16.225 / 16.734 | 10.250 / 11.444 | 18 |
| 64 | warm scan | 14.768 / 15.448 | 8.725 / 9.417 | 15 |

Warm p50 falls 29.2–43.0%; allocated bytes fall 22.7–36.7%. The 8-table fixture retains a longer transaction suffix than the maintained 64-table fixture, explaining its higher metadata/HEAD request count. Both sources use identical images; the table count alone does not describe their physical layout.

All **60 projection schedules** pass, independently verifying **500 source watermarks** and their five Parquet files. The following normal-drain costs are five-repetition medians in milliseconds. Column medians need not sum exactly to the total median.

| Tables | Backlog | Total | Discovery | Source | Parquet/publication | Status/ack | Maintenance |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 1 | 61.8 | 30.1 | 12.2 | 1.9 | 17.4 | 0 |
| 8 | 8 | 346.9 | 30.3 | 133.4 | 15.4 | 166.8 | 0 |
| 8 | 16 | 1211.5 | 79.8 | 500.7 | 33.8 | 479.4 | 117.0 |
| 64 | 1 | 127.7 | 75.0 | 10.0 | 3.1 | 39.0 | 0 |
| 64 | 8 | 438.5 | 89.5 | 74.9 | 24.2 | 249.2 | 0 |
| 64 | 16 | 1458.1 | 250.7 | 350.4 | 52.4 | 662.9 | 138.6 |

The 16-intent cases each need one maintenance retry. They attempt 290 PUTs, allocate cumulatively 132.7 / 163.3 MiB (8 / 64 tables), and perform 8,175 / 6,521 HEADs. Normal total drain time is 1.212 / 1.458 seconds; interruption plus recovery is 1.313 / 1.626 seconds. All 180 repeated notification drains attempt zero PUTs. Raw reports retain GET/range/HEAD/PUT attempts and bytes, phase allocations and elapsed times, source identities, retry counts, and per-watermark parity. Backend phase totals are independently checked against whole-operation counters.

For 16 intents, source reconstruction plus status/ack work accounts for roughly 81% of the 8-table drain and 69% of the 64-table drain. Parquet/publication is only 2.8–3.6%. The next investigation should target repeated reconstruction/discovery and acknowledgement commit work before changing the projection format. The observed backpressure also needs to remain part of any admission proposal; it cannot be erased from the cost comparison.

## Preserved failures and verification

`qualification-01` remains rejected: 64-table cold point p99 was 2.036x Gate 6, and scan p99 also exceeded its ceiling. The candidate had clustered tail spikes before the later baseline run; their cause was not established. The correction predeclared alternating pairs, prebuilt both executables, retained all samples and thresholds, and copied the original input images. The cache implementation was unchanged. This is local empirical timing evidence, not an explanation or exemption for the first failure.

The first projection matrix stopped at the existing 32-L0 acknowledgement backpressure boundary. Its replacement includes demanded maintenance, failed work and replay in total cost. The initial post-publication retry regression failed with zero acknowledged intents; its corrected run preserves the original manifest bytes and acknowledges the intent. Conflicting manifests and malformed intents retain quarantine behavior. Compiler/lint failures and the test's incorrect assumption about an empty backend's state token are preserved separately from behavioral failures.

Pre-audit source validation passes: 1,499 catalog/UC/Iceberg test executions across 55 suites (16 explicitly ignored), five API cross-protocol tests, 11 doctests (nine ignored), and four Python comparator regressions. These are execution counts, not deduplicated test identities. The two new ignored measurement drivers were run explicitly. All affected packages pass all-target/all-feature Clippy with denied warnings, formatting, rustdoc with denied warnings, and mdBook. The existing Gate 6 cache tests and maintenance/reclamation regressions ran as part of the catalog suite. Final command records preserve exact arguments, exit codes and log hashes; the audited Rust source matches both accepted measurement manifests.

`coverage.md` in the external evidence directory maps acceptance requirements to tests. The preserved pre-audit `review-bundle.tar.gz` contains the reviewed source files, source manifest, report and compact evidence; full raw inputs, baseline snapshots and logs remain in the evidence directory. A fresh read-only subagent audit approved this local slice with one nonblocking API visibility finding; its report is `/private/tmp/arco-runtime-reuse-audit-20260911-01/audit.md`.

The audit follow-up makes `projection_measurement` crate-private without `test-utils` and retains public access for the measurement driver with that feature. Only the module declaration changes; measurement logic and cache behavior are unchanged. Default and test-utils library checks pass, a downstream compile probe verifies rejection in default builds and access with test-utils, all nine runtime/projection regressions pass, and both feature configurations pass library Clippy with denied warnings. Formatting and diff checks pass. `audit-followup-01` preserves the commands, logs, current source manifest and follow-up bundle. The original timing evidence remains bound to the pre-cleanup source; this visibility-only follow-up does not claim a new timing or provider qualification run.

## Integration boundary

This is local MemoryBackend evidence. It does not qualify S3, capacity under provider latency, production concurrency, or the seven-day pilot. Gate 7 continues against its frozen source. Integration requires rerunning affected catalog/protocol, authenticated-cache isolation/cancellation, projection publication/recovery, and relevant Gate 7 fault/provider lanes against the integrated source. Existing Gate 7 evidence cannot qualify this changed runtime automatically.

Further catch-up changes must be justified by the measured stage breakdown. This slice adds no intent coalescing, changelog, partitioned projection, copy-on-write storage or workload admission changes.
