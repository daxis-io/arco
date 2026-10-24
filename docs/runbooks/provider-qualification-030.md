# Arco 0.3.0 provider qualification

Arco qualifies each storage provider with Arco's adapter, source revision, lockfile, and
conformance scenarios. Another project's cloud CI is useful as an organization example,
but its result cannot qualify Arco. The deterministic lane proves adapter behavior without
cloud credentials; only the opt-in live lane can produce R2 provider evidence.

## Current status

| Profile | Status | Evidence |
|---|---|---|
| Deterministic S3-compatible adapter | Runnable in ordinary CI | Local HTTP fault probe plus object-store CAS, range, and paging tests |
| Cloudflare R2 | Passed on `e9181eb1`; requalification required after dependency changes | [Owner-reviewed campaign](https://github.com/daxis-io/arco/actions/runs/39173326683), including storage, credential scope, owner recovery and all 13 native API/Flow cases |
| Amazon S3 | Separate existing gate | `.github/workflows/s3-conformance.yml`; its result does not qualify R2 |
| GCS and Azure | Separate existing gates | Provider-owned workflows and adapters; their results do not qualify R2 |

Cloudflare documents R2's S3 endpoint as
`https://<ACCOUNT_ID>.r2.cloudflarestorage.com`, its S3 region as `auto`, and support for
`HeadObject`, ranged `GetObject`, conditional `PutObject`, and `ListObjectsV2`. Arco probes
those operations through its existing `S3StorageBackend`; it does not assume AWS STS, IAM,
bucket-owner headers, object versioning, or any other AWS-only behavior. See the current
[R2 S3 API compatibility matrix](https://developers.cloudflare.com/r2/api/s3/api/).

The [first executed live attempt](https://github.com/daxis-io/arco/actions/runs/38072060598/attempts/2)
on `e29d0ed4` failed duplicate-create version consistency. The
[diagnostic campaign](https://github.com/daxis-io/arco/actions/runs/38336557074) confirmed that
R2 rejected the duplicate, returning the same ETag from HEAD but omitting PUT's internal
version ID. The R2 endpoint profile now consistently encodes ETag-only version tokens for
PUT, HEAD and listing, retaining the existing JSON shape and legacy CAS token decoding.
AWS, GCS and Azure retain their existing version identities. These are current-object ETag
preconditions; identical-content replacement does not establish a new generation identity.
Failed receipts and their objects remain retained. The corrected limited storage conformance
passed at `869eadc8`, but that receipt is not the native integration campaign described below.

The [native campaign](https://github.com/daxis-io/arco/actions/runs/39173326683) on
`e9181eb16a3d725de457ce3b03f199c891c22da5` passed and was owner-reviewed on
2026-10-18. The independent denial-control object was unchanged before and after
the run, and the scoped S3 credential received typed `PermissionDenied` before
native writes. Owner recovery used 212 logical calls and 23,627 submitted bytes
within its 512-call/32 MiB limits. All 13 native process cases passed. Both attempts
of the [earlier scope-failing campaign](https://github.com/daxis-io/arco/actions/runs/38500363914)
remain retained; updated bucket-scoped credentials resolved that failure without
changing the guard. The certificate dependency changes in the security follow-up
require a new campaign; this passing receipt does not qualify new binaries.

## CI profiles

`.github/workflows/r2-provider-qualification.yml` has three jobs with dependency ordering:

1. `deterministic` runs for relevant pull requests and `main` changes. It validates R2
   configuration admission and R2 PUT/HEAD identity, proves safe-read retry and single-attempt ambiguous conditional
   writes against a local HTTP fixture, runs the shared adapter contract, and verifies that
   the ignored live probe is discoverable. Its receipt always says
   `live_provider_executed=false` and `provider_gate=unqualified`.
2. `live-r2` runs only from `workflow_dispatch` with `run_live=true` in the protected
   `r2-qualification` environment. Missing configuration fails. The workflow retains logs
   on both success and failure; a green execution remains `evidence-pending-review` until
   the receipt and leftovers are reconciled.
3. `live-integration` runs after `live-r2` in the same protected environment. Before its
   owner/process writes to the write bucket it verifies the credential's typed 403 (`PermissionDenied`) response for
   a private marker in the denied-control bucket, then exercises accepted-CAS response loss
   at the client boundary, fresh caches and a retained reader. It builds the native API/Flow
   profile and runs catalog, task-authentication, frozen-intent, publication and recovery
   cases against R2. Its owner-reviewed campaign is recorded above; each later candidate
   still needs its own receipt. No provider claim follows from the deterministic harness.

Each artifact name includes the GitHub run ID and attempt. Its identity receipt records the
source SHA/ref, Rust and Cargo versions, `Cargo.lock` SHA-256, package/features, provider
identity, elapsed ceiling, job status, and gate classification. Deterministic receipts are
retained for 30 days; credentialed receipts, including failures, are retained for 90 days.
`live-r2` has a 1,800-second timeout and declares estimates of 512 physical requests
(including configured SDK retries), 1,024 submitted bytes, and 1,000 micro-USD. The native
`live-integration` job has a 45-minute timeout and declares 20,000 physical requests,
268,435,456 submitted bytes, and 100,000 micro-USD. Physical request, byte and cost values
are recorded estimates, not probe-enforced limits. The owner-recovery scenario independently
enforces 512 logical storage calls and 32 MiB submitted bytes before I/O. Stop either job if
account-side controls cannot bound the campaign independently.

## Dedicated live configuration

Create a disposable bucket and bucket-scoped token before enabling the protected environment.
Do not reuse application or production credentials. Configure names only in GitHub:

| Kind | Name | Requirement |
|---|---|---|
| Environment variable | `ARCO_TEST_R2_ACCOUNT_ID` | 32 lowercase hexadecimal characters |
| Environment variable | `ARCO_TEST_R2_BUCKET` | Dedicated lowercase DNS-style bucket name |
| Environment variable | `ARCO_TEST_R2_DENIED_BUCKET` | Distinct private bucket containing the owner-controlled `scope-probe.txt` marker |
| Environment variable | `ARCO_TEST_R2_SCOPE_ATTESTATION` | Fresh owner existence receipt for the denied marker; never an S3 credential |
| Environment secret | `R2_ACCESS_KEY_ID` | Token access key scoped to the test bucket |
| Environment secret | `R2_SECRET_ACCESS_KEY` | Matching token secret |

The scope attestation is a JSON receipt from an independent Wrangler OAuth remote GET of the
private denied-bucket marker. It binds the account, bucket, `scope-probe.txt`, its fixed
44-byte SHA-256, a private-object statement and an observation timestamp no more than 24 hours
old. The native job validates that receipt, then requires the S3 credential's HEAD request to
receive typed 403 (`PermissionDenied`) for the same marker before its owner/process writes to
the write bucket. A generic 403 without the owner-side existence attestation does not qualify
scope.

The workflow derives the exact endpoint, sets `AWS_REGION=auto`, and disables EC2 metadata.
The probe rejects inherited AWS profiles, session tokens, generic endpoint overrides, and
default-region overrides before traffic. The token needs object read, write, list, and delete
access only within the disposable bucket. Account-wide administration and bucket creation are
outside the campaign.

## Campaign procedure

1. Record the approved account ID, bucket, owner, expiry, spend ceiling, and rollback owner in
   the change ticket. Confirm the bucket contains no retained or customer data.
2. With independent Wrangler OAuth credentials, perform a remote GET of the private
   `scope-probe.txt` in the distinct denied-control bucket. Store the resulting fresh,
   sanitized JSON receipt as `ARCO_TEST_R2_SCOPE_ATTESTATION`; do not create it from the S3
   token or after the native job begins.
3. Configure the protected `r2-qualification` environment and dispatch
   `R2 Provider Qualification` with `run_live=true` against the final candidate SHA.
4. Review the retained limited-storage receipt and complete log. Confirm the exact candidate, lockfile, toolchain,
   endpoint/account/bucket identity, and that the following scenarios executed:
   - create-if-absent, matching compare-and-swap, stale-token rejection, recreation, and exactly
     one winner from concurrent compare-and-swap attempts;
   - HEAD-confirmed current-version identity followed by full content and range reads. The
     `StorageBackend` trait does not expose immutable historical version-addressed reads, so
     this is current-version consistency evidence only;
   - bounded ordered pagination with an exclusive cursor;
   - deterministic lost-response behavior showing that a conditional write is attempted once
     and remains uncertain until the caller reconciles with HEAD/read.
5. Review the retained native-integration receipt. Confirm the pre-write typed scope denial and
   owner existence attestation, accepted-CAS client-boundary recovery (`wire_loss=false`),
   empty disabled/default/pressure cache starts, retained-reader recovery, and the native API/Flow
   catalog, task-authentication, frozen-intent and publication cases.
6. Inspect the `conformance/r2/` prefix. Preserve unexpected leftovers and failed receipts for
   diagnosis. Delete only objects proven to belong to the completed disposable run under the
   separately approved cleanup procedure.
7. Mark the R2 gate `passed` only after owner review. A skipped job is `blocked` or `untested`;
   a deterministic green job is still `unqualified`; a failed live job is `failed` and its
   objects must be reconciled before retry.

Passing only the limited storage probe does not pass the native gates. The reviewed
campaign on `e9181eb1` has the following results; repeat them for changed dependencies:

| Gate | Owner-reviewed status on `e9181eb1` |
|---|---|
| Native accepted-CAS response loss, fresh caches and retained-reader recovery | Passed; client-boundary response loss only |
| Authorization and credential-scope enforcement | Passed; fresh owner attestation plus typed denied-bucket response |
| Native catalog, Flow, task-authentication, frozen-intent and publication recovery | Passed; all 13 process cases |

## Limits

The live storage probe covers the operations the Arco adapter exposes: object HEAD/current
get/range, conditional put, delete, and ordered bounded list. It does not certify immutable
historical version-addressed reads, multipart uploads, temporary credentials, AWS IAM/STS parity,
retention or GC behavior, lifecycle rules, public buckets, CORS, object locking, throughput,
latency, durability, or soak behavior. The native process qualifies only its issued HTTPS catalog
URLs for current full GET and range GET; it does not certify broader presigned-URL behavior,
expiry, or revocation semantics. The native owner case is a client-boundary lost-response
simulation after an accepted CAS (`wire_loss=false`); it does not claim a wire-level R2
response-loss experiment.
