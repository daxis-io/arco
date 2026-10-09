# Arco 0.3.0 provider qualification

Arco qualifies each storage provider with Arco's adapter, source revision, lockfile, and
conformance scenarios. Another project's cloud CI is useful as an organization example,
but its result cannot qualify Arco. The deterministic lane proves adapter behavior without
cloud credentials; only the opt-in live lane can produce R2 provider evidence.

## Current status

| Profile | Status | Evidence |
|---|---|---|
| Deterministic S3-compatible adapter | Runnable in ordinary CI | Local HTTP fault probe plus object-store CAS, range, and paging tests |
| Cloudflare R2 | **Blocked / unqualified** | No dedicated account, bucket, or credential scope has been assigned; no live campaign has run |
| Amazon S3 | Separate existing gate | `.github/workflows/s3-conformance.yml`; its result does not qualify R2 |
| GCS and Azure | Separate existing gates | Provider-owned workflows and adapters; their results do not qualify R2 |

Cloudflare documents R2's S3 endpoint as
`https://<ACCOUNT_ID>.r2.cloudflarestorage.com`, its S3 region as `auto`, and support for
`HeadObject`, ranged `GetObject`, conditional `PutObject`, and `ListObjectsV2`. Arco probes
those operations through its existing `S3StorageBackend`; it does not assume AWS STS, IAM,
bucket-owner headers, object versioning, or any other AWS-only behavior. See the current
[R2 S3 API compatibility matrix](https://developers.cloudflare.com/r2/api/s3/api/).

## CI profiles

`.github/workflows/r2-provider-qualification.yml` has two independent jobs:

1. `deterministic` runs for relevant pull requests and `main` changes. It validates R2
   configuration admission, proves safe-read retry and single-attempt ambiguous conditional
   writes against a local HTTP fixture, runs the shared adapter contract, and verifies that
   the ignored live probe is discoverable. Its receipt always says
   `live_provider_executed=false` and `provider_gate=unqualified`.
2. `live-r2` runs only from `workflow_dispatch` with `run_live=true` in the protected
   `r2-qualification` environment. Missing configuration fails. The workflow retains logs
   on both success and failure; a green execution remains `evidence-pending-review` until
   the receipt and leftovers are reconciled.

Each artifact name includes the GitHub run ID and attempt. Its identity receipt records the
source SHA/ref, Rust and Cargo versions, `Cargo.lock` SHA-256, package/features, provider
identity, elapsed ceiling, job status, and gate classification. Deterministic receipts are
retained for 30 days; credentialed receipts, including failures, are retained for 90 days.
The workflow enforces a 1,800-second elapsed ceiling. The fixed-size scenario also declares
estimated upper bounds of 512 requests (including configured SDK retries), 1,024 submitted
bytes, and 1,000 micro-USD. Those three estimates are recorded but are not metered or enforced
by the probe; stop the job if account-side controls cannot bound the campaign independently.

## Dedicated live configuration

Create a disposable bucket and bucket-scoped token before enabling the protected environment.
Do not reuse application or production credentials. Configure names only in GitHub:

| Kind | Name | Requirement |
|---|---|---|
| Environment variable | `ARCO_TEST_R2_ACCOUNT_ID` | 32 lowercase hexadecimal characters |
| Environment variable | `ARCO_TEST_R2_BUCKET` | Dedicated lowercase DNS-style bucket name |
| Environment secret | `R2_ACCESS_KEY_ID` | Token access key scoped to the test bucket |
| Environment secret | `R2_SECRET_ACCESS_KEY` | Matching token secret |

The workflow derives the exact endpoint, sets `AWS_REGION=auto`, and disables EC2 metadata.
The probe rejects inherited AWS profiles, session tokens, generic endpoint overrides, and
default-region overrides before traffic. The token needs object read, write, list, and delete
access only within the disposable bucket. Account-wide administration and bucket creation are
outside the campaign.

## Campaign procedure

1. Record the approved account ID, bucket, owner, expiry, spend ceiling, and rollback owner in
   the change ticket. Confirm the bucket contains no retained or customer data.
2. Configure the protected `r2-qualification` environment and dispatch
   `R2 Provider Qualification` with `run_live=true` against the final candidate SHA.
3. Review the retained receipt and complete log. Confirm the exact candidate, lockfile, toolchain,
   endpoint/account/bucket identity, and that the following scenarios executed:
   - create-if-absent, matching compare-and-swap, stale-token rejection, recreation, and exactly
     one winner from concurrent compare-and-swap attempts;
   - HEAD-confirmed current-version identity followed by full content and range reads. The
     `StorageBackend` trait does not expose immutable historical version-addressed reads, so
     this is current-version consistency evidence only;
   - bounded ordered pagination with an exclusive cursor;
   - deterministic lost-response behavior showing that a conditional write is attempted once
     and remains uncertain until the caller reconciles with HEAD/read.
4. Inspect the `conformance/r2/` prefix. Preserve unexpected leftovers and failed receipts for
   diagnosis. Delete only objects proven to belong to the completed disposable run under the
   separately approved cleanup procedure.
5. Mark the R2 gate `passed` only after owner review. A skipped job is `blocked` or `untested`;
   a deterministic green job is still `unqualified`; a failed live job is `failed` and its
   objects must be reconciled before retry.

Passing this limited live storage probe does not pass the following independent gates:

| Gate | Status until separately executed |
|---|---|
| Response loss after R2 accepts a write | Blocked / untested; the local fixture does not inject faults into R2 |
| Restart with disposable local caches | Untested |
| Retained-reader protection | Untested |
| Authorization and credential-scope enforcement | Blocked / untested |
| Publication visibility and reconciliation | Untested |

## Limits

The live probe covers the operations the Arco adapter exposes: object HEAD/current get/range,
conditional put, delete, and ordered bounded list. It does not expose or certify immutable
historical version-addressed reads. It also does not certify multipart uploads,
presigned URLs, temporary credentials, lifecycle rules, public buckets, CORS, object locking,
AWS IAM/STS parity, throughput, latency, durability, or production retention. The local
lost-response fixture proves Arco's no-automatic-retry boundary; a future provider fault
campaign is required to inject a response loss after R2 accepts a write.
