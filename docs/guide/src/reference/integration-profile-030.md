# Arco 0.3.0 integration profile

Arco owns catalog access, immutable Flow intent, task/attempt identity, durable
dispatch and recovery, and verifiable publication metadata. Consumers own SQL,
parameters, execution limits, analytical compatibility, runtimes and deployment.
No Turntable, Daxis or Axon service is required to use these interfaces.

| Boundary | Interface | Profile and limit |
| --- | --- | --- |
| Catalog | Existing catalog/CLI APIs and authorized artifact URLs | Workspace dataset access; unsupported row/column policy fails closed |
| Frozen execution | Run trigger with `runKey` or `Idempotency-Key`; canonical worker `payload` | One deployed manifest and exact code/execution/resource/I/O declarations; incomplete legacy recovery requires operator reconciliation |
| Worker delivery | `HttpTaskEnqueuer`, packaged HTTP dispatcher and sweeper | Durable acceptance and at-least-once delivery; operator queue owns durability, capacity and delivery; HTTP timers and priority refused |
| Publication | Versioned worker output descriptor and owner verification/inspection | Computation success is distinct from verified output; worker claims cannot verify visibility; supported format/schema restrictions apply |
| Projection read | `POST /internal/control-store/catalog-projection/urls` | Default-off operator route; authenticated workspace catalog scope; one pinned cut; no metastore selectors or commit/audit/authority files |
| Storage | Existing S3, GCS and Azure adapters | Implemented adapters are distinct from executed live qualification; R2 uses the S3-compatible adapter with R2-specific configuration |

The [frozen intent contract](frozen-flow-intent.md),
[read descriptor](catalog-read-handoff-030.md), and
[operator dispatch runbook](../../../runbooks/operator-owned-flow-dispatcher.md)
define the supported local integration behavior. Provider conformance follows the
[Arco-owned campaign](../../../runbooks/provider-qualification-030.md).
Signed URL authorization occurs at issuance; immediate URL revocation requires a
consumer-owned delivery boundary.

## Migration from 0.2.1

Rust cloud storage adapters now live in `arco-storage-s3`, `arco-storage-gcs` and
`arco-storage-azure`, composed by `arco-storage::from_bucket`. The portable storage
contract remains in `arco-core`; select the relevant provider features when
embedding. Bare bucket names retain the historical GCS default; use `s3://` or
`az://` to select another provider explicitly.

The API no longer requires `ARCO_COMPACTOR_URL` for production startup. Without
it, enabled Iceberg CRUD uses the existing local Tier1 compactor with the same
fencing and publication mechanisms. JWT, storage and posture requirements still
apply. Remote compaction remains an operator deployment choice.

New run-key reservations contain `acceptedPlanSha256`. Legacy reservations and
envelopes remain readable. A fully materialized legacy run can still be returned;
an incomplete reservation cannot be reconstructed from a newer deployment.
Retain accepted plans and referenced manifests while dispatch, repair, retry or
inspection is possible. Do not delete these objects as routine housekeeping.

Current state-store authority format 9 records commit timestamps and retention
anchors; segment format 2 includes nullable expiry hints. Exercise snapshot,
restore, retained-reader and downgrade compatibility against disposable fixtures
before changing a running authority. The existing authority defaults remain in
place: this release does not enable `control/v1`, expose test-only tenant identity,
or promote private authority-8 deletion. Main's retention Step 4 and private
authority-8 capacity Step 4 remain separate tracks.

Existing Python packages `arco` and `arco-flow` and their CLI scaffolds declare
0.3.0. Public callback additions are optional and old JSON/protobuf readers retain
their existing shape. The frozen protobuf baseline is unchanged; additions use
new fields. Consumers should inspect descriptor versions before use and verify
downloaded bytes against the supplied integrity metadata.

## Runnable local reference

Run the repository-owned examples and acceptance tests without a consumer service:

```sh
cargo shared test --locked -p arco-worker-contract
cargo shared test --locked -p arco-flow --features http-client --test operator_dispatch_contract
cargo shared test --locked -p arco-flow --features http-client --example operator_owned_dispatcher
cargo shared test --locked -p arco-api --all-features --lib accepted_trigger_
cargo shared test --locked -p arco-api --all-features --test control_store_operator_api
```

The filesystem queue is a single-host reference. Its local restart and callback
tests do not certify power-loss durability, multiple queue owners, retention policy,
provider behavior or throughput. Hosting and activation remain operator choices.

For the packaged API, dispatcher and sweeper proof, use a dedicated target and a
disposable Python test environment with `moto[server]` and `boto3`. The driver
executes a typed integer SQL parameter through SQLite, writes real Parquet with
the Rust fixture and inspects owner verification. It also checks catalog HTTP
range reads, uncertain acceptance, duplicate recovery, queue capacity, sweeper
repair and HTTP timer refusal. Ports 5187–5191 and 5198–5200 must be available.

```sh
export CARGO_TARGET_DIR=/absolute/path/to/retained-arco-030-proof-target
cargo build --locked --jobs 2 -p arco-api -p arco-flow \
  --bin arco-api --bin arco_flow_dispatcher --bin arco_flow_sweeper \
  --example pilot_flow_process_seed
cargo metadata --locked --no-deps --format-version 1 > /tmp/arco-030-proof-metadata.json
# Read target_directory from that metadata; pass its debug directory below.
/path/to/test-venv/bin/python crates/arco-flow/tests/independent_process_proof.py \
  /resolved/target_directory/debug /absolute/path/to/new-receipt-directory
```

The driver clears inherited Arco/cloud configuration, assigns fake local S3
credentials and sends every request to loopback. The packaged services retain
their default listener bindings; run this proof on an isolated test machine.
It never accesses a cloud bucket.
Its receipt records binary and fixture hashes and each executed case, including
partial failure. The private queue files contain task tokens: publish sanitized
receipts, not the queue directory. Local emulation does not qualify a provider.
The [independent integration workflow](../../../../.github/workflows/independent-integration-profile.yml)
builds and runs this profile on pull requests, main and release tags, retaining
sanitized receipts. A skipped or unexecuted job supplies no qualification.
