# Verified publication contract

Arco 0.3.0 separates successful computation from readable publication. A
successful worker callback first records the completed attempt and its immutable
publication descriptor with `Pending` visibility. Arco then verifies the object
through storage scoped to the authenticated tenant and Workspace. A process
interruption during verification therefore cannot erase the completed
computation or require the worker to run it again.

The version 1 descriptor contains:

- `manifestId`, which must equal `sha256:<checksumSha256>`;
- a scope-relative `objectPath` and the storage adapter's opaque current
  `objectVersion`;
- the exact byte size and lowercase SHA-256 digest;
- `format: "parquet"`; and
- `schemaRef: <manifestId>#parquet-schema`.

The supported version 1 profile accepts objects up to 256 MiB. Verification has
a 30-second deadline, hashes bounded 8 MiB ranges, checks leading Parquet magic,
parses at most 16 MiB of footer metadata, requires a non-empty physical schema,
and confirms that object version and size remain stable before and after reads.
The opaque version identifies the current object observed through the adapter;
the contract does not promise historical provider-version reads.

Worker-supplied visibility, publication time, errors, and owner evidence are
claims. Arco clears them before recording the descriptor. Verification produces
a separate `Visible` or `Failed` event. Missing descriptors for tasks that
require readable output remain `Pending`; legacy tasks that do not require a
readable output retain their existing computation-only completion behavior.

A callback replay for the same successful attempt may finish publication
verification without rerunning computation. Once an attempt has a descriptor,
the replay must present the same immutable descriptor identity. A changed path,
version, checksum, size, format, or schema reference conflicts.

`GET /api/v1/tasks/{taskId}/publication` uses the same task-scoped bearer token
and returns a descriptor only when the current attempt is `Visible` and carries
owner-minted evidence. Physical Parquet schema presence and content identity are
verified. Logical schema expectations and engine compatibility remain consumer
responsibilities.

Generic orchestration and root transactions cannot submit publication descriptors
or visible-output facts. These facts belong to the task-scoped callback and
storage-owner verification path; caller-provided owner evidence is never authority.
Legacy protobuf events remain readable. Required-output computation with no
publication state remains pending in public run inspection.
