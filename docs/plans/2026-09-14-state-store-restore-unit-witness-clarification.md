# Restore unit witness field clarification

**Status: frozen before unit-codec implementation or measurement.** This supplements, without
rewriting, the Plan7 wire contract SHA-256
`6f6d7dc81f70ab07d1183f16306331b81f3dc5dbec6e33366f20700fd974960b`.
It resolves nested field names that the contract described but did not enumerate. It provides
no runtime, authority, coverage, admission, or publication proof.

## Exact nested records

All records below use the frozen deny-unknown-fields, exact raw JCS decoding rules.

`DirectoryPosition` has exactly:

```text
role: existing physical::Role
root_b64: canonical unpadded base64url of the exact owning Plan7 directory root
path: ordered array of DirectoryPathEntry, at most eight entries
leaf: DirectoryPositionLeafWitness
```

`DirectoryPathEntry` is exactly `{page_sha256: String, child_index: u32}`. Its index names the
complete decoded child vector, including every preceding sibling. It is not an index into a
filtered search result. Structural bounds do not establish membership: the runtime rereads and
authenticates each page, child index, role, scope, digest, and interval against the pinned root.

`DirectoryPositionLeafWitness` and `InputWitness.directory_leaf` have the same exact fields:

```text
first_b64url: String
last_b64url: String
rows: u64
bytes: u32
digest: String
```

Reuse one private Rust type for these identical new wire shapes. They represent the first and
last exact directory keys, positive row count, encoded block byte length, and opaque leaf digest.
The key fields are canonical unpadded base64url. The digest is lowercase `sha256:` hex. Decoding
it must produce the exact 32 bytes in the authenticated directory leaf. The existing directory-v1
binary page and root codecs remain unchanged.

`OutputWitness.directory_leaf` remains the separately frozen shape:

```text
first_key_b64url: String
last_key_b64url: String
rows: u64
bytes: u32
digest: String
```

Its digest and key encodings have the same rules, but its field names are intentionally distinct.
Do not add aliases or silently accept the position/input spelling in an output witness. Output
membership in the candidate root is established later by the independent final builder and
coverage verifier; the output witness itself carries no candidate root or directory path.

The `role` fields reuse the existing physical role codec, whose values are bare strings `kv`,
`active_id`, and `delivery_order`. This is an existing codec, not a new sum type. Restore unit KV
inputs, output witnesses, and source/current cursor positions must select `kv`; the generic role
codec does not authorize substitution of either outbox index. New cursor, reservation, and
singleton sum types continue to use the frozen `kind` tag. Mode and singleton phase retain their
explicitly frozen bare-string exceptions.

## Verification boundary

An input's owning root must equal the corresponding authenticated source/current Plan7 root.
Before its exclusions or rows are used, runtime verification proves directory membership and
complete interval coverage, authenticates the descriptor and index, fences physical object
versions and sizes, and verifies decoded rows. Matching nested fields, shape checks, or a
self-consistent receipt hash alone cannot establish any of those facts. Final verification must
independently reconstruct them from the pinned source/current roots and selected output objects.
