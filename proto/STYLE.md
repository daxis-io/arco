# Proto Evolution Rules

## Field Number Allocation

- **1-15**: Reserved for frequently accessed fields (smaller wire encoding)
- **16-2047**: Standard fields
- **19000-19999**: Reserved by protobuf implementation
- **20+**: Event payload oneofs and extensions

## Wire Compatibility Rules

### Safe Changes (backward + forward compatible)
- Adding new fields with new field numbers
- Adding new enum values (with unknown handling)
- Adding new oneof members
- Removing fields (but keep field number reserved)
- Renaming fields (wire format uses numbers, not names)

### Breaking Changes (NEVER do these)
- Changing field numbers
- Changing field types (even between compatible-seeming types like int32/int64)
- Removing or reusing reserved field numbers
- Changing `repeated` to non-repeated or vice versa
- Changing `optional` to `required` (proto2)

### Deprecation Process
1. Add `[deprecated = true]` option to the field
2. Update documentation explaining replacement
3. Keep field in proto for at least 2 major versions
4. Add field number to `reserved` block after removal

## Versioning Strategy

- Package version in path: `arco.catalog.v1`, `arco.orchestration.v1`
- Minor changes: add fields, add enum values
- Major version bump: structural changes requiring new package

## Reserved Field Numbers

When removing fields, always reserve the number:

```protobuf
message Example {
  reserved 3, 7;  // Previously: deprecated_field, old_field
  reserved "deprecated_field", "old_field";
}
```

## Naming Conventions

- Messages: `PascalCase`
- Fields: `snake_case`
- Enums: `SCREAMING_SNAKE_CASE` with type prefix
- Services: `PascalCase` with `Service` suffix
- RPCs: `PascalCase` verb-noun (e.g., `GetTable`, `CommitOrchestrationBatch`)

## Change Review Checklist

Before merging proto changes:

- [ ] `buf lint` passes
- [ ] `cargo xtask proto-breaking-check` passes against `proto-baselines/post-hard-cut-v1.binpb`
- [ ] New fields have clear documentation
- [ ] Field numbers follow allocation guidelines
- [ ] Enum values include `_UNSPECIFIED = 0` default
- [ ] No field number reuse
- [ ] Deprecated fields marked with option

## JSON Contract Policy

The public proto surface must preserve both **binary protobuf/gRPC** compatibility and
**ProtoJSON** compatibility. The active Buf breaking policy is `WIRE_JSON` — see
[buf.yaml](../buf.yaml).

The frozen post-cut baseline image lives at
`proto-baselines/post-hard-cut-v1.binpb`. Run `cargo xtask proto-breaking-check` to verify that
current proto changes remain compatible with that baseline.
Regenerate the baseline with
`buf build proto -o proto-baselines/post-hard-cut-v1.binpb`. Keep source info in
the image because CI's pinned Buf version validates it for breaking checks.

## Pre-Freeze Hard-Cut Policy

The current `arco.*.v1` packages are allowed to receive intentional breaking
changes only as part of the documented pre-freeze hard cut that expands the
public API surface. After the post-hard-cut baseline is regenerated, `v1`
changes must be additive and must preserve binary and ProtoJSON compatibility.
Future broad reshapes require new `v2` packages.

## Pre-Freeze Hard-Cut Policy

The documented pre-freeze hard cut is complete. The current `arco.*.v1`
packages are the durable public proto surface represented by the frozen
post-cut baseline. New v1 changes must be additive and must preserve binary
and ProtoJSON compatibility. Run `cargo xtask proto-breaking-check` before
merging proto changes. Future broad reshapes require new `v2` packages.

A small allowlist of cross-domain value objects in `arco.common.v1` is still annotated with
`serde::{Serialize, Deserialize}` derives in
[crates/arco-proto/build.rs](../crates/arco-proto/build.rs) because those shapes are persisted in
JSON storage objects (control-plane transaction records, golden fixtures) and crossed across
crate boundaries. The current allowlist is:

- `arco.common.v1.PartitionKey`, `PartitionDimension`
- `arco.common.v1.ScalarValue`, `NullValue`

`TableFormat` no longer belongs in `arco.common.v1`; it lives with the catalog domain in
`arco.catalog.v1` because it is not a cross-domain value object.

`FileEntry` no longer belongs in `arco.common.v1`; published output files are owned by the
orchestration contract as `arco.orchestration.v1.FileEntry` because they sit on the public
`TaskOutput` boundary rather than acting as a shared cross-domain value object.

Rules for proto authors:

- Treat ProtoJSON field names and enum spellings as public contract once a message is exposed from
  `proto/arco/*/v1`.
- Keep `arco.common.v1` limited to true cross-domain value objects. Domain-specific nouns such as
  catalog-only enums belong in their owning package.
- Do not expand generated `serde` derives casually. Storage-bound JSON shapes should still prefer
  hand-written `serde` types in the consuming crate unless the shared proto shape is intentionally
  reused across crate boundaries or fixtures.
- If the JSON-eligible value-type set in `arco.common.v1` changes, update both this section and
  [build.rs](../crates/arco-proto/build.rs) so the policy and code stay in sync.
- Storage-internal or binary-only payloads stay in their consuming crate, not in the public proto module.
