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

- Package version in path: `arco.v1`, `arco.v2`
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
- RPCs: `PascalCase` verb-noun (e.g., `GetAsset`, `CreateRun`)

## Change Review Checklist

Before merging proto changes:

- [ ] `buf lint` passes
- [ ] `buf breaking` passes against main branch
- [ ] New fields have clear documentation
- [ ] Field numbers follow allocation guidelines
- [ ] Enum values include `_UNSPECIFIED = 0` default
- [ ] No field number reuse
- [ ] Deprecated fields marked with option
