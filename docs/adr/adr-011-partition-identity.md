# ADR-011: Canonical Partition Identity

## Status
Accepted

## Context
Arco's materialization engine needs deterministic partition identifiers for:
- Stable storage paths that don't change across re-materializations
- Cross-language consistency (Rust orchestration + Python data plane)
- URL-safe encoding for object store paths and API parameters
- Type-aware encoding to distinguish `"42"` (string) from `42` (integer)

Previous approaches used JSON serialization for partition keys, but JSON has issues:
- Key ordering is implementation-dependent
- No type distinction in the encoding (`42` vs `"42"`)
- Not URL-safe without additional encoding

## Decision
Implement a typed, URL-safe canonical partition key format:

### Grammar
```text
PARTITION_KEY_CANONICAL ::= dimension ("," dimension)*
dimension              ::= key "=" typed_value
key                    ::= [a-z][a-z0-9_]*   (alphabetically sorted)
typed_value            ::= type_tag ":" encoded_value

type_tag ::=
  "s" (string)  | "i" (int64)    | "b" (bool)
  "d" (date)    | "t" (timestamp) | "n" (null)

encoded_value ::=
  For "s": base64url_no_pad(utf8_bytes)
  For "i": decimal integer (no leading zeros except "0")
  For "b": "true" | "false"
  For "d": "YYYY-MM-DD"
  For "t": "YYYY-MM-DDTHH:MM:SS.ffffffZ"
  For "n": "null"
```

### Examples
- `date=d:2025-01-15` (single date partition)
- `date=d:2025-01-15,region=s:dXMtZWFzdA` (date + region, keys sorted)
- `active=b:true,count=i:42` (boolean + integer)

### Partition ID Derivation
Partition IDs are derived deterministically: `part_{sha256(asset_id:partition_key)[0:32]}`

This ensures the same asset partition always maps to the same ID, enabling stable storage paths.

### No Floats
Floats are excluded from partition keys because:
1. Float representation varies across languages
2. Partition keys should use semantically meaningful types (dates, integers)
3. Floats cause problems in storage paths

## Consequences

### Positive
- Deterministic partition identity across languages
- URL-safe without additional encoding (usable in S3 paths)
- Type-aware encoding prevents confusion between strings and numbers
- Stable storage paths across re-materializations
- Keys always sorted alphabetically for consistency

### Negative
- More verbose than simple JSON (`region=s:dXMtZWFzdA` vs `{"region":"us-east"}`)
- Requires base64 decoding to read string values
- Fixed timestamp precision (microseconds) may be more than needed
- Must validate key names (lowercase alphanumeric with underscores)
