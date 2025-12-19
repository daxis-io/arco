# ADR-010: Canonical JSON Serialization

## Status
Accepted

## Context
Arco needs deterministic serialization for:
- Computing stable fingerprints/hashes of DAG plans
- Signing and verifying event envelopes
- Cross-language interoperability (Rust control plane + Python data plane)

JSON serialization is inherently non-deterministic due to:
- Object key ordering (implementation-dependent)
- Whitespace handling
- Float representation (varies by language and runtime)
- Unicode escape sequences

## Decision
Implement a single, strict canonical JSON serializer with these properties:

1. **Object keys sorted lexicographically** (UTF-8 byte order)
2. **No whitespace** (compact output)
3. **No floats allowed** - all numbers must be integers
4. **UTF-8 output** with deterministic escaping

### Float Rejection Rationale
Floats are banned in ALL canonical JSON because:
1. Cross-language float stringification is non-deterministic (`1.1` may become `1.100000000000001` in some implementations)
2. The system must produce identical bytes in Rust and Python
3. Envelope signing/verification requires byte-exact reproducibility

Use integers for all numeric values (milliseconds instead of seconds, millicores for CPU, bytes for memory).

### Python Equivalent
```python
def reject_floats(obj):
    if isinstance(obj, float):
        raise ValueError("float values are not allowed in canonical JSON")
    if isinstance(obj, dict):
        for val in obj.values():
            reject_floats(val)
    elif isinstance(obj, list):
        for val in obj:
            reject_floats(val)

reject_floats(value)
json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
```

### Golden Test Verification
Cross-language determinism is verified via golden tests that ensure Rust and Python produce identical output for reference test vectors.

## Consequences

### Positive
- Deterministic fingerprints across all system components
- Cross-language envelope verification works correctly
- Single serialization function reduces API surface and misuse risk
- No need for floating-point edge case handling

### Negative
- Must convert all floating-point data to integers before serialization
- Slightly more verbose for time values (milliseconds vs seconds)
- Cannot use JSON for data that genuinely requires floats (use Parquet instead)
