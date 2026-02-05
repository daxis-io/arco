# Python SDK Alpha Execution Prompt

> **Use this prompt when invoking `/superpowers:execute-plan` in a fresh session.**

---

## Execution Prompt

```
Execute the Python SDK Alpha implementation plan at docs/plans/2025-12-17-python-sdk-alpha.md

## Critical Context

This SDK implements the **hybrid architecture** (Rust control plane + Python data plane) for the Arco/Servo orchestration platform. You are building the Python side of a cross-language contract where **Protobuf is the canonical schema** and **JSON is the wire format**.

### Contract-Critical Implementations (DO NOT DEVIATE)

Six critical contract fixes have been integrated into the plan. These are non-negotiable correctness requirements:

1. **AssetIn uses `__class_getitem__` returning TYPES, not instances**
   - `AssetIn["raw.events"]` must return a `type`, not an instance
   - This is required for `typing.get_type_hints()` to work
   - Test with: `assert isinstance(AssetIn["key"], type)`

2. **PartitionKey uses type-tagged strings for `map<string,string>`**
   - Proto contract: `PartitionKey { map<string, string> dimensions = 1; }`
   - All values MUST be strings with type prefixes: `s:`, `i:`, `d:`, `b:`, `t:`, `n:`
   - Example: `{"date": "d:2025-01-15", "tenant": "s:acme", "version": "i:42"}`
   - Floats are PROHIBITED (cross-language determinism)

3. **AssetId comes from Lockfile, NOT generated at import time**
   - Task 11.5 implements `.servo/state.json` lockfile
   - `Lockfile.get_or_create(key)` returns stable ID or creates new one
   - This ensures deploy idempotency and rename semantics

4. **Checks use full CheckDefinition objects, not names**
   - `AssetDefinition.checks` is `list[Check]`, NOT `list[str]`
   - Check objects carry type, severity, phase, and parameters

5. **IoConfig is required on AssetDefinition**
   - Includes format, compression, partition_by fields
   - Defaults to parquet + snappy

6. **AssetContext has report_progress() and report_metric()**
   - Workers stream progress to control plane
   - Metrics collected for observability

### Quality Standards (Enforce Ruthlessly)

| Gate | Requirement | Verification |
|------|-------------|--------------|
| Type Safety | 100% mypy strict | `mypy src/servo --strict` must pass |
| Coverage | ≥80% line coverage | `pytest --cov --cov-fail-under=80` |
| Linting | Zero ruff violations | `ruff check src/ tests/` |
| Docstrings | Google-style on public APIs | `ruff check --select=D` |
| Tests | TDD - write test FIRST, verify FAIL, then implement | Never skip the "verify fail" step |

### TDD Discipline (MANDATORY)

For EVERY implementation step:
1. **Write the failing test first** - Copy test code exactly from plan
2. **Run the test and VERIFY IT FAILS** - If it passes, something is wrong
3. **Implement minimal code** - Only what's needed to pass
4. **Run test and verify it PASSES** - Full green
5. **Commit immediately** - Small, atomic commits

**Why this matters:** Tests that never failed never tested anything. The "verify fail" step proves your test actually exercises the code path.

### Codebase Navigation

- **Working directory:** `python/arco/`
- **Source:** `src/servo/`
- **Tests:** `tests/unit/`, `tests/integration/`
- **Virtual env:** Create with `python -m venv .venv && source .venv/bin/activate`
- **Install dev:** `pip install -e ".[dev]"`

### Key Proto Contracts (Reference)

From `proto/arco/v1/common.proto`:
- `PartitionKey { map<string, string> dimensions = 1; }` - ALL STRING VALUES
- `AssetKey { string namespace = 1; string name = 2; }` - Pattern: `^[a-z][a-z0-9_]*$`
- `ScalarValue` - oneof with string, int64, bool, date, timestamp, null

From `proto/arco/v1/asset.proto`:
- `AssetDefinition` includes `IoConfig io = 50;`
- `CheckDefinition` has type, severity, phase, parameters

### Serialization Rules

- **JSON keys:** camelCase (matching protobuf JSON convention)
- **Canonical form:** sorted keys, no whitespace, deterministic
- **Fingerprints:** SHA-256 of canonical JSON
- **Floats:** PROHIBITED in partition keys

### Common Pitfalls to Avoid

1. **Don't use `AssetId.generate()` directly** - Always go through Lockfile
2. **Don't store raw Python types in PartitionKey** - Use `to_proto_dict()` for serialization
3. **Don't trust `__annotations__` directly** - Use `typing.get_type_hints()`
4. **Don't skip the "verify fail" step** - It's not optional
5. **Don't batch commits** - Commit after each passing test

### Verification Checkpoints

After each milestone, run full verification:

```bash
cd python/arco
# Format
ruff format src/ tests/
# Lint
ruff check src/ tests/
# Type check
mypy src/servo --strict
# Tests with coverage
pytest tests/ -v --cov=src/servo --cov-report=term-missing --cov-fail-under=80
```

### Task Execution Order

Execute tasks in order. Key dependencies:
- Tasks 1-6: Foundation (types, IDs, partitions, checks)
- Tasks 7-9: Registry and decorator
- Task 11.5: Lockfile (MUST complete before Task 12)
- Tasks 10-13: Manifest generation
- Tasks 14-16: CLI commands
- Tasks 17-20: Quality gates and testing

### Success Criteria

The plan is complete when:
1. All tests pass with ≥80% coverage
2. mypy strict mode passes
3. ruff shows zero violations
4. `servo deploy --dry-run` produces valid manifest JSON
5. Manifest JSON matches proto schema (camelCase keys, type-tagged partition values)

Begin execution with Task 1. Report progress after each task completion.
```

---

## Key Insights for the Worker

### Why This Architecture?

Arco uses a **serverless lakehouse** architecture with:
- **Rust control plane** - Strong consistency for DDL (manifests, schemas)
- **Python data plane** - Flexibility for data transformations
- **Protobuf contracts** - Single source of truth across languages

The Python SDK is the **developer-facing interface**. It must:
- Feel Pythonic (`@asset` decorator, type hints)
- Serialize to wire format that Rust can read exactly
- Generate manifests the control plane can deploy

### Why Type-Tagged Strings?

Different languages serialize primitives differently:
- Python: `True` vs `true`
- Go: `42` vs `"42"`
- Dates: ISO format varies

Type-tagged strings (`d:2025-01-15`) ensure:
- Cross-language determinism
- Same partition key = same hash everywhere
- No "ghost partitions" from serialization drift

### Why Lockfile for IDs?

Without lockfile:
- Import module → `AssetId.generate()` → new random ID
- Deploy → new ID registered
- Import again → different ID
- Deploy → duplicate asset!

With lockfile:
- First deploy → ID generated, saved to `.servo/state.json`
- Subsequent deploys → same ID loaded from lockfile
- Rename asset key → ID preserved (just update lockfile mapping)

### Why TDD Discipline?

Tests written after implementation tend to test "what the code does" rather than "what it should do". By writing tests first:
- You define the contract before implementation
- You prove the test can fail (it's actually testing something)
- You write minimal code (no over-engineering)
- You have immediate regression protection

The "verify fail" step catches:
- Tests that import the wrong module
- Assertions that always pass
- Mock setups that don't exercise real code

### Quality is Non-Negotiable

This SDK will be used by data engineers to define production pipelines. A bug in:
- **Serialization** → manifests rejected by control plane
- **ID generation** → duplicate assets, orphaned data
- **Partition keys** → wrong data selected for execution
- **Check definitions** → data quality issues go undetected

Every quality gate exists because we learned the hard way.
