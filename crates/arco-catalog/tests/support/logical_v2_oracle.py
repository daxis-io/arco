"""Independent authority-8 logical V2 oracle; JSON fixture on stdin is optional."""

import hashlib
import json
import struct
import sys


class FramedHash:
    def __init__(self, tag, scope):
        self.hash = hashlib.sha256()
        self.bytes = 0
        self.blob(tag.encode())
        self.u32(2)
        self.scope(scope)

    def raw(self, value):
        self.hash.update(value)
        self.bytes += len(value)

    def u8(self, value):
        self.raw(bytes([value]))

    def u32(self, value):
        self.raw(struct.pack(">I", value))

    def u64(self, value):
        self.raw(struct.pack(">Q", value))

    def blob(self, value):
        self.u64(len(value))
        self.raw(value)

    def scope(self, value):
        for part in scope_parts(value):
            self.blob(part.encode())

    def digest(self, value):
        self.raw(bytes.fromhex(valid_digest(value)))

    def finish(self):
        return self.hash.hexdigest()


def scope_parts(value):
    if isinstance(value, list) and len(value) == 3:
        return value
    if isinstance(value, dict):
        return [
            value.get("tenant_id", value.get("tenantId")),
            value.get("workspace_id", value.get("workspaceId")),
            value.get("domain"),
        ]
    raise ValueError("scope must be [tenant, workspace, domain] or a V2 scope object")


def valid_digest(value):
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("digest must be lowercase 64-hex")
    return value


def raw_hex(value, field):
    if not isinstance(value, str):
        raise ValueError(f"{field} must be hex")
    try:
        return bytes.fromhex(value)
    except ValueError as error:
        raise ValueError(f"invalid {field}") from error


def nonzero(value, field):
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be nonzero")
    return value


def text(value, field):
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be nonempty text")
    return value


def payload(value):
    if "payloadHex" in value:
        return raw_hex(value["payloadHex"], "payloadHex")
    if isinstance(value.get("payload"), list):
        return bytes(value["payload"])
    raise ValueError("addition requires payloadHex or byte-array payload")


def operation_fields(value):
    return (
        text(value.get("operationId", value.get("operation_id")), "operation ID"),
        text(value.get("family"), "operation family"),
        valid_digest(value.get("requestDigest", value.get("request_digest"))),
    )


def commit_id(scope, prior, sequence, operation):
    operation_id, family, request_digest = operation_fields(operation)
    out = FramedHash("arco/control-v2/logical-commit-id", scope)
    out.digest(prior)
    out.u64(nonzero(sequence, "nextSeq"))
    out.blob(operation_id.encode())
    out.blob(family.encode())
    out.digest(request_digest)
    return out.finish(), out.bytes


def encode_writes(out, writes, sequence):
    decoded = []
    for write in writes:
        key = raw_hex(write["rawKeyHex"], "rawKeyHex")
        generation = nonzero(write["generation"], "generation")
        if generation > sequence:
            raise ValueError("write generation exceeds sequence")
        value = None if write.get("rawValueHex") is None else raw_hex(write["rawValueHex"], "rawValueHex")
        decoded.append((key, generation, value))
    decoded.sort(key=lambda write: write[0])
    if any(left[0] == right[0] for left, right in zip(decoded, decoded[1:])):
        raise ValueError("duplicate write key")
    out.u64(len(decoded))
    for key, generation, value in decoded:
        out.blob(key)
        out.u64(generation)
        if value is None:
            out.u8(0)
        else:
            out.u8(1)
            out.blob(value)


def encode_additions(out, additions, scope, sequence, logical_commit_id):
    out.u64(len(additions))
    expected_scope = scope_parts(scope)
    for ordinal, addition in enumerate(additions):
        if addition.get("contractVersion", addition.get("contract_version")) != 2:
            raise ValueError("addition contract version must be 2")
        if addition.get("ordinal") != ordinal:
            raise ValueError("addition ordinal differs from caller order")
        if scope_parts(addition.get("sourceScope", addition.get("source_scope"))) != expected_scope:
            raise ValueError("addition scope differs")
        if addition.get("sourceLogicalSequence", addition.get("source_logical_sequence")) != sequence:
            raise ValueError("addition sequence differs")
        if addition.get("logicalCommitId", addition.get("logical_commit_id")) != logical_commit_id:
            raise ValueError("addition logical commit differs")
        intent_id = text(addition.get("intentId", addition.get("intent_id")), "intent ID")
        projection_kind = text(addition.get("projectionKind", addition.get("projection_kind")), "projection kind")
        body = payload(addition)
        if not body:
            raise ValueError("addition payload must be nonempty")
        out.u32(2)
        out.blob(intent_id.encode())
        out.blob(projection_kind.encode())
        out.scope(expected_scope)
        out.u64(sequence)
        out.digest(logical_commit_id)
        out.u64(ordinal)
        out.blob(body)


def encode_trims(out, trims, sequence):
    decoded = []
    for trim in trims:
        record_id = text(trim.get("recordId", trim.get("record_id")), "trim record ID")
        origin = nonzero(trim.get("originSequence", trim.get("origin_sequence")), "trim origin")
        if origin > sequence:
            raise ValueError("trim origin exceeds sequence")
        ordinal = trim.get("ordinal")
        if not isinstance(ordinal, int) or ordinal < 0:
            raise ValueError("trim ordinal must be nonnegative")
        decoded.append((record_id.encode(), origin, ordinal))
    decoded.sort(key=lambda trim: (trim[0], trim[1]))
    if any(left[:2] == right[:2] for left, right in zip(decoded, decoded[1:])):
        raise ValueError("duplicate trim incarnation")
    out.u64(len(decoded))
    for record_id, origin, ordinal in decoded:
        out.blob(record_id)
        out.u64(origin)
        out.u64(ordinal)


def encode_synthetic_intents(out, intents, scope, sequence):
    out.u64(len(intents))
    expected_scope = scope_parts(scope)
    previous_id = None
    previous_delivery = None
    for addition in intents:
        if addition.get("contractVersion", addition.get("contract_version")) != 2:
            raise ValueError("synthetic intent contract version must be 2")
        if scope_parts(addition.get("sourceScope", addition.get("source_scope"))) != expected_scope:
            raise ValueError("synthetic intent scope differs")
        origin = nonzero(addition.get("sourceLogicalSequence", addition.get("source_logical_sequence")), "synthetic intent origin")
        if origin > sequence:
            raise ValueError("synthetic intent origin exceeds declared sequence")
        logical_id = valid_digest(addition.get("logicalCommitId", addition.get("logical_commit_id")))
        ordinal = addition.get("ordinal")
        if not isinstance(ordinal, int) or ordinal < 0:
            raise ValueError("synthetic intent ordinal must be nonnegative")
        intent_id = text(addition.get("intentId", addition.get("intent_id")), "synthetic intent ID")
        projection_kind = text(addition.get("projectionKind", addition.get("projection_kind")), "synthetic projection kind")
        body = payload(addition)
        if not body:
            raise ValueError("synthetic intent payload must be nonempty")
        delivery = (origin, ordinal, intent_id.encode())
        if previous_id is not None and intent_id.encode() <= previous_id:
            raise ValueError("synthetic intent IDs must be strictly increasing")
        if previous_delivery is not None and delivery <= previous_delivery:
            raise ValueError("synthetic delivery tuples must be strictly increasing")
        previous_id = intent_id.encode()
        previous_delivery = delivery
        out.u32(2)
        out.blob(intent_id.encode())
        out.blob(projection_kind.encode())
        out.scope(expected_scope)
        out.u64(origin)
        out.digest(logical_id)
        out.u64(ordinal)
        out.blob(body)


def synthetic_genesis(fixture):
    scope = scope_parts(fixture["scope"])
    sequence = nonzero(fixture["logicalSequence"], "logicalSequence")
    writes = fixture.get("writes", [])
    intents = fixture.get("intentsV2", [])
    out = FramedHash("arco/control-v2/synthetic-genesis", scope)
    out.u64(sequence)
    decoded = []
    for write in writes:
        key = raw_hex(write["rawKeyHex"], "rawKeyHex")
        generation = nonzero(write["generation"], "generation")
        if generation > sequence:
            raise ValueError("synthetic write generation exceeds sequence")
        value = None if write.get("rawValueHex") is None else raw_hex(write["rawValueHex"], "rawValueHex")
        decoded.append((key, generation, value))
    out.u64(len(decoded))
    previous = None
    for key, generation, value in decoded:
        if previous is not None and key <= previous:
            raise ValueError("synthetic write keys must be strictly increasing")
        previous = key
        out.blob(key)
        out.u64(generation)
        if value is None:
            out.u8(0)
        else:
            out.u8(1)
            out.blob(value)
    encode_synthetic_intents(out, intents, scope, sequence)
    return {"syntheticGenesisHistory": out.finish(), "syntheticGenesisPreimageBytes": out.bytes}


def oracle(fixture):
    scope = scope_parts(fixture["scope"])
    genesis = FramedHash("arco/control-v2/history-genesis", scope).finish()
    prior = genesis if fixture.get("priorHistory") is None else valid_digest(fixture["priorHistory"])
    sequence = nonzero(fixture["nextSeq"], "nextSeq")
    logical_commit_id, commit_bytes = commit_id(scope, prior, sequence, fixture["operation"])
    out = FramedHash("arco/control-v2/history-step", scope)
    out.digest(prior)
    out.u64(sequence)
    out.digest(logical_commit_id)
    encode_writes(out, fixture.get("writes", []), sequence)
    encode_additions(out, fixture.get("additionsV2", []), scope, sequence, logical_commit_id)
    encode_trims(out, fixture.get("trims", []), sequence)
    return {
        "genesis": genesis,
        "logicalCommitId": logical_commit_id,
        "logicalHistory": out.finish(),
        "commitPreimageBytes": commit_bytes,
        "historyPreimageBytes": out.bytes,
    }


DEFAULT_FIXTURE = {
    "scope": ["tenant", "workspace", "catalog"],
    "priorHistory": None,
    "nextSeq": 7,
    "operation": {"operationId": "op-7", "family": "catalog", "requestDigest": "11" * 32},
    "writes": [
        {"rawKeyHex": "", "generation": 7, "rawValueHex": "00ff01"},
        {"rawKeyHex": "ff00", "generation": 7, "rawValueHex": None},
    ],
    "additionsV2": [{
        "contractVersion": 2,
        "intentId": "intent-0",
        "projectionKind": "audit",
        "sourceScope": ["tenant", "workspace", "catalog"],
        "sourceLogicalSequence": 7,
        "logicalCommitId": "86a903a03c1c45daf8578cda5491abba8bce53bd049a1e76b811951b85018ef0",
        "ordinal": 0,
        "payloadHex": "00ff0001",
    }],
    "trims": [{"recordId": "intent-old", "originSequence": 3, "ordinal": 9}],
}


fixture_text = sys.stdin.read().strip()
fixture = json.loads(fixture_text) if fixture_text else DEFAULT_FIXTURE
print(json.dumps(synthetic_genesis(fixture) if "logicalSequence" in fixture else oracle(fixture), sort_keys=True))
