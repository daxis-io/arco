# Frozen Flow execution intent

`POST /api/v1/workspaces/{workspace_id}/runs` accepts selection, partition and
label intent against one deployed manifest. Use `runKey` or `Idempotency-Key`
to recover an uncertain response. Reusing a key with different request intent
returns `409`. Reusing it after a new deployment returns the accepted run.

Before reserving the key, Arco writes canonical immutable metadata at
`accepted_plans/{plan_id}.json`. Version 1 contains the original run and task
events, a SHA-256 reference to the deployed manifest, and each task's exact
asset declaration. Its SHA-256 is the plan fingerprint. The reservation stores
`acceptedPlanSha256`; the run projection carries the server-owned
`arco.accepted_plan_sha256` label. Clients cannot supply that label.

The dispatcher and sweeper load this identity and place the accepted declaration
in the canonical worker envelope's existing `payload` field:

```json
{
  "version": 1,
  "manifest": {"manifestId": "immutable-deployment-id", "sha256": "manifest-digest"},
  "asset": {
    "key": {"namespace": "analytics", "name": "daily"},
    "code": {"artifact": "immutable-code-reference", "entrypoint": "compute"},
    "execution": {"payload": {"sql": "select :value", "parameters": {"value": {"type": "int64", "value": 7}}}},
    "resources": {"memoryBytes": 4096, "timeoutSeconds": 30},
    "io": {"inputs": [{"snapshotId": "immutable-input-id"}]}
  }
}
```

The example abbreviates the asset; dispatch preserves all its declared fields.
Arco preserves these values. The worker validates SQL, typed parameters,
requirements, artifact/input integrity, and compatibility before executing.
Partition and heartbeat scope remain in their existing envelope fields.

Recovery republishes the original event IDs, timestamps, initiator, task graph,
and code version. A run event without its task event is incomplete and is
reconciled through the same accepted plan. Recovery never substitutes the latest
deployment. Missing or corrupt accepted metadata or manifest bytes block recovery
and dispatch. Metadata is limited to 16 MiB and read with bounded ranges.

Legacy reservations without frozen proof can return an already materialized
run. An incomplete legacy reservation returns a conflict and requires operator
reconciliation; adding a request fingerprint alone does not prove the original
execution plan. Legacy run events and worker envelopes remain readable. Reruns
of a frozen parent retain its accepted payloads and manifest; reruns of legacy
parents retain their legacy empty execution payload.

Accepted plans and referenced deployment manifests must remain available while
runs can be dispatched, repaired, retried, or inspected. This release adds no
deletion of accepted-plan objects. A failed or losing acceptance can leave an
unreferenced immutable candidate; automatic cleanup is outside this profile.
Creating a run without an idempotency key still freezes execution, but a client
cannot recover the same run from response loss without knowing its run ID.
