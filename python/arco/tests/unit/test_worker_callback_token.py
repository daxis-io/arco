"""Worker callback token selection tests."""

from __future__ import annotations


def test_dispatch_payload_parses_task_token_fields() -> None:
    from arco_flow.worker.server import DispatchPayload

    payload = DispatchPayload.from_dict(
        {
            "runId": "run-1",
            "taskKey": "task-a",
            "attempt": 1,
            "attemptId": "att-1",
            "taskToken": "payload-token",
            "tokenExpiresAt": "2026-02-20T00:00:00Z",
        }
    )

    assert payload.task_token == "payload-token"
    assert payload.token_expires_at == "2026-02-20T00:00:00Z"


def test_payload_task_token_takes_precedence() -> None:
    from arco_flow.worker.server import _select_task_token

    assert _select_task_token("payload-token", "fallback-token") == "payload-token"


def test_fallback_task_token_used_when_payload_missing() -> None:
    from arco_flow.worker.server import _select_task_token

    assert _select_task_token(None, "fallback-token") == "fallback-token"
    assert _select_task_token(" ", "fallback-token") == "fallback-token"
