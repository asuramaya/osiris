"""`/stop` — the Stop hook's server half (thread #180 piece 2 (b), 2026-08-18). Same
exercise pattern as test_heartbeat_route.py's own `_FakeRequest` — no ASGI stack needed."""
from __future__ import annotations

import json
from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator import mounts
from src.parsers.base import EvidenceClass


class _FakeRequest:
    def __init__(self, body: dict[str, object]) -> None:
        self._body = body

    async def json(self) -> dict[str, object]:
        return self._body


async def test_stop_route_deliverable_phase_returns_the_shared_shape(actions: Actions) -> None:
    from src import mcp_server as srv

    agent = "agent:stoproute01"
    obj = await actions.create_or_find_object("Agent", agent, agent)
    await actions.assert_property(obj, "project", "osiris", agent, datetime.now(UTC), 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    await mounts.save_mount(actions.pool, job_dir="/j/stoproute01", agent_id=agent,
                            project="osiris", cwd="/repo", model=None, session_key=None)
    sid = "stoproute01-0000-4000-8000-000000000000"

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.stop_route(_FakeRequest({
            "phase": "deliverable", "cwd": "/repo", "session_id": sid,
        }))
    finally:
        srv._pool = saved_pool

    payload = json.loads(out.body)
    assert "error" not in payload
    result = payload["result"]
    for key in ("n", "senders", "window", "bands", "project"):
        assert key in result


async def test_stop_route_offload_phase_answers_none_for_an_unresolvable_session(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.stop_route(_FakeRequest({
            "phase": "offload", "cwd": "/nowhere", "session_id": "00000000-none",
        }))
    finally:
        srv._pool = saved_pool

    payload = json.loads(out.body)
    assert "error" not in payload
    assert payload["result"] is None


async def test_stop_route_unknown_phase_answers_400(actions: Actions) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.stop_route(_FakeRequest({"phase": "bogus"}))
    finally:
        srv._pool = saved_pool

    assert out.status_code == 400
    payload = json.loads(out.body)
    assert "error" in payload


async def test_stop_route_never_raises_on_a_malformed_body(actions: Actions) -> None:
    """Fail-open like every other route beside it: a body the hook never actually sends
    must degrade to a JSON error, never a 500 the caller can't parse or an unhandled
    exception."""
    from src import mcp_server as srv

    class _BoomRequest:
        async def json(self) -> dict[str, object]:
            raise ValueError("not json")

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.stop_route(_BoomRequest())
    finally:
        srv._pool = saved_pool
    assert out.status_code == 500
    assert "error" in json.loads(out.body)
