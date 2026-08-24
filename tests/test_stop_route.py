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


async def test_stop_route_stage_a_phase_fires_the_ported_courtesy_logic(
    actions: Actions,
) -> None:
    """dispatch 5441 LEG 1 parity fix: `stage_a` reaches `compute_stop_stage_a` — a seated
    agent with nothing leased gets state='pending' from a single route call, same as the
    pre-port `_stage_a_async` proved in tests/test_stophook.py."""
    from src import mcp_server as srv
    from src.orchestrator.seats import bind_holder

    agent, seat = "agent:stoproute02", "seat:stoproute02"
    seat_obj = await actions.create_or_find_object("Seat", seat, agent)
    await actions.assert_property(seat_obj, "handle", "stoproute2", agent,
                                  datetime.now(UTC), 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    await bind_holder(actions, seat_id=seat, agent_id=agent)
    job_dir = "/j/jobs/stoprout"  # must literally end '/jobs/<sid[:8]>' (find_session_row)
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id=agent,
                            project="osiris", cwd="/repo2", model=None, session_key=None)
    sid = "stoprout-0000-4000-8000-000000000000"  # sid[:8] == "stoprout"

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.stop_route(_FakeRequest({
            "phase": "stage_a", "cwd": "/repo2", "session_id": sid, "pct": 33,
        }))
    finally:
        srv._pool = saved_pool

    payload = json.loads(out.body)
    assert payload == {"result": "ok"}
    state = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='state' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", agent)
    assert state == "pending"


async def test_stop_route_alarms_on_its_own_internal_failure(
    actions: Actions, monkeypatch: object,
) -> None:
    """Unlike the whisper/session-end/precompact routes, /stop's except block used to
    swallow its own failure silently before this fix — dispatch 5441's own "clean up as
    you go" ask. Now it files the SAME hook-failure alarm its siblings already do."""
    from src import mcp_server as srv
    from src.orchestrator import stophook_logic

    async def _boom(*a: object, **k: object) -> None:
        raise RuntimeError("stage_a exploded")

    saved_pool = srv._pool
    srv._pool = actions.pool
    import pytest

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(stophook_logic, "compute_stop_stage_a", _boom)
        try:
            out = await srv.stop_route(_FakeRequest({
                "phase": "stage_a", "cwd": "/x", "session_id": "boomsid1",
            }))
        finally:
            srv._pool = saved_pool
    assert out.status_code == 500
    row = await actions.pool.fetchrow(
        "SELECT o.id FROM objects o "
        "JOIN current_assertions a ON a.object_id=o.id "
        "WHERE o.type='BlindSpot' AND a.name='surface' AND a.value #>> '{}' = 'hook/stop' "
        "LIMIT 1")
    assert row is not None
