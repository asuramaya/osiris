"""`/heartbeat` — the statusline's server half (thread #180, 2026-08-18). Every rendering
tab used to fork a fresh `asyncpg.connect()` per render (Thoth's measurement: 138 tx/s, 23
backends against an idle fleet of 16). This route runs the SAME logic
`scripts/osiris_statusline.py::_counts` has always run — see
`src.orchestrator.heartbeat.compute_heartbeat`'s own module docstring — against the MCP
server's already-warm shared pool instead. `_FakeRequest` mirrors test_sweep_ledger.py's own
pattern for exercising a `@mcp.custom_route` handler directly, no ASGI stack needed.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator import mounts
from src.parsers.base import EvidenceClass


class _FakeRequest:
    def __init__(self, body: dict[str, object]) -> None:
        self._body = body

    async def json(self) -> dict[str, object]:
        return self._body


async def test_heartbeat_route_bumps_last_seen_and_returns_the_shared_shape(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    agent = "agent:heartbeat01"
    obj = await actions.create_or_find_object("Agent", agent, agent)
    await actions.assert_property(obj, "project", "osiris", agent, datetime.now(UTC), 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    before = await mounts.save_mount(
        actions.pool, job_dir="/j/heartbeat01", agent_id=agent, project="osiris",
        cwd="/repo", model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour' WHERE agent_id = $1",
        agent)

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.heartbeat_route(_FakeRequest({
            "project_hint": "osiris", "session_id": "heartbeat01",
            "model_id": "claude-fable-5", "model_raw": "claude-fable-5",
        }))
    finally:
        srv._pool = saved_pool

    body = out.body
    import json
    payload = json.loads(body)
    assert "error" not in payload
    for key in ("briefs", "mail", "dm", "flight", "souls", "wakes", "owed", "owed_here",
                "sick", "spend", "resolved_project", "resolved_intent",
                "resolved_seat_handle"):
        assert key in payload

    row = await actions.pool.fetchrow(
        "SELECT last_seen FROM agent_mounts WHERE agent_id = $1", agent)
    assert row["last_seen"] > (before or datetime.min.replace(tzinfo=UTC))


async def test_heartbeat_route_never_raises_on_a_malformed_body(actions: Actions) -> None:
    """Fail-open like every other route beside it (automount/session-end/sweep): a body the
    hook never actually sends must degrade to a JSON error, never a 500 the caller can't
    parse or an unhandled exception."""
    from src import mcp_server as srv

    class _BoomRequest:
        async def json(self) -> dict[str, object]:
            raise ValueError("not json")

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.heartbeat_route(_BoomRequest())
    finally:
        srv._pool = saved_pool
    assert out.status_code == 500
    import json
    assert "error" in json.loads(out.body)


async def test_heartbeat_route_with_no_session_id_still_answers_project_counts(
    actions: Actions,
) -> None:
    """A blank statusline render (no session id yet resolved) is a real, accepted shape in
    `_counts` too — the route must answer with the project-scoped numbers, not refuse."""
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.heartbeat_route(_FakeRequest({"project_hint": "osiris"}))
    finally:
        srv._pool = saved_pool
    import json
    payload = json.loads(out.body)
    assert "error" not in payload
    assert payload["resolved_project"] == "osiris"


async def test_heartbeat_route_threads_cwd_into_the_seats_own_location_split(
    actions: Actions, tmp_path: Path,
) -> None:
    """The (A)/(B) statusline-precedence split (thread 6483/6487/6492) lives in
    compute_heartbeat itself (see test_heartbeat.py) — this only proves the route's own
    JSON contract actually carries `cwd` through, since a body the hook never sends
    (mismatched key, dropped field) would silently degrade to the pre-fix behavior with no
    test catching it."""
    from src import mcp_server as srv
    from src.orchestrator.seats import bind_holder, ensure_seat

    anchor = tmp_path / "routecwdcase"
    anchor.mkdir()
    (anchor / ".osiris").write_text('project = "StaleRouteName"\n')

    agent = "agent:routecwdcase"
    seat = await ensure_seat(actions, house="RouteHouse", handle="Routecwdcase",
                             anchor_cwd=str(anchor), source="test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id=agent)
    await actions.create_or_find_object("Agent", agent, agent)
    await mounts.save_mount(
        actions.pool, job_dir="/home/test/.claude/jobs/routecwd", agent_id=agent,
        project="RouteHouse", cwd=str(anchor), model="claude-fable-5", session_key="k")

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.heartbeat_route(_FakeRequest({
            "project_hint": "StaleRouteName", "session_id": "routecwd",
            "model_id": "claude-fable-5", "cwd": str(anchor),
        }))
    finally:
        srv._pool = saved_pool
    import json
    payload = json.loads(out.body)
    assert "error" not in payload
    assert payload["resolved_seat_handle"] == "Routecwdcase"
    assert payload["resolved_project"] == "RouteHouse"  # the graph wins at the seat's own anchor
