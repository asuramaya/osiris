"""`/spawn` — the SubagentStart/SubagentStop server half (mcp_server.py's spawn_route), plus
the fork orientation it now carries (obligation 706c27dc's second half, msg 6034, the
operator's own correction: "the subagent forks need to know they are forks though"). Same
exercise pattern as test_stop_route.py's own `_FakeRequest` — no ASGI stack needed.
"""
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


async def _name_parent(actions: Actions, agent: str) -> None:
    obj = await actions.create_or_find_object("Agent", agent, agent)
    await actions.assert_property(obj, "handle", "khnum", agent, datetime.now(UTC), 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)


async def test_spawn_start_of_a_fork_returns_orientation_naming_its_own_patronym(
    actions: Actions,
) -> None:
    from pathlib import Path

    from src import mcp_server as srv

    parent = "agent:sproutparent1"
    await _name_parent(actions, parent)
    sid = "sproutsess1-4000-8000-000000000001"
    # spawn_route resolves the parent via find_mount keyed on session_id[:8]'s own job_dir —
    # the SAME derivation osiris_hook.py's anchor filter uses (Key Technical Concepts: the
    # durable-job_dir door), so the mount must be registered under that exact path.
    await mounts.save_mount(actions.pool, job_dir=str(Path.home() / ".claude" / "jobs"
                                                       / sid[:8]),
                            agent_id=parent, project="osiris", cwd="/repo", model=None,
                            session_key=None)

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.spawn_route(_FakeRequest({
            "agent_id": "agent-sproutfork1abcd", "session_id": sid,
            "agent_type": "fork", "phase": "start",
        }))
    finally:
        srv._pool = saved_pool

    payload = json.loads(out.body)
    assert payload["spawn"] == "agent:sproutfork1abcd"
    assert "fork_orientation" in payload
    assert "you are a FORK" in payload["fork_orientation"]
    assert "never report the parent's own actions" in payload["fork_orientation"]


async def test_spawn_start_of_an_ordinary_subagent_carries_no_orientation(
    actions: Actions,
) -> None:
    """A fresh (non-fork) subagent has no inherited parent identity to confuse itself
    with — the orientation is scoped to agent_type == 'fork' only."""
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.spawn_route(_FakeRequest({
            "agent_id": "agent-sproutplain1abc", "session_id": "nosession",
            "agent_type": "general-purpose", "phase": "start",
        }))
    finally:
        srv._pool = saved_pool

    payload = json.loads(out.body)
    assert "fork_orientation" not in payload


async def test_spawn_stop_of_a_fork_carries_no_orientation(actions: Actions) -> None:
    """Stop has nothing left to orient — even a fork's Stop announcement stays silent on
    this field, since printing it there would be a stray, unexplained line."""
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.spawn_route(_FakeRequest({
            "agent_id": "agent-sproutfork2abcd", "session_id": "nosession",
            "agent_type": "fork", "phase": "stop",
        }))
    finally:
        srv._pool = saved_pool

    payload = json.loads(out.body)
    assert "fork_orientation" not in payload
