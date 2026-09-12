"""THE SETTINGS MENU's own MCP door pair (thread f4498ab304e4 piece 1) — the `settings`
tool wrapper. The pure functions (list_settings/get_setting/write_setting) are proven
directly in test_settings_service.py; this proves the TOOL routes action='list'/'get'/
'write' correctly and resolves the caller's mounted identity as `actor`, the same
mount-a-fake-ctx harness test_backup_settings_mcp.py's own tests use.
"""
from __future__ import annotations

from pathlib import Path

from src.actions.core import Actions


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def _mounted_ctx(actions: Actions, tmp_path: Path, agent: str):
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.mounts import save_mount

    job_dir = str(tmp_path / "jobs" / agent.replace(":", "")[:8])
    await actions.create_or_find_object("Agent", agent, "test")
    await save_mount(actions.pool, job_dir=job_dir, agent_id=agent, project="osiris",
                     cwd=str(tmp_path), model=None, session_key=None)
    ctx = _Ctx()
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent, session=agent, project="osiris", model=None, cwd=str(tmp_path))
    return ctx


async def test_settings_tool_list_needs_no_authority(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    ctx = await _mounted_ctx(actions, tmp_path, "agent:reader1")
    try:
        out = await srv.settings(action="list", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    keys = {s["key"] for s in out["settings"]}
    assert "daemon.pit_watch.enabled" in keys


async def test_settings_tool_get_needs_no_authority(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    ctx = await _mounted_ctx(actions, tmp_path, "agent:reader1")
    try:
        out = await srv.settings(action="get", key="miner.daily_budget_base", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out == {"key": "miner.daily_budget_base", "value": 5}


async def test_settings_tool_operator_write_succeeds(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    ctx = await _mounted_ctx(actions, tmp_path, "operator")
    try:
        out = await srv.settings(
            action="write", key="daemon.pit_watch.enabled", value=True, ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["value"] is True


async def test_settings_tool_worker_write_without_ruling_refused(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    ctx = await _mounted_ctx(actions, tmp_path, "agent:worker1")
    try:
        out = await srv.settings(
            action="write", key="daemon.pit_watch.enabled", value=True, ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "error" in out and "ruling" in out["error"]


async def test_settings_tool_write_requires_a_key(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    ctx = await _mounted_ctx(actions, tmp_path, "operator")
    try:
        out = await srv.settings(action="write", value=True, ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "error" in out and "key" in out["error"]


async def test_settings_tool_rejects_an_unknown_action(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    ctx = await _mounted_ctx(actions, tmp_path, "operator")
    try:
        out = await srv.settings(action="delete", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "error" in out
