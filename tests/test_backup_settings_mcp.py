"""backup_settings — the MCP door pair's own tool wrapper (thread f04cce36 piece 3).

The pure functions (get_backup_settings/write_backup_settings) are proven directly in
test_backup_settings.py; this proves the TOOL wrapper resolves the caller's mounted
identity as `actor` (`_actor_for`'s own convention every other write tool uses) and
routes action='get'/'write' correctly — same mount-a-fake-ctx harness
test_settle.py's own tools already use.
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


async def test_backup_settings_tool_get_needs_no_authority(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    ctx = await _mounted_ctx(actions, tmp_path, "agent:reader1")
    try:
        out = await srv.backup_settings(action="get", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["vault_path"] is None


async def test_backup_settings_tool_operator_write_succeeds(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    ctx = await _mounted_ctx(actions, tmp_path, "operator")
    try:
        out = await srv.backup_settings(
            action="write", vault_path="/mnt/nas/osiris-vault",
            because="operator switching targets", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["vault_path"] == "/mnt/nas/osiris-vault"


async def test_backup_settings_tool_worker_write_without_ruling_refused(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    ctx = await _mounted_ctx(actions, tmp_path, "agent:worker1")
    try:
        out = await srv.backup_settings(
            action="write", vault_path="/mnt/nas", because="testing", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "error" in out and "ruling" in out["error"]


async def test_backup_settings_tool_rejects_an_unknown_action(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    ctx = await _mounted_ctx(actions, tmp_path, "operator")
    try:
        out = await srv.backup_settings(action="delete", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "error" in out
