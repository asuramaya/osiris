"""thread(action='file'): put an existing thread under a project, without minting one."""
from __future__ import annotations

from typing import Any

from src import mcp_server as srv
from src.actions.core import Actions
from src.orchestrator import capture


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def _file(pool: Any, **kw: Any) -> dict[str, Any]:
    saved = srv._pool
    srv._pool = pool
    try:
        return await srv.thread(action="file", ctx=_Ctx(), **kw)
    finally:
        srv._pool = saved


async def _mk(actions: Actions) -> tuple[str, str]:
    await capture.open_thread(actions, "an unfiled loose end for the file action",
                              kind="task")
    tid = await capture._find_thread(actions.pool, "an unfiled loose end for the file")
    assert tid is not None
    from datetime import UTC, datetime
    await capture._mint_or_find_repo(
        actions, "repo:fileproj", datetime.now(UTC), source="test",
        evidence_class="self_declared", confidence=0.9)
    return str(tid), "fileproj"


async def test_file_links_the_thread_and_changes_nothing_else(actions: Actions) -> None:
    tid, repo = await _mk(actions)
    out = await _file(actions.pool, ref=tid, repo=repo, because="belongs here")
    assert out["status"] == "filed" and out["repo"] == "repo:fileproj"
    again = await _file(actions.pool, ref=tid, repo=repo)
    assert again["status"] == "already filed"
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1::uuid AND type='in_repo' "
        "AND (valid_until IS NULL OR valid_until > now())", tid)
    assert n == 1
    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1::uuid "
        "AND a.name='status' LIMIT 1", tid)
    assert status == "open"


async def test_file_refuses_an_unknown_project_and_never_mints_it(actions: Actions) -> None:
    tid, _ = await _mk(actions)
    out = await _file(actions.pool, ref=tid, repo="no-such-project-anywhere")
    assert "error" in out
    assert await capture._resolve_repo(actions.pool, "no-such-project-anywhere") is None


async def test_file_refuses_an_unknown_thread(actions: Actions) -> None:
    _, repo = await _mk(actions)
    out = await _file(actions.pool, ref="zzzzzzzz-not-a-thread", repo=repo)
    assert "error" in out
