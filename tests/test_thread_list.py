"""get_thread_list's "open" filter — dispatch #195 defect 2.

Measured live before this fix: 75.5% false-open (2,553 of 3,380 "active" Thread objects
were actually resolved/retracted, not open). `o.status='active'` is the OBJECT's own
lifecycle column (active vs merged/retired); the thread's own semantic `status` PROPERTY
(open/resolved, written by resolve_thread via a superseding assert_property) was never
checked at all.
"""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.capture import open_thread, resolve_thread


async def test_get_thread_list_excludes_a_resolved_thread(actions: Actions) -> None:
    from src import mcp_server as srv

    t = await open_thread(actions, "a thread that will be resolved", repo="threadlistproj")
    await resolve_thread(actions, str(t), because="done")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.get_thread_list("threadlistproj")
    finally:
        srv._pool = saved_pool
    ids = {th["id"] for th in out["threads"]}
    assert str(t)[:8] not in ids
    assert out["total"] == 0


async def test_get_thread_list_includes_a_genuinely_open_thread(actions: Actions) -> None:
    from src import mcp_server as srv

    t = await open_thread(actions, "a thread that stays open", repo="threadlistproj")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.get_thread_list("threadlistproj")
    finally:
        srv._pool = saved_pool
    ids = {th["id"] for th in out["threads"]}
    assert str(t)[:8] in ids
    assert out["total"] == 1


async def test_get_thread_list_mixed_population_reports_only_the_open_ones(
    actions: Actions,
) -> None:
    """The exact shape of the measured defect: a project with both open and resolved
    threads must report ONLY the open ones, and `total` must match that count, not the
    raw 'active object' count."""
    from src import mcp_server as srv

    open_t = await open_thread(actions, "still open", repo="threadlistproj")
    resolved_t = await open_thread(actions, "will be resolved", repo="threadlistproj")
    await resolve_thread(actions, str(resolved_t), because="done")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.get_thread_list("threadlistproj")
    finally:
        srv._pool = saved_pool
    ids = {th["id"] for th in out["threads"]}
    assert ids == {str(open_t)[:8]}
    assert out["total"] == 1


async def test_get_thread_list_limit_zero_count_only_also_excludes_resolved(
    actions: Actions,
) -> None:
    """The `limit=0` count-only path shares the same WHERE clause — must not regress
    separately from the body-returning path."""
    from src import mcp_server as srv

    await open_thread(actions, "still open", repo="threadlistproj")
    resolved_t = await open_thread(actions, "will be resolved", repo="threadlistproj")
    await resolve_thread(actions, str(resolved_t), because="done")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.get_thread_list("threadlistproj", limit=0)
    finally:
        srv._pool = saved_pool
    assert out["total"] == 1
    assert out["threads"] == []


async def test_get_thread_list_kind_and_owner_filters_still_compose(
    actions: Actions,
) -> None:
    """The fix must not shift the existing $N parameter numbering for kind/owner."""
    from src import mcp_server as srv

    await open_thread(actions, "an obligation for thoth", repo="threadlistproj",
                      kind="obligation", owner="agent:thoth")
    await open_thread(actions, "a question for nobody", repo="threadlistproj",
                      kind="question")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.get_thread_list("threadlistproj", kind="obligation",
                                        owner="agent:thoth")
    finally:
        srv._pool = saved_pool
    assert out["total"] == 1
    assert out["threads"][0]["summary"] == "an obligation for thoth"
