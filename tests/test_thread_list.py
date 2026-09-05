"""get_thread_list's "open" filter — dispatch #195 defect 2.

Measured live before this fix: 75.5% false-open (2,553 of 3,380 "active" Thread objects
were actually resolved/retracted, not open). `o.status='active'` is the OBJECT's own
lifecycle column (active vs merged/retired); the thread's own semantic `status` PROPERTY
(open/resolved, written by resolve_thread via a superseding assert_property) was never
checked at all.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator.capture import open_thread, record_decision, resolve_thread


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


async def test_get_thread_list_never_duplicates_a_thread_with_a_retracted_in_repo_link(
    actions: Actions,
) -> None:
    """THE RAMSTEIN DOUBLE-THREAD SPECIMEN (thread 1ba9d9be), reproduced directly: a
    thread whose `in_repo` edge was retracted and re-created (an ordinary fold/re-file,
    not a bug in itself) used to appear TWICE — the JOIN onto `links` had no
    `valid_until` filter, so it matched the retracted historical row AND the live one.
    NOT the multi-current-status-row leak (ruling 1335332e) — that class is a different
    table (`assertions`) entirely; confirmed independently already closed."""
    from datetime import UTC, datetime

    from src import mcp_server as srv

    t = await open_thread(actions, "a thread whose repo link gets refiled",
                          repo="threadlistproj")
    proj = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", "repo:threadlistproj")
    now = datetime.now(UTC)
    await actions.invalidate_link(t, proj, "in_repo", "test", now)
    await actions.create_link(t, proj, "in_repo", "test", now, 0.9)

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.get_thread_list("threadlistproj")
    finally:
        srv._pool = saved_pool
    ids = [th["id"] for th in out["threads"]]
    assert ids == [str(t)[:8]]  # exactly once, not twice
    assert out["total"] == 1


async def test_get_thread_list_honest_total_excludes_a_disagreement(actions: Actions) -> None:
    """THE HONEST COUNT (thread 0ae050d8, Thoth DM 6243): `total` counts by the `status`
    PROPERTY alone — a thread closed by a decision (resolves=) and then reopened by a
    DIFFERENT source's later 'open' write still reads property_status='open' and inflates
    `total`, even though a real closure edge already covers it (the `disagree` bucket).
    `honest_total` — closure_buckets' `open_both` count — must exclude it; `total` (its
    existing, unchanged contract) still includes it."""
    from src import mcp_server as srv

    open_t = await open_thread(actions, "genuinely open", repo="honestproj")
    disagree_t = await open_thread(actions, "closed then reopened by another source",
                                   repo="honestproj")
    await record_decision(actions, "settles it", repo="honestproj", resolves=str(disagree_t))
    later = datetime.now(UTC) + timedelta(seconds=1)
    await actions.assert_property(disagree_t, "status", "open", "agent:other", later, 0.9)

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.get_thread_list("honestproj")
    finally:
        srv._pool = saved_pool
    ids = {th["id"] for th in out["threads"]}
    assert ids == {str(open_t)[:8], str(disagree_t)[:8]}
    assert out["total"] == 2
    assert out["honest_total"] == 1
    assert "1 more carry a closure edge" in out["honest_total_note"]


# --- list_unfiled_threads: the H-bucket instrument gap (decision a49d2730/38755abe) —
# threads with NO in_repo edge at all, invisible to get_thread_list(project=...) no
# matter which project is asked. -------------------------------------------------------

async def test_list_unfiled_threads_excludes_a_filed_thread(actions: Actions) -> None:
    from src import mcp_server as srv

    await open_thread(actions, "unfiled-gate: has a project edge", repo="unfiledproj")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.list_unfiled_threads()
    finally:
        srv._pool = saved_pool
    summaries = {th["summary"] for th in out["threads"]}
    assert "unfiled-gate: has a project edge" not in summaries


async def test_list_unfiled_threads_includes_a_genuinely_unfiled_thread(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    t = await open_thread(actions, "unfiled-gate: no project edge at all")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.list_unfiled_threads()
    finally:
        srv._pool = saved_pool
    ids = {th["id"] for th in out["threads"]}
    assert str(t)[:8] in ids


async def test_list_unfiled_threads_excludes_a_resolved_one(actions: Actions) -> None:
    from src import mcp_server as srv

    t = await open_thread(actions, "unfiled-gate: resolved, no project edge")
    await resolve_thread(actions, str(t), because="done")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.list_unfiled_threads()
    finally:
        srv._pool = saved_pool
    ids = {th["id"] for th in out["threads"]}
    assert str(t)[:8] not in ids


async def test_list_unfiled_threads_filters_by_source(actions: Actions) -> None:
    from src import mcp_server as srv

    matching = await open_thread(
        actions, "unfiled-gate: source specimen A", source="a-test-miner")
    await open_thread(actions, "unfiled-gate: source specimen B", source="session")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.list_unfiled_threads(source="a-test-miner")
    finally:
        srv._pool = saved_pool
    ids = {th["id"] for th in out["threads"]}
    assert ids == {str(matching)[:8]}
    assert out["total"] == 1


async def test_list_unfiled_threads_filters_by_kind(actions: Actions) -> None:
    from src import mcp_server as srv

    obl = await open_thread(actions, "unfiled-gate: an obligation", kind="obligation")
    await open_thread(actions, "unfiled-gate: a plain thread")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.list_unfiled_threads(kind="obligation")
    finally:
        srv._pool = saved_pool
    ids = {th["id"] for th in out["threads"]}
    assert str(obl)[:8] in ids
    assert all(th["kind"] == "obligation" for th in out["threads"])


async def test_list_unfiled_threads_limit_zero_is_count_only(actions: Actions) -> None:
    from src import mcp_server as srv

    await open_thread(actions, "unfiled-gate: count-only specimen")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.list_unfiled_threads(limit=0)
    finally:
        srv._pool = saved_pool
    assert out["threads"] == []
    assert out["total"] >= 1


# --- the age-bin instrument (thread 6a1dfc52, Thoth dispatch 7098 item 2): creation age
# exposed and filterable in bulk, so binning a pile by age is a count, not a per-object
# pull at fleet scale. -----------------------------------------------------------------

async def test_list_unfiled_threads_exposes_created_at(actions: Actions) -> None:
    from src import mcp_server as srv

    t = await open_thread(actions, "unfiled-gate: created_at specimen")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.list_unfiled_threads()
    finally:
        srv._pool = saved_pool
    row = next(th for th in out["threads"] if th["id"] == str(t)[:8])
    assert "created_at" in row and "T" in row["created_at"]


async def test_list_unfiled_threads_min_age_days_excludes_a_fresh_thread(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    await open_thread(actions, "unfiled-gate: fresh, excluded by min_age_days")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.list_unfiled_threads(min_age_days=7)
    finally:
        srv._pool = saved_pool
    summaries = {th["summary"] for th in out["threads"]}
    assert "unfiled-gate: fresh, excluded by min_age_days" not in summaries


async def test_list_unfiled_threads_min_age_days_includes_an_old_thread(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    t = await open_thread(actions, "unfiled-gate: 30 days old")
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '30 days' WHERE id = $1", t)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.list_unfiled_threads(min_age_days=7)
    finally:
        srv._pool = saved_pool
    ids = {th["id"] for th in out["threads"]}
    assert str(t)[:8] in ids


async def test_list_unfiled_threads_max_age_days_excludes_an_old_thread(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    t = await open_thread(actions, "unfiled-gate: 90 days old, excluded by max_age_days")
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '90 days' WHERE id = $1", t)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.list_unfiled_threads(max_age_days=30)
    finally:
        srv._pool = saved_pool
    ids = {th["id"] for th in out["threads"]}
    assert str(t)[:8] not in ids


async def test_list_unfiled_threads_age_band_composes_min_and_max(actions: Actions) -> None:
    """The exact shape an age histogram needs: a (min, max] band."""
    from src import mcp_server as srv

    fresh = await open_thread(actions, "unfiled-gate: age-band fresh")
    mid = await open_thread(actions, "unfiled-gate: age-band mid (15d)")
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '15 days' WHERE id = $1", mid)
    old = await open_thread(actions, "unfiled-gate: age-band old (90d)")
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '90 days' WHERE id = $1", old)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.list_unfiled_threads(min_age_days=7, max_age_days=30)
    finally:
        srv._pool = saved_pool
    ids = {th["id"] for th in out["threads"]}
    assert ids == {str(mid)[:8]}
    assert str(fresh)[:8] not in ids
    assert str(old)[:8] not in ids


async def test_get_object_list_thread_branch_exposes_created_at_and_age_filter(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    t = await open_thread(actions, "aged-gate: filed and old", repo="threadlistproj")
    await actions.pool.execute(
        "UPDATE objects SET created_at = now() - interval '45 days' WHERE id = $1", t)
    fresh = await open_thread(actions, "aged-gate: filed and fresh", repo="threadlistproj")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.get_object_list("thread", "threadlistproj", min_age_days=30)
    finally:
        srv._pool = saved_pool
    ids = {th["id"] for th in out["threads"]}
    assert str(t)[:8] in ids
    assert str(fresh)[:8] not in ids
    row = next(th for th in out["threads"] if th["id"] == str(t)[:8])
    assert "created_at" in row and "T" in row["created_at"]
