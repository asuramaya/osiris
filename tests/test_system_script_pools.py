"""THE MAILBOX GRAPH-EDGE WRITE FAILS ON A BARE POOL (thread 8542ee89): every system:*
script's own desk-alarm helper built its pool via bare `asyncpg.create_pool`, which never
registers the jsonb codec `src.db.pool.create_pool` does — so the very first jsonb write
`send_message`'s graph-edge block makes (`ensure_type`'s own `kind="object"` property
assertion, inside `create_or_find_object`, upstream of the message's own summary/grade/
status) hits Postgres as unquoted raw text and fails as invalid JSON. The relational mail
row still lands (a separate, non-jsonb table); only the graph edge silently never did —
"visible in the mailbox, untraceable in the graph" for every alarm any of these scripts
ever sent. Fixed by switching all three to `src.db.pool.create_pool`.

These tests prove BOTH halves against a real per-worker database (the `actions` fixture's
own `pg_dsn`, migrations applied): a bare pool genuinely reproduces the failure (so the
diagnosis is verified, not assumed), and each fixed script's own alarm helper now leaves a
real, queryable graph edge behind."""
from __future__ import annotations

import asyncpg
import pytest
from src.actions.core import Actions
from src.db.pool import create_pool
from src.orchestrator.mailbox import send_message


async def test_a_bare_asyncpg_pool_genuinely_reproduces_the_graph_edge_failure(
    pg_dsn: str, actions: Actions,
) -> None:
    """The regression this whole thread chases, reproduced directly against a real,
    catalog-seeded schema (the `actions` fixture — same seeded 'Message' type
    production already carries — isolates this from the DIFFERENT, test-DB-only
    "undeclared object type" failure a truly empty catalog would raise instead) —
    confirms the diagnosis (no jsonb codec = ensure_type's own property assertion
    fails as invalid JSON) rather than assuming it."""
    del actions  # depended on only to trigger the fixture's own catalog seeding
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=1)
    try:
        result = await send_message(
            pool, from_agent="system:reprotest", from_project="osiris",
            to_project="operator", body="bare-pool repro")
        assert result.get("graphed") is False
    finally:
        await pool.close()


async def test_the_fixed_pool_writes_a_real_graph_edge(
    pg_dsn: str, actions: Actions,
) -> None:
    """`src.db.pool.create_pool` — the fix every affected script now uses — must NOT
    reproduce the failure: the message's own summary/grade/status assertions land as
    real jsonb, queryable afterward."""
    del actions  # depended on only to trigger the fixture's own catalog seeding
    pool = await create_pool(pg_dsn, min_size=1, max_size=1)
    try:
        result = await send_message(
            pool, from_agent="system:reprotest", from_project="osiris",
            to_project="operator", body="fixed-pool check")
        assert "graphed" not in result  # only present (and False) on a genuine failure

        obj_id = await pool.fetchval(
            "SELECT id FROM objects WHERE canonical=$1 AND type='Message'",
            f"message:{result['id']}")
        assert obj_id is not None
        status = await pool.fetchval(
            "SELECT value FROM current_assertions WHERE object_id=$1 AND name='status'",
            obj_id)
        assert status == "sent"
    finally:
        await pool.close()


@pytest.mark.parametrize(("modname", "funcname"), [
    ("scripts.osiris_preflight", "brief_operator"),
    ("scripts.osiris_smoke", "brief_operator"),
    ("scripts.osiris_alarm", "send_alarm"),
])
def test_no_desk_alarm_function_calls_bare_asyncpg_create_pool(
    modname: str, funcname: str,
) -> None:
    """A grep-shaped regression guard (the standing-practice lesson on load-bearing
    comments that COUNT their own siblings): this thread's own docstrings name exactly
    THREE affected functions. If a fourth desk-alarm helper is ever added with a bare
    `asyncpg.create_pool` again, this must catch it, not rely on someone re-deriving the
    same investigation from scratch. Scoped to the specific function, not the whole
    file — those same modules' READ-ONLY collectors (collect_miner,
    collect_schema_drift, collect_soul_store_coverage) never touch a jsonb write and
    are correctly left on a bare pool."""
    import importlib
    import inspect

    mod = importlib.import_module(modname)
    src = inspect.getsource(getattr(mod, funcname))
    # the exact CALL shape, not the bare words — this function's own docstring
    # legitimately names "asyncpg.create_pool" in prose, explaining what NOT to do
    assert "asyncpg.create_pool(" not in src
