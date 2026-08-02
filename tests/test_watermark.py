"""The graph watermark (ruling cf9286b2) — auto-refresh's whole mechanism. Every test here
demonstrates a property the design depends on: each table's marker moves independently
(no cross-table GREATEST that could mask a smaller table's own change), an empty table
reads None (never a false 0 == 0 "nothing changed" once it gets its first row), and
agent_mounts specifically does NOT move on a heartbeat (last_seen) — only on a genuinely
new mount (mounted_at) — the exact noise this design exists to filter out.
"""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.mounts import save_mount
from src.orchestrator.watermark import graph_watermark


async def test_six_markers_are_none_on_an_empty_set_of_tables(actions: Actions) -> None:
    """Six of the seven, not all seven: task #97 workstream 1's catalog seed (conftest's
    `actions` fixture, `is_known_object_type`/`seed_catalog`) writes real Actions calls the
    very first time any test in a pg container's lifetime asks for the catalog — which
    means `audit_log` alone can already be non-None here, depending on whether an earlier
    test in THIS run happened to be the one that triggered the seed. rooms/compositions/
    cases/fleet_messages/agent_mounts/agent_wakes carry no such exception (all six are
    unconditionally emptied by the `actions` fixture on every single test, catalog seed or
    not) — this asserts what's actually guaranteed, rather than a dict-equality check that
    quietly depends on test collection order the way conftest's own dev_pulses comment
    warns against."""
    mark = await graph_watermark(actions.pool)
    assert mark["fleet_messages"] is None
    assert mark["agent_mounts"] is None
    assert mark["agent_wakes"] is None
    assert mark["rooms"] is None
    assert mark["compositions"] is None
    assert mark["cases"] is None


async def test_audit_log_moves_on_any_graph_write_through_actions(actions: Actions) -> None:
    before = await graph_watermark(actions.pool)
    await actions.create_or_find_object("Thread", "thread:wm1", "test")
    after = await graph_watermark(actions.pool)
    assert after["audit_log"] is not None
    assert after["audit_log"] != before["audit_log"]
    # the other three are untouched by a pure graph write
    assert after["fleet_messages"] == before["fleet_messages"]
    assert after["agent_mounts"] == before["agent_mounts"]
    assert after["agent_wakes"] == before["agent_wakes"]


async def test_fleet_messages_moves_on_new_mail_only(actions: Actions) -> None:
    from src.orchestrator.mailbox import send_message

    # send_message refuses a to_project nobody has ever mounted under (f6f3e43e, shape 3 of
    # #117) -- seeded BEFORE the `before` snapshot, or the seed's own mount row would move
    # agent_mounts' watermark between before/after and break this test's own assertion that
    # only fleet_messages moves.
    await save_mount(actions.pool, job_dir="/test/seed/osiris", agent_id="agent:seed-osiris",
                     project="osiris", cwd="/test", model=None, session_key=None, alive=False)
    before = await graph_watermark(actions.pool)
    await send_message(actions.pool, from_agent="agent:wm1", from_project="osiris",
                       to_project="osiris", body="a real broadcast")
    after = await graph_watermark(actions.pool)
    assert after["fleet_messages"] is not None
    assert after["fleet_messages"] != before["fleet_messages"]
    assert after["audit_log"] == before["audit_log"]
    assert after["agent_mounts"] == before["agent_mounts"]
    assert after["agent_wakes"] == before["agent_wakes"]


async def test_agent_mounts_moves_on_a_new_mount(actions: Actions) -> None:
    before = await graph_watermark(actions.pool)
    await save_mount(actions.pool, job_dir="/j/wm1", agent_id="agent:wm1", project="osiris",
                     cwd="/w/osiris", model=None, session_key=None)
    after = await graph_watermark(actions.pool)
    assert after["agent_mounts"] is not None
    assert after["agent_mounts"] != before["agent_mounts"]


async def test_agent_mounts_does_not_move_on_a_heartbeat_re_mount(actions: Actions) -> None:
    """THE WHOLE REASON agent_mounts uses mounted_at, not last_seen (watermark.py's own
    docstring): last_seen updates on every heartbeat from every live session — using it
    here would make the marker move constantly regardless of whether a human would call
    anything "a change". A re-mount of the SAME job_dir (a heartbeat, not a new agent)
    must leave the marker exactly where it was."""
    await save_mount(actions.pool, job_dir="/j/wm2", agent_id="agent:wm2", project="osiris",
                     cwd="/w/osiris", model=None, session_key=None)
    before = await graph_watermark(actions.pool)
    # re-mount the SAME job_dir — a heartbeat re-attach, not a new fleet member
    await save_mount(actions.pool, job_dir="/j/wm2", agent_id="agent:wm2", project="osiris",
                     cwd="/w/osiris", model="claude-fable-5", session_key="k", alive=True)
    after = await graph_watermark(actions.pool)
    assert after["agent_mounts"] == before["agent_mounts"]


async def test_agent_wakes_moves_on_a_new_wake(actions: Actions) -> None:
    before = await graph_watermark(actions.pool)
    await actions.pool.execute(
        "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
        "VALUES ('osiris','agent:wm1',NULL,'resume')")
    after = await graph_watermark(actions.pool)
    assert after["agent_wakes"] is not None
    assert after["agent_wakes"] != before["agent_wakes"]
    assert after["audit_log"] == before["audit_log"]
    assert after["fleet_messages"] == before["fleet_messages"]
    assert after["agent_mounts"] == before["agent_mounts"]


async def test_rooms_moves_on_a_new_room(actions: Actions) -> None:
    """task #109 (Thoth DM 2133): the CATALOG half of auto-refresh — the room switcher's
    own list, not whatever composition happens to be on screen."""
    from src.orchestrator.compositions import create_room

    before = await graph_watermark(actions.pool)
    await create_room(actions.pool, "wm-catalog-room")
    after = await graph_watermark(actions.pool)
    assert after["rooms"] is not None
    assert after["rooms"] != before["rooms"]
    assert after["compositions"] == before["compositions"]
    assert after["cases"] == before["cases"]


async def test_compositions_moves_on_a_new_composition(actions: Actions) -> None:
    """The operator's exact cited case: a compositions backfill (new saved lenses) must
    move this marker so the composer sidebar's shelf stops reading a stale list."""
    from src.orchestrator.compositions import save_composition

    before = await graph_watermark(actions.pool)
    await save_composition(actions.pool, "wm-catalog-lens", {"op": "select"})
    after = await graph_watermark(actions.pool)
    assert after["compositions"] is not None
    assert after["compositions"] != before["compositions"]
    assert after["rooms"] == before["rooms"]
    assert after["cases"] == before["cases"]


async def test_cases_moves_on_a_new_case(actions: Actions) -> None:
    before = await graph_watermark(actions.pool)
    await actions.pool.execute(
        "INSERT INTO cases (name, owner) VALUES ('wm-catalog-case','test')")
    after = await graph_watermark(actions.pool)
    assert after["cases"] is not None
    assert after["cases"] != before["cases"]
    assert after["rooms"] == before["rooms"]
    assert after["compositions"] == before["compositions"]


async def test_cases_also_moves_on_archival_not_just_creation(actions: Actions) -> None:
    """list_rooms' own `cases` count (app.py) filters archived_at IS NULL — archiving a
    case shrinks that count with no new row inserted, so created_at alone would miss it.
    GREATEST(created_at, archived_at) catches both without a migration."""
    cid = await actions.pool.fetchval(
        "INSERT INTO cases (name, owner) VALUES ('wm-catalog-archive','test') RETURNING id")
    before = await graph_watermark(actions.pool)
    await actions.pool.execute("UPDATE cases SET archived_at=now() WHERE id=$1", cid)
    after = await graph_watermark(actions.pool)
    assert after["cases"] is not None
    assert after["cases"] != before["cases"]


async def test_markers_never_get_combined_into_one_cross_table_scalar(actions: Actions) -> None:
    """THE ACTUAL BUG a naive GREATEST() would introduce: once one table's sequence
    outgrows another's, a real change in the smaller table reads as no-change. Proven
    here by simulating exactly that shape — audit_log pushed far ahead of agent_wakes —
    and confirming a fresh agent_wakes row still shows up on ITS OWN key, independent of
    audit_log's much larger value."""
    for i in range(20):
        await actions.create_or_find_object("Thread", f"thread:wm-bulk-{i}", "test")
    mid = await graph_watermark(actions.pool)
    assert mid["audit_log"] is not None and mid["agent_wakes"] is None
    await actions.pool.execute(
        "INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
        "VALUES ('osiris','agent:wm3',NULL,'resume')")
    after = await graph_watermark(actions.pool)
    assert after["agent_wakes"] is not None  # NOT masked by audit_log's much larger id
    assert after["audit_log"] == mid["audit_log"]  # untouched by the wake alone
