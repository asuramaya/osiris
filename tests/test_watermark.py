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


async def test_all_four_markers_are_none_on_an_empty_set_of_tables(actions: Actions) -> None:
    mark = await graph_watermark(actions.pool)
    assert mark == {"audit_log": None, "fleet_messages": None, "agent_mounts": None,
                    "agent_wakes": None}


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
