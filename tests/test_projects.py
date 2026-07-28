"""THE STUB CULL (msg 1675) — retire_project, a sanctioned status-flip for a dead
SoftwareProject. Every test here demonstrates a refusal the live-signal check exists to
catch, or the disambiguation Thoth flagged by name (a stub project 'ra'/'seshat' is not the
seat of the same name)."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator.mounts import save_mount
from src.orchestrator.projects import retire_project
from src.orchestrator.seats import ensure_seat

NOW = datetime.now(UTC)


async def _stub_project(actions: Actions, canon: str, name: str | None = "stub") -> None:
    pid = await actions.create_or_find_object("SoftwareProject", canon, "test")
    if name is not None:
        await actions.assert_property(pid, "name", name, "test", NOW, 0.9)


async def test_retire_project_retires_a_dead_stub(actions: Actions) -> None:
    await _stub_project(actions, "repo:tmp", "tmp")
    out = await retire_project(actions, project="tmp", actor="agent:test",
                               because="reap: dead stub")
    assert out["retired_project"] == "repo:tmp"
    assert out["because"] == "reap: dead stub"
    assert len(out["id"]) == 8
    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:tmp'")
    assert row["status"] == "retired"
    event = await actions.pool.fetchrow(
        "SELECT event_type, payload FROM object_events WHERE event_type='status_change' "
        "ORDER BY id DESC LIMIT 1")
    assert event is not None
    assert event["payload"]["status"] == "retired"


async def test_retire_project_refuses_blank_because(actions: Actions) -> None:
    await _stub_project(actions, "repo:blank", "blank")
    out = await retire_project(actions, project="blank", actor="agent:test", because="  ")
    assert "because is required" in out["error"]
    row = await actions.pool.fetchrow("SELECT status FROM objects WHERE canonical='repo:blank'")
    assert row["status"] == "active"


async def test_retire_project_refuses_unknown_project(actions: Actions) -> None:
    out = await retire_project(actions, project="does-not-exist", actor="agent:test",
                               because="reap")
    assert "no such SoftwareProject" in out["error"]


async def test_retire_project_refuses_an_already_retired_project(actions: Actions) -> None:
    await _stub_project(actions, "repo:twice", "twice")
    first = await retire_project(actions, project="twice", actor="agent:test", because="reap")
    assert "retired_project" in first
    second = await retire_project(actions, project="twice", actor="agent:test",
                                  because="reap again")
    assert "already retired" in second["error"]


async def test_retire_project_refuses_a_project_with_a_commit(actions: Actions) -> None:
    pid = await actions.create_or_find_object("SoftwareProject", "repo:has-commits", "test")
    await actions.assert_property(pid, "name", "has-commits", "test", NOW, 0.9)
    c = await actions.create_or_find_object("Commit", "commit:c1", "test")
    await actions.create_link(c, pid, "in_repo", "test", NOW, 0.9)
    out = await retire_project(actions, project="has-commits", actor="agent:test", because="reap")
    assert "commit(s)" in out["error"]
    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:has-commits'")
    assert row["status"] == "active"


async def test_retire_project_refuses_a_project_with_an_open_thread(actions: Actions) -> None:
    pid = await actions.create_or_find_object("SoftwareProject", "repo:has-thread", "test")
    await actions.assert_property(pid, "name", "has-thread", "test", NOW, 0.9)
    t = await actions.create_or_find_object("Thread", "thread:t1", "test")
    await actions.create_link(t, pid, "in_repo", "test", NOW, 0.9)
    out = await retire_project(actions, project="has-thread", actor="agent:test", because="reap")
    assert "open thread(s)" in out["error"]


async def test_retire_project_ignores_a_resolved_thread(actions: Actions) -> None:
    pid = await actions.create_or_find_object("SoftwareProject", "repo:closed-thread", "test")
    await actions.assert_property(pid, "name", "closed-thread", "test", NOW, 0.9)
    t = await actions.create_or_find_object("Thread", "thread:t2", "test")
    await actions.create_link(t, pid, "in_repo", "test", NOW, 0.9)
    await actions.pool.execute("UPDATE objects SET status='resolved' WHERE id=$1", t)
    out = await retire_project(actions, project="closed-thread", actor="agent:test", because="reap")
    assert "retired_project" in out


async def test_retire_project_refuses_a_live_mount(actions: Actions) -> None:
    await _stub_project(actions, "repo:live-mount", "live-mount")
    await save_mount(actions.pool, job_dir="/j/1", agent_id="agent:x", project="live-mount",
                     cwd="/w/live-mount", model="claude-fable-5", session_key=None, alive=True)
    out = await retire_project(actions, project="live-mount", actor="agent:test", because="reap")
    assert "live mount" in out["error"]


async def test_retire_project_ignores_a_stale_mount(actions: Actions) -> None:
    await _stub_project(actions, "repo:stale-mount", "stale-mount")
    await save_mount(actions.pool, job_dir="/j/2", agent_id="agent:x", project="stale-mount",
                     cwd="/w/stale-mount", model="claude-fable-5", session_key=None, alive=True)
    stale = datetime.now(UTC) - timedelta(hours=6)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen=$1 WHERE project='stale-mount'", stale)
    out = await retire_project(actions, project="stale-mount", actor="agent:test", because="reap")
    assert "retired_project" in out


async def test_retire_project_resolves_by_short_id_and_bare_canonical(actions: Actions) -> None:
    pid = await actions.create_or_find_object("SoftwareProject", "repo:short-id", "test")
    await actions.assert_property(pid, "name", "short-id", "test", NOW, 0.9)
    short = str(pid)[:8]
    out = await retire_project(actions, project=short, actor="agent:test", because="reap")
    assert out["retired_project"] == "repo:short-id"


async def test_retire_project_never_touches_a_seat_of_the_same_name(actions: Actions) -> None:
    """The exact disambiguation Thoth flagged: 'ra' and 'seshat' name stub PROJECTS, not
    the seats of the same names — retiring the project must leave a same-named seat alone,
    and must never accidentally resolve INTO the seat."""
    seat = await ensure_seat(actions, house="osiris", handle="ra", source="test")
    await _stub_project(actions, "repo:ra", "ra")
    out = await retire_project(actions, project="ra", actor="agent:test", because="reap stub")
    assert out["retired_project"] == "repo:ra"          # resolved the PROJECT, not the seat
    seat_row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical=$1", seat["seat_id"])
    assert seat_row["status"] == "active"               # the seat is untouched
    project_row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:ra'")
    assert project_row["status"] == "retired"


async def test_retire_project_refuses_when_only_a_seat_of_that_name_exists(
    actions: Actions,
) -> None:
    """No SoftwareProject named 'ghost' exists — only a same-named Seat. The resolver must
    stay scoped to type='SoftwareProject' and refuse rather than silently retiring the seat."""
    await ensure_seat(actions, house="osiris", handle="ghost", source="test")
    out = await retire_project(actions, project="ghost", actor="agent:test", because="reap")
    assert "no such SoftwareProject" in out["error"]
