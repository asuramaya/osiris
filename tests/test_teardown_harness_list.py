"""Teardown (retire_project, vacate) ignores a merely-recent mount record when the harness's
own list was read and shows no body: a body ended outside osiris (claude stop, a crash)
must not block teardown for the whole 15-minute window. An unreadable list keeps the refusal."""
from __future__ import annotations

import os
from typing import Any

from src.actions.core import Actions
from src.orchestrator import projects, trigger
from src.orchestrator.mounts import save_mount
from src.orchestrator.seats import bind_holder, ensure_seat


async def _empty(**kw: Any) -> list[dict[str, Any]]:
    return []


async def _broken(**kw: Any) -> list[dict[str, Any]]:
    raise trigger.HarnessListUnreadable("down")


def _no_process() -> dict[str, list[int]]:
    return {}


async def _project_with_recent_mount(actions: Actions, name: str, cwd: str) -> None:
    await actions.create_or_find_object("SoftwareProject", f"repo:{name}", "test")
    await save_mount(actions.pool, job_dir=f"/tmp/jobs/{name}-job", agent_id=f"agent:{name}-b",
                     project=name, cwd=cwd, model=None, session_key=None)


async def test_retire_project_ignores_a_stale_mount_when_the_list_shows_no_body(
    actions: Actions,
) -> None:
    await _project_with_recent_mount(actions, "tdstale", "/tmp/tdstale-office")
    out = await projects.retire_project(
        actions, project="tdstale", actor="test", because="done",
        agents_json=_empty, live_by_cwd=_no_process)
    assert out.get("retired_project") == "repo:tdstale", out


async def test_retire_project_still_refuses_an_unreadable_list(actions: Actions) -> None:
    await _project_with_recent_mount(actions, "tdblind", "/tmp/tdblind-office")
    out = await projects.retire_project(
        actions, project="tdblind", actor="test", because="done",
        agents_json=_broken, live_by_cwd=_no_process)
    assert "live mount" in out["error"]


async def test_retire_project_refuses_when_the_list_shows_a_live_body(
    actions: Actions,
) -> None:
    await _project_with_recent_mount(actions, "tdlive", "/tmp/tdlive-office")

    async def _listed(**kw: Any) -> list[dict[str, Any]]:
        return [{"name": "[SC] x", "cwd": "/tmp/tdlive-office", "id": "aa11bb22",
                 "sessionId": "aa11bb22-0000", "status": "busy", "state": "working",
                 "pid": os.getpid()}]

    out = await projects.retire_project(
        actions, project="tdlive", actor="test", because="done",
        agents_json=_listed, live_by_cwd=_no_process)
    assert "live mount" in out["error"]


async def test_retire_project_refuses_when_a_process_is_in_the_directory(
    actions: Actions,
) -> None:
    """A resumed non-bg session is invisible to the list but still has a process there."""
    await _project_with_recent_mount(actions, "tdproc", "/tmp/tdproc-office")
    out = await projects.retire_project(
        actions, project="tdproc", actor="test", because="done", agents_json=_empty,
        live_by_cwd=lambda: {"/tmp/tdproc-office": [4242]})
    assert "live mount" in out["error"]
    out = await projects.retire_project(
        actions, project="tdproc", actor="test", because="done", agents_json=_empty,
        live_by_cwd=lambda: None)  # a blind census is not evidence either
    assert "live mount" in out["error"]


async def _held_seat(actions: Actions, handle: str, office: str) -> str:
    seat = await ensure_seat(actions, house="osiris", handle=handle, source="test")
    seat_id = seat["seat_id"]
    await actions.assert_property(
        await actions.create_or_find_object("Seat", seat_id, "test"),
        "anchor_cwd", office, "test", trigger.datetime.now(trigger.UTC), 0.9)
    await bind_holder(actions, seat_id=seat_id, agent_id=f"agent:{handle}-h", source="test")
    return seat_id


async def _says_live(pool: Any, holder: str) -> dict[str, Any]:
    return {"live": True, "last_seen": "2026-11-11T23:19:30+00:00", "ever_mounted": True}


async def test_vacate_ignores_a_stale_live_reading_when_the_list_shows_no_body(
    actions: Actions,
) -> None:
    seat_id = await _held_seat(actions, "tdvac1", "/tmp/tdvac1-office")
    out = await trigger.vacate_dead_seat(
        actions, seat_id=seat_id, actor="test", because="body ended with claude stop",
        liveness_fn=_says_live, agents_json=_empty, live_by_cwd=_no_process)
    assert out["status"] == "vacated"
    assert out["evidence"]["liveness"]["harness_list_confirms_no_body"] is True


async def test_vacate_still_refuses_an_unreadable_list_or_a_listed_body(
    actions: Actions,
) -> None:
    seat_id = await _held_seat(actions, "tdvac2", "/tmp/tdvac2-office")
    out = await trigger.vacate_dead_seat(
        actions, seat_id=seat_id, actor="test", because="x",
        liveness_fn=_says_live, agents_json=_broken, live_by_cwd=_no_process)
    assert out["status"] == "refused-live"

    async def _listed(**kw: Any) -> list[dict[str, Any]]:
        return [{"name": "[SC] y", "cwd": "/tmp/tdvac2-office", "id": "cc33dd44",
                 "sessionId": "cc33dd44-0000", "status": "busy", "state": "working",
                 "pid": os.getpid()}]

    out = await trigger.vacate_dead_seat(
        actions, seat_id=seat_id, actor="test", because="x",
        liveness_fn=_says_live, agents_json=_listed, live_by_cwd=_no_process)
    assert out["status"] == "refused-live"
