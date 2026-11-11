"""launch, resume and stop decide "is there a body" from the harness's own list: only when that
list was actually read, matched by name or directory, behind a real per-seat lock."""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.cli import cmd_launch
from src.orchestrator import census, trigger
from src.orchestrator.mounts import save_mount
from src.orchestrator.seats import ensure_seat


async def _seat(actions: Actions, tmp_path: Path, handle: str) -> Path:
    office = tmp_path / handle
    office.mkdir()
    await ensure_seat(actions, house="osiris", handle=handle, anchor_cwd=str(office),
                      source="test")
    return office


async def _recent_mount(actions: Actions, office: Path, handle: str) -> None:
    await save_mount(actions.pool, job_dir=f"/tmp/jobs/{handle}-job",
                     agent_id=f"agent:{handle}-body", project="p", cwd=str(office),
                     model=None, session_key=None)


async def _no_spawn(*a: Any, **k: Any) -> None:
    raise AssertionError("must not spawn")


async def test_unreadable_list_is_not_an_empty_list(actions: Actions, tmp_path: Path) -> None:
    office = await _seat(actions, tmp_path, "unread")

    async def _broken(**kw: Any) -> list[dict[str, Any]]:
        raise trigger.HarnessListUnreadable("timed out")

    twin = await trigger._launch_twin_check(actions.pool, _broken, str(office),
                                            handle="unread")
    assert twin["roster_ok"] is False and twin["harness"] is None
    out = await cmd_launch("unread", model=None, pool=actions.pool, spawn=_no_spawn,
                           agents_json=_broken)
    assert out == 1


async def test_unreadable_list_keeps_the_mount_refusal(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    office = await _seat(actions, tmp_path, "unread-mount")
    await _recent_mount(actions, office, "unread-mount")
    monkeypatch.setattr(census, "live_bodies_by_cwd", lambda: {})  # no process, but blind list

    async def _broken(**kw: Any) -> list[dict[str, Any]]:
        raise trigger.HarnessListUnreadable("exit 1")

    seat_id = await trigger._seat_for_target(Actions(actions.pool), "unread-mount")
    twin = await trigger._launch_twin_check(actions.pool, _broken, str(office),
                                            seat_id=seat_id, handle="unread-mount")
    assert twin["mounts"] is not None and twin["ghost_mount"] is None


async def test_a_mount_with_no_process_and_an_empty_list_is_a_ghost(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    office = await _seat(actions, tmp_path, "ghosty")
    await _recent_mount(actions, office, "ghosty")
    seat_id = await trigger._seat_for_target(Actions(actions.pool), "ghosty")

    async def _empty(**kw: Any) -> list[dict[str, Any]]:
        return []

    monkeypatch.setattr(census, "live_bodies_by_cwd", lambda: {})
    twin = await trigger._launch_twin_check(actions.pool, _empty, str(office),
                                            seat_id=seat_id, handle="ghosty")
    assert twin["mounts"] is None and twin["ghost_mount"] is not None
    assert twin["roster_ok"] is True
    monkeypatch.setattr(census, "live_bodies_by_cwd", lambda: {str(office): [7]})
    twin = await trigger._launch_twin_check(actions.pool, _empty, str(office),
                                            seat_id=seat_id, handle="ghosty")
    assert twin["mounts"] is not None and twin["ghost_mount"] is None


async def test_a_listed_body_matches_by_name_even_at_another_directory(
    actions: Actions, tmp_path: Path,
) -> None:
    office = await _seat(actions, tmp_path, "byname")

    async def _roster(**kw: Any) -> list[dict[str, Any]]:
        return [{"name": "[SC] byname", "cwd": "/elsewhere", "id": "a1b2c3d4",
                 "sessionId": "a1b2c3d4-0000", "state": "working"}]

    twin = await trigger._launch_twin_check(actions.pool, _roster, str(office),
                                            handle="byname")
    assert twin["harness"] is not None and twin["harness"]["id"] == "a1b2c3d4"


async def test_two_launches_at_once_spawn_one_body(actions: Actions, tmp_path: Path) -> None:
    await _seat(actions, tmp_path, "race")
    spawned: list[str] = []

    async def _spawn(repo: str, *, name: str, model: str | None, prompt: str) -> None:
        await asyncio.sleep(0.4)  # the harness takes a moment to list the new body
        spawned.append(name)

    async def _roster(**kw: Any) -> list[dict[str, Any]]:
        return [{"name": n, "cwd": str(tmp_path / "race"), "id": "x1", "state": "working"}
                for n in spawned]

    first, second = await asyncio.gather(
        cmd_launch("race", model=None, pool=actions.pool, spawn=_spawn, agents_json=_roster),
        cmd_launch("race", model=None, pool=actions.pool, spawn=_spawn, agents_json=_roster))
    assert (first, second) == (0, 0)
    assert len(spawned) == 1


async def test_fresh_on_a_running_seat_says_it_was_ignored(
    actions: Actions, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    office = await _seat(actions, tmp_path, "freshign")

    async def _roster(**kw: Any) -> list[dict[str, Any]]:
        return [{"name": "[SC] freshign", "cwd": str(office), "id": "f1", "state": "working"}]

    out = await cmd_launch("freshign", model=None, pool=actions.pool, spawn=_no_spawn,
                           agents_json=_roster, fresh=True)
    assert out == 0
    assert "--fresh was ignored" in capsys.readouterr().out


async def test_stop_names_and_stops_every_listed_body(actions: Actions, tmp_path: Path) -> None:
    from src.orchestrator.trigger import _window_name

    seat = await ensure_seat(actions, house="osiris", handle="twobody", source="test")
    office = "/tmp/twobody-office"
    await actions.assert_property(
        await actions.create_or_find_object("Seat", seat["seat_id"], "test"),
        "anchor_cwd", office, "test", trigger.datetime.now(trigger.UTC), 0.9)
    name = await _window_name(actions.pool, "osiris", "twobody", None)
    import os
    rows = [{"name": name, "cwd": office, "pid": os.getpid(), "id": i, "state": "working",
             "sessionId": f"{i}-aaaa"} for i in ("aa111111", "bb222222")]

    async def _roster(**kw: Any) -> list[dict[str, Any]]:
        return rows

    killed: list[str | None] = []

    async def _kill(pid: int, key: str | None) -> None:
        killed.append(key)

    out = await trigger._stop_orphan_harness_body(
        actions.pool, target_seat=seat["seat_id"], kill=_kill, agents_json=_roster,
        read_cmdline=lambda pid: b"claude\x00--bg")
    assert out["status"] == "stopped" and killed == ["aa111111", "bb222222"]
    assert [b["id"] for b in out["stopped_bodies"]] == ["aa111111", "bb222222"]
    assert "aa111111" in out["detail"] and "bb222222" in out["detail"]


async def test_stop_says_when_the_list_could_not_be_read(actions: Actions) -> None:
    seat = await ensure_seat(actions, house="osiris", handle="blindstop", source="test")

    async def _broken(**kw: Any) -> list[dict[str, Any]]:
        raise trigger.HarnessListUnreadable("timed out")

    out = await trigger._stop_orphan_harness_body(
        actions.pool, target_seat=seat["seat_id"], kill=_no_spawn, agents_json=_broken)
    assert out["status"] == "no-live-body" and out["list_unreadable"] is True


async def test_the_strict_list_raises_when_claude_will_not_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _boom(*a: Any, **k: Any) -> Any:
        raise FileNotFoundError("claude")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _boom)
    assert await trigger._claude_agents_json() == []
    with pytest.raises(trigger.HarnessListUnreadable):
        await trigger._claude_agents_json(strict=True)
