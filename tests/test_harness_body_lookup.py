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


async def test_a_working_body_wearing_a_spare_command_line_is_a_body(
    actions: Actions, tmp_path: Path,
) -> None:
    """Measured live: the harness reuses a pre-warmed spare as the body without changing its
    argv, so a busy, listed body showed `claude bg-spare --bg-spare <sock>`. The twin check
    read it as "not a session" and a second launch spawned a second body."""
    import os

    office = await _seat(actions, tmp_path, "claimed")
    spare_argv = b"claude\x00bg-spare\x00--bg-spare\x00/tmp/cc/spare/x.claim.sock"
    busy = {"name": "[SC] claimed", "cwd": str(office), "pid": os.getpid(), "id": "c1c1c1c1",
            "sessionId": "c1c1c1c1-0000", "status": "busy", "state": "working"}
    idle_unclaimed = {**busy, "status": "idle", "state": "running"}

    async def _busy(**kw: Any) -> list[dict[str, Any]]:
        return [busy]

    async def _idle(**kw: Any) -> list[dict[str, Any]]:
        return [idle_unclaimed]

    twin = await trigger._launch_twin_check(
        actions.pool, _busy, str(office), handle="claimed", read_cmdline=lambda p: spare_argv)
    assert twin["harness"] is not None and twin["stale_harness_row"] is None
    # a spare that never claimed (idle, never mounted) is still not a body
    twin = await trigger._launch_twin_check(
        actions.pool, _idle, str(office), handle="claimed", read_cmdline=lambda p: spare_argv)
    assert twin["harness"] is None and twin["stale_harness_row"] is not None
    # ...until it mounts: a fresh mount under its own session identity makes it a body
    await save_mount(actions.pool, job_dir="/tmp/jobs/claimed-job", agent_id="agent:c1c1c1c1",
                     project="p", cwd=str(office), model=None, session_key=None)
    twin = await trigger._launch_twin_check(
        actions.pool, _idle, str(office), handle="claimed", read_cmdline=lambda p: spare_argv)
    assert twin["harness"] is not None


async def test_launch_keeps_the_lock_until_the_listed_body_is_working_or_mounted(
    actions: Actions, tmp_path: Path,
) -> None:
    """A row can be listed before it is working or mounted. The second launch must not get
    the lock in that window: the first call waits for the row to be claimed."""
    office = await _seat(actions, tmp_path, "settle")
    polls = {"n": 0}
    spawned: list[str] = []

    async def _spawn(repo: str, *, name: str, model: str | None, prompt: str) -> None:
        spawned.append(name)

    async def _roster(**kw: Any) -> list[dict[str, Any]]:
        if not spawned:
            return []
        polls["n"] += 1
        busy = polls["n"] >= 5  # listed idle at first, working a few polls later
        return [{"name": spawned[0], "cwd": str(office), "id": "s1s1s1s1",
                 "sessionId": "s1s1s1s1-0000", "status": "busy" if busy else "idle",
                 "state": "working" if busy else "running"}]

    out = await cmd_launch("settle", model=None, pool=actions.pool, spawn=_spawn,
                           agents_json=_roster)
    assert out == 0 and polls["n"] >= 5


async def test_resume_adoption_holds_until_the_listed_body_is_working_or_mounted(
    actions: Actions, tmp_path: Path,
) -> None:
    office = str(tmp_path / "resumewait")
    polls = {"n": 0}

    async def _roster(**kw: Any) -> list[dict[str, Any]]:
        polls["n"] += 1
        busy = polls["n"] >= 4
        return [{"cwd": office, "id": "r1r1r1r1", "sessionId": "r1r1r1r1-0000",
                 "status": "busy" if busy else "idle",
                 "state": "working" if busy else "running"}]

    out = await trigger._adopt_resumed_body(
        actions.pool, agents_json=_roster, office=office, requested_sid="r1r1r1r1-0000",
        holder="agent:resumewait", project=None, delay=0.01)
    assert out["session_id"] == "r1r1r1r1-0000" and polls["n"] >= 4


async def test_stop_by_list_records_the_session_so_resume_can_find_it(
    actions: Actions,
) -> None:
    """After `osiris stop` ended a body found through the harness list, `resume` said "nothing
    to resume": only the holder-bound stop path recorded the session. Both must."""
    import os

    from src.orchestrator.trigger import _window_name

    seat = await ensure_seat(actions, house="osiris", handle="anchored", source="test")
    office = "/tmp/anchored-office"
    await actions.assert_property(
        await actions.create_or_find_object("Seat", seat["seat_id"], "test"),
        "anchor_cwd", office, "test", trigger.datetime.now(trigger.UTC), 0.9)
    name = await _window_name(actions.pool, "osiris", "anchored", None)
    row = {"name": name, "cwd": office, "pid": os.getpid(), "id": "ab12cd34",
           "state": "working", "sessionId": "ab12cd34-5678-4abc-9def-0123456789ab"}

    async def _roster(**kw: Any) -> list[dict[str, Any]]:
        return [row]

    async def _kill(pid: int, key: str | None) -> None:
        return None

    out = await trigger._stop_orphan_harness_body(
        actions.pool, target_seat=seat["seat_id"], kill=_kill, agents_json=_roster,
        read_cmdline=lambda p: b"claude\x00--bg", holder="agent:anchored-body",
        actor="test")
    assert out["status"] == "stopped"
    assert out["stopped_bodies"][0]["session_anchor_recorded"] is True
    session = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='agent:anchored-body' AND a.name='session'")
    assert session == "ab12cd34"


class _WedgedProc:
    """A `claude agents --json` that never answers."""

    returncode: int | None = None

    def __init__(self) -> None:
        self.killed = False
        self.reaped = False

    async def communicate(self) -> tuple[bytes, bytes]:
        await asyncio.sleep(3600)
        return b"", b""

    def kill(self) -> None:
        self.killed = True

    async def wait(self) -> int:
        self.reaped = True
        return -9


@pytest.mark.parametrize("strict", [True, False])
async def test_a_timed_out_agents_list_is_killed_and_reaped(
    monkeypatch: pytest.MonkeyPatch, strict: bool,
) -> None:
    """Strict or not, a wedged `claude agents --json` hits the deadline and its child is
    killed and reaped (it used to be left running, one per attempt)."""
    proc = _WedgedProc()

    async def _spawn(*a: Any, **k: Any) -> _WedgedProc:
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _spawn)
    monkeypatch.setattr(trigger, "_AGENTS_LIST_DEADLINE", 0.05)
    if strict:
        with pytest.raises(trigger.HarnessListUnreadable):
            await trigger._claude_agents_json(strict=True)
    else:
        assert await trigger._claude_agents_json() == []
    assert proc.killed and proc.reaped


class _AnsweringProc:
    def __init__(self, out: bytes, code: int = 0) -> None:
        self._out = out
        self.returncode = code

    async def communicate(self) -> tuple[bytes, bytes]:
        return self._out, b""


@pytest.mark.parametrize("out", [b"", b"  \n"])
async def test_strict_empty_output_is_unreadable_not_no_body(
    monkeypatch: pytest.MonkeyPatch, out: bytes,
) -> None:
    async def _spawn(*a: Any, **k: Any) -> _AnsweringProc:
        return _AnsweringProc(out)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _spawn)
    with pytest.raises(trigger.HarnessListUnreadable):
        await trigger._claude_agents_json(strict=True)
    assert await trigger._claude_agents_json() == []  # the fail-open callers still read []


async def test_strict_a_printed_empty_list_is_an_answer(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _spawn(*a: Any, **k: Any) -> _AnsweringProc:
        return _AnsweringProc(b"[]\n")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _spawn)
    assert await trigger._claude_agents_json(strict=True) == []


async def test_waiting_for_the_spawn_lock_pins_no_pool_connection(actions: Actions) -> None:
    """While one launch holds a seat's lock, others queued on it hold no connection: the
    pool's in-use count stays at one (the holder) however many wait."""
    pool = actions.pool
    in_use: list[int] = []

    async def _waiter() -> None:
        try:
            async with trigger.seat_spawn_lock(pool, "pinless", wait=1.5):
                raise AssertionError("must not get the lock while it is held")
        except trigger.SpawnInProgress:
            return

    async with trigger.seat_spawn_lock(pool, "pinless"):
        waiters = [asyncio.create_task(_waiter()) for _ in range(4)]
        for _ in range(10):
            await asyncio.sleep(0.1)
            in_use.append(pool.get_size() - pool.get_idle_size())
        await asyncio.gather(*waiters)
    # Before: each of the four waiters held a connection for its whole wait, so five were in
    # use at every sample. Now a waiter borrows one only for an instant per try, so some
    # sample sees just the holder's.
    assert min(in_use) == 1
    # the lock is free again afterwards
    async with trigger.seat_spawn_lock(pool, "pinless", wait=1.0):
        pass
