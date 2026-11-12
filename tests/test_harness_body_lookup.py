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
        # separate callers: a clean context, so they do not inherit the holder's "held" mark
        import contextvars

        waiters = [asyncio.create_task(_waiter(), context=contextvars.Context())
                   for _ in range(4)]
        for _ in range(10):
            await asyncio.sleep(0.1)
            in_use.append(pool.get_size() - pool.get_idle_size())
        await asyncio.gather(*waiters)
    # The lock and every waiter live on side connections: the pool's own slots stay free the
    # whole time, so a holder that goes on calling the pool can never starve it.
    assert max(in_use) == 0
    # the lock is free again afterwards
    async with trigger.seat_spawn_lock(pool, "pinless", wait=1.0):
        pass


async def test_every_door_takes_the_seat_lock_and_the_cli_does_not_deadlock_on_itself(
    actions: Actions, tmp_path: Path,
) -> None:
    """launch_seat, resume_seat, wake_worker and dispatch_dm refuse (spawning nothing) while
    another holder has the seat's lock, and a door called inside the CLI's own lock does not
    wait on itself."""
    from src.orchestrator.trigger import launch_seat, resume_seat, spawn_lock_key

    await _seat(actions, tmp_path, "doors")
    pool = actions.pool
    key = await spawn_lock_key(pool, "doors")
    assert key.startswith("seat:")  # one key per seat, whichever way a door names it

    async def _boom(*a: Any, **k: Any) -> Any:
        raise AssertionError("must not spawn while the lock is held elsewhere")

    # hold the lock from another task, so the doors see it busy
    ready = asyncio.Event()
    release = asyncio.Event()

    async def _holder() -> None:
        async with trigger.seat_spawn_lock(pool, key):
            ready.set()
            await release.wait()

    task = asyncio.create_task(_holder())
    await ready.wait()
    try:
        orig = trigger.seat_spawn_lock

        def _quick(p: Any, k: str, *, wait: float = 45.0) -> Any:
            return orig(p, k, wait=0.3)

        import pytest as _pt

        mp = _pt.MonkeyPatch()
        mp.setattr(trigger, "seat_spawn_lock", _quick)
        try:
            out = await launch_seat(Actions(pool), caller="operator", target="doors",
                                    operator_authorized=True, spawn=_boom)
            assert out["status"] == "refused-spawn-in-progress"
            out = await resume_seat(Actions(pool), caller="operator", target="doors",
                                    resume_spawn=_boom)
            assert out["status"] == "refused-spawn-in-progress"
            out = await trigger.wake_worker(Actions(pool), caller="operator", target="doors",
                                            message="hi", spawn=_boom)
            assert out["status"] == "refused-spawn-in-progress"
            # dispatch_dm takes the lock only where it can spawn (its body asks through
            # spawn_gate): a body that reaches that point finds it busy and refuses
            async def _reaches_the_spawn_point(*a: Any, spawn_gate: Any, **k: Any) -> Any:
                await spawn_gate()
                raise AssertionError("must not get past the spawn gate while the lock is held")

            mp.setattr(trigger, "_dispatch_dm_body", _reaches_the_spawn_point)
            mail = await trigger.dispatch_dm(pool, addressee="doors", msg_id=1, sender="x",
                                             spawn=_boom, fresh_spawn=_boom)
            assert mail["mode"] == "window-busy"
        finally:
            mp.undo()
    finally:
        release.set()
        await task
    # reentrant: holding the lock, the same task takes it again without waiting
    async with trigger.seat_spawn_lock(pool, key):
        async with trigger.seat_spawn_lock(pool, key, wait=0.1):
            pass


async def test_stop_waits_for_a_launch_in_its_spawn_window_and_then_sees_the_body(
    actions: Actions,
) -> None:
    """A stop that arrives while a launch is spawning used to read "no body", exit 0, and
    leave the new body running. It now waits for the seat's lock, so by the time it looks the
    body is listed and it stops it."""
    import os

    from src.orchestrator.trigger import _window_name, spawn_lock_key

    seat = await ensure_seat(actions, house="osiris", handle="stopwait", source="test")
    office = "/tmp/stopwait-office"
    await actions.assert_property(
        await actions.create_or_find_object("Seat", seat["seat_id"], "test"),
        "anchor_cwd", office, "test", trigger.datetime.now(trigger.UTC), 0.9)
    name = await _window_name(actions.pool, "osiris", "stopwait", None)
    key = await spawn_lock_key(actions.pool, "stopwait")
    listed: list[dict[str, Any]] = []

    async def _roster(**kw: Any) -> list[dict[str, Any]]:
        return list(listed)

    killed: list[str | None] = []

    async def _kill(pid: int, k: str | None) -> None:
        killed.append(k)

    async def _launching() -> None:
        async with trigger.seat_spawn_lock(actions.pool, key):
            await asyncio.sleep(0.5)  # the spawn window
            listed.append({"name": name, "cwd": office, "pid": os.getpid(), "id": "sw1sw1sw",
                           "state": "working", "sessionId": "sw1sw1sw-0000"})

    launching = asyncio.create_task(_launching(), context=__import__("contextvars").Context())
    await asyncio.sleep(0.15)  # the launch is mid-spawn and holds the lock
    out = await trigger.stop_seat(
        actions, caller="operator", target="stopwait", agents_json=_roster, kill=_kill,
        read_cmdline=lambda p: b"claude\x00--bg")
    await launching
    assert out["status"] == "stopped" and killed == ["sw1sw1sw"]


async def test_mcp_and_wake_lanes_default_to_the_strict_reader(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """launch_seat and resume_seat used the fail-open reader by default, so an unreadable
    list read as "no body" on the MCP and wake lanes. The default is now the strict one."""
    await _seat(actions, tmp_path, "strictdefault")
    calls: list[str] = []

    async def _strict(**kw: Any) -> list[dict[str, Any]]:
        calls.append("strict")
        raise trigger.HarnessListUnreadable("down")

    async def _loose(**kw: Any) -> list[dict[str, Any]]:
        calls.append("loose")
        return []

    monkeypatch.setattr(trigger, "_claude_agents_json_strict", _strict)
    monkeypatch.setattr(trigger, "_claude_agents_json", _loose)
    out = await trigger.launch_seat(Actions(actions.pool), caller="operator",
                                    target="strictdefault", operator_authorized=True,
                                    spawn=_no_spawn)
    assert out["status"] == "refused-harness-list-unreadable"
    await trigger.resume_seat(Actions(actions.pool), caller="operator",
                              target="strictdefault", resume_spawn=_no_spawn)
    assert "loose" not in calls and "strict" in calls


async def test_adoption_skips_a_leftover_row_and_takes_the_one_this_spawn_started(
    actions: Actions,
) -> None:
    import time

    office = "/tmp/adopt-office"
    spawned_at = time.time()
    leftover = {"cwd": office, "id": "old11111", "sessionId": "old11111-0000",
                "status": "idle", "state": "running",
                "startedAt": int((spawned_at - 3600) * 1000)}
    fresh = {"cwd": office, "id": "new22222", "sessionId": "new22222-0000",
             "status": "busy", "state": "working",
             "startedAt": int((spawned_at + 1) * 1000)}

    async def _roster(**kw: Any) -> list[dict[str, Any]]:
        return [leftover, fresh]

    out = await trigger._adopt_resumed_body(
        actions.pool, agents_json=_roster, office=office, requested_sid="req33333-0000",
        holder="agent:adoptholder", project=None, delay=0.01, started_after=spawned_at)
    assert out["session_id"] == "new22222-0000" and out["copied"] is True


async def test_the_hold_fails_closed_on_a_vanished_row_and_a_list_error(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row that vanished, or a list that errored, is not a claimed body: neither ends the
    wait early, only the bound does."""
    import time

    monkeypatch.setattr(trigger, "SPAWN_HOLD_BOUND", 0.6)
    office = "/tmp/hold-office"
    row = {"cwd": office, "id": "hh111111", "sessionId": "hh111111-0000",
           "status": "idle", "state": "running"}
    calls = {"n": 0}

    async def _roster(**kw: Any) -> list[dict[str, Any]]:
        calls["n"] += 1
        if calls["n"] == 1:
            return [row]            # listed, idle, unmounted
        if calls["n"] == 2:
            raise OSError("list failed")   # an error is not "claimed"
        return []                   # then the row vanished

    started = time.monotonic()
    await trigger._adopt_resumed_body(
        actions.pool, agents_json=_roster, office=office, requested_sid="hh111111-0000",
        holder="agent:holdholder", project=None, delay=0.05)
    assert time.monotonic() - started >= 0.5  # waited out the bound, did not return early
    assert calls["n"] >= 4


def test_waiters_wait_longer_than_the_hold_bound() -> None:
    import inspect

    default = inspect.signature(trigger.seat_spawn_lock).parameters["wait"].default
    assert default > trigger.SPAWN_HOLD_BOUND


async def test_a_listed_body_matches_the_handle_whatever_the_case(
    actions: Actions, tmp_path: Path,
) -> None:
    office = await _seat(actions, tmp_path, "casey")

    async def _roster(**kw: Any) -> list[dict[str, Any]]:
        return [{"name": "[SC] Casey", "cwd": "/elsewhere", "id": "ca5e0001",
                 "sessionId": "ca5e0001-0000", "state": "working"}]

    twin = await trigger._launch_twin_check(actions.pool, _roster, str(office),
                                            handle="casey")
    assert twin["harness"] is not None and twin["harness"]["id"] == "ca5e0001"


async def test_the_twin_checks_second_read_failing_clears_roster_ok(
    actions: Actions, tmp_path: Path,
) -> None:
    """The session-match read after the cwd read swallowed an unreadable list into [] and
    left roster_ok True. It now fails closed."""
    office = await _seat(actions, tmp_path, "secondread")
    seat_id = await trigger._seat_for_target(Actions(actions.pool), "secondread")
    # a seat with a holder reaches the second (session-match) read
    from src.orchestrator.seats import bind_holder

    await bind_holder(actions, seat_id=seat_id, agent_id="agent:secondread-h", source="test")
    calls = {"n": 0}

    async def _roster(**kw: Any) -> list[dict[str, Any]]:
        calls["n"] += 1
        if calls["n"] == 1:
            return []
        raise trigger.HarnessListUnreadable("down on the second read")

    twin = await trigger._launch_twin_check(actions.pool, _roster, str(office),
                                            seat_id=seat_id, handle="secondread")
    assert calls["n"] >= 2
    assert twin["roster_ok"] is False


async def test_the_missing_worktree_refusal_names_the_cli_command(
    actions: Actions, tmp_path: Path,
) -> None:
    await _seat(actions, tmp_path, "notree")
    seat_id = await trigger._seat_for_target(Actions(actions.pool), "notree")
    missing = str(tmp_path / "gone-tree")
    await actions.assert_property(
        await actions.create_or_find_object("Seat", seat_id, "test"),
        "tree_cwd", missing, "test", trigger.datetime.now(trigger.UTC), 0.9)

    async def _empty(**kw: Any) -> list[dict[str, Any]]:
        return []

    out = await trigger.launch_seat(Actions(actions.pool), caller="operator", target="notree",
                                    operator_authorized=True, spawn=_no_spawn,
                                    agents_json=_empty)
    assert "is missing" in out["detail"] or "missing" in out["detail"]
    assert f'osiris bind-seat-tree {seat_id} <the real directory> "<why>"' in out["detail"]
    assert "bind_seat_tree(" not in out["detail"]


async def test_holders_of_the_spawn_lock_cannot_starve_the_pool_they_keep_calling(
    pg_dsn: str,
) -> None:
    """Several doors hold different seats' locks and each calls the pool while it holds, with
    as many holders as the pool has slots: with the lock on a pool slot that deadlocked the
    pool (every slot a holder waiting for a second one); on its own connection it finishes."""
    from src.db.pool import create_pool

    pool = await create_pool(pg_dsn, min_size=1, max_size=2, acquire_timeout=3)
    try:
        async def _door(i: int) -> int:
            async with trigger.seat_spawn_lock(pool, f"starve-{i}"):
                await asyncio.sleep(0.2)
                return int(await pool.fetchval("SELECT $1::int", i))

        got = await asyncio.wait_for(asyncio.gather(*[_door(i) for i in range(6)]), timeout=10)
        assert sorted(got) == list(range(6))
    finally:
        await pool.close()


async def test_a_dispatch_that_ends_at_a_cheap_verdict_never_takes_the_spawn_lock(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The backstop sweep re-dispatches every unsettled message every tick and nearly all end
    at a cheap verdict (fyi, paused, settled, braked): about ten lock grants a second, each a
    database connection. The lock is asked for only where a body can start."""
    taken: list[str] = []
    orig = trigger.seat_spawn_lock

    def _count(p: Any, k: str, **kw: Any) -> Any:
        taken.append(k)
        return orig(p, k, **kw)

    monkeypatch.setattr(trigger, "seat_spawn_lock", _count)

    async def _cheap(*a: Any, **k: Any) -> dict[str, str]:
        return {"mode": "queued-fyi", "detail": "an fyi never wakes"}

    monkeypatch.setattr(trigger, "_dispatch_dm_body", _cheap)
    for _ in range(5):
        mail = await trigger.dispatch_dm(actions.pool, addressee="doors", msg_id=1, sender="x")
        assert mail["mode"] == "queued-fyi"
    assert taken == []


async def test_waiters_in_one_process_queue_in_memory_and_side_connections_are_capped(
    pg_dsn: str,
) -> None:
    """Many doors queued on one seat hold no Postgres backend while they wait (one lock in
    memory per key), and doors on different seats never hold more than the cap."""
    from src.db.pool import create_pool

    pool = await create_pool(pg_dsn, min_size=1, max_size=2, acquire_timeout=5)
    try:
        async def _backends() -> int:
            return int(await pool.fetchval(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
                "AND pid <> pg_backend_pid()"))

        base = await _backends()
        order: list[int] = []

        async def _same_seat(i: int) -> None:
            async with trigger.seat_spawn_lock(pool, "queued-seat", wait=20):
                order.append(i)
                await asyncio.sleep(0.05)

        import contextvars

        tasks = [asyncio.create_task(_same_seat(i), context=contextvars.Context())
                 for i in range(30)]
        await asyncio.sleep(0.3)
        # 30 doors, one holder at a time: at most one side connection on top of the pool's own
        assert await _backends() - base <= 1 + 2
        await asyncio.gather(*tasks)
        assert sorted(order) == list(range(30))

        peak = 0
        live = 0

        async def _other_seat(i: int) -> None:
            nonlocal peak, live
            async with trigger.seat_spawn_lock(pool, f"cap-{i}", wait=30):
                live += 1
                peak = max(peak, live)
                await asyncio.sleep(0.2)
                live -= 1

        await asyncio.gather(*[
            asyncio.create_task(_other_seat(i), context=contextvars.Context())
            for i in range(trigger.SIDE_CONNECTION_CAP * 2)])
        assert peak <= trigger.SIDE_CONNECTION_CAP
    finally:
        await pool.close()


async def test_a_child_task_that_outlives_the_door_does_not_inherit_its_lock(
    actions: Actions,
) -> None:
    pool = actions.pool
    child: asyncio.Task[bool] | None = None

    async def _later() -> bool:
        await asyncio.sleep(0.3)  # runs after the door has closed
        try:
            async with trigger.seat_spawn_lock(pool, "outlive", wait=0.2):
                return True
        except trigger.SpawnInProgress:
            return False

    async def _holder() -> None:
        async with trigger.seat_spawn_lock(pool, "outlive"):
            nonlocal child
            child = asyncio.create_task(_later())  # copies the door's context

    await _holder()
    # the door is closed: the child takes the lock for itself and succeeds (it was not a
    # silent pass-through, and it did not deadlock on a lock nobody holds)
    assert child is not None and await child is True


async def test_a_cancelled_door_still_releases_its_side_connection_and_lock(
    actions: Actions,
) -> None:
    pool = actions.pool
    inside = asyncio.Event()

    async def _door() -> None:
        async with trigger.seat_spawn_lock(pool, "cancelled"):
            inside.set()
            await asyncio.sleep(60)

    task = asyncio.create_task(_door())
    await inside.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task  # the cancellation is not swallowed on the way out
    async with trigger.seat_spawn_lock(pool, "cancelled", wait=2):
        pass  # and the lock is free again


async def test_a_full_server_is_waited_out_never_answered_with_a_pool_slot(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncpg
    import src.db.pool as dbpool

    real = dbpool.open_side_connection
    calls = {"n": 0}

    async def _full_twice(pool: Any) -> Any:
        calls["n"] += 1
        if calls["n"] <= 2:
            raise asyncpg.TooManyConnectionsError("sorry, too many clients already")
        return await real(pool)

    monkeypatch.setattr(dbpool, "open_side_connection", _full_twice)
    pool = actions.pool
    async with trigger.seat_spawn_lock(pool, "fullserver", wait=10):
        # held on a side connection once the server had room: no pool slot is pinned
        assert pool.get_size() - pool.get_idle_size() == 0
    assert calls["n"] == 3

    async def _always_full(pool: Any) -> Any:
        raise asyncpg.TooManyConnectionsError("sorry, too many clients already")

    monkeypatch.setattr(dbpool, "open_side_connection", _always_full)
    with pytest.raises(trigger.SpawnInProgress):
        async with trigger.seat_spawn_lock(pool, "fullserver", wait=1.2):
            raise AssertionError("must not hold the lock without a side connection")
