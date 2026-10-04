"""One offload tick at a time: a second tick must not start a second restic backup into the same
repository."""
from __future__ import annotations

import asyncio
import fcntl
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.orchestrator import offload_runner
from src.orchestrator.backup_settings import write_backup_settings


@pytest.fixture(autouse=True)
def _isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(offload_runner._RECEIPTS_ENV, str(tmp_path / "offload_receipts.json"))
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD", "a-test-password")


async def _one_target(actions: Actions) -> None:
    await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offload_targets=[{"name": "nas", "kind": "restic", "path_or_url": "sftp:nas:/r",
                          "schedule": "*-*-* 03:00:00", "enabled": True}])


async def test_a_tick_that_finds_another_running_does_nothing_and_says_so(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _one_target(actions)
    backups: list[str] = []
    monkeypatch.setattr(offload_runner, "_run_restic_backup",
                        lambda **kw: backups.append("ran"))
    lock = offload_runner._lock_path()
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a") as holder:
        fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)  # a tick is running elsewhere
        out = await offload_runner.run_offload_tick(actions.pool)

    assert out == {"skipped": "already running", "targets": []}
    assert backups == []
    assert offload_runner.offload_receipts() == {}  # it recorded nothing


async def test_two_ticks_started_together_run_one_backup(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _one_target(actions)
    started = asyncio.Event()
    release = asyncio.Event()
    backups: list[str] = []
    loop = asyncio.get_running_loop()

    def _slow_backup(**kw: object) -> None:
        backups.append("ran")
        loop.call_soon_threadsafe(started.set)
        # block this worker thread until the second tick has been refused
        asyncio.run_coroutine_threadsafe(release.wait(), loop).result(timeout=10)

    monkeypatch.setattr(offload_runner, "_run_restic_backup", _slow_backup)
    first = asyncio.create_task(offload_runner.run_offload_tick(actions.pool))
    await asyncio.wait_for(started.wait(), timeout=10)
    second = await offload_runner.run_offload_tick(actions.pool)
    release.set()
    first_out = await asyncio.wait_for(first, timeout=10)

    assert second["skipped"] == "already running"
    assert first_out["targets"] == [{"name": "nas", "ok": True}]
    assert backups == ["ran"]


async def test_the_lock_is_free_again_after_a_tick_so_the_next_one_runs(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _one_target(actions)
    monkeypatch.setattr(offload_runner, "_run_restic_backup", lambda **kw: None)

    first = await offload_runner.run_offload_tick(actions.pool)
    second = await offload_runner.run_offload_tick(actions.pool)

    assert first["targets"] == [{"name": "nas", "ok": True}]
    assert second["targets"] == [{"name": "nas", "ok": True}]  # not refused: the lock was freed
