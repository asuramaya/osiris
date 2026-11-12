"""Every restic attempt leaves one line beside the receipts, so a failure that a later success
overwrote can still be seen (the receipts hold only the latest state)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

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


def _script(monkeypatch: pytest.MonkeyPatch, *steps: tuple[str | None, int, str | None]) -> None:
    """Each tick's backup: (failure text, exit code, note)."""
    queue = list(steps)

    def fake(**kw: Any) -> str | None:
        fail, code, note = queue.pop(0)
        kw["info"]["exit_code"] = code
        if note:
            kw["notes"].append(note)
        return fail

    monkeypatch.setattr(offload_runner, "_run_restic_backup", fake)


def _lines(tmp_path: Path) -> list[dict[str, Any]]:
    return [json.loads(x) for x in (tmp_path / "offload_attempts.jsonl").read_text().splitlines()]


async def test_a_failure_survives_the_success_that_follows_it(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _one_target(actions)
    _script(monkeypatch, ("restic backup failed: Fatal: boom", 3, None), (None, 0, None))

    await offload_runner.run_offload_tick(actions.pool)
    await offload_runner.run_offload_tick(actions.pool)

    rows = _lines(tmp_path)
    assert [r["outcome"] for r in rows] == ["failed", "ok"]
    assert rows[0]["exit_code"] == 3 and "boom" in rows[0]["detail"]
    assert rows[1]["exit_code"] == 0 and rows[1]["name"] == "nas"
    assert isinstance(rows[0]["duration_s"], float) and rows[0]["at"]
    # the receipts alone have forgotten the failure
    assert offload_runner.offload_receipts()["nas"]["last_error"] is None


async def test_each_outcome_is_named(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _one_target(actions)
    _script(monkeypatch,
            (None, 3, "snapshot saved, but restic could not read some source files: x"),
            ("repository busy (lock by other/1)", 11, None),
            ("restic snapshots failed: ssh: No route to host", 255, None))
    for _ in range(3):
        await offload_runner.run_offload_tick(actions.pool)

    rows = _lines(tmp_path)
    assert [r["outcome"] for r in rows] == ["ok_with_note", "busy", "unreachable"]
    assert "could not read" in rows[0]["detail"] and rows[1]["exit_code"] == 11


async def test_a_skip_that_repeats_is_logged_once(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _one_target(actions)
    _script(monkeypatch, *[("repository busy (lock by other/1)", 11, None)] * 4,
            (None, 0, None), ("repository busy (lock by other/1)", 11, None))
    for _ in range(6):
        await offload_runner.run_offload_tick(actions.pool)

    assert [r["outcome"] for r in _lines(tmp_path)] == ["busy", "ok", "busy"]


def test_the_log_is_trimmed_by_size_to_the_newest_lines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(offload_runner, "_ATTEMPTS_MAX_BYTES", 2000)
    monkeypatch.setattr(offload_runner, "_ATTEMPTS_KEEP_LINES", 5)
    for i in range(60):
        offload_runner._log_attempt("nas", "failed", f"t{i:02d}", duration_s=1.0, detail="x" * 40)

    rows = _lines(tmp_path)
    assert len(rows) <= 60 and (tmp_path / "offload_attempts.jsonl").stat().st_size < 4000
    assert rows[-1]["at"] == "t59"  # the newest survives the trim
    assert not list(tmp_path.glob("*.tmp"))


def test_the_reader_is_newest_first_filters_by_target_and_survives_damage(
    tmp_path: Path,
) -> None:
    assert offload_runner.recent_attempts() == []  # no file at all
    offload_runner._log_attempt("nas", "failed", "t1", duration_s=1)
    offload_runner._log_attempt("usb", "ok", "t2", duration_s=1)
    with (tmp_path / "offload_attempts.jsonl").open("a") as handle:
        handle.write("{half a line\n")
    offload_runner._log_attempt("nas", "ok", "t3", duration_s=1)

    assert [r["at"] for r in offload_runner.recent_attempts()] == ["t3", "t2", "t1"]
    assert [r["at"] for r in offload_runner.recent_attempts("nas")] == ["t3", "t1"]
    assert len(offload_runner.recent_attempts(limit=1)) == 1


def test_a_log_that_cannot_be_written_never_fails_the_caller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "offload_attempts.jsonl").mkdir()  # a directory where the file should be

    offload_runner._log_attempt("nas", "ok", "t1", duration_s=1)  # no exception
