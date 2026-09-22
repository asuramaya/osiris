"""THE OPPORTUNISTIC OFFLOAD RUNNER (src/orchestrator/offload_runner.py, operator
ruling be21384a, Thoth mail 12813) — one tick per test, exercising the real
receipt-file mechanics and the real presence/skip/error branches. `restic` itself is
never actually invoked here for the happy path (no repository to sync to in a test
sandbox) — `_run_restic_backup` is monkeypatched at its own call site, the same
seam `test_soul_store.py`'s own rewrap tests use for their own subprocess-free
edges; the REAL subprocess boundary (`restic snapshots`/`init`/`backup`) is
exercised once, against a genuinely local `local:` repository, to prove the
plumbing (env vars, timeout, return-convention) actually works end to end."""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.orchestrator import offload_runner
from src.orchestrator.backup_settings import write_backup_settings


@pytest.fixture(autouse=True)
def _receipts_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "offload_receipts.json"
    monkeypatch.setenv(offload_runner._RECEIPTS_ENV, str(path))
    return path


@pytest.fixture(autouse=True)
def _restic_password(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD", "a-test-password")


def test_receipts_round_trip_and_merge_never_clobber_a_prior_success() -> None:
    assert offload_runner.offload_receipts() == {}
    offload_runner._write_receipt(
        "nas", {"last_successful_offload": "T1", "last_attempt_at": "T1", "last_error": None})
    offload_runner._write_receipt("nas", {"last_attempt_at": "T2", "last_error": "unreachable"})
    receipts = offload_runner.offload_receipts()
    assert receipts["nas"]["last_successful_offload"] == "T1"  # NEVER clobbered
    assert receipts["nas"]["last_attempt_at"] == "T2"
    assert receipts["nas"]["last_error"] == "unreachable"


async def test_run_offload_tick_skips_a_disabled_target(actions: Actions) -> None:
    await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offload_targets=[{"name": "nas", "kind": "restic",
                          "path_or_url": "sftp://nas/repo",
                          "schedule": "*-*-* 03:00:00", "enabled": False}])
    out = await offload_runner.run_offload_tick(actions.pool)
    assert out["targets"] == [{"name": "nas", "skipped": "disabled"}]
    assert offload_runner.offload_receipts() == {}  # never even attempted


async def test_run_offload_tick_skips_a_local_target_with_no_mountpoint(
    actions: Actions,
) -> None:
    await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offload_targets=[{"name": "drive", "kind": "local",
                          "path_or_url": "/mnt/drive", "expected_mountpoint": "/mnt/drive",
                          "schedule": "*-*-* 03:00:00", "enabled": True}])
    out = await offload_runner.run_offload_tick(actions.pool)
    assert out["targets"] == [
        {"name": "drive", "skipped": "not present (mountpoint absent)"}]
    assert offload_runner.offload_receipts() == {}


async def test_run_offload_tick_records_a_success_receipt(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(offload_runner, "_run_restic_backup", lambda **kw: None)
    await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offload_targets=[{"name": "nas", "kind": "restic",
                          "path_or_url": "sftp://nas/repo",
                          "schedule": "*-*-* 03:00:00", "enabled": True}])
    out = await offload_runner.run_offload_tick(actions.pool)
    assert out["targets"] == [{"name": "nas", "ok": True}]
    receipt = offload_runner.offload_receipts()["nas"]
    assert receipt["last_successful_offload"] is not None
    assert receipt["last_error"] is None


async def test_run_offload_tick_records_a_failure_receipt_never_raises(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        offload_runner, "_run_restic_backup", lambda **kw: "restic backup failed: boom")
    await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offload_targets=[{"name": "nas", "kind": "restic",
                          "path_or_url": "sftp://nas/repo",
                          "schedule": "*-*-* 03:00:00", "enabled": True}])
    out = await offload_runner.run_offload_tick(actions.pool)
    assert out["targets"] == [{"name": "nas", "ok": False, "error": "restic backup failed: boom"}]
    receipt = offload_runner.offload_receipts()["nas"]
    assert receipt["last_error"] == "restic backup failed: boom"
    assert "last_successful_offload" not in receipt  # never had one yet — not fabricated


async def test_run_offload_tick_degrades_the_whole_tick_on_a_missing_password(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OSIRIS_RESTIC_PASSWORD", raising=False)
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    out = await offload_runner.run_offload_tick(actions.pool)
    assert "error" in out
    assert "restic-key init" in out["error"]
    assert out["targets"] == []


@pytest.mark.skipif(shutil.which("restic") is None, reason="restic not installed on this box")
def test_run_restic_backup_real_round_trip_against_a_local_repository(tmp_path: Path) -> None:
    """The ONE real subprocess exercise of the actual restic boundary — a genuine
    `local:` repository under tmp_path, no network, proving the env-var contract
    (RESTIC_REPOSITORY/RESTIC_PASSWORD), the init-if-needed guard, and the return
    convention (None on success) all actually work, not just the mocked-out unit
    tests above."""
    repo = tmp_path / "repo"
    source = tmp_path / "source"
    source.mkdir()
    (source / "file.txt").write_text("real content")
    fail = offload_runner._run_restic_backup(
        repository=f"local:{repo}", password=b"a-real-password", source=source)
    assert fail is None, fail
    assert repo.exists()

