"""THE OFF-BOX BACKUP + RESTORE DRILL (thread cf134938 item 3, Thoth mail 8525 item 3):
both scripts take the restic repository URL as their only parameter, so the operator's
eventual backend ruling on cf134938 is a one-line config change. Proven here with REAL
restic against a real local-directory repository — no docker, no network needed for
that backend, so this is genuine functional verification, not mocked, same discipline
osiris_pitr_drill.py's own real run was held to (that one needed docker; this one
doesn't, so it gets the automated version of the same proof)."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("restic") is None, reason="restic is not installed on this box")

REPO_ROOT = Path(__file__).resolve().parent.parent
BACKUP_SCRIPT = REPO_ROOT / "scripts" / "osiris_offbox_backup.sh"


def _restic_env(password: str = "test-password") -> dict[str, str]:
    return {**os.environ, "RESTIC_PASSWORD": password}


def test_backup_script_refuses_with_no_repo_url(tmp_path: Path) -> None:
    result = subprocess.run(["bash", str(BACKUP_SCRIPT)], capture_output=True, text=True,
                            env=_restic_env())
    assert result.returncode != 0
    assert "usage" in result.stderr.lower()


def test_backup_then_restore_drill_round_trips_real_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The end-to-end proof: back up a fake vault into a fresh local restic repo, then
    run the restore drill against that SAME repo and confirm it reports PASS with the
    real file actually recovered."""
    from scripts.osiris_offbox_restore_drill import run_drill

    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "osiris-20260908-163007.dump").write_bytes(b"real dump bytes" * 100)

    repo = tmp_path / "restic-repo"
    env = _restic_env()
    env["OSIRIS_VAULT"] = str(vault)

    backup = subprocess.run(["bash", str(BACKUP_SCRIPT), f"local:{repo}"],
                            capture_output=True, text=True, env=env, timeout=120)
    assert backup.returncode == 0, backup.stderr

    monkeypatch.setenv("RESTIC_PASSWORD", "test-password")
    fail = run_drill(f"local:{repo}")
    assert fail is None, fail


def test_backup_is_idempotent_on_a_second_run(tmp_path: Path) -> None:
    """`restic init` must never be re-attempted against an already-initialized repo —
    a second backup run against the same repository must still succeed."""
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "osiris-20260908-163007.dump").write_bytes(b"v1")

    repo = tmp_path / "restic-repo"
    env = _restic_env()
    env["OSIRIS_VAULT"] = str(vault)

    first = subprocess.run(["bash", str(BACKUP_SCRIPT), f"local:{repo}"],
                           capture_output=True, text=True, env=env, timeout=120)
    assert first.returncode == 0, first.stderr

    (vault / "osiris-20260908-163007.dump").write_bytes(b"v2, changed since the first backup")
    second = subprocess.run(["bash", str(BACKUP_SCRIPT), f"local:{repo}"],
                            capture_output=True, text=True, env=env, timeout=120)
    assert second.returncode == 0, second.stderr


def test_restore_drill_fails_loudly_against_an_empty_repository(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A `restic check`-clean but never-backed-up-to repository must NOT be reported
    as a pass — check-clean is not restorable-proof."""
    from scripts.osiris_offbox_restore_drill import run_drill

    repo = tmp_path / "empty-repo"
    env = _restic_env()
    subprocess.run(["restic", "init"], env={**env, "RESTIC_REPOSITORY": f"local:{repo}"},
                   check=True, capture_output=True, text=True)

    monkeypatch.setenv("RESTIC_PASSWORD", "test-password")
    fail = run_drill(f"local:{repo}")
    assert fail is not None
    assert "zero files" in fail or "snapshot" in fail.lower()


def test_restore_drill_fails_loudly_against_a_nonexistent_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts.osiris_offbox_restore_drill import run_drill

    monkeypatch.setenv("RESTIC_PASSWORD", "test-password")
    fail = run_drill("local:/nonexistent/osiris-offbox-repo-does-not-exist")
    assert fail is not None
    assert "check" in fail.lower() or "fail" in fail.lower()
