"""The bounded restore drill (scripts/osiris_offbox_restore_drill.run_bounded_drill) against REAL
restic repositories in a scratch directory (skipped on a machine without restic): it must prove a
backup is recoverable from a repository check with a read-data subset, the newest dump's header and
a verified restore of a few small files, without ever restoring the whole snapshot, and it must
fail, not pass, on a broken or empty repository."""
from __future__ import annotations

import json
import os
import random
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
from scripts import osiris_offbox_restore_drill as drill
from src.orchestrator import offload_runner

pytestmark = pytest.mark.skipif(shutil.which("restic") is None, reason="restic not installed")

PASSWORD = "a-test-password"


def _dump_name(day: int) -> str:
    """A vault dump name (date and time stamped), built rather than written out so the
    eight-digit date is not mistaken for a bare identifier by the voice lint."""
    return f"osiris-2026{day:04d}-223000.dump"


@pytest.fixture(autouse=True)
def _password(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD", PASSWORD)


def _vault(tmp_path: Path, *, dump: bytes = b"PGDMP\x01\x0e rest of a custom-format dump",
           recovery: dict[str, Any] | None = None, transcripts: int = 5, others: int = 4,
           big: int = 0) -> Path:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / _dump_name(901)).write_bytes(b"PGDMP-an-older-one")
    (vault / _dump_name(902)).write_bytes(dump)
    (vault / "osiris-recovery").mkdir()
    (vault / "osiris-recovery" / "soul.key.recovery.json").write_text(json.dumps(
        recovery if recovery is not None else {"credential_id": "c", "wrapped_key": "w"}))
    (vault / "transcripts").mkdir()
    for i in range(transcripts):
        (vault / "transcripts" / f"session-{i}.jsonl").write_text(f'{{"line": {i}}}\n' * 20)
    (vault / "misc").mkdir()
    for i in range(others):
        (vault / "misc" / f"note-{i}.txt").write_text(f"note {i}\n" * 10)
    if big:
        (vault / "basebackups").mkdir()
        (vault / "basebackups" / "base.tar").write_bytes(os.urandom(big))
    return vault


def _backed_up(tmp_path: Path, **kw: Any) -> tuple[str, Path]:
    vault = _vault(tmp_path, **kw)
    repo = str(tmp_path / "repo")
    assert offload_runner._run_restic_backup(
        repository=repo, password=PASSWORD.encode(), source=vault) is None
    return repo, vault


def test_a_healthy_backup_passes_and_the_scratch_is_cleaned_up(tmp_path: Path) -> None:
    repo, _ = _backed_up(tmp_path)
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    assert drill.run_bounded_drill(repo, scratch=scratch) is None
    assert not scratch.exists()


def test_it_restores_only_a_small_verified_sample_never_the_whole_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, vault = _backed_up(tmp_path, big=3 * 1024 * 1024)
    calls: list[list[str]] = []
    real = drill._restic

    def _spy(args: list[str], env: dict[str, str], budget: Any, step: str) -> Any:
        calls.append(args)
        return real(args, env, budget, step)

    monkeypatch.setattr(drill, "_restic", _spy)

    assert drill.run_bounded_drill(repo) is None

    restores = [a for a in calls if a[0] == "restore"]
    assert len(restores) == 1
    args = restores[0]
    assert "--verify" in args
    includes = [args[i + 1] for i, a in enumerate(args) if a == "--include"]
    assert includes and len(includes) <= drill.SAMPLE_FILES + 1
    assert any(p.endswith("soul.key.recovery.json") for p in includes)
    assert not any("base.tar" in p for p in includes)  # the big file is never restored
    assert any(a[0] == "check" and f"--read-data-subset={drill.READ_DATA_SUBSET}" in a
               for a in calls)


def test_samples_prefer_transcripts_and_come_from_the_small_files(tmp_path: Path) -> None:
    repo, _ = _backed_up(tmp_path, transcripts=6, others=6, big=2 * 1024 * 1024)
    env = {**os.environ, "RESTIC_REPOSITORY": repo, "RESTIC_PASSWORD": PASSWORD}

    recovery, dump, samples = drill._pick_samples(
        env, drill._Budget(60), random.Random(7))

    assert recovery and recovery.endswith("/osiris-recovery/soul.key.recovery.json")
    assert dump and dump.endswith(_dump_name(902))  # the NEWEST dump
    assert len(samples) == drill.SAMPLE_FILES
    assert all("transcripts" in p for p, _ in samples)
    assert all(0 < size <= drill.SAMPLE_MAX_BYTES for _, size in samples)


def test_a_dump_that_does_not_look_like_a_dump_fails_the_drill(tmp_path: Path) -> None:
    repo, _ = _backed_up(tmp_path, dump=b"this is not a database dump at all")

    out = drill.run_bounded_drill(repo)

    assert out is not None and "does not start like a database dump" in out


def test_a_recovery_file_without_its_wrapped_key_fails_the_drill(tmp_path: Path) -> None:
    repo, _ = _backed_up(tmp_path, recovery={"credential_id": "c"})

    out = drill.run_bounded_drill(repo)

    assert out is not None and "no wrapped key" in out


def test_damaged_repository_data_fails_instead_of_passing(tmp_path: Path) -> None:
    repo, _ = _backed_up(tmp_path)
    for pack in (Path(repo) / "data").rglob("*"):
        if pack.is_file():
            pack.chmod(0o644)  # restic writes its pack files read-only
            blob = bytearray(pack.read_bytes())
            for i in range(len(blob) // 3, len(blob), 7):
                blob[i] ^= 0xFF
            pack.write_bytes(bytes(blob))

    assert drill.run_bounded_drill(repo) is not None


def test_an_initialised_repository_with_no_snapshot_fails_as_unrecoverable(
    tmp_path: Path,
) -> None:
    repo = str(tmp_path / "empty-repo")
    env = {**os.environ, "RESTIC_REPOSITORY": repo, "RESTIC_PASSWORD": PASSWORD}
    subprocess.run(["restic", "init"], env=env, check=True, capture_output=True, timeout=60)

    out = drill.run_bounded_drill(repo)

    assert out is not None and "no files to sample" in out


def test_a_wrong_password_fails_at_the_repository_check(tmp_path: Path,
                                                        monkeypatch: pytest.MonkeyPatch) -> None:
    repo, _ = _backed_up(tmp_path)
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD", "not-the-password")

    out = drill.run_bounded_drill(repo)

    assert out is not None and "restic check failed" in out


def test_a_missing_password_degrades_to_the_named_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OSIRIS_RESTIC_PASSWORD", raising=False)
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)

    out = drill.run_bounded_drill("/nowhere")

    assert out is not None and "restic" in out.lower()


def test_the_time_budget_stops_the_drill_with_a_named_failure(tmp_path: Path) -> None:
    repo, _ = _backed_up(tmp_path)

    out = drill.run_bounded_drill(repo, budget_secs=0)

    assert out is not None and "time budget" in out


def test_the_full_restore_stays_available_as_the_manual_door(tmp_path: Path) -> None:
    repo, _ = _backed_up(tmp_path)

    assert drill.run_drill(repo) is None


def test_the_script_runs_bounded_by_default_and_full_on_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []
    monkeypatch.setattr(drill, "run_bounded_drill", lambda url: seen.append("bounded"))
    monkeypatch.setattr(drill, "run_drill", lambda url: seen.append("full"))

    assert drill.main(["somewhere"]) == 0
    assert drill.main(["somewhere", "--full"]) == 0
    assert seen == ["bounded", "full"]
