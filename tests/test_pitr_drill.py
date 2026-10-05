"""The PITR drill: the config-text builder is pure and tested directly; the real docker
orchestration was proven by an actual run against a real base backup, the same discipline
osiris_archive_wal.sh's own WAL-writing half was verified with."""
from __future__ import annotations

import subprocess
from pathlib import Path

from scripts.osiris_pitr_drill import CONTAINER, DRILL_NAME, postgresql_auto_conf_pitr, run_drill

# --- never the live cluster ---------------------------------------------------------

def test_run_drill_refuses_when_drill_name_equals_the_source_container() -> None:
    """The live incident this guards: a drill target that
    collides with the container being drilled generates real WAL against the thing
    it's supposed to leave untouched, so this must refuse outright, before touching
    docker at all."""
    fail = run_drill(
        Path("/nonexistent.tar.gz"), None, "message:1",
        drill_name=CONTAINER)
    assert fail is not None
    assert "REFUSING" in fail
    assert CONTAINER in fail


def test_run_drill_default_drill_name_never_equals_the_default_container() -> None:
    """The DEFAULT shape (no caller override) must already be safe: a regression
    guard on the two module constants themselves, not just the runtime check."""
    assert DRILL_NAME != CONTAINER


def test_no_target_time_sets_no_recovery_target() -> None:
    """The default drill mode: replay everything the archive can produce, promote at
    the end, with no recovery_target_time/recovery_target_action lines at all."""
    conf = postgresql_auto_conf_pitr("cp /wal/%f %p", None)
    assert "restore_command = 'cp /wal/%f %p'" in conf
    assert "recovery_target_time" not in conf
    assert "recovery_target_action" not in conf


def test_an_explicit_target_time_is_set_with_a_promote_action() -> None:
    conf = postgresql_auto_conf_pitr("cp /wal/%f %p", "2026-09-08T21:50:00+00:00")
    assert "recovery_target_time = '2026-09-08T21:50:00+00:00'" in conf
    assert "recovery_target_action = 'promote'" in conf


def test_a_custom_target_action_is_honored() -> None:
    conf = postgresql_auto_conf_pitr("cp /wal/%f %p", "2026-09-08T21:50:00+00:00",
                                     target_action="pause")
    assert "recovery_target_action = 'pause'" in conf


# --- the drill must wait for recovery to END, not for the server to answer -----------------

class _FakeRun:
    """A scripted `subprocess.run`: each call is matched on a fragment of its argv."""

    def __init__(self, replies: list[tuple[str, int, str]]) -> None:
        self.replies = list(replies)
        self.calls: list[list[str]] = []

    def __call__(self, cmd: list[str], **kw: object):  # noqa: ANN204
        import subprocess

        self.calls.append(cmd)
        fragment, code, out = self.replies.pop(0) if len(self.replies) > 1 else self.replies[0]
        assert fragment in " ".join(cmd), (fragment, cmd)
        return subprocess.CompletedProcess(cmd, code, stdout=out, stderr="")


def test_wait_for_recovery_end_keeps_waiting_while_the_server_answers_in_recovery(
    monkeypatch,
) -> None:
    """A server in archive recovery answers read-only queries as soon as it is consistent,
    long before the archive is fully replayed. The drill used to treat that first answer
    as 'ready' and looked for a post-backup marker in a half-replayed copy."""
    from scripts import osiris_pitr_drill as drill

    fake = _FakeRun([
        ("pg_is_in_recovery", 0, "t|1A/17A00028"),
        ("inspect", 0, "running"),
        ("pg_is_in_recovery", 0, "t|2B/7A000000"),
        ("inspect", 0, "running"),
        ("pg_is_in_recovery", 0, "f|2B/7B000000"),
    ])
    monkeypatch.setattr(drill.subprocess, "run", fake)
    monkeypatch.setattr(drill.time, "sleep", lambda s: None)
    assert drill._wait_for_recovery_end("scratch") is None
    assert sum("pg_is_in_recovery" in " ".join(c) for c in fake.calls) == 3


def test_wait_for_recovery_end_reports_a_stalled_recovery_with_its_last_position(
    monkeypatch,
) -> None:
    import subprocess

    from scripts import osiris_pitr_drill as drill

    def fake(cmd, **kw):  # noqa: ANN001, ANN202
        out = "running" if "inspect" in cmd else "t|2B/7A000000"
        return subprocess.CompletedProcess(cmd, 0, stdout=out, stderr="")

    monkeypatch.setattr(drill.subprocess, "run", fake)
    monkeypatch.setattr(drill.time, "sleep", lambda s: None)
    clock = iter([0.0, 1.0, 2.0, 99999.0, 99999.0])
    monkeypatch.setattr(drill.time, "monotonic", lambda: next(clock))
    fail = drill._wait_for_recovery_end("scratch")
    assert fail is not None and "had not finished" in fail and "2B/7A000000" in fail


def test_wait_for_recovery_end_names_a_container_that_died(monkeypatch) -> None:
    from scripts import osiris_pitr_drill as drill

    fake = _FakeRun([
        ("pg_is_in_recovery", 1, ""), ("inspect", 0, "exited"), ("logs", 0, "boom"),
    ])
    monkeypatch.setattr(drill.subprocess, "run", fake)
    monkeypatch.setattr(drill.time, "sleep", lambda s: None)
    fail = drill._wait_for_recovery_end("scratch")
    assert fail is not None and "stopped during recovery (exited)" in fail


def test_gather_does_not_copy_what_the_vault_already_holds(tmp_path: Path, monkeypatch) -> None:
    """The vault archive is mounted into the drill container and read in place; only a
    segment still staged inside the live container, and not yet in the vault, is copied."""
    import subprocess

    from scripts import osiris_pitr_drill as drill

    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "000000010000000100000001").write_bytes(b"v")
    scratch = tmp_path / "scratch"

    def fake_run(cmd, **kw):  # noqa: ANN001, ANN202
        if cmd[:3] == ["docker", "exec", "pg"] and cmd[3] == "ls":
            return subprocess.CompletedProcess(
                cmd, 0, stdout="000000010000000100000001\n000000010000000100000002\n", stderr="")
        if cmd[3] == "cat":
            kw["stdout"].write(b"staged")
            return subprocess.CompletedProcess(cmd, 0)
        raise AssertionError(cmd)

    monkeypatch.setattr(drill.subprocess, "run", fake_run)
    total = drill._gather_wal_segments("pg", vault, scratch)
    assert [p.name for p in scratch.iterdir()] == ["000000010000000100000002"]
    assert total == 2


def test_wait_for_archive_waits_for_the_archiver_to_pass_the_marker_segment(
    monkeypatch,
) -> None:
    from scripts import osiris_pitr_drill as drill

    fake = _FakeRun([("pg_stat_archiver", 0, "f"), ("pg_stat_archiver", 0, "t")])
    monkeypatch.setattr(drill.subprocess, "run", fake)
    monkeypatch.setattr(drill.time, "sleep", lambda s: None)
    assert drill._wait_for_archive("pg", "000000010000000100000009") is True
    assert len(fake.calls) == 2


def test_scratch_postgres_never_inherits_production_sizing() -> None:
    from scripts import osiris_pitr_drill as drill

    args = " ".join(drill.SCRATCH_POSTGRES_ARGS)
    assert "shared_buffers=512MB" in args and "autovacuum=off" in args
    # recovery aborts if max_connections is below the primary's (72 at last measure)
    assert "max_connections=72" in args
    assert drill.SCRATCH_MEMORY == "6g"


def test_soul_round_trip_samples_pages_never_sorts_the_whole_table(monkeypatch) -> None:
    """Sorting by random() reads every raw_line in a multi-gigabyte table and timed out on a
    cold restored copy; the check samples a few hundred random pages instead, and only a
    table too small to yield a sampled page falls back to a plain LIMIT."""
    import subprocess

    from scripts import osiris_pitr_drill as drill

    seen: list[str] = []

    def fake_run(cmd, **kw):  # noqa: ANN001, ANN202
        seen.append(cmd[-1])
        out = "" if "TABLESAMPLE" in cmd[-1] else b"legacy plain".hex()
        return subprocess.CompletedProcess(cmd, 0, stdout=out, stderr="")

    monkeypatch.setattr(drill.subprocess, "run", fake_run)
    monkeypatch.setattr(drill, "_soul_encryption_state", lambda: "pending")
    assert drill._soul_round_trip_check("scratch") is None   # plaintext only, unfinished: unproven
    assert "TABLESAMPLE SYSTEM" in seen[0] and "random()" not in " ".join(seen)
    assert "TABLESAMPLE" not in seen[1] and seen[1].endswith("LIMIT 50")


def test_soul_round_trip_decrypts_a_long_row_it_reads_back_as_hex(monkeypatch) -> None:
    """A row longer than 57 bytes used to come back from Postgres' base64 encoder split over
    several 76-character lines, each decoded on its own into an undecryptable fragment, so
    every real row failed the round trip. Hex is one line per row: a row encrypted under the
    configured key now passes, and a row encrypted under a different key still fails."""
    import subprocess

    from cryptography.fernet import Fernet, MultiFernet
    from scripts import osiris_pitr_drill as drill

    mine, other = Fernet(Fernet.generate_key()), Fernet(Fernet.generate_key())
    monkeypatch.setattr(
        "src.ingest.soul_crypto.get_soul_fernet", lambda *a, **k: MultiFernet([mine]))
    long_line = b'{"type":"assistant","message":"' + b"x" * 400 + b'"}'

    def reply(token: bytes):  # noqa: ANN202
        def fake_run(cmd, **kw):  # noqa: ANN001, ANN202
            return subprocess.CompletedProcess(cmd, 0, stdout=token.hex() + "\n", stderr="")
        return fake_run

    monkeypatch.setattr(drill.subprocess, "run", reply(mine.encrypt(long_line)))
    assert drill._soul_round_trip_check("scratch") is None
    monkeypatch.setattr(drill.subprocess, "run", reply(other.encrypt(long_line)))
    assert "round-trip FAILED" in (drill._soul_round_trip_check("scratch") or "")


def _sample_reply(monkeypatch, drill, rows: list[bytes]) -> None:  # noqa: ANN001
    import subprocess

    def fake_run(cmd, **kw):  # noqa: ANN001, ANN202
        return subprocess.CompletedProcess(
            cmd, 0, stdout="".join(r.hex() + "\n" for r in rows), stderr="")

    monkeypatch.setattr(drill.subprocess, "run", fake_run)


def test_soul_round_trip_with_rows_but_none_encrypted_fails_when_encryption_is_complete(
    monkeypatch,
) -> None:
    from scripts import osiris_pitr_drill as drill

    _sample_reply(monkeypatch, drill, [b'{"plain":"json line"}', b'{"another":"one"}'])
    monkeypatch.setattr(drill, "_soul_encryption_state", lambda: "complete")
    fail = drill._soul_round_trip_check("scratch")
    assert fail is not None and "proved NOTHING" in fail and "sampled 2 row(s)" in fail


def test_soul_round_trip_unproven_is_named_but_not_a_failure_when_encryption_is_unfinished(
    monkeypatch, capsys,
) -> None:
    from scripts import osiris_pitr_drill as drill

    for state in ("no_key", "pending", "running", "unknown"):
        _sample_reply(monkeypatch, drill, [b'{"plain":"json line"}'])
        monkeypatch.setattr(drill, "_soul_encryption_state", lambda state=state: state)
        assert drill._soul_round_trip_check("scratch") is None
        out = capsys.readouterr().out
        assert "UNPROVEN" in out and state in out


def test_soul_round_trip_on_an_empty_table_passes_quietly(monkeypatch, capsys) -> None:
    from scripts import osiris_pitr_drill as drill

    _sample_reply(monkeypatch, drill, [])
    monkeypatch.setattr(drill, "_soul_encryption_state", lambda: "complete")
    assert drill._soul_round_trip_check("scratch") is None
    assert "UNPROVEN" not in capsys.readouterr().out


def test_soul_encryption_state_reads_the_progress_record_and_never_raises(monkeypatch) -> None:
    from scripts import osiris_pitr_drill as drill

    monkeypatch.setattr("src.orchestrator.soul_encrypt_progress.read_progress",
                        lambda: {"state": "complete"})
    monkeypatch.setattr("src.ingest.soul_crypto.soul_key_status", lambda **k: {"present": True})
    assert drill._soul_encryption_state() == "complete"
    monkeypatch.setattr("src.ingest.soul_crypto.soul_key_status", lambda **k: {"present": False})
    assert drill._soul_encryption_state() == "no_key"

    def boom() -> dict:
        raise OSError("unreadable")

    monkeypatch.setattr("src.orchestrator.soul_encrypt_progress.read_progress", boom)
    assert drill._soul_encryption_state() == "unknown"


# --- a passing drill is the only thing that marks a base backup verified ----------------

def test_a_passing_drill_marks_the_newest_base_backup_verified_and_a_failing_one_does_not(
    tmp_path: Path, monkeypatch,  # noqa: ANN001
) -> None:
    from scripts import osiris_pitr_drill
    from scripts.osiris_prune_ladder import is_base_backup_verified

    bases = tmp_path / "basebackups"
    bases.mkdir()
    old = bases / "osiris-basebackup-20260901-000000.tar.gz"
    new = bases / "osiris-basebackup-20260908-000000.tar.gz"
    old.write_bytes(b"old")
    new.write_bytes(b"new")
    monkeypatch.setattr(osiris_pitr_drill, "pick_and_ensure_marker", lambda: "message:1")

    monkeypatch.setattr(osiris_pitr_drill, "run_drill", lambda *a, **k: "replay failed")
    assert osiris_pitr_drill.main(["--vault", str(tmp_path)]) == 1
    assert not is_base_backup_verified(new)

    monkeypatch.setattr(osiris_pitr_drill, "run_drill", lambda *a, **k: None)
    assert osiris_pitr_drill.main(["--vault", str(tmp_path)]) == 0
    assert is_base_backup_verified(new)
    assert not is_base_backup_verified(old)


# --- the archive holds segments raw or compressed; recovery must read both ---------------------

def _restore(command: str, name: str, target: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["sh", "-c", command.replace("%f", name).replace("%p", str(target))],
                          capture_output=True, text=True, timeout=30)


def test_restore_command_reads_a_compressed_segment_back_to_the_raw_bytes(tmp_path) -> None:
    import shutil

    import pytest

    if shutil.which("zstd") is None:
        pytest.skip("zstd not installed")
    from scripts.osiris_pitr_drill import restore_command_for

    vault, staged = tmp_path / "vault", tmp_path / "staged"
    vault.mkdir(), staged.mkdir()
    payload = b"segment bytes " * 1000
    subprocess.run(["zstd", "-q", "-o", str(vault / "000000010000000000000007.zst")],
                   input=payload, check=True, timeout=30)
    cmd = restore_command_for((vault, staged))
    out = tmp_path / "recovered"
    assert _restore(cmd, "000000010000000000000007", out).returncode == 0
    assert out.read_bytes() == payload


def test_restore_command_still_reads_a_raw_segment_and_walks_the_directories_in_order(
    tmp_path,
) -> None:
    from scripts.osiris_pitr_drill import restore_command_for

    vault, staged = tmp_path / "vault", tmp_path / "staged"
    vault.mkdir(), staged.mkdir()
    (vault / "000000010000000000000001").write_bytes(b"raw in the vault")
    (staged / "000000010000000000000002").write_bytes(b"raw in the staging copy")
    (staged / "00000002.history").write_bytes(b"1\t0/5000000\tno recovery target\n")
    cmd = restore_command_for((vault, staged))
    for name, expect in (("000000010000000000000001", b"raw in the vault"),
                         ("000000010000000000000002", b"raw in the staging copy"),
                         ("00000002.history", b"1\t0/5000000\tno recovery target\n")):
        out = tmp_path / ("got-" + name)
        assert _restore(cmd, name, out).returncode == 0
        assert out.read_bytes() == expect


def test_restore_command_fails_when_no_directory_has_the_segment(tmp_path) -> None:
    """A nonzero exit is how recovery learns the archive has ended; it must not be masked."""
    from scripts.osiris_pitr_drill import restore_command_for

    vault = tmp_path / "vault"
    vault.mkdir()
    assert _restore(restore_command_for((vault,)), "000000010000000000000099",
                    tmp_path / "x").returncode != 0


def test_gather_skips_a_segment_copy_that_is_still_being_written(tmp_path, monkeypatch) -> None:
    from scripts import osiris_pitr_drill as drill

    vault = tmp_path / "vault"
    vault.mkdir()

    def fake_run(cmd, **kw):  # noqa: ANN001, ANN202
        if cmd[3] == "ls":
            return subprocess.CompletedProcess(
                cmd, 0, stdout="000000010000000100000002.zst\n000000010000000100000003.zst.tmp\n",
                stderr="")
        kw["stdout"].write(b"staged")
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(drill.subprocess, "run", fake_run)
    drill._gather_wal_segments("pg", vault, tmp_path / "scratch")
    assert [p.name for p in (tmp_path / "scratch").iterdir()] == ["000000010000000100000002.zst"]
